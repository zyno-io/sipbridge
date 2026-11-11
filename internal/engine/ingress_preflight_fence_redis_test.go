package engine

import (
	"bytes"
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/emiago/sipgo/sip"
	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/state"
)

// The original REGISTER has committed, but its business handler has not yet
// selected a final. A duplicate's preliminary read consumes its routing budget.
// Its fresh rejection must see the same Redis admission instead of sending an
// independent 503 and allowing the original handler to send a second final.
func TestRedisAdmittedRegisterSurvivesExpiredDuplicatePreflight(t *testing.T) {
	a := newNativeOwnerFixture(t)
	b := a.secondWorker(t)
	type arbitrationResult struct {
		callerErr error
		resultErr error
	}
	arbitrated := make(chan arbitrationResult, 1)
	f := newUDPOrderFixture(t, func(i *Ingress) {
		a.engine.cfg.Process = proto.Clone(i.process).(*pb.ProcessRef)
		i.stateWriter = a.engine
		i.stateAttempts = make(map[string]*physicalAttempt)
		i.stateCompletion = func(ctx context.Context, _ *pb.FlowRef, message *pb.SipMessage) (bool, error) {
			if message.Method != "REGISTER" {
				return false, nil
			}
			<-ctx.Done()
			return true, &stateReadFailure{ctx.Err()}
		}
		i.stateReject = func(ctx context.Context, flow *pb.FlowRef, message *pb.SipMessage, status uint32) error {
			callerErr := ctx.Err()
			err := b.engine.Reject(ctx, flow, message, status)
			arbitrated <- arbitrationResult{callerErr: callerErr, resultErr: err}
			return err
		}
	})
	// Establish the actual UDP FlowRef through the normal listener before
	// retaining admission. The barrier itself is only a fixture control reply.
	f.send(t, "ACK", "registration-flow-barrier", 1)
	barrier := f.next(t)
	flow := proto.Clone(barrier.GetIngress().Flow).(*pb.FlowRef)
	f.accept(t, barrier)
	waitUDPOrder(t, func() bool { count, lanes := f.pending(); return count == 0 && lanes == 0 })
	message, err := Observe(nativeProcessWire(t, "udp", sip.REGISTER, "preflight-admitted-register", 1, ""), false)
	if err != nil {
		t.Fatal(err)
	}
	ref, err := a.engine.Locate(context.Background(), message)
	if err != nil {
		t.Fatal(err)
	}
	result := a.apply(t, ref, "admit-before-preflight-timeout", stateEngineInput{Kind: "ingest", Application: a.app, ApplicationLeaseTerm: a.term,
		Ingest: &pb.OwnerIngest{Context: ref, Application: a.app, Flow: flow, Wire: message.Wire, PrincipalId: "phone", BindingId: "binding"}})
	if result.Error != "" {
		t.Fatal(result.Error)
	}
	_, before := stateTestLoad(t, a.engine, ref)
	c := before.Contexts[ref.Id]
	if c == nil || len(c.Requests) != 1 {
		t.Fatal("fixture did not retain exactly one admitted REGISTER")
	}
	var requestID, transactionID string
	for request, transaction := range c.Requests {
		requestID, transactionID = request, transaction
	}
	original := c.Transactions[transactionID]
	if original == nil || original.Native.LastResponse != nil || len(original.AdmittedFlowProofs) != 1 ||
		!proto.Equal(original.Flow, flow) || !bytes.Equal(original.Native.Origin.Wire, message.Wire) {
		t.Fatal("fixture did not retain an exact-flow, unanswered REGISTER admission")
	}
	if _, err = f.remote.WriteTo(message.Wire, f.listener.LocalAddr()); err != nil {
		t.Fatal(err)
	}
	select {
	case observed := <-arbitrated:
		if observed.callerErr != nil || observed.resultErr != nil {
			t.Fatalf("real native arbitration did not use a fresh budget: caller=%v result=%v", observed.callerErr, observed.resultErr)
		}
	case <-time.After(4 * time.Second):
		t.Fatal("expired duplicate never completed real native rejection arbitration")
	}
	waitUDPOrder(t, func() bool { count, lanes := f.pending(); return count == 0 && lanes == 0 })
	_, after := stateTestLoad(t, b.engine, ref)
	for _, candidate := range after.Contexts {
		if candidate.Rejection {
			t.Fatal("duplicate created a competing private refusal despite retained admission")
		}
	}
	retained := after.Contexts[ref.Id]
	if retained == nil || retained.Requests[requestID] != transactionID || retained.Transactions[transactionID] == nil ||
		retained.Transactions[transactionID].Native.LastResponse != nil ||
		!bytes.Equal(retained.Transactions[transactionID].Native.Origin.Wire, message.Wire) {
		t.Fatal("rejection arbitration changed the original unanswered REGISTER")
	}
	if err = f.remote.SetReadDeadline(time.Now().Add(100 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	buffer := make([]byte, 4096)
	if _, _, err = f.remote.ReadFrom(buffer); err == nil {
		t.Fatal("expired duplicate emitted a final before the admitted handler selected one")
	} else {
		var timeout net.Error
		if !errors.As(err, &timeout) || !timeout.Timeout() {
			t.Fatal(err)
		}
	}
	result = a.apply(t, ref, "original-register-challenge", stateEngineInput{Kind: "command", Application: a.app, ApplicationLeaseTerm: a.term,
		Command: &pb.Command{Context: ref, Sequence: 1, Action: &pb.Command_Respond{Respond: &pb.Respond{RequestId: requestID, Status: 401,
			Authority: a.responseAuthority(t, ref, requestID)}}}})
	if result.Error != "" {
		t.Fatalf("original admitted handler could not select its final: %s", result.Error)
	}
	record, group := stateTestLoad(t, a.engine, ref)
	tx := group.Contexts[ref.Id].Transactions[transactionID]
	if tx.Native.LastResponse == nil {
		t.Fatal("original REGISTER lost its selected final")
	}
	var selected state.Effect
	var body stateEngineEffect
	for _, effect := range record.Payload.Effects {
		var candidate stateEngineEffect
		if err = unmarshalRetainedFixture(effect.Payload, &candidate); err != nil {
			t.Fatal(err)
		}
		if effect.Kind == "deliver" && candidate.Transaction == transactionID && candidate.WireStatus == 401 {
			selected, body = effect, candidate
		}
	}
	if selected.ID == "" || !body.Native || body.Permit == nil || body.Authorization.GetAuthorityClass() != pb.EffectAuthorization_RESPONSE || !proto.Equal(body.Flow, flow) {
		t.Fatal("original final lost its exact-flow native response effect")
	}
	// Use the reducer-produced native permit, then perform a real Redis
	// reservation, UDP write and recorded completion through ingress.
	delivery := f.ingress.WriteCommitted(context.Background(), &pb.FlowWrite{Flow: flow, Effect: body.Authorization.Effect, Permit: body.Permit})
	if delivery.Outcome != pb.DeliveryResult_WRITTEN || !delivery.Recorded {
		t.Fatalf("original final was not durably delivered: %s", delivery)
	}
	if err = f.remote.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	n, _, err := f.remote.ReadFrom(buffer)
	if err != nil {
		t.Fatal("original admitted final was not received", err)
	}
	parsed, err := sip.ParseMessage(buffer[:n])
	response, ok := parsed.(*sip.Response)
	if err != nil || !ok || response.StatusCode != 401 || !bytes.Equal(buffer[:n], body.Wire) {
		t.Fatal("first final was not the original exact retained REGISTER challenge")
	}
}
