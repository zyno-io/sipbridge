package engine

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/emiago/sipgo/sip"
	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/execution"
	"sipbridge/internal/state"
)

func freshPresentation(t *testing.T, f nativeOwnerFixture) (*pb.ContextRef, stateEngineInput, *execution.Committed) {
	t.Helper()
	message := stateTestRequest("OPTIONS", 1, "", "presentation")
	ref, err := f.engine.Locate(context.Background(), message)
	if err != nil {
		t.Fatal(err)
	}
	group, err := f.engine.group(ref)
	if err != nil {
		t.Fatal(err)
	}
	in := stateEngineInput{Kind: "ingest", Context: ref, Operation: "present-input", Application: f.app, ApplicationLeaseTerm: f.term,
		Ingest: &pb.OwnerIngest{Context: ref, Application: f.app, Flow: f.flow, Wire: message.Wire, PrincipalId: "phone", BindingId: "binding"}}
	result, committed, err := f.engine.applyCommitted(context.Background(), group, in.Operation, in, time.Now().Add(2*time.Second), false)
	if err != nil || result.Error != "" || committed == nil || len(committed.LocalPresentations) != 1 {
		t.Fatalf("fresh projection: %v %+v %+v", err, result, committed)
	}
	return ref, in, committed
}

func TestPresentationFreshOnlyAndNeverStored(t *testing.T) {
	f := newNativeOwnerFixtureBackend(t, state.MemoryBackend)
	ref, in, committed := freshPresentation(t, f)
	record, _ := stateTestLoad(t, f.engine, ref)
	for _, effect := range record.Payload.Effects {
		if effect.Kind == "present" {
			t.Fatal("presentation persisted")
		}
	}
	if len(record.Payload.LocalPresentations) != 0 {
		t.Fatal("projection persisted")
	}
	group, _ := f.engine.group(ref)
	_, replay, err := f.engine.applyCommitted(context.Background(), group, in.Operation, in, time.Now().Add(2*time.Second), false)
	if err != nil || replay != nil {
		t.Fatalf("replay projected: %v %+v", err, replay)
	}
	if stateDispatchableEffect(state.Effect{Kind: "present", Status: state.EffectPending, DeadlineMS: time.Now().Add(time.Minute).UnixMilli()}, time.Now().UnixMilli()) {
		t.Fatal("scheduler dispatches presentation")
	}
	if committed.LocalPresentations[0].DeadlineMS > time.Now().Add(2*time.Second).UnixMilli() {
		t.Fatal("deadline extended")
	}
}

func TestPresentationCallsControllerOnceWithoutResultCommit(t *testing.T) {
	f := newNativeOwnerFixtureBackend(t, state.MemoryBackend)
	ref, _, committed := freshPresentation(t, f)
	conn, _, ctx, _ := stateControllerPair(t, f.engine, f.app, &pb.ProcessRef{InstanceId: "draining", Epoch: "other"})
	done := make(chan error, 1)
	go func() {
		_, wire, err := conn.Read(ctx)
		if err != nil {
			done <- err
			return
		}
		var req pb.Envelope
		if err = proto.Unmarshal(wire, &req); err != nil {
			done <- err
			return
		}
		if req.GetRequest() == nil {
			done <- errors.New("missing presentation")
			return
		}
		reply := &pb.Envelope{SessionId: req.SessionId, CorrelationId: req.CorrelationId, Payload: &pb.Envelope_Result{Result: &pb.OperationResult{Certainty: pb.OperationResult_ACCEPTED}}}
		data, err := proto.Marshal(reply)
		if err == nil {
			err = conn.Write(ctx, websocket.MessageBinary, data)
		}
		done <- err
	}()
	before, _ := stateTestLoad(t, f.engine, ref)
	f.engine.presentCommitted(ctx, committed, callbackFresh)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	after, _ := stateTestLoad(t, f.engine, ref)
	if before.Revision != after.Revision {
		t.Fatal("successful presentation committed a result")
	}
	if len(committed.LocalPresentations) != 0 {
		t.Fatal("presentation not consumed")
	}
	f.engine.presentCommitted(ctx, committed, callbackFresh)
}

func TestPresentationFailureArbitratesSelectedFinal(t *testing.T) {
	for _, finalFirst := range []bool{false, true} {
		t.Run(fmt.Sprintf("final-first=%t", finalFirst), func(t *testing.T) {
			f := newNativeOwnerFixtureBackend(t, state.MemoryBackend)
			ref, _, projection := freshPresentation(t, f)
			requestID := projection.LocalPresentations[0].Request.RequestId
			group, _ := f.engine.group(ref)
			fail := func() {
				in := stateEngineInput{Kind: "presentation-failed", Context: ref, Operation: requestID}
				result, _, err := f.engine.applyCommitted(context.Background(), group, "failure", in, time.Now().Add(2*time.Second), false)
				if err != nil || result.Error != "" {
					t.Fatalf("failure transition: %+v %v", result, err)
				}
			}
			if finalFirst {
				cmd := &pb.Command{Context: ref, Sequence: 1, Action: &pb.Command_Respond{Respond: &pb.Respond{RequestId: requestID, Status: 200, Authority: f.responseAuthority(t, ref, requestID)}}}
				result := f.apply(t, ref, "respond", stateEngineInput{Kind: "command", Application: f.app, ApplicationLeaseTerm: f.term, Command: cmd})
				if result.Error != "" {
					t.Fatal(result.Error)
				}
			}
			fail()
			_, saved := stateTestLoad(t, f.engine, ref)
			tx := saved.Contexts[ref.Id].Transactions[saved.Contexts[ref.Id].Requests[requestID]]
			want := uint32(503)
			if finalFirst {
				want = 200
			}
			if tx.RespondStatus != want {
				t.Fatalf("selected response %d, want %d", tx.RespondStatus, want)
			}
			if tx.PresentDeadlineMS != 0 {
				t.Fatal("completed presentation retained deadline")
			}
			if !finalFirst {
				cmd := &pb.Command{Context: ref, Sequence: 1, Action: &pb.Command_Respond{Respond: &pb.Respond{RequestId: requestID, Status: 200, Authority: f.responseAuthority(t, ref, requestID)}}}
				result := f.apply(t, ref, "late-response", stateEngineInput{Kind: "command", Application: f.app, ApplicationLeaseTerm: f.term, Command: cmd})
				if result.Error == "" {
					t.Fatal("late response replaced failure")
				}
			}
		})
	}
}

func TestPresentationAfterFlowAdoptionKeepsOriginalDeadline(t *testing.T) {
	r, c, tx := advertiseFixture(t, "tls", sip.OPTIONS, "")
	tx.Flow = proto.Clone(c.Flow).(*pb.FlowRef)
	tx.Flow.ConnectionId = "new-flow"
	r.deadline = r.now + 1300
	originalDeadline := r.deadline
	if err := r.presentRequest(c, tx); err != nil {
		t.Fatal(err)
	}
	if !tx.DeferredPresent || tx.PresentDeadlineMS != originalDeadline || len(r.payload.LocalPresentations) != 0 {
		t.Fatal("presentation did not defer with original deadline")
	}
	var index int
	var effect state.Effect
	for i, candidate := range r.payload.Effects {
		if candidate.Kind == "adopt" {
			index, effect = i, candidate
			break
		}
	}
	if effect.ID == "" {
		t.Fatal("adoption not created")
	}
	var body stateEngineEffect
	if err := body.unmarshalBinary(effect.Payload, false); err != nil {
		t.Fatal(err)
	}
	proposal := body.Envelope.GetFlowAdoption()
	r.now += 700
	r.deadline = r.now + 2000
	r.in = stateEngineInput{Reply: &pb.Envelope{Payload: &pb.Envelope_FlowAdoptionResult{FlowAdoptionResult: &pb.FlowAdoptionResult{Receipt: &pb.ContextDelivery{Flow: tx.Flow, NativeRevision: proposal.NativeRevision, DirectoryEpoch: "directory"}}}}}
	if err := r.finishFlowAdoption(index, effect, body, c); err != nil {
		t.Fatal(err)
	}
	if len(r.payload.LocalPresentations) != 1 || r.payload.LocalPresentations[0].DeadlineMS != originalDeadline || tx.PresentDeadlineMS != 0 || tx.DeferredPresent {
		t.Fatal("adoption changed or retained presentation deadline")
	}
}

func TestPresentationAfterOwnerProofKeepsOriginalDeadline(t *testing.T) {
	r, c, tx := advertiseFixture(t, "tls", sip.OPTIONS, "")
	c.Snapshot.Kind, c.OwnerValidated, tx.OwnerValidated = pb.ContextSnapshot_TRANSACTION, false, false
	r.deadline = r.now + 1100
	originalDeadline := r.deadline
	if err := r.presentRequest(c, tx); err != nil {
		t.Fatal(err)
	}
	if !tx.OwnerDeferred || tx.PresentDeadlineMS != originalDeadline || len(r.payload.LocalPresentations) != 0 {
		t.Fatal("owner proof did not defer presentation")
	}
	r.now += 500
	r.deadline = r.now + 2000
	r.in = ownerStatusInput(t, r, c, c.Snapshot.Application, true)
	if err := r.ownerProbeResult(); err != nil {
		t.Fatal(err)
	}
	if len(r.payload.LocalPresentations) != 1 || r.payload.LocalPresentations[0].DeadlineMS != originalDeadline || tx.PresentDeadlineMS != 0 || tx.OwnerDeferred {
		t.Fatal("owner proof changed or retained presentation deadline")
	}
}

func TestPresentationRejectedReplyProducesGuarded503(t *testing.T) {
	f := newNativeOwnerFixtureBackend(t, state.MemoryBackend)
	ref, _, committed := freshPresentation(t, f)
	requestID := committed.LocalPresentations[0].Request.RequestId
	conn, _, ctx, _ := stateControllerPair(t, f.engine, f.app, &pb.ProcessRef{InstanceId: "draining", Epoch: "other"})
	done := make(chan error, 1)
	go func() {
		_, wire, err := conn.Read(ctx)
		if err != nil {
			done <- err
			return
		}
		var req pb.Envelope
		if err = proto.Unmarshal(wire, &req); err != nil {
			done <- err
			return
		}
		reply := &pb.Envelope{SessionId: req.SessionId, CorrelationId: req.CorrelationId, Payload: &pb.Envelope_Result{Result: &pb.OperationResult{Certainty: pb.OperationResult_ACCEPTED, Error: "refused"}}}
		data, err := proto.Marshal(reply)
		if err == nil {
			err = conn.Write(ctx, websocket.MessageBinary, data)
		}
		done <- err
	}()
	f.engine.presentCommitted(ctx, committed, callbackFresh)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	_, saved := stateTestLoad(t, f.engine, ref)
	c := saved.Contexts[ref.Id]
	tx := c.Transactions[c.Requests[requestID]]
	if tx.RespondStatus != 503 || tx.Native.LastResponse == nil {
		t.Fatal("rejected callback did not select native 503")
	}
}
