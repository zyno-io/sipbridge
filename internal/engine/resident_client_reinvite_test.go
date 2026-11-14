package engine

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
)

func TestResidentUASLocalReinviteLateOfferUsesLocalTransaction(t *testing.T) {
	e, peer, conn, ctx, session, writes, _ := residentControlFixture(t)
	incoming := stateTestRequest("INVITE", 1, "", "uas-local-reinvite")
	route, err := e.LocateRoute(ctx, incoming)
	if err != nil {
		t.Fatal(err)
	}
	flow := &pb.FlowRef{Ingress: e.cfg.Process, ConnectionId: "phone", SourceIp: "192.0.2.12", SourcePort: 50300, Listener: "sip.example.test:5061", Transport: "tls"}
	ingestDone := make(chan error, 1)
	go func() {
		_, err := e.ingest(ctx, peer, &pb.OwnerIngest{Context: route.Context, Application: peer.Application(), Flow: flow, Wire: incoming.Wire}, time.Now().Add(5*time.Second))
		ingestDone <- err
	}()
	presented := readControllerEnvelope(t, ctx, conn)
	request := presented.GetRequest()
	if request == nil {
		t.Fatal("initial INVITE not presented")
	}
	admission := &pb.CommandAdmission{DirectoryEpoch: "directory", ExpiresAtMs: uint64(time.Now().Add(5 * time.Second).UnixMilli())}
	response := &pb.Command{Context: route.Context, Sequence: 1, Admission: admission, Action: &pb.Command_Respond{Respond: &pb.Respond{RequestId: request.RequestId, Status: 200}}}
	result, err := e.command(ctx, peer, response, "answer", time.Now().Add(5*time.Second))
	if err != nil || result.Certainty != pb.OperationResult_COMPLETED {
		t.Fatal("initial answer failed", result, err)
	}
	serverResponse := portable.RestoreMessage(<-writes, "tls", "", "")
	writeControllerEnvelope(t, ctx, conn, &pb.Envelope{SessionId: session, CorrelationId: presented.CorrelationId, Payload: &pb.Envelope_Result{Result: &pb.OperationResult{Certainty: pb.OperationResult_ACCEPTED}}})
	if err = <-ingestDone; err != nil {
		t.Fatal(err)
	}
	remote, err := portable.NewDialog(portable.UAC, portable.RestoreMessage(incoming.Wire, "tls", "", ""), serverResponse, portable.DefaultTimers())
	if err != nil {
		t.Fatal(err)
	}
	initialACK, err := portable.BuildDialogRequest(remote, sip.ACK, 1, nil, "z9hG4bK-uas-original-ack")
	if err != nil {
		t.Fatal(err)
	}
	ackDone := make(chan error, 1)
	go func() {
		_, err := e.tryNativeCompletion(ctx, flow, &pb.SipMessage{Wire: initialACK.WireBytes()})
		ackDone <- err
	}()
	event := readControllerEnvelope(t, ctx, conn)
	if event.GetDialogEvent().GetKind() != pb.DialogEvent_ACKNOWLEDGED {
		t.Fatal("initial ACK event missing")
	}
	writeControllerEnvelope(t, ctx, conn, &pb.Envelope{SessionId: session, CorrelationId: event.CorrelationId, Payload: &pb.Envelope_Result{Result: &pb.OperationResult{Certainty: pb.OperationResult_ACCEPTED}}})
	if err = <-ackDone; err != nil {
		t.Fatal(err)
	}
	c, err := e.findContext(route.Context)
	if err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	key := c.selectedDialog
	original := c.dialogs[key]
	serverTransaction := original.transaction
	priorCompletionFlow := proto.Clone(original.completionFlow).(*pb.FlowRef)
	c.mu.Unlock()
	localAdmission := &pb.CommandAdmission{DirectoryEpoch: "directory", ExpiresAtMs: uint64(time.Now().Add(5 * time.Second).UnixMilli()), SelectedFlow: flow}
	local := &pb.Command{Context: route.Context, Sequence: 2, Admission: localAdmission, Action: &pb.Command_DialogRequest{DialogRequest: &pb.DialogRequest{Method: "INVITE"}}}
	localResult := make(chan *pb.OperationResult, 1)
	localError := make(chan error, 1)
	go func() {
		result, err := e.commandResidentClient(ctx, peer, local, "local-reinvite", time.Now().Add(5*time.Second))
		localResult <- result
		localError <- err
	}()
	localWire := <-writes
	c.mu.Lock()
	current := c.dialogs[key]
	if current.transaction != serverTransaction || current.localTransaction == "" || current.localTransaction == serverTransaction || !proto.Equal(current.completionFlow, priorCompletionFlow) {
		c.mu.Unlock()
		t.Fatal("local INVITE changed server transaction or lost completion flow")
	}
	localKey := current.localTransaction
	c.mu.Unlock()
	offer := []byte("v=0\r\no=peer 1 1 IN IP4 192.0.2.1\r\ns=-\r\nt=0 0\r\n")
	final, err := portable.BuildResponseWithHeaders(portable.RestoreMessage(localWire, "tls", "", ""), 200, "OK", offer, "remote-tag", "sip:alice@phone.example.test:5061;transport=tls", []portable.Header{{Name: "Content-Type", Value: "application/sdp"}})
	if err != nil {
		t.Fatal(err)
	}
	observed, err := Observe(final.WireBytes(), false)
	if err != nil {
		t.Fatal(err)
	}
	if handled, err := e.completeResidentClient(ctx, flow, observed); err != nil || !handled {
		t.Fatal("local response failed", err)
	}
	if result = <-localResult; result == nil || result.Certainty != pb.OperationResult_COMPLETED {
		t.Fatal("local INVITE result missing", result)
	}
	if err = <-localError; err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	current = c.dialogs[key]
	if current.transaction != serverTransaction || current.localTransaction != localKey || !proto.Equal(current.completionFlow, priorCompletionFlow) {
		c.mu.Unlock()
		t.Fatal("response lost dialog identity or completion flow")
	}
	c.mu.Unlock()
	answer := []byte("v=0\r\no=local 2 2 IN IP4 192.0.2.2\r\ns=-\r\nt=0 0\r\n")
	ack := &pb.Command{Context: route.Context, Sequence: 3, Admission: localAdmission, Action: &pb.Command_DialogRequest{DialogRequest: &pb.DialogRequest{Method: "ACK", Body: answer, Headers: []*pb.Header{{Name: "Content-Type", Value: "application/sdp"}}}}}
	ackResult, err := e.commandResidentClient(ctx, peer, ack, "local-answer-ack", time.Now().Add(5*time.Second))
	if err != nil || ackResult.Certainty != pb.OperationResult_COMPLETED {
		t.Fatal("local ACK answer failed", ackResult, err)
	}
	wire := <-writes
	parsed, err := sip.ParseMessage(wire)
	if err != nil || parsed.CSeq().MethodName != sip.ACK || !bytes.Equal(parsed.Body(), answer) {
		t.Fatal("wrong ACK answer", err)
	}
	previousLocal := localKey
	for _, sequence := range []uint64{4, 5} {
		command := &pb.Command{Context: route.Context, Sequence: sequence, Admission: localAdmission, Action: &pb.Command_DialogRequest{DialogRequest: &pb.DialogRequest{Method: "INVITE"}}}
		resultCh := make(chan *pb.OperationResult, 1)
		errorCh := make(chan error, 1)
		go func() {
			result, err := e.commandResidentClient(ctx, peer, command, fmt.Sprintf("sequential-reinvite-%d", sequence), time.Now().Add(5*time.Second))
			resultCh <- result
			errorCh <- err
		}()
		inviteWire := <-writes
		c.mu.Lock()
		current = c.dialogs[key]
		currentLocal := current.localTransaction
		previousTx := c.transactions[previousLocal]
		if previousTx == nil {
			c.mu.Unlock()
			t.Fatal("previous re-INVITE disappeared before its deadline")
		}
		due := previousTx.native.Timers[portable.TimerM]
		if !due.Active {
			c.mu.Unlock()
			t.Fatal("previous re-INVITE completion timer missing")
		}
		due.At = time.Now().Add(-time.Second).UnixMilli()
		previousTx.native.Timers[portable.TimerM] = due
		c.mu.Unlock()
		e.fireResidentDeadline(residentDeadline{key: residentDeadlineKey{kind: residentTransactionDeadline, contextID: route.Context.Id, record: previousLocal, timer: portable.TimerM}, generation: due.Generation})
		c.mu.Lock()
		retired := c.transactions[previousLocal].retired
		c.mu.Unlock()
		if !retired {
			t.Fatal("superseded re-INVITE did not release its procedure")
		}
		response, buildErr := portable.BuildResponseWithHeaders(portable.RestoreMessage(inviteWire, "tls", "", ""), 200, "OK", nil, "remote-tag", "sip:alice@phone.example.test:5061;transport=tls", nil)
		if buildErr != nil {
			t.Fatal(buildErr)
		}
		observed, observeErr := Observe(response.WireBytes(), false)
		if observeErr != nil {
			t.Fatal(observeErr)
		}
		if handled, responseErr := e.completeResidentClient(ctx, flow, observed); responseErr != nil || !handled {
			t.Fatal("sequential re-INVITE response failed", handled, responseErr)
		}
		ackWire := <-writes
		parsed, parseErr := sip.ParseMessage(ackWire)
		if parseErr != nil || parsed.CSeq().MethodName != sip.ACK {
			t.Fatal("sequential re-INVITE ACK missing", parseErr)
		}
		if result := <-resultCh; result == nil || result.Certainty != pb.OperationResult_COMPLETED {
			t.Fatal("sequential re-INVITE did not complete", result)
		}
		if err := <-errorCh; err != nil {
			t.Fatal(err)
		}
		previousLocal = currentLocal
	}

}
