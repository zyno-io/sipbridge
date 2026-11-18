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
	for _, transport := range []string{"tls", "udp"} {
		t.Run(transport, func(t *testing.T) { testResidentUASLocalReinvite(t, transport) })
	}
}

func testResidentUASLocalReinvite(t *testing.T, transport string) {
	e, peer, conn, ctx, session, writes, _ := residentControlFixture(t)
	incoming := stateTestRequest("INVITE", 1, "", "uas-local-reinvite")
	if transport == "udp" {
		wire := bytes.ReplaceAll(incoming.Wire, []byte("SIP/2.0/TLS"), []byte("SIP/2.0/UDP"))
		wire = bytes.ReplaceAll(wire, []byte(";transport=tls"), []byte(";transport=udp"))
		observed, observeErr := Observe(wire, false)
		if observeErr != nil {
			t.Fatal(observeErr)
		}
		incoming = observed
	}
	route, err := e.LocateRoute(ctx, incoming)
	if err != nil {
		t.Fatal(err)
	}
	flow := &pb.FlowRef{Ingress: e.cfg.Process, ConnectionId: "phone", SourceIp: "192.0.2.12", SourcePort: 50300, Listener: "sip.example.test:5061", Transport: transport}
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
	serverResponse := portable.RestoreMessage(<-writes, transport, "", "")
	writeControllerEnvelope(t, ctx, conn, &pb.Envelope{SessionId: session, CorrelationId: presented.CorrelationId, Payload: &pb.Envelope_Result{Result: &pb.OperationResult{Certainty: pb.OperationResult_ACCEPTED}}})
	if err = <-ingestDone; err != nil {
		t.Fatal(err)
	}
	remote, err := portable.NewDialog(portable.UAC, portable.RestoreMessage(incoming.Wire, transport, "", ""), serverResponse, portable.DefaultTimers())
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
	final, err := portable.BuildResponseWithHeaders(portable.RestoreMessage(localWire, transport, "", ""), 200, "OK", offer, "remote-tag", "sip:alice@phone.example.test:5061;transport="+transport, []portable.Header{{Name: "Content-Type", Value: "application/sdp"}})
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
		saved := previousTx.successfulACK
		if saved == nil {
			c.mu.Unlock()
			t.Fatal("previous live INVITE lost its successful ACK owner")
		}
		previousFinal, previousACK := saved.final, saved.ack.WireBytes()
		beforeReplay := proto.Clone(c.snapshot).(*pb.ContextSnapshot)
		beforeM := previousTx.native.Timers[portable.TimerM]
		c.mu.Unlock()
		duplicate, observeErr := Observe(previousFinal.WireBytes(), false)
		if observeErr != nil {
			t.Fatal(observeErr)
		}
		if handled, replayErr := e.completeResidentClient(ctx, flow, duplicate); replayErr != nil || !handled {
			t.Fatal("previous 200 replay failed", handled, replayErr)
		}
		if replayWire := <-writes; !bytes.Equal(replayWire, previousACK) {
			t.Fatal("previous 200 selected a different ACK")
		}
		select {
		case result := <-resultCh:
			t.Fatal("previous 200 completed the new INVITE", result)
		default:
		}
		c.mu.Lock()
		if c.dialogs[key].localTransaction != currentLocal || !proto.Equal(c.snapshot, beforeReplay) || previousTx.native.Timers[portable.TimerM] != beforeM {
			c.mu.Unlock()
			t.Fatal("previous ACK replay changed new dialog, snapshot or original M")
		}
		var uncertain *pb.FlowWrite
		if sequence == 4 {
			replay, stageErr := e.replayResidentSuccessfulACKLocked(c, previousTx, previousFinal, time.Now())
			c.mu.Unlock()
			if stageErr != nil || len(replay) != 1 {
				t.Fatal("uncertain replay admission", len(replay), stageErr)
			}
			uncertain = replay[0]
			reserved, reserveErr := e.Reserve(ctx, uncertain)
			if reserveErr != nil || reserved.Known || !bytes.Equal(reserved.Wire, previousACK) {
				t.Fatal("exact old ACK reserve", reserveErr)
			}
			if completeErr := e.Complete(ctx, uncertain, &pb.DeliveryResult{Effect: uncertain.Effect, Outcome: pb.DeliveryResult_UNKNOWN}); completeErr != nil {
				t.Fatal(completeErr)
			}
			if handled, replayErr := e.completeResidentClient(ctx, flow, duplicate); replayErr != nil || !handled {
				t.Fatal("uncertain old ACK classification", handled, replayErr)
			}
			select {
			case wire := <-writes:
				t.Fatalf("uncertainty permitted another old ACK: %q", wire)
			default:
			}
			c.mu.Lock()
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
		if uncertain != nil {
			c.mu.Lock()
			held := c.transactions[previousLocal]
			if held == nil || held.retired || held.successfulACK == nil || held.native.Phase != portable.Terminated {
				c.mu.Unlock()
				t.Fatal("M dropped actual unknown ACK owner")
			}
			c.mu.Unlock()
			if completeErr := e.Complete(ctx, uncertain, &pb.DeliveryResult{Effect: uncertain.Effect, Outcome: pb.DeliveryResult_WRITTEN}); completeErr != nil {
				t.Fatal(completeErr)
			}
			e.acknowledgeResidentOutput(uncertain)
		}
		c.mu.Lock()
		oldTransaction := c.transactions[previousLocal]
		retired := oldTransaction.retired
		if oldTransaction.successfulACK != nil {
			c.mu.Unlock()
			t.Fatal("old ACK tuple survived actual M and proof closure")
		}
		c.mu.Unlock()
		if !retired {
			t.Fatal("superseded re-INVITE did not release its procedure")
		}
		response, buildErr := portable.BuildResponseWithHeaders(portable.RestoreMessage(inviteWire, transport, "", ""), 200, "OK", nil, "remote-tag", "sip:alice@phone.example.test:5061;transport="+transport, nil)
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

	if transport == "udp" {
		var failedFinal portable.Message
		var failedACK []byte
		var failedKey string
		for _, sequence := range []uint64{6, 7} {
			cmd := &pb.Command{Context: route.Context, Sequence: sequence, Admission: localAdmission, Action: &pb.Command_DialogRequest{DialogRequest: &pb.DialogRequest{Method: "INVITE"}}}
			resultCh, errorCh := make(chan *pb.OperationResult, 1), make(chan error, 1)
			go func() {
				result, err := e.commandResidentClient(ctx, peer, cmd, fmt.Sprintf("failure-replay-%d", sequence), time.Now().Add(5*time.Second))
				resultCh <- result
				errorCh <- err
			}()
			requestWire := <-writes
			if sequence == 7 {
				duplicate, err := Observe(failedFinal.WireBytes(), false)
				if err != nil {
					t.Fatal(err)
				}
				if handled, err := e.completeResidentClient(ctx, flow, duplicate); err != nil || !handled {
					t.Fatal("previous non-2xx completion", handled, err)
				}
				if wire := <-writes; !bytes.Equal(wire, failedACK) {
					t.Fatal("previous failure selected another ACK")
				}
				select {
				case result := <-resultCh:
					t.Fatal("old failure completed current INVITE", result)
				default:
				}
			}
			status := 200
			if sequence == 6 {
				status = 486
			}
			final, err := portable.BuildResponseWithHeaders(portable.RestoreMessage(requestWire, transport, "", ""), status, reason(uint32(status)), nil, "remote-tag", "sip:alice@phone.example.test:5061;transport="+transport, nil)
			if err != nil {
				t.Fatal(err)
			}
			observed, err := Observe(final.WireBytes(), false)
			if err != nil {
				t.Fatal(err)
			}
			if handled, err := e.completeResidentClient(ctx, flow, observed); err != nil || !handled {
				t.Fatal("failure replay fixture final", handled, err)
			}
			ack := <-writes
			if result := <-resultCh; result == nil || result.Certainty != pb.OperationResult_COMPLETED || result.SipStatus != uint32(status) {
				t.Fatal("failure replay command result", result)
			}
			if err := <-errorCh; err != nil {
				t.Fatal(err)
			}
			if sequence == 6 {
				failedFinal, failedACK = final, ack
				c.mu.Lock()
				failedKey = c.dialogs[key].localTransaction
				c.mu.Unlock()
			}
		}
		c.mu.Lock()
		failed := c.transactions[failedKey]
		due := failed.native.Timers[portable.TimerD]
		if !due.Active {
			c.mu.Unlock()
			t.Fatal("old failure lost original D")
		}
		due.At = time.Now().Add(-time.Second).UnixMilli()
		failed.native.Timers[portable.TimerD] = due
		c.mu.Unlock()
		e.fireResidentDeadline(residentDeadline{key: residentDeadlineKey{kind: residentTransactionDeadline, contextID: route.Context.Id, record: failedKey, timer: portable.TimerD}, generation: due.Generation})
		c.mu.Lock()
		if !failed.retired || c.snapshot.Ended {
			c.mu.Unlock()
			t.Fatal("old failure retirement changed current call")
		}
		c.mu.Unlock()
	}

}

func TestResidentInitialWinnerACKSurvivesLocalInviteReplacement(t *testing.T) {
	e, peer, conn, ctx, session, writes, _ := residentControlFixture(t)
	flow := &pb.FlowRef{Ingress: e.cfg.Process, ConnectionId: "phone", SourceIp: "192.0.2.12", SourcePort: 50300, Listener: "sip.example.test:5061", Transport: "tls"}
	admission := &pb.CommandAdmission{DirectoryEpoch: "directory", ExpiresAtMs: uint64(time.Now().Add(5 * time.Second).UnixMilli()), SelectedFlow: flow}
	open := &pb.Command{Context: &pb.ContextRef{Owner: peer.Application(), Id: "initial-winner-replacement"}, Sequence: 1, Admission: admission,
		Action: &pb.Command_OpenUac{OpenUac: &pb.OpenUac{Uri: "sip:phone@example.test", Method: "INVITE", Transport: "tls", BindingId: "binding", Headers: []*pb.Header{{Name: "From", Value: "<sip:service@example.test>"}}}}}
	accepted, err := e.commandResidentClient(ctx, peer, open, "open-initial-winner", time.Now().Add(5*time.Second))
	if err != nil || accepted.Certainty != pb.OperationResult_ACCEPTED {
		t.Fatal("initial INVITE admission", accepted, err)
	}
	firstWire := <-writes
	final, err := portable.BuildResponseWithHeaders(portable.RestoreMessage(firstWire, "tls", "", ""), 200, "OK", nil, "remote", "sip:phone@example.test", nil)
	if err != nil {
		t.Fatal(err)
	}
	observed, err := Observe(final.WireBytes(), false)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { _, err := e.completeResidentClient(ctx, flow, observed); done <- err }()
	oldACK := <-writes
	frame := readControllerEnvelope(t, ctx, conn)
	if frame.GetDialogEvent().GetKind() != pb.DialogEvent_ANSWERED {
		t.Fatal("initial ANSWERED missing", frame)
	}
	writeControllerEnvelope(t, ctx, conn, &pb.Envelope{SessionId: session, CorrelationId: frame.CorrelationId, Payload: &pb.Envelope_Result{Result: &pb.OperationResult{Certainty: pb.OperationResult_ACCEPTED}}})
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	c, err := e.findContext(accepted.Snapshot.Context)
	if err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	key := c.selectedDialog
	initialKey := c.dialogs[key].localTransaction
	c.mu.Unlock()
	command := &pb.Command{Context: accepted.Snapshot.Context, Sequence: 2, Admission: admission, Action: &pb.Command_DialogRequest{DialogRequest: &pb.DialogRequest{Method: "INVITE"}}}
	resultCh, errorCh := make(chan *pb.OperationResult, 1), make(chan error, 1)
	go func() {
		result, err := e.commandResidentClient(ctx, peer, command, "replace-initial-winner", time.Now().Add(5*time.Second))
		resultCh <- result
		errorCh <- err
	}()
	nextWire := <-writes
	c.mu.Lock()
	prior := c.transactions[initialKey]
	if prior == nil || prior.successfulACK == nil {
		c.mu.Unlock()
		t.Fatal("initial winner did not transfer its live ACK")
	}
	before := proto.Clone(c.snapshot).(*pb.ContextSnapshot)
	beforeM := prior.native.Timers[portable.TimerM]
	currentKey := c.dialogs[key].localTransaction
	c.mu.Unlock()
	if handled, err := e.completeResidentClient(ctx, flow, observed); err != nil || !handled {
		t.Fatal("initial winner duplicate", handled, err)
	}
	if wire := <-writes; !bytes.Equal(wire, oldACK) {
		t.Fatal("initial winner duplicate changed its ACK")
	}
	select {
	case result := <-resultCh:
		t.Fatal("initial final completed the new INVITE", result)
	default:
	}
	c.mu.Lock()
	if !proto.Equal(c.snapshot, before) || c.dialogs[key].localTransaction != currentKey || prior.native.Timers[portable.TimerM] != beforeM {
		c.mu.Unlock()
		t.Fatal("initial replay changed current dialog or M")
	}
	c.mu.Unlock()
	nextFinal, err := portable.BuildResponseWithHeaders(portable.RestoreMessage(nextWire, "tls", "", ""), 200, "OK", nil, "remote", "sip:phone@example.test", nil)
	if err != nil {
		t.Fatal(err)
	}
	nextObserved, err := Observe(nextFinal.WireBytes(), false)
	if err != nil {
		t.Fatal(err)
	}
	if handled, err := e.completeResidentClient(ctx, flow, nextObserved); err != nil || !handled {
		t.Fatal("replacement final", handled, err)
	}
	<-writes
	if result := <-resultCh; result == nil || result.Certainty != pb.OperationResult_COMPLETED {
		t.Fatal("replacement result", result)
	}
	if err := <-errorCh; err != nil {
		t.Fatal(err)
	}
}
