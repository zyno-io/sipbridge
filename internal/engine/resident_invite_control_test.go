package engine

import (
	"context"
	"testing"
	"time"

	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
)

func TestResidentInitialInviteResponseAndACK(t *testing.T) {
	e, peer, conn, ctx, session, writes, _ := residentControlFixture(t)
	message := stateTestRequest("INVITE", 1, "", "resident-invite")
	route, err := e.LocateRoute(ctx, message)
	if err != nil {
		t.Fatal(err)
	}
	flow := &pb.FlowRef{Ingress: e.cfg.Process, ConnectionId: "phone", SourceIp: "192.0.2.12", SourcePort: 50300, Listener: "sip.example.test:5061", Transport: "tls"}
	ingestDone := make(chan error, 1)
	go func() {
		_, err := e.ingest(ctx, peer, &pb.OwnerIngest{Context: route.Context, Application: peer.Application(), Flow: flow, Wire: message.Wire}, time.Now().Add(2*time.Second))
		ingestDone <- err
	}()
	presented := readControllerEnvelope(t, ctx, conn)
	request := presented.GetRequest()
	if request == nil {
		t.Fatal("INVITE not presented")
	}
	command := &pb.Command{Context: request.Snapshot.Context, Sequence: 1, Admission: &pb.CommandAdmission{DirectoryEpoch: "directory", ExpiresAtMs: uint64(time.Now().Add(2 * time.Second).UnixMilli())}, Action: &pb.Command_Respond{Respond: &pb.Respond{RequestId: request.RequestId, Status: 200}}}
	writeControllerEnvelope(t, ctx, conn, &pb.Envelope{SessionId: session, CorrelationId: "respond", RemainingBudgetMs: 2000, Payload: &pb.Envelope_Command{Command: command}})
	result := readControllerEnvelope(t, ctx, conn).GetResult()
	if result == nil || result.Certainty != pb.OperationResult_COMPLETED {
		t.Fatal("INVITE response failed", result)
	}
	var wire []byte
	select {
	case wire = <-writes:
	case <-ctx.Done():
		t.Fatal("response missing")
	}
	writeControllerEnvelope(t, ctx, conn, &pb.Envelope{SessionId: session, CorrelationId: presented.CorrelationId, Payload: &pb.Envelope_Result{Result: &pb.OperationResult{Certainty: pb.OperationResult_ACCEPTED}}})
	if err = <-ingestDone; err != nil {
		t.Fatal(err)
	}
	origin := portable.Message{Wire: message.Wire, Transport: "tls"}
	response := portable.Message{Wire: wire, Transport: "tls"}
	remote, err := portable.NewDialog(portable.UAC, origin, response, portable.DefaultTimers())
	if err != nil {
		t.Fatal(err)
	}
	ack, err := portable.BuildDialogRequest(remote, sip.ACK, 1, nil, "z9hG4bK-resident-ack")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		handled, err := e.tryNativeCompletion(ctx, flow, &pb.SipMessage{Wire: ack.Wire})
		if !handled && err == nil {
			t.Error("ACK not handled")
		}
		done <- err
	}()
	event := readControllerEnvelope(t, ctx, conn)
	if event.GetDialogEvent().GetKind() != pb.DialogEvent_ACKNOWLEDGED {
		t.Fatal("ACK event missing", event)
	}
	writeControllerEnvelope(t, ctx, conn, &pb.Envelope{SessionId: session, CorrelationId: event.CorrelationId, Payload: &pb.Envelope_Result{Result: &pb.OperationResult{Certainty: pb.OperationResult_COMPLETED}}})
	if err = <-done; err != nil {
		t.Fatal(err)
	}
	reinvite, err := portable.BuildDialogRequest(remote, sip.INVITE, 2, nil, "z9hG4bK-resident-reinvite")
	if err != nil {
		t.Fatal(err)
	}
	reinviteFlow := proto.Clone(flow).(*pb.FlowRef)
	reinviteFlow.ConnectionId = "replacement-tls"
	renewDone := make(chan error, 1)
	go func() {
		_, err := e.ingest(ctx, peer, &pb.OwnerIngest{Context: route.Context, Application: peer.Application(), Flow: reinviteFlow, Wire: reinvite.Wire, Admission: pb.OwnerIngest_TLS_DIALOG_CONTINUITY}, time.Now().Add(2*time.Second))
		renewDone <- err
	}()
	adoption := readControllerEnvelope(t, ctx, conn)
	proposal := adoption.GetFlowAdoption()
	if proposal == nil {
		t.Fatal("replacement flow was not proposed")
	}
	delivery := &pb.ContextDelivery{Flow: reinviteFlow, NativeRevision: proposal.NativeRevision, DirectoryEpoch: "directory", RegistrationTicket: "ticket"}
	writeControllerEnvelope(t, ctx, conn, &pb.Envelope{SessionId: session, CorrelationId: adoption.CorrelationId, Payload: &pb.Envelope_FlowAdoptionResult{FlowAdoptionResult: &pb.FlowAdoptionResult{Receipt: delivery}}})
	renewal := readControllerEnvelope(t, ctx, conn)
	if renewal.GetRequest() == nil || renewal.GetRequest().Message.Method != "INVITE" {
		t.Fatal("re-INVITE not presented", renewal)
	}
	stateBefore, err := e.findContext(route.Context)
	if err != nil {
		t.Fatal(err)
	}
	stateBefore.mu.Lock()
	priorDialog := stateBefore.dialogs[stateBefore.selectedDialog]
	priorDialog.ackWritten = true
	if err = e.replaceResidentCompletionFlowLocked(stateBefore, priorDialog, reinviteFlow); err != nil {
		stateBefore.mu.Unlock()
		t.Fatal(err)
	}
	stateBefore.mu.Unlock()
	reject := &pb.Command{Context: route.Context, Sequence: 2, Admission: &pb.CommandAdmission{DirectoryEpoch: "directory", ExpiresAtMs: uint64(time.Now().Add(2 * time.Second).UnixMilli()), ExpectedDelivery: delivery}, Action: &pb.Command_Respond{Respond: &pb.Respond{RequestId: renewal.GetRequest().RequestId, Status: 488}}}
	writeControllerEnvelope(t, ctx, conn, &pb.Envelope{SessionId: session, CorrelationId: "reject-reinvite", RemainingBudgetMs: 2000, Payload: &pb.Envelope_Command{Command: reject}})
	rejectResult := readControllerEnvelope(t, ctx, conn).GetResult()
	if rejectResult == nil || rejectResult.Certainty != pb.OperationResult_COMPLETED {
		t.Fatal("re-INVITE rejection failed", rejectResult)
	}
	stateBefore.mu.Lock()
	preserved := stateBefore.dialogs[stateBefore.selectedDialog]
	metadataOK := preserved.ackWritten && proto.Equal(preserved.completionFlow, reinviteFlow)
	stateBefore.mu.Unlock()
	if !metadataOK {
		t.Fatal("re-INVITE response lost retained dialog completion metadata")
	}
	var rejectedWire []byte
	select {
	case rejectedWire = <-writes:
	case <-ctx.Done():
		t.Fatal("re-INVITE rejection missing")
	}
	rejectAck, err := portable.BuildNon2xxACK(portable.Message{Wire: reinvite.Wire, Transport: "tls"}, portable.Message{Wire: rejectedWire, Transport: "tls"})
	if err != nil {
		t.Fatal(err)
	}
	if handled, ackErr := e.tryNativeCompletion(ctx, reinviteFlow, &pb.SipMessage{Wire: rejectAck.Wire}); ackErr != nil || !handled {
		t.Fatal("re-INVITE rejection ACK failed", handled, ackErr)
	}
	writeControllerEnvelope(t, ctx, conn, &pb.Envelope{SessionId: session, CorrelationId: renewal.CorrelationId, Payload: &pb.Envelope_Result{Result: &pb.OperationResult{Certainty: pb.OperationResult_ACCEPTED}}})
	if err = <-renewDone; err != nil {
		t.Fatal(err)
	}
	failedInvite, err := portable.BuildDialogRequest(remote, sip.INVITE, 3, nil, "z9hG4bK-failed-presentation")
	if err != nil {
		t.Fatal(err)
	}
	failedDone := make(chan error, 1)
	go func() {
		_, err := e.ingest(ctx, peer, &pb.OwnerIngest{Context: route.Context, Application: peer.Application(), Flow: reinviteFlow, Wire: failedInvite.Wire}, time.Now().Add(2*time.Second))
		failedDone <- err
	}()
	failedPresentation := readControllerEnvelope(t, ctx, conn)
	if failedPresentation.GetRequest() == nil {
		t.Fatal("second re-INVITE not presented")
	}
	writeControllerEnvelope(t, ctx, conn, &pb.Envelope{SessionId: session, CorrelationId: failedPresentation.CorrelationId, Payload: &pb.Envelope_Result{Result: &pb.OperationResult{Certainty: pb.OperationResult_NOT_ACCEPTED}}})
	if err = <-failedDone; err != nil {
		t.Fatal(err)
	}
	var failedWire []byte
	select {
	case failedWire = <-writes:
	case <-ctx.Done():
		t.Fatal("failed presentation did not send 503")
	}
	failedAck, err := portable.BuildNon2xxACK(portable.Message{Wire: failedInvite.Wire, Transport: "tls"}, portable.Message{Wire: failedWire, Transport: "tls"})
	if err != nil {
		t.Fatal(err)
	}
	if handled, ackErr := e.tryNativeCompletion(ctx, reinviteFlow, &pb.SipMessage{Wire: failedAck.Wire}); ackErr != nil || !handled {
		t.Fatal("failed presentation ACK missing", handled, ackErr)
	}
	state, err := e.findContext(route.Context)
	if err != nil {
		t.Fatal(err)
	}
	state.mu.Lock()
	pending := state.dialogs[state.selectedDialog].native.RemoteInvite.State == portable.InvitePending
	state.mu.Unlock()
	if pending {
		t.Fatal("failed presentation left remote INVITE offer pending")
	}
	cancelInvite, err := portable.BuildDialogRequest(remote, sip.INVITE, 4, nil, "z9hG4bK-cancel-reinvite")
	if err != nil {
		t.Fatal(err)
	}
	cancelInviteDone := make(chan error, 1)
	go func() {
		_, err := e.ingest(ctx, peer, &pb.OwnerIngest{Context: route.Context, Application: peer.Application(), Flow: reinviteFlow, Wire: cancelInvite.Wire}, time.Now().Add(2*time.Second))
		cancelInviteDone <- err
	}()
	cancelPresented := readControllerEnvelope(t, ctx, conn)
	if cancelPresented.GetRequest() == nil {
		t.Fatal("cancelled re-INVITE not presented")
	}
	cancelRequest, err := portable.BuildCancel(cancelInvite)
	if err != nil {
		t.Fatal(err)
	}
	cancelDone := make(chan error, 1)
	go func() {
		handled, err := e.tryNativeCompletion(ctx, reinviteFlow, &pb.SipMessage{Wire: cancelRequest.Wire})
		if !handled && err == nil {
			t.Error("re-INVITE CANCEL not handled")
		}
		cancelDone <- err
	}()
	cancelEvent := readControllerEnvelope(t, ctx, conn)
	if cancelEvent.GetDialogEvent().GetKind() != pb.DialogEvent_CANCELLED {
		t.Fatal("re-INVITE CANCEL event missing", cancelEvent)
	}
	writeControllerEnvelope(t, ctx, conn, &pb.Envelope{SessionId: session, CorrelationId: cancelEvent.CorrelationId, Payload: &pb.Envelope_Result{Result: &pb.OperationResult{Certainty: pb.OperationResult_COMPLETED}}})
	if err = <-cancelDone; err != nil {
		t.Fatal(err)
	}
	var cancelFinal []byte
	for range 2 {
		select {
		case output := <-writes:
			parsed, parseErr := sip.ParseMessage(output)
			if parseErr != nil {
				t.Fatal(parseErr)
			}
			if parsed.(*sip.Response).StatusCode == 487 {
				cancelFinal = output
			}
		case <-ctx.Done():
			t.Fatal("re-INVITE CANCEL outputs missing")
		}
	}
	if len(cancelFinal) == 0 {
		t.Fatal("CANCEL did not produce 487")
	}
	cancelAck, err := portable.BuildNon2xxACK(cancelInvite, portable.Message{Wire: cancelFinal, Transport: "tls"})
	if err != nil {
		t.Fatal(err)
	}
	if handled, ackErr := e.tryNativeCompletion(ctx, reinviteFlow, &pb.SipMessage{Wire: cancelAck.Wire}); ackErr != nil || !handled {
		t.Fatal("cancelled re-INVITE ACK missing", handled, ackErr)
	}
	writeControllerEnvelope(t, ctx, conn, &pb.Envelope{SessionId: session, CorrelationId: cancelPresented.CorrelationId, Payload: &pb.Envelope_Result{Result: &pb.OperationResult{Certainty: pb.OperationResult_ACCEPTED}}})
	if err = <-cancelInviteDone; err != nil {
		t.Fatal(err)
	}
	freshInvite, err := portable.BuildDialogRequest(remote, sip.INVITE, 5, nil, "z9hG4bK-after-cancel")
	if err != nil {
		t.Fatal(err)
	}
	freshDone := make(chan error, 1)
	go func() {
		_, err := e.ingest(ctx, peer, &pb.OwnerIngest{Context: route.Context, Application: peer.Application(), Flow: reinviteFlow, Wire: freshInvite.Wire}, time.Now().Add(2*time.Second))
		freshDone <- err
	}()
	fresh := readControllerEnvelope(t, ctx, conn)
	if fresh.GetRequest() == nil {
		t.Fatal("fresh re-INVITE blocked after cancellation", fresh)
	}
	writeControllerEnvelope(t, ctx, conn, &pb.Envelope{SessionId: session, CorrelationId: fresh.CorrelationId, Payload: &pb.Envelope_Result{Result: &pb.OperationResult{Certainty: pb.OperationResult_NOT_ACCEPTED}}})
	if err = <-freshDone; err != nil {
		t.Fatal(err)
	}
	select {
	case <-writes:
	case <-ctx.Done():
		t.Fatal("fresh re-INVITE response missing")
	}
	bye, err := portable.BuildDialogRequest(remote, sip.BYE, 6, nil, "z9hG4bK-resident-bye")
	if err != nil {
		t.Fatal(err)
	}
	byeDone := make(chan error, 1)
	go func() {
		handled, err := e.tryNativeCompletion(ctx, reinviteFlow, &pb.SipMessage{Wire: bye.Wire})
		if !handled && err == nil {
			t.Error("BYE not handled natively")
		}
		byeDone <- err
	}()
	ended := readControllerEnvelope(t, ctx, conn)
	if ended.GetDialogEvent().GetKind() != pb.DialogEvent_ENDED {
		t.Fatal("BYE end event missing", ended)
	}
	writeControllerEnvelope(t, ctx, conn, &pb.Envelope{SessionId: session, CorrelationId: ended.CorrelationId, Payload: &pb.Envelope_Result{Result: &pb.OperationResult{Certainty: pb.OperationResult_COMPLETED}}})
	if err = <-byeDone; err != nil {
		t.Fatal(err)
	}
	select {
	case wire = <-writes:
		if len(wire) == 0 {
			t.Fatal("BYE response empty")
		}
	case <-ctx.Done():
		t.Fatal("BYE response missing")
	}

}

func TestResidentCancelSelectsNative487WithoutPresentation(t *testing.T) {
	e, peer, conn, ctx, session, writes, _ := residentControlFixture(t)
	physicalFlows := make(chan *pb.FlowRef, 4)
	writer := e.cfg.WriteCommitted
	e.cfg.WriteCommitted = func(ctx context.Context, write *pb.FlowWrite) *pb.DeliveryResult {
		physicalFlows <- proto.Clone(write.Flow).(*pb.FlowRef)
		return writer(ctx, write)
	}
	message := stateTestRequest("INVITE", 1, "", "resident-cancel")
	route, err := e.LocateRoute(ctx, message)
	if err != nil {
		t.Fatal(err)
	}
	flow := &pb.FlowRef{Ingress: e.cfg.Process, ConnectionId: "phone", SourceIp: "192.0.2.12", SourcePort: 50300, Listener: "sip.example.test:5061", Transport: "tls"}
	ingestDone := make(chan error, 1)
	go func() {
		_, err := e.ingest(ctx, peer, &pb.OwnerIngest{Context: route.Context, Application: peer.Application(), Flow: flow, Wire: message.Wire}, time.Now().Add(2*time.Second))
		ingestDone <- err
	}()
	presented := readControllerEnvelope(t, ctx, conn)
	if presented.GetRequest() == nil {
		t.Fatal("INVITE not presented")
	}
	cancel, err := portable.BuildCancel(portable.Message{Wire: message.Wire, Transport: "tls"})
	if err != nil {
		t.Fatal(err)
	}
	replacement := proto.Clone(flow).(*pb.FlowRef)
	replacement.ConnectionId = "cancel-replacement"
	cancelDone := make(chan error, 1)
	go func() {
		handled, err := e.tryNativeCompletion(ctx, replacement, &pb.SipMessage{Wire: cancel.Wire})
		if !handled && err == nil {
			t.Error("CANCEL not handled")
		}
		cancelDone <- err
	}()
	event := readControllerEnvelope(t, ctx, conn)
	if event.GetDialogEvent().GetKind() != pb.DialogEvent_CANCELLED {
		t.Fatal("CANCEL event missing", event)
	}
	writeControllerEnvelope(t, ctx, conn, &pb.Envelope{SessionId: session, CorrelationId: event.CorrelationId, Payload: &pb.Envelope_Result{Result: &pb.OperationResult{Certainty: pb.OperationResult_COMPLETED}}})
	if err = <-cancelDone; err != nil {
		t.Fatal(err)
	}
	statuses := map[int]bool{}
	responseFlows := map[int]*pb.FlowRef{}
	for range 2 {
		select {
		case wire := <-writes:
			parsed, err := sip.ParseMessage(wire)
			if err != nil {
				t.Fatal(err)
			}
			status := parsed.(*sip.Response).StatusCode
			statuses[status] = true
			responseFlows[status] = <-physicalFlows
		case <-ctx.Done():
			t.Fatal("CANCEL responses missing")
		}
	}
	if !proto.Equal(responseFlows[200], replacement) || !proto.Equal(responseFlows[487], flow) {
		t.Fatal("paired CANCEL responses used incorrect return flows")
	}
	if !statuses[200] || !statuses[487] {
		t.Fatal("wrong CANCEL responses", statuses)
	}
	writeControllerEnvelope(t, ctx, conn, &pb.Envelope{SessionId: session, CorrelationId: presented.CorrelationId, Payload: &pb.Envelope_Result{Result: &pb.OperationResult{Certainty: pb.OperationResult_ACCEPTED}}})
	if err = <-ingestDone; err != nil {
		t.Fatal(err)
	}
}

func TestResidentOwnerLossSendsByeAfterAcknowledgedInvite(t *testing.T) {
	e, peer, conn, ctx, session, writes, _ := residentControlFixture(t)
	message := stateTestRequest("INVITE", 1, "", "owner-loss")
	route, err := e.LocateRoute(ctx, message)
	if err != nil {
		t.Fatal(err)
	}
	flow := &pb.FlowRef{Ingress: e.cfg.Process, ConnectionId: "phone", SourceIp: "192.0.2.12", SourcePort: 50300, Listener: "sip.example.test:5061", Transport: "tls"}
	done := make(chan error, 1)
	go func() {
		_, err := e.ingest(ctx, peer, &pb.OwnerIngest{Context: route.Context, Application: peer.Application(), Flow: flow, Wire: message.Wire}, time.Now().Add(2*time.Second))
		done <- err
	}()
	presented := readControllerEnvelope(t, ctx, conn)
	request := presented.GetRequest()
	if request == nil {
		t.Fatal("INVITE not presented")
	}
	response := &pb.Command{Context: route.Context, Sequence: 1, Admission: &pb.CommandAdmission{DirectoryEpoch: "directory", ExpiresAtMs: uint64(time.Now().Add(2 * time.Second).UnixMilli())}, Action: &pb.Command_Respond{Respond: &pb.Respond{RequestId: request.RequestId, Status: 200}}}
	writeControllerEnvelope(t, ctx, conn, &pb.Envelope{SessionId: session, CorrelationId: "owner-response", RemainingBudgetMs: 2000, Payload: &pb.Envelope_Command{Command: response}})
	if result := readControllerEnvelope(t, ctx, conn).GetResult(); result == nil || result.Certainty != pb.OperationResult_COMPLETED {
		t.Fatal("response failed", result)
	}
	var wire []byte
	select {
	case wire = <-writes:
	case <-ctx.Done():
		t.Fatal("response missing")
	}
	writeControllerEnvelope(t, ctx, conn, &pb.Envelope{SessionId: session, CorrelationId: presented.CorrelationId, Payload: &pb.Envelope_Result{Result: &pb.OperationResult{Certainty: pb.OperationResult_ACCEPTED}}})
	if err = <-done; err != nil {
		t.Fatal(err)
	}
	remote, err := portable.NewDialog(portable.UAC, portable.Message{Wire: message.Wire, Transport: "tls"}, portable.Message{Wire: wire, Transport: "tls"}, portable.DefaultTimers())
	if err != nil {
		t.Fatal(err)
	}
	ack, err := portable.BuildDialogRequest(remote, sip.ACK, 1, nil, "z9hG4bK-owner-loss-ack")
	if err != nil {
		t.Fatal(err)
	}
	replacement := proto.Clone(flow).(*pb.FlowRef)
	replacement.ConnectionId = "reconnected"
	ackDone := make(chan error, 1)
	go func() {
		_, err := e.tryNativeCompletion(ctx, replacement, &pb.SipMessage{Wire: ack.Wire})
		ackDone <- err
	}()
	event := readControllerEnvelope(t, ctx, conn)
	if event.GetDialogEvent().GetKind() != pb.DialogEvent_ACKNOWLEDGED {
		t.Fatal("ACK event missing")
	}
	writeControllerEnvelope(t, ctx, conn, &pb.Envelope{SessionId: session, CorrelationId: event.CorrelationId, Payload: &pb.Envelope_Result{Result: &pb.OperationResult{Certainty: pb.OperationResult_COMPLETED}}})
	if err = <-ackDone; err != nil {
		t.Fatal(err)
	}
	handled, err := e.tryNativeCompletion(ctx, flow, &pb.SipMessage{Wire: ack.Wire})
	if err != nil || !handled {
		t.Fatal("duplicate ACK not handled", handled, err)
	}
	c, err := e.findContext(route.Context)
	if err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	retained := c.dialogs[c.selectedDialog].completionFlow
	published := proto.Clone(c.flow).(*pb.FlowRef)
	c.mu.Unlock()
	if !proto.Equal(retained, replacement) || !proto.Equal(published, flow) {
		t.Fatal("duplicate ACK displaced accepted flow or published a route")
	}
	e.expireResidentOwner(peer.Application(), peer.ApplicationLease().Term)
	select {
	case bye := <-writes:
		parsed, parseErr := sip.ParseMessage(bye)
		if parseErr != nil {
			t.Fatal(parseErr)
		}
		request, ok := parsed.(*sip.Request)
		if !ok || request.Method != sip.BYE {
			t.Fatal("owner loss did not send cleanup BYE")
		}
	case <-ctx.Done():
		t.Fatal("owner cleanup BYE missing")
	}
}

func TestResidentUASAckTimeoutSendsByeAndByeTimeoutEndsDialog(t *testing.T) {
	e, peer, conn, ctx, session, writes, _ := residentControlFixture(t)
	message := stateTestRequest("INVITE", 1, "", "ack-timeout")
	route, err := e.LocateRoute(ctx, message)
	if err != nil {
		t.Fatal(err)
	}
	flow := &pb.FlowRef{Ingress: e.cfg.Process, ConnectionId: "phone", SourceIp: "192.0.2.12", SourcePort: 50300, Listener: "sip.example.test:5061", Transport: "tls"}
	done := make(chan error, 1)
	go func() {
		_, err := e.ingest(ctx, peer, &pb.OwnerIngest{Context: route.Context, Application: peer.Application(), Flow: flow, Wire: message.Wire}, time.Now().Add(2*time.Second))
		done <- err
	}()
	presented := readControllerEnvelope(t, ctx, conn)
	if presented.GetRequest() == nil {
		t.Fatal("INVITE not presented")
	}
	cmd := &pb.Command{Context: route.Context, Sequence: 1, Admission: &pb.CommandAdmission{DirectoryEpoch: "directory", ExpiresAtMs: uint64(time.Now().Add(2 * time.Second).UnixMilli())}, Action: &pb.Command_Respond{Respond: &pb.Respond{RequestId: presented.GetRequest().RequestId, Status: 200}}}
	writeControllerEnvelope(t, ctx, conn, &pb.Envelope{SessionId: session, CorrelationId: "final", RemainingBudgetMs: 2000, Payload: &pb.Envelope_Command{Command: cmd}})
	if result := readControllerEnvelope(t, ctx, conn).GetResult(); result == nil || result.Certainty != pb.OperationResult_COMPLETED {
		t.Fatal("INVITE final failed", result)
	}
	select {
	case <-writes:
	case <-ctx.Done():
		t.Fatal("INVITE final missing")
	}
	writeControllerEnvelope(t, ctx, conn, &pb.Envelope{SessionId: session, CorrelationId: presented.CorrelationId, Payload: &pb.Envelope_Result{Result: &pb.OperationResult{Certainty: pb.OperationResult_ACCEPTED}}})
	if err = <-done; err != nil {
		t.Fatal(err)
	}
	c, err := e.findContext(route.Context)
	if err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	dialogKey := c.selectedDialog
	d := c.dialogs[dialogKey]
	ackDeadline := d.native.Timers[portable.TimerACK]
	ackDeadline.At = time.Now().Add(-time.Second).UnixMilli()
	d.native.Timers[portable.TimerACK] = ackDeadline
	c.mu.Unlock()
	e.fireResidentDeadline(residentDeadline{key: residentDeadlineKey{kind: residentDialogDeadline, contextID: route.Context.Id, record: dialogKey, timer: portable.TimerACK}, generation: ackDeadline.Generation})
	var bye []byte
	select {
	case bye = <-writes:
	case <-ctx.Done():
		t.Fatal("ACK timeout did not send cleanup BYE")
	}
	parsed, err := sip.ParseMessage(bye)
	if err != nil {
		t.Fatal(err)
	}
	request, ok := parsed.(*sip.Request)
	if !ok || request.Method != sip.BYE {
		t.Fatal("ACK timeout output was not BYE")
	}
	key, err := sip.ClientTxKeyMake(request)
	if err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	tx := c.transactions[key]
	if tx == nil {
		c.mu.Unlock()
		t.Fatal("cleanup BYE transaction missing")
	}
	deadline := tx.native.Timers[portable.TimerF]
	deadline.At = time.Now().Add(-time.Second).UnixMilli()
	tx.native.Timers[portable.TimerF] = deadline
	c.mu.Unlock()
	e.fireResidentDeadline(residentDeadline{key: residentDeadlineKey{kind: residentTransactionDeadline, contextID: route.Context.Id, record: key, timer: portable.TimerF}, generation: deadline.Generation})
	event := readControllerEnvelope(t, ctx, conn)
	if event.GetDialogEvent().GetKind() != pb.DialogEvent_ENDED {
		t.Fatal("BYE timeout did not end dialog", event)
	}
	writeControllerEnvelope(t, ctx, conn, &pb.Envelope{SessionId: session, CorrelationId: event.CorrelationId, Payload: &pb.Envelope_Result{Result: &pb.OperationResult{Certainty: pb.OperationResult_COMPLETED}}})
	c.mu.Lock()
	ended := c.snapshot.Ended && c.dialogs[dialogKey].native.Phase == portable.DialogEnded
	c.mu.Unlock()
	if !ended {
		t.Fatal("BYE timeout left dialog live")
	}
}

func TestResidentInitialInviteTimeoutReportsFailure(t *testing.T) {
	e, peer, conn, ctx, session, writes, _ := residentControlFixture(t)
	flow := &pb.FlowRef{Ingress: e.cfg.Process, ConnectionId: "phone", SourceIp: "192.0.2.12", SourcePort: 50300, Listener: "sip.example.test:5061", Transport: "tls"}
	command := &pb.Command{Context: &pb.ContextRef{Owner: peer.Application(), Id: "timeout-invite"}, Sequence: 1, Admission: &pb.CommandAdmission{DirectoryEpoch: "directory", ExpiresAtMs: uint64(time.Now().Add(5 * time.Second).UnixMilli()), SelectedFlow: flow}, Action: &pb.Command_OpenUac{OpenUac: &pb.OpenUac{Uri: "sip:phone@example.test", Transport: "tls", BindingId: "binding", Headers: []*pb.Header{{Name: "From", Value: "<sip:service@example.test>"}}}}}
	result, err := e.commandResidentClient(ctx, peer, command, "open-timeout", time.Now().Add(5*time.Second))
	if err != nil || result.Certainty != pb.OperationResult_ACCEPTED {
		t.Fatal("INVITE not admitted", result, err)
	}
	var initialWire []byte
	select {
	case initialWire = <-writes:
	case <-ctx.Done():
		t.Fatal("INVITE output missing")
	}
	c, err := e.findContext(result.Snapshot.Context)
	if err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	var tx *residentTransaction
	for _, candidate := range c.transactions {
		if candidate.native.Family == portable.InviteClient {
			tx = candidate
			break
		}
	}
	if tx == nil {
		c.mu.Unlock()
		t.Fatal("INVITE transaction missing")
	}
	key := tx.native.Key
	due := tx.native.Timers[portable.TimerB]
	due.At = time.Now().Add(-time.Second).UnixMilli()
	tx.native.Timers[portable.TimerB] = due
	c.mu.Unlock()
	e.fireResidentDeadline(residentDeadline{key: residentDeadlineKey{kind: residentTransactionDeadline, contextID: result.Snapshot.Context.Id, record: key, timer: portable.TimerB}, generation: due.Generation})
	event := readControllerEnvelope(t, ctx, conn)
	if event.GetDialogEvent().GetKind() != pb.DialogEvent_FAILED {
		t.Fatal("INVITE timeout did not report failure", event)
	}
	writeControllerEnvelope(t, ctx, conn, &pb.Envelope{SessionId: session, CorrelationId: event.CorrelationId, Payload: &pb.Envelope_Result{Result: &pb.OperationResult{Certainty: pb.OperationResult_COMPLETED}}})
	c.mu.Lock()
	ended := c.snapshot.Ended
	c.mu.Unlock()
	if !ended {
		t.Fatal("INVITE timeout left context live")
	}
	late, err := portable.BuildResponseWithHeaders(portable.Message{Wire: initialWire, Transport: "tls"}, 200, "OK", nil, "late-fork", "sip:phone@example.test", nil)
	if err != nil {
		t.Fatal(err)
	}
	handled, err := e.tryNativeCompletion(ctx, flow, &pb.SipMessage{Wire: late.Wire})
	if err != nil || !handled {
		t.Fatal("late successful fork not handled", handled, err)
	}
	for _, want := range []sip.RequestMethod{sip.ACK, sip.BYE} {
		select {
		case wire := <-writes:
			parsed, parseErr := sip.ParseMessage(wire)
			if parseErr != nil {
				t.Fatal(parseErr)
			}
			request, ok := parsed.(*sip.Request)
			if !ok || request.Method != want {
				t.Fatalf("late fork expected %s", want)
			}
		case <-ctx.Done():
			t.Fatal("late fork cleanup missing")
		}
	}

}

func TestResidentOwnerLossDefersUASByeUntilACK(t *testing.T) {
	e, peer, conn, ctx, session, writes, _ := residentControlFixture(t)
	message := stateTestRequest("INVITE", 1, "", "owner-late-ack")
	route, err := e.LocateRoute(ctx, message)
	if err != nil {
		t.Fatal(err)
	}
	flow := &pb.FlowRef{Ingress: e.cfg.Process, ConnectionId: "phone", SourceIp: "192.0.2.12", SourcePort: 50300, Listener: "sip.example.test:5061", Transport: "tls"}
	done := make(chan error, 1)
	go func() {
		_, err := e.ingest(ctx, peer, &pb.OwnerIngest{Context: route.Context, Application: peer.Application(), Flow: flow, Wire: message.Wire}, time.Now().Add(2*time.Second))
		done <- err
	}()
	presented := readControllerEnvelope(t, ctx, conn)
	if presented.GetRequest() == nil {
		t.Fatal("INVITE not presented")
	}
	cmd := &pb.Command{Context: route.Context, Sequence: 1, Admission: &pb.CommandAdmission{DirectoryEpoch: "directory", ExpiresAtMs: uint64(time.Now().Add(2 * time.Second).UnixMilli())}, Action: &pb.Command_Respond{Respond: &pb.Respond{RequestId: presented.GetRequest().RequestId, Status: 200}}}
	writeControllerEnvelope(t, ctx, conn, &pb.Envelope{SessionId: session, CorrelationId: "owner-final", RemainingBudgetMs: 2000, Payload: &pb.Envelope_Command{Command: cmd}})
	if result := readControllerEnvelope(t, ctx, conn).GetResult(); result == nil || result.Certainty != pb.OperationResult_COMPLETED {
		t.Fatal("INVITE final failed", result)
	}
	var wire []byte
	select {
	case wire = <-writes:
	case <-ctx.Done():
		t.Fatal("INVITE final missing")
	}
	writeControllerEnvelope(t, ctx, conn, &pb.Envelope{SessionId: session, CorrelationId: presented.CorrelationId, Payload: &pb.Envelope_Result{Result: &pb.OperationResult{Certainty: pb.OperationResult_ACCEPTED}}})
	if err = <-done; err != nil {
		t.Fatal(err)
	}
	e.expireResidentOwner(peer.Application(), peer.ApplicationLease().Term)
	select {
	case <-writes:
		t.Fatal("owner loss sent BYE before ACK")
	default:
	}
	remote, err := portable.NewDialog(portable.UAC, portable.Message{Wire: message.Wire, Transport: "tls"}, portable.Message{Wire: wire, Transport: "tls"}, portable.DefaultTimers())
	if err != nil {
		t.Fatal(err)
	}
	ack, err := portable.BuildDialogRequest(remote, sip.ACK, 1, nil, "z9hG4bK-late-ack")
	if err != nil {
		t.Fatal(err)
	}
	ackDone := make(chan error, 1)
	go func() { _, err := e.tryNativeCompletion(ctx, flow, &pb.SipMessage{Wire: ack.Wire}); ackDone <- err }()
	event := readControllerEnvelope(t, ctx, conn)
	if event.GetDialogEvent().GetKind() != pb.DialogEvent_ACKNOWLEDGED {
		t.Fatal("late ACK event missing")
	}
	writeControllerEnvelope(t, ctx, conn, &pb.Envelope{SessionId: session, CorrelationId: event.CorrelationId, Payload: &pb.Envelope_Result{Result: &pb.OperationResult{Certainty: pb.OperationResult_COMPLETED}}})
	if err = <-ackDone; err != nil {
		t.Fatal(err)
	}
	select {
	case wire = <-writes:
		parsed, parseErr := sip.ParseMessage(wire)
		if parseErr != nil {
			t.Fatal(parseErr)
		}
		request, ok := parsed.(*sip.Request)
		if !ok || request.Method != sip.BYE {
			t.Fatal("late ACK did not release BYE")
		}
	case <-ctx.Done():
		t.Fatal("late ACK cleanup missing")
	}
}

func TestResidentUDPCancelReplayRespectsEmissionQuota(t *testing.T) {
	e, peer, conn, ctx, session, writes, _ := residentControlFixture(t)
	message := stateTestRequest("INVITE", 1, "", "udp-cancel-quota")
	route, err := e.LocateRoute(ctx, message)
	if err != nil {
		t.Fatal(err)
	}
	flow := &pb.FlowRef{Ingress: e.cfg.Process, ConnectionId: "udp-peer", SourceIp: "192.0.2.12", SourcePort: 50300, Listener: "sip.example.test:5060", Transport: "udp"}
	done := make(chan error, 1)
	go func() {
		_, err := e.ingest(ctx, peer, &pb.OwnerIngest{Context: route.Context, Application: peer.Application(), Flow: flow, Wire: message.Wire}, time.Now().Add(2*time.Second))
		done <- err
	}()
	presented := readControllerEnvelope(t, ctx, conn)
	if presented.GetRequest() == nil {
		t.Fatal("INVITE not presented")
	}
	cancel, err := portable.BuildCancel(portable.Message{Wire: message.Wire, Transport: "udp"})
	if err != nil {
		t.Fatal(err)
	}
	cancelDone := make(chan error, 1)
	go func() {
		_, err := e.tryNativeCompletion(ctx, flow, &pb.SipMessage{Wire: cancel.Wire})
		cancelDone <- err
	}()
	event := readControllerEnvelope(t, ctx, conn)
	if event.GetDialogEvent().GetKind() != pb.DialogEvent_CANCELLED {
		t.Fatal("CANCEL event missing")
	}
	writeControllerEnvelope(t, ctx, conn, &pb.Envelope{SessionId: session, CorrelationId: event.CorrelationId, Payload: &pb.Envelope_Result{Result: &pb.OperationResult{Certainty: pb.OperationResult_COMPLETED}}})
	if err = <-cancelDone; err != nil {
		t.Fatal(err)
	}
	for range 2 {
		select {
		case <-writes:
		case <-ctx.Done():
			t.Fatal("initial CANCEL output missing")
		}
	}
	writeControllerEnvelope(t, ctx, conn, &pb.Envelope{SessionId: session, CorrelationId: presented.CorrelationId, Payload: &pb.Envelope_Result{Result: &pb.OperationResult{Certainty: pb.OperationResult_ACCEPTED}}})
	if err = <-done; err != nil {
		t.Fatal(err)
	}
	count := 1
	for range stateTransactionEmissions + 3 {
		handled, err := e.tryNativeCompletion(ctx, flow, &pb.SipMessage{Wire: cancel.Wire})
		if err != nil || !handled {
			t.Fatal("CANCEL replay failed", handled, err)
		}
		select {
		case <-writes:
			count++
		default:
		}
	}
	if count != stateProfileTransmissions {
		t.Fatalf("CANCEL emitted %d times, want %d", count, stateProfileTransmissions)
	}
	parsed, err := sip.ParseMessage(cancel.Wire)
	if err != nil {
		t.Fatal(err)
	}
	key, err := sip.ServerTxKeyMake(parsed)
	if err != nil {
		t.Fatal(err)
	}
	c, err := e.findContext(route.Context)
	if err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	actual := c.transactions[key].emissions
	c.mu.Unlock()
	if int(actual) != stateProfileTransmissions {
		t.Fatalf("retained CANCEL quota = %d", actual)
	}
}

func TestResidentUAS2xxTimerDoesNotReplayUnknownAttempt(t *testing.T) {
	e, peer, conn, ctx, session, writes, _ := residentControlFixture(t)
	message := stateTestRequest("INVITE", 1, "", "unknown-2xx")
	route, err := e.LocateRoute(ctx, message)
	if err != nil {
		t.Fatal(err)
	}
	flow := &pb.FlowRef{Ingress: e.cfg.Process, ConnectionId: "phone", SourceIp: "192.0.2.12", SourcePort: 50300, Listener: "sip.example.test:5061", Transport: "tls"}
	done := make(chan error, 1)
	go func() {
		_, err := e.ingest(ctx, peer, &pb.OwnerIngest{Context: route.Context, Application: peer.Application(), Flow: flow, Wire: message.Wire}, time.Now().Add(2*time.Second))
		done <- err
	}()
	presented := readControllerEnvelope(t, ctx, conn)
	if presented.GetRequest() == nil {
		t.Fatal("INVITE not presented")
	}
	cmd := &pb.Command{Context: route.Context, Sequence: 1, Admission: &pb.CommandAdmission{DirectoryEpoch: "directory", ExpiresAtMs: uint64(time.Now().Add(2 * time.Second).UnixMilli())}, Action: &pb.Command_Respond{Respond: &pb.Respond{RequestId: presented.GetRequest().RequestId, Status: 200}}}
	writeControllerEnvelope(t, ctx, conn, &pb.Envelope{SessionId: session, CorrelationId: "unknown-final", RemainingBudgetMs: 2000, Payload: &pb.Envelope_Command{Command: cmd}})
	if result := readControllerEnvelope(t, ctx, conn).GetResult(); result == nil || result.Certainty != pb.OperationResult_COMPLETED {
		t.Fatal("INVITE final failed", result)
	}
	select {
	case <-writes:
	case <-ctx.Done():
		t.Fatal("INVITE final missing")
	}
	writeControllerEnvelope(t, ctx, conn, &pb.Envelope{SessionId: session, CorrelationId: presented.CorrelationId, Payload: &pb.Envelope_Result{Result: &pb.OperationResult{Certainty: pb.OperationResult_ACCEPTED}}})
	if err = <-done; err != nil {
		t.Fatal(err)
	}
	c, err := e.findContext(route.Context)
	if err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	dkey := c.selectedDialog
	d := c.dialogs[dkey]
	tx := c.transactions[d.transaction]
	var output *residentOutput
	for _, candidate := range c.outputs {
		if candidate.transaction == tx.native.Key && candidate.status == 200 {
			output = candidate
			break
		}
	}
	if output == nil {
		c.mu.Unlock()
		t.Fatal("final proof missing")
	}
	output.state = residentOutputUnknown
	output.settled = false
	due := d.native.Timers[portable.Timer2xx]
	if !due.Active {
		c.mu.Unlock()
		t.Fatal("native 2xx timer missing")
	}
	due.At = time.Now().Add(-time.Second).UnixMilli()
	d.native.Timers[portable.Timer2xx] = due
	beforeCount, beforeEmissions := len(c.outputs), tx.emissions
	c.mu.Unlock()
	e.fireResidentDeadline(residentDeadline{key: residentDeadlineKey{kind: residentDialogDeadline, contextID: route.Context.Id, record: dkey, timer: portable.Timer2xx}, generation: due.Generation})
	c.mu.Lock()
	afterCount, afterEmissions := len(c.outputs), tx.emissions
	active := d.native.Timers[portable.Timer2xx].Active
	c.mu.Unlock()
	if beforeCount != afterCount || beforeEmissions != afterEmissions || !active {
		t.Fatal("unknown 2xx emitted replay or stopped native timer")
	}
}
