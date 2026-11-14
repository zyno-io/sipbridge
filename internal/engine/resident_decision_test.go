package engine

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
	"google.golang.org/protobuf/proto"
	"sipbridge/internal/control"
	pb "sipbridge/internal/controlpb"
)

func residentDirectFlow(e *ResidentMemory) *pb.FlowRef {
	return &pb.FlowRef{Ingress: e.cfg.Process, ConnectionId: "phone", SourceIp: "192.0.2.12", SourcePort: 50300, Listener: "sip.example.test:5061", Transport: "tls"}
}

func residentDirectRequest(t *testing.T, e *ResidentMemory, peer *control.Peer, conn *websocket.Conn, ctx context.Context, flow *pb.FlowRef, message *pb.SipMessage) *pb.RequestPresented {
	t.Helper()
	if err := e.ReceiveRequest(ctx, peer, flow, message); err != nil {
		t.Fatal(err)
	}
	envelope := readControllerEnvelope(t, ctx, conn)
	request := envelope.GetRequest()
	if request == nil || envelope.RemainingBudgetMs == 0 || envelope.RemainingBudgetMs > 2000 {
		t.Fatal("missing bounded one-shot request notification", envelope)
	}
	return request
}

func residentDirectDecision(request *pb.RequestPresented, sequence uint64, status uint32) *pb.RequestDecision {
	decision := &pb.RequestDecision{Context: request.Snapshot.Context, RequestId: request.RequestId, Sequence: sequence, Status: status}
	if status < 300 {
		decision.Basis = pb.OwnerIngest_AUTHENTICATED_BINDING
		decision.PrincipalId, decision.BindingId = "account", "phone"
		decision.Admission = &pb.CommandAdmission{DirectoryEpoch: "directory", ExpectedDelivery: cloneResidentDelivery(request.Snapshot.Delivery), ExpiresAtMs: uint64(time.Now().Add(2 * time.Second).UnixMilli())}
	}
	if request.Message.Method == "SUBSCRIBE" && status >= 200 && status < 300 {
		decision.Headers = []*pb.Header{{Name: "Expires", Value: "90"}}
		decision.Notify = &pb.DialogRequest{Method: "NOTIFY", Headers: []*pb.Header{{Name: "Content-Type", Value: "application/simple-message-summary"}}, Body: []byte("Messages-Waiting: no\r\n")}
	}
	if proposal := request.FlowAdoption; proposal != nil && status < 300 {
		decision.Delivery = &pb.ContextDelivery{Flow: proto.Clone(proposal.Flow).(*pb.FlowRef), NativeRevision: proposal.NativeRevision, DirectoryEpoch: "directory", RegistrationTicket: "ordered-publication"}
	}
	return decision
}

func residentSendDecision(t *testing.T, ctx context.Context, conn *websocket.Conn, session, operation string, decision *pb.RequestDecision) *pb.OperationResult {
	t.Helper()
	writeControllerEnvelope(t, ctx, conn, &pb.Envelope{SessionId: session, CorrelationId: operation, RemainingBudgetMs: 2000, Payload: &pb.Envelope_RequestDecision{RequestDecision: decision}})
	for {
		envelope := readControllerEnvelope(t, ctx, conn)
		if ping := envelope.GetPing(); ping != nil {
			writeControllerEnvelope(t, ctx, conn, &pb.Envelope{SessionId: session, CorrelationId: envelope.CorrelationId, Payload: &pb.Envelope_Pong{Pong: ping}})
			continue
		}
		result := envelope.GetResult()
		if result == nil || envelope.CorrelationId != operation {
			t.Fatal("decision did not receive its one committed result", envelope)
		}
		return result
	}
}

func residentSecondController(t *testing.T, e *ResidentMemory, ctx context.Context) (*control.Peer, *websocket.Conn, string) {
	t.Helper()
	server := httptest.NewServer(e.control)
	t.Cleanup(server.Close)
	connection, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http")+"/v1/control", &websocket.DialOptions{Subprotocols: []string{control.Subprotocol}, HTTPHeader: http.Header{"Authorization": []string{"Bearer " + strings.Repeat("synthetic-test-token-", 3)}}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.CloseNow() })
	app := &pb.ProcessRef{InstanceId: "selected-controller", Epoch: "two"}
	caps := []string{Capability, control.StateCapability, control.NativeCompletionCapability, control.CommandAdmissionCapability, control.CommandBudgetCapability, control.DialogContinuityCapability, control.RequestDecisionCapability}
	writeControllerEnvelope(t, ctx, connection, &pb.Envelope{CorrelationId: "hello-two", Payload: &pb.Envelope_Hello{Hello: &pb.SessionHello{Controller: app, ConnectionAttempt: 1, Major: 1, Bounds: &pb.Bounds{MaxEnvelopeBytes: control.MaxEnvelope}, Capabilities: caps}}})
	ack := readControllerEnvelope(t, ctx, connection).GetHelloAck()
	if ack == nil || ack.ApplicationLease == nil {
		t.Fatal("second controller did not authenticate")
	}
	writeControllerEnvelope(t, ctx, connection, &pb.Envelope{SessionId: ack.SessionId, CorrelationId: "ready-two", Payload: &pb.Envelope_Status{Status: &pb.SessionStatus{RetainedReady: true, BindingsReady: true, AdmitCalls: true, AdmitSubscriptions: true}}})
	writeControllerEnvelope(t, ctx, connection, &pb.Envelope{SessionId: ack.SessionId, CorrelationId: "barrier-two", Payload: &pb.Envelope_Ping{Ping: &pb.Heartbeat{Nonce: "barrier-two"}}})
	if readControllerEnvelope(t, ctx, connection).GetPong().GetNonce() != "barrier-two" {
		t.Fatal("second controller not ready")
	}
	peer, err := e.control.Peer(app)
	if err != nil {
		t.Fatal(err)
	}
	return peer, connection, ack.SessionId
}

func TestResidentRequestDecisionThreeFramesAndSelectedOwner(t *testing.T) {
	e, recipient, conn, ctx, session, writes, count := residentControlFixture(t, true)
	selected, selectedConn, selectedSession := residentSecondController(t, e, ctx)
	flow := residentDirectFlow(e)
	request := residentDirectRequest(t, e, recipient, conn, ctx, flow, stateTestRequest("REGISTER", 1, "", "direct-registration"))
	if !sameProcess(request.Snapshot.Application, recipient.Application()) || count.Load() != 0 {
		t.Fatal("notification lost its recipient hint or prematurely emitted a response")
	}
	c, err := e.findContext(request.Snapshot.Context)
	if err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	unbound := c.directUnbound && c.ownerKey == ""
	c.mu.Unlock()
	if !unbound {
		t.Fatal("notification recipient was claimed as owner")
	}
	decision := residentDirectDecision(request, 1, 200)
	result := residentSendDecision(t, ctx, selectedConn, selectedSession, "selected-decision", decision)
	if result.Certainty != pb.OperationResult_ACCEPTED || result.SipStatus != 200 || !sameProcess(result.GetSnapshot().GetApplication(), selected.Application()) || result.Snapshot.ApplicationLeaseTerm != selected.ApplicationLease().Term {
		t.Fatal("first committed decision did not bind its authenticated controller", result)
	}
	if wire := <-writes; !bytes.Contains(wire, []byte("200 OK")) || count.Load() != 1 {
		t.Fatal("expected exactly one selected SIP response", string(wire))
	}
	replayed := residentSendDecision(t, ctx, selectedConn, selectedSession, "selected-decision", decision)
	if replayed.Certainty != pb.OperationResult_ACCEPTED || count.Load() != 1 {
		t.Fatal("exact decision replay changed commit certainty or repeated its output", replayed)
	}
	late := proto.Clone(decision).(*pb.RequestDecision)
	late.Sequence = 2
	if result := residentSendDecision(t, ctx, conn, session, "late-recipient", late); result.Certainty != pb.OperationResult_NOT_ACCEPTED || count.Load() != 1 {
		t.Fatal("late notification recipient claimed an already accepted request", result)
	}
}

func TestResidentRequestDecisionPolicyFreeDenialBindsSelectedOwner(t *testing.T) {
	e, recipient, conn, ctx, _, writes, _ := residentControlFixture(t, true)
	selected, selectedConn, session := residentSecondController(t, e, ctx)
	request := residentDirectRequest(t, e, recipient, conn, ctx, residentDirectFlow(e), stateTestRequest("REGISTER", 1, "", "direct-denial"))
	result := residentSendDecision(t, ctx, selectedConn, session, "selected-denial", residentDirectDecision(request, 1, 403))
	if result.Certainty != pb.OperationResult_ACCEPTED || !sameProcess(result.GetSnapshot().GetApplication(), selected.Application()) || result.Snapshot.ApplicationLeaseTerm != selected.ApplicationLease().Term || result.Snapshot.PrincipalId != "" || result.Snapshot.BindingId != "" || result.Snapshot.DirectoryEpoch != "" {
		t.Fatal("exact policy-free denial did not bind only its deciding application", result)
	}
	if wire := <-writes; !bytes.Contains(wire, []byte("403 Forbidden")) {
		t.Fatal("selected denial did not produce its final SIP response", string(wire))
	}
}

func TestResidentRequestDecisionOptionsWithoutAccountBinding(t *testing.T) {
	e, peer, conn, ctx, session, writes, _ := residentControlFixture(t, true)
	request := residentDirectRequest(t, e, peer, conn, ctx, residentDirectFlow(e), stateTestRequest("OPTIONS", 1, "", "direct-options"))
	decision := residentDirectDecision(request, 1, 200)
	decision.Basis, decision.PrincipalId, decision.BindingId = pb.OwnerIngest_EXISTING_FLOW, "", ""
	result := residentSendDecision(t, ctx, conn, session, "options", decision)
	if result.Certainty != pb.OperationResult_ACCEPTED || result.GetSnapshot().GetPrincipalId() != "" || result.GetSnapshot().GetBindingId() != "" {
		t.Fatal("same-flow OPTIONS invented an authenticated account", result)
	}
	<-writes
}

func TestResidentRequestDecisionSubscribeCommitsNotifyBeforeResponse(t *testing.T) {
	e, peer, conn, ctx, session, writes, count := residentControlFixture(t, true)
	flow := residentDirectFlow(e)
	message := stateTestRequest("SUBSCRIBE", 1, "", "direct-subscription")
	message.Wire = []byte(strings.Replace(string(message.Wire), "Content-Length:", "Event: message-summary\r\nExpires: 90\r\nContent-Length:", 1))
	request := residentDirectRequest(t, e, peer, conn, ctx, flow, message)
	c, err := e.findContext(request.Snapshot.Context)
	if err != nil {
		t.Fatal(err)
	}
	writer := e.cfg.WriteCommitted
	first := true
	e.cfg.WriteCommitted = func(ctx context.Context, write *pb.FlowWrite) *pb.DeliveryResult {
		if first {
			first = false
			c.mu.Lock()
			notify, response := 0, 0
			for _, output := range c.outputs {
				if output.method == "NOTIFY" {
					notify++
				}
				if output.status == 202 {
					response++
				}
			}
			ready := c.subscription != nil && c.subscription.LocalSequence == 1 && c.snapshot.CommandSequence == 1 && !c.directUnbound && notify == 1 && response == 1
			c.mu.Unlock()
			if !ready {
				t.Error("response emitted before response, subscription, and NOTIFY were committed together")
			}
		}
		return writer(ctx, write)
	}
	decision := residentDirectDecision(request, 1, 202)
	result := residentSendDecision(t, ctx, conn, session, "subscribe-decision", decision)
	if result.Certainty != pb.OperationResult_ACCEPTED || count.Load() != 2 {
		t.Fatal("subscription did not accept its two-output intent", result, count.Load())
	}
	response, notify := <-writes, <-writes
	if !bytes.HasPrefix(response, []byte("SIP/2.0 202")) || !bytes.HasPrefix(notify, []byte("NOTIFY ")) || !bytes.Contains(notify, decision.Notify.Body) {
		t.Fatal("subscription outputs lost order or content", string(response), string(notify))
	}
	reply, err := portable.ResponseFor(portable.RestoreMessage(notify, "tls", "", ""), 200, "OK", nil, "")
	if err != nil {
		t.Fatal(err)
	}
	if handled, err := e.TryNativeCompletion(ctx, flow, &pb.SipMessage{Wire: reply.WireBytes()}); err != nil || !handled {
		t.Fatal("NOTIFY response did not complete natively", handled, err)
	}
	if replay := residentSendDecision(t, ctx, conn, session, "subscribe-decision", decision); replay.Certainty != pb.OperationResult_ACCEPTED || count.Load() != 2 {
		t.Fatal("NOTIFY completion changed the immutable decision receipt", replay)
	}

	c.mu.Lock()
	remote := portable.Dialog{Version: portable.Version, Role: portable.UAC, Phase: portable.DialogConfirmed, CallID: c.snapshot.CallId, LocalTag: c.snapshot.RemoteTag, RemoteTag: c.snapshot.LocalTag, LocalURI: c.snapshot.RemoteUri, RemoteURI: c.snapshot.LocalUri, Target: c.snapshot.LocalUri, Contact: c.snapshot.RemoteUri, SentBy: "192.0.2.12:50300", Transport: "tls"}
	c.mu.Unlock()
	refresh, err := portable.BuildDialogRequestWithHeaders(remote, sip.SUBSCRIBE, 2, nil, "z9hG4bK-direct-refresh", []portable.Header{{Name: "Event", Value: "message-summary"}})
	if err != nil {
		t.Fatal(err)
	}
	newFlow := proto.Clone(flow).(*pb.FlowRef)
	newFlow.ConnectionId = "reopened-phone"
	refreshRequest := residentDirectRequest(t, e, peer, conn, ctx, newFlow, &pb.SipMessage{Wire: refresh.WireBytes()})
	if refreshRequest.FlowAdoption == nil || !proto.Equal(refreshRequest.FlowAdoption.Flow, newFlow) || refreshRequest.FlowAdoption.NativeRevision != 1 {
		t.Fatal("validated renewal did not propose the replacement flow", refreshRequest)
	}
	refreshDecision := residentDirectDecision(refreshRequest, 2, 200)
	refreshResult := residentSendDecision(t, ctx, conn, session, "refresh-decision", refreshDecision)
	if refreshResult.Certainty != pb.OperationResult_ACCEPTED || !proto.Equal(refreshResult.GetSnapshot().GetDelivery(), refreshDecision.Delivery) || count.Load() != 4 {
		t.Fatal("replacement flow/response/NOTIFY did not commit together", refreshResult, count.Load())
	}
	<-writes
	<-writes
}

func TestResidentRequestDecisionRequiredNotifySurvivesOwnerExpiry(t *testing.T) {
	for _, outcome := range []pb.DeliveryResult_Outcome{pb.DeliveryResult_WRITTEN, pb.DeliveryResult_UNKNOWN} {
		t.Run(outcome.String(), func(t *testing.T) {
			e, peer, conn, ctx, session, writes, count := residentControlFixture(t, true)
			message := stateTestRequest("SUBSCRIBE", 1, "", "direct-notify-expiry")
			message.Wire = []byte(strings.Replace(string(message.Wire), "Content-Length:", "Event: message-summary\r\nExpires: 90\r\nContent-Length:", 1))
			request := residentDirectRequest(t, e, peer, conn, ctx, residentDirectFlow(e), message)
			writer := e.cfg.WriteCommitted
			var notify *pb.FlowWrite
			e.cfg.WriteCommitted = func(ctx context.Context, write *pb.FlowWrite) *pb.DeliveryResult {
				if notify == nil && write.Permit.Authorization.AuthorityClass == pb.EffectAuthorization_RESPONSE {
					result := writer(ctx, write)
					// The accepted response may reach the client immediately before
					// controller lease expiry. Its exact promised NOTIFY remains due.
					e.mu.Lock()
					e.leases[residentControllerKey(peer.Application())].expires = time.Now().Add(-time.Second)
					e.mu.Unlock()
					e.expireResidentOwner(peer.Application(), peer.ApplicationLease().Term)
					return result
				}
				notify = proto.Clone(write).(*pb.FlowWrite)
				if write.Permit.Authorization.AuthorityClass != pb.EffectAuthorization_NATIVE_COMPLETION {
					t.Error("bundled NOTIFY remained revocable application work")
				}
				reserved, err := e.Reserve(ctx, write)
				if err != nil || reserved == nil || reserved.Known || !bytes.HasPrefix(reserved.Wire, []byte("NOTIFY ")) {
					t.Errorf("accepted NOTIFY could not start after owner expiry: %v %v", reserved, err)
					return &pb.DeliveryResult{Effect: write.Effect, Outcome: pb.DeliveryResult_NOT_WRITTEN, Error: "required NOTIFY reserve failed"}
				}
				count.Add(1)
				result := &pb.DeliveryResult{Effect: write.Effect, Outcome: outcome, Recorded: true}
				if err := e.Complete(ctx, write, result); err != nil {
					t.Error(err)
				}
				return result
			}
			result := residentSendDecision(t, ctx, conn, session, "notify-expiry", residentDirectDecision(request, 1, 202))
			if result.Certainty != pb.OperationResult_ACCEPTED || notify == nil || count.Load() != 2 {
				t.Fatal("accepted NOTIFY did not make its one promised attempt", result, count.Load())
			}
			<-writes // the response preceded expiry and the NOTIFY attempt
			known, err := e.Reserve(ctx, notify)
			if err != nil || known == nil || !known.Known || known.Outcome != pb.DeliveryResult_UNKNOWN || len(known.Wire) != 0 || count.Load() != 2 {
				t.Fatal("completed or uncertain mandatory NOTIFY granted another attempt", known, err)
			}
		})
	}
}

func TestResidentRequestDecisionRefusalRollsBackRequiredNotify(t *testing.T) {
	e, peer, conn, ctx, session, _, count := residentControlFixture(t, true)
	message := stateTestRequest("SUBSCRIBE", 1, "", "direct-capacity")
	message.Wire = []byte(strings.Replace(string(message.Wire), "Content-Length:", "Event: message-summary\r\nExpires: 90\r\nContent-Length:", 1))
	request := residentDirectRequest(t, e, peer, conn, ctx, residentDirectFlow(e), message)
	decision := residentDirectDecision(request, 1, 202)
	missingNotify := proto.Clone(decision).(*pb.RequestDecision)
	missingNotify.Notify = nil
	if result := residentSendDecision(t, ctx, conn, session, "missing-notify", missingNotify); result.Certainty != pb.OperationResult_NOT_ACCEPTED || count.Load() != 0 {
		t.Fatal("accepted subscription lacked its mandatory NOTIFY", result)
	}
	c, err := e.findContext(request.Snapshot.Context)
	if err != nil {
		t.Fatal(err)
	}
	e.mu.Lock()
	limit := e.maxBytes
	e.maxBytes = e.bytes + e.reserved
	e.mu.Unlock()
	c.mu.Lock()
	before := c.bytes + c.reserved
	c.mu.Unlock()
	result := residentSendDecision(t, ctx, conn, session, "capacity-refusal", decision)
	if result.Certainty != pb.OperationResult_NOT_ACCEPTED || count.Load() != 0 {
		t.Fatal("insufficient NOTIFY capacity emitted a partial response", result)
	}
	c.mu.Lock()
	tx := c.transactions[c.requests[request.RequestId]]
	clean := c.directUnbound && c.ownerKey == "" && c.subscription == nil && c.requiredNotifyBudget == nil && len(c.outputs) == 0 && tx.decisionPending && tx.native.LastResponse == nil && c.bytes+c.reserved == before
	c.mu.Unlock()
	if !clean {
		t.Fatal("refused atomic decision leaked state, owner, or capacity")
	}
	e.mu.Lock()
	e.maxBytes = limit
	e.mu.Unlock()
	decision.Sequence = 2
	if result := residentSendDecision(t, ctx, conn, session, "capacity-retry", decision); result.Certainty != pb.OperationResult_ACCEPTED || count.Load() != 2 {
		t.Fatal("clean later decision could not consume restored capacity", result)
	}
}

func TestResidentRequestDecisionFinalPublicationGuardRefundsBothBudgets(t *testing.T) {
	e, peer, conn, ctx, _, _, count := residentControlFixture(t, true)
	message := stateTestRequest("SUBSCRIBE", 1, "", "direct-final-guard")
	message.Wire = []byte(strings.Replace(string(message.Wire), "Content-Length:", "Event: message-summary\r\nExpires: 90\r\nContent-Length:", 1))
	request := residentDirectRequest(t, e, peer, conn, ctx, residentDirectFlow(e), message)
	c, err := e.findContext(request.Snapshot.Context)
	if err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	tx := c.transactions[c.requests[request.RequestId]]
	sub, snapshot, response, err := e.prepareResidentSubscription(c, tx, &pb.Respond{Status: 202}, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	_, notify, effects, err := e.prepareResidentNotify(&residentContext{snapshot: snapshot, subscription: sub, flow: c.flow}, &pb.DialogRequest{Method: "NOTIFY"}, "guard-notify", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	notify.budget, err = e.openResidentBudgetLocked(c, residentSmallBudget)
	if err != nil {
		t.Fatal(err)
	}
	beforeBytes, beforeReserved, beforeSlots := c.bytes, c.reserved, c.reservedOutputs
	responseBudget, notifyBudget := tx.budget.remaining, notify.budget.remaining
	decision := &residentDecision{application: peer.Application(), term: peer.ApplicationLease().Term, bindOwner: true}
	e.mu.Lock()
	e.leases[residentControllerKey(peer.Application())].expires = time.Now().Add(-time.Second)
	e.mu.Unlock()
	groups := []residentOutputGroup{{tx: tx, effects: []portable.Effect{{Kind: portable.Send, Message: &response}}}, {tx: notify, effects: effects}}
	writes, err := e.stageResidentOutputGroupsLocked(c, groups, "guarded-decision", tx.presentDeadline, func() error {
		return e.publishResidentDecisionAuthorityLocked(c, decision, tx.presentDeadline)
	})
	if err == nil || len(writes) != 0 || count.Load() != 0 || len(c.outputs) != 0 || !c.directUnbound || c.ownerKey != "" || c.bytes != beforeBytes || c.reserved != beforeReserved || c.reservedOutputs != beforeSlots || tx.budget.remaining != responseBudget || notify.budget.remaining != notifyBudget || tx.emissions != 0 || notify.emissions != 0 {
		t.Fatal("failed final authority guard published an owner/output or leaked either budget", err)
	}
}

func TestResidentRequestDecisionDeadlineAndNativeCancel(t *testing.T) {
	for _, cancel := range []bool{false, true} {
		t.Run(map[bool]string{false: "deadline", true: "cancel"}[cancel], func(t *testing.T) {
			e, peer, conn, ctx, session, writes, count := residentControlFixture(t, true)
			method := "REGISTER"
			if cancel {
				method = "INVITE"
			}
			message := stateTestRequest(method, 1, "", "direct-expiry")
			flow := residentDirectFlow(e)
			request := residentDirectRequest(t, e, peer, conn, ctx, flow, message)
			if cancel {
				cancelWire := stateTestRequest("CANCEL", 1, "", "direct-expiry")
				if handled, err := e.TryNativeCompletion(ctx, flow, cancelWire); err != nil || !handled {
					t.Fatal("native paired CANCEL failed", handled, err)
				}
				for range 2 {
					<-writes
				}
			} else {
				select {
				case wire := <-writes:
					if !bytes.Contains(wire, []byte("503 Service Unavailable")) {
						t.Fatal("unanswered request did not expire natively", string(wire))
					}
				case <-ctx.Done():
					t.Fatal("pending request had no actual deadline")
				}
			}
			writesBefore := count.Load()
			decision := residentDirectDecision(request, 1, 200)
			if result := residentSendDecision(t, ctx, conn, session, "late-decision", decision); result.Certainty != pb.OperationResult_NOT_ACCEPTED || count.Load() != writesBefore {
				t.Fatal("expired or cancelled request was resurrected", result)
			}
		})
	}
}

func TestResidentRequestDecisionUDPDuplicateAndCommitCertainty(t *testing.T) {
	e, peer, conn, ctx, session, writes, count := residentControlFixture(t, true)
	flow := residentDirectFlow(e)
	flow.Transport = "udp"
	message := stateTestRequest("REGISTER", 1, "", "direct-udp")
	message.Wire = bytes.ReplaceAll(message.Wire, []byte("/TLS "), []byte("/UDP "))
	request := residentDirectRequest(t, e, peer, conn, ctx, flow, message)
	if err := e.ReceiveRequest(ctx, peer, flow, message); err != nil || count.Load() != 0 {
		t.Fatal("pending UDP duplicate was notified or emitted twice", err)
	}
	decision := residentDirectDecision(request, 1, 200)
	result := residentSendDecision(t, ctx, conn, session, "udp-decision", decision)
	if result.Certainty != pb.OperationResult_ACCEPTED {
		t.Fatal("native intent receipt claimed a physical outcome", result)
	}
	<-writes
	if err := e.ReceiveRequest(ctx, peer, flow, message); err != nil {
		t.Fatal(err)
	}
	if wire := <-writes; !bytes.Contains(wire, []byte("200 OK")) || count.Load() != 2 {
		t.Fatal("known completed UDP transaction could not replay its response")
	}
}

func TestResidentRequestDecisionUnknownWriteDoesNotChangeCommitOrReplay(t *testing.T) {
	e, peer, conn, ctx, session, _, count := residentControlFixture(t, true)
	flow := residentDirectFlow(e)
	flow.Transport = "udp"
	message := stateTestRequest("REGISTER", 1, "", "direct-unknown")
	message.Wire = bytes.ReplaceAll(message.Wire, []byte("/TLS "), []byte("/UDP "))
	request := residentDirectRequest(t, e, peer, conn, ctx, flow, message)
	e.cfg.WriteCommitted = func(ctx context.Context, write *pb.FlowWrite) *pb.DeliveryResult {
		if _, err := e.Reserve(ctx, write); err != nil {
			t.Error(err)
		}
		count.Add(1)
		result := &pb.DeliveryResult{Effect: write.Effect, Outcome: pb.DeliveryResult_UNKNOWN, Recorded: true}
		if err := e.Complete(ctx, write, result); err != nil {
			t.Error(err)
		}
		return result
	}
	decision := residentDirectDecision(request, 1, 200)
	if result := residentSendDecision(t, ctx, conn, session, "unknown-decision", decision); result.Certainty != pb.OperationResult_ACCEPTED || count.Load() != 1 {
		t.Fatal("native intent receipt claimed a physical outcome", result)
	}
	if err := e.ReceiveRequest(ctx, peer, flow, message); err != nil || count.Load() != 1 {
		t.Fatal("UDP retry repeated an uncertain physical write", err, count.Load())
	}
	if result := residentSendDecision(t, ctx, conn, session, "unknown-decision", decision); result.Certainty != pb.OperationResult_ACCEPTED || count.Load() != 1 {
		t.Fatal("uncertain completion changed the immutable intent receipt", result)
	}
}

func residentDirectNativeEvent(t *testing.T, e *ResidentMemory, conn *websocket.Conn, ctx context.Context, session string, flow *pb.FlowRef, message portable.Message, kind pb.DialogEvent_Kind) {
	t.Helper()
	done := make(chan error, 1)
	go func() {
		handled, err := e.TryNativeCompletion(ctx, flow, &pb.SipMessage{Wire: message.WireBytes()})
		if !handled && err == nil {
			t.Error("native completion not handled")
		}
		done <- err
	}()
	envelope := readControllerEnvelope(t, ctx, conn)
	if envelope.GetDialogEvent().GetKind() != kind {
		t.Fatal("missing exact native lifecycle event", envelope, kind)
	}
	writeControllerEnvelope(t, ctx, conn, &pb.Envelope{SessionId: session, CorrelationId: envelope.CorrelationId, Payload: &pb.Envelope_Result{Result: &pb.OperationResult{Certainty: pb.OperationResult_COMPLETED}}})
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestResidentRequestDecisionDeferredInviteAndEstablishedContinuations(t *testing.T) {
	e, peer, conn, ctx, session, writes, count := residentControlFixture(t, true)
	flow := residentDirectFlow(e)
	message := stateTestRequest("INVITE", 1, "", "direct-invite")
	request := residentDirectRequest(t, e, peer, conn, ctx, flow, message)
	// The old response command cannot bypass decision admission.
	response := &pb.Command{Context: request.Snapshot.Context, Sequence: 1, Admission: &pb.CommandAdmission{DirectoryEpoch: "directory", ExpiresAtMs: uint64(time.Now().Add(2 * time.Second).UnixMilli())}, Action: &pb.Command_Respond{Respond: &pb.Respond{RequestId: request.RequestId, Status: 200}}}
	writeControllerEnvelope(t, ctx, conn, &pb.Envelope{SessionId: session, CorrelationId: "bypass", RemainingBudgetMs: 2000, Payload: &pb.Envelope_Command{Command: response}})
	if result := readControllerEnvelope(t, ctx, conn).GetResult(); result == nil || result.Certainty == pb.OperationResult_COMPLETED || count.Load() != 0 {
		t.Fatal("ordinary response bypassed direct admission", result)
	}
	if result := residentSendDecision(t, ctx, conn, session, "admit-invite", residentDirectDecision(request, 1, 0)); result.Certainty != pb.OperationResult_ACCEPTED {
		t.Fatal("INVITE could not defer application handling", result)
	}
	if trying := <-writes; !bytes.Contains(trying, []byte("100 Trying")) {
		t.Fatal("admitted INVITE lost native provisional", string(trying))
	}
	response.Sequence = 2
	writeControllerEnvelope(t, ctx, conn, &pb.Envelope{SessionId: session, CorrelationId: "answer-invite", RemainingBudgetMs: 2000, Payload: &pb.Envelope_Command{Command: response}})
	if result := readControllerEnvelope(t, ctx, conn).GetResult(); result == nil || result.Certainty != pb.OperationResult_COMPLETED {
		t.Fatal("ordinary asynchronous INVITE answer failed", result)
	}
	final := <-writes
	remote, err := portable.NewDialog(portable.UAC, portable.RestoreMessage(message.Wire, "tls", "", ""), portable.RestoreMessage(final, "tls", "", ""), portable.DefaultTimers())
	if err != nil {
		t.Fatal(err)
	}
	ack, err := portable.BuildDialogRequest(remote, sip.ACK, 1, nil, "z9hG4bK-direct-ack")
	if err != nil {
		t.Fatal(err)
	}
	residentDirectNativeEvent(t, e, conn, ctx, session, flow, ack, pb.DialogEvent_ACKNOWLEDGED)
	reinvite, err := portable.BuildDialogRequest(remote, sip.INVITE, 2, nil, "z9hG4bK-direct-reinvite")
	if err != nil {
		t.Fatal(err)
	}
	reopened := proto.Clone(flow).(*pb.FlowRef)
	reopened.ConnectionId = "new-tls"
	renewal := residentDirectRequest(t, e, peer, conn, ctx, reopened, &pb.SipMessage{Wire: reinvite.WireBytes()})
	decision := residentDirectDecision(renewal, 3, 0)
	decision.Basis = pb.OwnerIngest_TLS_DIALOG_CONTINUITY
	if result := residentSendDecision(t, ctx, conn, session, "admit-reinvite", decision); result.Certainty != pb.OperationResult_ACCEPTED || !proto.Equal(result.GetSnapshot().GetDelivery(), decision.Delivery) {
		t.Fatal("retained dialog could not adopt the native-validated TLS flow", result)
	}
	<-writes // native 100 follows admission
	reject := &pb.Command{Context: request.Snapshot.Context, Sequence: 4, Admission: &pb.CommandAdmission{DirectoryEpoch: "directory", ExpectedDelivery: decision.Delivery, ExpiresAtMs: uint64(time.Now().Add(2 * time.Second).UnixMilli())}, Action: &pb.Command_Respond{Respond: &pb.Respond{RequestId: renewal.RequestId, Status: 488}}}
	writeControllerEnvelope(t, ctx, conn, &pb.Envelope{SessionId: session, CorrelationId: "reject-reinvite", RemainingBudgetMs: 2000, Payload: &pb.Envelope_Command{Command: reject}})
	if result := readControllerEnvelope(t, ctx, conn).GetResult(); result == nil || result.Certainty != pb.OperationResult_COMPLETED {
		t.Fatal("re-INVITE rejection failed", result)
	}
	rejected := <-writes
	negativeACK, err := portable.BuildNon2xxACK(reinvite, portable.RestoreMessage(rejected, "tls", "", ""))
	if err != nil {
		t.Fatal(err)
	}
	if handled, err := e.TryNativeCompletion(ctx, reopened, &pb.SipMessage{Wire: negativeACK.WireBytes()}); err != nil || !handled {
		t.Fatal("negative re-INVITE ACK failed", handled, err)
	}
	pending, err := portable.BuildDialogRequest(remote, sip.INVITE, 3, nil, "z9hG4bK-direct-cancel-reinvite")
	if err != nil {
		t.Fatal(err)
	}
	pendingRequest := residentDirectRequest(t, e, peer, conn, ctx, reopened, &pb.SipMessage{Wire: pending.WireBytes()})
	cancel, err := portable.BuildCancel(pending)
	if err != nil {
		t.Fatal(err)
	}
	residentDirectNativeEvent(t, e, conn, ctx, session, reopened, cancel, pb.DialogEvent_CANCELLED)
	for range 2 {
		<-writes
	}
	if result := residentSendDecision(t, ctx, conn, session, "cancelled-reinvite", residentDirectDecision(pendingRequest, 5, 0)); result.Certainty != pb.OperationResult_NOT_ACCEPTED {
		t.Fatal("cancelled established transaction was accepted", result)
	}
	bye, err := portable.BuildDialogRequest(remote, sip.BYE, 4, nil, "z9hG4bK-direct-bye")
	if err != nil {
		t.Fatal(err)
	}
	residentDirectNativeEvent(t, e, conn, ctx, session, reopened, bye, pb.DialogEvent_ENDED)
	if final := <-writes; !bytes.Contains(final, []byte("200 OK")) {
		t.Fatal("post-OK native BYE was not answered", string(final))
	}
}
