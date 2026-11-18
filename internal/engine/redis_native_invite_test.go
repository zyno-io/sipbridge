package engine

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/proto"
	"sipbridge/internal/control"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/redisclient"
	"sipbridge/internal/rediscore"
)

func TestRedisNativeInitialInviteJSONDecisionAndRespond(t *testing.T) {
	testRedisNativeInitialInviteJSONDecisionAndRespond(t, false, false)
}

func TestRedisNativeCrossWorkerJSONReplyExecutor(t *testing.T) {
	testRedisNativeInitialInviteJSONDecisionAndRespond(t, true, false)
}

func testRedisNativeInitialInviteJSONDecisionAndRespond(t *testing.T, crossWorker, existingBye bool, byeFinal ...uint32) {
	address := os.Getenv("SIPBRIDGE_CORE_TEST_REDIS")
	if address == "" {
		t.Skip("isolated Redis fixture required")
	}
	fixtureBudget := 8 * time.Second
	if existingBye {
		fixtureBudget = 45 * time.Second
	}
	ctx, cancel := context.WithTimeout(context.Background(), fixtureBudget)
	defer cancel()
	root := "sipstate:test:native-invite-" + uuid.NewString() + ":"
	key := bytes.Repeat([]byte{7}, 32)
	process := &pb.ProcessRef{InstanceId: "native", Epoch: uuid.NewString()}
	application := &pb.ProcessRef{InstanceId: "controller", Epoch: uuid.NewString()}
	connection, err := rediscore.NewConnection(rediscore.ConnectionConfig{Redis: redisclient.Options{Address: address, IOTimeout: time.Second}, Profile: rediscore.ProfileConfig{Root: root, Service: "test", Generation: "one", IdentityKey: key, RecordBudget: 4 << 20}})
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	client := redis.NewClient(&redis.Options{Addr: address, MaxRetries: -1})
	keys := []string{"core:profile", "core:capacity", "core:selectors", "core:applications", "core:owner-expiry", "core:coordinator"}
	defer func() {
		for i, name := range keys {
			keys[i] = root + name
		}
		if e := client.Del(context.Background(), keys...).Err(); e != nil {
			t.Error(e)
		}
		_ = client.Close()
	}()
	token := strings.Repeat("synthetic-test-token-", 3)
	file := filepath.Join(t.TempDir(), "token")
	if err = os.WriteFile(file, []byte(token), 0600); err != nil {
		t.Fatal(err)
	}
	auth, err := control.NewBearerAuthenticator(file, "redis-native-test")
	if err != nil {
		t.Fatal(err)
	}
	var e *redisNative
	capabilities := []string{Capability, control.StateCapability, control.NativeCompletionCapability, control.CommandAdmissionCapability, control.CommandBudgetCapability, control.DialogContinuityCapability, control.RequestDecisionCapability}
	server, err := control.NewServer(control.Config{Process: process, Authenticator: auth, AllowPlain: true, StateAuthority: connection.Authority(), Capabilities: capabilities,
		OnApplicationLease: func(ctx context.Context, app *pb.ProcessRef, term string) (*pb.ApplicationLease, error) {
			return e.ApplicationLease(ctx, app, term)
		},
		OnControllerSession: func(ctx context.Context, peer *control.Peer, hello *pb.SessionHello) (*pb.ApplicationLease, error) {
			return e.ControllerSession(ctx, peer, hello)
		},
		OnControllerRenew: func(ctx context.Context, peer *control.Peer) (*pb.ApplicationLease, error) {
			return e.ControllerRenew(ctx, peer)
		},
		OnMessage: func(peer *control.Peer, envelope *pb.Envelope) error { return e.Handle(peer, envelope) },
		OnStatus:  func(peer *control.Peer, _ *pb.SessionStatus) error { return e.ControllerReady(ctx, peer) }})
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	written := make(chan []byte, 8)
	retried := make(chan []byte, 4)
	cfg := Config{Process: process, AdvertiseAddress: "sip.example.test:5061", ContactURI: "sips:sip.example.test:5061", StateIdentityKey: key}
	if existingBye {
		cfg.OnNativeDriverDiagnostic = func(d rediscore.DriverDiagnostic) { t.Logf("fixture driver diagnostic: %s/%s", d.Stage, d.Category) }
	}
	cfg.WriteNativeRetry = func(_ context.Context, _ *pb.FlowRef, wire []byte, _ time.Time) error {
		retried <- bytes.Clone(wire)
		return nil
	}
	cfg.WriteCommitted = func(ctx context.Context, write *pb.FlowWrite) *pb.DeliveryResult {
		result := &pb.DeliveryResult{Effect: write.Effect, Outcome: pb.DeliveryResult_NOT_WRITTEN}
		reserved, reserveErr := e.Reserve(ctx, write)
		if reserveErr != nil {
			result.Error = reserveErr.Error()
			return result
		}
		if reserved.Known {
			result.Outcome, result.Recorded = reserved.Outcome, true
			return result
		}
		if bytes.Contains(reserved.Wire, []byte("branch=z9hG4bK-native-boundary-unknown")) {
			result.Outcome = pb.DeliveryResult_UNKNOWN
			if completeErr := e.Complete(ctx, write, result); completeErr != nil {
				result.Error = completeErr.Error()
			}
			return result
		}
		select {
		case written <- reserved.Wire:
		case <-ctx.Done():
			return result
		}
		result.Outcome = pb.DeliveryResult_WRITTEN
		if completeErr := e.Complete(ctx, write, result); completeErr != nil {
			result.Error = completeErr.Error()
			return result
		}
		result.Recorded = true
		return result
	}
	e, err = newRedisNative(ctx, cfg, connection, server)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	keys = append(keys, "core:worker:"+e.worker, "core:timers:"+e.worker, "core:ready:"+e.worker)
	for e.Ready() != nil {
		select {
		case <-time.After(10 * time.Millisecond):
		case <-ctx.Done():
			t.Fatal("worker did not become ready")
		}
	}
	httpServer := httptest.NewServer(server)
	defer httpServer.Close()
	ws, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(httpServer.URL, "http")+"/v1/control", &websocket.DialOptions{Subprotocols: []string{control.Subprotocol}, HTTPHeader: http.Header{"Authorization": {"Bearer " + token}}})
	if err != nil {
		t.Fatal(err)
	}
	defer ws.CloseNow()
	writeControllerEnvelope(t, ctx, ws, &pb.Envelope{CorrelationId: "hello", Payload: &pb.Envelope_Hello{Hello: &pb.SessionHello{Controller: application, ConnectionAttempt: 1, Major: 1, Bounds: &pb.Bounds{MaxEnvelopeBytes: control.MaxEnvelope}, Capabilities: capabilities}}})
	ack := readControllerEnvelope(t, ctx, ws).GetHelloAck()
	if ack == nil || ack.ApplicationLease == nil {
		t.Fatal("JSON hello failed")
	}
	appID, err := nativeApplicationReference(key, application)
	if err != nil {
		t.Fatal(err)
	}
	keys = append(keys, "core:application:"+appID, "core:owner:"+ack.ApplicationLease.Term, "core:owned:"+ack.ApplicationLease.Term)
	writeControllerEnvelope(t, ctx, ws, &pb.Envelope{SessionId: ack.SessionId, CorrelationId: "ready", Payload: &pb.Envelope_Status{Status: &pb.SessionStatus{RetainedReady: true, BindingsReady: true, AdmitCalls: true, AdmitSubscriptions: true}}})
	writeControllerEnvelope(t, ctx, ws, &pb.Envelope{SessionId: ack.SessionId, CorrelationId: "barrier", Payload: &pb.Envelope_Ping{Ping: &pb.Heartbeat{Nonce: "barrier"}}})
	if readControllerEnvelope(t, ctx, ws).GetPong().GetNonce() != "barrier" {
		t.Fatal("controller did not become ready")
	}
	peer, err := server.Peer(application)
	if err != nil {
		t.Fatal(err)
	}
	flow := &pb.FlowRef{Ingress: process, ConnectionId: "phone", SourceIp: "192.0.2.12", SourcePort: 5061, Listener: "sip.example.test:5061", Transport: "tls"}
	message, err := portable.ParseMessage(stateTestRequest("INVITE", 1, "", "native-invite").Wire, "tls", "192.0.2.12:5061", "")
	if err != nil {
		t.Fatal(err)
	}
	if err = e.ReceiveRequestParsed(ctx, peer, flow, &pb.SipMessage{Wire: message.WireBytes()}, nil, message); err != nil {
		t.Fatal(err)
	}
	presentation := readControllerEnvelope(t, ctx, ws).GetRequest()
	if presentation == nil || presentation.Message.Method != "INVITE" {
		t.Fatal("initial INVITE was not presented")
	}
	keys = append(keys, "core:context:"+presentation.Snapshot.Context.Id, "core:deadlines:"+presentation.Snapshot.Context.Id)
	located, err := e.LocateRoute(ctx, &pb.SipMessage{Wire: message.WireBytes(), Method: "BYE", Uri: "sip:forged@example.test", Selectors: []string{"forged"}})
	if err != nil || located == nil || located.Context.Id != presentation.Snapshot.Context.Id || !sameProcess(located.Application, application) {
		t.Fatal("read-only ingress lookup did not select observed native route", err)
	}
	store, _, _, err := e.driver.Current()
	if err != nil {
		t.Fatal(err)
	}
	beforeDuplicate, err := store.ObserveContext(ctx, located.Context.Id, located.Context.State.Incarnation, "r:context")
	if err != nil {
		t.Fatal(err)
	}
	if handled, duplicateErr := e.TryNativeCompletionParsed(ctx, flow, &pb.SipMessage{Wire: message.WireBytes()}, nil, message); duplicateErr != nil || !handled {
		t.Fatal("exact pending reliable duplicate was not consumed", duplicateErr)
	}
	afterDuplicate, err := store.ObserveContext(ctx, located.Context.Id, located.Context.State.Incarnation, "r:context")
	if err != nil || beforeDuplicate.View.Revision != afterDuplicate.View.Revision || !bytes.Equal(beforeDuplicate.View.Records["r:context"], afterDuplicate.View.Records["r:context"]) {
		t.Fatal("read-only route/duplicate changed native authority", err)
	}
	originalWS, originalSession := ws, ack.SessionId
	issuer := process
	if crossWorker {
		var other *redisNative
		var closeOther func()
		other, ws, ack, closeOther = redisJSONReplyWorker(t, ctx, address, root, key, file, token, cfg, application, capabilities)
		defer closeOther()
		issuer = other.cfg.Process
		keys = append(keys, "core:worker:"+other.worker, "core:timers:"+other.worker, "core:ready:"+other.worker)
		if ack.ApplicationLease.Term != presentation.Snapshot.ApplicationLeaseTerm || !proto.Equal(other.StateAuthority(), e.StateAuthority()) {
			t.Fatal("workers did not share the actual application term and native authority")
		}
	}
	decision := residentDirectDecision(presentation, 1, 0)
	accepted := residentSendDecision(t, ctx, ws, ack.SessionId, "defer", decision)
	if crossWorker && !sameProcess(accepted.GetSnapshot().GetExecutor(), issuer) {
		t.Error("decision reply executor is not its responding worker")
	}
	if accepted.Certainty != pb.OperationResult_ACCEPTED {
		t.Fatal("INVITE deferral failed", accepted)
	}
	if replay := residentSendDecision(t, ctx, ws, ack.SessionId, "defer", decision); replay.Certainty != pb.OperationResult_UNKNOWN || replay.Error != "command sequence already consumed" {
		t.Fatal("consumed decision was not uncertain", replay)
	}
	command := &pb.Command{Context: presentation.Snapshot.Context, Sequence: 2, Admission: &pb.CommandAdmission{DirectoryEpoch: "directory", ExpiresAtMs: uint64(time.Now().Add(2 * time.Second).UnixMilli())}, Action: &pb.Command_Respond{Respond: &pb.Respond{RequestId: presentation.RequestId, Status: 180}}}
	writeControllerEnvelope(t, ctx, ws, &pb.Envelope{SessionId: ack.SessionId, CorrelationId: "ring", RemainingBudgetMs: 2000, Payload: &pb.Envelope_Command{Command: command}})
	result := readControllerEnvelope(t, ctx, ws).GetResult()
	if result == nil || result.Certainty != pb.OperationResult_COMPLETED || result.SipStatus != 180 {
		t.Fatal("ordinary provisional response did not complete", result)
	}
	if crossWorker && !sameProcess(result.GetSnapshot().GetExecutor(), issuer) {
		t.Error("command reply executor is not its responding worker")
	}
	for seen := false; !seen; {
		select {
		case wire := <-written:
			seen = bytes.HasPrefix(wire, []byte("SIP/2.0 180"))
			if !seen && !bytes.HasPrefix(wire, []byte("SIP/2.0 100")) {
				t.Fatal("unexpected INVITE response")
			}
		case <-ctx.Done():
			t.Fatal("INVITE provisional response not written")
		}
	}
	assertRedisNativeJSONInspection(t, ctx, ws, ack.SessionId, result.Snapshot, issuer)
	store, _, _, err = e.driver.Current()
	if err != nil {
		t.Fatal(err)
	}
	before, err := store.ObserveContext(ctx, presentation.Snapshot.Context.Id, presentation.Snapshot.Context.State.Incarnation, "r:procedure-"+presentation.RequestId)
	if err != nil {
		t.Fatal(err)
	}
	beforeProcedure, err := readPrivateProcedureRecord(before.View.Records["r:procedure-"+presentation.RequestId])
	if err != nil {
		t.Fatal(err)
	}
	writeControllerEnvelope(t, ctx, ws, &pb.Envelope{SessionId: ack.SessionId, CorrelationId: "ring", RemainingBudgetMs: 2000, Payload: &pb.Envelope_Command{Command: command}})
	if replay := readControllerEnvelope(t, ctx, ws).GetResult(); replay == nil || replay.Certainty != pb.OperationResult_UNKNOWN || replay.Error != "command sequence already consumed" {
		t.Fatal("consumed response was not uncertain", replay)
	}
	after, err := store.ObserveContext(ctx, presentation.Snapshot.Context.Id, presentation.Snapshot.Context.State.Incarnation, "r:procedure-"+presentation.RequestId)
	if err != nil {
		t.Fatal(err)
	}
	afterProcedure, err := readPrivateProcedureRecord(after.View.Records["r:procedure-"+presentation.RequestId])
	if err != nil || afterProcedure.Emissions != beforeProcedure.Emissions {
		t.Fatal("exact response replay added a physical emission", err)
	}
	command.Sequence, command.GetRespond().Status = 3, 200
	writeControllerEnvelope(t, ctx, ws, &pb.Envelope{SessionId: ack.SessionId, CorrelationId: "answer", RemainingBudgetMs: 2000, Payload: &pb.Envelope_Command{Command: command}})
	result = readControllerEnvelope(t, ctx, ws).GetResult()
	if result == nil || result.Certainty != pb.OperationResult_COMPLETED || result.SipStatus != 200 || result.Snapshot == nil || !result.Snapshot.Answered {
		t.Fatal("ordinary final response did not complete", result)
	}
	var finalWire []byte
	for seen := false; !seen; {
		select {
		case wire := <-written:
			seen = bytes.HasPrefix(wire, []byte("SIP/2.0 200"))
			if seen {
				finalWire = bytes.Clone(wire)
			}
			if !seen && !bytes.HasPrefix(wire, []byte("SIP/2.0 100")) {
				t.Fatal("final physical response differed")
			}
		case <-ctx.Done():
			t.Fatal("INVITE final response not written")
		}
	}
	select {
	case wire := <-retried:
		if !bytes.Equal(wire, finalWire) {
			t.Fatal("reliable 2xx derivative changed the exact physical bytes")
		}
	case <-ctx.Done():
		t.Fatal("reliable 2xx derivative did not run")
	}
	if existingBye && len(byeFinal) == 1 {
		testRedisBYEFinalCancelsACK(t, ctx, e, ws, ack, presentation, store, client, root, written, retried, byeFinal[0])
		return
	}
	if existingBye {
		testRedisACKTimeoutKeepsApplicationBYE(t, ctx, e, ws, ack, presentation, store, written, retried)
		return
	}
	remote, err := portable.NewDialog(portable.UAC, message, portable.RestoreMessage(finalWire, "tls", "", ""), portable.DefaultTimers())
	if err != nil {
		t.Fatal(err)
	}
	ackMessage, err := portable.BuildDialogRequest(remote, sip.ACK, 1, nil, "z9hG4bK-native-invite-ack")
	if err != nil {
		t.Fatal(err)
	}
	ackMessage = portable.RestoreMessage(ackMessage.WireBytes(), "tls", "192.0.2.12:5061", "")
	if handled, ackErr := e.TryNativeCompletionParsed(ctx, flow, &pb.SipMessage{Wire: ackMessage.WireBytes()}, nil, ackMessage); ackErr != nil || !handled {
		t.Fatal("native ACK did not complete the selected dialog", ackErr)
	}
	retryKey := nativeUDPRetryKey{Context: presentation.Snapshot.Context.Id, Incarnation: presentation.Snapshot.Context.State.Incarnation, Procedure: "r:dialog-" + presentation.RequestId}
	for {
		e.udp.mu.Lock()
		_, active := e.udp.entries[retryKey]
		e.udp.mu.Unlock()
		if !active {
			break
		}
		select {
		case <-time.After(10 * time.Millisecond):
		case <-ctx.Done():
			t.Fatal("native ACK did not stop the exact reliable derivative")
		}
	}
	if crossWorker {
		observed, readErr := store.ObserveContext(ctx, presentation.Snapshot.Context.Id, presentation.Snapshot.Context.State.Incarnation, "r:context")
		if readErr != nil {
			t.Fatal(readErr)
		}
		retained, decodeErr := decodeNativeContext(observed.View.Records["r:context"])
		if decodeErr != nil || !sameProcess(retained.Executor, process) || !proto.Equal(retained.Context, presentation.Snapshot.Context) ||
			!proto.Equal(retained.Flow, flow) || retained.ApplicationLeaseTerm != ack.ApplicationLease.Term || observed.View.Timer != beforeDuplicate.View.Timer {
			t.Fatal("reply projection changed retained ownership, timer assignment, flow or term", decodeErr)
		}
		for {
			frame := readControllerEnvelope(t, ctx, originalWS)
			if ping := frame.GetPing(); ping != nil {
				writeControllerEnvelope(t, ctx, originalWS, &pb.Envelope{SessionId: originalSession, CorrelationId: frame.CorrelationId, Payload: &pb.Envelope_Pong{Pong: ping}})
				continue
			}
			event := frame.GetDialogEvent()
			if event == nil || !sameProcess(event.GetSnapshot().GetExecutor(), process) || !proto.Equal(event.Snapshot.Context, presentation.Snapshot.Context) {
				t.Fatal("retained lifecycle metadata changed")
			}
			writeControllerEnvelope(t, ctx, originalWS, &pb.Envelope{SessionId: originalSession, CorrelationId: frame.CorrelationId, Payload: &pb.Envelope_Result{Result: &pb.OperationResult{Certainty: pb.OperationResult_ACCEPTED}}})
			if event.Kind == pb.DialogEvent_ACKNOWLEDGED {
				break
			}
		}
		return
	}
	udpWire := bytes.ReplaceAll(stateTestRequest("INVITE", 1, "", "native-udp-g").Wire, []byte("SIP/2.0/TLS"), []byte("SIP/2.0/UDP"))
	udpWire = bytes.ReplaceAll(udpWire, []byte("redis-native-call@example.test"), []byte("native-udp-g@example.test"))
	udpMessage, err := portable.ParseMessage(udpWire, "udp", "192.0.2.12:5061", "")
	if err != nil {
		t.Fatal(err)
	}
	udpFacts, err := udpMessage.Facts()
	if err != nil {
		t.Fatal(err)
	}
	udpFlow := &pb.FlowRef{Ingress: process, ConnectionId: "udp-phone", SourceIp: "192.0.2.12", SourcePort: 5061, Listener: "sip.example.test:5060", Transport: "udp"}
	udpID, udpIncarnation, udpRequestID := uuid.NewString(), uuid.NewString(), uuid.NewString()
	udpContext := &pb.ContextRef{Id: udpID, Owner: process, State: &pb.StateRef{Authority: e.StateAuthority(), GroupId: udpID, Incarnation: udpIncarnation, CallId: udpFacts.CallID}}
	keys = append(keys, "core:context:"+udpID, "core:deadlines:"+udpID)
	udpDeadline := time.Now().Add(2 * time.Second)
	udpPresentation, err := receiveRedisInvite(ctx, store, redisRegisterReceive{IdentityKey: key, Context: udpContext, Authority: e.StateAuthority(), Application: application, Executor: process, Lease: rediscore.ApplicationLeaseRef{Application: appID, Term: ack.ApplicationLease.Term}, Flow: udpFlow, Message: udpMessage, RequestID: udpRequestID, LocalTag: "udp-local", Timer: rediscore.TimerAssignment{Worker: e.worker, Epoch: process.Epoch, Term: process.Epoch}, Deadline: udpDeadline})
	if err != nil {
		t.Fatal(err)
	}
	udpDecision := residentDirectDecision(udpPresentation, 1, 486)
	if _, err = commitRedisInviteFinal(ctx, store, key, application, rediscore.ApplicationLeaseRef{Application: appID, Term: ack.ApplicationLease.Term}, udpDecision, "udp-reject", "sip:edge@example.test", "edge.example.test:5060", udpDeadline); err != nil {
		t.Fatal("select UDP negative final", err)
	}
	var udpFinal []byte
	for udpFinal == nil {
		select {
		case wire := <-written:
			if bytes.HasPrefix(wire, []byte("SIP/2.0 486")) {
				udpFinal = bytes.Clone(wire)
			}
		case <-ctx.Done():
			t.Fatal("UDP negative final was not written")
		}
	}
	udpRetryObserved := false
	for !udpRetryObserved {
		select {
		case wire := <-retried:
			if bytes.HasPrefix(wire, []byte("SIP/2.0 486")) {
				if !bytes.Equal(wire, udpFinal) {
					t.Fatal("Timer G changed the exact physical final")
				}
				udpRetryObserved = true
			}
		case <-ctx.Done():
			t.Fatal("Timer G did not retransmit")
		}
	}
	duplicateWrites := 0
	e.cfg.WriteNativeRetry = func(_ context.Context, gotFlow *pb.FlowRef, wire []byte, _ time.Time) error {
		duplicateWrites++
		if !bytes.Equal(wire, udpFinal) || !sameProcess(gotFlow.Ingress, process) || gotFlow.ConnectionId != udpFlow.ConnectionId {
			t.Error("arrival-triggered replay changed the selected physical response")
		}
		return nil
	}
	udpBefore, err := store.ObserveContext(ctx, udpID, udpIncarnation, "r:procedure-"+udpRequestID)
	if err != nil {
		t.Fatal(err)
	}
	if handled, duplicateErr := e.TryNativeCompletionParsed(ctx, udpFlow, &pb.SipMessage{Wire: udpMessage.WireBytes()}, nil, udpMessage); duplicateErr != nil || !handled || duplicateWrites != 1 {
		t.Fatal("positively written UDP final was not replayed exactly once", duplicateErr, duplicateWrites)
	}
	udpAfter, err := store.ObserveContext(ctx, udpID, udpIncarnation, "r:procedure-"+udpRequestID)
	if err != nil || udpBefore.View.Revision != udpAfter.View.Revision || !bytes.Equal(udpBefore.View.Records["r:procedure-"+udpRequestID], udpAfter.View.Records["r:procedure-"+udpRequestID]) {
		t.Fatal("UDP replay changed its original native lifetime", err)
	}
	negativeACK, err := portable.BuildNon2xxACK(udpMessage, portable.RestoreMessage(udpFinal, "udp", "", ""))
	if err != nil {
		t.Fatal(err)
	}
	negativeACK = portable.RestoreMessage(negativeACK.WireBytes(), "udp", "192.0.2.12:5061", "")
	if handled, ackErr := e.TryNativeCompletionParsed(ctx, udpFlow, &pb.SipMessage{Wire: negativeACK.WireBytes()}, nil, negativeACK); ackErr != nil || !handled {
		t.Fatal("UDP non-2xx ACK did not match", ackErr)
	}
	udpKey := nativeUDPRetryKey{Context: udpID, Incarnation: udpIncarnation, Procedure: "r:procedure-" + udpRequestID}
	for {
		e.udp.mu.Lock()
		_, active := e.udp.entries[udpKey]
		e.udp.mu.Unlock()
		if !active {
			break
		}
		select {
		case <-time.After(10 * time.Millisecond):
		case <-ctx.Done():
			t.Fatal("UDP ACK did not stop Timer G derivative")
		}
	}
	unknownID, unknownIncarnation, unknownRequestID := uuid.NewString(), uuid.NewString(), uuid.NewString()
	keys = append(keys, "core:context:"+unknownID, "core:deadlines:"+unknownID)
	unknownMessage, err := portable.ParseMessage(stateTestRequest("INVITE", 1, "", "-native-boundary-unknown").Wire, "udp", "192.0.2.12:5061", "")
	if err != nil {
		t.Fatal(err)
	}
	unknownContext := &pb.ContextRef{Owner: process, Id: unknownID, State: &pb.StateRef{Authority: e.StateAuthority(), GroupId: unknownID, Incarnation: unknownIncarnation, CallId: "redis-native-call@example.test"}}
	unknownDeadline := time.Now().Add(time.Second)
	unknownPresentation, err := receiveRedisInvite(ctx, store, redisRegisterReceive{IdentityKey: key, Context: unknownContext, Authority: e.StateAuthority(), Application: application, Executor: process, Lease: rediscore.ApplicationLeaseRef{Application: appID, Term: ack.ApplicationLease.Term}, Flow: udpFlow, Message: unknownMessage, RequestID: unknownRequestID, LocalTag: "unknown-local", Timer: rediscore.TimerAssignment{Worker: e.worker, Epoch: process.Epoch, Term: process.Epoch}, Deadline: unknownDeadline})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = commitRedisInviteFinal(ctx, store, key, application, rediscore.ApplicationLeaseRef{Application: appID, Term: ack.ApplicationLease.Term}, residentDirectDecision(unknownPresentation, 1, 486), "unknown-reject", "sip:edge@example.test", "edge.example.test:5060", unknownDeadline); err != nil {
		t.Fatal(err)
	}
	unknownOutputField := "r:invite-output-" + unknownRequestID + "-1"
	var unknownWrite *pb.FlowWrite
	for unknownWrite == nil {
		view, readErr := store.ObserveContext(ctx, unknownID, unknownIncarnation, unknownOutputField)
		if readErr != nil {
			t.Fatal(readErr)
		}
		output, decodeErr := decodeNativeOutput(view.View.Records[unknownOutputField])
		if decodeErr != nil {
			t.Fatal(decodeErr)
		}
		proof, inspectErr := e.InspectWrite(ctx, output.Write)
		if inspectErr != nil {
			t.Fatal(inspectErr)
		}
		ref, refErr := nativeOutputRef(output.Write)
		if refErr != nil {
			t.Fatal(refErr)
		}
		selected, inspectErr := store.InspectOutput(ctx, ref)
		if inspectErr == nil && selected.State == rediscore.OutputUnknown {
			if proof.Outcome != pb.DeliveryResult_UNKNOWN || proof.Recorded {
				t.Fatal("historical inspection invented a definite physical outcome")
			}
			unknownWrite = output.Write
			break
		}
		select {
		case <-time.After(time.Millisecond):
		case <-ctx.Done():
			t.Fatal("unknown physical attempt was not recorded")
		}
	}
	beforeUnknown := duplicateWrites
	if handled, duplicateErr := e.TryNativeCompletionParsed(ctx, udpFlow, &pb.SipMessage{Wire: unknownMessage.WireBytes()}, nil, unknownMessage); duplicateErr != nil || !handled || duplicateWrites != beforeUnknown {
		t.Fatal("uncertain final permitted a replay", duplicateErr)
	}
}
