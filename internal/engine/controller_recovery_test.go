package engine

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
	"github.com/google/uuid"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"sipbridge/internal/control"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/state"
)

// These tests use real authenticated WebSockets and the native CAS engine with
// portable SIP vectors. They qualify the control/owner boundary, not a network
// SIP/media deployment. The process-wire suite exercises that separate surface.
func TestControllerReattachThroughControlPreservesNativeContexts(t *testing.T) {
	for _, backend := range []string{state.MemoryBackend, state.RedisBackend} {
		t.Run(backend, func(t *testing.T) { testControllerReattachThroughControl(t, backend) })
	}
}

type recoveryControlFixture struct {
	owner          nativeOwnerFixture
	server         *control.Server
	address, token string
}

func newRecoveryControlFixture(t *testing.T, backend string) recoveryControlFixture {
	t.Helper()
	f := newNativeOwnerFixtureBackend(t, backend)
	ctx, cancel := context.WithCancel(context.Background())
	e := f.engine
	e.ctx, e.cancel = ctx, cancel
	e.work, e.background = make(chan struct{}, 64), make(chan struct{}, 48)
	e.scheduled, e.dispatching = make(map[string]bool), make(map[string]bool)
	token := strings.Repeat("synthetic-controller-token-", 3)
	tokenFile := filepath.Join(t.TempDir(), "control-token")
	if err := os.WriteFile(tokenFile, []byte(token), 0600); err != nil {
		t.Fatal(err)
	}
	auth, err := control.NewBearerAuthenticator(tokenFile, "controller-recovery-test")
	if err != nil {
		t.Fatal(err)
	}
	cfg := control.Config{Process: e.cfg.Process, Authenticator: auth, AllowPlain: true,
		Capabilities: []string{Capability, control.StateCapability, control.DirectoryReconcileCapability, control.CompletionRelayCapability,
			control.NativeCompletionCapability, control.CommandAdmissionCapability, control.ControllerReattachCapability},
		StateAuthority: e.StateAuthority(), HeartbeatInterval: time.Second, HeartbeatTimeout: 6 * time.Second,
		OnControllerSession: e.ControllerSession, OnControllerRenew: e.ControllerRenew,
		OnStatus: func(peer *control.Peer, status *pb.SessionStatus) error {
			if status.RetainedReady && status.BindingsReady {
				return e.ControllerReady(ctx, peer)
			}
			return nil
		},
		OnMessage: e.Handle,
	}
	server, err := control.NewServer(cfg)
	if err != nil {
		t.Fatal(err)
	}
	e.control = server
	t.Cleanup(e.Close)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- server.Serve(ctx, listener) }()
	t.Cleanup(func() {
		cancel()
		server.Close()
		listener.Close()
		if err := <-done; err != nil {
			t.Error(err)
		}
	})
	return recoveryControlFixture{owner: f, server: server, address: "ws://" + listener.Addr().String() + "/v2/control", token: token}
}

func (f recoveryControlFixture) connect(t *testing.T, process *pb.ProcessRef, attempt uint64, recovery *pb.ControllerRecovery) (*websocket.Conn, *pb.SessionHelloAck) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	headers := http.Header{"Authorization": []string{"Bearer " + f.token}}
	connection, _, err := websocket.Dial(ctx, f.address, &websocket.DialOptions{HTTPHeader: headers, Subprotocols: []string{control.Subprotocol}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { connection.CloseNow() })
	recoverySend(t, connection, &pb.Envelope{CorrelationId: "hello", Payload: &pb.Envelope_Hello{Hello: &pb.SessionHello{Controller: process, ConnectionAttempt: attempt, Major: 2,
		Capabilities: []string{Capability, control.StateCapability, control.DirectoryReconcileCapability, control.CompletionRelayCapability,
			control.NativeCompletionCapability, control.CommandAdmissionCapability, control.ControllerReattachCapability}, Bounds: &pb.Bounds{MaxEnvelopeBytes: control.MaxEnvelope}, Recovery: recovery}}})
	reply, err := recoveryRead(connection, "", "hello")
	if err != nil || reply.GetHelloAck() == nil {
		t.Fatalf("controller hello: %v", err)
	}
	// Reading a frame can beat the server publishing the completed write.
	recoveryRoundtrip(t, connection, reply.SessionId)
	return connection, reply.GetHelloAck()
}

func recoverySend(t *testing.T, connection *websocket.Conn, envelope *pb.Envelope) {
	t.Helper()
	data, err := protojson.Marshal(envelope)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := connection.Write(ctx, websocket.MessageText, data); err != nil {
		t.Fatal(err)
	}
}

func recoveryRead(connection *websocket.Conn, sessionID, correlation string) (*pb.Envelope, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	for {
		kind, data, err := connection.Read(ctx)
		if err != nil {
			return nil, err
		}
		if kind != websocket.MessageText {
			return nil, fmt.Errorf("non-text control reply")
		}
		envelope := new(pb.Envelope)
		if err := protojson.Unmarshal(data, envelope); err != nil {
			return nil, err
		}
		if ping := envelope.GetPing(); ping != nil {
			response, err := protojson.Marshal(&pb.Envelope{SessionId: sessionID, CorrelationId: envelope.CorrelationId, Payload: &pb.Envelope_Pong{Pong: ping}})
			if err != nil {
				return nil, err
			}
			if err := connection.Write(ctx, websocket.MessageText, response); err != nil {
				return nil, err
			}
			continue
		}
		if envelope.CorrelationId == correlation {
			return envelope, nil
		}
		if envelope.GetDialogEvent() != nil {
			continue
		}
		return nil, fmt.Errorf("unexpected correlated control reply %q (%T)", envelope.CorrelationId, envelope.Payload)
	}
}

func recoveryRoundtrip(t *testing.T, connection *websocket.Conn, sessionID string) {
	t.Helper()
	recoverySend(t, connection, &pb.Envelope{SessionId: sessionID, CorrelationId: "barrier", Payload: &pb.Envelope_Ping{Ping: &pb.Heartbeat{Nonce: "barrier"}}})
	reply, err := recoveryRead(connection, sessionID, "barrier")
	if err != nil || reply.GetPong().GetNonce() != "barrier" {
		t.Fatalf("control barrier: %v", err)
	}
}

func (f recoveryControlFixture) ready(t *testing.T, connection *websocket.Conn, ack *pb.SessionHelloAck) {
	t.Helper()
	recoverySend(t, connection, &pb.Envelope{SessionId: ack.SessionId, CorrelationId: "ready", Payload: &pb.Envelope_Status{Status: &pb.SessionStatus{RetainedReady: true, BindingsReady: true, AdmitCalls: true}}})
	recoveryRoundtrip(t, connection, ack.SessionId)
}

func recoveryEstablished(t *testing.T, f nativeOwnerFixture, callID string, generation uint64) (*pb.ContextRef, []byte) {
	t.Helper()
	message := stateTestRequest("INVITE", 1, "", callID)
	if callID != "native-owner" {
		var err error
		message, err = Observe(bytes.ReplaceAll(message.Wire, []byte("redis-native-call@example.test"), []byte(callID+"@example.test")), false)
		if err != nil {
			t.Fatal(err)
		}
	}
	ref, err := f.engine.Locate(context.Background(), message)
	if err != nil {
		t.Fatal(err)
	}
	result := f.apply(t, ref, callID+"-ingest", stateEngineInput{Kind: "ingest", Application: f.app, ApplicationLeaseTerm: f.term, ApplicationGeneration: generation,
		Ingest: &pb.OwnerIngest{Context: ref, Application: f.app, Flow: f.flow, Wire: message.Wire, PrincipalId: "phone", BindingId: "binding"}})
	if result.Error != "" {
		t.Fatal(result.Error)
	}
	_, group := stateTestLoad(t, f.engine, ref)
	var requestID string
	for id := range group.Contexts[ref.Id].Requests {
		requestID = id
	}
	result = f.apply(t, ref, callID+"-answer", stateEngineInput{Kind: "command", Application: f.app, ApplicationLeaseTerm: f.term, ApplicationGeneration: generation,
		Command: &pb.Command{Context: ref, Sequence: 1, Action: &pb.Command_Respond{Respond: &pb.Respond{RequestId: requestID, Status: 200, Authority: f.responseAuthority(t, ref, requestID)}}}})
	if result.Error != "" {
		t.Fatal(result.Error)
	}
	_, group = stateTestLoad(t, f.engine, ref)
	var remote portable.Dialog
	for _, transaction := range group.Contexts[ref.Id].Transactions {
		if transaction.Native.Family == portable.InviteServer && transaction.Native.LastResponse != nil {
			remote, err = portable.NewDialog(portable.UAC, transaction.Native.Origin, *transaction.Native.LastResponse, portable.DefaultTimers())
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	ack, err := portable.BuildDialogRequest(remote, sip.ACK, 1, nil, "z9hG4bK-recovery-ack")
	if err != nil {
		t.Fatal(err)
	}
	result = f.apply(t, ref, callID+"-ack", stateEngineInput{Kind: "ingest", Application: f.app, ApplicationLeaseTerm: f.term, ApplicationGeneration: generation,
		Ingest: &pb.OwnerIngest{Context: ref, Application: f.app, Flow: f.flow, Wire: ack.Wire}})
	if result.Error != "" {
		t.Fatal(result.Error)
	}
	_, group = stateTestLoad(t, f.engine, ref)
	snapshot := group.Contexts[ref.Id].Snapshot
	if !snapshot.Answered || !snapshot.InitialAck || snapshot.Ended || snapshot.AttachmentGeneration != generation {
		t.Fatal("fixture did not establish a confirmed native dialog")
	}
	return ref, ack.Wire
}

func recoveryUnknownInfo(t *testing.T, f nativeOwnerFixture, ref *pb.ContextRef) (state.Effect, *pb.FlowWrite) {
	t.Helper()
	result := f.apply(t, ref, "unknown-info", stateEngineInput{Kind: "command", Application: f.app, ApplicationLeaseTerm: f.term, ApplicationGeneration: 1,
		Command: &pb.Command{Context: ref, Sequence: 2, Admission: policyDecision(f.flow), Action: &pb.Command_DialogRequest{DialogRequest: &pb.DialogRequest{Method: "INFO"}}}})
	if result.Error != "" {
		t.Fatal(result.Error)
	}
	record, _ := stateTestLoad(t, f.engine, ref)
	for _, effect := range record.Payload.Effects {
		var body stateEngineEffect
		if effect.Kind != "deliver" || json.Unmarshal(effect.Payload, &body) != nil || body.Command != "unknown-info" {
			continue
		}
		write := &pb.FlowWrite{Flow: body.Permit.Flow, Effect: body.Authorization.Effect, Permit: body.Permit}
		if _, err := f.engine.Reserve(context.Background(), write); err != nil {
			t.Fatal(err)
		}
		if err := f.engine.Complete(context.Background(), write, &pb.DeliveryResult{Effect: write.Effect, Outcome: pb.DeliveryResult_UNKNOWN}); err != nil {
			t.Fatal(err)
		}
		updated, _ := stateTestLoad(t, f.engine, ref)
		for _, saved := range updated.Payload.Effects {
			if saved.ID == effect.ID {
				if saved.Status != state.EffectUnknown {
					t.Fatal("fixture did not retain physical uncertainty")
				}
				return saved, write
			}
		}
	}
	t.Fatal("native INFO did not create a retained physical effect")
	return state.Effect{}, nil
}

func testControllerReattachThroughControl(t *testing.T, backend string) {
	f := newRecoveryControlFixture(t, backend)
	ctx := context.Background()
	foreign := f.owner.admitCall(t, "foreign-owner-call")
	actual := &pb.ProcessRef{InstanceId: "recoverable-controller", Epoch: "first-process"}
	first, initial := f.connect(t, actual, 1, nil)
	f.ready(t, first, initial)
	original, err := f.server.Peer(actual)
	if err != nil {
		t.Fatal(err)
	}
	f.owner.app, f.owner.term = initial.ApplicationLease.Application, initial.ApplicationLease.Term
	established, _ := recoveryEstablished(t, f.owner, "recover-call", 1)
	uncertain, ackWire := recoveryEstablished(t, f.owner, "uncertain-call", 1)
	unknown, unknownWrite := recoveryUnknownInfo(t, f.owner, uncertain)
	_, before := stateTestLoad(t, f.owner.engine, established)
	initialSnapshot := proto.Clone(before.Contexts[established.Id].Snapshot).(*pb.ContextSnapshot)

	// A wrong proof on a higher connection attempt must leave a live holder alone.
	bad, _, err := websocket.Dial(ctx, f.address, &websocket.DialOptions{HTTPHeader: http.Header{"Authorization": []string{"Bearer " + f.token}}, Subprotocols: []string{control.Subprotocol}})
	if err != nil {
		t.Fatal(err)
	}
	defer bad.CloseNow()
	recoverySend(t, bad, &pb.Envelope{CorrelationId: "wrong-proof", Payload: &pb.Envelope_Hello{Hello: &pb.SessionHello{Controller: actual, ConnectionAttempt: 2, Major: 2, Capabilities: initial.Capabilities, Bounds: &pb.Bounds{MaxEnvelopeBytes: control.MaxEnvelope},
		Recovery: &pb.ControllerRecovery{Application: f.owner.app, Term: f.owner.term, Token: base64.RawURLEncoding.EncodeToString(make([]byte, 32)), AttemptId: uuid.NewString(), AttachmentGeneration: 1}}}})
	if _, err := recoveryRead(bad, "", "wrong-proof"); err == nil {
		t.Fatal("incorrect recovery token accepted")
	}
	if original.Context().Err() != nil {
		t.Fatal("invalid proof disconnected the valid holder")
	}
	recoveryRoundtrip(t, first, initial.SessionId)

	first.CloseNow()
	select {
	case <-original.Context().Done():
	case <-time.After(time.Second):
		t.Fatal("closed controller remained live")
	}
	leaseID := f.owner.engine.applicationLeaseID(f.owner.app)
	deadline := time.NewTimer(7 * time.Second)
	defer deadline.Stop()
	poll := time.NewTicker(10 * time.Millisecond)
	defer poll.Stop()
	var expired state.ApplicationLease
	for {
		expired, err = f.owner.engine.store.ReadApplicationLease(ctx, leaseID)
		if err != nil {
			t.Fatal(err)
		}
		if expired.ExpiresAtMS <= expired.CheckedAtMS {
			break
		}
		select {
		case <-poll.C:
		case <-deadline.C:
			t.Fatal("recoverable holder did not expire in its advertised five-second lifetime")
		}
	}
	// Exercise a due native owner timer while the controller is orphaned.
	record, orphan := stateTestLoad(t, f.owner.engine, established)
	timerID := "t-" + f.owner.engine.id(established.Id, "owner", "probe", "")[:48]
	for _, timer := range record.Payload.Timers {
		if timer.ID == timerID && timer.DueMS <= time.Now().UnixMilli() {
			result := f.owner.apply(t, established, "orphan-owner-probe", stateEngineInput{Kind: "timer", Timer: timer.ID, Generation: timer.Generation})
			if result.Error != "" {
				t.Fatal(result.Error)
			}
		}
	}
	_, orphan = stateTestLoad(t, f.owner.engine, established)
	if orphan.Contexts[established.Id].OwnerLost || orphan.Contexts[established.Id].Snapshot.Ended {
		t.Fatal("native owner timer ended an established call inside its reattach window")
	}

	replacement := &pb.ProcessRef{InstanceId: actual.InstanceId, Epoch: "replacement-process"}
	proof := &pb.ControllerRecovery{Application: f.owner.app, Term: f.owner.term, Token: initial.ApplicationLease.RecoveryToken, AttemptId: uuid.NewString(), AttachmentGeneration: 1}
	second, attached := f.connect(t, replacement, 1, proof)
	if !proto.Equal(attached.ApplicationLease.Application, f.owner.app) || attached.ApplicationLease.Term != f.owner.term || attached.ApplicationLease.AttachmentGeneration != 2 {
		t.Fatal("reattach recreated lineage instead of transferring the holder")
	}
	pending, err := f.owner.engine.store.ReadApplicationLease(ctx, leaseID)
	if err != nil {
		t.Fatal(err)
	}
	if pending.AttachmentUntilMS != expired.ExpiresAtMS+int64(initial.ApplicationLease.ReattachTimeoutMs) {
		t.Fatal("reattach changed the original bounded restoration deadline")
	}
	// Repeating the exact claim models a lost helloAck. It cannot rotate again or
	// buy a fresh timeout, even though the new transport has a higher attempt.
	second.CloseNow()
	second, attached = f.connect(t, replacement, 2, proof)
	if attached.ApplicationLease.AttachmentGeneration != 2 {
		t.Fatal("lost-reply replay transferred ownership twice")
	}
	holder, err := f.server.Peer(f.owner.app)
	if err != nil {
		t.Fatal(err)
	}
	if err := holder.ResumeApplicationLease(); err != nil {
		t.Fatal(err)
	}
	renewed, err := f.owner.engine.store.ReadApplicationLease(ctx, leaseID)
	if err != nil || renewed.AttachmentUntilMS != pending.AttachmentUntilMS {
		t.Fatal("heartbeat or lost-reply replay extended unfinished restoration")
	}

	found := map[string]bool{}
	cursor := ""
	pages := 0
	for {
		recoverySend(t, second, &pb.Envelope{SessionId: attached.SessionId, CorrelationId: "inventory", RemainingBudgetMs: 2000, Payload: &pb.Envelope_ControllerContexts{ControllerContexts: &pb.ControllerContexts{Limit: 1, Cursor: cursor}}})
		inventory, err := recoveryRead(second, attached.SessionId, "inventory")
		if err != nil || inventory.GetControllerContextsResult() == nil {
			t.Fatalf("owner inventory unavailable before readiness: %v", err)
		}
		page := inventory.GetControllerContextsResult()
		if len(page.Contexts) > 1 {
			t.Fatal("owner inventory exceeded requested page size")
		}
		for _, ref := range page.Contexts {
			found[ref.Id+":"+ref.GetState().GetGroupId()] = true
		}
		pages++
		if page.Cursor == "" {
			break
		}
		if pages > 4 || page.Cursor == cursor {
			t.Fatal("owner inventory pagination did not advance")
		}
		cursor = page.Cursor
	}
	if pages < 2 {
		t.Fatal("owner inventory did not exercise cursor roundtrip")
	}
	if len(found) != 2 || !found[established.Id+":"+established.State.GroupId] || !found[uncertain.Id+":"+uncertain.State.GroupId] || found[foreign.Id+":"+foreign.State.GroupId] {
		t.Fatal("owner inventory omitted retained calls or exposed a foreign owner")
	}
	recoverySend(t, second, &pb.Envelope{SessionId: attached.SessionId, CorrelationId: "inspect", RemainingBudgetMs: 2000, Payload: &pb.Envelope_Inspect{Inspect: &pb.ContextInspect{Context: established}}})
	inspected, err := recoveryRead(second, attached.SessionId, "inspect")
	if err != nil || inspected.GetResult().GetSnapshot() == nil || inspected.GetResult().Snapshot.Ended {
		t.Fatalf("retained dialog inspection failed: %v", err)
	}
	snap := inspected.GetResult().Snapshot
	if snap.CallId != initialSnapshot.CallId || snap.LocalTag != initialSnapshot.LocalTag || snap.RemoteTag != initialSnapshot.RemoteTag || snap.ApplicationLeaseTerm != initialSnapshot.ApplicationLeaseTerm {
		t.Fatal("reattach changed SIP dialog identity")
	}
	command := &pb.Command{Context: established, Sequence: 2, Admission: policyDecision(f.owner.flow), Action: &pb.Command_DialogRequest{DialogRequest: &pb.DialogRequest{Method: "INFO"}}}
	recoverySend(t, second, &pb.Envelope{SessionId: attached.SessionId, CorrelationId: "too-early", RemainingBudgetMs: 2000, Payload: &pb.Envelope_Command{Command: command}})
	early, err := recoveryRead(second, attached.SessionId, "too-early")
	if err != nil || early.GetResult().GetCertainty() != pb.OperationResult_NOT_ACCEPTED {
		t.Fatal("fresh command ran before application restoration")
	}
	f.ready(t, second, attached)
	// A native duplicate ACK gives the uncertain dialog a normal reduction after
	// activation. It must adopt the new generation without replaying its INFO.
	acked := f.owner.apply(t, uncertain, "post-reattach-ack", stateEngineInput{Kind: "ingest", Application: f.owner.app, ApplicationLeaseTerm: f.owner.term, ApplicationGeneration: 2,
		Ingest: &pb.OwnerIngest{Context: uncertain, Application: f.owner.app, Flow: f.owner.flow, Wire: ackWire}})
	if acked.Error != "" {
		t.Fatal(acked.Error)
	}
	afterUnknown, _ := stateTestLoad(t, f.owner.engine, uncertain)
	preserved := false
	for _, effect := range afterUnknown.Payload.Effects {
		if effect.ID == unknown.ID {
			var before, after stateEngineEffect
			if json.Unmarshal(unknown.Payload, &before) != nil || json.Unmarshal(effect.Payload, &after) != nil {
				t.Fatal("physical evidence corrupt")
			}
			if effect.Status != state.EffectUnknown || effect.AttemptID != unknown.AttemptID || !bytes.Equal(before.Wire, after.Wire) {
				t.Fatal("reattach discarded or replayed UNKNOWN physical evidence")
			}
			preserved = true
		}
	}
	if !preserved {
		t.Fatal("UNKNOWN physical attempt disappeared during lazy adoption")
	}
	physical, err := f.owner.engine.InspectWrite(ctx, unknownWrite)
	if err != nil || physical.Outcome != pb.DeliveryResult_UNKNOWN {
		t.Fatal("reattach changed exact physical write evidence")
	}
	result := f.owner.apply(t, established, "reattached-info", stateEngineInput{Kind: "command", Application: f.owner.app, ApplicationLeaseTerm: f.owner.term, ApplicationGeneration: 2, Command: command})
	if result.Error != "" {
		t.Fatal(result.Error)
	}
	_, restored := stateTestLoad(t, f.owner.engine, established)
	if restored.Contexts[established.Id].Snapshot.AttachmentGeneration != 2 {
		t.Fatal("confirmed dialog did not adopt the new fenced holder")
	}
	revision := restored.Contexts[established.Id].Snapshot.Revision
	stale, err := f.owner.engine.handle(ctx, original, &pb.Envelope{CorrelationId: "stale-holder", RemainingBudgetMs: 2000, Payload: &pb.Envelope_Command{Command: &pb.Command{Context: established, Sequence: 3, Action: &pb.Command_Cancel{Cancel: &pb.Cancel{}}}}}, time.Now().Add(2*time.Second))
	if err != nil || stale.Certainty != pb.OperationResult_NOT_ACCEPTED {
		t.Fatal("old attachment generation was not rejected")
	}
	_, untouched := stateTestLoad(t, f.owner.engine, established)
	if untouched.Contexts[established.Id].Snapshot.Revision != revision {
		t.Fatal("stale holder changed native state")
	}
}
