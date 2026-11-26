package engine

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
	"sipbridge/internal/control"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/rediscore"
	"sipbridge/internal/selectedmessage"
)

func TestRedisNativeResumesPreparedDialogByeOnce(t *testing.T) {
	testRedisNativeResumesPreparedDialog(t, "BYE")
}

func TestRedisNativeResumesPreparedDialogReinviteAndAdopts(t *testing.T) {
	testRedisNativeResumesPreparedDialog(t, "INVITE")
}

func testRedisNativeResumesPreparedDialog(t *testing.T, method string) {
	t.Helper()
	var closeFirst atomic.Bool
	var active *pb.ContextRef
	var store *rediscore.Store
	e, peer, conn, ctx, session, flow, writes := peerACKRedisFixtureWithCapabilities(t, false, func(flow *pb.FlowRef) bool {
		if flow.ConnectionId != "phone" || !closeFirst.Load() {
			return true
		}
		// The flow drops as soon as command admission is visible. Extra optimistic
		// preflight checks before publication must all see the original socket.
		seen, err := store.ObserveContext(context.Background(), active.Id, active.State.Incarnation, "r:result-2")
		return err != nil || len(seen.View.Records["r:result-2"]) == 0
	}, control.DialogSocketCapability, control.DialogSocketResumeCapability)
	initial := stateTestRequest("INVITE", 1, "", "resume-dialog")
	if err := e.ReceiveRequest(ctx, peer, flow, initial); err != nil {
		t.Fatal(err)
	}
	request := peerACKRead(t, ctx, conn, session).GetRequest()
	if request == nil {
		t.Fatal("initial request missing")
	}
	answered := residentSendDecision(t, ctx, conn, session, "initial-answer", residentDirectDecision(request, 1, 200))
	if answered.GetCertainty() != pb.OperationResult_ACCEPTED {
		t.Fatal("initial response", answered)
	}
	final := peerACKFinal(t, ctx, writes)
	if method == "INVITE" {
		remote, err := portable.NewDialog(portable.UAC, portable.RestoreMessage(initial.Wire, "tls", "", ""), portable.RestoreMessage(final, "tls", "", ""), portable.DefaultTimers())
		if err != nil {
			t.Fatal(err)
		}
		ack, err := portable.BuildDialogRequestWithHeaders(remote, sip.ACK, 1, nil, "z9hG4bK-initial-ack-for-resume", nil)
		if err != nil {
			t.Fatal(err)
		}
		if handled, err := e.TryNativeCompletion(ctx, flow, &pb.SipMessage{Wire: ack.WireBytes()}); err != nil || !handled {
			t.Fatal("initial ACK", handled, err)
		}
		frame := peerACKRead(t, ctx, conn, session)
		if frame.GetDialogEvent().GetKind() != pb.DialogEvent_ACKNOWLEDGED {
			t.Fatal("initial ACK event missing", frame)
		}
		writeControllerEnvelope(t, ctx, conn, &pb.Envelope{SessionId: session, CorrelationId: frame.CorrelationId,
			Payload: &pb.Envelope_Result{Result: &pb.OperationResult{Certainty: pb.OperationResult_ACCEPTED}}})
	}
	ref, prior := answered.GetSnapshot().GetContext(), answered.GetSnapshot().GetDelivery()
	active = ref
	var err error
	store, _, _, err = e.driver.Current()
	if err != nil {
		t.Fatal(err)
	}
	replacement, replacementPeer, replacementFlow, replacementWrites := prepareDialogResumePeer(t, ctx, e, peer, flow)
	deadline := time.Now().Add(4 * time.Second)
	create := &pb.Command{Context: ref, Sequence: 2, Admission: &pb.CommandAdmission{
		DirectoryEpoch: answered.GetSnapshot().GetDirectoryEpoch(), ExpiresAtMs: uint64(deadline.UnixMilli()), SelectedFlow: flow, ExpectedDelivery: prior,
	}, Action: &pb.Command_DialogRequest{DialogRequest: &pb.DialogRequest{Method: method}}}
	closeFirst.Store(true)
	parked, err := e.commandApplicationBye(ctx, peer, create, "original-dialog-command", deadline)
	if err != nil || parked.GetCertainty() != pb.OperationResult_ACCEPTED || parked.GetDialogRecovery().GetDisposition() != pb.DialogRecovery_RESUMABLE {
		t.Fatalf("prepared %s was not parked: %v %v", method, parked, err)
	}
	select {
	case oldWire := <-writes:
		t.Fatalf("old socket received bytes: %s", oldWire)
	default:
	}
	view, err := store.ObserveContext(ctx, ref.Id, ref.State.Incarnation, "r:result-2")
	if err != nil {
		t.Fatal(err)
	}
	result, err := decodeNativeResult(view.View.Records["r:result-2"])
	if err != nil || !result.Pending || result.Output == "" {
		t.Fatal("parked receipt lost its original transaction", err)
	}
	old, err := store.Read(ctx, view.Identity, result.Output)
	if err != nil {
		t.Fatal(err)
	}
	metadata, err := decodeNativeOutput(old.Records[result.Output])
	if err != nil {
		t.Fatal(err)
	}
	oldRef, err := nativeOutputRef(metadata.Write)
	if err != nil {
		t.Fatal(err)
	}
	proof, err := store.InspectOutput(ctx, oldRef)
	if err != nil || proof.State != rediscore.OutputResumeParked {
		t.Fatal("park did not retain unstarted output", proof.State, err)
	}
	origin, err := store.Read(ctx, view.Identity, proof.MessageField)
	if err != nil {
		t.Fatal(err)
	}
	message, err := selectedmessage.DecodeOpaque(origin.Records[proof.MessageField])
	if err != nil {
		t.Fatal(err)
	}
	admission := proto.Clone(create.Admission).(*pb.CommandAdmission)
	admission.SelectedFlow, admission.RegistrationFallback = replacementFlow, true
	admission.FallbackDelivery = &pb.ContextDelivery{Flow: replacementFlow, NativeRevision: prior.GetNativeRevision() + 1,
		DirectoryEpoch: admission.DirectoryEpoch, RegistrationTicket: admission.DirectoryEpoch + ":1"}
	resume := &pb.Command{Context: ref, Sequence: 2, Admission: admission,
		Action: &pb.Command_ResumeDialogRequest{ResumeDialogRequest: &pb.ResumeDialogRequest{OperationId: parked.DialogRecovery.OperationId}}}
	type commandResult struct {
		answer *pb.OperationResult
		err    error
	}
	returned := make(chan commandResult, 1)
	go func() {
		answer, err := replacement.commandResumeDialog(ctx, replacementPeer, resume, deadline)
		returned <- commandResult{answer: answer, err: err}
	}()
	var wire []byte
	select {
	case wire = <-replacementWrites:
		if !bytes.Equal(wire, message.WireBytes()) || !bytes.HasPrefix(wire, []byte(method+" ")) {
			t.Fatalf("replacement socket did not write the original %s: %s", method, wire)
		}
	case <-time.After(time.Until(deadline)):
		select {
		case early := <-returned:
			t.Fatalf("replacement write missing: %v %v", early.answer, early.err)
		default:
			t.Fatal("replacement write missing")
		}
	}
	if _, err := store.ReserveOutput(ctx, oldRef); !errors.Is(err, rediscore.ErrAbsent) {
		t.Fatalf("A retained write permission after replacement: %v", err)
	}
	wantCertainty := pb.OperationResult_ACCEPTED
	if method == "INVITE" {
		wantCertainty = pb.OperationResult_COMPLETED
		for {
			view, err := store.Read(ctx, view.Identity, metadata.Procedure)
			if err != nil {
				t.Fatal(err)
			}
			procedure, err := readPrivateProcedureRecord(view.Records[metadata.Procedure])
			if err != nil {
				t.Fatal(err)
			}
			if procedure.OutputConsumed {
				break
			}
			if !time.Now().Before(deadline) {
				t.Fatal("replacement re-INVITE write proof missing")
			}
			time.Sleep(time.Millisecond)
		}
		response, err := portable.BuildResponseWithHeaders(portable.RestoreMessage(wire, "tls", "", ""), 200, "OK", nil, "", "<sips:phone@192.0.2.12:5061>", nil)
		if err != nil {
			t.Fatal(err)
		}
		if matched, err := replacement.TryNativeCompletion(ctx, replacementFlow, &pb.SipMessage{Wire: response.WireBytes()}); err != nil || !matched {
			t.Fatal("response on replacement", matched, err)
		}
	}
	resultWithError := <-returned
	answer := resultWithError.answer
	if resultWithError.err != nil || answer.GetCertainty() != wantCertainty || answer.GetRequest().GetMethod() != method || answer.DialogRecovery != nil || !bytes.Equal(answer.Request.Wire, wire) {
		t.Fatalf("resumed request: %v %v", answer, resultWithError.err)
	}
	if method == "INVITE" && !proto.Equal(answer.GetSnapshot().GetDelivery(), admission.FallbackDelivery) {
		t.Fatal("accepted re-INVITE did not adopt replacement socket", answer)
	}
	duplicate, err := replacement.commandResumeDialog(ctx, replacementPeer, resume, deadline)
	if err != nil || duplicate.GetCertainty() != wantCertainty || !bytes.Equal(duplicate.GetRequest().GetWire(), answer.Request.Wire) {
		t.Fatalf("duplicate did not observe existing write: %v %v", duplicate, err)
	}
	select {
	case wire := <-writes:
		t.Fatalf("old socket wrote after resume: %s", wire)
	case wire := <-replacementWrites:
		if method != "INVITE" || !bytes.HasPrefix(wire, []byte("ACK ")) {
			t.Fatalf("duplicate resume wrote a second command: %s", wire)
		}
	default:
	}
}

func prepareDialogResumePeer(t *testing.T, ctx context.Context, source *redisNative, sourcePeer *control.Peer, sourceFlow *pb.FlowRef) (*redisNative, *control.Peer, *pb.FlowRef, <-chan []byte) {
	t.Helper()
	flow := proto.Clone(sourceFlow).(*pb.FlowRef)
	flow.Ingress = &pb.ProcessRef{InstanceId: "native-b", Epoch: uuid.NewString()}
	flow.ConnectionId = "current-registration"
	flow.SourcePort++
	const token = "synthetic-resume-fixture-token-synthetic-resume-fixture-token"
	file := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(file, []byte(token), 0600); err != nil {
		t.Fatal(err)
	}
	auth, err := control.NewBearerAuthenticator(file, "redis-peer-resume-test")
	if err != nil {
		t.Fatal(err)
	}
	var engine *redisNative
	capabilities := []string{Capability, control.StateCapability, control.NativeCompletionCapability, control.CommandAdmissionCapability,
		control.CommandBudgetCapability, control.DialogContinuityCapability, control.RequestDecisionCapability,
		control.DialogSocketCapability, control.DialogSocketResumeCapability}
	server, err := control.NewServer(control.Config{Process: flow.Ingress, Authenticator: auth, AllowPlain: true, StateAuthority: source.StateAuthority(), Capabilities: capabilities,
		OnApplicationLease: func(ctx context.Context, app *pb.ProcessRef, term string) (*pb.ApplicationLease, error) {
			return engine.ApplicationLease(ctx, app, term)
		},
		OnMessage: func(peer *control.Peer, envelope *pb.Envelope) error { return engine.Handle(peer, envelope) },
		OnStatus:  func(peer *control.Peer, _ *pb.SessionStatus) error { return engine.ControllerReady(ctx, peer) },
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(server.Close)
	writes := make(chan []byte, 2)
	cfg := source.cfg
	cfg.Process = flow.Ingress
	cfg.LiveFlow = func(candidate *pb.FlowRef) bool { return proto.Equal(flow, candidate) }
	cfg.WriteCommitted = func(ctx context.Context, write *pb.FlowWrite) *pb.DeliveryResult {
		result := &pb.DeliveryResult{Effect: write.Effect, Outcome: pb.DeliveryResult_NOT_WRITTEN}
		reservation, err := engine.Reserve(ctx, write)
		if err != nil {
			result.Error = err.Error()
			return result
		}
		if reservation.Known {
			result.Outcome, result.Recorded = reservation.Outcome, true
			return result
		}
		if !proto.Equal(write.Flow, flow) {
			result.Error = "unexpected resumed socket"
			return result
		}
		select {
		case writes <- reservation.Wire:
		case <-ctx.Done():
			return result
		}
		result.Outcome = pb.DeliveryResult_WRITTEN
		if err := engine.Complete(ctx, write, result); err != nil {
			result.Error = err.Error()
			return result
		}
		result.Recorded = true
		return result
	}
	engine, err = newRedisNative(ctx, cfg, source.connection, server)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(engine.Close)
	for engine.Ready() != nil {
		select {
		case <-time.After(time.Millisecond):
		case <-ctx.Done():
			t.Fatal("replacement did not become ready")
		}
	}
	httpServer := httptest.NewServer(server)
	t.Cleanup(httpServer.Close)
	ws, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(httpServer.URL, "http")+"/v1/control", &websocket.DialOptions{Subprotocols: []string{control.Subprotocol}, HTTPHeader: http.Header{"Authorization": {"Bearer " + token}}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ws.CloseNow() })
	writeControllerEnvelope(t, ctx, ws, &pb.Envelope{CorrelationId: "hello", Payload: &pb.Envelope_Hello{Hello: &pb.SessionHello{Controller: sourcePeer.Application(),
		ConnectionAttempt: 1, Major: 1, Bounds: &pb.Bounds{MaxEnvelopeBytes: control.MaxEnvelope}, Capabilities: capabilities}}})
	hello := readControllerEnvelope(t, ctx, ws).GetHelloAck()
	if hello == nil {
		t.Fatal("replacement controller hello missing")
	}
	writeControllerEnvelope(t, ctx, ws, &pb.Envelope{SessionId: hello.SessionId, CorrelationId: "ready", Payload: &pb.Envelope_Status{Status: &pb.SessionStatus{RetainedReady: true, BindingsReady: true, AdmitCalls: true}}})
	writeControllerEnvelope(t, ctx, ws, &pb.Envelope{SessionId: hello.SessionId, CorrelationId: "barrier", Payload: &pb.Envelope_Ping{Ping: &pb.Heartbeat{Nonce: "barrier"}}})
	if readControllerEnvelope(t, ctx, ws).GetPong().GetNonce() != "barrier" {
		t.Fatal("replacement controller not ready")
	}
	peer, err := server.Peer(sourcePeer.Application())
	if err != nil {
		t.Fatal(err)
	}
	return engine, peer, flow, writes
}
