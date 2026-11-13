package engine

import (
	"bytes"
	"context"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"google.golang.org/protobuf/proto"
	"sipbridge/internal/control"
	pb "sipbridge/internal/controlpb"
)

// This fixture uses the real canonical control transport and runtime constructor.
// Its socket adapter records writes in a channel; actual TLS SIP is covered by
// the separate wire qualification and matched performance workload.
func residentControlFixture(t *testing.T, direct ...bool) (*ResidentMemory, *control.Peer, *websocket.Conn, context.Context, string, <-chan []byte, *atomic.Int64) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	process := &pb.ProcessRef{InstanceId: "native", Epoch: "one"}
	application := &pb.ProcessRef{InstanceId: "controller", Epoch: "one"}
	authority := NewResidentMemoryAuthority("test", "one")
	token := strings.Repeat("synthetic-test-token-", 3)
	file := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(file, []byte(token), 0600); err != nil {
		t.Fatal(err)
	}
	auth, err := control.NewBearerAuthenticator(file, "resident-test")
	if err != nil {
		t.Fatal(err)
	}
	var e *ResidentMemory
	capabilities := []string{Capability, control.StateCapability, control.NativeCompletionCapability, control.CommandAdmissionCapability}
	if len(direct) > 0 && direct[0] {
		capabilities = append(capabilities, control.CommandBudgetCapability, control.DialogContinuityCapability, control.RequestDecisionCapability)
	}
	server, err := control.NewServer(control.Config{Process: process, Authenticator: auth, AllowPlain: true, StateAuthority: authority, Capabilities: capabilities,
		OnApplicationLease: func(ctx context.Context, app *pb.ProcessRef, term string) (*pb.ApplicationLease, error) {
			return e.ApplicationLease(ctx, app, term)
		},
		OnMessage: func(peer *control.Peer, envelope *pb.Envelope) error { return e.Handle(peer, envelope) },
		OnStatus:  func(peer *control.Peer, status *pb.SessionStatus) error { return e.ControllerReady(ctx, peer) }})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(server.Close)
	writes := make(chan []byte, 4)
	count := new(atomic.Int64)
	cfg := Config{Process: process, AdvertiseAddress: "sip.example.test:5061", ContactURI: "sips:sip.example.test:5061", StateIdentityKey: bytes.Repeat([]byte{1}, 32)}
	cfg.WriteCommitted = func(ctx context.Context, write *pb.FlowWrite) *pb.DeliveryResult {
		result := &pb.DeliveryResult{Effect: write.Effect, Outcome: pb.DeliveryResult_NOT_WRITTEN}
		reserved, err := e.Reserve(ctx, write)
		if err != nil {
			result.Error = err.Error()
			return result
		}
		if reserved.Known {
			result.Outcome, result.Recorded = reserved.Outcome, true
			return result
		}
		count.Add(1)
		writes <- append([]byte(nil), reserved.Wire...)
		result.Outcome = pb.DeliveryResult_WRITTEN
		if err := e.Complete(ctx, write, result); err != nil {
			result.Error = err.Error()
			return result
		}
		result.Recorded = true
		return result
	}
	e, err = NewResidentMemory(ctx, cfg, ResidentMemoryOptions{Authority: authority, MaxContexts: 64, MaxBytes: 64 << 20}, server)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(e.Close)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	go server.Serve(ctx, listener)
	connection, _, err := websocket.Dial(ctx, "ws://"+listener.Addr().String()+"/v1/control", &websocket.DialOptions{Subprotocols: []string{control.Subprotocol}, HTTPHeader: http.Header{"Authorization": []string{"Bearer " + token}}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.CloseNow() })
	writeControllerEnvelope(t, ctx, connection, &pb.Envelope{CorrelationId: "hello", Payload: &pb.Envelope_Hello{Hello: &pb.SessionHello{Controller: application, ConnectionAttempt: 1, Major: 1, Bounds: &pb.Bounds{MaxEnvelopeBytes: control.MaxEnvelope}, Capabilities: capabilities}}})
	ack := readControllerEnvelope(t, ctx, connection).GetHelloAck()
	if ack == nil || ack.ApplicationLease == nil {
		t.Fatal("missing native hello/lease")
	}
	for _, capability := range ack.Capabilities {
		if capability == control.ControllerReattachCapability {
			t.Fatal("memory advertised checkpoint recovery")
		}
	}
	writeControllerEnvelope(t, ctx, connection, &pb.Envelope{SessionId: ack.SessionId, CorrelationId: "ready", Payload: &pb.Envelope_Status{Status: &pb.SessionStatus{RetainedReady: true, BindingsReady: true, AdmitCalls: true, AdmitSubscriptions: true}}})
	writeControllerEnvelope(t, ctx, connection, &pb.Envelope{SessionId: ack.SessionId, CorrelationId: "barrier", Payload: &pb.Envelope_Ping{Ping: &pb.Heartbeat{Nonce: "barrier"}}})
	if readControllerEnvelope(t, ctx, connection).GetPong().GetNonce() != "barrier" {
		t.Fatal("peer publication failed")
	}
	peer, err := server.Peer(application)
	if err != nil {
		t.Fatal(err)
	}
	return e, peer, connection, ctx, ack.SessionId, writes, count
}

func TestResidentControlRegistrationDirectResponseAndExactReplay(t *testing.T) {
	e, peer, connection, ctx, session, writes, count := residentControlFixture(t)
	message := stateTestRequest("REGISTER", 1, "", "resident-register")
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
	presented := readControllerEnvelope(t, ctx, connection)
	request := presented.GetRequest()
	if request == nil || request.Message.Method != "REGISTER" {
		t.Fatal("fresh REGISTER was not presented directly")
	}
	if count.Load() != 0 {
		t.Fatal("response preceded controller decision")
	}
	command := &pb.Command{Context: request.Snapshot.Context, Sequence: 1, Admission: &pb.CommandAdmission{DirectoryEpoch: "directory", ExpiresAtMs: uint64(time.Now().Add(2 * time.Second).UnixMilli())},
		Action: &pb.Command_Respond{Respond: &pb.Respond{RequestId: request.RequestId, Status: 200}}}
	envelope := &pb.Envelope{SessionId: session, CorrelationId: "respond-one", RemainingBudgetMs: 2000, Payload: &pb.Envelope_Command{Command: command}}
	writeControllerEnvelope(t, ctx, connection, envelope)
	result := readControllerEnvelope(t, ctx, connection).GetResult()
	if result == nil || result.Certainty != pb.OperationResult_COMPLETED || result.SipStatus != 200 || result.GetSnapshot().GetCommandSequence() != 1 {
		t.Fatal("REGISTER response did not settle its exact command", result)
	}
	select {
	case wire := <-writes:
		if !bytes.Contains(wire, []byte("200 OK")) {
			t.Fatal("wrong physical response")
		}
	case <-ctx.Done():
		t.Fatal("physical write missing")
	}
	writeControllerEnvelope(t, ctx, connection, &pb.Envelope{SessionId: session, CorrelationId: presented.CorrelationId, Payload: &pb.Envelope_Result{Result: &pb.OperationResult{Certainty: pb.OperationResult_ACCEPTED}}})
	if err := <-ingestDone; err != nil {
		t.Fatal(err)
	}
	writeControllerEnvelope(t, ctx, connection, envelope)
	replayed := readControllerEnvelope(t, ctx, connection).GetResult()
	if replayed == nil || replayed.SipStatus != 200 || count.Load() != 1 {
		t.Fatal("exact command replay repeated a write or lost its result", replayed)
	}
	invalid := proto.Clone(command).(*pb.Command)
	invalid.Sequence = 2
	invalid.GetRespond().RequestId = "missing-request"
	writeControllerEnvelope(t, ctx, connection, &pb.Envelope{SessionId: session, CorrelationId: "refused-two", RemainingBudgetMs: 2000, Payload: &pb.Envelope_Command{Command: invalid}})
	refused := readControllerEnvelope(t, ctx, connection).GetResult()
	if refused == nil || refused.Certainty != pb.OperationResult_NOT_ACCEPTED || refused.GetSnapshot().GetCommandSequence() != 2 || count.Load() != 1 {
		t.Fatal("semantic refusal lost sequence disposition or changed output", refused)
	}
}
