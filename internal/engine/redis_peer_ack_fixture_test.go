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
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"sipbridge/internal/control"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/redisclient"
	"sipbridge/internal/rediscore"
)

func peerACKRedisFixture(t *testing.T, optIn bool) (*redisNative, *control.Peer, *websocket.Conn, context.Context, string, *pb.FlowRef, <-chan []byte) {
	t.Helper()
	return peerACKRedisFixtureWithCapabilities(t, optIn, nil)
}

func peerACKRedisFixtureWithCapabilities(t *testing.T, optIn bool, liveFlow func(*pb.FlowRef) bool, extraCapabilities ...string) (*redisNative, *control.Peer, *websocket.Conn, context.Context, string, *pb.FlowRef, <-chan []byte) {
	t.Helper()
	address := os.Getenv("SIPBRIDGE_CORE_TEST_REDIS")
	if address == "" {
		t.Skip("isolated Redis fixture required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	t.Cleanup(cancel)
	root := "sipstate:test:peer-ack-" + uuid.NewString() + ":"
	key := bytes.Repeat([]byte{7}, 32)
	process := &pb.ProcessRef{InstanceId: "native", Epoch: uuid.NewString()}
	application := &pb.ProcessRef{InstanceId: "controller", Epoch: uuid.NewString()}
	connection, err := rediscore.NewConnection(rediscore.ConnectionConfig{Redis: redisclient.Options{Address: address, IOTimeout: time.Second}, Profile: rediscore.ProfileConfig{Root: root, Service: "test", Generation: "one", IdentityKey: key, RecordBudget: 4 << 20}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = connection.Close()
		client := redis.NewClient(&redis.Options{Addr: address, MaxRetries: -1})
		defer client.Close()
		keys, err := client.Keys(context.Background(), root+"*").Result()
		if err != nil {
			t.Error(err)
			return
		}
		if len(keys) > 0 {
			if err := client.Del(context.Background(), keys...).Err(); err != nil {
				t.Error(err)
			}
		}
	})
	token := strings.Repeat("synthetic-test-token-", 3)
	file := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(file, []byte(token), 0600); err != nil {
		t.Fatal(err)
	}
	auth, err := control.NewBearerAuthenticator(file, "redis-peer-ack-test")
	if err != nil {
		t.Fatal(err)
	}
	var e *redisNative
	capabilities := []string{Capability, control.StateCapability, control.NativeCompletionCapability, control.CommandAdmissionCapability, control.CommandBudgetCapability, control.DialogContinuityCapability, control.RequestDecisionCapability}
	capabilities = append(capabilities, extraCapabilities...)
	if optIn {
		capabilities = append(capabilities, control.ReinviteAckCapability)
	}
	server, err := control.NewServer(control.Config{Process: process, Authenticator: auth, AllowPlain: true, StateAuthority: connection.Authority(), Capabilities: append(append([]string(nil), capabilities...), control.ReinviteAckCapability),
		OnApplicationLease: func(ctx context.Context, app *pb.ProcessRef, term string) (*pb.ApplicationLease, error) {
			lease, err := e.ApplicationLease(ctx, app, term)
			if err != nil {
				t.Logf("fixture lease error: %v", err)
			}
			return lease, err
		},
		OnMessage: func(peer *control.Peer, envelope *pb.Envelope) error { return e.Handle(peer, envelope) },
		OnStatus:  func(peer *control.Peer, _ *pb.SessionStatus) error { return e.ControllerReady(ctx, peer) },
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(server.Close)
	written := make(chan []byte, 32)
	cfg := Config{Process: process, AdvertiseAddress: "sip.example.test:5061", ContactURI: "sips:sip.example.test:5061", StateIdentityKey: key, LiveFlow: liveFlow}
	cfg.OnNativeDriverDiagnostic = func(d rediscore.DriverDiagnostic) { t.Logf("fixture driver: %s/%s", d.Stage, d.Category) }
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
		select {
		case written <- reserved.Wire:
		case <-ctx.Done():
			return result
		}
		result.Outcome = pb.DeliveryResult_WRITTEN
		if err := e.Complete(ctx, write, result); err != nil {
			result.Error = err.Error()
			return result
		}
		result.Recorded = true
		return result
	}
	e, err = newRedisNative(ctx, cfg, connection, server)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(e.Close)
	for e.Ready() != nil {
		select {
		case <-time.After(time.Millisecond):
		case <-ctx.Done():
			t.Fatal("worker did not become ready")
		}
	}
	httpServer := httptest.NewServer(server)
	t.Cleanup(httpServer.Close)
	ws, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(httpServer.URL, "http")+"/v1/control", &websocket.DialOptions{Subprotocols: []string{control.Subprotocol}, HTTPHeader: http.Header{"Authorization": {"Bearer " + token}}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ws.CloseNow() })
	writeControllerEnvelope(t, ctx, ws, &pb.Envelope{CorrelationId: "hello", Payload: &pb.Envelope_Hello{Hello: &pb.SessionHello{Controller: application, ConnectionAttempt: 1, Major: 1, Bounds: &pb.Bounds{MaxEnvelopeBytes: control.MaxEnvelope}, Capabilities: capabilities}}})
	hello := readControllerEnvelope(t, ctx, ws).GetHelloAck()
	if hello == nil {
		t.Fatal("hello missing")
	}
	writeControllerEnvelope(t, ctx, ws, &pb.Envelope{SessionId: hello.SessionId, CorrelationId: "ready", Payload: &pb.Envelope_Status{Status: &pb.SessionStatus{RetainedReady: true, BindingsReady: true, AdmitCalls: true}}})
	writeControllerEnvelope(t, ctx, ws, &pb.Envelope{SessionId: hello.SessionId, CorrelationId: "barrier", Payload: &pb.Envelope_Ping{Ping: &pb.Heartbeat{Nonce: "barrier"}}})
	if readControllerEnvelope(t, ctx, ws).GetPong().GetNonce() != "barrier" {
		t.Fatal("controller not ready")
	}
	peer, err := server.Peer(application)
	if err != nil {
		t.Fatal(err)
	}
	flow := &pb.FlowRef{Ingress: process, ConnectionId: "phone", SourceIp: "192.0.2.12", SourcePort: 5061, Listener: "sip.example.test:5061", Transport: "tls"}
	return e, peer, ws, ctx, hello.SessionId, flow, written
}
