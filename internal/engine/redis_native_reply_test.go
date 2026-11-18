package engine

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/google/uuid"
	"sipbridge/internal/control"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/redisclient"
	"sipbridge/internal/rediscore"
)

// A second real worker uses the same application and Redis profile, but a
// distinct authenticated JSON control connection and physical process epoch.
func redisJSONReplyWorker(t *testing.T, ctx context.Context, address, root string, key []byte, tokenFile, token string, cfg Config, application *pb.ProcessRef, capabilities []string) (*redisNative, *websocket.Conn, *pb.SessionHelloAck, func()) {
	t.Helper()
	cfg.Process = &pb.ProcessRef{InstanceId: "reply-worker", Epoch: uuid.NewString()}
	connection, err := rediscore.NewConnection(rediscore.ConnectionConfig{Redis: redisclient.Options{Address: address, IOTimeout: time.Second}, Profile: rediscore.ProfileConfig{Root: root, Service: "test", Generation: "one", IdentityKey: key, RecordBudget: 4 << 20}})
	if err != nil {
		t.Fatal(err)
	}
	auth, err := control.NewBearerAuthenticator(tokenFile, "redis-native-test")
	if err != nil {
		connection.Close()
		t.Fatal(err)
	}
	var engine *redisNative
	server, err := control.NewServer(control.Config{Process: cfg.Process, Authenticator: auth, AllowPlain: true, StateAuthority: connection.Authority(), Capabilities: capabilities,
		OnApplicationLease: func(ctx context.Context, app *pb.ProcessRef, term string) (*pb.ApplicationLease, error) {
			return engine.ApplicationLease(ctx, app, term)
		},
		OnControllerSession: func(ctx context.Context, peer *control.Peer, hello *pb.SessionHello) (*pb.ApplicationLease, error) {
			return engine.ControllerSession(ctx, peer, hello)
		},
		OnControllerRenew: func(ctx context.Context, peer *control.Peer) (*pb.ApplicationLease, error) {
			return engine.ControllerRenew(ctx, peer)
		},
		OnMessage: func(peer *control.Peer, envelope *pb.Envelope) error { return engine.Handle(peer, envelope) },
		OnStatus:  func(peer *control.Peer, _ *pb.SessionStatus) error { return engine.ControllerReady(ctx, peer) },
	})
	if err != nil {
		connection.Close()
		t.Fatal(err)
	}
	engine, err = newRedisNative(ctx, cfg, connection, server)
	if err != nil {
		server.Close()
		connection.Close()
		t.Fatal(err)
	}
	httpServer := httptest.NewServer(server)
	var ws *websocket.Conn
	closeWorker := func() {
		if ws != nil {
			ws.CloseNow()
		}
		httpServer.Close()
		engine.Close()
		server.Close()
		connection.Close()
	}
	for engine.Ready() != nil {
		select {
		case <-time.After(10 * time.Millisecond):
		case <-ctx.Done():
			closeWorker()
			t.Fatal("second worker did not become ready")
		}
	}
	ws, _, err = websocket.Dial(ctx, "ws"+strings.TrimPrefix(httpServer.URL, "http")+"/v1/control", &websocket.DialOptions{Subprotocols: []string{control.Subprotocol}, HTTPHeader: http.Header{"Authorization": {"Bearer " + token}}})
	if err != nil {
		closeWorker()
		t.Fatal(err)
	}
	writeControllerEnvelope(t, ctx, ws, &pb.Envelope{CorrelationId: "hello", Payload: &pb.Envelope_Hello{Hello: &pb.SessionHello{Controller: application, ConnectionAttempt: 1, Major: 1, Bounds: &pb.Bounds{MaxEnvelopeBytes: control.MaxEnvelope}, Capabilities: capabilities}}})
	hello := readControllerEnvelope(t, ctx, ws).GetHelloAck()
	if hello == nil || hello.ApplicationLease == nil {
		closeWorker()
		t.Fatal("second JSON hello failed")
	}
	writeControllerEnvelope(t, ctx, ws, &pb.Envelope{SessionId: hello.SessionId, CorrelationId: "ready", Payload: &pb.Envelope_Status{Status: &pb.SessionStatus{RetainedReady: true, BindingsReady: true, AdmitCalls: true, AdmitSubscriptions: true}}})
	writeControllerEnvelope(t, ctx, ws, &pb.Envelope{SessionId: hello.SessionId, CorrelationId: "barrier", Payload: &pb.Envelope_Ping{Ping: &pb.Heartbeat{Nonce: "barrier"}}})
	if readControllerEnvelope(t, ctx, ws).GetPong().GetNonce() != "barrier" {
		closeWorker()
		t.Fatal("second controller not ready")
	}
	return engine, ws, hello, closeWorker
}
