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
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/emiago/sipgo/sip/portable"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/proto"
	"sipbridge/internal/control"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/redisclient"
	"sipbridge/internal/rediscore"
)

func TestRedisNativeStandaloneJSON(t *testing.T) {
	address := os.Getenv("SIPBRIDGE_CORE_TEST_REDIS")
	if address == "" {
		t.Skip("isolated Redis fixture required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	root := "sipstate:test:native-transaction-" + uuid.NewString() + ":"
	key := bytes.Repeat([]byte{7}, 32)
	process := &pb.ProcessRef{InstanceId: "native", Epoch: uuid.NewString()}
	application := &pb.ProcessRef{InstanceId: "controller", Epoch: uuid.NewString()}
	connection, err := rediscore.NewConnection(rediscore.ConnectionConfig{Redis: redisclient.Options{Address: address, IOTimeout: time.Second}, Profile: rediscore.ProfileConfig{Root: root, Service: "test", Generation: "one", IdentityKey: key, RecordBudget: 32 << 20}})
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	client := redis.NewClient(&redis.Options{Addr: address, MaxRetries: -1})
	defer client.Close()
	keys := []string{}
	defer func() {
		found, e := client.Keys(context.Background(), root+"*").Result()
		if e != nil {
			t.Error(e)
			return
		}
		if len(found) > 0 {
			if e = client.Del(context.Background(), found...).Err(); e != nil {
				t.Error(e)
			}
		}
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
	var engine *redisNative
	capabilities := []string{Capability, control.StateCapability, control.NativeCompletionCapability, control.CommandAdmissionCapability, control.CommandBudgetCapability, control.DialogContinuityCapability, control.RequestDecisionCapability}
	server, err := control.NewServer(control.Config{Process: process, Authenticator: auth, AllowPlain: true, StateAuthority: connection.Authority(), Capabilities: capabilities,
		OnApplicationLease: func(ctx context.Context, app *pb.ProcessRef, term string) (*pb.ApplicationLease, error) {
			lease, x := engine.ApplicationLease(ctx, app, term)
			if x != nil {
				t.Log("application lease", x)
			}
			return lease, x
		},
		OnControllerSession: func(ctx context.Context, peer *control.Peer, hello *pb.SessionHello) (*pb.ApplicationLease, error) {
			return engine.ControllerSession(ctx, peer, hello)
		},
		OnControllerRenew: func(ctx context.Context, peer *control.Peer) (*pb.ApplicationLease, error) {
			return engine.ControllerRenew(ctx, peer)
		},
		OnMessage: func(peer *control.Peer, envelope *pb.Envelope) error { return engine.Handle(peer, envelope) },
		OnStatus:  func(peer *control.Peer, _ *pb.SessionStatus) error { return engine.ControllerReady(ctx, peer) }})
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	written := make(chan []byte, 4)
	flow := &pb.FlowRef{Ingress: process, ConnectionId: "phone", SourceIp: "192.0.2.12", SourcePort: 5061, Listener: "sip.example.test:5061", Transport: "tls"}
	opened := make(chan string, 1)
	retries := make(chan []byte, 8)
	cfg := Config{Process: process, AdvertiseAddress: "sip.example.test:5061", ContactURI: "sips:sip.example.test:5061", StateIdentityKey: key,
		OpenNetwork: func(_ context.Context, uri string, target *pb.NetworkTarget) (*pb.FlowRef, error) {
			opened <- uri
			selected := proto.Clone(flow).(*pb.FlowRef)
			selected.Transport = target.Transport
			return selected, nil
		}}
	cfg.WriteNativeRetry = func(_ context.Context, _ *pb.FlowRef, wire []byte, _ time.Time) error {
		retries <- bytes.Clone(wire)
		return nil
	}
	cfg.WriteCommitted = func(ctx context.Context, write *pb.FlowWrite) *pb.DeliveryResult {
		result := &pb.DeliveryResult{Effect: write.Effect, Outcome: pb.DeliveryResult_NOT_WRITTEN}
		reserved, reserveErr := engine.Reserve(ctx, write)
		if reserveErr != nil {
			result.Error = reserveErr.Error()
			return result
		}
		if reserved.Known {
			result.Outcome, result.Recorded = reserved.Outcome, true
			return result
		}
		select {
		case written <- bytes.Clone(reserved.Wire):
		case <-ctx.Done():
			return result
		}
		result.Outcome = pb.DeliveryResult_WRITTEN
		if completeErr := engine.Complete(ctx, write, result); completeErr != nil {
			if !errors.Is(completeErr, rediscore.ErrConflict) {
				t.Errorf("complete physical write: %v", completeErr)
			}
			result.Error = completeErr.Error()
			return result
		}
		result.Recorded = true
		return result
	}
	engine, err = newRedisNative(ctx, cfg, connection, server)
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()
	keys = append(keys, "core:worker:"+engine.worker, "core:timers:"+engine.worker, "core:ready:"+engine.worker)
	for engine.Ready() != nil {
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
	hello := readControllerEnvelope(t, ctx, ws).GetHelloAck()
	if hello == nil || hello.ApplicationLease == nil {
		t.Fatal("JSON hello failed")
	}
	appID, err := nativeApplicationReference(key, application)
	if err != nil {
		t.Fatal(err)
	}
	keys = append(keys, "core:application:"+appID, "core:owner:"+hello.ApplicationLease.Term, "core:owned:"+hello.ApplicationLease.Term)
	writeControllerEnvelope(t, ctx, ws, &pb.Envelope{SessionId: hello.SessionId, CorrelationId: "ready", Payload: &pb.Envelope_Status{Status: &pb.SessionStatus{RetainedReady: true, BindingsReady: true, AdmitCalls: true, AdmitSubscriptions: true}}})
	writeControllerEnvelope(t, ctx, ws, &pb.Envelope{SessionId: hello.SessionId, CorrelationId: "barrier", Payload: &pb.Envelope_Ping{Ping: &pb.Heartbeat{Nonce: "barrier"}}})
	if readControllerEnvelope(t, ctx, ws).GetPong().GetNonce() != "barrier" {
		t.Fatal("controller not ready")
	}

	for index, method := range []string{"OPTIONS", "MESSAGE", "NOTIFY"} {
		transport := []string{"tls", "tcp", "udp"}[index]
		selected := proto.Clone(flow).(*pb.FlowRef)
		selected.Transport = transport
		policy := &pb.CommandAdmission{DirectoryEpoch: "directory", ExpiresAtMs: uint64(time.Now().Add(3 * time.Second).UnixMilli()), SelectedFlow: selected}
		if method == "MESSAGE" {
			policy.SelectedFlow = nil
		}
		command := &pb.Command{Context: &pb.ContextRef{Id: strings.ToLower(method)}, Sequence: 1, Admission: policy, Action: &pb.Command_OpenUac{OpenUac: &pb.OpenUac{Method: method, Uri: "sip:phone@example.test", Transport: transport, Network: &pb.NetworkTarget{Transport: transport}, Headers: []*pb.Header{{Name: "Event", Value: "check-sync"}}, Body: []byte("provisioning")}}}
		writeControllerEnvelope(t, ctx, ws, &pb.Envelope{SessionId: hello.SessionId, CorrelationId: method, RemainingBudgetMs: 3000, Payload: &pb.Envelope_Command{Command: command}})
		var wire []byte
		select {
		case wire = <-written:
		case <-ctx.Done():
			t.Fatal("standalone not written", method)
		}
		if !bytes.HasPrefix(wire, []byte(method+" ")) || !bytes.Contains(wire, []byte("Event: check-sync")) {
			t.Fatal("wrong exact request", method)
		}
		if method == "MESSAGE" {
			select {
			case <-opened:
			case <-ctx.Done():
				t.Fatal("network not opened")
			}
		}
		if method == "NOTIFY" {
			select {
			case repeat := <-retries:
				if !bytes.Equal(repeat, wire) {
					t.Fatal("UDP E changed wire")
				}
			case <-time.After(time.Second):
				t.Fatal("UDP E not armed")
			}
		}
		request := portable.RestoreMessage(wire, transport, "", "")
		response, e := portable.ResponseFor(request, 200, "OK", []byte("finished"), "remote")
		if e != nil {
			t.Fatal(e)
		}
		response = portable.RestoreMessage(response.WireBytes(), transport, "192.0.2.12:5061", "")
		if handled, e := engine.TryNativeCompletion(ctx, selected, &pb.SipMessage{Wire: response.WireBytes()}); e != nil || !handled {
			t.Fatal("matched standalone response", method, e)
		}
		for {
			reply := readControllerEnvelope(t, ctx, ws)
			if ping := reply.GetPing(); ping != nil {
				writeControllerEnvelope(t, ctx, ws, &pb.Envelope{SessionId: hello.SessionId, CorrelationId: reply.CorrelationId, Payload: &pb.Envelope_Pong{Pong: ping}})
				continue
			}
			result := reply.GetResult()
			if reply.CorrelationId != method || result == nil || result.Certainty != pb.OperationResult_COMPLETED || result.SipStatus != 200 || result.Snapshot == nil || result.Snapshot.Kind != pb.ContextSnapshot_TRANSACTION || !result.Snapshot.Ended || result.Request == nil || !bytes.Equal(result.Request.Wire, wire) || result.Response == nil || !bytes.Equal(result.Response.Wire, response.WireBytes()) {
				t.Fatal("standalone JSON result", method, reply)
			}
			break
		}
		if method == "NOTIFY" {
			until := time.Now().Add(time.Second)
			for {
				engine.udp.mu.Lock()
				remaining := len(engine.udp.entries)
				engine.udp.mu.Unlock()
				if remaining == 0 {
					break
				}
				if time.Now().After(until) {
					t.Fatal("final hint did not stop E")
				}
				time.Sleep(5 * time.Millisecond)
			}
		}
		writeControllerEnvelope(t, ctx, ws, &pb.Envelope{SessionId: hello.SessionId, CorrelationId: method + "-duplicate", RemainingBudgetMs: 2000, Payload: &pb.Envelope_Command{Command: command}})
		for {
			reply := readControllerEnvelope(t, ctx, ws)
			if ping := reply.GetPing(); ping != nil {
				writeControllerEnvelope(t, ctx, ws, &pb.Envelope{SessionId: hello.SessionId, CorrelationId: reply.CorrelationId, Payload: &pb.Envelope_Pong{Pong: ping}})
				continue
			}
			if reply.CorrelationId != method+"-duplicate" || reply.GetResult().GetCertainty() != pb.OperationResult_UNKNOWN {
				t.Fatal("consumed initial command replayed", reply)
			}
			break
		}
		select {
		case <-written:
			t.Fatal("duplicate wrote again")
		default:
		}
	}
}
