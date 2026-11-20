package engine

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
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

type phoneTestWrite struct {
	write *pb.FlowWrite
	wire  []byte
}

type phoneServingFixture struct {
	e                *RedisNative
	ctx              context.Context
	client           *redis.Client
	root, token, url string
	server           *control.Server
	caps             []string
	writes           chan phoneTestWrite
	outcome          atomic.Int32
	live             atomic.Bool
	expireShared     atomic.Bool
}

func newPhoneServingFixture(t *testing.T) *phoneServingFixture {
	t.Helper()
	address := os.Getenv("SIPBRIDGE_CORE_TEST_REDIS")
	if address == "" {
		t.Skip("isolated Redis fixture required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	t.Cleanup(cancel)
	f := &phoneServingFixture{ctx: ctx, root: "sipstate:test:phone-domain-" + uuid.NewString() + ":", token: strings.Repeat("synthetic-test-token-", 3), writes: make(chan phoneTestWrite, 32)}
	f.live.Store(true)
	f.expireShared.Store(true)
	f.outcome.Store(int32(pb.DeliveryResult_WRITTEN))
	f.client = redis.NewClient(&redis.Options{Addr: address, MaxRetries: -1})
	t.Cleanup(func() {
		keys, err := f.client.Keys(context.Background(), f.root+"*").Result()
		if err != nil || len(keys) > 1024 {
			t.Error("fixture cleanup inventory", err)
			return
		}
		if len(keys) > 0 {
			if err := f.client.Del(context.Background(), keys...).Err(); err != nil {
				t.Error(err)
			}
		}
		_ = f.client.Close()
	})
	key := bytes.Repeat([]byte{9}, 32)
	connection, err := rediscore.NewConnection(rediscore.ConnectionConfig{Redis: redisclient.Options{Address: address, IOTimeout: time.Second}, Profile: rediscore.ProfileConfig{Root: f.root, Service: "test", Generation: "one", IdentityKey: key, RecordBudget: 32 << 20}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	process := &pb.ProcessRef{InstanceId: "native", Epoch: uuid.NewString()}
	file := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(file, []byte(f.token), 0600); err != nil {
		t.Fatal(err)
	}
	auth, err := control.NewBearerAuthenticator(file, "phone-test")
	if err != nil {
		t.Fatal(err)
	}
	f.caps = []string{Capability, control.StateCapability, control.NativeCompletionCapability, control.CommandAdmissionCapability, control.CommandBudgetCapability, control.DialogContinuityCapability, control.RequestDecisionCapability}
	f.server, err = control.NewServer(control.Config{Process: process, Authenticator: auth, AllowPlain: true, StateAuthority: connection.Authority(), Capabilities: f.caps,
		OnApplicationLease: func(ctx context.Context, app *pb.ProcessRef, term string) (*pb.ApplicationLease, error) {
			return f.e.ApplicationLease(ctx, app, term)
		},
		OnMessage: func(peer *control.Peer, envelope *pb.Envelope) error { return f.e.Handle(peer, envelope) },
		OnStatus:  func(*control.Peer, *pb.SessionStatus) error { return nil },
		OnDisconnect: func(peer *control.Peer) {
			if f.expireShared.Load() {
				lease := peer.ApplicationLease()
				_ = f.e.DisconnectApplication(context.Background(), lease.Application, lease.Term)
			}
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(f.server.Close)
	cfg := Config{Process: process, AdvertiseAddress: "sip.example.test:5061", PlainAdvertiseAddress: "sip.example.test:5060", ContactURI: "sips:sip.example.test:5061", StateIdentityKey: key,
		LiveFlow: func(*pb.FlowRef) bool { return f.live.Load() }}
	cfg.WriteCommitted = func(ctx context.Context, write *pb.FlowWrite) *pb.DeliveryResult {
		result := &pb.DeliveryResult{Effect: write.Effect, Outcome: pb.DeliveryResult_NOT_WRITTEN}
		reserved, err := f.e.Reserve(ctx, write)
		if err != nil {
			result.Error = err.Error()
			return result
		}
		if reserved.Known {
			result.Outcome, result.Recorded = reserved.Outcome, true
			return result
		}
		f.writes <- phoneTestWrite{write: proto.Clone(write).(*pb.FlowWrite), wire: bytes.Clone(reserved.Wire)}
		result.Outcome = pb.DeliveryResult_WRITTEN
		if bytes.HasPrefix(reserved.Wire, []byte("NOTIFY ")) {
			result.Outcome = pb.DeliveryResult_Outcome(f.outcome.Load())
		}
		if err := f.e.Complete(ctx, write, result); err != nil {
			result.Error = err.Error()
			return result
		}
		result.Recorded = true
		return result
	}
	f.e, err = NewRedisNative(ctx, cfg, connection, f.server)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(f.e.Close)
	for f.e.Ready() != nil {
		select {
		case <-time.After(5 * time.Millisecond):
		case <-ctx.Done():
			t.Fatal("worker not ready")
		}
	}
	httpServer := httptest.NewServer(f.server)
	t.Cleanup(httpServer.Close)
	f.url = "ws" + strings.TrimPrefix(httpServer.URL, "http") + "/v1/control"
	return f
}

func (f *phoneServingFixture) controller(t *testing.T, app *pb.ProcessRef, attempt uint64) (*control.Peer, *websocket.Conn, string) {
	t.Helper()
	ws, _, err := websocket.Dial(f.ctx, f.url, &websocket.DialOptions{Subprotocols: []string{control.Subprotocol}, HTTPHeader: http.Header{"Authorization": {"Bearer " + f.token}}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ws.CloseNow() })
	writeControllerEnvelope(t, f.ctx, ws, &pb.Envelope{CorrelationId: "hello", Payload: &pb.Envelope_Hello{Hello: &pb.SessionHello{Controller: app, ConnectionAttempt: attempt, Major: 1, Bounds: &pb.Bounds{MaxEnvelopeBytes: control.MaxEnvelope}, Capabilities: f.caps}}})
	ack := readControllerEnvelope(t, f.ctx, ws).GetHelloAck()
	if ack == nil {
		t.Fatal("hello failed")
	}
	writeControllerEnvelope(t, f.ctx, ws, &pb.Envelope{SessionId: ack.SessionId, CorrelationId: "ready", Payload: &pb.Envelope_Status{Status: &pb.SessionStatus{RetainedReady: true, BindingsReady: true, AdmitCalls: true, AdmitSubscriptions: true}}})
	writeControllerEnvelope(t, f.ctx, ws, &pb.Envelope{SessionId: ack.SessionId, CorrelationId: "barrier", Payload: &pb.Envelope_Ping{Ping: &pb.Heartbeat{Nonce: "barrier"}}})
	if readControllerEnvelope(t, f.ctx, ws).GetPong() == nil {
		t.Fatal("ready barrier missing")
	}
	peer, err := f.server.Peer(app)
	if err != nil {
		t.Fatal(err)
	}
	return peer, ws, ack.SessionId
}

func (f *phoneServingFixture) flow(transport string) *pb.FlowRef {
	return &pb.FlowRef{Ingress: f.e.cfg.Process, ConnectionId: "phone", Transport: transport, SourceIp: "192.0.2.12", SourcePort: 50300, Listener: "sip.example.test:5061"}
}

func (f *phoneServingFixture) subscribe(t *testing.T, peer *control.Peer, ws *websocket.Conn, session string, expires ...string) (*pb.ContextRef, []byte) {
	t.Helper()
	flow := f.flow("tls")
	expiry := "90"
	if len(expires) > 0 {
		expiry = expires[0]
	}
	message := stateTestRequest("SUBSCRIBE", 1, "", "local-sub")
	message.Wire = []byte(strings.Replace(string(message.Wire), "Content-Length:", "Event: message-summary\r\nExpires: "+expiry+"\r\nContent-Length:", 1))
	if err := f.e.ReceiveRequest(f.ctx, peer, flow, message); err != nil {
		t.Fatal(err)
	}
	request := readControllerEnvelope(t, f.ctx, ws).GetRequest()
	if request == nil {
		t.Fatal("missing phone direct request")
	}
	decision := residentDirectDecision(request, 1, 202)
	decision.Headers = []*pb.Header{{Name: "Expires", Value: expiry}}
	result := residentSendDecision(t, f.ctx, ws, session, "subscribe", decision)
	if result.Certainty != pb.OperationResult_ACCEPTED || result.Snapshot.Kind != pb.ContextSnapshot_SUBSCRIPTION {
		t.Fatal("subscription not committed", result)
	}
	var notify []byte
	for range 2 {
		select {
		case write := <-f.writes:
			if bytes.HasPrefix(write.wire, []byte("NOTIFY ")) {
				notify = write.wire
			}
		case <-f.ctx.Done():
			t.Fatal("missing atomic response/NOTIFY outputs")
		}
	}
	if len(notify) == 0 {
		t.Fatal("required NOTIFY missing")
	}
	return result.Snapshot.Context, notify
}

func TestRedisServingPhoneSubscriptionSoleLocalAuthority(t *testing.T) {
	f := newPhoneServingFixture(t)
	app := &pb.ProcessRef{InstanceId: "controller", Epoch: uuid.NewString()}
	peer, ws, session := f.controller(t, app, 1)
	ref, notify := f.subscribe(t, peer, ws, session)
	if !localPhoneID(ref.Id) || ref.State.Incarnation == ref.State.Authority.PrimaryId || f.e.phone.leases != nil {
		t.Fatal("local ownership or session authority duplicated")
	}
	if n := f.client.Exists(f.ctx, f.root+"core:context:"+ref.Id).Val(); n != 0 {
		t.Fatal("phone state mirrored into Redis")
	}
	response, err := portable.ResponseFor(portable.RestoreMessage(notify, "tls", "", ""), 200, "OK", nil, "")
	if err != nil {
		t.Fatal(err)
	}
	if handled, err := f.e.TryNativeCompletion(f.ctx, f.flow("tls"), &pb.SipMessage{Wire: response.WireBytes()}); err != nil || !handled {
		t.Fatal("local NOTIFY final misrouted", handled, err)
	}
	for _, mutate := range []func(*pb.ContextRef){
		func(v *pb.ContextRef) { v.Owner.Epoch = "foreign" },
		func(v *pb.ContextRef) { v.State.CallId = "forged-call" },
		func(v *pb.ContextRef) { v.Id = residentPhonePrefix + strings.Repeat("a", 32); v.State.GroupId = v.Id },
		func(v *pb.ContextRef) { v.State = nil },
	} {
		foreign := proto.Clone(ref).(*pb.ContextRef)
		mutate(foreign)
		if result, err := f.e.phone.inspectResident(f.ctx, app, foreign); err == nil || result != nil {
			t.Fatal("foreign/malformed local identity produced absence", result, err)
		}
	}
	f.live.Store(false)
	f.e.FlowClosed(f.flow("tls"))
	c, err := f.e.phone.findContext(ref)
	if err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	ended := c.snapshot.Ended
	c.mu.Unlock()
	if !ended {
		t.Fatal("socket close did not end pinned subscription")
	}
}

func TestRedisServingPhoneReleasedProcessorAndStaleCapability(t *testing.T) {
	for _, sameTerm := range []bool{true, false} {
		name := "new-processor"
		if sameTerm {
			name = "same-term-new-session"
		}
		t.Run(name, func(t *testing.T) { testRedisPhoneReleasedProcessor(t, sameTerm) })
	}
}

func testRedisPhoneReleasedProcessor(t *testing.T, sameTerm bool) {
	f := newPhoneServingFixture(t)
	app := &pb.ProcessRef{InstanceId: "controller", Epoch: uuid.NewString()}
	peer, ws, session := f.controller(t, app, 1)
	ref, notify := f.subscribe(t, peer, ws, session)
	oldCapability := peer.ApplicationCapability(f.server)
	oldTerm := peer.ApplicationLease().Term
	response, err := portable.ResponseFor(portable.RestoreMessage(notify, "tls", "", ""), 200, "OK", nil, "")
	if err != nil {
		t.Fatal(err)
	}
	if handled, err := f.e.TryNativeCompletion(f.ctx, f.flow("tls"), &pb.SipMessage{Wire: response.WireBytes()}); err != nil || !handled {
		t.Fatal(handled, err)
	}
	// Simulate the already-supported failed exact Redis expiry at disconnect:
	// the old local capability is released, but the shared term remains live.
	f.expireShared.Store(!sameTerm)
	_ = ws.CloseNow()
	c, err := f.e.phone.findContext(ref)
	if err != nil {
		t.Fatal(err)
	}
	for {
		c.mu.Lock()
		lost := c.ownerLost
		c.mu.Unlock()
		if lost {
			break
		}
		select {
		case <-time.After(time.Millisecond):
		case <-f.ctx.Done():
			t.Fatal("local capability not released")
		}
	}
	// The old established defer has completed before the claim is reusable.
	var next *control.Peer
	for {
		if _, err := f.server.Peer(app); err != nil {
			break
		}
		select {
		case <-time.After(time.Millisecond):
		case <-f.ctx.Done():
			t.Fatal("old session remained claimed")
		}
	}
	nextApplication := app
	if !sameTerm {
		nextApplication = &pb.ProcessRef{InstanceId: "successor", Epoch: uuid.NewString()}
	}
	next, nextWS, nextSession := f.controller(t, nextApplication, 2)
	if (sameTerm && next.ApplicationLease().Term != oldTerm) || next.ApplicationCapability(f.server) == oldCapability {
		t.Fatal("fixture did not exercise distinct same-term capability")
	}
	inspection, err := f.e.phone.inspectResident(f.ctx, nextApplication, ref)
	if err != nil || inspection.Snapshot == nil {
		t.Fatal("released subscription not inspectable", inspection, err)
	}
	cmd := &pb.Command{Context: ref, Sequence: inspection.Snapshot.CommandSequence + 1,
		Admission: &pb.CommandAdmission{DirectoryEpoch: "directory", ExpiresAtMs: uint64(time.Now().Add(2 * time.Second).UnixMilli()), SelectedFlow: f.flow("tls")},
		Action:    &pb.Command_DialogRequest{DialogRequest: &pb.DialogRequest{Method: "NOTIFY", Headers: []*pb.Header{{Name: "Content-Type", Value: "application/simple-message-summary"}}, Body: []byte("Messages-Waiting: yes\r\n")}}}
	resultCh := make(chan *pb.OperationResult, 1)
	go func() {
		result, err := f.e.phone.commandResidentExistingClient(f.ctx, next, cmd, "fresh-notify", time.Now().Add(2*time.Second))
		if err != nil {
			t.Error(err)
		}
		resultCh <- result
	}()
	var written phoneTestWrite
	select {
	case written = <-f.writes:
	case result := <-resultCh:
		t.Fatal("new processor command refused before write", result)
	case <-f.ctx.Done():
		t.Fatal("new processor did not notify")
	}
	if !bytes.HasPrefix(written.wire, []byte("NOTIFY ")) {
		t.Fatal("new command did not emit NOTIFY")
	}
	// A delayed old release inventory must not remove the new same-term owner.
	f.e.phone.expireResidentCapability(oldCapability)
	c.mu.Lock()
	live := !c.ownerLost && c.applicationCapability == next.ApplicationCapability(f.server) && c.ownerKey == residentOwnerIndexKey(nextApplication, next.ApplicationLease().Term) && sameProcess(c.snapshot.Application, nextApplication)
	c.mu.Unlock()
	if !live {
		t.Fatal("old release removed successor's same-term capability")
	}
	final, err := portable.ResponseFor(portable.RestoreMessage(written.wire, "tls", "", ""), 200, "OK", nil, "")
	if err != nil {
		t.Fatal(err)
	}
	if handled, err := f.e.TryNativeCompletion(f.ctx, f.flow("tls"), &pb.SipMessage{Wire: final.WireBytes()}); err != nil || !handled {
		t.Fatal(handled, err)
	}
	select {
	case result := <-resultCh:
		if result == nil || result.Certainty != pb.OperationResult_COMPLETED {
			t.Fatal("fresh processor result", result)
		}
	case <-f.ctx.Done():
		t.Fatal("fresh NOTIFY result missing")
	}
	_ = nextWS
	_ = nextSession
}

func TestRedisServingPhoneCannotAdoptLiveProcessor(t *testing.T) {
	f := newPhoneServingFixture(t)
	peer, ws, session := f.controller(t, &pb.ProcessRef{InstanceId: "original", Epoch: uuid.NewString()}, 1)
	ref, _ := f.subscribe(t, peer, ws, session)
	next, _, _ := f.controller(t, &pb.ProcessRef{InstanceId: "successor", Epoch: uuid.NewString()}, 1)
	cmd := &pb.Command{Context: ref, Sequence: 2, Admission: &pb.CommandAdmission{DirectoryEpoch: "directory", ExpiresAtMs: uint64(time.Now().Add(time.Second).UnixMilli()), SelectedFlow: f.flow("tls")}, Action: &pb.Command_DialogRequest{DialogRequest: &pb.DialogRequest{Method: "NOTIFY"}}}
	result, err := f.e.phone.commandResidentExistingClient(f.ctx, next, cmd, "premature", time.Now().Add(time.Second))
	if err != nil || result.Certainty != pb.OperationResult_NOT_ACCEPTED {
		t.Fatal("live processor displaced", result, err)
	}
	c, err := f.e.phone.findContext(ref)
	if err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	unchanged := c.snapshot.CommandSequence == 1 && sameProcess(c.snapshot.Application, peer.Application()) && !c.ownerLost
	c.mu.Unlock()
	if !unchanged || len(f.writes) != 0 {
		t.Fatal("premature adoption consumed sequence or wrote")
	}
}

func TestRedisServingPhoneRequiredNotifyUncertainty(t *testing.T) {
	f := newPhoneServingFixture(t)
	f.outcome.Store(int32(pb.DeliveryResult_UNKNOWN))
	peer, ws, session := f.controller(t, &pb.ProcessRef{InstanceId: "controller", Epoch: uuid.NewString()}, 1)
	ref, notify := f.subscribe(t, peer, ws, session)
	c, err := f.e.phone.findContext(ref)
	if err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	var effect string
	var original *residentOutput
	for id, output := range c.outputs {
		if output.method == "NOTIFY" {
			effect, original = id, output
		}
	}
	c.mu.Unlock()
	if original == nil {
		t.Fatal("UNKNOWN required NOTIFY was discarded")
	}
	_ = ws.CloseNow()
	for {
		c.mu.Lock()
		lost := c.ownerLost
		c.mu.Unlock()
		if lost {
			break
		}
		select {
		case <-time.After(time.Millisecond):
		case <-f.ctx.Done():
			t.Fatal("old control not released")
		}
	}
	response, err := portable.ResponseFor(portable.RestoreMessage(notify, "tls", "", ""), 200, "OK", nil, "")
	if err != nil {
		t.Fatal(err)
	}
	if handled, err := f.e.TryNativeCompletion(f.ctx, f.flow("tls"), &pb.SipMessage{Wire: response.WireBytes()}); err != nil || !handled {
		t.Fatal(handled, err)
	}
	c.mu.Lock()
	retained := c.outputs[effect] == original && original.state == residentOutputUnknown && !c.snapshot.Ended
	c.mu.Unlock()
	if !retained || len(f.writes) != 0 {
		t.Fatal("native final/control loss inferred a physical write or replayed UNKNOWN")
	}
}

func TestRedisServingLocalRegisterRetirementAndUDPDuplicate(t *testing.T) {
	for _, name := range []string{"tls", "udp", "tls-with-parked-call"} {
		t.Run(name, func(t *testing.T) {
			f := newPhoneServingFixture(t)
			peer, ws, session := f.controller(t, &pb.ProcessRef{InstanceId: "controller", Epoch: uuid.NewString()}, 1)
			transport := "tls"
			if name == "udp" {
				transport = "udp"
			}
			var originalLease context.Context
			if name == "tls-with-parked-call" {
				originalLease = parkPhoneFixtureCall(t, f, peer)
			}
			read := func() *pb.Envelope {
				for {
					envelope := readControllerEnvelope(t, f.ctx, ws)
					ping := envelope.GetPing()
					if ping == nil {
						return envelope
					}
					writeControllerEnvelope(t, f.ctx, ws, &pb.Envelope{SessionId: session, CorrelationId: envelope.CorrelationId, Payload: &pb.Envelope_Pong{Pong: ping}})
				}
			}
			flow := f.flow(transport)
			message := stateTestRequest("REGISTER", 1, "", "local-register")
			if transport == "udp" {
				message.Wire = bytes.ReplaceAll(message.Wire, []byte("SIP/2.0/TLS"), []byte("SIP/2.0/UDP"))
				message.Wire = bytes.ReplaceAll(message.Wire, []byte("transport=tls"), []byte("transport=udp"))
			}
			if handled, err := f.e.TryNativeCompletion(f.ctx, flow, message); handled || err != nil {
				t.Fatal("fresh phone request misclassified", handled, err)
			}
			if err := f.e.ReceiveRequest(f.ctx, peer, flow, message); err != nil {
				t.Fatal(err)
			}
			request := read().GetRequest()
			if request == nil {
				t.Fatal("missing REGISTER presentation")
			}
			result := residentSendDecision(t, f.ctx, ws, session, "register", residentDirectDecision(request, 1, 200))
			if result.Certainty != pb.OperationResult_ACCEPTED {
				t.Fatal(result)
			}
			var final phoneTestWrite
			select {
			case final = <-f.writes:
			case <-f.ctx.Done():
				t.Fatal("REGISTER final missing")
			}
			if !bytes.HasPrefix(final.wire, []byte("SIP/2.0 200")) {
				t.Fatal("REGISTER final wrong")
			}
			ref := request.Snapshot.Context
			if f.client.Exists(f.ctx, f.root+"core:context:"+ref.Id).Val() != 0 {
				t.Fatal("REGISTER mirrored into Redis")
			}
			if transport == "tls" {
				for {
					_, err := f.e.phone.findContext(ref)
					if err != nil {
						break
					}
					select {
					case <-time.After(time.Millisecond):
					case <-f.ctx.Done():
						t.Fatal("reliable REGISTER retained after exact W/ack")
					}
				}
				inspection, err := f.e.phone.inspectResident(f.ctx, peer.Application(), ref)
				if err != nil || inspection.RetirementProof == nil {
					t.Fatal("valid local absence missing", inspection, err)
				}
				// The retained business binding supplies only its live FlowRef;
				// there is no REGISTER ContextRef in this new call command.
				open := &pb.Command{Context: &pb.ContextRef{Id: "after-register"}, Sequence: 1,
					Admission: &pb.CommandAdmission{DirectoryEpoch: "directory", ExpiresAtMs: uint64(time.Now().Add(2 * time.Second).UnixMilli()), SelectedFlow: flow},
					Action:    &pb.Command_OpenUac{OpenUac: &pb.OpenUac{Uri: "sip:phone@example.test", Transport: "tls", Headers: []*pb.Header{{Name: "From", Value: "<sip:service@example.test>"}}}}}
				writeControllerEnvelope(t, f.ctx, ws, &pb.Envelope{SessionId: session, CorrelationId: "outbound-after-register", RemainingBudgetMs: 2000, Payload: &pb.Envelope_Command{Command: open}})
				opened := read().GetResult()
				if opened == nil || opened.Certainty != pb.OperationResult_ACCEPTED || opened.Request.GetMethod() != "INVITE" || localPhoneID(opened.GetSnapshot().GetContext().GetId()) {
					t.Fatal("retired local REGISTER blocked shared call", opened)
				}
				var invite phoneTestWrite
				select {
				case invite = <-f.writes:
				case <-f.ctx.Done():
					t.Fatal("outbound shared INVITE missing")
				}
				if !bytes.HasPrefix(invite.wire, []byte("INVITE ")) {
					t.Fatal("outbound call not sent")
				}
				if f.client.Exists(f.ctx, f.root+"core:context:after-register").Val() != 1 {
					t.Fatal("call was not solely shared")
				}
				if originalLease != nil {
					completePhoneFixtureCall(t, f, flow, invite.wire, opened.Snapshot.Context)
					_, live, _, err := f.e.driver.Current()
					if err != nil || live != originalLease || originalLease.Err() != nil {
						t.Fatal("unrelated work replaced the original worker binding", err)
					}
					if err := peer.ResumeApplicationLease(); err != nil {
						t.Fatal("same controller session cannot renew", err)
					}
				}
				return
			}
			c, err := f.e.phone.findContext(ref)
			if err != nil {
				t.Fatal(err)
			}
			c.mu.Lock()
			var originalJ portable.Deadline
			for _, tx := range c.transactions {
				originalJ = tx.native.Timers[portable.TimerJ]
			}
			c.mu.Unlock()
			if handled, err := f.e.TryNativeCompletion(f.ctx, flow, message); !handled || err != nil {
				t.Fatal("UDP duplicate missed local owner", handled, err)
			}
			select {
			case replay := <-f.writes:
				if !bytes.Equal(replay.wire, final.wire) {
					t.Fatal("UDP changed exact selected response")
				}
			case <-f.ctx.Done():
				t.Fatal("UDP response not replayed")
			}
			c.mu.Lock()
			sameJ := false
			for _, tx := range c.transactions {
				sameJ = tx.native.Timers[portable.TimerJ] == originalJ
			}
			c.mu.Unlock()
			if !sameJ {
				t.Fatal("duplicate extended original Timer J")
			}
		})
	}
}

func parkPhoneFixtureCall(t *testing.T, f *phoneServingFixture, peer *control.Peer) context.Context {
	t.Helper()
	store, lease, until, err := f.e.driver.Current()
	if err != nil {
		t.Fatal(err)
	}
	id := rediscore.Identity{Context: "bad-call", Incarnation: "fixture-incarnation"}
	now := time.Now()
	timer := rediscore.TimerAssignment{Worker: f.e.worker, Epoch: f.e.cfg.Process.Epoch, Term: f.e.cfg.Process.Epoch}
	_, err = store.Commit(f.ctx, rediscore.Publication{Identity: id, Create: true, Deadline: now.Add(time.Second),
		Changes: []rediscore.Change{{Field: "r:context", Value: []byte("invalid context frame")}},
		Timers:  &rediscore.TimerMutation{Target: timer, Changes: []rediscore.TimerChange{{Member: "call-retire", Due: now.Add(-time.Second)}}}})
	if err != nil {
		t.Fatal(err)
	}
	// An unrelated malformed projection also cannot veto an ordinary call
	// publication just because it sorts before the new owner's wake hint.
	if err = f.client.ZAdd(f.ctx, f.root+"core:owner-expiry", redis.Z{Score: float64(now.Add(-time.Minute).UnixMilli()), Member: "!bad-owner"}).Err(); err != nil {
		t.Fatal(err)
	}
	for {
		parked, err := f.client.Exists(f.ctx, f.root+"core:parked:"+id.Context).Result()
		if err != nil {
			t.Fatal(err)
		}
		if parked == 1 && time.Now().After(until) {
			break
		}
		select {
		case <-time.After(10 * time.Millisecond):
		case <-lease.Done():
			t.Fatal("one malformed call fenced the worker")
		case <-f.ctx.Done():
			t.Fatal("native fault did not park across renewal")
		}
	}
	if err := peer.ResumeApplicationLease(); err != nil {
		t.Fatal("fault blocked renewal of the live controller", err)
	}
	return lease
}

func completePhoneFixtureCall(t *testing.T, f *phoneServingFixture, flow *pb.FlowRef, invite []byte, ref *pb.ContextRef) {
	t.Helper()
	origin := portable.RestoreMessage(invite, "tls", "", "")
	final, err := portable.BuildResponseWithHeaders(origin, 200, "OK", nil, "phone-fixture-tag", "<sips:phone@192.0.2.12:50300>", nil)
	if err != nil {
		t.Fatal(err)
	}
	if handled, err := f.e.TryNativeCompletion(f.ctx, flow, &pb.SipMessage{Wire: final.WireBytes()}); err != nil || !handled {
		t.Fatal("healthy call's answer did not complete", handled, err)
	}
	waitWire := func(prefix string) {
		t.Helper()
		select {
		case write := <-f.writes:
			if !bytes.HasPrefix(write.wire, []byte(prefix)) {
				t.Fatal("unexpected SIP message while completing healthy call")
			}
		case <-f.ctx.Done():
			t.Fatal("missing SIP message while completing healthy call", prefix)
		}
	}
	waitWire("ACK ")
	peer, err := portable.NewDialog(portable.UAS, origin, final, portable.DefaultTimers())
	if err != nil {
		t.Fatal(err)
	}
	bye, err := portable.BuildDialogRequest(peer, "BYE", 2, nil, "z9hG4bK-healthy-peer-bye")
	if err != nil {
		t.Fatal(err)
	}
	if handled, err := f.e.TryNativeCompletion(f.ctx, flow, &pb.SipMessage{Wire: bye.WireBytes()}); err != nil || !handled {
		t.Fatal("healthy call's BYE did not complete", handled, err)
	}
	waitWire("SIP/2.0 200")
	for {
		members, err := f.client.ZRange(f.ctx, f.root+"core:deadlines:"+ref.Id, 0, 15).Result()
		if err != nil {
			t.Fatal(err)
		}
		if !slices.ContainsFunc(members, func(member string) bool { return strings.HasPrefix(member, "out-call-bye-output-") }) {
			return
		}
		select {
		case <-time.After(10 * time.Millisecond):
		case <-f.ctx.Done():
			t.Fatal("healthy BYE cutoff did not run")
		}
	}
}

func TestRedisServingPhoneOwnExpiryAndRegisterIndependence(t *testing.T) {
	f := newPhoneServingFixture(t)
	peer, ws, session := f.controller(t, &pb.ProcessRef{InstanceId: "controller", Epoch: uuid.NewString()}, 1)
	ref, _ := f.subscribe(t, peer, ws, session, "1")
	c, err := f.e.phone.findContext(ref)
	if err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	originalExpiry := c.subscription.ExpiresMS
	originalFlow := proto.Clone(c.flow).(*pb.FlowRef)
	c.mu.Unlock()
	newFlow := proto.Clone(originalFlow).(*pb.FlowRef)
	newFlow.ConnectionId = "replacement"
	if err := f.e.ReceiveRequest(f.ctx, peer, newFlow, stateTestRequest("REGISTER", 2, "", "new-registration")); err != nil {
		t.Fatal(err)
	}
	request := readControllerEnvelope(t, f.ctx, ws).GetRequest()
	if request == nil || request.Snapshot.Context.Id == ref.Id {
		t.Fatal("REGISTER reused SUB native context")
	}
	result := residentSendDecision(t, f.ctx, ws, session, "register-new-flow", residentDirectDecision(request, 1, 200))
	if result.Certainty != pb.OperationResult_ACCEPTED {
		t.Fatal(result)
	}
	select {
	case <-f.writes:
	case <-f.ctx.Done():
		t.Fatal("REGISTER response missing")
	}
	c.mu.Lock()
	unchanged := c.subscription.ExpiresMS == originalExpiry && proto.Equal(c.flow, originalFlow) && !c.snapshot.Ended
	c.mu.Unlock()
	if !unchanged {
		t.Fatal("REGISTER moved, renewed or ended SUB")
	}
	for {
		c.mu.Lock()
		ended := c.snapshot.Ended
		c.mu.Unlock()
		if ended {
			break
		}
		select {
		case <-time.After(5 * time.Millisecond):
		case <-f.ctx.Done():
			t.Fatal("own SUB expiry not executed")
		}
	}
}

func TestRedisServingOldUnstartedNotifyCannotBorrowSuccessorCapability(t *testing.T) {
	f := newPhoneServingFixture(t)
	app := &pb.ProcessRef{InstanceId: "controller", Epoch: uuid.NewString()}
	peer, ws, session := f.controller(t, app, 1)
	ref, initial := f.subscribe(t, peer, ws, session)
	final, err := portable.ResponseFor(portable.RestoreMessage(initial, "tls", "", ""), 200, "OK", nil, "")
	if err != nil {
		t.Fatal(err)
	}
	if handled, err := f.e.TryNativeCompletion(f.ctx, f.flow("tls"), &pb.SipMessage{Wire: final.WireBytes()}); err != nil || !handled {
		t.Fatal(handled, err)
	}
	writer := f.e.phone.cfg.WriteCommitted
	blocked, proceed := make(chan *pb.FlowWrite, 1), make(chan struct{})
	var first atomic.Bool
	f.e.phone.cfg.WriteCommitted = func(ctx context.Context, write *pb.FlowWrite) *pb.DeliveryResult {
		if write.Permit.Authorization.AuthorityClass == pb.EffectAuthorization_APPLICATION && first.CompareAndSwap(false, true) {
			blocked <- write
			<-proceed
		}
		return writer(ctx, write)
	}
	defer func() {
		select {
		case <-proceed:
		default:
			close(proceed)
		}
	}()
	command := func(sequence uint64) *pb.Command {
		return &pb.Command{Context: ref, Sequence: sequence, Admission: &pb.CommandAdmission{DirectoryEpoch: "directory", ExpiresAtMs: uint64(time.Now().Add(2 * time.Second).UnixMilli()), SelectedFlow: f.flow("tls")}, Action: &pb.Command_DialogRequest{DialogRequest: &pb.DialogRequest{Method: "NOTIFY", Headers: []*pb.Header{{Name: "Content-Type", Value: "application/simple-message-summary"}}, Body: []byte("Messages-Waiting: no\r\n")}}}
	}
	oldResult := make(chan *pb.OperationResult, 1)
	go func() {
		result, err := f.e.phone.commandResidentExistingClient(f.ctx, peer, command(2), "old-unstarted", time.Now().Add(2*time.Second))
		if err != nil {
			t.Error(err)
		}
		oldResult <- result
	}()
	var oldWrite *pb.FlowWrite
	select {
	case oldWrite = <-blocked:
	case <-f.ctx.Done():
		t.Fatal("old APP output not staged")
	}
	f.expireShared.Store(false)
	_ = ws.CloseNow()
	c, err := f.e.phone.findContext(ref)
	if err != nil {
		t.Fatal(err)
	}
	for {
		c.mu.Lock()
		lost := c.ownerLost
		c.mu.Unlock()
		if lost {
			break
		}
		select {
		case <-time.After(time.Millisecond):
		case <-f.ctx.Done():
			t.Fatal("old owner not released")
		}
	}
	for {
		if _, err := f.server.Peer(app); err != nil {
			break
		}
		select {
		case <-time.After(time.Millisecond):
		case <-f.ctx.Done():
			t.Fatal("old session not closed")
		}
	}
	next, _, _ := f.controller(t, app, 2)
	if next.ApplicationLease().Term != peer.ApplicationLease().Term {
		t.Fatal("same-term replacement not exercised")
	}
	newResult := make(chan *pb.OperationResult, 1)
	go func() {
		result, err := f.e.phone.commandResidentExistingClient(f.ctx, next, command(3), "new-command", time.Now().Add(2*time.Second))
		if err != nil {
			t.Error(err)
		}
		newResult <- result
	}()
	var nextWrite phoneTestWrite
	select {
	case nextWrite = <-f.writes:
	case <-f.ctx.Done():
		t.Fatal("successor NOTIFY missing")
	}
	if reserved, err := f.e.Reserve(f.ctx, oldWrite); err == nil || reserved != nil {
		t.Fatal("old APP output borrowed successor's live same-term capability", reserved, err)
	}
	close(proceed)
	final, err = portable.ResponseFor(portable.RestoreMessage(nextWrite.wire, "tls", "", ""), 200, "OK", nil, "")
	if err != nil {
		t.Fatal(err)
	}
	if handled, err := f.e.TryNativeCompletion(f.ctx, f.flow("tls"), &pb.SipMessage{Wire: final.WireBytes()}); err != nil || !handled {
		t.Fatal(handled, err)
	}
	select {
	case result := <-newResult:
		if result == nil || result.Certainty != pb.OperationResult_COMPLETED {
			t.Fatal(result)
		}
	case <-f.ctx.Done():
		t.Fatal("new result missing")
	}
	select {
	case result := <-oldResult:
		if result == nil || result.Certainty != pb.OperationResult_UNKNOWN {
			t.Fatal("old unstarted output acquired success", result)
		}
	case <-f.ctx.Done():
		t.Fatal("old result missing")
	}
	if len(f.writes) != 0 {
		t.Fatal("old APP output reached writer after release")
	}
}

func TestRedisServingPhoneRequiresFinalDecision(t *testing.T) {
	for _, method := range []string{"REGISTER", "SUBSCRIBE"} {
		t.Run(method, func(t *testing.T) {
			f := newPhoneServingFixture(t)
			peer, ws, session := f.controller(t, &pb.ProcessRef{InstanceId: "controller", Epoch: uuid.NewString()}, 1)
			message := stateTestRequest(method, 1, "", "requires-final")
			if err := f.e.ReceiveRequest(f.ctx, peer, f.flow("tls"), message); err != nil {
				t.Fatal(err)
			}
			request := readControllerEnvelope(t, f.ctx, ws).GetRequest()
			if request == nil {
				t.Fatal("request missing")
			}
			result := residentSendDecision(t, f.ctx, ws, session, "invalid-defer", residentDirectDecision(request, 1, 0))
			if result.Certainty != pb.OperationResult_NOT_ACCEPTED || len(f.writes) != 0 {
				t.Fatal("phone status0 accepted or wrote", result)
			}
			c, err := f.e.phone.findContext(request.Snapshot.Context)
			if err != nil {
				t.Fatal(err)
			}
			c.mu.Lock()
			unbound := c.directUnbound && c.ownerKey == "" && c.snapshot.CommandSequence == 0
			c.mu.Unlock()
			if !unbound {
				t.Fatal("invalid deferred decision published phone authority")
			}
		})
	}
}

func TestRedisServingPhoneNativeRefusal(t *testing.T) {
	for _, name := range []string{"REGISTER", "SUBSCRIBE", "SUBSCRIBE-tagged"} {
		t.Run(name, func(t *testing.T) {
			f := newPhoneServingFixture(t)
			method := strings.TrimSuffix(name, "-tagged")
			tag := ""
			status := uint32(503)
			if strings.HasSuffix(name, "-tagged") {
				tag = "retired-tag"
				status = 481
			}
			flow := f.flow("udp")
			message := stateTestRequest(method, 1, tag, "local-refusal")
			message.Wire = bytes.ReplaceAll(message.Wire, []byte("SIP/2.0/TLS"), []byte("SIP/2.0/UDP"))
			if err := f.e.Reject(f.ctx, flow, message, status); err != nil {
				t.Fatal(err)
			}
			var final phoneTestWrite
			select {
			case final = <-f.writes:
			case <-f.ctx.Done():
				t.Fatal("native refusal missing")
			}
			if !localPhoneID(final.write.Effect.Context.Id) {
				t.Fatal("phone refusal owned by Redis")
			}
			if handled, err := f.e.TryNativeCompletion(f.ctx, flow, message); !handled || err != nil {
				t.Fatal("local refusal duplicate missed", handled, err)
			}
			select {
			case replay := <-f.writes:
				if !bytes.Equal(replay.wire, final.wire) {
					t.Fatal("refusal replay wire changed")
				}
			case <-f.ctx.Done():
				t.Fatal("local UDP refusal replay missing")
			}
		})
	}
}

func TestRedisServingPhoneRefusalReferenceIsNotReused(t *testing.T) {
	f := newPhoneServingFixture(t)
	flow := f.flow("tls")
	message := stateTestRequest("REGISTER", 1, "", "refusal-reuse")
	if err := f.e.Reject(f.ctx, flow, message, 503); err != nil {
		t.Fatal(err)
	}
	var first phoneTestWrite
	select {
	case first = <-f.writes:
	case <-f.ctx.Done():
		t.Fatal("first refusal missing")
	}
	for {
		_, err := f.e.phone.findContext(first.write.Effect.Context)
		if err != nil {
			break
		}
		select {
		case <-time.After(time.Millisecond):
		case <-f.ctx.Done():
			t.Fatal("first reliable refusal not retired")
		}
	}
	if err := f.e.Reject(f.ctx, flow, message, 503); err != nil {
		t.Fatal(err)
	}
	select {
	case second := <-f.writes:
		if proto.Equal(second.write.Effect.Context, first.write.Effect.Context) {
			t.Fatal("retired refusal identity reused")
		}
	case <-f.ctx.Done():
		t.Fatal("fresh refusal missing")
	}
}

func TestRedisServingLocalCapabilityIndependentOfSharedReadiness(t *testing.T) {
	f := newPhoneServingFixture(t)
	peer, ws, session := f.controller(t, &pb.ProcessRef{InstanceId: "controller", Epoch: uuid.NewString()}, 1)
	ref, initial := f.subscribe(t, peer, ws, session)
	final, err := portable.ResponseFor(portable.RestoreMessage(initial, "tls", "", ""), 200, "OK", nil, "")
	if err != nil {
		t.Fatal(err)
	}
	if handled, err := f.e.TryNativeCompletion(f.ctx, f.flow("tls"), &pb.SipMessage{Wire: final.WireBytes()}); err != nil || !handled {
		t.Fatal(handled, err)
	}
	if err := f.e.driver.Close(); err != nil {
		t.Fatal(err)
	}
	if f.e.Ready() == nil || f.e.phone.validateController(f.ctx, peer) != nil {
		t.Fatal("fixture did not separate shared binding from actual local capability")
	}
	cmd := &pb.Command{Context: ref, Sequence: 2, Admission: &pb.CommandAdmission{DirectoryEpoch: "directory", ExpiresAtMs: uint64(time.Now().Add(2 * time.Second).UnixMilli()), SelectedFlow: f.flow("tls")}, Action: &pb.Command_DialogRequest{DialogRequest: &pb.DialogRequest{Method: "NOTIFY", Headers: []*pb.Header{{Name: "Content-Type", Value: "application/simple-message-summary"}}}}}
	resultCh := make(chan *pb.OperationResult, 1)
	go func() {
		result, err := f.e.phone.commandResidentExistingClient(f.ctx, peer, cmd, "local-driver-unready", time.Now().Add(2*time.Second))
		if err != nil {
			t.Error(err)
		}
		resultCh <- result
	}()
	var written phoneTestWrite
	select {
	case written = <-f.writes:
	case <-f.ctx.Done():
		t.Fatal("healthy local capability lost phone authority with shared driver")
	}
	final, err = portable.ResponseFor(portable.RestoreMessage(written.wire, "tls", "", ""), 200, "OK", nil, "")
	if err != nil {
		t.Fatal(err)
	}
	if handled, err := f.e.TryNativeCompletion(f.ctx, f.flow("tls"), &pb.SipMessage{Wire: final.WireBytes()}); err != nil || !handled {
		t.Fatal(handled, err)
	}
	select {
	case result := <-resultCh:
		if result == nil || result.Certainty != pb.OperationResult_COMPLETED {
			t.Fatal(result)
		}
	case <-f.ctx.Done():
		t.Fatal("local result missing")
	}
	open := &pb.Command{Context: &pb.ContextRef{Id: "shared-unready"}, Sequence: 1, Admission: &pb.CommandAdmission{DirectoryEpoch: "directory", ExpiresAtMs: uint64(time.Now().Add(time.Second).UnixMilli()), SelectedFlow: f.flow("tls")}, Action: &pb.Command_OpenUac{OpenUac: &pb.OpenUac{Uri: "sip:phone@example.test", Transport: "tls"}}}
	result, err := f.e.commandOpenUAC(f.ctx, peer, open, "shared-still-fenced", time.Now().Add(time.Second))
	if err == nil || result != nil || f.client.Exists(f.ctx, f.root+"core:context:shared-unready").Val() != 0 || len(f.writes) != 0 {
		t.Fatal("local capability bypassed shared call binding", result, err)
	}
}

func TestRedisServingSubscriptionSameSequenceNewBranchRejects(t *testing.T) {
	f := newPhoneServingFixture(t)
	peer, ws, session := f.controller(t, &pb.ProcessRef{InstanceId: "controller", Epoch: uuid.NewString()}, 1)
	ref, initial := f.subscribe(t, peer, ws, session)
	complete := func(wire []byte) {
		final, err := portable.ResponseFor(portable.RestoreMessage(wire, "tls", "", ""), 200, "OK", nil, "")
		if err != nil {
			t.Fatal(err)
		}
		if handled, err := f.e.TryNativeCompletion(f.ctx, f.flow("tls"), &pb.SipMessage{Wire: final.WireBytes()}); err != nil || !handled {
			t.Fatal(handled, err)
		}
	}
	complete(initial)
	c, err := f.e.phone.findContext(ref)
	if err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	localTag := c.snapshot.LocalTag
	c.mu.Unlock()
	renewal := stateTestRequest("SUBSCRIBE", 2, localTag, "local-refresh")
	renewal.Wire = []byte(strings.Replace(string(renewal.Wire), "Content-Length:", "Event: message-summary\r\nExpires: 60\r\nContent-Length:", 1))
	if err := f.e.ReceiveRequest(f.ctx, peer, f.flow("tls"), renewal); err != nil {
		t.Fatal(err)
	}
	presented := readControllerEnvelope(t, f.ctx, ws).GetRequest()
	if presented == nil || !proto.Equal(presented.Snapshot.Context, ref) {
		t.Fatal("refresh did not select pinned SUB")
	}
	decision := residentDirectDecision(presented, 2, 202)
	decision.Headers = []*pb.Header{{Name: "Expires", Value: "60"}}
	if result := residentSendDecision(t, f.ctx, ws, session, "refresh", decision); result.Certainty != pb.OperationResult_ACCEPTED {
		t.Fatal(result)
	}
	for range 2 {
		select {
		case write := <-f.writes:
			if bytes.HasPrefix(write.wire, []byte("NOTIFY ")) {
				complete(write.wire)
			}
		case <-f.ctx.Done():
			t.Fatal("refresh output missing")
		}
	}
	c.mu.Lock()
	before := *c.subscription
	before.Routes = append([]string(nil), before.Routes...)
	sequence := c.snapshot.CommandSequence
	flow := proto.Clone(c.flow).(*pb.FlowRef)
	c.mu.Unlock()
	changed, err := Observe(bytes.ReplaceAll(renewal.Wire, []byte("z9hG4bKlocal-refresh"), []byte("z9hG4bKother-refresh")), false)
	if err != nil {
		t.Fatal(err)
	}
	handled, receiveErr := f.e.TryNativeCompletion(f.ctx, f.flow("tls"), changed)
	if receiveErr == nil && !handled {
		receiveErr = f.e.ReceiveRequest(f.ctx, peer, f.flow("tls"), changed)
	}
	if receiveErr == nil {
		t.Fatal("same CSeq/new branch was admitted as a refresh or duplicate")
	}
	status := uint32(503)
	var rejection *ingestError
	if errors.As(receiveErr, &rejection) {
		status = rejection.status
	}
	if status != 500 && status != 503 {
		t.Fatal("unexpected stale CSeq rejection", status, receiveErr)
	}
	writer := f.e.phone.cfg.WriteCommitted
	acknowledge := make(chan struct{})
	defer func() {
		select {
		case <-acknowledge:
		default:
			close(acknowledge)
		}
	}()
	var held atomic.Bool
	f.e.phone.cfg.WriteCommitted = func(ctx context.Context, write *pb.FlowWrite) *pb.DeliveryResult {
		result := writer(ctx, write)
		if write.Permit.Authorization.AuthorityClass == pb.EffectAuthorization_RESPONSE && !proto.Equal(write.Effect.Context, ref) && held.CompareAndSwap(false, true) {
			<-acknowledge
		}
		return result
	}
	rejected := make(chan error, 1)
	go func() { rejected <- f.e.Reject(f.ctx, f.flow("tls"), changed, status) }()
	var refusalWire []byte
	select {
	case final := <-f.writes:
		refusalWire = final.wire
		if !bytes.Contains(final.wire, []byte("z9hG4bKother-refresh")) || !bytes.HasPrefix(final.wire, []byte("SIP/2.0 "+strconv.FormatUint(uint64(status), 10))) {
			t.Fatal("wrong exact new-branch refusal")
		}
		if proto.Equal(final.write.Effect.Context, ref) {
			t.Fatal("new-branch refusal mutated accepted subscription context")
		}
	case err := <-rejected:
		t.Fatal("rejected new branch produced no native final", err)
	case <-f.ctx.Done():
		t.Fatal("stale CSeq had no wire rejection")
	}
	otherFlow := proto.Clone(f.flow("tls")).(*pb.FlowRef)
	otherFlow.ConnectionId = "foreign-socket"
	if handled, err := f.e.TryNativeCompletion(f.ctx, otherFlow, changed); !handled || err == nil {
		t.Fatal("owned native refusal replay moved flow", handled, err)
	}
	if len(f.writes) != 0 {
		t.Fatal("foreign flow emitted a retained refusal")
	}
	if handled, err := f.e.TryNativeCompletion(f.ctx, f.flow("tls"), changed); !handled || err != nil {
		t.Fatal("exact retained refusal duplicate missed", handled, err)
	}
	if len(f.writes) > 0 {
		if replay := <-f.writes; !bytes.Equal(replay.wire, refusalWire) {
			t.Fatal("retained refusal duplicate changed exact response")
		}
	}
	close(acknowledge)
	if err := <-rejected; err != nil {
		t.Fatal("native refusal acknowledgement failed", err)
	}
	c.mu.Lock()
	unchanged := reflect.DeepEqual(*c.subscription, before) && c.snapshot.CommandSequence == sequence && proto.Equal(c.flow, flow)
	c.mu.Unlock()
	if !unchanged {
		t.Fatal("stale new-branch rejection changed subscription lifetime or sequence")
	}
	if err := f.e.Reject(f.ctx, f.flow("tls"), renewal, 503); err == nil {
		t.Fatal("native refusal competed with an admitted business transaction")
	}
}

func TestResidentSubscriptionSameSequenceNewBranchRejects(t *testing.T) {
	e, peer, ws, ctx, session, writes, _ := residentControlFixture(t, true)
	flow := residentDirectFlow(e)
	message := stateTestRequest("SUBSCRIBE", 1, "", "memory-sub")
	message.Wire = []byte(strings.Replace(string(message.Wire), "Content-Length:", "Event: message-summary\r\nExpires: 90\r\nContent-Length:", 1))
	initial := residentDirectRequest(t, e, peer, ws, ctx, flow, message)
	if result := residentSendDecision(t, ctx, ws, session, "memory-subscribe", residentDirectDecision(initial, 1, 202)); result.Certainty != pb.OperationResult_ACCEPTED {
		t.Fatal(result)
	}
	completeNotify := func() {
		for range 2 {
			select {
			case wire := <-writes:
				if bytes.HasPrefix(wire, []byte("NOTIFY ")) {
					response, err := portable.ResponseFor(portable.RestoreMessage(wire, "tls", "", ""), 200, "OK", nil, "")
					if err != nil {
						t.Fatal(err)
					}
					if handled, err := e.TryNativeCompletion(ctx, flow, &pb.SipMessage{Wire: response.WireBytes()}); err != nil || !handled {
						t.Fatal(handled, err)
					}
				}
			case <-ctx.Done():
				t.Fatal("SUB response/required NOTIFY missing")
			}
		}
	}
	completeNotify()
	c, err := e.findContext(initial.Snapshot.Context)
	if err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	localTag := c.snapshot.LocalTag
	c.mu.Unlock()
	renewal := stateTestRequest("SUBSCRIBE", 2, localTag, "memory-refresh")
	renewal.Wire = []byte(strings.Replace(string(renewal.Wire), "Content-Length:", "Event: message-summary\r\nExpires: 60\r\nContent-Length:", 1))
	presented := residentDirectRequest(t, e, peer, ws, ctx, flow, renewal)
	decision := residentDirectDecision(presented, 2, 202)
	decision.Headers = []*pb.Header{{Name: "Expires", Value: "60"}}
	if result := residentSendDecision(t, ctx, ws, session, "memory-refresh", decision); result.Certainty != pb.OperationResult_ACCEPTED {
		t.Fatal(result)
	}
	completeNotify()
	c.mu.Lock()
	before := *c.subscription
	before.Routes = append([]string(nil), before.Routes...)
	sequence := c.snapshot.CommandSequence
	c.mu.Unlock()
	changed, err := Observe(bytes.ReplaceAll(renewal.Wire, []byte("z9hG4bKmemory-refresh"), []byte("z9hG4bKother-refresh")), false)
	if err != nil {
		t.Fatal(err)
	}
	if handled, err := e.TryNativeCompletion(ctx, flow, changed); handled || err != nil {
		t.Fatal("new branch classified as completed", handled, err)
	}
	err = e.ReceiveRequest(ctx, peer, flow, changed)
	var rejection *ingestError
	if !errors.As(err, &rejection) || rejection.status != 500 {
		t.Fatal("stale CSeq did not select normal rejection", err)
	}
	if err := e.Reject(ctx, flow, changed, rejection.status); err != nil {
		t.Fatal("resident stale CSeq produced no wire final", err)
	}
	select {
	case final := <-writes:
		if !bytes.HasPrefix(final, []byte("SIP/2.0 500")) || !bytes.Contains(final, []byte("z9hG4bKother-refresh")) {
			t.Fatal("wrong resident new-branch final")
		}
	case <-ctx.Done():
		t.Fatal("resident stale CSeq final missing")
	}
	c.mu.Lock()
	unchanged := reflect.DeepEqual(*c.subscription, before) && c.snapshot.CommandSequence == sequence && proto.Equal(c.flow, flow)
	c.mu.Unlock()
	if !unchanged {
		t.Fatal("rejection changed subscription metadata")
	}
	if err := e.Reject(ctx, flow, renewal, 503); err == nil {
		t.Fatal("refusal competed with admitted transaction")
	}
}
