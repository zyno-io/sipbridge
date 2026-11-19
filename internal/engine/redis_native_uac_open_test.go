package engine

import (
	"bytes"
	"context"
	"errors"
	"maps"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
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
	"sipbridge/internal/nativepb"
	"sipbridge/internal/redisclient"
	"sipbridge/internal/rediscore"
	"sipbridge/internal/selectedmessage"
)

func TestRedisNativeOpenUACCommand(t *testing.T) {
	testRedisNativeOpenUACCommand(t, 0)
}

func TestRedisNativeOpenUACFailureDiagnostics(t *testing.T) {
	t.Run("worker-binding", func(t *testing.T) { testRedisNativeOpenUACCommand(t, -1) })
	t.Run("publication", func(t *testing.T) { testRedisNativeOpenUACCommand(t, -2) })
}

func TestRedisNativeCancelDuringCredentialLookup(t *testing.T) {
	testRedisNativeOpenUACCommand(t, 1)
}

func TestRedisNativeCancelDuringSecondCredentialLookup(t *testing.T) {
	testRedisNativeOpenUACCommand(t, 2)
}

func testRedisNativeOpenUACCommand(t *testing.T, cancelLookup int) {
	address := os.Getenv("SIPBRIDGE_CORE_TEST_REDIS")
	if address == "" {
		t.Skip("isolated Redis fixture required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	root := "sipstate:test:native-uac-open-" + uuid.NewString() + ":"
	key := bytes.Repeat([]byte{7}, 32)
	process := &pb.ProcessRef{InstanceId: "native", Epoch: uuid.NewString()}
	application := &pb.ProcessRef{InstanceId: "controller", Epoch: uuid.NewString()}
	connectionAddress := address
	var decisionProxy *redisReservationFaultProxy
	if cancelLookup >= 12 && cancelLookup <= 22 {
		decisionProxy = newRedisReservationFaultProxy(t, address)
		connectionAddress = decisionProxy.address
	}
	connection, err := rediscore.NewConnection(rediscore.ConnectionConfig{Redis: redisclient.Options{Address: connectionAddress, IOTimeout: time.Second}, Profile: rediscore.ProfileConfig{Root: root, Service: "test", Generation: "one", IdentityKey: key, RecordBudget: 32 << 20}})
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	client := redis.NewClient(&redis.Options{Addr: address, MaxRetries: -1})
	defer client.Close()
	keys := []string{"core:profile", "core:capacity", "core:selectors", "core:applications", "core:owner-expiry", "core:coordinator", "core:context:outbound", "core:deadlines:outbound", "core:context:network", "core:deadlines:network"}
	defer func() {
		for i, name := range keys {
			keys[i] = root + name
		}
		if e := client.Del(context.Background(), keys...).Err(); e != nil {
			t.Error(e)
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
			return engine.ApplicationLease(ctx, app, term)
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
	transport := "tls"
	if cancelLookup == 4 || cancelLookup >= 5 && cancelLookup <= 7 || cancelLookup == 11 {
		transport = "udp"
	}
	flow := &pb.FlowRef{Ingress: process, ConnectionId: "phone", SourceIp: "192.0.2.12", SourcePort: 5061, Listener: "sip.example.test:5061", Transport: transport}
	opened := make(chan string, 1)
	cfg := Config{Process: process, AdvertiseAddress: "sip.example.test:5061", ContactURI: "sips:sip.example.test:5061", StateIdentityKey: key,
		OpenNetwork: func(_ context.Context, uri string, target *pb.NetworkTarget) (*pb.FlowRef, error) {
			opened <- uri
			return flow, nil
		}}
	var previousACK *redisPreviousACKPause
	if cancelLookup >= 5 && cancelLookup <= 7 {
		previousACK = newRedisPreviousACKPause([]pb.DeliveryResult_Outcome{pb.DeliveryResult_WRITTEN, pb.DeliveryResult_NOT_WRITTEN, pb.DeliveryResult_UNKNOWN}[cancelLookup-5])
	}
	if cancelLookup == 11 {
		previousACK = newRedisPreviousACKPause(pb.DeliveryResult_WRITTEN)
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
		if previousACK != nil && previousACK.selectWrite(reserved.Wire, transport) {
			return previousACK.write(ctx, engine, write, reserved.Wire, written)
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
	if previousACK != nil {
		defer previousACK.unblock()
	}
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
	command := func(id string, selected *pb.FlowRef) *pb.Command {
		out := &pb.Command{Context: &pb.ContextRef{Id: id}, Sequence: 1, Admission: &pb.CommandAdmission{DirectoryEpoch: "directory", ExpiresAtMs: uint64(time.Now().Add(3 * time.Second).UnixMilli()), SelectedFlow: selected}, Action: &pb.Command_OpenUac{OpenUac: &pb.OpenUac{Uri: "sip:phone@example.test", Transport: transport, Network: &pb.NetworkTarget{Transport: transport, Credential: &pb.CredentialRef{Id: "carrier", Version: "one"}}, Headers: []*pb.Header{{Name: "From", Value: "<sip:service@example.test>"}}}}}
		if previousACK != nil {
			out.GetOpenUac().Body = []byte("v=0\r\ns=offer\r\n")
			out.GetOpenUac().Headers = append(out.GetOpenUac().Headers, &pb.Header{Name: "Content-Type", Value: "application/sdp"})
		}
		return out
	}
	send := func(id, operation string, selected *pb.FlowRef) *pb.OperationResult {
		t.Helper()
		writeControllerEnvelope(t, ctx, ws, &pb.Envelope{SessionId: hello.SessionId, CorrelationId: operation, RemainingBudgetMs: 3000, Payload: &pb.Envelope_Command{Command: command(id, selected)}})
		for {
			reply := readControllerEnvelope(t, ctx, ws)
			if ping := reply.GetPing(); ping != nil {
				writeControllerEnvelope(t, ctx, ws, &pb.Envelope{SessionId: hello.SessionId, CorrelationId: reply.CorrelationId, Payload: &pb.Envelope_Pong{Pong: ping}})
				continue
			}
			if reply.GetDialogEvent() != nil {
				writeControllerEnvelope(t, ctx, ws, &pb.Envelope{SessionId: hello.SessionId, CorrelationId: reply.CorrelationId, Payload: &pb.Envelope_Result{Result: &pb.OperationResult{Certainty: pb.OperationResult_ACCEPTED}}})
				continue
			}
			if reply.CorrelationId == operation {
				return reply.GetResult()
			}
			t.Fatal("unexpected controller response", reply.CorrelationId)
		}
	}
	if cancelLookup < 0 {
		stage := "initial UAC worker binding: "
		if cancelLookup == -1 {
			if err = engine.driver.Close(); err != nil {
				t.Fatal(err)
			}
		} else {
			stage = "initial UAC publication: "
			store, _, _, currentErr := engine.driver.Current()
			if currentErr != nil {
				t.Fatal(currentErr)
			}
			if err = store.ExpireApplication(ctx, rediscore.ApplicationLeaseRef{Application: appID, Term: hello.ApplicationLease.Term}); err != nil {
				t.Fatal(err)
			}
			if _, err = store.AcquireApplication(ctx, appID, uuid.NewString()); err != nil {
				t.Fatal(err)
			}
		}
		before, readErr := client.HGetAll(ctx, root+"core:capacity").Result()
		if readErr != nil {
			t.Fatal(readErr)
		}
		result := send("outbound", "selected", flow)
		if result == nil || result.Certainty != pb.OperationResult_UNKNOWN || !strings.HasPrefix(result.Error, stage) || !strings.Contains(result.Error, rediscore.ErrAuthority.Error()) {
			t.Fatal("initial failure diagnostic", result)
		}
		if cancelLookup == -2 && !strings.HasSuffix(result.Error, "commit application-term") {
			t.Fatal("publication guard not identified", result.Error)
		}
		exists, readErr := client.Exists(ctx, root+"core:context:outbound").Result()
		if readErr != nil || exists != 0 || len(written) != 0 {
			t.Fatal("refused initial command published context or output", readErr)
		}
		after, readErr := client.HGetAll(ctx, root+"core:capacity").Result()
		if readErr != nil || !maps.Equal(before, after) {
			t.Fatal("refused initial command changed capacity", readErr)
		}
		return
	}
	if cancelLookup == 4 {
		ids := make([]string, 65)
		for index := range ids {
			ids[index] = "proposal-" + strconv.Itoa(index)
			keys = append(keys, "core:context:"+ids[index], "core:deadlines:"+ids[index])
		}
		testRedisNativeUACCapacityAfterFailures(t, ctx, engine, ws, hello, flow, send, written, client, root, ids)
		return
	}
	result := send("outbound", "selected", flow)
	if result == nil || result.Certainty != pb.OperationResult_ACCEPTED || result.Snapshot == nil || result.Snapshot.Context.State == nil || result.Request == nil || result.Request.Method != "INVITE" {
		t.Fatal("initial selected-flow receipt", result)
	}
	select {
	case wire := <-written:
		if !bytes.HasPrefix(wire, []byte("INVITE ")) {
			t.Fatal("wrong SIP write")
		}
	case <-ctx.Done():
		t.Fatal("INVITE not written")
	}
	store, _, _, err := engine.driver.Current()
	if err != nil {
		t.Fatal(err)
	}
	witness := rediscore.ApplicationLeaseRef{Application: appID, Term: hello.ApplicationLease.Term}
	identity := rediscore.Identity{Context: "outbound", Incarnation: result.Snapshot.Context.State.Incarnation, Owner: appID, Term: witness.Term}
	view, err := store.ReadApplication(ctx, identity, witness, "r:context")
	credit := redisInviteCompletionCredit(flow.Transport)
	if err != nil || view.Reserved <= credit-(64<<10) || view.Reserved > credit {
		t.Fatal("production logical headroom missing", err)
	}
	retained, err := decodeNativeContext(view.Records["r:context"])
	if err != nil || len(retained.InitialProcedures) != 1 {
		t.Fatal("initial owner missing", err)
	}
	ownerView, err := store.ReadApplication(ctx, identity, witness, retained.InitialProcedures[0])
	if err != nil {
		t.Fatal(err)
	}
	owner, err := readPrivateProcedureRecord(ownerView.Records[retained.InitialProcedures[0]])
	if err != nil || owner.CreditContribution != uint64(credit) {
		t.Fatal("original contribution missing", err)
	}
	waitOutputs := func() {
		t.Helper()
	outer:
		for {
			view, e := store.Read(ctx, identity, "r:context")
			if e != nil {
				t.Fatal(e)
			}
			current, e := decodeNativeContext(view.Records["r:context"])
			if e != nil {
				t.Fatal(e)
			}
			selected, e := store.Read(ctx, identity, current.InitialProcedures...)
			if e != nil {
				t.Fatal(e)
			}
			if selected.Revision != view.Revision || selected.Timer != view.Timer {
				continue
			}
			ready := true
			for _, field := range current.InitialProcedures {
				proc, e := readPrivateProcedureRecord(selected.Records[field])
				if e != nil {
					t.Fatal(e)
				}
				if !proc.OutputConsumed {
					ready = false
				}
				for _, outputField := range proc.Outputs {
					if !strings.HasPrefix(outputField, "r:uac-ack-output-") {
						continue
					}
					outputView, e := store.Read(ctx, identity, outputField)
					if e != nil {
						t.Fatal(e)
					}
					if outputView.Revision != selected.Revision || outputView.Timer != selected.Timer {
						continue outer
					}
					output, e := decodeNativeOutput(outputView.Records[outputField])
					if e != nil {
						t.Fatal(e)
					}
					ref, e := nativeOutputRef(output.Write)
					if e != nil {
						t.Fatal(e)
					}
					_, e = store.InspectOutput(ctx, ref)
					if e == nil {
						ready = false
					} else if !errors.Is(e, rediscore.ErrAbsent) {
						t.Fatal(e)
					}
				}
			}
			if ready {
				return
			}
			select {
			case <-ctx.Done():
				t.Fatal("native outputs not consumed")
			case <-time.After(5 * time.Millisecond):
			}
		}
	}
	waitOutputs()
	originView, err := store.Read(ctx, identity, owner.Origin)
	if err != nil {
		t.Fatal(err)
	}
	origin, err := selectedmessage.DecodeOpaque(originView.Records[owner.Origin])
	if err != nil {
		t.Fatal(err)
	}
	if previousACK != nil {
		testRedisPreviousACKCompletionBeforeResume(t, ctx, engine, ws, hello, flow, result, store, identity, origin, written, previousACK, cancelLookup == 11)
		return
	}
	if cancelLookup == 3 || cancelLookup >= 8 && cancelLookup <= 10 || cancelLookup >= 12 && cancelLookup <= 22 {
		keys = append(keys, "core:context:after-ack-cutoff", "core:deadlines:after-ack-cutoff")
		testRedisInitialACKAfterApplicationBYE(t, ctx, engine, ws, hello, flow, result, store, identity, origin, written, cancelLookup >= 8, cancelLookup == 9, cancelLookup == 10, root, decisionProxy, cancelLookup)
		return
	}
	challenge := digestReply(t, origin, 401)
	challenge = portable.RestoreMessage(challenge.WireBytes(), "tls", "192.0.2.12:5061", "")
	responseDone := make(chan error, 1)
	go func() { _, e := engine.tryUACResponseCompletion(ctx, flow, challenge, nil); responseDone <- e }()
	readLookup := func() *pb.Envelope {
		t.Helper()
		for {
			value := readControllerEnvelope(t, ctx, ws)
			if ping := value.GetPing(); ping != nil {
				writeControllerEnvelope(t, ctx, ws, &pb.Envelope{SessionId: hello.SessionId, CorrelationId: value.CorrelationId, Payload: &pb.Envelope_Pong{Pong: ping}})
				continue
			}
			if value.GetDialogEvent() != nil {
				writeControllerEnvelope(t, ctx, ws, &pb.Envelope{SessionId: hello.SessionId, CorrelationId: value.CorrelationId, Payload: &pb.Envelope_Result{Result: &pb.OperationResult{Certainty: pb.OperationResult_ACCEPTED}}})
				continue
			}
			if value.GetCredentialLookup().GetCredential().GetId() != "carrier" {
				t.Fatal("missing credential lookup", value)
			}
			return value
		}
	}
	lookupReady := make(chan *pb.Envelope, 1)
	go func() { lookupReady <- readLookup() }()
	var lookup *pb.Envelope
	select {
	case lookup = <-lookupReady:
	case e := <-responseDone:
		t.Fatal("challenge ended before lookup", e)
	case <-ctx.Done():
		t.Fatal("challenge lookup timed out")
	}
	cancelChallenge := func(attempt int) {
		cancelCommand := &pb.Command{Context: result.Snapshot.Context, Sequence: 2, Action: &pb.Command_Cancel{Cancel: &pb.Cancel{}}}
		writeControllerEnvelope(t, ctx, ws, &pb.Envelope{SessionId: hello.SessionId, CorrelationId: "cancel-lookup", RemainingBudgetMs: 2000, Payload: &pb.Envelope_Command{Command: cancelCommand}})
		for {
			message := readControllerEnvelope(t, ctx, ws)
			if ping := message.GetPing(); ping != nil {
				writeControllerEnvelope(t, ctx, ws, &pb.Envelope{SessionId: hello.SessionId, CorrelationId: message.CorrelationId, Payload: &pb.Envelope_Pong{Pong: ping}})
				continue
			}
			if event := message.GetDialogEvent(); event != nil {
				if event.Kind != pb.DialogEvent_FAILED || !event.Snapshot.Ended {
					t.Fatal("wrong cancellation lifecycle", event)
				}
				writeControllerEnvelope(t, ctx, ws, &pb.Envelope{SessionId: hello.SessionId, CorrelationId: message.CorrelationId, Payload: &pb.Envelope_Result{Result: &pb.OperationResult{Certainty: pb.OperationResult_ACCEPTED}}})
				continue
			}
			if message.CorrelationId != "cancel-lookup" || message.GetResult().GetCertainty() != pb.OperationResult_ACCEPTED {
				t.Fatal("CANCEL not accepted", message)
			}
			break
		}
		writeControllerEnvelope(t, ctx, ws, &pb.Envelope{SessionId: hello.SessionId, CorrelationId: lookup.CorrelationId, Payload: &pb.Envelope_CredentialResult{CredentialResult: &pb.CredentialResult{Username: "carrier-user", Password: "fixture-secret", Version: "one"}}})
		<-responseDone
		select {
		case wire := <-written:
			if !bytes.HasPrefix(wire, []byte("ACK ")) {
				t.Fatal("unexpected authenticated retry")
			}
		case <-ctx.Done():
			t.Fatal("challenge ACK missing")
		}
		// The socket writer can publish ACK consumption after the ACK bytes and
		// credential result have been observed. Select one stable native view.
		waitOutputs()
		var current *nativepb.Context
		var parents *rediscore.View
		for {
			after, e := store.Read(ctx, identity, "r:context")
			if e != nil {
				t.Fatal(e)
			}
			current, e = decodeNativeContext(after.Records["r:context"])
			if e != nil || !current.Ended || current.CallRetireAtMs == 0 || len(current.InitialProcedures) != attempt || len(current.NoninitialProcedures) != 0 {
				t.Fatal("cancelled context not retained", e)
			}
			parents, e = store.Read(ctx, identity, current.InitialProcedures...)
			if e != nil {
				t.Fatal("select challenged parent", e)
			}
			if parents.Revision == after.Revision && parents.Timer == after.Timer {
				break
			}
			select {
			case <-ctx.Done():
				t.Fatal("challenged parent view did not stabilize")
			case <-time.After(5 * time.Millisecond):
			}
		}
		matched := 0
		for _, field := range current.InitialProcedures {
			parent, x := readPrivateProcedureRecord(parents.Records[field])
			if x != nil {
				t.Fatal(x)
			}
			if parent.DigestHandled && parent.DigestAttempts == uint32(attempt) && parent.Native.Phase == string(portable.Terminated) {
				matched++
			}
		}
		if matched != 1 {
			t.Fatal("latest challenged parent not retained")
		}
		// Observing the ACK bytes is earlier than the writer's Redis proof.
		// Finish its actual native consumption before teardown cancels the engine.
		waitOutputs()
		select {
		case wire := <-written:
			t.Fatalf("unexpected output after CANCEL: %q", wire)
		default:
		}
	}
	if cancelLookup == 1 {
		cancelChallenge(1)
		return
	}
	if handled, e := engine.tryUACResponseCompletion(ctx, flow, challenge, nil); e != nil || !handled {
		t.Fatal("duplicate challenge", e)
	}
	writeControllerEnvelope(t, ctx, ws, &pb.Envelope{SessionId: hello.SessionId, CorrelationId: lookup.CorrelationId, Payload: &pb.Envelope_CredentialResult{CredentialResult: &pb.CredentialResult{Username: "carrier-user", Password: "fixture-secret", Version: "one"}}})
	if err = <-responseDone; err != nil {
		t.Fatal("digest challenge", err)
	}
	var retry portable.Message
	for i := 0; i < 2; i++ {
		select {
		case wire := <-written:
			if bytes.HasPrefix(wire, []byte("INVITE ")) {
				retry = portable.RestoreMessage(wire, "tls", "", "")
			}
		case <-ctx.Done():
			t.Fatal("digest ACK or retry missing")
		}
	}
	if retry.IsZero() || bytes.Contains(retry.WireBytes(), []byte("fixture-secret")) || !bytes.Contains(retry.WireBytes(), []byte("Authorization:")) {
		t.Fatal("invalid digest retry")
	}
	waitOutputs()
	proxyChallenge := digestReply(t, retry, 407)
	proxyChallenge = portable.RestoreMessage(proxyChallenge.WireBytes(), "tls", "192.0.2.12:5061", "")
	go func() { _, e := engine.tryUACResponseCompletion(ctx, flow, proxyChallenge, nil); responseDone <- e }()
	go func() { lookupReady <- readLookup() }()
	select {
	case lookup = <-lookupReady:
	case e := <-responseDone:
		t.Fatal("proxy challenge ended before lookup", e)
	case <-ctx.Done():
		t.Fatal("proxy challenge lookup timed out")
	}
	if cancelLookup == 2 {
		cancelChallenge(2)
		return
	}
	writeControllerEnvelope(t, ctx, ws, &pb.Envelope{SessionId: hello.SessionId, CorrelationId: lookup.CorrelationId, Payload: &pb.Envelope_CredentialResult{CredentialResult: &pb.CredentialResult{Username: "carrier-user", Password: "fixture-secret", Version: "one"}}})
	if err = <-responseDone; err != nil {
		t.Fatal("proxy digest challenge", err)
	}
	retry = portable.Message{}
	for i := 0; i < 2; i++ {
		select {
		case wire := <-written:
			if bytes.HasPrefix(wire, []byte("INVITE ")) {
				retry = portable.RestoreMessage(wire, "tls", "", "")
			}
		case <-ctx.Done():
			t.Fatal("proxy digest ACK or retry missing")
		}
	}
	if retry.IsZero() || !bytes.Contains(retry.WireBytes(), []byte("Proxy-Authorization:")) {
		t.Fatal("proxy digest retry missing")
	}
	refreshed, err := store.Read(ctx, identity, "r:context", "r:result-1")
	if err != nil {
		t.Fatal(err)
	}
	withRetry, err := decodeNativeContext(refreshed.Records["r:context"])
	if err != nil || len(withRetry.InitialProcedures) != 3 {
		t.Fatal("digest child missing", err)
	}
	retainedResult, err := decodeNativeResult(refreshed.Records["r:result-1"])
	if err != nil || retainedResult.Sequence != 1 || retainedResult.Certainty != pb.OperationResult_ACCEPTED {
		t.Fatal("original receipt changed", err)
	}
	waitOutputs()
	final, err := portable.BuildResponseWithHeaders(retry, 200, "OK", nil, "phone-tag", "<sips:phone@192.0.2.12:5061>", nil)
	if err != nil {
		t.Fatal(err)
	}
	final = portable.RestoreMessage(final.WireBytes(), "tls", "192.0.2.12:5061", "")
	if handled, e := engine.tryUACResponseCompletion(ctx, flow, final, nil); e != nil || !handled {
		t.Fatal("authenticated final", e)
	}
	var initialACK []byte
	select {
	case initialACK = <-written:
		if !bytes.HasPrefix(initialACK, []byte("ACK ")) {
			t.Fatal("authenticated ACK missing")
		}
	case <-ctx.Done():
		t.Fatal("authenticated ACK not written")
	}
	for {
		state, readErr := store.Read(ctx, identity, "r:context", "r:result-1")
		if readErr != nil {
			t.Fatal(readErr)
		}
		current, contextErr := decodeNativeContext(state.Records["r:context"])
		initial, resultErr := decodeNativeResult(state.Records["r:result-1"])
		if contextErr != nil || resultErr != nil {
			t.Fatal("read ACK settlement", contextErr, resultErr)
		}
		if current.InitialAck && initial.Certainty == pb.OperationResult_COMPLETED {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("authenticated ACK was not settled")
		case <-time.After(5 * time.Millisecond):
		}
	}
	for index, method := range []string{"INFO", "NOTIFY"} {
		operation := "dialog-" + method
		request := &pb.Command{Context: result.Snapshot.Context, Sequence: uint64(index + 2), Admission: &pb.CommandAdmission{DirectoryEpoch: "directory", ExpiresAtMs: uint64(time.Now().Add(2 * time.Second).UnixMilli()), SelectedFlow: flow}, Action: &pb.Command_DialogRequest{DialogRequest: &pb.DialogRequest{Method: method}}}
		writeControllerEnvelope(t, ctx, ws, &pb.Envelope{SessionId: hello.SessionId, CorrelationId: operation, RemainingBudgetMs: 2000, Payload: &pb.Envelope_Command{Command: request}})
		var wire []byte
		select {
		case wire = <-written:
			if !bytes.HasPrefix(wire, []byte(method+" ")) {
				t.Fatalf("%s request was not written", method)
			}
		case <-ctx.Done():
			t.Fatalf("%s output missing", method)
		}
		message := portable.RestoreMessage(wire, "tls", "", "")
		response, responseErr := portable.ResponseFor(message, 200, "OK", nil, "")
		if responseErr != nil {
			t.Fatal(responseErr)
		}
		response = portable.RestoreMessage(response.WireBytes(), "tls", "192.0.2.12:5061", "")
		if handled, responseErr := engine.tryUACResponseCompletion(ctx, flow, response, nil); responseErr != nil || !handled {
			t.Fatalf("%s response: %v", method, responseErr)
		}
		for {
			reply := readControllerEnvelope(t, ctx, ws)
			if reply.GetDialogEvent() != nil {
				writeControllerEnvelope(t, ctx, ws, &pb.Envelope{SessionId: hello.SessionId, CorrelationId: reply.CorrelationId, Payload: &pb.Envelope_Result{Result: &pb.OperationResult{Certainty: pb.OperationResult_ACCEPTED}}})
				continue
			}
			if ping := reply.GetPing(); ping != nil {
				writeControllerEnvelope(t, ctx, ws, &pb.Envelope{SessionId: hello.SessionId, CorrelationId: reply.CorrelationId, Payload: &pb.Envelope_Pong{Pong: ping}})
				continue
			}
			answer := reply.GetResult()
			if reply.CorrelationId != operation || answer == nil || answer.Certainty != pb.OperationResult_COMPLETED || answer.SipStatus != 200 || answer.Request == nil || !bytes.Equal(answer.Request.Wire, wire) || answer.Snapshot == nil || answer.Snapshot.Ended {
				t.Fatalf("%s command result: %+v", method, reply)
			}
			break
		}
	}
	// InitialAck is published before the exact proof is acknowledged. Wait for
	// that already-started consumer so this command exercises replacement of a
	// positively written, released attempt under the still-live original M.
	for {
		state, readErr := store.Read(ctx, identity, "r:context")
		if readErr != nil {
			t.Fatal(readErr)
		}
		current, readErr := decodeNativeContext(state.Records["r:context"])
		if readErr != nil || len(current.Dialogs) == 0 {
			t.Fatal("selected dialog missing", readErr)
		}
		dialogView, readErr := store.Read(ctx, identity, current.Dialogs[0])
		if readErr != nil {
			t.Fatal(readErr)
		}
		dialog, readErr := readPrivateDialogRecord(dialogView.Records[current.Dialogs[0]])
		if readErr != nil || !dialog.AckWritten {
			t.Fatal("initial ACK not written", readErr)
		}
		pending := false
		for _, field := range dialog.Outputs {
			if !strings.HasPrefix(field, "r:uac-dialog-ack-output-") {
				continue
			}
			view, e := store.Read(ctx, identity, field)
			if e != nil {
				t.Fatal(e)
			}
			if len(view.Records[field]) == 0 {
				continue
			}
			metadata, e := decodeNativeOutput(view.Records[field])
			if e != nil {
				t.Fatal(e)
			}
			ref, e := nativeOutputRef(metadata.Write)
			if e != nil {
				t.Fatal(e)
			}
			if _, e = store.InspectOutput(ctx, ref); e == nil {
				pending = true
			} else if !errors.Is(e, rediscore.ErrAbsent) {
				t.Fatal(e)
			}
		}
		if !pending {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("initial ACK proof was not released")
		case <-time.After(5 * time.Millisecond):
		}
	}
	reinvite := &pb.Command{Context: result.Snapshot.Context, Sequence: 4, Admission: &pb.CommandAdmission{DirectoryEpoch: "directory", ExpiresAtMs: uint64(time.Now().Add(2 * time.Second).UnixMilli()), SelectedFlow: flow}, Action: &pb.Command_DialogRequest{DialogRequest: &pb.DialogRequest{Method: "INVITE", Body: []byte("v=0\r\ns=offer\r\n"), Headers: []*pb.Header{{Name: "Content-Type", Value: "application/sdp"}}}}}
	writeControllerEnvelope(t, ctx, ws, &pb.Envelope{SessionId: hello.SessionId, CorrelationId: "dialog-invite", RemainingBudgetMs: 2000, Payload: &pb.Envelope_Command{Command: reinvite}})
	var inviteWire []byte
	select {
	case inviteWire = <-written:
		if !bytes.HasPrefix(inviteWire, []byte("INVITE ")) {
			t.Fatal("re-INVITE was not written")
		}
	case <-ctx.Done():
		t.Fatal("re-INVITE output missing")
	}
	glare := proto.Clone(reinvite).(*pb.Command)
	glare.Sequence++
	writeControllerEnvelope(t, ctx, ws, &pb.Envelope{SessionId: hello.SessionId, CorrelationId: "glare", RemainingBudgetMs: 2000, Payload: &pb.Envelope_Command{Command: glare}})
	for {
		reply := readControllerEnvelope(t, ctx, ws)
		if ping := reply.GetPing(); ping != nil {
			writeControllerEnvelope(t, ctx, ws, &pb.Envelope{SessionId: hello.SessionId, CorrelationId: reply.CorrelationId, Payload: &pb.Envelope_Pong{Pong: ping}})
			continue
		}
		refused := reply.GetResult()
		if reply.CorrelationId != "glare" || refused.GetCertainty() != pb.OperationResult_NOT_ACCEPTED || refused.GetSipStatus() != 491 || refused.GetError() != "SIP 491: INVITE procedure pending" {
			t.Fatal("overlapping re-INVITE did not return its SIP refusal", reply)
		}
		break
	}
	select {
	case wire := <-written:
		t.Fatalf("refused re-INVITE sent %q", wire)
	default:
	}
	// The previous accepted transaction remains independently responsible for
	// retransmitting its exact ACK while its original M is live.
	if handled, e := engine.tryUACResponseCompletion(ctx, flow, final, nil); e != nil || !handled {
		t.Fatal("old successful final replay", e)
	}
	select {
	case replay := <-written:
		if !bytes.Equal(replay, initialACK) {
			t.Fatal("old successful final did not replay its exact ACK")
		}
	case <-ctx.Done():
		t.Fatal("old successful ACK replay missing")
	}
	invite := portable.RestoreMessage(inviteWire, "tls", "", "")
	inviteFinal, err := portable.BuildResponseWithHeaders(invite, 200, "OK", []byte("v=0\r\ns=answer\r\n"), "", "<sips:phone@192.0.2.12:5061>", []portable.Header{{Name: "Content-Type", Value: "application/sdp"}})
	if err != nil {
		t.Fatal(err)
	}
	inviteFinal = portable.RestoreMessage(inviteFinal.WireBytes(), "tls", "192.0.2.12:5061", "")
	if handled, e := engine.tryUACResponseCompletion(ctx, flow, inviteFinal, nil); e != nil || !handled {
		t.Fatal("re-INVITE final", e)
	}
	var inviteACK []byte
	select {
	case inviteACK = <-written:
		if !bytes.HasPrefix(inviteACK, []byte("ACK ")) {
			t.Fatal("re-INVITE ACK missing")
		}
	case <-ctx.Done():
		t.Fatal("re-INVITE ACK not written")
	}
	for {
		reply := readControllerEnvelope(t, ctx, ws)
		if ping := reply.GetPing(); ping != nil {
			writeControllerEnvelope(t, ctx, ws, &pb.Envelope{SessionId: hello.SessionId, CorrelationId: reply.CorrelationId, Payload: &pb.Envelope_Pong{Pong: ping}})
			continue
		}
		if reply.GetDialogEvent() != nil {
			writeControllerEnvelope(t, ctx, ws, &pb.Envelope{SessionId: hello.SessionId, CorrelationId: reply.CorrelationId, Payload: &pb.Envelope_Result{Result: &pb.OperationResult{Certainty: pb.OperationResult_ACCEPTED}}})
			continue
		}
		answer := reply.GetResult()
		if reply.CorrelationId != "dialog-invite" || answer == nil || answer.Certainty != pb.OperationResult_COMPLETED || answer.SipStatus != 200 || answer.Request == nil || !bytes.Equal(answer.Request.Wire, inviteWire) {
			t.Fatal("re-INVITE command result", reply)
		}
		break
	}
	// The writer callback can precede settlement. Wait for the exact positive
	// ACK witness and the ordinary physical cutoff before testing its replay.
	inviteIDs, err := nativeSelectorIDs(inviteFinal, false, false)
	if err != nil || len(inviteIDs) == 0 {
		t.Fatal("re-INVITE selector", err)
	}
	for {
		selected, e := store.LookupSelector(ctx, inviteIDs[0], "r:context")
		if e != nil {
			t.Fatal(e)
		}
		p, e := readPrivateProcedureRecord(selected.View.Records[selected.Record])
		if e != nil {
			t.Fatal(e)
		}
		current, e := decodeNativeContext(selected.View.Records["r:context"])
		if e != nil || len(current.Dialogs) == 0 {
			t.Fatal("re-INVITE dialog", e)
		}
		view, e := store.Read(ctx, selected.Identity, current.Dialogs[0])
		if e != nil {
			t.Fatal(e)
		}
		d, e := readPrivateDialogRecord(view.Records[current.Dialogs[0]])
		if e != nil {
			t.Fatal(e)
		}
		pending := false
		for _, field := range p.Outputs {
			if strings.HasPrefix(field, "r:app-invite-ack-output-") {
				pending = true
			}
		}
		if d.AckWritten && !pending {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("re-INVITE ACK proof did not close")
		case <-time.After(5 * time.Millisecond):
		}
	}
	bye := &pb.Command{Context: result.Snapshot.Context, Sequence: glare.Sequence + 1, Admission: &pb.CommandAdmission{DirectoryEpoch: "directory", ExpiresAtMs: uint64(time.Now().Add(2 * time.Second).UnixMilli()), SelectedFlow: flow}, Action: &pb.Command_DialogRequest{DialogRequest: &pb.DialogRequest{Method: "BYE"}}}
	writeControllerEnvelope(t, ctx, ws, &pb.Envelope{SessionId: hello.SessionId, CorrelationId: "application-bye", RemainingBudgetMs: 2000, Payload: &pb.Envelope_Command{Command: bye}})
	var byeWire []byte
	select {
	case byeWire = <-written:
		if !bytes.HasPrefix(byeWire, []byte("BYE ")) {
			t.Fatal("application BYE was not written")
		}
	case <-ctx.Done():
		t.Fatal("application BYE output missing")
	}
	for {
		reply := readControllerEnvelope(t, ctx, ws)
		if ping := reply.GetPing(); ping != nil {
			writeControllerEnvelope(t, ctx, ws, &pb.Envelope{SessionId: hello.SessionId, CorrelationId: reply.CorrelationId, Payload: &pb.Envelope_Pong{Pong: ping}})
			continue
		}
		if reply.GetDialogEvent() != nil {
			writeControllerEnvelope(t, ctx, ws, &pb.Envelope{SessionId: hello.SessionId, CorrelationId: reply.CorrelationId, Payload: &pb.Envelope_Result{Result: &pb.OperationResult{Certainty: pb.OperationResult_ACCEPTED}}})
			continue
		}
		answer := reply.GetResult()
		if reply.CorrelationId != "application-bye" || answer == nil || answer.Certainty != pb.OperationResult_ACCEPTED || answer.Request == nil || !bytes.Equal(answer.Request.Wire, byeWire) {
			t.Fatal("application BYE waiter did not capture written request", reply)
		}
		break
	}
	if handled, e := engine.tryUACResponseCompletion(ctx, flow, inviteFinal, nil); e != nil || !handled {
		t.Fatal("re-INVITE final replay during BYE", e)
	}
	select {
	case replay := <-written:
		if !bytes.Equal(replay, inviteACK) {
			t.Fatal("re-INVITE final replay changed its ACK")
		}
	case <-ctx.Done():
		t.Fatal("re-INVITE ACK replay missing during BYE")
	}
	byeRequest := portable.RestoreMessage(byeWire, "tls", "", "")
	byeFinal, err := portable.ResponseFor(byeRequest, 200, "OK", nil, "")
	if err != nil {
		t.Fatal(err)
	}
	byeFinal = portable.RestoreMessage(byeFinal.WireBytes(), "tls", "192.0.2.12:5061", "")
	if handled, e := engine.tryUACResponseCompletion(ctx, flow, byeFinal, nil); e != nil || !handled {
		t.Fatal("application BYE final", e)
	}
	duplicate := send("outbound", "duplicate", flow)
	if duplicate == nil || duplicate.Certainty != pb.OperationResult_UNKNOWN {
		t.Fatal("duplicate context was re-admitted", duplicate)
	}
	select {
	case <-written:
		t.Fatal("duplicate emitted another INVITE")
	case <-time.After(100 * time.Millisecond):
	}
	result = send("network", "network-open", nil)
	if result == nil || result.Certainty != pb.OperationResult_ACCEPTED {
		t.Fatal("network route receipt", result)
	}
	select {
	case uri := <-opened:
		if uri != "sip:phone@example.test" {
			t.Fatal("wrong network target", uri)
		}
	case <-ctx.Done():
		t.Fatal("network route not resolved")
	}
	var networkWire []byte
	select {
	case networkWire = <-written:
		if !bytes.HasPrefix(networkWire, []byte("INVITE ")) {
			t.Fatal("wrong network SIP write")
		}
	case <-ctx.Done():
		t.Fatal("network INVITE not written")
	}
	networkOrigin := portable.RestoreMessage(networkWire, "tls", "", "")
	offer, err := portable.BuildResponseWithHeaders(networkOrigin, 200, "OK", []byte("v=0\r\ns=offer\r\n"), "late-phone", "<sips:phone@192.0.2.12:5061>", []portable.Header{{Name: "Content-Type", Value: "application/sdp"}})
	if err != nil {
		t.Fatal(err)
	}
	offer = portable.RestoreMessage(offer.WireBytes(), "tls", "192.0.2.12:5061", "")
	if handled, e := engine.tryUACResponseCompletion(ctx, flow, offer, nil); e != nil || !handled {
		t.Fatal("late offer", e)
	}
	answer := []byte("v=0\r\ns=answer\r\n")
	ack := &pb.Command{Context: result.Snapshot.Context, Sequence: 2, Admission: &pb.CommandAdmission{DirectoryEpoch: "directory", ExpiresAtMs: uint64(time.Now().Add(2 * time.Second).UnixMilli()), SelectedFlow: flow}, Action: &pb.Command_DialogRequest{DialogRequest: &pb.DialogRequest{Method: "ACK", Body: answer, Headers: []*pb.Header{{Name: "Content-Type", Value: "application/sdp"}}}}}
	writeControllerEnvelope(t, ctx, ws, &pb.Envelope{SessionId: hello.SessionId, CorrelationId: "answer-ack", RemainingBudgetMs: 2000, Payload: &pb.Envelope_Command{Command: ack}})
	var ackWire []byte
	select {
	case ackWire = <-written:
		if !bytes.HasPrefix(ackWire, []byte("ACK ")) || !bytes.Contains(ackWire, answer) {
			t.Fatal("late offer ACK wire")
		}
	case <-ctx.Done():
		t.Fatal("late offer ACK not written")
	}
	for {
		reply := readControllerEnvelope(t, ctx, ws)
		if ping := reply.GetPing(); ping != nil {
			writeControllerEnvelope(t, ctx, ws, &pb.Envelope{SessionId: hello.SessionId, CorrelationId: reply.CorrelationId, Payload: &pb.Envelope_Pong{Pong: ping}})
			continue
		}
		if reply.GetDialogEvent() != nil {
			writeControllerEnvelope(t, ctx, ws, &pb.Envelope{SessionId: hello.SessionId, CorrelationId: reply.CorrelationId, Payload: &pb.Envelope_Result{Result: &pb.OperationResult{Certainty: pb.OperationResult_ACCEPTED}}})
			continue
		}
		answerResult := reply.GetResult()
		if reply.CorrelationId != "answer-ack" || answerResult == nil || answerResult.Certainty != pb.OperationResult_COMPLETED || answerResult.Request == nil || !bytes.Equal(answerResult.Request.Wire, ackWire) {
			t.Fatal("late offer ACK result", reply)
		}
		break
	}
	networkIdentity := rediscore.Identity{Context: "network", Incarnation: result.Snapshot.Context.State.Incarnation, Owner: appID, Term: hello.ApplicationLease.Term}
	networkState, err := store.Read(ctx, networkIdentity, "r:context", "r:result-1")
	if err != nil {
		t.Fatal(err)
	}
	networkContext, err := decodeNativeContext(networkState.Records["r:context"])
	if err != nil || !networkContext.InitialAck {
		t.Fatal("late offer ACK witness", err)
	}
	original, err := decodeNativeResult(networkState.Records["r:result-1"])
	if err != nil || original.Certainty != pb.OperationResult_COMPLETED || original.Response == "" {
		t.Fatal("late offer original result", err)
	}
}
