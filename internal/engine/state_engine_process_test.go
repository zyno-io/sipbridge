package engine

import (
	"bufio"
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"sipbridge/internal/control"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/state"
)

type processPhaseObservation struct {
	Worker string
	Event  stateProcessEvent
}

type processObservation struct {
	Worker   string
	Envelope *pb.Envelope
}
type processWorker struct {
	process *pb.ProcessRef
	command *exec.Cmd
	input   io.WriteCloser
	sendMu  sync.Mutex
	events  chan stateProcessEvent
	done    chan error
	ready   stateProcessEvent
	peer    *processController
}

type processController struct {
	worker     *processWorker
	harness    *processHarness
	connection *websocket.Conn
	session    string
	mu         sync.Mutex
	pending    map[string]chan *pb.Envelope
	lease      *pb.ApplicationLease
	done       chan struct{}
}

type processHarness struct {
	t              *testing.T
	ctx            context.Context
	store          *state.Store
	cfg            stateProcessConfig
	clientTLS      *tls.Config
	application    *pb.ProcessRef
	directoryEpoch string
	recoverable    bool
	mu             sync.Mutex
	workers        map[string]*processWorker
	observations   []processObservation
	refs           map[string]*pb.ContextRef
	permits        map[string]*pb.SendPermit
	flow           *pb.FlowRef
	phones         []*processPhone
	phases         []processPhaseObservation
	notify         chan struct{}
	// Optional test-only delay after an immutable grant is retained by the
	// controller fixture, before its reply crosses the real control socket.
	beforeEgressReply func(*pb.EgressRequest)
	// A narrow authority policy lets process cases distinguish inbound dialog
	// cleanup from a fresh outbound transaction's completion grant.
	completionScope func(*pb.EgressRequest) (string, error)
	// Optional fixture-only placement and read-only credential replies. The
	// default control routing and cases without credential work are unchanged.
	ingestWorker     func(*pb.OwnerIngest) *processWorker
	credentialLookup func(*processWorker, string, *pb.CredentialLookup) *pb.CredentialResult
	// A focused controller policy exercises route refusal after real ingest.
	routeRequest func(*processController, *pb.Envelope) bool
}

func newRelayProcessHarness(t *testing.T) *processHarness {
	return newProcessHarnessForProfile(t, false, 35*time.Second)
}

func newNativeProcessHarness(t *testing.T) *processHarness {
	return newProcessHarnessForProfile(t, true, 35*time.Second)
}

func newProcessHarnessForProfile(t *testing.T, autonomous bool, budget time.Duration) *processHarness {
	t.Helper()
	var engine *StateEngine
	if autonomous {
		cfg := state.Config{Redis: state.RedisOptions{Address: os.Getenv("SIPBRIDGE_STATE_TEST_REDIS")}, Prefix: "sipstate:process-test", Namespace: "test-" + uuid.NewString(),
			NamespaceKey: bytes.Repeat([]byte{17}, 32), Partitions: 1, Generation: "generation", Versions: NativeStateVersions()}
		if cfg.Redis.Address == "" {
			t.Skip("set SIPBRIDGE_STATE_TEST_REDIS for native process qualification")
		}
		store, err := state.Open(context.Background(), cfg)
		if err != nil {
			t.Fatal(err)
		}
		if err = store.Ready(context.Background()); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			defer store.Close()
			client := redis.NewClient(&redis.Options{Addr: cfg.Redis.Address})
			defer client.Close()
			var cursor uint64
			for {
				keys, next, err := client.Scan(context.Background(), cursor, cfg.Prefix+":"+cfg.Namespace+":*", 128).Result()
				if err != nil {
					t.Error(err)
					return
				}
				if len(keys) > 0 {
					if err = client.Del(context.Background(), keys...).Err(); err != nil {
						t.Error(err)
					}
				}
				if next == 0 {
					return
				}
				cursor = next
			}
		})
		engine = &StateEngine{store: store}
	} else {
		engine, _ = historicalStateTestEngine(t)
	}
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	t.Cleanup(cancel)
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	identity, _ := url.Parse("spiffe://test/state-process")
	certificate := &x509.Certificate{SerialNumber: big.NewInt(1), NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, URIs: []*url.URL{identity}}
	der, err := x509.CreateCertificate(rand.Reader, certificate, certificate, public, private)
	if err != nil {
		t.Fatal(err)
	}
	key, err := x509.MarshalPKCS8PrivateKey(private)
	if err != nil {
		t.Fatal(err)
	}
	certPEM, keyPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: key})
	directory := t.TempDir()
	certPath, keyPath := filepath.Join(directory, "certificate.pem"), filepath.Join(directory, "private.pem")
	if err := os.WriteFile(certPath, certPEM, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyPath, keyPEM, 0600); err != nil {
		t.Fatal(err)
	}
	pair, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM(certPEM)
	h := &processHarness{t: t, ctx: ctx, store: engine.store.(*state.Store), application: &pb.ProcessRef{InstanceId: "controller-fixture", Epoch: "original-owner"}, directoryEpoch: "fixture-directory", workers: make(map[string]*processWorker), refs: make(map[string]*pb.ContextRef), permits: make(map[string]*pb.SendPermit), notify: make(chan struct{}, 1), clientTLS: &tls.Config{MinVersion: tls.VersionTLS13, RootCAs: roots, Certificates: []tls.Certificate{pair}}}
	h.cfg = stateProcessConfig{Certificate: certPath, Key: keyPath, Execution: os.Getenv("SIPBRIDGE_TEST_EXECUTION_MODE"), Store: state.Config{Redis: state.RedisOptions{Address: os.Getenv("SIPBRIDGE_STATE_TEST_REDIS")}, Prefix: "sipstate:engine-test", Namespace: engine.store.Namespace(), NamespaceKey: bytes.Repeat([]byte{17}, 32), Partitions: 1, Generation: "generation", Versions: state.Versions{Schema: "1", Reducer: "7", Timers: "1"}, Limits: engine.store.Limits(), ClaimTTL: 500 * time.Millisecond}}
	if h.cfg.Execution == "" {
		// Process fixtures exercise the current one-shot presentation handoff;
		// the claimed-input driver intentionally has no committed projection.
		h.cfg.Execution = "cas"
	}
	if autonomous {
		h.cfg.PlainSIP = true
		h.cfg.Store.Prefix = "sipstate:process-test"
		h.cfg.Store.Versions = NativeStateVersions()
	}
	t.Cleanup(h.capture)
	return h
}

func (h *processHarness) start(name string) *processWorker {
	h.t.Helper()
	cfg := h.cfg
	cfg.Process = &pb.ProcessRef{InstanceId: name, Epoch: uuid.NewString()}
	data, _ := marshalRetainedFixture(cfg)
	command := exec.CommandContext(h.ctx, os.Args[0], "-test.run=^TestStateProcessHelper$", "-test.v")
	command.Env = append(os.Environ(), "SIPBRIDGE_PROCESS_HELPER="+string(data))
	input, err := command.StdinPipe()
	if err != nil {
		h.t.Fatal(err)
	}
	output, err := command.StdoutPipe()
	if err != nil {
		h.t.Fatal(err)
	}
	log, err := os.Create(filepath.Join(h.t.TempDir(), name+".log"))
	if err != nil {
		h.t.Fatal(err)
	}
	command.Stderr = log
	w := &processWorker{process: cfg.Process, command: command, input: input, events: make(chan stateProcessEvent, 128), done: make(chan error, 1)}
	if err := command.Start(); err != nil {
		h.t.Fatal(err)
	}
	go func() {
		scanner := bufio.NewScanner(output)
		for scanner.Scan() {
			line := scanner.Text()
			if !strings.HasPrefix(line, "STATE-PROCESS ") {
				_, _ = log.WriteString(line + "\n")
				continue
			}
			var event stateProcessEvent
			if unmarshalRetainedFixture([]byte(strings.TrimPrefix(line, "STATE-PROCESS ")), &event) == nil {
				h.mu.Lock()
				h.phases = append(h.phases, processPhaseObservation{Worker: w.process.InstanceId, Event: event})
				h.mu.Unlock()
				if !strings.HasPrefix(event.Kind, "reserve-") {
					w.events <- event
				}
			}
		}
	}()
	go func() { w.done <- command.Wait(); close(w.done); log.Close() }()
	h.t.Cleanup(func() {
		_ = command.Process.Kill()
		<-w.done
		if h.t.Failed() {
			data, _ := os.ReadFile(log.Name())
			h.t.Logf("%s stderr: %s", name, data)
		}
	})
	w.ready = h.workerEvent(w, "ready", "")
	h.mu.Lock()
	h.workers[name] = w
	h.mu.Unlock()
	h.connect(w, 1)
	return w
}

func (h *processHarness) connect(w *processWorker, attempt uint64) {
	h.connectController(w, attempt, h.application, nil)
}
func (h *processHarness) connectController(w *processWorker, attempt uint64, holder *pb.ProcessRef, recovery *pb.ControllerRecovery) {
	h.t.Helper()
	connection, _, err := websocket.Dial(h.ctx, "wss://"+w.ready.Control+"/v1/control", &websocket.DialOptions{Subprotocols: []string{control.Subprotocol}, HTTPClient: &http.Client{Transport: &http.Transport{TLSClientConfig: h.clientTLS.Clone()}}})
	if err != nil {
		h.t.Fatal(err)
	}
	p := &processController{worker: w, harness: h, connection: connection, pending: make(map[string]chan *pb.Envelope), done: make(chan struct{})}
	autonomous := h.cfg.Store.Versions == NativeStateVersions()
	capabilities := stateProcessCapabilities(autonomous)
	if h.recoverable {
		capabilities = append(capabilities, control.ControllerReattachCapability)
	}
	writeControllerEnvelope(h.t, h.ctx, connection, &pb.Envelope{CorrelationId: "hello", Payload: &pb.Envelope_Hello{Hello: &pb.SessionHello{Controller: holder, Recovery: recovery, ConnectionAttempt: attempt, Major: 1, Capabilities: capabilities, Bounds: &pb.Bounds{MaxEnvelopeBytes: control.MaxEnvelope}}}})
	hello := readControllerEnvelope(h.t, h.ctx, connection)
	ack := hello.GetHelloAck()
	if ack == nil {
		h.t.Fatalf("subprocess sent %T before its hello acknowledgement", hello.Payload)
	}
	p.session = ack.SessionId
	if autonomous {
		application := holder
		if recovery != nil {
			application = recovery.Application
		}
		if ack.ApplicationLease == nil || !proto.Equal(ack.ApplicationLease.Application, application) || ack.ApplicationLease.Term == "" || ack.ApplicationLease.Authority == nil || ack.ApplicationLease.Authority.SchemaVersion != 1 {
			h.t.Fatal("native process hello did not acquire its real SIP authority lease")
		}
		p.lease = proto.Clone(ack.ApplicationLease).(*pb.ApplicationLease)
	}
	h.mu.Lock()
	w.peer = p
	h.mu.Unlock()
	go p.read()
	h.t.Cleanup(func() { connection.CloseNow() })
	if err := p.send(&pb.Envelope{CorrelationId: uuid.NewString(), Payload: &pb.Envelope_Status{Status: &pb.SessionStatus{RetainedReady: true, BindingsReady: true, AdmitCalls: true, AdmitSubscriptions: true}}}); err != nil {
		h.t.Fatal(err)
	}
	if _, err := p.call(&pb.Envelope{Payload: &pb.Envelope_Ping{Ping: &pb.Heartbeat{Nonce: uuid.NewString()}}}); err != nil {
		h.t.Fatal(err)
	}
}

func (h *processHarness) workerEvent(w *processWorker, kind, id string) stateProcessEvent {
	h.t.Helper()
	for {
		select {
		case event := <-w.events:
			if event.Kind == "error" {
				h.t.Fatal(event.Error)
			}
			if event.Kind == kind && (id == "" || event.ID == id) {
				return event
			}
		case err := <-w.done:
			h.t.Fatalf("%s exited waiting for %s: %v", w.process.InstanceId, kind, err)
		case <-h.ctx.Done():
			h.t.Fatalf("%s timed out waiting for %s/%s", w.process.InstanceId, kind, id)
		}
	}
}
func (h *processHarness) arm(w *processWorker, rule stateProcessRule) {
	h.t.Helper()
	w.sendMu.Lock()
	err := json.NewEncoder(w.input).Encode(stateProcessCommand{Op: "arm", Rule: rule})
	w.sendMu.Unlock()
	if err != nil {
		h.t.Fatal(err)
	}
	h.workerEvent(w, "arm", rule.ID)
}
func (h *processHarness) release(w *processWorker) {
	h.t.Helper()
	w.sendMu.Lock()
	err := json.NewEncoder(w.input).Encode(stateProcessCommand{Op: "release"})
	w.sendMu.Unlock()
	if err != nil {
		h.t.Fatal(err)
	}
}
func (h *processHarness) kill(w *processWorker) {
	h.t.Helper()
	if err := w.command.Process.Kill(); err != nil {
		h.t.Fatal(err)
	}
	select {
	case <-w.done:
	case <-h.ctx.Done():
		h.t.Fatal("subprocess kill not observed")
	}
}

func (p *processController) send(envelope *pb.Envelope) error {
	envelope.SessionId = p.session
	data, err := proto.Marshal(envelope)
	if err != nil {
		return err
	}
	return p.connection.Write(p.harness.ctx, websocket.MessageBinary, data)
}
func (p *processController) call(envelope *pb.Envelope) (*pb.Envelope, error) {
	if envelope.CorrelationId == "" {
		envelope.CorrelationId = uuid.NewString()
	}
	if envelope.GetPing() == nil {
		envelope.RemainingBudgetMs = 2000
		if envelope.GetCommand() != nil {
			envelope.RemainingBudgetMs = 4000
		}
	}
	if err := p.admitNativeCommand(envelope); err != nil {
		return nil, err
	}
	result := make(chan *pb.Envelope, 1)
	p.mu.Lock()
	p.pending[envelope.CorrelationId] = result
	p.mu.Unlock()
	defer func() { p.mu.Lock(); delete(p.pending, envelope.CorrelationId); p.mu.Unlock() }()
	if err := p.send(envelope); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(p.harness.ctx, 5*time.Second)
	defer cancel()
	select {
	case reply := <-result:
		return reply, nil
	case <-p.done:
		return nil, fmt.Errorf("fixture control session closed")
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (p *processController) admitNativeCommand(envelope *pb.Envelope) error {
	if p.harness.cfg.Store.Versions != NativeStateVersions() {
		return nil
	}
	command := envelope.GetCommand()
	if command == nil || command.GetCancel() != nil || command.Admission != nil {
		return nil
	}
	if p.lease == nil || command.Context == nil {
		return fmt.Errorf("native fixture command has no application lease or context")
	}

	var delivery *pb.ContextDelivery
	binding := ""
	if command.Context.State != nil {
		group, err := p.harness.store.GroupForCallID(command.Context.State.CallId)
		if err != nil {
			return err
		}
		record, err := p.harness.store.Load(p.harness.ctx, group)
		if err != nil {
			return err
		}
		decoded, err := decodeStateGroup(record)
		if err != nil {
			return err
		}
		context := decoded.Contexts[command.Context.Id]
		if context == nil || context.Snapshot == nil {
			return fmt.Errorf("native fixture command context is missing")
		}
		if context.Delivery != nil {
			delivery = proto.Clone(context.Delivery).(*pb.ContextDelivery)
		}
		binding = context.Snapshot.BindingId
	}

	var selectedFlow *pb.FlowRef
	request := command.GetDialogRequest() != nil
	if open := command.GetOpenUac(); open != nil {
		request = true
		binding = open.BindingId
	}
	if request && binding != "" {
		p.harness.mu.Lock()
		if p.harness.flow != nil {
			selectedFlow = proto.Clone(p.harness.flow).(*pb.FlowRef)
		}
		p.harness.mu.Unlock()
	}

	p.harness.mu.Lock()
	epoch := p.harness.directoryEpoch
	p.harness.mu.Unlock()
	command.Admission = &pb.CommandAdmission{
		DirectoryEpoch:   epoch,
		ExpiresAtMs:      uint64(time.Now().Add(time.Duration(envelope.RemainingBudgetMs) * time.Millisecond).UnixMilli()),
		SelectedFlow:     selectedFlow,
		ExpectedDelivery: delivery,
	}
	return nil
}

func (p *processController) read() {
	defer close(p.done)
	for {
		kind, data, err := p.connection.Read(p.harness.ctx)
		if err != nil {
			return
		}
		if kind != websocket.MessageBinary {
			continue
		}
		envelope := new(pb.Envelope)
		if proto.Unmarshal(data, envelope) != nil {
			return
		}
		p.mu.Lock()
		pending := p.pending[envelope.CorrelationId]
		p.mu.Unlock()
		if pending != nil {
			p.harness.mu.Lock()
			p.harness.observations = append(p.harness.observations, processObservation{Worker: p.worker.process.InstanceId, Envelope: envelope})
			p.harness.mu.Unlock()
			select {
			case pending <- envelope:
			default:
			}
			continue
		}
		if ping := envelope.GetPing(); ping != nil {
			_ = p.send(&pb.Envelope{CorrelationId: envelope.CorrelationId, Payload: &pb.Envelope_Pong{Pong: ping}})
			continue
		}
		p.harness.mu.Lock()
		p.harness.observations = append(p.harness.observations, processObservation{Worker: p.worker.process.InstanceId, Envelope: envelope})
		p.harness.mu.Unlock()
		select {
		case p.harness.notify <- struct{}{}:
		default:
		}
		go p.handle(envelope)
	}
}
func (p *processController) reply(request, reply *pb.Envelope) {
	reply.CorrelationId = request.CorrelationId
	_ = p.send(reply)
}
func (p *processController) handle(envelope *pb.Envelope) {
	h := p.harness
	switch body := envelope.Payload.(type) {
	case *pb.Envelope_Ingress:
		in := body.Ingress
		h.mu.Lock()
		route := h.routeRequest
		h.mu.Unlock()
		if route != nil && route(p, envelope) {
			return
		}
		h.mu.Lock()
		h.refs[in.Context.State.CallId] = in.Context
		h.flow = in.Flow
		h.mu.Unlock()
		// The fixture admits its bounded queue immediately, then exercises the
		// actual native control admission. It supplies only fixture authority.
		p.reply(envelope, &pb.Envelope{Payload: &pb.Envelope_RouteResult{RouteResult: &pb.RouteResult{Outcome: pb.RouteResult_ACCEPTED}}})
		admission := pb.OwnerIngest_EXISTING_FLOW
		if in.Message.Status > 0 {
			admission = pb.OwnerIngest_TLS_TRANSACTION_CONTINUITY
		} else if parsed, err := sip.ParseMessage(in.Message.Wire); err == nil && tag(parsed.To().Params) != "" {
			admission = pb.OwnerIngest_TLS_DIALOG_CONTINUITY
		}
		ingest := &pb.OwnerIngest{Context: in.Context, Application: h.application, Flow: in.Flow, Wire: in.Message.Wire, Authenticated: true, PrincipalId: "fixture-phone", BindingId: "fixture-binding", Admission: admission}
		h.mu.Lock()
		chooseWorker := h.ingestWorker
		h.mu.Unlock()
		worker := p.worker
		if chooseWorker != nil {
			if selected := chooseWorker(ingest); selected != nil {
				worker = selected
			}
		}
		h.mu.Lock()
		controller := worker.peer
		h.mu.Unlock()
		_, _ = controller.call(&pb.Envelope{Payload: &pb.Envelope_Ingest{Ingest: ingest}})
	case *pb.Envelope_CredentialLookup:
		h.mu.Lock()
		lookup := h.credentialLookup
		h.mu.Unlock()
		if lookup != nil {
			if result := lookup(p.worker, envelope.CorrelationId, body.CredentialLookup); result != nil {
				p.reply(envelope, &pb.Envelope{Payload: &pb.Envelope_CredentialResult{CredentialResult: result}})
			}
		}
	case *pb.Envelope_ProbeApplication:
		probe := body.ProbeApplication
		if h.cfg.Store.Versions == NativeStateVersions() {
			h.t.Error("native process fixture unexpectedly requested Controller owner-liveness proof")
			p.reply(envelope, &pb.Envelope{Payload: &pb.Envelope_Result{Result: &pb.OperationResult{Certainty: pb.OperationResult_NOT_ACCEPTED, Error: "unexpected directory proof"}}})
			return
		}
		p.reply(envelope, &pb.Envelope{Payload: &pb.Envelope_ApplicationStatus{ApplicationStatus: &pb.ApplicationStatus{Context: probe.Context, Application: probe.Application, DirectoryEpoch: "fixture-directory", Live: true, CheckedAtMs: uint64(time.Now().UnixMilli())}}})
	case *pb.Envelope_Request:
		p.reply(envelope, &pb.Envelope{Payload: &pb.Envelope_Result{Result: &pb.OperationResult{Certainty: pb.OperationResult_ACCEPTED}}})
	case *pb.Envelope_DialogEvent:
		p.reply(envelope, &pb.Envelope{Payload: &pb.Envelope_Result{Result: &pb.OperationResult{Certainty: pb.OperationResult_ACCEPTED}}})
	case *pb.Envelope_Egress:
		egress := body.Egress
		if h.cfg.Store.Versions == NativeStateVersions() && egress.Authorization.AuthorityClass != pb.EffectAuthorization_APPLICATION {
			h.t.Error("retained native completion depended on a Controller egress grant")
			p.reply(envelope, &pb.Envelope{Payload: &pb.Envelope_Result{Result: &pb.OperationResult{Certainty: pb.OperationResult_NOT_ACCEPTED, Error: "unexpected native egress grant"}}})
			return
		}
		h.mu.Lock()
		epoch := h.directoryEpoch
		permit := h.permits[egress.Authorization.Effect.Id]
		var scopeError error
		if permit == nil && int64(egress.Authorization.DeadlineMs) > time.Now().UnixMilli() {
			flow := egress.ReturnFlow
			if flow == nil {
				flow = h.flow
			}
			scope := egress.Authorization.CompletionScope
			if scope == "" {
				scope = "scope-" + egress.Snapshot.Context.Id
				if h.cfg.Store.Versions == NativeStateVersions() {
					scope = fixtureResponseAuthority(egress.Snapshot, egress.Message, epoch).CompletionScope
				}
			}
			if h.completionScope != nil {
				scope, scopeError = h.completionScope(egress)
			}
			if scopeError == nil {
				permit = &pb.SendPermit{Authorization: proto.Clone(egress.Authorization).(*pb.EffectAuthorization), Flow: proto.Clone(flow).(*pb.FlowRef), DirectoryEpoch: epoch, OrderingProof: "fixture-proof", CompletionScope: scope}
				h.permits[egress.Authorization.Effect.Id] = permit
			}
		}
		beforeReply := h.beforeEgressReply
		h.mu.Unlock()
		if beforeReply != nil {
			beforeReply(egress)
		}
		if scopeError != nil {
			p.reply(envelope, &pb.Envelope{Payload: &pb.Envelope_Result{Result: &pb.OperationResult{Certainty: pb.OperationResult_NOT_ACCEPTED, Error: scopeError.Error()}}})
		} else if permit == nil || !proto.Equal(permit.Authorization, egress.Authorization) {
			p.reply(envelope, &pb.Envelope{Payload: &pb.Envelope_Result{Result: &pb.OperationResult{Certainty: pb.OperationResult_NOT_ACCEPTED, Error: "send permit not found"}}})
		} else {
			p.reply(envelope, &pb.Envelope{Payload: &pb.Envelope_SendPermit{SendPermit: proto.Clone(permit).(*pb.SendPermit)}})
		}
	case *pb.Envelope_DeliverEffect:
		h.deliver(p, envelope, body.DeliverEffect, false)
	case *pb.Envelope_InspectEffect:
		h.deliver(p, envelope, body.InspectEffect, true)
	case *pb.Envelope_FlowAdoption:
		proposal := body.FlowAdoption
		h.mu.Lock()
		epoch := h.directoryEpoch
		h.mu.Unlock()
		p.reply(envelope, &pb.Envelope{Payload: &pb.Envelope_FlowAdoptionResult{FlowAdoptionResult: &pb.FlowAdoptionResult{Receipt: &pb.ContextDelivery{Flow: proposal.Flow, NativeRevision: proposal.NativeRevision, DirectoryEpoch: epoch, RegistrationTicket: "1"}}}})
	}
}
func (h *processHarness) deliver(origin *processController, request *pb.Envelope, write *pb.FlowWrite, inspect bool) {
	h.mu.Lock()
	worker := h.workers[write.Flow.Ingress.InstanceId]
	h.mu.Unlock()
	result := &pb.DeliveryResult{Effect: write.Effect, Outcome: pb.DeliveryResult_UNKNOWN}
	if worker != nil && sameProcess(worker.process, write.Flow.Ingress) {
		h.mu.Lock()
		controller := worker.peer
		h.mu.Unlock()
		command := &pb.Envelope{Payload: &pb.Envelope_Write{Write: write}}
		if inspect {
			command.Payload = &pb.Envelope_InspectEffect{InspectEffect: write}
		}
		if reply, err := controller.call(command); err == nil && reply.GetDelivery() != nil {
			result = reply.GetDelivery()
		}
	}
	origin.reply(request, &pb.Envelope{Payload: &pb.Envelope_Delivery{Delivery: result}})
}

func (h *processHarness) wait(predicate func(processObservation) bool) processObservation {
	h.t.Helper()
	for {
		h.mu.Lock()
		for _, observation := range h.observations {
			if predicate(observation) {
				h.mu.Unlock()
				return observation
			}
		}
		h.mu.Unlock()
		select {
		case <-h.notify:
		case <-h.ctx.Done():
			h.mu.Lock()
			for _, o := range h.observations[max(0, len(h.observations)-24):] {
				h.t.Logf("%s payload=%T result=%s delivery=%s", o.Worker, o.Envelope.Payload, o.Envelope.GetResult().String(), o.Envelope.GetDelivery().String())
			}
			h.mu.Unlock()
			h.t.Fatal("control observation timed out")
		}
	}
}
func (h *processHarness) request(callID string, cseq uint32) processObservation {
	return h.wait(func(o processObservation) bool {
		r := o.Envelope.GetRequest()
		if r == nil {
			return false
		}
		m, _ := sip.ParseMessage(r.Message.Wire)
		return m.CallID().Value() == callID && m.CSeq().SeqNo == cseq
	})
}
func (h *processHarness) result(peer *processWorker, cmd *pb.Command) *pb.OperationResult {
	h.t.Helper()
	reply, err := peer.peer.call(&pb.Envelope{Payload: &pb.Envelope_Command{Command: cmd}})
	if err != nil {
		h.t.Fatal(err)
	}
	result := reply.GetResult()
	if result == nil || result.Error != "" || result.Certainty == pb.OperationResult_NOT_ACCEPTED || result.Certainty == pb.OperationResult_UNKNOWN {
		h.mu.Lock()
		for _, o := range h.observations {
			if delivery := o.Envelope.GetDelivery(); delivery != nil {
				h.t.Logf("%s delivery=%s", o.Worker, delivery.String())
			} else {
				h.t.Logf("%s payload=%T result=%s", o.Worker, o.Envelope.Payload, o.Envelope.GetResult().String())
			}
		}
		h.mu.Unlock()
		if cmd.Context.State != nil {
			rec, g := h.record(cmd.Context.State.CallId)
			for _, effect := range rec.Payload.Effects {
				h.t.Logf("retained %s %s %s", effect.ID, effect.Kind, effect.Status)
			}
			for _, c := range g.Contexts {
				h.t.Logf("native result=%s", c.Result.String())
			}
		}
		h.t.Fatalf("command failed: %+v", result)
	}
	if result.Snapshot != nil && result.Snapshot.Context != nil && result.Snapshot.Context.State != nil {
		h.mu.Lock()
		h.refs[result.Snapshot.CallId] = result.Snapshot.Context
		h.mu.Unlock()
	}
	return result
}
func (h *processHarness) respond(worker *processWorker, presented *pb.RequestPresented, status uint32) *pb.OperationResult {
	response := &pb.Respond{RequestId: presented.RequestId, Status: status, Body: []byte("v=0\r\n")}
	if h.cfg.Store.Versions == NativeStateVersions() {
		h.mu.Lock()
		epoch := h.directoryEpoch
		h.mu.Unlock()
		response.Authority = fixtureResponseAuthority(presented.Snapshot, presented.Message, epoch)
	}
	return h.result(worker, &pb.Command{Context: presented.Snapshot.Context, Sequence: presented.Snapshot.CommandSequence + 1, Action: &pb.Command_Respond{Respond: response}})
}
func (h *processHarness) record(callID string) (state.Record, stateGroup) {
	h.t.Helper()
	ref, err := h.store.GroupForCallID(callID)
	if err != nil {
		h.t.Fatal(err)
	}
	record, err := h.store.Load(h.ctx, ref)
	if err != nil {
		h.t.Fatal(err)
	}
	g, err := decodeStateGroup(record)
	if err != nil {
		h.t.Fatal(err)
	}
	return record, g
}

type processPhone struct {
	connection net.Conn
	reader     *bufio.Reader
	received   []sip.Message
	sent       [][]byte
}

func (h *processHarness) phone(worker *processWorker) *processPhone {
	h.t.Helper()
	conn, err := tls.Dial("tcp", worker.ready.SIP, h.clientTLS.Clone())
	if err != nil {
		h.t.Fatal(err)
	}
	h.t.Cleanup(func() { conn.Close() })
	phone := &processPhone{connection: conn, reader: bufio.NewReader(conn)}
	h.mu.Lock()
	h.phones = append(h.phones, phone)
	h.mu.Unlock()
	return phone
}
func (p *processPhone) send(t *testing.T, wire []byte) {
	t.Helper()
	p.sent = append(p.sent, append([]byte(nil), wire...))
	if _, err := p.connection.Write(wire); err != nil {
		t.Fatal(err)
	}
}
func (p *processPhone) receive(t *testing.T, method sip.RequestMethod, status int) sip.Message {
	t.Helper()
	_ = p.connection.SetReadDeadline(time.Now().Add(5 * time.Second))
	for {
		var header strings.Builder
		length := 0
		for {
			line, err := p.reader.ReadString('\n')
			if err != nil {
				t.Fatal(err)
			}
			header.WriteString(line)
			if line == "\r\n" {
				break
			}
			if strings.HasPrefix(strings.ToLower(line), "content-length:") {
				length, _ = strconv.Atoi(strings.TrimSpace(strings.SplitN(line, ":", 2)[1]))
			}
		}
		body := make([]byte, length)
		if _, err := io.ReadFull(p.reader, body); err != nil {
			t.Fatal(err)
		}
		message, err := sip.ParseMessage(append([]byte(header.String()), body...))
		if err != nil {
			t.Fatal(err)
		}
		p.received = append(p.received, message)
		if message.CSeq().MethodName != method {
			continue
		}
		if status == 0 {
			if _, ok := message.(*sip.Request); !ok {
				continue
			}
		} else {
			if response, ok := message.(*sip.Response); !ok || status > 0 && response.StatusCode != status {
				continue
			}
		}
		return message
	}
}

func TestProcessPhoneReceiveSeparatesRequestsAndResponses(t *testing.T) {
	for _, status := range []int{0, 200} {
		t.Run(strconv.Itoa(status), func(t *testing.T) {
			client, peer := net.Pipe()
			t.Cleanup(func() { _ = client.Close(); _ = peer.Close() })
			phone := &processPhone{connection: client, reader: bufio.NewReader(client)}
			wire := processRequest(t, sip.INVITE, "receive-message-kind", 1, "")
			parsed, err := sip.ParseMessage(wire)
			if err != nil {
				t.Fatal(err)
			}
			response := []byte(sip.NewResponseFromRequest(parsed.(*sip.Request), 200, "OK", nil).String())
			frames := append(append([]byte(nil), wire...), response...)
			if status == 0 {
				frames = append(append([]byte(nil), response...), wire...)
			}
			written := make(chan error, 1)
			go func() { _, err := peer.Write(frames); written <- err }()
			selected := phone.receive(t, sip.INVITE, status)
			_, request := selected.(*sip.Request)
			if request != (status == 0) || len(phone.received) != 2 {
				t.Fatal("message kind selector accepted the opposite direction or lost observed wire")
			}
			if err := <-written; err != nil {
				t.Fatal(err)
			}
		})
	}
}

func processRequest(t *testing.T, method sip.RequestMethod, callID string, cseq uint32, localTag string) []byte {
	t.Helper()
	to := "<sip:service@example.test>"
	if localTag != "" {
		to += ";tag=" + localTag
	}
	body, contentType := "", ""
	if method == sip.INVITE {
		body, contentType = "v=0\r\n", "Content-Type: application/sdp\r\n"
	}
	return []byte(fmt.Sprintf("%s sip:service@example.test SIP/2.0\r\nVia: SIP/2.0/TLS phone.example.test:5061;branch=z9hG4bK-%s-%d-%s;rport\r\nFrom: <sip:phone@example.test>;tag=phone\r\nTo: %s\r\nCall-ID: %s\r\nCSeq: %d %s\r\nContact: <sip:phone@phone.example.test:5061;transport=tls>\r\n%sContent-Length: %d\r\n\r\n%s", method, callID, cseq, method, to, callID, cseq, method, contentType, len(body), body))
}

func processOnlyContext(t *testing.T, group stateGroup) *stateContext {
	t.Helper()
	if len(group.Contexts) != 1 {
		t.Fatalf("wanted one context, got %d", len(group.Contexts))
	}
	for _, c := range group.Contexts {
		return c
	}
	return nil
}

func processEstablish(h *processHarness, a *processWorker, phone *processPhone, callID string) *pb.ContextRef {
	h.t.Helper()
	phone.send(h.t, processRequest(h.t, sip.INVITE, callID, 1, ""))
	presented := h.request(callID, 1).Envelope.GetRequest()
	h.respond(a, presented, 200)
	response := phone.receive(h.t, sip.INVITE, 200)
	phone.send(h.t, processRequest(h.t, sip.ACK, callID, 1, tag(response.To().Params)))
	h.wait(func(o processObservation) bool {
		event := o.Envelope.GetDialogEvent()
		return event != nil && event.Kind == pb.DialogEvent_ACKNOWLEDGED && event.Snapshot.CallId == callID
	})
	return presented.Snapshot.Context
}

func TestStateNativeProcessLossDuringActiveUASPhases(t *testing.T) {
	processLossDuringActiveUASPhases(t, newRelayProcessHarness)
}

func TestStateNativeCurrentProcessLossDuringActiveUASPhases(t *testing.T) {
	processLossDuringActiveUASPhases(t, newNativeProcessHarness)
}

func processLossDuringActiveUASPhases(t *testing.T, harness func(*testing.T) *processHarness) {
	t.Helper()
	for _, phase := range []string{"initial-proceeding", "awaiting-ack", "remote-reinvite", "bye-admitted"} {
		t.Run(phase, func(t *testing.T) {
			h := harness(t)
			a, b := h.start("sip-a"), h.start("sip-b")
			old := h.phone(a)
			callID := phase + "-" + uuid.NewString()
			initial := processRequest(t, sip.INVITE, callID, 1, "")
			old.send(t, initial)
			presented := h.request(callID, 1).Envelope.GetRequest()
			ref := presented.Snapshot.Context
			localTag := presented.Snapshot.LocalTag
			var replay []byte
			switch phase {
			case "initial-proceeding":
				h.respond(a, presented, 183)
				old.receive(t, sip.INVITE, 183)
				replay = initial
			case "awaiting-ack":
				h.respond(a, presented, 200)
				old.receive(t, sip.INVITE, 200)
			default:
				h.respond(a, presented, 200)
				old.receive(t, sip.INVITE, 200)
				old.send(t, processRequest(t, sip.ACK, callID, 1, localTag))
				h.wait(func(o processObservation) bool {
					event := o.Envelope.GetDialogEvent()
					return event != nil && event.Kind == pb.DialogEvent_ACKNOWLEDGED && event.Snapshot.CallId == callID
				})
				if phase == "remote-reinvite" {
					replay = processRequest(t, sip.INVITE, callID, 2, localTag)
					old.send(t, replay)
					presented = h.request(callID, 2).Envelope.GetRequest()
				} else {
					replay = processRequest(t, sip.BYE, callID, 2, localTag)
					rule := stateProcessRule{ID: "bye-admitted", Stage: "begin", CallID: callID, Method: "BYE", InDialog: true}
					h.arm(a, rule)
					old.send(t, replay)
					h.workerEvent(a, "barrier", rule.ID)
				}
			}
			beforeRecord, beforeGroup := h.record(callID)
			before := processOnlyContext(t, beforeGroup)
			h.kill(a)
			// The old process is gone before a replacement TLS connection exists.
			replacement := h.phone(b)
			if phase == "awaiting-ack" {
				replacement.send(t, processRequest(t, sip.ACK, callID, 1, localTag))
				h.wait(func(o processObservation) bool {
					event := o.Envelope.GetDialogEvent()
					return o.Worker == "sip-b" && event != nil && event.Kind == pb.DialogEvent_ACKNOWLEDGED && event.Snapshot.CallId == callID
				})
			} else {
				replacement.send(t, replay)
				if phase == "bye-admitted" {
					replacement.receive(t, sip.BYE, 200)
				} else {
					// Observe exact replacement-flow native admission, not a timer.
					h.wait(func(o processObservation) bool {
						in := o.Envelope.GetIngress()
						return o.Worker == "sip-b" && in != nil && bytes.Equal(in.Message.Wire, replay)
					})
					// Serial control ingestion must precede the final command. Inspect
					// until its exact server transaction's retained return flow is B.
					h.waitReplacementTransaction(callID, presented.RequestId, b.process)
					_, g := h.record(callID)
					current := processOnlyContext(t, g)
					copy := proto.Clone(presented).(*pb.RequestPresented)
					copy.Snapshot = current.Snapshot
					h.respond(b, copy, 200)
					replacement.receive(t, sip.INVITE, 200)
					sequence := uint32(1)
					if phase == "remote-reinvite" {
						sequence = 2
					}
					replacement.send(t, processRequest(t, sip.ACK, callID, sequence, localTag))
				}
			}
			if phase != "bye-admitted" {
				sequence := uint32(2)
				if phase == "remote-reinvite" {
					sequence = 3
				}
				replacement.send(t, processRequest(t, sip.BYE, callID, sequence, localTag))
				replacement.receive(t, sip.BYE, 200)
			}
			ended := h.wait(func(o processObservation) bool {
				event := o.Envelope.GetDialogEvent()
				return event != nil && event.Kind == pb.DialogEvent_ENDED && event.Snapshot.CallId == callID
			})
			if !stateSameContext(ref, ended.Envelope.GetDialogEvent().Snapshot.Context) || !sameProcess(ended.Envelope.GetDialogEvent().Snapshot.Application, h.application) {
				t.Fatal("active-phase takeover changed logical/application identity")
			}
			afterRecord, afterGroup := h.record(callID)
			after := processOnlyContext(t, afterGroup)
			if afterRecord.Ref.Incarnation != beforeRecord.Ref.Incarnation || after.Snapshot.LocalTag != before.Snapshot.LocalTag || !after.Snapshot.Ended {
				t.Fatal("active-phase takeover recreated protocol state")
			}
			t.Logf("phase=%s group=%s incarnation=%s revision=%s->%s original=%s replacement=%s", phase, afterRecord.Ref.ID, afterRecord.Ref.Incarnation, beforeRecord.Revision, afterRecord.Revision, a.process.Epoch, b.process.Epoch)
		})
	}
}

func (h *processHarness) waitReplacementTransaction(callID, requestID string, process *pb.ProcessRef) {
	h.t.Helper()
	// Polling observes a committed predicate, never uses elapsed time as proof.
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		_, g := h.record(callID)
		c := processOnlyContext(h.t, g)
		if tx := c.Transactions[c.Requests[requestID]]; tx != nil && tx.Flow != nil && sameProcess(tx.Flow.Ingress, process) {
			return
		}
		select {
		case <-ticker.C:
		case <-h.ctx.Done():
			h.t.Fatal("replacement transaction flow never committed")
		}
	}
}

func TestStateNativeProcessLostInviteCompletionStillACKsResponseOnB(t *testing.T) {
	processLostInviteCompletionStillACKsResponseOnB(t, newRelayProcessHarness)
}

func TestStateNativeCurrentProcessLostInviteCompletionStillACKsResponseOnB(t *testing.T) {
	processLostInviteCompletionStillACKsResponseOnB(t, newNativeProcessHarness)
}

func processLostInviteCompletionStillACKsResponseOnB(t *testing.T, harness func(*testing.T) *processHarness) {
	t.Helper()
	h := harness(t)
	a, b := h.start("sip-a"), h.start("sip-b")
	phone := h.phone(a)
	seedCall := "flow-seed-" + uuid.NewString()
	phone.send(t, processRequest(t, sip.INFO, seedCall, 1, ""))
	seed := h.request(seedCall, 1).Envelope.GetRequest()
	h.respond(a, seed, 200)
	phone.receive(t, sip.INFO, 200)
	rule := stateProcessRule{ID: "invite-written-before-complete", Stage: "complete", Method: "INVITE", Class: pb.EffectAuthorization_APPLICATION}
	h.arm(a, rule)
	placeholder := &pb.ContextRef{Owner: a.process, Id: "outbound-" + uuid.NewString()}
	opened := h.result(a, &pb.Command{Context: placeholder, Sequence: 1, Action: &pb.Command_OpenUac{OpenUac: &pb.OpenUac{Uri: "sip:phone@example.test", Transport: "tls", BindingId: "fixture-binding", PrincipalId: "fixture-phone", Headers: []*pb.Header{{Name: "From", Value: "<sip:service@example.test>"}}}}})
	invite := phone.receive(t, sip.INVITE, 0).(*sip.Request)
	barrier := h.workerEvent(a, "barrier", rule.ID)
	callID := invite.CallID().Value()
	before, _ := h.record(callID)
	var old state.Effect
	for _, effect := range before.Payload.Effects {
		if effect.ID == barrier.Token {
			old = effect
		}
	}
	if old.Kind != "deliver" || old.Status != state.EffectDispatching {
		t.Fatal("writer barrier did not retain an actually-started dispatch")
	}
	h.kill(a)
	replacement := h.phone(b)
	response := sip.NewResponseFromRequest(invite, 200, "OK", nil)
	response.To().Params.Add("tag", "phone-answer")
	response.AppendHeader(&sip.ContactHeader{Address: sip.Uri{Scheme: "sip", User: "phone", Host: "phone.example.test", Port: 5061, UriParams: sip.HeaderParams{{K: "transport", V: "tls"}}}})
	replacement.send(t, []byte(response.String()))
	ack := replacement.receive(t, sip.ACK, 0)
	if ack.CSeq().SeqNo != invite.CSeq().SeqNo || tag(ack.To().Params) != "phone-answer" {
		t.Fatal("response-first reconnect lost exact INVITE ACK identity")
	}
	answered := h.wait(func(o processObservation) bool {
		event := o.Envelope.GetDialogEvent()
		return event != nil && event.Kind == pb.DialogEvent_ANSWERED && event.Snapshot.CallId == callID
	}).Envelope.GetDialogEvent()
	if !stateSameContext(opened.Snapshot.Context, answered.Snapshot.Context) || !sameProcess(answered.Snapshot.Application, h.application) {
		t.Fatal("missing INVITE write receipt created a new application dialog")
	}
	after, g := h.record(callID)
	found := false
	for _, effect := range after.Payload.Effects {
		if effect.ID == old.ID {
			found = true
			if effect.Status != state.EffectDispatching && effect.Status != state.EffectUnknown {
				t.Fatal("dead physical writer uncertainty was falsely resolved")
			}
		}
	}
	if !found {
		t.Fatal("old dispatch evidence was dropped before reconciliation")
	}
	// Incoming BYE remains independently processable despite the old uncertainty.
	c := processOnlyContext(t, g)
	bye := fmt.Sprintf("BYE sip:service@example.test SIP/2.0\r\nVia: SIP/2.0/TLS phone.example.test:5061;branch=z9hG4bK-response-bye;rport\r\nFrom: %s\r\nTo: %s\r\nCall-ID: %s\r\nCSeq: 2 BYE\r\nContent-Length: 0\r\n\r\n", response.To().Value(), response.From().Value(), callID)
	replacement.send(t, []byte(bye))
	replacement.receive(t, sip.BYE, 200)
	if c.Snapshot.RemoteTag != "phone-answer" {
		t.Fatal("response branch not retained")
	}
	_, terminal := processWaitRecord(h, callID, func(_ state.Record, current *stateContext) bool {
		return current.Snapshot.Ended && current.Dialogs[current.Dialog].Phase == portable.DialogEnded
	})
	priorACKs := terminal.ACKEmissions[terminal.Dialog].Count
	priorTransactions := len(terminal.Transactions)
	replacement.send(t, []byte(response.String()))
	_, terminal = processWaitRecord(h, callID, func(_ state.Record, current *stateContext) bool {
		return current.ACKEmissions[current.Dialog].Count > priorACKs
	})
	replayedACK := replacement.receive(t, sip.ACK, 0)
	if replayedACK.String() != ack.String() || !terminal.Snapshot.Ended || !terminal.Snapshot.Answered || terminal.Dialogs[terminal.Dialog].Phase != portable.DialogEnded || len(terminal.Transactions) != priorTransactions || terminal.Snapshot.DialogId != answered.Snapshot.DialogId {
		t.Fatal("ended winner ACK replay changed exact wire, terminal state, winner or BYE procedure")
	}
	h.mu.Lock()
	answers := make(map[string]bool)
	for _, observed := range h.observations {
		if event := observed.Envelope.GetDialogEvent(); event != nil && event.Snapshot.CallId == callID && event.Kind == pb.DialogEvent_ANSWERED {
			answers[observed.Envelope.CorrelationId] = true
		}
	}
	h.mu.Unlock()
	if len(answers) != 1 {
		t.Fatal("ended winner ACK replay published another ANSWERED event")
	}
	t.Logf("group=%s incarnation=%s originalEffect=%s attempt=%s oldStatus=%s ACK=%s", after.Ref.ID, after.Ref.Incarnation, old.ID, old.AttemptID, old.Status, ack.CSeq().Value())
}

// Optional ignored artifacts retain complete fixture wire/control traffic plus
// exact committed records; the fixture never uses production credentials/media.
func (h *processHarness) capture() {
	directory := os.Getenv("SIPBRIDGE_PROCESS_EVIDENCE_DIR")
	if directory == "" {
		return
	}
	h.mu.Lock()
	refs := make(map[string]*pb.ContextRef, len(h.refs))
	for id, ref := range h.refs {
		refs[id] = proto.Clone(ref).(*pb.ContextRef)
	}
	observations := append([]processObservation(nil), h.observations...)
	phones := append([]*processPhone(nil), h.phones...)
	phases := append([]processPhaseObservation(nil), h.phases...)
	h.mu.Unlock()
	type controlRecord struct {
		Worker   string
		Envelope json.RawMessage
	}
	type phoneRecord struct{ Sent, Received []string }
	evidence := struct {
		Test, BinarySHA256 string
		Refs               map[string]*pb.ContextRef
		Records            map[string]state.Record
		Control            []controlRecord
		Phones             []phoneRecord
		Phases             []processPhaseObservation
	}{Test: h.t.Name(), Refs: refs, Records: make(map[string]state.Record), Phases: phases}
	if binary, err := os.ReadFile(os.Args[0]); err == nil {
		sum := sha256.Sum256(binary)
		evidence.BinarySHA256 = hex.EncodeToString(sum[:])
	}
	for id, ref := range refs {
		r := state.Ref{ID: ref.State.GroupId, Incarnation: ref.State.Incarnation, CallID: id, Partition: uint16(ref.State.Partition)}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		record, err := h.store.Load(ctx, r)
		cancel()
		if err == nil {
			evidence.Records[id] = record
		}
	}
	for _, observed := range observations {
		wire, err := protojson.Marshal(observed.Envelope)
		if err == nil {
			evidence.Control = append(evidence.Control, controlRecord{observed.Worker, wire})
		}
	}
	for _, phone := range phones {
		entry := phoneRecord{}
		for _, wire := range phone.sent {
			entry.Sent = append(entry.Sent, string(wire))
		}
		for _, message := range phone.received {
			entry.Received = append(entry.Received, message.String())
		}
		evidence.Phones = append(evidence.Phones, entry)
	}
	if err := os.MkdirAll(directory, 0700); err != nil {
		h.t.Errorf("process evidence directory: %v", err)
		return
	}
	data, err := json.MarshalIndent(evidence, "", "  ")
	if err != nil {
		h.t.Errorf("process evidence encode: %v", err)
		return
	}
	path := filepath.Join(directory, strings.ReplaceAll(h.t.Name(), "/", "_")+".json")
	if err := os.WriteFile(path, append(data, '\n'), 0600); err != nil {
		h.t.Errorf("process evidence write: %v", err)
		return
	}
	h.t.Logf("process/wire evidence: %s", path)
}
