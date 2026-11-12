package engine

// These helpers run only in an explicitly spawned test subprocess. No product
// endpoint or runtime fault hook is compiled into the sipbridge service.
import (
	"bufio"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/emiago/sipgo/sip"
	"sipbridge/internal/control"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/execution"
	executionfixture "sipbridge/internal/execution/fixture"
	"sipbridge/internal/state"
)

type stateProcessConfig struct {
	Store               state.Config   `json:"store"`
	Process             *pb.ProcessRef `json:"process"`
	Certificate, Key    string
	PreferredController string
	PlainSIP            bool
	Execution           string
}

type stateProcessEvent struct {
	Kind, ID, Stage, Error, Group, Token string
	SIP, Control                         string
	PlainTCP, PlainUDP                   string
	ClaimUntil                           int64
	AtMS, DeadlineMS                     int64
	PendingKind, PendingID               string
}

type stateProcessRule struct {
	ID, Stage, CallID, Method string
	InDialog                  bool
	Class                     pb.EffectAuthorization_Class
}

type stateProcessCommand struct {
	Op   string
	Rule stateProcessRule
}

type stateProcessBarrier struct {
	mu           sync.Mutex
	rule         stateProcessRule
	entered      bool
	group        string
	pending      string
	effect       string
	release      chan struct{}
	emit         func(stateProcessEvent)
	busyObserved map[string]bool
}

func (b *stateProcessBarrier) command(cmd stateProcessCommand) {
	b.mu.Lock()
	if cmd.Op == "release" {
		if b.release != nil {
			close(b.release)
			b.release = nil
		}
		b.entered = false
		b.rule = stateProcessRule{}
	} else if cmd.Op == "arm" {
		b.rule, b.entered, b.group, b.pending = cmd.Rule, false, "", ""
		b.release = make(chan struct{})
	}
	b.mu.Unlock()
	b.emit(stateProcessEvent{Kind: cmd.Op, ID: cmd.Rule.ID})
}

func (b *stateProcessBarrier) blocked(ref state.Ref) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.entered && b.rule.Stage == "begin" && b.group == ref.ID
}

func (b *stateProcessBarrier) blockedInput(ref state.Ref, id string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.entered && b.rule.Stage == "begin" && b.group == ref.ID && b.pending == id
}

func (b *stateProcessBarrier) matchBegin(step *state.Step) {
	var input stateEngineInput
	if unmarshalRetainedFixture(step.Input.Payload, &input) != nil {
		return
	}
	var wire []byte
	if input.Ingest != nil {
		wire = input.Ingest.Wire
	} else if input.Kind == "native-ingest" && input.NativeCompletion && input.Message != nil {
		// Single-read completion has not selected a context at this read barrier.
		wire = input.Message.Wire
	} else {
		return
	}
	m, err := sip.ParseMessage(wire)
	if err != nil {
		return
	}
	b.mu.Lock()
	rule := b.rule
	if b.entered || rule.Stage != "begin" || rule.CallID != m.CallID().Value() || rule.Method != string(m.CSeq().MethodName) || rule.InDialog && tag(m.To().Params) == "" {
		b.mu.Unlock()
		return
	}
	b.entered, b.group = true, step.Record.Ref.ID
	b.pending = step.Input.ID
	release := b.release
	b.mu.Unlock()
	b.emit(stateProcessEvent{Kind: "barrier", ID: rule.ID, Stage: rule.Stage, Group: step.Record.Ref.ID, Token: step.Claim.Token, ClaimUntil: step.Claim.UntilMS})
	<-release
}

func (b *stateProcessBarrier) firstBusy(id string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.busyObserved == nil {
		b.busyObserved = make(map[string]bool)
	}
	if b.busyObserved[id] {
		return false
	}
	b.busyObserved[id] = true
	return true
}

type stateProcessStore struct {
	*state.Store
	barrier *stateProcessBarrier
}

func (s *stateProcessStore) ReadUpdate(ctx context.Context, ref state.Ref, input state.Input) (state.UpdateResult, error) {
	result, err := s.Store.ReadUpdate(ctx, ref, input)
	if err == nil && result.Update != nil {
		// Pause the actual native CAS admission before reduction/commit. Unlike
		// historical Begin, this read owns no durable claim or pending input.
		s.barrier.matchBegin(&state.Step{Record: result.Update.Record, Input: result.Update.Input})
	}
	return result, err
}

func (s *stateProcessStore) Begin(ctx context.Context, ref state.Ref, worker string, input state.Input) (state.BeginResult, error) {
	if s.barrier.blocked(ref) && input.Kind != "reserve" && input.Kind != "complete" {
		blocked, err := s.blockedOriginalInput(ctx, ref)
		if err != nil {
			return state.BeginResult{}, err
		}
		if blocked {
			return state.BeginResult{}, state.ErrBusy
		}
	}
	result, err := s.Store.Begin(ctx, ref, worker, input)
	if errors.Is(err, state.ErrBusy) && input.Kind == "reserve" && s.barrier.firstBusy(input.ID) {
		var admitted stateEngineInput
		_ = unmarshalRetainedFixture(input.Payload, &admitted)
		event := stateProcessEvent{Kind: "reserve-busy", Stage: "begin", Group: ref.ID, Token: admitted.Effect}
		if current, loadErr := s.Store.Load(ctx, ref); loadErr == nil && current.Pending != nil {
			event.PendingKind, event.PendingID = current.Pending.Kind, current.Pending.ID
		} else if loadErr != nil {
			event.Error = loadErr.Error()
		}
		s.barrier.emit(event)
	}
	if err == nil && result.Step != nil {
		s.barrier.matchBegin(result.Step)
	}
	return result, err
}
func (s *stateProcessStore) Resume(ctx context.Context, ref state.Ref, worker string) (state.BeginResult, error) {
	if s.barrier.blocked(ref) {
		blocked, err := s.blockedOriginalInput(ctx, ref)
		if err != nil {
			return state.BeginResult{}, err
		}
		if blocked {
			return state.BeginResult{}, state.ErrIdle
		}
	}
	return s.Store.Resume(ctx, ref, worker)
}

// Pause only the abandoned input. Once another executor commits it, this native
// process must still run successor timers and its own exact-flow delivery work.
func (s *stateProcessStore) blockedOriginalInput(ctx context.Context, ref state.Ref) (bool, error) {
	record, err := s.Store.Load(ctx, ref)
	if err != nil {
		return false, err
	}
	return record.Pending != nil && s.barrier.blockedInput(ref, record.Pending.ID), nil
}
func (s *stateProcessStore) Commit(ctx context.Context, step state.Step, next state.Payload, outcome []byte) (state.Receipt, error) {
	receipt, err := s.Store.Commit(ctx, step, next, outcome)
	if err == nil {
		s.observeDigestCommit(step.Record.Ref, step.Input)
	}
	if errors.Is(err, state.ErrStale) {
		s.barrier.emit(stateProcessEvent{Kind: "stale", Group: step.Record.Ref.ID, Token: step.Claim.Token})
	}
	return receipt, err
}

func (s *stateProcessStore) CompareAndSwap(ctx context.Context, update state.Update, next state.Payload, outcome []byte) (state.Receipt, error) {
	receipt, err := s.Store.CompareAndSwap(ctx, update, next, outcome)
	if err == nil {
		s.observeDigestCommit(update.Record.Ref, update.Input)
	}
	return receipt, err
}

func (s *stateProcessStore) observeDigestCommit(ref state.Ref, input state.Input) {
	if input.Kind != "digest-result" {
		return
	}
	var admitted stateEngineInput
	if unmarshalRetainedFixture(input.Payload, &admitted) == nil {
		// Observe the real commit's immutable deadline without emitting SIP or
		// credentials. Both drivers use this same metadata-only test barrier.
		s.barrier.emit(stateProcessEvent{Kind: "digest-commit", ID: admitted.Effect, Stage: "digest-result", Group: ref.ID, DeadlineMS: input.DeadlineMS})
	}
}

type stateProcessWriter struct {
	*StateEngine
	barrier *stateProcessBarrier
}

func (w *stateProcessWriter) Reserve(ctx context.Context, write *pb.FlowWrite) (*CommittedWrite, error) {
	w.barrier.emit(stateProcessEvent{Kind: "reserve-start", Stage: "reserve", Token: write.GetEffect().GetId(), Group: write.GetEffect().GetContext().GetState().GetGroupId(), DeadlineMS: int64(write.GetPermit().GetAuthorization().GetDeadlineMs())})
	if err := w.waitWrite(ctx, write, "no-start"); err != nil {
		return nil, err
	}
	result, err := w.StateEngine.Reserve(ctx, write)
	if err != nil {
		w.barrier.emit(stateProcessEvent{Kind: "reserve-error", Stage: "reserve", Token: write.GetEffect().GetId(), Error: err.Error(), Group: write.GetEffect().GetContext().GetState().GetGroupId(), DeadlineMS: int64(write.GetPermit().GetAuthorization().GetDeadlineMs())})
	}
	if err == nil && result != nil && !result.Known && len(result.Wire) > 0 {
		err = w.waitWrite(ctx, write, "reserve")
	}
	return result, err
}
func (w *stateProcessWriter) Complete(ctx context.Context, write *pb.FlowWrite, result *pb.DeliveryResult) error {
	if err := w.waitWrite(ctx, write, "complete"); err != nil {
		return err
	}
	return w.StateEngine.Complete(ctx, write, result)
}
func (w *stateProcessWriter) waitWrite(ctx context.Context, write *pb.FlowWrite, stage string) error {
	w.barrier.mu.Lock()
	rule := w.barrier.rule
	if rule.Stage == stage && w.barrier.entered && w.barrier.effect == write.Effect.Id {
		release := w.barrier.release
		w.barrier.mu.Unlock()
		<-release
		return nil
	}
	armed := rule.Stage == stage && !w.barrier.entered
	w.barrier.mu.Unlock()
	if armed {
		ref, err := w.group(write.Effect.Context)
		if err != nil {
			return err
		}
		record, err := w.store.Load(ctx, ref)
		if err != nil {
			return err
		}
		for _, effect := range record.Payload.Effects {
			var body stateEngineEffect
			if effect.ID != write.Effect.Id || unmarshalRetainedFixture(effect.Payload, &body) != nil || body.Authorization == nil || body.Authorization.AuthorityClass != rule.Class || body.WireMethod != rule.Method || ref.CallID != rule.CallID && rule.CallID != "" {
				continue
			}
			w.barrier.mu.Lock()
			if w.barrier.entered {
				release, same := w.barrier.release, w.barrier.effect == write.Effect.Id
				w.barrier.mu.Unlock()
				if same {
					<-release
				}
				break
			}
			w.barrier.entered = true
			w.barrier.effect = write.Effect.Id
			release := w.barrier.release
			w.barrier.mu.Unlock()
			w.barrier.emit(stateProcessEvent{Kind: "barrier", ID: rule.ID, Stage: rule.Stage, Group: ref.ID, Token: effect.ID})
			if stage == "no-start" {
				// The real physical writer already installed its exclusive memo,
				// but neither native Reserve nor any socket syscall has run.
				return errors.New("fixture proven no-start before native Reserve")
			}
			<-release
			break
		}
	}
	return nil
}

func TestStateProcessHelper(t *testing.T) {
	raw := os.Getenv("SIPBRIDGE_PROCESS_HELPER")
	if raw == "" {
		return
	}
	var cfg stateProcessConfig
	if err := unmarshalRetainedFixture([]byte(raw), &cfg); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store, err := state.Open(ctx, cfg.Store)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.Ready(ctx); err != nil {
		t.Fatal(err)
	}
	var output sync.Mutex
	emit := func(event stateProcessEvent) {
		event.AtMS = time.Now().UnixMilli()
		data, _ := marshalRetainedFixture(event)
		output.Lock()
		defer output.Unlock()
		fmt.Println("STATE-PROCESS " + string(data))
	}
	barrier := &stateProcessBarrier{emit: emit}
	material, err := control.NewTLSContext(control.TLSFiles{Certificate: cfg.Certificate, Key: cfg.Key, Trust: cfg.Certificate})
	if err != nil {
		t.Fatal(err)
	}
	keyPair, err := tls.LoadX509KeyPair(cfg.Certificate, cfg.Key)
	if err != nil {
		t.Fatal(err)
	}
	controlListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer controlListener.Close()
	sipListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer sipListener.Close()
	var plainTCP net.Listener
	var plainUDP net.PacketConn
	plainAdvertisement := ""
	if cfg.PlainSIP {
		plainTCP, err = net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		defer plainTCP.Close()
		plainUDP, err = net.ListenPacket("udp", plainTCP.Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		defer plainUDP.Close()
		plainAdvertisement = plainTCP.Addr().String()
	}
	var native *StateEngine
	var ingress *Ingress
	a := store.Authority()
	schema, err := strconv.ParseUint(cfg.Store.Versions.Schema, 10, 32)
	if err != nil {
		t.Fatal(err)
	}
	autonomous := cfg.Store.Versions == NativeStateVersions()
	serverConfig := control.Config{Process: cfg.Process, TLS: material, AllowedURIs: []string{"spiffe://test/state-process"}, Capabilities: stateProcessCapabilities(autonomous), StateAuthority: &pb.StateAuthority{Service: store.Namespace(), Generation: a.Generation, PrimaryId: a.RunID, SchemaVersion: uint32(schema)}, OnStatus: func(peer *control.Peer, status *pb.SessionStatus) error {
		if status.RetainedReady && status.BindingsReady {
			return native.ControllerReady(peer.Context(), peer)
		}
		return nil
	}, OnMessage: func(peer *control.Peer, envelope *pb.Envelope) error {
		if envelope.GetWrite() != nil {
			return ingress.Write(peer, envelope)
		}
		if envelope.GetInspectEffect() != nil {
			return ingress.InspectEffect(peer, envelope)
		}
		return native.Handle(peer, envelope)
	}}
	if autonomous {
		serverConfig.Capabilities = append(serverConfig.Capabilities, control.ControllerReattachCapability)
		serverConfig.OnControllerSession = func(ctx context.Context, peer *control.Peer, hello *pb.SessionHello) (*pb.ApplicationLease, error) {
			return native.ControllerSession(ctx, peer, hello)
		}
		serverConfig.OnControllerRenew = func(ctx context.Context, peer *control.Peer) (*pb.ApplicationLease, error) {
			return native.ControllerRenew(ctx, peer)
		}
		serverConfig.OnApplicationLease = func(ctx context.Context, application *pb.ProcessRef, term string) (*pb.ApplicationLease, error) {
			return native.ApplicationLease(ctx, application, term)
		}
	}
	server, err := control.NewServer(serverConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	native, err = newStateForFixture(ctx, Config{Process: cfg.Process, StateIdentityKey: cfg.Store.NamespaceKey, AdvertiseAddress: sipListener.Addr().String(), PlainAdvertiseAddress: plainAdvertisement, ContactURI: "sip:bridge@" + sipListener.Addr().String() + ";transport=tls", OpenNetwork: func(ctx context.Context, uri string, target *pb.NetworkTarget) (*pb.FlowRef, error) {
		return ingress.OpenNetwork(ctx, uri, target)
	}, LiveFlow: func(flow *pb.FlowRef) bool { return ingress.LiveFlow(flow) },
		WriteCommitted: func(ctx context.Context, write *pb.FlowWrite) *pb.DeliveryResult {
			return ingress.WriteCommitted(ctx, write)
		},
		InspectCommitted: func(ctx context.Context, write *pb.FlowWrite) *pb.DeliveryResult {
			return ingress.InspectCommitted(ctx, write)
		}}, store, server, cfg.Execution)
	if err != nil {
		t.Fatal(err)
	}
	defer native.Close()
	// Replacing the adapter is synchronized before listeners/control are exposed;
	// the new runtime is the production reducer/driver with a test-only barrier.
	native.lifecycleMu.Lock()
	if autonomous || cfg.Execution == "cas" {
		native.runtime, err = execution.NewCAS(&stateProcessStore{Store: store, barrier: barrier}, execution.ReducerFunc(native.reduce), 64)
	} else {
		native.runtime, err = executionfixture.New(&stateProcessStore{Store: store, barrier: barrier}, execution.ReducerFunc(native.reduce), cfg.Process.InstanceId, 64)
	}
	native.lifecycleMu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	ingress = NewIngress(cfg.Process, server, sipListener.Addr().String(), 64, IngressOptions{PreferredController: cfg.PreferredController})
	locate := func(ctx context.Context, message *pb.SipMessage) (*pb.ContextSnapshot, error) {
		ref, err := native.LocateRoute(ctx, message)
		if err != nil {
			emit(stateProcessEvent{Kind: "locate-error", Error: err.Error()})
		}
		return ref, err
	}
	reject := func(ctx context.Context, flow *pb.FlowRef, message *pb.SipMessage, status uint32) error {
		err := native.Reject(ctx, flow, message, status)
		if err != nil {
			emit(stateProcessEvent{Kind: "rejection-error", Error: err.Error()})
		}
		return err
	}
	if err = ingress.ConfigureStateRuntime(&stateProcessWriter{StateEngine: native, barrier: barrier}, locate, reject); err != nil {
		t.Fatal(err)
	}
	if autonomous {
		ingress.ConfigureNativeCompletion(native.TryNativeCompletion)
	}
	go func() {
		if err := server.Serve(ctx, controlListener); err != nil {
			emit(stateProcessEvent{Kind: "error", Error: err.Error()})
		}
	}()
	go func() {
		if err := ingress.Serve(ctx, sipListener, &tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{keyPair}}); err != nil {
			emit(stateProcessEvent{Kind: "error", Error: err.Error()})
		}
	}()
	ready := stateProcessEvent{Kind: "ready", SIP: sipListener.Addr().String(), Control: controlListener.Addr().String()}
	if cfg.PlainSIP {
		ready.PlainTCP, ready.PlainUDP = plainTCP.Addr().String(), plainUDP.LocalAddr().String()
		go func() {
			if err := ingress.Serve(ctx, plainTCP, nil); err != nil {
				emit(stateProcessEvent{Kind: "error", Error: err.Error()})
			}
		}()
		go func() {
			if err := ingress.ServeUDP(ctx, plainUDP); err != nil {
				emit(stateProcessEvent{Kind: "error", Error: err.Error()})
			}
		}()
	}
	emit(ready)
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		var cmd stateProcessCommand
		if err := unmarshalRetainedFixture(scanner.Bytes(), &cmd); err != nil {
			t.Fatal(err)
		}
		barrier.command(cmd)
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
}

func stateProcessCapabilities(autonomous bool) []string {
	capabilities := []string{Capability, control.CommandBudgetCapability, DialogContinuityCapability, control.StateCapability}
	if autonomous {
		capabilities = append(capabilities, control.NativeCompletionCapability)
	}
	return capabilities
}
