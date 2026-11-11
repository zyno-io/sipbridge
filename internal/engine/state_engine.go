package engine

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/emiago/sipgo/sip/portable"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
	"sipbridge/internal/control"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/execution"
	"sipbridge/internal/state"
)

const stateEffectRecoveryMS = int64(1000)

// StateEngine retains execution resources only. Every context/transaction value
// is decoded for one transition, committed, and discarded.
type StateEngine struct {
	cfg            Config
	store          state.Backend
	control        *control.Server
	runtime        execution.Driver
	ctx            context.Context
	cancel         context.CancelFunc
	work           chan struct{}
	background     chan struct{}
	scheduled      map[string]bool
	physicalGroups map[string]bool
	wg             sync.WaitGroup
	lifecycleMu    sync.Mutex
	closed         bool
	dispatchMu     sync.Mutex
	dispatching    map[string]bool
	waiters        map[string]map[chan struct{}]struct{}
	scheduler      schedulerStatistics
}

type stateEngineInput struct {
	Application           *pb.ProcessRef     `json:"application,omitempty"`
	ApplicationGeneration uint64             `json:"applicationGeneration,omitempty"`
	ApplicationLeaseTerm  string             `json:"applicationLeaseTerm,omitempty"`
	Command               *pb.Command        `json:"command,omitempty"`
	Context               *pb.ContextRef     `json:"context,omitempty"`
	Delivery              *pb.DeliveryResult `json:"delivery,omitempty"`
	Effect                string             `json:"effect,omitempty"`
	Executor              *pb.ProcessRef     `json:"executor,omitempty"`
	Failure               string             `json:"failure,omitempty"`
	Flow                  *pb.FlowRef        `json:"flow,omitempty"`
	Generation            string             `json:"generation,omitempty"`
	Ingest                *pb.OwnerIngest    `json:"ingest,omitempty"`
	Kind                  string             `json:"kind"`
	Message               *pb.SipMessage     `json:"message,omitempty"`
	NativeCompletion      bool               `json:"nativeCompletion,omitempty"`
	Operation             string             `json:"operation,omitempty"`
	Reply                 *pb.Envelope       `json:"reply,omitempty"`
	Status                uint32             `json:"status,omitempty"`
	Timer                 string             `json:"timer,omitempty"`
	Write                 *pb.FlowWrite      `json:"write,omitempty"`
}

type stateEngineEffect struct {
	AfterACK      string                  `json:"afterACK,omitempty"`
	Application   *pb.ProcessRef          `json:"application,omitempty"`
	Authorization *pb.EffectAuthorization `json:"authorization,omitempty"`
	Cancelled     bool                    `json:"cancelled,omitempty"`
	Command       string                  `json:"command,omitempty"`
	Context       string                  `json:"context"`
	Dialog        string                  `json:"dialog,omitempty"`
	Envelope      *pb.Envelope            `json:"envelope,omitempty"`
	Executor      *pb.ProcessRef          `json:"executor,omitempty"`
	Flow          *pb.FlowRef             `json:"flow,omitempty"`
	Native        bool                    `json:"native,omitempty"`
	Permit        *pb.SendPermit          `json:"permit,omitempty"`
	Policy        *statePolicyAdmission   `json:"policy,omitempty"` // Initial transport setup only, until its TX exists.
	Rejection     bool                    `json:"rejection,omitempty"`
	Settled       bool                    `json:"settled,omitempty"`
	Transaction   string                  `json:"transaction,omitempty"`
	Wire          []byte                  `json:"wire,omitempty"`
	WireHash      string                  `json:"wireHash,omitempty"`
	WireMethod    string                  `json:"wireMethod,omitempty"`
	WireSequence  uint32                  `json:"wireSequence,omitempty"`
	WireStatus    uint32                  `json:"wireStatus,omitempty"`
}

func NewState(ctx context.Context, cfg Config, store state.Backend, server *control.Server) (*StateEngine, error) {
	if cfg.Process == nil || cfg.AdvertiseAddress == "" || cfg.ContactURI == "" || len(cfg.StateIdentityKey) < 32 || store == nil || server == nil {
		return nil, errors.New("engine identity, store, key and control required")
	}
	if store.Versions() != NativeStateVersions() {
		return nil, errors.New("unsupported SIP state profile: require 1/1/1")
	}
	validation := &StateEngine{cfg: cfg}
	for _, transport := range []string{"tls", "udp", "tcp"} {
		if _, _, err := validation.advertisement(transport); err != nil {
			return nil, err
		}
	}
	ctx, cancel := context.WithCancel(ctx)
	e := &StateEngine{cfg: cfg, store: store, control: server, ctx: ctx, cancel: cancel, work: make(chan struct{}, 64), background: make(chan struct{}, 48), scheduled: make(map[string]bool), dispatching: make(map[string]bool)}
	e.cfg.StateIdentityKey = append([]byte(nil), cfg.StateIdentityKey...)
	var err error
	e.runtime, err = execution.NewCAS(store, execution.ReducerFunc(e.reduce), 64)
	if err != nil {
		cancel()
		return nil, err
	}
	e.wg.Add(1)
	go func() { defer e.wg.Done(); e.schedule() }()
	return e, nil
}

// Hand-built legacy reducer fixtures have no computation runtime.
func (e *StateEngine) usesCAS() bool { return e.runtime != nil && e.runtime.UsesCAS() }

func (e *StateEngine) Close() {
	e.lifecycleMu.Lock()
	e.closed = true
	e.cancel()
	e.lifecycleMu.Unlock()
	e.wg.Wait()
}
func (e *StateEngine) StateAvailable() bool { return e.store != nil && e.store.Available() }
func (e *StateEngine) Ready() error         { return e.store.Ready(e.ctx) }
func (e *StateEngine) StateAuthority() *pb.StateAuthority {
	v, _ := strconv.ParseUint(e.store.Versions().Schema, 10, 32)
	a := e.store.Authority()
	return &pb.StateAuthority{Service: e.store.Namespace(), Generation: a.Generation, PrimaryId: a.RunID, SchemaVersion: uint32(v)}
}
func (e *StateEngine) id(parts ...string) string {
	h := hmac.New(sha256.New, e.cfg.StateIdentityKey)
	for _, p := range parts {
		fmt.Fprintf(h, "%d:", len(p))
		h.Write([]byte(p))
	}
	return hex.EncodeToString(h.Sum(nil))
}
func (e *StateEngine) contextRef(ref state.Ref, id string) *pb.ContextRef {
	return &pb.ContextRef{Owner: proto.Clone(e.cfg.Process).(*pb.ProcessRef), Id: id, State: &pb.StateRef{Authority: e.StateAuthority(), GroupId: ref.ID, Incarnation: ref.Incarnation, CallId: ref.CallID, Partition: uint32(ref.Partition)}}
}
func (e *StateEngine) group(ref *pb.ContextRef) (state.Ref, error) {
	if ref == nil || ref.State == nil || ref.Id == "" || !proto.Equal(ref.State.Authority, e.StateAuthority()) || ref.State.Partition >= uint32(e.store.PartitionCount()) {
		return state.Ref{}, state.ErrAuthority
	}
	r := state.Ref{ID: ref.State.GroupId, Partition: uint16(ref.State.Partition), CallID: ref.State.CallId, Incarnation: ref.State.Incarnation}
	expected, err := e.store.GroupForCallID(r.CallID)
	if err != nil || expected.ID != r.ID || expected.Partition != r.Partition || r.Incarnation == "" {
		return state.Ref{}, state.ErrConflict
	}
	return r, nil
}

func (e *StateEngine) apply(ctx context.Context, ref state.Ref, id string, in stateEngineInput, deadline time.Time, create bool) (*pb.OperationResult, error) {
	result, _, err := e.applyCommitted(ctx, ref, id, in, deadline, create)
	return result, err
}

func (e *StateEngine) applyCommitted(ctx context.Context, ref state.Ref, id string, in stateEngineInput, deadline time.Time, create bool) (*pb.OperationResult, *execution.Committed, error) {
	if admission := in.Command.GetAdmission(); admission != nil && admission.ExpiresAtMs > 0 && admission.ExpiresAtMs <= 1<<53-1 {
		semantic := time.UnixMilli(int64(admission.ExpiresAtMs))
		if semantic.Before(deadline) {
			deadline = semantic
		}
	}
	if err := stateValidateRetainedInput(in); err != nil {
		return nil, nil, err
	}
	data, err := in.MarshalBinary()
	if err != nil {
		return nil, nil, err
	}
	sum := sha256.Sum256(data)
	input := state.Input{ID: id, Kind: in.Kind, Fingerprint: hex.EncodeToString(sum[:]), Payload: data, DeadlineMS: deadline.UnixMilli(), ReceiptUntilMS: deadline.Add(2 * time.Minute).UnixMilli(), Create: create}
	if in.Application != nil && in.ApplicationLeaseTerm != "" {
		input.ApplicationLeaseIDs = []string{e.applicationLeaseID(in.Application)}
	}
	if in.Kind == "locate" && in.Message != nil && e.autonomousCompletion() {
		input.Proof, input.ReceiptUntilMS = true, input.DeadlineMS
	}
	switch in.Kind {
	case "effect-start", "effect-result", "owner-result", "owner-control", "digest-result", "reserve", "complete", "timer", "reject", "native-ingest":
		input.Proof = true
		input.ReceiptUntilMS = input.DeadlineMS
	}
	if in.Kind == "ingest" && in.Ingest != nil {
		if message, err := Observe(in.Ingest.Wire, false); err == nil && (message.Status > 0 || message.Method == "ACK" || message.Method == "CANCEL" || message.Method == "BYE") {
			input.Proof = true
			input.ReceiptUntilMS = input.DeadlineMS
		}
	}

	if in.Kind == "command" && in.Command != nil && (in.Command.GetCancel() != nil || in.Command.GetDialogRequest().GetMethod() == "BYE") {
		input.Proof = true
		input.ReceiptUntilMS = input.DeadlineMS
	}
	// Control remaining-budget values are relative. A retry inspects the original
	// immutable admission rather than regenerating its absolute receipt window.
	if !e.usesCAS() {
		if record, loadErr := e.store.Load(ctx, ref); loadErr == nil {
			receipts := append([]state.Receipt(nil), record.Receipts...)
			if record.Proof != nil {
				receipts = append(receipts, *record.Proof)
			}
			for _, prior := range receipts {
				if prior.OperationID == id && prior.Fingerprint == input.Fingerprint && prior.ExpiresMS > time.Now().UnixMilli() {
					var result pb.OperationResult
					if json.Unmarshal(prior.Outcome, &result) != nil {
						return nil, nil, state.ErrCorrupt
					}
					return &result, nil, nil
				}
			}
			if record.Pending != nil && record.Pending.ID == id && record.Pending.Fingerprint == input.Fingerprint {
				input = *record.Pending
			}
		} else if !errors.Is(loadErr, state.ErrNotFound) {
			// No mutation was attempted. Preserve positive read unavailability so
			// ingress can refuse the request instead of manufacturing ErrUnknown
			// by attempting Begin against an already unreadable store.
			return nil, nil, classifyStateReadFailure(loadErr)
		}
	}
	applied, err := e.runtime.ApplyCommitted(ctx, ref, input)
	for e.usesCAS() && errors.Is(err, errStateControlPreflight) {
		// The validated read proved no computation was committed. Drop that
		// observation before external recovery, then retry the same immutable
		// input with a fresh read and its original deadline.
		preflightContext, stop := context.WithDeadline(ctx, deadline)
		started := time.Now()
		preflightErr := e.preflightControl(preflightContext, ref, in, deadline)
		stop()
		if timing := execution.RequestTiming(ctx); timing != nil {
			timing.Preflight += time.Since(started)
		}
		if preflightErr != nil {
			return nil, nil, classifyStateReadFailure(errors.Join(execution.ErrUncommitted, preflightErr))
		}
		applied, err = e.runtime.ApplyCommitted(ctx, ref, input)
	}
	receipt := applied.Receipt
	if !e.usesCAS() && errors.Is(err, state.ErrConflict) {
		// Concurrent equal callers may race before either can observe the first
		// admission's relative control budget. Reuse only a proven exact input.
		if current, loadErr := e.store.Load(ctx, ref); loadErr == nil {
			if current.Pending != nil && current.Pending.ID == input.ID && current.Pending.Fingerprint == input.Fingerprint {
				receipt, err = e.runtime.Apply(ctx, ref, *current.Pending)
			} else {
				receipts := append([]state.Receipt(nil), current.Receipts...)
				if current.Proof != nil {
					receipts = append(receipts, *current.Proof)
				}
				for _, prior := range receipts {
					if prior.OperationID == input.ID && prior.Fingerprint == input.Fingerprint {
						receipt = prior
						err = nil
						break
					}
				}
			}
		}
	}
	if err != nil {
		if e.usesCAS() && errors.Is(err, execution.ErrUncommitted) {
			if in.Command.GetAdmission() != nil && (errors.Is(err, state.ErrExpired) || errors.Is(err, context.DeadlineExceeded)) {
				return &pb.OperationResult{Certainty: pb.OperationResult_NOT_ACCEPTED, SipStatus: 503, Error: "command policy deadline elapsed"}, nil, nil
			}
			return nil, nil, classifyStateReadFailure(err)
		}
		return nil, nil, errors.Join(state.ErrUnknown, err)
	}
	var result pb.OperationResult
	if len(receipt.Outcome) > 0 && json.Unmarshal(receipt.Outcome, &result) != nil {
		return nil, nil, errors.Join(state.ErrUnknown, state.ErrCorrupt)
	}
	if in.Kind == "complete" || in.Kind == "ingest" || in.Kind == "native-ingest" {
		e.signalOperation(ref)
	}
	return &result, applied.Committed, nil
}

// Locate supplies process-independent directory identity; it performs no SIP or
// application side effect. The actual ingest transition is admitted separately.
func (e *StateEngine) Locate(ctx context.Context, message *pb.SipMessage) (*pb.ContextRef, error) {
	snapshot, err := e.LocateRoute(ctx, message)
	if err != nil {
		return nil, err
	}
	return snapshot.Context, nil
}

// LocateRoute keeps only the already-returned routing snapshot available to
// ingress. It grants no mutation authority and creates no second stored value.
func (e *StateEngine) LocateRoute(ctx context.Context, message *pb.SipMessage) (*pb.ContextSnapshot, error) {
	if message == nil {
		return nil, errors.New("SIP message required")
	}
	observed, err := Observe(message.Wire, false)
	if err != nil {
		return nil, err
	}
	message = observed
	callID, err := stateMessageCallID(message.Wire)
	if err != nil {
		return nil, err
	}
	ref, err := e.store.GroupForCallID(callID)
	if err != nil {
		return nil, err
	}
	if e.autonomousCompletion() {
		to, from := message.Addresses["to"].GetValues(), message.Addresses["from"].GetValues()
		if message.Status == 0 && len(to) > 0 && len(from) > 0 && to[0].GetParams()["tag"] != "" && from[0].GetParams()["tag"] != "" {
			snapshot, err := e.locateExisting(ctx, ref, message)
			if err != nil || snapshot != nil {
				return snapshot, err
			}
		}
		result, err := e.apply(ctx, ref, uuid.NewString(), stateEngineInput{Kind: "locate", Message: message}, time.Now().Add(2*time.Second), true)
		if err != nil {
			return nil, err
		}
		if result.Snapshot == nil || result.Snapshot.Context == nil || result.Error != "" {
			status := result.SipStatus
			if status < 300 || status > 699 {
				status = 503
			}
			return nil, rejectIngest(status, "native context unavailable")
		}
		return result.Snapshot, nil
	}
	record, err := e.store.Load(ctx, ref)
	possibleRecovery := false
	if errors.Is(err, state.ErrNotFound) {
		possibleRecovery = true
		deadline := time.Now().Add(2 * time.Second)
		if _, err = e.apply(ctx, ref, uuid.NewString(), stateEngineInput{Kind: "locate"}, deadline, true); err != nil {
			return nil, err
		}
		record, err = e.store.Load(ctx, ref)
	}
	if err != nil {
		if possibleRecovery {
			return nil, errors.Join(state.ErrUnknown, err)
		}
		return nil, classifyStateReadFailure(err)
	}
	group, err := decodeStateGroup(record)
	if err != nil {
		if possibleRecovery {
			return nil, errors.Join(state.ErrUnknown, err)
		}
		return nil, classifyStateReadFailure(err)
	}
	for _, selector := range message.Selectors {
		if id := group.Selectors[selector]; id != "" {
			return &pb.ContextSnapshot{Context: e.contextRef(record.Ref, id)}, nil
		}
	}
	return &pb.ContextSnapshot{Context: e.contextRef(record.Ref, e.id("context", callID, fmt.Sprint(message.Selectors))[:48])}, nil
}

// locateExisting returns only a fresh routing hint; ingest must validate again.
func (e *StateEngine) locateExisting(ctx context.Context, ref state.Ref, message *pb.SipMessage) (*pb.ContextSnapshot, error) {
	e.lifecycleMu.Lock()
	if e.closed {
		e.lifecycleMu.Unlock()
		return nil, errors.New("native state engine closed")
	}
	select {
	case e.work <- struct{}{}:
	default:
		e.lifecycleMu.Unlock()
		return nil, nil
	}
	e.wg.Add(1)
	e.lifecycleMu.Unlock()
	defer e.wg.Done()
	defer func() { <-e.work }()
	view, now, err := e.store.ReadState(ctx, ref)
	if errors.Is(err, state.ErrNotFound) || err == nil && view.HasPending {
		return nil, nil
	}
	if err != nil {
		return nil, classifyStateReadFailure(err)
	}
	record := stateViewProtocolRecord(view)
	group, err := decodeStateGroup(record)
	if err != nil {
		return nil, classifyStateReadFailure(err)
	}
	r := &stateReduction{engine: e, record: record, group: group, payload: record.Payload, now: now}
	r.pruneTransactions()
	if err := r.refreshSelectors(); err != nil {
		return nil, err
	}
	for _, selector := range message.Selectors {
		c := r.group.Contexts[r.group.Selectors[selector]]
		if c == nil {
			continue
		}
		if c.Rejection || c.OwnerLost || c.Snapshot.Ended {
			return nil, nil
		}
		if err := stateRetiredInvite(c, message, now); err != nil {
			var refusal *portable.ProtocolError
			if errors.As(err, &refusal) {
				return nil, rejectIngest(uint32(refusal.Status), "native context unavailable")
			}
			return nil, err
		}
		return stateSnapshot(c, e.cfg.Process), nil
	}
	return nil, nil
}

func (e *StateEngine) rejectionContextID(flow *pb.FlowRef, message *pb.SipMessage, _ uint32) (string, error) {
	encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(flow)
	if err != nil {
		return "", err
	}
	return "reject-" + e.id("rejection-v1", string(encoded), string(message.Wire))[:40], nil
}

// Reject commits an isolated exact-flow refusal before ingest. It never borrows
// an admitted transaction, publishes selectors, invokes a handler or answers ACK.
func (e *StateEngine) Reject(ctx context.Context, flow *pb.FlowRef, message *pb.SipMessage, status uint32) error {
	if flow == nil || message == nil || status < 300 || status > 699 {
		return errors.New("invalid native rejection")
	}
	var ref *pb.ContextRef
	var err error
	if e.autonomousCompletion() {
		callID, parseErr := stateMessageCallID(message.Wire)
		if parseErr != nil {
			return parseErr
		}
		group, groupErr := e.store.GroupForCallID(callID)
		if groupErr != nil {
			return groupErr
		}
		view, _, loadErr := e.store.ReadState(ctx, group)
		possibleRecovery := false
		if errors.Is(loadErr, state.ErrNotFound) {
			if message.Method == "ACK" {
				return nil
			} // Never allocate in response to an unknown ACK.
			possibleRecovery = true
			_, loadErr = e.apply(ctx, group, uuid.NewString(), stateEngineInput{Kind: "locate"}, time.Now().Add(2*time.Second), true)
			if loadErr == nil {
				view, _, loadErr = e.store.ReadState(ctx, group)
			}
		}
		if loadErr != nil {
			if possibleRecovery {
				return errors.Join(state.ErrUnknown, loadErr)
			}
			return classifyStateReadFailure(loadErr)
		}
		ref = e.contextRef(view.Ref, "private-refusal")
		view = state.StateView{}
	} else {
		ref, err = e.Locate(ctx, message)
		if err != nil {
			return err
		}
	}
	if message.Method != "ACK" {
		ref.Id, err = e.rejectionContextID(flow, message, status)
		if err != nil {
			return err
		}
	}
	group, err := e.group(ref)
	if err != nil {
		return err
	}
	application := e.cfg.Process // Private native refusals have no Controller owner.
	if !e.autonomousCompletion() {
		peers := e.control.ReadyPeers()
		if len(peers) == 0 && message.Method != "ACK" {
			return errors.New("rejection relay unavailable")
		}
		application = nil
		if len(peers) > 0 {
			application = peers[0].Process()
		}
	}
	// Each observed packet is a new receive event. The retained refusal procedure
	// coalesces unstarted output and bounds replay; a prior receipt is not a send.
	operation := e.id("reject", ref.State.Incarnation, ref.Id, uuid.NewString())
	result, committed, err := e.applyCommitted(ctx, group, operation, stateEngineInput{Kind: "reject", Context: ref, Application: application, Flow: flow, Message: message, Status: status, Operation: operation}, time.Now().Add(2*time.Second), false)
	if err != nil {
		return err
	}
	if result.Error != "" {
		return errors.New(result.Error)
	}
	e.dispatchCommitted(group, committed, "", "")
	return nil
}

func (e *StateEngine) Handle(peer *control.Peer, envelope *pb.Envelope) error {
	started := time.Now()
	deadline := started.Add(time.Duration(envelope.RemainingBudgetMs) * time.Millisecond)
	e.lifecycleMu.Lock()
	if e.closed {
		e.lifecycleMu.Unlock()
		return errors.New("state engine closed")
	}
	select {
	case e.work <- struct{}{}:
	default:
		e.lifecycleMu.Unlock()
		return peer.Send(&pb.Envelope{CorrelationId: envelope.CorrelationId, Payload: &pb.Envelope_Result{Result: &pb.OperationResult{Certainty: pb.OperationResult_NOT_ACCEPTED, Error: "state execution capacity"}}})
	}
	e.wg.Add(1)
	e.lifecycleMu.Unlock()
	go func() {
		defer e.wg.Done()
		defer func() { <-e.work }()
		ctx, cancel := context.WithDeadline(e.ctx, deadline)
		defer cancel()
		ctx, timing := execution.WithTiming(ctx)
		if request := envelope.GetControllerContexts(); request != nil {
			result, err := e.controllerContexts(ctx, peer, request)
			if err != nil {
				_ = peer.Send(&pb.Envelope{CorrelationId: envelope.CorrelationId, Payload: &pb.Envelope_Result{Result: &pb.OperationResult{Certainty: pb.OperationResult_NOT_ACCEPTED, Error: "controller inventory unavailable"}}})
			} else {
				_ = peer.Send(&pb.Envelope{CorrelationId: envelope.CorrelationId, Payload: &pb.Envelope_ControllerContextsResult{ControllerContextsResult: result}})
			}
			return
		}
		result, err := e.handle(ctx, peer, envelope, deadline)
		if err != nil {
			result = &pb.OperationResult{Certainty: pb.OperationResult_UNKNOWN, Error: err.Error()}
		}
		replyStarted := time.Now()
		sendErr := peer.Send(&pb.Envelope{CorrelationId: envelope.CorrelationId, Payload: &pb.Envelope_Result{Result: result}})
		e.reportControlTiming(envelope, result, timing, started, replyStarted, sendErr != nil)
	}()
	return nil
}
func (e *StateEngine) handle(ctx context.Context, peer *control.Peer, envelope *pb.Envelope, deadline time.Time) (*pb.OperationResult, error) {
	var ref *pb.ContextRef
	if err := e.validateControllerAttachment(ctx, peer); err != nil {
		return &pb.OperationResult{Certainty: pb.OperationResult_NOT_ACCEPTED, Error: "controller attachment unavailable"}, nil
	}
	in := stateEngineInput{Application: peer.Application(), ApplicationGeneration: peer.ApplicationGeneration(), Operation: envelope.CorrelationId}
	if lease := peer.ApplicationLease(); lease != nil {
		in.ApplicationLeaseTerm = lease.Term
	}
	switch body := envelope.Payload.(type) {
	case *pb.Envelope_Ingest:
		in.Kind = "ingest"
		in.Ingest = body.Ingest
		ref = body.Ingest.GetContext()
	case *pb.Envelope_Command:
		in.Kind = "command"
		in.Command = body.Command
		ref = body.Command.GetContext()
		if ref != nil && ref.State == nil && body.Command.GetOpenUac() != nil {
			group, err := e.bootstrapGroup(peer.Application(), ref)
			if err != nil {
				return nil, err
			}
			if _, err = e.apply(ctx, group, e.id("outbound-locate", ref.Id), stateEngineInput{Kind: "locate"}, deadline, true); err != nil {
				return nil, err
			}
			view, _, err := e.store.ReadState(ctx, group)
			if err != nil {
				return nil, err
			}
			ref = e.contextRef(view.Ref, ref.Id)
			view = state.StateView{}
			in.Command = proto.Clone(body.Command).(*pb.Command)
			in.Command.Context = ref
		}
	case *pb.Envelope_Inspect:
		return e.inspectContext(ctx, peer.Application(), body.Inspect.GetContext())
	default:
		return &pb.OperationResult{Certainty: pb.OperationResult_NOT_ACCEPTED, Error: "unsupported state engine message"}, nil
	}
	in.Context = ref
	group, err := e.group(ref)
	if err != nil {
		return nil, err
	}
	if !e.usesCAS() {
		preflightStarted := time.Now()
		preflightErr := e.preflightControl(ctx, group, in, deadline)
		if timing := execution.RequestTiming(ctx); timing != nil {
			timing.Preflight += time.Since(preflightStarted)
		}
		if preflightErr != nil {
			return nil, preflightErr
		}
	}
	result, committed, err := e.applyCommitted(ctx, group, envelope.CorrelationId, in, deadline, false)
	if err == nil && in.Command != nil && result.Certainty == pb.OperationResult_ACCEPTED {
		wait := in.Command.GetRespond() != nil || in.Command.GetDialogRequest() != nil || in.Command.GetOpenUac() != nil && in.Command.GetOpenUac().Method != "" && in.Command.GetOpenUac().Method != "INVITE"
		if wait {
			signal, stop := e.watchOperation(group)
			defer stop()
			e.dispatchCommitted(group, committed, "", "")
			return e.awaitOperationSignal(ctx, group, ref.Id, envelope.CorrelationId, result, in.Command.GetRespond() != nil, signal, committed != nil)
		}
	}
	if err == nil {
		e.dispatchCommitted(group, committed, "", "")
	}
	return e.resultForExecutor(result), err
}

func (e *StateEngine) Reserve(ctx context.Context, write *pb.FlowWrite) (*CommittedWrite, error) {
	if write == nil || write.Effect == nil || write.Effect.Context == nil || write.Effect.AttemptId == "" || write.Flow == nil || !sameProcess(write.Flow.Ingress, e.cfg.Process) {
		return nil, errors.New("invalid local effect attempt")
	}
	ref, err := e.group(write.Effect.Context)
	if err != nil {
		return nil, err
	}
	if !e.usesCAS() {
		// A computation receipt for an earlier reservation is not permission to
		// restart that attempt after its completion. Inspect current effect state
		// before consulting the idempotent transition receipt.
		record, err := e.store.Load(ctx, ref)
		if err != nil {
			return nil, err
		}
		found := false
		for _, effect := range record.Payload.Effects {
			if effect.ID != write.Effect.Id {
				continue
			}
			found = true
			var body stateEngineEffect
			if body.unmarshalMetadata(effect.Payload) != nil || body.Permit == nil || body.Authorization == nil || !proto.Equal(body.Permit, write.Permit) || !proto.Equal(body.Permit.Flow, write.Flow) || !stateSameContext(body.Authorization.Effect.Context, write.Effect.Context) || body.Authorization.Effect.AttemptId != write.Effect.AttemptId {
				return nil, errors.New("effect authority mismatch")
			}
			if effect.AttemptID != "" && effect.AttemptID != write.Effect.AttemptId {
				return nil, errors.New("effect attempt mismatch")
			}
			if effect.Status == state.EffectWritten {
				return &CommittedWrite{Known: true, Outcome: pb.DeliveryResult_WRITTEN}, nil
			}
			if effect.Status == state.EffectNotWritten {
				return &CommittedWrite{Known: true, Outcome: pb.DeliveryResult_NOT_WRITTEN}, nil
			}
			if effect.Status == state.EffectDispatching && len(body.Wire) == 0 {
				return &CommittedWrite{Known: true, Outcome: pb.DeliveryResult_UNKNOWN}, nil
			}
			if effect.Status == state.EffectUnknown || effect.Status == state.EffectCancelled {
				return &CommittedWrite{Known: true, Outcome: pb.DeliveryResult_UNKNOWN}, nil
			}
		}
		if !found {
			return &CommittedWrite{Known: true, Outcome: pb.DeliveryResult_UNKNOWN}, nil
		}
	}
	copy := proto.Clone(write).(*pb.FlowWrite)
	deadline := time.Now().Add(2 * time.Second)
	if write.Permit.GetAuthorization().GetDeadlineMs() > 0 {
		deadline = time.UnixMilli(int64(write.Permit.Authorization.DeadlineMs))
	}
	operation := e.id("reserve", write.Effect.Id, write.Effect.AttemptId)
	if e.usesCAS() {
		// Internal computation has no new replay record. Never consult a legacy
		// proof that once returned wire for this same physical attempt.
		operation = e.id("reserve", write.Effect.Id, write.Effect.AttemptId, uuid.NewString())
	}
	result, err := e.apply(ctx, ref, operation, stateEngineInput{Kind: "reserve", Context: write.Effect.Context, Executor: e.cfg.Process, Write: copy, Effect: write.Effect.Id}, deadline, false)
	if err != nil {
		return nil, err
	}
	if result.Error != "" {
		return nil, errors.New(result.Error)
	}
	if result.Response != nil {
		return &CommittedWrite{Known: true, Outcome: pb.DeliveryResult_Outcome(result.SipStatus)}, nil
	}
	if result.Request == nil {
		return nil, state.ErrCorrupt
	}
	return &CommittedWrite{Wire: append([]byte(nil), result.Request.Wire...)}, nil
}
func (e *StateEngine) Complete(ctx context.Context, write *pb.FlowWrite, delivery *pb.DeliveryResult) error {
	if write == nil || write.Effect == nil || delivery == nil {
		return errors.New("delivery evidence required")
	}
	ref, err := e.group(write.Effect.Context)
	if err != nil {
		return err
	}
	copy := proto.Clone(write).(*pb.FlowWrite)
	physical := proto.Clone(delivery).(*pb.DeliveryResult)
	physical.Recorded = false // A transport acknowledgement is not part of the retained physical fact.
	result, committed, err := e.applyCommitted(ctx, ref, e.id("completion", write.Effect.Id, write.Effect.AttemptId, physical.Outcome.String()), stateEngineInput{Kind: "complete", Context: write.Effect.Context, Executor: e.cfg.Process, Write: copy, Delivery: physical, Effect: write.Effect.Id}, time.Now().Add(2*time.Second), false)
	if err != nil {
		return err
	}
	if result == nil || result.Error != "" || result.Certainty == pb.OperationResult_NOT_ACCEPTED {
		return errors.New("native completion was not committed")
	}
	e.dispatchCommitted(ref, committed, "", "")
	return nil
}

type stateScheduleCursor struct{ partition, admission uint16 }

func (e *StateEngine) scheduleHints(ctx context.Context, cursor *stateScheduleCursor) {
	_, _ = e.scheduleLeasedHints(ctx, e.ctx, cursor, "")
}

func (e *StateEngine) scheduleLeasedHints(ctx, leader context.Context, cursor *stateScheduleCursor, token string) (bool, error) {
	count := stateSchedulePartitions(e.store.PartitionCount())
	first := cursor.partition
	var batches []state.HintBatch
	var err error
	if token == "" {
		batches, err = e.store.ScheduleHints(ctx, first, count, 8)
	} else {
		batches, err = e.store.SchedulerHints(ctx, token, first, count, 8)
	}
	cursor.partition = (first + uint16(count)) % e.store.PartitionCount()
	start := int(cursor.admission)
	if cursor.partition == 0 {
		cursor.admission = (cursor.admission + 1) % uint16(count)
	}
	if err != nil {
		return false, err
	}
	corrupt := false
	for n := range batches {
		hints := batches[(start+n)%len(batches)]
		corrupt = corrupt || hints.ReadyError != nil || hints.DueError != nil
		if hints.ReadyError == nil {
			for _, hint := range hints.Ready {
				e.scheduleGroupIn(leader, hint.Ref)
			}
		}
		if hints.DueError == nil {
			for _, hint := range hints.Due {
				e.scheduleGroupIn(leader, hint.Ref)
			}
		}
	}
	return corrupt, nil
}

func stateSchedulePartitions(partitions uint16) int {
	return min(8, max(1, (int(partitions)+7)/8))
}

type stateSchedulePlan struct {
	ref     state.Ref
	pending bool
	retire  bool
	timers  []state.Timer
	effects []state.Effect
}

func (e *StateEngine) schedulePlan(ctx context.Context, ref state.Ref) (stateSchedulePlan, error) {
	view, now, err := e.store.ReadState(ctx, ref)
	if err != nil {
		return stateSchedulePlan{}, err
	}
	record := stateViewProtocolRecord(view)
	return stateSchedulePlan{ref: record.Ref, pending: view.HasPending,
		retire: record.Payload.Retention.Terminal && record.Payload.ActiveTransactions == 0 && len(record.Payload.Effects) == 0 && len(record.Payload.Timers) == 0,
		timers: e.dueTimers(record, now), effects: e.dispatchableEffects(record.Payload.Effects, now)}, nil
}

// Only protocol-only preflight/discovery, historical outcome inspection and
// already-admitted completion observation consume this adapter. It carries no replay history, observed leases
// or CAS token and must never authorize a write.
func stateViewProtocolRecord(view state.StateView) state.Record {
	return state.Record{Ref: view.Ref, Authority: view.Authority, Versions: view.Versions,
		Revision: view.Revision, Payload: view.Payload}
}

func (e *StateEngine) applyScheduledTimers(ctx context.Context, ref state.Ref, timers []state.Timer) bool {
	changed := false
	for _, timer := range timers {
		_, committed, err := e.applyCommitted(ctx, ref, e.id("timer", ref.Incarnation, timer.ID, timer.Generation), stateEngineInput{Kind: "timer", Timer: timer.ID, Generation: timer.Generation}, time.Now().Add(2*time.Second), false)
		e.scheduler.fail(schedulerTimerFailure, err)
		changed = changed || err == nil && (!e.usesCAS() || committed != nil)
	}
	return changed
}

func (e *StateEngine) dispatch(ref state.Ref) {
	e.dispatchAfterEffect(ref, "", "")
}

func (e *StateEngine) dispatchCommitted(ref state.Ref, committed *execution.Committed, effectID, previousKind string) {
	if committed == nil {
		e.dispatchAfterEffect(ref, effectID, previousKind)
		return
	}
	if effectID != "" && !stateRPCProgressed(effectID, previousKind, committed.Effects) {
		return
	}
	e.dispatchPrepared(committed.Ref, e.dispatchableEffects(committed.Effects, time.Now().UnixMilli()))
}

func (e *StateEngine) dispatchAfterEffect(ref state.Ref, effectID, previousKind string) {
	e.dispatchWork(func() {
		view, _, err := e.store.ReadState(e.ctx, ref)
		if err != nil {
			return
		}
		if effectID != "" && !stateRPCProgressed(effectID, previousKind, view.Payload.Effects) {
			return
		}
		effects := e.dispatchableEffects(view.Payload.Effects, time.Now().UnixMilli())
		ref := view.Ref
		view = state.StateView{} // Never retain the authoritative SIP snapshot across RPC.
		e.dispatchEffects(ref, effects)
	})
}

// A successful reduction/receipt may leave the RPC effect unchanged. Only a
// forward effect transition chains immediately; retained uncertainty is paced
// by the durable recovery timer instead of recursively calling the same RPC.
func stateRPCProgressed(effectID, previousKind string, effects []state.Effect) bool {
	for _, effect := range effects {
		if effect.ID != effectID {
			continue
		}
		if previousKind == "wire" && effect.Kind == "deliver" && effect.Status == state.EffectAuthorized {
			return true
		}
		if effect.Status != state.EffectWritten && effect.Status != state.EffectNotWritten && effect.Status != state.EffectCancelled {
			return false
		}
		if effect.Kind != "deliver" || previousKind == "wire" {
			return true
		}
		var body stateEngineEffect
		return body.unmarshalMetadata(effect.Payload) == nil && body.Settled
	}
	return true // A completed/collected effect no longer needs its RPC repeated.
}

func (e *StateEngine) dispatchPrepared(ref state.Ref, effects []state.Effect) {
	if len(effects) == 0 {
		return
	}
	e.dispatchWork(func() { e.dispatchEffects(ref, effects) })
}

func (e *StateEngine) dispatchWork(work func()) {
	e.lifecycleMu.Lock()
	if e.closed {
		e.lifecycleMu.Unlock()
		return
	}
	select {
	case e.background <- struct{}{}:
	default:
		e.lifecycleMu.Unlock()
		return
	}
	select {
	case e.work <- struct{}{}:
	default:
		<-e.background
		e.lifecycleMu.Unlock()
		return
	}
	e.wg.Add(1)
	e.lifecycleMu.Unlock()
	go func() {
		defer e.wg.Done()
		defer func() { <-e.work; <-e.background }()
		work()
	}()
}

// The production writer no longer originates Controller grant, write-relay or
// reconciliation RPCs. Legacy relay fixtures retain their explicit driver.
func stateTrustedRelayEffect(effect state.Effect) bool {
	switch effect.Kind {
	case "wire":
		return true
	case "deliver":
		// Selection needs only this identity bit. The dispatcher validates the
		// complete owned body once before any physical or control operation.
		var identity stateEngineEffect
		return identity.unmarshalMetadata(effect.Payload) != nil || !identity.Native
	default:
		return false
	}
}

func stateSupportedEffectKind(kind string) bool {
	switch kind {
	case "present", "event", "adopt", "open", "credential", "owner", "wire", "deliver":
		return true
	default:
		return false
	}
}

func (e *StateEngine) dispatchableEffects(effects []state.Effect, now int64) []state.Effect {
	eligible := stateDispatchableEffects(effects, now)
	filtered := eligible[:0]
	worker := e.dispatchWorker(e.cfg.Process)
	for _, effect := range eligible {
		if effect.Worker != "" && effect.Worker != state.AnyDispatchWorker && effect.Worker != worker {
			continue
		}
		if e.autonomousCompletion() && stateTrustedRelayEffect(effect) {
			continue
		}
		filtered = append(filtered, effect)
	}
	return filtered
}

func stateDispatchableEffects(effects []state.Effect, now int64) []state.Effect {
	var eligible []state.Effect
	for _, effect := range effects {
		if stateDispatchableEffect(effect, now) {
			eligible = append(eligible, effect)
		}

	}
	return eligible
}

func stateDispatchableEffect(effect state.Effect, now int64) bool {
	if !stateSupportedEffectKind(effect.Kind) || effect.Kind == "owner" && effect.Status == state.EffectUnknown {
		return false
	}
	known := effect.Kind == "deliver" && (effect.Status == state.EffectWritten || effect.Status == state.EffectNotWritten)
	if known {
		var body stateEngineEffect
		return body.unmarshalMetadata(effect.Payload) == nil && !body.Settled
	}
	if effect.Status != state.EffectPending && effect.Status != state.EffectAuthorized && effect.Status != state.EffectDispatching && effect.Status != state.EffectUnknown {
		return false
	}
	return effect.Kind == "owner" || effect.DeadlineMS+120000 > now || stateHistoricalDelivery(effect) || stateHistoricalAuthorization(effect)
}

func (e *StateEngine) dispatchEffects(ref state.Ref, effects []state.Effect) {
	eventPrefix := make(map[string]bool)
	for _, effect := range effects {
		if effect.Kind == "event" {
			var body stateEngineEffect
			if body.unmarshalMetadata(effect.Payload) != nil || eventPrefix[body.Context] {
				continue
			}
			eventPrefix[body.Context] = true
		}
		e.dispatchEffect(ref, effect)
	}
}

func (e *StateEngine) acquireDispatch(effectID string) func() {
	e.dispatchMu.Lock()
	defer e.dispatchMu.Unlock()
	if e.dispatching[effectID] || len(e.dispatching) >= 128 {
		return nil
	}
	e.dispatching[effectID] = true
	return sync.OnceFunc(func() {
		e.dispatchMu.Lock()
		delete(e.dispatching, effectID)
		e.dispatchMu.Unlock()
	})
}

func (e *StateEngine) dispatchEffect(ref state.Ref, effect state.Effect) {
	e.dispatchEffectWithin(e.ctx, ref, effect, true)
}

func (e *StateEngine) dispatchEffectWithin(parent context.Context, ref state.Ref, effect state.Effect, chain bool) {
	var body stateEngineEffect
	if body.unmarshalMetadata(effect.Payload) != nil {
		return
	}
	if !stateSupportedEffectKind(effect.Kind) || e.autonomousCompletion() && (effect.Kind == "wire" || effect.Kind == "deliver" && !body.Native) {
		return
	}
	if effect.Kind == "wire" && body.Cancelled && effect.DeadlineMS > time.Now().UnixMilli() {
		return // Before expiry a lookup could accidentally issue a missing APP grant.
	}
	if body.AfterACK != "" && !e.ackDependencyReady(ref, body) {
		return
	}
	recovery := effect.Status == state.EffectDispatching || effect.Status == state.EffectUnknown || effect.Status == state.EffectWritten || effect.Status == state.EffectNotWritten
	deadline := time.UnixMilli(effect.DeadlineMS)
	if effect.Kind == "event" {
		// Pending notifications can wait behind physical effects. Their original
		// deadline anchors recovery; it must never be rewritten on a later start.
		deadline = time.UnixMilli(min(time.Now().Add(2*time.Second).UnixMilli(), effect.DeadlineMS+120000))
	} else if recovery || effect.Kind == "owner" || effect.Kind == "deliver" && deadline.Before(time.Now()) {
		deadline = time.Now().Add(2 * time.Second)
	}
	ctx, cancel := context.WithDeadline(parent, deadline)
	defer cancel()
	if ctx.Err() != nil {
		return
	}
	if e.autonomousCompletion() && effect.Kind == "deliver" && body.Native {
		release := e.acquireDispatch(effect.ID)
		if release == nil {
			return
		}
		defer release()
		e.dispatchNativeCompletion(ctx, ref, effect, body, release)
		return
	}
	var peer *control.Peer
	var err error
	if effect.Kind != "open" && effect.Kind != "credential" {
		application := effect.Kind == "wire" && body.Authorization.GetAuthorityClass() == pb.EffectAuthorization_APPLICATION
		metadataRecovery := effect.Kind == "wire" && (body.Cancelled || effect.DeadlineMS <= time.Now().UnixMilli())
		if application && !metadataRecovery {
			peer, err = e.control.Peer(body.Application)
			if err != nil {
				return
			}
		} else if effect.Kind == "wire" || effect.Kind == "deliver" || effect.Kind == "owner" {
			peers := e.control.ReadyPeers()
			if len(peers) > 0 {
				peer = peers[0]
			} else {
				return
			}
		} else {
			peer, err = e.control.Peer(body.Application)
			if err != nil {
				return
			}
		}
	}
	if effect.Kind == "open" && recovery {
		return
	}
	// This map bounds concurrent transport RPCs only. Protocol/effect state is
	// always reloaded and the receiver deduplicates the retained operation ID.
	release := e.acquireDispatch(effect.ID)
	if release == nil {
		return
	}
	defer release()
	if effect.Kind != "deliver" && !(e.usesCAS() && (effect.Kind == "credential" || e.receiverEffect(effect.Kind))) {
		var started *pb.OperationResult
		var err error
		if !recovery {
			startDeadline := time.UnixMilli(effect.DeadlineMS)
			if effect.Kind == "event" {
				startDeadline = deadline
			} else if effect.Kind == "owner" {
				startDeadline = time.Now().Add(2 * time.Second)
			}
			started, err = e.apply(ctx, ref, e.id("rpc-start", effect.ID), stateEngineInput{Kind: "effect-start", Effect: effect.ID}, startDeadline, false)
		}
		if !recovery && (err != nil || started.Snapshot == nil || !sameProcess(started.Snapshot.Executor, e.cfg.Process)) {
			return
		}
		// A recovery reuses the exact committed receiver identity; it never
		// changes a physical effect attempt or authorizes an expired start.
	}
	if effect.Kind == "owner" {
		e.dispatchOwnerProbe(ctx, ref, effect, body, release)
		return
	}
	if effect.Kind == "credential" {
		e.dispatchCredential(ctx, ref, effect, body, release)
		return
	}
	if effect.Kind == "open" {
		var target pb.OpenUac
		if stateDecodeNetworkTarget(body.Wire, &target) != nil {
			return
		}
		var flow *pb.FlowRef
		var err error
		if e.cfg.OpenNetwork == nil {
			err = errors.New("network transport unavailable")
		} else {
			network := target.Network
			if network == nil {
				network = &pb.NetworkTarget{Transport: target.Transport}
			}
			flow, err = e.cfg.OpenNetwork(ctx, target.Uri, network)
		}
		in := stateEngineInput{Kind: "effect-result", Effect: effect.ID, Executor: e.cfg.Process, Flow: flow}
		if err != nil {
			in.Failure = "control RPC outcome unavailable"
		}
		if _, committed, err := e.applyCommitted(e.ctx, ref, stateEffectResultID(e, "opened", effect.ID, in), in, time.UnixMilli(effect.DeadlineMS+120000), false); err == nil {
			release()
			e.dispatchCommitted(ref, committed, effect.ID, effect.Kind)
		}
		return
	}
	executor := e.cfg.Process
	if recovery && body.Executor != nil {
		executor = body.Executor
	}
	// A fresh legacy start has just committed this executor even though the
	// pre-start body above had none. Only startless/recovered work has no executor.
	if e.receiverEffect(effect.Kind) && body.Executor == nil && (recovery || e.usesCAS()) {
		executor = nil
	}
	if effect.Kind == "wire" {
		if err := body.materializeMessage(); err != nil {
			return
		}
		if body.Envelope == nil {
			return
		}
		request := proto.Clone(body.Envelope).(*pb.Envelope)
		if snapshot := request.GetEgress().GetSnapshot(); snapshot != nil {
			snapshot.Executor = proto.Clone(e.cfg.Process).(*pb.ProcessRef)
			if snapshot.Context != nil {
				snapshot.Context.Owner = proto.Clone(e.cfg.Process).(*pb.ProcessRef)
			}
		}
		reply, err := peer.Call(ctx, request)
		in := stateEngineInput{Kind: "effect-result", Effect: effect.ID, Executor: executor, Reply: stateCanonicalReply(reply)}
		if err != nil {
			in.Failure = "control RPC outcome unavailable"
		}
		resultDeadline := time.UnixMilli(effect.DeadlineMS + 120000)
		if body.Cancelled || effect.DeadlineMS <= time.Now().UnixMilli() {
			resultDeadline = time.Now().Add(2 * time.Second)
		}
		if _, committed, err := e.applyCommitted(e.ctx, ref, stateEffectResultID(e, "authorized", effect.ID, in), in, resultDeadline, false); err == nil {
			release()
			e.dispatchCommitted(ref, committed, effect.ID, effect.Kind)
		}
		return
	}
	if effect.Kind == "deliver" {
		if body.Permit == nil || body.Authorization == nil {
			return
		}
		write := &pb.FlowWrite{Flow: body.Permit.Flow, Effect: body.Authorization.Effect, Permit: body.Permit}
		if effect.Status == state.EffectWritten || effect.Status == state.EffectNotWritten {
			return // Terminal physical evidence cannot start another delivery.
		}
		request := &pb.Envelope{CorrelationId: e.id("deliver", effect.ID), RemainingBudgetMs: 2000, Payload: &pb.Envelope_DeliverEffect{DeliverEffect: write}}
		if recovery || effect.DeadlineMS <= time.Now().UnixMilli() {
			request.CorrelationId = e.id("inspect-effect", effect.ID)
			request.Payload = &pb.Envelope_InspectEffect{InspectEffect: write}
		}
		_, _ = peer.Call(ctx, request)
		release()
		return
	}
	// Presentation and lifecycle effects use stable IDs. The result receipt is a
	// separate transition; the Controller receiver must reconcile that same operation.
	if body.Envelope == nil {
		return
	}
	if err := body.materializeMessage(); err != nil {
		return
	}
	request := body.Envelope
	if effect.Kind == "event" {
		// Remaining budget is session framing, not the retained receiver identity.
		// Include the start transition and any shorter parent budget in this cap.
		until, _ := ctx.Deadline()
		remaining := time.Until(until).Milliseconds()
		if remaining <= 0 {
			return
		}
		request = proto.Clone(request).(*pb.Envelope)
		request.RemainingBudgetMs = uint32(min(2000, remaining))
	}
	reply, err := peer.Call(ctx, request)
	in := stateEngineInput{Kind: "effect-result", Effect: effect.ID, Executor: executor, Reply: stateCanonicalReply(reply)}
	if err != nil {
		in.Failure = "control RPC outcome unavailable"
	}
	resultDeadline := time.UnixMilli(effect.DeadlineMS + 120000)
	if _, committed, err := e.applyCommitted(e.ctx, ref, stateEffectResultID(e, "observed", effect.ID, in), in, resultDeadline, false); err == nil {
		release()
		e.dispatchCommitted(ref, committed, effect.ID, effect.Kind)
	}
}
