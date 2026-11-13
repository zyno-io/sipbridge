package engine

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
	"sipbridge/internal/control"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/execution"
	"sipbridge/internal/state"
)

func NewResidentMemoryAuthority(namespace, generation string) *pb.StateAuthority {
	return &pb.StateAuthority{Service: namespace, Generation: generation, PrimaryId: uuid.NewString(), SchemaVersion: 1}
}

func NewResidentMemory(parent context.Context, cfg Config, options ResidentMemoryOptions, server *control.Server) (*ResidentMemory, error) {
	if options.MaxContexts == 0 {
		options.MaxContexts = 128
	}
	if options.MaxBytes == 0 {
		options.MaxBytes = 1 << 30
	}
	if !validSendProcess(cfg.Process) || len(cfg.StateIdentityKey) < 32 || server == nil || options.Authority == nil ||
		options.Authority.Service == "" || options.Authority.Generation == "" || options.Authority.PrimaryId == "" || options.Authority.SchemaVersion != 1 ||
		options.MaxContexts < 1 || options.MaxBytes < 1 {
		return nil, errors.New("resident engine identity, authority, control and limits required")
	}
	for _, transport := range []string{"tls", "tcp", "udp"} {
		if _, _, err := advertisement(cfg, transport); err != nil {
			return nil, err
		}
	}
	ctx, cancel := context.WithCancel(parent)
	e := &ResidentMemory{cfg: cfg, control: server, authority: proto.Clone(options.Authority).(*pb.StateAuthority), ctx: ctx, cancel: cancel,
		contexts: make(map[string]*residentContext), transactions: make(map[string]*residentContext), dialogs: make(map[string]*residentContext),
		leases: make(map[string]*residentLease), deadlineWake: make(chan struct{}, 1), work: make(chan struct{}, 64), ingressWork: make(chan struct{}, 48),
		maxContexts: options.MaxContexts, maxBytes: options.MaxBytes}
	e.cfg.StateIdentityKey = append([]byte(nil), cfg.StateIdentityKey...)
	e.wg.Add(1)
	go func() { defer e.wg.Done(); e.scheduleResident() }()
	return e, nil
}

func (e *ResidentMemory) Close() {
	e.mu.Lock()
	e.closed = true
	e.cancel()
	e.mu.Unlock()
	e.wg.Wait()
}

func (e *ResidentMemory) Ready() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return errors.New("resident engine closed")
	}
	return e.ctx.Err()
}

func (e *ResidentMemory) StateAuthority() *pb.StateAuthority {
	return proto.Clone(e.authority).(*pb.StateAuthority)
}

func (e *ResidentMemory) id(parts ...string) string {
	h := hmac.New(sha256.New, e.cfg.StateIdentityKey)
	for _, part := range parts {
		fmt.Fprintf(h, "%d:", len(part))
		h.Write([]byte(part))
	}
	return hex.EncodeToString(h.Sum(nil))
}

func (e *ResidentMemory) contextRef(callID, id string) *pb.ContextRef {
	return &pb.ContextRef{Owner: proto.Clone(e.cfg.Process).(*pb.ProcessRef), Id: id,
		State: &pb.StateRef{Authority: e.StateAuthority(), GroupId: id, Incarnation: e.authority.PrimaryId, CallId: callID}}
}

func (e *ResidentMemory) validateRef(ref *pb.ContextRef) error {
	if ref == nil || !state.ValidReference(ref.Id) || ref.State == nil || !proto.Equal(ref.State.Authority, e.authority) ||
		ref.State.GroupId != ref.Id || !state.ValidReference(ref.State.Incarnation) || ref.State.Partition != 0 || ref.State.CallId == "" {
		return state.ErrAuthority
	}
	return nil
}

// Index selection never holds the index mutex while waiting for a context lock.
// A caller must check the immutable context identity after taking that lock.
func (e *ResidentMemory) findContext(ref *pb.ContextRef) (*residentContext, error) {
	if err := e.validateRef(ref); err != nil {
		return nil, err
	}
	e.mu.Lock()
	c := e.contexts[ref.Id]
	e.mu.Unlock()
	if c == nil {
		return nil, state.ErrNotFound
	}
	return c, nil
}

// residentContextCurrentLocked is called with c.mu held, after index selection.
// This prevents a stale pointer or a recycled bare ID from addressing a new
// incarnation. It follows the context-before-index lock ordering.
func (e *ResidentMemory) residentContextCurrentLocked(c *residentContext, ref *pb.ContextRef) bool {
	if c == nil || ref == nil || c.snapshot == nil || c.snapshot.Context == nil || c.snapshot.Context.Id != ref.Id || !proto.Equal(c.snapshot.Context.State, ref.State) {
		return false
	}
	e.mu.Lock()
	current := e.contexts[ref.Id] == c
	e.mu.Unlock()
	return current
}

func (e *ResidentMemory) wakeDeadline() {
	select {
	case e.deadlineWake <- struct{}{}:
	default:
	}
}

func (e *ResidentMemory) SchedulerStatistics() SchedulerStatistics {
	e.mu.Lock()
	defer e.mu.Unlock()
	return SchedulerStatistics{OwnedGroups: uint64(len(e.contexts))}
}

type ResidentStatistics struct {
	Contexts                               int
	RetainedBytes, CompletionReservedBytes int64
}

// Counters are maintained with publication, never reconstructed by a metrics
// walk through native records.
func (e *ResidentMemory) ResidentStatistics() ResidentStatistics {
	e.mu.Lock()
	defer e.mu.Unlock()
	return ResidentStatistics{Contexts: len(e.contexts), RetainedBytes: e.bytes, CompletionReservedBytes: e.reserved}
}

func (e *ResidentMemory) ExecutionStatistics() []execution.Statistic       { return nil }
func (e *ResidentMemory) OperationObservationStatistics() (uint64, uint64) { return 0, 0 }

func (e *ResidentMemory) CallbackStatistics() ([]CallbackStatistic, []PhysicalLocalityStatistic) {
	return e.callbackMetrics.snapshot()
}

func (e *ResidentMemory) advertisement(transport string) (string, string, error) {
	return advertisement(e.cfg, transport)
}
