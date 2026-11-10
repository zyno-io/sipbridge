package engine

import (
	"context"
	"errors"
	"time"

	"google.golang.org/protobuf/proto"
	"sipbridge/internal/control"
	pb "sipbridge/internal/controlpb"
)

// CommittedWrite is a stored effect reservation, never caller-selected wire.
type CommittedWrite struct {
	Wire    []byte
	Known   bool
	Outcome pb.DeliveryResult_Outcome
}

// These startup-wired callbacks accept only a committed effect reference/permit.
// committedResult re-reads and reserves native state before touching a socket.
func (i *Ingress) WriteCommitted(ctx context.Context, write *pb.FlowWrite) *pb.DeliveryResult {
	return i.committedResult(ctx, write)
}

func (i *Ingress) InspectCommitted(ctx context.Context, write *pb.FlowWrite) *pb.DeliveryResult {
	return i.inspectCommitted(ctx, write)
}

type StateWriter interface {
	Reserve(context.Context, *pb.FlowWrite) (*CommittedWrite, error)
	Complete(context.Context, *pb.FlowWrite, *pb.DeliveryResult) error
}

type StateInspector interface {
	InspectWrite(context.Context, *pb.FlowWrite) (*pb.DeliveryResult, error)
}

type physicalAttempt struct {
	result  *pb.DeliveryResult // nil while this process's one physical attempt runs
	noStart bool               // this memo never received permission to enter the socket syscall
	until   time.Time          // original monotonic physical-start bound, never renewed
}

// Completed local evidence may expire even if its Redis acknowledgement was
// lost. Native reservation still prevents another write of that exact attempt.
// The caller holds i.mu; an actual in-flight syscall is never evicted.
func (i *Ingress) physicalAttemptLocked(key string, now time.Time) *physicalAttempt {
	attempt := i.stateAttempts[key]
	if attempt != nil && attempt.result != nil && !attempt.until.IsZero() && !now.Before(attempt.until) {
		delete(i.stateAttempts, key)
		return nil
	}
	return attempt
}

// LiveFlow is point-in-time transport evidence for an explicit authority barrier.
// Later closure still follows the normal exact-flow write fencing.
func (i *Ingress) LiveFlow(ref *pb.FlowRef) bool {
	if ref == nil || !sameProcess(ref.Ingress, i.process) {
		return false
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	flow := i.flows[ref.ConnectionId]
	return flow != nil && proto.Equal(flow.ref, ref) && !flow.closed.Load()
}

// ConfigureStateRuntime is startup-only. Logical work and writing remain in the
// same process, with separate APIs; no reducer receives the socket capability.
func (i *Ingress) ConfigureStateRuntime(writer StateWriter, locate func(context.Context, *pb.SipMessage) (*pb.ContextSnapshot, error), reject ...func(context.Context, *pb.FlowRef, *pb.SipMessage, uint32) error) error {
	if writer == nil || locate == nil || i.stateWriter != nil {
		return errors.New("invalid state runtime configuration")
	}
	if len(reject) > 1 {
		return errors.New("invalid state rejection configuration")
	}
	i.stateWriter = writer
	i.stateLocate = locate
	if len(reject) == 1 {
		i.stateReject = reject[0]
	}
	i.stateAttempts = make(map[string]*physicalAttempt)
	return nil
}

// InspectEffect resolves process-local physical evidence without reserving a new
// dispatch or touching the socket. Expired permits may only use this path.
func (i *Ingress) InspectEffect(peer *control.Peer, envelope *pb.Envelope) error {
	select {
	case i.writes <- struct{}{}:
		go func() {
			defer func() { <-i.writes }()
			result := i.inspectCommitted(peer.Context(), envelope.GetInspectEffect())
			_ = peer.Send(&pb.Envelope{CorrelationId: envelope.CorrelationId, Payload: &pb.Envelope_Delivery{Delivery: result}})
		}()
		return nil
	default:
		return peer.Send(&pb.Envelope{CorrelationId: envelope.CorrelationId, Payload: &pb.Envelope_Delivery{Delivery: &pb.DeliveryResult{Outcome: pb.DeliveryResult_UNKNOWN, Error: "inspection capacity"}}})
	}
}

func (i *Ingress) inspectCommitted(ctx context.Context, cmd *pb.FlowWrite) *pb.DeliveryResult {
	result := &pb.DeliveryResult{Outcome: pb.DeliveryResult_UNKNOWN}
	if cmd != nil {
		result.Effect = cmd.Effect
	}
	if i.stateWriter == nil || cmd == nil || cmd.Effect == nil || cmd.Permit == nil || cmd.Flow == nil || !sameProcess(cmd.Flow.Ingress, i.process) || cmd.Effect.Id == "" || cmd.Effect.AttemptId == "" || len(cmd.Wire) != 0 {
		result.Error = "invalid physical evidence inspection"
		return result
	}
	key := cmd.Effect.Id + "/" + cmd.Effect.AttemptId
	i.mu.Lock()
	attempt := i.physicalAttemptLocked(key, time.Now())
	if attempt != nil && attempt.result != nil {
		result = proto.Clone(attempt.result).(*pb.DeliveryResult)
	} else {
		result.Error = "no completed physical evidence in this process"
	}
	i.mu.Unlock()
	if attempt != nil && result.Outcome != pb.DeliveryResult_UNKNOWN {
		i.recordCompletion(ctx, cmd, result, key, attempt)
	} else if attempt == nil {
		if inspector, ok := i.stateWriter.(StateInspector); ok {
			lookup, cancel := context.WithTimeout(ctx, 2*time.Second)
			stored, err := inspector.InspectWrite(lookup, cmd)
			cancel()
			if err == nil && stored != nil {
				result = stored
			}
		}
	}
	return result
}

func (i *Ingress) writeCommitted(peer *control.Peer, envelope *pb.Envelope) error {
	cmd := envelope.GetWrite()
	result := i.committedResult(peer.Context(), cmd)
	return peer.Send(&pb.Envelope{CorrelationId: envelope.CorrelationId, Payload: &pb.Envelope_Delivery{Delivery: result}})
}

func (i *Ingress) committedResult(parent context.Context, cmd *pb.FlowWrite) *pb.DeliveryResult {
	result := &pb.DeliveryResult{Outcome: pb.DeliveryResult_UNKNOWN}
	if cmd != nil {
		result.Effect = cmd.Effect
	}
	if i.stateWriter == nil || cmd == nil || cmd.Effect == nil || cmd.Permit == nil || cmd.Flow == nil ||
		!sameProcess(cmd.Flow.Ingress, i.process) || cmd.Effect.Id == "" || cmd.Effect.AttemptId == "" || len(cmd.Wire) != 0 {
		result.Error = "invalid committed physical write"
		return result
	}
	// Retain a monotonic upper bound before any Redis wait or stream queueing.
	// A later wall-clock rollback cannot revive a paused emission permit.
	start := time.Now()
	startDeadline := start
	if auth := cmd.Permit.Authorization; auth != nil && auth.DeadlineMs <= 1<<53-1 {
		remaining := int64(auth.DeadlineMs) - start.UnixMilli()
		if remaining > 0 {
			startDeadline = start.Add(time.Duration(min(remaining, 2000)) * time.Millisecond)
		}
	}
	key := cmd.Effect.Id + "/" + cmd.Effect.AttemptId
	i.mu.Lock()
	if prior := i.physicalAttemptLocked(key, start); prior != nil {
		complete := prior.result != nil
		if complete {
			result = proto.Clone(prior.result).(*pb.DeliveryResult)
		} else {
			result.Error = "physical write is already in progress"
		}
		i.mu.Unlock()
		if complete {
			i.recordCompletion(parent, cmd, result, key, prior)
		}
		return result
	}
	if len(i.stateAttempts) >= 1024 {
		for key := range i.stateAttempts {
			i.physicalAttemptLocked(key, start)
		}
	}
	if len(i.stateAttempts) >= 1024 {
		i.mu.Unlock()
		result.Error = "physical evidence capacity"
		return result
	}
	attempt := &physicalAttempt{until: startDeadline}
	i.stateAttempts[key] = attempt
	flow := i.flows[cmd.Flow.ConnectionId]
	i.mu.Unlock()

	ctx, cancel := context.WithTimeout(parent, 2*time.Second)
	reserved, err := i.stateWriter.Reserve(ctx, cmd)
	cancel()
	if err == nil && reserved != nil && reserved.Known && reserved.Outcome == pb.DeliveryResult_UNKNOWN {
		// Native state supplied no permission for this new memo to write. Its
		// UNKNOWN/retired effect can never return to authorized under this id.
		// Keep that outcome unknown without retaining invented local syscall
		// evidence; a real in-flight/partial syscall already has its own memo.
		i.mu.Lock()
		if i.stateAttempts[key] == attempt {
			delete(i.stateAttempts, key)
		}
		i.mu.Unlock()
		return result
	}
	// Redis computation cannot hold a stream shared by unrelated dialogs.
	// Reservation selects this one attempt; only framing/syscall uses the lock.
	if flow != nil {
		flow.write.Lock()
	}
	if err != nil {
		result.Outcome = pb.DeliveryResult_NOT_WRITTEN
		result.Error = "effect reservation unavailable or uncertain"
	} else if reserved == nil {
		result.Outcome = pb.DeliveryResult_NOT_WRITTEN
		result.Error = "invalid effect reservation"
	} else if reserved.Known {
		result.Outcome = reserved.Outcome
	} else if cmd.Permit.Authorization == nil || cmd.Permit.Authorization.DeadlineMs > 1<<53-1 || !time.Now().Before(startDeadline) || uint64(time.Now().UnixMilli()) >= cmd.Permit.Authorization.DeadlineMs {
		result.Outcome = pb.DeliveryResult_NOT_WRITTEN
		result.Error = "effect start deadline elapsed before socket write"
	} else if flow == nil || !proto.Equal(flow.ref, cmd.Flow) || flow.poisoned || flow.closed.Load() {
		result.Outcome = pb.DeliveryResult_NOT_WRITTEN
		result.Error = "authorized flow closed before write"
	} else if len(reserved.Wire) == 0 || len(reserved.Wire) > 65536 {
		result.Outcome = pb.DeliveryResult_NOT_WRITTEN
		result.Error = "invalid committed wire bound"
	} else if !flow.admitSIPUse() {
		result.Outcome = pb.DeliveryResult_NOT_WRITTEN
		result.Error = "unused flow expired before write"
	} else {
		n, writeErr := flow.writeWire(reserved.Wire)
		if writeErr == nil && n == len(reserved.Wire) {
			result.Outcome = pb.DeliveryResult_WRITTEN
			flow.retainedUntil = time.Now().Add(4 * time.Hour)
		} else {
			result.Error = "socket write failed or partial"
		}
	}
	if flow != nil {
		flow.write.Unlock()
	}
	i.mu.Lock()
	// A terminal Known result gave this new memo no socket-start permission.
	// Genuine partial/UNKNOWN attempts retain their original memo above.
	attempt.noStart = err != nil || reserved == nil || reserved.Known
	attempt.result = proto.Clone(result).(*pb.DeliveryResult)
	i.mu.Unlock()
	i.recordCompletion(parent, cmd, result, key, attempt)
	return result
}

func (i *Ingress) recordCompletion(parent context.Context, cmd *pb.FlowWrite, result *pb.DeliveryResult, key string, attempt *physicalAttempt) {
	// Losing this acknowledgement retains local physical evidence, including
	// after a flow closes. It cannot authorize another write on retry.
	ctx, cancel := context.WithTimeout(parent, 2*time.Second)
	err := i.stateWriter.Complete(ctx, cmd, result)
	cancel()
	recorded := err == nil
	if err != nil && attempt.noStart && result.Outcome == pb.DeliveryResult_NOT_WRITTEN {
		if inspector, ok := i.stateWriter.(StateInspector); ok {
			lookup, done := context.WithTimeout(parent, 2*time.Second)
			stored, inspectErr := inspector.InspectWrite(lookup, cmd)
			done()
			if inspectErr == nil && stored != nil && stored.Recorded && stored.Effect != nil && stored.Effect.Id == cmd.Effect.Id && stored.Effect.AttemptId == cmd.Effect.AttemptId &&
				stateSameContext(stored.Effect.Context, cmd.Effect.Context) && (stored.Outcome == pb.DeliveryResult_WRITTEN || stored.Outcome == pb.DeliveryResult_NOT_WRITTEN) {
				// A removed earlier memo may already have recorded this exact attempt.
				// Only that authoritative terminal fact can replace this new memo's
				// local no-syscall absence; no reservation or socket call is repeated.
				result.Outcome, result.Error, result.Recorded = stored.Outcome, stored.Error, true
				result.Effect = proto.Clone(stored.Effect).(*pb.StateEffectRef)
				recorded = true
			}
		}
	}
	if recorded && result.Outcome != pb.DeliveryResult_UNKNOWN {
		result.Recorded = true
		i.mu.Lock()
		if i.stateAttempts[key] == attempt {
			delete(i.stateAttempts, key)
		}
		i.mu.Unlock()
	}
}
