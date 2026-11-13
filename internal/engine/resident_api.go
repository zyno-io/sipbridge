package engine

import (
	"context"
	"errors"
	"time"

	"google.golang.org/protobuf/proto"
	"sipbridge/internal/control"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/state"
)

// Handle admits bounded control work without holding a context lock during the
// request or its reply. Completion APIs have their own reserved state capacity
// and are never blocked behind this application-work limit.
func (e *ResidentMemory) Handle(peer *control.Peer, envelope *pb.Envelope) error {
	if peer == nil || envelope == nil || envelope.CorrelationId == "" {
		return errors.New("control identity required")
	}
	deadline := time.Now().Add(time.Duration(envelope.RemainingBudgetMs) * time.Millisecond)
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return errors.New("resident engine closed")
	}
	work := e.work
	if envelope.GetIngest() != nil {
		work = e.ingressWork
	}
	select {
	case work <- struct{}{}:
	default:
		e.mu.Unlock()
		return peer.Send(&pb.Envelope{CorrelationId: envelope.CorrelationId, Payload: &pb.Envelope_Result{Result: &pb.OperationResult{Certainty: pb.OperationResult_NOT_ACCEPTED, Error: "state execution capacity"}}})
	}
	e.wg.Add(1)
	e.mu.Unlock()
	go func() {
		defer e.wg.Done()
		defer func() { <-work }()
		ctx, cancel := context.WithDeadline(e.ctx, deadline)
		defer cancel()
		result, err := e.handleResident(ctx, peer, envelope, deadline)
		if err != nil {
			result = &pb.OperationResult{Certainty: pb.OperationResult_UNKNOWN, Error: err.Error()}
		}
		_ = peer.Send(&pb.Envelope{CorrelationId: envelope.CorrelationId, Payload: &pb.Envelope_Result{Result: result}})
	}()
	return nil
}

func (e *ResidentMemory) handleResident(ctx context.Context, peer *control.Peer, envelope *pb.Envelope, deadline time.Time) (*pb.OperationResult, error) {
	if err := e.validateController(ctx, peer); err != nil {
		return &pb.OperationResult{Certainty: pb.OperationResult_NOT_ACCEPTED, Error: "controller attachment unavailable"}, nil
	}
	switch body := envelope.Payload.(type) {
	case *pb.Envelope_Ingest:
		return e.ingest(ctx, peer, body.Ingest, deadline)
	case *pb.Envelope_Command:
		return e.command(ctx, peer, body.Command, envelope.CorrelationId, deadline)
	case *pb.Envelope_Inspect:
		return e.inspectResident(ctx, peer.Application(), body.Inspect.GetContext())
	default:
		return &pb.OperationResult{Certainty: pb.OperationResult_NOT_ACCEPTED, Error: "unsupported resident engine message"}, nil
	}
}

func (e *ResidentMemory) inspectResident(ctx context.Context, application *pb.ProcessRef, ref *pb.ContextRef) (*pb.OperationResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if ref == nil {
		return nil, state.ErrAuthority
	}
	if ref.State == nil {
		if !validSendProcess(application) || !state.ValidReference(ref.Id) {
			return nil, state.ErrAuthority
		}
		// Outbound creation uses the supplied context ID. The application
		// identity is checked before returning any resident snapshot.
		e.mu.Lock()
		c := e.contexts[ref.Id]
		e.mu.Unlock()
		if c == nil {
			return &pb.OperationResult{Certainty: pb.OperationResult_NOT_ACCEPTED, Error: "context not found"}, nil
		}
		c.mu.Lock()
		defer c.mu.Unlock()
		return e.inspectResidentLocked(c, application)
	}
	c, err := e.findContext(ref)
	if errors.Is(err, state.ErrNotFound) {
		return &pb.OperationResult{Certainty: pb.OperationResult_COMPLETED, RetirementProof: &pb.ContextRetirementProof{
			Context: proto.Clone(ref).(*pb.ContextRef), Authority: e.StateAuthority(), Reason: pb.ContextRetirementProof_CONTEXT_ABSENT}}, nil
	}
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if !e.residentContextCurrentLocked(c, ref) {
		return nil, state.ErrStale
	}
	return e.inspectResidentLocked(c, application)
}

func (e *ResidentMemory) inspectResidentLocked(c *residentContext, application *pb.ProcessRef) (*pb.OperationResult, error) {
	if c.snapshot == nil || !sameProcess(c.snapshot.Application, application) {
		return nil, errors.New("application owner mismatch")
	}
	if c.command != nil {
		return residentCommandResultLocked(c, c.command), nil
	}
	return &pb.OperationResult{Certainty: pb.OperationResult_COMPLETED, Snapshot: proto.Clone(c.snapshot).(*pb.ContextSnapshot)}, nil
}

func (e *ResidentMemory) LocateRoute(ctx context.Context, message *pb.SipMessage) (*pb.ContextSnapshot, error) {
	return e.locate(ctx, message)
}
func (e *ResidentMemory) Reject(ctx context.Context, flow *pb.FlowRef, message *pb.SipMessage, status uint32) error {
	return e.reject(ctx, flow, message, status)
}
func (e *ResidentMemory) TryNativeCompletion(ctx context.Context, flow *pb.FlowRef, message *pb.SipMessage) (bool, error) {
	return e.tryNativeCompletion(ctx, flow, message)
}
