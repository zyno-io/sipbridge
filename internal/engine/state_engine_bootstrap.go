package engine

import (
	"context"
	"errors"

	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/state"
)

func (e *StateEngine) bootstrapGroup(application *pb.ProcessRef, placeholder *pb.ContextRef) (state.Ref, error) {
	if application == nil || placeholder == nil || placeholder.State != nil || !state.ValidReference(placeholder.Id) || !state.ValidReference(application.InstanceId) || !state.ValidReference(application.Epoch) {
		return state.Ref{}, errors.New("invalid creation inspection identity")
	}
	callID := e.id("outbound-call", application.InstanceId, application.Epoch, placeholder.Id) + "@sipbridge"
	return e.store.GroupForCallID(callID)
}

func (e *StateEngine) inspectContext(ctx context.Context, application *pb.ProcessRef, ref *pb.ContextRef) (*pb.OperationResult, error) {
	var group state.Ref
	var err error
	if ref != nil && ref.State == nil {
		group, err = e.bootstrapGroup(application, ref)
	} else {
		group, err = e.group(ref)
	}
	if err != nil {
		return nil, err
	}
	record, err := e.store.Load(ctx, group)
	if errors.Is(err, state.ErrNotFound) {
		return e.retiredContextResult(ref, pb.ContextRetirementProof_GROUP_ABSENT), nil
	}
	if errors.Is(err, state.ErrStale) && e.autonomousCompletion() && ref.GetState() != nil {
		current := group
		current.Incarnation = ""
		successor, readErr := e.store.Load(ctx, current)
		if errors.Is(readErr, state.ErrNotFound) {
			return e.retiredContextResult(ref, pb.ContextRetirementProof_GROUP_ABSENT), nil
		}
		if readErr == nil && successor.Ref.Incarnation != ref.State.Incarnation {
			return e.retiredContextResult(ref, pb.ContextRetirementProof_OBSOLETE_INCARNATION), nil
		}
		if readErr != nil {
			return nil, readErr
		}
	}
	if err != nil {
		return nil, err
	}
	g, err := decodeStateGroup(record)
	if err != nil {
		return nil, err
	}
	c := g.Contexts[ref.Id]
	if c != nil && !sameProcess(c.Snapshot.Application, application) && c.Snapshot.Kind != pb.ContextSnapshot_SUBSCRIPTION {
		return nil, errors.New("application owner mismatch")
	}
	uncertain := false
	if record.Pending != nil {
		var pending stateEngineInput
		if err := pending.UnmarshalBinary(record.Pending.Payload); err != nil {
			return nil, state.ErrCorrupt
		}
		// Sequence discovery reads committed state. A pending internal effect or
		// timer cannot allocate a public command sequence; its unfinished work
		// remains serialized ahead of any subsequent command by the driver.
		uncertain = c == nil || pending.Kind == "command" && pending.Context != nil && pending.Context.Id == ref.Id
	}
	if uncertain {
		result := &pb.OperationResult{Certainty: pb.OperationResult_UNKNOWN, Error: "native operation pending"}
		if c != nil {
			result.Snapshot = stateSnapshot(c, e.cfg.Process)
		}
		return result, nil
	}
	if c == nil {
		if e.autonomousCompletion() {
			if _, reserved := g.Allocations[ref.Id]; reserved {
				return &pb.OperationResult{Certainty: pb.OperationResult_UNKNOWN, Error: "native admission reservation pending"}, nil
			}
			if e.allocatedContext(ref) || g.OutboundID == ref.Id {
				return e.retiredContextResult(ref, pb.ContextRetirementProof_CONTEXT_ABSENT), nil
			}
		}
		return &pb.OperationResult{Certainty: pb.OperationResult_NOT_ACCEPTED, Error: "context not found"}, nil
	}
	result := &pb.OperationResult{Certainty: pb.OperationResult_COMPLETED}
	if c.Result != nil {
		result = proto.Clone(c.Result).(*pb.OperationResult)
	}
	projectWrittenPublicBye(c, result)
	result.Snapshot = stateSnapshot(c, e.cfg.Process)
	if err := stateMaterializeResultMessages(result); err != nil {
		return nil, err
	}
	return result, nil
}

func (e *StateEngine) retiredContextResult(ref *pb.ContextRef, reason pb.ContextRetirementProof_Reason) *pb.OperationResult {
	result := &pb.OperationResult{Certainty: pb.OperationResult_NOT_ACCEPTED, Error: "context not found"}
	if e.autonomousCompletion() && ref.GetState() != nil && ref.State.Incarnation != "" {
		result.Certainty, result.Error = pb.OperationResult_COMPLETED, ""
		result.RetirementProof = &pb.ContextRetirementProof{Context: proto.Clone(ref).(*pb.ContextRef), Authority: e.StateAuthority(), Reason: reason}
	}
	return result
}

func (e *StateEngine) resultForExecutor(result *pb.OperationResult) *pb.OperationResult {
	if result == nil || result.Snapshot == nil {
		return result
	}
	reply := proto.Clone(result).(*pb.OperationResult)
	reply.Snapshot.Executor = proto.Clone(e.cfg.Process).(*pb.ProcessRef)
	if reply.Snapshot.Context != nil {
		reply.Snapshot.Context.Owner = proto.Clone(e.cfg.Process).(*pb.ProcessRef)
	}
	return reply
}
