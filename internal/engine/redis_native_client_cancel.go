package engine

import (
	"context"
	"errors"
	"time"

	"google.golang.org/protobuf/proto"
	"sipbridge/internal/control"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/rediscore"
	"sipbridge/internal/state"
)

// commandClientCancel retains only the original bounded command attempt; the
// resulting INVITE and paired transaction remain native obligations.
func (e *redisNative) commandClientCancel(parent context.Context, peer *control.Peer, command *pb.Command, operation string, deadline time.Time) (*pb.OperationResult, error) {
	if command == nil || command.GetCancel() == nil || command.Context == nil || command.Context.State == nil || !state.ValidReference(command.Context.Id) || command.Context.State.GroupId != command.Context.Id || command.Context.State.Partition != 0 || !state.ValidReference(command.Context.State.Incarnation) || !proto.Equal(command.Context.State.Authority, e.StateAuthority()) || command.Sequence == 0 || command.Sequence > 1<<53-1 || !validSendText(operation, 160, false) {
		return &pb.OperationResult{Certainty: pb.OperationResult_NOT_ACCEPTED, Error: "invalid CANCEL command"}, nil
	}
	lease, err := e.controllerWitness(peer)
	if err != nil {
		return &pb.OperationResult{Certainty: pb.OperationResult_NOT_ACCEPTED, Error: "controller attachment unavailable"}, nil
	}
	capture, err := captureRedisNotifyWait(command)
	if err != nil {
		return nil, err
	}
	ctx, cancel, store, err := e.operation(parent, deadline)
	if err != nil {
		return nil, err
	}
	defer cancel()
	executor := rediscore.TimerAssignment{Worker: e.worker, Epoch: e.cfg.Process.Epoch, Term: e.cfg.Process.Epoch}
	for attempts := 0; attempts < 8; attempts++ {
		selected, x := store.ObserveApplicationContext(ctx, command.Context.Id, command.Context.State.Incarnation, lease, "r:context")
		if x != nil {
			return nil, x
		}
		current, x := decodeNativeContext(selected.View.Records["r:context"])
		if x != nil {
			return nil, x
		}
		if selected.Identity.Owner != lease.Application || selected.Identity.Term != lease.Term || current.Context == nil || !proto.Equal(current.Context.State, command.Context.State) || !sameProcess(current.Application, peer.Application()) || current.ApplicationLeaseTerm != lease.Term {
			return nil, rediscore.ErrAuthority
		}
		if command.Sequence <= current.CommandSequence {
			return &pb.OperationResult{Certainty: pb.OperationResult_UNKNOWN, Error: "command sequence already consumed"}, nil
		}
		if current.CommandSequence >= 1<<53-1 || current.CommandSequence+1 != command.Sequence {
			return &pb.OperationResult{Certainty: pb.OperationResult_NOT_ACCEPTED, Error: "stale command sequence or sequence gap"}, nil
		}
		result, x := publishRedisClientCancel(ctx, store, e.cfg.StateIdentityKey, peer.Application(), lease, command, capture, operation, executor, deadline)
		if x == nil {
			return result, nil
		}
		if errors.Is(x, rediscore.ErrConflict) && ctx.Err() == nil && time.Now().Before(deadline) && attempts < 7 {
			continue
		}
		if errors.Is(x, errStatePolicyAdmission) || errors.Is(x, rediscore.ErrCapacity) {
			return &pb.OperationResult{Certainty: pb.OperationResult_NOT_ACCEPTED, Error: x.Error()}, nil
		}
		return nil, x
	}
	return nil, rediscore.ErrConflict
}
