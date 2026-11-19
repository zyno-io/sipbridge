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

// commandApplicationAnswerACK keeps only the original bounded command waiter.
// The committed ACK and its native completion continue independently.
func (e *redisNative) commandApplicationAnswerACK(parent context.Context, peer *control.Peer, command *pb.Command, operation string, deadline time.Time) (*pb.OperationResult, error) {
	if command == nil || command.GetDialogRequest() == nil || command.GetDialogRequest().Method != "ACK" || command.Context == nil || command.Context.State == nil || !state.ValidReference(command.Context.Id) || command.Context.State.GroupId != command.Context.Id || command.Context.State.Partition != 0 || !state.ValidReference(command.Context.State.Incarnation) || !proto.Equal(command.Context.State.Authority, e.StateAuthority()) || command.Sequence == 0 || command.Sequence > 1<<53-1 || !validSendText(operation, 160, false) {
		return &pb.OperationResult{Certainty: pb.OperationResult_NOT_ACCEPTED, Error: "invalid ACK command"}, nil
	}
	lease, err := e.controllerWitness(peer)
	if err != nil {
		return &pb.OperationResult{Certainty: pb.OperationResult_NOT_ACCEPTED, Error: "controller attachment unavailable"}, nil
	}
	capture, err := captureRedisNotifyWait(command)
	if err != nil {
		return nil, err
	}
	hint := rediscore.ResultHint{Context: command.Context.Id, Incarnation: command.Context.State.Incarnation, Sequence: command.Sequence}
	waiter, err := e.registerResultWaiter(hint)
	if err != nil {
		return &pb.OperationResult{Certainty: pb.OperationResult_NOT_ACCEPTED, Error: "state execution capacity"}, nil
	}
	defer e.releaseResultWaiter(hint, waiter)
	store, workerLease, until, err := e.driver.Current()
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithDeadline(parent, deadline)
	defer cancel()
	stop := context.AfterFunc(workerLease, cancel)
	defer stop()
	executor := rediscore.TimerAssignment{Worker: e.worker, Epoch: e.cfg.Process.Epoch, Term: e.cfg.Process.Epoch}
	var identity rediscore.Identity
	for attempts := 0; attempts < 8; attempts++ {
		observed, x := store.ObserveApplicationContext(ctx, hint.Context, hint.Incarnation, lease, "r:context")
		if x != nil {
			return nil, x
		}
		current, x := decodeNativeContext(observed.View.Records["r:context"])
		if x != nil {
			return nil, x
		}
		if observed.Identity.Owner != lease.Application || observed.Identity.Term != lease.Term || current.Context == nil || !proto.Equal(current.Context.State, command.Context.State) || !sameProcess(current.Application, peer.Application()) || current.ApplicationLeaseTerm != lease.Term {
			return nil, rediscore.ErrAuthority
		}
		if command.Sequence <= current.CommandSequence {
			return &pb.OperationResult{Certainty: pb.OperationResult_UNKNOWN, Error: "command sequence already consumed"}, nil
		}
		if current.CommandSequence >= 1<<53-1 || current.CommandSequence+1 != command.Sequence {
			return &pb.OperationResult{Certainty: pb.OperationResult_NOT_ACCEPTED, Error: "stale command sequence or sequence gap"}, nil
		}
		identity, x = publishRedisApplicationAnswerACK(ctx, store, e.cfg.StateIdentityKey, peer.Application(), lease, command, capture, operation, executor, deadline)
		if x == nil {
			break
		}
		if errors.Is(x, rediscore.ErrConflict) && ctx.Err() == nil && time.Now().Before(deadline) && attempts < 7 {
			continue
		}
		if errors.Is(x, errStatePolicyAdmission) || errors.Is(x, rediscore.ErrCapacity) {
			return commandRefusal(x), nil
		}
		return nil, x
	}
	return e.awaitRedisApplicationByeResult(ctx, store, identity, lease, capture, operation, deadline, until, waiter, "ACK")
}
