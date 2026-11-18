package engine

import (
	"context"
	"errors"
	"slices"
	"strconv"
	"time"

	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
	"sipbridge/internal/control"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/rediscore"
	"sipbridge/internal/state"
)

// commandInitialInviteRespond is a private qualification path. No serving
// selector uses it until the complete UAS lifecycle has qualified.
func (e *redisNative) commandInitialInviteRespond(parent context.Context, peer *control.Peer, command *pb.Command, operation string, deadline time.Time) (*pb.OperationResult, error) {
	if command == nil || command.GetRespond() == nil || command.Context == nil || command.Context.State == nil || !state.ValidReference(command.Context.Id) || command.Context.State.GroupId != command.Context.Id || command.Context.State.Partition != 0 || !state.ValidReference(command.Context.State.Incarnation) || !proto.Equal(command.Context.State.Authority, e.StateAuthority()) || command.Sequence < 2 || command.Sequence > 1<<53-1 || !validSendText(operation, 160, false) {
		return &pb.OperationResult{Certainty: pb.OperationResult_NOT_ACCEPTED, Error: "invalid INVITE response command"}, nil
	}
	requestID := command.GetRespond().RequestId
	parsed, err := uuid.Parse(requestID)
	if err != nil || parsed.String() != requestID {
		return &pb.OperationResult{Certainty: pb.OperationResult_NOT_ACCEPTED, Error: "invalid INVITE request identity"}, nil
	}
	witness, err := e.controllerWitness(peer)
	if err != nil {
		return &pb.OperationResult{Certainty: pb.OperationResult_NOT_ACCEPTED, Error: "controller attachment unavailable"}, nil
	}
	capture, err := captureRedisNotifyWait(command)
	if err != nil {
		return nil, err
	}
	key := rediscore.ResultHint{Context: command.Context.Id, Incarnation: command.Context.State.Incarnation, Sequence: command.Sequence}
	waiter, err := e.registerResultWaiter(key)
	if err != nil {
		return &pb.OperationResult{Certainty: pb.OperationResult_NOT_ACCEPTED, Error: "state execution capacity"}, nil
	}
	defer e.releaseResultWaiter(key, waiter)
	store, workerLease, until, err := e.driver.Current()
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithDeadline(parent, deadline)
	defer cancel()
	stop := context.AfterFunc(workerLease, cancel)
	defer stop()
	var identity rediscore.Identity
	var inDialog bool
	for attempts := 0; attempts < 3; attempts++ {
		field := "r:result-" + strconv.FormatUint(key.Sequence, 10)
		selected, x := store.ObserveApplicationContext(ctx, key.Context, key.Incarnation, witness, "r:context", field)
		if x != nil {
			return nil, x
		}
		current, x := decodeNativeContext(selected.View.Records["r:context"])
		if x != nil {
			return nil, x
		}
		if selected.Identity.Owner != witness.Application || selected.Identity.Term != witness.Term || current.Context == nil || !proto.Equal(current.Context.State, command.Context.State) || current.Kind != pb.ContextSnapshot_UAS && current.Kind != pb.ContextSnapshot_UAC || !sameProcess(current.Application, peer.Application()) || current.ApplicationLeaseTerm != witness.Term {
			return nil, rediscore.ErrAuthority
		}
		identity = selected.Identity
		if command.Sequence <= current.CommandSequence {
			return &pb.OperationResult{Certainty: pb.OperationResult_UNKNOWN, Error: "command sequence already consumed"}, nil
		}
		inDialog = slices.Contains(current.NoninitialProcedures, "r:procedure-"+requestID)
		if !inDialog && current.Kind != pb.ContextSnapshot_UAS {
			return nil, rediscore.ErrAuthority
		}
		if current.CommandSequence >= 1<<53-1 || current.CommandSequence+1 != command.Sequence {
			return &pb.OperationResult{Certainty: pb.OperationResult_NOT_ACCEPTED, Error: "stale command sequence or sequence gap"}, nil
		}
		if inDialog {
			identity, x = publishRedisDialogRespond(ctx, store, e.cfg.StateIdentityKey, peer.Application(), witness, command, capture, operation, nativeSIPAdvertisement{contactURI: e.cfg.ContactURI, address: e.cfg.AdvertiseAddress, plainAddress: e.cfg.PlainAdvertiseAddress}, rediscore.TimerAssignment{Worker: e.worker, Epoch: e.cfg.Process.Epoch, Term: e.cfg.Process.Epoch}, deadline)
		} else {
			contact, sentBy, advertisementErr := advertisement(e.cfg, current.Transport)
			if advertisementErr != nil {
				return nil, advertisementErr
			}
			identity, x = publishRedisInviteRespond(ctx, store, e.cfg.StateIdentityKey, peer.Application(), witness, command, capture, operation, contact, sentBy, rediscore.TimerAssignment{Worker: e.worker, Epoch: e.cfg.Process.Epoch, Term: e.cfg.Process.Epoch}, deadline)
		}
		if x == nil {
			break
		}
		if errors.Is(x, rediscore.ErrConflict) && ctx.Err() == nil && time.Now().Before(deadline) {
			if attempts == 2 {
				return nil, x
			}
			continue
		}
		if errors.Is(x, errStatePolicyAdmission) || errors.Is(x, rediscore.ErrCapacity) {
			return &pb.OperationResult{Certainty: pb.OperationResult_NOT_ACCEPTED, Error: x.Error()}, nil
		}
		return nil, x
	}
	command = nil
	return e.awaitRedisInviteRespondResult(ctx, store, identity, witness, capture, requestID, operation, deadline, until, waiter, inDialog)
}
