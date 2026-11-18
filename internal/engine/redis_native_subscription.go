package engine

import (
	"context"
	"errors"
	"slices"
	"strconv"
	"time"

	"google.golang.org/protobuf/proto"
	"sipbridge/internal/control"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/rediscore"
	"sipbridge/internal/state"
)

func (e *redisNative) decideDirect(parent context.Context, peer *control.Peer, decision *pb.RequestDecision, operation string, deadline time.Time) (*pb.OperationResult, error) {
	witness, err := e.controllerWitness(peer)
	if err != nil {
		return &pb.OperationResult{Certainty: pb.OperationResult_NOT_ACCEPTED, Error: "controller attachment unavailable"}, nil
	}
	if decision == nil || decision.Context == nil || decision.Context.State == nil || !state.ValidReference(decision.Context.Id) || decision.Context.State.GroupId != decision.Context.Id || decision.Context.State.Partition != 0 || !state.ValidReference(decision.Context.State.Incarnation) || !proto.Equal(decision.Context.State.Authority, e.StateAuthority()) || decision.Sequence == 0 || decision.Sequence > 1<<53-1 || !state.ValidReference(decision.RequestId) || len(decision.RequestId) > 180 || !validSendText(operation, 160, false) {
		return &pb.OperationResult{Certainty: pb.OperationResult_NOT_ACCEPTED, Error: "invalid direct request decision"}, nil
	}
	ctx, cancel, store, err := e.operation(parent, deadline)
	if err != nil {
		return nil, err
	}
	defer cancel()
	originField := "r:origin-" + decision.RequestId
	fields := []string{"r:context", "r:procedure-" + decision.RequestId, originField, "r:result-" + strconv.FormatUint(decision.Sequence, 10)}
	if decision.Sequence > 1 {
		fields = append(fields, "r:result-"+strconv.FormatUint(decision.Sequence-1, 10))
	}

	for attempt := 0; attempt < 3; attempt++ {
		selected, err := store.ObserveApplicationWitness(ctx, decision.Context.Id, decision.Context.State.Incarnation, witness, fields...)
		if err != nil {
			return nil, err
		}
		current, err := decodeNativeContext(selected.View.Records["r:context"])
		if err != nil {
			return nil, err
		}
		if decision.Sequence <= current.CommandSequence {
			if current.Context == nil || !proto.Equal(current.Context.State, decision.Context.State) || selected.Identity.Owner != "" && (selected.Identity.Owner != witness.Application || selected.Identity.Term != witness.Term) {
				return nil, rediscore.ErrAuthority
			}
			return &pb.OperationResult{Certainty: pb.OperationResult_UNKNOWN, Error: "command sequence already consumed"}, nil
		}
		procedureField := "r:procedure-" + decision.RequestId
		inDialog := slices.Contains(current.NoninitialProcedures, procedureField)
		if inDialog {

			candidate, prepareErr := prepareRedisDialogDecision(ctx, store, selected, current, e.cfg.StateIdentityKey, peer.Application(), witness, decision, operation, deadline)
			if redisDirectDecisionConflict(nil, prepareErr) && candidate.selected == nil && ctx.Err() == nil && time.Now().Before(deadline) {
				// Preparation has only selected records; neither publication helper
				// ran. Reobserve authority and sequence under the original deadline.
				continue
			}
			var result *pb.OperationResult
			if prepareErr != nil {
				result, err = refuseRedisDialogDecision(ctx, store, candidate, witness, decision, operation, rediscore.TimerAssignment{Worker: e.worker, Epoch: e.cfg.Process.Epoch, Term: e.cfg.Process.Epoch}, deadline, prepareErr)
			} else {
				result, err = commitRedisDialogDecision(ctx, store, candidate, e.cfg.StateIdentityKey, witness, decision, operation, nativeSIPAdvertisement{contactURI: e.cfg.ContactURI, address: e.cfg.AdvertiseAddress, plainAddress: e.cfg.PlainAdvertiseAddress}, rediscore.TimerAssignment{Worker: e.worker, Epoch: e.cfg.Process.Epoch, Term: e.cfg.Process.Epoch}, deadline)
			}
			if redisDirectDecisionConflict(result, err) && ctx.Err() == nil && time.Now().Before(deadline) {
				// A plain conflict is a definite pre-write rejection. Both
				// helpers may mutate the candidate, so select a fresh one.
				continue
			}
			return result, err
		}
		if current.Kind == pb.ContextSnapshot_UAS {

			if decision.Status == 0 {
				return commitRedisInviteDeferral(ctx, store, e.cfg.StateIdentityKey, peer.Application(), witness, decision, operation, deadline)
			}
			contact, sentBy, advertiseErr := advertisement(e.cfg, current.Transport)
			if advertiseErr != nil {
				return nil, advertiseErr
			}
			return commitRedisInviteFinal(ctx, store, e.cfg.StateIdentityKey, peer.Application(), witness, decision, operation, contact, sentBy, deadline)
		}
		return nil, errors.New("direct decision requires shared call context")
	}
	return nil, rediscore.ErrConflict
}

// redisDirectDecisionConflict excludes both returned dispositions and any
// uncertainty, even when an underlying error also matches ErrConflict.
func redisDirectDecisionConflict(result *pb.OperationResult, err error) bool {
	return result == nil && errors.Is(err, rediscore.ErrConflict) && !errors.Is(err, rediscore.ErrUnknown)
}
