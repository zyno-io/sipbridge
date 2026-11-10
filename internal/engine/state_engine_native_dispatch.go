package engine

import (
	"context"

	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/state"
)

func (e *StateEngine) dispatchNativeCompletion(ctx context.Context, ref state.Ref, effect state.Effect, body stateEngineEffect, release func()) {
	if body.Permit == nil || body.Authorization == nil || body.Permit.Flow == nil ||
		!sameProcess(body.Permit.Flow.Ingress, e.cfg.Process) {
		return // The exact ingress discovers the same authoritative effect through Redis.
	}
	write := &pb.FlowWrite{Flow: body.Permit.Flow, Effect: body.Authorization.Effect, Permit: body.Permit}
	var delivery *pb.DeliveryResult
	if effect.Status == state.EffectAuthorized {
		if e.cfg.WriteCommitted == nil {
			return
		}
		delivery = e.cfg.WriteCommitted(ctx, write)
	} else {
		if e.cfg.InspectCommitted == nil {
			return
		}
		delivery = e.cfg.InspectCommitted(ctx, write)
	}
	if delivery == nil || !delivery.Recorded ||
		(delivery.Outcome != pb.DeliveryResult_WRITTEN && delivery.Outcome != pb.DeliveryResult_NOT_WRITTEN) {
		return
	}
	// Complete already committed native settlement and normal reclamation.
	release()
	if effect.Status != state.EffectAuthorized {
		e.dispatchAfterEffect(ref, effect.ID, effect.Kind)
	}
}
