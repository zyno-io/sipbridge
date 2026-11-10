package engine

import (
	"errors"

	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/state"
)

func (e *StateEngine) receiverEffect(kind string) bool {
	return e.autonomousCompletion() && (kind == "present" || kind == "event")
}

// Nil-executor receiver effects may have been produced by a different CAS worker.
// Their authority is the retained receiver identity and current native owner,
// never the process that happened to perform the control RPC.
func (r *stateReduction) receiverResultOwner(effect state.Effect, body stateEngineEffect, c *stateContext) error {
	if effect.Status != state.EffectPending && effect.Status != state.EffectUnknown {
		return errors.New("receiver effect cannot complete")
	}
	var snapshot *pb.ContextSnapshot
	if effect.Kind == "present" {
		snapshot = body.Envelope.GetRequest().GetSnapshot()
	} else if effect.Kind == "event" {
		snapshot = body.Envelope.GetDialogEvent().GetSnapshot()
	}
	if snapshot == nil || body.Envelope.GetCorrelationId() != effect.ID || !stateSameContext(snapshot.Context, c.Snapshot.Context) ||
		!sameProcess(snapshot.Application, body.Application) || snapshot.ApplicationLeaseTerm != c.Snapshot.ApplicationLeaseTerm {
		return errors.New("receiver effect owner mismatch")
	}
	_, err := r.effectNativeOwner(body)
	return err
}
