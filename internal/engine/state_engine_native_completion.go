package engine

import (
	"errors"

	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/state"
)

func (e *StateEngine) nativeCompletionScope(ref *pb.ContextRef) string {
	if ref == nil || ref.State == nil || ref.State.Authority == nil {
		return ""
	}
	a := ref.State.Authority
	return e.id("native-completion-v1", a.Service, a.Generation, a.PrimaryId, ref.State.GroupId, ref.State.Incarnation, ref.State.CallId, ref.Id)
}

// Only reducer-produced native obligations enter this path. Public control
// commands cannot supply effects, classes, scopes, writer destinations or wire.
// Authority and physical outcome remain in the existing immutable effect record.
func (r *stateReduction) prepareAutonomousEffects() error {
	if !r.engine.autonomousCompletion() {
		return nil
	}
	for index := 0; index < len(r.payload.Effects); index++ {
		effect := r.payload.Effects[index]
		if effect.Kind != "wire" && effect.Kind != "deliver" {
			continue
		}
		var body stateEngineEffect
		if body.UnmarshalJSON(effect.Payload) != nil {
			return state.ErrCorrupt
		}
		c := r.group.Contexts[body.Context]
		if c == nil || body.Authorization == nil {
			return state.ErrCorrupt
		}
		applicationPolicy := false
		if effect.Kind == "wire" && body.Authorization.AuthorityClass == pb.EffectAuthorization_APPLICATION {
			if c.OwnerLost && body.Permit == nil {
				// Authorization may exist in the trusted ledger, but this stage has
				// never committed physical permission in SIP Redis. Cancel the exact
				// intent so even a late old permit cannot reserve a socket write.
				body.Cancelled, body.Settled = true, true
				body.Executor, body.Envelope, body.Wire = nil, nil, nil
				effect.Status = state.EffectCancelled
				r.setEffect(index, effect, body)
				continue
			}
			tx := c.Transactions[body.Transaction]
			applicationPolicy = tx != nil && tx.Policy != nil && tx.Policy.Command == body.Command
			if !applicationPolicy {
				continue // Old commands keep the temporary trusted-permit path.
			}
		}
		if effect.Kind == "wire" && effect.Status == state.EffectPending {
			request := body.Envelope.GetEgress()
			if request == nil || request.Snapshot == nil || request.Message == nil || body.Flow == nil ||
				body.Authorization.CompletionScope != r.engine.nativeCompletionScope(c.Snapshot.Context) ||
				!proto.Equal(request.Authorization, body.Authorization) || !stateSameContext(request.Snapshot.Context, c.Snapshot.Context) {
				return errors.New("native completion obligation authority mismatch")
			}
			fingerprint, err := SendFingerprint(request.Snapshot, request.Message, body.Authorization, body.Flow)
			if err != nil || fingerprint != body.Authorization.Fingerprint || effect.ID != "e-"+fingerprint {
				return errors.New("native completion obligation fingerprint mismatch")
			}
			body.Native = true
			body.Permit = &pb.SendPermit{Authorization: proto.Clone(body.Authorization).(*pb.EffectAuthorization),
				Flow: proto.Clone(body.Flow).(*pb.FlowRef), DirectoryEpoch: r.engine.StateAuthority().PrimaryId,
				OrderingProof: r.engine.id("native-write-authority-v1", fingerprint), CompletionScope: body.Authorization.CompletionScope}
			if applicationPolicy {
				body.Permit.DirectoryEpoch = c.Transactions[body.Transaction].Policy.Decision.DirectoryEpoch
			}
			body.Executor, body.Envelope = nil, nil
			effect.Kind, effect.Status = "deliver", state.EffectAuthorized
			if !applicationPolicy {
				effect.ApplicationLease = nil // Native completion survives its admitted application's fence.
			}
			r.setEffect(index, effect, body)
			continue
		}
		if effect.Kind == "deliver" && body.Native && effect.Status == state.EffectAuthorized && effect.DeadlineMS <= r.now {
			// Redis still proves no reservation exists. A competing physical
			// reservation would change the revision/status before this commit.
			effect.Status = state.EffectNotWritten
			effect.AttemptID = body.Authorization.GetEffect().GetAttemptId()
			body.Settled = true
			r.setEffect(index, effect, body)
			r.releaseNoStartServerFinal(c, effect, body)
			r.responseCommandNoStart(c, effect, body)
			if err := r.releaseExpiredACKCleanup(c, body, effect.DeadlineMS); err != nil {
				return err
			}
			continue
		}
		if effect.Kind == "deliver" && (body.Native || c.OwnerLost && c.Snapshot.Ended) &&
			(effect.Status == state.EffectWritten || effect.Status == state.EffectNotWritten) {
			// This acknowledges native physical evidence only. Trusted application
			// bookkeeping remains explicit deferred debt until Controller removes it.
			body.Settled = true
			r.setEffect(index, effect, body)
		}
	}
	return nil
}
