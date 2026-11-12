package execution

import "sipbridge/internal/state"

// Result separates the retained replay receipt from transient dispatch hints.
// A nil projection includes replay/uncertain inspection, read-only and legacy paths.
type Result struct {
	Receipt   state.Receipt
	Committed *Committed
}

// Committed is an owned projection of one proven commit, never an authoritative
// protocol aggregate. Local observations are optional bounded public result DTOs.
// Its presence with zero effects is different from absent dispatch evidence.
type Committed struct {
	Ref               state.Ref
	Revision          string
	Effects           []state.Effect
	LocalObservations []state.LocalObservation
}

func committedEffects(ref state.Ref, revision string, payload state.Payload) *Committed {
	projection := &Committed{Ref: ref, Revision: revision, Effects: append([]state.Effect(nil), payload.Effects...),
		LocalObservations: append([]state.LocalObservation(nil), payload.LocalObservations...)}
	for n := range projection.Effects {
		effect := &projection.Effects[n]
		effect.Payload = append([]byte(nil), effect.Payload...)
		if effect.ApplicationLease != nil {
			lease := *effect.ApplicationLease
			effect.ApplicationLease = &lease
		}
	}
	for n := range projection.LocalObservations {
		projection.LocalObservations[n].Outcome = append([]byte(nil), projection.LocalObservations[n].Outcome...)
	}
	return projection
}
