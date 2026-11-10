package execution

import "sipbridge/internal/state"

// Result separates the retained replay receipt from transient dispatch hints.
// A nil projection includes replay/uncertain inspection, read-only and legacy paths.
type Result struct {
	Receipt   state.Receipt
	Committed *Committed
}

// Committed is an owned projection of one proven commit, never a protocol snapshot.
// Its presence with zero effects is different from absent dispatch evidence.
type Committed struct {
	Ref     state.Ref
	Effects []state.Effect
}

func committedEffects(ref state.Ref, effects []state.Effect) *Committed {
	projection := &Committed{Ref: ref, Effects: append([]state.Effect(nil), effects...)}
	for n := range projection.Effects {
		effect := &projection.Effects[n]
		effect.Payload = append([]byte(nil), effect.Payload...)
		if effect.ApplicationLease != nil {
			lease := *effect.ApplicationLease
			effect.ApplicationLease = &lease
		}
	}
	return projection
}
