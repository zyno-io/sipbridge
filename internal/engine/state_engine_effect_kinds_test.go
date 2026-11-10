package engine

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/state"
)

func TestUnsupportedEffectCannotDispatchOrAcceptResult(t *testing.T) {
	body, err := json.Marshal(stateEngineEffect{Context: "context", Application: &pb.ProcessRef{InstanceId: "controller", Epoch: "epoch"},
		Envelope: &pb.Envelope{Payload: &pb.Envelope_DialogEvent{DialogEvent: &pb.DialogEvent{Kind: pb.DialogEvent_ENDED}}}})
	if err != nil {
		t.Fatal(err)
	}
	for _, status := range []state.EffectStatus{state.EffectPending, state.EffectDispatching, state.EffectWritten, state.EffectNotWritten} {
		t.Run(string(status), func(t *testing.T) {
			effect := state.Effect{ID: "unknown-effect", Kind: "unsupported", Status: status, DeadlineMS: time.Now().Add(time.Minute).UnixMilli(), Payload: body}
			if effects := stateDispatchableEffects([]state.Effect{effect}, time.Now().UnixMilli()); len(effects) != 0 {
				t.Fatal("unsupported kind became scheduled work")
			}
			// No controller or runtime is available: the closed kind check must
			// refuse before touching either external boundary, even with an envelope.
			engine := &StateEngine{}
			engine.dispatchEffectWithin(context.Background(), state.Ref{}, effect, false)
			reduction := &stateReduction{payload: state.Payload{Effects: []state.Effect{effect}}}
			if _, _, _, err := reduction.findEffect(effect.ID); !errors.Is(err, state.ErrCorrupt) {
				t.Fatalf("unsupported result target was accepted: %v", err)
			}
			if err := reduction.reclaimEffects(); !errors.Is(err, state.ErrCorrupt) {
				t.Fatalf("unsupported kind was silently accepted during reclamation: %v", err)
			}
		})
	}
}
