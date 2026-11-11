package engine

import (
	"context"
	"testing"

	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/state"
)

func TestNativePhysicalCompletionNeedsNoControllerSettlement(t *testing.T) {
	for _, mode := range []string{"written", "not-written", "unknown", "unrecorded"} {
		t.Run(mode, func(t *testing.T) {
			f := newNativeOwnerFixture(t)
			ref := f.admit(t)
			_, group := stateTestLoad(t, f.engine, ref)
			var requestID string
			for id := range group.Contexts[ref.Id].Requests {
				requestID = id
			}
			result := f.apply(t, ref, "select-response", stateEngineInput{Kind: "command", Application: f.app, ApplicationLeaseTerm: f.term,
				Command: &pb.Command{Context: ref, Sequence: 1, Action: &pb.Command_Respond{Respond: &pb.Respond{
					RequestId: requestID, Status: 486, Authority: f.responseAuthority(t, ref, requestID),
				}}}})
			if result.Error != "" {
				t.Fatal(result.Error)
			}
			record, _ := stateTestLoad(t, f.engine, ref)
			var selected state.Effect
			var body stateEngineEffect
			for _, effect := range record.Payload.Effects {
				var candidate stateEngineEffect
				if err := unmarshalRetainedFixture(effect.Payload, &candidate); err != nil {
					t.Fatal(err)
				}
				if effect.Kind == "deliver" && candidate.Native && effect.Status == state.EffectAuthorized {
					selected, body = effect, candidate
				}
			}
			if selected.ID == "" {
				t.Fatal("response did not create an exact native physical obligation")
			}
			calls, released := 0, false
			f.engine.cfg.WriteCommitted = func(ctx context.Context, write *pb.FlowWrite) *pb.DeliveryResult {
				calls++
				delivery := &pb.DeliveryResult{Effect: write.Effect, Outcome: pb.DeliveryResult_WRITTEN}
				if mode == "unrecorded" {
					return delivery
				}
				reserved, err := f.engine.Reserve(ctx, write)
				if err != nil || reserved.Known || len(reserved.Wire) == 0 {
					t.Fatalf("native physical reservation: %+v %v", reserved, err)
				}
				if mode == "not-written" {
					delivery.Outcome = pb.DeliveryResult_NOT_WRITTEN
				} else if mode == "unknown" {
					delivery.Outcome = pb.DeliveryResult_UNKNOWN
				}
				if err := f.engine.Complete(ctx, write, delivery); err != nil {
					t.Fatal(err)
				}
				delivery.Recorded = mode != "unknown"
				return delivery
			}
			// The native dispatcher has no controller connection or RPC capability.
			f.engine.dispatchNativeCompletion(context.Background(), record.Ref, selected, body, func() { released = true })
			if calls != 1 || released != (mode == "written" || mode == "not-written") {
				t.Fatal("native dispatch released uncertainty or failed to accept its local recorded outcome")
			}
			record, group = stateTestLoad(t, f.engine, ref)
			if group.Contexts[ref.Id].Snapshot.CommandSequence != 1 || group.Contexts[ref.Id].Snapshot.ApplicationLeaseTerm != f.term {
				t.Fatal("local physical completion consumed another command or changed its owner")
			}
			found := false
			for _, effect := range record.Payload.Effects {
				if effect.ID != selected.ID {
					continue
				}
				found = true
				var current stateEngineEffect
				if err := unmarshalRetainedFixture(effect.Payload, &current); err != nil {
					t.Fatal(err)
				}
				if effect.DeadlineMS != selected.DeadlineMS {
					t.Fatal("physical completion renewed the original deadline")
				}
				if mode == "unknown" && (effect.Status != state.EffectUnknown || effect.AttemptID != body.Authorization.Effect.AttemptId || current.Settled) {
					t.Fatal("UNKNOWN lost its exact physical attempt evidence")
				}
				if mode == "unrecorded" && (effect.Status != state.EffectAuthorized || current.Settled) {
					t.Fatal("unrecorded transport return replaced authoritative physical state")
				}
				if (mode == "written" || mode == "not-written") && !current.Settled {
					t.Fatal("recorded native outcome still required a controller acknowledgement")
				}
			}
			if !found && (mode == "unknown" || mode == "unrecorded") {
				t.Fatal("uncertain physical obligation disappeared")
			}
		})
	}
}
