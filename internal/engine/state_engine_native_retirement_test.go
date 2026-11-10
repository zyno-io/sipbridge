package engine

import (
	"context"
	"encoding/json"
	"testing"

	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/state"
)

func TestNativePreWireFailureExpiresNotificationWithLiveOwner(t *testing.T) {
	for _, authorizationUnknown := range []bool{false, true} {
		name := "command-admission"
		if authorizationUnknown {
			name = "unresolved-authorization"
		}
		t.Run(name, func(t *testing.T) {
			f := newNativeOwnerFixture(t)
			ref := f.outboundRef(t)
			opened := f.apply(t, ref, "prewire-open", stateEngineInput{Kind: "command", Application: f.app, ApplicationLeaseTerm: f.term,
				Command: &pb.Command{Context: ref, Sequence: 1, Action: &pb.Command_OpenUac{OpenUac: &pb.OpenUac{Uri: "sip:phone@example.test", Transport: "tls"}}}})
			if opened.Error != "" {
				t.Fatal(opened.Error)
			}
			record, _ := stateTestLoad(t, f.engine, ref)
			var openID string
			for _, effect := range record.Payload.Effects {
				if effect.Kind == "open" {
					openID = effect.ID
				}
			}
			if openID == "" {
				t.Fatal("pre-wire transport computation not created")
			}
			f.apply(t, ref, "start-open", stateEngineInput{Kind: "effect-start", Effect: openID})
			failed := f.apply(t, ref, "failed-open", stateEngineInput{Kind: "effect-result", Effect: openID, Executor: f.engine.cfg.Process, Failure: "fixture transport unavailable"})
			if failed.Error != "" {
				t.Fatal(failed.Error)
			}
			lease, err := f.engine.ApplicationLease(context.Background(), f.app, f.term)
			if err != nil || lease.Term != f.term {
				t.Fatalf("live owner lease lost: %v", err)
			}
			record, group := stateTestLoad(t, f.engine, ref)
			c := group.Contexts[ref.Id]
			if !c.OwnerValidated || c.OwnerDirectoryEpoch == "" || c.OwnerLost || !c.Snapshot.Ended || len(c.Transactions) != 0 {
				t.Fatal("fixture must retain its command admission and fail before SIP send")
			}
			r := &stateReduction{engine: f.engine, record: record, group: group, payload: record.Payload, now: int64(lease.CheckedAtMs)}
			var notification string
			for index, effect := range r.payload.Effects {
				if effect.Kind == "event" {
					effect.Status, effect.AttemptID, effect.DeadlineMS = state.EffectUnknown, "failed-notification", r.now-120000
					r.payload.Effects[index], notification = effect, effect.ID
				}
			}
			if notification == "" {
				t.Fatal("FAILED notification missing")
			}
			if authorizationUnknown {
				body, err := json.Marshal(stateEngineEffect{Context: ref.Id})
				if err != nil {
					t.Fatal(err)
				}
				r.payload.Effects = append(r.payload.Effects, state.Effect{ID: "unresolved-send-authority", Kind: "wire", Status: state.EffectUnknown, Payload: body})
			}
			if err = r.expireTerminalSetupNotifications(); err != nil {
				t.Fatal(err)
			}
			_, effect, _, err := r.findEffect(notification)
			if err != nil {
				t.Fatal(err)
			}
			if authorizationUnknown {
				if effect.Status != state.EffectUnknown {
					t.Fatal("reporting deadline erased unresolved send authorization")
				}
				return
			}
			if effect.Status != state.EffectCancelled {
				t.Fatal("command admission stranded an expired pre-wire failure notification")
			}
			c.RetireMS, c.ReplayUntilMS = r.now-1, 0
			if err = r.appendLifecycleTimers(); err != nil {
				t.Fatal(err)
			}
			if r.group.Contexts[ref.Id] != nil || len(r.payload.Effects) != 0 {
				t.Fatal("pre-wire setup requires a Controller cleanup acknowledgement")
			}
		})
	}
}
