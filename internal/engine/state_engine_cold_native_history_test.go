package engine

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/emiago/sipgo/sip/portable"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/execution"
	"sipbridge/internal/state"
)

func TestNativeColdPhysicalHistoryStopsLifecyclePollingAndAcceptsExactLateProof(t *testing.T) {
	for _, backend := range []string{state.MemoryBackend, state.RedisBackend} {
		for _, status := range []state.EffectStatus{state.EffectDispatching, state.EffectUnknown} {
			t.Run(backend+"/"+string(status), func(t *testing.T) {
				f := newNativeOwnerFixtureBackend(t, backend)
				ref := f.admit(t)
				_, group := stateTestLoad(t, f.engine, ref)
				var request string
				for id := range group.Contexts[ref.Id].Requests {
					request = id
				}
				result := f.apply(t, ref, "response", stateEngineInput{Kind: "command", Application: f.app, ApplicationLeaseTerm: f.term,
					Command: &pb.Command{Context: ref, Sequence: 1, Action: &pb.Command_Respond{Respond: &pb.Respond{
						RequestId: request, Status: 486, Authority: f.responseAuthority(t, ref, request),
					}}}})
				if result.Error != "" {
					t.Fatal(result.Error)
				}
				record, _ := stateTestLoad(t, f.engine, ref)
				var write *pb.FlowWrite
				for _, effect := range record.Payload.Effects {
					var body stateEngineEffect
					if err := body.UnmarshalJSON(effect.Payload); err != nil {
						t.Fatal(err)
					}
					if effect.Kind == "deliver" && body.Native && effect.Status == state.EffectAuthorized {
						write = &pb.FlowWrite{Flow: body.Flow, Effect: body.Authorization.Effect, Permit: body.Permit}
						break
					}
				}
				if write == nil {
					t.Fatal("fixture did not select native delivery")
				}
				reserved, err := f.engine.Reserve(context.Background(), write)
				if err != nil || reserved.Known || len(reserved.Wire) == 0 {
					t.Fatalf("reserve: %+v %v", reserved, err)
				}
				if status == state.EffectUnknown {
					if err := f.engine.Complete(context.Background(), write, &pb.DeliveryResult{Effect: write.Effect, Outcome: pb.DeliveryResult_UNKNOWN}); err != nil {
						t.Fatal(err)
					}
				}
				logical, err := f.engine.group(ref)
				if err != nil {
					t.Fatal(err)
				}
				var original state.Effect
				var obsolete state.Timer
				writer, err := execution.NewCAS(f.engine.store, execution.ReducerFunc(func(record state.Record, _ state.Input, now int64) (state.Payload, []byte, error) {
					group, err := decodeStateGroup(record)
					if err != nil {
						return record.Payload, nil, err
					}
					c := group.Contexts[ref.Id]
					c.Snapshot.Ended, c.OwnerLost = true, true
					c.RetireMS, c.ReplayUntilMS, c.OwnerCleanupAtMS = now-120001, now-1, 0
					c.Transactions, c.Requests, c.Dialogs = nil, nil, nil
					c.Subscription = &stateSubscription{Event: "dialog", Final: true}
					for _, effect := range record.Payload.Effects {
						if effect.ID == write.Effect.Id {
							original = effect
							break
						}
					}
					if original.Status != status || original.AttemptID != write.Effect.AttemptId {
						t.Fatal("lost original reservation")
					}
					original.DeadlineMS = now - 120001
					r := &stateReduction{engine: f.engine}
					obsolete = state.Timer{ID: r.timerID(ref.Id, "context", "retire", ""), Generation: "1", DueMS: 1, DeadlineMS: 1}
					payload := record.Payload
					payload.Effects, payload.Timers, payload.ActiveTransactions = []state.Effect{original}, []state.Timer{obsolete}, 0
					payload.ProtocolState, err = json.Marshal(group)
					return payload, nil, err
				}), 1)
				if err != nil {
					t.Fatal(err)
				}
				deadline := time.Now().Add(2 * time.Second).UnixMilli()
				if _, err := writer.Apply(context.Background(), logical, state.Input{ID: "cold-fixture", Kind: "fixture", Fingerprint: "cold-fixture", DeadlineMS: deadline, ReceiptUntilMS: deadline}); err != nil {
					t.Fatal(err)
				}
				if !f.engine.applyScheduledTimers(context.Background(), logical, []state.Timer{obsolete}) {
					t.Fatal("cold projection did not commit")
				}
				after, group := stateTestLoad(t, f.engine, ref)
				if len(after.Payload.Timers) != 0 || len(after.Payload.Effects) != 1 || !reflect.DeepEqual(after.Payload.Effects[0], original) || group.Contexts[ref.Id] == nil {
					t.Fatal("cold maintenance changed evidence, retired context or kept timers")
				}
				r := &stateReduction{engine: f.engine, record: after, group: group, payload: after.Payload, now: time.Now().UnixMilli()}
				if r.contextRetirementProven(ref.Id, group.Contexts[ref.Id]) {
					t.Fatal("cold uncertainty proved retirement")
				}
				var cursor uint64
				for {
					next, _, err := f.engine.store.RepairSchedule(context.Background(), cursor, 128)
					if err != nil {
						t.Fatal(err)
					}
					cursor = next
					if cursor == 0 {
						break
					}
				}
				plan, err := f.engine.schedulePlan(context.Background(), logical)
				if err != nil || plan.retire || len(plan.timers) != 0 || len(plan.effects) != 1 || !reflect.DeepEqual(plan.effects[0], original) {
					t.Fatalf("repair changed cold history or independent physical dispatch: %+v %v", plan, err)
				}
				// Replaying an obsolete timer cannot resume the one-second loop.
				f.engine.applyScheduledTimers(context.Background(), logical, []state.Timer{obsolete})
				again, _ := stateTestLoad(t, f.engine, ref)
				if again.Revision != after.Revision {
					t.Fatal("obsolete timer rewrote cold state")
				}
				repeated, err := f.engine.Reserve(context.Background(), write)
				if err != nil || !repeated.Known || repeated.Outcome != pb.DeliveryResult_UNKNOWN || len(repeated.Wire) != 0 {
					t.Fatalf("cold attempt permitted another write: %+v %v", repeated, err)
				}
				// Coldness changes scheduling only; original exact late evidence still applies.
				if err := f.engine.Complete(context.Background(), write, &pb.DeliveryResult{Effect: write.Effect, Outcome: pb.DeliveryResult_WRITTEN}); err != nil {
					t.Fatalf("exact late proof refused: %v", err)
				}
			})
		}
	}
}

func TestNativeColdHistoryKeepsEveryLiveContinuationScheduled(t *testing.T) {
	f := newNativeOwnerFixtureBackend(t, state.MemoryBackend)
	ref := f.admit(t)
	record, group := stateTestLoad(t, f.engine, ref)
	c := group.Contexts[ref.Id]
	c.Snapshot.Ended, c.OwnerLost, c.RetireMS, c.ReplayUntilMS = true, true, 1, 0
	c.Transactions, c.Dialogs, c.OwnerCleanupAtMS = nil, nil, 0
	body, err := json.Marshal(stateEngineEffect{Context: ref.Id, Native: true})
	if err != nil {
		t.Fatal(err)
	}
	r := &stateReduction{engine: f.engine, record: record, group: group, now: 200000, payload: state.Payload{Effects: []state.Effect{
		{ID: "uncertain", Kind: "deliver", Status: state.EffectDispatching, DeadlineMS: 1, Payload: body},
	}}}
	if !r.coldNativeHistory(ref.Id, c) {
		t.Fatal("fixture is not cold")
	}
	for _, test := range []struct {
		name   string
		change func()
		reset  func()
	}{
		{"live", func() { c.Snapshot.Ended = false }, func() { c.Snapshot.Ended = true }},
		{"owner", func() { c.OwnerLost = false }, func() { c.OwnerLost = true }},
		{"replay", func() { c.ReplayUntilMS = r.now + 1 }, func() { c.ReplayUntilMS = 0 }},
		{"retire", func() { c.RetireMS = r.now + 1 }, func() { c.RetireMS = 1 }},
		{"deadline horizon", func() { r.payload.Effects[0].DeadlineMS = r.now - 119999 }, func() { r.payload.Effects[0].DeadlineMS = 1 }},
		{"transaction", func() { c.Transactions = map[string]*stateTransaction{"tx": {}} }, func() { c.Transactions = nil }},
		{"dialog timer", func() {
			c.Dialogs = map[string]portable.Dialog{"dialog": {Timers: map[portable.Timer]portable.Deadline{"timer": {Active: true}}}}
		}, func() { c.Dialogs = nil }},
		{"request fence", func() { r.requestFence = &stateRequestFence{context: ref.Id} }, func() { r.requestFence = nil }},
		{"corrupt evidence", func() { r.payload.Effects[0].Payload = []byte("invalid") }, func() { r.payload.Effects[0].Payload = body }},
		{"cleanup", func() { c.OwnerCleanupAtMS = 1 }, func() { c.OwnerCleanupAtMS = 0 }},
		{"pending", func() { r.payload.Effects[0].Status = state.EffectPending }, func() { r.payload.Effects[0].Status = state.EffectDispatching }},
		{"known unsettled", func() { r.payload.Effects[0].Status = state.EffectWritten }, func() { r.payload.Effects[0].Status = state.EffectDispatching }},
		{"other work", func() {
			r.payload.Effects = append(r.payload.Effects, state.Effect{ID: "other", Kind: "event", Status: state.EffectPending, Payload: body})
		}, func() { r.payload.Effects = r.payload.Effects[:1] }},
	} {
		t.Run(test.name, func(t *testing.T) {
			test.change()
			defer test.reset()
			if r.coldNativeHistory(ref.Id, c) {
				t.Fatal("live work classified cold")
			}
		})
	}
}
