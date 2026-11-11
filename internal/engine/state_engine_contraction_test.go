package engine

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/emiago/sipgo/sip/portable"
	"github.com/redis/go-redis/v9"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/execution"
	"sipbridge/internal/state"
)

func TestNativeColdTrustedHistoryStopsPollingWithoutInventingOutcome(t *testing.T) {
	f := newNativeOwnerFixture(t)
	ref := f.admit(t)
	_, admitted := stateTestLoad(t, f.engine, ref)
	var request string
	for id := range admitted.Contexts[ref.Id].Requests {
		request = id
	}
	response := f.apply(t, ref, "select-provisional", stateEngineInput{Kind: "command", Application: f.app, ApplicationLeaseTerm: f.term,
		Command: &pb.Command{Context: ref, Sequence: 1, Action: &pb.Command_Respond{Respond: &pb.Respond{RequestId: request, Status: 180, Authority: f.responseAuthority(t, ref, request)}}}})
	if response.Error != "" {
		t.Fatal(response.Error)
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
		c.Snapshot.Ended, c.RetireMS, c.ReplayUntilMS = true, now-120001, now-1
		c.Transactions, c.Requests, c.Dialogs = nil, nil, nil
		c.Subscription = nil
		c.OwnerCleanupAtMS = 0
		var retained stateEngineEffect
		for _, effect := range record.Payload.Effects {
			if effect.Kind == "deliver" {
				if err := unmarshalRetainedFixture(effect.Payload, &retained); err != nil {
					return record.Payload, nil, err
				}
				break
			}
		}
		if retained.Authorization == nil {
			t.Fatal("fixture has no actual physical authorization")
		}
		retained.Native = false
		body, err := marshalRetainedFixture(retained)
		if err != nil {
			return record.Payload, nil, err
		}
		original = state.Effect{ID: "old-physical-history", Kind: "deliver", Status: state.EffectUnknown,
			AttemptID: "original-attempt", DeadlineMS: now - 120001, Payload: body}
		r := &stateReduction{engine: f.engine}
		obsolete = state.Timer{ID: r.timerID(ref.Id, "context", "retire", ""), Generation: "1", DueMS: 1, DeadlineMS: 1}
		payload := record.Payload
		payload.Effects, payload.Timers, payload.ActiveTransactions = []state.Effect{original}, []state.Timer{obsolete}, 0
		payload.ProtocolState, err = marshalRetainedFixture(group)
		return payload, nil, err
	}), 1)
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second).UnixMilli()
	if _, err := writer.Apply(context.Background(), logical, state.Input{ID: "old-history", Kind: "fixture", Fingerprint: "old-history", DeadlineMS: deadline, ReceiptUntilMS: deadline}); err != nil {
		t.Fatal(err)
	}
	before, _ := stateTestLoad(t, f.engine, ref)
	if !f.engine.applyScheduledTimers(context.Background(), logical, []state.Timer{obsolete}) {
		t.Fatal("obsolete timer did not commit a guarded projection update")
	}
	after, group := stateTestLoad(t, f.engine, ref)
	if after.Revision == before.Revision || after.Pending != nil || len(after.Payload.Timers) != 0 || len(after.Payload.Effects) != 1 ||
		!reflect.DeepEqual(after.Payload.Effects[0], original) || group.Contexts[ref.Id] == nil {
		t.Fatal("cold history changed outcome evidence or retained polling")
	}
	// Repair republishes persisted scheduling projections. Removing only a ZSET
	// entry would fail this assertion as soon as the next repair scan ran.
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
	key := f.prefix + "{p0000}:group:" + logical.ID
	for _, index := range []string{"ready", "due"} {
		if _, err := f.client.ZScore(context.Background(), f.prefix+"{p0000}:"+index, key).Result(); err != redis.Nil {
			t.Fatalf("repair rescheduled cold history in %s: %v", index, err)
		}
	}
	plan, err := f.engine.schedulePlan(context.Background(), logical)
	if err != nil || plan.retire || len(plan.effects) != 0 || len(plan.timers) != 0 {
		t.Fatalf("cold uncertainty scheduled dispatch or retirement: %+v %v", plan, err)
	}
	// A separate live native procedure must remain schedulable in the same
	// Redis namespace; cold history is not a namespace-wide recovery barrier.
	liveRef := f.admitCall(t, "live-alongside-cold-history")
	_, live := stateTestLoad(t, f.engine, liveRef)
	if live.Contexts[liveRef.Id] == nil || len(live.Contexts[liveRef.Id].Transactions) == 0 {
		t.Fatal("cold history blocked live native work")
	}
}

func TestNativeColdHistoryRequiresEntireContextToBeQuiescent(t *testing.T) {
	f := newNativeOwnerFixture(t)
	ref := f.admit(t)
	record, group := stateTestLoad(t, f.engine, ref)
	c := group.Contexts[ref.Id]
	c.Snapshot.Ended, c.RetireMS, c.ReplayUntilMS = true, 1, 0
	c.Transactions, c.Dialogs = nil, nil
	body, _ := marshalRetainedFixture(stateEngineEffect{Context: ref.Id})
	r := &stateReduction{engine: f.engine, group: group, record: record, now: time.Now().UnixMilli(), payload: state.Payload{Effects: []state.Effect{
		{ID: "old", Kind: "deliver", Status: state.EffectUnknown, DeadlineMS: 1, Payload: body},
	}}}
	if !r.coldTrustedHistory(ref.Id, c) {
		t.Fatal("fixture is not cold")
	}
	for _, test := range []struct {
		name   string
		change func()
		reset  func()
	}{
		{"live context", func() { c.Snapshot.Ended = false }, func() { c.Snapshot.Ended = true }},
		{"retained transaction", func() { c.Transactions = map[string]*stateTransaction{"tx": {}} }, func() { c.Transactions = nil }},
		{"native timer", func() {
			c.Dialogs = map[string]portable.Dialog{"dialog": {Timers: map[portable.Timer]portable.Deadline{"timer": {Active: true}}}}
		}, func() { c.Dialogs = nil }},
		{"owner cleanup", func() { c.OwnerCleanupAtMS = 1 }, func() { c.OwnerCleanupAtMS = 0 }},
		{"pending work", func() { r.payload.Effects[0].Status = state.EffectPending }, func() { r.payload.Effects[0].Status = state.EffectUnknown }},
		{"native uncertainty", func() {
			r.payload.Effects[0].Payload = mustRetainedEffect(t, stateEngineEffect{Context: ref.Id, Native: true})
		}, func() { r.payload.Effects[0].Payload = body }},
	} {
		t.Run(test.name, func(t *testing.T) {
			test.change()
			defer test.reset()
			if r.coldTrustedHistory(ref.Id, c) {
				t.Fatal("active protocol/physical work was classified as cold")
			}
		})
	}
}

func TestNativeBareApplicationCommandRefusesBeforeSequenceOrEffects(t *testing.T) {
	f := newNativeOwnerFixture(t)
	ref := f.admit(t)
	before, group := stateTestLoad(t, f.engine, ref)
	var request string
	for id := range group.Contexts[ref.Id].Requests {
		request = id
	}
	logical, err := f.engine.group(ref)
	if err != nil {
		t.Fatal(err)
	}
	result, err := f.engine.apply(context.Background(), logical, "old-bare-command", stateEngineInput{Kind: "command", Context: ref,
		Operation: "old-bare-command", Application: f.app, ApplicationLeaseTerm: f.term,
		Command: &pb.Command{Context: ref, Sequence: 1, Action: &pb.Command_Respond{Respond: &pb.Respond{RequestId: request, Status: 180}}}}, time.Now().Add(time.Second), false)
	if err != nil || result.Certainty != pb.OperationResult_NOT_ACCEPTED || result.Error == "" {
		t.Fatalf("legacy application command was accepted: %v %v", result, err)
	}
	after, decoded := stateTestLoad(t, f.engine, ref)
	if decoded.Contexts[ref.Id].Snapshot.CommandSequence != 0 || !reflect.DeepEqual(before.Payload.Effects, after.Payload.Effects) {
		t.Fatal("legacy refusal consumed command sequence or emitted SIP work")
	}
}
