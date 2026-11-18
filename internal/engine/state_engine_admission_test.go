package engine

import (
	"context"
	"fmt"
	"testing"
	"time"

	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/state"
)

func TestNativeAdmissionReservationIsSingleUseAndRetirementStable(t *testing.T) {
	f := newNativeOwnerFixture(t)
	ctx := context.Background()
	message := stateTestRequest("OPTIONS", 1, "", "single-use")
	ref, err := f.engine.Locate(ctx, message)
	if err != nil {
		t.Fatal(err)
	}
	_, before := stateTestLoad(t, f.engine, ref)
	repeated, err := f.engine.Locate(ctx, message)
	if err != nil || !stateSameContext(ref, repeated) {
		t.Fatalf("lookup duplicated reservation: %v", err)
	}
	_, after := stateTestLoad(t, f.engine, ref)
	if before.Allocations[ref.Id] != after.Allocations[ref.Id] || after.NextAllocation != 1 {
		t.Fatal("lookup extended or duplicated reservation")
	}
	inspection, err := f.engine.inspectContext(ctx, f.app, ref)
	if err != nil || inspection.Certainty != pb.OperationResult_UNKNOWN || inspection.RetirementProof != nil {
		t.Fatalf("live reservation treated as retired: %v / %v", inspection, err)
	}
	altered := stateTestRequest("OPTIONS", 2, "", "single-use-altered")
	denied := f.apply(t, ref, "altered", stateEngineInput{Kind: "ingest", Application: f.app, ApplicationLeaseTerm: f.term, Ingest: &pb.OwnerIngest{Context: ref, Application: f.app, Flow: f.flow, Wire: altered.Wire}})
	if denied.Certainty != pb.OperationResult_NOT_ACCEPTED {
		t.Fatal("reservation admitted different bytes")
	}
	admitted := f.apply(t, ref, "admit", stateEngineInput{Kind: "ingest", Application: f.app, ApplicationLeaseTerm: f.term, Ingest: &pb.OwnerIngest{Context: ref, Application: f.app, Flow: f.flow, Wire: message.Wire}})
	if admitted.Error != "" {
		t.Fatal(admitted.Error)
	}
	record, group := stateTestLoad(t, f.engine, ref)
	if _, ok := group.Allocations[ref.Id]; ok {
		t.Fatal("allocation did not consume reservation")
	}
	c := group.Contexts[ref.Id]
	// Isolate reclamation after all SIP/physical evidence has ended. The next
	// regression separately keeps real UNKNOWN evidence and must refuse deletion.
	c.Transactions, c.Dialogs = nil, nil
	c.Snapshot.Ended, c.OwnerLost = true, true
	c.RetireMS, c.ReplayUntilMS = time.Now().Add(-time.Second).UnixMilli(), 0
	r := &stateReduction{engine: f.engine, record: record, group: group, payload: record.Payload, now: time.Now().UnixMilli()}
	r.payload.Effects, r.payload.Timers = nil, nil
	if err = r.appendLifecycleTimers(); err != nil {
		t.Fatal(err)
	}
	if r.group.Contexts[ref.Id] != nil || len(r.payload.Effects) != 0 {
		t.Fatal("native retirement still requires a Controller cleanup RPC")
	}
	data, err := marshalRetainedFixture(r.group)
	if err != nil {
		t.Fatal(err)
	}
	r.payload.ProtocolState = data
	begun, err := f.engine.store.(*state.Store).Begin(ctx, record.Ref, "retire-fixture", state.Input{ID: "retire-fixture", Kind: "fixture", Fingerprint: "retire-fixture", DeadlineMS: time.Now().Add(time.Second).UnixMilli(), ReceiptUntilMS: time.Now().Add(time.Minute).UnixMilli()})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.engine.store.(*state.Store).Commit(ctx, *begun.Step, r.payload, nil); err != nil {
		t.Fatal(err)
	}
	inspection, err = f.engine.inspectContext(ctx, f.app, ref)
	if err != nil || inspection.GetRetirementProof().GetReason() != pb.ContextRetirementProof_CONTEXT_ABSENT {
		t.Fatalf("missing stable retirement proof: %v / %v", inspection, err)
	}
	late := f.apply(t, ref, "late-old-ingest", stateEngineInput{Kind: "ingest", Application: f.app, ApplicationLeaseTerm: f.term, Ingest: &pb.OwnerIngest{Context: ref, Application: f.app, Flow: f.flow, Wire: message.Wire}})
	if late.Certainty != pb.OperationResult_NOT_ACCEPTED {
		t.Fatal("delayed creator resurrected retired identity")
	}
	next, err := f.engine.Locate(ctx, message)
	if err != nil || next.Id == ref.Id {
		t.Fatalf("new lookup reused retired identity: %v", err)
	}
	newAdmission := f.apply(t, next, "next-admit", stateEngineInput{Kind: "ingest", Application: f.app, ApplicationLeaseTerm: f.term, Ingest: &pb.OwnerIngest{Context: next, Application: f.app, Flow: f.flow, Wire: message.Wire}})
	if newAdmission.Error != "" {
		t.Fatalf("fresh reservation was refused: %s", newAdmission.Error)
	}
}

func TestNativeAdmissionReservationExpiresWithoutRevival(t *testing.T) {
	f := newNativeOwnerFixture(t)
	ctx := context.Background()
	message := stateTestRequest("OPTIONS", 1, "", "expired-admission")
	ref, err := f.engine.Locate(ctx, message)
	if err != nil {
		t.Fatal(err)
	}
	_, group := stateTestLoad(t, f.engine, ref)
	time.Sleep(time.Until(time.UnixMilli(group.Allocations[ref.Id].ExpiresMS + 5)))
	late := f.apply(t, ref, "expired-ingest", stateEngineInput{Kind: "ingest", Application: f.app, ApplicationLeaseTerm: f.term, Ingest: &pb.OwnerIngest{Context: ref, Application: f.app, Flow: f.flow, Wire: message.Wire}})
	if late.Certainty != pb.OperationResult_NOT_ACCEPTED {
		t.Fatal("expired reservation admitted a context")
	}
	inspection, err := f.engine.inspectContext(ctx, f.app, ref)
	if err != nil || inspection.GetRetirementProof().GetReason() != pb.ContextRetirementProof_CONTEXT_ABSENT {
		t.Fatalf("expired reservation was not reclaimed: %v / %v", inspection, err)
	}
	next, err := f.engine.Locate(ctx, message)
	if err != nil || next.Id == ref.Id {
		t.Fatalf("expired identity revived: %v", err)
	}
}

func TestNativeAdmissionQuotaAndPendingCreator(t *testing.T) {
	f := newNativeOwnerFixture(t)
	var ref *pb.ContextRef
	for index := 0; index < stateAdmissionLimit; index++ {
		var err error
		ref, err = f.engine.Locate(context.Background(), stateTestRequest("OPTIONS", index+1, "", fmt.Sprintf("quota-%d", index)))
		if err != nil {
			t.Fatal(err)
		}
	}
	if _, err := f.engine.Locate(context.Background(), stateTestRequest("OPTIONS", 100, "", "quota-denied")); err == nil {
		t.Fatal("reservation quota exceeded")
	}
	record, group := stateTestLoad(t, f.engine, ref)
	if len(group.Allocations) != stateAdmissionLimit {
		t.Fatal("quota refusal damaged existing reservations")
	}
	pending, err := (stateEngineInput{Kind: "ingest"}).MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	input := state.Input{ID: "pending-creator", Kind: "ingest", Fingerprint: "pending-creator", Payload: pending, DeadlineMS: time.Now().Add(time.Second).UnixMilli(), ReceiptUntilMS: time.Now().Add(time.Minute).UnixMilli()}
	if _, err := f.engine.store.(*state.Store).Begin(context.Background(), record.Ref, "paused-creator", input); err != nil {
		t.Fatal(err)
	}
	inspection, err := f.engine.inspectContext(context.Background(), f.app, ref)
	if err != nil || inspection.Certainty != pb.OperationResult_UNKNOWN || inspection.RetirementProof != nil {
		t.Fatalf("pending creator treated as retired: %v / %v", inspection, err)
	}
}

func TestNativeRetirementPreservesPhysicalUnknown(t *testing.T) {
	f := newNativeOwnerFixture(t)
	ref := f.admit(t)
	record, group := stateTestLoad(t, f.engine, ref)
	c := group.Contexts[ref.Id]
	c.Transactions, c.Dialogs = nil, nil
	c.Snapshot.Ended, c.OwnerLost, c.RetireMS = true, true, time.Now().Add(-time.Second).UnixMilli()
	body, err := marshalRetainedFixture(stateEngineEffect{Context: ref.Id})
	if err != nil {
		t.Fatal(err)
	}
	r := &stateReduction{engine: f.engine, record: record, group: group, now: time.Now().UnixMilli(), payload: state.Payload{Effects: []state.Effect{{ID: "started-write", Kind: "deliver", Status: state.EffectUnknown, DeadlineMS: 1, Payload: body}}}}
	if err = r.appendLifecycleTimers(); err != nil {
		t.Fatal(err)
	}
	if r.group.Contexts[ref.Id] == nil || r.payload.Effects[0].Status != state.EffectUnknown {
		t.Fatal("retirement discarded a real physical UNKNOWN")
	}
}
