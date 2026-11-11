package engine

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/state"
)

type selectorStateViewStore struct {
	state.Backend
	views int
}

func (s *selectorStateViewStore) Load(context.Context, state.Ref) (state.Record, error) {
	return state.Record{}, errors.New("full history read is not part of selector discovery")
}

func (s *selectorStateViewStore) LoadAt(context.Context, state.Ref) (state.Record, int64, error) {
	return state.Record{}, 0, errors.New("full history read is not part of selector discovery")
}

func (s *selectorStateViewStore) ReadState(ctx context.Context, ref state.Ref) (state.StateView, int64, error) {
	s.views++
	return s.Backend.ReadState(ctx, ref)
}

func TestSelectorAndScheduleUseStateViewWithoutFullHistory(t *testing.T) {
	f, ref, renewal := requestFenceSubscriptionFixture(t)
	f.engine.work = make(chan struct{}, 8)
	original := f.engine.store
	observed := &selectorStateViewStore{Backend: original}
	f.engine.store = observed
	defer func() { f.engine.store = original }()
	if snapshot, err := f.engine.LocateRoute(context.Background(), renewal); err != nil || snapshot == nil || !stateSameContext(snapshot.Context, ref) {
		t.Fatal("selector failed without full replay history", err)
	}
	groupRef, err := f.engine.group(ref)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.engine.schedulePlan(context.Background(), groupRef); err != nil {
		t.Fatal("schedule plan failed without full replay history", err)
	}
	if observed.views != 2 {
		t.Fatalf("selector/scheduler did not use their bounded state views: %d", observed.views)
	}
}

func TestStateViewKeepsNativeDispositionVersionChecks(t *testing.T) {
	f, ref, _ := requestFenceSubscriptionFixture(t)
	record, group := stateTestLoad(t, f.engine, ref)
	if record.Versions != NativeStateVersions() {
		t.Fatal("native-version fixture required")
	}
	changed := false
	for _, c := range group.Contexts {
		for _, tx := range c.Transactions {
			if !c.Rejection && stateServerTransaction(tx) {
				tx.AdmittedFlowProofs = nil
				changed = true
			}
		}
	}
	if !changed {
		t.Fatal("fixture did not retain a native server transaction")
	}
	encoded, err := group.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	payload := record.Payload
	payload.ProtocolState = encoded
	view := state.StateView{Ref: record.Ref, Authority: record.Authority, Versions: record.Versions, Revision: record.Revision, Payload: payload}
	if _, err := decodeStateGroup(stateViewProtocolRecord(view)); !errors.Is(err, state.ErrCorrupt) {
		t.Fatal("state view bypassed version-conditioned native disposition checks", err)
	}
}

func TestLocateRouteReadOnlySubscriptionHitPreservesRecord(t *testing.T) {
	f, ref, renewal := requestFenceSubscriptionFixture(t)
	f.engine.work = make(chan struct{}, 8)
	before, _ := stateTestLoad(t, f.engine, ref)

	for attempt := 0; attempt < 3; attempt++ {
		snapshot, err := f.engine.LocateRoute(context.Background(), renewal)
		if err != nil || snapshot == nil || !stateSameContext(snapshot.Context, ref) {
			t.Fatalf("tagged subscription renewal lost its selector: snapshot=%v err=%v", snapshot, err)
		}
	}
	after, _ := stateTestLoad(t, f.engine, ref)
	// Lease observations come from the read, not from the retained record.
	before.ApplicationLeases, after.ApplicationLeases = nil, nil
	if !reflect.DeepEqual(before, after) {
		t.Fatal("read-only selector hits changed the retained record, revision or receipts")
	}
}

func TestLocateRouteUsesRawWireInsteadOfForgedMessageFields(t *testing.T) {
	f, ref, renewal := requestFenceSubscriptionFixture(t)
	f.engine.work = make(chan struct{}, 8)
	forged := proto.Clone(renewal).(*pb.SipMessage)
	forged.Method, forged.Status, forged.Addresses = "REGISTER", 200, nil
	forged.Selectors = []string{"forged-selector-that-must-not-route"}

	snapshot, err := f.engine.LocateRoute(context.Background(), forged)
	if err != nil || snapshot == nil || !stateSameContext(snapshot.Context, ref) {
		t.Fatalf("routing trusted forged protobuf fields instead of the tagged wire: snapshot=%v err=%v", snapshot, err)
	}
}

func TestLocateRoutePendingRetainsAtomicGate(t *testing.T) {
	f, ref, renewal := requestFenceSubscriptionFixture(t)
	f.engine.work = make(chan struct{}, 1)
	groupRef, err := f.engine.group(ref)
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Minute).UnixMilli()
	if _, err = f.engine.store.(*state.Store).Begin(context.Background(), groupRef, "pending-route-worker", state.Input{
		ID: "pending-route", Kind: "locate", Fingerprint: "pending-route",
		DeadlineMS: deadline, ReceiptUntilMS: deadline + int64(time.Minute/time.Millisecond),
	}); err != nil {
		t.Fatal(err)
	}
	pending, _ := stateTestLoad(t, f.engine, ref)
	if pending.Pending == nil {
		t.Fatal("fixture did not retain the pending allocation gate")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if snapshot, locateErr := f.engine.LocateRoute(ctx, renewal); locateErr == nil || snapshot != nil {
		t.Fatalf("pending group returned a stale selector hit: snapshot=%v err=%v", snapshot, locateErr)
	}
	afterPending, _ := stateTestLoad(t, f.engine, ref)
	if afterPending.Revision != pending.Revision || !reflect.DeepEqual(afterPending.Pending, pending.Pending) ||
		!reflect.DeepEqual(afterPending.Payload, pending.Payload) || !reflect.DeepEqual(afterPending.Receipts, pending.Receipts) {
		t.Fatal("pending fallback changed the admitted input or retained state")
	}
}

func TestLocateRouteBusyReadOnlySlotFallsBackToAtomicSelector(t *testing.T) {
	f, ref, renewal := requestFenceSubscriptionFixture(t)
	f.engine.work = make(chan struct{}, 1)
	f.engine.work <- struct{}{}

	snapshot, err := f.engine.LocateRoute(context.Background(), renewal)
	if err != nil || snapshot == nil || !stateSameContext(snapshot.Context, ref) {
		t.Fatalf("full read-only slot did not fall back to the atomic selector: snapshot=%v err=%v", snapshot, err)
	}
	if len(f.engine.work) != 1 {
		t.Fatal("busy fast-path fallback consumed a slot it did not acquire")
	}
}
