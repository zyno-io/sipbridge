package execution

import (
	"context"
	"testing"

	"sipbridge/internal/state"
)

func statisticCount(runtime *CASRuntime, kind, event string) uint64 {
	for _, value := range runtime.Statistics() {
		if value.Kind == kind && value.Event == event {
			return value.Count
		}
	}
	return 0
}

func TestCASStatisticsSeparateAcknowledgedCommitFromUnknownAndReplay(t *testing.T) {
	in := state.Input{ID: "operation", Kind: "command", Fingerprint: "payload"}
	known := &state.Receipt{OperationID: in.ID, Fingerprint: in.Fingerprint, Outcome: []byte("known")}
	store := &casScriptedStore{reads: []casRead{updateRead(in, true), {result: state.UpdateResult{Receipt: known}}},
		casErrors: []error{state.ErrUnknown}, inspection: state.Inspection{Receipt: known}}
	runtime := casTestDriver(t, store)
	for range 2 {
		if _, err := runtime.ApplyCommitted(context.Background(), state.Ref{ID: "group"}, in); err != nil {
			t.Fatal(err)
		}
	}
	if statisticCount(runtime, "command", "commit") != 0 || statisticCount(runtime, "command", "unknown") != 1 ||
		statisticCount(runtime, "command", "receipt") != 2 || statisticCount(runtime, "command", "reduce") != 1 || statisticCount(runtime, "command", "read") != 2 {
		t.Fatal("uncertain mutation or replay was counted as a fresh acknowledged commit", runtime.Statistics())
	}
}

func TestCASStatisticsClassifyConflictRetryWithoutDynamicLabels(t *testing.T) {
	in := state.Input{ID: "operation", Kind: "arbitrary-request-identity", Fingerprint: "payload"}
	store := &casScriptedStore{reads: []casRead{updateRead(in, true), updateRead(in, true)}, casErrors: []error{state.ErrStale, nil}}
	runtime := casTestDriver(t, store)
	if _, err := runtime.ApplyCommitted(context.Background(), state.Ref{ID: "group"}, in); err != nil {
		t.Fatal(err)
	}
	if statisticCount(runtime, "other", "commit") != 1 || statisticCount(runtime, "other", "conflict") != 1 || statisticCount(runtime, "other", "reduce") != 2 {
		t.Fatal("failed attempts inflated commit count", runtime.Statistics())
	}
	for _, value := range runtime.Statistics() {
		if value.Kind != "other" {
			t.Fatal("input data escaped the fixed metric vocabulary")
		}
	}
}

func TestCASStatisticsReadOnlyOwnerCheckDoesNotCountCommit(t *testing.T) {
	in := state.Input{ID: "owner", Kind: "owner-check", Fingerprint: "payload", Proof: true}
	store := &casScriptedStore{reads: []casRead{updateRead(in, true)}}
	runtime, err := NewCAS(store, ReducerFunc(func(record state.Record, _ state.Input, _ int64) (state.Payload, []byte, error) {
		return record.Payload, []byte("completed-no-op"), ErrReadOnly
	}), 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.ApplyCommitted(context.Background(), state.Ref{ID: "group"}, in); err != nil {
		t.Fatal(err)
	}
	if statisticCount(runtime, "owner-check", "commit") != 0 || statisticCount(runtime, "owner-check", "readonly") != 1 || store.casCalls != 0 {
		t.Fatal("healthy owner check became a protocol commit", runtime.Statistics())
	}
}
