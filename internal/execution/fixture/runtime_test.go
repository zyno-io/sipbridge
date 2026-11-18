package fixture

import (
	"context"
	"errors"
	"testing"

	"sipbridge/internal/execution"
	"sipbridge/internal/state"
)

type scriptedStore struct {
	begins      []state.BeginResult
	beginRefs   []state.Ref
	commits     []string
	failCommit  error
	inspection  state.Inspection
	inspections int
}

func (s *scriptedStore) Begin(_ context.Context, ref state.Ref, _ string, _ state.Input) (state.BeginResult, error) {
	s.beginRefs = append(s.beginRefs, ref)
	if len(s.begins) == 0 {
		return state.BeginResult{}, errors.New("unexpected begin")
	}
	v := s.begins[0]
	s.begins = s.begins[1:]
	return v, nil
}
func (s *scriptedStore) Resume(ctx context.Context, ref state.Ref, worker string) (state.BeginResult, error) {
	return s.Begin(ctx, ref, worker, state.Input{})
}
func (s *scriptedStore) Commit(_ context.Context, step state.Step, _ state.Payload, out []byte) (state.Receipt, error) {
	s.commits = append(s.commits, step.Input.ID)
	return state.Receipt{OperationID: step.Input.ID, Fingerprint: step.Input.Fingerprint, Outcome: out}, s.failCommit
}
func (s *scriptedStore) InspectOperation(context.Context, state.Ref, string, string) (state.Inspection, error) {
	s.inspections++
	return s.inspection, nil
}
func pending(id string) state.BeginResult {
	return state.BeginResult{Step: &state.Step{Record: state.Record{Ref: state.Ref{ID: "group", Incarnation: "incarnation"}}, Input: state.Input{ID: id, Fingerprint: id}, NowMS: 100}}
}

func TestRecoveredInputFinishesBeforeCandidateAndPinsIncarnation(t *testing.T) {
	store := &scriptedStore{begins: []state.BeginResult{pending("older"), pending("candidate")}}
	var reduced []string
	driver, err := New(store, execution.ReducerFunc(func(r state.Record, in state.Input, now int64) (state.Payload, []byte, error) {
		reduced = append(reduced, in.ID)
		if now != 100 {
			t.Fatal("lost storage time")
		}
		return r.Payload, []byte(in.ID), nil
	}), "replica", 1)
	if err != nil {
		t.Fatal(err)
	}
	result, err := driver.Apply(context.Background(), state.Ref{ID: "group"}, state.Input{ID: "candidate", Fingerprint: "candidate"})
	if err != nil || result.OperationID != "candidate" || len(reduced) != 2 || reduced[0] != "older" || store.beginRefs[1].Incarnation != "incarnation" {
		t.Fatalf("recovery: %+v %v reduced=%v refs=%+v", result, err, reduced, store.beginRefs)
	}
}

func TestKnownReceiptNeverReducesOrCommitsAgain(t *testing.T) {
	known := &state.Receipt{OperationID: "known", Outcome: []byte("completed")}
	store := &scriptedStore{begins: []state.BeginResult{{Receipt: known}}}
	driver, _ := New(store, execution.ReducerFunc(func(state.Record, state.Input, int64) (state.Payload, []byte, error) {
		t.Fatal("reduced known receipt")
		return state.Payload{}, nil, nil
	}), "replica", 1)
	result, err := driver.Apply(context.Background(), state.Ref{}, state.Input{ID: "known"})
	if err != nil || string(result.Outcome) != "completed" || len(store.commits) != 0 {
		t.Fatalf("receipt replay: %+v %v", result, err)
	}
}

func TestLostCommitAcknowledgementInspectsWithoutReexecution(t *testing.T) {
	receipt := &state.Receipt{OperationID: "input", Outcome: []byte("committed")}
	store := &scriptedStore{begins: []state.BeginResult{pending("input")}, failCommit: state.ErrUnknown, inspection: state.Inspection{Receipt: receipt}}
	reductions := 0
	driver, _ := New(store, execution.ReducerFunc(func(r state.Record, in state.Input, now int64) (state.Payload, []byte, error) {
		reductions++
		return r.Payload, []byte("computed"), nil
	}), "replica", 1)
	result, err := driver.Apply(context.Background(), state.Ref{}, state.Input{ID: "input", Fingerprint: "input"})
	if err != nil || string(result.Outcome) != "committed" || reductions != 1 || len(store.commits) != 1 || store.inspections != 1 {
		t.Fatalf("uncertain commit: %+v %v reductions=%d commits=%v inspections=%d", result, err, reductions, store.commits, store.inspections)
	}
}

func TestReducerFailureLeavesInputForExplicitRecovery(t *testing.T) {
	failure := errors.New("corrupt protocol state")
	store := &scriptedStore{begins: []state.BeginResult{pending("input")}}
	driver, _ := New(store, execution.ReducerFunc(func(state.Record, state.Input, int64) (state.Payload, []byte, error) {
		return state.Payload{}, nil, failure
	}), "replica", 1)
	_, err := driver.Apply(context.Background(), state.Ref{}, state.Input{ID: "input", Fingerprint: "input"})
	if !errors.Is(err, failure) || len(store.commits) != 0 {
		t.Fatalf("reducer failure: %v commits=%v", err, store.commits)
	}
}
