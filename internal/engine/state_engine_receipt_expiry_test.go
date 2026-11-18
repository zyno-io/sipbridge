package engine

import (
	"context"
	"errors"
	"testing"
	"time"

	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/execution"
	"sipbridge/internal/state"
)

func receiptCommand(t *testing.T, f nativeOwnerFixture, ref *pb.ContextRef, operation string, sequence uint64, status uint32) stateEngineInput {
	t.Helper()
	_, group := stateTestLoad(t, f.engine, ref)
	var requestID string
	for id := range group.Contexts[ref.Id].Requests {
		requestID = id
	}
	command := &pb.Command{Context: ref, Sequence: sequence, Action: &pb.Command_Respond{Respond: &pb.Respond{RequestId: requestID, Status: status}}}
	f.admitCommand(t, ref, operation, command)
	return stateEngineInput{Kind: "command", Context: ref, Operation: operation, Application: f.app, ApplicationLeaseTerm: f.term, Command: command}
}

func TestExpiredOperationMarkerCannotReplayOrSubstituteCurrentResult(t *testing.T) {
	for _, backend := range []string{state.MemoryBackend, state.RedisBackend} {
		for _, refused := range []bool{false, true} {
			name := "accepted"
			if refused {
				name = "refused"
			}
			t.Run(backend+"/"+name, func(t *testing.T) {
				f := newNativeOwnerFixtureBackend(t, backend)
				ref := f.admit(t)
				groupRef, err := f.engine.group(ref)
				if err != nil {
					t.Fatal(err)
				}
				sequence := uint64(1)
				if refused {
					sequence = 2 // No command may skip the first sequence.
				}
				original := receiptCommand(t, f, ref, "original", sequence, 180)
				deadline := time.Now().Add(time.Second)
				first, committed, err := f.engine.applyCommitted(context.Background(), groupRef, "original", original, deadline, false)
				if err != nil || committed == nil || first == nil || (first.Certainty == pb.OperationResult_NOT_ACCEPTED) != refused {
					t.Fatal("fixture did not commit original operation", err)
				}
				nextSequence := uint64(2)
				if refused {
					nextSequence = 1
				}
				later := receiptCommand(t, f, ref, "later", nextSequence, 183)
				current, _, err := f.engine.applyCommitted(context.Background(), groupRef, "later", later, time.Now().Add(2*time.Second), false)
				if err != nil || current.Error != "" || current.Snapshot == nil {
					t.Fatal("fixture did not advance current command", err)
				}
				time.Sleep(max(0, time.Until(deadline)) + 5*time.Millisecond)
				before, _ := stateTestLoad(t, f.engine, ref)
				found := false
				for _, receipt := range before.Receipts {
					if receipt.OperationID == "original" {
						found = true
						if !receipt.OutcomeDiscarded || len(receipt.Outcome) != 0 || receipt.ResultUntilMS != deadline.UnixMilli() {
							t.Fatal("full read returned expired historical payload or renewed cutoff")
						}
					}
				}
				if !found {
					t.Fatal("commit evidence disappeared with its result")
				}
				// A sibling with a fresh budget must observe the original marker.
				sibling := &StateEngine{cfg: f.engine.cfg, store: f.engine.store}
				sibling.cfg.Process = &pb.ProcessRef{InstanceId: "sibling", Epoch: "another-epoch"}
				reductions := 0
				sibling.runtime, err = execution.NewCAS(sibling.store, execution.ReducerFunc(func(state.Record, state.Input, int64) (state.Payload, []byte, error) {
					reductions++
					return state.Payload{}, nil, errors.New("expired receipt attempted another reduction")
				}), 1)
				if err != nil {
					t.Fatal(err)
				}
				replay, projection, err := sibling.applyCommitted(context.Background(), groupRef, "original", original, time.Now().Add(2*time.Second), false)
				if err != nil || projection != nil || replay.Certainty != pb.OperationResult_UNKNOWN || replay.Error == "" || replay.Snapshot != nil || replay.Request != nil || replay.Response != nil || reductions != 0 {
					t.Fatal("expired result was reexecuted, fabricated or returned as definite", err)
				}
				after, _ := stateTestLoad(t, f.engine, ref)
				if after.Revision != before.Revision || len(after.Payload.Effects) != len(before.Payload.Effects) {
					t.Fatal("marker replay mutated native work")
				}
			})
		}
	}
}

type delayedReceiptCommit struct {
	state.Backend
	operation string
	lost      bool
}

func (s delayedReceiptCommit) CompareAndSwap(ctx context.Context, update state.Update, payload state.Payload, outcome []byte) (state.Receipt, error) {
	receipt, err := s.Backend.CompareAndSwap(ctx, update, payload, outcome)
	if err != nil || update.Input.ID != s.operation {
		return receipt, err
	}
	time.Sleep(max(0, time.Until(time.UnixMilli(update.Input.ResultUntilMS))) + 5*time.Millisecond)
	if s.lost {
		return state.Receipt{}, state.ErrUnknown
	}
	return receipt, nil
}

func TestResultCutoffPreservesFreshCommitButNotUncertainProjection(t *testing.T) {
	for _, backend := range []string{state.MemoryBackend, state.RedisBackend} {
		for _, lost := range []bool{false, true} {
			name := "definite"
			if lost {
				name = "lost-acknowledgement"
			}
			t.Run(backend+"/"+name, func(t *testing.T) {
				f := newNativeOwnerFixtureBackend(t, backend)
				ref := f.admit(t)
				input := receiptCommand(t, f, ref, "delayed", 1, 180)
				refGroup, err := f.engine.group(ref)
				if err != nil {
					t.Fatal(err)
				}
				f.engine.runtime, err = execution.NewCAS(delayedReceiptCommit{Backend: f.engine.store, operation: "delayed", lost: lost}, execution.ReducerFunc(f.engine.reduce), 1)
				if err != nil {
					t.Fatal(err)
				}
				result, committed, err := f.engine.applyCommitted(context.Background(), refGroup, "delayed", input, time.Now().Add(time.Second), false)
				if lost {
					if !errors.Is(err, state.ErrUnknown) || result != nil || committed != nil {
						t.Fatal("lost acknowledgement published a fresh result projection", err)
					}
				} else if err != nil || committed == nil || result == nil || result.Snapshot == nil || result.Error != "" {
					t.Fatal("definite fresh invocation lost its own result after acknowledgement delay", err)
				}
				before, _ := stateTestLoad(t, f.engine, ref)
				replay, projection, err := f.engine.applyCommitted(context.Background(), refGroup, "delayed", input, time.Now().Add(time.Second), false)
				if err != nil || projection != nil || replay.Certainty != pb.OperationResult_UNKNOWN || replay.Snapshot != nil {
					t.Fatal("late retry resurrected a fresh or historical result", err)
				}
				after, _ := stateTestLoad(t, f.engine, ref)
				if after.Revision != before.Revision {
					t.Fatal("lost reply caused duplicate native execution")
				}
			})
		}
	}
}
