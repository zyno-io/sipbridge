package engine

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"reflect"
	"strconv"
	"testing"
	"time"

	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/execution"
	executionfixture "sipbridge/internal/execution/fixture"
	"sipbridge/internal/state"
)

func staleTimerFixture(t *testing.T, due time.Time) (nativeOwnerFixture, *pb.ContextRef, state.Timer) {
	t.Helper()
	f, ref, input := preflightFixture(t, "REGISTER")
	f.flow.Transport = "udp"
	input.Ingest.Flow = f.flow
	if result := f.apply(t, ref, "initial-register", input); result.Error != "" {
		t.Fatal(result.Error)
	}
	_, group := stateTestLoad(t, f.engine, ref)
	var requestID string
	for id := range group.Contexts[ref.Id].Requests {
		requestID = id
	}
	if requestID == "" {
		t.Fatal("REGISTER fixture did not retain its application request")
	}
	response := f.apply(t, ref, "select-final", stateEngineInput{Kind: "command", Application: f.app, ApplicationLeaseTerm: f.term,
		Command: &pb.Command{Context: ref, Sequence: 1, Action: &pb.Command_Respond{Respond: &pb.Respond{RequestId: requestID, Status: 200,
			Authority: f.responseAuthority(t, ref, requestID)}}}})
	if response.Error != "" {
		t.Fatalf("REGISTER final response failed: %s", response.Error)
	}
	selectedTimer := ""
	logical, err := f.engine.group(ref)
	if err != nil {
		t.Fatal(err)
	}
	writer, err := executionfixture.New(f.engine.store.(*state.Store), execution.ReducerFunc(func(record state.Record, _ state.Input, _ int64) (state.Payload, []byte, error) {
		decoded, err := decodeStateGroup(record)
		if err != nil {
			return record.Payload, nil, err
		}
		c := decoded.Contexts[ref.Id]
		payload := record.Payload
		payload.Timers = append([]state.Timer(nil), payload.Timers...)
		found := false
		for key, tx := range c.Transactions {
			for name, deadline := range tx.Native.Timers {
				if deadline.Active {
					deadline.At = due.UnixMilli()
					tx.Native.Timers[name] = deadline
					selectedTimer = (&stateReduction{engine: f.engine}).timerID(ref.Id, "tx", key, name)
					found = true
					break
				}
			}
			if found {
				break
			}
		}
		if !found {
			t.Fatal("REGISTER final response did not retain an active native timer")
		}
		for i := range payload.Timers {
			if payload.Timers[i].ID == selectedTimer {
				payload.Timers[i].DueMS = due.UnixMilli()
				payload.Timers[i].DeadlineMS = due.UnixMilli()
			}
		}
		payload.ProtocolState, err = marshalRetainedFixture(decoded)
		return payload, nil, err
	}), "fixture-timer-writer", 1)
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second).UnixMilli()
	if _, err = writer.Apply(context.Background(), logical, state.Input{ID: "fixture-timer-due", Kind: "fixture", Fingerprint: "fixture", Proof: true, DeadlineMS: deadline, ReceiptUntilMS: deadline}); err != nil {
		t.Fatal(err)
	}
	record, _ := stateTestLoad(t, f.engine, ref)
	for _, timer := range record.Payload.Timers {
		if timer.ID == selectedTimer {
			if timer.DueMS != due.UnixMilli() {
				t.Fatal("native timer and projection disagree")
			}
			return f, ref, timer
		}
	}
	t.Fatal("selected native timer was not projected into the retained timer list")
	return nativeOwnerFixture{}, nil, state.Timer{}
}

func installTimerCAS(t *testing.T, e *StateEngine, process string) {
	t.Helper()
	runtime, err := execution.NewCAS(e.store, execution.ReducerFunc(e.reduce), 8)
	if err != nil {
		t.Fatal(err)
	}
	e.runtime = runtime
}

func TestStateCASStaleAndFutureTimersAreReadOnly(t *testing.T) {
	for _, test := range []struct {
		name   string
		future bool
		alter  func(state.Timer) (string, string)
	}{
		{name: "missing timer", alter: func(timer state.Timer) (string, string) { return "missing-" + timer.ID, timer.Generation }},
		{name: "stale generation", alter: func(timer state.Timer) (string, string) {
			generation, err := strconv.ParseUint(timer.Generation, 10, 64)
			if err != nil {
				t.Fatalf("invalid timer generation fixture: %v", err)
			}
			return timer.ID, strconv.FormatUint(generation+1, 10)
		}},
		{name: "not yet due", future: true, alter: func(timer state.Timer) (string, string) { return timer.ID, timer.Generation }},
	} {
		t.Run(test.name, func(t *testing.T) {
			due := time.Now().Add(-time.Second)
			if test.future {
				due = time.Now().Add(time.Hour)
			}
			f, ref, timer := staleTimerFixture(t, due)
			installTimerCAS(t, f.engine, "stale-timer-cas")
			before, _ := stateTestLoad(t, f.engine, ref)
			timerID, generation := test.alter(timer)
			id := f.engine.id("timer", ref.State.Incarnation, timerID, generation)
			calls := preflightEvalCalls(t, f)
			result, committed, err := f.engine.applyCommitted(context.Background(), state.Ref{
				ID: ref.State.GroupId, Partition: uint16(ref.State.Partition), CallID: ref.State.CallId, Incarnation: ref.State.Incarnation,
			}, id, stateEngineInput{Kind: "timer", Timer: timerID, Generation: generation}, time.Now().Add(time.Second), false)
			if err != nil || committed != nil || result == nil || result.Certainty != pb.OperationResult_NOT_ACCEPTED ||
				result.Error != "timer is not due" || result.Snapshot != nil {
				t.Fatalf("negative timer check was not a read-only refusal: result=%+v committed=%v err=%v", result, committed != nil, err)
			}
			if got := preflightEvalCalls(t, f) - calls; got != 1 {
				t.Fatalf("negative timer check used %d Redis scripts, want one read and no CAS", got)
			}
			after, _ := stateTestLoad(t, f.engine, ref)
			if after.Revision != before.Revision || !bytes.Equal(after.Payload.ProtocolState, before.Payload.ProtocolState) ||
				!reflect.DeepEqual(after.Payload.Timers, before.Payload.Timers) || !reflect.DeepEqual(after.Payload.Effects, before.Payload.Effects) {
				t.Fatal("negative timer check changed the retained revision, protocol state, timers or effects")
			}
		})
	}
}

func TestStateCASTwoWorkersApplyOneOverdueTimerTransition(t *testing.T) {
	f, ref, timer := staleTimerFixture(t, time.Now().Add(-time.Second))
	installTimerCAS(t, f.engine, "timer-cas-a")
	engineB := &StateEngine{cfg: f.engine.cfg, store: f.engine.store, ctx: context.Background()}
	installTimerCAS(t, engineB, "timer-cas-b")
	before, _ := stateTestLoad(t, f.engine, ref)
	group := state.Ref{ID: ref.State.GroupId, Partition: uint16(ref.State.Partition), CallID: ref.State.CallId, Incarnation: ref.State.Incarnation}
	idA := f.engine.id("timer", group.Incarnation, timer.ID, timer.Generation, "worker-a")
	idB := f.engine.id("timer", group.Incarnation, timer.ID, timer.Generation, "worker-b")
	in := stateEngineInput{Kind: "timer", Timer: timer.ID, Generation: timer.Generation}
	start := make(chan struct{})
	type outcome struct {
		result    *pb.OperationResult
		committed *execution.Committed
		err       error
	}
	results := make(chan outcome, 2)
	for _, worker := range []struct {
		engine *StateEngine
		id     string
	}{{engine: f.engine, id: idA}, {engine: engineB, id: idB}} {
		go func(worker struct {
			engine *StateEngine
			id     string
		}) {
			<-start
			result, committed, err := worker.engine.applyCommitted(context.Background(), group, worker.id, in, time.Now().Add(3*time.Second), false)
			results <- outcome{result: result, committed: committed, err: err}
		}(worker)
	}
	close(start)
	commits := 0
	completed := 0
	readonly := 0
	for range 2 {
		got := <-results
		if got.err != nil || got.result == nil {
			t.Fatalf("overdue timer attempt failed: result=%+v err=%v", got.result, got.err)
		}
		if got.result.Certainty == pb.OperationResult_COMPLETED {
			completed++
		} else if got.result.Certainty == pb.OperationResult_NOT_ACCEPTED && got.result.Error == "timer is not due" {
			readonly++
		} else {
			t.Fatalf("overdue timer attempt had unexpected result: %+v", got.result)
		}
		if got.committed != nil {
			commits++
		}
	}
	after, _ := stateTestLoad(t, f.engine, ref)
	beforeRevision, err := strconv.ParseUint(before.Revision, 10, 64)
	if err != nil || before.Revision != strconv.FormatUint(beforeRevision, 10) {
		t.Fatalf("invalid retained revision %q: %v", before.Revision, err)
	}
	if commits != 1 || completed != 1 || readonly != 1 || after.Revision != strconv.FormatUint(beforeRevision+1, 10) {
		t.Fatalf("exact timer produced %d transitions and revision %s; want one transition and revision %s", commits, after.Revision, strconv.FormatUint(beforeRevision+1, 10))
	}
	for _, retained := range after.Payload.Timers {
		if retained.ID == timer.ID && retained.Generation == timer.Generation {
			t.Fatal("completed timer generation remained current")
		}
	}
}

func TestStateCASLegacyPendingStaleTimerFailsClosed(t *testing.T) {
	f, ref, timer := staleTimerFixture(t, time.Now().Add(-time.Second))
	group := state.Ref{ID: ref.State.GroupId, Partition: uint16(ref.State.Partition), CallID: ref.State.CallId, Incarnation: ref.State.Incarnation}
	generation, err := strconv.ParseUint(timer.Generation, 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	in := stateEngineInput{Kind: "timer", Timer: timer.ID, Generation: strconv.FormatUint(generation+1, 10)}
	payload, err := marshalRetainedFixture(in)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(payload)
	deadline := time.Now().Add(3 * time.Second).UnixMilli()
	id := f.engine.id("timer", group.Incarnation, timer.ID, timer.Generation)
	begun, err := f.engine.store.(*state.Store).Begin(context.Background(), group, "old-worker", state.Input{ID: id, Kind: "timer",
		Fingerprint: hex.EncodeToString(digest[:]), Payload: payload, Proof: true, DeadlineMS: deadline, ReceiptUntilMS: deadline})
	if err != nil || begun.Step == nil {
		t.Fatalf("legacy pending timer fixture failed: %v", err)
	}
	key := f.prefix + "{p0000}:group:" + group.ID
	if err = f.client.HSet(context.Background(), key, "claimUntilMs", "0").Err(); err != nil {
		t.Fatal(err)
	}
	installTimerCAS(t, f.engine, "cas-timer-refusal")
	assertCASPendingRefused(t, f.engine, group)
}
