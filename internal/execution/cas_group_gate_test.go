package execution

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"sipbridge/internal/state"
)

type casGateStore struct {
	mu        sync.Mutex
	reads     int
	casCalls  int
	readStart chan state.Ref
	casError  error
	receipts  map[string]state.Receipt
}

func (s *casGateStore) ReadUpdate(_ context.Context, ref state.Ref, input state.Input) (state.UpdateResult, error) {
	s.mu.Lock()
	s.reads++
	receipt, replay := s.receipts[input.ID]
	s.mu.Unlock()
	if s.readStart != nil {
		s.readStart <- ref
	}
	if replay {
		return state.UpdateResult{Receipt: &receipt}, nil
	}
	return state.UpdateResult{Update: &state.Update{Record: state.Record{Ref: ref}, Input: input, NowMS: 100}}, nil
}

func (s *casGateStore) CompareAndSwap(_ context.Context, update state.Update, _ state.Payload, outcome []byte) (state.Receipt, error) {
	s.mu.Lock()
	s.casCalls++
	err := s.casError
	s.casError = nil
	s.mu.Unlock()
	return state.Receipt{OperationID: update.Input.ID, Fingerprint: update.Input.Fingerprint, Outcome: outcome}, err
}

func (s *casGateStore) InspectOperation(context.Context, state.Ref, string, string) (state.Inspection, error) {
	return state.Inspection{}, nil
}

func (s *casGateStore) readCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.reads
}

func (s *casGateStore) casCallCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.casCalls
}

func (s *casGateStore) setReceipt(receipt state.Receipt) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.receipts == nil {
		s.receipts = make(map[string]state.Receipt)
	}
	s.receipts[receipt.OperationID] = receipt
}

func newGatedCASTestRuntime(t *testing.T, store *casGateStore, reducer Reducer) *CASRuntime {
	return newGatedCASTestRuntimeWithConcurrency(t, store, reducer, 4)
}

func newGatedCASTestRuntimeWithConcurrency(t *testing.T, store *casGateStore, reducer Reducer, concurrency int) *CASRuntime {
	t.Helper()
	runtime, err := NewCAS(store, reducer, concurrency)
	if err != nil {
		t.Fatal(err)
	}
	return runtime
}

func gatedInput(id string) state.Input {
	return state.Input{ID: id, Fingerprint: id, Kind: "command", DeadlineMS: time.Now().Add(time.Second).UnixMilli()}
}

func TestCASGroupGateSerializesIncarnationsAndCancelledWaiterDoesNotRead(t *testing.T) {
	store := &casGateStore{readStart: make(chan state.Ref, 4)}
	firstReduction := make(chan struct{})
	releaseFirst := make(chan struct{})
	started := make(chan string, 4)
	runtime := newGatedCASTestRuntime(t, store, ReducerFunc(func(record state.Record, input state.Input, _ int64) (state.Payload, []byte, error) {
		started <- input.ID
		if input.ID == "first" {
			close(firstReduction)
			<-releaseFirst
		}
		return record.Payload, []byte(input.ID), nil
	}))
	firstRef := state.Ref{Partition: 7, ID: "same-group", Incarnation: "incarnation-a"}
	secondRef := state.Ref{Partition: 7, ID: "same-group", Incarnation: "incarnation-b"}
	firstDone := make(chan error, 1)
	go func() {
		_, err := runtime.ApplyCommitted(context.Background(), firstRef, gatedInput("first"))
		firstDone <- err
	}()
	select {
	case <-firstReduction:
	case <-time.After(time.Second):
		t.Fatal("first reducer did not start")
	}
	if id := <-started; id != "first" {
		t.Fatalf("unexpected first reducer: %s", id)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, waitErr := runtime.ApplyCommitted(ctx, secondRef, gatedInput("cancelled"))
	if !errors.Is(waitErr, ErrUncommitted) || !errors.Is(waitErr, context.DeadlineExceeded) {
		t.Fatalf("cancelled group waiter returned %v", waitErr)
	}
	if got := store.readCount(); got != 1 {
		t.Fatalf("cancelled waiter read Redis while waiting for group gate: reads=%d", got)
	}

	secondStarted := make(chan struct{})
	secondDone := make(chan error, 1)
	go func() {
		close(secondStarted)
		_, err := runtime.ApplyCommitted(context.Background(), secondRef, gatedInput("second"))
		secondDone <- err
	}()
	<-secondStarted
	select {
	case id := <-started:
		t.Fatalf("different incarnation reduced concurrently: %s", id)
	case <-time.After(20 * time.Millisecond):
	}
	if got := store.readCount(); got != 1 {
		t.Fatalf("queued incarnation read Redis before the group gate: reads=%d", got)
	}

	close(releaseFirst)
	if err := <-firstDone; err != nil {
		t.Fatalf("first CAS failed: %v", err)
	}
	if id := <-started; id != "second" {
		t.Fatalf("successor reducer did not start after release: %s", id)
	}
	if err := <-secondDone; err != nil || store.readCount() != 2 {
		t.Fatalf("successor incarnation did not proceed after release: reads=%d err=%v", store.readCount(), err)
	}
}

func TestCASGroupGateAllowsDifferentStripesAndSerializesCollisions(t *testing.T) {
	store := &casGateStore{}
	entered := make(chan string, 4)
	release := make(chan struct{})
	runtime := newGatedCASTestRuntime(t, store, ReducerFunc(func(record state.Record, input state.Input, _ int64) (state.Payload, []byte, error) {
		entered <- input.ID
		<-release
		return record.Payload, []byte(input.ID), nil
	}))
	var differentA, differentB, collisionA, collisionB state.Ref
	seen := make(map[uint32]state.Ref)
	for i := 0; i < 10000 && (differentA.ID == "" || collisionA.ID == ""); i++ {
		ref := state.Ref{Partition: 3, ID: fmt.Sprintf("group-%d", i)}
		index := casGroupGateIndex(ref)
		if prior, ok := seen[index]; ok {
			if collisionA.ID == "" {
				collisionA, collisionB = prior, ref
			}
		} else {
			seen[index] = ref
		}
		if differentA.ID == "" {
			differentA = ref
		} else if index != casGroupGateIndex(differentA) && differentB.ID == "" {
			differentB = ref
		}
	}
	if differentB.ID == "" || collisionB.ID == "" {
		t.Fatal("could not find test groups for distinct and colliding stripes")
	}

	startPair := func(a, b state.Ref, idA, idB string) <-chan error {
		done := make(chan error, 2)
		go func() { _, err := runtime.ApplyCommitted(context.Background(), a, gatedInput(idA)); done <- err }()
		go func() { _, err := runtime.ApplyCommitted(context.Background(), b, gatedInput(idB)); done <- err }()
		return done
	}
	waitEntered := func(expected int) map[string]bool {
		t.Helper()
		got := make(map[string]bool)
		for len(got) < expected {
			select {
			case id := <-entered:
				got[id] = true
			case <-time.After(time.Second):
				t.Fatalf("only %d reducers entered; wanted %d", len(got), expected)
			}
		}
		return got
	}

	done := startPair(differentA, differentB, "parallel-a", "parallel-b")
	if got := waitEntered(2); !got["parallel-a"] || !got["parallel-b"] {
		t.Fatalf("different stripes did not compute concurrently: %v", got)
	}
	close(release)
	for range 2 {
		if err := <-done; err != nil {
			t.Fatalf("different-stripe CAS failed: %v", err)
		}
	}

	release = make(chan struct{})
	done = startPair(collisionA, collisionB, "collision-a", "collision-b")
	first := waitEntered(1)
	select {
	case id := <-entered:
		t.Fatalf("colliding stripes computed concurrently: first=%v second=%s", first, id)
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	if len(waitEntered(1)) != 1 {
		t.Fatal("second colliding group did not proceed after release")
	}
	for range 2 {
		if err := <-done; err != nil {
			t.Fatalf("collision-stripe CAS failed: %v", err)
		}
	}
}

func TestCASGroupGateReleasesAfterReducerAndUnknownCommitErrors(t *testing.T) {
	store := &casGateStore{}
	var mu sync.Mutex
	failReduction := true
	runtime := newGatedCASTestRuntime(t, store, ReducerFunc(func(record state.Record, input state.Input, _ int64) (state.Payload, []byte, error) {
		mu.Lock()
		fail := failReduction
		failReduction = false
		mu.Unlock()
		if fail {
			return state.Payload{}, nil, errors.New("reducer failure")
		}
		return record.Payload, []byte(input.ID), nil
	}))
	ref := state.Ref{Partition: 2, ID: "release-errors"}
	if _, err := runtime.ApplyCommitted(context.Background(), ref, gatedInput("reducer-error")); err == nil {
		t.Fatal("expected reducer error")
	}
	store.mu.Lock()
	store.casError = state.ErrUnknown
	store.mu.Unlock()
	if _, err := runtime.ApplyCommitted(context.Background(), ref, gatedInput("unknown-cas")); !errors.Is(err, state.ErrUnknown) {
		t.Fatalf("expected unknown commit result, got %v", err)
	}
	if _, err := runtime.ApplyCommitted(context.Background(), ref, gatedInput("after-errors")); err != nil {
		t.Fatalf("gate remained held after reducer or unknown commit error: %v", err)
	}
}

func TestCASGroupGateExpiryAllowsOnlyExactReplayRead(t *testing.T) {
	for _, replay := range []bool{false, true} {
		t.Run(map[bool]string{false: "no-replay", true: "exact-replay"}[replay], func(t *testing.T) {
			store := &casGateStore{}
			firstReduction := make(chan struct{})
			releaseFirst := make(chan struct{})
			var reducerCalls int
			runtime := newGatedCASTestRuntime(t, store, ReducerFunc(func(record state.Record, input state.Input, _ int64) (state.Payload, []byte, error) {
				reducerCalls++
				if input.ID == "holder" {
					close(firstReduction)
					<-releaseFirst
				}
				return record.Payload, []byte(input.ID), nil
			}))
			ref := state.Ref{Partition: 4, ID: "expiry-held-stripe"}
			firstDone := make(chan error, 1)
			go func() {
				_, err := runtime.ApplyCommitted(context.Background(), ref, gatedInput("holder"))
				firstDone <- err
			}()
			select {
			case <-firstReduction:
			case <-time.After(time.Second):
				t.Fatal("stripe holder did not reach reducer")
			}

			input := gatedInput("waiter")
			input.DeadlineMS = time.Now().Add(25 * time.Millisecond).UnixMilli()
			if replay {
				store.setReceipt(state.Receipt{OperationID: input.ID, Fingerprint: input.Fingerprint, Outcome: []byte("committed")})
			}
			started := time.Now()
			result, err := runtime.ApplyCommitted(context.Background(), ref, input)
			if time.Since(started) > time.Second {
				t.Fatal("semantic deadline did not bound the held-stripe wait")
			}
			if replay {
				if err != nil || string(result.Receipt.Outcome) != "committed" {
					t.Fatalf("expired exact replay was not returned: %+v, %v", result, err)
				}
			} else if !errors.Is(err, ErrUncommitted) || !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("expired fresh work returned %v", err)
			}
			if store.readCount() != 2 || store.casCallCount() != 0 || reducerCalls != 1 {
				t.Fatalf("deadline fallback did more than one read-only replay check: reads=%d CAS=%d reductions=%d", store.readCount(), store.casCallCount(), reducerCalls)
			}
			close(releaseFirst)
			if err := <-firstDone; err != nil {
				t.Fatalf("stripe holder failed: %v", err)
			}
		})
	}
}

func TestCASGlobalSlotExpiryFallbackUsesExisting64SlotBound(t *testing.T) {
	const concurrency = 64
	store := &casGateStore{}
	entered := make(chan string, concurrency)
	release := make(chan struct{})
	runtime := newGatedCASTestRuntimeWithConcurrency(t, store, ReducerFunc(func(record state.Record, input state.Input, _ int64) (state.Payload, []byte, error) {
		entered <- input.ID
		<-release
		return record.Payload, []byte(input.ID), nil
	}), concurrency)

	refs := make([]state.Ref, 0, concurrency)
	stripes := make(map[uint32]bool, concurrency)
	for i := 0; len(refs) < concurrency && i < 100000; i++ {
		ref := state.Ref{Partition: 11, ID: fmt.Sprintf("slot-group-%d", i)}
		stripe := casGroupGateIndex(ref)
		if stripes[stripe] {
			continue
		}
		stripes[stripe] = true
		refs = append(refs, ref)
	}
	if len(refs) != concurrency {
		t.Fatal("could not find independent stripes for global-slot test")
	}
	done := make(chan error, concurrency)
	for i, ref := range refs {
		go func(i int, ref state.Ref) {
			_, err := runtime.ApplyCommitted(context.Background(), ref, gatedInput(fmt.Sprintf("slot-holder-%d", i)))
			done <- err
		}(i, ref)
	}
	for i := 0; i < concurrency; i++ {
		select {
		case <-entered:
		case <-time.After(time.Second):
			close(release)
			t.Fatal("not all global execution slots became occupied")
		}
	}

	input := gatedInput("slot-expired-replay")
	input.DeadlineMS = time.Now().Add(40 * time.Millisecond).UnixMilli()
	store.setReceipt(state.Receipt{OperationID: input.ID, Fingerprint: input.Fingerprint, Outcome: []byte("committed")})
	replayDone := make(chan struct {
		result Result
		err    error
	}, 1)
	go func() {
		result, err := runtime.ApplyCommitted(context.Background(), state.Ref{Partition: 12, ID: "slot-waiter"}, input)
		replayDone <- struct {
			result Result
			err    error
		}{result: result, err: err}
	}()
	time.Sleep(60 * time.Millisecond)
	if reads := store.readCount(); reads != concurrency {
		close(release)
		t.Fatalf("slot waiter read before an existing slot became available: reads=%d", reads)
	}
	close(release)
	for i := 0; i < concurrency; i++ {
		if err := <-done; err != nil {
			t.Fatalf("slot holder failed: %v", err)
		}
	}
	select {
	case replay := <-replayDone:
		if replay.err != nil || string(replay.result.Receipt.Outcome) != "committed" {
			t.Fatalf("bounded global-slot replay failed: %+v, %v", replay.result, replay.err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("bounded global-slot replay did not finish")
	}
	if reads := store.readCount(); reads != concurrency+1 || store.casCallCount() != concurrency {
		t.Fatalf("expiry fallback performed fresh computation: reads=%d CAS=%d", reads, store.casCallCount())
	}
}

func TestCASParentCancellationWhileWaitingForGlobalSlotDoesNotRead(t *testing.T) {
	store := &casGateStore{}
	entered := make(chan struct{})
	release := make(chan struct{})
	runtime := newGatedCASTestRuntimeWithConcurrency(t, store, ReducerFunc(func(record state.Record, input state.Input, _ int64) (state.Payload, []byte, error) {
		if input.ID == "holder" {
			close(entered)
			<-release
		}
		return record.Payload, []byte(input.ID), nil
	}), 1)
	holderDone := make(chan error, 1)
	go func() {
		_, err := runtime.ApplyCommitted(context.Background(), state.Ref{Partition: 1, ID: "slot-parent-cancel"}, gatedInput("holder"))
		holderDone <- err
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("global slot holder did not reach reducer")
	}

	parent, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx, timing := WithTiming(parent)
	waiterDone := make(chan error, 1)
	waiterStarted := make(chan struct{})
	go func() {
		close(waiterStarted)
		_, err := runtime.ApplyCommitted(ctx, state.Ref{Partition: 2, ID: "slot-cancel-waiter"}, gatedInput("cancelled-slot-waiter"))
		waiterDone <- err
	}()
	<-waiterStarted
	time.Sleep(20 * time.Millisecond)
	cancel()
	if err := <-waiterDone; !errors.Is(err, ErrUncommitted) || !errors.Is(err, context.Canceled) {
		t.Fatalf("global-slot cancellation returned %v", err)
	}
	if timing.Slot == 0 || store.readCount() != 1 {
		t.Fatalf("cancelled global-slot waiter reached Redis: slot-wait=%s reads=%d", timing.Slot, store.readCount())
	}
	close(release)
	if err := <-holderDone; err != nil {
		t.Fatalf("global slot holder failed: %v", err)
	}
}
