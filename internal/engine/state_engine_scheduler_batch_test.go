package engine

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"sipbridge/internal/execution"
	executionfixture "sipbridge/internal/execution/fixture"
	"sipbridge/internal/state"
)

type heldBatchStore struct {
	blockedScheduleStore
	started chan state.Ref
	release chan struct{}
}

func (s *heldBatchStore) Resume(ctx context.Context, ref state.Ref, _ string) (state.BeginResult, error) {
	select {
	case s.started <- ref:
	case <-ctx.Done():
		return state.BeginResult{}, ctx.Err()
	}
	select {
	case <-s.release:
		return state.BeginResult{}, state.ErrIdle
	case <-ctx.Done():
		return state.BeginResult{}, ctx.Err()
	}
}

func TestStateSchedulerBatchSaturationRotatesAcrossNamespaceSweeps(t *testing.T) {
	for _, sample := range []struct {
		name             string
		partitions       uint16
		activePartitions int
		emptyPasses      int
		leaders          []uint16
		final            stateScheduleCursor
	}{
		{"64/8", 64, 8, 7, []uint16{0, 1, 2, 3, 4, 5, 6, 7}, stateScheduleCursor{}},
		{"9/2", 9, 9, 0, []uint16{0, 2, 4, 6, 8, 1, 3, 5, 7}, stateScheduleCursor{admission: 1}},
	} {
		t.Run(sample.name, func(t *testing.T) {
			testStateSchedulerBatchSaturation(t, sample.partitions, sample.activePartitions, sample.emptyPasses, sample.leaders, sample.final)
		})
	}
}

func testStateSchedulerBatchSaturation(t *testing.T, partitions uint16, activePartitions, emptyPasses int, leaders []uint16, final stateScheduleCursor) {
	t.Helper()
	engine, _ := historicalStateTestEnginePartitioned(t, partitions)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	engine.ctx, engine.cancel = ctx, cancel
	engine.work, engine.background = make(chan struct{}, 64), make(chan struct{}, 48)
	engine.scheduled = make(map[string]bool)
	t.Cleanup(engine.Close)
	client := redis.NewClient(&redis.Options{Addr: os.Getenv("SIPBRIDGE_STATE_TEST_REDIS")})
	t.Cleanup(func() { client.Close() })
	counts := make([]int, activePartitions)
	for n, remaining := 0, activePartitions*16; remaining > 0; n++ {
		ref, err := engine.store.GroupForCallID(fmt.Sprintf("scheduler-burst-%d", n))
		if err != nil {
			t.Fatal(err)
		}
		if int(ref.Partition) >= activePartitions || counts[ref.Partition] >= 16 {
			continue
		}
		seed := state.Input{ID: "seed", Kind: "locate", Fingerprint: "seed", Create: true,
			DeadlineMS: time.Now().Add(time.Minute).UnixMilli(), ReceiptUntilMS: time.Now().Add(2 * time.Minute).UnixMilli()}
		if _, err := engine.store.(*state.Store).Begin(ctx, ref, "seed", seed); err != nil {
			t.Fatal(err)
		}
		prefix := fmt.Sprintf("sipstate:engine-test:%s:{p%04d}:", engine.store.Namespace(), ref.Partition)
		class := "ready"
		if counts[ref.Partition] >= 8 {
			class = "due"
			// Keep exactly eight independent ready and due groups, even if a
			// test run outlasts their original temporary computation claims.
			if err := client.ZRem(ctx, prefix+"ready", prefix+"group:"+ref.ID).Err(); err != nil {
				t.Fatal(err)
			}
		}
		if err := client.ZAdd(ctx, prefix+class, redis.Z{Score: 1, Member: prefix + "group:" + ref.ID}).Err(); err != nil {
			t.Fatal(err)
		}
		counts[ref.Partition]++
		remaining--
	}
	fake := &heldBatchStore{started: make(chan state.Ref, 16)}
	var err error
	engine.runtime, err = executionfixture.New(fake, execution.ReducerFunc(engine.reduce), "burst-test", 64)
	if err != nil {
		t.Fatal(err)
	}
	var cursor stateScheduleCursor
	for visit, leader := range leaders {
		fake.release = make(chan struct{})
		engine.scheduleHints(ctx, &cursor)
		for n := 0; n < 16; n++ {
			select {
			case ref := <-fake.started:
				if ref.Partition != leader {
					t.Fatalf("visit %d favored partition %d, want %d", visit, ref.Partition, leader)
				}
			case <-time.After(time.Second):
				t.Fatalf("visit %d never filled bounded admission", visit)
			}
		}
		engine.lifecycleMu.Lock()
		active := len(engine.scheduled)
		engine.lifecycleMu.Unlock()
		if active != 16 || len(engine.work) != 16 || len(engine.background) != 16 {
			t.Fatalf("batch changed admission bounds: active=%d work=%d background=%d", active, len(engine.work), len(engine.background))
		}
		// Real 64-partition sweeps take eight passes. Rotating every pass
		// would alias back to the same admission order and fail this test.
		for pass := 0; pass < emptyPasses; pass++ {
			engine.scheduleHints(ctx, &cursor)
		}
		close(fake.release)
		engine.wg.Wait()
	}
	if cursor != final {
		t.Fatalf("cursor failed to complete its cycle: %+v, want %+v", cursor, final)
	}
}

func TestStateSchedulerBatchFailureAdvancesCursor(t *testing.T) {
	engine, _ := historicalStateTestEnginePartitioned(t, 64)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	cursor := stateScheduleCursor{partition: 56, admission: 3}
	engine.scheduleHints(ctx, &cursor)
	if cursor.partition != 0 || cursor.admission != 4 {
		t.Fatalf("failed poll pinned the cursor: %+v", cursor)
	}
}
