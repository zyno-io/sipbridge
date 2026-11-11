package engine

import (
	"context"
	"errors"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"sipbridge/internal/execution"
	executionfixture "sipbridge/internal/execution/fixture"
	"sipbridge/internal/state"
)

type blockedScheduleStore struct {
	hot     string
	started chan struct{}
	healthy chan struct{}
	calls   atomic.Int64
	resumes atomic.Int64
}

func (s *blockedScheduleStore) Begin(ctx context.Context, ref state.Ref, _ string, _ state.Input) (state.BeginResult, error) {
	if ref.ID != s.hot {
		return state.BeginResult{}, errors.New("unexpected begin")
	}
	s.calls.Add(1)
	select {
	case s.started <- struct{}{}:
	default:
	}
	<-ctx.Done()
	return state.BeginResult{}, ctx.Err()
}
func (s *blockedScheduleStore) Commit(context.Context, state.Step, state.Payload, []byte) (state.Receipt, error) {
	return state.Receipt{}, errors.New("unused commit")
}
func (s *blockedScheduleStore) InspectOperation(context.Context, state.Ref, string, string) (state.Inspection, error) {
	return state.Inspection{}, errors.New("unused inspect")
}
func (s *blockedScheduleStore) Resume(ctx context.Context, ref state.Ref, _ string) (state.BeginResult, error) {
	s.resumes.Add(1)
	if ref.ID != s.hot {
		select {
		case s.healthy <- struct{}{}:
		default:
		}
	}
	return state.BeginResult{}, state.ErrIdle
}

func TestStateSchedulerCorruptHintClassDoesNotStarveOtherClass(t *testing.T) {
	for _, healthyClass := range []string{"ready", "due"} {
		t.Run(healthyClass, func(t *testing.T) {
			engine, _ := historicalStateTestEngine(t)
			ctx, cancel := context.WithCancel(context.Background())
			engine.ctx, engine.cancel = ctx, cancel
			engine.work, engine.background = make(chan struct{}, 64), make(chan struct{}, 48)
			engine.scheduled = make(map[string]bool)
			t.Cleanup(engine.Close)
			client := redis.NewClient(&redis.Options{Addr: os.Getenv("SIPBRIDGE_STATE_TEST_REDIS")})
			t.Cleanup(func() { client.Close() })
			ref, err := engine.store.GroupForCallID("healthy-" + healthyClass)
			if err != nil {
				t.Fatal(err)
			}
			payload, err := marshalRetainedFixture(stateEngineInput{Kind: "locate"})
			if err != nil {
				t.Fatal(err)
			}
			input := state.Input{ID: "seed", Kind: "locate", Fingerprint: "seed", Payload: payload, Create: true,
				DeadlineMS: time.Now().Add(5 * time.Second).UnixMilli(), ReceiptUntilMS: time.Now().Add(time.Minute).UnixMilli()}
			begun, err := engine.store.(*state.Store).Begin(ctx, ref, "seed", input)
			if err != nil {
				t.Fatal(err)
			}
			ref = begun.Step.Record.Ref
			prefix := "sipstate:engine-test:" + engine.store.Namespace() + ":{p0000}:"
			key := prefix + "group:" + ref.ID
			poisoned := prefix + "due"
			if healthyClass == "ready" {
				if err := client.HSet(ctx, key, "claimUntilMs", "1").Err(); err != nil {
					t.Fatal(err)
				}
				if err := client.ZAdd(ctx, prefix+"ready", redis.Z{Score: 1, Member: key}).Err(); err != nil {
					t.Fatal(err)
				}
			} else {
				if _, err := engine.store.(*state.Store).Commit(ctx, *begun.Step, state.Payload{Timers: []state.Timer{{ID: "due", Generation: "1", DueMS: 1, DeadlineMS: 1}}}, nil); err != nil {
					t.Fatal(err)
				}
				poisoned = prefix + "ready"
			}
			if err := client.ZAdd(ctx, poisoned, redis.Z{Score: 1, Member: "persistent-poisoned-member"}).Err(); err != nil {
				t.Fatal(err)
			}
			before, err := engine.store.Load(ctx, ref)
			if err != nil {
				t.Fatal(err)
			}
			engine.wg.Add(1)
			go func() { defer engine.wg.Done(); engine.schedule() }()
			deadline := time.Now().Add(2 * time.Second)
			for {
				after, err := engine.store.Load(ctx, ref)
				if err != nil {
					t.Fatal(err)
				}
				if after.Revision != before.Revision && after.Pending == nil && len(after.Payload.Timers) == 0 {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("a corrupt hint in the other class starved healthy " + healthyClass + " work")
				}
				time.Sleep(10 * time.Millisecond)
			}
			if _, err := client.ZScore(ctx, poisoned, "persistent-poisoned-member").Result(); err != nil {
				t.Fatal("fixture removed the poisoned member instead of isolating it", err)
			}
		})
	}
}

func TestStateSchedulerPartitionBatchKeepsSmallNamespacesWithinSweep(t *testing.T) {
	for _, sample := range []struct {
		partitions uint16
		batch      int
	}{{1, 1}, {2, 1}, {8, 1}, {9, 2}, {16, 2}, {17, 3}, {32, 4}, {63, 8}, {64, 8}, {4096, 8}} {
		batch := stateSchedulePartitions(sample.partitions)
		if batch != sample.batch || batch < 1 || batch > 8 {
			t.Fatalf("partition batch for %d: %d", sample.partitions, batch)
		}
		if sample.partitions <= 64 && (int(sample.partitions)+batch-1)/batch > 8 {
			t.Fatalf("healthy partition sweep exceeds160ms: %d partitions, %d per pass", sample.partitions, batch)
		}
	}
}

func TestStateSchedulerQuiescentGroupDoesNotResumeOrMutate(t *testing.T) {
	engine, _ := historicalStateTestEngine(t)
	ctx, cancel := context.WithCancel(context.Background())
	engine.ctx, engine.cancel = ctx, cancel
	engine.work, engine.background = make(chan struct{}, 64), make(chan struct{}, 48)
	engine.scheduled = make(map[string]bool)
	t.Cleanup(engine.Close)
	ref, err := engine.store.GroupForCallID("quiescent-scheduler")
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	seed := state.Input{ID: "seed", Kind: "locate", Fingerprint: "seed", Create: true, DeadlineMS: now.Add(time.Second).UnixMilli(), ReceiptUntilMS: now.Add(time.Minute).UnixMilli()}
	begun, err := engine.store.(*state.Store).Begin(ctx, ref, "seed", seed)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := engine.store.(*state.Store).Commit(ctx, *begun.Step, state.Payload{Timers: []state.Timer{{ID: "future", Generation: "1", DueMS: now.Add(time.Hour).UnixMilli(), DeadlineMS: now.Add(time.Hour).UnixMilli()}}}, nil); err != nil {
		t.Fatal(err)
	}
	ref = begun.Step.Record.Ref
	before, err := engine.store.Load(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	fake := &blockedScheduleStore{hot: ref.ID, started: make(chan struct{}, 1), healthy: make(chan struct{}, 1)}
	engine.runtime, err = executionfixture.New(fake, execution.ReducerFunc(engine.reduce), "quiescent-test", 64)
	if err != nil {
		t.Fatal(err)
	}
	for n := 0; n < 5; n++ {
		engine.scheduleGroup(ref, n%2 == 0)
		engine.wg.Wait()
	}
	if fake.calls.Load() != 0 || fake.resumes.Load() != 0 || len(engine.background) != 0 || len(engine.work) != 0 {
		t.Fatal("future timer/no-effect hints created reducer recovery or retained dispatch workers")
	}
	after, err := engine.store.Load(ctx, ref)
	if err != nil || after.Revision != before.Revision || after.Pending != nil {
		t.Fatalf("quiescent scheduling mutated native state: %v", err)
	}
}

func TestStateSchedulerEligibilityPreservesHistoricalRecovery(t *testing.T) {
	body := func(value stateEngineEffect) []byte { encoded, _ := marshalRetainedFixture(value); return encoded }
	effects := []state.Effect{
		{ID: "unsupported", Kind: "unsupported", Status: state.EffectPending, DeadlineMS: 300000},
		{ID: "completed-event", Kind: "event", Status: state.EffectWritten, DeadlineMS: 1},
		{ID: "owner-backoff", Kind: "owner", Status: state.EffectUnknown, DeadlineMS: 1},
		{ID: "owner-rearmed", Kind: "owner", Status: state.EffectPending, DeadlineMS: 1},
		{ID: "settled", Kind: "deliver", Status: state.EffectWritten, DeadlineMS: 1, Payload: body(stateEngineEffect{Settled: true})},
		{ID: "unsettled", Kind: "deliver", Status: state.EffectNotWritten, DeadlineMS: 1, Payload: body(stateEngineEffect{})},
		{ID: "physical-unknown", Kind: "deliver", Status: state.EffectUnknown, DeadlineMS: 1},
		{ID: "cancelled-authorization", Kind: "wire", Status: state.EffectUnknown, DeadlineMS: 1, Payload: body(stateEngineEffect{Cancelled: true})},
		{ID: "expired-event", Kind: "event", Status: state.EffectUnknown, DeadlineMS: 1},
	}
	eligible := stateDispatchableEffects(effects, 200000)
	want := []string{"owner-rearmed", "unsettled", "physical-unknown", "cancelled-authorization"}
	if len(eligible) != len(want) {
		t.Fatalf("recovery candidates lost or terminal work rescheduled: %+v", eligible)
	}
	for index, effect := range eligible {
		if effect.ID != want[index] {
			t.Fatalf("recovery order changed: %+v", eligible)
		}
	}
}

func TestStateSchedulerBoundsAndDeduplicatesOneStuckGroup(t *testing.T) {
	engine, _ := historicalStateTestEngine(t)
	parent, cancel := context.WithCancel(context.Background())
	defer cancel()
	engine.ctx = parent
	engine.cancel = cancel
	engine.work = make(chan struct{}, 64)
	engine.background = make(chan struct{}, 48)
	engine.scheduled = make(map[string]bool)
	hot, err := engine.store.GroupForCallID("stuck-scheduler")
	if err != nil {
		t.Fatal(err)
	}

	healthy, err := engine.store.GroupForCallID("healthy-scheduler")
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	seed := state.Input{ID: "seed", Kind: "locate", Fingerprint: "seed", Create: true, DeadlineMS: now.Add(time.Second).UnixMilli(), ReceiptUntilMS: now.Add(time.Minute).UnixMilli()}
	begun, err := engine.store.(*state.Store).Begin(parent, healthy, "seed", seed)
	if err != nil {
		t.Fatal(err)
	}
	// Keep a real pending input so this group exercises Resume; a completed
	// healthy group no longer needs a recovery request just to be inspected.
	healthy = begun.Step.Record.Ref
	hotSeed := seed
	hotSeed.ID = "hot-seed"
	hotSeed.Fingerprint = "hot-seed"
	hotBegin, err := engine.store.(*state.Store).Begin(parent, hot, "hot-seed", hotSeed)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = engine.store.(*state.Store).Commit(parent, *hotBegin.Step, state.Payload{Timers: []state.Timer{{ID: "due", Generation: "1", DueMS: now.Add(-time.Second).UnixMilli(), DeadlineMS: now.Add(-time.Second).UnixMilli()}}}, nil); err != nil {
		t.Fatal(err)
	}
	hot = hotBegin.Step.Record.Ref
	fake := &blockedScheduleStore{hot: hot.ID, started: make(chan struct{}, 1), healthy: make(chan struct{}, 1)}
	engine.runtime, err = executionfixture.New(fake, execution.ReducerFunc(engine.reduce), "scheduler-test", 64)
	if err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	engine.scheduleGroup(hot, true)
	select {
	case <-fake.started:
	case <-time.After(time.Second):
		t.Fatal("stuck work never started")
	}
	for n := 0; n < 100; n++ {
		engine.scheduleGroup(hot, n%2 == 0)
	}
	engine.scheduleGroup(healthy, true)
	select {
	case <-fake.healthy:
	case <-time.After(time.Second):
		t.Fatal("one stuck group starved healthy group")
	}
	if fake.calls.Load() != 1 {
		t.Fatal("duplicate hint classes allocated duplicate stuck-group workers")
	}
	for time.Since(started) < 3*time.Second {
		engine.lifecycleMu.Lock()
		active := engine.scheduled[hot.ID+":"+hot.Incarnation]
		engine.lifecycleMu.Unlock()
		if !active {
			engine.Close()
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	engine.Close()
	t.Fatal("due-group processing ignored its bounded execution context")
}
