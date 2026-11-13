package engine

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/execution"
	"sipbridge/internal/state"
)

type observedSchedulerStore struct {
	state.Backend
	loads         atomic.Uint64
	repairPasses  atomic.Uint64
	dropRelease   atomic.Bool
	lastError     atomic.Value
	acquires      atomic.Uint64
	renewals      atomic.Uint64
	registrations atomic.Uint64
	registered    chan struct{}
	registerOnce  sync.Once
	takeovers     atomic.Uint64
}

type slowSchedulerRepairStore struct {
	state.Backend
	started chan struct{}
}

func (s *slowSchedulerRepairStore) RepairScheduler(ctx context.Context, token string, cursor uint64, count int) (uint64, int, error) {
	select {
	case s.started <- struct{}{}:
	default:
	}
	timer := time.NewTimer(600 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return cursor, 0, ctx.Err()
	case <-timer.C:
	}
	return s.Backend.RepairScheduler(ctx, token, cursor, count)
}

func (s *observedSchedulerStore) LeaseScheduler(ctx context.Context, token string, acquire bool) (state.SchedulerLease, error) {
	if acquire {
		s.acquires.Add(1)
	} else {
		s.renewals.Add(1)
	}
	until, err := s.Backend.LeaseScheduler(ctx, token, acquire)
	if err != nil && !errors.Is(err, state.ErrBusy) {
		s.lastError.Store("lease: " + err.Error())
	}
	return until, err
}

func (s *observedSchedulerStore) RegisterDispatchWorker(ctx context.Context, worker string) error {
	s.registrations.Add(1)
	err := s.Backend.RegisterDispatchWorker(ctx, worker)
	if err == nil {
		s.registerOnce.Do(func() { close(s.registered) })
	}
	return err
}

func (s *observedSchedulerStore) TakeoverTimers(ctx context.Context, token, worker string, limit int) error {
	s.takeovers.Add(1)
	return s.Backend.TakeoverTimers(ctx, token, worker, limit)
}

func (s *observedSchedulerStore) WaitSchedule(ctx context.Context, cursor string) (string, error) {
	next, err := s.Backend.WaitSchedule(ctx, cursor)
	if err != nil && ctx.Err() == nil {
		s.lastError.Store("wake: " + err.Error())
	}
	return next, err
}

func (s *observedSchedulerStore) RepairScheduler(ctx context.Context, token string, cursor uint64, count int) (uint64, int, error) {
	next, processed, err := s.Backend.RepairScheduler(ctx, token, cursor, count)
	if err != nil {
		s.lastError.Store("repair: " + err.Error())
	} else if next == 0 {
		s.repairPasses.Add(1)
	}
	return next, processed, err
}

func (s *observedSchedulerStore) Load(ctx context.Context, ref state.Ref) (state.Record, error) {
	s.loads.Add(1)
	return s.Backend.Load(ctx, ref)
}

func (s *observedSchedulerStore) LoadAt(ctx context.Context, ref state.Ref) (state.Record, int64, error) {
	s.loads.Add(1)
	return s.Backend.LoadAt(ctx, ref)
}

func (s *observedSchedulerStore) ReadState(ctx context.Context, ref state.Ref) (state.StateView, int64, error) {
	s.loads.Add(1)
	return s.Backend.ReadState(ctx, ref)
}

func (s *observedSchedulerStore) ReleaseScheduler(ctx context.Context, token string) error {
	if s.dropRelease.Load() {
		return nil // Simulate a killed process that cannot release its lease.
	}
	return s.Backend.ReleaseScheduler(ctx, token)
}

func schedulerTestStore(t *testing.T, backend string) (state.Backend, state.Backend) {
	t.Helper()
	f := newNativeOwnerFixtureBackend(t, backend)
	cfg := f.config
	cfg.Backend, cfg.Namespace, cfg.Partitions = backend, "scheduler-"+uuid.NewString(), 64
	// These pure timer groups need little payload space. Admit the many-group
	// maintenance fixture without reserving native 64-MiB capacity per group.
	cfg.Limits.GroupBytes, cfg.Memory.MaxGroups = 1<<20, 256
	cfg.Limits.InputBytes, cfg.Limits.OutcomeBytes = 16<<10, 16<<10
	s, err := state.OpenBackend(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Ready(context.Background()); err != nil {
		t.Fatal(err)
	}
	peer := s
	if backend == state.RedisBackend {
		peer, err = state.OpenBackend(cfg)
		if err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() {
		s.Close()
		if peer != s {
			peer.Close()
		}
		if backend == state.RedisBackend {
			var cursor uint64
			for {
				keys, next, err := f.client.Scan(context.Background(), cursor, cfg.Prefix+":"+cfg.Namespace+":*", 128).Result()
				if err != nil {
					t.Error(err)
					return
				}
				if len(keys) > 0 {
					if err := f.client.Del(context.Background(), keys...).Err(); err != nil {
						t.Error(err)
					}
				}
				if next == 0 {
					return
				}
				cursor = next
			}
		}
	})
	return s, peer
}

// This small pure reducer isolates scheduler timing/admission from SIP semantics;
// the native/wire suites exercise the unchanged SIP reducer and physical guards.
func startSchedulerTestEngine(t *testing.T, backend state.Backend) (*StateEngine, *observedSchedulerStore) {
	t.Helper()
	s := &observedSchedulerStore{Backend: backend, registered: make(chan struct{})}
	ctx, cancel := context.WithCancel(context.Background())
	e := &StateEngine{ctx: ctx, cancel: cancel, store: s, cfg: Config{StateIdentityKey: []byte("scheduler-fixture-identity-key-32bytes"), Process: &pb.ProcessRef{InstanceId: uuid.NewString(), Epoch: "fixture"}},
		work: make(chan struct{}, 64), background: make(chan struct{}, 48), scheduled: make(map[string]bool), dispatching: make(map[string]bool)}
	var err error
	e.runtime, err = execution.NewCAS(s, execution.ReducerFunc(func(record state.Record, input state.Input, now int64) (state.Payload, []byte, error) {
		var event stateEngineInput
		if unmarshalRetainedFixture(input.Payload, &event) != nil || event.Kind != "timer" {
			return state.Payload{}, nil, errors.New("unexpected scheduler fixture input")
		}
		payload := record.Payload
		payload.Timers = nil
		for _, timer := range record.Payload.Timers {
			if timer.ID != event.Timer || timer.Generation != event.Generation || timer.DueMS > now {
				payload.Timers = append(payload.Timers, timer)
			}
		}
		outcome, err := json.Marshal(&pb.OperationResult{Certainty: pb.OperationResult_COMPLETED})
		return payload, outcome, err
	}), 8)
	if err != nil {
		t.Fatal(err)
	}
	e.wg.Add(1)
	go func() { defer e.wg.Done(); e.schedule() }()
	t.Cleanup(e.Close)
	select {
	case <-s.registered:
	case <-time.After(5 * time.Second):
		t.Fatal("scheduler worker failed to register")
	}
	return e, s
}

func schedulerSeedTimer(t *testing.T, s state.Backend, due time.Time) state.Ref {
	t.Helper()
	var ref state.Ref
	// Exercise a full 64-partition sweep, including the last partition.
	for {
		candidate, err := s.GroupForCallID(uuid.NewString())
		if err != nil {
			t.Fatal(err)
		}
		if candidate.Partition == 63 {
			ref = candidate
			break
		}
	}
	id := uuid.NewString()
	in := state.Input{ID: id, Kind: "seed", Fingerprint: id, Create: true, DeadlineMS: time.Now().Add(time.Second).UnixMilli(), ReceiptUntilMS: time.Now().Add(2 * time.Second).UnixMilli()}
	read, err := s.ReadUpdate(context.Background(), ref, in)
	if err != nil || read.Update == nil {
		t.Fatal(err)
	}
	// Real native reducers also derive deadlines from the validated store clock;
	// the allowed host/Redis clock offset must not become fictitious timer lag.
	timerDue := read.Update.NowMS + max(1, time.Until(due).Milliseconds())
	_, err = s.CompareAndSwap(context.Background(), *read.Update,
		state.Payload{Timers: []state.Timer{{ID: "test-timer", Generation: "1", DueMS: timerDue, DeadlineMS: timerDue}}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	return read.Update.Record.Ref
}

func schedulerAwait(t *testing.T, deadline time.Time, predicate func() bool) {
	t.Helper()
	for !predicate() {
		if time.Now().After(deadline) {
			t.Fatal("scheduler condition did not complete within its budget")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestSchedulerPairIdleWaitsWithoutQueuePollingOrRecordLoads(t *testing.T) {
	for _, backend := range []string{state.MemoryBackend, state.RedisBackend} {
		t.Run(backend, func(t *testing.T) {
			s, peer := schedulerTestStore(t, backend)
			ref := schedulerSeedTimer(t, s, time.Now().Add(time.Minute))
			a, observedA := startSchedulerTestEngine(t, s)
			b, observedB := startSchedulerTestEngine(t, peer)
			t.Cleanup(func() {
				if t.Failed() {
					t.Log("scheduler diagnostics", a.SchedulerStatistics(), b.SchedulerStatistics(), observedA.lastError.Load(), observedB.lastError.Load())
				}
			})
			schedulerAwait(t, time.Now().Add(time.Second), func() bool { return a.SchedulerStatistics().Leader || b.SchedulerStatistics().Leader })
			schedulerAwait(t, time.Now().Add(5*time.Second), func() bool { return observedA.repairPasses.Load()+observedB.repairPasses.Load() > 0 })
			beforeAcquire := observedA.acquires.Load() + observedB.acquires.Load()
			beforeRenew := observedA.renewals.Load() + observedB.renewals.Load()
			beforeRegister := observedA.registrations.Load() + observedB.registrations.Load()
			observedAt := time.Now()
			time.Sleep(2200 * time.Millisecond) // Completed startup repair must stay completed across many lease renewals.
			elapsed := time.Since(observedAt)
			ceil := func(period time.Duration) uint64 { return uint64((elapsed + period - 1) / period) }
			if observedA.takeovers.Load()+observedB.takeovers.Load() != 0 {
				t.Fatal("idle pair checked orphan takeover without a timed owner")
			}
			if observedA.acquires.Load()+observedB.acquires.Load()-beforeAcquire > ceil(state.SchedulerLeaseLifetime)+1 {
				t.Fatal("follower polled coordinator acquisition before observed expiry")
			}
			if observedA.renewals.Load()+observedB.renewals.Load()-beforeRenew > ceil(state.SchedulerLeaseLifetime/2) {
				t.Fatal("leader renewed more often than the documented cadence")
			}
			if observedA.registrations.Load()+observedB.registrations.Load()-beforeRegister > 2*ceil(state.DispatchWorkerLifetime/2) {
				t.Fatal("worker presence renewed more often than the documented cadence")
			}
			left, right := a.SchedulerStatistics(), b.SchedulerStatistics()
			if left.Leader == right.Leader || left.Acquisitions+right.Acquisitions != 1 {
				t.Fatal("idle pair churned or duplicated scheduler leadership", left, right, observedA.lastError.Load(), observedB.lastError.Load())
			}
			if left.HeadReads+right.HeadReads != 1 || left.HintBatches+right.HintBatches != 0 || observedA.loads.Load()+observedB.loads.Load() != 0 {
				t.Fatal("idle scheduler polled queues or loaded protocol records", left, right, observedA.loads.Load(), observedB.loads.Load())
			}
			// Redis SCAN may need multiple pages even with one matching group:
			// unrelated namespaces share the physical keyspace. Count completed
			// repair passes, not an assumed one-page database.
			if observedA.repairPasses.Load()+observedB.repairPasses.Load() != 1 {
				t.Fatal("idle scheduler continuously rescanned the namespace", left, right)
			}
			record, err := s.Load(context.Background(), ref)
			if err != nil || record.Revision != "1" {
				t.Fatal("idle scheduling generated a protocol revision", err)
			}
			due := time.Now().Add(100 * time.Millisecond)
			earlier := schedulerSeedTimer(t, s, due)
			schedulerAwait(t, due.Add(450*time.Millisecond), func() bool {
				record, err := s.Load(context.Background(), earlier)
				return err == nil && len(record.Payload.Timers) == 0
			})
			t.Logf("new earlier timer dispatched with %s lag across 64 partitions", time.Since(due))
		})
	}
}

func TestSchedulerSlowRepairPreservesLeadershipAndTimerAdmission(t *testing.T) {
	for _, backend := range []string{state.MemoryBackend, state.RedisBackend} {
		t.Run(backend, func(t *testing.T) {
			s, peer := schedulerTestStore(t, backend)
			for range 192 {
				schedulerSeedTimer(t, s, time.Now().Add(time.Minute))
			}
			started := make(chan struct{}, 1)
			a, observedA := startSchedulerTestEngine(t, &slowSchedulerRepairStore{Backend: s, started: started})
			b, observedB := startSchedulerTestEngine(t, &slowSchedulerRepairStore{Backend: peer, started: started})
			select {
			case <-started:
			case <-time.After(2 * time.Second):
				t.Fatal("leader never started its maintenance page")
			}
			due := time.Now().Add(50 * time.Millisecond)
			ref := schedulerSeedTimer(t, s, due)
			schedulerAwait(t, due.Add(450*time.Millisecond), func() bool {
				record, err := s.Load(context.Background(), ref)
				return err == nil && len(record.Payload.Timers) == 0
			})
			schedulerAwait(t, time.Now().Add(time.Second), func() bool {
				return a.SchedulerStatistics().RepairPages+b.SchedulerStatistics().RepairPages == 1
			})
			left, right := a.SchedulerStatistics(), b.SchedulerStatistics()
			if left.Acquisitions+right.Acquisitions != 1 || left.Leader == right.Leader {
				t.Fatal("maintenance stalled renewal or restarted leadership", left, right, observedA.lastError.Load(), observedB.lastError.Load())
			}
		})
	}
}

func TestSchedulerPairAbruptLeaderLossRebuildsAndDispatches(t *testing.T) {
	for _, backend := range []string{state.MemoryBackend, state.RedisBackend} {
		t.Run(backend, func(t *testing.T) {
			s, peer := schedulerTestStore(t, backend)
			a, observedA := startSchedulerTestEngine(t, s)
			b, observedB := startSchedulerTestEngine(t, peer)
			t.Cleanup(func() {
				if t.Failed() {
					t.Log("scheduler diagnostics", a.SchedulerStatistics(), b.SchedulerStatistics(), observedA.lastError.Load(), observedB.lastError.Load())
				}
			})
			schedulerAwait(t, time.Now().Add(time.Second), func() bool { return a.SchedulerStatistics().Leader || b.SchedulerStatistics().Leader })
			leader, follower, observed := a, b, observedA
			if b.SchedulerStatistics().Leader {
				leader, follower, observed = b, a, observedB
			}
			observed.dropRelease.Store(true)
			due := time.Now().Add(100 * time.Millisecond)
			ref := schedulerSeedTimer(t, s, due)
			lost := time.Now()
			leader.cancel()
			// The lease-loss budget starts at cancellation, after seed admission.
			schedulerAwait(t, lost.Add(state.SchedulerLeaseLifetime+750*time.Millisecond), func() bool {
				record, err := s.Load(context.Background(), ref)
				return follower.SchedulerStatistics().Leader && err == nil && len(record.Payload.Timers) == 0
			})
			if follower.SchedulerStatistics().Acquisitions != 1 {
				t.Fatal("follower did not acquire fresh leadership")
			}
			t.Logf("abrupt failover timer dispatched with %s lag across 64 partitions", time.Since(due))
		})
	}
}
