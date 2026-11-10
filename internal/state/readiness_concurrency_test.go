package state

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

type readinessCountingHook struct {
	times, infos, markers, scripts atomic.Int64
	mode                           atomic.Int64 // 1: exhausted caller; 2: transport failure.
	entered, release               chan struct{}
	once                           sync.Once
}

func (h *readinessCountingHook) DialHook(next redis.DialHook) redis.DialHook { return next }
func (h *readinessCountingHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}
func (h *readinessCountingHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, command redis.Cmder) error {
		switch command.Name() {
		case "time":
			h.times.Add(1)
			if h.entered != nil {
				h.once.Do(func() {
					close(h.entered)
					select {
					case <-h.release:
					case <-ctx.Done():
					}
				})
			}
		case "info":
			h.infos.Add(1)
		case "mget":
			h.markers.Add(1)
		case "evalsha", "eval":
			h.scripts.Add(1)
			switch h.mode.Load() {
			case 1:
				<-ctx.Done()
				return ctx.Err()
			case 2:
				return io.ErrUnexpectedEOF
			}
		}
		return next(ctx, command)
	}
}

func (h *readinessCountingHook) assertValidations(t *testing.T, want int64) {
	t.Helper()
	if got := h.markers.Load(); got != 0 || h.times.Load() != want || h.infos.Load() != 2*want {
		t.Fatalf("full validation amplification: TIME=%d INFO=%d MGET=%d, want %d/%d/%d", h.times.Load(), h.infos.Load(), got, want, 2*want, want)
	}
}

func TestConcurrentShortSchedulerBudgetsDoNotAmplifyReadiness(t *testing.T) {
	s := testStore(t, func(c *Config) { c.Redis.PoolSize = 1 })
	readyStore(t, s)
	ref := group(t, s, "healthy-during-short-scheduler-budgets")
	step := begin(t, s, ref, "initial", true)
	if _, err := s.Commit(context.Background(), *step, Payload{}, nil); err != nil {
		t.Fatal(err)
	}
	hook := &readinessCountingHook{}
	s.client.AddHook(hook)
	hook.mode.Store(1)
	const callers = 32
	var work sync.WaitGroup
	results := make(chan error, callers)
	for n := 0; n < callers; n++ {
		work.Add(1)
		go func() {
			defer work.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()
			_, err := s.ReadyHints(ctx, 0, 8)
			results <- err
		}()
	}
	work.Wait()
	close(results)
	for err := range results {
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatal("fixture did not expire each short caller", err)
		}
	}
	if !s.Available() {
		t.Error("scheduler cancellation poisoned shared operational health")
	}
	hook.mode.Store(0)
	// Exercise every former ready=false preflight, including the direct SCAN
	// repair path, after all callers have exhausted their own budget.
	results = make(chan error, callers)
	for n := 0; n < callers; n++ {
		work.Add(1)
		go func(n int) {
			defer work.Done()
			var err error
			switch n % 5 {
			case 0:
				_, err = s.Load(context.Background(), ref)
			case 1:
				_, err = s.ReadApplicationLease(context.Background(), "absent-owner")
			case 2:
				_, err = s.DueHints(context.Background(), 0, 8)
			case 3:
				_, _, err = s.RepairSchedule(context.Background(), 0, 128)
			case 4:
				_, err = s.Begin(context.Background(), group(t, s, fmt.Sprintf("next-%d", n)), "worker", input(fmt.Sprintf("next-%d", n), true))
			}
			results <- err
		}(n)
	}
	work.Wait()
	close(results)
	for err := range results {
		if err != nil {
			t.Fatal("healthy work failed after short caller timeouts", err)
		}
	}
	hook.assertValidations(t, 0)
	t.Logf("%d short scheduler timeouts followed by %d concurrent operations: TIME=%d INFO=%d MGET=%d", callers, callers, hook.times.Load(), hook.infos.Load(), hook.markers.Load())
	if hook.scripts.Load() < callers {
		t.Fatal("fixture did not exercise concurrent real scripts")
	}
}

func TestTransportFailureRecoversHealthWithoutExtraStartupChecks(t *testing.T) {
	s := testStore(t, func(c *Config) { c.Redis.PoolSize = 1 })
	readyStore(t, s)
	observation := s.transport.Load()
	hook := &readinessCountingHook{}
	s.client.AddHook(hook)
	hook.mode.Store(2)
	if _, err := s.ReadApplicationLease(context.Background(), "absent-owner"); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatal("transport failure was hidden", err)
	}
	if s.Available() {
		t.Fatal("real transport failure remained operationally available")
	}
	if s.validated.Load() != observation {
		t.Fatal("transport failure discarded primary observation")
	}
	hook.mode.Store(0)
	if _, err := s.ReadApplicationLease(context.Background(), "absent-owner"); err != nil {
		t.Fatal(err)
	}
	if !s.Available() {
		t.Fatal("verified atomic script did not restore health")
	}
	hook.assertValidations(t, 0)
}

func TestReadinessCoalescesAndOutlivesItsShortFirstCaller(t *testing.T) {
	s := testStore(t, func(c *Config) { c.Redis.PoolSize = 1 })
	readyStore(t, s)
	// An actual primary change likewise produces a new observation without a
	// matching complete proof; the tagged Sentinel test covers the real switch.
	s.transport.Store(&primaryObservation{runID: s.primaryID()})
	hook := &readinessCountingHook{entered: make(chan struct{}), release: make(chan struct{})}
	s.client.AddHook(hook)
	first := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
		defer cancel()
		_, err := s.ReadApplicationLease(ctx, "owner")
		first <- err
	}()
	select {
	case <-hook.entered:
	case <-time.After(time.Second):
		t.Fatal("validation did not start")
	}
	const callers = 32
	results := make(chan error, callers)
	for n := 0; n < callers; n++ {
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			_, err := s.ReadApplicationLease(ctx, "owner")
			results <- err
		}()
	}
	if err := <-first; !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("short first caller did not retain its deadline", err)
	}
	close(hook.release)
	for n := 0; n < callers; n++ {
		if err := <-results; err != nil {
			t.Fatal("first caller cancelled another caller's authority validation", err)
		}
	}
	hook.assertValidations(t, 1)
	t.Logf("%d concurrent admissions after a cancelled first caller: TIME=%d INFO=%d MGET=%d", callers, hook.times.Load(), hook.infos.Load(), hook.markers.Load())
	if !s.Available() {
		t.Fatal("successful shared validation never restored availability")
	}
}

func TestExecutionWindowMissDoesNotPoisonHealthyReadiness(t *testing.T) {
	s := testStore(t, nil)
	readyStore(t, s)
	hook := &readinessCountingHook{}
	s.client.AddHook(hook)
	if _, err := s.run(context.Background(), stateScript("return fail('CLOCK')"), group(t, s, "late-window"), nil, false); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	if !s.Available() {
		t.Fatal("execution-window miss poisoned healthy clocks/store")
	}
	if _, err := s.ReadApplicationLease(context.Background(), "absent-owner"); err != nil {
		t.Fatal(err)
	}
	hook.assertValidations(t, 0)
}
