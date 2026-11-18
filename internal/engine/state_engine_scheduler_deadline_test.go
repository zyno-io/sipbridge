package engine

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"sipbridge/internal/state"
)

type delayedSchedulerRenewal struct {
	state.Backend
	started chan struct{}
	release chan struct{}
}

func (s *delayedSchedulerRenewal) LeaseScheduler(ctx context.Context, token string, acquire bool) (state.SchedulerLease, error) {
	lease, err := s.Backend.LeaseScheduler(ctx, token, acquire)
	if !acquire && err == nil {
		select {
		case s.started <- struct{}{}:
		default:
		}
		// Model a committed renewal whose response is delivered after local expiry.
		<-s.release
	}
	return lease, err
}

func TestSchedulerLateRenewalCannotReviveLocalLeadership(t *testing.T) {
	backend, _ := schedulerTestStore(t, state.MemoryBackend)
	lease, err := backend.LeaseScheduler(context.Background(), "original-leader", true)
	if err != nil || !lease.Held {
		t.Fatal(lease, err)
	}
	store := &delayedSchedulerRenewal{Backend: backend, started: make(chan struct{}, 1), release: make(chan struct{})}
	ctx, cancel := context.WithCancel(context.Background())
	e := &StateEngine{ctx: ctx, cancel: cancel, store: store}
	done := make(chan struct{})
	var release sync.Once
	t.Cleanup(func() {
		release.Do(func() { close(store.release) })
		cancel()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Error("leader did not stop")
		}
	})
	go func() { defer close(done); e.scheduleLeader("original-leader", lease.Until) }()
	select {
	case <-store.started:
	case <-time.After(state.SchedulerLeaseLifetime):
		t.Fatal("leader did not renew")
	}
	timer := time.NewTimer(max(0, time.Until(lease.Until)+25*time.Millisecond))
	defer timer.Stop()
	<-timer.C
	release.Do(func() { close(store.release) })
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("late response revived expired local leadership")
	}
	if e.SchedulerStatistics().Leader {
		t.Fatal("expired leader remained eligible")
	}
}

type failedLeaseWatch struct {
	state.Backend
	acquires atomic.Uint64
	watches  atomic.Uint64
}

func (s *failedLeaseWatch) LeaseScheduler(ctx context.Context, token string, acquire bool) (state.SchedulerLease, error) {
	if acquire {
		s.acquires.Add(1)
	}
	return s.Backend.LeaseScheduler(ctx, token, acquire)
}

func (s *failedLeaseWatch) WaitSchedulerLease(context.Context, string) (string, error) {
	s.watches.Add(1)
	return "", errors.New("lease watch unavailable")
}

func TestSchedulerFailedHandoffWatchBacksOff(t *testing.T) {
	backend, _ := schedulerTestStore(t, state.MemoryBackend)
	lease, err := backend.LeaseScheduler(context.Background(), "other-leader", true)
	if err != nil || !lease.Held {
		t.Fatal(lease, err)
	}
	store := &failedLeaseWatch{Backend: backend}
	ctx, cancel := context.WithTimeout(context.Background(), 750*time.Millisecond)
	defer cancel()
	e := &StateEngine{ctx: ctx, cancel: cancel, store: store}
	e.schedule()
	if store.acquires.Load() == 0 || store.watches.Load() == 0 {
		t.Fatal("fixture did not exercise contention and watch failure")
	}
	if store.acquires.Load() > 3 || store.watches.Load() > 3 {
		t.Fatal("failed watch caused a busy acquisition loop", store.acquires.Load(), store.watches.Load())
	}
}
