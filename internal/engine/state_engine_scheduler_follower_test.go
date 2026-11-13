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

type followerWatchStore struct {
	state.Backend
	observations   atomic.Int64
	active         atomic.Int64
	mu             sync.Mutex
	contexts       []<-chan struct{}
	cursors        []string
	shortLease     time.Duration
	join           chan struct{}
	joinEntered    chan struct{}
	watchReady     chan struct{}
	watchReadyOnce sync.Once
	fail           error
	synthetic      chan string
}

func (s *followerWatchStore) LeaseScheduler(ctx context.Context, token string, acquire bool) (state.SchedulerLease, error) {
	lease, err := s.Backend.LeaseScheduler(ctx, token, acquire)
	if err == nil && acquire {
		s.observations.Add(1)
		if !lease.Held {
			lease.Until = time.Now().Add(15 * time.Millisecond)
			lease.Cursor = "0-0"
		}
		if lease.Held && s.shortLease > 0 {
			lease.Until = time.Now().Add(s.shortLease)
			select {
			case <-s.watchReady:
			case <-ctx.Done():
				return state.SchedulerLease{}, ctx.Err()
			}
		}
	}
	return lease, err
}
func (s *followerWatchStore) WaitSchedulerLease(ctx context.Context, cursor string) (string, error) {
	s.active.Add(1)
	defer s.active.Add(-1)
	s.mu.Lock()
	s.contexts = append(s.contexts, ctx.Done())
	s.cursors = append(s.cursors, cursor)
	secondRead := len(s.cursors) > 1
	s.mu.Unlock()
	if secondRead && s.watchReady != nil {
		s.watchReadyOnce.Do(func() { close(s.watchReady) })
	}
	if s.fail != nil {
		return "", s.fail
	}
	if s.synthetic != nil {
		select {
		case next := <-s.synthetic:
			return next, nil
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}
	next, err := s.Backend.WaitSchedulerLease(ctx, cursor)
	if err != nil && ctx.Err() != nil && s.join != nil {
		close(s.joinEntered)
		<-s.join
	}
	return next, err
}

func TestFollowerKeepsWatchAcrossLeaseObservationsAndHandoff(t *testing.T) {
	s, _ := schedulerTestStore(t, state.MemoryBackend)
	held, err := s.LeaseScheduler(context.Background(), "owner", true)
	if err != nil || !held.Held {
		t.Fatal(held, err)
	}
	initial, err := s.LeaseScheduler(context.Background(), "follower", true)
	if err != nil || initial.Held {
		t.Fatal(initial, err)
	}
	initial.Until = time.Now().Add(15 * time.Millisecond)
	spy := &followerWatchStore{Backend: s}
	ctx, cancel := context.WithCancel(context.Background())
	e := &StateEngine{ctx: ctx, store: spy}
	type answer struct {
		lease state.SchedulerLease
		err   error
	}
	result := make(chan answer, 1)
	done := make(chan struct{})
	defer func() { cancel(); <-done }()
	go func() {
		defer close(done)
		lease, err := e.waitSchedulerLease("follower", initial)
		result <- answer{lease, err}
	}()
	schedulerAwait(t, time.Now().Add(time.Second), func() bool { return spy.observations.Load() >= 4 })
	if spy.active.Load() != 1 {
		t.Fatal("follower did not retain exactly one active watch")
	}
	started := time.Now()
	if err := s.ReleaseScheduler(context.Background(), "owner"); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-result:
		if got.err != nil || !got.lease.Held {
			t.Fatal(got)
		}
		if time.Since(started) > 300*time.Millisecond {
			t.Fatal("handoff did not wake promptly")
		}
	case <-time.After(time.Second):
		t.Fatal("handoff blocked")
	}
	if spy.active.Load() != 0 {
		t.Fatal("watch was not joined before leadership")
	}
	spy.mu.Lock()
	defer spy.mu.Unlock()
	if len(spy.contexts) < 1 {
		t.Fatal("no follower watch")
	}
	for _, observed := range spy.contexts {
		if observed != spy.contexts[0] {
			t.Fatal("busy observation replaced watch context")
		}
	}
}

func TestFollowerRejectsLeaseExpiredWhileJoiningWatch(t *testing.T) {
	s, _ := schedulerTestStore(t, state.MemoryBackend)
	held, err := s.LeaseScheduler(context.Background(), "owner", true)
	if err != nil || !held.Held {
		t.Fatal(held, err)
	}
	initial, err := s.LeaseScheduler(context.Background(), "follower", true)
	if err != nil || initial.Held {
		t.Fatal(initial, err)
	}
	initial.Until = time.Now().Add(time.Second)
	spy := &followerWatchStore{Backend: s, shortLease: 20 * time.Millisecond, join: make(chan struct{}), joinEntered: make(chan struct{}), watchReady: make(chan struct{})}
	releaseJoin := sync.OnceFunc(func() { close(spy.join) })
	defer releaseJoin()
	ctx, cancel := context.WithCancel(context.Background())
	e := &StateEngine{ctx: ctx, store: spy}
	result := make(chan error, 1)
	done := make(chan struct{})
	defer func() { cancel(); releaseJoin(); <-done }()
	go func() { defer close(done); _, err := e.waitSchedulerLease("follower", initial); result <- err }()
	schedulerAwait(t, time.Now().Add(time.Second), func() bool { return spy.active.Load() == 1 })
	if err := s.ReleaseScheduler(context.Background(), "owner"); err != nil {
		t.Fatal(err)
	}
	select {
	case <-spy.joinEntered:
	case <-time.After(time.Second):
		t.Fatal("watch cancellation did not enter join")
	}
	time.Sleep(40 * time.Millisecond)
	releaseJoin()
	select {
	case err := <-result:
		if !errors.Is(err, state.ErrStale) {
			t.Fatal("expired acquired lease accepted", err)
		}
	case <-time.After(time.Second):
		t.Fatal("join did not finish")
	}
	if spy.active.Load() != 0 {
		t.Fatal("watch remained active")
	}
}

func TestFollowerWatchFailureDoesNotAcquire(t *testing.T) {
	s, _ := schedulerTestStore(t, state.MemoryBackend)
	expected := errors.New("watch failed")
	spy := &followerWatchStore{Backend: s, fail: expected}
	e := &StateEngine{ctx: context.Background(), store: spy}
	_, err := e.waitSchedulerLease("follower", state.SchedulerLease{Until: time.Now().Add(time.Second), Cursor: "0-0"})
	if !errors.Is(err, expected) || spy.observations.Load() != 0 {
		t.Fatal("watch failure did not end follower", err)
	}
}

func TestFollowerPreservesAdvancedCursorAfterBusy(t *testing.T) {
	s, _ := schedulerTestStore(t, state.MemoryBackend)
	held, err := s.LeaseScheduler(context.Background(), "owner", true)
	if err != nil || !held.Held {
		t.Fatal(held, err)
	}
	initial, err := s.LeaseScheduler(context.Background(), "follower", true)
	if err != nil || initial.Held {
		t.Fatal(initial, err)
	}
	initial.Until = time.Now().Add(time.Second)
	spy := &followerWatchStore{Backend: s, synthetic: make(chan string, 2)}
	ctx, cancel := context.WithCancel(context.Background())
	e := &StateEngine{ctx: ctx, store: spy}
	result := make(chan error, 1)
	done := make(chan struct{})
	defer func() { cancel(); <-done }()
	go func() { defer close(done); _, err := e.waitSchedulerLease("follower", initial); result <- err }()
	schedulerAwait(t, time.Now().Add(time.Second), func() bool { return spy.active.Load() == 1 })
	spy.synthetic <- "9-0"
	schedulerAwait(t, time.Now().Add(time.Second), func() bool {
		spy.mu.Lock()
		defer spy.mu.Unlock()
		return spy.observations.Load() > 0 && len(spy.cursors) > 1 && spy.cursors[len(spy.cursors)-1] == "9-0"
	})
	if err := s.ReleaseScheduler(context.Background(), "owner"); err != nil {
		t.Fatal(err)
	}
	spy.synthetic <- "10-0"
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("handoff blocked")
	}
	spy.mu.Lock()
	defer spy.mu.Unlock()
	for i := 1; i < len(spy.contexts); i++ {
		if spy.contexts[i] != spy.contexts[0] {
			t.Fatal("watch context replaced")
		}
	}
	for i, cursor := range spy.cursors {
		if i > 0 && cursor == "0-0" {
			t.Fatal("busy observation rewound watch cursor")
		}
	}
}
