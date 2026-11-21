package rediscore

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestDriverStaleNativeSelectionKeepsLiveGeneration(t *testing.T) {
	cfg, client := connectionFixture(t)
	connection, err := NewConnection(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	observed := make(chan context.Context, 2)
	finished := make(chan error, 1)
	driver, err := NewDriver(DriverConfig{Connection: connection, Worker: "worker", Epoch: "epoch",
		Due: func(lease context.Context, store *Store, due *DueContext) error {
			observed <- lease
			if due.Revision == 1 {
				// A normal ingress operation wins after the driver selected revision 1.
				_, e := store.Commit(lease, Publication{Identity: due.Identity, Revision: due.Revision,
					Deadline: time.Now().Add(time.Second), Changes: []Change{{Field: "r:context", Value: []byte{1}}}})
				if e != nil {
					return e
				}
				return ErrConflict
			}
			_, e := store.Commit(lease, Publication{Identity: due.Identity, Revision: due.Revision, Deadline: time.Now().Add(time.Second),
				Timers: &TimerMutation{Expected: due.Assignment, Target: due.Assignment, RequireLease: true,
					Changes: []TimerChange{{Member: due.Member, Delete: true}}}})
			finished <- e
			return e
		}, ReadyOutput: func(context.Context, *Store, ReadyOutput) error { return nil },
		OwnerExpired: func(context.Context, *Store, ExpiredApplicationContext) error { return nil }})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- driver.Run(ctx) }()
	defer func() { cancel(); _ = driver.Close(); <-done }()
	for driver.Ready() != nil {
		select {
		case <-time.After(5 * time.Millisecond):
		case <-ctx.Done():
			t.Fatal("driver did not become ready")
		}
	}
	store, _, _, err := driver.Current()
	if err != nil {
		t.Fatal(err)
	}
	id := Identity{Context: "context", Incarnation: "incarnation"}
	t.Cleanup(func() {
		if e := client.Del(context.Background(), store.contextKey(id.Context), store.contextDeadlineKey(id.Context),
			store.workerKey("worker"), store.workerTimerKey("worker"), store.workerReadyKey("worker"), store.ownerExpiryKey(), store.coordinatorKey()).Err(); e != nil {
			t.Error(e)
		}
	})
	_, err = store.Commit(ctx, Publication{Identity: id, Create: true, Deadline: time.Now().Add(time.Second),
		Timers: &TimerMutation{Target: TimerAssignment{Worker: "worker", Epoch: "epoch", Term: "epoch"},
			Changes: []TimerChange{{Member: "duty", Due: time.Now().Add(-time.Second)}}}})
	if err != nil {
		t.Fatal(err)
	}
	var first, second context.Context
	select {
	case first = <-observed:
	case <-ctx.Done():
		t.Fatal("initial native duty was not selected")
	}
	select {
	case second = <-observed:
	case <-time.After(WorkerRenewInterval / 2):
		t.Fatal("winning revision waited for lease renewal")
	case <-ctx.Done():
		t.Fatal("winning revision was not selected")
	}
	if first != second || first.Err() != nil || driver.Ready() != nil {
		t.Fatal("ordinary context conflict fenced the healthy worker generation")
	}
	select {
	case err = <-finished:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("current duty did not finish")
	}
}

func TestDriverYieldsAfterRepeatedTimerRevisionConflicts(t *testing.T) {
	bad := Identity{Context: "bad", Incarnation: "bad-incarnation"}
	good := Identity{Context: "good", Incarnation: "good-incarnation"}
	published := make(chan struct{})
	type completion struct {
		conflicts int32
		err       error
	}
	finished := make(chan completion, 1)
	var conflicts atomic.Int32
	fixture := startIsolationDriver(t, DriverConfig{
		Due: func(ctx context.Context, store *Store, due *DueContext) error {
			if due.Identity == bad {
				select {
				case <-published:
				case <-ctx.Done():
					return ctx.Err()
				}
				_, err := store.Commit(ctx, Publication{Identity: due.Identity, Revision: due.Revision,
					Deadline: time.Now().Add(time.Second), Changes: []Change{{Field: "r:context", Value: []byte{1}}}})
				if err != nil {
					return err
				}
				conflicts.Add(1)
				return ErrConflict
			}
			_, err := store.Commit(ctx, Publication{Identity: due.Identity, Revision: due.Revision, Deadline: time.Now().Add(time.Second),
				Timers: &TimerMutation{Expected: due.Assignment, Target: due.Assignment, RequireLease: true,
					Changes: []TimerChange{{Member: due.Member, Delete: true}}}})
			finished <- completion{conflicts.Load(), err}
			return err
		},
		ReadyOutput:  func(context.Context, *Store, ReadyOutput) error { return nil },
		OwnerExpired: func(context.Context, *Store, ExpiredApplicationContext) error { return nil },
	})
	now := time.Now()
	for i, identity := range []Identity{bad, good} {
		_, err := fixture.store.Commit(context.Background(), Publication{Identity: identity, Create: true, Deadline: now.Add(time.Second),
			Timers: &TimerMutation{Target: TimerAssignment{Worker: "worker", Epoch: "epoch", Term: "epoch"},
				Changes: []TimerChange{{Member: "duty", Due: now.Add(time.Duration(i-2) * time.Second)}}}})
		if err != nil {
			t.Fatal(err)
		}
	}
	close(published)
	select {
	case result := <-finished:
		if result.err != nil {
			t.Fatal(result.err)
		}
		if result.conflicts != 2 {
			t.Fatal("expected the original attempt and one immediate retry", result.conflicts)
		}
	case <-time.After(WorkerRenewInterval / 2):
		t.Fatal("repeated revision conflicts delayed the unrelated due timer")
	}
}
