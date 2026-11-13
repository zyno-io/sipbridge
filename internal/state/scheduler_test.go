package state

import (
	"context"
	"errors"
	"testing"
	"time"
)

func schedulerTimer(t *testing.T, s Backend, name string, due int64) Ref {
	t.Helper()
	ref := contractRef(t, s, name)
	update := contractRead(t, s, ref, contractInput(name, true))
	_, err := s.CompareAndSwap(context.Background(), update, Payload{Timers: []Timer{{ID: "timer", Generation: "1", DueMS: due, DeadlineMS: due}}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	return update.Record.Ref
}

func schedulerHead(t *testing.T, s Backend, token string, rebuild bool) ScheduleHead {
	t.Helper()
	ctx := context.Background()
	if _, err := s.LeaseScheduler(ctx, token, false); err != nil {
		t.Fatal(err)
	}
	head, err := s.ScheduleHead(ctx, token, rebuild, ScheduleProtocol)
	if err != nil {
		t.Fatal(err)
	}
	return head
}

func TestSchedulerExclusiveLeaseExpiryAndStaleRelease(t *testing.T) {
	backendContract(t, func(t *testing.T, s Backend) {
		ctx := context.Background()
		until, err := s.LeaseScheduler(ctx, "first", true)
		if err != nil || !until.Held || !until.Until.After(time.Now()) {
			t.Fatal("first scheduler not acquired", err)
		}
		if observed, err := s.LeaseScheduler(ctx, "second", true); err != nil || observed.Held || !observed.Until.After(time.Now()) {
			t.Fatal("two scheduler leaders admitted", err)
		}
		if _, err := s.ScheduleHead(ctx, "second", true, ScheduleProtocol); !errors.Is(err, ErrStale) {
			t.Fatal("follower rebuilt or read queue heads", err)
		}
		time.Sleep(time.Until(until.Until) + 25*time.Millisecond)
		if _, err := s.LeaseScheduler(ctx, "first", false); !errors.Is(err, ErrStale) {
			t.Fatal("expired leadership revived by renewal", err)
		}
		if _, err := s.LeaseScheduler(ctx, "second", true); err != nil {
			t.Fatal("replacement scheduler unavailable", err)
		}
		if err := s.ReleaseScheduler(ctx, "first"); err != nil {
			t.Fatal(err)
		}
		if _, err := s.LeaseScheduler(ctx, "second", false); err != nil {
			t.Fatal("old release deleted successor", err)
		}
		if _, err := s.SchedulerHints(ctx, "first", 0, 1, 8); !errors.Is(err, ErrStale) {
			t.Fatal("old leader enumerated work", err)
		}
		if _, _, err := s.RepairScheduler(ctx, "first", 0, 128); !errors.Is(err, ErrStale) {
			t.Fatal("old leader repaired indexes", err)
		}
	})
}

func TestSchedulerEarlierInsertionReadWaitRaceAndCoalescing(t *testing.T) {
	backendContract(t, func(t *testing.T, s Backend) {
		ctx := context.Background()
		if _, err := s.LeaseScheduler(ctx, "leader", true); err != nil {
			t.Fatal(err)
		}
		empty := schedulerHead(t, s, "leader", true)
		if empty.DueMS != 0 || empty.Cursor != "0-0" {
			t.Fatal("empty namespace invented work", empty)
		}
		now := time.Now().UnixMilli()
		first := schedulerTimer(t, s, "first-timer", now+120000)
		head := schedulerHead(t, s, "leader", false)
		if head.DueMS != now+120000 || head.After < time.Minute {
			t.Fatal("future head missing or scheduled immediately", head)
		}
		// Advance the queue after the head read but before starting XREAD.
		schedulerTimer(t, s, "earlier-timer", now+60000)
		schedulerTimer(t, s, "earliest-timer", now+30000)
		wait, cancel := context.WithTimeout(ctx, time.Second)
		next, err := s.WaitSchedule(wait, head.Cursor)
		cancel()
		if err != nil || next == head.Cursor {
			t.Fatal("read/wait race lost the earlier deadline", next, err)
		}
		latest := schedulerHead(t, s, "leader", false)
		if latest.DueMS != now+30000 {
			t.Fatal("coalescing lost the current earliest head", latest)
		}
		if redisStore, ok := s.(*Store); ok {
			if length := redisStore.client.XLen(ctx, redisStore.schedulerKeys()[1]).Val(); length != 1 {
				t.Fatal("wake stream retained event history", length)
			}
			if count := redisStore.client.ZCard(ctx, redisStore.schedulerKeys()[0]).Val(); count > int64(2*s.PartitionCount()) {
				t.Fatal("head projection grew per protocol group", count)
			}
		}
		// Wakeups and lease maintenance must not produce protocol revisions.
		record, err := s.Load(ctx, first)
		if err != nil || record.Revision != "1" || len(record.Payload.Timers) != 1 {
			t.Fatal("scheduler metadata changed protocol state", record.Revision, err)
		}
		batch, err := s.SchedulerHints(ctx, "leader", first.Partition, 1, 8)
		if err != nil || len(batch[0].Due) != 0 {
			t.Fatal("future deadline dispatched early", batch, err)
		}
	})
}

func TestSchedulerWakeDoesNotConsumeSuccessorNotification(t *testing.T) {
	backendContract(t, func(t *testing.T, s Backend) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if _, err := s.LeaseScheduler(ctx, "leader", true); err != nil {
			t.Fatal(err)
		}
		head := schedulerHead(t, s, "leader", true)
		type result struct {
			cursor string
			err    error
		}
		results := make(chan result, 2)
		for range 2 {
			go func() {
				cursor, err := s.WaitSchedule(ctx, head.Cursor)
				results <- result{cursor, err}
			}()
		}
		schedulerTimer(t, s, "wake-both", time.Now().Add(time.Minute).UnixMilli())
		var first string
		for range 2 {
			select {
			case result := <-results:
				if result.err != nil || result.cursor == head.Cursor || first != "" && first != result.cursor {
					t.Fatal("one watcher stole another's wakeup", result)
				}
				first = result.cursor
			case <-ctx.Done():
				t.Fatal("a watcher lost the queue notification")
			}
		}
	})
}

func TestSchedulerPostponementAndRemovalKeepAuthoritativeTimers(t *testing.T) {
	backendContract(t, func(t *testing.T, s Backend) {
		ctx := context.Background()
		if _, err := s.LeaseScheduler(ctx, "leader", true); err != nil {
			t.Fatal(err)
		}
		now := time.Now().UnixMilli()
		ref := schedulerTimer(t, s, "postponed", now+10000)
		before := schedulerHead(t, s, "leader", true)
		update := contractRead(t, s, ref, contractInput("postpone", false))
		payload := update.Record.Payload
		payload.Timers[0].DueMS, payload.Timers[0].DeadlineMS = now+60000, now+60000
		if _, err := s.CompareAndSwap(ctx, update, payload, nil); err != nil {
			t.Fatal(err)
		}
		after := schedulerHead(t, s, "leader", false)
		if after.DueMS != now+60000 || after.Cursor != before.Cursor {
			t.Fatal("postponement required a new wake or retained old head", before, after)
		}
		update = contractRead(t, s, ref, contractInput("cancel", false))
		payload = update.Record.Payload
		payload.Timers = nil
		if _, err := s.CompareAndSwap(ctx, update, payload, nil); err != nil {
			t.Fatal(err)
		}
		if head := schedulerHead(t, s, "leader", false); head.DueMS != 0 {
			t.Fatal("removed timer remained in queue", head)
		}
		record, err := s.Load(ctx, ref)
		if err != nil || record.Revision != "3" {
			t.Fatal("head removal consumed a protocol record", err)
		}
	})
}

func TestSchedulerDelayCannotOverflow(t *testing.T) {
	if delay := scheduleDelay(1, 1<<53-1); delay <= 0 {
		t.Fatal("large valid deadline overflowed into immediate scheduling", delay)
	}
}

func TestSchedulerBlockingWakeDoesNotOccupyExecutionPool(t *testing.T) {
	s := testStore(t, func(cfg *Config) { cfg.Redis.PoolSize = 1 })
	ctx := context.Background()
	if _, err := s.LeaseScheduler(ctx, "leader", true); err != nil {
		t.Fatal(err)
	}
	head := schedulerHead(t, s, "leader", true)
	wait, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := s.WaitSchedule(wait, head.Cursor)
		done <- err
	}()
	deadline := time.Now().Add(200 * time.Millisecond)
	for s.notifications.PoolStats().TotalConns == 0 {
		if time.Now().After(deadline) {
			t.Fatal("notification connection did not start")
		}
		time.Sleep(time.Millisecond)
	}
	select {
	case err := <-done:
		t.Fatal("empty queue unexpectedly woke", err)
	default:
	}
	renew, stop := context.WithTimeout(ctx, 100*time.Millisecond)
	_, err := s.LeaseScheduler(renew, "leader", false)
	stop()
	if err != nil {
		t.Fatal("blocking notification monopolized the execution connection", err)
	}
	schedulerTimer(t, s, "wake-dedicated-connection", time.Now().Add(time.Minute).UnixMilli())
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
