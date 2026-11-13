package state

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

func TestSchedulerLeaseContentionAndConditionalWake(t *testing.T) {
	backendContract(t, func(t *testing.T, s Backend) {
		ctx := context.Background()
		owner, err := s.LeaseScheduler(ctx, "owner", true)
		if err != nil || !owner.Held {
			t.Fatal(owner, err)
		}
		follower, err := s.LeaseScheduler(ctx, "follower", true)
		if err != nil || follower.Held || !follower.Until.After(time.Now()) {
			t.Fatal(follower, err)
		}
		if err = s.ReleaseScheduler(ctx, "other"); err != nil {
			t.Fatal(err)
		}
		check, cancel := context.WithTimeout(ctx, 30*time.Millisecond)
		_, err = s.WaitSchedulerLease(check, follower.Cursor)
		cancel()
		if err == nil {
			t.Fatal("wrong-token release woke contender")
		}
		if err = s.ReleaseScheduler(ctx, "owner"); err != nil {
			t.Fatal(err)
		}
		wait, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()
		next, err := s.WaitSchedulerLease(wait, follower.Cursor)
		if err != nil || next == follower.Cursor {
			t.Fatal(next, err)
		}
		replacement, err := s.LeaseScheduler(ctx, "follower", true)
		if err != nil || !replacement.Held {
			t.Fatal(replacement, err)
		}
	})
}

func TestSchedulerRotatesDueClasses(t *testing.T) {
	backendContract(t, func(t *testing.T, s Backend) {
		ctx := context.Background()
		now := time.Now().UnixMilli() - 1000
		schedulerTimer(t, s, "fair-unowned", now)
		switch x := s.(type) {
		case *MemoryStore:
			if _, err := x.lock(ctx); err != nil {
				t.Fatal(err)
			}
			x.setApplicationExpiry("fair", now)
			x.timerWorkers = map[string]int64{strings.Repeat("a", 64): now}
			x.unlock()
		case *Store:
			if err := x.client.ZAdd(ctx, x.root+"application-expiry", redis.Z{Score: float64(now), Member: "fair"}).Err(); err != nil {
				t.Fatal(err)
			}
			if err := x.client.ZAdd(ctx, x.root+"timer:workers", redis.Z{Score: float64(now), Member: strings.Repeat("a", 64)}).Err(); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := s.LeaseScheduler(ctx, "fair", true); err != nil {
			t.Fatal(err)
		}
		for class := ScheduleClass(0); class < ScheduleClassCount; class++ {
			head, err := s.ScheduleHead(ctx, "fair", true, class)
			if err != nil || head.Class != class || head.DueMS > now {
				t.Fatal(class, head, err)
			}
		}
	})
}

func TestOwnerHintsFollowPresenceAndWakeFirstTimer(t *testing.T) {
	backendContract(t, func(t *testing.T, s Backend) {
		ctx := context.Background()
		worker := strings.Repeat("a", 64)
		if err := s.RegisterDispatchWorker(ctx, worker); err != nil {
			t.Fatal(err)
		}
		if _, err := s.LeaseScheduler(ctx, "coord", true); err != nil {
			t.Fatal(err)
		}
		before, err := s.ScheduleHead(ctx, "coord", true, ScheduleProtocol)
		if err != nil || before.DueMS != 0 {
			t.Fatal(before, err)
		}
		ref := contractRef(t, s, "owner-hint")
		update := contractRead(t, s, ref, contractInput("create-owner-hint", true))
		due := update.NowMS + 60000
		payload := Payload{TimerWorker: worker, Timers: []Timer{{ID: "t", Generation: "1", DueMS: due, DeadlineMS: due}}}
		if _, err = s.CompareAndSwap(ctx, update, payload, nil); err != nil {
			t.Fatal(err)
		}
		ref = update.Record.Ref
		wait, cancel := context.WithTimeout(ctx, time.Second)
		_, err = s.WaitSchedule(wait, before.Cursor)
		cancel()
		if err != nil {
			t.Fatal("first owner hint failed to wake coordinator", err)
		}
		head, err := s.ScheduleHead(ctx, "coord", false, ScheduleProtocol)
		if err != nil || head.Class != ScheduleWorkerExpiry || head.DueMS <= update.NowMS {
			t.Fatal(head, err)
		}
		// Mark both the presence and hint expired: subsequent unrelated commits must
		// preserve the evidence instead of granting a new lease.
		switch x := s.(type) {
		case *MemoryStore:
			if _, err = x.lock(ctx); err != nil {
				t.Fatal(err)
			}
			x.dispatchWorkers[worker] = 1
			x.timerWorkers[worker] = 1
			x.unlock()
		case *Store:
			if err = x.client.ZAdd(ctx, x.root+"scheduler:workers", redis.Z{Score: 1, Member: worker}).Err(); err != nil {
				t.Fatal(err)
			}
			if err = x.client.ZAdd(ctx, x.root+"timer:workers", redis.Z{Score: 1, Member: worker}).Err(); err != nil {
				t.Fatal(err)
			}
		}
		update = contractRead(t, s, ref, contractInput("touch-owner-hint", false))
		if _, err = s.CompareAndSwap(ctx, update, payload, nil); err != nil {
			t.Fatal(err)
		}
		head, err = s.ScheduleHead(ctx, "coord", false, ScheduleWorkerExpiry)
		if err != nil || head.Class != ScheduleWorkerExpiry || head.DueMS != 1 {
			t.Fatal("dead worker was renewed", head, err)
		}
		update = contractRead(t, s, ref, contractInput("remove-owner-hint", false))
		if _, err = s.CompareAndSwap(ctx, update, Payload{TimerWorker: worker}, nil); err != nil {
			t.Fatal(err)
		}
		head, err = s.ScheduleHead(ctx, "coord", false, ScheduleWorkerExpiry)
		if err != nil || head.DueMS != 0 {
			t.Fatal("empty queue retained owner hint", head, err)
		}
	})
}

func TestSchedulerLeaseWakePreflightDoesNotMutateLease(t *testing.T) {
	s := testStore(t, nil)
	ctx := context.Background()
	key := s.root + "scheduler:lease:wake"
	if err := s.client.Set(ctx, key, "wrong-type", 0).Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.LeaseScheduler(ctx, "leader", true); err == nil {
		t.Fatal("invalid wake key accepted")
	}
	if n := s.client.Exists(ctx, s.schedulerKeys()[2]).Val(); n != 0 {
		t.Fatal("lease mutated before wake validation")
	}
}

func TestSchedulerHeadCorruptionDoesNotRebuild(t *testing.T) {
	s := testStore(t, nil)
	ctx := context.Background()
	if _, err := s.LeaseScheduler(ctx, "leader", true); err != nil {
		t.Fatal(err)
	}
	heads := s.schedulerKeys()[0]
	if err := s.client.ZAdd(ctx, heads, redis.Z{Score: 1, Member: "sentinel"}).Err(); err != nil {
		t.Fatal(err)
	}
	if err := s.client.Set(ctx, s.root+"timer:workers", "wrong-type", 0).Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ScheduleHead(ctx, "leader", true, ScheduleProtocol); err == nil {
		t.Fatal("corrupt owner index accepted")
	}
	if score, err := s.client.ZScore(ctx, heads, "sentinel").Result(); err != nil || score != 1 {
		t.Fatal("head rebuild happened before validation", score, err)
	}
}

func TestSchedulerLeasePermissionPreflightDoesNotMutate(t *testing.T) {
	for _, denied := range []string{"xread", "xadd", "pexpire"} {
		t.Run(denied, func(t *testing.T) {
			admin := testStore(t, nil)
			ctx := context.Background()
			user := "lease-" + uuid.NewString()
			if err := admin.client.ACLSetUser(ctx, user, "reset", "on", ">fixture-only", "~"+admin.root+"*", "+@all", "-"+denied).Err(); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { admin.client.ACLDelUser(ctx, user) })
			cfg := admin.cfg
			cfg.Redis.Username = user
			cfg.Redis.Password = "fixture-only"
			restricted, err := Open(ctx, cfg)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { restricted.Close() })
			if _, err = restricted.LeaseScheduler(ctx, "leader", true); !errors.Is(err, ErrAuthority) {
				t.Fatal("expected permission refusal", err)
			}
			if n := admin.client.Exists(ctx, admin.schedulerKeys()[2]).Val(); n != 0 {
				t.Fatal("lease mutated despite denied wake permission")
			}
		})
	}
}
