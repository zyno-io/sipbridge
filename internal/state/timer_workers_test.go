package state

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func expireTimerWorker(t *testing.T, backend Backend, worker string) {
	t.Helper()
	ctx := context.Background()
	switch s := backend.(type) {
	case *Store:
		if err := s.client.ZAdd(ctx, s.root+"timer:workers", redis.Z{Score: 1, Member: worker}).Err(); err != nil {
			t.Fatal(err)
		}
	case *MemoryStore:
		now, err := s.lock(ctx)
		if err != nil {
			t.Fatal(err)
		}
		s.timerWorkers[worker] = now - 1
		s.unlock()
	default:
		t.Fatal("unexpected timer fixture backend")
	}
}

func TestTimerColdRecoveryAdmitsWorkerDespiteOrphanedEpochQueues(t *testing.T) {
	backendContract(t, func(t *testing.T, s Backend) {
		ctx := context.Background()
		for n := 1; n <= 256; n++ {
			worker := fmt.Sprintf("%064x", n)
			ref := contractRef(t, s, fmt.Sprintf("orphan-%d", n))
			// A dead process may leave a derivative hint after its group retired.
			// Cold recovery must admit a replacement before draining those hints.
			switch backend := s.(type) {
			case *Store:
				if err := backend.client.ZAdd(ctx, backend.root+"timer:workers", redis.Z{Score: 1, Member: worker}).Err(); err != nil {
					t.Fatal(err)
				}
				if err := backend.client.ZAdd(ctx, backend.timerKey(worker), redis.Z{Score: 1, Member: backend.keys(ref)[0]}).Err(); err != nil {
					t.Fatal(err)
				}
			case *MemoryStore:
				if _, err := backend.lock(ctx); err != nil {
					t.Fatal(err)
				}
				if backend.timerWorkers == nil {
					backend.timerWorkers = make(map[string]int64)
					backend.timerQueues = make(map[string]*hintHeap)
				}
				backend.timerWorkers[worker] = 1
				queue := &hintHeap{}
				queue.set(ref, 1)
				backend.timerQueues[worker] = queue
				backend.unlock()
			}
		}
		replacement := strings.Repeat("f", 64)
		if err := s.RegisterDispatchWorker(ctx, replacement); err != nil {
			t.Fatal("orphan queues prevented a replacement from joining", err)
		}
		if _, err := s.LeaseScheduler(ctx, "recovery", true); err != nil {
			t.Fatal(err)
		}
		err := s.TakeoverTimers(ctx, "recovery", replacement, 8)
		if err != nil {
			t.Fatal("admitted replacement could not begin bounded recovery", err)
		}
	})
}

func TestOwnedTimerQueueIsolationAndTargetedWake(t *testing.T) {
	backendContract(t, func(t *testing.T, s Backend) {
		ctx := context.Background()
		a, b := strings.Repeat("a", 64), strings.Repeat("b", 64)
		for _, worker := range []string{a, b} {
			if err := s.RegisterDispatchWorker(ctx, worker); err != nil {
				t.Fatal(err)
			}
		}
		beforeA, err := s.DispatchHead(ctx, a)
		if err != nil {
			t.Fatal(err)
		}
		beforeB, err := s.DispatchHead(ctx, b)
		if err != nil {
			t.Fatal(err)
		}
		ref := contractRef(t, s, "owned-timer")
		update := contractRead(t, s, ref, contractInput("first", true))
		due := update.NowMS - 1
		payload := Payload{TimerWorker: a, Timers: []Timer{{ID: "timer", Generation: "1", DueMS: due, DeadlineMS: due}}}
		if _, err := s.CompareAndSwap(ctx, update, payload, nil); err != nil {
			t.Fatal(err)
		}
		ref = update.Record.Ref
		view, _, err := s.ReadState(ctx, ref)
		if err != nil || view.TimerWorker != a {
			t.Fatal("timer executor projection was not retained", err)
		}
		owned, err := s.DispatchHints(ctx, a, 8)
		if err != nil || len(owned) != 1 || owned[0].Ref.ID != ref.ID {
			t.Fatal("owner missed its due timer", owned, err)
		}
		foreign, err := s.DispatchHints(ctx, b, 8)
		if err != nil || len(foreign) != 0 {
			t.Fatal("healthy foreign worker received a timer", foreign, err)
		}
		afterB, err := s.DispatchHead(ctx, b)
		if err != nil || afterB.Cursor != beforeB.Cursor || afterB.DueMS != 0 {
			t.Fatal("foreign commit woke unrelated worker", afterB, err)
		}
		wait, cancel := context.WithTimeout(ctx, time.Second)
		cursor, err := s.WaitDispatch(wait, a, beforeA.Cursor)
		cancel()
		if err != nil || cursor == beforeA.Cursor {
			t.Fatal("owner missed targeted wake", err)
		}
		if err := s.AckDispatch(ctx, a, ref, view.Revision); err != nil {
			t.Fatal(err)
		}
		owned, err = s.DispatchHints(ctx, a, 8)
		if err != nil || len(owned) != 1 {
			t.Fatal("physical acknowledgement erased a timer", err)
		}
		if _, err := s.LeaseScheduler(ctx, "coordinator", true); err != nil {
			t.Fatal(err)
		}
		head, err := s.ScheduleHead(ctx, "coordinator", true, ScheduleProtocol)
		if err != nil || head.DueMS == 0 || head.Class != ScheduleWorkerExpiry {
			t.Fatal("coordinator did not observe owner expiry", head, err)
		}
	})
}

func TestTimerTakeoverMovesOnlySchedulingAndFencesOldAcknowledgement(t *testing.T) {
	backendContract(t, func(t *testing.T, s Backend) {
		ctx := context.Background()
		a, b := strings.Repeat("a", 64), strings.Repeat("b", 64)
		for _, worker := range []string{a, b} {
			if err := s.RegisterDispatchWorker(ctx, worker); err != nil {
				t.Fatal(err)
			}
		}
		ref := contractRef(t, s, "takeover")
		update := contractRead(t, s, ref, contractInput("first", true))
		due := update.NowMS - 1
		payload := Payload{TimerWorker: a, Timers: []Timer{{ID: "timer", Generation: "1", DueMS: due, DeadlineMS: due}},
			Effects: []Effect{{ID: "physical", Kind: "deliver", Status: EffectUnknown, AttemptID: "attempt", DeadlineMS: update.NowMS + 10000, Worker: a}}}
		if _, err := s.CompareAndSwap(ctx, update, payload, nil); err != nil {
			t.Fatal(err)
		}
		ref = update.Record.Ref
		if _, err := s.LeaseScheduler(ctx, "coordinator", true); err != nil {
			t.Fatal(err)
		}
		if err := s.TakeoverTimers(ctx, "coordinator", b, 8); err != nil {
			t.Fatal(err)
		}
		view, _, err := s.ReadState(ctx, ref)
		if err != nil || view.TimerWorker != a {
			t.Fatal("live owner was stolen", err)
		}
		expireTimerWorker(t, s, a)
		if err := s.RegisterDispatchWorker(ctx, b); err != nil {
			t.Fatal(err)
		}
		err = s.TakeoverTimers(ctx, "coordinator", b, 1)
		if err != nil {
			t.Fatal("single group takeover failed", err)
		}
		view, _, err = s.ReadState(ctx, ref)
		if err != nil || view.TimerWorker != b || view.Revision != "1" || len(view.Payload.Effects) != 1 || view.Payload.Effects[0].Worker != a || view.Payload.Effects[0].Status != EffectUnknown {
			t.Fatal("takeover changed protocol/physical authority", err)
		}
		if err := s.AckDispatch(ctx, a, ref, "1"); err != nil {
			t.Fatal(err)
		}
		owned, err := s.DispatchHints(ctx, b, 8)
		if err != nil || len(owned) != 1 {
			t.Fatal("old worker acknowledgement erased replacement timer", err)
		}
		update = contractRead(t, s, ref, contractInput("settle-timer", false))
		if _, err := s.CompareAndSwap(ctx, update, Payload{TimerWorker: a}, nil); err != nil {
			t.Fatal(err)
		}
		view, _, err = s.ReadState(ctx, ref)
		if err != nil || view.TimerWorker != b {
			t.Fatal("old executor reclaimed timer ownership through a commit", err)
		}
		head, err := s.DispatchHead(ctx, b)
		if err != nil || head.DueMS != 0 {
			t.Fatal("settled timer retained queue membership", head, err)
		}
	})
}
