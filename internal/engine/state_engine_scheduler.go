package engine

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
)

type schedulerStatistics struct {
	leader       atomic.Bool
	acquisitions atomic.Uint64
	headReads    atomic.Uint64
	hintBatches  atomic.Uint64
	wakeups      atomic.Uint64
	repairPages  atomic.Uint64
}

type SchedulerStatistics struct {
	Leader                                                     bool
	Acquisitions, HeadReads, HintBatches, Wakeups, RepairPages uint64
}

func (e *StateEngine) SchedulerStatistics() SchedulerStatistics {
	return SchedulerStatistics{Leader: e.scheduler.leader.Load(), Acquisitions: e.scheduler.acquisitions.Load(),
		HeadReads: e.scheduler.headReads.Load(), HintBatches: e.scheduler.hintBatches.Load(), Wakeups: e.scheduler.wakeups.Load(),
		RepairPages: e.scheduler.repairPages.Load()}
}

func (e *StateEngine) schedule() {
	if e.cfg.Process != nil {
		done := make(chan struct{})
		go func() { defer close(done); e.schedulePhysical() }()
		defer func() { <-done }()
	}
	for e.ctx.Err() == nil {
		token := uuid.NewString()
		ctx, stop := context.WithTimeout(e.ctx, 100*time.Millisecond)
		until, err := e.store.LeaseScheduler(ctx, token, true)
		stop()
		if err == nil {
			e.scheduler.acquisitions.Add(1)
			e.scheduleLeader(token, until)
			// Conditional release is only an optimization for graceful handoff.
			ctx, stop = context.WithTimeout(context.WithoutCancel(e.ctx), 100*time.Millisecond)
			_ = e.store.ReleaseScheduler(ctx, token)
			stop()
		}
		timer := time.NewTimer(100 * time.Millisecond)
		select {
		case <-e.ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

func (e *StateEngine) scheduleLeader(token string, until time.Time) {
	leader, cancel := context.WithCancel(e.ctx)
	defer cancel()
	pass := func() (context.Context, context.CancelFunc) {
		return context.WithDeadline(leader, minTime(until, time.Now().Add(100*time.Millisecond)))
	}
	ctx, stop := pass()
	e.scheduler.headReads.Add(1)
	head, err := e.store.ScheduleHead(ctx, token, true)
	stop()
	if err != nil || !time.Now().Before(until) {
		return
	}
	e.scheduler.leader.Store(true)
	defer e.scheduler.leader.Store(false)
	wake := make(chan struct{}, 1)
	failure := make(chan struct{}, 1)
	watchDone := make(chan struct{})
	go func(cursor string) {
		defer close(watchDone)
		for leader.Err() == nil {
			next, err := e.store.WaitSchedule(leader, cursor)
			if err != nil {
				select {
				case failure <- struct{}{}:
				default:
				}
				return
			}
			if next != cursor {
				cursor = next
				select {
				case wake <- struct{}{}:
				default:
				}
			}
		}
	}(head.Cursor)
	defer func() { e.scheduler.leader.Store(false); cancel(); <-watchDone }()

	renew := time.NewTicker(50 * time.Millisecond)
	defer renew.Stop()
	repair := time.NewTicker(time.Second)
	defer repair.Stop()
	timer := time.NewTimer(time.Hour)
	defer timer.Stop()
	var nextPass time.Time
	arm := func() {
		timer.Stop()
		if head.DueMS != 0 {
			timer.Reset(max(head.After, time.Until(nextPass), 0))
		}
	}
	arm()
	var hints stateScheduleCursor
	idle := true
	var repairCursor uint64
	repairing := true
	for leader.Err() == nil && time.Now().Before(until) {
		refresh := false
		select {
		case <-leader.Done():
			return
		case <-failure:
			return
		case <-renew.C:
			ctx, stop := pass()
			nextUntil, err := e.store.LeaseScheduler(ctx, token, false)
			stop()
			if err != nil {
				return
			}
			until = nextUntil
		case <-repair.C:
			if repairing {
				ctx, stop := pass()
				next, _, err := e.store.RepairScheduler(ctx, token, repairCursor, 128)
				stop()
				if err == nil {
					e.scheduler.repairPages.Add(1)
					repairCursor, repairing = next, next != 0
				} else {
					return
				}
			}
		case <-wake:
			e.scheduler.wakeups.Add(1)
			refresh = true
		case <-timer.C:
			e.scheduler.hintBatches.Add(1)
			if idle {
				hints.partition = head.Partition
				idle = false
			}
			ctx, stop := pass()
			corrupt, err := e.scheduleLeasedHints(ctx, leader, &hints, token)
			stop()
			if err != nil {
				return
			}
			repairing = repairing || corrupt
			nextPass = time.Now().Add(20 * time.Millisecond)
			refresh = true
		}
		if refresh {
			ctx, stop := pass()
			e.scheduler.headReads.Add(1)
			head, err = e.store.ScheduleHead(ctx, token, false)
			stop()
			if err != nil {
				return
			}
			if head.DueMS == 0 || head.After > 0 {
				idle = true
			}
			arm()
		}
	}
}

func minTime(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}
