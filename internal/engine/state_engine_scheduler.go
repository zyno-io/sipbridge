package engine

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
)

type schedulerStatistics struct {
	leader        atomic.Bool
	acquisitions  atomic.Uint64
	headReads     atomic.Uint64
	hintBatches   atomic.Uint64
	wakeups       atomic.Uint64
	repairPages   atomic.Uint64
	workerHeads   atomic.Uint64
	workerBatches atomic.Uint64
	workerWakes   atomic.Uint64
	ownedGroups   atomic.Uint64
	failures      schedulerFailureCounters
}

type SchedulerStatistics struct {
	Leader                                                     bool
	Acquisitions, HeadReads, HintBatches, Wakeups, RepairPages uint64
	WorkerHeadReads, WorkerBatches, WorkerWakeups, OwnedGroups uint64
	Failures                                                   []SchedulerFailure
}

func (e *StateEngine) SchedulerStatistics() SchedulerStatistics {
	return SchedulerStatistics{Leader: e.scheduler.leader.Load(), Acquisitions: e.scheduler.acquisitions.Load(),
		HeadReads: e.scheduler.headReads.Load(), HintBatches: e.scheduler.hintBatches.Load(), Wakeups: e.scheduler.wakeups.Load(),
		RepairPages: e.scheduler.repairPages.Load(), Failures: e.scheduler.failureSnapshot(),
		WorkerHeadReads: e.scheduler.workerHeads.Load(), WorkerBatches: e.scheduler.workerBatches.Load(),
		WorkerWakeups: e.scheduler.workerWakes.Load(), OwnedGroups: e.scheduler.ownedGroups.Load()}
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
		timer := time.NewTimer(500 * time.Millisecond)
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
	// Queue discovery and maintenance must never hold up lease renewal. The
	// deadline remains a scheduling hint; every store operation fences its token.
	var deadlineMu sync.Mutex
	deadline := func() time.Time {
		deadlineMu.Lock()
		defer deadlineMu.Unlock()
		return until
	}
	renewDone := make(chan struct{})
	go func() {
		defer close(renewDone)
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-leader.Done():
				return
			case <-ticker.C:
				ctx, stop := context.WithDeadline(leader, minTime(deadline(), time.Now().Add(100*time.Millisecond)))
				next, err := e.store.LeaseScheduler(ctx, token, false)
				stop()
				if err != nil {
					e.scheduler.fail(schedulerLeaseFailure, err)
					cancel()
					return
				}
				deadlineMu.Lock()
				until = next
				deadlineMu.Unlock()
			}
		}
	}()
	defer func() { cancel(); <-renewDone }()
	pass := func() (context.Context, context.CancelFunc) {
		return context.WithDeadline(leader, minTime(deadline(), time.Now().Add(100*time.Millisecond)))
	}
	ctx, stop := pass()
	e.scheduler.headReads.Add(1)
	head, err := e.store.ScheduleHead(ctx, token, true)
	stop()
	if err != nil || !time.Now().Before(deadline()) {
		e.scheduler.fail(schedulerHeadFailure, err)
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
				if leader.Err() == nil {
					e.scheduler.fail(schedulerWatchFailure, err)
				}
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

	repair := time.NewTicker(time.Second)
	defer repair.Stop()
	takeover := time.NewTimer(500 * time.Millisecond)
	defer takeover.Stop()
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
	type repairResult struct {
		cursor uint64
		err    error
	}
	repaired := make(chan repairResult, 1)
	var repairDone chan struct{}
	defer func() {
		cancel()
		if repairDone != nil {
			<-repairDone
		}
	}()
	for leader.Err() == nil && time.Now().Before(deadline()) {
		refresh := false
		select {
		case <-leader.Done():
			return
		case <-failure:
			return
		case <-takeover.C:
			ctx, stop := pass()
			more, err := e.store.TakeoverTimers(ctx, token, e.dispatchWorker(e.cfg.Process), 64)
			stop()
			if err != nil && leader.Err() == nil {
				e.scheduler.fail(schedulerRepairFailure, err)
			}
			pause := 500 * time.Millisecond
			if more && err == nil {
				pause = 20 * time.Millisecond
			}
			takeover.Reset(pause)
		case <-repair.C:
			if repairing && repairDone == nil {
				repairDone = make(chan struct{})
				go func(cursor uint64, done chan struct{}) {
					defer close(done)
					ctx, stop := context.WithTimeout(leader, 2*time.Second)
					defer stop()
					next, _, err := e.store.RepairScheduler(ctx, token, cursor, 128)
					repaired <- repairResult{cursor: next, err: err}
				}(repairCursor, repairDone)
			}
		case result := <-repaired:
			<-repairDone
			repairDone = nil
			if result.err != nil {
				e.scheduler.fail(schedulerRepairFailure, result.err)
				return
			}
			e.scheduler.repairPages.Add(1)
			repairCursor, repairing = result.cursor, result.cursor != 0
		case <-wake:
			e.scheduler.wakeups.Add(1)
			refresh = true
		case <-timer.C:
			e.scheduler.hintBatches.Add(1)
			if head.ApplicationExpiry {
				ctx, stop := context.WithTimeout(leader, 2*time.Second)
				err := e.processApplicationExpiry(ctx)
				stop()
				e.scheduler.fail(schedulerGroupFailure, err)
				nextPass = time.Now().Add(20 * time.Millisecond)
				refresh = true
				break
			}
			if idle {
				hints.partition = head.Partition
				idle = false
			}
			ctx, stop := pass()
			corrupt, err := e.scheduleLeasedHints(ctx, leader, &hints, token)
			stop()
			if err != nil {
				e.scheduler.fail(schedulerHintFailure, err)
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
				e.scheduler.fail(schedulerHeadFailure, err)
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
