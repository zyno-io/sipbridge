package engine

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"sipbridge/internal/state"
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
		lease, err := e.store.LeaseScheduler(ctx, token, true)
		stop()
		if err == nil && !lease.Held {
			lease, err = e.waitSchedulerLease(token, lease)
		}
		if err == nil {
			e.scheduler.acquisitions.Add(1)
			e.scheduleLeader(token, lease.Until)
			ctx, stop = context.WithTimeout(context.WithoutCancel(e.ctx), 100*time.Millisecond)
			_ = e.store.ReleaseScheduler(ctx, token)
			stop()
		}
		// Errors/lost leadership back off; healthy contention waits its observed lease.
		timer := time.NewTimer(500 * time.Millisecond)
		select {
		case <-e.ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

// waitSchedulerLease keeps one role-scoped notification watch while successive
// conservative holder deadlines trigger fresh atomic lease observations.
func (e *StateEngine) waitSchedulerLease(token string, lease state.SchedulerLease) (state.SchedulerLease, error) {
	ctx, cancel := context.WithCancel(e.ctx)
	wake := make(chan struct{}, 1)
	failed := make(chan error, 1)
	done := make(chan struct{})
	go func(cursor string) {
		defer close(done)
		for ctx.Err() == nil {
			next, err := e.store.WaitSchedulerLease(ctx, cursor)
			if err != nil {
				if ctx.Err() == nil {
					failed <- err
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
	}(lease.Cursor)
	defer func() { cancel(); <-done }()
	timer := time.NewTimer(max(0, time.Until(lease.Until)))
	defer timer.Stop()
	for {
		select {
		case <-e.ctx.Done():
			return state.SchedulerLease{}, e.ctx.Err()
		case err := <-failed:
			e.scheduler.fail(schedulerWatchFailure, err)
			return state.SchedulerLease{}, err
		case <-wake:
		case <-timer.C:
		}
		select {
		case err := <-failed:
			e.scheduler.fail(schedulerWatchFailure, err)
			return state.SchedulerLease{}, err
		default:
		}
		call, stop := context.WithTimeout(e.ctx, 100*time.Millisecond)
		next, err := e.store.LeaseScheduler(call, token, true)
		stop()
		if err != nil {
			return state.SchedulerLease{}, err
		}
		if next.Held {
			cancel()
			<-done
			select {
			case err := <-failed:
				e.scheduler.fail(schedulerWatchFailure, err)
				return state.SchedulerLease{}, err
			default:
			}
			if !time.Now().Before(next.Until) {
				return state.SchedulerLease{}, state.ErrStale
			}
			return next, nil
		}
		lease = next
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(max(0, time.Until(lease.Until)))
	}
}

func (e *StateEngine) scheduleLeader(token string, until time.Time) {
	leader, cancel := context.WithCancel(e.ctx)
	defer cancel()
	var deadlineMu sync.Mutex
	deadline := func() time.Time {
		deadlineMu.Lock()
		defer deadlineMu.Unlock()
		return until
	}
	renewDone := make(chan struct{})
	go func() {
		defer close(renewDone)
		timer := time.NewTimer(max(0, time.Until(deadline().Add(-state.SchedulerLeaseLifetime/2))))
		defer timer.Stop()
		for {
			select {
			case <-leader.Done():
				return
			case <-timer.C:
				previous := deadline()
				ctx, stop := context.WithDeadline(leader, minTime(previous, time.Now().Add(100*time.Millisecond)))
				next, err := e.store.LeaseScheduler(ctx, token, false)
				stop()
				if err == nil && (!next.Held || !time.Now().Before(previous)) {
					err = state.ErrStale
				}
				if err != nil {
					e.scheduler.fail(schedulerLeaseFailure, err)
					cancel()
					return
				}
				deadlineMu.Lock()
				until = next.Until
				deadlineMu.Unlock()
				timer.Reset(max(0, time.Until(next.Until.Add(-state.SchedulerLeaseLifetime/2))))
			}
		}
	}()
	defer func() { cancel(); <-renewDone }()
	pass := func() (context.Context, context.CancelFunc) {
		return context.WithDeadline(leader, minTime(deadline(), time.Now().Add(100*time.Millisecond)))
	}
	prefer := state.ScheduleProtocol
	ctx, stop := pass()
	e.scheduler.headReads.Add(1)
	head, err := e.store.ScheduleHead(ctx, token, true, prefer)
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

	repair := time.NewTimer(time.Second)
	defer repair.Stop()
	var repairC <-chan time.Time = repair.C
	timer := time.NewTimer(time.Hour)
	timer.Stop()
	defer timer.Stop()
	var dueC <-chan time.Time
	var nextPass time.Time
	arm := func() {
		timer.Stop()
		dueC = nil
		if head.DueMS != 0 {
			timer.Reset(max(head.After, time.Until(nextPass), 0))
			dueC = timer.C
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
		case <-repairC:
			repairC = nil
			repairDone = make(chan struct{})
			go func(cursor uint64, done chan struct{}) {
				defer close(done)
				ctx, stop := context.WithTimeout(leader, 2*time.Second)
				defer stop()
				next, _, err := e.store.RepairScheduler(ctx, token, cursor, 128)
				repaired <- repairResult{cursor: next, err: err}
			}(repairCursor, repairDone)
		case result := <-repaired:
			<-repairDone
			repairDone = nil
			if result.err != nil {
				e.scheduler.fail(schedulerRepairFailure, result.err)
				return
			}
			e.scheduler.repairPages.Add(1)
			repairCursor, repairing = result.cursor, result.cursor != 0
			if repairing {
				repair.Reset(time.Second)
				repairC = repair.C
			}
		case <-wake:
			e.scheduler.wakeups.Add(1)
			refresh = true
		case <-dueC:
			e.scheduler.hintBatches.Add(1)
			switch head.Class {
			case state.ScheduleApplicationExpiry:
				ctx, stop := context.WithTimeout(leader, 2*time.Second)
				err := e.processApplicationExpiry(ctx)
				stop()
				e.scheduler.fail(schedulerGroupFailure, err)
			case state.ScheduleWorkerExpiry:
				ctx, stop := pass()
				err := e.store.TakeoverTimers(ctx, token, e.dispatchWorker(e.cfg.Process), 64)
				stop()
				if err != nil {
					e.scheduler.fail(schedulerRepairFailure, err)
					return
				}
			case state.ScheduleProtocol:
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
				if corrupt && !repairing {
					repairing = true
					repairCursor = 0
					repair.Reset(time.Second)
					repairC = repair.C
				}
			default:
				e.scheduler.fail(schedulerHeadFailure, state.ErrCorrupt)
				return
			}
			prefer = (head.Class + 1) % state.ScheduleClassCount
			nextPass = time.Now().Add(20 * time.Millisecond)
			refresh = true
		}
		if refresh {
			ctx, stop := pass()
			e.scheduler.headReads.Add(1)
			head, err = e.store.ScheduleHead(ctx, token, false, prefer)
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
