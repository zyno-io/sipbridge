package engine

import (
	pb "sipbridge/internal/controlpb"
	"time"
)

// scheduleResident sleeps until a real local deadline or an earlier insertion.
// No idle-context or per-owner polling is performed.
func (e *ResidentMemory) scheduleResident() {
	var timer *time.Timer
	defer func() {
		if timer != nil {
			timer.Stop()
		}
	}()
	for {
		e.mu.Lock()
		item, exists := e.deadlines.next()
		e.mu.Unlock()
		if !exists {
			select {
			case <-e.ctx.Done():
				return
			case <-e.deadlineWake:
				continue
			}
		}
		wait := time.Until(item.at)
		if wait > 0 {
			if timer == nil {
				timer = time.NewTimer(wait)
			} else {
				timer.Reset(wait)
			}
			select {
			case <-e.ctx.Done():
				return
			case <-e.deadlineWake:
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				continue
			case <-timer.C:
			}
		}
		e.mu.Lock()
		item, exists = e.deadlines.popDue(time.Now())
		e.mu.Unlock()
		if exists && !e.expireController(item) {
			e.fireResidentDeadline(item)
		}
	}
}

// Timer output already consumes bounded resident output records and bytes. Each
// scheduled batch owns only those immutable writes; the scheduler never waits on
// the socket and shutdown waits for its bounded batch to return.
func (e *ResidentMemory) dispatchResidentTimerOutputs(writes []*pb.FlowWrite) {
	if len(writes) == 0 {
		return
	}
	e.wg.Add(1)
	go func() { defer e.wg.Done(); e.dispatchResident(e.ctx, writes) }()
}
