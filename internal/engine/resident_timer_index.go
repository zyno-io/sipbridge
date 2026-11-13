package engine

import (
	"time"

	"github.com/emiago/sipgo/sip/portable"
)

// Only schedule/cancel effects from the touched native reducer update the heap.
// This neither enumerates contexts nor reconstructs a context-wide timer list.
func (e *ResidentMemory) scheduleResidentTransactionLocked(c *residentContext, tx *residentTransaction, effects []portable.Effect) error {
	now := time.Now()
	e.mu.Lock()
	for _, effect := range effects {
		key := residentDeadlineKey{kind: residentTransactionDeadline, contextID: c.snapshot.Context.Id, record: tx.native.Key, timer: effect.Timer}
		switch effect.Kind {
		case portable.Schedule:
			if effect.Deadline.Active {
				at := now.Add(time.UnixMilli(effect.Deadline.At).Sub(now))
				e.deadlines.set(key, effect.Deadline.Generation, at)
			}
		case portable.CancelTimer:
			e.deadlines.remove(key)
		}
	}
	e.mu.Unlock()
	e.wakeDeadline()
	return nil
}

func (e *ResidentMemory) scheduleResidentDialogLocked(c *residentContext, key string, effects []portable.Effect) error {
	e.mu.Lock()
	for _, effect := range effects {
		timerKey := residentDeadlineKey{kind: residentDialogDeadline, contextID: c.snapshot.Context.Id, record: key, timer: effect.Timer}
		switch effect.Kind {
		case portable.Schedule:
			if effect.Deadline.Active {
				e.deadlines.set(timerKey, effect.Deadline.Generation, time.UnixMilli(effect.Deadline.At))
			}
		case portable.CancelTimer:
			e.deadlines.remove(timerKey)
		}
	}
	e.mu.Unlock()
	e.wakeDeadline()
	return nil
}

// Synchronize only the two transactions touched by the paired CANCEL reducer.
func (e *ResidentMemory) syncResidentTransactionTimersLocked(c *residentContext, tx *residentTransaction, before portable.Transaction) {
	e.mu.Lock()
	for name := range before.Timers {
		if !tx.native.Timers[name].Active {
			e.deadlines.remove(residentDeadlineKey{kind: residentTransactionDeadline, contextID: c.snapshot.Context.Id, record: tx.native.Key, timer: name})
		}
	}
	for name, due := range tx.native.Timers {
		if due.Active {
			e.deadlines.set(residentDeadlineKey{kind: residentTransactionDeadline, contextID: c.snapshot.Context.Id, record: tx.native.Key, timer: name}, due.Generation, time.UnixMilli(due.At))
		}
	}
	e.mu.Unlock()
	e.wakeDeadline()
}
