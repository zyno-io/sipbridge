package engine

import (
	"context"
	"time"

	"sipbridge/internal/rediscore"
)

// yieldRedisCallRetirement gives another actual indexed duty precedence over
// the terminal head. The caller must invoke this before any call-retirement
// witness; the immutable first-end cutoff is not changed.
func yieldRedisCallRetirement(ctx context.Context, store *rediscore.Store, due *rediscore.DueContext, deadline time.Time) (bool, error) {
	if store == nil || due == nil || due.Member != "call-retire" || due.CheckedAtMS < due.DeadlineMS || due.CheckedAtMS >= 1<<53-1 || !time.Now().Before(deadline) {
		return false, errNativeRecord
	}
	other, err := store.PeekContextDeadline(ctx, due.Identity, due.Revision, due.Member)
	if err != nil {
		return false, err
	}
	if other == nil {
		return false, nil
	}
	target := max(other.DeadlineMS, due.CheckedAtMS)
	if target >= 1<<53-1 {
		return false, errNativeRecord
	}
	_, err = store.Commit(ctx, rediscore.Publication{Identity: due.Identity, Revision: due.Revision, Deadline: deadline, SpendReservation: true, Timers: &rediscore.TimerMutation{Expected: due.Assignment, Target: due.Assignment, RequireLease: true, Changes: []rediscore.TimerChange{{Member: due.Member, Due: time.UnixMilli(target + 1)}}}})
	return err == nil, err
}

// dispatchRedisCallRetirement remains private until every supported owner
// combination has a bounded closure. It always yields another indexed duty
// before selecting a terminal witness.
func dispatchRedisCallRetirement(ctx context.Context, store *rediscore.Store, due *rediscore.DueContext, deadline time.Time) (bool, error) {
	yielded, err := yieldRedisCallRetirement(ctx, store, due, deadline)
	if err != nil || yielded {
		return false, err
	}
	view, err := store.Read(ctx, due.Identity, "r:context")
	if err != nil {
		return false, err
	}
	if view.Revision != due.Revision || view.Timer != due.Assignment {
		return false, rediscore.ErrConflict
	}
	current, err := decodeNativeContext(view.Records["r:context"])
	if err != nil {
		return false, err
	}
	if !current.Ended {
		handled, e := forgetRedisUnboundCancelledSetup(ctx, store, due, current, deadline)
		if e != nil || handled {
			return false, e
		}
		return retireRedisQuiescentCall(ctx, store, due, deadline)
	}
	owners, err := selectRedisTerminalOwners(ctx, store, due, current, deadline)
	if err != nil || owners == nil {
		return false, err
	}
	if owners.early != "" {
		return false, terminateRedisTerminalEarlyDialog(ctx, store, due, owners, deadline)
	}
	if owners.output != "" {
		_, err = reconcileRedisTerminalOutput(ctx, store, due, owners, deadline)
		return false, err
	}
	if len(current.NoninitialProcedures) != 0 {
		_, err = retireRedisTerminalProcedure(ctx, store, due, owners, deadline)
		return false, err
	}
	return retireRedisQuiescentCall(ctx, store, due, deadline)
}
