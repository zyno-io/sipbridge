package engine

import (
	"context"
	"time"

	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/rediscore"
)

// Only actual in-flight callbacks are retained here, never quiet contexts or
// native protocol state. Redis remains the authority for event eligibility.
type redisNativeEventKey struct{ context, incarnation, member string }

func redisLifecycleCutoff(due *rediscore.DueContext, deadlineMS int64) (time.Time, error) {
	if due == nil || due.Observed.IsZero() || due.Observed == due.Observed.Round(0) || due.CheckedAtMS <= 0 || deadlineMS <= due.CheckedAtMS {
		return time.Time{}, rediscore.ErrClock
	}
	remaining := deadlineMS - due.CheckedAtMS
	if remaining > int64((1<<63-1)/time.Millisecond) {
		return time.Time{}, rediscore.ErrCorrupt
	}
	cutoff := due.Observed.Add(time.Duration(remaining) * time.Millisecond)
	if cutoff == cutoff.Round(0) {
		return time.Time{}, rediscore.ErrCorrupt
	}
	return cutoff, nil
}

func (e *redisNative) dispatchLifecycle(parent context.Context, store *rediscore.Store, due *rediscore.DueContext) error {
	started := time.Now()
	commitDeadline := started.Add(2 * time.Second)
	if parentDeadline, ok := parent.Deadline(); ok && parentDeadline.Before(commitDeadline) {
		commitDeadline = started.Add(parentDeadline.Sub(started))
	}
	operation, cancel := context.WithDeadline(parent, commitDeadline)
	defer cancel()
	selected, public, err := selectRedisNativeLifecycle(operation, store, due)
	if err != nil {
		return err
	}
	if due.CheckedAtMS >= selected.record.DeadlineMs {
		return completeRedisNativeLifecycle(operation, store, selected, false, commitDeadline)
	}
	eventCutoff, err := redisLifecycleCutoff(due, selected.record.DeadlineMs)
	if err != nil {
		return err
	}
	if !time.Now().Before(eventCutoff) {
		return deferRedisNativeLifecycle(operation, store, selected, commitDeadline)
	}
	key := redisNativeEventKey{due.Identity.Context, due.Identity.Incarnation, due.Member}
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return context.Canceled
	}
	_, busy := e.events[key]
	acquired := false
	if !busy {
		select {
		case e.eventWork <- struct{}{}:
			e.events[key] = struct{}{}
			e.wg.Add(1)
			acquired = true
		default:
		}
	}
	e.mu.Unlock()
	if !acquired {
		return deferRedisNativeLifecycle(operation, store, selected, commitDeadline)
	}
	release := func() {
		e.mu.Lock()
		delete(e.events, key)
		e.mu.Unlock()
		<-e.eventWork
		e.wg.Done()
	}
	// Redis latency spends the original call budget rather than starting a fresh
	// two-second budget after the scheduling publication.
	callDeadline := minTime(commitDeadline, eventCutoff)
	staged, err := prepareRedisNativeLifecycleAttempt(operation, store, selected, callDeadline, commitDeadline)
	if err != nil {
		release()
		return err
	}
	go func() {
		defer release()
		accepted := false
		if parent.Err() == nil && time.Now().Before(callDeadline) {
			ctx, cancel := context.WithDeadline(parent, callDeadline)
			peer, peerErr := e.control.Peer(public.Snapshot.Application)
			if peerErr == nil && public.Kind == pb.DialogEvent_REINVITE_ACKNOWLEDGED && !acceptsReinviteACK(peer, public.Snapshot.ApplicationLeaseTerm) {
				// Executor transfer cannot bypass the selected session's opt-in.
				accepted = true
			} else if peerErr == nil && ctx.Err() == nil {
				reply, callErr := peer.Call(ctx, &pb.Envelope{CorrelationId: due.Member,
					RemainingBudgetMs: uint32(max(1, time.Until(callDeadline).Milliseconds())),
					Payload:           &pb.Envelope_DialogEvent{DialogEvent: public}})
				if callErr == nil && reply != nil {
					result := reply.GetResult()
					accepted = result != nil && result.Error == "" && (result.Certainty == pb.OperationResult_ACCEPTED || result.Certainty == pb.OperationResult_COMPLETED)
				}
			}
			cancel()
		}
		// A failed/uncertain metadata acknowledgement leaves the staged eligibility
		// in Redis. It never causes another application call in this goroutine.
		if parent.Err() == nil {
			until := minTime(time.Now().Add(2*time.Second), eventCutoff)
			if time.Now().Before(until) {
				completion, finish := context.WithDeadline(parent, until)
				_ = completeRedisNativeLifecycle(completion, store, staged, accepted, until)
				finish()
			}
		}
	}()
	return nil
}
