package engine

import (
	"context"
	"errors"
	"strings"
	"time"

	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/rediscore"
)

// settleRedisCallByeOutputAndRedrive consumes an already recorded exact proof.
// Raw fallback only records proof and never invokes this native continuation.
func settleRedisCallByeOutputAndRedrive(ctx context.Context, store *rediscore.Store, write *pb.FlowWrite, capture *nativeOutputCapture, executor rediscore.TimerAssignment, deadline time.Time) error {
	selected, err := inspectRedisCallByeOutput(ctx, store, write, capture)
	if err != nil {
		return err
	}
	proof := selected.capture.proof
	if proof.State == rediscore.OutputStarted || proof.State == rediscore.OutputUnknown {
		return nil
	}
	if selected.selected == nil || proof.State != rediscore.OutputWritten && proof.State != rediscore.OutputNotWritten || proof.DeadlineMS <= 0 || proof.Acknowledged && !selected.procedure.OutputConsumed {
		return errNativeRecord
	}
	member := "out-" + strings.TrimPrefix(selected.capture.proof.MetadataField, "r:")
	assignment := selected.selected.View.Timer
	target := assignment
	if target == (rediscore.TimerAssignment{}) {
		target = executor
	}
	publication := rediscore.Publication{Identity: selected.selected.Identity, Revision: selected.selected.View.Revision, Deadline: deadline, SpendReservation: true, Timers: &rediscore.TimerMutation{Expected: assignment, Target: target, Changes: []rediscore.TimerChange{{Member: member, Due: time.UnixMilli(selected.capture.proof.DeadlineMS)}}}}
	_, err = store.Commit(ctx, publication)
	if errors.Is(err, rediscore.ErrExpired) && assignment != (rediscore.TimerAssignment{}) && assignment != executor && time.Now().Before(deadline) && ctx.Err() == nil {
		publication.Timers.Target = executor
		_, err = store.Commit(ctx, publication)
	}
	if err != nil {
		return err
	}
	if !proof.Settled {
		return store.SettleOutput(ctx, selected.capture.ref, false, true)
	}
	return nil
}
