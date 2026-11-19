package engine

import (
	"context"
	"errors"
	"strconv"
	"time"

	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/nativepb"
	"sipbridge/internal/rediscore"
)

// publishRedisInviteRespondRefusal consumes an admitted semantic refusal
// without changing the selected transaction, flow or physical outputs.
func publishRedisInviteRespondRefusal(ctx context.Context, store *rediscore.Store, lease rediscore.ApplicationLeaseRef, command *pb.Command, capture redisNotifyWaitCapture, operation string, executor rediscore.TimerAssignment, deadline time.Time, selected *rediscore.ObservedContext, current *nativepb.Context, policy *pb.CommandAdmission, previous []rediscore.TimerChange, failure error) (rediscore.Identity, error) {
	var zero rediscore.Identity
	if store == nil || selected == nil || selected.View == nil || current == nil || policy == nil || command == nil || failure == nil || ctx.Err() != nil || !time.Now().Before(deadline) {
		return zero, errNativeRecord
	}
	result := &nativepb.Result{Certainty: pb.OperationResult_NOT_ACCEPTED, Error: residentCommandError(failure.Error()), SipStatus: commandRefusalStatus(failure), CutoffMs: deadline.UnixMilli(), Sequence: command.Sequence, OperationId: operation, Fingerprint: capture.fingerprint[:], DecidingLeaseTerm: lease.Term}
	resultBytes, err := encodeNativeResult(result)
	if err != nil {
		return zero, err
	}
	next := proto.Clone(current).(*nativepb.Context)
	next.CommandSequence, next.Revision, next.DirectoryEpoch = command.Sequence, current.Revision+1, policy.DirectoryEpoch
	contextBytes, err := encodeNativeContext(next)
	if err != nil {
		return zero, err
	}
	until := minTime(deadline, time.UnixMilli(int64(policy.ExpiresAtMs)))
	publication := rediscore.Publication{Identity: selected.Identity, Revision: selected.View.Revision, Deadline: until, Application: &lease, Changes: []rediscore.Change{{Field: "r:context", Value: contextBytes}, {Field: "r:result-" + strconv.FormatUint(command.Sequence, 10), Value: resultBytes}}, ResultHint: &rediscore.ResultHint{Context: selected.Identity.Context, Incarnation: selected.Identity.Incarnation, Sequence: command.Sequence}}
	if len(previous) != 0 {
		target := selected.View.Timer
		if target.Worker == "" {
			target = executor
		}
		publication.Timers = &rediscore.TimerMutation{Expected: selected.View.Timer, Target: target, Changes: previous}
	}
	_, err = store.Commit(ctx, publication)
	if errors.Is(err, rediscore.ErrExpired) && publication.Timers != nil && selected.View.Timer != (rediscore.TimerAssignment{}) && selected.View.Timer != executor && ctx.Err() == nil && time.Now().Before(until) {
		publication.Timers.Target = executor
		_, err = store.Commit(ctx, publication)
	}
	if err != nil {
		return zero, err
	}
	if ctx.Err() != nil || !time.Now().Before(until) {
		return zero, rediscore.ErrUnknown
	}
	return selected.Identity, nil
}
