package engine

import (
	"context"
	"crypto/hmac"
	"slices"
	"strconv"
	"time"

	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/rediscore"
)

// awaitRedisInviteRespondResult observes an already-admitted exact receipt.
// Admission/replay must have passed the ordinary current-owner guard first.
func (e *redisNative) awaitRedisInviteRespondResult(ctx context.Context, store *rediscore.Store, identity rediscore.Identity, lease rediscore.ApplicationLeaseRef, capture redisNotifyWaitCapture, requestID, operation string, deadline, leaseUntil time.Time, waiter *nativeResultWaiter, dialog ...bool) (*pb.OperationResult, error) {
	if store == nil || capture.state == nil || capture.sequence == 0 || waiter == nil || identity.Owner != lease.Application || identity.Term != lease.Term || !time.Now().Before(deadline) || leaseUntil == leaseUntil.Round(0) {
		return nil, errNativeRecord
	}
	ctx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	field := "r:result-" + strconv.FormatUint(capture.sequence, 10)
	procedureField := "r:procedure-" + requestID
	inDialog := len(dialog) == 1 && dialog[0]
	for {
		signal := e.resultWaiterSignal(waiter)
		selected, err := store.ObserveApplicationWitness(ctx, identity.Context, identity.Incarnation, lease, "r:context", field)
		if err != nil {
			return nil, err
		}
		current, err := decodeNativeContext(selected.View.Records["r:context"])
		if err != nil {
			return nil, err
		}
		result, err := decodeNativeResult(selected.View.Records[field])
		if err != nil {
			return nil, err
		}
		initialValid := !inDialog && current.Kind == pb.ContextSnapshot_UAS && slices.Contains(current.InitialProcedures, procedureField) && result.ServerRequestId == ""
		dialogValid := inDialog && (current.Kind == pb.ContextSnapshot_UAS || current.Kind == pb.ContextSnapshot_UAC) && (result.Pending && slices.Contains(current.NoninitialProcedures, procedureField) && result.ServerRequestId == "" || !result.Pending && result.ServerRequestId == requestID && !slices.Contains(current.InitialProcedures, procedureField))
		if current.Context == nil || !proto.Equal(current.Context.State, capture.state) || !initialValid && !dialogValid || current.CommandSequence < capture.sequence || result.Sequence != capture.sequence || result.OperationId != operation || result.DecidingLeaseTerm != lease.Term || !hmac.Equal(result.Fingerprint, capture.fingerprint[:]) || result.Request != "" || result.Response != "" || result.RetirementProof != nil {
			return nil, rediscore.ErrConflict
		}
		snapshot, err := nativeProjectSnapshot(current, nil, nil, nil)
		if err != nil {
			return nil, err
		}
		var originalCutoff time.Time
		if result.Pending {
			if result.Output == "" || result.CutoffMs <= 0 {
				return nil, errNativeRecord
			}
			offset := result.CutoffMs - leaseUntil.UnixMilli()
			if offset > int64((24*time.Hour)/time.Millisecond) || offset < -int64((24*time.Hour)/time.Millisecond) {
				return nil, errNativeRecord
			}
			originalCutoff = nativeUDPRetryDeadline(leaseUntil, result.CutoffMs)
			if !time.Now().Before(originalCutoff) {
				if ctx.Err() != nil {
					return nil, ctx.Err()
				}
				if !time.Now().Before(deadline) {
					return nil, context.DeadlineExceeded
				}
				return &pb.OperationResult{Certainty: pb.OperationResult_UNKNOWN, Error: "command outcome unavailable before deadline", Snapshot: snapshot}, nil
			}
		}

		if !result.Pending {
			if ctx.Err() != nil || !time.Now().Before(deadline) {
				return nil, context.DeadlineExceeded
			}
			return &pb.OperationResult{Certainty: result.Certainty, Error: result.Error, SipStatus: result.SipStatus, Snapshot: snapshot}, nil
		}
		timer := time.NewTimer(time.Until(originalCutoff))
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return nil, ctx.Err()
		case <-signal:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
		case <-timer.C:
			if ctx.Err() != nil || !time.Now().Before(deadline) {
				return nil, context.DeadlineExceeded
			}
			// Re-read once at the actual deadline: a concurrent native result or
			// newer snapshot must win over the stale pre-wait selection.
			continue
		}
	}
}
