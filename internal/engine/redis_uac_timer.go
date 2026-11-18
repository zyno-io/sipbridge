package engine

import (
	"context"
	"errors"
	"slices"
	"strings"
	"time"

	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
	"github.com/google/uuid"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/nativepb"
	"sipbridge/internal/rediscore"
	"sipbridge/internal/selectedmessage"
)

// fireRedisUACDeadline advances an exact initial INVITE client deadline.
func fireRedisUACDeadline(ctx context.Context, store *rediscore.Store, due *rediscore.DueContext, deadline time.Time) error {
	if store == nil || due == nil || due.CheckedAtMS < due.DeadlineMS || !time.Now().Before(deadline) || !strings.HasPrefix(due.Member, "call-tx-") {
		return errNativeRecord
	}
	parts := strings.Split(strings.TrimPrefix(due.Member, "call-tx-"), "-")
	if len(parts) != 2 || len(parts[0]) != 32 {
		return errNativeRecord
	}
	timer := portable.Timer(parts[1])
	if timer != portable.TimerB && timer != portable.TimerD && timer != portable.TimerM && timer != portable.TimerCancel {
		return errNativeRecord
	}
	procedureField := "r:uac-procedure-" + parts[0]
	view, err := store.Read(ctx, due.Identity, "r:context", procedureField, "r:result-1")
	if err != nil {
		return err
	}
	if view.Revision != due.Revision || view.Timer != due.Assignment {
		return rediscore.ErrConflict
	}
	current, err := decodeNativeContext(view.Records["r:context"])
	if err != nil {
		return err
	}
	procedure, err := decodeNativeProcedure(view.Records[procedureField])
	if err != nil {
		return err
	}
	result, err := decodeNativeResult(view.Records["r:result-1"])
	if err != nil {
		return err
	}
	d := procedure.Native.Timers[timer]
	if current.Kind != pb.ContextSnapshot_UAC || current.Context == nil || current.Context.State == nil || current.Context.Id != due.Identity.Context || current.Context.State.Incarnation != due.Identity.Incarnation || !slices.Contains(current.InitialProcedures, procedureField) || procedure.Native.Family != portable.InviteClient || procedure.Metadata.CommandResult != "r:result-1" || procedure.Refs.Origin != "r:uac-origin-"+parts[0] || result.Sequence != 1 || !d.Active || d.At != due.DeadlineMS || d.Generation == 0 {
		return errNativeRecord
	}
	if _, _, err = selectRedisUACRoot(ctx, store, due.Identity, view.Revision, view.Timer, current, procedureField, procedure); err != nil {
		return err
	}
	fields := []string{procedure.Refs.Origin}
	if procedure.Refs.LastResponse != "" {
		fields = append(fields, procedure.Refs.LastResponse)
	}
	if procedure.Refs.ACK != "" {
		fields = append(fields, procedure.Refs.ACK)
	}
	wires, err := store.Read(ctx, due.Identity, fields...)
	if errors.Is(err, rediscore.ErrAbsent) {
		return rediscore.ErrConflict
	}
	if err != nil {
		return err
	}
	if wires.Revision != view.Revision || wires.Timer != due.Assignment {
		return rediscore.ErrConflict
	}
	memo, err := selectedmessage.NewMemo(len(fields))
	if err != nil {
		return err
	}
	tx, err := procedure.attach(&nativeSelectedMessages{records: wires.Records, memo: memo, provenance: "admitted"}, true, true, true)
	if err != nil {
		return err
	}
	facts, err := tx.Origin.Facts()
	if err != nil || facts.Method != sip.INVITE || facts.ToTag != "" {
		return errNativeRecord
	}
	next, effects, err := portable.StepTransaction(tx, portable.Input{Kind: portable.TimerFired, Now: due.CheckedAtMS, Timer: timer, Generation: d.Generation})
	if err != nil {
		return err
	}
	timers := []rediscore.TimerChange{{Member: due.Member, Delete: true}}
	failure := ""
	for _, effect := range effects {
		switch effect.Kind {
		case portable.CancelTimer:
			if effect.Timer != portable.TimerA && effect.Timer != timer {
				timers = append(timers, rediscore.TimerChange{Member: "call-tx-" + parts[0] + "-" + string(effect.Timer), Delete: true})
			}
		case portable.Schedule:
			if effect.Timer != portable.TimerA {
				timers = append(timers, rediscore.TimerChange{Member: "call-tx-" + parts[0] + "-" + string(effect.Timer), Due: time.UnixMilli(effect.Deadline.At)})
			}
		case portable.Ended:
			failure = effect.Error
		case portable.Diagnostic:
		default:
			return errNativeRecord
		}
	}
	var released []rediscore.Change
	if timer == portable.TimerM && procedure.Metadata.SuccessfulAck != nil {
		candidate, e := buildPrivateProcedureRecord(next, procedure.Refs, procedure.Metadata)
		if e != nil {
			return e
		}
		released, _, e = planRedisSuccessfulACKRelease(ctx, store, due, current, procedureField, candidate)
		if e != nil {
			return e
		}
		procedure.Metadata.SuccessfulAck, procedure.Metadata.Events = candidate.SuccessfulAck, candidate.Events
	}
	var credit int64
	if timer == portable.TimerM {
		credit = releaseRedisSettledSetupCredit(current, procedure.Metadata)
	}
	procData, err := encodeNativeProcedure(next, procedure.Refs, procedure.Metadata)
	if err != nil {
		return err
	}
	changes := append([]rediscore.Change{{Field: procedureField, Value: procData}}, released...)
	if failure != "" && !current.Answered && !current.Ended {
		if timer != portable.TimerB && timer != portable.TimerCancel || current.Revision >= 1<<53-1 {
			return errNativeRecord
		}
		current.Ended = true
		current.Revision++
		now := time.UnixMilli(due.CheckedAtMS)
		if current.CallRetireAtMs == 0 {
			current.CallRetireAtMs = now.Add(2 * time.Minute).UnixMilli()
		}
		snapshot, e := nativeProjectSnapshot(current, nil, nil, nil)
		if e != nil {
			return e
		}
		eventID := uuid.NewString()
		event, e := encodeNativeLifecycle(&nativepb.Lifecycle{Kind: pb.DialogEvent_FAILED, Snapshot: snapshot, OperationId: result.OperationId, DeadlineMs: now.Add(2 * time.Minute).UnixMilli(), NextMs: now.UnixMilli()})
		if e != nil {
			return e
		}
		contextData, e := encodeNativeContext(current)
		if e != nil {
			return e
		}
		changes = append(changes, rediscore.Change{Field: "r:context", Value: contextData}, rediscore.Change{Field: "r:event-" + eventID, Value: event})
		timers = append(timers, rediscore.TimerChange{Member: "event-" + eventID, Due: now}, rediscore.TimerChange{Member: "call-retire", Due: time.UnixMilli(current.CallRetireAtMs)})
		if result.Certainty == pb.OperationResult_ACCEPTED || result.Certainty == pb.OperationResult_UNKNOWN {
			result.Certainty, result.SipStatus, result.Pending = pb.OperationResult_COMPLETED, 0, false
			if result.Request != "" {
				result.Error = failure
			}
			data, e := encodeNativeResult(result)
			if e != nil {
				return e
			}
			changes = append(changes, rediscore.Change{Field: "r:result-1", Value: data})
		}
	}
	_, err = store.Commit(ctx, rediscore.Publication{Identity: due.Identity, Revision: view.Revision, Deadline: deadline, CreditDelta: credit, SpendReservation: true, Changes: changes, Timers: &rediscore.TimerMutation{Expected: due.Assignment, Target: due.Assignment, RequireLease: true, Changes: timers}})
	return err
}
