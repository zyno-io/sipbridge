package engine

import (
	"context"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/nativepb"
	"sipbridge/internal/rediscore"
	"sipbridge/internal/selectedmessage"
)

// fireRedisApplicationInviteDeadline applies only the selected re-INVITE's
// original SDK deadline; it never ends the established call on a B timeout.
func fireRedisApplicationInviteDeadline(ctx context.Context, store *rediscore.Store, due *rediscore.DueContext, deadline time.Time) error {
	const prefix = "app-invite-"
	if store == nil || due == nil || due.CheckedAtMS < due.DeadlineMS || !time.Now().Before(deadline) || !strings.HasPrefix(due.Member, prefix) {
		return errNativeRecord
	}
	parts := strings.Split(strings.TrimPrefix(due.Member, prefix), "-")
	if len(parts) != 2 || len(parts[0]) != 32 {
		return errNativeRecord
	}
	id, timer := parts[0], portable.Timer(parts[1])
	if timer != portable.TimerB && timer != portable.TimerD && timer != portable.TimerM && timer != portable.TimerCancel {
		return errNativeRecord
	}
	procedureField, originField, outputField := "r:app-invite-"+id, "r:app-invite-origin-"+id, "r:app-invite-output-"+id
	view, err := store.Read(ctx, due.Identity, "r:context", procedureField, originField, outputField)
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
	var output *nativepb.Output
	if len(view.Records[outputField]) != 0 {
		output, err = decodeNativeOutput(view.Records[outputField])
		if err != nil || !validReadyApplicationInvite(output, outputField) {
			return errNativeRecord
		}
	}
	d := procedure.Native.Timers[timer]
	if current.Context == nil || current.Context.State == nil || current.Context.Id != due.Identity.Context || current.Context.State.Incarnation != due.Identity.Incarnation || current.Kind != pb.ContextSnapshot_UAS && current.Kind != pb.ContextSnapshot_UAC || !slices.Contains(current.NoninitialProcedures, procedureField) || procedure.Native.Family != portable.InviteClient || procedure.Refs.Origin != originField || procedure.Metadata.CommandResult == "" || !d.Active || d.At != due.DeadlineMS || d.Generation == 0 {
		return errNativeRecord
	}
	fields := []string{procedure.Metadata.CommandResult}
	if output != nil {
		fields = append(fields, output.Dialog)
	}
	if procedure.Refs.LastResponse != "" {
		fields = append(fields, procedure.Refs.LastResponse)
	}
	if procedure.Refs.ACK != "" {
		fields = append(fields, procedure.Refs.ACK)
	}
	other, err := store.Read(ctx, due.Identity, fields...)
	if err != nil {
		return err
	}
	if other.Revision != view.Revision || other.Timer != view.Timer {
		return rediscore.ErrConflict
	}
	if output != nil {
		dialog, e := readPrivateDialogRecord(other.Records[output.Dialog])
		if e != nil || !slices.Contains(current.Dialogs, output.Dialog) || dialog.LocalInvite == nil || dialog.LocalInvite.Request != originField || !slices.Contains(dialog.Outputs, outputField) || !slices.Contains(procedure.Metadata.Outputs, outputField) || procedure.Metadata.CommandResult != output.Result {
			return errNativeRecord
		}
	}
	result, err := decodeNativeResult(other.Records[procedure.Metadata.CommandResult])
	if err != nil || result.Sequence == 0 || result.Sequence > current.CommandSequence || procedure.Metadata.CommandResult != "r:result-"+strconv.FormatUint(result.Sequence, 10) {
		return errNativeRecord
	}
	if output != nil && (result.OperationId != output.Operation || result.Output != outputField || result.DecidingLeaseTerm != output.Write.Permit.Authorization.ApplicationLeaseTerm) || output == nil && (!procedure.Metadata.OutputConsumed || result.Output != "" || result.Pending || result.Certainty != pb.OperationResult_COMPLETED) {
		return errNativeRecord
	}
	expectedID, err := redisApplicationByeID(result.Fingerprint)
	if err != nil || expectedID != id {
		return errNativeRecord
	}
	for field, value := range other.Records {
		view.Records[field] = value
	}
	memo, err := selectedmessage.NewMemo(3)
	if err != nil {
		return err
	}
	tx, err := procedure.attach(&nativeSelectedMessages{records: view.Records, memo: memo, provenance: "admitted"}, true, procedure.Refs.LastResponse != "", procedure.Refs.ACK != "")
	if err != nil {
		return err
	}
	facts, err := tx.Origin.Facts()
	if err != nil || facts.Method != sip.INVITE || facts.ToTag == "" {
		return errNativeRecord
	}
	next, effects, err := portable.StepTransaction(tx, portable.Input{Kind: portable.TimerFired, Now: due.CheckedAtMS, Timer: timer, Generation: d.Generation})
	if err != nil {
		return err
	}
	timers := []rediscore.TimerChange{{Member: due.Member, Delete: true}}
	for _, effect := range effects {
		switch effect.Kind {
		case portable.CancelTimer:
			if effect.Timer != portable.TimerA && effect.Timer != timer {
				timers = append(timers, rediscore.TimerChange{Member: prefix + id + "-" + string(effect.Timer), Delete: true})
			}
		case portable.Schedule:
			if effect.Timer != portable.TimerA {
				timers = append(timers, rediscore.TimerChange{Member: prefix + id + "-" + string(effect.Timer), Due: time.UnixMilli(effect.Deadline.At)})
			}
		case portable.Ended, portable.Diagnostic:
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
	data, err := encodeNativeProcedure(next, procedure.Refs, procedure.Metadata)
	if err != nil {
		return err
	}
	changes := append([]rediscore.Change{{Field: procedureField, Value: data}}, released...)
	var hint *rediscore.ResultHint
	if next.Phase == portable.Terminated {
		if result.Pending && result.CutoffMs <= due.CheckedAtMS {
			result.Pending, result.Certainty, result.Error, result.SipStatus = false, pb.OperationResult_UNKNOWN, "SIP delivery unavailable or uncertain", 0
			value, e := encodeNativeResult(result)
			if e != nil {
				return e
			}
			changes = append(changes, rediscore.Change{Field: procedure.Metadata.CommandResult, Value: value})
			hint = &rediscore.ResultHint{Context: due.Identity.Context, Incarnation: due.Identity.Incarnation, Sequence: result.Sequence}
		}
		if output != nil && procedure.Metadata.OutputConsumed {
			timers = append(timers, rediscore.TimerChange{Member: "out-" + strings.TrimPrefix(outputField, "r:"), Due: time.UnixMilli(int64(output.Write.Permit.Authorization.DeadlineMs))})
		}
		timers = append(timers, rediscore.TimerChange{Member: "app-invite-retire-" + id, Due: time.UnixMilli(max(due.CheckedAtMS, result.CutoffMs))})
	}
	publication := rediscore.Publication{Identity: due.Identity, Revision: view.Revision, Deadline: deadline, SpendReservation: true, Changes: changes, ResultHint: hint, Timers: &rediscore.TimerMutation{Expected: due.Assignment, Target: due.Assignment, RequireLease: true, Changes: timers}}
	if procedure.Metadata.Flow != nil && procedure.Metadata.Flow.Transport == "udp" {
		worker, e := nativeWorkerReference(procedure.Metadata.Flow.Ingress)
		if e != nil {
			return e
		}
		publication.UDPHint = &rediscore.UDPHint{Worker: worker, Context: due.Identity.Context, Incarnation: due.Identity.Incarnation, Procedure: procedureField, Stop: true}
	}
	_, err = store.Commit(ctx, publication)
	return err
}
