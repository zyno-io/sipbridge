package engine

import (
	"context"
	"errors"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/emiago/sipgo/sip/portable"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/nativepb"
	"sipbridge/internal/rediscore"
	"sipbridge/internal/selectedmessage"
)

// fireRedisCleanupByeTimer executes a cleanup client's original F or K. An F
// timeout ends its selected dialog and stages one snapshot-only lifecycle debt.
func fireRedisCleanupByeTimer(ctx context.Context, store *rediscore.Store, due *rediscore.DueContext, deadline time.Time) error {
	if store == nil || due == nil || due.CheckedAtMS < due.DeadlineMS || !time.Now().Before(deadline) {
		return errNativeRecord
	}
	generic := strings.HasPrefix(due.Member, "app-client-")
	application := strings.HasPrefix(due.Member, "app-bye-") || generic
	prefix := "call-client-"
	if generic {
		prefix = "app-client-"
	} else if application {
		prefix = "app-bye-"
	}
	if !strings.HasPrefix(due.Member, prefix) {
		return errNativeRecord
	}
	parts := strings.Split(strings.TrimPrefix(due.Member, prefix), "-")
	if len(parts) != 2 || len(parts[0]) != 32 || parts[1] != string(portable.TimerF) && parts[1] != string(portable.TimerK) {
		return errNativeRecord
	}
	id, timerKind := parts[0], portable.Timer(parts[1])
	procedureField, originField, outputField := "r:call-cleanup-"+id, "r:call-cleanup-origin-"+id, "r:call-cleanup-output-"+id
	if application {
		procedureField, originField, outputField = "r:"+prefix+id, "r:"+prefix+"origin-"+id, "r:"+prefix+"output-"+id
	}
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
	output, err := decodeNativeOutput(view.Records[outputField])
	if err != nil {
		return err
	}
	timer := procedure.Native.Timers[timerKind]
	if current.Context == nil || current.Context.State == nil || current.Context.Id != due.Identity.Context || current.Context.State.Incarnation != due.Identity.Incarnation || current.Kind != pb.ContextSnapshot_UAS && current.Kind != pb.ContextSnapshot_UAC || !slices.Contains(current.NoninitialProcedures, procedureField) || procedure.Native.Family != portable.NonInviteClient || procedure.Metadata.Cleanup == application || application && procedure.Metadata.CommandResult == "" || !application && procedure.Metadata.CommandResult != "" || procedure.Refs.Origin != originField || !redisApplicationClientMethod(output.Method) || generic == (output.Method == "BYE") || output.Status != 0 || output.Procedure != procedureField || output.Dialog == "" || !slices.Contains(current.Dialogs, output.Dialog) || !proto.Equal(output.Write.Flow, procedure.Metadata.Flow) || !timer.Active || timer.Generation == 0 || timer.At != due.DeadlineMS {
		return errNativeRecord
	}
	fields := []string{output.Dialog}
	if application {
		fields = append(fields, procedure.Metadata.CommandResult)
	}
	if procedure.Refs.LastResponse != "" {
		fields = append(fields, procedure.Refs.LastResponse)
	}
	other, err := store.Read(ctx, due.Identity, fields...)
	if err != nil {
		return err
	}
	if other.Revision != view.Revision || other.Timer != view.Timer {
		return rediscore.ErrConflict
	}
	var result *nativepb.Result
	if application {
		result, err = decodeNativeResult(other.Records[procedure.Metadata.CommandResult])
		if err != nil || output.Result != procedure.Metadata.CommandResult || result.Sequence == 0 || result.Sequence > current.CommandSequence || output.Result != "r:result-"+strconv.FormatUint(result.Sequence, 10) || result.OperationId != output.Operation || result.Output != outputField || result.DecidingLeaseTerm != output.Write.Permit.Authorization.ApplicationLeaseTerm {
			return errNativeRecord
		}
	}
	dialog, err := decodeNativeDialog(other.Records[output.Dialog])
	if err != nil || !generic && (dialog.Metadata.LocalTransaction != procedure.Native.Key || dialog.Refs.LocalBye != originField) || !slices.Contains(dialog.Metadata.Outputs, outputField) {
		return errNativeRecord
	}
	view.Records[output.Dialog] = other.Records[output.Dialog]
	if procedure.Refs.LastResponse != "" {
		view.Records[procedure.Refs.LastResponse] = other.Records[procedure.Refs.LastResponse]
	}
	memo, err := selectedmessage.NewMemo(8)
	if err != nil {
		return err
	}
	messages := &nativeSelectedMessages{records: view.Records, memo: memo, provenance: "admitted"}
	tx, err := procedure.attach(messages, true, procedure.Refs.LastResponse != "", false)
	if err != nil {
		return err
	}
	facts, err := tx.Origin.Facts()
	if err != nil || !facts.IsRequest || string(facts.Method) != output.Method {
		return errNativeRecord
	}
	next, effects, err := portable.StepTransaction(tx, portable.Input{Kind: portable.TimerFired, Now: due.CheckedAtMS, Timer: timerKind, Generation: timer.Generation})
	if err != nil || next.Phase != portable.Terminated {
		return errNativeRecord
	}
	for _, effect := range effects {
		if effect.Kind != portable.Ended && effect.Kind != portable.CancelTimer {
			return errors.New("unexpected cleanup BYE timer effect")
		}
	}
	procedureBytes, err := encodeNativeProcedure(next, procedure.Refs, procedure.Metadata)
	if err != nil {
		return err
	}
	changes := []rediscore.Change{{Field: procedureField, Value: procedureBytes}}
	var resultHint *rediscore.ResultHint
	if application && timerKind == portable.TimerF && result.Certainty != pb.OperationResult_COMPLETED {
		failure := ""
		for _, effect := range effects {
			if effect.Kind == portable.Ended && effect.Error != "" {
				failure = effect.Error
				break
			}
		}
		if failure == "" {
			return errNativeRecord
		}
		result.Pending = false
		result.Certainty, result.SipStatus, result.Error = pb.OperationResult_COMPLETED, 0, failure
		result.Request, result.Response = originField, ""
		value, e := encodeNativeResult(result)
		if e != nil {
			return e
		}
		changes = append(changes, rediscore.Change{Field: output.Result, Value: value})
		resultHint = &rediscore.ResultHint{Context: due.Identity.Context, Incarnation: due.Identity.Incarnation, Sequence: result.Sequence}
	}
	timers := []rediscore.TimerChange{{Member: due.Member, Delete: true}}
	if timerKind == portable.TimerF && !generic {
		// Only the local BYE is needed to prove and apply DialogTerminate; its
		// other immutable messages remain selected but are not re-parsed here.
		var dialogFields []string
		for _, invite := range []*nativeInviteRefs{dialog.Refs.Local, dialog.Refs.Remote} {
			if invite != nil {
				for _, field := range []string{invite.Request, invite.Final, invite.ACK} {
					if _, exists := view.Records[field]; field != "" && !exists && !slices.Contains(dialogFields, field) {
						dialogFields = append(dialogFields, field)
					}
				}
			}
		}
		if len(dialogFields) != 0 {
			dialogMessages, e := store.Read(ctx, due.Identity, dialogFields...)
			if e != nil {
				return e
			}
			if dialogMessages.Revision != view.Revision || dialogMessages.Timer != view.Timer {
				return rediscore.ErrConflict
			}
			for _, field := range dialogFields {
				view.Records[field] = dialogMessages.Records[field]
			}
		}
		all := nativeInviteSelection{Request: true, Final: true, ACK: true}
		restored, e := dialog.attach(messages, nativeDialogSelection{Local: all, Remote: all, LocalBye: true})
		if e != nil || restored.LocalBye == nil || !portable.MatchRequest(*restored.LocalBye, tx.Origin) {
			return errNativeRecord
		}
		nextDialog, dialogEffects, e := portable.StepDialog(restored, portable.DialogInput{Kind: portable.DialogTerminate, Now: due.CheckedAtMS})
		if e != nil {
			return e
		}
		for _, effect := range dialogEffects {
			if effect.Kind == portable.CancelTimer && effect.Timer != portable.Timer2xx {
				timers = append(timers, rediscore.TimerChange{Member: callDialogTimerMember(output.Dialog, effect.Timer), Delete: true})
			}
		}
		dialogBytes, e := encodeNativeDialog(nextDialog, dialog.Refs, dialog.Metadata)
		if e != nil {
			return e
		}
		changes = append(changes, rediscore.Change{Field: output.Dialog, Value: dialogBytes})
		if !current.Ended && current.LocalTag == nextDialog.LocalTag && current.RemoteTag == nextDialog.RemoteTag {
			if current.Revision >= 1<<53-1 || due.CheckedAtMS > (1<<53-1)-int64((2*time.Minute)/time.Millisecond) {
				return errNativeRecord
			}
			current.Ended = true
			current.Revision++
			if current.CallRetireAtMs == 0 {
				current.CallRetireAtMs = due.CheckedAtMS + int64((2*time.Minute)/time.Millisecond)
			}
			snapshot, x := nativeProjectSnapshot(current, &nextDialog, nil, nil)
			if x != nil {
				return x
			}
			eventID := uuid.NewString()
			event, x := encodeNativeLifecycle(&nativepb.Lifecycle{Kind: pb.DialogEvent_ENDED, Snapshot: snapshot, DeadlineMs: due.CheckedAtMS + int64((2*time.Minute)/time.Millisecond), NextMs: due.CheckedAtMS})
			if x != nil {
				return x
			}
			contextBytes, x := encodeNativeContext(current)
			if x != nil {
				return x
			}
			changes = append(changes, rediscore.Change{Field: "r:context", Value: contextBytes}, rediscore.Change{Field: "r:event-" + eventID, Value: event})
			timers = append(timers, rediscore.TimerChange{Member: "event-" + eventID, Due: time.UnixMilli(due.CheckedAtMS)})
		}
	}
	if current.Ended {
		if current.CallRetireAtMs <= 0 || current.CallRetireAtMs > 1<<53-1 {
			return errNativeRecord
		}
		timers = append(timers, rediscore.TimerChange{Member: "call-retire", Due: time.UnixMilli(current.CallRetireAtMs)})
	}
	if procedure.Metadata.OutputConsumed {
		if output.Write.Permit.Authorization.DeadlineMs == 0 {
			return errNativeRecord
		}
		timers = append(timers, rediscore.TimerChange{Member: "out-" + strings.TrimPrefix(outputField, "r:"), Due: time.UnixMilli(int64(output.Write.Permit.Authorization.DeadlineMs))})
	}
	publication := rediscore.Publication{Identity: due.Identity, Revision: view.Revision, Deadline: deadline, SpendReservation: true, Changes: changes, ResultHint: resultHint, Timers: &rediscore.TimerMutation{Expected: due.Assignment, Target: due.Assignment, RequireLease: true, Changes: timers}}
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
