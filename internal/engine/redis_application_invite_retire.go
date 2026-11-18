package engine

import (
	"context"
	"errors"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/emiago/sipgo/sip/portable"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/nativepb"
	"sipbridge/internal/rediscore"
)

// retireRedisApplicationInvite follows one actual local InviteClient. Its
// selected dialog, output, CANCEL child and original waiter each re-drive it.
func retireRedisApplicationInvite(ctx context.Context, store *rediscore.Store, due *rediscore.DueContext, key []byte, deadline time.Time) error {
	const prefix = "app-invite-retire-"
	if store == nil || due == nil || due.CheckedAtMS < due.DeadlineMS || !time.Now().Before(deadline) || !strings.HasPrefix(due.Member, prefix) {
		return errNativeRecord
	}
	id := strings.TrimPrefix(due.Member, prefix)
	if len(id) != 32 {
		return errNativeRecord
	}
	field := "r:app-invite-" + id
	view, err := store.Read(ctx, due.Identity, "r:context", field)
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
	procedure, err := readPrivateProcedureRecord(view.Records[field])
	if err != nil {
		return err
	}
	if current.Context == nil || current.Context.State == nil || current.Context.Id != due.Identity.Context || current.Context.State.Incarnation != due.Identity.Incarnation || current.Kind != pb.ContextSnapshot_UAS && current.Kind != pb.ContextSnapshot_UAC || !slices.Contains(current.NoninitialProcedures, field) || procedure.Native.Version != portable.Version || procedure.Native.Family != string(portable.InviteClient) || procedure.Native.Key == "" || procedure.Origin != "r:app-invite-origin-"+id || procedure.CommandResult == "" || procedure.CreditContribution == 0 || procedure.CreditContribution > 1<<53-1 {
		return errNativeRecord
	}
	resultView, err := store.Read(ctx, due.Identity, procedure.CommandResult)
	if err != nil {
		return err
	}
	if resultView.Revision != due.Revision || resultView.Timer != due.Assignment {
		return rediscore.ErrConflict
	}
	result, err := decodeNativeResult(resultView.Records[procedure.CommandResult])
	if err != nil || result.Sequence == 0 || result.Sequence > current.CommandSequence || procedure.CommandResult != "r:result-"+strconv.FormatUint(result.Sequence, 10) {
		return errNativeRecord
	}
	expectedID, err := redisApplicationByeID(result.Fingerprint)
	if err != nil || expectedID != id {
		return errNativeRecord
	}
	timers := []rediscore.TimerChange{{Member: due.Member, Delete: true}}
	if current.Ended {
		if current.CallRetireAtMs <= 0 {
			return errNativeRecord
		}
		timers = append(timers, rediscore.TimerChange{Member: "call-retire", Due: time.UnixMilli(current.CallRetireAtMs)})
	}
	var changes []rediscore.Change
	var hint *rediscore.ResultHint
	terminated := procedure.Native.Phase == string(portable.Terminated) && (procedure.Native.Timers == nil || nativeDeadlinesInactive(procedure.Native.Timers.Values))
	if terminated && result.Pending {
		if result.CutoffMs > due.CheckedAtMS {
			timers[0] = rediscore.TimerChange{Member: due.Member, Due: time.UnixMilli(result.CutoffMs)}
		} else {
			result.Pending, result.Certainty, result.Error, result.SipStatus = false, pb.OperationResult_UNKNOWN, "SIP delivery unavailable or uncertain", 0
			data, e := encodeNativeResult(result)
			if e != nil {
				return e
			}
			changes = append(changes, rediscore.Change{Field: procedure.CommandResult, Value: data})
			hint = &rediscore.ResultHint{Context: due.Identity.Context, Incarnation: due.Identity.Incarnation, Sequence: result.Sequence}
		}
	}
	if !terminated || result.Pending {
		_, err = store.Commit(ctx, rediscore.Publication{Identity: due.Identity, Revision: due.Revision, Deadline: deadline, SpendReservation: true, Changes: changes, ResultHint: hint, Timers: &rediscore.TimerMutation{Expected: due.Assignment, Target: due.Assignment, RequireLease: true, Changes: timers}})
		return err
	}
	var dialog *nativepb.Dialog
	var dialogField string
	if current.CallId != "" && current.LocalTag != "" && current.RemoteTag != "" {
		tags := []string{current.LocalTag, current.RemoteTag}
		slices.Sort(tags)
		selected, e := store.LookupSelector(ctx, nativeSelector(rediscore.SelectorDialog, current.CallId, tags[0], tags[1]))
		if e != nil && !errors.Is(e, rediscore.ErrAbsent) {
			return e
		}
		if e == nil {
			if selected.Identity != due.Identity || selected.View.Revision != due.Revision || selected.View.Timer != due.Assignment {
				return rediscore.ErrConflict
			}
			if !slices.Contains(current.Dialogs, selected.Record) {
				return errNativeRecord
			}
			dialog, e = readPrivateDialogRecord(selected.View.Records[selected.Record])
			if e != nil {
				return e
			}
			dialogField = selected.Record
		}
	}
	cancelID, err := redisClientCancelID(key, current, procedure.Native.Key)
	if err != nil {
		return err
	}
	cleanupHeld := false
	if dialogField != "" {
		if current.Context.State.Authority == nil {
			return errNativeRecord
		}
		cleanupID := nativeIdentity(key, "uac-cleanup", current.Context.State.Authority.PrimaryId, current.Context.State.Incarnation, current.Context.Id, dialogField)[:32]
		cleanupHeld = slices.Contains(current.NoninitialProcedures, "r:call-cleanup-"+cleanupID)
	} else {
		for _, child := range current.NoninitialProcedures {
			if strings.HasPrefix(child, "r:call-cleanup-") {
				return errNativeRecord
			}
		}
	}
	if len(procedure.Outputs) != 0 || slices.Contains(current.NoninitialProcedures, "r:cancel-client-"+cancelID) || cleanupHeld {
		if result.Output != "" {
			outputView, e := store.Read(ctx, due.Identity, result.Output)
			if e != nil {
				return e
			}
			if outputView.Revision != due.Revision || outputView.Timer != due.Assignment {
				return rediscore.ErrConflict
			}
			output, e := decodeNativeOutput(outputView.Records[result.Output])
			if e != nil || !validReadyApplicationInvite(output, result.Output) || output.Procedure != field || output.Result != procedure.CommandResult {
				return errNativeRecord
			}
			timers = append(timers, rediscore.TimerChange{Member: "out-" + strings.TrimPrefix(result.Output, "r:"), Due: time.UnixMilli(int64(output.Write.Permit.Authorization.DeadlineMs))})
		}
		_, err = store.Commit(ctx, rediscore.Publication{Identity: due.Identity, Revision: due.Revision, Deadline: deadline, SpendReservation: true, Changes: changes, ResultHint: hint, Timers: &rediscore.TimerMutation{Expected: due.Assignment, Target: due.Assignment, RequireLease: true, Changes: timers}})
		return err
	}
	if result.Output != "" {
		return errNativeRecord
	}
	if dialog != nil && dialog.LocalInvite != nil && dialog.LocalInvite.Request == procedure.Origin {
		_, err = store.Commit(ctx, rediscore.Publication{Identity: due.Identity, Revision: due.Revision, Deadline: deadline, SpendReservation: true, Changes: changes, ResultHint: hint, Timers: &rediscore.TimerMutation{Expected: due.Assignment, Target: due.Assignment, RequireLease: true, Changes: timers}})
		return err
	}
	fields := append([]string(nil), current.InitialProcedures...)
	currentResult := "r:result-" + strconv.FormatUint(current.CommandSequence, 10)
	if currentResult != procedure.CommandResult {
		fields = append(fields, currentResult)
	}
	if procedure.SuccessfulAck != nil && procedure.SuccessfulAck.AckResult != "" && procedure.SuccessfulAck.AckResult != procedure.CommandResult && procedure.SuccessfulAck.AckResult != currentResult {
		fields = append(fields, procedure.SuccessfulAck.AckResult)
	}
	owners, err := store.Read(ctx, due.Identity, fields...)
	if err != nil {
		return err
	}
	if owners.Revision != due.Revision || owners.Timer != due.Assignment {
		return rediscore.ErrConflict
	}
	var root *nativepb.Procedure
	var rootField string
	for _, anchorField := range current.InitialProcedures {
		anchor, e := readPrivateProcedureRecord(owners.Records[anchorField])
		if e != nil || anchor.Native.Version != portable.Version || anchor.Native.Family != string(portable.InviteClient) && anchor.Native.Family != string(portable.InviteServer) {
			return errNativeRecord
		}
		if anchor.Root == "" {
			if root != nil {
				return errNativeRecord
			}
			root, rootField = anchor, anchorField
		}
	}
	if root == nil || current.Kind == pb.ContextSnapshot_UAS && root.Native.Family != string(portable.InviteServer) || current.Kind == pb.ContextSnapshot_UAC && root.Native.Family != string(portable.InviteClient) {
		return errNativeRecord
	}
	for _, anchorField := range current.InitialProcedures {
		anchor, e := readPrivateProcedureRecord(owners.Records[anchorField])
		if e != nil || anchorField != rootField && (anchor.Root != rootField || anchor.Native.Family != string(portable.InviteClient) || anchor.CommandResult == "" || anchor.CommandResult != root.CommandResult) {
			return errNativeRecord
		}
	}
	wires := []string{procedure.Origin, procedure.LastResponse, procedure.Ack}
	if procedure.SuccessfulAck != nil {
		wires = append(wires, procedure.SuccessfulAck.FinalResponse, procedure.SuccessfulAck.Ack)
	}
	held := make(map[string]bool, len(wires))
	for _, wire := range wires {
		if wire != "" {
			held[wire] = result.Request == wire || result.Response == wire || dialog != nil && nativeDialogReferencesWire(dialog, wire) || nativeProcedureReferencesWire(root, wire)
		}
	}
	if currentResult != procedure.CommandResult {
		latest, e := decodeNativeResult(owners.Records[currentResult])
		if e != nil {
			return e
		}
		for _, wire := range wires {
			if wire != "" && (latest.Request == wire || latest.Response == wire) {
				held[wire] = true
			}
		}
	}
	if procedure.SuccessfulAck != nil && procedure.SuccessfulAck.AckResult != "" {
		linked := result
		if procedure.SuccessfulAck.AckResult != procedure.CommandResult {
			linked, err = decodeNativeResult(owners.Records[procedure.SuccessfulAck.AckResult])
			if err != nil {
				return err
			}
		}
		if linked.Request != procedure.SuccessfulAck.Ack {
			return errNativeRecord
		}
		held[linked.Request] = true
	}
	var eventFields []string
	for _, event := range append(append([]string(nil), root.Events...), procedure.Events...) {
		if !slices.Contains(eventFields, event) {
			eventFields = append(eventFields, event)
		}
	}
	if len(eventFields) > 0 {
		events, e := store.Read(ctx, due.Identity, eventFields...)
		if e != nil {
			return e
		}
		if events.Revision != due.Revision || events.Timer != due.Assignment {
			return rediscore.ErrConflict
		}
		for _, eventField := range eventFields {
			event, e := decodeNativeLifecycle(events.Records[eventField])
			if e != nil || event.Root != rootField {
				return errNativeRecord
			}
			if event.Message != "" && slices.Contains(wires, event.Message) {
				held[event.Message] = true
			}
			if index := slices.Index(event.Owners, field); index >= 0 {
				if event.Message == "" || !slices.Contains(wires, event.Message) {
					return errNativeRecord
				}
				event.Owners = slices.Delete(event.Owners, index, index+1)
				data, x := encodeNativeLifecycle(event)
				if x != nil {
					return x
				}
				changes = append(changes, rediscore.Change{Field: eventField, Value: data})
			}
		}
	}
	index := slices.Index(current.NoninitialProcedures, field)
	current.NoninitialProcedures = slices.Delete(current.NoninitialProcedures, index, index+1)
	contextData, err := encodeNativeContext(current)
	if err != nil {
		return err
	}
	changes = append(changes, rediscore.Change{Field: "r:context", Value: contextData}, rediscore.Change{Field: field, Delete: true})
	for _, wire := range wires {
		if wire != "" && !held[wire] {
			changes = append(changes, rediscore.Change{Field: wire, Delete: true})
			held[wire] = true
		}
	}
	if current.CommandSequence > result.Sequence {
		timers = append(timers, rediscore.TimerChange{Member: "orig-result-" + strconv.FormatUint(result.Sequence, 10), Due: time.UnixMilli(result.CutoffMs)})
	}
	_, err = store.Commit(ctx, rediscore.Publication{Identity: due.Identity, Revision: due.Revision, Deadline: deadline, SpendReservation: true, CreditDelta: -int64(procedure.CreditContribution), Changes: changes, ResultHint: hint, Selectors: []rediscore.SelectorChange{{Selector: nativeSelector(rediscore.SelectorClientTransaction, procedure.Native.Key), Record: field, Delete: true}}, Timers: &rediscore.TimerMutation{Expected: due.Assignment, Target: due.Assignment, RequireLease: true, Changes: timers}})
	return err
}
