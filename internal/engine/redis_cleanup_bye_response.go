package engine

import (
	"context"
	"errors"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/nativepb"
	"sipbridge/internal/rediscore"
	"sipbridge/internal/selectedmessage"
	"sipbridge/internal/storagecodec"
)

// completeRedisCleanupByeResponse advances a matched native cleanup BYE. The
// lifecycle debt and transaction share the one selected response record.
func completeRedisCleanupByeResponse(ctx context.Context, store *rediscore.Store, flow *pb.FlowRef, message portable.Message, selectors []string, deadline time.Time) (bool, error) {
	facts, err := message.Facts()
	if err != nil {
		return false, err
	}
	if facts.IsRequest || !redisApplicationClientMethod(string(facts.CSeqMethod)) {
		return false, nil
	}
	if store == nil || !time.Now().Before(deadline) || validateRedisNotifyResponseMetadata(message, flow) != nil {
		return true, errNativeRecord
	}
	ids, err := nativeSelectorIDs(message, false, false)
	if err != nil || len(ids) == 0 || ids[0].Kind != rediscore.SelectorClientTransaction {
		return true, errNativeRecord
	}
	selected, err := store.LookupSelector(ctx, ids[0], "r:context")
	if errors.Is(err, rediscore.ErrAbsent) {
		return false, nil
	}
	if err != nil {
		return true, err
	}
	generic := facts.CSeqMethod != sip.BYE
	application := strings.HasPrefix(selected.Record, "r:app-bye-") || strings.HasPrefix(selected.Record, "r:app-client-")
	prefix := "r:call-cleanup-"
	if application {
		prefix = "r:" + redisApplicationClientPrefix(string(facts.CSeqMethod))
	}
	if !strings.HasPrefix(selected.Record, prefix) || generic && !application {
		return false, nil
	}
	id := strings.TrimPrefix(selected.Record, prefix)
	if len(id) != 32 {
		return true, errNativeRecord
	}
	originField, responseField, outputField := "r:call-cleanup-origin-"+id, "r:call-cleanup-response-"+id, "r:call-cleanup-output-"+id
	if application {
		originField, responseField, outputField = prefix+"origin-"+id, prefix+"response-"+id, prefix+"output-"+id
	}
	current, err := decodeNativeContext(selected.View.Records["r:context"])
	if err != nil {
		return true, err
	}
	procedure, err := decodeNativeProcedure(selected.View.Records[selected.Record])
	if err != nil {
		return true, err
	}
	if current.Context == nil || current.Context.State == nil || current.Context.Id != selected.Identity.Context || current.Context.State.Incarnation != selected.Identity.Incarnation || current.Kind != pb.ContextSnapshot_UAS && current.Kind != pb.ContextSnapshot_UAC || current.CallId != facts.CallID || !slices.Contains(current.NoninitialProcedures, selected.Record) || procedure.Native.Family != portable.NonInviteClient || procedure.Metadata.Cleanup == application || application && procedure.Metadata.CommandResult == "" || !application && procedure.Metadata.CommandResult != "" || procedure.Refs.Origin != originField || procedure.Refs.LastResponse != "" && procedure.Refs.LastResponse != responseField || !proto.Equal(procedure.Metadata.Flow, flow) {
		return true, errNativeRecord
	}
	fields := []string{originField, outputField}
	if application {
		fields = append(fields, procedure.Metadata.CommandResult)
	}
	if procedure.Refs.LastResponse != "" {
		fields = append(fields, responseField)
	}
	view, err := store.Read(ctx, selected.Identity, fields...)
	if err != nil {
		return true, err
	}
	if view.Revision != selected.View.Revision || view.Timer != selected.View.Timer {
		return true, rediscore.ErrConflict
	}
	output, err := decodeNativeOutput(view.Records[outputField])
	if err != nil || output.Method != string(facts.CSeqMethod) || output.Procedure != selected.Record || output.Dialog == "" || !slices.Contains(current.Dialogs, output.Dialog) || !proto.Equal(output.Write.Flow, flow) || application && (output.Result != procedure.Metadata.CommandResult || output.Operation == "") || !application && (output.Result != "" || output.Operation != "") {
		return true, errNativeRecord
	}
	var result *nativepb.Result
	if application {
		result, err = decodeNativeResult(view.Records[output.Result])
		if err != nil || result.Sequence == 0 || result.Sequence > current.CommandSequence || output.Result != "r:result-"+strconv.FormatUint(result.Sequence, 10) || result.OperationId != output.Operation || result.Output != outputField || result.DecidingLeaseTerm != output.Write.Permit.Authorization.ApplicationLeaseTerm {
			return true, errNativeRecord
		}
	}
	dv, err := store.Read(ctx, selected.Identity, output.Dialog)
	if err != nil {
		return true, err
	}
	if dv.Revision != view.Revision || dv.Timer != view.Timer {
		return true, rediscore.ErrConflict
	}
	dialog, err := decodeNativeDialog(dv.Records[output.Dialog])
	if err != nil || !generic && (dialog.Metadata.LocalTransaction != procedure.Native.Key || dialog.Refs.LocalBye != originField) || !slices.Contains(dialog.Metadata.Outputs, outputField) {
		return true, errNativeRecord
	}
	memo, err := selectedmessage.NewMemo(9)
	if err != nil {
		return true, err
	}
	message, err = memo.Adopt(message, "admitted")
	if err != nil {
		return true, err
	}
	messages := &nativeSelectedMessages{records: view.Records, memo: memo, provenance: "admitted"}
	tx, err := procedure.attach(messages, true, procedure.Refs.LastResponse != "", false)
	if err != nil {
		return true, err
	}
	if !portable.MatchResponse(tx.Origin, message) {
		return true, errNativeRecord
	}
	now := time.Now()
	next, effects, err := portable.StepTransaction(tx, portable.Input{Kind: portable.ReceiveResponse, Now: now.UnixMilli(), Message: message})
	if err != nil {
		return true, err
	}
	if len(effects) == 0 {
		return true, nil
	}
	if next.LastResponse == nil || !next.LastResponse.EqualWire(message) {
		return true, errNativeRecord
	}
	fields = nil
	if !generic {
		for _, invite := range []*nativeInviteRefs{dialog.Refs.Local, dialog.Refs.Remote} {
			if invite != nil {
				for _, field := range []string{invite.Request, invite.Final, invite.ACK} {
					if field != "" && !slices.Contains(fields, field) {
						fields = append(fields, field)
					}
				}
			}
		}
	}
	if len(fields) > 0 {
		extra, e := store.Read(ctx, selected.Identity, fields...)
		if e != nil {
			return true, e
		}
		if extra.Revision != view.Revision || extra.Timer != view.Timer {
			return true, rediscore.ErrConflict
		}
		for field, data := range extra.Records {
			view.Records[field] = data
		}
	}
	var nextDialog portable.Dialog
	var dialogEffects []portable.Effect
	if !generic {
		all := nativeInviteSelection{Request: true, Final: true, ACK: true}
		restored, e := dialog.attach(messages, nativeDialogSelection{Local: all, Remote: all, LocalBye: true})
		if e != nil || restored.LocalBye == nil || !portable.MatchRequest(*restored.LocalBye, tx.Origin) || !portable.MatchResponse(tx.Origin, message) || !portable.MatchDialogResponse(restored, message) {
			return true, errNativeRecord
		}
		nextDialog, dialogEffects, err = portable.StepDialog(restored, portable.DialogInput{Kind: portable.DialogReceiveResponse, Now: now.UnixMilli(), Message: message})
		if err != nil {
			return true, err
		}
	}
	responseBytes, err := selectedmessage.Marshal(message)
	if err != nil || len(responseBytes) > redisNotifyResponseRecordLimit {
		return true, errNativeRecord
	}
	procBytes, err := encodeNativeProcedure(next, nativeProcedureRefs{Origin: originField, LastResponse: responseField}, procedure.Metadata)
	if err != nil {
		return true, err
	}
	if current.Revision >= 1<<53-1 || now.UnixMilli() > (1<<53-1)-int64((2*time.Minute)/time.Millisecond) {
		return true, errNativeRecord
	}
	current.Revision++
	changes := []rediscore.Change{{Field: selected.Record, Value: procBytes}, {Field: responseField, Value: responseBytes}}
	if generic {
		deliveryChanges, x := adoptRegistrationFallback(current, procedure.Metadata, dialog.Native, flow, message, facts.Status)
		if x != nil {
			return true, x
		}
		changes = append(changes, deliveryChanges...)
	}
	if !generic {
		dialogBytes, e := encodeNativeDialog(nextDialog, dialog.Refs, dialog.Metadata)
		if e != nil {
			return true, e
		}
		changes = append(changes, rediscore.Change{Field: output.Dialog, Value: dialogBytes})
	}
	var timers []rediscore.TimerChange
	for _, effect := range effects {
		if effect.Timer == portable.TimerE {
			continue
		}
		member := "call-client-" + id + "-" + string(effect.Timer)
		if application {
			member = redisApplicationClientPrefix(string(facts.CSeqMethod)) + id + "-" + string(effect.Timer)
		}
		if effect.Kind == portable.Schedule {
			timers = append(timers, rediscore.TimerChange{Member: member, Due: time.UnixMilli(effect.Deadline.At)})
		}
		if effect.Kind == portable.CancelTimer {
			timers = append(timers, rediscore.TimerChange{Member: member, Delete: true})
		}
	}
	var hint *rediscore.ResultHint
	if application && facts.Status >= 200 && result.Certainty != pb.OperationResult_COMPLETED {
		result.Pending = false
		result.Certainty, result.SipStatus, result.Error = pb.OperationResult_COMPLETED, uint32(facts.Status), ""
		result.Request, result.Response = originField, responseField
		value, e := encodeNativeResult(result)
		if e != nil {
			return true, e
		}
		changes = append(changes, rediscore.Change{Field: output.Result, Value: value})
		hint = &rediscore.ResultHint{Context: selected.Identity.Context, Incarnation: selected.Identity.Incarnation, Sequence: result.Sequence}
	}
	ended := false
	for _, effect := range dialogEffects {
		if effect.Kind == portable.Ended {
			ended = true
		}
		if effect.Kind == portable.CancelTimer && effect.Timer != portable.Timer2xx {
			timers = append(timers, rediscore.TimerChange{Member: callDialogTimerMember(output.Dialog, effect.Timer), Delete: true})
		}
	}
	if ended && !current.Ended && current.LocalTag == nextDialog.LocalTag && current.RemoteTag == nextDialog.RemoteTag {
		if len(current.InitialProcedures) == 0 {
			return true, errNativeRecord
		}
		roots, e := store.Read(ctx, selected.Identity, current.InitialProcedures...)
		if e != nil {
			return true, e
		}
		if roots.Revision != view.Revision || roots.Timer != view.Timer {
			return true, rediscore.ErrConflict
		}
		remoteRequest, remoteFinal := "", ""
		if dialog.Refs.Remote != nil {
			remoteRequest, remoteFinal = dialog.Refs.Remote.Request, dialog.Refs.Remote.Final
		}
		rootField, root, reinviteOwner, e := resolveRedisCleanupRoot(ctx, store, selected.Identity, view.Revision, view.Timer, current, roots.Records, dialog.Metadata.Transaction, remoteRequest, remoteFinal)
		if e != nil {
			return true, e
		}
		if len(root.Events) >= 3 {
			return true, errNativeRecord
		}
		current.Ended = true
		if current.CallRetireAtMs == 0 {
			current.CallRetireAtMs = now.Add(2 * time.Minute).UnixMilli()
		}
		snapshot, x := nativeProjectSnapshot(current, &nextDialog, nil, nil)
		if x != nil {
			return true, x
		}
		eventID := uuid.NewString()
		eventField := "r:event-" + eventID
		if reinviteOwner != "" && !application {
			if !procedure.Metadata.Cleanup || len(procedure.Metadata.Events) != 0 {
				return true, errNativeRecord
			}
			procedure.Metadata.Events = append(procedure.Metadata.Events, eventField)
			procBytes, x = encodeNativeProcedure(next, nativeProcedureRefs{Origin: originField, LastResponse: responseField}, procedure.Metadata)
			if x != nil {
				return true, x
			}
			changes[0].Value = procBytes
		}
		owners := []string{selected.Record}
		if application && result.Response == responseField {
			owners = append(owners, output.Result)
			slices.Sort(owners)
		}
		event, x := encodeNativeLifecycle(&nativepb.Lifecycle{Kind: pb.DialogEvent_ENDED, Snapshot: snapshot, Message: responseField, Root: rootField, Owners: owners, Selectors: append([]string(nil), selectors...), DeadlineMs: now.Add(2 * time.Minute).UnixMilli(), NextMs: now.UnixMilli()})
		if x != nil {
			return true, x
		}
		root.Events = append(root.Events, eventField)
		slices.Sort(root.Events)
		rootBytes, x := marshalNativeRecord(storagecodec.NativeProcedure, root)
		if x != nil {
			return true, x
		}
		changes = append(changes, rediscore.Change{Field: rootField, Value: rootBytes}, rediscore.Change{Field: eventField, Value: event})
		timers = append(timers, rediscore.TimerChange{Member: "event-" + eventID, Due: now})
	}
	if current.Ended {
		if current.CallRetireAtMs <= 0 {
			return true, errNativeRecord
		}
		timers = append(timers, rediscore.TimerChange{Member: "call-retire", Due: time.UnixMilli(current.CallRetireAtMs)})
	}
	if procedure.Metadata.OutputConsumed && next.Phase == portable.Terminated {
		timers = append(timers, rediscore.TimerChange{Member: "out-" + strings.TrimPrefix(outputField, "r:"), Due: time.UnixMilli(int64(output.Write.Permit.Authorization.DeadlineMs))})
	}
	contextBytes, err := encodeNativeContext(current)
	if err != nil {
		return true, err
	}
	changes = append(changes, rediscore.Change{Field: "r:context", Value: contextBytes})
	publication := rediscore.Publication{Identity: selected.Identity, Revision: view.Revision, Deadline: deadline, SpendReservation: true, Changes: changes, ResultHint: hint}
	if flow.Transport == "udp" {
		worker, e := nativeWorkerReference(flow.Ingress)
		if e != nil {
			return true, e
		}
		publication.UDPHint = &rediscore.UDPHint{Worker: worker, Context: selected.Identity.Context, Incarnation: selected.Identity.Incarnation, Procedure: selected.Record, Stop: facts.Status >= 200}
	}
	if len(timers) > 0 {
		publication.Timers = &rediscore.TimerMutation{Expected: view.Timer, Target: view.Timer, Changes: timers}
	}
	_, err = store.Commit(ctx, publication)
	return true, err
}
