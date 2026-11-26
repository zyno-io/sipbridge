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
	"sipbridge/internal/rediscore"
	"sipbridge/internal/selectedmessage"
)

// completeRedisApplicationInviteResponse applies the exact selected client and
// dialog reducers together; any generated ACK is an independent native output.
func completeRedisApplicationInviteResponse(ctx context.Context, store *rediscore.Store, flow *pb.FlowRef, message portable.Message, key []byte, deadline time.Time, preselected ...*rediscore.SelectorLookup) (bool, error) {
	facts, err := message.Facts()
	if err != nil {
		return false, err
	}
	if facts.IsRequest || facts.CSeqMethod != sip.INVITE {
		return false, nil
	}
	if store == nil || len(key) < 32 || len(key) > 1024 || !time.Now().Before(deadline) || validateRedisNotifyResponseMetadata(message, flow) != nil {
		return true, errNativeRecord
	}
	selectors, err := nativeSelectorIDs(message, false, false)
	if err != nil || len(selectors) == 0 || selectors[0].Kind != rediscore.SelectorClientTransaction {
		return true, errNativeRecord
	}
	var selected *rediscore.SelectorLookup
	if len(preselected) > 1 {
		return true, errNativeRecord
	}
	if len(preselected) == 1 {
		selected = preselected[0]
	}
	if selected == nil {
		selected, err = store.LookupSelector(ctx, selectors[0], "r:context")
		if errors.Is(err, rediscore.ErrAbsent) {
			return false, nil
		}
		if err != nil {
			return true, err
		}
	}
	const prefix = "r:app-invite-"
	if !strings.HasPrefix(selected.Record, prefix) {
		return false, nil
	}
	id := strings.TrimPrefix(selected.Record, prefix)
	if len(id) != 32 {
		return true, errNativeRecord
	}
	originField, responseField, outputField := prefix+"origin-"+id, prefix+"response-"+id, prefix+"output-"+id
	current, err := decodeNativeContext(selected.View.Records["r:context"])
	if err != nil {
		return true, err
	}
	procedure, err := decodeNativeProcedure(selected.View.Records[selected.Record])
	if err != nil {
		return true, err
	}
	if current.Context == nil || current.Context.State == nil || current.Context.Id != selected.Identity.Context || current.Context.State.Incarnation != selected.Identity.Incarnation || current.Kind != pb.ContextSnapshot_UAS && current.Kind != pb.ContextSnapshot_UAC || current.CallId != facts.CallID || !slices.Contains(current.NoninitialProcedures, selected.Record) || procedure.Native.Family != portable.InviteClient || procedure.Refs.Origin != originField || procedure.Metadata.CommandResult == "" || !proto.Equal(procedure.Metadata.Flow, flow) {
		return true, errNativeRecord
	}
	fields := []string{originField, outputField, procedure.Metadata.CommandResult}
	for _, field := range []string{procedure.Refs.LastResponse, procedure.Refs.ACK} {
		if field != "" && !slices.Contains(fields, field) {
			fields = append(fields, field)
		}
	}
	view, err := store.Read(ctx, selected.Identity, fields...)
	if err != nil {
		return true, err
	}
	if view.Revision != selected.View.Revision || view.Timer != selected.View.Timer {
		return true, rediscore.ErrConflict
	}
	output, err := decodeNativeOutput(view.Records[outputField])
	if err != nil || !validReadyApplicationInvite(output, outputField) || output.Procedure != selected.Record || output.Result != procedure.Metadata.CommandResult || !slices.Contains(current.Dialogs, output.Dialog) || !slices.Contains(procedure.Metadata.Outputs, outputField) || !proto.Equal(output.Write.Flow, flow) {
		return true, errNativeRecord
	}
	result, err := decodeNativeResult(view.Records[output.Result])
	if err != nil || result.Sequence == 0 || result.Sequence > current.CommandSequence || output.Result != "r:result-"+strconv.FormatUint(result.Sequence, 10) || result.OperationId != output.Operation || result.Output != outputField || result.DecidingLeaseTerm != output.Write.Permit.Authorization.ApplicationLeaseTerm {
		return true, errNativeRecord
	}
	dialogView, err := store.Read(ctx, selected.Identity, output.Dialog)
	if err != nil {
		return true, err
	}
	if dialogView.Revision != view.Revision || dialogView.Timer != view.Timer {
		return true, rediscore.ErrConflict
	}
	dialog, err := decodeNativeDialog(dialogView.Records[output.Dialog])
	if err != nil || dialog.Metadata.LocalTransaction != procedure.Native.Key || dialog.Refs.Local == nil || dialog.Refs.Local.Request != originField || !slices.Contains(dialog.Metadata.Outputs, outputField) {
		return true, errNativeRecord
	}
	fields = nil
	for _, invite := range []*nativeInviteRefs{dialog.Refs.Local, dialog.Refs.Remote} {
		if invite != nil {
			for _, field := range []string{invite.Request, invite.Final, invite.ACK} {
				if field != "" && len(view.Records[field]) == 0 && !slices.Contains(fields, field) {
					fields = append(fields, field)
				}
			}
		}
	}
	if dialog.Refs.LocalBye != "" && len(view.Records[dialog.Refs.LocalBye]) == 0 && !slices.Contains(fields, dialog.Refs.LocalBye) {
		fields = append(fields, dialog.Refs.LocalBye)
	}
	if len(fields) > 0 {
		other, e := store.Read(ctx, selected.Identity, fields...)
		if e != nil {
			return true, e
		}
		if other.Revision != view.Revision || other.Timer != view.Timer {
			return true, rediscore.ErrConflict
		}
		for field, value := range other.Records {
			view.Records[field] = value
		}
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
	tx, err := procedure.attach(messages, true, procedure.Refs.LastResponse != "", procedure.Refs.ACK != "")
	if err != nil || !portable.MatchResponse(tx.Origin, message) {
		return true, errNativeRecord
	}
	all := nativeInviteSelection{Request: true, Final: true, ACK: true}
	previous, err := dialog.attach(messages, nativeDialogSelection{Local: all, Remote: all, LocalBye: true})
	if err != nil || previous.LocalInvite == nil || !portable.MatchRequest(previous.LocalInvite.Request, tx.Origin) || !portable.MatchDialogResponse(previous, message) {
		return true, errNativeRecord
	}
	now := time.Now()
	next, txEffects, err := portable.StepTransaction(tx, portable.Input{Kind: portable.ReceiveResponse, Now: now.UnixMilli(), Message: message})
	if err != nil {
		return true, err
	}
	var paired redisPairedClientCancel
	for _, effect := range txEffects {
		if effect.Kind == portable.StartTransaction {
			if paired.field != "" {
				return true, errNativeRecord
			}
			parent, e := buildPrivateProcedureRecord(tx, procedure.Refs, procedure.Metadata)
			if e != nil {
				return true, e
			}
			paired, e = prepareRedisPairedClientCancel(ctx, store, selected.Identity, view.Revision, view.Timer, current, selected.Record, parent, next, txEffects, key, deadline)
			if e != nil {
				return true, e
			}
			if paired.field == "" {
				return true, errNativeRecord
			}
		}
	}
	candidate, err := nativeReceiveReinviteResponse(previous, message, now.UnixMilli(), func() string {
		return sip.RFC3261BranchMagicCookie + nativeIdentity(key, "application-reinvite-ack", current.Context.State.Incarnation, selected.Record, result.OperationId)[:32]
	})
	if err != nil {
		return true, err
	}
	if next.LastResponse == nil || !next.LastResponse.EqualWire(message) {
		return true, errNativeRecord
	}
	response, err := selectedmessage.Marshal(message)
	if err != nil || len(response) > redisNotifyResponseRecordLimit {
		return true, errNativeRecord
	}
	procedure.Refs.LastResponse = responseField
	if facts.Status >= 200 {
		dialog.Refs.Local.Final = responseField
	}
	if current.Revision >= 1<<53-1 {
		return true, errNativeRecord
	}
	current.Revision++
	if paired.field != "" {
		current.NoninitialProcedures = append(current.NoninitialProcedures, paired.field)
		slices.Sort(current.NoninitialProcedures)
	}
	if facts.Status >= 200 && facts.Status < 300 {
		body, e := tx.Origin.Body()
		if e != nil {
			return true, e
		}
		remote, e := message.Body()
		if e != nil {
			return true, e
		}
		current.LocalSdp, current.RemoteSdp, current.RemoteTarget = body, remote, candidate.Dialog.Target
	}
	deliveryChanges, err := adoptRegistrationFallback(current, procedure.Metadata, previous, flow, message, facts.Status)
	if err != nil {
		return true, err
	}
	var outputs []rediscore.OutputIntent
	var outputChanges []rediscore.Change
	var timers []rediscore.TimerChange
	for _, effect := range append(txEffects, candidate.ACKEffects...) {
		if effect.Kind != portable.Send {
			continue
		}
		if effect.Message == nil || len(outputs) != 0 {
			return true, errNativeRecord
		}
		ackField := prefix + "tx-ack-" + id
		ackDialog := ""
		if facts.Status >= 200 && facts.Status < 300 {
			ackField, ackDialog = prefix+"dialog-ack-"+id, output.Dialog
			dialog.Refs.Local.ACK = ackField
			dialog.Metadata.AckEmissions++
		} else {
			procedure.Refs.ACK = ackField
			procedure.Metadata.AckEmissions++
		}
		if dialog.Metadata.AckEmissions > stateProfileTransmissions || procedure.Metadata.AckEmissions > stateProfileTransmissions {
			return true, errNativeRecord
		}
		ackOutput := prefix + "ack-output-" + uuid.NewString()
		snapshot, e := nativeProjectSnapshot(current, &candidate.Dialog, nil, nil)
		if e != nil {
			return true, e
		}
		intent, changes, e := prepareRedisUACFailureACK(snapshot, flow, key, *effect.Message, selected.Record, ackDialog, "", ackField, ackOutput, result.OperationId, minTime(deadline, now.Add(2*time.Second)), effect.Replay, ackDialog != "" && (next.Cancelled || selected.Identity.Owner == ""))
		if e != nil {
			return true, e
		}
		outputs, outputChanges = append(outputs, intent), append(outputChanges, changes...)
		procedure.Metadata.Outputs = append(procedure.Metadata.Outputs, ackOutput)
		slices.Sort(procedure.Metadata.Outputs)
		if ackDialog != "" {
			dialog.Metadata.Outputs = append(dialog.Metadata.Outputs, ackOutput)
			slices.Sort(dialog.Metadata.Outputs)
		}
		timers = append(timers, rediscore.TimerChange{Member: "out-" + strings.TrimPrefix(ackOutput, "r:"), Due: intent.Deadline})
	}
	for _, effect := range txEffects {
		if effect.Timer == portable.TimerA {
			continue
		}
		member := "app-invite-" + id + "-" + string(effect.Timer)
		if effect.Kind == portable.Schedule {
			timers = append(timers, rediscore.TimerChange{Member: member, Due: time.UnixMilli(effect.Deadline.At)})
		} else if effect.Kind == portable.CancelTimer {
			timers = append(timers, rediscore.TimerChange{Member: member, Delete: true})
		}
	}
	procData, err := encodeNativeProcedure(next, procedure.Refs, procedure.Metadata)
	if err != nil {
		return true, err
	}
	dialogData, err := encodeNativeDialog(candidate.Dialog, dialog.Refs, dialog.Metadata)
	if err != nil {
		return true, err
	}
	contextData, err := encodeNativeContext(current)
	if err != nil {
		return true, err
	}
	changes := []rediscore.Change{{Field: selected.Record, Value: procData}, {Field: output.Dialog, Value: dialogData}, {Field: "r:context", Value: contextData}, {Field: responseField, Value: response}}
	changes = append(changes, deliveryChanges...)
	changes = append(changes, outputChanges...)
	changes = append(changes, paired.changes...)
	outputs = append(outputs, paired.outputs...)
	timers = append(timers, paired.timers...)
	var hint *rediscore.ResultHint
	if facts.Status >= 200 && result.Certainty != pb.OperationResult_COMPLETED {
		result.Pending, result.Certainty, result.SipStatus, result.Error = false, pb.OperationResult_COMPLETED, uint32(facts.Status), ""
		result.Request, result.Response = originField, responseField
		value, e := encodeNativeResult(result)
		if e != nil {
			return true, e
		}
		changes = append(changes, rediscore.Change{Field: output.Result, Value: value})
		hint = &rediscore.ResultHint{Context: selected.Identity.Context, Incarnation: selected.Identity.Incarnation, Sequence: result.Sequence}
	}
	publication := rediscore.Publication{Identity: selected.Identity, Revision: view.Revision, Deadline: deadline, SpendReservation: true, Changes: changes, NewOutputs: outputs, Selectors: paired.selectors, ResultHint: hint, Timers: &rediscore.TimerMutation{Expected: view.Timer, Target: view.Timer, RequireLease: true, Changes: timers}}
	if flow.Transport == "udp" {
		worker, e := nativeWorkerReference(flow.Ingress)
		if e != nil {
			return true, e
		}
		publication.UDPHint = &rediscore.UDPHint{Worker: worker, Context: selected.Identity.Context, Incarnation: selected.Identity.Incarnation, Procedure: selected.Record, Stop: true}
	}
	_, err = store.Commit(ctx, publication)
	return true, err
}
