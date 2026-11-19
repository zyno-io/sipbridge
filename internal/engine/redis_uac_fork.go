package engine

import (
	"context"
	"errors"
	"fmt"
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

// receiveRedisUACAdditionalSuccess handles an actual late, losing or repeated
// successful fork against the one retained initial transaction anchor.
func receiveRedisUACAdditionalSuccess(ctx context.Context, store *rediscore.Store, identity rediscore.Identity, revision uint64, assignment rediscore.TimerAssignment, current *nativepb.Context, procedureField string, proc nativeProcedureRecord, result *nativepb.Result, flow *pb.FlowRef, message portable.Message, key []byte, executor rediscore.TimerAssignment, deadline time.Time) (bool, error) {
	facts, err := message.Facts()
	if err != nil {
		return true, err
	}
	if facts.IsRequest || facts.Status < 200 || facts.Status >= 300 || facts.ToTag == "" || !facts.HasContact || current.Context == nil || current.Context.State == nil || current.Context.State.Authority == nil {
		return true, errNativeRecord
	}
	if proc.Native.Phase != portable.Calling && proc.Native.Phase != portable.Proceeding && proc.Native.Phase != portable.Accepted && proc.Native.Phase != portable.Terminated && proc.Native.Phase != portable.Completed {
		return true, errNativeRecord
	}
	rootField, root, err := selectRedisUACRoot(ctx, store, identity, revision, assignment, current, procedureField, proc)
	if err != nil {
		return true, err
	}
	selectors, err := nativeSelectorIDs(message, false, false)
	if err != nil || len(selectors) < 2 || selectors[1].Kind != rediscore.SelectorDialog {
		return true, errNativeRecord
	}
	dialogField := "r:uac-dialog-" + nativeIdentity(key, "uac-dialog", current.Context.State.Authority.PrimaryId, current.Context.State.Incarnation, current.Context.Id, facts.FromTag, facts.ToTag)[:32]
	fields := []string{proc.Refs.Origin, dialogField}
	if proc.Refs.LastResponse != "" {
		fields = append(fields, proc.Refs.LastResponse)
	}
	if proc.Refs.ACK != "" {
		fields = append(fields, proc.Refs.ACK)
	}
	fields = append(fields, root.Events...)
	view, err := store.Read(ctx, identity, fields...)
	if errors.Is(err, rediscore.ErrAbsent) {
		return true, rediscore.ErrConflict
	}
	if err != nil {
		return true, err
	}
	if view.Revision != revision || view.Timer != assignment {
		return true, rediscore.ErrConflict
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
	tx, err := proc.attach(messages, true, proc.Refs.LastResponse != "", proc.Refs.ACK != "")
	if err != nil {
		return true, err
	}
	if !portable.MatchResponse(tx.Origin, message) || !proto.Equal(proc.Metadata.Flow, flow) {
		return true, errors.New("UAC fork response mismatch")
	}
	var prior *portable.Dialog
	var dialog nativeDialogRecord
	if data := view.Records[dialogField]; data != nil {
		dialog, err = decodeNativeDialog(data)
		if err != nil {
			return true, err
		}
		if !slices.Contains(current.Dialogs, dialogField) || dialog.Native.Version != portable.Version || dialog.Native.Role != portable.UAC || dialog.Native.CallID != facts.CallID || dialog.Native.LocalTag != facts.FromTag || dialog.Native.RemoteTag != facts.ToTag || dialog.Refs.Local == nil || dialog.Refs.Local.Request != proc.Refs.Origin || !validUACDialogLocalAssociation(dialog.Refs.LocalBye, dialog.Metadata.LocalTransaction, tx.Key) {
			return true, errNativeRecord
		}
		extra := []string{}
		if dialog.Refs.Local.Final != "" && dialog.Refs.Local.Final != proc.Refs.LastResponse {
			extra = append(extra, dialog.Refs.Local.Final)
		}
		if dialog.Refs.Local.ACK != "" && dialog.Refs.Local.ACK != proc.Refs.ACK {
			extra = append(extra, dialog.Refs.Local.ACK)
		}
		if dialog.Refs.LocalBye != "" {
			extra = append(extra, dialog.Refs.LocalBye)
		}
		if dialog.Refs.Remote != nil {
			for _, field := range []string{dialog.Refs.Remote.Request, dialog.Refs.Remote.Final, dialog.Refs.Remote.ACK} {
				if field != "" && len(view.Records[field]) == 0 && !slices.Contains(extra, field) {
					extra = append(extra, field)
				}
			}
		}
		if len(extra) > 0 {
			more, e := store.Read(ctx, identity, extra...)
			if e != nil {
				return true, e
			}
			if more.Revision != revision || more.Timer != assignment {
				return true, rediscore.ErrConflict
			}
			for field, data := range more.Records {
				messages.records[field] = data
			}
		}
		all := nativeInviteSelection{Request: true, Final: true, ACK: true}
		value, e := dialog.attach(messages, nativeDialogSelection{Local: all, Remote: all, LocalBye: dialog.Refs.LocalBye != ""})
		if e != nil {
			return true, e
		}
		prior = &value
	} else {
		if len(current.Dialogs) >= 16 {
			return true, errNativeRecord
		}
		dialog = nativeDialogRecord{Metadata: &nativepb.Dialog{Transaction: tx.Key, LocalTransaction: tx.Key}}
	}
	now := time.Now()
	if ctx.Err() != nil || !now.Before(deadline) {
		return true, rediscore.ErrExpired
	}
	next, effects, err := portable.StepTransaction(tx, portable.Input{Kind: portable.ReceiveResponse, Now: now.UnixMilli(), Message: message})
	if err != nil {
		return true, err
	}
	winner := current.Answered && current.LocalTag == facts.FromTag && current.RemoteTag == facts.ToTag
	cleanup := !winner || current.Ended || tx.Cancelled || identity.Owner == ""
	candidate, err := nativeReceiveInitialInviteDialog(tx.Origin, message, prior, cleanup, now.UnixMilli(), func() string {
		return sip.RFC3261BranchMagicCookie + nativeIdentity(key, "ack", current.Context.State.Incarnation, facts.FromTag+"/"+facts.ToTag, result.OperationId)[:32]
	})
	if err != nil {
		return true, err
	}
	if candidate.Dialog.LocalInvite == nil || candidate.Dialog.LocalInvite.Final == nil {
		return true, fmt.Errorf("fork final: %w", errNativeRecord)
	}
	var changes []rediscore.Change
	var timers []rediscore.TimerChange
	oldHeld := false
	txReplaced := next.LastResponse != nil && next.LastResponse.Equal(message) && (tx.LastResponse == nil || !tx.LastResponse.Equal(message))
	retainedEvents := make([]string, 0, len(root.Events))
	for _, field := range root.Events {
		event, e := decodeNativeLifecycle(view.Records[field])
		if e != nil {
			return true, e
		}
		if event.Root != rootField || event.Message == "" {
			return true, errNativeRecord
		}
		if event.Kind != pb.DialogEvent_PROVISIONAL {
			retainedEvents = append(retainedEvents, field)
			continue
		}
		if len(event.Owners) > 1 || len(event.Owners) == 1 && event.Owners[0] == procedureField && event.Message != proc.Refs.LastResponse {
			return true, errNativeRecord
		}
		changes = append(changes, rediscore.Change{Field: field, Delete: true})
		timers = append(timers, rediscore.TimerChange{Member: strings.TrimPrefix(field, "r:"), Delete: true})
		// A terminated transaction may keep its earlier response. Removing
		// transient delivery debt must not delete that native owner's wire.
		if len(event.Owners) == 0 && event.Message != proc.Refs.LastResponse && result.Request != event.Message && result.Response != event.Message {
			changes = append(changes, rediscore.Change{Field: event.Message, Delete: true})
		}
	}
	root.Events = retainedEvents
	if txReplaced && proc.Refs.LastResponse != "" {
		oldHeld = result.Request == proc.Refs.LastResponse || result.Response == proc.Refs.LastResponse
		oldFacts, e := tx.LastResponse.Facts()
		if e != nil {
			return true, e
		}
		if oldFacts.Status >= 200 && oldFacts.Status < 300 && oldFacts.FromTag != "" && oldFacts.ToTag != "" && oldFacts.HasContact {
			oldDialogField := "r:uac-dialog-" + nativeIdentity(key, "uac-dialog", current.Context.State.Authority.PrimaryId, current.Context.State.Incarnation, current.Context.Id, oldFacts.FromTag, oldFacts.ToTag)[:32]
			if !slices.Contains(current.Dialogs, oldDialogField) {
				return true, errNativeRecord
			}
			oldDialogData := view.Records[oldDialogField]
			if oldDialogData == nil {
				oldView, e := store.Read(ctx, identity, oldDialogField)
				if e != nil {
					return true, e
				}
				if oldView.Revision != revision || oldView.Timer != assignment {
					return true, rediscore.ErrConflict
				}
				oldDialogData = oldView.Records[oldDialogField]
			}
			oldDialog, e := readPrivateDialogRecord(oldDialogData)
			if e != nil {
				return true, e
			}
			if oldDialog.Native.Version != portable.Version || oldDialog.Native.CallId != facts.CallID || oldDialog.LocalInvite == nil || oldDialog.LocalInvite.FinalResponse != proc.Refs.LastResponse {
				return true, errNativeRecord
			}
			oldHeld = true
		}
		for _, field := range root.Events {
			event, e := decodeNativeLifecycle(view.Records[field])
			if e != nil {
				return true, e
			}
			if event.Root != rootField || event.Message == "" {
				return true, errNativeRecord
			}
			if event.Message == proc.Refs.LastResponse {
				index, ok := slices.BinarySearch(event.Owners, procedureField)
				if !ok {
					return true, errNativeRecord
				}
				event.Owners = slices.Delete(event.Owners, index, index+1)
				data, e := encodeNativeLifecycle(event)
				if e != nil {
					return true, e
				}
				changes = append(changes, rediscore.Change{Field: field, Value: data})
				oldHeld = true
			}
		}
		if !oldHeld {
			changes = append(changes, rediscore.Change{Field: proc.Refs.LastResponse, Delete: true})
		}
	}
	responseField := ""
	if prior != nil && dialog.Refs.Local.Final != "" && prior.LocalInvite != nil && prior.LocalInvite.Final != nil && prior.LocalInvite.Final.Equal(message) {
		responseField = dialog.Refs.Local.Final
	}
	if responseField == "" && tx.LastResponse != nil && tx.LastResponse.Equal(message) {
		responseField = proc.Refs.LastResponse
	}
	if responseField == "" {
		responseField = "r:uac-response-" + uuid.NewString()
		wire, e := selectedmessage.Marshal(message)
		if e != nil {
			return true, e
		}
		changes = append(changes, rediscore.Change{Field: responseField, Value: wire})
	}
	if txReplaced {
		proc.Refs.LastResponse = responseField
	} else if next.LastResponse != nil && (tx.LastResponse == nil || !next.LastResponse.Equal(*tx.LastResponse)) {
		return true, errNativeRecord
	}
	if current.Revision >= 1<<53-1 {
		return true, errNativeRecord
	}
	current.Revision++
	for _, effect := range effects {
		switch effect.Kind {
		case portable.Schedule, portable.CancelTimer:
			if effect.Timer == portable.TimerA {
				continue
			}
			change := rediscore.TimerChange{Member: "call-tx-" + strings.TrimPrefix(procedureField, "r:uac-procedure-") + "-" + string(effect.Timer), Delete: effect.Kind == portable.CancelTimer}
			if !change.Delete {
				change.Due = time.UnixMilli(effect.Deadline.At)
			}
			timers = append(timers, change)
		case portable.ReportResponse, portable.Ended:
		default:
			return true, errors.New("unexpected UAC transaction effect")
		}
	}
	for _, effect := range candidate.DialogEffects {
		switch effect.Kind {
		case portable.Schedule, portable.CancelTimer:
			change := rediscore.TimerChange{Member: callDialogTimerMember(dialogField, effect.Timer), Delete: effect.Kind == portable.CancelTimer}
			if !change.Delete {
				change.Due = time.UnixMilli(effect.Deadline.At)
			}
			found := false
			for _, prior := range timers {
				if prior.Member == change.Member {
					if prior.Delete != change.Delete || !prior.Due.Equal(change.Due) {
						return true, errNativeRecord
					}
					found = true
					break
				}
			}
			if !found {
				timers = append(timers, change)
			}
		case portable.ReportResponse, portable.NeedACK, portable.NeedBYE, portable.Send, portable.Ended, portable.Acknowledged:
		default:
			return true, errors.New("unexpected UAC dialog effect")
		}
	}

	refs := nativeDialogRefs{Local: &nativeInviteRefs{Request: proc.Refs.Origin, Final: responseField}, Remote: dialog.Refs.Remote}
	if dialog.Refs.Local != nil {
		refs.Local.ACK = dialog.Refs.Local.ACK
		refs.LocalBye = dialog.Refs.LocalBye
	}
	if candidate.Dialog.LocalInvite.ACK != nil && refs.Local.ACK == "" {
		refs.Local.ACK = "r:uac-dialog-ack-" + strings.TrimPrefix(dialogField, "r:uac-dialog-")
	}
	var emitted *portable.Message
	replay := false
	for _, effect := range candidate.ACKEffects {
		switch effect.Kind {
		case portable.Send:
			if effect.Message == nil || emitted != nil {
				return true, errNativeRecord
			}
			copy := *effect.Message
			emitted = &copy
			replay = effect.Replay
		case portable.NeedBYE:
			// This is an obligation only. Physical cleanup is staged after
			// the exact ACK obtains positive WRITTEN proof.
			if !cleanup {
				return true, errNativeRecord
			}
		default:
			return true, errors.New("unexpected fork ACK effect")
		}
	}
	var outputs []rediscore.OutputIntent
	if emitted != nil {
		pending := false
		if replay && dialog.Metadata.AckEmissions < stateProfileTransmissions {
			release, closeTimers, blocked, e := planRedisConsumedACKReplayOutputs(ctx, store, identity, revision, assignment, current, procedureField, proc, dialogField, redisACKReplayDialogFromRecord(dialog), *emitted, refs.Local.ACK, flow, result)
			if e != nil {
				return true, e
			}
			changes = append(changes, release...)
			timers = append(timers, closeTimers...)
			pending = blocked
		} else {
			for _, field := range dialog.Metadata.Outputs {
				if strings.HasPrefix(field, "r:uac-dialog-ack-output-") || strings.HasPrefix(field, "r:app-ack-output-") {
					pending = true
					break
				}
			}
		}
		if !pending && dialog.Metadata.AckEmissions < stateProfileTransmissions {
			snapshot, e := nativeProjectSnapshot(current, nil, nil, nil)
			if e != nil {
				return true, e
			}
			outputField := "r:uac-dialog-ack-output-" + uuid.NewString()
			until := minTime(deadline, now.Add(2*time.Second))
			resultField := ""
			if winner {
				resultField = "r:result-1"
			}
			intent, outChanges, e := prepareRedisUACFailureACK(snapshot, flow, key, *emitted, procedureField, dialogField, resultField, refs.Local.ACK, outputField, result.OperationId, until, replay, cleanup)
			if e != nil {
				return true, fmt.Errorf("fork ACK output: %w", e)
			}
			changes = append(changes, outChanges...)
			outputs = append(outputs, intent)
			proc.Metadata.Outputs = append(proc.Metadata.Outputs, outputField)
			slices.Sort(proc.Metadata.Outputs)
			dialog.Metadata.Outputs = append(dialog.Metadata.Outputs, outputField)
			slices.Sort(dialog.Metadata.Outputs)
			dialog.Metadata.AckEmissions++
			timers = append(timers, rediscore.TimerChange{Member: "out-" + strings.TrimPrefix(outputField, "r:"), Due: until})
		}
	}
	var credit int64
	if prior == nil {
		if len(current.Dialogs) > 0 {
			credit = redisAdditionalForkCredit
			root.CreditContribution += uint64(credit)
		}
		current.Dialogs = append(current.Dialogs, dialogField)
		slices.Sort(current.Dialogs)
	}
	dialogData, err := encodeNativeDialog(candidate.Dialog, refs, dialog.Metadata)
	if err != nil {
		return true, fmt.Errorf("fork dialog encode: %w", err)
	}
	procData, err := encodeNativeProcedure(next, proc.Refs, proc.Metadata)
	if err != nil {
		return true, fmt.Errorf("fork procedure encode: %w", err)
	}
	contextData, err := encodeNativeContext(current)
	if err != nil {
		return true, fmt.Errorf("fork context encode: %w", err)
	}
	changes = append(changes, rediscore.Change{Field: dialogField, Value: dialogData}, rediscore.Change{Field: procedureField, Value: procData}, rediscore.Change{Field: "r:context", Value: contextData})
	if rootField != procedureField {
		data, e := marshalNativeRecord(storagecodec.NativeProcedure, root)
		if e != nil {
			return true, e
		}
		changes = append(changes, rediscore.Change{Field: rootField, Value: data})
	}
	publication := rediscore.Publication{Identity: identity, Revision: revision, Deadline: deadline, CreditDelta: credit, SpendReservation: true, Changes: changes, NewOutputs: outputs}
	if prior == nil {
		publication.Selectors = []rediscore.SelectorChange{{Selector: selectors[1], Record: dialogField}}
	}
	if len(timers) > 0 {
		target := assignment
		if target == (rediscore.TimerAssignment{}) {
			target = executor
		}
		publication.Timers = &rediscore.TimerMutation{Expected: assignment, Target: target, Changes: timers}
	}
	if flow.Transport == "udp" {
		worker, e := nativeWorkerReference(flow.Ingress)
		if e != nil {
			return true, e
		}
		publication.UDPHint = &rediscore.UDPHint{Worker: worker, Context: identity.Context, Incarnation: identity.Incarnation, Procedure: procedureField, Stop: true}
	}
	_, err = store.Commit(ctx, publication)
	if errors.Is(err, rediscore.ErrExpired) && publication.Timers != nil && assignment != (rediscore.TimerAssignment{}) && assignment != executor && time.Now().Before(deadline) && ctx.Err() == nil {
		publication.Timers.Target = executor
		_, err = store.Commit(ctx, publication)
	}
	return true, err
}

// This operation-local view keeps native scalars and wire references separate
// from the metadata whose owner lists the eventual replay CAS will update.
type redisACKReplayDialog struct {
	metadata                                                *nativepb.Dialog
	local                                                   nativeInviteRefs
	localBye, callID, localTag, remoteTag, transport, phase string
	sequence                                                uint32
	state                                                   portable.InviteState
	initial                                                 bool
}

func redisACKReplayDialogFromRecord(record nativeDialogRecord) *redisACKReplayDialog {
	if record.Native.LocalInvite == nil || record.Refs.Local == nil {
		return nil
	}
	invite := record.Native.LocalInvite
	return &redisACKReplayDialog{metadata: record.Metadata, local: *record.Refs.Local, localBye: record.Refs.LocalBye, callID: record.Native.CallID, localTag: record.Native.LocalTag, remoteTag: record.Native.RemoteTag, transport: record.Native.Transport, phase: string(record.Native.Phase), sequence: invite.Sequence, state: invite.State, initial: invite.Initial}
}

func redisACKReplayDialogFromPrivate(record *nativepb.Dialog) *redisACKReplayDialog {
	if record.Native == nil || record.Native.LocalInvite == nil || record.LocalInvite == nil {
		return nil
	}
	invite := record.Native.LocalInvite
	return &redisACKReplayDialog{metadata: record, local: nativeInviteRefs{Request: record.LocalInvite.Request, Final: record.LocalInvite.FinalResponse, ACK: record.LocalInvite.Ack}, localBye: record.LocalBye, callID: record.Native.CallId, localTag: record.Native.LocalTag, remoteTag: record.Native.RemoteTag, transport: record.Native.Transport, phase: record.Native.Phase, sequence: invite.Sequence, state: portable.InviteState(invite.State), initial: invite.Initial}
}

// planRedisConsumedACKReplayOutputs releases only consumed physical metadata
// for an already positively written cached ACK. The caller joins these changes
// with its actual SDK-selected replay; no wire/native/result owner is released.
func planRedisConsumedACKReplayOutputs(ctx context.Context, store *rediscore.Store, identity rediscore.Identity, revision uint64, assignment rediscore.TimerAssignment, current *nativepb.Context, procedureField string, procedure nativeProcedureRecord, dialogField string, dialog *redisACKReplayDialog, ack portable.Message, ackField string, flow *pb.FlowRef, commandResult *nativepb.Result) ([]rediscore.Change, []rediscore.TimerChange, bool, error) {
	isACKOutput := func(field string) bool {
		return strings.HasPrefix(field, "r:uac-dialog-ack-output-") || strings.HasPrefix(field, "r:app-invite-ack-output-") || strings.HasPrefix(field, "r:app-ack-output-") || strings.HasPrefix(field, "r:retained-ack-output-")
	}
	var fields []string
	listed := procedure.Metadata.Outputs
	if dialog != nil {
		listed = dialog.metadata.Outputs
	}
	for _, field := range listed {
		if isACKOutput(field) && (dialog != nil || strings.HasPrefix(field, "r:retained-ack-output-")) {
			fields = append(fields, field)
		}
	}
	if len(fields) == 0 {
		return nil, nil, false, nil
	}
	facts, err := ack.Facts()
	if err != nil || !facts.IsRequest || facts.Method != sip.ACK || facts.CallID != current.CallId || ackField == "" || !validPolicyFlow(flow) || ack.Transport() != flow.Transport || procedure.Native.Version != portable.Version || procedure.Native.Family != portable.InviteClient || slices.Contains(current.InitialProcedures, procedureField) == slices.Contains(current.NoninitialProcedures, procedureField) {
		return nil, nil, false, errNativeRecord
	}
	if dialog != nil {
		if !dialog.metadata.AckWritten {
			return nil, nil, true, nil
		}
		if !slices.Contains(current.Dialogs, dialogField) || dialog.callID != facts.CallID || dialog.localTag != facts.FromTag || dialog.remoteTag != facts.ToTag || dialog.state != portable.InviteAcked || dialog.sequence != facts.CSeq || dialog.local.Request != procedure.Refs.Origin || dialog.local.Final == "" || dialog.local.ACK != ackField || dialog.metadata.AckEmissions == 0 || !proto.Equal(dialog.metadata.CompletionFlow, flow) {
			return nil, nil, false, errNativeRecord
		}
	} else if dialogField != "" || procedure.Metadata.SuccessfulAck == nil || !validNativeSuccessfulACK(procedure.Metadata.SuccessfulAck) || procedure.Metadata.SuccessfulAck.Ack != ackField || !proto.Equal(procedure.Metadata.SuccessfulAck.Flow, flow) {
		return nil, nil, false, errNativeRecord
	}
	view, err := store.Read(ctx, identity, fields...)
	if err != nil {
		return nil, nil, false, err
	}
	if view.Revision != revision || view.Timer != assignment {
		return nil, nil, false, rediscore.ErrConflict
	}
	outputs := make([]*nativepb.Output, len(fields))
	var receiptFields []string
	for index, field := range fields {
		output, e := decodeNativeOutput(view.Records[field])
		if e != nil {
			return nil, nil, false, e
		}
		if output.Method != "ACK" || output.Status != 0 || output.AckSequence != facts.CSeq || output.Procedure != procedureField || output.Dialog != dialogField || !slices.Contains(procedure.Metadata.Outputs, field) || !proto.Equal(output.Write.Effect.Context, current.Context) || output.Write.Permit.Authorization.AuthorityClass != pb.EffectAuthorization_NATIVE_COMPLETION || output.Write.Permit.Authorization.DeadlineMs == 0 || output.Write.Permit.Authorization.DeadlineMs > 1<<53-1 || !proto.Equal(output.Write.Flow, flow) {
			return nil, nil, false, errNativeRecord
		}
		switch {
		case strings.HasPrefix(field, "r:uac-dialog-ack-output-"):
			id := strings.TrimPrefix(procedureField, "r:uac-procedure-")
			attempt := strings.TrimPrefix(field, "r:uac-dialog-ack-output-")
			parsed, x := uuid.Parse(attempt)
			if dialog == nil || current.Kind != pb.ContextSnapshot_UAC || procedureField != "r:uac-procedure-"+id || len(id) != 32 || procedure.Refs.Origin != "r:uac-origin-"+id || procedure.Metadata.CommandResult != "r:result-1" || !dialog.initial || !strings.HasPrefix(dialogField, "r:uac-dialog-") || len(strings.TrimPrefix(dialogField, "r:uac-dialog-")) != 32 || !validRedisUACDialogACKWire(strings.TrimPrefix(dialogField, "r:uac-dialog-"), ackField) || x != nil || parsed.String() != attempt || output.Result != "" && output.Result != "r:result-1" || output.Result == "" && !output.CleanupAfterAck || commandResult == nil || commandResult.Sequence != 1 || output.Operation != commandResult.OperationId {
				return nil, nil, false, errNativeRecord
			}
		case strings.HasPrefix(field, "r:app-invite-ack-output-"):
			id := strings.TrimPrefix(procedureField, "r:app-invite-")
			if dialog == nil || !validReadyApplicationInviteACK(output, field) || !slices.Contains(current.NoninitialProcedures, procedureField) || procedure.Refs.Origin != "r:app-invite-origin-"+id || !validRedisApplicationInviteDialogACKWire(id, ackField) || commandResult == nil || output.Operation != commandResult.OperationId {
				return nil, nil, false, errNativeRecord
			}
		case strings.HasPrefix(field, "r:app-ack-output-"):
			id := strings.TrimPrefix(field, "r:app-ack-output-")
			if dialog == nil || !validReadyApplicationACK(output, field) || output.Result != dialog.metadata.AckResult || output.Result == procedure.Metadata.CommandResult || ackField != "r:app-ack-wire-"+id {
				return nil, nil, false, errNativeRecord
			}
			receiptFields = append(receiptFields, output.Result)
		case strings.HasPrefix(field, "r:retained-ack-output-"):
			if dialog != nil || !validReadyRetainedACK(output, field) {
				return nil, nil, false, errNativeRecord
			}
		default:
			return nil, nil, false, errNativeRecord
		}
		if dialog != nil {
			if e = validateRedisInviteACKLocalAssociation(ctx, store, identity, revision, assignment, current, dialog.callID, dialog.localTag, dialog.remoteTag, dialog.transport, dialog.phase, dialog.localBye, dialog.metadata.LocalTransaction, procedure.Native.Key, output.CleanupAfterAck); e != nil {
				return nil, nil, false, e
			}
		}
		outputs[index] = output
	}
	receipts := make(map[string]*nativepb.Result, len(receiptFields))
	if len(receiptFields) != 0 {
		selected, e := store.Read(ctx, identity, receiptFields...)
		if e != nil {
			return nil, nil, false, e
		}
		if selected.Revision != revision || selected.Timer != assignment {
			return nil, nil, false, rediscore.ErrConflict
		}
		for index, field := range fields {
			output := outputs[index]
			if !strings.HasPrefix(field, "r:app-ack-output-") {
				continue
			}
			receipt, e := decodeNativeResult(selected.Records[output.Result])
			if e != nil {
				return nil, nil, false, e
			}
			id, e := redisApplicationByeID(receipt.Fingerprint)
			if e != nil || field != "r:app-ack-output-"+id || receipt.Sequence == 0 || receipt.Sequence > current.CommandSequence || output.Result != "r:result-"+strconv.FormatUint(receipt.Sequence, 10) || receipt.Pending || receipt.Output != field || receipt.OperationId != output.Operation || receipt.DecidingLeaseTerm != output.Write.Permit.Authorization.ApplicationLeaseTerm || receipt.Request != "" && receipt.Request != ackField {
				return nil, nil, false, errNativeRecord
			}
			receipts[output.Result] = receipt
		}
	}
	// A present proof remains independently owned even if this cached wire had
	// an earlier W. In particular, that W cannot consume a newer U attempt.
	for _, output := range outputs {
		ref, e := nativeOutputRef(output.Write)
		if e != nil {
			return nil, nil, false, e
		}
		if _, e = store.InspectOutput(ctx, ref); e == nil {
			return nil, nil, true, nil
		} else if !errors.Is(e, rediscore.ErrAbsent) {
			return nil, nil, false, e
		}
	}
	var changes []rediscore.Change
	var timers []rediscore.TimerChange
	for field, receipt := range receipts {
		receipt.Output = ""
		data, e := encodeNativeResult(receipt)
		if e != nil {
			return nil, nil, false, e
		}
		changes = append(changes, rediscore.Change{Field: field, Value: data})
		if current.CommandSequence > receipt.Sequence {
			timers = append(timers, rediscore.TimerChange{Member: "orig-result-" + strconv.FormatUint(receipt.Sequence, 10), Due: time.UnixMilli(receipt.CutoffMs)})
		}
	}
	for _, field := range fields {
		index, ok := slices.BinarySearch(procedure.Metadata.Outputs, field)
		if !ok {
			return nil, nil, false, errNativeRecord
		}
		procedure.Metadata.Outputs = slices.Delete(procedure.Metadata.Outputs, index, index+1)
		if dialog != nil {
			index, ok = slices.BinarySearch(dialog.metadata.Outputs, field)
			if !ok {
				return nil, nil, false, errNativeRecord
			}
			dialog.metadata.Outputs = slices.Delete(dialog.metadata.Outputs, index, index+1)
		}
		changes = append(changes, rediscore.Change{Field: field, Delete: true})
		timers = append(timers, rediscore.TimerChange{Member: "out-" + strings.TrimPrefix(field, "r:"), Delete: true})
	}
	return changes, timers, false, nil
}
