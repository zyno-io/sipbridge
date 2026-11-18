package engine

import (
	"context"
	"errors"
	"fmt"
	"net"
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

// receiveRedisUACProvisional qualifies provisional and initial failure
// responses after any eligible digest challenge has been handled separately.
func receiveRedisUACProvisional(ctx context.Context, store *rediscore.Store, flow *pb.FlowRef, message portable.Message, key []byte, executor rediscore.TimerAssignment, deadline time.Time, preselected ...*rediscore.SelectorLookup) (bool, error) {
	facts, err := message.Facts()
	if err != nil {
		return false, err
	}
	if facts.IsRequest || facts.CSeqMethod != sip.INVITE {
		return false, nil
	}
	success := facts.Status >= 200 && facts.Status < 300
	failure := facts.Status >= 300 && facts.Status <= 699
	if facts.Status < 100 || facts.Status >= 200 && !failure && !success {
		return true, errors.New("UAC response path not implemented")
	}
	if success && (facts.ToTag == "" || !facts.HasContact) {
		return true, errors.New("successful INVITE requires remote tag and Contact")
	}
	if store == nil || len(key) < 32 || len(key) > 1024 || !validPolicyFlow(flow) || message.Transport() != flow.Transport || message.Source() != net.JoinHostPort(flow.SourceIp, strconv.Itoa(int(flow.SourcePort))) || !time.Now().Before(deadline) {
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
		selected, err = store.LookupSelector(ctx, selectors[0], "r:context", "r:result-1")
		if errors.Is(err, rediscore.ErrAbsent) {
			return false, nil
		}
		if err != nil {
			return true, err
		}
	}
	if !strings.HasPrefix(selected.Record, "r:uac-procedure-") {
		return true, errNativeRecord
	}
	id := strings.TrimPrefix(selected.Record, "r:uac-procedure-")
	if len(id) != 32 {
		return true, errNativeRecord
	}
	current, err := decodeNativeContext(selected.View.Records["r:context"])
	if err != nil {
		return true, err
	}
	proc, err := decodeNativeProcedure(selected.View.Records[selected.Record])
	if err != nil {
		return true, err
	}
	result, err := decodeNativeResult(selected.View.Records["r:result-1"])
	if err != nil {
		return true, err
	}
	if current.Kind != pb.ContextSnapshot_UAC || current.Context == nil || current.Context.State == nil || current.Context.Id != selected.Identity.Context || current.Context.State.Incarnation != selected.Identity.Incarnation || current.CallId != facts.CallID || !slices.Contains(current.InitialProcedures, selected.Record) || proc.Native.Family != portable.InviteClient || proc.Metadata.CommandResult != "r:result-1" || result.Sequence != 1 || proc.Refs.Origin != "r:uac-origin-"+id || !proto.Equal(proc.Metadata.Flow, flow) {
		return true, errNativeRecord
	}
	rootField, root, err := selectRedisUACRoot(ctx, store, selected.Identity, selected.View.Revision, selected.View.Timer, current, selected.Record, proc)
	if err != nil {
		return true, err
	}
	if success && (current.Answered || current.Ended || proc.Native.Cancelled || selected.Identity.Owner == "" || proc.Native.Phase == portable.Accepted || proc.Native.Phase == portable.Terminated || proc.Native.Phase == portable.Completed) {
		return receiveRedisUACAdditionalSuccess(ctx, store, selected.Identity, selected.View.Revision, selected.View.Timer, current, selected.Record, proc, result, flow, message, key, executor, deadline)
	}
	if failure && (proc.Native.Phase == portable.Completed || proc.Native.Phase == portable.Terminated) {
		return replayRedisUACFailure(ctx, store, selected.Identity, selected.View.Revision, selected.View.Timer, current, selected.Record, proc, result.OperationId, message, flow, key, executor, deadline)
	}
	if current.Ended && !failure || success && (current.Answered || proc.Native.Cancelled || selected.Identity.Owner == "") || proc.Native.Phase != portable.Calling && proc.Native.Phase != portable.Proceeding {
		return true, errNativeRecord
	}
	fields := []string{proc.Refs.Origin}
	if proc.Refs.LastResponse != "" {
		fields = append(fields, proc.Refs.LastResponse)
	}
	fields = append(fields, root.Events...)
	dialogField := ""
	if !failure && facts.Status > 100 && facts.ToTag != "" && facts.HasContact {
		authority := current.Context.State.Authority
		if authority == nil {
			return true, errNativeRecord
		}
		dialogField = "r:uac-dialog-" + nativeIdentity(key, "uac-dialog", authority.PrimaryId, current.Context.State.Incarnation, current.Context.Id, facts.FromTag, facts.ToTag)[:32]
		fields = append(fields, dialogField)
	}
	view, err := store.Read(ctx, selected.Identity, fields...)
	if errors.Is(err, rediscore.ErrAbsent) {
		return true, rediscore.ErrConflict
	}
	if err != nil {
		return true, err
	}
	if view.Revision != selected.View.Revision || view.Timer != selected.View.Timer {
		return true, rediscore.ErrConflict
	}
	memo, err := selectedmessage.NewMemo(3)
	if err != nil {
		return true, err
	}
	message, err = memo.Adopt(message, "admitted")
	if err != nil {
		return true, err
	}
	messages := &nativeSelectedMessages{records: view.Records, memo: memo, provenance: "admitted"}
	tx, err := proc.attach(messages, true, proc.Refs.LastResponse != "", false)
	if err != nil {
		return true, err
	}
	if !portable.MatchResponse(tx.Origin, message) {
		return true, errors.New("UAC response mismatch")
	}
	now := time.Now()
	if ctx.Err() != nil || !now.Before(deadline) {
		return true, rediscore.ErrExpired
	}
	var next portable.Transaction
	var effects []portable.Effect
	var ack *portable.Message
	if failure {
		candidate, e := nativeReceiveInitialInviteFailure(tx, message, now.UnixMilli())
		if e != nil {
			return true, e
		}
		if candidate.ACK == nil || candidate.Replay || proc.Metadata.AckEmissions >= stateProfileTransmissions || proc.Refs.ACK != "" {
			return true, errNativeRecord
		}
		next, effects, ack = candidate.Transaction, candidate.Effects, candidate.ACK
	} else {
		next, effects, err = portable.StepTransaction(tx, portable.Input{Kind: portable.ReceiveResponse, Now: now.UnixMilli(), Message: message})
		if err != nil {
			return true, err
		}
	}
	if next.LastResponse == nil || !next.LastResponse.EqualWire(message) {
		return true, errNativeRecord
	}
	var paired redisPairedClientCancel
	for _, effect := range effects {
		if effect.Kind == portable.StartTransaction {
			if paired.field != "" {
				return true, errNativeRecord
			}
			parent, e := buildPrivateProcedureRecord(tx, proc.Refs, proc.Metadata)
			if e != nil {
				return true, e
			}
			paired, e = prepareRedisPairedClientCancel(ctx, store, selected.Identity, view.Revision, view.Timer, current, selected.Record, parent, next, effects, key, deadline)
			if e != nil || paired.field == "" {
				return true, errNativeRecord
			}
		}
	}
	var timers []rediscore.TimerChange
	for _, effect := range effects {
		switch effect.Kind {
		case portable.CancelTimer:
			if effect.Timer != portable.TimerA {
				timers = append(timers, rediscore.TimerChange{Member: "call-tx-" + id + "-" + string(effect.Timer), Delete: true})
			}
		case portable.Schedule:
			if effect.Timer != portable.TimerA {
				timers = append(timers, rediscore.TimerChange{Member: "call-tx-" + id + "-" + string(effect.Timer), Due: time.UnixMilli(effect.Deadline.At)})
			}
		case portable.ReportResponse, portable.Ended, portable.StartTransaction:
		case portable.Send:
			if !failure || effect.Message == nil || ack == nil || !effect.Message.Equal(*ack) {
				return true, errNativeRecord
			}
		default:
			return true, errors.New("unsupported provisional INVITE effect")
		}
	}
	reuse := tx.LastResponse != nil && tx.LastResponse.Equal(message)
	var changes []rediscore.Change
	hasProvisional := false
	oldHeld := false
	retainedEvents := make([]string, 0, len(root.Events))
	for _, field := range root.Events {
		event, e := decodeNativeLifecycle(view.Records[field])
		if e != nil {
			return true, e
		}
		if event.Root != rootField || event.Message == "" {
			return true, errNativeRecord
		}
		if event.Kind == pb.DialogEvent_PROVISIONAL {
			if hasProvisional {
				return true, errNativeRecord
			}
			hasProvisional = true
			if failure || success {
				// A final supersedes transient ringing history. The only
				// possible other owner is this initial transaction; its old
				// response is replaced below in the same publication.
				if len(event.Owners) > 1 || len(event.Owners) == 1 && event.Owners[0] == selected.Record && event.Message != proc.Refs.LastResponse {
					return true, errNativeRecord
				}
				changes = append(changes, rediscore.Change{Field: field, Delete: true})
				timers = append(timers, rediscore.TimerChange{Member: "event-" + strings.TrimPrefix(field, "r:event-"), Delete: true})
				if len(event.Owners) == 0 && event.Message != proc.Refs.LastResponse {
					changes = append(changes, rediscore.Change{Field: event.Message, Delete: true})
				}
				continue
			}
		}
		retainedEvents = append(retainedEvents, field)
		if event.Message == proc.Refs.LastResponse && proc.Refs.LastResponse != "" {
			index, ok := slices.BinarySearch(event.Owners, selected.Record)
			if !ok {
				return true, errNativeRecord
			}
			if !reuse {
				event.Owners = slices.Delete(event.Owners, index, index+1)
				data, e := encodeNativeLifecycle(event)
				if e != nil {
					return true, e
				}
				changes = append(changes, rediscore.Change{Field: field, Value: data})
				oldHeld = true
			}
		}
	}
	root.Events = retainedEvents
	responseField := proc.Refs.LastResponse
	if !reuse {
		if proc.Refs.LastResponse != "" && !oldHeld {
			changes = append(changes, rediscore.Change{Field: proc.Refs.LastResponse, Delete: true})
		}
		responseField = "r:uac-response-" + uuid.NewString()
		wire, e := selectedmessage.Marshal(message)
		if e != nil {
			return true, e
		}
		changes = append(changes, rediscore.Change{Field: responseField, Value: wire})
	}
	proc.Refs.LastResponse = responseField
	if current.Revision >= 1<<53-1 {
		return true, errNativeRecord
	}
	wasEnded := current.Ended
	current.Revision++
	if paired.field != "" {
		current.NoninitialProcedures = append(current.NoninitialProcedures, paired.field)
		slices.Sort(current.NoninitialProcedures)
	}
	if failure && !wasEnded {
		current.Ended = true
		if current.CallRetireAtMs == 0 {
			current.CallRetireAtMs = now.Add(2 * time.Minute).UnixMilli()
		}
		timers = append(timers, rediscore.TimerChange{Member: "call-retire", Due: time.UnixMilli(current.CallRetireAtMs)})
	}
	selectorChanges := append([]rediscore.SelectorChange(nil), paired.selectors...)
	var successACK *portable.Message
	var successDialog *nativepb.Dialog
	var successDialogRefs nativeDialogRefs
	var successDialogNative portable.Dialog
	if dialogField != "" {
		existing := view.Records[dialogField]
		var previous *portable.Dialog
		if existing == nil {
			if len(current.Dialogs) >= 16 || len(selectors) < 2 || selectors[1].Kind != rediscore.SelectorDialog {
				return true, errNativeRecord
			}
		} else {
			dialog, e := decodeNativeDialog(existing)
			if e != nil {
				return true, e
			}
			if !slices.Contains(current.Dialogs, dialogField) || dialog.Native.Version != portable.Version || dialog.Native.Role != portable.UAC || dialog.Native.CallID != facts.CallID || dialog.Native.LocalTag != facts.FromTag || dialog.Native.RemoteTag != facts.ToTag || dialog.Refs.Local == nil || dialog.Refs.Local.Request != proc.Refs.Origin || dialog.Refs.Local.Final != "" || dialog.Refs.Local.ACK != "" || dialog.Metadata.Transaction != tx.Key || dialog.Metadata.LocalTransaction != tx.Key {
				return true, errNativeRecord
			}
			attached, e := dialog.attach(messages, nativeDialogSelection{Local: nativeInviteSelection{Request: true}})
			if e != nil {
				return true, e
			}
			previous = &attached
			successDialog = dialog.Metadata
		}
		candidate, e := nativeReceiveInitialInviteDialog(tx.Origin, message, previous, false, now.UnixMilli(), func() string {
			return sip.RFC3261BranchMagicCookie + nativeIdentity(key, "ack", current.Context.State.Incarnation, facts.FromTag+"/"+facts.ToTag, result.OperationId)[:32]
		})
		if e != nil {
			return true, e
		}
		if existing == nil {
			current.Dialogs = append(current.Dialogs, dialogField)
			slices.Sort(current.Dialogs)
			selectorChanges = append(selectorChanges, rediscore.SelectorChange{Selector: selectors[1], Record: dialogField})
			successDialog = &nativepb.Dialog{Transaction: tx.Key, LocalTransaction: tx.Key}
		}
		successDialogNative = candidate.Dialog
		successDialogRefs = nativeDialogRefs{Local: &nativeInviteRefs{Request: proc.Refs.Origin}}
		if success {
			successDialogRefs.Local.Final = responseField
			for _, effect := range candidate.ACKEffects {
				if effect.Kind != portable.Send || effect.Message == nil || successACK != nil {
					return true, errNativeRecord
				}
				a := *effect.Message
				successACK = &a
			}
			if successACK == nil && candidate.Dialog.Offer != portable.RemoteOfferInACK {
				return true, errNativeRecord
			}
			for _, effect := range append(candidate.DialogEffects, candidate.ACKEffects...) {
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
				case portable.NeedACK, portable.Send, portable.Ended, portable.Acknowledged, portable.ReportResponse:
				default:
					return true, fmt.Errorf("unsupported initial INVITE dialog effect %s", effect.Kind)
				}
			}
			body, e := message.Body()
			if e != nil {
				return true, e
			}
			current.Answered = true
			current.LocalTag = candidate.Dialog.LocalTag
			current.RemoteTag = candidate.Dialog.RemoteTag
			current.RemoteTarget = candidate.Dialog.Target
			current.RemoteSdp = body
		} else if len(candidate.DialogEffects) != 0 || len(candidate.ACKEffects) != 0 {
			return true, errNativeRecord
		}
		if !success {
			if existing == nil {
				dialogData, e := encodeNativeDialog(candidate.Dialog, successDialogRefs, successDialog)
				if e != nil {
					return true, e
				}
				changes = append(changes, rediscore.Change{Field: dialogField, Value: dialogData})
			}
		}
	}

	if failure && !wasEnded || success || !failure && !hasProvisional {
		publicSelectors, e := frozenMessageSelectors(message, facts, false, false)
		if e != nil {
			return true, e
		}
		snapshot, e := nativeProjectSnapshot(current, nil, nil, nil)
		if e != nil {
			return true, e
		}
		eventID := uuid.NewString()
		field := "r:event-" + eventID
		kind := pb.DialogEvent_PROVISIONAL
		if failure {
			kind = pb.DialogEvent_FAILED
		}
		if success {
			kind = pb.DialogEvent_ANSWERED
		}
		owners := []string{selected.Record}
		if success {
			owners = append(owners, dialogField)
			slices.Sort(owners)
		}
		event, e := encodeNativeLifecycle(&nativepb.Lifecycle{Kind: kind, Snapshot: snapshot, OperationId: result.OperationId, Message: responseField, Root: rootField, Owners: owners, Selectors: publicSelectors, DeadlineMs: now.Add(2 * time.Minute).UnixMilli(), NextMs: now.UnixMilli()})
		if e != nil {
			return true, e
		}
		changes = append(changes, rediscore.Change{Field: field, Value: event})
		root.Events = append(root.Events, field)
		slices.Sort(root.Events)
		timers = append(timers, rediscore.TimerChange{Member: "event-" + eventID, Due: now})
	}
	outputs := append([]rediscore.OutputIntent(nil), paired.outputs...)
	changes = append(changes, paired.changes...)
	timers = append(timers, paired.timers...)
	if success {
		if successDialog == nil || successDialogRefs.Local == nil {
			return true, errNativeRecord
		}
		if successACK != nil {
			snapshot, e := nativeProjectSnapshot(current, nil, nil, nil)
			if e != nil {
				return true, e
			}
			ackField, outputField := "r:uac-dialog-ack-"+strings.TrimPrefix(dialogField, "r:uac-dialog-"), "r:uac-dialog-ack-output-"+uuid.NewString()
			until := minTime(deadline, now.Add(2*time.Second))
			intent, ackChanges, e := prepareRedisUACFailureACK(snapshot, flow, key, *successACK, selected.Record, dialogField, "r:result-1", ackField, outputField, result.OperationId, until, false, false)
			if e != nil {
				return true, e
			}
			changes = append(changes, ackChanges...)
			outputs = append(outputs, intent)
			successDialogRefs.Local.ACK = ackField
			successDialog.AckEmissions++
			successDialog.Outputs = append(successDialog.Outputs, outputField)
			slices.Sort(successDialog.Outputs)
			proc.Metadata.Outputs = append(proc.Metadata.Outputs, outputField)
			slices.Sort(proc.Metadata.Outputs)
			timers = append(timers, rediscore.TimerChange{Member: "out-" + strings.TrimPrefix(outputField, "r:"), Due: until})
		}
		dialogData, e := encodeNativeDialog(successDialogNative, successDialogRefs, successDialog)
		if e != nil {
			return true, e
		}
		changes = append(changes, rediscore.Change{Field: dialogField, Value: dialogData})
	}
	if failure {
		snapshot, e := nativeProjectSnapshot(current, nil, nil, nil)
		if e != nil {
			return true, e
		}
		ackField, outputField := "r:uac-ack-"+id, "r:uac-ack-output-"+uuid.NewString()
		until := minTime(deadline, now.Add(2*time.Second))
		intent, ackChanges, e := prepareRedisUACFailureACK(snapshot, flow, key, *ack, selected.Record, "", "", ackField, outputField, result.OperationId, until, false, false)
		if e != nil {
			return true, e
		}
		changes = append(changes, ackChanges...)
		outputs = append(outputs, intent)
		proc.Refs.ACK = ackField
		proc.Metadata.AckEmissions++
		proc.Metadata.Outputs = append(proc.Metadata.Outputs, outputField)
		slices.Sort(proc.Metadata.Outputs)
		timers = append(timers, rediscore.TimerChange{Member: "out-" + strings.TrimPrefix(outputField, "r:"), Due: until})
	}
	contextData, err := encodeNativeContext(current)
	if err != nil {
		return true, err
	}
	procData, err := encodeNativeProcedure(next, proc.Refs, proc.Metadata)
	if err != nil {
		return true, err
	}
	changes = append(changes, rediscore.Change{Field: "r:context", Value: contextData}, rediscore.Change{Field: selected.Record, Value: procData})
	if rootField != selected.Record {
		data, e := marshalNativeRecord(storagecodec.NativeProcedure, root)
		if e != nil {
			return true, e
		}
		changes = append(changes, rediscore.Change{Field: rootField, Value: data})
	}
	target := view.Timer
	if target == (rediscore.TimerAssignment{}) {
		target = executor
	}
	publication := rediscore.Publication{Identity: selected.Identity, Revision: view.Revision, Deadline: deadline, SpendReservation: true, Changes: changes, Selectors: selectorChanges, NewOutputs: outputs}
	if len(timers) != 0 {
		publication.Timers = &rediscore.TimerMutation{Expected: view.Timer, Target: target, Changes: timers}
	}
	if flow.Transport == "udp" {
		worker, e := nativeWorkerReference(flow.Ingress)
		if e != nil {
			return true, e
		}
		publication.UDPHint = &rediscore.UDPHint{Worker: worker, Context: selected.Identity.Context, Incarnation: selected.Identity.Incarnation, Procedure: selected.Record, Stop: true}
	}
	_, err = store.Commit(ctx, publication)
	if errors.Is(err, rediscore.ErrExpired) && publication.Timers != nil && view.Timer != (rediscore.TimerAssignment{}) && view.Timer != executor && time.Now().Before(deadline) && ctx.Err() == nil {
		publication.Timers.Target = executor
		_, err = store.Commit(ctx, publication)
	}
	return true, err
}
