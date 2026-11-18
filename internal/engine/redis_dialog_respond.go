package engine

import (
	"context"
	"errors"
	"slices"
	"strconv"
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

func publishRedisDialogRespond(ctx context.Context, store *rediscore.Store, key []byte, application *pb.ProcessRef, lease rediscore.ApplicationLeaseRef, command *pb.Command, capture redisNotifyWaitCapture, operation string, advertise nativeSIPAdvertisement, executor rediscore.TimerAssignment, deadline time.Time) (rediscore.Identity, error) {
	var zero rediscore.Identity
	if store == nil || command == nil || command.GetRespond() == nil || command.Context == nil || command.Context.State == nil || capture.sequence != command.Sequence || !proto.Equal(capture.state, command.Context.State) || command.Sequence < 2 || !validSendText(operation, 160, false) || !time.Now().Before(deadline) {
		return zero, errNativeRecord
	}
	response := command.GetRespond()
	parsed, err := uuid.Parse(response.RequestId)
	if err != nil || parsed.String() != response.RequestId || response.Status < 100 || response.Status > 699 {
		return zero, errNativeRecord
	}
	app, err := nativeApplicationReference(key, application)
	if err != nil || app != lease.Application {
		return zero, rediscore.ErrAuthority
	}
	id, sequence := response.RequestId, strconv.FormatUint(command.Sequence, 10)
	procedureField, originField := "r:procedure-"+id, "r:origin-"+id
	resultField, priorField := "r:result-"+sequence, "r:result-"+strconv.FormatUint(command.Sequence-1, 10)
	selected, err := store.ObserveApplicationContext(ctx, command.Context.Id, command.Context.State.Incarnation, lease, "r:context", procedureField, originField, resultField, priorField)
	if err != nil {
		return zero, err
	}
	current, err := decodeNativeContext(selected.View.Records["r:context"])
	if err != nil {
		return zero, err
	}
	procedure, err := decodeNativeProcedure(selected.View.Records[procedureField])
	if err != nil {
		return zero, err
	}
	if current.Context == nil || !proto.Equal(current.Context, command.Context) || current.Kind != pb.ContextSnapshot_UAS && current.Kind != pb.ContextSnapshot_UAC || current.Ended || current.CommandSequence+1 != command.Sequence || current.Revision >= 1<<53-1 || !slices.Contains(current.NoninitialProcedures, procedureField) || !sameProcess(current.Application, application) || current.ApplicationLeaseTerm != lease.Term || procedure.Native.Family != portable.NonInviteServer && procedure.Native.Family != portable.InviteServer || procedure.Metadata.DecisionPending || procedure.Metadata.RequestId != id || procedure.Refs.Origin != originField || len(selected.View.Records[resultField]) != 0 {
		return zero, errNativeRecord
	}
	fields := []string{originField}
	if procedure.Refs.LastResponse != "" {
		fields = append(fields, procedure.Refs.LastResponse)
	}
	view, err := store.ReadApplication(ctx, selected.Identity, lease, fields...)
	if err != nil {
		return zero, err
	}
	if view.Revision != selected.View.Revision || view.Timer != selected.View.Timer {
		return zero, rediscore.ErrConflict
	}
	memo, err := selectedmessage.NewMemo(len(fields) + 1)
	if err != nil {
		return zero, err
	}
	messages := &nativeSelectedMessages{records: view.Records, memo: memo, provenance: "admitted"}
	tx, err := procedure.attach(messages, true, procedure.Refs.LastResponse != "", false)
	if err != nil {
		return zero, err
	}
	facts, err := tx.Origin.Facts()
	if err != nil || !facts.IsRequest || facts.Method != sip.INVITE && !redisDialogNonInvite(facts.Method) || facts.Method == sip.INVITE && procedure.Native.Family != portable.InviteServer || facts.Method != sip.INVITE && procedure.Native.Family != portable.NonInviteServer {
		return zero, errNativeRecord
	}
	ids, err := nativeSelectorIDs(tx.Origin, false, false)
	if err != nil || len(ids) != 2 || ids[1].Kind != rediscore.SelectorDialog {
		return zero, errNativeRecord
	}
	dialogSelection, err := store.LookupSelector(ctx, ids[1], "r:context")
	if err != nil {
		return zero, err
	}
	if dialogSelection.Identity != selected.Identity || dialogSelection.View.Revision != selected.View.Revision || dialogSelection.View.Timer != selected.View.Timer || !slices.Contains(current.Dialogs, dialogSelection.Record) {
		return zero, rediscore.ErrConflict
	}
	dialog, err := readPrivateDialogRecord(dialogSelection.View.Records[dialogSelection.Record])
	if err != nil {
		return zero, err
	}
	snapshot, err := nativeProjectSnapshot(current, nil, nil, nil)
	if err != nil {
		return zero, err
	}
	now := time.Now()
	if err = validateNativeCommandPolicy(snapshot, command, operation, now, deadline); err != nil {
		return zero, err
	}
	prior, err := decodeNativeResult(selected.View.Records[priorField])
	if err != nil {
		return zero, err
	}
	timers, err := planRedisDialogPriorReceipt(ctx, store, selected, current, prior)
	if err != nil {
		return zero, err
	}
	until := minTime(deadline, time.UnixMilli(int64(command.Admission.ExpiresAtMs)))
	until = minTime(until, now.Add(2*time.Second))
	refuse := func(cause error) (rediscore.Identity, error) {
		_, e := publishRedisDialogRefusal(ctx, store, selected, current, lease, command.Sequence, id, operation, command.Admission.DirectoryEpoch, capture.fingerprint, until, deadline, timers, executor, cause)
		if e != nil {
			return zero, e
		}
		return selected.Identity, nil
	}
	if response.PrincipalId != "" && current.PrincipalId != "" && response.PrincipalId != current.PrincipalId {
		return refuse(errors.New("principal mismatch"))
	}
	if procedure.Metadata.Emissions >= stateTransactionEmissions || response.Status < 200 && procedure.Metadata.ProvisionalEmissions >= stateProfileTransmissions || response.Status >= 200 && procedure.Metadata.Emissions-procedure.Metadata.ProvisionalEmissions >= stateProfileTransmissions {
		return refuse(errors.New("dialog response emission capacity exhausted"))
	}
	contact, err := redisDialogResponseContact(advertise, storageDecodePortableDialog(dialog.Native), tx.Origin, response.Status)
	if err != nil {
		return refuse(err)
	}
	native, err := nativeServerBuildResponseForFlow(tx, int(response.Status), reason(response.Status), response.Body, current.LocalTag, contact, stateHeaders(response.Headers), procedure.Metadata.Flow, now.UnixMilli())
	if err != nil {
		return refuse(err)
	}
	sends := 0
	for _, effect := range native.Effects {
		if effect.Kind == portable.Send {
			sends++
		}
	}
	if sends != 1 {
		return refuse(errors.New("server transaction cannot select response"))
	}
	next := proto.Clone(current).(*nativepb.Context)
	next.CommandSequence, next.Revision, next.DirectoryEpoch = command.Sequence, current.Revision+1, command.Admission.DirectoryEpoch
	if response.PrincipalId != "" {
		next.PrincipalId, next.BindingId = response.PrincipalId, response.BindingId
	}
	var dialogNext *portable.Dialog
	if facts.Method == sip.INVITE && response.Status >= 200 {
		oldNative, oldLocal, oldRemote, oldBye := dialog.Native, dialog.LocalInvite, dialog.RemoteInvite, dialog.LocalBye
		dialogRecord := nativeDialogFromPrivate(dialog)
		dialog.Native, dialog.LocalInvite, dialog.RemoteInvite, dialog.LocalBye = oldNative, oldLocal, oldRemote, oldBye
		if dialogRecord.Refs.Remote == nil || dialogRecord.Refs.Remote.Request != originField {
			return zero, errNativeRecord
		}
		if dialogRecord.Refs.Local != nil && dialogRecord.Refs.Local.Request != originField {
			extra, e := store.ReadApplication(ctx, selected.Identity, lease, dialogRecord.Refs.Local.Request)
			if e != nil {
				return zero, e
			}
			if extra.Revision != selected.View.Revision || extra.Timer != selected.View.Timer {
				return zero, rediscore.ErrConflict
			}
			messages.records[dialogRecord.Refs.Local.Request] = extra.Records[dialogRecord.Refs.Local.Request]
		}
		previous, e := dialogRecord.attach(messages, nativeDialogSelection{Local: nativeInviteSelection{Request: true}, Remote: nativeInviteSelection{Request: true}})
		if e != nil {
			return zero, e
		}
		changed, effects, e := portable.StepDialog(previous, portable.DialogInput{Kind: portable.DialogSendResponse, Now: now.UnixMilli(), Message: native.Message, Answer: len(response.Body) > 0})
		if e != nil {
			return refuse(e)
		}
		for _, effect := range effects {
			switch effect.Kind {
			case portable.Respond:
			case portable.Schedule:
				if effect.Timer != portable.Timer2xx {
					timers = append(timers, rediscore.TimerChange{Member: callDialogTimerMember(dialogSelection.Record, effect.Timer), Due: time.UnixMilli(effect.Deadline.At)})
				}
			case portable.CancelTimer:
				if effect.Timer != portable.Timer2xx {
					timers = append(timers, rediscore.TimerChange{Member: callDialogTimerMember(dialogSelection.Record, effect.Timer), Delete: true})
				}
			default:
				return zero, errNativeRecord
			}
		}
		if response.Status < 300 {
			retire, retireErr := planRedisSupersededInviteRetirement(ctx, store, selected, current, dialog.Transaction, tx.Key, now)
			if retireErr != nil {
				return zero, retireErr
			}
			timers = append(timers, retire...)
			dialog.Transaction = tx.Key
			next.Answered, next.LocalSdp = true, append([]byte(nil), response.Body...)
			next.RemoteSdp, e = tx.Origin.Body()
			if e != nil {
				return zero, e
			}
			next.LocalTag, next.RemoteTag, next.RemoteTarget = changed.LocalTag, changed.RemoteTag, changed.Target
		}
		dialogNext = &changed
	}
	snapshot, err = nativeProjectSnapshot(next, dialogNext, nil, nil)
	if err != nil {
		return zero, err
	}
	responseField, outputField := "r:dialog-response-"+id+"-"+sequence, "r:dialog-output-"+id+"-"+sequence
	intent, changes, shared, err := prepareRedisCallResponseWithResult(string(facts.Method), response.Status, snapshot, procedure.Metadata.Flow, key, native.Message, procedureField, dialogSelection.Record, responseField, "r:dialog-wire-"+id+"-"+sequence, outputField, resultField, operation, command.Admission, until, false)
	if err != nil {
		return zero, err
	}
	if !shared {
		data, e := selectedmessage.Marshal(native.Message)
		if e != nil {
			return zero, e
		}
		changes = append(changes, rediscore.Change{Field: responseField, Value: data})
	}
	for _, effect := range native.Effects {
		switch effect.Kind {
		case portable.Send, portable.Ended:
		case portable.Schedule:
			if effect.Timer != portable.TimerG {
				timers = append(timers, rediscore.TimerChange{Member: "call-tx-" + id + "-" + string(effect.Timer), Due: time.UnixMilli(effect.Deadline.At)})
			}
		case portable.CancelTimer:
			if effect.Timer != portable.TimerG {
				timers = append(timers, rediscore.TimerChange{Member: "call-tx-" + id + "-" + string(effect.Timer), Delete: true})
			}
		default:
			return zero, errNativeRecord
		}
	}
	private := procedure.Metadata
	private.CommandResult = resultField
	private.Emissions++
	private.Outputs = append(private.Outputs, outputField)
	slices.Sort(private.Outputs)
	if response.Status < 200 {
		private.ProvisionalEmissions++
	}
	refs := procedure.Refs
	refs.LastResponse = responseField
	procedureBytes, err := encodeNativeProcedure(native.Transaction, refs, private)
	if err != nil {
		return zero, err
	}
	dialog.Outputs = append(dialog.Outputs, outputField)
	slices.Sort(dialog.Outputs)
	if dialogNext != nil {
		if err = replaceRedisRemoteInvite(dialog, *dialogNext, originField, responseField); err != nil {
			return zero, err
		}
	}
	dialogBytes, err := marshalNativeRecord(storagecodec.NativeDialog, dialog)
	if err != nil {
		return zero, err
	}
	contextBytes, err := encodeNativeContext(next)
	if err != nil {
		return zero, err
	}
	resultBytes, err := encodeNativeResult(&nativepb.Result{Certainty: pb.OperationResult_UNKNOWN, Pending: true, CutoffMs: deadline.UnixMilli(), Sequence: command.Sequence, OperationId: operation, Fingerprint: capture.fingerprint[:], DecidingLeaseTerm: lease.Term, Output: outputField})
	if err != nil {
		return zero, err
	}
	timers = append(timers, rediscore.TimerChange{Member: "out-" + outputField[2:], Due: until})
	changes = append(changes, rediscore.Change{Field: "r:context", Value: contextBytes}, rediscore.Change{Field: procedureField, Value: procedureBytes}, rediscore.Change{Field: dialogSelection.Record, Value: dialogBytes}, rediscore.Change{Field: resultField, Value: resultBytes})
	target := selected.View.Timer
	if target == (rediscore.TimerAssignment{}) {
		target = executor
	}
	publication := rediscore.Publication{Identity: selected.Identity, Revision: selected.View.Revision, Deadline: until, Application: &lease, SpendReservation: true, Changes: changes, NewOutputs: []rediscore.OutputIntent{intent}, Timers: &rediscore.TimerMutation{Expected: selected.View.Timer, Target: target, Changes: timers}}
	_, err = store.Commit(ctx, publication)
	if errors.Is(err, rediscore.ErrExpired) && selected.View.Timer != (rediscore.TimerAssignment{}) && selected.View.Timer != executor && ctx.Err() == nil && time.Now().Before(until) {
		publication.Timers.Target = executor
		_, err = store.Commit(ctx, publication)
	}
	if err != nil {
		return zero, err
	}
	if ctx.Err() != nil || !time.Now().Before(deadline) {
		return zero, rediscore.ErrUnknown
	}
	return selected.Identity, nil
}
