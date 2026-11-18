package engine

import (
	"bytes"
	"context"
	"errors"
	"slices"
	"strconv"
	"time"

	"github.com/emiago/sipgo/sip/portable"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/nativepb"
	"sipbridge/internal/rediscore"
	"sipbridge/internal/selectedmessage"
)

// publishRedisInviteRespond admits one initial-INVITE response. Its result is
// pending until its own physical proof is recorded; this does not wait for it.
func publishRedisInviteRespond(ctx context.Context, store *rediscore.Store, key []byte, application *pb.ProcessRef, lease rediscore.ApplicationLeaseRef, command *pb.Command, capture redisNotifyWaitCapture, operation, contact, sentBy string, executor rediscore.TimerAssignment, deadline time.Time) (rediscore.Identity, error) {
	var zero rediscore.Identity
	if store == nil || command == nil || command.GetRespond() == nil || command.Context == nil || command.Context.State == nil || capture.sequence != command.Sequence || !proto.Equal(capture.state, command.Context.State) || command.Sequence < 2 || command.Sequence > 1<<53-1 || !validSendText(operation, 160, false) || !time.Now().Before(deadline) {
		return zero, errNativeRecord
	}
	response := command.GetRespond()
	id, err := uuid.Parse(response.RequestId)
	if err != nil || id.String() != response.RequestId || response.Status < 100 || response.Status > 699 {
		return zero, errNativeRecord
	}
	app, err := nativeApplicationReference(key, application)
	if err != nil || app != lease.Application {
		return zero, rediscore.ErrAuthority
	}
	requestID, sequence := response.RequestId, strconv.FormatUint(command.Sequence, 10)
	procedureField, originField := "r:procedure-"+requestID, "r:origin-"+requestID
	resultField, priorField := "r:result-"+sequence, "r:result-"+strconv.FormatUint(command.Sequence-1, 10)
	responseField, physicalField, outputField := "r:invite-response-"+requestID+"-"+sequence, "r:invite-wire-"+requestID+"-"+sequence, "r:invite-output-"+requestID+"-"+sequence
	dialogField := "r:dialog-" + requestID
	selected, err := store.ObserveApplicationContext(ctx, command.Context.Id, command.Context.State.Incarnation, lease, "r:context", procedureField, originField, resultField, priorField, "r:delivery-tag", responseField, physicalField, outputField, dialogField)
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
	if selected.Identity.Owner != lease.Application || selected.Identity.Term != lease.Term || current.Context == nil || !proto.Equal(current.Context.State, capture.state) || current.Kind != pb.ContextSnapshot_UAS || !sameProcess(current.Application, application) || current.ApplicationLeaseTerm != lease.Term || current.CommandSequence+1 != command.Sequence || current.Revision >= 1<<53-1 || !slices.Contains(current.InitialProcedures, procedureField) || procedure.Native.Family != portable.InviteServer || procedure.Metadata.RequestId != requestID || procedure.Metadata.DecisionPending || procedure.Refs.Origin != originField || procedure.Metadata.ProvisionalEmissions > procedure.Metadata.Emissions || len(selected.View.Records[resultField]) != 0 || len(selected.View.Records[responseField]) != 0 || len(selected.View.Records[physicalField]) != 0 || len(selected.View.Records[outputField]) != 0 {
		return zero, rediscore.ErrConflict
	}
	snapshot, err := nativeProjectSnapshot(current, nil, nil, nil)
	if err != nil {
		return zero, err
	}
	now := time.Now()
	if err = validateNativeCommandPolicy(snapshot, command, operation, now, deadline); err != nil {
		return zero, err
	}
	policy := proto.Clone(command.Admission).(*pb.CommandAdmission)
	prior, err := decodeNativeResult(selected.View.Records[priorField])
	if err != nil || prior.Sequence != command.Sequence-1 {
		return zero, errNativeRecord
	}
	previous, err := planPreviousInviteResult(ctx, store, selected, procedure.Metadata, prior, requestID)
	if err != nil {
		return zero, err
	}
	if procedure.Metadata.Emissions >= stateTransactionEmissions || response.Status < 200 && procedure.Metadata.ProvisionalEmissions >= stateProfileTransmissions || response.Status >= 200 && procedure.Metadata.Emissions-procedure.Metadata.ProvisionalEmissions >= stateProfileTransmissions {
		return publishRedisInviteRespondRefusal(ctx, store, lease, command, capture, operation, executor, deadline, selected, current, policy, previous, errors.New("INVITE response emission capacity exhausted"))
	}
	if response.PrincipalId != "" && current.PrincipalId != "" && response.PrincipalId != current.PrincipalId {
		return publishRedisInviteRespondRefusal(ctx, store, lease, command, capture, operation, executor, deadline, selected, current, policy, previous, errors.New("principal mismatch"))
	}
	until := minTime(deadline, time.UnixMilli(int64(policy.ExpiresAtMs)))
	until = minTime(until, now.Add(2*time.Second))
	previousOutput := ""
	previousHeld := false
	if procedure.Refs.LastResponse != "" {
		fields := []string{procedure.Refs.LastResponse}
		// A selected final from another native transition is still loaded for
		// the SDK's ordinary refusal; only this producer's provisional namespace
		// can participate in the displaced-message release below.
		previousOutput, _ = redisInitialResponseOutputField(requestID, procedure.Refs.LastResponse)
		previousHeld = slices.Contains(procedure.Metadata.Outputs, previousOutput)
		if previousOutput != "" && !previousHeld {
			fields = append(fields, previousOutput)
		}
		last, e := store.Read(ctx, selected.Identity, fields...)
		if e != nil {
			return zero, e
		}
		if last.Revision != selected.View.Revision || last.Timer != selected.View.Timer {
			return zero, rediscore.ErrConflict
		}
		selected.View.Records[procedure.Refs.LastResponse] = last.Records[procedure.Refs.LastResponse]
		if previousOutput != "" && !previousHeld {
			selected.View.Records[previousOutput] = last.Records[previousOutput]
		}
	}
	memo, err := selectedmessage.NewMemo(2)
	if err != nil {
		return zero, err
	}
	tx, err := procedure.attach(&nativeSelectedMessages{records: selected.View.Records, memo: memo, provenance: "admitted"}, true, procedure.Refs.LastResponse != "", false)
	if err != nil {
		return zero, err
	}
	native, err := nativePrepareInitialInviteResponse(tx, response, current.LocalTag, contact, sentBy, now.UnixMilli())
	if err != nil {
		return publishRedisInviteRespondRefusal(ctx, store, lease, command, capture, operation, executor, deadline, selected, current, policy, previous, err)
	}
	displaced := ""
	if tx.LastResponse != nil {
		facts, e := tx.LastResponse.Facts()
		if e != nil {
			return zero, errNativeRecord
		}
		if facts.Status >= 100 && facts.Status < 200 {
			if facts.IsRequest || facts.CSeqMethod != "INVITE" || previousOutput == "" {
				return zero, errNativeRecord
			}
			if !previousHeld {
				if len(selected.View.Records[previousOutput]) != 0 {
					return zero, errNativeRecord
				}
				// Its only remaining owner is the procedure reference replaced in
				// this same CAS. Initial-UAS 1xx is never a dialog final or an
				// ordinary Respond receipt's request/response wire.
				displaced = procedure.Refs.LastResponse
			}
		}
	}
	next := proto.Clone(current).(*nativepb.Context)
	next.CommandSequence, next.Revision, next.DirectoryEpoch = command.Sequence, current.Revision+1, policy.DirectoryEpoch
	if response.PrincipalId != "" {
		next.PrincipalId, next.BindingId = response.PrincipalId, response.BindingId
	}
	var selectors []rediscore.SelectorChange
	var changes []rediscore.Change
	if displaced != "" {
		changes = append(changes, rediscore.Change{Field: displaced, Delete: true})
	}
	var timers []rediscore.TimerChange
	for _, effect := range native.Effects {
		switch effect.Kind {
		case portable.Send:
		case portable.CancelTimer:
			if effect.Timer != portable.TimerG {
				timers = append(timers, rediscore.TimerChange{Member: "call-tx-" + requestID + "-" + string(effect.Timer), Delete: true})
			}
		case portable.Schedule:
			if effect.Timer != portable.TimerG {
				timers = append(timers, rediscore.TimerChange{Member: "call-tx-" + requestID + "-" + string(effect.Timer), Due: time.UnixMilli(effect.Deadline.At)})
			}
		default:
			return zero, errNativeRecord
		}
	}
	if native.Dialog != nil {
		if len(current.Dialogs) != 0 || len(selected.View.Records[dialogField]) != 0 {
			return zero, rediscore.ErrConflict
		}
		dialog := *native.Dialog
		next.Answered, next.LocalSdp = true, append([]byte(nil), response.Body...)
		next.RemoteSdp, err = tx.Origin.Body()
		if err != nil {
			return zero, err
		}
		next.LocalTag, next.RemoteTag, next.RemoteTarget = dialog.LocalTag, dialog.RemoteTag, dialog.Target
		next.Dialogs = []string{dialogField}
		tags := []string{dialog.LocalTag, dialog.RemoteTag}
		slices.Sort(tags)
		selectors = append(selectors, rediscore.SelectorChange{Selector: nativeSelector(rediscore.SelectorDialog, dialog.CallID, tags[0], tags[1]), Record: dialogField})
		encoded, e := encodeNativeDialog(dialog, nativeDialogRefs{Remote: &nativeInviteRefs{Request: originField, Final: responseField}}, &nativepb.Dialog{Transaction: tx.Key, CompletionFlow: proto.Clone(procedure.Metadata.Flow).(*pb.FlowRef)})
		if e != nil {
			return zero, e
		}
		changes = append(changes, rediscore.Change{Field: dialogField, Value: encoded})
		for _, effect := range native.DialogEffects {
			switch effect.Kind {
			case portable.Respond:
			case portable.Schedule:
				if effect.Timer != portable.Timer2xx {
					timers = append(timers, rediscore.TimerChange{Member: callDialogTimerMember(dialogField, effect.Timer), Due: time.UnixMilli(effect.Deadline.At)})
				}
			case portable.CancelTimer:
				if effect.Timer != portable.Timer2xx {
					timers = append(timers, rediscore.TimerChange{Member: callDialogTimerMember(dialogField, effect.Timer), Delete: true})
				}
			default:
				return zero, errNativeRecord
			}
		}
	}
	snapshot, err = nativeProjectSnapshot(next, native.Dialog, nil, nil)
	if err != nil {
		return zero, err
	}
	outputDialog := ""
	if native.Dialog != nil {
		outputDialog = dialogField
	}
	intent, outputChanges, shared, err := prepareRedisCallResponseWithResult("INVITE", response.Status, snapshot, procedure.Metadata.Flow, key, native.Message, procedureField, outputDialog, responseField, physicalField, outputField, resultField, operation, policy, until, false)
	if err != nil {
		return zero, err
	}
	changes = append(changes, outputChanges...)
	if !shared {
		wire, e := selectedmessage.Marshal(native.Message)
		if e != nil {
			return zero, e
		}
		changes = append(changes, rediscore.Change{Field: responseField, Value: wire})
	}
	metadata := proto.Clone(procedure.Metadata).(*nativepb.Procedure)
	metadata.CommandResult = resultField
	metadata.Emissions++
	if response.Status < 200 {
		metadata.ProvisionalEmissions++
	}
	metadata.Outputs = append(metadata.Outputs, outputField)
	slices.Sort(metadata.Outputs)
	refs := procedure.Refs
	refs.LastResponse = responseField
	encoded, err := encodeNativeProcedure(native.Transaction, refs, metadata)
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
	timers = append(timers, previous...)
	if response.Status >= 200 {
		redrive, e := planRedisProvisionalOutputRedrive(ctx, store, selected, procedureField, procedure.Metadata)
		if e != nil {
			return zero, e
		}
		timers = append(timers, redrive...)
	}
	timers = append(timers, rediscore.TimerChange{Member: "out-" + outputField[2:], Due: until})
	tag, err := nativeDeliveryTagChange(next.Delivery)
	if err != nil {
		return zero, err
	}
	if old := selected.View.Records[tag.Field]; len(old) != 0 && !bytes.Equal(old, tag.Value) {
		return zero, rediscore.ErrAuthority
	}
	changes = append(changes, rediscore.Change{Field: "r:context", Value: contextBytes}, rediscore.Change{Field: procedureField, Value: encoded}, rediscore.Change{Field: resultField, Value: resultBytes}, tag)
	target := selected.View.Timer
	if target.Worker == "" {
		target = executor
	}
	publication := rediscore.Publication{Identity: selected.Identity, Revision: selected.View.Revision, Deadline: until, Application: &lease, SpendReservation: true, Changes: changes, NewOutputs: []rediscore.OutputIntent{intent}, Selectors: selectors, Timers: &rediscore.TimerMutation{Expected: selected.View.Timer, Target: target, Changes: timers}}
	_, err = store.Commit(ctx, publication)
	if errors.Is(err, rediscore.ErrExpired) && selected.View.Timer != (rediscore.TimerAssignment{}) && selected.View.Timer != executor && time.Now().Before(until) && ctx.Err() == nil {
		publication.Timers.Target = executor
		_, err = store.Commit(ctx, publication)
	}
	if err != nil {
		return zero, err
	}
	if ctx.Err() != nil || !time.Now().Before(until) {
		return zero, rediscore.ErrUnknown
	}
	return selected.Identity, nil
}
