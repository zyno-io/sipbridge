package engine

import (
	"bytes"
	"context"
	"errors"
	"slices"
	"strconv"
	"time"

	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/nativepb"
	"sipbridge/internal/rediscore"
	"sipbridge/internal/selectedmessage"
	"sipbridge/internal/storagecodec"
)

// publishRedisApplicationAnswerACK selects the existing local INVITE and its
// dialog, then atomically stages the controller's answer and exact ACK output.
func publishRedisApplicationAnswerACK(ctx context.Context, store *rediscore.Store, key []byte, application *pb.ProcessRef, lease rediscore.ApplicationLeaseRef, command *pb.Command, capture redisNotifyWaitCapture, operation string, executor rediscore.TimerAssignment, deadline time.Time) (rediscore.Identity, error) {
	var zero rediscore.Identity
	if store == nil || command == nil || command.GetDialogRequest() == nil || command.GetDialogRequest().Method != "ACK" || command.Context == nil || command.Context.State == nil || capture.sequence != command.Sequence || !proto.Equal(capture.state, command.Context.State) || command.Sequence < 2 || command.Sequence > 1<<53-1 || !validSendText(operation, 160, false) || !time.Now().Before(deadline) {
		return zero, errNativeRecord
	}
	app, err := nativeApplicationReference(key, application)
	if err != nil || app != lease.Application {
		return zero, rediscore.ErrAuthority
	}
	resultField := "r:result-" + strconv.FormatUint(command.Sequence, 10)
	priorField := "r:result-" + strconv.FormatUint(command.Sequence-1, 10)
	selected, err := store.ObserveApplicationContext(ctx, command.Context.Id, command.Context.State.Incarnation, lease, "r:context", resultField, priorField, "r:delivery-tag")
	if err != nil {
		return zero, err
	}
	current, err := decodeNativeContext(selected.View.Records["r:context"])
	if err != nil {
		return zero, err
	}
	if selected.Identity.Owner != lease.Application || selected.Identity.Term != lease.Term || current.Context == nil || !proto.Equal(current.Context.State, capture.state) || current.Kind != pb.ContextSnapshot_UAS && current.Kind != pb.ContextSnapshot_UAC || !sameProcess(current.Application, application) || current.ApplicationLeaseTerm != lease.Term || current.CommandSequence+1 != command.Sequence || current.Revision >= 1<<53-1 || current.Ended || len(selected.View.Records[resultField]) != 0 {
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
	policy.ExpiresAtMs = min(policy.ExpiresAtMs, uint64(deadline.UnixMilli()))
	refuse := func(cause error) (rediscore.Identity, error) {
		return publishRedisApplicationByeRefusal(ctx, store, selected, current, lease, command, capture, operation, policy, executor, deadline, cause)
	}
	if len(current.Dialogs) == 0 {
		return refuse(errors.New("dialog unavailable"))
	}
	tags := []string{current.LocalTag, current.RemoteTag}
	slices.Sort(tags)
	lookup, err := store.LookupSelector(ctx, nativeSelector(rediscore.SelectorDialog, current.CallId, tags[0], tags[1]))
	if err != nil {
		return zero, err
	}
	if lookup.Identity != selected.Identity || lookup.View.Revision != selected.View.Revision || lookup.View.Timer != selected.View.Timer {
		return zero, rediscore.ErrConflict
	}
	dialogField := lookup.Record
	if !slices.Contains(current.Dialogs, dialogField) {
		return zero, errNativeRecord
	}
	dialog, err := decodeNativeDialog(lookup.View.Records[dialogField])
	if err != nil {
		return zero, err
	}
	if dialog.Native.CallID != current.CallId || dialog.Native.LocalTag != current.LocalTag || dialog.Native.RemoteTag != current.RemoteTag {
		return zero, errNativeRecord
	}
	if dialog.Native.LocalInvite == nil || dialog.Native.Offer != portable.RemoteOfferInACK {
		return refuse(errors.New("dialog has no pending ACK answer"))
	}
	if dialog.Refs.Local == nil || dialog.Refs.Local.Request == "" || dialog.Refs.Local.Final == "" || dialog.Refs.Local.ACK != "" || dialog.Metadata.LocalTransaction == "" {
		return zero, errNativeRecord
	}
	if dialog.Metadata.AckEmissions >= stateProfileTransmissions {
		return refuse(errors.New("ACK emission limit reached"))
	}
	transaction, err := store.LookupSelector(ctx, nativeSelector(rediscore.SelectorClientTransaction, dialog.Metadata.LocalTransaction))
	if err != nil {
		return zero, err
	}
	if transaction.Identity != selected.Identity || transaction.View.Revision != selected.View.Revision || transaction.View.Timer != selected.View.Timer {
		return zero, rediscore.ErrConflict
	}
	procedureField := transaction.Record
	if !slices.Contains(current.InitialProcedures, procedureField) && !slices.Contains(current.NoninitialProcedures, procedureField) {
		return zero, errNativeRecord
	}
	procedure, err := readPrivateProcedureRecord(transaction.View.Records[procedureField])
	if err != nil {
		return zero, err
	}
	if procedure.Native.Version != portable.Version || procedure.Native.Family != string(portable.InviteClient) || procedure.Native.Key != dialog.Metadata.LocalTransaction || procedure.Origin != dialog.Refs.Local.Request {
		return zero, errNativeRecord
	}
	fields := []string{}
	for _, invite := range []*nativeInviteRefs{dialog.Refs.Local, dialog.Refs.Remote} {
		if invite != nil {
			for _, f := range []string{invite.Request, invite.Final, invite.ACK} {
				if f != "" && !slices.Contains(fields, f) {
					fields = append(fields, f)
				}
			}
		}
	}
	if dialog.Refs.LocalBye != "" {
		fields = append(fields, dialog.Refs.LocalBye)
	}
	wires, err := store.Read(ctx, selected.Identity, fields...)
	if err != nil {
		return zero, err
	}
	if wires.Revision != selected.View.Revision || wires.Timer != selected.View.Timer {
		return zero, rediscore.ErrConflict
	}
	memo, err := selectedmessage.NewMemo(8)
	if err != nil {
		return zero, err
	}
	all := nativeInviteSelection{Request: true, Final: true, ACK: true}
	native, err := dialog.attach(&nativeSelectedMessages{records: wires.Records, memo: memo, provenance: "admitted"}, nativeDialogSelection{Local: all, Remote: all, LocalBye: true})
	if err != nil {
		return zero, err
	}
	id, err := redisApplicationByeID(capture.fingerprint[:])
	if err != nil {
		return zero, err
	}
	wireField, outputField := "r:app-ack-wire-"+id, "r:app-ack-output-"+id
	absent, err := store.Read(ctx, selected.Identity, wireField, outputField)
	if err != nil {
		return zero, err
	}
	if absent.Revision != selected.View.Revision || absent.Timer != selected.View.Timer || len(absent.Records[wireField])+len(absent.Records[outputField]) != 0 {
		return zero, rediscore.ErrConflict
	}
	next, effects, err := nativePrepareLocalAnswerACK(native, command.GetDialogRequest(), sip.RFC3261BranchMagicCookie+nativeIdentity(key, "answer-ack", current.Context.State.Incarnation, current.LocalTag+"/"+current.RemoteTag, operation)[:32], now.UnixMilli())
	if err != nil {
		return refuse(err)
	}
	if next.LocalInvite == nil {
		return zero, errNativeRecord
	}
	var ack *portable.Message
	needBYE := false
	for _, effect := range effects {
		switch effect.Kind {
		case portable.Send:
			if effect.Message == nil || ack != nil || effect.Replay {
				return zero, errNativeRecord
			}
			ack = effect.Message
		case portable.NeedBYE:
			if needBYE || !next.LocalInvite.CancelRequested {
				return zero, errNativeRecord
			}
			needBYE = true
		default:
			return zero, errNativeRecord
		}
	}
	if ack == nil || next.LocalInvite == nil || next.LocalInvite.ACK == nil || !next.LocalInvite.ACK.Equal(*ack) {
		return zero, errNativeRecord
	}
	flow := current.Flow
	if policy.SelectedFlow != nil {
		flow = policy.SelectedFlow
	}
	if !validPolicyFlow(flow) || flow.Transport != current.Transport {
		return refuse(errors.New("ACK output flow unavailable"))
	}
	nextContext := proto.Clone(current).(*nativepb.Context)
	nextContext.CommandSequence = command.Sequence
	nextContext.Revision++
	nextContext.DirectoryEpoch = policy.DirectoryEpoch
	nextContext.LocalSdp = append([]byte(nil), command.GetDialogRequest().Body...)
	snapshot, err = nativeProjectSnapshot(nextContext, &next, nil, nil)
	if err != nil {
		return zero, err
	}
	output, outputChanges, err := prepareRedisApplicationDialogOutput(snapshot, flow, *ack, key, policy, operation, procedureField, dialogField, resultField, wireField, outputField, lease, deadline, "ACK", next.LocalInvite.Sequence)
	if err != nil {
		return zero, err
	}
	dialog.Refs.Local.ACK = wireField
	dialog.Metadata.AckResult = resultField
	dialog.Metadata.AckEmissions++
	dialog.Metadata.AckWritten = false
	dialog.Metadata.CompletionFlow = nil
	dialog.Metadata.Outputs = append(dialog.Metadata.Outputs, outputField)
	slices.Sort(dialog.Metadata.Outputs)
	procedure.Outputs = append(procedure.Outputs, outputField)
	slices.Sort(procedure.Outputs)
	dialogData, err := encodeNativeDialog(next, dialog.Refs, dialog.Metadata)
	if err != nil {
		return zero, err
	}
	procData, err := marshalNativeRecord(storagecodec.NativeProcedure, procedure)
	if err != nil {
		return zero, err
	}
	resultData, err := encodeNativeResult(&nativepb.Result{Certainty: pb.OperationResult_UNKNOWN, Pending: true, CutoffMs: deadline.UnixMilli(), Sequence: command.Sequence, OperationId: operation, Fingerprint: capture.fingerprint[:], DecidingLeaseTerm: lease.Term, Output: outputField})
	if err != nil {
		return zero, err
	}
	contextData, err := encodeNativeContext(nextContext)
	if err != nil {
		return zero, err
	}
	prior, err := decodeNativeResult(selected.View.Records[priorField])
	if err != nil || prior.Sequence+1 != command.Sequence {
		return zero, errNativeRecord
	}
	timers, err := planRedisDialogPriorReceipt(ctx, store, selected, current, prior)
	if err != nil {
		return zero, err
	}
	timers = append(timers, rediscore.TimerChange{Member: "out-" + outputField[2:], Due: output.Deadline})
	tag, err := nativeDeliveryTagChange(nextContext.Delivery)
	if err != nil {
		return zero, err
	}
	if old := selected.View.Records[tag.Field]; len(old) != 0 && !bytes.Equal(old, tag.Value) {
		return zero, rediscore.ErrAuthority
	}
	changes := []rediscore.Change{{Field: "r:context", Value: contextData}, {Field: dialogField, Value: dialogData}, {Field: procedureField, Value: procData}, {Field: resultField, Value: resultData}, tag}
	changes = append(changes, outputChanges...)
	target := selected.View.Timer
	if target == (rediscore.TimerAssignment{}) {
		target = executor
	}
	publication := rediscore.Publication{Identity: selected.Identity, Revision: selected.View.Revision, Deadline: output.Deadline, Application: &lease, SpendReservation: true, Changes: changes, NewOutputs: []rediscore.OutputIntent{output}, Timers: &rediscore.TimerMutation{Expected: selected.View.Timer, Target: target, Changes: timers}}
	_, err = store.Commit(ctx, publication)
	if errors.Is(err, rediscore.ErrExpired) && selected.View.Timer != (rediscore.TimerAssignment{}) && selected.View.Timer != executor && ctx.Err() == nil && time.Now().Before(output.Deadline) {
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
