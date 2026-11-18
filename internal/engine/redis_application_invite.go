package engine

import (
	"bytes"
	"context"
	"errors"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/nativepb"
	"sipbridge/internal/rediscore"
	"sipbridge/internal/selectedmessage"
)

// publishRedisApplicationInvite selects and atomically stages one local re-INVITE.
func publishRedisApplicationInvite(ctx context.Context, store *rediscore.Store, key []byte, application *pb.ProcessRef, lease rediscore.ApplicationLeaseRef, command *pb.Command, capture redisNotifyWaitCapture, operation string, executor rediscore.TimerAssignment, deadline, leaseUntil time.Time) (rediscore.Identity, error) {
	var zero rediscore.Identity
	if store == nil || command == nil || command.GetDialogRequest() == nil || command.GetDialogRequest().Method != "INVITE" || command.Context == nil || command.Context.State == nil || capture.sequence != command.Sequence || !proto.Equal(capture.state, command.Context.State) || command.Sequence < 2 || command.Sequence > 1<<53-1 || !validSendText(operation, 160, false) || !time.Now().Before(deadline) {
		return zero, errNativeRecord
	}
	app, err := nativeApplicationReference(key, application)
	if err != nil || app != lease.Application {
		return zero, rediscore.ErrAuthority
	}
	seq := strconv.FormatUint(command.Sequence, 10)
	resultField := "r:result-" + seq
	priorField := "r:result-" + strconv.FormatUint(command.Sequence-1, 10)
	selected, err := store.ObserveApplicationContext(ctx, command.Context.Id, command.Context.State.Incarnation, lease, "r:context", resultField, priorField, "r:delivery-tag")
	if err != nil {
		return zero, err
	}
	current, err := decodeNativeContext(selected.View.Records["r:context"])
	if err != nil {
		return zero, err
	}
	if selected.Identity.Owner != lease.Application || selected.Identity.Term != lease.Term || current.Context == nil || !proto.Equal(current.Context.State, capture.state) || current.Kind != pb.ContextSnapshot_UAS && current.Kind != pb.ContextSnapshot_UAC || !sameProcess(current.Application, application) || current.ApplicationLeaseTerm != lease.Term || current.CommandSequence+1 != command.Sequence || current.Revision >= 1<<53-1 || current.Ended || len(selected.View.Records[resultField]) != 0 || len(current.Dialogs) == 0 || len(current.InitialProcedures)+len(current.NoninitialProcedures) >= stateNativeProcedures {
		return zero, rediscore.ErrConflict
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
	field := lookup.Record
	if !slices.Contains(current.Dialogs, field) {
		return zero, errNativeRecord
	}
	dialog, err := decodeNativeDialog(lookup.View.Records[field])
	if err != nil {
		return zero, err
	}
	if dialog.Native.CallID != current.CallId || dialog.Native.LocalTag != current.LocalTag || dialog.Native.RemoteTag != current.RemoteTag {
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
	snapshot, err := nativeProjectSnapshot(current, &native, nil, nil)
	if err != nil {
		return zero, err
	}
	now := time.Now()
	if err = validateNativeCommandPolicy(snapshot, command, operation, now, deadline); err != nil {
		return zero, err
	}
	policy := proto.Clone(command.Admission).(*pb.CommandAdmission)
	policy.ExpiresAtMs = min(policy.ExpiresAtMs, uint64(deadline.UnixMilli()))
	previousDialog, err := readPrivateDialogRecord(lookup.View.Records[field])
	if err != nil {
		return zero, err
	}
	previousChanges, previousTimers, previousField, promotionUntil, err := planRedisPreviousInviteRelease(ctx, store, selected, current, field, previousDialog, leaseUntil)
	var pending *previousInviteOutputPending
	if errors.As(err, &pending) {
		advanced, advanceErr := advanceRedisPreviousInviteOutput(ctx, store, pending, key, executor, deadline)
		if advanceErr != nil {
			return zero, advanceErr
		}
		if advanced {
			return zero, rediscore.ErrConflict
		}
	}
	if errors.Is(err, errPreviousInviteACKPending) {
		return publishRedisApplicationByeRefusal(ctx, store, selected, current, lease, command, capture, operation, policy, executor, deadline, err)
	}
	if err != nil {
		return zero, err
	}
	dialog.Metadata.Outputs = previousDialog.Outputs
	id, err := redisApplicationByeID(capture.fingerprint[:])
	if err != nil {
		return zero, err
	}
	prefix := "app-invite-"
	procField, wireField, outputField := "r:"+prefix+id, "r:"+prefix+"origin-"+id, "r:"+prefix+"output-"+id
	absent, err := store.Read(ctx, selected.Identity, procField, wireField, outputField)
	if err != nil {
		return zero, err
	}
	if absent.Revision != selected.View.Revision || absent.Timer != selected.View.Timer {
		return zero, rediscore.ErrConflict
	}
	if len(absent.Records[procField])+len(absent.Records[wireField])+len(absent.Records[outputField]) != 0 {
		return zero, rediscore.ErrConflict
	}
	request := command.GetDialogRequest()
	next, tx, effects, err := nativePrepareDialogRequest(native, portable.DialogInput{Kind: portable.DialogLocalRequest, Now: now.UnixMilli(), Method: sip.RequestMethod(request.Method), Branch: sip.RFC3261BranchMagicCookie + id, Body: request.Body, Headers: stateHeaders(request.Headers), Offer: len(request.Body) > 0}, now.UnixMilli())
	if err != nil {
		return publishRedisApplicationByeRefusal(ctx, store, selected, current, lease, command, capture, operation, policy, executor, deadline, err)
	}
	flow := current.Flow
	if policy.SelectedFlow != nil {
		flow = policy.SelectedFlow
	}
	if !validPolicyFlow(flow) || flow.Transport != current.Transport {
		return publishRedisApplicationByeRefusal(ctx, store, selected, current, lease, command, capture, operation, policy, executor, deadline, errors.New("INVITE output flow unavailable"))
	}
	nextContext := proto.Clone(current).(*nativepb.Context)
	nextContext.CommandSequence = command.Sequence
	nextContext.Revision++
	nextContext.DirectoryEpoch = policy.DirectoryEpoch
	nextContext.NoninitialProcedures = append(nextContext.NoninitialProcedures, procField)
	slices.Sort(nextContext.NoninitialProcedures)
	snapshot, err = nativeProjectSnapshot(nextContext, &next, nil, nil)
	if err != nil {
		return zero, err
	}
	output, outputChanges, err := prepareRedisApplicationDialogOutput(snapshot, flow, tx.Origin, key, policy, operation, procField, field, resultField, wireField, outputField, lease, deadline, "INVITE", 0)
	if err != nil {
		if time.Now().Before(deadline) && ctx.Err() == nil {
			return publishRedisApplicationByeRefusal(ctx, store, selected, current, lease, command, capture, operation, policy, executor, deadline, err)
		}
		return zero, err
	}
	metadata := &nativepb.Procedure{Flow: proto.Clone(flow).(*pb.FlowRef), Application: proto.Clone(application).(*pb.ProcessRef), Policy: policy, Emissions: 1, CommandResult: resultField, CreditContribution: uint64(residentSmallBudget), Outputs: []string{outputField}}
	procBytes, err := encodeNativeProcedure(tx, nativeProcedureRefs{Origin: wireField}, metadata)
	if err != nil {
		return zero, err
	}
	if next.LocalInvite == nil || !next.LocalInvite.Request.Equal(tx.Origin) {
		return zero, errNativeRecord
	}
	dialog.Metadata.LocalTransaction = tx.Key
	dialog.Metadata.AckResult = ""
	dialog.Metadata.AckWritten = false
	dialog.Metadata.AckEmissions = 0
	dialog.Metadata.CompletionFlow = nil
	dialog.Refs.Local = &nativeInviteRefs{Request: wireField}
	dialog.Metadata.Outputs = append(dialog.Metadata.Outputs, outputField)
	slices.Sort(dialog.Metadata.Outputs)
	dialogBytes, err := encodeNativeDialog(next, dialog.Refs, dialog.Metadata)
	if err != nil {
		return zero, err
	}
	resultBytes, err := encodeNativeResult(&nativepb.Result{Certainty: pb.OperationResult_UNKNOWN, Pending: true, CutoffMs: deadline.UnixMilli(), Sequence: command.Sequence, OperationId: operation, Fingerprint: capture.fingerprint[:], DecidingLeaseTerm: lease.Term, Output: outputField})
	if err != nil {
		return zero, err
	}
	contextBytes, err := encodeNativeContext(nextContext)
	if err != nil {
		return zero, err
	}
	tag, err := nativeDeliveryTagChange(nextContext.Delivery)
	if err != nil {
		return zero, err
	}
	if old := selected.View.Records[tag.Field]; len(old) != 0 && !bytes.Equal(old, tag.Value) {
		return zero, rediscore.ErrAuthority
	}
	prior, err := decodeNativeResult(selected.View.Records[priorField])
	if err != nil || prior.Sequence+1 != command.Sequence {
		return zero, errNativeRecord
	}
	// The prior receipt's actual producer retains responsibility for expiry.
	timers, err := planRedisDialogPriorReceipt(ctx, store, selected, current, prior)
	if err != nil {
		return zero, err
	}
	timers = append(timers, previousTimers...)
	selectors, err := nativeSelectorIDs(tx.Origin, true, false)
	if err != nil || len(selectors) == 0 {
		return zero, errNativeRecord
	}
	selectorChanges := make([]rediscore.SelectorChange, 0, len(selectors))
	for _, selector := range selectors {
		if selector.Kind == rediscore.SelectorClientTransaction {
			selectorChanges = append(selectorChanges, rediscore.SelectorChange{Selector: selector, Record: procField})
		}
	}
	if len(selectorChanges) != 1 {
		return zero, errNativeRecord
	}
	timers = append(timers, rediscore.TimerChange{Member: "out-" + outputField[2:], Due: output.Deadline})
	if strings.HasPrefix(previousField, "r:app-invite-") {
		timers = append(timers, rediscore.TimerChange{Member: "app-invite-retire-" + strings.TrimPrefix(previousField, "r:app-invite-"), Due: now})
	}
	sends, timerB := 0, 0
	for _, effect := range effects {
		if effect.Kind == portable.Send {
			sends++
		}
		if effect.Kind == portable.Schedule && effect.Timer != portable.TimerA {
			if effect.Timer != portable.TimerB {
				return zero, errNativeRecord
			}
			timerB++
			timers = append(timers, rediscore.TimerChange{Member: prefix + id + "-" + string(effect.Timer), Due: time.UnixMilli(effect.Deadline.At)})
		}
	}
	if sends != 1 || timerB != 1 {
		return zero, errNativeRecord
	}
	changes := []rediscore.Change{{Field: "r:context", Value: contextBytes}, {Field: field, Value: dialogBytes}, {Field: procField, Value: procBytes}, {Field: resultField, Value: resultBytes}, tag}
	changes = append(changes, previousChanges...)
	changes = append(changes, outputChanges...)
	target := selected.View.Timer
	if target.Worker == "" {
		target = executor
	}
	commitDeadline := output.Deadline
	if !promotionUntil.IsZero() {
		commitDeadline = minTime(commitDeadline, promotionUntil)
	}
	publication := rediscore.Publication{Identity: selected.Identity, Revision: selected.View.Revision, Deadline: commitDeadline, Application: &lease, CreditDelta: residentSmallBudget, Changes: changes, NewOutputs: []rediscore.OutputIntent{output}, Selectors: selectorChanges, Timers: &rediscore.TimerMutation{Expected: selected.View.Timer, Target: target, Changes: timers}}
	_, err = store.Commit(ctx, publication)
	if errors.Is(err, rediscore.ErrExpired) && selected.View.Timer != (rediscore.TimerAssignment{}) && selected.View.Timer != executor && time.Now().Before(commitDeadline) && ctx.Err() == nil {
		publication.Timers.Target = executor
		_, err = store.Commit(ctx, publication)
	}
	if errors.Is(err, rediscore.ErrExpired) && !promotionUntil.IsZero() && !time.Now().Before(promotionUntil) && time.Now().Before(output.Deadline) && time.Now().Before(leaseUntil) && ctx.Err() == nil {
		return zero, rediscore.ErrConflict
	}
	if err != nil {
		return zero, err
	}
	if ctx.Err() != nil || !time.Now().Before(deadline) {
		return zero, rediscore.ErrUnknown
	}
	return selected.Identity, nil
}
