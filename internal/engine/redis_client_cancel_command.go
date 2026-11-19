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
	"sipbridge/internal/storagecodec"
)

// publishRedisClientCancel atomically records the command and the native
// INVITE cancellation transition. Calling deliberately creates no child until
// the SDK emits the paired CANCEL on a later provisional response.
func publishRedisClientCancel(ctx context.Context, store *rediscore.Store, key []byte, application *pb.ProcessRef, lease rediscore.ApplicationLeaseRef, command *pb.Command, capture redisNotifyWaitCapture, operation string, executor rediscore.TimerAssignment, deadline time.Time) (*pb.OperationResult, error) {
	if store == nil || command == nil || command.GetCancel() == nil || command.Context == nil || command.Context.State == nil || capture.sequence != command.Sequence || !proto.Equal(capture.state, command.Context.State) || command.Sequence < 2 || command.Sequence > 1<<53-1 || !validSendText(operation, 160, false) || !time.Now().Before(deadline) {
		return nil, errNativeRecord
	}
	app, err := nativeApplicationReference(key, application)
	if err != nil || app != lease.Application {
		return nil, rediscore.ErrAuthority
	}
	resultField := "r:result-" + strconv.FormatUint(command.Sequence, 10)
	priorField := "r:result-" + strconv.FormatUint(command.Sequence-1, 10)
	fieldsToSelect := []string{"r:context", resultField, priorField}
	if priorField != "r:result-1" {
		fieldsToSelect = append(fieldsToSelect, "r:result-1")
	}
	selected, err := store.ObserveApplicationContext(ctx, command.Context.Id, command.Context.State.Incarnation, lease, fieldsToSelect...)
	if err != nil {
		return nil, err
	}
	current, err := decodeNativeContext(selected.View.Records["r:context"])
	if err != nil {
		return nil, err
	}
	if selected.Identity.Owner != lease.Application || selected.Identity.Term != lease.Term || current.Context == nil || !proto.Equal(current.Context.State, capture.state) || current.Kind != pb.ContextSnapshot_UAS && current.Kind != pb.ContextSnapshot_UAC || !sameProcess(current.Application, application) || current.ApplicationLeaseTerm != lease.Term || current.CommandSequence+1 != command.Sequence || current.Revision >= 1<<53-1 || current.Ended || len(selected.View.Records[resultField]) != 0 {
		return nil, rediscore.ErrConflict
	}
	snapshot, err := nativeProjectSnapshot(current, nil, nil, nil)
	if err != nil {
		return nil, err
	}
	now := time.Now()
	if err = validateNativeCommandPolicy(snapshot, command, operation, now, deadline); err != nil {
		return nil, err
	}
	var parentField string
	var parent nativeProcedureRecord
	challenged := false
	if current.Answered {
		tags := []string{current.LocalTag, current.RemoteTag}
		slices.Sort(tags)
		dialogSelection, e := store.LookupSelector(ctx, nativeSelector(rediscore.SelectorDialog, current.CallId, tags[0], tags[1]))
		if e != nil {
			return nil, e
		}
		if dialogSelection.Identity != selected.Identity || dialogSelection.View.Revision != selected.View.Revision || dialogSelection.View.Timer != selected.View.Timer {
			return nil, rediscore.ErrConflict
		}
		if !slices.Contains(current.Dialogs, dialogSelection.Record) {
			return nil, errNativeRecord
		}
		dialog, e := decodeNativeDialog(dialogSelection.View.Records[dialogSelection.Record])
		if e != nil {
			return nil, e
		}
		if dialog.Native.CallID != current.CallId || dialog.Native.LocalTag != current.LocalTag || dialog.Native.RemoteTag != current.RemoteTag {
			return nil, errNativeRecord
		}
		if dialog.Native.LocalInvite == nil {
			return refuseRedisClientCancel(ctx, store, selected, current, lease, command, capture, operation, executor, deadline, errors.New("cancellable INVITE unavailable"))
		}
		if dialog.Native.LocalInvite.State != portable.InvitePending {
			return publishRedisCompletedInviteCancel(ctx, store, key, selected, current, dialogSelection.Record, dialog, lease, command, capture, operation, executor, deadline)
		}
		if dialog.Metadata.LocalTransaction == "" || dialog.Refs.Local == nil || dialog.Refs.Local.Request == "" {
			return nil, errNativeRecord
		}
		transaction, e := store.LookupSelector(ctx, nativeSelector(rediscore.SelectorClientTransaction, dialog.Metadata.LocalTransaction))
		if e != nil {
			return nil, e
		}
		if transaction.Identity != selected.Identity || transaction.View.Revision != selected.View.Revision || transaction.View.Timer != selected.View.Timer {
			return nil, rediscore.ErrConflict
		}
		parentField = transaction.Record
		if !slices.Contains(current.NoninitialProcedures, parentField) || !strings.HasPrefix(parentField, "r:app-invite-") {
			return nil, errNativeRecord
		}
		parent, e = decodeNativeProcedure(transaction.View.Records[parentField])
		if e != nil {
			return nil, e
		}
		if parent.Native.Version != portable.Version || parent.Native.Family != portable.InviteClient || parent.Native.Key != dialog.Metadata.LocalTransaction || parent.Refs.Origin != dialog.Refs.Local.Request || parent.Metadata.CreditContribution == 0 || parent.Metadata.Root != "" {
			return nil, errNativeRecord
		}
	} else {
		if len(current.InitialProcedures) == 0 {
			return refuseRedisClientCancel(ctx, store, selected, current, lease, command, capture, operation, executor, deadline, errors.New("cancellable INVITE unavailable"))
		}
		selectedProcs, e := store.Read(ctx, selected.Identity, current.InitialProcedures...)
		if e != nil {
			return nil, e
		}
		if selectedProcs.Revision != selected.View.Revision || selectedProcs.Timer != selected.View.Timer {
			return nil, rediscore.ErrConflict
		}
		var challengeField string
		var challenge nativeProcedureRecord
		var highest uint32
		for _, field := range current.InitialProcedures {
			proc, x := decodeNativeProcedure(selectedProcs.Records[field])
			if x != nil {
				return nil, x
			}
			if proc.Native.Family != portable.InviteClient {
				return nil, errNativeRecord
			}
			highest = max(highest, proc.Metadata.DigestAttempts)
			if proc.Native.Phase == portable.Calling || proc.Native.Phase == portable.Proceeding {
				if parentField != "" {
					return refuseRedisClientCancel(ctx, store, selected, current, lease, command, capture, operation, executor, deadline, errors.New("multiple active INVITE transactions"))
				}
				parentField, parent = field, proc
			} else if proc.Metadata.DigestHandled && proc.Metadata.DigestAttempts > 0 && (proc.Native.Phase == portable.Completed || proc.Native.Phase == portable.Terminated) {
				if challengeField == "" || proc.Metadata.DigestAttempts > challenge.Metadata.DigestAttempts {
					challengeField, challenge = field, proc
				} else if proc.Metadata.DigestAttempts == challenge.Metadata.DigestAttempts {
					return nil, errNativeRecord
				}
			}
		}
		if parentField != "" && parent.Metadata.DigestAttempts < highest {
			return nil, errNativeRecord
		}
		if parentField == "" && challengeField != "" && challenge.Metadata.DigestAttempts == highest {
			parentField, parent, challenged = challengeField, challenge, true
		}
		if parentField == "" {
			return refuseRedisClientCancel(ctx, store, selected, current, lease, command, capture, operation, executor, deadline, errors.New("cancellable INVITE unavailable"))
		}
		if _, _, err = selectRedisUACRoot(ctx, store, selected.Identity, selected.View.Revision, selected.View.Timer, current, parentField, parent); err != nil {
			return nil, err
		}
	}
	fields := []string{parent.Refs.Origin}
	if parent.Refs.LastResponse != "" {
		fields = append(fields, parent.Refs.LastResponse)
	}
	if parent.Refs.ACK != "" {
		fields = append(fields, parent.Refs.ACK)
	}
	originView, err := store.Read(ctx, selected.Identity, fields...)
	if err != nil {
		return nil, err
	}
	if originView.Revision != selected.View.Revision || originView.Timer != selected.View.Timer {
		return nil, rediscore.ErrConflict
	}
	memo, err := selectedmessage.NewMemo(len(fields))
	if err != nil {
		return nil, err
	}
	tx, err := parent.attach(&nativeSelectedMessages{records: originView.Records, memo: memo, provenance: "admitted"}, true, parent.Refs.LastResponse != "", parent.Refs.ACK != "")
	if err != nil {
		return nil, err
	}
	if challenged {
		if tx.Cancelled || tx.LastResponse == nil || parent.Metadata.Credential == nil || parent.Metadata.CommandResult != "r:result-1" {
			return nil, errNativeRecord
		}
		facts, e := tx.LastResponse.Facts()
		if e != nil || facts.IsRequest || facts.Status != 401 && facts.Status != 407 {
			return nil, errNativeRecord
		}
	} else if tx.Phase != portable.Calling && tx.Phase != portable.Proceeding {
		return nil, errNativeRecord
	}
	next, effects, _, _, err := nativeRequestClientCancel(tx, now.UnixMilli())
	if err != nil {
		return nil, err
	}
	parentRecord, err := buildPrivateProcedureRecord(next, parent.Refs, parent.Metadata)
	if err != nil {
		return nil, err
	}
	child, err := prepareRedisPairedClientCancel(ctx, store, selected.Identity, selected.View.Revision, selected.View.Timer, current, parentField, parentRecord, next, effects, key, deadline)
	if err != nil {
		return nil, err
	}
	nextContext := proto.Clone(current).(*nativepb.Context)
	nextContext.CommandSequence = command.Sequence
	nextContext.Revision++
	if challenged {
		nextContext.Ended = true
		if nextContext.CallRetireAtMs == 0 {
			nextContext.CallRetireAtMs = now.Add(2 * time.Minute).UnixMilli()
		}
	}
	if command.Admission != nil {
		nextContext.DirectoryEpoch = command.Admission.DirectoryEpoch
	}
	if child.field != "" {
		nextContext.NoninitialProcedures = append(nextContext.NoninitialProcedures, child.field)
		slices.Sort(nextContext.NoninitialProcedures)
	}
	contextData, err := encodeNativeContext(nextContext)
	if err != nil {
		return nil, err
	}
	parentData, err := marshalNativeRecord(storagecodec.NativeProcedure, parentRecord)
	if err != nil {
		return nil, err
	}
	receipt := &nativepb.Result{Certainty: pb.OperationResult_ACCEPTED, CutoffMs: deadline.UnixMilli(), Sequence: command.Sequence, OperationId: operation, Fingerprint: capture.fingerprint[:], DecidingLeaseTerm: lease.Term}
	resultData, err := encodeNativeResult(receipt)
	if err != nil {
		return nil, err
	}
	prior, err := decodeNativeResult(selected.View.Records[priorField])
	if err != nil || prior.Sequence+1 != command.Sequence {
		return nil, errNativeRecord
	}
	timers, err := planRedisDialogPriorReceipt(ctx, store, selected, current, prior)
	if err != nil {
		return nil, err
	}
	for _, effect := range effects {
		switch effect.Kind {
		case portable.StartTransaction:
		case portable.CancelTimer, portable.Schedule:
			if effect.Timer == portable.TimerA {
				continue
			}
			prefix, id := "call-tx-", strings.TrimPrefix(parentField, "r:uac-procedure-")
			if strings.HasPrefix(parentField, "r:app-invite-") {
				prefix, id = "app-invite-", strings.TrimPrefix(parentField, "r:app-invite-")
			}
			if len(id) != 32 {
				return nil, errNativeRecord
			}
			change := rediscore.TimerChange{Member: prefix + id + "-" + string(effect.Timer), Delete: effect.Kind == portable.CancelTimer}
			if !change.Delete {
				change.Due = time.UnixMilli(effect.Deadline.At)
			}
			timers = append(timers, change)
		default:
			return nil, errNativeRecord
		}
	}
	timers = append(timers, child.timers...)
	changes := []rediscore.Change{{Field: "r:context", Value: contextData}, {Field: parentField, Value: parentData}, {Field: resultField, Value: resultData}}
	changes = append(changes, child.changes...)
	if challenged {
		original, e := decodeNativeResult(selected.View.Records["r:result-1"])
		if e != nil || original.Sequence != 1 || original.OperationId == "" {
			return nil, errNativeRecord
		}
		eventSnapshot, e := nativeProjectSnapshot(nextContext, nil, nil, nil)
		if e != nil {
			return nil, e
		}
		eventID := uuid.NewString()
		event, e := encodeNativeLifecycle(&nativepb.Lifecycle{Kind: pb.DialogEvent_FAILED, Snapshot: eventSnapshot, OperationId: original.OperationId, DeadlineMs: now.Add(2 * time.Minute).UnixMilli(), NextMs: now.UnixMilli()})
		if e != nil {
			return nil, e
		}
		changes = append(changes, rediscore.Change{Field: "r:event-" + eventID, Value: event})
		timers = append(timers, rediscore.TimerChange{Member: "event-" + eventID, Due: now}, rediscore.TimerChange{Member: "call-retire", Due: time.UnixMilli(nextContext.CallRetireAtMs)})
	}
	target := selected.View.Timer
	if target == (rediscore.TimerAssignment{}) {
		target = executor
	}
	until := deadline
	if command.Admission != nil {
		until = minTime(until, time.UnixMilli(int64(command.Admission.ExpiresAtMs)))
	}
	publication := rediscore.Publication{Identity: selected.Identity, Revision: selected.View.Revision, Deadline: until, Application: &lease, SpendReservation: true, Changes: changes, NewOutputs: child.outputs, Selectors: child.selectors}
	if len(timers) != 0 {
		publication.Timers = &rediscore.TimerMutation{Expected: selected.View.Timer, Target: target, Changes: timers}
	}
	_, err = store.Commit(ctx, publication)
	if errors.Is(err, rediscore.ErrExpired) && publication.Timers != nil && selected.View.Timer != (rediscore.TimerAssignment{}) && selected.View.Timer != executor && ctx.Err() == nil && time.Now().Before(until) {
		publication.Timers.Target = executor
		_, err = store.Commit(ctx, publication)
	}
	if err != nil {
		return nil, err
	}
	if ctx.Err() != nil || !time.Now().Before(deadline) {
		return nil, rediscore.ErrUnknown
	}
	public, err := nativeProjectSnapshot(nextContext, nil, nil, nil)
	if err != nil {
		return nil, err
	}
	return &pb.OperationResult{Certainty: pb.OperationResult_ACCEPTED, Snapshot: public}, nil
}

// refuseRedisClientCancel consumes an authorized command without creating
// native state or an output, matching Memory's semantic refusal.
func refuseRedisClientCancel(ctx context.Context, store *rediscore.Store, selected *rediscore.ObservedContext, current *nativepb.Context, lease rediscore.ApplicationLeaseRef, command *pb.Command, capture redisNotifyWaitCapture, operation string, executor rediscore.TimerAssignment, deadline time.Time, cause error) (*pb.OperationResult, error) {
	if store == nil || selected == nil || selected.View == nil || current == nil || command == nil || cause == nil {
		return nil, errNativeRecord
	}
	prior, err := decodeNativeResult(selected.View.Records["r:result-"+strconv.FormatUint(command.Sequence-1, 10)])
	if err != nil || prior.Sequence+1 != command.Sequence {
		return nil, errNativeRecord
	}
	timers, err := planRedisDialogPriorReceipt(ctx, store, selected, current, prior)
	if err != nil {
		return nil, err
	}
	next := proto.Clone(current).(*nativepb.Context)
	next.CommandSequence = command.Sequence
	next.Revision++
	until := deadline
	if command.Admission != nil {
		next.DirectoryEpoch = command.Admission.DirectoryEpoch
		until = minTime(until, time.UnixMilli(int64(command.Admission.ExpiresAtMs)))
	}
	if !time.Now().Before(until) || ctx.Err() != nil {
		return nil, rediscore.ErrExpired
	}
	refusal := commandRefusal(cause)
	refusal.Error = residentCommandError(refusal.Error)
	receipt, err := encodeNativeResult(&nativepb.Result{Certainty: refusal.Certainty, Error: refusal.Error, SipStatus: refusal.SipStatus, CutoffMs: deadline.UnixMilli(), Sequence: command.Sequence, OperationId: operation, Fingerprint: capture.fingerprint[:], DecidingLeaseTerm: lease.Term})
	if err != nil {
		return nil, err
	}
	contextData, err := encodeNativeContext(next)
	if err != nil {
		return nil, err
	}
	publication := rediscore.Publication{Identity: selected.Identity, Revision: selected.View.Revision, Deadline: until, Application: &lease, SpendReservation: true, Changes: []rediscore.Change{{Field: "r:context", Value: contextData}, {Field: "r:result-" + strconv.FormatUint(command.Sequence, 10), Value: receipt}}}
	if len(timers) != 0 {
		target := selected.View.Timer
		if target == (rediscore.TimerAssignment{}) {
			target = executor
		}
		publication.Timers = &rediscore.TimerMutation{Expected: selected.View.Timer, Target: target, Changes: timers}
	}
	_, err = store.Commit(ctx, publication)
	if errors.Is(err, rediscore.ErrExpired) && publication.Timers != nil && selected.View.Timer != (rediscore.TimerAssignment{}) && selected.View.Timer != executor && ctx.Err() == nil && time.Now().Before(until) {
		publication.Timers.Target = executor
		_, err = store.Commit(ctx, publication)
	}
	if err != nil {
		return nil, err
	}
	if ctx.Err() != nil || !time.Now().Before(deadline) {
		return nil, rediscore.ErrUnknown
	}
	snapshot, err := nativeProjectSnapshot(next, nil, nil, nil)
	if err != nil {
		return nil, err
	}
	refusal.Snapshot = snapshot
	return refusal, nil
}
