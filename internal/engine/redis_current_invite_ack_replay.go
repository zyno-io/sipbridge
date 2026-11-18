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

// replayRedisCurrentInviteACK uses only the still-selected dialog ACK and the
// SDK's unchanged original M. It does not alter BYE, SDP or command outcomes.
func replayRedisCurrentInviteACK(ctx context.Context, store *rediscore.Store, selected *rediscore.SelectorLookup, current *nativepb.Context, proc nativeProcedureRecord, flow *pb.FlowRef, response portable.Message, key []byte, executor rediscore.TimerAssignment, leaseUntil, deadline time.Time) (bool, error) {
	if selected == nil || current == nil || current.Context == nil || current.Context.State == nil || current.Context.Id != selected.Identity.Context || current.Context.State.Incarnation != selected.Identity.Incarnation || current.Kind != pb.ContextSnapshot_UAC && current.Kind != pb.ContextSnapshot_UAS || current.CallId == "" || current.LocalTag == "" || current.RemoteTag == "" || proc.Native.Phase != portable.Accepted || proc.Native.Family != portable.InviteClient || proc.Refs.Origin == "" || proc.Metadata.CommandResult == "" || !slices.Contains(current.NoninitialProcedures, selected.Record) || !proto.Equal(proc.Metadata.Flow, flow) || validateRedisNotifyResponseMetadata(response, flow) != nil {
		return true, errNativeRecord
	}
	tags := []string{current.LocalTag, current.RemoteTag}
	slices.Sort(tags)
	selection, err := store.LookupSelector(ctx, nativeSelector(rediscore.SelectorDialog, current.CallId, tags[0], tags[1]))
	if err != nil {
		return true, err
	}
	if selection.Identity != selected.Identity || selection.View.Revision != selected.View.Revision || selection.View.Timer != selected.View.Timer {
		return true, rediscore.ErrConflict
	}
	if !slices.Contains(current.Dialogs, selection.Record) {
		return true, errNativeRecord
	}
	dialog, err := readPrivateDialogRecord(selection.View.Records[selection.Record])
	if err != nil {
		return true, err
	}
	if dialog.Native.Version != portable.Version || dialog.Native.CallId != current.CallId || dialog.Native.LocalTag != current.LocalTag || dialog.Native.RemoteTag != current.RemoteTag || dialog.Native.LocalInvite == nil || dialog.LocalInvite == nil || dialog.LocalInvite.Request != proc.Refs.Origin || dialog.LocalInvite.FinalResponse == "" {
		return true, errNativeRecord
	}
	fields := []string{proc.Refs.Origin, dialog.LocalInvite.FinalResponse, proc.Metadata.CommandResult}
	for _, f := range []string{proc.Refs.LastResponse, proc.Refs.ACK, dialog.LocalInvite.Ack} {
		if f != "" && !slices.Contains(fields, f) {
			fields = append(fields, f)
		}
	}
	view, err := store.Read(ctx, selected.Identity, fields...)
	if err != nil {
		return true, err
	}
	if view.Revision != selected.View.Revision || view.Timer != selected.View.Timer {
		return true, rediscore.ErrConflict
	}
	memo, err := selectedmessage.NewMemo(len(fields) + 1)
	if err != nil {
		return true, err
	}
	response, err = memo.Adopt(response, "admitted")
	if err != nil {
		return true, err
	}
	messages := &nativeSelectedMessages{records: view.Records, memo: memo, provenance: "admitted"}
	final, err := messages.get(dialog.LocalInvite.FinalResponse)
	if err != nil {
		return true, err
	}
	if !response.EqualWire(final) {
		return true, errNativeRecord
	}
	if dialog.LocalInvite.Ack == "" {
		return true, nil
	} // a remote offer still awaits its application answer
	if !dialog.AckWritten {
		for _, field := range proc.Metadata.Outputs {
			if strings.HasPrefix(field, "r:app-invite-ack-output-") || strings.HasPrefix(field, "r:app-ack-output-") {
				return true, nil
			}
		}
	}
	if !dialog.AckWritten || dialog.Native.LocalInvite.State != string(portable.InviteAcked) || !validPolicyFlow(dialog.CompletionFlow) || dialog.AckEmissions == 0 {
		return true, errNativeRecord
	}
	if dialog.AckEmissions >= stateProfileTransmissions {
		return true, nil
	}
	timer := proc.Native.Timers[portable.TimerM]
	if !timer.Active || timer.Generation == 0 || leaseUntil.IsZero() || leaseUntil == leaseUntil.Round(0) || !time.Now().Before(leaseUntil) {
		return true, errNativeRecord
	}
	offset := timer.At - leaseUntil.UnixMilli()
	if offset > int64((24*time.Hour)/time.Millisecond) || offset < -int64((24*time.Hour)/time.Millisecond) {
		return true, errNativeRecord
	}
	mUntil := nativeUDPRetryDeadline(leaseUntil, timer.At)
	remaining := time.Until(mUntil)
	if remaining <= 0 {
		return true, nil
	}
	tx, err := proc.attach(messages, true, proc.Refs.LastResponse != "", proc.Refs.ACK != "")
	if err != nil {
		return true, err
	}
	ack, err := messages.get(dialog.LocalInvite.Ack)
	if err != nil {
		return true, err
	}
	logicalNow := timer.At - int64((remaining+time.Millisecond-1)/time.Millisecond)
	next, _, err := nativeSuccessfulACKReplay(tx, response, final, ack, logicalNow)
	if err != nil {
		return true, err
	}
	result, err := decodeNativeResult(view.Records[proc.Metadata.CommandResult])
	if err != nil || result.Sequence == 0 || result.Sequence > current.CommandSequence || proc.Metadata.CommandResult != "r:result-"+strconv.FormatUint(result.Sequence, 10) {
		return true, errNativeRecord
	}
	id := strings.TrimPrefix(selected.Record, "r:app-invite-")
	expected, err := redisApplicationByeID(result.Fingerprint)
	if err != nil || expected != id || !validRedisApplicationInviteDialogACKWire(id, dialog.LocalInvite.Ack) {
		return true, errNativeRecord
	}
	release, closeTimers, pending, err := planRedisConsumedACKReplayOutputs(ctx, store, selected.Identity, selected.View.Revision, selected.View.Timer, current, selected.Record, proc, selection.Record, redisACKReplayDialogFromPrivate(dialog), ack, dialog.LocalInvite.Ack, dialog.CompletionFlow, result)
	if err != nil || pending {
		return true, err
	}
	until := minTime(minTime(deadline, time.Now().Add(2*time.Second)), mUntil)
	if ctx.Err() != nil || !time.Now().Before(until) {
		return true, rediscore.ErrExpired
	}
	snapshot, err := nativeProjectSnapshot(current, nil, nil, nil)
	if err != nil {
		return true, err
	}
	field := "r:app-invite-ack-output-" + uuid.NewString()
	intent, changes, err := prepareRedisUACFailureACK(snapshot, dialog.CompletionFlow, key, ack, selected.Record, selection.Record, "", dialog.LocalInvite.Ack, field, result.OperationId, until, true, false)
	if err != nil {
		return true, err
	}
	if len(changes) != 1 || changes[0].Field != field {
		return true, errNativeRecord
	}
	dialog.AckEmissions++
	dialog.Outputs = append(dialog.Outputs, field)
	slices.Sort(dialog.Outputs)
	proc.Metadata.Outputs = append(proc.Metadata.Outputs, field)
	slices.Sort(proc.Metadata.Outputs)
	pdata, err := encodeNativeProcedure(next, proc.Refs, proc.Metadata)
	if err != nil {
		return true, err
	}
	ddata, err := marshalNativeRecord(storagecodec.NativeDialog, dialog)
	if err != nil {
		return true, err
	}
	changes = append(changes, release...)
	changes = append(changes, rediscore.Change{Field: selected.Record, Value: pdata}, rediscore.Change{Field: selection.Record, Value: ddata})
	target := selected.View.Timer
	if target == (rediscore.TimerAssignment{}) {
		target = executor
	}
	closeTimers = append(closeTimers, rediscore.TimerChange{Member: "out-" + strings.TrimPrefix(field, "r:"), Due: until})
	publication := rediscore.Publication{Identity: selected.Identity, Revision: view.Revision, Deadline: until, SpendReservation: true, Changes: changes, NewOutputs: []rediscore.OutputIntent{intent}, Timers: &rediscore.TimerMutation{Expected: selected.View.Timer, Target: target, Changes: closeTimers}}
	_, err = store.Commit(ctx, publication)
	if errors.Is(err, rediscore.ErrExpired) && selected.View.Timer != (rediscore.TimerAssignment{}) && selected.View.Timer != executor && ctx.Err() == nil && time.Now().Before(until) {
		publication.Timers.Target = executor
		_, err = store.Commit(ctx, publication)
	}
	if errors.Is(err, rediscore.ErrExpired) && ctx.Err() == nil && time.Now().Before(deadline) && !time.Now().Before(mUntil) {
		return true, nil
	}
	return true, err
}
