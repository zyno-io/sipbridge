package engine

import (
	"context"
	"errors"
	"slices"
	"strings"
	"time"

	"github.com/emiago/sipgo/sip/portable"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/nativepb"
	"sipbridge/internal/rediscore"
	"sipbridge/internal/selectedmessage"
)

// receiveRedisRetainedSuccessfulACK follows one actual previous InviteClient
// after its dialog association moved. It never changes the new dialog or any
// command receipt and cannot extend the original transaction M.
func receiveRedisRetainedSuccessfulACK(ctx context.Context, store *rediscore.Store, identity rediscore.Identity, revision uint64, assignment rediscore.TimerAssignment, current *nativepb.Context, procedureField string, procedure nativeProcedureRecord, flow *pb.FlowRef, message portable.Message, key []byte, executor rediscore.TimerAssignment, leaseUntil, deadline time.Time) (bool, error) {
	retained := procedure.Metadata.SuccessfulAck
	if retained == nil {
		return false, nil
	}
	if store == nil || current == nil || current.Context == nil || current.Context.State == nil || !proto.Equal(procedure.Metadata.Flow, flow) || leaseUntil.IsZero() || leaseUntil == leaseUntil.Round(0) || !time.Now().Before(leaseUntil) || !time.Now().Before(deadline) || !validNativeSuccessfulACK(retained) {
		return true, errNativeRecord
	}
	fields := []string{procedure.Refs.Origin, retained.FinalResponse, retained.Ack}
	for _, field := range []string{procedure.Refs.LastResponse, procedure.Refs.ACK} {
		if field != "" && !slices.Contains(fields, field) {
			fields = append(fields, field)
		}
	}
	view, err := store.Read(ctx, identity, fields...)
	if err != nil {
		return true, err
	}
	if view.Revision != revision || view.Timer != assignment {
		return true, rediscore.ErrConflict
	}
	memo, err := selectedmessage.NewMemo(len(fields) + 1)
	if err != nil {
		return true, err
	}
	message, err = memo.Adopt(message, "admitted")
	if err != nil {
		return true, err
	}
	messages := &nativeSelectedMessages{records: view.Records, memo: memo, provenance: "admitted"}
	final, err := messages.get(retained.FinalResponse)
	if err != nil {
		return true, err
	}
	incomingFacts, err := message.Facts()
	if err != nil {
		return true, err
	}
	finalFacts, err := final.Facts()
	if err != nil {
		return true, err
	}
	if incomingFacts.CallID != finalFacts.CallID || incomingFacts.FromTag != finalFacts.FromTag || incomingFacts.ToTag != finalFacts.ToTag {
		return false, nil
	}
	if !message.EqualWire(final) {
		return true, errNativeRecord
	}
	attributeNativeCall(ctx, current)
	if procedure.Native.Phase == portable.Terminated {
		return true, nil
	}
	if procedure.Native.Phase != portable.Accepted {
		return true, errNativeRecord
	}
	m := procedure.Native.Timers[portable.TimerM]
	if !m.Active || m.At <= 0 {
		return true, errNativeRecord
	}
	offset := m.At - leaseUntil.UnixMilli()
	if offset > int64((24*time.Hour)/time.Millisecond) || offset < -int64((24*time.Hour)/time.Millisecond) {
		return true, errNativeRecord
	}
	mUntil := nativeUDPRetryDeadline(leaseUntil, m.At)
	remaining := time.Until(mUntil)
	if remaining <= 0 {
		return true, nil
	}
	if retained.Emissions >= stateProfileTransmissions {
		return true, nil
	}
	tx, err := procedure.attach(messages, true, procedure.Refs.LastResponse != "", procedure.Refs.ACK != "")
	if err != nil {
		return true, err
	}
	ack, err := messages.get(retained.Ack)
	if err != nil {
		return true, err
	}
	logicalNow := m.At - int64((remaining+time.Millisecond-1)/time.Millisecond)
	next, _, err := nativeSuccessfulACKReplay(tx, message, final, ack, logicalNow)
	if err != nil {
		return true, err
	}
	release, closeTimers, pending, err := planRedisConsumedACKReplayOutputs(ctx, store, identity, revision, assignment, current, procedureField, procedure, "", nil, ack, retained.Ack, retained.Flow, nil)
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
	outputField := "r:retained-ack-output-" + uuid.NewString()
	intent, changes, err := prepareRedisUACFailureACK(snapshot, retained.Flow, key, ack, procedureField, "", "", retained.Ack, outputField, "", until, true, false)
	if err != nil {
		return true, err
	}
	if len(changes) != 1 || changes[0].Field != outputField {
		return true, errNativeRecord
	}
	retained.Emissions++
	procedure.Metadata.Outputs = append(procedure.Metadata.Outputs, outputField)
	slices.Sort(procedure.Metadata.Outputs)
	procedureData, err := encodeNativeProcedure(next, procedure.Refs, procedure.Metadata)
	if err != nil {
		return true, err
	}
	changes = append(changes, release...)
	changes = append(changes, rediscore.Change{Field: procedureField, Value: procedureData})
	target := assignment
	if target == (rediscore.TimerAssignment{}) {
		target = executor
	}
	closeTimers = append(closeTimers, rediscore.TimerChange{Member: "out-" + strings.TrimPrefix(outputField, "r:"), Due: until})
	publication := rediscore.Publication{Identity: identity, Revision: revision, Deadline: until, SpendReservation: true, Changes: changes, NewOutputs: []rediscore.OutputIntent{intent}, Timers: &rediscore.TimerMutation{Expected: assignment, Target: target, Changes: closeTimers}}
	_, err = store.Commit(ctx, publication)
	if errors.Is(err, rediscore.ErrExpired) && assignment != (rediscore.TimerAssignment{}) && assignment != executor && ctx.Err() == nil && time.Now().Before(until) {
		publication.Timers.Target = executor
		_, err = store.Commit(ctx, publication)
	}
	if errors.Is(err, rediscore.ErrExpired) && ctx.Err() == nil && time.Now().Before(deadline) && !time.Now().Before(mUntil) {
		return true, nil
	}
	return true, err
}
