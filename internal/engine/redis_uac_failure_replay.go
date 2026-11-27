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

// replayRedisUACFailure reuses the immutable SDK ACK only after the previous
// exact ACK attempt has been consumed and its owner reference removed.
func replayRedisUACFailure(ctx context.Context, store *rediscore.Store, identity rediscore.Identity, revision uint64, assignment rediscore.TimerAssignment, current *nativepb.Context, procedureField string, proc nativeProcedureRecord, operation string, message portable.Message, flow *pb.FlowRef, key []byte, executor rediscore.TimerAssignment, deadline time.Time) (bool, error) {
	if proc.Refs.Origin == "" || proc.Refs.LastResponse == "" || proc.Refs.ACK == "" {
		return true, errNativeRecord
	}
	view, err := store.Read(ctx, identity, proc.Refs.Origin, proc.Refs.LastResponse, proc.Refs.ACK)
	if errors.Is(err, rediscore.ErrAbsent) {
		return true, rediscore.ErrConflict
	}
	if err != nil {
		return true, err
	}
	if view.Revision != revision || view.Timer != assignment {
		return true, rediscore.ErrConflict
	}
	memo, err := selectedmessage.NewMemo(4)
	if err != nil {
		return true, err
	}
	message, err = memo.Adopt(message, "admitted")
	if err != nil {
		return true, err
	}
	tx, err := proc.attach(&nativeSelectedMessages{records: view.Records, memo: memo, provenance: "admitted"}, true, true, true)
	if err != nil {
		return true, err
	}
	if !portable.MatchResponse(tx.Origin, message) || !proto.Equal(proc.Metadata.Flow, flow) {
		return true, errors.New("UAC failure replay mismatch")
	}
	attributeNativeCall(ctx, current)
	if tx.Reliable || tx.Phase == portable.Terminated || proc.Metadata.AckEmissions >= stateProfileTransmissions {
		return true, nil
	}
	timer := tx.Timers[portable.TimerD]
	if tx.Phase != portable.Completed || !timer.Active || timer.Generation == 0 || timer.At <= time.Now().UnixMilli() {
		return true, nil
	}
	for _, field := range proc.Metadata.Outputs {
		if strings.HasPrefix(field, "r:uac-ack-output-") {
			return true, nil
		}
	}
	candidate, err := nativeReceiveInitialInviteFailure(tx, message, time.Now().UnixMilli())
	if err != nil {
		return true, err
	}
	nextTimer := candidate.Transaction.Timers[portable.TimerD]
	if candidate.ACK == nil || !candidate.Replay || tx.ACK == nil || !candidate.ACK.Equal(*tx.ACK) || candidate.Transaction.Phase != portable.Completed || nextTimer != timer {
		return true, errNativeRecord
	}
	now := time.Now()
	deadline = minTime(deadline, time.UnixMilli(timer.At))
	until := minTime(deadline, now.Add(2*time.Second))
	if ctx.Err() != nil || !now.Before(until) {
		return true, rediscore.ErrExpired
	}
	if current.Revision >= 1<<53-1 {
		return true, errNativeRecord
	}
	current.Revision++
	snapshot, err := nativeProjectSnapshot(current, nil, nil, nil)
	if err != nil {
		return true, err
	}
	outputField := "r:uac-ack-output-" + uuid.NewString()
	intent, changes, err := prepareRedisUACFailureACK(snapshot, flow, key, *candidate.ACK, procedureField, "", "", proc.Refs.ACK, outputField, operation, until, true, false)
	if err != nil {
		return true, err
	}
	// The selected ACK wire is already retained by the procedure; publish only
	// the new physical metadata rather than copying its wire into a new field.
	if len(changes) != 1 || changes[0].Field != outputField {
		return true, errNativeRecord
	}
	proc.Metadata.AckEmissions++
	proc.Metadata.Outputs = append(proc.Metadata.Outputs, outputField)
	// Output references are ordered even when several other attempts are live.
	slices.Sort(proc.Metadata.Outputs)
	data, err := encodeNativeProcedure(candidate.Transaction, proc.Refs, proc.Metadata)
	if err != nil {
		return true, err
	}
	contextData, err := encodeNativeContext(current)
	if err != nil {
		return true, err
	}
	changes = append(changes, rediscore.Change{Field: procedureField, Value: data}, rediscore.Change{Field: "r:context", Value: contextData})
	target := assignment
	if target == (rediscore.TimerAssignment{}) {
		target = executor
	}
	publication := rediscore.Publication{Identity: identity, Revision: revision, Deadline: deadline, SpendReservation: true, Changes: changes, NewOutputs: []rediscore.OutputIntent{intent}, Timers: &rediscore.TimerMutation{Expected: assignment, Target: target, Changes: []rediscore.TimerChange{{Member: "out-" + strings.TrimPrefix(outputField, "r:"), Due: until}}}}
	_, err = store.Commit(ctx, publication)
	if errors.Is(err, rediscore.ErrExpired) && assignment != (rediscore.TimerAssignment{}) && assignment != executor && time.Now().Before(deadline) && ctx.Err() == nil {
		publication.Timers.Target = executor
		_, err = store.Commit(ctx, publication)
	}
	return true, err
}
