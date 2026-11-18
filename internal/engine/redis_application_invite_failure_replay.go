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
func replayRedisPreviousInviteFailure(ctx context.Context, store *rediscore.Store, identity rediscore.Identity, revision uint64, assignment rediscore.TimerAssignment, current *nativepb.Context, procedureField string, proc nativeProcedureRecord, operation string, message portable.Message, flow *pb.FlowRef, key []byte, executor rediscore.TimerAssignment, leaseUntil, deadline time.Time) (bool, error) {
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
	if validateRedisNotifyResponseMetadata(message, flow) != nil || !portable.MatchResponse(tx.Origin, message) || tx.LastResponse == nil || !tx.LastResponse.EqualWire(message) || !proto.Equal(proc.Metadata.Flow, flow) {
		return true, errors.New("UAC failure replay mismatch")
	}
	if tx.Reliable || tx.Phase == portable.Terminated || proc.Metadata.AckEmissions >= stateProfileTransmissions {
		return true, nil
	}
	timer := tx.Timers[portable.TimerD]
	if leaseUntil.IsZero() || leaseUntil == leaseUntil.Round(0) || !time.Now().Before(leaseUntil) {
		return true, rediscore.ErrAuthority
	}
	if tx.Phase != portable.Completed || !timer.Active || timer.Generation == 0 || timer.At-leaseUntil.UnixMilli() > int64((24*time.Hour)/time.Millisecond) || timer.At-leaseUntil.UnixMilli() < -int64((24*time.Hour)/time.Millisecond) || time.Until(nativeUDPRetryDeadline(leaseUntil, timer.At)) <= 0 {
		return true, nil
	}
	for _, field := range proc.Metadata.Outputs {
		if strings.HasPrefix(field, "r:app-invite-ack-output-") {
			return true, nil
		}
	}
	dUntil := nativeUDPRetryDeadline(leaseUntil, timer.At)
	logicalNow := timer.At - int64((time.Until(dUntil)+time.Millisecond-1)/time.Millisecond)
	next, effects, err := portable.StepTransaction(tx, portable.Input{Kind: portable.ReceiveResponse, Message: message, Now: logicalNow})
	if err != nil {
		return true, err
	}
	var ack *portable.Message
	for _, effect := range effects {
		if effect.Kind == portable.Send {
			if effect.Message == nil || !effect.Replay || ack != nil {
				return true, errNativeRecord
			}
			ack = effect.Message
		} else if effect.Kind != portable.Diagnostic && effect.Kind != portable.ReportResponse {
			return true, errNativeRecord
		}
	}
	nextTimer := next.Timers[portable.TimerD]
	if ack == nil || tx.ACK == nil || !ack.Equal(*tx.ACK) || next.Phase != portable.Completed || nextTimer != timer {
		return true, errNativeRecord
	}
	now := time.Now()
	deadline = minTime(deadline, dUntil)
	until := minTime(deadline, now.Add(2*time.Second))
	if ctx.Err() != nil || !now.Before(until) {
		return true, rediscore.ErrExpired
	}
	snapshot, err := nativeProjectSnapshot(current, nil, nil, nil)
	if err != nil {
		return true, err
	}
	outputField := "r:app-invite-ack-output-" + uuid.NewString()
	intent, changes, err := prepareRedisUACFailureACK(snapshot, flow, key, *ack, procedureField, "", "", proc.Refs.ACK, outputField, operation, until, true, false)
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
	data, err := encodeNativeProcedure(next, proc.Refs, proc.Metadata)
	if err != nil {
		return true, err
	}
	changes = append(changes, rediscore.Change{Field: procedureField, Value: data})
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
