package engine

import (
	"context"
	"errors"
	"slices"
	"strings"
	"time"

	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/rediscore"
	"sipbridge/internal/selectedmessage"
)

// receiveRedisClientCancelResponse advances only the selected paired CANCEL.
// It neither changes the INVITE result nor manufactures a dialog event.
func receiveRedisClientCancelResponse(ctx context.Context, store *rediscore.Store, flow *pb.FlowRef, message portable.Message, key []byte, executor rediscore.TimerAssignment, deadline time.Time) (bool, error) {
	facts, err := message.Facts()
	if err != nil {
		return false, err
	}
	if facts.IsRequest || facts.CSeqMethod != sip.CANCEL {
		return false, nil
	}
	if store == nil || validateRedisNotifyResponseMetadata(message, flow) != nil || !time.Now().Before(deadline) {
		return true, errNativeRecord
	}
	ids, err := nativeSelectorIDs(message, false, false)
	if err != nil || len(ids) == 0 || ids[0].Kind != rediscore.SelectorClientTransaction {
		return true, errNativeRecord
	}
	selected, err := store.LookupSelector(ctx, ids[0], "r:context")
	if errors.Is(err, rediscore.ErrAbsent) {
		return false, nil
	}
	if err != nil {
		return true, err
	}
	if !strings.HasPrefix(selected.Record, "r:cancel-client-") {
		return false, nil
	}
	id := strings.TrimPrefix(selected.Record, "r:cancel-client-")
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
	if current.Context == nil || current.Context.State == nil || current.Context.Id != selected.Identity.Context || current.Context.State.Incarnation != selected.Identity.Incarnation || current.Kind != pb.ContextSnapshot_UAS && current.Kind != pb.ContextSnapshot_UAC || current.CallId != facts.CallID || !slices.Contains(current.NoninitialProcedures, selected.Record) || proc.Native.Version != portable.Version || proc.Native.Family != portable.NonInviteClient || !proc.Metadata.Cleanup || proc.Metadata.CommandResult != "" || proc.Metadata.CreditContribution != 0 || proc.Refs.Origin != "r:cancel-client-origin-"+id || proc.Refs.LastResponse != "" && proc.Refs.LastResponse != "r:cancel-client-response-"+id || !proto.Equal(proc.Metadata.Flow, flow) {
		return true, errNativeRecord
	}
	fields := []string{proc.Refs.Origin}
	if proc.Refs.LastResponse != "" {
		fields = append(fields, proc.Refs.LastResponse)
	}
	view, err := store.Read(ctx, selected.Identity, fields...)
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
	tx, err := proc.attach(&nativeSelectedMessages{records: view.Records, memo: memo, provenance: "admitted"}, true, proc.Refs.LastResponse != "", false)
	if err != nil || !portable.MatchResponse(tx.Origin, message) {
		return true, errNativeRecord
	}
	if _, err = selectRedisClientCancelParent(ctx, store, selected.Identity, view.Revision, view.Timer, current, selected.Record, proc.Native.Key, proc.Refs.Origin, proc.Metadata.Flow, tx.Origin, key); err != nil {
		return true, err
	}
	if tx.Phase == portable.Terminated {
		return true, nil
	}
	now := time.Now()
	next, effects, err := portable.StepTransaction(tx, portable.Input{Kind: portable.ReceiveResponse, Now: now.UnixMilli(), Message: message})
	if err != nil {
		return true, err
	}
	if len(effects) == 0 {
		return true, nil
	}
	if next.LastResponse == nil || !next.LastResponse.EqualWire(message) {
		return true, errNativeRecord
	}
	var changes []rediscore.Change
	responseField := "r:cancel-client-response-" + id
	if tx.LastResponse == nil || !tx.LastResponse.Equal(message) {
		wire, e := selectedmessage.Marshal(message)
		if e != nil {
			return true, e
		}
		changes = append(changes, rediscore.Change{Field: responseField, Value: wire})
	}
	proc.Refs.LastResponse = responseField
	data, err := encodeNativeProcedure(next, proc.Refs, proc.Metadata)
	if err != nil {
		return true, err
	}
	changes = append(changes, rediscore.Change{Field: selected.Record, Value: data})
	var timers []rediscore.TimerChange
	for _, effect := range effects {
		switch effect.Kind {
		case portable.CancelTimer:
			if effect.Timer != portable.TimerE {
				timers = append(timers, rediscore.TimerChange{Member: "cancel-client-" + id + "-" + string(effect.Timer), Delete: true})
			}
		case portable.Schedule:
			if effect.Timer != portable.TimerE {
				timers = append(timers, rediscore.TimerChange{Member: "cancel-client-" + id + "-" + string(effect.Timer), Due: time.UnixMilli(effect.Deadline.At)})
			}
		case portable.ReportResponse, portable.Ended:
		default:
			return true, errNativeRecord
		}
	}
	if next.Phase == portable.Terminated {
		if len(proc.Metadata.Outputs) == 0 {
			timers = append(timers, rediscore.TimerChange{Member: "cancel-client-retire-" + id, Due: now})
		} else if proc.Metadata.OutputConsumed {
			for _, field := range proc.Metadata.Outputs {
				outputView, e := store.Read(ctx, selected.Identity, field)
				if e != nil {
					return true, e
				}
				if outputView.Revision != view.Revision || outputView.Timer != view.Timer {
					return true, rediscore.ErrConflict
				}
				output, e := decodeNativeOutput(outputView.Records[field])
				if e != nil || output.Write.Permit.Authorization.DeadlineMs == 0 {
					return true, errNativeRecord
				}
				timers = append(timers, rediscore.TimerChange{Member: "out-" + strings.TrimPrefix(field, "r:"), Due: time.UnixMilli(int64(output.Write.Permit.Authorization.DeadlineMs))})
			}
		}
	}
	target := view.Timer
	if target == (rediscore.TimerAssignment{}) {
		target = executor
	}
	publication := rediscore.Publication{Identity: selected.Identity, Revision: view.Revision, Deadline: deadline, SpendReservation: true, Changes: changes}
	if len(timers) != 0 {
		publication.Timers = &rediscore.TimerMutation{Expected: view.Timer, Target: target, Changes: timers}
	}
	if flow.Transport == "udp" {
		worker, e := nativeWorkerReference(flow.Ingress)
		if e != nil {
			return true, e
		}
		publication.UDPHint = &rediscore.UDPHint{Worker: worker, Context: selected.Identity.Context, Incarnation: selected.Identity.Incarnation, Procedure: selected.Record, Stop: facts.Status >= 200}
	}
	_, err = store.Commit(ctx, publication)
	if errors.Is(err, rediscore.ErrExpired) && publication.Timers != nil && view.Timer != (rediscore.TimerAssignment{}) && view.Timer != executor && ctx.Err() == nil && time.Now().Before(deadline) {
		publication.Timers.Target = executor
		_, err = store.Commit(ctx, publication)
	}
	return true, err
}
