package engine

import (
	"context"
	"errors"
	"slices"
	"strconv"
	"time"

	"github.com/emiago/sipgo/sip/portable"
	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/nativepb"
	"sipbridge/internal/rediscore"
	"sipbridge/internal/selectedmessage"
	"sipbridge/internal/storagecodec"
)

// finishRedisAbandonedApplicationACK applies only the locked initial-winner
// owner-loss exception after exact U has been forgotten or a definite N was
// consumed. It never infers a physical outcome or starts an ACK-dependent BYE.
func finishRedisAbandonedApplicationACK(ctx context.Context, store *rediscore.Store, due *rediscore.DueContext, owners *terminalCallOwners, output *nativepb.Output, outputField string, deadline time.Time) (bool, error) {
	if store == nil || due == nil || owners == nil || owners.current == nil || owners.unprovedACK == "" || output == nil || output.Dialog != owners.unprovedACK || due.Identity.Owner != "" || due.Identity.Term != "" || owners.current.Kind != pb.ContextSnapshot_UAC || !owners.current.Ended || !owners.current.Answered || due.CheckedAtMS < owners.current.CallRetireAtMs || !time.Now().Before(deadline) {
		return false, errNativeRecord
	}
	dialog := owners.dialogs[owners.unprovedACK]
	proc := owners.procedures[output.Procedure]
	result := owners.results[output.Result]
	if dialog == nil || proc == nil || result == nil || dialog.AckWritten || dialog.AckResult != output.Result || dialog.LocalBye != "" || len(dialog.Outputs) != 1 || dialog.Outputs[0] != outputField || result.Pending || result.Certainty != pb.OperationResult_UNKNOWN || result.Output != outputField || result.Request != "" || result.Response != "" || result.CutoffMs > due.CheckedAtMS || !slices.Contains(proc.Outputs, outputField) {
		return false, errNativeRecord
	}
	ref, err := nativeOutputRef(output.Write)
	if err != nil {
		return false, err
	}
	if _, err = store.InspectOutput(ctx, ref); !errors.Is(err, rediscore.ErrAbsent) {
		if err == nil {
			return false, rediscore.ErrConflict
		}
		return false, err
	}
	candidate := proto.Clone(dialog).(*nativepb.Dialog)
	record := nativeDialogFromPrivate(candidate)
	fields := callDialogMessageRefs(record.Refs)
	wires, err := store.Read(ctx, due.Identity, fields...)
	if err != nil {
		return false, err
	}
	if wires.Revision != due.Revision || wires.Timer != due.Assignment {
		return false, rediscore.ErrConflict
	}
	memo, err := selectedmessage.NewMemo(len(fields))
	if err != nil {
		return false, err
	}
	native, err := record.complete(&nativeSelectedMessages{records: wires.Records, memo: memo, provenance: "admitted"})
	if err != nil {
		return false, err
	}
	next, effects, err := portable.StepDialog(native, portable.DialogInput{Kind: portable.DialogTerminate, Now: due.CheckedAtMS})
	if err != nil {
		return false, err
	}
	if next.Phase != portable.DialogEnded {
		return false, errNativeRecord
	}
	for _, effect := range effects {
		if effect.Kind != portable.Ended && effect.Kind != portable.CancelTimer {
			return false, errNativeRecord
		}
	}
	index, ok := slices.BinarySearch(proc.Outputs, outputField)
	if !ok {
		return false, errNativeRecord
	}
	proc = proto.Clone(proc).(*nativepb.Procedure)
	proc.Outputs = slices.Delete(proc.Outputs, index, index+1)
	record.Metadata.Outputs = nil
	dialogData, err := encodeNativeDialog(next, record.Refs, record.Metadata)
	if err != nil {
		return false, err
	}
	procData, err := marshalNativeRecord(storagecodec.NativeProcedure, proc)
	if err != nil {
		return false, err
	}
	result = proto.Clone(result).(*nativepb.Result)
	result.Output = ""
	resultData, err := encodeNativeResult(result)
	if err != nil {
		return false, err
	}
	timers := []rediscore.TimerChange{{Member: due.Member, Due: time.UnixMilli(owners.current.CallRetireAtMs)}}
	if owners.current.CommandSequence > result.Sequence {
		timers = append(timers, rediscore.TimerChange{Member: "orig-result-" + strconv.FormatUint(result.Sequence, 10), Due: time.UnixMilli(result.CutoffMs)})
	}
	_, err = store.Commit(ctx, rediscore.Publication{Identity: due.Identity, Revision: due.Revision, Deadline: deadline, SpendReservation: true, Changes: []rediscore.Change{{Field: output.Procedure, Value: procData}, {Field: output.Dialog, Value: dialogData}, {Field: output.Result, Value: resultData}, {Field: outputField, Delete: true}}, Timers: &rediscore.TimerMutation{Expected: due.Assignment, Target: due.Assignment, RequireLease: true, Changes: timers}})
	return err == nil, err
}
