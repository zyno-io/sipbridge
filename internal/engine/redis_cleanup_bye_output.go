package engine

import (
	"bytes"
	"context"
	"errors"
	"slices"
	"strings"
	"time"

	"github.com/emiago/sipgo/sip/portable"
	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/nativepb"
	"sipbridge/internal/rediscore"
	"sipbridge/internal/storagecodec"
)

type selectedCleanupByeOutput struct {
	capture   nativeOutputCapture
	selected  *rediscore.ObservedContext
	procedure *nativepb.Procedure
}

// inspectRedisCleanupByeOutput selects only the exact physical and native
// ownership facts. Controller lease state cannot authorize or fence completion.
func inspectRedisCleanupByeOutput(ctx context.Context, store *rediscore.Store, write *pb.FlowWrite, capture *nativeOutputCapture) (selectedCleanupByeOutput, error) {
	var out selectedCleanupByeOutput
	if store == nil {
		return out, errNativeRecord
	}
	if capture == nil {
		ref, err := nativeOutputRef(write)
		if err != nil {
			return out, err
		}
		proof, err := store.InspectOutput(ctx, ref)
		if err != nil {
			return out, err
		}
		metadata, err := decodeNativeOutput(proof.Metadata)
		if err != nil {
			return out, err
		}
		capture = &nativeOutputCapture{ref: ref, proof: proof, metadata: metadata}
	}
	metadata, proof := capture.metadata, capture.proof
	if metadata == nil || write == nil || write.Permit == nil || write.Permit.Authorization == nil || !proto.Equal(metadata.Write, write) || metadata.Method != "BYE" || metadata.Status != 0 || metadata.Result != "" || metadata.Operation != "" || metadata.Dialog == "" || capture.ref.RequireApplication || write.Permit.Authorization.AuthorityClass != pb.EffectAuthorization_TERMINAL_CLEANUP || proof.DeadlineMS <= 0 || write.Permit.Authorization.DeadlineMs != uint64(proof.DeadlineMS) {
		return out, errNativeRecord
	}
	id := strings.TrimPrefix(metadata.Procedure, "r:call-cleanup-")
	if len(id) != 32 || metadata.Procedure != "r:call-cleanup-"+id || proof.MetadataField != "r:call-cleanup-output-"+id || proof.MessageField != "r:call-cleanup-origin-"+id {
		return out, errNativeRecord
	}
	selected, err := store.ObserveContext(ctx, capture.ref.Context, capture.ref.Incarnation, "r:context", metadata.Procedure, metadata.Dialog, proof.MetadataField)
	if err != nil {
		return out, err
	}
	if !bytes.Equal(selected.View.Records[proof.MetadataField], proof.Metadata) {
		return out, rediscore.ErrConflict
	}
	current, err := decodeNativeContext(selected.View.Records["r:context"])
	if err != nil {
		return out, err
	}
	procedure, err := readPrivateProcedureRecord(selected.View.Records[metadata.Procedure])
	if err != nil {
		return out, err
	}
	dialog, err := readPrivateDialogRecord(selected.View.Records[metadata.Dialog])
	if err != nil {
		return out, err
	}
	if current.Context == nil || !proto.Equal(current.Context, write.Effect.Context) || current.Kind != pb.ContextSnapshot_UAS && current.Kind != pb.ContextSnapshot_UAC || !slices.Contains(current.NoninitialProcedures, metadata.Procedure) || !slices.Contains(current.Dialogs, metadata.Dialog) || !slices.Contains(dialog.Outputs, proof.MetadataField) || dialog.LocalTransaction != procedure.Native.Key || dialog.LocalBye != proof.MessageField || procedure.Native.Version != portable.Version || procedure.Native.Family != string(portable.NonInviteClient) || procedure.Origin != proof.MessageField || !procedure.Cleanup || procedure.CommandResult != "" || !proto.Equal(procedure.Flow, write.Flow) {
		return out, errNativeRecord
	}
	out.capture, out.selected, out.procedure = *capture, selected, procedure
	return out, nil
}

func settleRedisCleanupByeOutput(ctx context.Context, store *rediscore.Store, write *pb.FlowWrite, capture *nativeOutputCapture, executor rediscore.TimerAssignment, deadline time.Time) error {
	selected, err := inspectRedisCleanupByeOutput(ctx, store, write, capture)
	if err != nil {
		return err
	}
	return settleRedisClientByeSelected(ctx, store, selected, executor, deadline)
}

// settleRedisClientByeSelected shares exact output settlement for native and
// admitted application BYEs after their distinct authority checks.
func settleRedisClientByeSelected(ctx context.Context, store *rediscore.Store, selected selectedCleanupByeOutput, executor rediscore.TimerAssignment, deadline time.Time) error {
	proof := selected.capture.proof
	if proof.State == rediscore.OutputStarted || proof.State == rediscore.OutputUnknown {
		return nil
	}
	if proof.State != rediscore.OutputWritten && proof.State != rediscore.OutputNotWritten || proof.Acknowledged && !selected.procedure.OutputConsumed {
		return errNativeRecord
	}
	if proof.Settled {
		return nil
	}
	target := selected.selected.View.Timer
	if target == (rediscore.TimerAssignment{}) {
		target = executor
	}
	publication := rediscore.Publication{Identity: selected.selected.Identity, Revision: selected.selected.View.Revision, Deadline: deadline, SpendReservation: true, Timers: &rediscore.TimerMutation{Expected: selected.selected.View.Timer, Target: target, Changes: []rediscore.TimerChange{{Member: "out-" + strings.TrimPrefix(proof.MetadataField, "r:"), Due: time.UnixMilli(proof.DeadlineMS)}}}}
	_, err := store.Commit(ctx, publication)
	if errors.Is(err, rediscore.ErrExpired) && selected.selected.View.Timer != (rediscore.TimerAssignment{}) && selected.selected.View.Timer != executor && ctx.Err() == nil && time.Now().Before(deadline) {
		publication.Timers.Target = executor
		_, err = store.Commit(ctx, publication)
	}
	if err != nil {
		return err
	}
	return store.SettleOutput(ctx, selected.capture.ref, false, true)
}

func acknowledgeRedisCleanupByeOutput(ctx context.Context, store *rediscore.Store, write *pb.FlowWrite, deadline time.Time) error {
	selected, err := inspectRedisCleanupByeOutput(ctx, store, write, nil)
	if err != nil {
		return err
	}
	return acknowledgeRedisClientByeSelected(ctx, store, selected, deadline)
}

func acknowledgeRedisClientByeSelected(ctx context.Context, store *rediscore.Store, selected selectedCleanupByeOutput, deadline time.Time) error {
	if !selected.capture.proof.Settled || selected.capture.proof.State != rediscore.OutputWritten && selected.capture.proof.State != rediscore.OutputNotWritten {
		return errNativeRecord
	}
	if !selected.procedure.OutputConsumed {
		procedure := proto.Clone(selected.procedure).(*nativepb.Procedure)
		procedure.OutputConsumed = true
		value, e := marshalNativeRecord(storagecodec.NativeProcedure, procedure)
		if e != nil {
			return e
		}
		_, e = store.Commit(ctx, rediscore.Publication{Identity: selected.selected.Identity, Revision: selected.selected.View.Revision, Deadline: deadline, SpendReservation: true, Changes: []rediscore.Change{{Field: selected.capture.metadata.Procedure, Value: value}}})
		if e != nil {
			return e
		}
	}
	return store.SettleOutput(ctx, selected.capture.ref, true, false)
}
