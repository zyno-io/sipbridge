package engine

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"slices"
	"strings"
	"time"

	"github.com/emiago/sipgo/sip/portable"
	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/rediscore"
)

// dispatchRedisCallByeOutputCutoff consumes the exact remote-BYE response duty.
func dispatchRedisCallByeOutputCutoff(ctx context.Context, store *rediscore.Store, due *rediscore.DueContext, deadline time.Time) error {
	const prefix = "out-call-bye-output-"
	if store == nil || due == nil || !strings.HasPrefix(due.Member, prefix) || due.CheckedAtMS < due.DeadlineMS || !time.Now().Before(deadline) {
		return errNativeRecord
	}
	id := strings.TrimPrefix(due.Member, prefix)
	raw, err := hex.DecodeString(id)
	if err != nil || len(raw) != 16 || hex.EncodeToString(raw) != id {
		return errNativeRecord
	}
	procedureField, outputField := "r:call-bye-"+id, "r:call-bye-output-"+id
	selected, err := store.ObserveContext(ctx, due.Identity.Context, due.Identity.Incarnation, "r:context", procedureField, outputField)
	if err != nil {
		return err
	}
	if selected.Identity != due.Identity || selected.View.Revision != due.Revision || selected.View.Timer != due.Assignment {
		return rediscore.ErrConflict
	}
	current, err := decodeNativeContext(selected.View.Records["r:context"])
	if err != nil {
		return err
	}
	procedure, err := readPrivateProcedureRecord(selected.View.Records[procedureField])
	if err != nil {
		return err
	}
	metadataBytes := selected.View.Records[outputField]
	output, err := decodeNativeOutput(metadataBytes)
	if err != nil {
		return err
	}
	if current.Context == nil || current.Context.State == nil || !proto.Equal(current.Context, output.Write.Effect.Context) ||
		current.Kind != pb.ContextSnapshot_UAS && current.Kind != pb.ContextSnapshot_UAC || !slices.Contains(current.NoninitialProcedures, procedureField) ||
		!slices.Contains(current.Dialogs, output.Dialog) || output.Procedure != procedureField || output.Dialog == "" ||
		output.Method != "BYE" || output.Status != 200 || output.Result != "" || output.Operation != "" || output.CleanupAfterAck || output.AckSequence != 0 ||
		output.Write.Permit.Authorization.AuthorityClass != pb.EffectAuthorization_RESPONSE ||
		procedure.Native.Version != portable.Version || procedure.Native.Family != string(portable.NonInviteServer) ||
		procedure.Native.Reliable != (output.Write.Flow.Transport != "udp") ||
		procedure.Native.Phase != string(portable.Completed) && procedure.Native.Phase != string(portable.Terminated) ||
		procedure.Native.Reliable && procedure.Native.Phase != string(portable.Terminated) ||
		procedure.Origin != "r:call-bye-origin-"+id || procedure.LastResponse != "r:call-bye-final-"+id ||
		procedure.CommandResult != "" || procedure.DecisionPending || !proto.Equal(procedure.Flow, output.Write.Flow) {
		return errNativeRecord
	}
	dialogView, err := store.ObserveContext(ctx, due.Identity.Context, due.Identity.Incarnation, output.Dialog)
	if err != nil {
		return err
	}
	if dialogView.Identity != due.Identity || dialogView.View.Revision != due.Revision || dialogView.View.Timer != due.Assignment {
		return rediscore.ErrConflict
	}
	dialog, err := readPrivateDialogRecord(dialogView.View.Records[output.Dialog])
	if err != nil {
		return err
	}
	if dialog.Native.Version != portable.Version || dialog.Native.Phase != string(portable.DialogEnded) || dialog.Native.CallId != current.CallId || !slices.Contains(dialog.Outputs, outputField) {
		return errNativeRecord
	}
	ref, err := nativeOutputRef(output.Write)
	if err != nil || ref.RequireApplication {
		return errNativeRecord
	}
	physicalDeadline := output.Write.Permit.Authorization.DeadlineMs
	if physicalDeadline == 0 || physicalDeadline > 1<<53-1 {
		return errNativeRecord
	}
	original := int64(physicalDeadline)
	proof, err := store.InspectOutput(ctx, ref)
	if errors.Is(err, rediscore.ErrAbsent) {
		if due.DeadlineMS != original {
			if procedure.Native.Timers == nil {
				return errNativeRecord
			}
			timer := procedure.Native.Timers.Values[string(portable.TimerJ)]
			if !procedure.OutputConsumed || procedure.Native.Reliable || procedure.Native.Phase != string(portable.Completed) || timer == nil || !timer.Active || timer.Generation == 0 || timer.At != due.DeadlineMS {
				return errNativeRecord
			}
		}
		return retireRedisCallByeKnown(ctx, store, due, due.Assignment, deadline)
	}
	if err != nil {
		return err
	}
	if original <= 0 || due.DeadlineMS != original || proof.DeadlineMS != original || proof.MetadataField != outputField ||
		proof.MessageField != "r:call-bye-final-"+id && proof.MessageField != "r:call-bye-wire-"+id || !bytes.Equal(proof.Metadata, metadataBytes) {
		return rediscore.ErrConflict
	}
	phase, err := store.ExpireOutput(ctx, due, ref)
	if err != nil {
		return err
	}
	if phase == rediscore.OutputWritten || phase == rediscore.OutputNotWritten {
		return retireRedisCallByeKnown(ctx, store, due, due.Assignment, deadline)
	}
	if phase != rediscore.OutputUnknown {
		return errNativeRecord
	}
	latest, err := store.InspectOutput(ctx, ref)
	if err != nil {
		return err
	}
	if latest.State != rediscore.OutputUnknown || latest.Acknowledged || latest.Settled || latest.MetadataField != outputField || latest.DeadlineMS != original || !bytes.Equal(latest.Metadata, metadataBytes) {
		return rediscore.ErrConflict
	}
	timers := []rediscore.TimerChange{{Member: due.Member, Delete: true}}
	if current.Ended {
		if current.CallRetireAtMs <= 0 {
			return errNativeRecord
		}
		timers = append(timers, rediscore.TimerChange{Member: "call-retire", Due: time.UnixMilli(current.CallRetireAtMs)})
	}
	_, err = store.Commit(ctx, rediscore.Publication{Identity: due.Identity, Revision: due.Revision, Deadline: deadline, SpendReservation: true,
		Timers: &rediscore.TimerMutation{Expected: due.Assignment, Target: due.Assignment, RequireLease: true, Changes: timers}})
	return err
}
