package engine

import (
	"bytes"
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
	"sipbridge/internal/rediscore"
	"sipbridge/internal/storagecodec"
)

// dispatchRedisDialogOutputCutoff consumes one selected dialog-server attempt.
func dispatchRedisDialogOutputCutoff(ctx context.Context, store *rediscore.Store, due *rediscore.DueContext, deadline time.Time) error {
	const prefix = "out-dialog-output-"
	if store == nil || due == nil || !strings.HasPrefix(due.Member, prefix) || due.CheckedAtMS < due.DeadlineMS || !time.Now().Before(deadline) {
		return errNativeRecord
	}
	rest := strings.TrimPrefix(due.Member, prefix)
	if len(rest) < 38 || rest[36] != '-' {
		return errNativeRecord
	}
	id := rest[:36]
	parsed, err := uuid.Parse(id)
	sequence, seqErr := strconv.ParseUint(rest[37:], 10, 53)
	if err != nil || parsed.String() != id || seqErr != nil || rest[37:] != strconv.FormatUint(sequence, 10) {
		return errNativeRecord
	}
	procedureField, outputField := "r:procedure-"+id, "r:dialog-output-"+rest
	responseField, physicalField := "r:dialog-response-"+rest, "r:dialog-wire-"+rest
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
	output, err := decodeNativeOutput(selected.View.Records[outputField])
	if err != nil {
		return err
	}
	if sequence == 0 && output.Result != "" {
		return errNativeRecord
	}
	family := redisDialogServerFamily(output.Method)
	if current.Context == nil || current.Context.State == nil || !proto.Equal(current.Context, output.Write.Effect.Context) || current.Kind != pb.ContextSnapshot_UAS && current.Kind != pb.ContextSnapshot_UAC || !slices.Contains(current.NoninitialProcedures, procedureField) || !slices.Contains(current.Dialogs, output.Dialog) || !slices.Contains(procedure.Outputs, outputField) || procedure.Native.Version != portable.Version || family == "" || procedure.Native.Family != family || procedure.RequestId != id || !proto.Equal(procedure.Flow, output.Write.Flow) || output.Procedure != procedureField || int64(output.Write.Permit.Authorization.DeadlineMs) != due.DeadlineMS || output.Result != "" && output.Result != "r:result-"+strconv.FormatUint(sequence, 10) {
		return errNativeRecord
	}
	ref, err := nativeOutputRef(output.Write)
	if err != nil || ref.RequireApplication {
		return errNativeRecord
	}
	proof, err := store.InspectOutput(ctx, ref)
	if err == nil {
		if proof.MetadataField != outputField || !bytes.Equal(proof.Metadata, selected.View.Records[outputField]) {
			return rediscore.ErrConflict
		}
		phase, e := store.ExpireOutput(ctx, due, ref)
		if e != nil {
			return e
		}
		proof, e = store.InspectOutput(ctx, ref)
		if e != nil {
			return e
		}
		if proof.State != phase || !bytes.Equal(proof.Metadata, selected.View.Records[outputField]) {
			return rediscore.ErrConflict
		}
		if e = settleRedisInitialInviteOutput(ctx, store, output.Write, &nativeOutputCapture{ref: ref, proof: proof, metadata: output}, due.Assignment, deadline); e != nil {
			return e
		}
		if phase == rediscore.OutputUnknown {
			observed, x := store.ObserveContext(ctx, due.Identity.Context, due.Identity.Incarnation, "r:context", procedureField, output.Dialog, outputField)
			if x != nil {
				return x
			}
			if observed.Identity != due.Identity || observed.View.Timer != due.Assignment || !bytes.Equal(observed.View.Records[outputField], selected.View.Records[outputField]) {
				return rediscore.ErrConflict
			}
			owner, x := readPrivateProcedureRecord(observed.View.Records[procedureField])
			if x != nil || !slices.Contains(owner.Outputs, outputField) {
				return errNativeRecord
			}
			dialog, x := readPrivateDialogRecord(observed.View.Records[output.Dialog])
			if x != nil || !slices.Contains(dialog.Outputs, outputField) {
				return errNativeRecord
			}
			root, x := decodeNativeContext(observed.View.Records["r:context"])
			if x != nil {
				return x
			}
			timers := []rediscore.TimerChange{{Member: due.Member, Delete: true}}
			if root.Ended && root.CallRetireAtMs > 0 {
				timers = append(timers, rediscore.TimerChange{Member: "call-retire", Due: time.UnixMilli(root.CallRetireAtMs)})
			}
			_, x = store.Commit(ctx, rediscore.Publication{Identity: due.Identity, Revision: observed.View.Revision, Deadline: deadline, SpendReservation: true, Timers: &rediscore.TimerMutation{Expected: due.Assignment, Target: due.Assignment, RequireLease: true, Changes: timers}})
			return x
		}
		if phase != rediscore.OutputWritten && phase != rediscore.OutputNotWritten {
			return errNativeRecord
		}
		if !proof.Acknowledged {
			if e = acknowledgeRedisInitialInviteOutput(ctx, store, output.Write); e != nil {
				return e
			}
		}
	} else if !errors.Is(err, rediscore.ErrAbsent) {
		return err
	}
	fields := []string{"r:context", procedureField, output.Dialog, outputField, responseField, physicalField}
	if output.Result != "" {
		fields = append(fields, output.Result)
	}
	observed, err := store.ObserveContext(ctx, due.Identity.Context, due.Identity.Incarnation, fields...)
	if err != nil {
		return err
	}
	if observed.Identity != due.Identity || observed.View.Timer != due.Assignment || !bytes.Equal(observed.View.Records[outputField], selected.View.Records[outputField]) {
		return rediscore.ErrConflict
	}
	current, err = decodeNativeContext(observed.View.Records["r:context"])
	if err != nil {
		return err
	}
	procedure, err = readPrivateProcedureRecord(observed.View.Records[procedureField])
	if err != nil {
		return err
	}
	dialog, err := readPrivateDialogRecord(observed.View.Records[output.Dialog])
	if err != nil {
		return err
	}
	pi, di := slices.Index(procedure.Outputs, outputField), slices.Index(dialog.Outputs, outputField)
	if !slices.Contains(current.NoninitialProcedures, procedureField) || !slices.Contains(current.Dialogs, output.Dialog) || pi < 0 || di < 0 {
		return errNativeRecord
	}
	procedure.Outputs, dialog.Outputs = slices.Delete(procedure.Outputs, pi, pi+1), slices.Delete(dialog.Outputs, di, di+1)
	procedureBytes, err := marshalNativeRecord(storagecodec.NativeProcedure, procedure)
	if err != nil {
		return err
	}
	dialogBytes, err := marshalNativeRecord(storagecodec.NativeDialog, dialog)
	if err != nil {
		return err
	}
	changes := []rediscore.Change{{Field: procedureField, Value: procedureBytes}, {Field: output.Dialog, Value: dialogBytes}, {Field: outputField, Delete: true}}
	if len(observed.View.Records[physicalField]) != 0 {
		changes = append(changes, rediscore.Change{Field: physicalField, Delete: true})
	}
	dialogOwnsResponse := dialog.LocalInvite != nil && dialog.LocalInvite.FinalResponse == responseField || dialog.RemoteInvite != nil && dialog.RemoteInvite.FinalResponse == responseField
	if procedure.LastResponse != responseField && !dialogOwnsResponse && len(observed.View.Records[responseField]) != 0 {
		changes = append(changes, rediscore.Change{Field: responseField, Delete: true})
	}
	timers := []rediscore.TimerChange{{Member: due.Member, Delete: true}}
	if procedure.Native.Phase == string(portable.Terminated) && len(procedure.Outputs) == 0 {
		timers = append(timers, rediscore.TimerChange{Member: "call-proc-" + id, Due: time.UnixMilli(due.DeadlineMS)})
	}
	if output.Result != "" {
		result, e := decodeNativeResult(observed.View.Records[output.Result])
		if e != nil || result.Sequence != sequence || result.OperationId != output.Operation || result.Output != outputField || result.Pending || result.ServerRequestId != id {
			return errNativeRecord
		}
		result.Output = ""
		value, e := encodeNativeResult(result)
		if e != nil {
			return e
		}
		changes = append(changes, rediscore.Change{Field: output.Result, Value: value})
		if current.CommandSequence > sequence {
			timers = append(timers, rediscore.TimerChange{Member: inviteResultTimer(id, sequence), Due: time.UnixMilli(result.CutoffMs)})
		}
	}
	if current.Ended && current.CallRetireAtMs > 0 {
		timers = append(timers, rediscore.TimerChange{Member: "call-retire", Due: time.UnixMilli(current.CallRetireAtMs)})
	}
	_, err = store.Commit(ctx, rediscore.Publication{Identity: due.Identity, Revision: observed.View.Revision, Deadline: deadline, SpendReservation: true, Changes: changes, Timers: &rediscore.TimerMutation{Expected: due.Assignment, Target: due.Assignment, RequireLease: true, Changes: timers}})
	return err
}
