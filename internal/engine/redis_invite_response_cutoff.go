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

// dispatchRedisInitialInviteOutputCutoff reconciles one original physical
// deadline. UNKNOWN retains its exact procedure/result ownership without a
// polling head; the bounded fully-ended-call path owns its eventual release.
func dispatchRedisInitialInviteOutputCutoff(ctx context.Context, store *rediscore.Store, due *rediscore.DueContext, deadline time.Time) error {
	if store == nil || due == nil || due.CheckedAtMS < due.DeadlineMS || !time.Now().Before(deadline) || !strings.HasPrefix(due.Member, "out-invite-output-") {
		return errNativeRecord
	}
	rest := strings.TrimPrefix(due.Member, "out-invite-output-")
	if len(rest) < 38 || rest[36] != '-' {
		return errNativeRecord
	}
	id := rest[:36]
	parsed, err := uuid.Parse(id)
	sequence, seqErr := strconv.ParseUint(rest[37:], 10, 53)
	if err != nil || parsed.String() != id || seqErr != nil || rest[37:] != strconv.FormatUint(sequence, 10) {
		return errNativeRecord
	}
	procedureField := "r:procedure-" + id
	outputField := "r:invite-output-" + rest
	responseField, physicalField := "r:invite-response-"+rest, "r:invite-wire-"+rest
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
	physical := int64(output.Write.Permit.Authorization.DeadlineMs)
	grace := output.Status < 200 && physical > 0 && physical <= (1<<53-1)-rediscore.OutputObsoleteGrace.Milliseconds() && due.DeadlineMS == physical+rediscore.OutputObsoleteGrace.Milliseconds()
	if current.Context == nil || current.Context.State == nil || current.Context.Id != due.Identity.Context || current.Context.State.Incarnation != due.Identity.Incarnation || current.Kind != pb.ContextSnapshot_UAS || !slices.Contains(current.InitialProcedures, procedureField) || !proto.Equal(current.Context, output.Write.Effect.Context) || procedure.Native.Version != portable.Version || procedure.Native.Family != string(portable.InviteServer) || procedure.RequestId != id || !proto.Equal(procedure.Flow, output.Write.Flow) || !slices.Contains(procedure.Outputs, outputField) || output.Procedure != procedureField || output.Method != "INVITE" || output.Status < 100 || output.Status > 699 || physical != due.DeadlineMS && !grace || output.Result != "" && output.Result != "r:result-"+strconv.FormatUint(sequence, 10) {
		return errNativeRecord
	}
	if sequence == 0 && !validRedisInitialInviteTimeoutOutput(due.Identity, current, procedure, output, id, outputField) {
		return errNativeRecord
	}
	if grace {
		released, err := forgetRedisProvisionalOutput(ctx, store, due, selected, current, procedure, output, outputField, responseField, deadline)
		if err != nil || !released {
			return err
		}
		original := *due
		original.DeadlineMS = physical
		due = &original
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
		if phase != rediscore.OutputWritten && phase != rediscore.OutputNotWritten && phase != rediscore.OutputUnknown {
			return errNativeRecord
		}
		proof, e = store.InspectOutput(ctx, ref)
		if e != nil || proof.State != phase || proof.MetadataField != outputField || !bytes.Equal(proof.Metadata, selected.View.Records[outputField]) {
			if e != nil {
				return e
			}
			return rediscore.ErrConflict
		}
		if e = settleRedisInitialInviteOutput(ctx, store, output.Write, &nativeOutputCapture{ref: ref, proof: proof, metadata: output}, due.Assignment, deadline); e != nil {
			return e
		}
		if phase == rediscore.OutputUnknown {
			// Settlement may have advanced the result revision. Re-read the
			// exact owner before deleting only the expired scheduling head.
			fields := []string{"r:context", procedureField, outputField}
			if output.Result != "" {
				fields = append(fields, output.Result)
			}
			observed, x := store.ObserveContext(ctx, due.Identity.Context, due.Identity.Incarnation, fields...)
			if x != nil {
				return x
			}
			if observed.Identity != due.Identity || observed.View.Timer != due.Assignment || !bytes.Equal(observed.View.Records[outputField], selected.View.Records[outputField]) {
				return rediscore.ErrConflict
			}
			root, x := decodeNativeContext(observed.View.Records["r:context"])
			if x != nil || root.Context == nil || root.Context.State == nil || root.Context.Id != due.Identity.Context || root.Context.State.Incarnation != due.Identity.Incarnation || !slices.Contains(root.InitialProcedures, procedureField) {
				return errNativeRecord
			}
			owner, x := readPrivateProcedureRecord(observed.View.Records[procedureField])
			if x != nil || !slices.Contains(owner.Outputs, outputField) {
				return errNativeRecord
			}
			if output.Result != "" {
				result, e := decodeNativeResult(observed.View.Records[output.Result])
				if e != nil || result.Output != outputField || result.Sequence != sequence || result.OperationId != output.Operation {
					return errNativeRecord
				}
			}
			next := rediscore.TimerChange{Member: due.Member, Delete: true}
			if redisProvisionalSuperseded(owner, output, responseField) {
				next.Delete, next.Due = false, time.UnixMilli(physical+rediscore.OutputObsoleteGrace.Milliseconds())
			}
			_, x = store.Commit(ctx, rediscore.Publication{Identity: due.Identity, Revision: observed.View.Revision, Deadline: deadline, SpendReservation: true, Timers: &rediscore.TimerMutation{Expected: due.Assignment, Target: due.Assignment, RequireLease: true, Changes: []rediscore.TimerChange{next}}})
			return x
		}
		if !proof.Acknowledged {
			if e = acknowledgeRedisInitialInviteOutput(ctx, store, output.Write); e != nil {
				return e
			}
		}
	} else if !errors.Is(err, rediscore.ErrAbsent) {
		return err
	}
	fields := []string{"r:context", procedureField, outputField, responseField, physicalField}
	if output.Result != "" {
		fields = append(fields, output.Result)
	}
	if output.Dialog != "" {
		fields = append(fields, output.Dialog)
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
	if err != nil || current.Context == nil || current.Context.State == nil || !slices.Contains(current.InitialProcedures, procedureField) || !slices.Contains(procedure.Outputs, outputField) {
		return errNativeRecord
	}
	keepResponse := procedure.LastResponse == responseField
	if output.Dialog != "" {
		dialog, e := readPrivateDialogRecord(observed.View.Records[output.Dialog])
		if e != nil || !slices.Contains(current.Dialogs, output.Dialog) {
			return errNativeRecord
		}
		if dialog.LocalInvite != nil && dialog.LocalInvite.FinalResponse == responseField || dialog.RemoteInvite != nil && dialog.RemoteInvite.FinalResponse == responseField {
			keepResponse = true
		}
	}
	index := slices.Index(procedure.Outputs, outputField)
	procedure.Outputs = slices.Delete(procedure.Outputs, index, index+1)
	procedureBytes, err := marshalNativeRecord(storagecodec.NativeProcedure, procedure)
	if err != nil {
		return err
	}
	changes := []rediscore.Change{{Field: procedureField, Value: procedureBytes}, {Field: outputField, Delete: true}}
	timers := []rediscore.TimerChange{{Member: due.Member, Delete: true}}
	if current.CallRetireAtMs > 0 {
		timers = append(timers, rediscore.TimerChange{Member: "call-retire", Due: time.UnixMilli(current.CallRetireAtMs)})
	}
	if len(observed.View.Records[physicalField]) != 0 {
		changes = append(changes, rediscore.Change{Field: physicalField, Delete: true})
	}
	if !keepResponse && len(observed.View.Records[responseField]) != 0 {
		changes = append(changes, rediscore.Change{Field: responseField, Delete: true})
	}
	if output.Result != "" {
		result, e := decodeNativeResult(observed.View.Records[output.Result])
		if e != nil || result.Sequence != sequence || result.OperationId != output.Operation || result.Output != outputField || result.Pending {
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
	_, err = store.Commit(ctx, rediscore.Publication{Identity: due.Identity, Revision: observed.View.Revision, Deadline: deadline, SpendReservation: true, Changes: changes, Timers: &rediscore.TimerMutation{Expected: due.Assignment, Target: due.Assignment, RequireLease: true, Changes: timers}})
	return err
}
