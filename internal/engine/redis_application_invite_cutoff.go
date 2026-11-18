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
	"sipbridge/internal/rediscore"
	"sipbridge/internal/storagecodec"
)

// dispatchRedisApplicationInviteCutoff reconciles the original attempt. U
// remains owned; definite evidence follows the normal receipt-first consumer.
func dispatchRedisApplicationInviteCutoff(ctx context.Context, store *rediscore.Store, due *rediscore.DueContext, deadline time.Time) error {
	const prefix = "out-app-invite-output-"
	if store == nil || due == nil || due.CheckedAtMS < due.DeadlineMS || !time.Now().Before(deadline) || !strings.HasPrefix(due.Member, prefix) {
		return errNativeRecord
	}
	id := strings.TrimPrefix(due.Member, prefix)
	if len(id) != 32 {
		return errNativeRecord
	}
	procedureField, outputField := "r:app-invite-"+id, "r:app-invite-output-"+id
	view, err := store.Read(ctx, due.Identity, "r:context", procedureField, outputField)
	if err != nil {
		return err
	}
	if view.Revision != due.Revision || view.Timer != due.Assignment {
		return rediscore.ErrConflict
	}
	current, err := decodeNativeContext(view.Records["r:context"])
	if err != nil {
		return err
	}
	procedure, err := readPrivateProcedureRecord(view.Records[procedureField])
	if err != nil {
		return err
	}
	output, err := decodeNativeOutput(view.Records[outputField])
	if err != nil || !validReadyApplicationInvite(output, outputField) || current.Context == nil || current.Context.State == nil || current.Context.Id != due.Identity.Context || current.Context.State.Incarnation != due.Identity.Incarnation || current.Kind != pb.ContextSnapshot_UAS && current.Kind != pb.ContextSnapshot_UAC || !proto.Equal(current.Context, output.Write.Effect.Context) || output.Procedure != procedureField || int64(output.Write.Permit.Authorization.DeadlineMs) != due.DeadlineMS || !slices.Contains(current.NoninitialProcedures, procedureField) || !slices.Contains(current.Dialogs, output.Dialog) || procedure.Native.Version != portable.Version || procedure.Native.Family != string(portable.InviteClient) || procedure.Origin != "r:app-invite-origin-"+id || procedure.CommandResult != output.Result || !slices.Contains(procedure.Outputs, outputField) || !proto.Equal(procedure.Flow, output.Write.Flow) {
		return errNativeRecord
	}
	ref, err := nativeOutputRef(output.Write)
	if err != nil || !ref.RequireApplication {
		return errNativeRecord
	}
	proof, err := store.InspectOutput(ctx, ref)
	if err == nil {
		if proof.MetadataField != outputField || !bytes.Equal(proof.Metadata, view.Records[outputField]) || proof.DeadlineMS != due.DeadlineMS {
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
		if proof.State != phase || proof.MetadataField != outputField || !bytes.Equal(proof.Metadata, view.Records[outputField]) {
			return rediscore.ErrConflict
		}
		capture := &nativeOutputCapture{ref: ref, proof: proof, metadata: output}
		if phase == rediscore.OutputUnknown {
			if e = settleRedisApplicationByeOutput(ctx, store, output.Write, capture, due.Assignment, deadline); e != nil {
				return e
			}
			selected, x := inspectRedisApplicationByeOutput(ctx, store, output.Write, nil)
			if x != nil {
				return x
			}
			if selected.selected.Identity != due.Identity || selected.selected.View.Timer != due.Assignment || selected.capture.proof.State != rediscore.OutputUnknown || !bytes.Equal(selected.capture.proof.Metadata, view.Records[outputField]) {
				return rediscore.ErrConflict
			}
			current, x = decodeNativeContext(selected.selected.View.Records["r:context"])
			if x != nil {
				return x
			}
			timers := []rediscore.TimerChange{{Member: due.Member, Delete: true}}
			if current.Ended {
				if current.CallRetireAtMs <= 0 {
					return errNativeRecord
				}
				timers = append(timers, rediscore.TimerChange{Member: "call-retire", Due: time.UnixMilli(current.CallRetireAtMs)})
			}
			_, e = store.Commit(ctx, rediscore.Publication{Identity: due.Identity, Revision: selected.selected.View.Revision, Deadline: deadline, SpendReservation: true, Timers: &rediscore.TimerMutation{Expected: due.Assignment, Target: due.Assignment, RequireLease: true, Changes: timers}})
			return e
		}
		if phase != rediscore.OutputWritten && phase != rediscore.OutputNotWritten {
			return errNativeRecord
		}
		if e = settleRedisApplicationByeOutput(ctx, store, output.Write, capture, due.Assignment, deadline); e != nil {
			return e
		}
		if !proof.Acknowledged {
			return acknowledgeRedisApplicationByeOutput(ctx, store, output.Write, deadline)
		}
		return nil
	}
	if !errors.Is(err, rediscore.ErrAbsent) {
		return err
	}
	if !procedure.OutputConsumed {
		return errNativeRecord
	}
	if procedure.Native.Phase != string(portable.Terminated) {
		_, err = store.Commit(ctx, rediscore.Publication{Identity: due.Identity, Revision: due.Revision, Deadline: deadline, SpendReservation: true, Timers: &rediscore.TimerMutation{Expected: due.Assignment, Target: due.Assignment, RequireLease: true, Changes: []rediscore.TimerChange{{Member: due.Member, Delete: true}}}})
		return err
	}
	owners, err := store.Read(ctx, due.Identity, output.Dialog, output.Result)
	if err != nil {
		return err
	}
	if owners.Revision != due.Revision || owners.Timer != due.Assignment {
		return rediscore.ErrConflict
	}
	dialog, err := readPrivateDialogRecord(owners.Records[output.Dialog])
	if err != nil || dialog.LocalInvite == nil || dialog.LocalInvite.Request != procedure.Origin {
		return errNativeRecord
	}
	result, err := decodeNativeResult(owners.Records[output.Result])
	if err != nil || result.Output != outputField || result.Sequence == 0 || result.Sequence > current.CommandSequence || result.OperationId != output.Operation || result.DecidingLeaseTerm != output.Write.Permit.Authorization.ApplicationLeaseTerm {
		return errNativeRecord
	}
	expectedID, err := redisApplicationByeID(result.Fingerprint)
	if err != nil || expectedID != id {
		return errNativeRecord
	}
	if result.Pending {
		// The original waiter owns the pending result until its fixed cutoff.
		// Its retirement duty will re-drive this output after that cutoff.
		timers := []rediscore.TimerChange{{Member: due.Member, Delete: true}, {Member: "app-invite-retire-" + id, Due: time.UnixMilli(max(due.CheckedAtMS, result.CutoffMs))}}
		_, err = store.Commit(ctx, rediscore.Publication{Identity: due.Identity, Revision: due.Revision, Deadline: deadline, SpendReservation: true, Timers: &rediscore.TimerMutation{Expected: due.Assignment, Target: due.Assignment, RequireLease: true, Changes: timers}})
		return err
	}
	pi, di := slices.Index(procedure.Outputs, outputField), slices.Index(dialog.Outputs, outputField)
	if pi < 0 || di < 0 {
		return errNativeRecord
	}
	procedure.Outputs = slices.Delete(procedure.Outputs, pi, pi+1)
	dialog.Outputs = slices.Delete(dialog.Outputs, di, di+1)
	result.Output = ""
	procedureData, err := marshalNativeRecord(storagecodec.NativeProcedure, procedure)
	if err != nil {
		return err
	}
	dialogData, err := marshalNativeRecord(storagecodec.NativeDialog, dialog)
	if err != nil {
		return err
	}
	resultData, err := encodeNativeResult(result)
	if err != nil {
		return err
	}
	timers := []rediscore.TimerChange{{Member: due.Member, Delete: true}, {Member: "app-invite-retire-" + id, Due: time.UnixMilli(due.CheckedAtMS)}}
	if current.Ended {
		if current.CallRetireAtMs <= 0 {
			return errNativeRecord
		}
		timers = append(timers, rediscore.TimerChange{Member: "call-retire", Due: time.UnixMilli(current.CallRetireAtMs)})
	}
	_, err = store.Commit(ctx, rediscore.Publication{Identity: due.Identity, Revision: due.Revision, Deadline: deadline, SpendReservation: true, Changes: []rediscore.Change{{Field: procedureField, Value: procedureData}, {Field: output.Dialog, Value: dialogData}, {Field: output.Result, Value: resultData}, {Field: outputField, Delete: true}}, Timers: &rediscore.TimerMutation{Expected: due.Assignment, Target: due.Assignment, RequireLease: true, Changes: timers}})
	return err
}
