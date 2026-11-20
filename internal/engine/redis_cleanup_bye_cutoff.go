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
	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/nativepb"
	"sipbridge/internal/rediscore"
	"sipbridge/internal/storagecodec"
)

// dispatchRedisCleanupByeCutoff reconciles one exact cleanup output. Unknown
// attempts remain reachable without a recurring physical deadline.
func dispatchRedisCleanupByeCutoff(ctx context.Context, store *rediscore.Store, due *rediscore.DueContext, deadline time.Time) error {
	if store == nil || due == nil || due.CheckedAtMS < due.DeadlineMS || !time.Now().Before(deadline) {
		return errNativeRecord
	}
	generic := strings.HasPrefix(due.Member, "out-app-client-output-")
	application := strings.HasPrefix(due.Member, "out-app-bye-output-") || generic
	prefix := "out-call-cleanup-output-"
	if generic {
		prefix = "out-app-client-output-"
	} else if application {
		prefix = "out-app-bye-output-"
	}
	if !strings.HasPrefix(due.Member, prefix) {
		return errNativeRecord
	}
	id := strings.TrimPrefix(due.Member, prefix)
	if len(id) != 32 {
		return errNativeRecord
	}
	procedureField, outputField := "r:call-cleanup-"+id, "r:call-cleanup-output-"+id
	if application {
		kind := "app-bye-"
		if generic {
			kind = "app-client-"
		}
		procedureField, outputField = "r:"+kind+id, "r:"+kind+"output-"+id
	}
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
	if err != nil {
		return err
	}
	if current.Context == nil || current.Context.State == nil || current.Context.Id != due.Identity.Context || current.Context.State.Incarnation != due.Identity.Incarnation || current.Kind != pb.ContextSnapshot_UAS && current.Kind != pb.ContextSnapshot_UAC || !slices.Contains(current.NoninitialProcedures, procedureField) || !slices.Contains(current.Dialogs, output.Dialog) || output.Procedure != procedureField || !redisApplicationClientMethod(output.Method) || generic == (output.Method == "BYE") || output.Status != 0 || output.Write.Permit.Authorization.DeadlineMs != uint64(due.DeadlineMS) || !proto.Equal(current.Context, output.Write.Effect.Context) || procedure.Native.Version != portable.Version || procedure.Native.Family != string(portable.NonInviteClient) || procedure.Cleanup == application || application && (procedure.CommandResult == "" || output.Result != procedure.CommandResult) || !application && (procedure.CommandResult != "" || output.Result != "") || !proto.Equal(procedure.Flow, output.Write.Flow) {
		return errNativeRecord
	}
	dialogView, err := store.Read(ctx, due.Identity, output.Dialog)
	if err != nil {
		return err
	}
	if dialogView.Revision != view.Revision || dialogView.Timer != view.Timer {
		return rediscore.ErrConflict
	}
	dialog, err := readPrivateDialogRecord(dialogView.Records[output.Dialog])
	if err != nil {
		return err
	}
	if !slices.Contains(dialog.Outputs, outputField) || !generic && (dialog.LocalTransaction != procedure.Native.Key || dialog.LocalBye != procedure.Origin) {
		return errNativeRecord
	}
	ref, err := nativeOutputRef(output.Write)
	if err != nil || ref.RequireApplication != application {
		return errNativeRecord
	}
	proof, err := store.InspectOutput(ctx, ref)
	if err == nil {
		if proof.MetadataField != outputField || !bytes.Equal(proof.Metadata, view.Records[outputField]) {
			return rediscore.ErrConflict
		}
		phase, e := store.ExpireOutput(ctx, due, ref)
		if e != nil {
			return e
		}
		if phase == rediscore.OutputUnknown {
			revision := due.Revision
			if application {
				proof, e = store.InspectOutput(ctx, ref)
				if e != nil {
					return e
				}
				if proof.State != phase || !bytes.Equal(proof.Metadata, view.Records[outputField]) {
					return rediscore.ErrConflict
				}
				if e = settleRedisApplicationByeOutput(ctx, store, output.Write, &nativeOutputCapture{ref: ref, proof: proof, metadata: output}, due.Assignment, deadline); e != nil {
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
				revision = selected.selected.View.Revision
			}
			timers := []rediscore.TimerChange{{Member: due.Member, Delete: true}}
			if current.Ended {
				if current.CallRetireAtMs <= 0 {
					return errNativeRecord
				}
				timers = append(timers, rediscore.TimerChange{Member: "call-retire", Due: time.UnixMilli(current.CallRetireAtMs)})
			}
			_, e = store.Commit(ctx, rediscore.Publication{Identity: due.Identity, Revision: revision, Deadline: deadline, SpendReservation: true, Timers: &rediscore.TimerMutation{Expected: due.Assignment, Target: due.Assignment, RequireLease: true, Changes: timers}})
			return e
		}
		if phase != rediscore.OutputWritten && phase != rediscore.OutputNotWritten {
			return errNativeRecord
		}
		proof, e = store.InspectOutput(ctx, ref)
		if e != nil {
			return e
		}
		if proof.State != phase || proof.MetadataField != outputField || !bytes.Equal(proof.Metadata, view.Records[outputField]) {
			return rediscore.ErrConflict
		}
		capture := &nativeOutputCapture{ref: ref, proof: proof, metadata: output}
		if application {
			e = settleRedisApplicationByeOutput(ctx, store, output.Write, capture, due.Assignment, deadline)
		} else {
			e = settleRedisCleanupByeOutput(ctx, store, output.Write, capture, due.Assignment, deadline)
		}
		if e != nil {
			return e
		}
		if !proof.Acknowledged {
			if application {
				return acknowledgeRedisApplicationByeOutput(ctx, store, output.Write, deadline)
			}
			return acknowledgeRedisCleanupByeOutput(ctx, store, output.Write, deadline)
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
	if len(procedure.Events) != 0 {
		// The exact event retains this cleanup owner and re-drives its output.
		_, err = store.Commit(ctx, rediscore.Publication{Identity: due.Identity, Revision: due.Revision, Deadline: deadline, Timers: &rediscore.TimerMutation{Expected: due.Assignment, Target: due.Assignment, RequireLease: true, Changes: []rediscore.TimerChange{{Member: due.Member, Delete: true}}}})
		return err
	}
	index := slices.Index(dialog.Outputs, outputField)
	dialog.Outputs = slices.Delete(dialog.Outputs, index, index+1)
	encoded, err := marshalNativeRecord(storagecodec.NativeDialog, dialog)
	if err != nil {
		return err
	}
	timers := []rediscore.TimerChange{{Member: due.Member, Delete: true}}
	if current.Ended {
		if current.CallRetireAtMs <= 0 {
			return errNativeRecord
		}
		timers = append(timers, rediscore.TimerChange{Member: "call-retire", Due: time.UnixMilli(current.CallRetireAtMs)})
	}
	var result *nativepb.Result
	var resultChange *rediscore.Change
	var credit int64
	if application {
		rv, e := store.Read(ctx, due.Identity, output.Result)
		if e != nil {
			return e
		}
		if rv.Revision != due.Revision || rv.Timer != due.Assignment {
			return rediscore.ErrConflict
		}
		result, e = decodeNativeResult(rv.Records[output.Result])
		if e != nil || result.Sequence == 0 || result.Sequence > current.CommandSequence || output.Result != "r:result-"+strconv.FormatUint(result.Sequence, 10) || result.OperationId != output.Operation || result.Output != outputField || result.Pending {
			return errNativeRecord
		}
		if procedure.CreditContribution == 0 || procedure.CreditContribution > 1<<53-1 {
			return errNativeRecord
		}
		credit = -int64(procedure.CreditContribution)
		result.Output = ""
		value, e := encodeNativeResult(result)
		if e != nil {
			return e
		}
		resultChange = &rediscore.Change{Field: output.Result, Value: value}
		if current.CommandSequence > result.Sequence {
			timers = append(timers, rediscore.TimerChange{Member: "orig-result-" + strconv.FormatUint(result.Sequence, 10), Due: time.UnixMilli(result.CutoffMs)})
		}
	}
	var changes []rediscore.Change
	var selector rediscore.SelectorChange
	if application {
		changes, selector, err = planRedisCleanupByeRetirement(ctx, store, due, current, dialog, procedureField, procedure, result)
	} else {
		changes, selector, err = planRedisCleanupByeRetirement(ctx, store, due, current, dialog, procedureField, procedure)
	}
	if err != nil {
		return err
	}
	if dialog.Transaction != "" {
		parent, parentErr := store.LookupSelector(ctx, nativeSelector(rediscore.SelectorServerTransaction, dialog.Transaction))
		if parentErr != nil && !errors.Is(parentErr, rediscore.ErrAbsent) {
			return parentErr
		}
		if parentErr == nil {
			if parent.Identity != due.Identity || parent.View.Revision != due.Revision || parent.View.Timer != due.Assignment {
				return rediscore.ErrConflict
			}
			if slices.Contains(current.NoninitialProcedures, parent.Record) {
				owner, e := readPrivateProcedureRecord(parent.View.Records[parent.Record])
				if e != nil || owner.Native.Version != portable.Version || owner.Native.Family != string(portable.InviteServer) || owner.Native.Key != dialog.Transaction || dialog.RemoteInvite == nil || owner.Origin != dialog.RemoteInvite.Request || owner.LastResponse != dialog.RemoteInvite.FinalResponse {
					return errNativeRecord
				}
				if owner.Native.Phase == string(portable.Terminated) && len(owner.Events) == 0 && len(owner.Outputs) == 0 {
					timers = append(timers, rediscore.TimerChange{Member: "call-proc-" + owner.RequestId, Due: time.UnixMilli(due.CheckedAtMS)})
				}
			}
		}
	}
	// A cleanup BYE following a written local re-INVITE ACK borrows that
	// INVITE's continuation credit. Its origin field identifies the one exact
	// parent without scanning other procedures or decoding SIP again.
	if !application && dialog.LocalInvite != nil && strings.HasPrefix(dialog.LocalInvite.Request, "r:app-invite-origin-") {
		id := strings.TrimPrefix(dialog.LocalInvite.Request, "r:app-invite-origin-")
		parentField := "r:app-invite-" + id
		if len(id) != 32 || !slices.Contains(current.NoninitialProcedures, parentField) {
			return errNativeRecord
		}
		parentView, e := store.Read(ctx, due.Identity, parentField)
		if e != nil {
			return e
		}
		if parentView.Revision != due.Revision || parentView.Timer != due.Assignment {
			return rediscore.ErrConflict
		}
		owner, e := readPrivateProcedureRecord(parentView.Records[parentField])
		if e != nil || owner.Native.Version != portable.Version || owner.Native.Family != string(portable.InviteClient) || owner.Native.Key == "" || owner.Origin != dialog.LocalInvite.Request || owner.CreditContribution == 0 {
			return errNativeRecord
		}
		if owner.Native.Phase == string(portable.Terminated) && len(owner.Outputs) == 0 {
			timers = append(timers, rediscore.TimerChange{Member: "app-invite-retire-" + id, Due: time.UnixMilli(due.CheckedAtMS)})
		}
	}
	changes = append(changes, rediscore.Change{Field: output.Dialog, Value: encoded}, rediscore.Change{Field: outputField, Delete: true})
	if resultChange != nil {
		changes = append(changes, *resultChange)
	}
	_, err = store.Commit(ctx, rediscore.Publication{Identity: due.Identity, Revision: due.Revision, Deadline: deadline, SpendReservation: true, CreditDelta: credit, Changes: changes, Selectors: []rediscore.SelectorChange{selector}, Timers: &rediscore.TimerMutation{Expected: due.Assignment, Target: due.Assignment, RequireLease: true, Changes: timers}})
	return err
}
