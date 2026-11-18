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
	"sipbridge/internal/storagepb"
)

func nativeDeadlinesInactive(values map[string]*storagepb.PortableDeadline) bool {
	for _, timer := range values {
		if timer == nil || timer.Active {
			return false
		}
	}
	return true
}

// forgetRedisEndedCleanupBye is a deliberately narrow private qualification:
// exactly one actual noninitial cleanup procedure and one uncertain output.
// Other native debts fail closed for their own bounded terminal traversal.
func forgetRedisEndedCleanupBye(ctx context.Context, store *rediscore.Store, due *rediscore.DueContext, deadline time.Time) (bool, error) {
	if store == nil || due == nil || due.Member != "call-retire" || due.CheckedAtMS < due.DeadlineMS || !time.Now().Before(deadline) {
		return false, errNativeRecord
	}
	head, err := store.Read(ctx, due.Identity, "r:context")
	if err != nil {
		return false, err
	}
	if head.Revision != due.Revision || head.Timer != due.Assignment {
		return false, rediscore.ErrConflict
	}
	current, err := decodeNativeContext(head.Records["r:context"])
	if err != nil {
		return false, err
	}
	if current.Context == nil || current.Context.State == nil || current.Context.Id != due.Identity.Context || current.Context.State.Incarnation != due.Identity.Incarnation || current.Kind != pb.ContextSnapshot_UAS && current.Kind != pb.ContextSnapshot_UAC || !current.Ended || current.CallRetireAtMs <= 0 || len(current.NoninitialProcedures) != 1 || len(current.InitialProcedures) == 0 {
		return false, errNativeRecord
	}
	other, err := store.PeekContextDeadline(ctx, due.Identity, due.Revision, due.Member)
	if err != nil {
		return false, err
	}
	if other != nil {
		return false, nil
	}
	procedureField := current.NoninitialProcedures[0]
	fields := append([]string(nil), current.InitialProcedures...)
	fields = append(fields, current.Dialogs...)
	fields = append(fields, procedureField)
	resultField := ""
	if current.CommandSequence != 0 {
		resultField = "r:result-" + strconv.FormatUint(current.CommandSequence, 10)
		fields = append(fields, resultField)
	}
	view, err := store.Read(ctx, due.Identity, fields...)
	if err != nil {
		return false, err
	}
	if view.Revision != due.Revision || view.Timer != due.Assignment {
		return false, rediscore.ErrConflict
	}
	if resultField != "" {
		result, e := decodeNativeResult(view.Records[resultField])
		if e != nil || result.Sequence != current.CommandSequence {
			return false, errNativeRecord
		}
		if result.Pending || result.Output != "" {
			return false, nil
		}
		if result.CutoffMs > due.CheckedAtMS {
			_, err = store.Commit(ctx, rediscore.Publication{Identity: due.Identity, Revision: due.Revision, Deadline: deadline, SpendReservation: true, Timers: &rediscore.TimerMutation{Expected: due.Assignment, Target: due.Assignment, RequireLease: true, Changes: []rediscore.TimerChange{{Member: due.Member, Due: time.UnixMilli(result.CutoffMs)}}}})
			return false, err
		}
	}
	for _, field := range current.InitialProcedures {
		p, e := readPrivateProcedureRecord(view.Records[field])
		if e != nil {
			return false, e
		}
		if p.Native.Version != portable.Version || p.Native.Family != string(portable.InviteServer) && p.Native.Family != string(portable.InviteClient) || p.Native.Phase != string(portable.Terminated) || len(p.Outputs) != 0 || len(p.Events) != 0 || p.Native.Timers != nil && !nativeDeadlinesInactive(p.Native.Timers.Values) {
			return false, nil
		}
	}
	procedure, err := readPrivateProcedureRecord(view.Records[procedureField])
	if err != nil {
		return false, err
	}
	if procedure.Native.Version != portable.Version || procedure.Native.Family != string(portable.NonInviteClient) || procedure.Native.Key == "" || procedure.Native.Phase != string(portable.Terminated) || !procedure.Cleanup || procedure.OutputConsumed || procedure.CommandResult != "" || procedure.Native.Timers != nil && !nativeDeadlinesInactive(procedure.Native.Timers.Values) {
		return false, nil
	}
	id := strings.TrimPrefix(procedureField, "r:call-cleanup-")
	if len(id) != 32 || procedureField != "r:call-cleanup-"+id || procedure.Origin != "r:call-cleanup-origin-"+id {
		return false, errNativeRecord
	}
	var ownerField string
	var owner *nativepb.Dialog
	for _, field := range current.Dialogs {
		d, e := readPrivateDialogRecord(view.Records[field])
		if e != nil {
			return false, e
		}
		if d.Native.Version != portable.Version || d.Native.CallId != current.CallId || d.Native.Phase != string(portable.DialogEnded) || d.Native.Timers != nil && !nativeDeadlinesInactive(d.Native.Timers.Values) {
			return false, nil
		}
		if d.LocalTransaction == procedure.Native.Key && d.LocalBye == procedure.Origin {
			if owner != nil {
				return false, errNativeRecord
			}
			owner, ownerField = d, field
		} else if len(d.Outputs) != 0 {
			return false, nil
		}
	}
	if owner == nil || len(owner.Outputs) != 1 {
		return false, nil
	}
	outputField := owner.Outputs[0]
	if outputField != "r:call-cleanup-output-"+id {
		return false, errNativeRecord
	}
	selected, err := store.Read(ctx, due.Identity, outputField)
	if err != nil {
		return false, err
	}
	if selected.Revision != due.Revision || selected.Timer != due.Assignment {
		return false, rediscore.ErrConflict
	}
	output, err := decodeNativeOutput(selected.Records[outputField])
	if err != nil {
		return false, err
	}
	if output.Procedure != procedureField || output.Dialog != ownerField || output.Method != "BYE" || output.Status != 0 || output.Result != "" || output.Operation != "" || output.Write.Permit.Authorization.AuthorityClass != pb.EffectAuthorization_TERMINAL_CLEANUP || !proto.Equal(output.Write.Effect.Context, current.Context) || !proto.Equal(output.Write.Flow, procedure.Flow) || output.Write.Permit.Authorization.DeadlineMs == 0 || output.Write.Permit.Authorization.DeadlineMs > uint64((1<<53-1)-rediscore.OutputObsoleteGrace.Milliseconds()) {
		return false, errNativeRecord
	}
	physicalCutoff := int64(output.Write.Permit.Authorization.DeadlineMs) + rediscore.OutputObsoleteGrace.Milliseconds()
	cutoff := max(current.CallRetireAtMs, physicalCutoff)
	if due.DeadlineMS != cutoff {
		_, err = store.Commit(ctx, rediscore.Publication{Identity: due.Identity, Revision: due.Revision, Deadline: deadline, SpendReservation: true, Timers: &rediscore.TimerMutation{Expected: due.Assignment, Target: due.Assignment, RequireLease: true, Changes: []rediscore.TimerChange{{Member: due.Member, Due: time.UnixMilli(cutoff)}}}})
		return false, err
	}
	if due.CheckedAtMS < cutoff {
		return false, nil
	}
	ref, err := nativeOutputRef(output.Write)
	if err != nil || ref.RequireApplication {
		return false, errNativeRecord
	}
	proof, err := store.InspectOutput(ctx, ref)
	if err == nil {
		if proof.MetadataField != outputField || proof.MessageField != procedure.Origin || !bytes.Equal(proof.Metadata, selected.Records[outputField]) || proof.DeadlineMS != int64(output.Write.Permit.Authorization.DeadlineMs) {
			return false, errNativeRecord
		}
		if proof.State == rediscore.OutputWritten || proof.State == rediscore.OutputNotWritten {
			capture := &nativeOutputCapture{ref: ref, proof: proof, metadata: output}
			if err = settleRedisCleanupByeOutput(ctx, store, output.Write, capture, due.Assignment, deadline); err != nil {
				return false, err
			}
			if !proof.Acknowledged {
				err = acknowledgeRedisCleanupByeOutput(ctx, store, output.Write, deadline)
			}
			return false, err
		}
		if proof.State != rediscore.OutputUnknown {
			return false, errNativeRecord
		}
		if err = store.ForgetObsoleteCallOutput(ctx, due, ref, current.CallRetireAtMs, true); err != nil {
			return false, err
		}
		return false, nil
	}
	if !errors.Is(err, rediscore.ErrAbsent) {
		return false, err
	}
	changes, selector, err := planRedisCleanupByeRetirement(ctx, store, due, current, owner, procedureField, procedure)
	if err != nil {
		return false, err
	}
	index := slices.Index(owner.Outputs, outputField)
	owner.Outputs = slices.Delete(owner.Outputs, index, index+1)
	ownerBytes, err := marshalNativeRecord(storagecodec.NativeDialog, owner)
	if err != nil {
		return false, err
	}
	changes = append(changes, rediscore.Change{Field: ownerField, Value: ownerBytes}, rediscore.Change{Field: outputField, Delete: true})
	_, err = store.Commit(ctx, rediscore.Publication{Identity: due.Identity, Revision: due.Revision, Deadline: deadline, SpendReservation: true, Changes: changes, Selectors: []rediscore.SelectorChange{selector}, Timers: &rediscore.TimerMutation{Expected: due.Assignment, Target: due.Assignment, RequireLease: true, Changes: []rediscore.TimerChange{{Member: due.Member, Due: time.UnixMilli(cutoff)}}}})
	return err == nil, err
}
