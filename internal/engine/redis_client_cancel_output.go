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

type selectedClientCancelOutput struct {
	capture   nativeOutputCapture
	selected  *rediscore.ObservedContext
	current   *nativepb.Context
	procedure *nativepb.Procedure
}

func inspectRedisClientCancelOutput(ctx context.Context, store *rediscore.Store, write *pb.FlowWrite, capture *nativeOutputCapture) (selectedClientCancelOutput, error) {
	var out selectedClientCancelOutput
	if store == nil || write == nil || write.Effect == nil || write.Effect.Context == nil || write.Flow == nil || write.Permit == nil || write.Permit.Authorization == nil {
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
	meta, proof := capture.metadata, capture.proof
	if meta == nil || !proto.Equal(meta.Write, write) || capture.ref.RequireApplication || write.Permit.Authorization.AuthorityClass != pb.EffectAuthorization_TERMINAL_CLEANUP || meta.Method != "CANCEL" || meta.Status != 0 || meta.Dialog != "" || meta.Result != "" || meta.Operation != "" || proof.DeadlineMS <= 0 || write.Permit.Authorization.DeadlineMs != uint64(proof.DeadlineMS) || !strings.HasPrefix(proof.MetadataField, "r:cancel-client-output-") {
		return out, errNativeRecord
	}
	id := strings.TrimPrefix(proof.MetadataField, "r:cancel-client-output-")
	if len(id) != 32 || meta.Procedure != "r:cancel-client-"+id || proof.MessageField != "r:cancel-client-origin-"+id {
		return out, errNativeRecord
	}
	selected, err := store.ObserveContext(ctx, capture.ref.Context, capture.ref.Incarnation, "r:context", meta.Procedure, proof.MetadataField)
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
	proc, err := readPrivateProcedureRecord(selected.View.Records[meta.Procedure])
	if err != nil {
		return out, err
	}
	if current.Context == nil || !proto.Equal(current.Context, write.Effect.Context) || current.Kind != pb.ContextSnapshot_UAC && current.Kind != pb.ContextSnapshot_UAS || !slices.Contains(current.NoninitialProcedures, meta.Procedure) || proc.Native.Version != portable.Version || proc.Native.Family != string(portable.NonInviteClient) || !proc.Cleanup || proc.CreditContribution != 0 || proc.CommandResult != "" || proc.Origin != proof.MessageField || !slices.Contains(proc.Outputs, proof.MetadataField) || !proto.Equal(proc.Flow, write.Flow) || proof.Acknowledged && !proc.OutputConsumed {
		return out, errNativeRecord
	}
	return selectedClientCancelOutput{capture: *capture, selected: selected, current: current, procedure: proc}, nil
}

// settleRedisClientCancelOutput restores the original cutoff before settling a
// definite proof. UNKNOWN does not authorize any native or physical outcome.
func settleRedisClientCancelOutput(ctx context.Context, store *rediscore.Store, write *pb.FlowWrite, capture *nativeOutputCapture, executor rediscore.TimerAssignment, deadline time.Time) error {
	selected, err := inspectRedisClientCancelOutput(ctx, store, write, capture)
	if err != nil {
		return err
	}
	proof := selected.capture.proof
	if proof.State == rediscore.OutputStarted || proof.State == rediscore.OutputUnknown {
		return nil
	}
	if proof.State != rediscore.OutputWritten && proof.State != rediscore.OutputNotWritten {
		return errNativeRecord
	}
	target := selected.selected.View.Timer
	if target == (rediscore.TimerAssignment{}) {
		target = executor
	}
	publication := rediscore.Publication{Identity: selected.selected.Identity, Revision: selected.selected.View.Revision, Deadline: deadline, SpendReservation: true, Timers: &rediscore.TimerMutation{Expected: selected.selected.View.Timer, Target: target, Changes: []rediscore.TimerChange{{Member: "out-" + strings.TrimPrefix(proof.MetadataField, "r:"), Due: time.UnixMilli(proof.DeadlineMS)}}}}
	_, err = store.Commit(ctx, publication)
	if errors.Is(err, rediscore.ErrExpired) && selected.selected.View.Timer != (rediscore.TimerAssignment{}) && selected.selected.View.Timer != executor && ctx.Err() == nil && time.Now().Before(deadline) {
		publication.Timers.Target = executor
		_, err = store.Commit(ctx, publication)
	}
	if err != nil {
		return err
	}
	if !proof.Settled {
		return store.SettleOutput(ctx, selected.capture.ref, false, true)
	}
	return nil
}

func acknowledgeRedisClientCancelOutput(ctx context.Context, store *rediscore.Store, write *pb.FlowWrite, deadline time.Time) error {
	selected, err := inspectRedisClientCancelOutput(ctx, store, write, nil)
	if err != nil {
		return err
	}
	if !selected.capture.proof.Settled || selected.capture.proof.State != rediscore.OutputWritten && selected.capture.proof.State != rediscore.OutputNotWritten {
		return errNativeRecord
	}
	if !selected.procedure.OutputConsumed {
		selected.procedure.OutputConsumed = true
		data, e := marshalNativeRecord(storagecodec.NativeProcedure, selected.procedure)
		if e != nil {
			return e
		}
		_, e = store.Commit(ctx, rediscore.Publication{Identity: selected.selected.Identity, Revision: selected.selected.View.Revision, Deadline: deadline, SpendReservation: true, Changes: []rediscore.Change{{Field: selected.capture.metadata.Procedure, Value: data}}})
		if e != nil {
			return e
		}
	}
	return store.SettleOutput(ctx, selected.capture.ref, true, false)
}

func retireRedisClientCancelOutput(ctx context.Context, store *rediscore.Store, due *rediscore.DueContext, deadline time.Time) error {
	const prefix = "out-cancel-client-output-"
	if store == nil || due == nil || !strings.HasPrefix(due.Member, prefix) || due.CheckedAtMS < due.DeadlineMS || !time.Now().Before(deadline) {
		return errNativeRecord
	}
	id := strings.TrimPrefix(due.Member, prefix)
	if len(id) != 32 {
		return errNativeRecord
	}
	field, procedureField := "r:cancel-client-output-"+id, "r:cancel-client-"+id
	view, err := store.Read(ctx, due.Identity, "r:context", procedureField, field)
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
	proc, err := readPrivateProcedureRecord(view.Records[procedureField])
	if err != nil {
		return err
	}
	output, err := decodeNativeOutput(view.Records[field])
	if err != nil {
		return err
	}
	if current.Context == nil || !slices.Contains(current.NoninitialProcedures, procedureField) || proc.Native.Version != portable.Version || proc.Native.Family != string(portable.NonInviteClient) || !proc.Cleanup || proc.Origin != "r:cancel-client-origin-"+id || !slices.Contains(proc.Outputs, field) || output.Procedure != procedureField || output.Method != "CANCEL" || output.Status != 0 || output.Result != "" || output.Dialog != "" || !proto.Equal(output.Write.GetEffect().GetContext(), current.Context) || !proto.Equal(output.Write.Flow, proc.Flow) {
		return errNativeRecord
	}
	ref, err := nativeOutputRef(output.Write)
	if err != nil {
		return err
	}
	proof, err := store.InspectOutput(ctx, ref)
	if err == nil {
		if proof.MetadataField != field || !bytes.Equal(proof.Metadata, view.Records[field]) || proof.DeadlineMS != due.DeadlineMS {
			return rediscore.ErrConflict
		}
		if proof.State == rediscore.OutputPending || proof.State == rediscore.OutputStarted {
			_, e := store.ExpireOutput(ctx, due, ref)
			if e != nil {
				return e
			}
			proof, e = store.InspectOutput(ctx, ref)
			if e != nil {
				return e
			}
		}
		capture := &nativeOutputCapture{ref: ref, proof: proof, metadata: output}
		if proof.State == rediscore.OutputUnknown {
			selected, e := inspectRedisClientCancelOutput(ctx, store, output.Write, capture)
			if e != nil {
				return e
			}
			if selected.selected.Identity != due.Identity || selected.selected.View.Revision != due.Revision || selected.selected.View.Timer != due.Assignment {
				return rediscore.ErrConflict
			}
			timers := []rediscore.TimerChange{{Member: due.Member, Delete: true}}
			if current.Ended {
				if current.CallRetireAtMs <= 0 {
					return errNativeRecord
				}
				timers = append(timers, rediscore.TimerChange{Member: "call-retire", Due: time.UnixMilli(current.CallRetireAtMs)})
			}
			_, e = store.Commit(ctx, rediscore.Publication{Identity: due.Identity, Revision: due.Revision, Deadline: deadline, SpendReservation: true, Timers: &rediscore.TimerMutation{Expected: due.Assignment, Target: due.Assignment, RequireLease: true, Changes: timers}})
			return e
		}
		if proof.State != rediscore.OutputWritten && proof.State != rediscore.OutputNotWritten {
			return rediscore.ErrUnknown
		}
		if err = settleRedisClientCancelOutput(ctx, store, output.Write, capture, due.Assignment, deadline); err != nil {
			return err
		}
		return acknowledgeRedisClientCancelOutput(ctx, store, output.Write, deadline)
	}
	if !errors.Is(err, rediscore.ErrAbsent) {
		return err
	}
	if !proc.OutputConsumed {
		return errors.New("CANCEL proof absence lacks consumed marker")
	}
	if proc.Native.Phase != string(portable.Terminated) {
		// The response selector still needs the exact original metadata until
		// F/K closes the native transaction; only its consumed physical head ends.
		timers := []rediscore.TimerChange{{Member: due.Member, Delete: true}}
		if current.Ended {
			if current.CallRetireAtMs <= 0 {
				return errNativeRecord
			}
			timers = append(timers, rediscore.TimerChange{Member: "call-retire", Due: time.UnixMilli(current.CallRetireAtMs)})
		}
		_, err = store.Commit(ctx, rediscore.Publication{Identity: due.Identity, Revision: due.Revision, Deadline: deadline, SpendReservation: true, Timers: &rediscore.TimerMutation{Expected: due.Assignment, Target: due.Assignment, RequireLease: true, Changes: timers}})
		return err
	}
	index, ok := slices.BinarySearch(proc.Outputs, field)
	if !ok {
		return errNativeRecord
	}
	proc.Outputs = slices.Delete(proc.Outputs, index, index+1)
	data, err := marshalNativeRecord(storagecodec.NativeProcedure, proc)
	if err != nil {
		return err
	}
	timers := []rediscore.TimerChange{{Member: due.Member, Delete: true}}
	if proc.Native.Phase == string(portable.Terminated) && len(proc.Outputs) == 0 {
		timers = append(timers, rediscore.TimerChange{Member: "cancel-client-retire-" + id, Due: time.UnixMilli(due.CheckedAtMS)})
	}
	if current.Ended {
		if current.CallRetireAtMs <= 0 {
			return errNativeRecord
		}
		timers = append(timers, rediscore.TimerChange{Member: "call-retire", Due: time.UnixMilli(current.CallRetireAtMs)})
	}
	_, err = store.Commit(ctx, rediscore.Publication{Identity: due.Identity, Revision: due.Revision, Deadline: deadline, SpendReservation: true, Changes: []rediscore.Change{{Field: procedureField, Value: data}, {Field: field, Delete: true}}, Timers: &rediscore.TimerMutation{Expected: due.Assignment, Target: due.Assignment, RequireLease: true, Changes: timers}})
	return err
}
