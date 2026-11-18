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

type selectedUACFailureACK struct {
	capture   nativeOutputCapture
	selected  *rediscore.ObservedContext
	procedure *nativepb.Procedure
	current   *nativepb.Context
}

func inspectRedisUACFailureACK(ctx context.Context, store *rediscore.Store, write *pb.FlowWrite, capture *nativeOutputCapture) (selectedUACFailureACK, error) {
	var out selectedUACFailureACK
	if store == nil || write == nil || write.Effect == nil || write.Effect.Context == nil || write.Flow == nil || write.Permit == nil {
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
	meta := capture.metadata
	if meta == nil || !proto.Equal(meta.Write, write) || capture.ref.RequireApplication || meta.Method != "ACK" || meta.Status != 0 || meta.Dialog != "" || meta.Result != "" || !strings.HasPrefix(meta.Procedure, "r:uac-procedure-") || !strings.HasPrefix(capture.proof.MetadataField, "r:uac-ack-output-") {
		return out, errNativeRecord
	}
	id := strings.TrimPrefix(meta.Procedure, "r:uac-procedure-")
	if len(id) != 32 || capture.proof.MessageField != "r:uac-ack-"+id {
		return out, errNativeRecord
	}
	selected, err := store.ObserveContext(ctx, capture.ref.Context, capture.ref.Incarnation, "r:context", meta.Procedure, capture.proof.MetadataField)
	if err != nil {
		return out, err
	}
	if !bytes.Equal(selected.View.Records[capture.proof.MetadataField], capture.proof.Metadata) {
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
	if current.Kind != pb.ContextSnapshot_UAC || !proto.Equal(current.Context, write.Effect.Context) || !slices.Contains(current.InitialProcedures, meta.Procedure) || proc.Native.Version != portable.Version || proc.Native.Family != string(portable.InviteClient) || proc.Native.Phase != string(portable.Completed) && proc.Native.Phase != string(portable.Terminated) || proc.Origin != "r:uac-origin-"+id || proc.Ack != capture.proof.MessageField || proc.AckEmissions == 0 || !slices.Contains(proc.Outputs, capture.proof.MetadataField) || !proto.Equal(proc.Flow, write.Flow) {
		return out, errNativeRecord
	}
	return selectedUACFailureACK{capture: *capture, selected: selected, procedure: proc, current: current}, nil
}

// The recovered path and live callback both index the exact due before native
// settlement. U never grants a new attempt or a physical outcome.
func settleRedisUACFailureACK(ctx context.Context, store *rediscore.Store, write *pb.FlowWrite, capture *nativeOutputCapture, executor rediscore.TimerAssignment, deadline time.Time) error {
	selected, err := inspectRedisUACFailureACK(ctx, store, write, capture)
	if err != nil {
		return err
	}
	proof := selected.capture.proof
	if proof.State == rediscore.OutputStarted || proof.State == rediscore.OutputUnknown {
		return nil
	}
	if proof.State != rediscore.OutputWritten && proof.State != rediscore.OutputNotWritten || proof.DeadlineMS <= 0 {
		return errNativeRecord
	}
	assignment := selected.selected.View.Timer
	target := assignment
	if target == (rediscore.TimerAssignment{}) {
		target = executor
	}
	publication := rediscore.Publication{Identity: selected.selected.Identity, Revision: selected.selected.View.Revision, Deadline: deadline, SpendReservation: true, Timers: &rediscore.TimerMutation{Expected: assignment, Target: target, Changes: []rediscore.TimerChange{{Member: "out-" + strings.TrimPrefix(proof.MetadataField, "r:"), Due: time.UnixMilli(proof.DeadlineMS)}}}}
	_, err = store.Commit(ctx, publication)
	if errors.Is(err, rediscore.ErrExpired) && assignment != (rediscore.TimerAssignment{}) && assignment != executor && time.Now().Before(deadline) && ctx.Err() == nil {
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

// Only an actual dispatcher or recovered exact consumer acknowledges; the
// ACK has no W-dependent native transition and needs no procedure marker.
func acknowledgeRedisUACFailureACK(ctx context.Context, store *rediscore.Store, write *pb.FlowWrite, capture *nativeOutputCapture) error {
	selected, err := inspectRedisUACFailureACK(ctx, store, write, capture)
	if err != nil {
		return err
	}
	proof := selected.capture.proof
	if !proof.Settled || proof.State != rediscore.OutputWritten && proof.State != rediscore.OutputNotWritten {
		return errNativeRecord
	}
	return store.SettleOutput(ctx, selected.capture.ref, true, false)
}

// retireRedisUACFailureACK consumes a known exact outcome, or after proof
// release removes only this attempt. Absence never asserts W or N.
func retireRedisUACFailureACK(ctx context.Context, store *rediscore.Store, due *rediscore.DueContext, deadline time.Time) error {
	const prefix = "out-uac-ack-output-"
	if store == nil || due == nil || !strings.HasPrefix(due.Member, prefix) || due.CheckedAtMS < due.DeadlineMS || !time.Now().Before(deadline) {
		return errNativeRecord
	}
	field := "r:" + strings.TrimPrefix(due.Member, "out-")
	view, err := store.Read(ctx, due.Identity, "r:context", field)
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
	output, err := decodeNativeOutput(view.Records[field])
	if err != nil {
		return err
	}
	if output.Write == nil || output.Write.Effect == nil || output.Write.Effect.Context == nil || output.Write.Flow == nil || output.Write.Permit == nil {
		return errNativeRecord
	}
	if current.Kind != pb.ContextSnapshot_UAC || output.Method != "ACK" || output.Dialog != "" || output.Result != "" || !strings.HasPrefix(output.Procedure, "r:uac-procedure-") || len(strings.TrimPrefix(output.Procedure, "r:uac-procedure-")) != 32 || !proto.Equal(output.Write.Effect.Context, current.Context) || !slices.Contains(current.InitialProcedures, output.Procedure) {
		return errNativeRecord
	}
	ref, err := nativeOutputRef(output.Write)
	if err != nil {
		return err
	}
	if ref.RequireApplication {
		return errNativeRecord
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
			selected, e := inspectRedisUACFailureACK(ctx, store, output.Write, capture)
			if e != nil {
				return e
			}
			if selected.selected.Identity != due.Identity || selected.selected.View.Revision != due.Revision || selected.selected.View.Timer != due.Assignment {
				return rediscore.ErrConflict
			}
			if !selected.current.Ended || selected.current.CallRetireAtMs <= 0 {
				return errNativeRecord
			}
			_, e = store.Commit(ctx, rediscore.Publication{Identity: due.Identity, Revision: due.Revision, Deadline: deadline, SpendReservation: true, Timers: &rediscore.TimerMutation{Expected: due.Assignment, Target: due.Assignment, RequireLease: true, Changes: []rediscore.TimerChange{{Member: due.Member, Delete: true}, {Member: "call-retire", Due: time.UnixMilli(selected.current.CallRetireAtMs)}}}})
			return e
		}
		if proof.State != rediscore.OutputWritten && proof.State != rediscore.OutputNotWritten {
			return rediscore.ErrUnknown
		}
		if err = settleRedisUACFailureACK(ctx, store, output.Write, capture, due.Assignment, deadline); err != nil {
			return err
		}
		proof, err = store.InspectOutput(ctx, ref)
		if err != nil {
			return err
		}
		capture.proof = proof
		return acknowledgeRedisUACFailureACK(ctx, store, output.Write, capture)
	}
	if !errors.Is(err, rediscore.ErrAbsent) {
		return err
	}
	selected, err := store.Read(ctx, due.Identity, output.Procedure)
	if err != nil {
		return err
	}
	if selected.Revision != view.Revision || selected.Timer != view.Timer {
		return rediscore.ErrConflict
	}
	proc, err := readPrivateProcedureRecord(selected.Records[output.Procedure])
	if err != nil {
		return err
	}
	if proc.Native.Version != portable.Version || proc.Native.Family != string(portable.InviteClient) || proc.Native.Phase != string(portable.Completed) && proc.Native.Phase != string(portable.Terminated) || proc.Origin != "r:uac-origin-"+strings.TrimPrefix(output.Procedure, "r:uac-procedure-") || proc.Ack != "r:uac-ack-"+strings.TrimPrefix(output.Procedure, "r:uac-procedure-") || proc.AckEmissions == 0 || !proto.Equal(proc.Flow, output.Write.Flow) {
		return errNativeRecord
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
	if current.Ended {
		if current.CallRetireAtMs <= 0 {
			return errNativeRecord
		}
		timers = append(timers, rediscore.TimerChange{Member: "call-retire", Due: time.UnixMilli(current.CallRetireAtMs)})
	}
	_, err = store.Commit(ctx, rediscore.Publication{Identity: due.Identity, Revision: due.Revision, Deadline: deadline, SpendReservation: true, Changes: []rediscore.Change{{Field: output.Procedure, Value: data}, {Field: field, Delete: true}}, Timers: &rediscore.TimerMutation{Expected: due.Assignment, Target: due.Assignment, RequireLease: true, Changes: timers}})
	return err
}
