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
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/nativepb"
	"sipbridge/internal/rediscore"
	"sipbridge/internal/selectedmessage"
	"sipbridge/internal/storagecodec"
)

type selectedRetainedACK struct {
	capture   nativeOutputCapture
	selected  *rediscore.ObservedContext
	current   *nativepb.Context
	procedure *nativepb.Procedure
}

func validReadyRetainedACK(meta *nativepb.Output, field string) bool {
	const prefix = "r:retained-ack-output-"
	if meta == nil || meta.Write == nil || meta.Write.Effect == nil || meta.Write.Effect.Context == nil || meta.Write.Flow == nil || meta.Write.Permit == nil || meta.Write.Permit.Authorization == nil || meta.Write.Permit.Authorization.AuthorityClass != pb.EffectAuthorization_NATIVE_COMPLETION || meta.Method != "ACK" || meta.Status != 0 || meta.AckSequence == 0 || !meta.Replay || meta.CleanupAfterAck || meta.Dialog != "" || meta.Result != "" || meta.Operation != "" || !strings.HasPrefix(field, prefix) {
		return false
	}
	attempt := strings.TrimPrefix(field, prefix)
	parsed, err := uuid.Parse(attempt)
	if err != nil || parsed.String() != attempt {
		return false
	}
	var id string
	switch {
	case strings.HasPrefix(meta.Procedure, "r:uac-procedure-"):
		id = strings.TrimPrefix(meta.Procedure, "r:uac-procedure-")
	case strings.HasPrefix(meta.Procedure, "r:app-invite-"):
		id = strings.TrimPrefix(meta.Procedure, "r:app-invite-")
	default:
		return false
	}
	decoded, err := hex.DecodeString(id)
	return err == nil && len(decoded) == 16 && hex.EncodeToString(decoded) == id
}

func inspectRedisRetainedACK(ctx context.Context, store *rediscore.Store, write *pb.FlowWrite, capture *nativeOutputCapture) (selectedRetainedACK, error) {
	var out selectedRetainedACK
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
	meta, proof := capture.metadata, capture.proof
	if !validReadyRetainedACK(meta, proof.MetadataField) || !proto.Equal(meta.Write, write) || capture.ref.RequireApplication || proof.DeadlineMS <= 0 || write.Permit.Authorization.DeadlineMs != uint64(proof.DeadlineMS) {
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
	initial := slices.Contains(current.InitialProcedures, meta.Procedure)
	noninitial := slices.Contains(current.NoninitialProcedures, meta.Procedure)
	if current.Context == nil || !proto.Equal(current.Context, write.Effect.Context) || current.Kind != pb.ContextSnapshot_UAS && current.Kind != pb.ContextSnapshot_UAC || initial == noninitial || strings.HasPrefix(meta.Procedure, "r:uac-procedure-") != initial || proc.Native.Version != portable.Version || proc.Native.Family != string(portable.InviteClient) || proc.Native.Phase != string(portable.Accepted) && proc.Native.Phase != string(portable.Terminated) || !slices.Contains(proc.Outputs, proof.MetadataField) || proc.SuccessfulAck == nil || !validNativeSuccessfulACK(proc.SuccessfulAck) || proc.SuccessfulAck.Ack != proof.MessageField || !proto.Equal(proc.SuccessfulAck.Flow, write.Flow) {
		return out, errNativeRecord
	}
	wire, err := store.Read(ctx, selected.Identity, proc.SuccessfulAck.Ack)
	if err != nil {
		return out, err
	}
	if wire.Revision != selected.View.Revision || wire.Timer != selected.View.Timer {
		return out, rediscore.ErrConflict
	}
	ack, err := selectedmessage.DecodeOpaque(wire.Records[proc.SuccessfulAck.Ack])
	if err != nil {
		return out, err
	}
	facts, err := ack.Facts()
	if err != nil || !facts.IsRequest || string(facts.Method) != "ACK" || facts.CSeq != meta.AckSequence || ack.Transport() != write.Flow.Transport {
		return out, errNativeRecord
	}
	return selectedRetainedACK{capture: *capture, selected: selected, current: current, procedure: proc}, nil
}

func settleRedisRetainedACK(ctx context.Context, store *rediscore.Store, write *pb.FlowWrite, capture *nativeOutputCapture, executor rediscore.TimerAssignment, deadline time.Time) error {
	selected, err := inspectRedisRetainedACK(ctx, store, write, capture)
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
	assignment := selected.selected.View.Timer
	target := assignment
	if target == (rediscore.TimerAssignment{}) {
		target = executor
	}
	publication := rediscore.Publication{Identity: selected.selected.Identity, Revision: selected.selected.View.Revision, Deadline: deadline, SpendReservation: true, Timers: &rediscore.TimerMutation{Expected: assignment, Target: target, Changes: []rediscore.TimerChange{{Member: "out-" + strings.TrimPrefix(proof.MetadataField, "r:"), Due: time.UnixMilli(proof.DeadlineMS)}}}}
	_, err = store.Commit(ctx, publication)
	if errors.Is(err, rediscore.ErrExpired) && assignment != (rediscore.TimerAssignment{}) && assignment != executor && ctx.Err() == nil && time.Now().Before(deadline) {
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

func acknowledgeRedisRetainedACK(ctx context.Context, store *rediscore.Store, write *pb.FlowWrite, capture *nativeOutputCapture) error {
	selected, err := inspectRedisRetainedACK(ctx, store, write, capture)
	if err != nil {
		return err
	}
	proof := selected.capture.proof
	if !proof.Settled || proof.State != rediscore.OutputWritten && proof.State != rediscore.OutputNotWritten {
		return errNativeRecord
	}
	return store.SettleOutput(ctx, selected.capture.ref, true, false)
}

// The procedure still owns its saved ACK after proof cleanup. Original M and
// all other physical attempts must close before the saved pair can be released.
func retireRedisRetainedACK(ctx context.Context, store *rediscore.Store, due *rediscore.DueContext, deadline time.Time) error {
	const prefix = "out-retained-ack-output-"
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
	output, err := decodeNativeOutput(view.Records[field])
	if err != nil || !validReadyRetainedACK(output, field) || int64(output.Write.Permit.Authorization.DeadlineMs) != due.DeadlineMS {
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
			if _, err = store.ExpireOutput(ctx, due, ref); err != nil {
				return err
			}
			proof, err = store.InspectOutput(ctx, ref)
			if err != nil {
				return err
			}
		}
		capture := &nativeOutputCapture{ref: ref, proof: proof, metadata: output}
		if proof.State == rediscore.OutputUnknown {
			selected, e := inspectRedisRetainedACK(ctx, store, output.Write, capture)
			if e != nil {
				return e
			}
			if selected.selected.Identity != due.Identity || selected.selected.View.Revision != due.Revision || selected.selected.View.Timer != due.Assignment || selected.capture.proof.State != rediscore.OutputUnknown {
				return rediscore.ErrConflict
			}
			timers := []rediscore.TimerChange{{Member: due.Member, Delete: true}}
			if selected.current.Ended {
				if selected.current.CallRetireAtMs <= 0 {
					return errNativeRecord
				}
				timers = append(timers, rediscore.TimerChange{Member: "call-retire", Due: time.UnixMilli(selected.current.CallRetireAtMs)})
			}
			_, e = store.Commit(ctx, rediscore.Publication{Identity: due.Identity, Revision: due.Revision, Deadline: deadline, SpendReservation: true, Timers: &rediscore.TimerMutation{Expected: due.Assignment, Target: due.Assignment, RequireLease: true, Changes: timers}})
			return e
		}
		if proof.State != rediscore.OutputWritten && proof.State != rediscore.OutputNotWritten {
			return rediscore.ErrUnknown
		}
		if err = settleRedisRetainedACK(ctx, store, output.Write, capture, due.Assignment, deadline); err != nil {
			return err
		}
		return acknowledgeRedisRetainedACK(ctx, store, output.Write, nil)
	}
	if !errors.Is(err, rediscore.ErrAbsent) {
		return err
	}
	current, err := decodeNativeContext(view.Records["r:context"])
	if err != nil {
		return err
	}
	owners, err := store.Read(ctx, due.Identity, output.Procedure)
	if err != nil {
		return err
	}
	if owners.Revision != due.Revision || owners.Timer != due.Assignment {
		return rediscore.ErrConflict
	}
	proc, err := readPrivateProcedureRecord(owners.Records[output.Procedure])
	if err != nil {
		return err
	}
	initial := slices.Contains(current.InitialProcedures, output.Procedure)
	noninitial := slices.Contains(current.NoninitialProcedures, output.Procedure)
	if current.Context == nil || current.Context.State == nil || current.Context.Id != due.Identity.Context || current.Context.State.Incarnation != due.Identity.Incarnation || current.Kind != pb.ContextSnapshot_UAS && current.Kind != pb.ContextSnapshot_UAC || !proto.Equal(current.Context, output.Write.Effect.Context) || initial == noninitial || strings.HasPrefix(output.Procedure, "r:uac-procedure-") != initial || proc.Native.Version != portable.Version || proc.Native.Family != string(portable.InviteClient) || proc.Native.Phase != string(portable.Accepted) && proc.Native.Phase != string(portable.Terminated) || proc.SuccessfulAck == nil || !validNativeSuccessfulACK(proc.SuccessfulAck) || !proto.Equal(proc.SuccessfulAck.Flow, output.Write.Flow) || !slices.Contains(proc.Outputs, field) {
		return errNativeRecord
	}
	proc.Outputs = slices.DeleteFunc(proc.Outputs, func(f string) bool { return f == field })
	var released []rediscore.Change
	if proc.Native.Phase == string(portable.Terminated) {
		released, _, err = planRedisSuccessfulACKRelease(ctx, store, due, current, output.Procedure, proc)
		if err != nil {
			return err
		}
	}
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
	if strings.HasPrefix(output.Procedure, "r:app-invite-") {
		timers = append(timers, rediscore.TimerChange{Member: "app-invite-retire-" + strings.TrimPrefix(output.Procedure, "r:app-invite-"), Due: time.UnixMilli(due.CheckedAtMS)})
	}
	changes := append([]rediscore.Change{{Field: output.Procedure, Value: data}, {Field: field, Delete: true}}, released...)
	_, err = store.Commit(ctx, rediscore.Publication{Identity: due.Identity, Revision: due.Revision, Deadline: deadline, SpendReservation: true, Changes: changes, Timers: &rediscore.TimerMutation{Expected: due.Assignment, Target: due.Assignment, RequireLease: true, Changes: timers}})
	return err
}
