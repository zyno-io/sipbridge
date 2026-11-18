package engine

import (
	"bytes"
	"context"
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

// settleRedisUACInitialOutput consumes a recorded physical result. The caller
// supplies the current live executor; raw proof fallback never calls this.
func settleRedisUACInitialOutput(ctx context.Context, store *rediscore.Store, write *pb.FlowWrite, capture *nativeOutputCapture, executor rediscore.TimerAssignment, deadline time.Time) error {
	if store == nil || capture == nil || capture.metadata == nil || !proto.Equal(capture.metadata.Write, write) || !capture.ref.RequireApplication || capture.metadata.Method != "INVITE" || capture.metadata.Status != 0 || capture.metadata.Replay || capture.metadata.Dialog != "" || !strings.HasPrefix(capture.metadata.Procedure, "r:uac-procedure-") {
		return errNativeRecord
	}
	id := strings.TrimPrefix(capture.metadata.Procedure, "r:uac-procedure-")
	if len(id) != 32 || capture.metadata.Result != "r:result-1" || capture.proof.MetadataField != "r:uac-output-"+id {
		return errNativeRecord
	}
	proof := capture.proof
	if proof.State == rediscore.OutputStarted || proof.State == rediscore.OutputUnknown {
		return nil
	}
	if proof.State != rediscore.OutputWritten && proof.State != rediscore.OutputNotWritten || proof.DeadlineMS <= 0 {
		return errNativeRecord
	}
	selected, err := store.ObserveContext(ctx, capture.ref.Context, capture.ref.Incarnation, "r:context", capture.metadata.Procedure, capture.metadata.Result, capture.proof.MetadataField)
	if err != nil {
		return err
	}
	if !bytes.Equal(selected.View.Records[capture.proof.MetadataField], proof.Metadata) {
		return rediscore.ErrConflict
	}
	current, err := decodeNativeContext(selected.View.Records["r:context"])
	if err != nil {
		return err
	}
	proc, err := readPrivateProcedureRecord(selected.View.Records[capture.metadata.Procedure])
	if err != nil {
		return err
	}
	result, err := decodeNativeResult(selected.View.Records[capture.metadata.Result])
	if err != nil {
		return err
	}
	if current.Kind != pb.ContextSnapshot_UAC || !proto.Equal(current.Context, write.Effect.Context) || !slices.Contains(current.InitialProcedures, capture.metadata.Procedure) || proc.Native.Family != string(portable.InviteClient) || proc.Origin != "r:uac-origin-"+id || proc.CommandResult != capture.metadata.Result || !slices.Contains(proc.Outputs, capture.proof.MetadataField) || !proto.Equal(proc.Flow, write.Flow) || result.Sequence != 1 || result.OperationId != capture.metadata.Operation || proof.Acknowledged && !proc.OutputConsumed {
		return errNativeRecord
	}
	if _, err = validateRedisUACOutputRoot(ctx, store, selected.Identity, selected.View.Revision, selected.View.Timer, current, capture.metadata.Procedure, proc, capture.proof.MetadataField, result); err != nil {
		return err
	}
	now := time.Now()
	if ctx.Err() != nil || !now.Before(deadline) {
		return rediscore.ErrExpired
	}
	timers := []rediscore.TimerChange{{Member: "out-" + strings.TrimPrefix(capture.proof.MetadataField, "r:"), Due: time.UnixMilli(proof.DeadlineMS)}}
	var changes []rediscore.Change
	if proof.State == rediscore.OutputNotWritten && proc.LastResponse == "" && proc.Native.Phase != string(portable.Terminated) && !current.Ended {
		originView, e := store.Read(ctx, selected.Identity, proc.Origin)
		if errors.Is(e, rediscore.ErrAbsent) {
			return rediscore.ErrConflict
		}
		if e != nil {
			return e
		}
		if originView.Revision != selected.View.Revision || originView.Timer != selected.View.Timer {
			return rediscore.ErrConflict
		}
		refs := nativeProcedureRefs{Origin: proc.Origin, LastResponse: proc.LastResponse, ACK: proc.Ack}
		native := storageDecodePortableTransaction(proc.Native)
		proc.Native, proc.Origin, proc.LastResponse, proc.Ack = nil, "", "", ""
		procedure := nativeProcedureRecord{Native: native, Refs: refs, Metadata: proc}
		memo, e := selectedmessage.NewMemo(1)
		if e != nil {
			return e
		}
		tx, e := procedure.attach(&nativeSelectedMessages{records: originView.Records, memo: memo, provenance: "admitted"}, true, false, false)
		if e != nil {
			return e
		}
		facts, e := tx.Origin.Facts()
		if e != nil || facts.ToTag != "" {
			return errNativeRecord
		}
		next, effects, e := portable.StepTransaction(tx, portable.Input{Kind: portable.TransportFailure, Now: now.UnixMilli(), Error: "SIP write did not start"})
		if e != nil {
			return e
		}
		for _, effect := range effects {
			switch effect.Kind {
			case portable.CancelTimer:
				if effect.Timer != portable.TimerA {
					timers = append(timers, rediscore.TimerChange{Member: "call-tx-" + id + "-" + string(effect.Timer), Delete: true})
				}
			case portable.Schedule:
				if effect.Timer != portable.TimerA {
					timers = append(timers, rediscore.TimerChange{Member: "call-tx-" + id + "-" + string(effect.Timer), Due: time.UnixMilli(effect.Deadline.At)})
				}
			case portable.Ended, portable.Diagnostic:
			default:
				return errors.New("unexpected initial INVITE failure effect")
			}
		}
		if current.Revision >= 1<<53-1 {
			return errNativeRecord
		}
		current.Ended = true
		current.Revision++
		if current.CallRetireAtMs == 0 {
			current.CallRetireAtMs = now.Add(2 * time.Minute).UnixMilli()
		}
		snapshot, e := nativeProjectSnapshot(current, nil, nil, nil)
		if e != nil {
			return e
		}
		eventID := uuid.NewString()
		event, e := encodeNativeLifecycle(&nativepb.Lifecycle{Kind: pb.DialogEvent_FAILED, Snapshot: snapshot, OperationId: result.OperationId, DeadlineMs: now.Add(2 * time.Minute).UnixMilli(), NextMs: now.UnixMilli()})
		if e != nil {
			return e
		}
		contextData, e := encodeNativeContext(current)
		if e != nil {
			return e
		}
		procData, e := encodeNativeProcedure(next, procedure.Refs, procedure.Metadata)
		if e != nil {
			return e
		}
		changes = append(changes, rediscore.Change{Field: "r:context", Value: contextData}, rediscore.Change{Field: capture.metadata.Procedure, Value: procData}, rediscore.Change{Field: "r:event-" + eventID, Value: event})
		timers = append(timers, rediscore.TimerChange{Member: "event-" + eventID, Due: now}, rediscore.TimerChange{Member: "call-retire", Due: time.UnixMilli(current.CallRetireAtMs)})
		if result.Certainty == pb.OperationResult_ACCEPTED || result.Certainty == pb.OperationResult_UNKNOWN {
			result.Certainty = pb.OperationResult_COMPLETED
			result.Error = "SIP write did not start"
			result.SipStatus = 0
			result.Request = ""
			result.Response = ""
			result.Pending = false
			data, e := encodeNativeResult(result)
			if e != nil {
				return e
			}
			changes = append(changes, rediscore.Change{Field: capture.metadata.Result, Value: data})
		}
	}
	target := selected.View.Timer
	if target == (rediscore.TimerAssignment{}) {
		target = executor
	}
	publication := rediscore.Publication{Identity: selected.Identity, Revision: selected.View.Revision, Deadline: deadline, SpendReservation: true, Changes: changes, Timers: &rediscore.TimerMutation{Expected: selected.View.Timer, Target: target, Changes: timers}}
	_, err = store.Commit(ctx, publication)
	if errors.Is(err, rediscore.ErrExpired) && selected.View.Timer != (rediscore.TimerAssignment{}) && selected.View.Timer != executor && time.Now().Before(deadline) && ctx.Err() == nil {
		publication.Timers.Target = executor
		_, err = store.Commit(ctx, publication)
	}
	if err != nil {
		return err
	}
	if !proof.Settled {
		return store.SettleOutput(ctx, capture.ref, false, true)
	}
	return nil
}

// acknowledgeRedisUACInitialOutput is called only after an actual consumer
// returns. Exact proof remains pinned until the consumption marker commits.
func acknowledgeRedisUACInitialOutput(ctx context.Context, store *rediscore.Store, capture *nativeOutputCapture, deadline time.Time) error {
	if store == nil || capture == nil || capture.metadata == nil || !capture.ref.RequireApplication || capture.metadata.Method != "INVITE" || capture.metadata.Status != 0 || capture.metadata.Replay || capture.metadata.Result != "r:result-1" || !strings.HasPrefix(capture.metadata.Procedure, "r:uac-procedure-") || !capture.proof.Settled || capture.proof.State != rediscore.OutputWritten && capture.proof.State != rediscore.OutputNotWritten {
		return errNativeRecord
	}
	id := strings.TrimPrefix(capture.metadata.Procedure, "r:uac-procedure-")
	if len(id) != 32 || capture.proof.MetadataField != "r:uac-output-"+id {
		return errNativeRecord
	}
	view, err := store.ObserveContext(ctx, capture.ref.Context, capture.ref.Incarnation, "r:context", capture.metadata.Procedure, capture.metadata.Result, capture.proof.MetadataField)
	if err != nil {
		return err
	}
	if !bytes.Equal(view.View.Records[capture.proof.MetadataField], capture.proof.Metadata) {
		return rediscore.ErrConflict
	}
	proc, err := readPrivateProcedureRecord(view.View.Records[capture.metadata.Procedure])
	if err != nil {
		return err
	}
	current, err := decodeNativeContext(view.View.Records["r:context"])
	if err != nil {
		return err
	}
	result, err := decodeNativeResult(view.View.Records[capture.metadata.Result])
	if err != nil {
		return err
	}
	if current.Kind != pb.ContextSnapshot_UAC || !proto.Equal(current.Context, capture.metadata.Write.GetEffect().GetContext()) || !slices.Contains(current.InitialProcedures, capture.metadata.Procedure) || proc.Native.Family != string(portable.InviteClient) || proc.Origin != "r:uac-origin-"+id || !proto.Equal(proc.Flow, capture.metadata.Write.GetFlow()) || !slices.Contains(proc.Outputs, capture.proof.MetadataField) || result.OperationId != capture.metadata.Operation {
		return errNativeRecord
	}
	if _, err = validateRedisUACOutputRoot(ctx, store, view.Identity, view.View.Revision, view.View.Timer, current, capture.metadata.Procedure, proc, capture.proof.MetadataField, result); err != nil {
		return err
	}
	if !proc.OutputConsumed {
		proc.OutputConsumed = true
		data, e := marshalNativeRecord(storagecodec.NativeProcedure, proc)
		if e != nil {
			return e
		}
		_, e = store.Commit(ctx, rediscore.Publication{Identity: view.Identity, Revision: view.View.Revision, Deadline: deadline, SpendReservation: true, Changes: []rediscore.Change{{Field: capture.metadata.Procedure, Value: data}}})
		if e != nil {
			return e
		}
	}
	return store.SettleOutput(ctx, capture.ref, true, false)
}

// retireRedisUACInitialOutput removes only the consumed physical attempt;
// the initial transaction, its origin and its pooled contribution remain for
// late responses and call retirement.
func retireRedisUACInitialOutput(ctx context.Context, store *rediscore.Store, due *rediscore.DueContext, deadline time.Time) error {
	const prefix = "out-uac-output-"
	if store == nil || due == nil || due.CheckedAtMS < due.DeadlineMS || !strings.HasPrefix(due.Member, prefix) || !time.Now().Before(deadline) {
		return errNativeRecord
	}
	id := strings.TrimPrefix(due.Member, prefix)
	if len(id) != 32 {
		return errNativeRecord
	}
	outputField, procedureField := "r:uac-output-"+id, "r:uac-procedure-"+id
	view, err := store.Read(ctx, due.Identity, "r:context", procedureField, "r:result-1", outputField)
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
	result, err := decodeNativeResult(view.Records["r:result-1"])
	if err != nil {
		return err
	}
	output, err := decodeNativeOutput(view.Records[outputField])
	if err != nil {
		return err
	}
	if current.Kind != pb.ContextSnapshot_UAC || !slices.Contains(current.InitialProcedures, procedureField) || proc.Native.Family != string(portable.InviteClient) || output.Method != "INVITE" || output.Procedure != procedureField || output.Result != "r:result-1" || !proto.Equal(output.Write.Effect.Context, current.Context) {
		return errNativeRecord
	}
	if _, err = validateRedisUACOutputRoot(ctx, store, due.Identity, due.Revision, due.Assignment, current, procedureField, proc, outputField, result); err != nil {
		return err
	}
	ref, err := nativeOutputRef(output.Write)
	if err != nil {
		return err
	}
	proof, err := store.InspectOutput(ctx, ref)
	if err == nil {
		if proof.MetadataField != outputField || !bytes.Equal(proof.Metadata, view.Records[outputField]) {
			return rediscore.ErrConflict
		}
		if proof.State != rediscore.OutputWritten && proof.State != rediscore.OutputNotWritten {
			return rediscore.ErrUnknown
		}
		capture := &nativeOutputCapture{ref: ref, proof: proof, metadata: output}
		if err = settleRedisUACInitialOutput(ctx, store, output.Write, capture, due.Assignment, deadline); err != nil {
			return err
		}
		proof, err = store.InspectOutput(ctx, ref)
		if err != nil {
			return err
		}
		capture.proof = proof
		return acknowledgeRedisUACInitialOutput(ctx, store, capture, deadline)
	}
	if !errors.Is(err, rediscore.ErrAbsent) {
		return err
	}
	if !proc.OutputConsumed {
		return errors.New("initial INVITE proof absence lacks consumed marker")
	}
	index, ok := slices.BinarySearch(proc.Outputs, outputField)
	if !ok {
		return errNativeRecord
	}
	proc.Outputs = slices.Delete(proc.Outputs, index, index+1)
	if proc.Root == "" {
		result.Output = ""
	}
	procData, err := marshalNativeRecord(storagecodec.NativeProcedure, proc)
	if err != nil {
		return err
	}
	resultData, err := encodeNativeResult(result)
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
	_, err = store.Commit(ctx, rediscore.Publication{Identity: due.Identity, Revision: due.Revision, Deadline: deadline, SpendReservation: true, Changes: []rediscore.Change{{Field: procedureField, Value: procData}, {Field: "r:result-1", Value: resultData}, {Field: outputField, Delete: true}}, Timers: &rediscore.TimerMutation{Expected: due.Assignment, Target: due.Assignment, RequireLease: true, Changes: timers}})
	return err
}
