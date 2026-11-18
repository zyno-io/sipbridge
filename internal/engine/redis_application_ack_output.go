package engine

import (
	"bytes"
	"context"
	"encoding/hex"
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

func validReadyApplicationACK(metadata *nativepb.Output, field string) bool {
	if metadata == nil || metadata.Write == nil || metadata.Write.Permit == nil || metadata.Write.Permit.Authorization == nil || metadata.Method != "ACK" || metadata.Status != 0 || metadata.AckSequence == 0 || metadata.Procedure == "" || metadata.Dialog == "" || metadata.Operation == "" || metadata.Write.Permit.Authorization.AuthorityClass != pb.EffectAuthorization_NATIVE_COMPLETION || !strings.HasPrefix(field, "r:app-ack-output-") || !strings.HasPrefix(metadata.Result, "r:result-") {
		return false
	}
	id := strings.TrimPrefix(field, "r:app-ack-output-")
	decoded, err := hex.DecodeString(id)
	if err != nil || len(decoded) != 16 || id != hex.EncodeToString(decoded) {
		return false
	}
	sequence, err := strconv.ParseUint(strings.TrimPrefix(metadata.Result, "r:result-"), 10, 64)
	return err == nil && sequence > 0 && sequence <= 1<<53-1 && metadata.Result == "r:result-"+strconv.FormatUint(sequence, 10)
}

type selectedApplicationACK struct {
	capture   nativeOutputCapture
	selected  *rediscore.ObservedContext
	current   *nativepb.Context
	procedure *nativepb.Procedure
	dialog    *nativepb.Dialog
	result    *nativepb.Result
}

func inspectRedisApplicationACK(ctx context.Context, store *rediscore.Store, write *pb.FlowWrite, capture *nativeOutputCapture) (selectedApplicationACK, error) {
	var out selectedApplicationACK
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
	if meta == nil || !proto.Equal(meta.Write, write) || capture.ref.RequireApplication || write.Permit.Authorization.AuthorityClass != pb.EffectAuthorization_NATIVE_COMPLETION || meta.Method != "ACK" || meta.Status != 0 || meta.Dialog == "" || meta.Procedure == "" || meta.Result == "" || meta.Operation == "" || meta.AckSequence == 0 || proof.DeadlineMS <= 0 || write.Permit.Authorization.DeadlineMs != uint64(proof.DeadlineMS) || !strings.HasPrefix(proof.MetadataField, "r:app-ack-output-") {
		return out, errNativeRecord
	}
	id := strings.TrimPrefix(proof.MetadataField, "r:app-ack-output-")
	if len(id) != 32 || proof.MessageField != "r:app-ack-wire-"+id {
		return out, errNativeRecord
	}
	selected, err := store.ObserveContext(ctx, capture.ref.Context, capture.ref.Incarnation, "r:context", meta.Procedure, meta.Dialog, meta.Result, proof.MetadataField)
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
	dialog, err := readPrivateDialogRecord(selected.View.Records[meta.Dialog])
	if err != nil {
		return out, err
	}
	result, err := decodeNativeResult(selected.View.Records[meta.Result])
	if err != nil {
		return out, err
	}
	if current.Context == nil || !proto.Equal(current.Context, write.Effect.Context) || current.Kind != pb.ContextSnapshot_UAC && current.Kind != pb.ContextSnapshot_UAS || !slices.Contains(current.InitialProcedures, meta.Procedure) && !slices.Contains(current.NoninitialProcedures, meta.Procedure) || !slices.Contains(current.Dialogs, meta.Dialog) || proc.Native.Version != portable.Version || proc.Native.Family != string(portable.InviteClient) || !slices.Contains(proc.Outputs, proof.MetadataField) || dialog.Native.Version != portable.Version || dialog.Native.LocalInvite == nil || dialog.Native.LocalInvite.Sequence != meta.AckSequence || dialog.LocalInvite == nil || dialog.LocalInvite.Request != proc.Origin || dialog.LocalInvite.Ack != proof.MessageField || dialog.AckResult != meta.Result || !validUACDialogLocalAssociation(dialog.LocalBye, dialog.LocalTransaction, proc.Native.Key) || !slices.Contains(dialog.Outputs, proof.MetadataField) || result.Sequence == 0 || result.Sequence > current.CommandSequence || meta.Result != "r:result-"+strconv.FormatUint(result.Sequence, 10) || result.OperationId != meta.Operation || result.Output != proof.MetadataField || result.DecidingLeaseTerm != write.Permit.Authorization.ApplicationLeaseTerm {
		return out, errNativeRecord
	}
	expectedID, err := redisApplicationByeID(result.Fingerprint)
	if err != nil || expectedID != id {
		return out, errNativeRecord
	}
	return selectedApplicationACK{capture: *capture, selected: selected, current: current, procedure: proc, dialog: dialog, result: result}, nil
}

// settleRedisApplicationACK records the exact positive native ACK witness and
// the independent application result before releasing any definite proof.
func settleRedisApplicationACK(ctx context.Context, store *rediscore.Store, write *pb.FlowWrite, capture *nativeOutputCapture, key []byte, executor rediscore.TimerAssignment, deadline time.Time) error {
	selected, err := inspectRedisApplicationACK(ctx, store, write, capture)
	if err != nil {
		return err
	}
	proof := selected.capture.proof
	if proof.State == rediscore.OutputStarted {
		return nil
	}
	if proof.State != rediscore.OutputUnknown && proof.State != rediscore.OutputWritten && proof.State != rediscore.OutputNotWritten {
		return errNativeRecord
	}
	current, dialog, result := selected.current, selected.dialog, selected.result
	initialInvite, finalField := dialog.Native.LocalInvite.Initial, dialog.LocalInvite.FinalResponse
	var changes []rediscore.Change
	var outputs []rediscore.OutputIntent
	var selectors []rediscore.SelectorChange
	var hint *rediscore.ResultHint
	timers := []rediscore.TimerChange{}
	if proof.State != rediscore.OutputUnknown {
		timers = append(timers, rediscore.TimerChange{Member: "out-" + strings.TrimPrefix(proof.MetadataField, "r:"), Due: time.UnixMilli(proof.DeadlineMS)})
	}
	if proof.State == rediscore.OutputWritten {
		dialog.AckWritten = true
		dialog.CompletionFlow = proto.Clone(write.Flow).(*pb.FlowRef)
		contextChanged := false
		if current.Kind == pb.ContextSnapshot_UAC && initialInvite && !current.InitialAck {
			if current.Revision >= 1<<53-1 {
				return errNativeRecord
			}
			current.InitialAck = true
			current.Revision++
			contextChanged = true
		}
		cleanup := (current.Ended || selected.selected.Identity.Owner == "" || dialog.Native.LocalInvite.CancelRequested || selected.capture.metadata.CleanupAfterAck) && dialog.Native.Phase != string(portable.DialogEnded) && dialog.LocalBye == ""
		if cleanup {
			candidate, e := prepareRedisUACCleanup(ctx, store, selectedUACSuccessACK{capture: selected.capture, selected: selected.selected, current: current, procedure: selected.procedure, dialog: dialog}, write, key, time.Now(), deadline)
			if e != nil {
				return e
			}
			changes = append(changes, rediscore.Change{Field: selected.capture.metadata.Dialog, Value: candidate.dialogData})
			changes = append(changes, candidate.changes...)
			outputs = append(outputs, candidate.outputs...)
			selectors = append(selectors, candidate.selectors...)
			timers = append(timers, candidate.timers...)
			contextChanged = true
		} else {
			data, e := marshalNativeRecord(storagecodec.NativeDialog, dialog)
			if e != nil {
				return e
			}
			changes = append(changes, rediscore.Change{Field: selected.capture.metadata.Dialog, Value: data})
		}
		if contextChanged {
			data, e := encodeNativeContext(current)
			if e != nil {
				return e
			}
			changes = append(changes, rediscore.Change{Field: "r:context", Value: data})
		}
	}
	// The initial INVITE result is a separate retained marker. Its request
	// sentinel controls whether its response payload is still visible.
	if proof.State == rediscore.OutputWritten && current.Kind == pb.ContextSnapshot_UAC && initialInvite && current.Answered && !current.Ended && selected.selected.Identity.Owner != "" && !selected.procedure.Native.Cancelled {
		rootField := selected.capture.metadata.Procedure
		root := selected.procedure
		if !slices.Contains(current.InitialProcedures, rootField) {
			return errNativeRecord
		}
		if root.Root != "" {
			if root.CreditContribution != 0 {
				return errNativeRecord
			}
			rootField = root.Root
			if !slices.Contains(current.InitialProcedures, rootField) {
				return errNativeRecord
			}
			rootView, e := store.Read(ctx, selected.selected.Identity, rootField)
			if e != nil {
				return e
			}
			if rootView.Revision != selected.selected.View.Revision || rootView.Timer != selected.selected.View.Timer {
				return rediscore.ErrConflict
			}
			root, e = readPrivateProcedureRecord(rootView.Records[rootField])
			if e != nil || root.Root != "" || root.Native.Version != portable.Version || root.Native.Family != string(portable.InviteClient) || root.CreditContribution == 0 {
				return errNativeRecord
			}
		}
		if root.CommandResult != "r:result-1" || selected.procedure.CommandResult != root.CommandResult || selected.capture.metadata.Result == root.CommandResult {
			return errNativeRecord
		}
		fields := append([]string{root.CommandResult}, root.Events...)
		view, e := store.Read(ctx, selected.selected.Identity, fields...)
		if e != nil {
			return e
		}
		if view.Revision != selected.selected.View.Revision || view.Timer != selected.selected.View.Timer {
			return rediscore.ErrConflict
		}
		original, e := decodeNativeResult(view.Records[root.CommandResult])
		if e != nil || original.Sequence != 1 || original.Request != "" && original.Request != root.Origin || original.Request == "" && original.Response != "" {
			return errNativeRecord
		}
		if original.Certainty == pb.OperationResult_ACCEPTED || original.Certainty == pb.OperationResult_UNKNOWN {
			original.Certainty, original.Error, original.SipStatus, original.Pending = pb.OperationResult_COMPLETED, "", 0, false
			if original.Request != "" {
				original.Response = finalField
				for _, field := range root.Events {
					event, eventErr := decodeNativeLifecycle(view.Records[field])
					if eventErr != nil || event.Root != rootField {
						return errNativeRecord
					}
					if event.Message == original.Response && !slices.Contains(event.Owners, root.CommandResult) {
						event.Owners = append(event.Owners, root.CommandResult)
						slices.Sort(event.Owners)
						data, encodeErr := encodeNativeLifecycle(event)
						if encodeErr != nil {
							return encodeErr
						}
						changes = append(changes, rediscore.Change{Field: field, Value: data})
					}
				}
			}
			data, e := encodeNativeResult(original)
			if e != nil {
				return e
			}
			changes = append(changes, rediscore.Change{Field: root.CommandResult, Value: data})
		}
	}
	if result.Pending || proof.State == rediscore.OutputWritten && result.Certainty == pb.OperationResult_UNKNOWN {
		wasPending := result.Pending
		result.Pending = false
		result.Certainty, result.Error, result.SipStatus = pb.OperationResult_UNKNOWN, "ACK delivery unavailable or uncertain", 0
		if proof.State == rediscore.OutputWritten {
			result.Certainty, result.Error = pb.OperationResult_COMPLETED, ""
			if wasPending {
				result.Request = proof.MessageField
			}
		}
		data, e := encodeNativeResult(result)
		if e != nil {
			return e
		}
		changes = append(changes, rediscore.Change{Field: selected.capture.metadata.Result, Value: data})
		hint = &rediscore.ResultHint{Context: selected.selected.Identity.Context, Incarnation: selected.selected.Identity.Incarnation, Sequence: result.Sequence}
	}
	if len(changes) != 0 || len(timers) != 0 {
		target := selected.selected.View.Timer
		if target == (rediscore.TimerAssignment{}) {
			target = executor
		}
		publication := rediscore.Publication{Identity: selected.selected.Identity, Revision: selected.selected.View.Revision, Deadline: deadline, SpendReservation: true, Changes: changes, NewOutputs: outputs, Selectors: selectors, ResultHint: hint}
		if len(timers) != 0 {
			publication.Timers = &rediscore.TimerMutation{Expected: selected.selected.View.Timer, Target: target, Changes: timers}
		}
		_, err = store.Commit(ctx, publication)
		if errors.Is(err, rediscore.ErrExpired) && publication.Timers != nil && selected.selected.View.Timer != (rediscore.TimerAssignment{}) && selected.selected.View.Timer != executor && ctx.Err() == nil && time.Now().Before(deadline) {
			publication.Timers.Target = executor
			_, err = store.Commit(ctx, publication)
		}
		if err != nil {
			return err
		}
	}
	if proof.State == rediscore.OutputUnknown {
		return nil
	}
	if !proof.Settled {
		return store.SettleOutput(ctx, selected.capture.ref, false, true)
	}
	return nil
}

func acknowledgeRedisApplicationACK(ctx context.Context, store *rediscore.Store, write *pb.FlowWrite, deadline time.Time) error {
	selected, err := inspectRedisApplicationACK(ctx, store, write, nil)
	if err != nil {
		return err
	}
	if !selected.capture.proof.Settled || selected.capture.proof.State != rediscore.OutputWritten && selected.capture.proof.State != rediscore.OutputNotWritten {
		return errNativeRecord
	}
	if selected.capture.proof.State == rediscore.OutputWritten && !selected.dialog.AckWritten {
		return errNativeRecord
	}
	return store.SettleOutput(ctx, selected.capture.ref, true, false)
}

// retireRedisApplicationACK follows only this attempt's physical cutoff. An
// UNKNOWN proof retains its exact procedure/dialog ownership for call retirement.
func retireRedisApplicationACK(ctx context.Context, store *rediscore.Store, due *rediscore.DueContext, key []byte, deadline time.Time) error {
	const prefix = "out-app-ack-output-"
	if store == nil || due == nil || !strings.HasPrefix(due.Member, prefix) || due.CheckedAtMS < due.DeadlineMS || !time.Now().Before(deadline) {
		return errNativeRecord
	}
	id := strings.TrimPrefix(due.Member, prefix)
	if len(id) != 32 {
		return errNativeRecord
	}
	field := "r:app-ack-output-" + id
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
	if !validReadyApplicationACK(output, field) || !proto.Equal(output.Write.GetEffect().GetContext(), current.Context) {
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
			selected, e := inspectRedisApplicationACK(ctx, store, output.Write, capture)
			if e != nil {
				return e
			}
			if selected.selected.Identity != due.Identity || selected.selected.View.Revision != due.Revision || selected.selected.View.Timer != due.Assignment {
				return rediscore.ErrConflict
			}
			var changes []rediscore.Change
			var hint *rediscore.ResultHint
			if selected.result.Pending {
				selected.result.Pending = false
				selected.result.Certainty = pb.OperationResult_UNKNOWN
				selected.result.Error = "ACK delivery unavailable or uncertain"
				data, encodeErr := encodeNativeResult(selected.result)
				if encodeErr != nil {
					return encodeErr
				}
				changes = append(changes, rediscore.Change{Field: output.Result, Value: data})
				hint = &rediscore.ResultHint{Context: due.Identity.Context, Incarnation: due.Identity.Incarnation, Sequence: selected.result.Sequence}
			}
			timers := []rediscore.TimerChange{{Member: due.Member, Delete: true}}
			if current.Ended {
				if current.CallRetireAtMs <= 0 {
					return errNativeRecord
				}
				timers = append(timers, rediscore.TimerChange{Member: "call-retire", Due: time.UnixMilli(current.CallRetireAtMs)})
			}
			_, e = store.Commit(ctx, rediscore.Publication{Identity: due.Identity, Revision: due.Revision, Deadline: deadline, SpendReservation: true, Changes: changes, ResultHint: hint, Timers: &rediscore.TimerMutation{Expected: due.Assignment, Target: due.Assignment, RequireLease: true, Changes: timers}})
			return e
		}
		if proof.State != rediscore.OutputWritten && proof.State != rediscore.OutputNotWritten {
			return rediscore.ErrUnknown
		}
		if err = settleRedisApplicationACK(ctx, store, output.Write, capture, key, due.Assignment, deadline); err != nil {
			return err
		}
		return acknowledgeRedisApplicationACK(ctx, store, output.Write, deadline)
	}
	if !errors.Is(err, rediscore.ErrAbsent) {
		return err
	}
	selected, err := store.Read(ctx, due.Identity, output.Procedure, output.Dialog, output.Result)
	if err != nil {
		return err
	}
	if selected.Revision != due.Revision || selected.Timer != due.Assignment {
		return rediscore.ErrConflict
	}
	proc, err := readPrivateProcedureRecord(selected.Records[output.Procedure])
	if err != nil {
		return err
	}
	dialog, err := readPrivateDialogRecord(selected.Records[output.Dialog])
	if err != nil {
		return err
	}
	result, err := decodeNativeResult(selected.Records[output.Result])
	if err != nil {
		return err
	}
	if !slices.Contains(current.InitialProcedures, output.Procedure) && !slices.Contains(current.NoninitialProcedures, output.Procedure) || !slices.Contains(current.Dialogs, output.Dialog) || proc.Native.Family != string(portable.InviteClient) || dialog.Native.LocalInvite == nil || dialog.Native.LocalInvite.Sequence != output.AckSequence || dialog.LocalInvite == nil || dialog.LocalInvite.Ack != "r:app-ack-wire-"+id || dialog.LocalInvite.Request != proc.Origin || dialog.AckResult != output.Result || !validUACDialogLocalAssociation(dialog.LocalBye, dialog.LocalTransaction, proc.Native.Key) || result.Pending || result.Output != field || output.Result != "r:result-"+strconv.FormatUint(result.Sequence, 10) || result.OperationId != output.Operation {
		return errNativeRecord
	}
	expectedID, err := redisApplicationByeID(result.Fingerprint)
	if err != nil || expectedID != id || result.DecidingLeaseTerm != output.Write.Permit.Authorization.ApplicationLeaseTerm {
		return errNativeRecord
	}
	pi, ok := slices.BinarySearch(proc.Outputs, field)
	if !ok {
		return errNativeRecord
	}
	proc.Outputs = slices.Delete(proc.Outputs, pi, pi+1)
	di, ok := slices.BinarySearch(dialog.Outputs, field)
	if !ok {
		return errNativeRecord
	}
	dialog.Outputs = slices.Delete(dialog.Outputs, di, di+1)
	result.Output = ""
	procData, err := marshalNativeRecord(storagecodec.NativeProcedure, proc)
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
	timers := []rediscore.TimerChange{{Member: due.Member, Delete: true}}
	if current.CommandSequence > result.Sequence {
		timers = append(timers, rediscore.TimerChange{Member: "orig-result-" + strconv.FormatUint(result.Sequence, 10), Due: time.UnixMilli(result.CutoffMs)})
	}
	if current.Ended {
		if current.CallRetireAtMs <= 0 {
			return errNativeRecord
		}
		timers = append(timers, rediscore.TimerChange{Member: "call-retire", Due: time.UnixMilli(current.CallRetireAtMs)})
	}
	_, err = store.Commit(ctx, rediscore.Publication{Identity: due.Identity, Revision: due.Revision, Deadline: deadline, SpendReservation: true, Changes: []rediscore.Change{{Field: output.Procedure, Value: procData}, {Field: output.Dialog, Value: dialogData}, {Field: output.Result, Value: resultData}, {Field: field, Delete: true}}, Timers: &rediscore.TimerMutation{Expected: due.Assignment, Target: due.Assignment, RequireLease: true, Changes: timers}})
	return err
}
