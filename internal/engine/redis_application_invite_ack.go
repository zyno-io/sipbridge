package engine

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/emiago/sipgo/sip/portable"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/nativepb"
	"sipbridge/internal/rediscore"
	"sipbridge/internal/storagecodec"
)

type selectedApplicationInviteACK struct {
	capture   nativeOutputCapture
	selected  *rediscore.ObservedContext
	current   *nativepb.Context
	procedure *nativepb.Procedure
	dialog    *nativepb.Dialog
}

func validRedisApplicationInviteDialogACKWire(id, wire string) bool {
	return wire == "r:app-invite-dialog-ack-"+id || validRedisApplicationACKWire(wire)
}

func validRedisApplicationACKWire(wire string) bool {
	const prefix = "r:app-ack-wire-"
	if !strings.HasPrefix(wire, prefix) {
		return false
	}
	encoded := strings.TrimPrefix(wire, prefix)
	decoded, err := hex.DecodeString(encoded)
	return err == nil && len(decoded) == 16 && hex.EncodeToString(decoded) == encoded
}

func validReadyApplicationInviteACK(metadata *nativepb.Output, field string) bool {
	const outputPrefix = "r:app-invite-ack-output-"
	const procedurePrefix = "r:app-invite-"
	if metadata == nil || metadata.Write == nil || metadata.Write.Effect == nil || metadata.Write.Effect.Context == nil || metadata.Write.Flow == nil || metadata.Write.Permit == nil || metadata.Write.Permit.Authorization == nil || metadata.Write.Permit.Authorization.AuthorityClass != pb.EffectAuthorization_NATIVE_COMPLETION || metadata.Method != "ACK" || metadata.Status != 0 || metadata.Result != "" || !validSendText(metadata.Operation, 160, false) || metadata.AckSequence == 0 || !strings.HasPrefix(field, outputPrefix) || !strings.HasPrefix(metadata.Procedure, procedurePrefix) {
		return false
	}
	id := strings.TrimPrefix(metadata.Procedure, procedurePrefix)
	decoded, err := hex.DecodeString(id)
	attempt := strings.TrimPrefix(field, outputPrefix)
	parsed, parseErr := uuid.Parse(attempt)
	return err == nil && len(decoded) == 16 && id == hex.EncodeToString(decoded) && metadata.Procedure == procedurePrefix+id && parseErr == nil && parsed.String() == attempt
}

func inspectRedisApplicationInviteACK(ctx context.Context, store *rediscore.Store, write *pb.FlowWrite, capture *nativeOutputCapture) (selectedApplicationInviteACK, error) {
	var out selectedApplicationInviteACK
	if store == nil || write == nil || write.Effect == nil || write.Flow == nil || write.Permit == nil {
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
	if !validReadyApplicationInviteACK(meta, proof.MetadataField) || !proto.Equal(meta.Write, write) || capture.ref.RequireApplication || proof.DeadlineMS <= 0 || write.Permit.Authorization.DeadlineMs != uint64(proof.DeadlineMS) {
		return out, errNativeRecord
	}
	id := strings.TrimPrefix(meta.Procedure, "r:app-invite-")
	fields := []string{"r:context", meta.Procedure, proof.MetadataField}
	if meta.Dialog != "" {
		fields = append(fields, meta.Dialog)
	}
	selected, err := store.ObserveContext(ctx, capture.ref.Context, capture.ref.Incarnation, fields...)
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
	procedure, err := readPrivateProcedureRecord(selected.View.Records[meta.Procedure])
	if err != nil {
		return out, err
	}
	if current.Context == nil || !proto.Equal(current.Context, write.Effect.Context) || current.Kind != pb.ContextSnapshot_UAS && current.Kind != pb.ContextSnapshot_UAC || !slices.Contains(current.NoninitialProcedures, meta.Procedure) || procedure.Native.Version != portable.Version || procedure.Native.Family != string(portable.InviteClient) || procedure.Origin != "r:app-invite-origin-"+id || !slices.Contains(procedure.Outputs, proof.MetadataField) || !proto.Equal(procedure.Flow, write.Flow) {
		return out, errNativeRecord
	}
	var dialog *nativepb.Dialog
	if meta.Dialog == "" {
		if proof.MessageField != "r:app-invite-tx-ack-"+id || procedure.Ack != proof.MessageField || procedure.AckEmissions == 0 {
			return out, errNativeRecord
		}
	} else {
		dialog, err = readPrivateDialogRecord(selected.View.Records[meta.Dialog])
		if err != nil || !slices.Contains(current.Dialogs, meta.Dialog) || !slices.Contains(dialog.Outputs, proof.MetadataField) || dialog.Native.LocalInvite == nil || dialog.Native.LocalInvite.Sequence != meta.AckSequence || dialog.LocalInvite == nil || dialog.LocalInvite.Request != procedure.Origin || dialog.LocalInvite.Ack != proof.MessageField || !validRedisApplicationInviteDialogACKWire(id, proof.MessageField) || dialog.AckEmissions == 0 {
			return out, errNativeRecord
		}
		if err = validateRedisInviteACKAssociation(ctx, store, selected.Identity, selected.View.Revision, selected.View.Timer, current, dialog, procedure, meta); err != nil {
			return out, err
		}
	}
	return selectedApplicationInviteACK{capture: *capture, selected: selected, current: current, procedure: procedure, dialog: dialog}, nil
}

// settleRedisApplicationInviteACK records a positive dialog ACK and any
// required cleanup before releasing its exact definite physical proof.
func settleRedisApplicationInviteACK(ctx context.Context, store *rediscore.Store, write *pb.FlowWrite, capture *nativeOutputCapture, key []byte, executor rediscore.TimerAssignment, deadline time.Time) error {
	selected, err := inspectRedisApplicationInviteACK(ctx, store, write, capture)
	if err != nil {
		return fmt.Errorf("select re-INVITE ACK: %w", err)
	}
	proof := selected.capture.proof
	if proof.State == rediscore.OutputStarted || proof.State == rediscore.OutputUnknown {
		return nil
	}
	if proof.State != rediscore.OutputWritten && proof.State != rediscore.OutputNotWritten {
		return errNativeRecord
	}
	timers := []rediscore.TimerChange{{Member: "out-" + strings.TrimPrefix(proof.MetadataField, "r:"), Due: time.UnixMilli(proof.DeadlineMS)}}
	var changes []rediscore.Change
	var outputs []rediscore.OutputIntent
	var selectors []rediscore.SelectorChange
	if proof.State == rediscore.OutputWritten && selected.dialog != nil {
		selected.dialog.AckWritten = true
		selected.dialog.CompletionFlow = proto.Clone(write.Flow).(*pb.FlowRef)
		cleanup := selected.capture.metadata.CleanupAfterAck || selected.dialog.Native.LocalInvite.CancelRequested || selected.current.Ended || selected.procedure.Native.Cancelled || selected.selected.Identity.Owner == ""
		if cleanup && selected.dialog.Native.Phase != string(portable.DialogEnded) && selected.dialog.LocalBye == "" {
			candidate, e := prepareRedisUACCleanup(ctx, store, selectedUACSuccessACK{capture: selected.capture, selected: selected.selected, current: selected.current, procedure: selected.procedure, dialog: selected.dialog}, write, key, time.Now(), deadline)
			if e != nil {
				return e
			}
			changes = append(changes, candidate.changes...)
			changes = append(changes, rediscore.Change{Field: selected.capture.metadata.Dialog, Value: candidate.dialogData})
			outputs, selectors, timers = append(outputs, candidate.outputs...), append(selectors, candidate.selectors...), append(timers, candidate.timers...)
			contextData, e := encodeNativeContext(selected.current)
			if e != nil {
				return e
			}
			changes = append(changes, rediscore.Change{Field: "r:context", Value: contextData})
		} else {
			data, e := marshalNativeRecord(storagecodec.NativeDialog, selected.dialog)
			if e != nil {
				return e
			}
			changes = append(changes, rediscore.Change{Field: selected.capture.metadata.Dialog, Value: data})
		}
	}
	assignment := selected.selected.View.Timer
	target := assignment
	if target == (rediscore.TimerAssignment{}) {
		target = executor
	}
	publication := rediscore.Publication{Identity: selected.selected.Identity, Revision: selected.selected.View.Revision, Deadline: deadline, SpendReservation: true, Changes: changes, NewOutputs: outputs, Selectors: selectors, Timers: &rediscore.TimerMutation{Expected: assignment, Target: target, Changes: timers}}
	_, err = store.Commit(ctx, publication)
	if err != nil {
		return fmt.Errorf("publish re-INVITE ACK: %w", err)
	}
	if !proof.Settled {
		return store.SettleOutput(ctx, selected.capture.ref, false, true)
	}
	return nil
}

func acknowledgeRedisApplicationInviteACK(ctx context.Context, store *rediscore.Store, write *pb.FlowWrite, deadline time.Time) error {
	selected, err := inspectRedisApplicationInviteACK(ctx, store, write, nil)
	if err != nil {
		return err
	}
	if !selected.capture.proof.Settled || selected.capture.proof.State != rediscore.OutputWritten && selected.capture.proof.State != rediscore.OutputNotWritten || selected.capture.proof.State == rediscore.OutputWritten && selected.dialog != nil && !selected.dialog.AckWritten {
		return errNativeRecord
	}
	return store.SettleOutput(ctx, selected.capture.ref, true, false)
}

// retireRedisApplicationInviteACK removes only this attempt after the real
// consumer released its definite proof; U retains all native owners.
func retireRedisApplicationInviteACK(ctx context.Context, store *rediscore.Store, due *rediscore.DueContext, key []byte, deadline time.Time) error {
	const prefix = "out-app-invite-ack-output-"
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
	if err != nil || !validReadyApplicationInviteACK(output, field) || current.Context == nil || current.Context.State == nil || current.Context.Id != due.Identity.Context || current.Context.State.Incarnation != due.Identity.Incarnation || current.Kind != pb.ContextSnapshot_UAS && current.Kind != pb.ContextSnapshot_UAC || !proto.Equal(output.Write.Effect.Context, current.Context) || int64(output.Write.Permit.Authorization.DeadlineMs) != due.DeadlineMS {
		return errNativeRecord
	}
	ref, err := nativeOutputRef(output.Write)
	if err != nil {
		return err
	}
	proof, err := store.InspectOutput(ctx, ref)
	if err == nil {
		if proof.MetadataField != field || !bytes.Equal(proof.Metadata, view.Records[field]) || proof.DeadlineMS != due.DeadlineMS {
			return errNativeRecord
		}
		if proof.State == rediscore.OutputPending || proof.State == rediscore.OutputStarted {
			if _, e := store.ExpireOutput(ctx, due, ref); e != nil {
				return e
			}
			proof, err = store.InspectOutput(ctx, ref)
			if err != nil {
				return err
			}
		}
		if proof.State == rediscore.OutputUnknown {
			selected, e := inspectRedisApplicationInviteACK(ctx, store, output.Write, &nativeOutputCapture{ref: ref, proof: proof, metadata: output})
			if e != nil {
				return e
			}
			if selected.selected.Identity != due.Identity || selected.selected.View.Revision != due.Revision || selected.selected.View.Timer != due.Assignment || selected.capture.proof.State != rediscore.OutputUnknown {
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
		if e := settleRedisApplicationInviteACK(ctx, store, output.Write, &nativeOutputCapture{ref: ref, proof: proof, metadata: output}, key, due.Assignment, deadline); e != nil {
			return e
		}
		return acknowledgeRedisApplicationInviteACK(ctx, store, output.Write, deadline)
	}
	if !errors.Is(err, rediscore.ErrAbsent) {
		return err
	}
	fields := []string{output.Procedure}
	if output.Dialog != "" {
		fields = append(fields, output.Dialog)
	}
	owners, err := store.Read(ctx, due.Identity, fields...)
	if err != nil {
		return err
	}
	if owners.Revision != due.Revision || owners.Timer != due.Assignment {
		return rediscore.ErrConflict
	}
	proc, err := readPrivateProcedureRecord(owners.Records[output.Procedure])
	id := strings.TrimPrefix(output.Procedure, "r:app-invite-")
	if err != nil || len(id) != 32 || output.Procedure != "r:app-invite-"+id || !slices.Contains(current.NoninitialProcedures, output.Procedure) || proc.Native.Version != portable.Version || proc.Native.Family != string(portable.InviteClient) || proc.Origin != "r:app-invite-origin-"+id || !proto.Equal(proc.Flow, output.Write.Flow) {
		return errNativeRecord
	}
	if output.Dialog == "" && (proc.Ack != "r:app-invite-tx-ack-"+id || proc.AckEmissions == 0) {
		return errNativeRecord
	}
	index := slices.Index(proc.Outputs, field)
	if index < 0 {
		return errNativeRecord
	}
	proc.Outputs = slices.Delete(proc.Outputs, index, index+1)
	procData, err := marshalNativeRecord(storagecodec.NativeProcedure, proc)
	if err != nil {
		return err
	}
	changes := []rediscore.Change{{Field: output.Procedure, Value: procData}, {Field: field, Delete: true}}
	if output.Dialog != "" {
		dialog, e := readPrivateDialogRecord(owners.Records[output.Dialog])
		if e != nil || !slices.Contains(current.Dialogs, output.Dialog) || dialog.Native.LocalInvite == nil || dialog.Native.LocalInvite.Sequence != output.AckSequence || dialog.LocalInvite == nil || dialog.LocalInvite.Request != proc.Origin || !validRedisApplicationInviteDialogACKWire(id, dialog.LocalInvite.Ack) || dialog.AckEmissions == 0 {
			return errNativeRecord
		}
		if e = validateRedisInviteACKAssociation(ctx, store, due.Identity, due.Revision, due.Assignment, current, dialog, proc, output); e != nil {
			return e
		}
		i := slices.Index(dialog.Outputs, field)
		if i < 0 {
			return errNativeRecord
		}
		dialog.Outputs = slices.Delete(dialog.Outputs, i, i+1)
		data, e := marshalNativeRecord(storagecodec.NativeDialog, dialog)
		if e != nil {
			return e
		}
		changes = append(changes, rediscore.Change{Field: output.Dialog, Value: data})
	}
	timers := []rediscore.TimerChange{{Member: due.Member, Delete: true}, {Member: "app-invite-retire-" + id, Due: time.UnixMilli(due.CheckedAtMS)}}
	if current.Ended {
		if current.CallRetireAtMs <= 0 {
			return errNativeRecord
		}
		timers = append(timers, rediscore.TimerChange{Member: "call-retire", Due: time.UnixMilli(current.CallRetireAtMs)})
	}
	_, err = store.Commit(ctx, rediscore.Publication{Identity: due.Identity, Revision: due.Revision, Deadline: deadline, SpendReservation: true, Changes: changes, Timers: &rediscore.TimerMutation{Expected: due.Assignment, Target: due.Assignment, RequireLease: true, Changes: timers}})
	return err
}
