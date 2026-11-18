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

type selectedUACSuccessACK struct {
	capture   nativeOutputCapture
	selected  *rediscore.ObservedContext
	current   *nativepb.Context
	procedure *nativepb.Procedure
	root      *nativepb.Procedure
	rootField string
	dialog    *nativepb.Dialog
	result    *nativepb.Result
}

// The cleanup procedure may already be retired while its BYE remains dialog-owned.
func validUACDialogLocalAssociation(localBye, localTransaction, initialKey string) bool {
	if localBye == "" {
		return localTransaction == initialKey
	}
	const prefix = "r:call-cleanup-origin-"
	return strings.HasPrefix(localBye, prefix) && len(strings.TrimPrefix(localBye, prefix)) == 32 && localTransaction != "" && localTransaction != initialKey
}

func validRedisUACDialogACKWire(dialogID, wire string) bool {
	return wire == "r:uac-dialog-ack-"+dialogID || validRedisApplicationACKWire(wire)
}

// inspectRedisUACSuccessACK binds one physical proof to the current exact
// initial-dialog ACK and the original result without reading SIP packets.
func inspectRedisUACSuccessACK(ctx context.Context, store *rediscore.Store, write *pb.FlowWrite, capture *nativeOutputCapture) (selectedUACSuccessACK, error) {
	var out selectedUACSuccessACK
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
	if meta == nil || !proto.Equal(meta.Write, write) || capture.ref.RequireApplication || meta.Method != "ACK" || meta.Status != 0 || meta.Dialog == "" || meta.Result != "" && meta.Result != "r:result-1" || meta.Result == "" && !meta.CleanupAfterAck || !strings.HasPrefix(meta.Procedure, "r:uac-procedure-") || !strings.HasPrefix(meta.Dialog, "r:uac-dialog-") || !strings.HasPrefix(capture.proof.MetadataField, "r:uac-dialog-ack-output-") {
		return out, errNativeRecord
	}
	id := strings.TrimPrefix(meta.Procedure, "r:uac-procedure-")
	dialogID := strings.TrimPrefix(meta.Dialog, "r:uac-dialog-")
	if len(id) != 32 || len(dialogID) != 32 || !validRedisUACDialogACKWire(dialogID, capture.proof.MessageField) {
		return out, errNativeRecord
	}
	fields := []string{"r:context", meta.Procedure, meta.Dialog, capture.proof.MetadataField}
	if meta.Result != "" {
		fields = append(fields, meta.Result)
	}
	selected, err := store.ObserveContext(ctx, capture.ref.Context, capture.ref.Incarnation, fields...)
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
	dialog, err := readPrivateDialogRecord(selected.View.Records[meta.Dialog])
	if err != nil {
		return out, err
	}
	var result *nativepb.Result
	if meta.Result != "" {
		result, err = decodeNativeResult(selected.View.Records[meta.Result])
		if err != nil {
			return out, err
		}
	}
	if current.Kind != pb.ContextSnapshot_UAC || !proto.Equal(current.Context, write.Effect.Context) || !slices.Contains(current.InitialProcedures, meta.Procedure) || !slices.Contains(current.Dialogs, meta.Dialog) || proc.Native.Version != portable.Version || proc.Native.Family != string(portable.InviteClient) || proc.Origin != "r:uac-origin-"+id || proc.CommandResult != "r:result-1" || !slices.Contains(proc.Outputs, capture.proof.MetadataField) || !proto.Equal(proc.Flow, write.Flow) || dialog.Native.Version != portable.Version || dialog.Native.Role != string(portable.UAC) || dialog.Native.CallId != current.CallId || dialog.Native.LocalInvite == nil || !dialog.Native.LocalInvite.Initial || dialog.Native.LocalInvite.Sequence != meta.AckSequence || dialog.LocalInvite == nil || dialog.LocalInvite.Request != proc.Origin || dialog.LocalInvite.FinalResponse == "" || dialog.LocalInvite.Ack != capture.proof.MessageField || dialog.AckEmissions == 0 || !slices.Contains(dialog.Outputs, capture.proof.MetadataField) || result != nil && (result.Sequence != 1 || result.OperationId != meta.Operation) {
		return out, errNativeRecord
	}
	if err = validateRedisInviteACKAssociation(ctx, store, selected.Identity, selected.View.Revision, selected.View.Timer, current, dialog, proc, meta); err != nil {
		return out, err
	}
	root, rootField := proc, meta.Procedure
	if proc.Root != "" {
		rootField = proc.Root
		if rootField == meta.Procedure || proc.CreditContribution != 0 || !slices.Contains(current.InitialProcedures, rootField) {
			return out, errNativeRecord
		}
		view, e := store.Read(ctx, selected.Identity, rootField)
		if e != nil {
			return out, e
		}
		if view.Revision != selected.View.Revision || view.Timer != selected.View.Timer {
			return out, rediscore.ErrConflict
		}
		root, e = readPrivateProcedureRecord(view.Records[rootField])
		if e != nil {
			return out, e
		}
		if root.Root != "" || root.Native.Version != portable.Version || root.Native.Family != string(portable.InviteClient) || root.CommandResult != proc.CommandResult || root.CreditContribution == 0 {
			return out, errNativeRecord
		}
	}
	return selectedUACSuccessACK{capture: *capture, selected: selected, current: current, procedure: proc, root: root, rootField: rootField, dialog: dialog, result: result}, nil
}

// settleRedisUACSuccessACK publishes positive ACK evidence before settling
// the exact physical proof. A late definite proof re-indexes its original due.
func settleRedisUACSuccessACK(ctx context.Context, store *rediscore.Store, write *pb.FlowWrite, capture *nativeOutputCapture, key []byte, executor rediscore.TimerAssignment, deadline time.Time) error {
	selected, err := inspectRedisUACSuccessACK(ctx, store, write, capture)
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
	var changes []rediscore.Change
	var outputs []rediscore.OutputIntent
	var selectors []rediscore.SelectorChange
	timers := []rediscore.TimerChange{{Member: "out-" + strings.TrimPrefix(proof.MetadataField, "r:"), Due: time.UnixMilli(proof.DeadlineMS)}}
	var hint *rediscore.ResultHint
	if proof.State == rediscore.OutputWritten {
		dialog := selected.dialog
		finalField := dialog.LocalInvite.FinalResponse
		current := selected.current
		winner := current.Answered && current.CallId == dialog.Native.CallId && current.LocalTag == dialog.Native.LocalTag && current.RemoteTag == dialog.Native.RemoteTag
		dialogChanged := !dialog.AckWritten || !proto.Equal(dialog.CompletionFlow, write.Flow)
		dialog.AckWritten = true
		dialog.CompletionFlow = proto.Clone(write.Flow).(*pb.FlowRef)
		contextChanged := false
		if winner && !current.InitialAck {
			if current.Revision >= 1<<53-1 {
				return errNativeRecord
			}
			current.InitialAck = true
			current.Revision++
			contextChanged = true
		}
		cleanup := selected.capture.metadata.CleanupAfterAck || dialog.Native.LocalInvite.CancelRequested || !winner || current.Ended || selected.procedure.Native.Cancelled || selected.selected.Identity.Owner == ""
		var cleanupDialog []byte
		if cleanup && dialog.Native.Phase != string(portable.DialogEnded) && dialog.LocalBye == "" {
			now := time.Now()
			if ctx.Err() != nil || !now.Before(deadline) {
				return rediscore.ErrExpired
			}
			candidate, e := prepareRedisUACCleanup(ctx, store, selected, write, key, now, deadline)
			if e != nil {
				return e
			}
			cleanupDialog = candidate.dialogData
			changes = append(changes, candidate.changes...)
			outputs = append(outputs, candidate.outputs...)
			selectors = append(selectors, candidate.selectors...)
			timers = append(timers, candidate.timers...)
			contextChanged = true
		}
		if cleanupDialog != nil {
			changes = append(changes, rediscore.Change{Field: selected.capture.metadata.Dialog, Value: cleanupDialog})
		} else if dialogChanged {
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
		result := selected.result
		if winner && result != nil && !current.Ended && selected.selected.Identity.Owner != "" && !selected.procedure.Native.Cancelled && (result.Certainty == pb.OperationResult_ACCEPTED || result.Certainty == pb.OperationResult_UNKNOWN) {
			if result.Request != "" && result.Request != selected.root.Origin || result.Request == "" && result.Response != "" {
				return errNativeRecord
			}
			payloadLive := result.Request != ""
			result.Certainty = pb.OperationResult_COMPLETED
			result.Error = ""
			result.SipStatus = 0
			if payloadLive {
				result.Response = finalField
			}
			result.Pending = false
			if payloadLive && len(selected.root.Events) > 0 {
				events, e := store.Read(ctx, selected.selected.Identity, selected.root.Events...)
				if errors.Is(e, rediscore.ErrAbsent) {
					return rediscore.ErrConflict
				}
				if e != nil {
					return e
				}
				if events.Revision != selected.selected.View.Revision || events.Timer != selected.selected.View.Timer {
					return rediscore.ErrConflict
				}
				for _, field := range selected.root.Events {
					event, e := decodeNativeLifecycle(events.Records[field])
					if e != nil {
						return e
					}
					if event.Root != selected.rootField {
						return errNativeRecord
					}
					if event.Message == result.Response && !slices.Contains(event.Owners, selected.capture.metadata.Result) {
						event.Owners = append(event.Owners, selected.capture.metadata.Result)
						slices.Sort(event.Owners)
						data, e := encodeNativeLifecycle(event)
						if e != nil {
							return e
						}
						changes = append(changes, rediscore.Change{Field: field, Value: data})
					}
				}
			}
			data, e := encodeNativeResult(result)
			if e != nil {
				return e
			}
			changes = append(changes, rediscore.Change{Field: selected.capture.metadata.Result, Value: data})
			hint = &rediscore.ResultHint{Context: selected.selected.Identity.Context, Incarnation: selected.selected.Identity.Incarnation, Sequence: result.Sequence}
		}
	}
	assignment := selected.selected.View.Timer
	target := assignment
	if target == (rediscore.TimerAssignment{}) {
		target = executor
	}
	publication := rediscore.Publication{Identity: selected.selected.Identity, Revision: selected.selected.View.Revision, Deadline: deadline, SpendReservation: true, Changes: changes, NewOutputs: outputs, Selectors: selectors, ResultHint: hint, Timers: &rediscore.TimerMutation{Expected: assignment, Target: target, Changes: timers}}
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

// Actual or recovered consumption acknowledges only after native settlement.
func acknowledgeRedisUACSuccessACK(ctx context.Context, store *rediscore.Store, write *pb.FlowWrite, capture *nativeOutputCapture) error {
	selected, err := inspectRedisUACSuccessACK(ctx, store, write, capture)
	if err != nil {
		return err
	}
	proof := selected.capture.proof
	if !proof.Settled || proof.State != rediscore.OutputWritten && proof.State != rediscore.OutputNotWritten || proof.State == rediscore.OutputWritten && !selected.dialog.AckWritten {
		return errNativeRecord
	}
	return store.SettleOutput(ctx, selected.capture.ref, true, false)
}

// retireRedisUACSuccessACK consumes one exact definite ACK or releases only
// its physical metadata after proof removal. U keeps its native owners and
// proof while dropping the exhausted physical head.
func retireRedisUACSuccessACK(ctx context.Context, store *rediscore.Store, due *rediscore.DueContext, key []byte, deadline time.Time) error {
	const prefix = "out-uac-dialog-ack-output-"
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
	if output.Write == nil || output.Write.Effect == nil || output.Write.Effect.Context == nil || output.Write.Flow == nil || output.Write.Permit == nil || current.Kind != pb.ContextSnapshot_UAC || output.Method != "ACK" || output.Status != 0 || output.Result != "" && output.Result != "r:result-1" || output.Result == "" && !output.CleanupAfterAck || !strings.HasPrefix(output.Procedure, "r:uac-procedure-") || !strings.HasPrefix(output.Dialog, "r:uac-dialog-") || !proto.Equal(output.Write.Effect.Context, current.Context) || !slices.Contains(current.InitialProcedures, output.Procedure) || !slices.Contains(current.Dialogs, output.Dialog) {
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
			selected, e := inspectRedisUACSuccessACK(ctx, store, output.Write, capture)
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
		if err = settleRedisUACSuccessACK(ctx, store, output.Write, capture, key, due.Assignment, deadline); err != nil {
			return err
		}
		proof, err = store.InspectOutput(ctx, ref)
		if err != nil {
			return err
		}
		capture.proof = proof
		return acknowledgeRedisUACSuccessACK(ctx, store, output.Write, capture)
	}
	if !errors.Is(err, rediscore.ErrAbsent) {
		return err
	}
	fields := []string{output.Procedure, output.Dialog}
	if output.Result != "" {
		fields = append(fields, output.Result)
	}
	selected, err := store.Read(ctx, due.Identity, fields...)
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
	dialog, err := readPrivateDialogRecord(selected.Records[output.Dialog])
	if err != nil {
		return err
	}
	var result *nativepb.Result
	if output.Result != "" {
		result, err = decodeNativeResult(selected.Records[output.Result])
		if err != nil {
			return err
		}
	}
	procID := strings.TrimPrefix(output.Procedure, "r:uac-procedure-")
	dialogID := strings.TrimPrefix(output.Dialog, "r:uac-dialog-")
	if len(procID) != 32 || len(dialogID) != 32 || proc.Native.Version != portable.Version || proc.Native.Family != string(portable.InviteClient) || proc.Origin != "r:uac-origin-"+procID || proc.CommandResult != "r:result-1" || !proto.Equal(proc.Flow, output.Write.Flow) || dialog.Native.Version != portable.Version || dialog.Native.Role != string(portable.UAC) || dialog.Native.LocalInvite == nil || !dialog.Native.LocalInvite.Initial || dialog.Native.LocalInvite.Sequence != output.AckSequence || dialog.LocalInvite == nil || dialog.LocalInvite.Request != proc.Origin || !validRedisUACDialogACKWire(dialogID, dialog.LocalInvite.Ack) || result != nil && (result.Sequence != 1 || result.OperationId != output.Operation) {
		return errNativeRecord
	}
	if err = validateRedisInviteACKAssociation(ctx, store, due.Identity, due.Revision, due.Assignment, current, dialog, proc, output); err != nil {
		return err
	}
	pi, ok := slices.BinarySearch(proc.Outputs, field)
	if !ok {
		return errNativeRecord
	}
	di, ok := slices.BinarySearch(dialog.Outputs, field)
	if !ok {
		return errNativeRecord
	}
	proc.Outputs = slices.Delete(proc.Outputs, pi, pi+1)
	dialog.Outputs = slices.Delete(dialog.Outputs, di, di+1)
	procData, err := marshalNativeRecord(storagecodec.NativeProcedure, proc)
	if err != nil {
		return err
	}
	dialogData, err := marshalNativeRecord(storagecodec.NativeDialog, dialog)
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
	_, err = store.Commit(ctx, rediscore.Publication{Identity: due.Identity, Revision: due.Revision, Deadline: deadline, SpendReservation: true, Changes: []rediscore.Change{{Field: output.Procedure, Value: procData}, {Field: output.Dialog, Value: dialogData}, {Field: field, Delete: true}}, Timers: &rediscore.TimerMutation{Expected: due.Assignment, Target: due.Assignment, RequireLease: true, Changes: timers}})
	return err
}
