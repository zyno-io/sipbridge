package engine

import (
	"context"
	"errors"
	"slices"
	"strconv"
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

var errPreviousInviteACKPending = &portable.ProtocolError{Status: 491, Reason: "previous INVITE ACK output still pending"}

// previousInviteOutputPending carries only this operation's exact selected
// attempt. It is not retained and cannot grant a replacement by itself.
type previousInviteOutputPending struct {
	field string
	write *pb.FlowWrite
}

func (*previousInviteOutputPending) Error() string { return errPreviousInviteACKPending.Error() }
func (*previousInviteOutputPending) Unwrap() error { return errPreviousInviteACKPending }

// advanceRedisPreviousInviteOutput completes an already-written attempt without
// issuing socket I/O. The caller must reselect before any business publication.
func advanceRedisPreviousInviteOutput(ctx context.Context, store *rediscore.Store, pending *previousInviteOutputPending, key []byte, executor rediscore.TimerAssignment, deadline time.Time) (bool, error) {
	if pending == nil || pending.write == nil {
		return false, errNativeRecord
	}
	ctx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	ref, err := nativeOutputRef(pending.write)
	if err != nil {
		return false, err
	}
	proof, err := store.InspectOutput(ctx, ref)
	if errors.Is(err, rediscore.ErrAbsent) {
		return false, rediscore.ErrConflict
	}
	if err != nil {
		return false, err
	}
	if proof.State != rediscore.OutputWritten {
		return false, nil
	}
	metadata, err := decodeNativeOutput(proof.Metadata)
	if err != nil || proof.MetadataField != pending.field || !proto.Equal(metadata.Write, pending.write) {
		return false, errNativeRecord
	}
	ack := strings.HasPrefix(pending.field, "r:app-ack-output-") || strings.HasPrefix(pending.field, "r:app-invite-ack-output-") || strings.HasPrefix(pending.field, "r:uac-dialog-ack-output-")
	invite := strings.HasPrefix(pending.field, "r:app-invite-output-")
	class := metadata.Write.Permit.Authorization.AuthorityClass
	if metadata.Status != 0 || !(ack && metadata.Method == "ACK" && class == pb.EffectAuthorization_NATIVE_COMPLETION || invite && metadata.Method == "INVITE" && class == pb.EffectAuthorization_APPLICATION) {
		return false, errNativeRecord
	}
	if err = settleNativeOutput(ctx, store, pending.write, executor, key); err != nil {
		if errors.Is(err, rediscore.ErrAbsent) {
			return false, rediscore.ErrConflict
		}
		return false, err
	}
	switch {
	case strings.HasPrefix(pending.field, "r:app-ack-output-"):
		err = acknowledgeRedisApplicationACK(ctx, store, pending.write, deadline)
	case strings.HasPrefix(pending.field, "r:app-invite-ack-output-"):
		err = acknowledgeRedisApplicationInviteACK(ctx, store, pending.write, deadline)
	case strings.HasPrefix(pending.field, "r:uac-dialog-ack-output-"):
		err = acknowledgeRedisUACSuccessACK(ctx, store, pending.write, nil)
	case strings.HasPrefix(pending.field, "r:app-invite-output-"):
		err = acknowledgeRedisApplicationByeOutput(ctx, store, pending.write, deadline)
	default:
		return false, errNativeRecord
	}
	if errors.Is(err, rediscore.ErrAbsent) {
		return false, rediscore.ErrConflict
	}
	return err == nil, err
}

// planRedisPreviousInviteRelease selects the actual owners of the currently
// selected local INVITE before its dialog references are replaced. Its
// returned mutations are committed only with that replacement.
func planRedisPreviousInviteRelease(ctx context.Context, store *rediscore.Store, selected *rediscore.ObservedContext, current *nativepb.Context, dialogField string, dialog *nativepb.Dialog, leaseUntil time.Time) ([]rediscore.Change, []rediscore.TimerChange, string, time.Time, error) {
	if store == nil || selected == nil || selected.View == nil || current == nil || dialog == nil || dialog.Native == nil || leaseUntil.IsZero() || leaseUntil == leaseUntil.Round(0) || !time.Now().Before(leaseUntil) {
		return nil, nil, "", time.Time{}, errNativeRecord
	}
	if dialog.LocalInvite == nil {
		if dialog.AckResult != "" {
			return nil, nil, "", time.Time{}, errNativeRecord
		}
		return nil, nil, "", time.Time{}, nil
	}
	if dialog.LocalTransaction == "" || dialog.LocalInvite.Request == "" || !slices.Contains(current.Dialogs, dialogField) {
		return nil, nil, "", time.Time{}, errNativeRecord
	}
	old, err := store.LookupSelector(ctx, nativeSelector(rediscore.SelectorClientTransaction, dialog.LocalTransaction))
	if err != nil {
		return nil, nil, "", time.Time{}, err
	}
	if old.Identity != selected.Identity || old.View.Revision != selected.View.Revision || old.View.Timer != selected.View.Timer {
		return nil, nil, "", time.Time{}, rediscore.ErrConflict
	}
	procedure, err := readPrivateProcedureRecord(old.View.Records[old.Record])
	if err != nil || !slices.Contains(current.InitialProcedures, old.Record) && !slices.Contains(current.NoninitialProcedures, old.Record) || procedure.Native.Version != portable.Version || procedure.Native.Family != string(portable.InviteClient) || procedure.Native.Key != dialog.LocalTransaction || procedure.Origin != dialog.LocalInvite.Request {
		return nil, nil, "", time.Time{}, errNativeRecord
	}
	fields := append([]string(nil), current.InitialProcedures...)
	fields = append(fields, procedure.Outputs...)
	view, err := store.Read(ctx, selected.Identity, fields...)
	if err != nil {
		return nil, nil, "", time.Time{}, err
	}
	if view.Revision != selected.View.Revision || view.Timer != selected.View.Timer {
		return nil, nil, "", time.Time{}, rediscore.ErrConflict
	}
	type removable struct {
		field  string
		output *nativepb.Output
	}
	var removals []removable
	for _, field := range procedure.Outputs {
		output, e := decodeNativeOutput(view.Records[field])
		if e != nil || output.Procedure != old.Record {
			return nil, nil, "", time.Time{}, errNativeRecord
		}
		if output.Dialog == dialogField && (output.Method == "ACK" || output.Method == "INVITE" && strings.HasPrefix(field, "r:app-invite-output-")) {
			if !slices.Contains(dialog.Outputs, field) {
				return nil, nil, "", time.Time{}, errNativeRecord
			}
			if current.Ended || output.Write == nil || output.Write.Permit == nil || output.Write.Permit.Authorization == nil || output.Write.Effect == nil || !proto.Equal(output.Write.Effect.Context, current.Context) {
				return nil, nil, "", time.Time{}, errNativeRecord
			}
			if output.Method == "ACK" {
				if !dialog.AckWritten || !proto.Equal(dialog.CompletionFlow, output.Write.Flow) || dialog.Native.LocalInvite == nil || dialog.Native.LocalInvite.State != string(portable.InviteAcked) || output.AckSequence != dialog.Native.LocalInvite.Sequence || dialog.LocalInvite.Ack == "" || output.Write.Permit.Authorization.AuthorityClass != pb.EffectAuthorization_NATIVE_COMPLETION {
					return nil, nil, "", time.Time{}, &previousInviteOutputPending{field: field, write: output.Write}
				}
				switch {
				case strings.HasPrefix(field, "r:app-ack-output-"):
					if !validReadyApplicationACK(output, field) || output.Result != dialog.AckResult {
						return nil, nil, "", time.Time{}, errNativeRecord
					}
				case strings.HasPrefix(field, "r:app-invite-ack-output-"):
					id := strings.TrimPrefix(old.Record, "r:app-invite-")
					if !validReadyApplicationInviteACK(output, field) || len(id) != 32 || dialog.LocalInvite.Ack != "r:app-invite-dialog-ack-"+id {
						return nil, nil, "", time.Time{}, errNativeRecord
					}
				case strings.HasPrefix(field, "r:uac-dialog-ack-output-"):
					attempt := strings.TrimPrefix(field, "r:uac-dialog-ack-output-")
					parsed, x := uuid.Parse(attempt)
					dialogID := strings.TrimPrefix(dialogField, "r:uac-dialog-")
					if x != nil || parsed.String() != attempt || len(dialogID) != 32 || dialogField != "r:uac-dialog-"+dialogID || dialog.LocalInvite.Ack != "r:uac-dialog-ack-"+dialogID {
						return nil, nil, "", time.Time{}, errNativeRecord
					}
				default:
					return nil, nil, "", time.Time{}, errNativeRecord
				}
			} else if !procedure.OutputConsumed || !validReadyApplicationInvite(output, field) || output.Result != procedure.CommandResult || procedure.Native.Phase != string(portable.Accepted) && procedure.Native.Phase != string(portable.Completed) && procedure.Native.Phase != string(portable.Terminated) {
				return nil, nil, "", time.Time{}, &previousInviteOutputPending{field: field, write: output.Write}
			}
			ref, x := nativeOutputRef(output.Write)
			if x != nil {
				return nil, nil, "", time.Time{}, x
			}
			_, x = store.InspectOutput(ctx, ref)
			if x == nil {
				return nil, nil, "", time.Time{}, &previousInviteOutputPending{field: field, write: output.Write}
			}
			if !errors.Is(x, rediscore.ErrAbsent) {
				return nil, nil, "", time.Time{}, x
			}
			removals = append(removals, removable{field: field, output: output})
		}
	}
	if dialog.Native.LocalInvite != nil && dialog.Native.LocalInvite.State == string(portable.InviteAcked) && !dialog.AckWritten {
		return nil, nil, "", time.Time{}, errPreviousInviteACKPending
	}
	if procedure.SuccessfulAck != nil {
		return nil, nil, "", time.Time{}, errNativeRecord
	}
	var promotionUntil time.Time
	if procedure.Native.Phase == string(portable.Accepted) {
		m := procedure.Native.Timers
		if m == nil || m.Values[string(portable.TimerM)] == nil || !m.Values[string(portable.TimerM)].Active || !dialog.AckWritten || !validPolicyFlow(dialog.CompletionFlow) || dialog.Native.LocalInvite == nil || dialog.Native.LocalInvite.State != string(portable.InviteAcked) || dialog.Native.LocalInvite.CancelRequested || dialog.LocalInvite.FinalResponse == "" || dialog.LocalInvite.Ack == "" || dialog.AckEmissions == 0 {
			return nil, nil, "", time.Time{}, errPreviousInviteACKPending
		}
		offset := m.Values[string(portable.TimerM)].At - leaseUntil.UnixMilli()
		if offset > int64((24*time.Hour)/time.Millisecond) || offset < -int64((24*time.Hour)/time.Millisecond) {
			return nil, nil, "", time.Time{}, errNativeRecord
		}
		cutoff := nativeUDPRetryDeadline(leaseUntil, m.Values[string(portable.TimerM)].At)
		if time.Now().Before(cutoff) {
			promotionUntil = cutoff
			procedure.SuccessfulAck = &nativepb.SuccessfulAck{FinalResponse: dialog.LocalInvite.FinalResponse, Ack: dialog.LocalInvite.Ack, Flow: proto.Clone(dialog.CompletionFlow).(*pb.FlowRef), Emissions: dialog.AckEmissions}
		}
	}
	var rootField string
	var root *nativepb.Procedure
	for _, field := range current.InitialProcedures {
		candidate, e := readPrivateProcedureRecord(view.Records[field])
		if e != nil || candidate.Native.Version != portable.Version || candidate.Native.Family != string(portable.InviteClient) && candidate.Native.Family != string(portable.InviteServer) {
			return nil, nil, "", time.Time{}, errNativeRecord
		}
		if candidate.Root == "" {
			if root != nil {
				return nil, nil, "", time.Time{}, errNativeRecord
			}
			rootField, root = field, candidate
		}
	}
	if root == nil || current.Kind == pb.ContextSnapshot_UAS && root.Native.Family != string(portable.InviteServer) || current.Kind == pb.ContextSnapshot_UAC && root.Native.Family != string(portable.InviteClient) || current.Kind != pb.ContextSnapshot_UAS && current.Kind != pb.ContextSnapshot_UAC {
		return nil, nil, "", time.Time{}, errNativeRecord
	}
	for _, field := range current.InitialProcedures {
		candidate, e := readPrivateProcedureRecord(view.Records[field])
		if e != nil || field != rootField && (candidate.Root != rootField || candidate.Native.Family != string(portable.InviteClient) || candidate.CommandResult == "" || candidate.CommandResult != root.CommandResult) {
			return nil, nil, "", time.Time{}, errNativeRecord
		}
	}
	resultFields := []string{procedure.CommandResult, dialog.AckResult, root.CommandResult, "r:result-" + strconv.FormatUint(current.CommandSequence, 10)}
	var extra []string
	for _, field := range append(append(resultFields, root.Events...), procedure.Events...) {
		if field != "" && !slices.Contains(extra, field) {
			extra = append(extra, field)
		}
	}
	records, err := store.Read(ctx, selected.Identity, extra...)
	if err != nil {
		return nil, nil, "", time.Time{}, err
	}
	if records.Revision != selected.View.Revision || records.Timer != selected.View.Timer {
		return nil, nil, "", time.Time{}, rediscore.ErrConflict
	}
	wires := []string{dialog.LocalInvite.Request, dialog.LocalInvite.FinalResponse, dialog.LocalInvite.Ack}
	held := make(map[string]bool, len(wires))
	for _, wire := range wires {
		if wire != "" {
			held[wire] = nativeProcedureReferencesWire(procedure, wire) || nativeProcedureReferencesWire(root, wire) || dialog.LocalBye == wire || dialog.RemoteInvite != nil && (dialog.RemoteInvite.Request == wire || dialog.RemoteInvite.FinalResponse == wire || dialog.RemoteInvite.Ack == wire)
		}
	}
	for _, field := range resultFields {
		if field == "" {
			continue
		}
		result, e := decodeNativeResult(records.Records[field])
		if e != nil {
			return nil, nil, "", time.Time{}, e
		}
		if field == dialog.AckResult {
			id, x := redisApplicationByeID(result.Fingerprint)
			removingOutput := false
			for _, item := range removals {
				removingOutput = removingOutput || item.field == result.Output && item.output.Result == field
			}
			if x != nil || dialog.LocalInvite.Ack != "r:app-ack-wire-"+id || result.Output != "" && !removingOutput || result.Pending || result.Request != "" && result.Request != dialog.LocalInvite.Ack {
				return nil, nil, "", time.Time{}, errNativeRecord
			}
			if procedure.SuccessfulAck != nil && result.Request == dialog.LocalInvite.Ack {
				procedure.SuccessfulAck.AckResult = field
			}
		}
		for _, wire := range wires {
			if wire != "" && (result.Request == wire || result.Response == wire) {
				held[wire] = true
			}
		}
	}
	var changes []rediscore.Change
	var timerChanges []rediscore.TimerChange
	for _, item := range removals {
		if item.output.Result != "" && (strings.HasPrefix(item.field, "r:app-ack-output-") || item.output.Method == "INVITE") {
			result, e := decodeNativeResult(records.Records[item.output.Result])
			if e != nil || result.Pending || result.Output != item.field || result.OperationId != item.output.Operation || result.DecidingLeaseTerm != item.output.Write.Permit.Authorization.ApplicationLeaseTerm {
				return nil, nil, "", time.Time{}, errNativeRecord
			}
			expectedID, e := redisApplicationByeID(result.Fingerprint)
			if e != nil {
				return nil, nil, "", time.Time{}, e
			}
			if item.output.Method == "INVITE" {
				if item.output.Result != procedure.CommandResult || result.Certainty != pb.OperationResult_COMPLETED || item.field != "r:app-invite-output-"+expectedID {
					return nil, nil, "", time.Time{}, errNativeRecord
				}
			} else {
				if item.output.Result != dialog.AckResult || item.output.Result == procedure.CommandResult || item.field != "r:app-ack-output-"+expectedID {
					return nil, nil, "", time.Time{}, errNativeRecord
				}
				timerChanges = append(timerChanges, rediscore.TimerChange{Member: "orig-result-" + strconv.FormatUint(result.Sequence, 10), Due: time.UnixMilli(result.CutoffMs)})
			}
			result.Output = ""
			data, e := encodeNativeResult(result)
			if e != nil {
				return nil, nil, "", time.Time{}, e
			}
			changes = append(changes, rediscore.Change{Field: item.output.Result, Value: data})
		}
		pi, di := slices.Index(procedure.Outputs, item.field), slices.Index(dialog.Outputs, item.field)
		if pi < 0 || di < 0 {
			return nil, nil, "", time.Time{}, errNativeRecord
		}
		procedure.Outputs = slices.Delete(procedure.Outputs, pi, pi+1)
		dialog.Outputs = slices.Delete(dialog.Outputs, di, di+1)
		changes = append(changes, rediscore.Change{Field: item.field, Delete: true})
		timerChanges = append(timerChanges, rediscore.TimerChange{Member: "out-" + strings.TrimPrefix(item.field, "r:"), Delete: true})
	}
	if len(removals) > 0 || procedure.SuccessfulAck != nil {
		data, e := marshalNativeRecord(storagecodec.NativeProcedure, procedure)
		if e != nil {
			return nil, nil, "", time.Time{}, e
		}
		changes = append(changes, rediscore.Change{Field: old.Record, Value: data})
	}
	var events []string
	for _, field := range append(append([]string(nil), root.Events...), procedure.Events...) {
		if slices.Contains(events, field) {
			continue
		}
		events = append(events, field)
		event, e := decodeNativeLifecycle(records.Records[field])
		if e != nil || event.Root != rootField {
			return nil, nil, "", time.Time{}, errNativeRecord
		}
		if event.Message == "" || !slices.Contains(wires, event.Message) {
			continue
		}
		held[event.Message] = true
		if index := slices.Index(event.Owners, dialogField); index >= 0 {
			event.Owners = slices.Delete(event.Owners, index, index+1)
			data, x := encodeNativeLifecycle(event)
			if x != nil {
				return nil, nil, "", time.Time{}, x
			}
			changes = append(changes, rediscore.Change{Field: field, Value: data})
		}
	}
	for _, wire := range wires {
		if wire != "" && !held[wire] {
			changes = append(changes, rediscore.Change{Field: wire, Delete: true})
			held[wire] = true
		}
	}
	return changes, timerChanges, old.Record, promotionUntil, nil
}
