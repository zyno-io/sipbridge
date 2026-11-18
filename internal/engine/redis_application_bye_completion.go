package engine

import (
	"bytes"
	"context"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/emiago/sipgo/sip/portable"
	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/rediscore"
)

// inspectRedisApplicationByeOutput checks the selected immutable physical
// attempt and actual native owners without granting new application authority.
func inspectRedisApplicationByeOutput(ctx context.Context, store *rediscore.Store, write *pb.FlowWrite, capture *nativeOutputCapture) (selectedCleanupByeOutput, error) {
	var out selectedCleanupByeOutput
	if store == nil {
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
	metadata, proof := capture.metadata, capture.proof
	if metadata == nil || write == nil || write.Permit == nil || write.Permit.Authorization == nil || !proto.Equal(metadata.Write, write) || !redisApplicationClientMethod(metadata.Method) && metadata.Method != "INVITE" || metadata.Status != 0 || metadata.Result == "" || metadata.Operation == "" || metadata.Dialog == "" || !capture.ref.RequireApplication || write.Permit.Authorization.AuthorityClass != pb.EffectAuthorization_APPLICATION || proof.DeadlineMS <= 0 || write.Permit.Authorization.DeadlineMs != uint64(proof.DeadlineMS) {
		return out, errNativeRecord
	}
	prefix := "r:" + redisApplicationClientPrefix(metadata.Method)
	if metadata.Method == "INVITE" {
		prefix = "r:app-invite-"
	}
	id := strings.TrimPrefix(metadata.Procedure, prefix)
	if len(id) != 32 || metadata.Procedure != prefix+id || proof.MetadataField != prefix+"output-"+id || proof.MessageField != prefix+"origin-"+id {
		return out, errNativeRecord
	}
	selected, err := store.ObserveContext(ctx, capture.ref.Context, capture.ref.Incarnation, "r:context", metadata.Procedure, metadata.Dialog, proof.MetadataField, metadata.Result)
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
	procedure, err := readPrivateProcedureRecord(selected.View.Records[metadata.Procedure])
	if err != nil {
		return out, err
	}
	dialog, err := readPrivateDialogRecord(selected.View.Records[metadata.Dialog])
	if err != nil {
		return out, err
	}
	result, err := decodeNativeResult(selected.View.Records[metadata.Result])
	if err != nil {
		return out, err
	}
	expectedID, err := redisApplicationByeID(result.Fingerprint)
	if err != nil || expectedID != id {
		return out, errNativeRecord
	}
	family := string(portable.NonInviteClient)
	if metadata.Method == "INVITE" {
		family = string(portable.InviteClient)
	}
	if current.Context == nil || !proto.Equal(current.Context, write.Effect.Context) || current.Kind != pb.ContextSnapshot_UAS && current.Kind != pb.ContextSnapshot_UAC || !slices.Contains(current.NoninitialProcedures, metadata.Procedure) || !slices.Contains(current.Dialogs, metadata.Dialog) || !slices.Contains(procedure.Outputs, proof.MetadataField) || !slices.Contains(dialog.Outputs, proof.MetadataField) || metadata.Method == "BYE" && (dialog.LocalTransaction != procedure.Native.Key || dialog.LocalBye != proof.MessageField) || metadata.Method == "INVITE" && (dialog.LocalInvite == nil || dialog.LocalInvite.Request != proof.MessageField) || procedure.Native.Version != portable.Version || procedure.Native.Family != family || procedure.Origin != proof.MessageField || procedure.Cleanup || procedure.CommandResult != metadata.Result || !proto.Equal(procedure.Flow, write.Flow) || result.Sequence == 0 || result.Sequence > current.CommandSequence || metadata.Result != "r:result-"+strconv.FormatUint(result.Sequence, 10) || result.OperationId != metadata.Operation || result.Output != proof.MetadataField || result.DecidingLeaseTerm != write.Permit.Authorization.ApplicationLeaseTerm {
		return out, errNativeRecord
	}
	out.capture, out.selected, out.procedure = *capture, selected, procedure
	return out, nil
}

// settleRedisApplicationByeOutput publishes this attempt's physical receipt
// before any definite proof can be released. Native final/F may later upgrade it.
func settleRedisApplicationByeOutput(ctx context.Context, store *rediscore.Store, write *pb.FlowWrite, capture *nativeOutputCapture, executor rediscore.TimerAssignment, deadline time.Time) error {
	selected, err := inspectRedisApplicationByeOutput(ctx, store, write, capture)
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
	result, err := decodeNativeResult(selected.selected.View.Records[selected.capture.metadata.Result])
	if err != nil {
		return err
	}
	certainty, failure, request := pb.OperationResult_UNKNOWN, "SIP delivery unavailable or uncertain", ""
	if proof.State == rediscore.OutputWritten && selected.capture.metadata.Method == "BYE" {
		certainty, failure, request = pb.OperationResult_ACCEPTED, "", proof.MessageField
	}
	update := proof.State != rediscore.OutputWritten && result.Pending || result.Pending && certainty == pb.OperationResult_ACCEPTED || result.Certainty == pb.OperationResult_UNKNOWN && certainty == pb.OperationResult_ACCEPTED
	if update {
		result.Pending = false
		result.Certainty, result.SipStatus, result.Error = certainty, 0, failure
		result.Request, result.Response = request, ""
		value, e := encodeNativeResult(result)
		if e != nil {
			return e
		}
		_, e = store.Commit(ctx, rediscore.Publication{Identity: selected.selected.Identity, Revision: selected.selected.View.Revision, Deadline: deadline, SpendReservation: true, Changes: []rediscore.Change{{Field: selected.capture.metadata.Result, Value: value}}, ResultHint: &rediscore.ResultHint{Context: selected.selected.Identity.Context, Incarnation: selected.selected.Identity.Incarnation, Sequence: result.Sequence}})
		if e != nil {
			return e
		}
		if proof.State == rediscore.OutputUnknown {
			return nil
		}
		selected, err = inspectRedisApplicationByeOutput(ctx, store, write, capture)
		if err != nil {
			return err
		}
	}
	if proof.State == rediscore.OutputUnknown {
		return nil
	}
	return settleRedisClientByeSelected(ctx, store, selected, executor, deadline)
}

func acknowledgeRedisApplicationByeOutput(ctx context.Context, store *rediscore.Store, write *pb.FlowWrite, deadline time.Time) error {
	selected, err := inspectRedisApplicationByeOutput(ctx, store, write, nil)
	if err != nil {
		return err
	}
	return acknowledgeRedisClientByeSelected(ctx, store, selected, deadline)
}
