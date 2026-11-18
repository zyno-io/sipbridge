package engine

import (
	"bytes"
	"context"
	"errors"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/nativepb"
	"sipbridge/internal/rediscore"
)

type selectedInitialInviteOutput struct {
	capture  nativeOutputCapture
	selected *rediscore.ObservedContext
	result   *nativepb.Result
}

// Select only metadata. A later response on this procedure cannot replace the
// exact result retained for this output, and completion needs no live app lease.
func inspectRedisInitialInviteOutput(ctx context.Context, store *rediscore.Store, write *pb.FlowWrite, capture *nativeOutputCapture) (selectedInitialInviteOutput, error) {
	var out selectedInitialInviteOutput
	if store == nil || write == nil || write.Effect == nil || write.Flow == nil || write.Permit == nil || write.Permit.Authorization == nil {
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
	if metadata == nil || !proto.Equal(metadata.Write, write) || metadata.Method != "INVITE" && !redisDialogNonInvite(sip.RequestMethod(metadata.Method)) || metadata.Status < 100 || metadata.Status > 699 || capture.ref.RequireApplication || proof.DeadlineMS <= 0 || metadata.Write.Permit.Authorization.DeadlineMs != uint64(proof.DeadlineMS) {
		return out, errNativeRecord
	}
	dialogServer := strings.HasPrefix(proof.MetadataField, "r:dialog-output-")
	if !dialogServer && metadata.Method != "INVITE" {
		return out, errNativeRecord
	}
	id := strings.TrimPrefix(metadata.Procedure, "r:procedure-")
	parsed, err := uuid.Parse(id)
	if err != nil || parsed.String() != id || metadata.Procedure != "r:procedure-"+id {
		return out, errNativeRecord
	}
	prefix := "r:invite-output-" + id + "-"
	responsePrefix, wirePrefix := "r:invite-response-", "r:invite-wire-"
	if dialogServer {
		prefix = "r:dialog-output-" + id + "-"
		responsePrefix, wirePrefix = "r:dialog-response-", "r:dialog-wire-"
	}
	sequence, err := strconv.ParseUint(strings.TrimPrefix(proof.MetadataField, prefix), 10, 64)
	if err != nil || sequence > 1<<53-1 || proof.MetadataField != prefix+strconv.FormatUint(sequence, 10) {
		return out, errNativeRecord
	}
	if dialogServer && sequence == 0 && metadata.Result != "" {
		return out, errNativeRecord
	}
	suffix := id + "-" + strconv.FormatUint(sequence, 10)
	if proof.MessageField != responsePrefix+suffix && proof.MessageField != wirePrefix+suffix || metadata.Result == "" && metadata.Operation != "" || metadata.Result != "" && metadata.Result != "r:result-"+strconv.FormatUint(sequence, 10) {
		return out, errNativeRecord
	}
	fields := []string{"r:context", metadata.Procedure, proof.MetadataField}
	if metadata.Result != "" {
		fields = append(fields, metadata.Result)
	}
	if dialogServer {
		if metadata.Dialog == "" {
			return out, errNativeRecord
		}
		fields = append(fields, metadata.Dialog)
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
	procedure, err := readPrivateProcedureRecord(selected.View.Records[metadata.Procedure])
	if err != nil {
		return out, err
	}
	if current.Context == nil || current.Context.State == nil || current.Context.Id != capture.ref.Context || current.Context.State.Incarnation != capture.ref.Incarnation || !proto.Equal(current.Context, write.Effect.Context) || current.CommandSequence < sequence || procedure.Native.Version != portable.Version || procedure.Native.Reliable != (write.Flow.Transport != "udp") || procedure.RequestId != id || procedure.Origin != "r:origin-"+id || !proto.Equal(procedure.Flow, write.Flow) {
		return out, errNativeRecord
	}
	if dialogServer {
		dialog, e := readPrivateDialogRecord(selected.View.Records[metadata.Dialog])
		family := string(portable.NonInviteServer)
		if metadata.Method == "INVITE" {
			family = string(portable.InviteServer)
		}
		if e != nil || current.Kind != pb.ContextSnapshot_UAS && current.Kind != pb.ContextSnapshot_UAC || !slices.Contains(current.NoninitialProcedures, metadata.Procedure) || !slices.Contains(current.Dialogs, metadata.Dialog) || !slices.Contains(dialog.Outputs, proof.MetadataField) || !slices.Contains(procedure.Outputs, proof.MetadataField) || procedure.Native.Family != family {
			return out, errNativeRecord
		}
	} else if current.Kind != pb.ContextSnapshot_UAS || !slices.Contains(current.InitialProcedures, metadata.Procedure) || !slices.Contains(procedure.Outputs, proof.MetadataField) || procedure.Native.Family != string(portable.InviteServer) {
		return out, errNativeRecord
	} else if metadata.Status >= 200 && metadata.Status < 300 {
		if metadata.Dialog == "" || !slices.Contains(current.Dialogs, metadata.Dialog) {
			return out, errNativeRecord
		}
	} else if metadata.Dialog != "" {
		return out, errNativeRecord
	}
	if sequence == 0 && !dialogServer && !validRedisInitialInviteTimeoutOutput(selected.Identity, current, procedure, metadata, id, proof.MetadataField) {
		return out, errNativeRecord
	}
	if metadata.Result != "" {
		result, e := decodeNativeResult(selected.View.Records[metadata.Result])
		if e != nil {
			return out, e
		}
		if result.Sequence != sequence || result.Output != proof.MetadataField || result.OperationId != metadata.Operation || result.DecidingLeaseTerm != write.Permit.Authorization.ApplicationLeaseTerm || result.Request != "" || result.Response != "" || (!dialogServer || result.Pending) && result.ServerRequestId != "" || dialogServer && !result.Pending && result.ServerRequestId != id || result.RetirementProof != nil || result.Certainty != pb.OperationResult_UNKNOWN && result.Certainty != pb.OperationResult_COMPLETED || result.SipStatus != 0 && result.SipStatus != metadata.Status {
			return out, errNativeRecord
		}
		out.result = result
	}
	out.capture, out.selected = *capture, selected
	return out, nil
}

func settleRedisInitialInviteOutput(ctx context.Context, store *rediscore.Store, write *pb.FlowWrite, capture *nativeOutputCapture, executor rediscore.TimerAssignment, deadline time.Time) error {
	selected, err := inspectRedisInitialInviteOutput(ctx, store, write, capture)
	if err != nil {
		return err
	}
	proof, metadata := selected.capture.proof, selected.capture.metadata
	if proof.State == rediscore.OutputStarted {
		return nil
	}
	if proof.State != rediscore.OutputWritten && proof.State != rediscore.OutputNotWritten && proof.State != rediscore.OutputUnknown {
		return errNativeRecord
	}
	publication := rediscore.Publication{Identity: selected.selected.Identity, Revision: selected.selected.View.Revision, Deadline: deadline, SpendReservation: true}
	if result := selected.result; result != nil {
		certainty, failure := pb.OperationResult_UNKNOWN, "SIP delivery unavailable or uncertain"
		if proof.State == rediscore.OutputWritten {
			certainty, failure = pb.OperationResult_COMPLETED, ""
		}
		if result.Certainty == pb.OperationResult_COMPLETED && certainty != pb.OperationResult_COMPLETED {
			return errNativeRecord
		}
		if result.Pending || result.Certainty != certainty || result.SipStatus != metadata.Status || result.Error != failure {
			result.Pending, result.Certainty, result.SipStatus, result.Error = false, certainty, metadata.Status, failure
			if strings.HasPrefix(proof.MetadataField, "r:dialog-output-") {
				result.ServerRequestId = strings.TrimPrefix(metadata.Procedure, "r:procedure-")
			}
			encoded, e := encodeNativeResult(result)
			if e != nil {
				return e
			}
			publication.Changes = []rediscore.Change{{Field: metadata.Result, Value: encoded}}
			publication.ResultHint = &rediscore.ResultHint{Context: selected.selected.Identity.Context, Incarnation: selected.selected.Identity.Incarnation, Sequence: result.Sequence}
		}
	}
	assignment := selected.selected.View.Timer
	if proof.State != rediscore.OutputUnknown {
		target := assignment
		if target == (rediscore.TimerAssignment{}) {
			target = executor
		}
		publication.Timers = &rediscore.TimerMutation{Expected: assignment, Target: target, Changes: []rediscore.TimerChange{{Member: "out-" + strings.TrimPrefix(proof.MetadataField, "r:"), Due: time.UnixMilli(proof.DeadlineMS)}}}
	}
	if len(publication.Changes) != 0 || publication.Timers != nil {
		_, err = store.Commit(ctx, publication)
		if errors.Is(err, rediscore.ErrExpired) && publication.Timers != nil && assignment != (rediscore.TimerAssignment{}) && assignment != executor && ctx.Err() == nil && time.Now().Before(deadline) {
			publication.Timers.Target = executor
			_, err = store.Commit(ctx, publication)
		}
		if err != nil {
			return err
		}
	}
	if proof.State == rediscore.OutputUnknown || proof.Settled {
		return nil
	}
	return store.SettleOutput(ctx, selected.capture.ref, false, true)
}

// Called only by an actual returned dispatcher or recovered definite consumer.
// Multiple responses have independent proofs; no procedure-wide marker applies.
func acknowledgeRedisInitialInviteOutput(ctx context.Context, store *rediscore.Store, write *pb.FlowWrite) error {
	selected, err := inspectRedisInitialInviteOutput(ctx, store, write, nil)
	if err != nil {
		return err
	}
	proof := selected.capture.proof
	if !proof.Settled || proof.State != rediscore.OutputWritten && proof.State != rediscore.OutputNotWritten {
		return errNativeRecord
	}
	if result := selected.result; result != nil {
		certainty, failure := pb.OperationResult_UNKNOWN, "SIP delivery unavailable or uncertain"
		if proof.State == rediscore.OutputWritten {
			certainty, failure = pb.OperationResult_COMPLETED, ""
		}
		if result.Pending || result.Certainty != certainty || result.Error != failure || result.SipStatus != selected.capture.metadata.Status {
			return errNativeRecord
		}
	}
	if err = recordRedisServerFinalWritten(ctx, store, selected); err != nil {
		return err
	}
	return store.SettleOutput(ctx, selected.capture.ref, true, false)
}
