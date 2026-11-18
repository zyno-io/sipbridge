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
	"sipbridge/internal/storagecodec"
)

type selectedRedisCancelOutput struct {
	capture   nativeOutputCapture
	selected  *rediscore.ObservedContext
	procedure *nativepb.Procedure
	own       bool
}

// inspectRedisCancelOutput reads only the exact native ownership metadata.
// Each response keeps its own immutable proof; no SIP parsing is necessary.
func inspectRedisCancelOutput(ctx context.Context, store *rediscore.Store, write *pb.FlowWrite, capture *nativeOutputCapture) (selectedRedisCancelOutput, error) {
	var out selectedRedisCancelOutput
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
	if metadata == nil || !proto.Equal(metadata.Write, write) || metadata.Result != "" || metadata.Operation != "" || metadata.Dialog != "" || capture.ref.RequireApplication || write.Permit.Authorization.AuthorityClass != pb.EffectAuthorization_RESPONSE || proof.DeadlineMS <= 0 || uint64(proof.DeadlineMS) != write.Permit.Authorization.DeadlineMs {
		return out, errNativeRecord
	}
	prefix, wirePrefix := "r:call-cancel-invite-output-", "r:call-cancel-invite-"
	own := metadata.Method == "CANCEL"
	if own {
		prefix, wirePrefix = "r:call-cancel-output-", "r:call-cancel-"
		if metadata.Status != 200 && metadata.Status != 481 {
			return out, errNativeRecord
		}
	} else if metadata.Method != "INVITE" || metadata.Status != 487 {
		return out, errNativeRecord
	}
	id := strings.TrimPrefix(proof.MetadataField, prefix)
	if len(id) != 32 || proof.MetadataField != prefix+id || proof.MessageField != wirePrefix+"final-"+id && proof.MessageField != wirePrefix+"wire-"+id {
		return out, errNativeRecord
	}
	selected, err := store.ObserveContext(ctx, capture.ref.Context, capture.ref.Incarnation, "r:context", metadata.Procedure, proof.MetadataField)
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
	if current.Kind != pb.ContextSnapshot_UAS && current.Kind != pb.ContextSnapshot_UAC || current.Context == nil || current.Context.State == nil || current.Context.Id != capture.ref.Context || current.Context.State.Incarnation != capture.ref.Incarnation || !proto.Equal(current.Context, write.Effect.Context) || procedure.Native.Version != portable.Version || procedure.Native.Reliable != (write.Flow.Transport != "udp") || !proto.Equal(procedure.Flow, write.Flow) || !slices.Contains(procedure.Outputs, proof.MetadataField) {
		return out, errNativeRecord
	}
	if own {
		if metadata.Procedure != "r:call-cancel-"+id || procedure.RequestId != id || procedure.Native.Family != string(portable.NonInviteServer) || procedure.Origin != "r:call-cancel-origin-"+id || procedure.LastResponse != "r:call-cancel-final-"+id || !slices.Contains(current.NoninitialProcedures, metadata.Procedure) || proof.Acknowledged && !procedure.OutputConsumed || procedure.Native.Phase != string(portable.Completed) && procedure.Native.Phase != string(portable.Terminated) || procedure.Native.Reliable && procedure.Native.Phase != string(portable.Terminated) {
			return out, errNativeRecord
		}
	} else {
		requestID := procedure.RequestId
		parsed, e := uuid.Parse(requestID)
		if e != nil || parsed.String() != requestID || metadata.Procedure != "r:procedure-"+requestID || procedure.Native.Family != string(portable.InviteServer) || procedure.Origin != "r:origin-"+requestID || procedure.LastResponse != "r:call-cancel-invite-final-"+id || !slices.Contains(current.InitialProcedures, metadata.Procedure) || procedure.Native.Phase != string(portable.Completed) && procedure.Native.Phase != string(portable.Confirmed) && procedure.Native.Phase != string(portable.Terminated) {
			return out, errNativeRecord
		}
	}
	out.capture, out.selected, out.procedure, out.own = *capture, selected, procedure, own
	return out, nil
}

func settleRedisCancelOutput(ctx context.Context, store *rediscore.Store, write *pb.FlowWrite, capture *nativeOutputCapture, executor rediscore.TimerAssignment, deadline time.Time) error {
	selected, err := inspectRedisCancelOutput(ctx, store, write, capture)
	if err != nil {
		return err
	}
	proof := selected.capture.proof
	if proof.State == rediscore.OutputStarted || proof.State == rediscore.OutputUnknown {
		return nil
	}
	if proof.State != rediscore.OutputWritten && proof.State != rediscore.OutputNotWritten || !time.Now().Before(deadline) {
		return errNativeRecord
	}
	if proof.Settled {
		return nil
	}
	assignment := selected.selected.View.Timer
	target := assignment
	if target == (rediscore.TimerAssignment{}) {
		target = executor
	}
	publication := rediscore.Publication{Identity: selected.selected.Identity, Revision: selected.selected.View.Revision, Deadline: deadline, SpendReservation: true,
		Timers: &rediscore.TimerMutation{Expected: assignment, Target: target, Changes: []rediscore.TimerChange{{Member: "out-" + proof.MetadataField[2:], Due: time.UnixMilli(proof.DeadlineMS)}}}}
	_, err = store.Commit(ctx, publication)
	if errors.Is(err, rediscore.ErrExpired) && assignment != (rediscore.TimerAssignment{}) && assignment != executor && ctx.Err() == nil && time.Now().Before(deadline) {
		publication.Timers.Target = executor
		_, err = store.Commit(ctx, publication)
	}
	if err != nil {
		return err
	}
	return store.SettleOutput(ctx, selected.capture.ref, false, true)
}

// Actual returned dispatchers or exact recovered consumers call this only
// after native settlement. It never releases the other response's proof.
func acknowledgeRedisCancelOutput(ctx context.Context, store *rediscore.Store, write *pb.FlowWrite, deadline time.Time) error {
	selected, err := inspectRedisCancelOutput(ctx, store, write, nil)
	if err != nil {
		return err
	}
	proof := selected.capture.proof
	if !proof.Settled || proof.State != rediscore.OutputWritten && proof.State != rediscore.OutputNotWritten || !time.Now().Before(deadline) {
		return errNativeRecord
	}
	if selected.own && !selected.procedure.OutputConsumed {
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
