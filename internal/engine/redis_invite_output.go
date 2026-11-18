package engine

import (
	"bytes"
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
)

func nativeReadyInviteTrying(metadata *nativepb.Output, field string) (bool, error) {
	if metadata == nil || metadata.Write == nil || metadata.Write.Permit == nil || metadata.Write.Permit.Authorization == nil || metadata.Write.Permit.Authorization.AuthorityClass != pb.EffectAuthorization_RESPONSE || metadata.Method != "INVITE" || metadata.Status < 100 || metadata.Status > 699 {
		return false, errNativeRecord
	}
	id := strings.TrimPrefix(metadata.Procedure, "r:procedure-")
	parsed, err := uuid.Parse(id)
	if err != nil || parsed.String() != id || metadata.Procedure != "r:procedure-"+id {
		return false, errNativeRecord
	}
	if field == "r:output-"+id+"-100" {
		if metadata.Status != 100 || metadata.Result != "" || metadata.Operation != "" || metadata.Dialog != "" {
			return false, errNativeRecord
		}
		return true, nil
	}
	prefix := "r:invite-output-" + id + "-"
	if !strings.HasPrefix(field, prefix) {
		return false, errNativeRecord
	}
	if metadata.Status >= 200 && metadata.Status < 300 && metadata.Dialog != "r:dialog-"+id || (metadata.Status < 200 || metadata.Status >= 300) && metadata.Dialog != "" {
		return false, errNativeRecord
	}
	sequence, err := strconv.ParseUint(strings.TrimPrefix(field, prefix), 10, 53)
	if err != nil || field != prefix+strconv.FormatUint(sequence, 10) || metadata.Result == "" && (sequence != 1 && !(sequence == 0 && metadata.Status == 503) || metadata.Operation != "") || metadata.Result != "" && (sequence == 0 || metadata.Result != "r:result-"+strconv.FormatUint(sequence, 10) || metadata.Operation == "") {
		return false, errNativeRecord
	}
	return false, nil
}

// settleRedisInviteTryingOutput records only native settlement. The caller
// separately acknowledges after the actual dispatcher has returned. A Trying
// response has no business result or final-response marker to update.
func settleRedisInviteTryingOutput(ctx context.Context, store *rediscore.Store, write *pb.FlowWrite, capture *nativeOutputCapture, executor rediscore.TimerAssignment, deadline time.Time) error {
	if store == nil || capture == nil || capture.metadata == nil || !proto.Equal(capture.metadata.Write, write) || capture.metadata.Method != "INVITE" || capture.metadata.Status != 100 || capture.metadata.Dialog != "" || capture.metadata.Result != "" || capture.ref.RequireApplication {
		return errNativeRecord
	}
	id := strings.TrimPrefix(capture.metadata.Procedure, "r:procedure-")
	parsed, err := uuid.Parse(id)
	if err != nil || parsed.String() != id || capture.metadata.Procedure != "r:procedure-"+id || capture.proof.MetadataField != "r:output-"+id+"-100" {
		return errNativeRecord
	}
	switch capture.proof.State {
	case rediscore.OutputStarted, rediscore.OutputUnknown:
		return nil
	case rediscore.OutputWritten, rediscore.OutputNotWritten:
	default:
		return errors.New("Trying has no definite completion proof")
	}
	selected, err := store.ObserveContext(ctx, capture.ref.Context, capture.ref.Incarnation, "r:context", capture.metadata.Procedure, capture.proof.MetadataField)
	if err != nil {
		return err
	}
	if !bytes.Equal(selected.View.Records[capture.proof.MetadataField], capture.proof.Metadata) {
		return rediscore.ErrConflict
	}
	current, err := decodeNativeContext(selected.View.Records["r:context"])
	if err != nil {
		return err
	}
	procedure, err := readPrivateProcedureRecord(selected.View.Records[capture.metadata.Procedure])
	if err != nil {
		return err
	}
	if !proto.Equal(current.Context, write.Effect.Context) || current.Kind != pb.ContextSnapshot_UAS || !slices.Contains(current.InitialProcedures, capture.metadata.Procedure) || !slices.Contains(procedure.Outputs, capture.proof.MetadataField) || procedure.Native.Version != portable.Version || procedure.Native.Family != string(portable.InviteServer) || procedure.RequestId != id || !proto.Equal(procedure.Flow, write.Flow) || procedure.Emissions == 0 || procedure.ProvisionalEmissions == 0 {
		return errors.New("Trying completion evidence changed")
	}
	if capture.proof.DeadlineMS <= 0 || capture.proof.DeadlineMS != int64(write.Permit.Authorization.DeadlineMs) || !time.Now().Before(deadline) {
		return errNativeRecord
	}
	if capture.proof.Settled {
		return nil
	}
	target := selected.View.Timer
	if target == (rediscore.TimerAssignment{}) {
		target = executor
	}
	publication := rediscore.Publication{Identity: selected.Identity, Revision: selected.View.Revision, Deadline: deadline, SpendReservation: true, Timers: &rediscore.TimerMutation{Expected: selected.View.Timer, Target: target, Changes: []rediscore.TimerChange{{Member: "out-" + strings.TrimPrefix(capture.proof.MetadataField, "r:"), Due: time.UnixMilli(capture.proof.DeadlineMS)}}}}
	_, err = store.Commit(ctx, publication)
	if errors.Is(err, rediscore.ErrExpired) && selected.View.Timer != (rediscore.TimerAssignment{}) && selected.View.Timer != executor && ctx.Err() == nil && time.Now().Before(deadline) {
		publication.Timers.Target = executor
		_, err = store.Commit(ctx, publication)
	}
	if err != nil {
		return err
	}
	return store.SettleOutput(ctx, capture.ref, false, true)
}

// The dispatcher acknowledges only after returning from the exact attempt.
func acknowledgeRedisInviteTryingOutput(ctx context.Context, store *rediscore.Store, write *pb.FlowWrite, executor rediscore.TimerAssignment, deadline time.Time) error {
	ref, err := nativeOutputRef(write)
	if err != nil {
		return err
	}
	proof, err := store.InspectOutput(ctx, ref)
	if err != nil {
		return err
	}
	if !proof.Settled || proof.State != rediscore.OutputWritten && proof.State != rediscore.OutputNotWritten {
		return errNativeRecord
	}
	metadata, err := decodeNativeOutput(proof.Metadata)
	if err != nil {
		return err
	}
	if err = settleRedisInviteTryingOutput(ctx, store, write, &nativeOutputCapture{ref: ref, proof: proof, metadata: metadata}, executor, deadline); err != nil {
		return err
	}
	return store.SettleOutput(ctx, ref, true, false)
}
