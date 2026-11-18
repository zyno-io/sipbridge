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
	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/nativepb"
	"sipbridge/internal/rediscore"
	"sipbridge/internal/selectedmessage"
)

// The original re-INVITE proof is still present here; its dispatcher releases
// it after optional local A admission. Resend ticks never return to Redis.
func (e *redisNative) armRedisApplicationInviteUDP(ctx context.Context, store *rediscore.Store, ref rediscore.OutputRef, expected *nativepb.Output) error {
	if e.udp == nil || store == nil || !ref.RequireApplication || expected == nil || expected.Write == nil || expected.Write.Flow == nil || expected.Method != "INVITE" || expected.Status != 0 || expected.Result == "" || expected.Dialog == "" || expected.Replay || expected.Write.Flow.Transport != "udp" || !sameProcess(expected.Write.Flow.Ingress, e.cfg.Process) {
		return errNativeRecord
	}
	id := strings.TrimPrefix(expected.Procedure, "r:app-invite-")
	if len(id) != 32 || expected.Procedure != "r:app-invite-"+id {
		return errNativeRecord
	}
	origin := "r:app-invite-origin-" + id
	proof, err := store.InspectOutput(ctx, ref)
	if err != nil {
		return err
	}
	if proof.State != rediscore.OutputWritten || !proof.Settled || proof.MetadataField != "r:app-invite-output-"+id || proof.MessageField != origin {
		return errors.New("re-INVITE attempt not positively recorded")
	}
	metadata, err := decodeNativeOutput(proof.Metadata)
	if err != nil || !proto.Equal(metadata, expected) {
		return errNativeRecord
	}
	// A stop observed during this entire read suppresses subsequent arming.
	captured := time.Now()
	view, err := store.ObserveContext(ctx, ref.Context, ref.Incarnation, "r:context", metadata.Procedure, metadata.Dialog, metadata.Result, origin, proof.MetadataField)
	if err != nil {
		return err
	}
	if !bytes.Equal(view.View.Records[proof.MetadataField], proof.Metadata) {
		return rediscore.ErrConflict
	}
	current, err := decodeNativeContext(view.View.Records["r:context"])
	if err != nil {
		return err
	}
	procedure, err := decodeNativeProcedure(view.View.Records[metadata.Procedure])
	if err != nil {
		return err
	}
	dialog, err := readPrivateDialogRecord(view.View.Records[metadata.Dialog])
	if err != nil {
		return err
	}
	result, err := decodeNativeResult(view.View.Records[metadata.Result])
	if err != nil {
		return err
	}
	expectedID, err := redisApplicationByeID(result.Fingerprint)
	if err != nil || expectedID != id || !validReadyApplicationInvite(metadata, proof.MetadataField) || current.Kind != pb.ContextSnapshot_UAC && current.Kind != pb.ContextSnapshot_UAS || current.Ended || !proto.Equal(current.Context, metadata.Write.Effect.Context) || !slices.Contains(current.NoninitialProcedures, metadata.Procedure) || !slices.Contains(current.Dialogs, metadata.Dialog) || !slices.Contains(dialog.Outputs, proof.MetadataField) || dialog.LocalTransaction != procedure.Native.Key || dialog.LocalInvite == nil || dialog.LocalInvite.Request != origin || procedure.Native.Family != portable.InviteClient || procedure.Native.Phase != portable.Calling || procedure.Native.Cancelled || procedure.Refs.Origin != origin || procedure.Refs.LastResponse != "" || procedure.Refs.ACK != "" || procedure.Metadata.CommandResult != metadata.Result || result.Sequence == 0 || result.Sequence > current.CommandSequence || metadata.Result != "r:result-"+strconv.FormatUint(result.Sequence, 10) || result.Output != proof.MetadataField || result.OperationId != metadata.Operation || result.DecidingLeaseTerm != metadata.Write.Permit.Authorization.ApplicationLeaseTerm || !slices.Contains(procedure.Metadata.Outputs, proof.MetadataField) || !proto.Equal(procedure.Metadata.Flow, metadata.Write.Flow) {
		return errNativeRecord
	}
	memo, err := selectedmessage.NewMemo(1)
	if err != nil {
		return err
	}
	tx, err := procedure.attach(&nativeSelectedMessages{records: view.View.Records, memo: memo, provenance: "admitted"}, true, false, false)
	if err != nil || tx.Origin.WireHash() != proof.WireHash {
		return errNativeRecord
	}
	return e.udp.Arm(nativeUDPRetrySeed{Key: nativeUDPRetryKey{Context: ref.Context, Incarnation: ref.Incarnation, Procedure: metadata.Procedure}, Attempt: ref, Proof: proof, Captured: captured, Transaction: tx, Flow: metadata.Write.Flow})
}
