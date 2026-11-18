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
	"sipbridge/internal/selectedmessage"
)

// The original positive proof is still present here; its dispatcher releases
// it after optional local A admission. Resend ticks never return to Redis.
func (e *redisNative) armRedisInitialUACUDP(ctx context.Context, store *rediscore.Store, ref rediscore.OutputRef, expected *nativepb.Output) error {
	if e.udp == nil || store == nil || !ref.RequireApplication || expected == nil || expected.Write == nil || expected.Write.Flow == nil || expected.Method != "INVITE" || expected.Status != 0 || expected.Result != "r:result-1" || expected.Dialog != "" || expected.Replay || expected.Write.Flow.Transport != "udp" || !sameProcess(expected.Write.Flow.Ingress, e.cfg.Process) {
		return errNativeRecord
	}
	id := strings.TrimPrefix(expected.Procedure, "r:uac-procedure-")
	if len(id) != 32 || expected.Procedure != "r:uac-procedure-"+id {
		return errNativeRecord
	}
	origin := "r:uac-origin-" + id
	proof, err := store.InspectOutput(ctx, ref)
	if err != nil {
		return err
	}
	if proof.State != rediscore.OutputWritten || !proof.Settled || proof.MetadataField != "r:uac-output-"+id || proof.MessageField != origin {
		return errors.New("initial INVITE attempt not positively recorded")
	}
	metadata, err := decodeNativeOutput(proof.Metadata)
	if err != nil || !proto.Equal(metadata, expected) {
		return errNativeRecord
	}
	// A stop observed during this entire read suppresses subsequent arming.
	captured := time.Now()
	view, err := store.ObserveContext(ctx, ref.Context, ref.Incarnation, "r:context", metadata.Procedure, origin, proof.MetadataField)
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
	if current.Kind != pb.ContextSnapshot_UAC || current.Ended || !proto.Equal(current.Context, metadata.Write.Effect.Context) || !slices.Contains(current.InitialProcedures, metadata.Procedure) || procedure.Native.Family != portable.InviteClient || procedure.Native.Phase != portable.Calling || procedure.Native.Cancelled || procedure.Refs.Origin != origin || procedure.Refs.LastResponse != "" || procedure.Refs.ACK != "" || procedure.Metadata.CommandResult != metadata.Result || !slices.Contains(procedure.Metadata.Outputs, proof.MetadataField) || !proto.Equal(procedure.Metadata.Flow, metadata.Write.Flow) {
		return errNativeRecord
	}
	if _, _, err = selectRedisUACRoot(ctx, store, view.Identity, view.View.Revision, view.View.Timer, current, metadata.Procedure, procedure); err != nil {
		return err
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
