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

// armRedisClientCancelUDP creates only a process-local E derivative after the
// exact selected CANCEL has a positive recorded write. Ticks do not use Redis.
func (e *redisNative) armRedisClientCancelUDP(ctx context.Context, store *rediscore.Store, ref rediscore.OutputRef, expected *nativepb.Output) error {
	if e.udp == nil || store == nil || ref.RequireApplication || expected == nil || expected.Write == nil || expected.Write.Flow == nil || expected.Write.Flow.Transport != "udp" || !sameProcess(expected.Write.Flow.Ingress, e.cfg.Process) || expected.Method != "CANCEL" || expected.Status != 0 || expected.Dialog != "" || expected.Result != "" {
		return errNativeRecord
	}
	proof, err := store.InspectOutput(ctx, ref)
	if err != nil {
		return err
	}
	if proof.State != rediscore.OutputWritten || !proof.Settled {
		return errors.New("paired CANCEL not positively recorded")
	}
	metadata, err := decodeNativeOutput(proof.Metadata)
	if err != nil || !proto.Equal(metadata, expected) || !strings.HasPrefix(proof.MetadataField, "r:cancel-client-output-") {
		return errNativeRecord
	}
	id := strings.TrimPrefix(proof.MetadataField, "r:cancel-client-output-")
	if len(id) != 32 || metadata.Procedure != "r:cancel-client-"+id || proof.MessageField != "r:cancel-client-origin-"+id {
		return errNativeRecord
	}
	captured := time.Now()
	selected, err := store.ObserveContext(ctx, ref.Context, ref.Incarnation, "r:context", metadata.Procedure, proof.MessageField, proof.MetadataField)
	if err != nil {
		return err
	}
	if !bytes.Equal(selected.View.Records[proof.MetadataField], proof.Metadata) {
		return rediscore.ErrConflict
	}
	current, err := decodeNativeContext(selected.View.Records["r:context"])
	if err != nil {
		return err
	}
	proc, err := decodeNativeProcedure(selected.View.Records[metadata.Procedure])
	if err != nil {
		return err
	}
	if current.Context == nil || !proto.Equal(current.Context, metadata.Write.Effect.Context) || current.Kind != pb.ContextSnapshot_UAS && current.Kind != pb.ContextSnapshot_UAC || !slices.Contains(current.NoninitialProcedures, metadata.Procedure) || proc.Native.Version != portable.Version || proc.Native.Family != portable.NonInviteClient || proc.Native.Phase != portable.Trying || !proc.Metadata.Cleanup || proc.Refs.Origin != proof.MessageField || proc.Refs.LastResponse != "" || !slices.Contains(proc.Metadata.Outputs, proof.MetadataField) || !proto.Equal(proc.Metadata.Flow, metadata.Write.Flow) {
		return errNativeRecord
	}
	memo, err := selectedmessage.NewMemo(1)
	if err != nil {
		return err
	}
	tx, err := proc.attach(&nativeSelectedMessages{records: selected.View.Records, memo: memo, provenance: "admitted"}, true, false, false)
	if err != nil || tx.Origin.WireHash() != proof.WireHash {
		return errNativeRecord
	}
	if _, err = selectRedisClientCancelParent(ctx, store, selected.Identity, selected.View.Revision, selected.View.Timer, current, metadata.Procedure, proc.Native.Key, proc.Refs.Origin, proc.Metadata.Flow, tx.Origin, e.cfg.StateIdentityKey); err != nil {
		return err
	}
	return e.udp.Arm(nativeUDPRetrySeed{Key: nativeUDPRetryKey{Context: ref.Context, Incarnation: ref.Incarnation, Procedure: metadata.Procedure}, Attempt: ref, Proof: proof, Captured: captured, Transaction: tx, Flow: metadata.Write.Flow})
}
