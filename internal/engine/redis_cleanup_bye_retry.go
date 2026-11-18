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

// armRedisCleanupByeUDP reuses the process-local E/F derivative after the
// exact cleanup BYE attempt is positively recorded and before acknowledgement.
func (e *redisNative) armRedisCleanupByeUDP(ctx context.Context, store *rediscore.Store, ref rediscore.OutputRef, expected *nativepb.Output) error {
	if e.udp == nil || store == nil || expected == nil || expected.Write == nil || expected.Write.Flow == nil || expected.Write.Flow.Transport != "udp" || !sameProcess(expected.Write.Flow.Ingress, e.cfg.Process) || !redisApplicationClientMethod(expected.Method) || expected.Status != 0 {
		return errNativeRecord
	}
	proof, err := store.InspectOutput(ctx, ref)
	if err != nil {
		return err
	}
	if proof.State != rediscore.OutputWritten || !proof.Settled {
		return errors.New("cleanup BYE not positively recorded")
	}
	metadata, err := decodeNativeOutput(proof.Metadata)
	application := strings.HasPrefix(proof.MetadataField, "r:app-bye-output-") || strings.HasPrefix(proof.MetadataField, "r:app-client-output-")
	if err != nil || !proto.Equal(metadata, expected) || (metadata.Result != "") != application || metadata.Dialog == "" {
		return errNativeRecord
	}
	if application {
		prefix := "r:" + redisApplicationClientPrefix(metadata.Method)
		id := strings.TrimPrefix(proof.MetadataField, prefix+"output-")
		if len(id) != 32 || metadata.Procedure != prefix+id || proof.MessageField != prefix+"origin-"+id {
			return errNativeRecord
		}
	}
	// The stop-before-arm window covers the whole post-proof selected read.
	captured := time.Now()
	selected, err := store.ObserveContext(ctx, ref.Context, ref.Incarnation, "r:context", metadata.Procedure, metadata.Dialog, proof.MessageField, proof.MetadataField)
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
	procedure, err := decodeNativeProcedure(selected.View.Records[metadata.Procedure])
	if err != nil {
		return err
	}
	dialog, err := readPrivateDialogRecord(selected.View.Records[metadata.Dialog])
	if err != nil {
		return err
	}
	if current.Context == nil || !proto.Equal(current.Context, metadata.Write.Effect.Context) || current.Kind != pb.ContextSnapshot_UAS && current.Kind != pb.ContextSnapshot_UAC || !slices.Contains(current.NoninitialProcedures, metadata.Procedure) || !slices.Contains(current.Dialogs, metadata.Dialog) || !slices.Contains(dialog.Outputs, proof.MetadataField) || application && !slices.Contains(procedure.Metadata.Outputs, proof.MetadataField) || metadata.Method == "BYE" && (dialog.LocalTransaction != procedure.Native.Key || dialog.LocalBye != proof.MessageField) || procedure.Native.Family != portable.NonInviteClient || procedure.Metadata.Cleanup == application || procedure.Metadata.CommandResult != metadata.Result || procedure.Refs.Origin != proof.MessageField || !proto.Equal(procedure.Metadata.Flow, metadata.Write.Flow) {
		return errNativeRecord
	}
	memo, err := selectedmessage.NewMemo(1)
	if err != nil {
		return err
	}
	tx, err := procedure.attach(&nativeSelectedMessages{records: selected.View.Records, memo: memo, provenance: "admitted"}, true, false, false)
	if err != nil {
		return err
	}
	if tx.Origin.WireHash() != proof.WireHash {
		return errNativeRecord
	}
	return e.udp.Arm(nativeUDPRetrySeed{Key: nativeUDPRetryKey{Context: ref.Context, Incarnation: ref.Incarnation, Procedure: metadata.Procedure}, Attempt: ref, Proof: proof, Captured: captured, Transaction: tx, Flow: metadata.Write.Flow})
}
