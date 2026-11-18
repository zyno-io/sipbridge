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
	"sipbridge/internal/selectedmessage"
)

// armRedisInitialInviteRetry is called only before acknowledging the exact
// settled WRITTEN attempt. It never changes Redis state or reissues an attempt.
func (e *redisNative) armRedisInitialInviteRetry(ctx context.Context, store *rediscore.Store, ref rediscore.OutputRef, expected *nativepb.Output) error {
	if e.udp == nil || store == nil || expected == nil || expected.Write == nil || expected.Write.Flow == nil || !sameProcess(expected.Write.Flow.Ingress, e.cfg.Process) || expected.Method != "INVITE" || expected.Status < 200 {
		return errNativeRecord
	}
	if expected.Status >= 300 && expected.Write.Flow.Transport != "udp" {
		return nil // Timer G is UDP-only.
	}
	proof, err := store.InspectOutput(ctx, ref)
	if err != nil {
		return err
	}
	if proof.State != rediscore.OutputWritten || !proof.Settled {
		return errors.New("INVITE attempt not positively recorded")
	}
	metadata, err := decodeNativeOutput(proof.Metadata)
	if err != nil || !proto.Equal(metadata, expected) {
		return errNativeRecord
	}
	dialogServer := strings.HasPrefix(proof.MetadataField, "r:dialog-output-")
	if !dialogServer {
		trying, e := nativeReadyInviteTrying(metadata, proof.MetadataField)
		if e != nil || trying {
			return errNativeRecord
		}
	}
	id := strings.TrimPrefix(metadata.Procedure, "r:procedure-")
	parsed, err := uuid.Parse(id)
	prefix := "r:invite-output-" + id + "-"
	responsePrefix := "r:invite-response-"
	if dialogServer {
		prefix, responsePrefix = "r:dialog-output-"+id+"-", "r:dialog-response-"
	}
	if err != nil || parsed.String() != id || !strings.HasPrefix(proof.MetadataField, prefix) {
		return errNativeRecord
	}
	sequence, err := strconv.ParseUint(strings.TrimPrefix(proof.MetadataField, prefix), 10, 53)
	if err != nil || sequence == 0 && !dialogServer || sequence == 0 && metadata.Result != "" || proof.MetadataField != prefix+strconv.FormatUint(sequence, 10) {
		return errNativeRecord
	}
	originField, logicalField := "r:origin-"+id, responsePrefix+id+"-"+strconv.FormatUint(sequence, 10)
	fields := []string{"r:context", metadata.Procedure, proof.MetadataField, originField, logicalField}
	if !slices.Contains(fields, proof.MessageField) {
		fields = append(fields, proof.MessageField)
	}
	if metadata.Dialog != "" {
		fields = append(fields, metadata.Dialog)
	}
	// The hint fence covers the whole selected read, not merely its reply.
	captured := time.Now()
	selected, err := store.ObserveContext(ctx, ref.Context, ref.Incarnation, fields...)
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
	procedure, err := readPrivateProcedureRecord(selected.View.Records[metadata.Procedure])
	if err != nil {
		return err
	}
	classValid := !dialogServer && current.Kind == pb.ContextSnapshot_UAS && slices.Contains(current.InitialProcedures, metadata.Procedure) || dialogServer && (current.Kind == pb.ContextSnapshot_UAS || current.Kind == pb.ContextSnapshot_UAC) && slices.Contains(current.NoninitialProcedures, metadata.Procedure)
	if current.Context == nil || !proto.Equal(current.Context, metadata.Write.Effect.Context) || !classValid || !slices.Contains(procedure.Outputs, proof.MetadataField) || procedure.Native.Version != portable.Version || procedure.Native.Family != string(portable.InviteServer) || procedure.RequestId != id || procedure.Origin != originField || procedure.LastResponse != logicalField || !proto.Equal(procedure.Flow, metadata.Write.Flow) {
		return errNativeRecord
	}
	if dialogServer {
		dialog, e := readPrivateDialogRecord(selected.View.Records[metadata.Dialog])
		if e != nil || !slices.Contains(current.Dialogs, metadata.Dialog) || !slices.Contains(dialog.Outputs, proof.MetadataField) {
			return errNativeRecord
		}
	}
	memo, err := selectedmessage.NewMemo(2)
	if err != nil {
		return err
	}
	messages := &nativeSelectedMessages{records: selected.View.Records, memo: memo, provenance: "admitted"}
	wire, err := selectedmessage.DecodeWire(selected.View.Records[proof.MessageField])
	if err != nil {
		return err
	}
	key := nativeUDPRetryKey{Context: ref.Context, Incarnation: ref.Incarnation, Procedure: metadata.Procedure}
	seed := nativeInviteRetryProof{Key: key, Attempt: ref, Proof: proof, Captured: captured, Flow: metadata.Write.Flow, Wire: wire}
	if metadata.Status >= 300 {
		decoded, decodeErr := decodeNativeProcedure(selected.View.Records[metadata.Procedure])
		if decodeErr != nil {
			return decodeErr
		}
		transaction, attachErr := decoded.attach(messages, true, true, false)
		if attachErr != nil {
			return attachErr
		}
		return e.udp.ArmInviteServer(nativeInviteServerRetrySeed{nativeInviteRetryProof: seed, Transaction: transaction})
	}
	if metadata.Dialog == "" || !slices.Contains(current.Dialogs, metadata.Dialog) {
		return errNativeRecord
	}
	dialog, err := decodeNativeDialog(selected.View.Records[metadata.Dialog])
	if err != nil || dialog.Metadata.Transaction != procedure.Native.Key || dialog.Refs.Remote == nil || dialog.Refs.Remote.Request != originField || dialog.Refs.Remote.Final != logicalField {
		return errNativeRecord
	}
	restored, err := dialog.attach(messages, nativeDialogSelection{Remote: nativeInviteSelection{Request: true, Final: true}})
	if err != nil {
		return err
	}
	return e.udp.ArmInviteDialog(nativeInviteDialogRetrySeed{nativeInviteRetryProof: seed, Dialog: restored})
}
