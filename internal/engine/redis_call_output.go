package engine

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"time"

	"github.com/emiago/sipgo/sip/portable"
	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/nativepb"
	"sipbridge/internal/rediscore"
	"sipbridge/internal/storagecodec"
)

type selectedCallByeOutput struct {
	capture   nativeOutputCapture
	selected  *rediscore.ObservedContext
	procedure *nativepb.Procedure
}

// Only BYE server responses use this completion path. Outgoing BYE and ACK
// have different native obligations and cannot borrow this settlement rule.
func inspectRedisCallByeOutput(ctx context.Context, store *rediscore.Store, write *pb.FlowWrite, capture *nativeOutputCapture) (selectedCallByeOutput, error) {
	var out selectedCallByeOutput
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
	metadata := capture.metadata
	if metadata == nil || !proto.Equal(metadata.Write, write) || metadata.Method != "BYE" || metadata.Status != 200 || metadata.Result != "" || !strings.HasPrefix(metadata.Procedure, "r:call-bye-") || !nativeMessageField(metadata.Dialog) || metadata.Dialog == "" {
		return out, errNativeRecord
	}
	id := strings.TrimPrefix(metadata.Procedure, "r:call-bye-")
	if len(id) != 32 || capture.proof.MetadataField != "r:call-bye-output-"+id || capture.ref.RequireApplication {
		return out, errNativeRecord
	}
	out.capture = *capture
	if capture.proof.State != rediscore.OutputWritten && capture.proof.State != rediscore.OutputNotWritten {
		return out, nil
	}
	selected, err := store.ObserveContext(ctx, capture.ref.Context, capture.ref.Incarnation, "r:context", metadata.Procedure, metadata.Dialog, capture.proof.MetadataField)
	if err != nil {
		return out, err
	}
	if !bytes.Equal(selected.View.Records[capture.proof.MetadataField], capture.proof.Metadata) {
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
	dialog, err := decodeNativeDialog(selected.View.Records[metadata.Dialog])
	if err != nil {
		return out, err
	}
	reliable := write.Flow.Transport != "udp"
	if !proto.Equal(current.Context, write.Effect.Context) || current.Kind != pb.ContextSnapshot_UAS && current.Kind != pb.ContextSnapshot_UAC ||
		dialog.Native.Version != portable.Version || dialog.Native.Phase != portable.DialogEnded || dialog.Native.CallID != current.CallId ||
		procedure.Native.Version != portable.Version || procedure.Native.Family != string(portable.NonInviteServer) || procedure.Native.Reliable != reliable ||
		procedure.Native.Phase != string(portable.Completed) && procedure.Native.Phase != string(portable.Terminated) || reliable && procedure.Native.Phase != string(portable.Terminated) ||
		procedure.Origin != "r:call-bye-origin-"+id || procedure.LastResponse != "r:call-bye-final-"+id || procedure.CommandResult != "" || procedure.DecisionPending || !proto.Equal(procedure.Flow, write.Flow) {
		return out, errors.New("BYE server completion evidence changed")
	}
	out.selected, out.procedure = selected, procedure
	return out, nil
}

func completeRedisCallByeOutput(ctx context.Context, store *rediscore.Store, write *pb.FlowWrite, result *pb.DeliveryResult) error {
	if err := (nativeRedisWriter{store: store}).Complete(ctx, write, result); err != nil {
		return err
	}
	return settleRedisCallByeOutput(ctx, store, write, nil)
}

func settleRedisCallByeOutput(ctx context.Context, store *rediscore.Store, write *pb.FlowWrite, capture *nativeOutputCapture) error {
	selected, err := inspectRedisCallByeOutput(ctx, store, write, capture)
	if err != nil {
		return err
	}
	switch selected.capture.proof.State {
	case rediscore.OutputWritten, rediscore.OutputNotWritten:
		if selected.capture.proof.Acknowledged && !selected.procedure.OutputConsumed {
			return errors.New("BYE acknowledgement lacks consumption marker")
		}
		if selected.capture.proof.Settled {
			return nil
		}
		return store.SettleOutput(ctx, selected.capture.ref, false, true)
	case rediscore.OutputStarted, rediscore.OutputUnknown:
		return nil
	default:
		return errors.New("BYE has no completion proof")
	}
}

// Only an actual dispatcher or recovered consumer calls this after return.
// An uncertain marker CAS leaves the proof pinned for exact reconciliation.
func acknowledgeRedisCallByeOutput(ctx context.Context, store *rediscore.Store, write *pb.FlowWrite, deadline time.Time) error {
	selected, err := inspectRedisCallByeOutput(ctx, store, write, nil)
	if err != nil {
		return err
	}
	proof := selected.capture.proof
	if !proof.Settled || proof.State != rediscore.OutputWritten && proof.State != rediscore.OutputNotWritten {
		return errors.New("BYE has no settled definite proof")
	}
	if !selected.procedure.OutputConsumed {
		procedure := selected.procedure
		procedure.OutputConsumed = true
		if proof.State == rediscore.OutputWritten {
			procedure.FinalResponseWrittenAcknowledged = true
		}
		encoded, e := marshalNativeRecord(storagecodec.NativeProcedure, procedure)
		if e != nil {
			return e
		}
		_, e = store.Commit(ctx, rediscore.Publication{Identity: selected.selected.Identity, Revision: selected.selected.View.Revision, Deadline: deadline,
			SpendReservation: true, Changes: []rediscore.Change{{Field: selected.capture.metadata.Procedure, Value: encoded}}})
		if e != nil {
			return e
		}
	}
	return store.SettleOutput(ctx, selected.capture.ref, true, false)
}
