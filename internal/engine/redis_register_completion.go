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
	"sipbridge/internal/state"
	"sipbridge/internal/storagecodec"
)

// selectedRegisterOutput captures only the records required to settle one
// exact physical REGISTER result. Observation itself grants no write authority.
type selectedRegisterOutput struct {
	ref        rediscore.OutputRef
	inspection rediscore.OutputInspection
	selected   *rediscore.ObservedContext
	current    *nativepb.Context
	procedure  *nativepb.Procedure
	status     uint32
	method     string
}

type nativeOutputCapture struct {
	ref      rediscore.OutputRef
	proof    rediscore.OutputInspection
	metadata *nativepb.Output
}

func inspectRedisRegisterOutput(ctx context.Context, store *rediscore.Store, write *pb.FlowWrite, capture ...*nativeOutputCapture) (selectedRegisterOutput, error) {
	var out selectedRegisterOutput
	if store == nil {
		return out, errNativeRecord
	}
	var ref rediscore.OutputRef
	var inspection rediscore.OutputInspection
	var metadata *nativepb.Output
	var err error
	if len(capture) > 1 {
		return out, errNativeRecord
	}
	if len(capture) == 1 {
		if capture[0] == nil || capture[0].metadata == nil || !proto.Equal(capture[0].metadata.Write, write) {
			return out, errNativeRecord
		}
		ref, inspection, metadata = capture[0].ref, capture[0].proof, capture[0].metadata
	} else {
		ref, err = nativeOutputRef(write)
		if err != nil {
			return out, err
		}
		inspection, err = store.InspectOutput(ctx, ref)
		if err != nil {
			return out, err
		}
	}
	if inspection.State == rediscore.OutputPending || inspection.State == rediscore.OutputStarted || inspection.State == rediscore.OutputUnknown {
		return selectedRegisterOutput{ref: ref, inspection: inspection}, nil
	}
	if metadata == nil {
		metadata, err = decodeNativeOutput(inspection.Metadata)
		if err != nil {
			return out, err
		}
	}
	if !proto.Equal(metadata.Write, write) || metadata.Status < 200 || metadata.Status > 699 || !strings.HasPrefix(metadata.Procedure, "r:procedure-") {
		return out, errors.New("invalid REGISTER output proof")
	}
	requestID := strings.TrimPrefix(metadata.Procedure, "r:procedure-")
	if !state.ValidReference(requestID) || len(requestID) > 180 {
		return out, errNativeRecord
	}
	originField, responseField := "r:origin-"+requestID, "r:response-"+requestID
	fields := []string{"r:context", metadata.Procedure, inspection.MetadataField}
	if metadata.Result != "" {
		fields = append(fields, metadata.Result)
	}
	selected, err := store.ObserveContext(ctx, ref.Context, ref.Incarnation, fields...)
	if err != nil {
		return out, err
	}
	if !bytes.Equal(selected.View.Records[inspection.MetadataField], inspection.Metadata) {
		return out, errors.New("REGISTER physical attempt changed")
	}
	current, err := decodeNativeContext(selected.View.Records["r:context"])
	if err != nil {
		return out, err
	}
	if !nativeRegisterOrRefusalOutput(current, metadata) {
		return out, errNativeRecord
	}
	procedure, err := readPrivateProcedureRecord(selected.View.Records[metadata.Procedure])
	if err != nil {
		return out, err
	}
	reliable := write.Flow.Transport != "udp"
	if current.Context == nil || !proto.Equal(current.Context, write.Effect.Context) || current.Kind != pb.ContextSnapshot_TRANSACTION || procedure.RequestId != requestID || procedure.Origin != originField || procedure.LastResponse != responseField || procedure.DecisionPending || !proto.Equal(procedure.Flow, write.Flow) || procedure.Native.Version != portable.Version || procedure.Native.Family != string(portable.NonInviteServer) || procedure.Native.Reliable != reliable || procedure.Native.ResponseTag != current.LocalTag || (procedure.Native.Phase != string(portable.Completed) && procedure.Native.Phase != string(portable.Terminated)) || reliable && procedure.Native.Phase != string(portable.Terminated) {
		return out, errors.New("REGISTER completion context changed")
	}
	if metadata.Result != "" {
		result, e := decodeNativeResult(selected.View.Records[metadata.Result])
		if e != nil || result.Certainty != pb.OperationResult_ACCEPTED || result.SipStatus != metadata.Status || result.OperationId != metadata.Operation || procedure.CommandResult != metadata.Result {
			return out, errors.New("REGISTER result evidence changed")
		}
	}
	return selectedRegisterOutput{ref: ref, inspection: inspection, selected: selected, current: current, procedure: procedure, status: metadata.Status, method: metadata.Method}, nil
}

// completeRedisRegisterOutput records the physical outcome first. A definite
// output is marked native-settled only after its selected transaction and result
// are checked. UNKNOWN retains the exact proof and never authorizes a retry.
func completeRedisRegisterOutput(ctx context.Context, store *rediscore.Store, write *pb.FlowWrite, result *pb.DeliveryResult) error {
	if err := (nativeRedisWriter{store: store}).Complete(ctx, write, result); err != nil {
		return err
	}
	return settleRedisRegisterOutput(ctx, store, write, nil)
}

func settleRedisRegisterOutput(ctx context.Context, store *rediscore.Store, write *pb.FlowWrite, capture *nativeOutputCapture) error {
	var selected selectedRegisterOutput
	var err error
	if capture == nil {
		selected, err = inspectRedisRegisterOutput(ctx, store, write)
	} else {
		selected, err = inspectRedisRegisterOutput(ctx, store, write, capture)
	}
	if err != nil {
		return err
	}
	switch selected.inspection.State {
	case rediscore.OutputWritten, rediscore.OutputNotWritten:
		if selected.inspection.Settled {
			return nil
		}
		if selected.inspection.Acknowledged && !selected.procedure.FinalResponseWrittenAcknowledged {
			return errors.New("REGISTER acknowledgement lacks native proof")
		}
		return store.SettleOutput(ctx, selected.ref, false, true)
	case rediscore.OutputUnknown, rediscore.OutputStarted:
		return nil
	default:
		return errors.New("REGISTER output has no completion proof")
	}
}

// acknowledgeRedisRegisterOutput is called only after the dispatcher actually
// returned for this exact write. A reliable written final persists its native
// proof before releasing physical metadata.
func acknowledgeRedisRegisterOutput(ctx context.Context, store *rediscore.Store, write *pb.FlowWrite, deadline time.Time) error {
	selected, err := inspectRedisRegisterOutput(ctx, store, write)
	if err != nil {
		return err
	}
	if !selected.inspection.Settled || (selected.inspection.State != rediscore.OutputWritten && selected.inspection.State != rediscore.OutputNotWritten) {
		return errors.New("REGISTER output has no settled definite proof")
	}
	reliableWritten := selected.inspection.State == rediscore.OutputWritten && nativeReliableRegisterOrRefusalMayRetire(selected.current, selected.method, selected.status, write.Flow.Transport)
	if selected.inspection.State == rediscore.OutputNotWritten {
		// Keep exact N evidence while UDP Timer J is active. The J consumer
		// can acknowledge it after the native procedure terminates.
		if selected.procedure.Native.Phase != string(portable.Terminated) {
			return nil
		}
		if !selected.current.Ended {
			if selected.current.Revision >= 1<<53-1 {
				return errNativeRecord
			}
			current := proto.Clone(selected.current).(*nativepb.Context)
			current.Ended = true
			current.Revision++
			encoded, e := encodeNativeContext(current)
			if e != nil {
				return e
			}
			_, e = store.Commit(ctx, rediscore.Publication{Identity: selected.selected.Identity, Revision: selected.selected.View.Revision, Deadline: deadline, SpendReservation: true, Changes: []rediscore.Change{{Field: "r:context", Value: encoded}}})
			if e != nil {
				return e
			}
		}
		if err = store.SettleOutput(ctx, selected.ref, true, false); err != nil {
			return err
		}
		err = retireRedisRegister(ctx, store, write, selected.inspection.MetadataField, deadline, true, false)
		if errors.Is(err, rediscore.ErrConflict) || errors.Is(err, rediscore.ErrAbsent) {
			return nil
		}
		return err
	}
	if selected.inspection.State == rediscore.OutputWritten && !selected.procedure.FinalResponseWrittenAcknowledged {
		metadata := selected.procedure
		metadata.FinalResponseWrittenAcknowledged = true
		encoded, e := marshalNativeRecord(storagecodec.NativeProcedure, metadata)
		if e != nil {
			return e
		}
		_, e = store.Commit(ctx, rediscore.Publication{Identity: selected.selected.Identity, Revision: selected.selected.View.Revision, Deadline: deadline, SpendReservation: true, Changes: []rediscore.Change{{Field: "r:procedure-" + metadata.RequestId, Value: encoded}}})
		if e != nil {
			return e
		}
	}
	if err = store.SettleOutput(ctx, selected.ref, true, false); err != nil {
		return err
	}
	if !reliableWritten && (selected.inspection.State != rediscore.OutputWritten || write.Flow.Transport != "udp" || selected.procedure.Native.Phase != string(portable.Terminated)) {
		return nil
	}
	// Additional retained records or deadlines fence terminal retirement and
	// leave the indexed output cutoff to re-drive it later.
	if write.Flow.Transport == "udp" {
		err = retireRedisRegisterPostJ(ctx, store, write, selected.inspection.MetadataField, deadline)
	} else {
		err = retireRedisRegisterWritten(ctx, store, write, selected.inspection.MetadataField, deadline)
	}
	if errors.Is(err, rediscore.ErrConflict) || errors.Is(err, rediscore.ErrAbsent) {
		return nil
	}
	return err
}
