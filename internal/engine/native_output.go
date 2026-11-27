package engine

import (
	"context"
	"crypto/sha256"
	"errors"
	"time"

	"github.com/emiago/sipgo/sip/portable"
	"google.golang.org/protobuf/proto"
	"sipbridge/internal/capture"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/nativepb"
	"sipbridge/internal/rediscore"
	"sipbridge/internal/selectedmessage"
	"sipbridge/internal/state"
	"sipbridge/internal/storagecodec"
)

func validateNativeFlowWrite(write *pb.FlowWrite) error {
	if write == nil || write.Effect == nil || write.Effect.Context == nil || write.Effect.Context.State == nil || write.Permit == nil || write.Permit.Authorization == nil || write.Flow == nil || write.Flow.Ingress == nil || !proto.Equal(write.Effect, write.Permit.Authorization.Effect) || !proto.Equal(write.Flow, write.Permit.Flow) {
		return errNativeRecord
	}
	return nil
}
func nativeFlowWriteHash(write *pb.FlowWrite) ([32]byte, error) {
	if err := validateNativeFlowWrite(write); err != nil {
		return [32]byte{}, err
	}
	data, err := (proto.MarshalOptions{Deterministic: true}).Marshal(write)
	if err != nil {
		return [32]byte{}, err
	}
	return sha256.Sum256(data), nil
}
func nativeDeliveryTag(delivery *pb.ContextDelivery) ([32]byte, error) {
	h := sha256.New()
	_, _ = h.Write([]byte("sipbridge.native.delivery.v1\x00"))
	if delivery == nil {
		_, _ = h.Write([]byte{0})
	} else {
		_, _ = h.Write([]byte{1})
		data, err := (proto.MarshalOptions{Deterministic: true}).Marshal(delivery)
		if err != nil {
			return [32]byte{}, err
		}
		_, _ = h.Write(data)
	}
	var sum [32]byte
	h.Sum(sum[:0])
	return sum, nil
}
func encodeNativeOutput(v *nativepb.Output) ([]byte, error) {
	if v == nil || !nativeMessageField(v.Procedure) || !nativeMessageField(v.Result) || !nativeMessageField(v.Dialog) {
		return nil, errNativeRecord
	}
	if err := validateNativeFlowWrite(v.Write); err != nil {
		return nil, err
	}
	if v.Drain != nil && (len(v.Drain.Transaction) != 32 || len(v.Drain.Ack) != 32 || v.Drain.Status > 65535) {
		return nil, errNativeRecord
	}
	data, err := storagecodec.Marshal(storagecodec.NativeOutput, v)
	if err != nil {
		return nil, err
	}
	if len(data) > storagecodec.MaxNativeOutputBytes {
		return nil, errNativeRecord
	}
	return data, nil
}
func decodeNativeOutput(data []byte) (*nativepb.Output, error) {
	if len(data) > storagecodec.MaxNativeOutputBytes {
		return nil, errNativeRecord
	}
	v := new(nativepb.Output)
	if err := storagecodec.Unmarshal(data, storagecodec.NativeOutput, v); err != nil {
		return nil, err
	}
	if !nativeMessageField(v.Procedure) || !nativeMessageField(v.Result) || !nativeMessageField(v.Dialog) {
		return nil, errNativeRecord
	}
	if err := validateNativeFlowWrite(v.Write); err != nil {
		return nil, err
	}
	if v.Drain != nil && (len(v.Drain.Transaction) != 32 || len(v.Drain.Ack) != 32 || v.Drain.Status > 65535) {
		return nil, errNativeRecord
	}
	return v, nil
}

// nativeDeliveryTagChange must accompany the matching context delivery update.
func nativeDeliveryTagChange(delivery *pb.ContextDelivery) (rediscore.Change, error) {
	tag, err := nativeDeliveryTag(delivery)
	if err != nil {
		return rediscore.Change{}, err
	}
	return rediscore.Change{Field: "r:delivery-tag", Value: tag[:]}, nil
}

// nativePreparePhysicalOutput produces only the two selected record changes and
// exact physical intent; the caller atomically publishes them with native state.
func nativePreparePhysicalOutput(metadata *nativepb.Output, message portable.Message, messageField, metadataField string, application *rediscore.ApplicationLeaseRef, delivery *pb.ContextDelivery) (rediscore.OutputIntent, []rediscore.Change, error) {
	return nativePreparePhysicalOutputMode(metadata, message, messageField, metadataField, application, delivery, false)
}

// The selected wire already exists under this exact field; Redis still
// atomically validates and pins it when the new physical attempt is admitted.
func nativePrepareExistingPhysicalOutput(metadata *nativepb.Output, message portable.Message, messageField, metadataField string) (rediscore.OutputIntent, []rediscore.Change, error) {
	return nativePreparePhysicalOutputMode(metadata, message, messageField, metadataField, nil, nil, true)
}

func nativePreparePhysicalOutputMode(metadata *nativepb.Output, message portable.Message, messageField, metadataField string, application *rediscore.ApplicationLeaseRef, delivery *pb.ContextDelivery, existing bool) (rediscore.OutputIntent, []rediscore.Change, error) {
	if metadata == nil || !nativeMessageField(messageField) || !nativeMessageField(metadataField) || messageField == "" || metadataField == "" || messageField == metadataField || messageField == "r:delivery-tag" || metadataField == "r:delivery-tag" || message.WireLen() == 0 || message.WireLen() > storagecodec.MaxSelectedMessageFieldBytes || len(message.Transport()) > storagecodec.MaxSelectedMessageFieldBytes || len(message.Source()) > storagecodec.MaxSelectedMessageFieldBytes || len(message.Destination()) > storagecodec.MaxSelectedMessageFieldBytes {
		return rediscore.OutputIntent{}, nil, errNativeRecord
	}
	proof, err := nativeFlowWriteHash(metadata.Write)
	if err != nil {
		return rediscore.OutputIntent{}, nil, err
	}
	encoded, err := encodeNativeOutput(metadata)
	if err != nil {
		return rediscore.OutputIntent{}, nil, err
	}
	write := metadata.Write
	worker, err := nativeWorkerReference(write.Flow.Ingress)
	if err != nil {
		return rediscore.OutputIntent{}, nil, err
	}
	auth := write.Permit.Authorization
	if auth.DeadlineMs == 0 || auth.DeadlineMs > 1<<53-1 || message.Transport() != write.Flow.Transport {
		return rediscore.OutputIntent{}, nil, errNativeRecord
	}
	intent := rediscore.OutputIntent{ID: write.Effect.Id, Attempt: write.Effect.AttemptId, Worker: worker, Epoch: write.Flow.Ingress.Epoch, Connection: write.Flow.ConnectionId, MessageField: messageField, MetadataField: metadataField, WireHash: message.WireHash(), ProofHash: proof, Deadline: time.UnixMilli(int64(auth.DeadlineMs))}
	for _, field := range []string{metadata.Procedure, metadata.Result, metadata.Dialog} {
		if field == "" {
			continue
		}
		if field == messageField || field == metadataField {
			return rediscore.OutputIntent{}, nil, errNativeRecord
		}
		duplicate := false
		for _, previous := range intent.Dependencies {
			if previous == field {
				duplicate = true
				break
			}
		}
		if !duplicate {
			intent.Dependencies = append(intent.Dependencies, field)
		}
	}
	changes := make([]rediscore.Change, 0, 2)
	if !existing {
		wire, e := selectedmessage.Marshal(message)
		if e != nil {
			return rediscore.OutputIntent{}, nil, e
		}
		changes = append(changes, rediscore.Change{Field: messageField, Value: wire})
	}
	changes = append(changes, rediscore.Change{Field: metadataField, Value: encoded})
	if auth.AuthorityClass == pb.EffectAuthorization_APPLICATION {
		if application == nil || application.Term != auth.ApplicationLeaseTerm {
			return rediscore.OutputIntent{}, nil, errNativeRecord
		}
		tag, e := nativeDeliveryTag(delivery)
		if e != nil {
			return rediscore.OutputIntent{}, nil, e
		}
		intent.Application = application
		intent.DeliveryTag = tag
	} else {
		if application != nil || delivery != nil {
			return rediscore.OutputIntent{}, nil, errNativeRecord
		}
		switch auth.AuthorityClass {
		case pb.EffectAuthorization_NATIVE_COMPLETION, pb.EffectAuthorization_TERMINAL_CLEANUP, pb.EffectAuthorization_RESPONSE:
		default:
			return rediscore.OutputIntent{}, nil, errNativeRecord
		}
	}
	return intent, changes, nil
}
func nativeOutputRef(write *pb.FlowWrite) (rediscore.OutputRef, error) {
	proof, err := nativeFlowWriteHash(write)
	if err != nil {
		return rediscore.OutputRef{}, err
	}
	worker, err := nativeWorkerReference(write.Flow.Ingress)
	if err != nil {
		return rediscore.OutputRef{}, err
	}
	app := false
	switch write.Permit.Authorization.AuthorityClass {
	case pb.EffectAuthorization_APPLICATION:
		app = true
	case pb.EffectAuthorization_NATIVE_COMPLETION, pb.EffectAuthorization_TERMINAL_CLEANUP, pb.EffectAuthorization_RESPONSE:
	default:
		return rediscore.OutputRef{}, errNativeRecord
	}
	return rediscore.OutputRef{Context: write.Effect.Context.Id, Incarnation: write.Effect.Context.State.Incarnation, ID: write.Effect.Id, Attempt: write.Effect.AttemptId, Worker: worker, Epoch: write.Flow.Ingress.Epoch, Connection: write.Flow.ConnectionId, ProofHash: proof, RequireApplication: app}, nil
}

type nativeRedisWriter struct{ store *rediscore.Store }

func (w nativeRedisWriter) Reserve(ctx context.Context, write *pb.FlowWrite) (*CommittedWrite, error) {
	if w.store == nil {
		return nil, errNativeRecord
	}
	ref, err := nativeOutputRef(write)
	if err != nil {
		return nil, err
	}
	result, err := w.store.ReserveOutput(ctx, ref)
	if errors.Is(err, rediscore.ErrUnknown) || errors.Is(err, rediscore.ErrAbsent) {
		return &CommittedWrite{Known: true, Outcome: pb.DeliveryResult_UNKNOWN}, nil
	}
	if err != nil {
		return nil, err
	}
	if result.Outcome != "" {
		outcome := pb.DeliveryResult_UNKNOWN
		if result.Outcome == "WRITTEN" {
			outcome = pb.DeliveryResult_WRITTEN
		} else if result.Outcome == "NOT_WRITTEN" {
			outcome = pb.DeliveryResult_NOT_WRITTEN
		}
		return &CommittedWrite{Known: true, Outcome: outcome}, nil
	}
	metadata, err := decodeNativeOutput(result.Metadata)
	if err != nil {
		return &CommittedWrite{Known: true, Outcome: pb.DeliveryResult_UNKNOWN}, nil
	}
	if !proto.Equal(metadata.Write, write) {
		return &CommittedWrite{Known: true, Outcome: pb.DeliveryResult_UNKNOWN}, nil
	}
	var drain *state.LocalDrainObservation
	if metadata.Drain != nil {
		d := metadata.Drain
		drain = &state.LocalDrainObservation{Status: uint16(d.Status), Request: d.Request, Invite: d.Invite, Ack: d.IsAck, Cancel: d.Cancel, Initial: d.Initial}
		copy(drain.Transaction[:], d.Transaction)
		copy(drain.ACK[:], d.Ack)
	}
	var attribution capture.Attribution
	if current, decodeErr := decodeNativeContext(result.ContextRecord); decodeErr == nil {
		attribution.NativeCallID = current.NativeCallId
	}
	return &CommittedWrite{Wire: result.Wire, Until: result.Until, Drain: drain, attachment: result.Attachment, Attribution: attribution}, nil
}
func (w nativeRedisWriter) Complete(ctx context.Context, write *pb.FlowWrite, result *pb.DeliveryResult) error {
	if w.store == nil || result == nil || !proto.Equal(result.Effect, write.GetEffect()) {
		return errNativeRecord
	}
	ref, err := nativeOutputRef(write)
	if err != nil {
		return err
	}
	outcome := "UNKNOWN"
	switch result.Outcome {
	case pb.DeliveryResult_WRITTEN:
		outcome = "WRITTEN"
	case pb.DeliveryResult_NOT_WRITTEN:
		outcome = "NOT_WRITTEN"
	}
	return w.store.CompleteOutput(ctx, ref, outcome)
}

// nativeReadyWrites exposes only verified authorization metadata. It does not
// reserve an attempt or read its selected wire record.
func nativeReadyWrites(ctx context.Context, store *rediscore.Store, worker, epoch string, limit int) ([]*pb.FlowWrite, error) {
	if store == nil {
		return nil, errNativeRecord
	}
	ready, err := store.PeekReady(ctx, worker, epoch, limit)
	if err != nil {
		return nil, err
	}
	out := make([]*pb.FlowWrite, 0, len(ready))
	for _, entry := range ready {
		metadata, e := decodeNativeOutput(entry.Metadata)
		if e != nil {
			return nil, e
		}
		ref, e := nativeOutputRef(metadata.Write)
		if e != nil || ref != entry.Ref || metadata.Write.Permit.Authorization.DeadlineMs != uint64(entry.DeadlineMS) {
			return nil, errNativeRecord
		}
		out = append(out, metadata.Write)
	}
	return out, nil
}
