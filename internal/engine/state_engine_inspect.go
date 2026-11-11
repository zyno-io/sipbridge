package engine

import (
	"context"
	"errors"

	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/state"
)

// InspectWrite reads historical committed outcome authority, without reserving
// an attempt or requiring its original physical process to remain alive.
func (e *StateEngine) InspectWrite(ctx context.Context, write *pb.FlowWrite) (*pb.DeliveryResult, error) {
	if write == nil || write.Effect == nil || write.Permit == nil || write.Flow == nil {
		return nil, errors.New("historical effect proof required")
	}
	ref, err := e.group(write.Effect.Context)
	if err != nil {
		return nil, err
	}
	record, err := e.store.Load(ctx, ref)
	if err != nil {
		return nil, err
	}
	result := &pb.DeliveryResult{Outcome: pb.DeliveryResult_UNKNOWN, Effect: proto.Clone(write.Effect).(*pb.StateEffectRef)}
	for _, effect := range record.Payload.Effects {
		if effect.ID != write.Effect.Id {
			continue
		}
		var body stateEngineEffect
		if body.unmarshalMetadata(effect.Payload) != nil || body.Permit == nil || body.Authorization == nil || !proto.Equal(body.Permit, write.Permit) || !proto.Equal(body.Permit.Flow, write.Flow) || !stateSameContext(body.Authorization.Effect.Context, write.Effect.Context) || body.Authorization.Effect.AttemptId != write.Effect.AttemptId || effect.AttemptID != "" && effect.AttemptID != write.Effect.AttemptId {
			return nil, errors.New("historical effect authority mismatch")
		}
		switch effect.Status {
		case state.EffectWritten:
			result.Outcome = pb.DeliveryResult_WRITTEN
			result.Recorded = true
		case state.EffectNotWritten:
			result.Outcome = pb.DeliveryResult_NOT_WRITTEN
			result.Recorded = true
		}
		return result, nil
	}
	return result, nil
}
