package control

import (
	"errors"

	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
)

func decodeEnvelope(data []byte, bound int) (*pb.Envelope, error) {
	if len(data) == 0 || len(data) > bound || len(data) > MaxEnvelope {
		return nil, errors.New("invalid binary envelope bytes")
	}
	envelope := &pb.Envelope{}
	if err := unmarshalControl(data, envelope); err != nil {
		return nil, err
	}
	return envelope, nil
}

func unmarshalControl(data []byte, message proto.Message) error {
	return (proto.UnmarshalOptions{DiscardUnknown: true, RecursionLimit: 32}).Unmarshal(data, message)
}

func encodeEnvelope(envelope *pb.Envelope, bound int) ([]byte, error) {
	if envelope == nil {
		return nil, errors.New("outgoing envelope required")
	}
	size := proto.Size(envelope)
	if size == 0 || size > bound || size > MaxEnvelope {
		return nil, errors.New("outgoing envelope exceeds negotiated bound")
	}
	return proto.Marshal(envelope)
}

// These enums and the command action determine application or completion
// behavior. Protobuf may decode an unknown numeric enum or a future oneof arm;
// neither grants authority to execute the current operation.
func validEnvelopeActions(envelope *pb.Envelope) bool {
	if command := envelope.GetCommand(); command != nil && command.Action == nil {
		return false
	}
	if ingest := envelope.GetIngest(); ingest != nil {
		if _, ok := pb.OwnerIngest_Admission_name[int32(ingest.Admission)]; !ok {
			return false
		}
	}
	if decision := envelope.GetRequestDecision(); decision != nil {
		if _, ok := pb.OwnerIngest_Admission_name[int32(decision.Basis)]; !ok {
			return false
		}
	}
	if result := envelope.GetResult(); result != nil {
		if _, ok := pb.OperationResult_Certainty_name[int32(result.Certainty)]; !ok {
			return false
		}
		if proof := result.RetirementProof; proof != nil {
			if _, ok := pb.ContextRetirementProof_Reason_name[int32(proof.Reason)]; !ok {
				return false
			}
		}
	}
	if delivery := envelope.GetDelivery(); delivery != nil {
		if _, ok := pb.DeliveryResult_Outcome_name[int32(delivery.Outcome)]; !ok {
			return false
		}
	}
	if route := envelope.GetRouteResult(); route != nil {
		if _, ok := pb.RouteResult_Outcome_name[int32(route.Outcome)]; !ok {
			return false
		}
	}
	return true
}
