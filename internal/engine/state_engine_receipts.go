package engine

import (
	"encoding/json"

	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/state"
)

// A receipt owns its historical snapshot. Only parsed SIP projections are
// redundant with its exact wire; unlike a context's cached result, the snapshot
// must never be replaced by the current context on replay.
func marshalStateReceiptResult(result *pb.OperationResult) ([]byte, error) {
	if result.GetSnapshot().GetContext() == nil {
		// Reserve also uses Request for arbitrary outbound wire and an empty
		// Response as a known-outcome sentinel, not public SIP projections.
		return json.Marshal(result)
	}
	compact := stateResultForStorage(result)
	compact.Snapshot = result.Snapshot
	compact.ProtoReflect().SetUnknown(result.ProtoReflect().GetUnknown())
	return json.Marshal(compact)
}

func unmarshalStateReceiptResult(data []byte, result *pb.OperationResult) error {
	if len(data) > 0 && json.Unmarshal(data, result) != nil {
		return state.ErrCorrupt
	}
	if result.GetSnapshot().GetContext() != nil {
		return stateMaterializeResultMessages(result)
	}
	return nil
}

// A discarded result remains a positive commit marker. It might represent an
// acceptance or a refusal; neither the current context nor an empty payload can
// recreate the historical outcome or authorize another execution.
func projectStateReceiptResult(receipt state.Receipt, result *pb.OperationResult) error {
	if receipt.OutcomeDiscarded {
		if len(receipt.Outcome) != 0 {
			return state.ErrCorrupt
		}
		*result = pb.OperationResult{Certainty: pb.OperationResult_UNKNOWN, Error: "operation committed; result retention elapsed"}
		return nil
	}
	return unmarshalStateReceiptResult(receipt.Outcome, result)
}
