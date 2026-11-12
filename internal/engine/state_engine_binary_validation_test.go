package engine

import (
	"bytes"
	"errors"
	"reflect"
	"testing"

	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/state"
	"sipbridge/internal/storagecodec"
)

func TestBinaryGroupRejectsSemanticErrorsBeforePublishing(t *testing.T) {
	for _, transactions := range []map[string]*stateTransaction{
		{"null": nil},
		{"policy": {Policy: &statePolicyAdmission{}}},
	} {
		value := stateGroup{Version: 1, Contexts: map[string]*stateContext{"context": {Transactions: transactions}}}
		data, err := value.MarshalBinary()
		if err != nil {
			t.Fatal(err)
		}
		decoded := stateGroup{Version: 17, Retired: map[string]int64{"unchanged": 42}}
		before := decoded
		if err := decoded.UnmarshalBinary(data); !errors.Is(err, state.ErrCorrupt) || !reflect.DeepEqual(decoded, before) {
			t.Fatal("semantic error published partial state", err)
		}
	}
}

func TestBinaryGroupDirectDecodeNormalizesRetainedResult(t *testing.T) {
	value := stateGroup{Version: 1, Contexts: map[string]*stateContext{"context": {
		Snapshot: &pb.ContextSnapshot{Answered: true},
		Result: &pb.OperationResult{Certainty: pb.OperationResult_COMPLETED,
			Snapshot: &pb.ContextSnapshot{Ended: true}, Request: &pb.SipMessage{Wire: []byte("exact wire"), Method: "INVITE", Body: []byte("projection")}},
	}}}
	// Encode the preceding representation without the current writer's result
	// projection removal. Read-side semantic normalization must still apply.
	data, err := storagecodec.Marshal(storagecodec.Group, storageEncodeEngineStateGroup(value))
	if err != nil {
		t.Fatal(err)
	}
	var decoded stateGroup
	if err := decoded.UnmarshalBinary(data); err != nil {
		t.Fatal(err)
	}
	c := decoded.Contexts["context"]
	if !c.Snapshot.Answered || c.Result.Snapshot != nil || c.Result.Request.Method != "" || len(c.Result.Request.Body) != 0 ||
		!bytes.Equal(c.Result.Request.Wire, value.Contexts["context"].Result.Request.Wire) {
		t.Fatal("direct decoder bypassed result normalization or changed current snapshot/wire authority")
	}
}
