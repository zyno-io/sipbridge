package execution

import (
	"bytes"
	"testing"

	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/state"
)

func TestCommittedResultDTOIsIndependentAndBounded(t *testing.T) {
	result := &pb.OperationResult{Snapshot: &pb.ContextSnapshot{Revision: 17}, Request: &pb.SipMessage{Wire: []byte("immutable wire")}}
	projection := committedEffects(state.Ref{}, "1", state.Payload{LocalResult: result})
	if projection.LocalResult == nil {
		t.Fatal("fresh commit lost transient result")
	}
	result.Snapshot.Revision = 18
	result.Request.Wire[0] = 'X'
	if projection.LocalResult.Snapshot.Revision != 17 || string(projection.LocalResult.Request.Wire) != "immutable wire" {
		t.Fatal("result projection aliases reducer-owned values")
	}
	result.Request.Wire = bytes.Repeat([]byte{'a'}, 1<<20)
	if committedEffects(state.Ref{}, "2", state.Payload{LocalResult: result}).LocalResult != nil {
		t.Fatal("oversized DTO bypassed retained-receipt fallback")
	}
}
