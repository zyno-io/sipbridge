package engine

import (
	"encoding"
	"encoding/json"
	"google.golang.org/protobuf/proto"
	"testing"

	pb "sipbridge/internal/controlpb"
)

// Fixture builders encode private retained roots through the production binary
// codecs. JSON remains available only for public receipt/debug comparisons and
// deliberately opaque generic-store payloads; this is not a runtime fallback.
func marshalRetainedFixture(value interface{}) ([]byte, error) {
	if typed, ok := value.(encoding.BinaryMarshaler); ok {
		return typed.MarshalBinary()
	}
	if tx, ok := value.(*stateTransaction); ok {
		return proto.MarshalOptions{Deterministic: true}.Marshal(storageEncodeEngineStateTransaction(*tx))
	}
	if open, ok := value.(*pb.OpenUac); ok {
		return stateEncodeNetworkTarget(open)
	}
	return json.Marshal(value)
}
func unmarshalRetainedFixture(data []byte, value interface{}) error {
	if typed, ok := value.(encoding.BinaryUnmarshaler); ok {
		return typed.UnmarshalBinary(data)
	}
	if open, ok := value.(*pb.OpenUac); ok {
		return stateDecodeNetworkTarget(data, open)
	}
	return json.Unmarshal(data, value)
}

func mustRetainedEffect(t testing.TB, value stateEngineEffect) []byte {
	t.Helper()
	data, err := value.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	return data
}
