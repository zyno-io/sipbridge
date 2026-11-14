package control

import (
	"bytes"
	"testing"

	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
	pb "sipbridge/internal/controlpb"
)

func TestBinaryEnvelopeStandardCodecAndBounds(t *testing.T) {
	want := &pb.Envelope{CorrelationId: "probe", Payload: &pb.Envelope_Ping{Ping: &pb.Heartbeat{Nonce: "nonce"}}}
	wire, err := encodeEnvelope(want, MaxEnvelope)
	if err != nil {
		t.Fatal(err)
	}
	wire = protowire.AppendTag(wire, 500, protowire.VarintType)
	wire = protowire.AppendVarint(wire, 17)
	got, err := decodeEnvelope(wire, MaxEnvelope)
	if err != nil || !proto.Equal(got, want) || len(got.ProtoReflect().GetUnknown()) != 0 {
		t.Fatal("known payload or unknown-field discard changed", err)
	}
	for _, input := range [][]byte{nil, {0xff}, {0x0a, 0x80}, bytes.Repeat([]byte{1}, MaxEnvelope+1)} {
		if _, err := decodeEnvelope(input, MaxEnvelope); err == nil {
			t.Fatal("invalid or oversized protobuf accepted")
		}
	}
	if _, err := decodeEnvelope(wire, len(wire)-1); err == nil {
		t.Fatal("negotiated inbound bound ignored")
	}
	if _, err := encodeEnvelope(want, proto.Size(want)-1); err == nil {
		t.Fatal("negotiated outbound bound ignored")
	}
}

func TestBinaryEnvelopeRecursionLimit(t *testing.T) {
	value := structpb.NewNullValue()
	for i := 0; i < 40; i++ {
		value = structpb.NewListValue(&structpb.ListValue{Values: []*structpb.Value{value}})
	}
	wire, err := proto.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	if err := unmarshalControl(wire, &structpb.Value{}); err == nil {
		t.Fatal("protobuf recursion limit ignored")
	}
}
