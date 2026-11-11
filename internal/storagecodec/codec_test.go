package storagecodec

import (
	"bytes"
	"strings"
	"testing"

	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/storagepb"
)

func record(body []byte) []byte {
	out := append([]byte(nil), magic...)
	out = append(out, Input)
	return append(out, body...)
}
func field(number protowire.Number, value []byte) []byte {
	out := protowire.AppendTag(nil, number, protowire.BytesType)
	return protowire.AppendBytes(out, value)
}
func TestStrictWireRejectsAmbiguousAndUnboundedValues(t *testing.T) {
	oneof := append(field(13, nil), field(14, nil)...)
	duplicate := append(field(1, []byte("first")), field(1, []byte("second"))...)
	entry := append(field(1, []byte("same")), field(2, []byte("value"))...)
	duplicateMap := append(field(3, entry), field(3, entry)...)
	oversizedList := bytes.Repeat(field(6, []byte("cap")), MaxCollection+1)
	for _, sample := range []struct {
		name   string
		body   []byte
		target proto.Message
	}{
		{"unknown", field(63, nil), new(pb.ProcessRef)},
		{"duplicate", duplicate, new(pb.ProcessRef)},
		{"oneof", oneof, new(pb.Envelope)},
		{"duplicate-map-key", duplicateMap, new(pb.Address)},
		{"list-limit", oversizedList, new(pb.SessionHello)},
		{"wrong-wire-type", []byte{8, 1}, new(pb.ProcessRef)},
		{"invalid-utf8", field(1, []byte{255}), new(pb.ProcessRef)},
		{"invalid-enum", []byte{8, 127}, new(pb.RouteResult)},
		{"truncated-length", []byte{10, 255}, new(pb.ProcessRef)},
		{"invalid-bool", []byte{8, 2}, new(storagepb.StateRetention)},
	} {
		t.Run(sample.name, func(t *testing.T) {
			if err := Unmarshal(record(sample.body), Input, sample.target); err == nil {
				t.Fatal("malformed binary accepted")
			}
		})
	}
}
func TestBinaryHeaderAndOwnership(t *testing.T) {
	original := &storagepb.StatePayload{ProtocolState: []byte{0, 255, 1}, Effects: &storagepb.StatePayloadEffectsValues{Values: []*storagepb.StateEffect{{Id: "effect", Payload: []byte("wire")}}}}
	encoded, err := Marshal(Payload, original)
	if err != nil {
		t.Fatal(err)
	}
	var decoded storagepb.StatePayload
	if err := Unmarshal(encoded, Payload, &decoded); err != nil {
		t.Fatal(err)
	}
	clear(encoded)
	original.ProtocolState[0] = 99
	if !bytes.Equal(decoded.ProtocolState, []byte{0, 255, 1}) || string(decoded.Effects.Values[0].Payload) != "wire" {
		t.Fatal("decoder aliases input or source graph")
	}
	for _, data := range [][]byte{nil, []byte(`{}`), []byte(`{"protocolState":"d2lyZQ=="}`), append([]byte("SIPB\x02\x01"), 1), bytes.Repeat([]byte{0}, MaxBytes+1)} {
		if err := Unmarshal(data, Payload, new(storagepb.StatePayload)); err == nil {
			t.Fatal("unsupported record accepted")
		}
	}
	encoded, err = Marshal(Payload, original)
	if err != nil {
		t.Fatal(err)
	}
	if err := Unmarshal(encoded, Effect, new(storagepb.StatePayload)); err == nil {
		t.Fatal("record kind confusion")
	}
}
func TestCodecMetricsUseOnlyFixedLabels(t *testing.T) {
	value := &pb.ProcessRef{InstanceId: "secret-identity"}
	_, _ = Marshal(Input, value)
	var out strings.Builder
	WriteMetrics(&out)
	if strings.Contains(out.String(), "secret-identity") || !strings.Contains(out.String(), `kind="input",operation="encode"`) {
		t.Fatal("metrics leak identity or omit kind")
	}
}
func FuzzStrictRetainedWire(f *testing.F) {
	valid, _ := Marshal(Payload, &storagepb.StatePayload{ProtocolState: []byte("opaque")})
	f.Add(valid)
	f.Add([]byte("{}"))
	f.Add(record([]byte{10, 255}))
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 1<<20 {
			t.Skip()
		}
		var value storagepb.StatePayload
		_ = Unmarshal(data, Payload, &value)
	})
}
