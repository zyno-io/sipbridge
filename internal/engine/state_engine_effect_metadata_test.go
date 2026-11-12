package engine

import (
	"bytes"
	"testing"

	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/storagecodec"
)

func metadataFixtureEffect(t testing.TB, kind string, rejection bool) stateEngineEffect {
	t.Helper()
	sample := selectorWireCases()[0]
	message, err := Observe(sample.wire, kind == "egress")
	if err != nil {
		t.Fatal(err)
	}
	envelope := &pb.Envelope{CorrelationId: "fixture"}
	switch kind {
	case "request":
		envelope.Payload = &pb.Envelope_Request{Request: &pb.RequestPresented{Message: message}}
	case "egress":
		envelope.Payload = &pb.Envelope_Egress{Egress: &pb.EgressRequest{Message: message}}
	case "event":
		envelope.Payload = &pb.Envelope_DialogEvent{DialogEvent: &pb.DialogEvent{Message: message}}
	default:
		t.Fatal("unknown fixture arm")
	}
	return stateEngineEffect{Context: "fixture", Envelope: envelope, Wire: message.Wire, Rejection: rejection}
}

func TestEffectMetadataMaterializesOnlyAtProjectionBoundary(t *testing.T) {
	for _, kind := range []string{"request", "egress", "event"} {
		for _, rejection := range []bool{false, true} {
			t.Run(kind+map[bool]string{false: "", true: "/rejection"}[rejection], func(t *testing.T) {
				original := metadataFixtureEffect(t, kind, rejection)
				encoded, err := original.MarshalBinary()
				if err != nil {
					t.Fatal(err)
				}
				var full, metadata stateEngineEffect
				if err = full.UnmarshalBinary(encoded); err != nil {
					t.Fatal(err)
				}
				if err = metadata.unmarshalMetadata(encoded); err != nil {
					t.Fatal(err)
				}
				if metadata.Envelope.GetRequest().GetMessage() != nil || metadata.Envelope.GetEgress().GetMessage() != nil || metadata.Envelope.GetDialogEvent().GetMessage() != nil {
					t.Fatal("metadata decode allocated a public SIP projection")
				}
				stored, err := metadata.MarshalBinary()
				if err != nil || !bytes.Equal(encoded, stored) {
					t.Fatal("metadata rewrite changed retained bytes", err)
				}
				if err = metadata.materializeMessage(); err != nil {
					t.Fatal(err)
				}
				if !proto.Equal(metadata.Envelope, full.Envelope) || !bytes.Equal(metadata.Wire, full.Wire) {
					t.Fatal("deferred projection changed controller message")
				}
				// Generated decoding owns the returned data, independently of the record.
				clear(encoded)
				if !bytes.Equal(metadata.Wire, original.Wire) {
					t.Fatal("metadata aliases encoded record")
				}
			})
		}
	}
}

func TestEffectMetadataPreservesWireRefusalAndTypedArms(t *testing.T) {
	original := metadataFixtureEffect(t, "request", false)
	for _, sample := range selectorWireCases() {
		if !bytes.HasPrefix([]byte(sample.name), []byte("missing/")) && sample.name != "unsupported-branch" {
			continue
		}
		broken := original
		broken.Wire = sample.wire
		encoded, err := broken.MarshalBinary()
		if err != nil {
			t.Fatal(err)
		}
		var full, metadata stateEngineEffect
		fullErr := full.UnmarshalBinary(encoded)
		metadataErr := metadata.unmarshalMetadata(encoded)
		if fullErr == nil || metadataErr == nil || fullErr.Error() != metadataErr.Error() || metadata.Envelope != nil {
			t.Fatal("metadata decode changed corrupt-wire refusal")
		}
	}
	// Open targets and credential challenges use typed binary Wire, not SIP.
	for _, envelope := range []*pb.Envelope{nil, {Payload: &pb.Envelope_CredentialLookup{CredentialLookup: &pb.CredentialLookup{}}}} {
		value := stateEngineEffect{Envelope: envelope, Wire: []byte("non-SIP typed fixture")}
		encoded, err := storagecodec.Marshal(storagecodec.Effect, storageEncodeEngineStateEngineEffect(value))
		if err != nil {
			t.Fatal(err)
		}
		var decoded stateEngineEffect
		if err = decoded.unmarshalMetadata(encoded); err != nil {
			t.Fatal("non-SIP arm acquired SIP validation", err)
		}
	}
}

var effectMetadataBenchmarkResult stateEngineEffect

func BenchmarkEffectMetadataEncoding(b *testing.B) {
	value := stateEffectForStorage(metadataFixtureEffect(b, "request", false))
	b.ReportAllocs()
	for n := 0; n < b.N; n++ {
		if _, err := value.MarshalBinary(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkEffectMetadataProjection(b *testing.B) {
	value := metadataFixtureEffect(b, "request", false)
	encoded, err := value.MarshalBinary()
	if err != nil {
		b.Fatal(err)
	}
	for _, full := range []bool{false, true} {
		name := "metadata"
		if full {
			name = "controller"
		}
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for n := 0; n < b.N; n++ {
				var decoded stateEngineEffect
				var err error
				if full {
					err = decoded.UnmarshalBinary(encoded)
				} else {
					err = decoded.unmarshalMetadata(encoded)
				}
				if err != nil {
					b.Fatal(err)
				}
				effectMetadataBenchmarkResult = decoded
			}
		})
	}
}
