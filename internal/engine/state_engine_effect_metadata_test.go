package engine

import (
	"bytes"
	"testing"

	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/state"
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

func TestEffectMetadataReusesObservedMessageAndPreservesWeakValidation(t *testing.T) {
	valid := selectorWireCases()[0].wire
	for name, wire := range map[string][]byte{
		"method-mismatch": bytes.ReplaceAll(valid, []byte("CSeq: 7 INVITE"), []byte("CSeq: 7 BYE")),
	} {
		t.Run(name, func(t *testing.T) {
			expected, err := Observe(wire, false)
			if err != nil {
				t.Fatal(err)
			}
			value := metadataFixtureEffect(t, "request", false)
			value.Wire = wire
			encoded, err := value.MarshalBinary()
			if err != nil {
				t.Fatal(err)
			}
			effect := state.Effect{ID: "same-effect", Payload: encoded}
			r := new(stateReduction)
			var first, second stateEngineEffect
			if err := r.unmarshalEffectMetadata(&first, effect); err != nil {
				t.Fatal(err)
			}
			if err := r.unmarshalEffectMetadata(&second, effect); err != nil || first.prepared != second.prepared {
				t.Fatal("repeated metadata scan did not reuse its immutable message", err)
			}
			if err := second.materializeMessage(); err != nil || !proto.Equal(expected, second.Envelope.GetRequest().GetMessage()) {
				t.Fatalf("observed projection changed: %v\nexpected: %v\ngot: %v", err, expected, second.Envelope.GetRequest().GetMessage())
			}
			if _, err := second.prepared.Facts(); err == nil {
				t.Fatal("observed validation bypassed strict portable validation")
			}
			value.Wire = valid
			effect.Payload, err = value.MarshalBinary()
			if err != nil {
				t.Fatal(err)
			}
			var changed stateEngineEffect
			if err := r.unmarshalEffectMetadata(&changed, effect); err != nil || changed.prepared == first.prepared || !changed.prepared.EqualWireBytes(valid) {
				t.Fatal("changed effect wire reused stale facts", err)
			}
		})
	}
}

func TestPhysicalEffectLazyMessageClassification(t *testing.T) {
	_, _, tx := forkFixture(t)
	valid := forkReply(t, tx, 200, "remote")
	for _, sample := range []struct {
		name   string
		wire   []byte
		final  bool
		opaque bool
	}{
		{"valid", valid.WireBytes(), true, false},
		{"missing mandatory identity", []byte("SIP/2.0 200 OK\r\nContent-Length: 0\r\n\r\n"), false, true},
		{"invalid wire", []byte("not SIP"), false, true},
	} {
		t.Run(sample.name, func(t *testing.T) {
			value := stateEngineEffect{Wire: sample.wire}
			encoded, err := value.MarshalBinary()
			if err != nil {
				t.Fatal(err)
			}
			effect := state.Effect{ID: "physical", Kind: "deliver", Payload: encoded}
			r := new(stateReduction)
			var first, second stateEngineEffect
			if err := r.unmarshalEffectMetadata(&first, effect); err != nil {
				t.Fatal("unrelated metadata read eagerly validated physical wire", err)
			}
			if first.prepared.IsZero() {
				t.Fatal("physical wire has no lazy immutable holder")
			}
			final, opaque := stateResponseClass(first)
			if final != sample.final || opaque != sample.opaque {
				t.Fatalf("classification changed: final=%t opaque=%t", final, opaque)
			}
			if err := r.unmarshalEffectMetadata(&second, effect); err != nil || second.prepared != first.prepared {
				t.Fatal("repeated physical scan did not reuse its holder", err)
			}
			final, opaque = stateResponseClass(second)
			if final != sample.final || opaque != sample.opaque {
				t.Fatal("repeated classification changed", final, opaque)
			}
		})
	}
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
