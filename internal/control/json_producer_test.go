package control

import (
	"bytes"
	"errors"
	"math/rand"
	"strings"
	"testing"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	pb "sipbridge/internal/controlpb"
)

// Keep the preceding producer path as an independent acceptance oracle.
func encodeJSONEnvelopeRoundTrip(envelope *pb.Envelope) ([]byte, error) {
	data, err := protojson.Marshal(envelope)
	if err != nil {
		return nil, err
	}
	if _, err := decodeJSONEnvelope(data); err != nil {
		return nil, err
	}
	return data, nil
}

func assertJSONProducerParity(t *testing.T, envelope *pb.Envelope) {
	t.Helper()
	previous, previousError := encodeJSONEnvelopeRoundTrip(envelope)
	current, currentError := encodeJSONEnvelope(envelope)
	if (previousError == nil) != (currentError == nil) {
		t.Fatalf("producer acceptance changed: previous=%v current=%v", previousError, currentError)
	}
	if currentError == nil {
		// ProtoJSON may choose insignificant whitespace independently per call.
		decoded, err := decodeJSONEnvelope(current)
		if err != nil {
			t.Fatal(err)
		}
		previousDecoded, err := decodeJSONEnvelope(previous)
		if err != nil || !proto.Equal(decoded, previousDecoded) {
			t.Fatal("message value changed")
		}
	}
}

func TestJSONProducerProfileParity(t *testing.T) {
	cases := []*pb.Envelope{
		nil,
		{},
		{Payload: &pb.Envelope_Command{}},
		{Payload: &pb.Envelope_Command{Command: &pb.Command{}}},
		{Payload: &pb.Envelope_Ping{Ping: &pb.Heartbeat{Nonce: "18446744073709551615"}}},
		{Payload: &pb.Envelope_RouteResult{RouteResult: &pb.RouteResult{Outcome: pb.RouteResult_Outcome(999)}}},
		{Payload: &pb.Envelope_Result{Result: &pb.OperationResult{Request: &pb.SipMessage{Headers: []*pb.Header{nil}, Wire: []byte{0, 255, 128}}}}},
		{CorrelationId: string([]byte{255}), Payload: &pb.Envelope_Ping{Ping: &pb.Heartbeat{}}},
		{Payload: &pb.Envelope_Ingress{Ingress: &pb.IngressReceived{Message: &pb.SipMessage{Addresses: map[string]*pb.AddressList{string([]byte{255}): {}}}}}},
		{Payload: &pb.Envelope_Ingress{Ingress: &pb.IngressReceived{Message: &pb.SipMessage{Wire: bytes.Repeat([]byte{255}, MaxEnvelope)}}}},
		{CorrelationId: strings.Repeat("x", MaxEnvelope), Payload: &pb.Envelope_Ping{Ping: &pb.Heartbeat{}}},
	}
	for i, envelope := range cases {
		t.Run(string(rune('a'+i)), func(t *testing.T) { assertJSONProducerParity(t, envelope) })
	}
	// Exercise every top-level payload even if a future additive variant is not
	// selected by the seeded corpus. The nested oneof cases retain the oracle.
	payloads := (&pb.Envelope{}).ProtoReflect().Descriptor().Oneofs().ByName("payload").Fields()
	for i := 0; i < payloads.Len(); i++ {
		envelope := &pb.Envelope{}
		message := envelope.ProtoReflect().Mutable(payloads.Get(i)).Message()
		fillJSONProducerMessage(message, rand.New(rand.NewSource(int64(i))), 0)
		assertJSONProducerParity(t, envelope)
	}
	for seed := int64(0); seed < 512; seed++ {
		envelope := &pb.Envelope{}
		fillJSONProducerMessage(envelope.ProtoReflect(), rand.New(rand.NewSource(seed)), 0)
		assertJSONProducerParity(t, envelope)
	}
}

func FuzzJSONProducerProfileParity(f *testing.F) {
	for _, seed := range []int64{0, 1, 17, 100, 511} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, seed int64) {
		envelope := &pb.Envelope{}
		fillJSONProducerMessage(envelope.ProtoReflect(), rand.New(rand.NewSource(seed)), 0)
		assertJSONProducerParity(t, envelope)
	})
}

func fillJSONProducerMessage(message protoreflect.Message, random *rand.Rand, depth int) {
	if depth >= 8 {
		return
	}
	fields := message.Descriptor().Fields()
	selected := make(map[protoreflect.FullName]int)
	oneofs := message.Descriptor().Oneofs()
	for i := 0; i < oneofs.Len(); i++ {
		oneof := oneofs.Get(i)
		selected[oneof.FullName()] = random.Intn(oneof.Fields().Len() + 1)
	}
	for i := 0; i < fields.Len(); i++ {
		field := fields.Get(i)
		if oneof := field.ContainingOneof(); oneof != nil {
			selection := selected[oneof.FullName()]
			if selection == oneof.Fields().Len() || oneof.Fields().Get(selection) != field {
				continue
			}
		} else if random.Intn(4) == 0 {
			continue
		}
		switch {
		case field.IsMap():
			entries := message.Mutable(field).Map()
			for j := 0; j < random.Intn(4); j++ {
				value := entries.NewValue()
				if field.MapValue().Kind() == protoreflect.MessageKind {
					fillJSONProducerMessage(value.Message(), random, depth+1)
				} else {
					value = jsonProducerScalarFixture(field.MapValue(), random)
				}
				entries.Set(protoreflect.ValueOfString(string(rune('a'+j))).MapKey(), value)
			}
		case field.IsList():
			entries := message.Mutable(field).List()
			for j := 0; j < random.Intn(4); j++ {
				value := entries.NewElement()
				if field.Kind() == protoreflect.MessageKind {
					fillJSONProducerMessage(value.Message(), random, depth+1)
				} else {
					value = jsonProducerScalarFixture(field, random)
				}
				entries.Append(value)
			}
		case field.Kind() == protoreflect.MessageKind:
			fillJSONProducerMessage(message.Mutable(field).Message(), random, depth+1)
		default:
			message.Set(field, jsonProducerScalarFixture(field, random))
		}
	}
}

func jsonProducerScalarFixture(field protoreflect.FieldDescriptor, random *rand.Rand) protoreflect.Value {
	switch field.Kind() {
	case protoreflect.StringKind:
		if random.Intn(32) == 0 {
			return protoreflect.ValueOfString(string([]byte{255}))
		}
		return protoreflect.ValueOfString("\x00\"\\\n\r\t\u2028é🚀")
	case protoreflect.BoolKind:
		return protoreflect.ValueOfBool(random.Intn(2) == 0)
	case protoreflect.Uint32Kind:
		return protoreflect.ValueOfUint32(random.Uint32())
	case protoreflect.Uint64Kind:
		return protoreflect.ValueOfUint64(random.Uint64())
	case protoreflect.BytesKind:
		return protoreflect.ValueOfBytes([]byte{0, 1, 255, 128, byte(random.Intn(256))})
	case protoreflect.EnumKind:
		if random.Intn(8) == 0 {
			return protoreflect.ValueOfEnum(-1)
		}
		values := field.Enum().Values()
		return protoreflect.ValueOfEnum(values.Get(random.Intn(values.Len())).Number())
	default:
		panic("uncovered control scalar kind")
	}
}

func TestJSONProducerContainerDepth(t *testing.T) {
	heartbeat := (&pb.Heartbeat{}).ProtoReflect()
	if err := validateJSONProducerMessage(heartbeat, 31); err != nil {
		t.Fatal(err)
	}
	if err := validateJSONProducerMessage(heartbeat, 32); err == nil {
		t.Fatal("32 nested containers accepted")
	}
	message := (&pb.SipMessage{Headers: []*pb.Header{{Name: "a"}}}).ProtoReflect()
	if err := validateJSONProducerMessage(message, 29); err != nil {
		t.Fatal(err)
	}
	if err := validateJSONProducerMessage(message, 30); err == nil {
		t.Fatal("repeated-message container not counted")
	}
	message = (&pb.SipMessage{Addresses: map[string]*pb.AddressList{"a": {}}}).ProtoReflect()
	if err := validateJSONProducerMessage(message, 30); err == nil {
		t.Fatal("map-message container not counted")
	}
}

func BenchmarkJSONProducer(b *testing.B) {
	for name, envelope := range map[string]*pb.Envelope{
		"ping": {Payload: &pb.Envelope_Ping{Ping: &pb.Heartbeat{Nonce: "123"}}},
		"request": {CorrelationId: "request-1", Payload: &pb.Envelope_Request{Request: &pb.RequestPresented{
			RequestId: "notify-1", Message: &pb.SipMessage{Method: "NOTIFY", Wire: bytes.Repeat([]byte("w"), 1024), Body: bytes.Repeat([]byte("b"), 512), Headers: []*pb.Header{{Name: "event", Value: "dialog"}, {Name: "subscription-state", Value: "active;expires=300"}}},
		}}},
	} {
		for mode, encode := range map[string]func(*pb.Envelope) ([]byte, error){"roundtrip": encodeJSONEnvelopeRoundTrip, "typed": encodeJSONEnvelope} {
			b.Run(name+"/"+mode, func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					if _, err := encode(envelope); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

// Preceding producer validation, retained only as an independent test oracle.
// ProtoJSON already guarantees canonical field names, scalar encodings, UTF-8
// and unique object keys. Check the stricter profile constraints directly on
// the owned producer value instead of parsing and decoding its JSON again.
func validateJSONProducerMessage(message protoreflect.Message, depth int) error {
	if depth >= 32 {
		return errors.New("JSON nesting limit")
	}
	oneofs := message.Descriptor().Oneofs()
	for i := 0; i < oneofs.Len(); i++ {
		if message.WhichOneof(oneofs.Get(i)) == nil {
			return errors.New("missing JSON variant")
		}
	}
	var validationError error
	message.Range(func(field protoreflect.FieldDescriptor, value protoreflect.Value) bool {
		switch {
		case field.IsMap():
			if depth+1 >= 32 {
				validationError = errors.New("JSON nesting limit")
				return false
			}
			value.Map().Range(func(_ protoreflect.MapKey, entry protoreflect.Value) bool {
				validationError = validateJSONProducerScalar(entry, field.MapValue(), depth+2)
				return validationError == nil
			})
		case field.IsList():
			if depth+1 >= 32 {
				validationError = errors.New("JSON nesting limit")
				return false
			}
			entries := value.List()
			for i := 0; i < entries.Len(); i++ {
				validationError = validateJSONProducerScalar(entries.Get(i), field, depth+2)
				if validationError != nil {
					break
				}
			}
		default:
			validationError = validateJSONProducerScalar(value, field, depth+1)
		}
		return validationError == nil
	})
	return validationError
}

func validateJSONProducerScalar(value protoreflect.Value, field protoreflect.FieldDescriptor, depth int) error {
	switch field.Kind() {
	case protoreflect.MessageKind:
		return validateJSONProducerMessage(value.Message(), depth)
	case protoreflect.StringKind, protoreflect.BoolKind, protoreflect.Uint64Kind, protoreflect.Uint32Kind, protoreflect.BytesKind:
		return nil
	case protoreflect.EnumKind:
		if field.Enum().Values().ByNumber(value.Enum()) != nil {
			return nil
		}
	}
	return errors.New("invalid JSON field type or value")
}
