package control

import (
	"bytes"
	"encoding/json"
	"testing"

	"unicode/utf8"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/reflect/protoreflect"
	pb "sipbridge/internal/controlpb"
)

func TestJSONAppendStringProfile(t *testing.T) {
	var controls []byte
	for c := byte(0); c < 32; c++ {
		controls = append(controls, c)
	}
	for _, value := range []string{string(controls), "\"\\/\x7fλé😀\u2028\u2029", "replacement:\ufffd", string([]byte{255})} {
		wire, err := appendJSONString(nil, value)
		previous, previousErr := json.Marshal(value)
		if value == string([]byte{255}) {
			if err == nil {
				t.Fatal("invalid UTF-8 accepted")
			}
			continue
		}
		if err != nil || previousErr != nil || !json.Valid(wire) {
			t.Fatalf("invalid JSON quote: %q %v", wire, err)
		}
		var decoded, expected string
		if err := json.Unmarshal(wire, &decoded); err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(previous, &expected); err != nil || decoded != expected {
			t.Fatalf("quoted value changed: %q", wire)
		}
		assertJSONProducerParity(t, &pb.Envelope{Payload: &pb.Envelope_Ping{Ping: &pb.Heartbeat{Nonce: value}}})
	}
}

func TestJSONAppendContainerDepth(t *testing.T) {
	for _, fixture := range []struct {
		message protoreflect.Message
		depth   int
		valid   bool
	}{
		{(&pb.Heartbeat{}).ProtoReflect(), 31, true},
		{(&pb.Heartbeat{}).ProtoReflect(), 32, false},
		{(&pb.SipMessage{Headers: []*pb.Header{{Name: "a"}}}).ProtoReflect(), 29, true},
		{(&pb.SipMessage{Headers: []*pb.Header{{Name: "a"}}}).ProtoReflect(), 30, false},
		{(&pb.SipMessage{Addresses: map[string]*pb.AddressList{"a": {}}}).ProtoReflect(), 29, true},
		{(&pb.SipMessage{Addresses: map[string]*pb.AddressList{"a": {}}}).ProtoReflect(), 30, false},
	} {
		_, err := appendJSONMessage(nil, fixture.message, fixture.depth)
		if (err == nil) != fixture.valid {
			t.Fatalf("depth %d acceptance changed: %v", fixture.depth, err)
		}
	}
}

func FuzzJSONAppendString(f *testing.F) {
	for _, seed := range []string{"plain", "\x00\x07\x0b\r\n\t\b\f\"\\", "λ😀\u2028\u2029", string([]byte{255})} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, value string) {
		wire, err := appendJSONString(nil, value)
		if !utf8.ValidString(value) {
			if err == nil {
				t.Fatal("invalid UTF-8 accepted")
			}
			return
		}
		if err != nil {
			t.Fatal(err)
		}
		var decoded string
		if err = json.Unmarshal(wire, &decoded); err != nil || decoded != value {
			t.Fatalf("JSON quoting changed: %q: %v", wire, err)
		}
	})
}

func BenchmarkJSONAppendProducer(b *testing.B) {
	request := &pb.Envelope{CorrelationId: "request-1", SessionId: "session-1", RemainingBudgetMs: 2000,
		Payload: &pb.Envelope_Request{Request: &pb.RequestPresented{RequestId: "request-1",
			Snapshot: &pb.ContextSnapshot{Context: &pb.ContextRef{Id: "context-1", Owner: &pb.ProcessRef{InstanceId: "core", Epoch: "epoch"}}, Application: &pb.ProcessRef{InstanceId: "controller", Epoch: "epoch"}, ApplicationLeaseTerm: "lease", PrincipalId: "phone", BindingId: "binding", CommandSequence: 100},
			Message:  &pb.SipMessage{Wire: bytes.Repeat([]byte("w"), 1024), Body: bytes.Repeat([]byte("b"), 512), Method: "SUBSCRIBE", Headers: []*pb.Header{{Name: "event", Value: "dialog"}}, Addresses: map[string]*pb.AddressList{"to": {Values: []*pb.Address{{Uri: "sip:1000@example.test", Params: map[string]string{"tag": "phone"}}}}}},
		}}}
	for name, encode := range map[string]func(*pb.Envelope) ([]byte, error){
		"previous": func(envelope *pb.Envelope) ([]byte, error) {
			wire, err := protojson.Marshal(envelope)
			if err == nil {
				err = validateJSONProducerMessage(envelope.ProtoReflect(), 0)
			}
			return wire, err
		},
		"append": encodeJSONEnvelope,
	} {
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if _, err := encode(request); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
