package control

import (
	"bytes"
	"encoding/json"
	"math/rand"
	"os"
	"strings"
	"testing"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	pb "sipbridge/internal/controlpb"
)

func assertJSONStreamParity(t *testing.T, data []byte) bool {
	t.Helper()
	previous, previousError := decodeJSONEnvelopeTree(data)
	current, currentError := decodeJSONEnvelope(data)
	if (previousError == nil) != (currentError == nil) {
		t.Fatalf("acceptance changed for %q: tree=%v stream=%v", data, previousError, currentError)
	}
	if currentError == nil && !proto.Equal(previous, current) {
		t.Fatalf("decoded value changed for %q", data)
	}
	return currentError == nil
}

func TestJSONStreamSharedCorpusParity(t *testing.T) {
	data, err := os.ReadFile("testdata/json-cases.json")
	if err != nil {
		t.Fatal(err)
	}
	var cases []struct {
		Name string `json:"name"`
		JSON string `json:"json"`
	}
	if err := json.Unmarshal(data, &cases); err != nil {
		t.Fatal(err)
	}
	for _, fixture := range cases {
		t.Run(fixture.Name, func(t *testing.T) { assertJSONStreamParity(t, []byte(fixture.JSON)) })
	}
}

func TestJSONStreamMalformedAndUnknownParity(t *testing.T) {
	cases := []struct {
		name, wire string
		valid      bool
	}{
		{"empty", ``, false},
		{"null envelope", `null`, false},
		{"array envelope", `[]`, false},
		{"trailing value", `{"ping":{}} {}`, false},
		{"missing variant", `{"futureOperation":{}}`, false},
		{"missing nested variant", `{"command":{"futureAction":{}}}`, false},
		{"multiple variants", `{"ping":{},"pong":{}}`, false},
		{"multiple nested variants", `{"command":{"cancel":{},"respond":{}}}`, false},
		{"duplicate escaped field", `{"ping":{},"p\u0069ng":{}}`, false},
		{"duplicate nested field", `{"ping":{"nonce":"a","n\u006fnce":"b"}}`, false},
		{"duplicate unknown field", `{"ping":{},"future":{"a":1,"\u0061":2}}`, false},
		{"duplicate unknown array object", `{"ping":{},"future":[{"a":1,"a":2}]}`, false},
		{"duplicate message map key", `{"ingress":{"message":{"addresses":{"to":{},"t\u006f":{}}}}}`, false},
		{"duplicate scalar map key", `{"ingress":{"message":{"addresses":{"to":{"values":[{"params":{"tag":"a","t\u0061g":"b"}}]}}}}}`, false},
		{"unknown scalars", `{"ping":{},"future":[null,true,1e400,"text"]}`, true},
		{"unknown nested values", `{"ping":{"future":{"x":[{},[null,false],{"y":3}]}},"future_operation":{}}`, true},
		{"unknown fields in map message", `{"ingress":{"message":{"addresses":{"to":{"future":{"tag":null},"values":[]}}}}}`, true},
		{"noncanonical field", `{"ping":{},"remaining_budget_ms":1}`, false},
		{"noncanonical nested field", `{"hello":{"connection_attempt":"1"}}`, false},
		{"null known message", `{"ping":null}`, false},
		{"null known scalar", `{"ping":{"nonce":null}}`, false},
		{"null known repeated", `{"ingress":{"message":{"headers":null}}}`, false},
		{"null repeated item", `{"ingress":{"message":{"headers":[null]}}}`, false},
		{"null known map", `{"ingress":{"message":{"addresses":null}}}`, false},
		{"null map message", `{"ingress":{"message":{"addresses":{"to":null}}}}`, false},
		{"wrong string", `{"ping":{"nonce":1}}`, false},
		{"wrong boolean", `{"result":{"snapshot":{"answered":"true"}}}`, false},
		{"boolean", `{"result":{"snapshot":{"answered":true}}}`, true},
		{"uint64 maximum", `{"command":{"sequence":"18446744073709551615","cancel":{}}}`, true},
		{"uint64 overflow", `{"command":{"sequence":"18446744073709551616","cancel":{}}}`, false},
		{"uint64 number", `{"command":{"sequence":1,"cancel":{}}}`, false},
		{"uint64 leading zero", `{"command":{"sequence":"01","cancel":{}}}`, false},
		{"uint64 exponent string", `{"command":{"sequence":"1e2","cancel":{}}}`, false},
		{"uint64 negative string", `{"command":{"sequence":"-1","cancel":{}}}`, false},
		{"uint32 maximum", `{"hello":{"major":4294967295}}`, true},
		{"uint32 overflow", `{"hello":{"major":4294967296}}`, false},
		{"uint32 string", `{"hello":{"major":"1"}}`, false},
		{"uint32 fraction", `{"hello":{"major":1.5}}`, false},
		{"uint32 integral exponent", `{"hello":{"major":1e0}}`, true},
		{"known enum", `{"routeResult":{"outcome":"ACCEPTED"}}`, true},
		{"unknown enum", `{"routeResult":{"outcome":"FUTURE"}}`, false},
		{"numeric enum", `{"routeResult":{"outcome":1}}`, false},
		{"padded base64", `{"ingest":{"wire":"AQ=="}}`, true},
		{"base64 pad bits", `{"ingest":{"wire":"AR=="}}`, false},
		{"unpadded base64", `{"ingest":{"wire":"AQ"}}`, false},
		{"base64 URL alphabet", `{"ingest":{"wire":"-w=="}}`, false},
		{"base64 newline", `{"ingest":{"wire":"AQ==\n"}}`, false},
		{"wrong object shape", `{"ingress":{"message":{"headers":{}}}}`, false},
		{"wrong array shape", `{"ingress":{"message":{"addresses":[]}}}`, false},
		{"missing value", `{"ping":{"nonce":}}`, false},
		{"wrong closing token", `{"ping":[]]}`, false},
		{"unpaired surrogate", `{"ping":{"nonce":"\ud800"}}`, false},
	}
	for _, fixture := range cases {
		t.Run(fixture.name, func(t *testing.T) {
			if valid := assertJSONStreamParity(t, []byte(fixture.wire)); valid != fixture.valid {
				t.Fatalf("unexpected fixture acceptance: %v", valid)
			}
		})
	}
	assertJSONStreamParity(t, []byte("{\"ping\":{\"nonce\":\"\xff\"}}"))
	assertJSONStreamParity(t, bytes.Repeat([]byte(" "), MaxEnvelope+1))
}

func TestJSONStreamContainerDepthParity(t *testing.T) {
	for depth := 0; depth <= 34; depth++ {
		wire := `{"ping":{},"future":` + strings.Repeat("[", depth) + "0" + strings.Repeat("]", depth) + "}"
		if valid := assertJSONStreamParity(t, []byte(wire)); valid != (depth <= 31) {
			t.Fatalf("unknown array depth %d: valid=%v", depth, valid)
		}
		wire = `{"ingress":{"message":{"addresses":{"x":{"values":[{"future":` + strings.Repeat("[", depth) + "0" + strings.Repeat("]", depth) + `}]}}}}}`
		if valid := assertJSONStreamParity(t, []byte(wire)); valid != (depth <= 25) {
			t.Fatalf("map/list/message depth %d: valid=%v", depth, valid)
		}
	}
	for _, wire := range []string{
		`{"ping":{},"future":[{"x":[null,true,"end"]}]}`,
		`{"ingress":{"message":{"headers":[{"name":"x","value":"value"}]}}}`,
	} {
		for end := 0; end <= len(wire); end++ {
			assertJSONStreamParity(t, []byte(wire[:end]))
		}
	}
}

// Generate independently owned protobuf fixtures, then normalize only fixture
// scalars/required variants so every seed also exercises successful decoding.
func normalizeJSONStreamFixture(message protoreflect.Message) {
	oneofs := message.Descriptor().Oneofs()
	for i := 0; i < oneofs.Len(); i++ {
		oneof := oneofs.Get(i)
		if message.WhichOneof(oneof) == nil {
			field := oneof.Fields().Get(0)
			message.Set(field, message.NewField(field))
		}
	}
	message.Range(func(field protoreflect.FieldDescriptor, value protoreflect.Value) bool {
		switch {
		case field.IsMap():
			value.Map().Range(func(key protoreflect.MapKey, entry protoreflect.Value) bool {
				if field.MapValue().Kind() == protoreflect.MessageKind {
					normalizeJSONStreamFixture(entry.Message())
				} else {
					value.Map().Set(key, normalizeJSONStreamScalar(field.MapValue(), entry))
				}
				return true
			})
		case field.IsList():
			for i := 0; i < value.List().Len(); i++ {
				entry := value.List().Get(i)
				if field.Kind() == protoreflect.MessageKind {
					normalizeJSONStreamFixture(entry.Message())
				} else {
					value.List().Set(i, normalizeJSONStreamScalar(field, entry))
				}
			}
		case field.Kind() == protoreflect.MessageKind:
			normalizeJSONStreamFixture(value.Message())
		default:
			message.Set(field, normalizeJSONStreamScalar(field, value))
		}
		return true
	})
}

func normalizeJSONStreamScalar(field protoreflect.FieldDescriptor, value protoreflect.Value) protoreflect.Value {
	switch field.Kind() {
	case protoreflect.StringKind:
		return protoreflect.ValueOfString(strings.ToValidUTF8(value.String(), "fixture"))
	case protoreflect.EnumKind:
		if field.Enum().Values().ByNumber(value.Enum()) == nil {
			return protoreflect.ValueOfEnum(field.Enum().Values().Get(0).Number())
		}
	}
	return value
}

func TestJSONStreamGeneratedParity(t *testing.T) {
	payloads := (&pb.Envelope{}).ProtoReflect().Descriptor().Oneofs().ByName("payload").Fields()
	for index := 0; index < payloads.Len()+512; index++ {
		envelope := &pb.Envelope{}
		if index < payloads.Len() {
			envelope.ProtoReflect().Mutable(payloads.Get(index))
		} else {
			fillJSONProducerMessage(envelope.ProtoReflect(), rand.New(rand.NewSource(int64(index))), 0)
		}
		normalizeJSONStreamFixture(envelope.ProtoReflect())
		data, err := protojson.Marshal(envelope)
		if err != nil {
			t.Fatal(err)
		}
		if !assertJSONStreamParity(t, data) {
			t.Fatalf("generated canonical fixture %d rejected", index)
		}
		// Unknown scalars and containers remain additive, but all duplicate keys
		// are rejected even when the duplicate subtree has no schema descriptor.
		augmented := append([]byte(`{"future":[null,{"x":[true,"text",123]}],`), data[1:]...)
		if !assertJSONStreamParity(t, augmented) {
			t.Fatal("additive unknown field rejected")
		}
		duplicate := append([]byte(`{"future":{"a":1,"\u0061":2},`), data[1:]...)
		if assertJSONStreamParity(t, duplicate) {
			t.Fatal("unknown duplicate accepted")
		}
	}
}

func FuzzJSONStreamDecoderParity(f *testing.F) {
	for _, seed := range []string{
		`{"ping":{}}`, `{"command":{"cancel":{}}}`, `{"ping":{},"p\u0069ng":{}}`,
		`{"ping":{},"future":[null,{"a":1,"a":2}]}`, `{"hello":{"major":1e0}}`,
		`{"command":{"sequence":"18446744073709551615","cancel":{}}}`,
		`{"ingress":{"message":{"addresses":{"to":{"values":[{"params":{"tag":"a"}}]}}}}}`,
		`{"ingest":{"wire":"AR=="}}`, `{"ping":{"nonce":"\ud800"}}`,
		`{"ping":{},"future":` + strings.Repeat("[", 32) + "0" + strings.Repeat("]", 32) + "}",
	} {
		f.Add([]byte(seed))
	}
	f.Fuzz(func(t *testing.T, wire []byte) { assertJSONStreamParity(t, wire) })
}

func BenchmarkJSONStreamDecoder(b *testing.B) {
	for name, envelope := range map[string]*pb.Envelope{
		"ping": {Payload: &pb.Envelope_Ping{Ping: &pb.Heartbeat{Nonce: "123"}}},
		"ingest": {Payload: &pb.Envelope_Ingest{Ingest: &pb.OwnerIngest{
			Context:     &pb.ContextRef{Id: "context-1", Owner: &pb.ProcessRef{InstanceId: "perf-core", Epoch: "core-epoch"}},
			Application: &pb.ProcessRef{InstanceId: "perf-app", Epoch: "app-epoch"},
			Flow:        &pb.FlowRef{ConnectionId: "tls-phone", SourceIp: "192.0.2.1", SourcePort: 5061, Listener: "core:5061", Transport: "tls"},
			Wire:        bytes.Repeat([]byte("w"), 1024), Authenticated: true, PrincipalId: "perf-phone", BindingId: "perf-binding", Admission: pb.OwnerIngest_AUTHENTICATED_BINDING,
		}}},
		"request": {CorrelationId: "request-1", Payload: &pb.Envelope_Request{Request: &pb.RequestPresented{
			RequestId: "notify-1", Message: &pb.SipMessage{Method: "NOTIFY", Wire: bytes.Repeat([]byte("w"), 1024), Body: bytes.Repeat([]byte("b"), 512), Headers: []*pb.Header{{Name: "event", Value: "dialog"}, {Name: "subscription-state", Value: "active;expires=300"}}, Addresses: map[string]*pb.AddressList{"to": {Values: []*pb.Address{{Uri: "sip:1000@bench.invalid", Params: map[string]string{"tag": "phone-tag"}}}}}},
		}}},
	} {
		data, err := protojson.Marshal(envelope)
		if err != nil {
			b.Fatal(err)
		}
		for mode, decode := range map[string]func([]byte) (*pb.Envelope, error){"tree": decodeJSONEnvelopeTree, "stream": decodeJSONEnvelope} {
			b.Run(name+"/"+mode, func(b *testing.B) {
				b.ReportAllocs()
				b.SetBytes(int64(len(data)))
				for i := 0; i < b.N; i++ {
					if _, err := decode(data); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
