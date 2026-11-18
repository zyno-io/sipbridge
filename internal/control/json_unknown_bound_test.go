package control

import (
	"testing"

	"google.golang.org/protobuf/proto"
)

func TestJSONUnknownFieldsDoNotBecomeProtobufUnknownWire(t *testing.T) {
	for _, test := range []struct{ name, known, withUnknown string }{
		{"heartbeat", `{"ping":{"nonce":"n"}}`, `{"ping":{"nonce":"n","future":{"nested":[1,2]}},"unknownField":{"value":"x"}}`},
		{"flow-write", `{"write":{"flow":{"ingress":{"instanceId":"w","epoch":"e"}},"effect":{"context":{"state":{"authority":{"service":"s"}}}},"permit":{"authorization":{"effect":{"context":{"state":{"authority":{"service":"s"}}}}}}}}`, `{"write":{"future":[1],"flow":{"future":true,"ingress":{"instanceId":"w","epoch":"e","future":"x"}},"effect":{"context":{"future":{},"state":{"authority":{"service":"s","future":"x"}}}},"permit":{"future":{},"authorization":{"effect":{"context":{"state":{"authority":{"service":"s","future":7}}}}}}},"future":null}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			known, err := decodeJSONEnvelope([]byte(test.known))
			if err != nil {
				t.Fatal(err)
			}
			unknown, err := decodeJSONEnvelope([]byte(test.withUnknown))
			if err != nil {
				t.Fatal(err)
			}
			if !proto.Equal(known, unknown) || len(unknown.ProtoReflect().GetUnknown()) != 0 {
				t.Fatal("additive JSON retained unknown protobuf wire")
			}
			if write := unknown.GetWrite(); write != nil && (len(write.ProtoReflect().GetUnknown()) != 0 || len(write.Flow.ProtoReflect().GetUnknown()) != 0 || len(write.Flow.Ingress.ProtoReflect().GetUnknown()) != 0 || len(write.Effect.Context.State.Authority.ProtoReflect().GetUnknown()) != 0 || len(write.Permit.Authorization.Effect.Context.State.Authority.ProtoReflect().GetUnknown()) != 0) {
				t.Fatal("nested JSON retained unknown protobuf wire")
			}
		})
	}
}
