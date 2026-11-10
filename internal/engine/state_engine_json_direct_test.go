package engine

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	pb "sipbridge/internal/controlpb"
)

func TestRetainedDirectInputDecoderMatchesOuterUnmarshal(t *testing.T) {
	for _, test := range []struct {
		name  string
		data  []byte
		valid bool
	}{
		{"nil", nil, false},
		{"empty", []byte{}, false},
		{"null", []byte(`null`), true},
		{"whitespace", []byte(" \n\t{}\r\n"), true},
		{"empty-proto", []byte(`{"command":null,"reply":{}}`), true},
		{"framing-only", []byte(`{"command":{"sequence":"2"},"reply":{"correlationId":"frame"}}`), true},
		{"oneof", []byte(`{"command":{"context":{"id":"ctx"},"respond":{"status":200}},"reply":{"result":{"certainty":"COMPLETED"}}}`), true},
		{"duplicate-last-oneof", []byte(`{"command":{"cancel":{}},"command":{"respond":{"status":200}}}`), true},
		{"conflicting-oneofs", []byte(`{"command":{"cancel":{},"respond":{"status":200}}}`), false},
		{"unknown-proto", []byte(`{"reply":{"unknown":true}}`), false},
		{"typed-error", []byte(`{"kind":12}`), false},
		{"truncated", []byte(`{"kind":`), false},
		{"trailing-object", []byte(`{"kind":"locate"}{}`), false},
		{"trailing-junk", []byte(`{} false`), false},
		{"too-deep", []byte(`{"ignored":` + strings.Repeat("[", 10000) + "0" + strings.Repeat("]", 10000) + `}`), false},
	} {
		t.Run(test.name, func(t *testing.T) {
			original := stateEngineInput{Kind: "keep", Message: &pb.SipMessage{Wire: []byte("keep")}}
			outer, direct := original, original
			before, err := json.Marshal(original)
			if err != nil {
				t.Fatal(err)
			}
			outerErr := json.Unmarshal(test.data, &outer)
			directErr := direct.UnmarshalJSON(test.data)
			if (outerErr == nil) != test.valid || (directErr == nil) != test.valid {
				t.Fatalf("direct/outer validation diverged: outer=%v direct=%v", outerErr, directErr)
			}
			outerBytes, outerErr := json.Marshal(outer)
			directBytes, directErr := json.Marshal(direct)
			if outerErr != nil || directErr != nil || !bytes.Equal(outerBytes, directBytes) {
				t.Fatal("direct decoder changed canonical input bytes")
			}
			if !test.valid && !bytes.Equal(directBytes, before) {
				t.Fatal("failed input decode changed its prior receiver")
			}
		})
	}
}

func TestRetainedDirectEffectDecoderMatchesOuterUnmarshal(t *testing.T) {
	message := retainedCodecMessage(t)
	wireEffect, err := json.Marshal(stateEngineEffect{Context: "ctx", Envelope: &pb.Envelope{
		Payload: &pb.Envelope_Request{Request: &pb.RequestPresented{RequestId: "request", Message: message}}}})
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name  string
		data  []byte
		valid bool
	}{
		{"nil", nil, false},
		{"empty", []byte{}, false},
		{"null", []byte(`null`), true},
		{"whitespace", []byte(" \n\t{}\r\n"), true},
		{"null-proto", []byte(`{"envelope":null}`), true},
		{"empty-proto", []byte(`{"envelope":{}}`), true},
		{"framing-only", []byte(`{"envelope":{"sessionId":"session","correlationId":"frame"}}`), true},
		{"oneof", []byte(`{"envelope":{"result":{"certainty":"COMPLETED"}}}`), true},
		{"duplicate-last-oneof", []byte(`{"envelope":{"ping":{"nonce":"first"}},"envelope":{"pong":{"nonce":"last"}}}`), true},
		{"conflicting-oneofs", []byte(`{"envelope":{"ping":{},"pong":{}}}`), false},
		{"unknown-proto", []byte(`{"envelope":{"unknown":true}}`), false},
		{"typed-error", []byte(`{"native":"true"}`), false},
		{"truncated", []byte(`{"envelope":`), false},
		{"trailing-object", []byte(`{"context":"ctx"}{}`), false},
		{"trailing-junk", []byte(`{} false`), false},
		{"too-deep", []byte(`{"ignored":` + strings.Repeat("[", 10000) + "0" + strings.Repeat("]", 10000) + `}`), false},
		{"wire-restoration", wireEffect, true},
		{"invalid-wire", []byte(`{"wire":"aW52YWxpZC13aXJl","envelope":{"request":{"requestId":"r"}}}`), false},
	} {
		t.Run(test.name, func(t *testing.T) {
			original := stateEngineEffect{Context: "keep", Wire: []byte("keep")}
			outer, direct := original, original
			before, err := json.Marshal(original)
			if err != nil {
				t.Fatal(err)
			}
			outerErr := json.Unmarshal(test.data, &outer)
			directErr := direct.UnmarshalJSON(test.data)
			if (outerErr == nil) != test.valid || (directErr == nil) != test.valid {
				t.Fatalf("direct/outer validation diverged: outer=%v direct=%v", outerErr, directErr)
			}
			outerBytes, outerErr := json.Marshal(outer)
			directBytes, directErr := json.Marshal(direct)
			if outerErr != nil || directErr != nil || !bytes.Equal(outerBytes, directBytes) {
				t.Fatal("direct decoder changed canonical effect bytes")
			}
			if !test.valid && !bytes.Equal(directBytes, before) {
				t.Fatal("failed effect decode changed its prior receiver")
			}
			if test.name == "wire-restoration" {
				owned := append([]byte(nil), test.data...)
				for index := range test.data {
					test.data[index] = 'X'
				}
				if !bytes.Equal(direct.Wire, message.Wire) || !bytes.Equal(direct.Envelope.GetRequest().GetMessage().GetWire(), message.Wire) {
					t.Fatal("direct decoder retained caller-owned wire bytes")
				}
				prior := direct.Wire
				if err = direct.UnmarshalJSON(owned); err != nil {
					t.Fatal(err)
				}
				direct.Wire[0] = 'X'
				if !bytes.Equal(prior, message.Wire) {
					t.Fatal("later direct decode reused an earlier receiver's wire buffer")
				}
			}
		})
	}
}
