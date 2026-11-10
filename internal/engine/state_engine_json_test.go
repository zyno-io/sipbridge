package engine

import (
	"bytes"
	"encoding/json"
	"reflect"
	"testing"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
)

// Frozen pre-cleanup decoders provide the migration compatibility oracle.
func legacyStateUnmarshalFields(data []byte, value any, fields map[string]proto.Message) error {
	var object map[string]json.RawMessage
	if err := json.Unmarshal(data, &object); err != nil {
		return err
	}
	for name, message := range fields {
		encoded := object[name]
		delete(object, name)
		if len(encoded) > 0 && string(encoded) != "null" {
			if err := protojson.Unmarshal(encoded, message); err != nil {
				return err
			}
		}
	}
	rest, err := json.Marshal(object)
	if err != nil {
		return err
	}
	return json.Unmarshal(rest, value)
}
func legacyStateDecodeInput(in *stateEngineInput, data []byte) error {
	type plain stateEngineInput
	command, reply := new(pb.Command), new(pb.Envelope)
	var value plain
	if err := legacyStateUnmarshalFields(data, &value, map[string]proto.Message{"command": command, "reply": reply}); err != nil {
		return err
	}
	*in = stateEngineInput(value)
	if command.Context != nil || command.Action != nil {
		in.Command = command
	}
	if reply.Payload != nil {
		in.Reply = reply
	}
	return nil
}
func legacyStateDecodeEffect(effect *stateEngineEffect, data []byte) error {
	type plain stateEngineEffect
	envelope := new(pb.Envelope)
	var value plain
	if err := legacyStateUnmarshalFields(data, &value, map[string]proto.Message{"envelope": envelope}); err != nil {
		return err
	}
	*effect = stateEngineEffect(value)
	if envelope.Payload != nil {
		effect.Envelope = envelope
		if len(effect.Wire) > 0 {
			outbound := envelope.GetEgress() != nil
			if outbound || envelope.GetRequest() != nil || envelope.GetDialogEvent() != nil {
				message, err := Observe(effect.Wire, outbound)
				if err != nil {
					return err
				}
				if effect.Rejection {
					message.Selectors = nil
				}
				if egress := envelope.GetEgress(); egress != nil && egress.Message == nil {
					egress.Message = message
				}
				if request := envelope.GetRequest(); request != nil && request.Message == nil {
					request.Message = message
				}
				if event := envelope.GetDialogEvent(); event != nil && event.Message == nil {
					event.Message = message
				}
			}
		}
	}
	return nil
}

func retainedCodecMessage(t testing.TB) *pb.SipMessage {
	t.Helper()
	wire := []byte("OPTIONS sip:phone@example.test SIP/2.0\r\nVia: SIP/2.0/TLS sip.example.test:5061;branch=z9hG4bK-codec\r\nFrom: <sip:alice@example.test>;tag=local\r\nTo: <sip:phone@example.test>\r\nCall-ID: codec@example.test\r\nCSeq: 1 OPTIONS\r\nContent-Length: 4\r\n\r\n")
	wire = append(wire, 0, 255, 254, 3)
	observed, err := Observe(wire, false)
	if err != nil {
		t.Fatal(err)
	}
	return observed
}

func TestRetainedCodecCanonicalCompatibilityAndOwnership(t *testing.T) {
	message := retainedCodecMessage(t)
	context := &pb.ContextRef{Id: "context"}
	commands := []*pb.Command{
		{Context: context, Sequence: 1, Action: &pb.Command_Respond{Respond: &pb.Respond{RequestId: "request", Status: 200}}},
		{Context: context, Sequence: 1, Action: &pb.Command_OpenUac{OpenUac: &pb.OpenUac{Uri: "sip:phone@example.test"}}},
		{Context: context, Sequence: 2, Action: &pb.Command_DialogRequest{DialogRequest: &pb.DialogRequest{Method: "BYE"}}},
		{Context: context, Sequence: 2, Action: &pb.Command_Cancel{Cancel: &pb.Cancel{}}},
	}
	for _, command := range commands {
		original := stateEngineInput{Kind: "command", Command: command, Message: message}
		data, err := json.Marshal(original)
		if err != nil {
			t.Fatal(err)
		}
		var previous, current stateEngineInput
		if err = legacyStateDecodeInput(&previous, data); err != nil {
			t.Fatal(err)
		}
		if err = json.Unmarshal(data, &current); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(previous, current) {
			t.Fatal("canonical input changed")
		}
		encoded, err := json.Marshal(current)
		if err != nil || !bytes.Equal(encoded, data) {
			t.Fatal("input fingerprint bytes changed", err)
		}
		for n := range data {
			data[n] = 'X'
		}
		after, _ := json.Marshal(current)
		if !bytes.Equal(after, encoded) {
			t.Fatal("input aliases decoder bytes")
		}
	}
	envelopes := []*pb.Envelope{
		{Payload: &pb.Envelope_Result{Result: &pb.OperationResult{Certainty: pb.OperationResult_COMPLETED}}},
		{Payload: &pb.Envelope_SendPermit{SendPermit: &pb.SendPermit{
			Authorization: &pb.EffectAuthorization{
				Effect:         &pb.StateEffectRef{Context: context, Id: "effect", AttemptId: "attempt"},
				AuthorityClass: pb.EffectAuthorization_APPLICATION, DeadlineMs: 1<<53 + 1, ApplicationLeaseTerm: "lease-term",
			},
			Flow: &pb.FlowRef{Ingress: &pb.ProcessRef{InstanceId: "sip", Epoch: "epoch"}, ConnectionId: "connection",
				SourceIp: "192.0.2.1", SourcePort: 5061, Listener: "sip.example.test:5061", Transport: "tls"},
			DirectoryEpoch: "directory", OrderingProof: "ordering",
		}}},
		{Payload: &pb.Envelope_Request{Request: &pb.RequestPresented{RequestId: "request", Message: message}}},
		{Payload: &pb.Envelope_Egress{Egress: &pb.EgressRequest{Message: message}}},
		{Payload: &pb.Envelope_DialogEvent{DialogEvent: &pb.DialogEvent{Kind: pb.DialogEvent_ENDED, Message: message}}},
		{Payload: &pb.Envelope_FlowAdoptionResult{FlowAdoptionResult: &pb.FlowAdoptionResult{Error: "unavailable"}}},
		{Payload: &pb.Envelope_ApplicationStatus{ApplicationStatus: &pb.ApplicationStatus{Application: &pb.ProcessRef{InstanceId: "controller"}}}},
		{Payload: &pb.Envelope_AuthorityReceipt{AuthorityReceipt: &pb.AuthorityReceipt{Context: context}}},
		{Payload: &pb.Envelope_CredentialLookup{CredentialLookup: &pb.CredentialLookup{Context: context}}},
	}
	for _, envelope := range envelopes {
		for _, rejected := range []bool{false, true} {
			original := stateEngineEffect{Context: "context", Rejection: rejected, Envelope: envelope}
			data, err := json.Marshal(original)
			if err != nil {
				t.Fatal(err)
			}
			var previous, current stateEngineEffect
			if err = legacyStateDecodeEffect(&previous, data); err != nil {
				t.Fatal(err)
			}
			if err = json.Unmarshal(data, &current); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(previous, current) {
				t.Fatal("canonical effect changed")
			}
			encoded, err := json.Marshal(current)
			if err != nil || !bytes.Equal(encoded, data) {
				t.Fatal("effect bytes changed", err)
			}
			for n := range data {
				data[n] = 'X'
			}
			after, _ := json.Marshal(current)
			if !bytes.Equal(after, encoded) {
				t.Fatal("effect aliases decoder bytes")
			}
		}
		data, _ := json.Marshal(stateEngineInput{Kind: "effect-result", Reply: envelope})
		var previous, current stateEngineInput
		if err := legacyStateDecodeInput(&previous, data); err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(data, &current); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(previous, current) {
			t.Fatal("canonical reply changed")
		}
	}
}

func TestRetainedCodecEmptyAttachmentAndGoldenEncoder(t *testing.T) {
	for _, data := range []string{`{}`, `{"command":null,"reply":null}`, `{"command":{},"reply":{}}`, `{"command":{"sequence":"2"},"reply":{"sessionId":"session"}}`} {
		var value stateEngineInput
		if err := json.Unmarshal([]byte(data), &value); err != nil || value.Command != nil || value.Reply != nil {
			t.Fatal("empty proto attached", data, err)
		}
	}
	for _, data := range []string{`{}`, `{"envelope":null}`, `{"envelope":{}}`, `{"envelope":{"correlationId":"framing-only"}}`} {
		var value stateEngineEffect
		if err := json.Unmarshal([]byte(data), &value); err != nil || value.Envelope != nil {
			t.Fatal("empty envelope attached", data, err)
		}
	}
	input := stateEngineInput{Kind: "command", Command: &pb.Command{Context: &pb.ContextRef{Id: "ctx"}, Sequence: 2, Action: &pb.Command_Respond{Respond: &pb.Respond{RequestId: "request", Status: 200}}}}
	data, err := json.Marshal(input)
	want := `{"command":{"context":{"id":"ctx"},"sequence":"2","respond":{"requestId":"request","status":200}},"kind":"command"}`
	if err != nil || string(data) != want {
		t.Fatalf("encoder/fingerprint changed: %s, %v", data, err)
	}
}

func TestRetainedCodecFailureDoesNotReplaceReceiver(t *testing.T) {
	for _, data := range []string{`{"kind":`, `{"command":{"unknown":true}}`, `{"kind":12}`} {
		value := stateEngineInput{Kind: "keep", Message: &pb.SipMessage{Wire: []byte("keep")}}
		before, _ := json.Marshal(value)
		if err := json.Unmarshal([]byte(data), &value); err == nil {
			t.Fatal("invalid input accepted", data)
		}
		after, _ := json.Marshal(value)
		if !bytes.Equal(before, after) {
			t.Fatal("failed input changed receiver")
		}
	}
	for _, data := range []string{`{"envelope":`, `{"envelope":{"unknown":true}}`, `{"wire":"aW52YWxpZC13aXJl","envelope":{"request":{"requestId":"r"}}}`} {
		value := stateEngineEffect{Context: "keep", Wire: []byte("keep")}
		before, _ := json.Marshal(value)
		if err := json.Unmarshal([]byte(data), &value); err == nil {
			t.Fatal("invalid effect accepted", data)
		}
		after, _ := json.Marshal(value)
		if !bytes.Equal(before, after) {
			t.Fatal("failed effect changed receiver")
		}
	}
}

func TestRetainedCodecNoncanonicalTypedSemantics(t *testing.T) {
	var input stateEngineInput
	if err := json.Unmarshal([]byte(`{"command":{"cancel":{}},"command":{"respond":{"status":200}},"context":{"id":"first"},"context":{"owner":{"instance_id":"second"}}}`), &input); err != nil {
		t.Fatal(err)
	}
	if input.Command.GetCancel() != nil || input.Command.GetRespond().GetStatus() != 200 || input.Context.Id != "first" || input.Context.Owner.InstanceId != "second" {
		t.Fatal("duplicate field policy changed")
	}
	if err := json.Unmarshal([]byte(`{"Command":{"cancel":{}}}`), &input); err != nil || input.Command.GetCancel() == nil {
		t.Fatal("typed case-insensitive decoding changed", err)
	}
	var effect stateEngineEffect
	if err := json.Unmarshal([]byte(`{"envelope":{"ping":{"nonce":"first"}},"envelope":{"pong":{"nonce":"last"}}}`), &effect); err != nil || effect.Envelope.GetPing() != nil || effect.Envelope.GetPong().GetNonce() != "last" {
		t.Fatal("duplicate envelope was merged", err)
	}
}

func BenchmarkRetainedEffectDecoder(b *testing.B) {
	message := retainedCodecMessage(b)
	data, err := json.Marshal(stateEngineEffect{Envelope: &pb.Envelope{Payload: &pb.Envelope_Request{Request: &pb.RequestPresented{Message: message, RequestId: "request"}}}})
	if err != nil {
		b.Fatal(err)
	}
	for _, legacy := range []bool{true, false} {
		name := "typed"
		if legacy {
			name = "legacy-map"
		}
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				var value stateEngineEffect
				var err error
				if legacy {
					err = legacyStateDecodeEffect(&value, data)
				} else {
					err = value.UnmarshalJSON(data)
				}
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
