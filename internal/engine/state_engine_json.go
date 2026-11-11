package engine

import (
	"encoding/json"
	"errors"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
)

// Human-readable diagnostic fixtures use ordinary JSON for portable SIP values and protobuf JSON
// for oneof-bearing control messages. encoding/json cannot reconstruct a Go
// protobuf oneof interface, so these fields must retain their schema names.
// Only the two private retained structs call this helper; their ordinary fields
// are declared in lexical JSON-key order, matching the former sorted map.
func stateMarshalFields(value any, fields map[string]proto.Message) ([]byte, error) {
	shadow := false
	for _, message := range fields {
		if message != nil && message.ProtoReflect().IsValid() {
			shadow = true
			break
		}
	}
	if !shadow {
		return json.Marshal(value)
	}
	data, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	var object map[string]json.RawMessage
	if err = json.Unmarshal(data, &object); err != nil {
		return nil, err
	}
	for name, message := range fields {
		if message == nil || !message.ProtoReflect().IsValid() {
			continue
		}
		encoded, err := protojson.Marshal(message)
		if err != nil {
			return nil, err
		}
		object[name] = encoded
	}
	return json.Marshal(object)
}
func (in stateEngineInput) MarshalJSON() ([]byte, error) {
	type plain stateEngineInput
	value := plain(in)
	value.Command = nil
	value.Reply = nil
	return stateMarshalFields(value, map[string]proto.Message{"command": in.Command, "reply": in.Reply})
}
func (in *stateEngineInput) UnmarshalJSON(data []byte) error {
	type plain stateEngineInput
	command, reply := new(pb.Command), new(pb.Envelope)
	var value struct {
		plain
		Command json.RawMessage `json:"command,omitempty"`
		Reply   json.RawMessage `json:"reply,omitempty"`
	}
	if err := json.Unmarshal(data, &value); err != nil {
		return err
	}
	if len(value.Command) > 0 && string(value.Command) != "null" {
		if err := protojson.Unmarshal(value.Command, command); err != nil {
			return err
		}
	}
	if len(value.Reply) > 0 && string(value.Reply) != "null" {
		if err := protojson.Unmarshal(value.Reply, reply); err != nil {
			return err
		}
	}
	decoded := stateEngineInput(value.plain)
	if command.Context != nil || command.Action != nil {
		decoded.Command = command
	}
	if reply.Payload != nil {
		decoded.Reply = reply
	}
	*in = decoded
	return nil
}
func (effect stateEngineEffect) MarshalJSON() ([]byte, error) {
	type plain stateEngineEffect
	value := plain(effect)
	value.Envelope = nil
	var envelope *pb.Envelope
	if effect.Envelope != nil {
		envelope = proto.Clone(effect.Envelope).(*pb.Envelope)
		var message *pb.SipMessage
		if egress := envelope.GetEgress(); egress != nil {
			message = egress.Message
			egress.Message = nil
		}
		if request := envelope.GetRequest(); request != nil {
			message = request.Message
			request.Message = nil
		}
		if event := envelope.GetDialogEvent(); event != nil {
			message = event.Message
			event.Message = nil
		}
		if message != nil {
			if len(value.Wire) == 0 {
				value.Wire = append([]byte(nil), message.Wire...)
			}
		}
	}
	return stateMarshalFields(value, map[string]proto.Message{"envelope": envelope})
}
func (effect *stateEngineEffect) UnmarshalJSON(data []byte) error {
	type plain stateEngineEffect
	envelope := new(pb.Envelope)
	var value struct {
		plain
		Envelope json.RawMessage `json:"envelope,omitempty"`
	}
	if err := json.Unmarshal(data, &value); err != nil {
		return err
	}
	if len(value.Envelope) > 0 && string(value.Envelope) != "null" {
		if err := protojson.Unmarshal(value.Envelope, envelope); err != nil {
			return err
		}
	}
	decoded := stateEngineEffect(value.plain)
	if envelope.Payload != nil {
		decoded.Envelope = envelope
		if len(decoded.Wire) > 0 {
			outbound := envelope.GetEgress() != nil
			if outbound || envelope.GetRequest() != nil || envelope.GetDialogEvent() != nil {
				message, err := Observe(decoded.Wire, outbound)
				if err != nil {
					return err
				}
				if decoded.Rejection {
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
	*effect = decoded
	return nil
}

// Session framing is transient. Retained RPC outcomes bind only the logical
// result, so reconnecting a controller cannot change an operation fingerprint.
func stateCanonicalReply(reply *pb.Envelope) *pb.Envelope {
	if reply == nil {
		return nil
	}
	result := proto.Clone(reply).(*pb.Envelope)
	result.SessionId = ""
	result.CorrelationId = ""
	result.RemainingBudgetMs = 0
	return result
}

func stateValidateRetainedInput(in stateEngineInput) error {
	if in.Reply != nil && in.Reply.GetCredentialResult() != nil {
		return errors.New("transient credential result cannot be retained")
	}
	return nil
}

func stateEffectResultID(engine *StateEngine, kind, effect string, input stateEngineInput) string {
	encoded, _ := input.MarshalBinary()
	return engine.id(kind, effect, string(encoded))
}
