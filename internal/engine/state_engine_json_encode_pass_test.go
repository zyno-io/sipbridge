package engine

import (
	"bytes"
	"encoding/json"
	"reflect"
	"sort"
	"strings"
	"testing"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
)

// Frozen encoder: compatibility must include exact operation-fingerprint bytes,
// not merely equivalent decoded JSON or field values.
func legacyStateMarshalFields(value any, fields map[string]proto.Message) ([]byte, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal(data, &object); err != nil {
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

func legacyStateEncodeInput(in stateEngineInput) ([]byte, error) {
	type plain stateEngineInput
	value := plain(in)
	value.Command, value.Reply = nil, nil
	return legacyStateMarshalFields(value, map[string]proto.Message{"command": in.Command, "reply": in.Reply})
}

func legacyStateEncodeEffect(effect stateEngineEffect) ([]byte, error) {
	type plain stateEngineEffect
	value := plain(effect)
	value.Envelope = nil
	var envelope *pb.Envelope
	if effect.Envelope != nil {
		envelope = proto.Clone(effect.Envelope).(*pb.Envelope)
		var message *pb.SipMessage
		if egress := envelope.GetEgress(); egress != nil {
			message, egress.Message = egress.Message, nil
		}
		if request := envelope.GetRequest(); request != nil {
			message, request.Message = request.Message, nil
		}
		if event := envelope.GetDialogEvent(); event != nil {
			message, event.Message = event.Message, nil
		}
		if message != nil && len(value.Wire) == 0 {
			value.Wire = append([]byte(nil), message.Wire...)
		}
	}
	return legacyStateMarshalFields(value, map[string]proto.Message{"envelope": envelope})
}

func TestRetainedEncoderFieldsRemainLexicallyOrdered(t *testing.T) {
	for _, typ := range []reflect.Type{reflect.TypeOf(stateEngineInput{}), reflect.TypeOf(stateEngineEffect{})} {
		var keys []string
		for index := 0; index < typ.NumField(); index++ {
			field := typ.Field(index)
			key := strings.Split(field.Tag.Get("json"), ",")[0]
			if key == "" || key == "-" || field.Anonymous {
				t.Fatalf("%s.%s needs an explicit retained JSON key", typ.Name(), field.Name)
			}
			keys = append(keys, key)
		}
		if !sort.StringsAreSorted(keys) {
			t.Fatalf("%s keys must preserve frozen sorted-map bytes: %v", typ.Name(), keys)
		}
	}
}

func TestRetainedEncoderFastPathMatchesFrozenBytes(t *testing.T) {
	message := retainedCodecMessage(t)
	process := &pb.ProcessRef{InstanceId: "<&>\u2028", Epoch: "epoch"}
	ref := &pb.ContextRef{Owner: process, Id: "context"}
	flow := &pb.FlowRef{Ingress: process, ConnectionId: "flow", Transport: "tls"}
	full := stateEngineInput{Kind: "ingest", Application: process, ApplicationLeaseTerm: "term", Context: ref,
		Executor: process, Flow: flow, Ingest: &pb.OwnerIngest{Context: ref, Flow: flow, Wire: message.Wire},
		Delivery: &pb.DeliveryResult{Outcome: pb.DeliveryResult_UNKNOWN}, Message: message,
		NativeCompletion: true, Operation: "op", Effect: "effect", Failure: "<&>\u2029", Timer: "timer", Generation: "1", Status: 503,
		Write: &pb.FlowWrite{Flow: flow}}
	inputs := []stateEngineInput{{}, {Kind: "timer"}, full}
	for _, command := range []*pb.Command{nil, {}, {Context: ref, Sequence: 1, Action: &pb.Command_Cancel{Cancel: &pb.Cancel{}}}} {
		for _, reply := range []*pb.Envelope{nil, {}, {Payload: &pb.Envelope_Result{Result: &pb.OperationResult{Certainty: pb.OperationResult_COMPLETED}}}} {
			in := full
			in.Command, in.Reply = command, reply
			inputs = append(inputs, in)
		}
	}
	for index, input := range inputs {
		want, oldErr := legacyStateEncodeInput(input)
		got, err := input.MarshalJSON()
		if err != nil || oldErr != nil || !bytes.Equal(got, want) {
			t.Fatalf("input %d changed exact encoding: %v / %v\n%s\n%s", index, err, oldErr, got, want)
		}
	}
	fullEffect := stateEngineEffect{Context: "context", Native: true, Rejection: true, Wire: message.Wire, Flow: flow,
		Permit: &pb.SendPermit{Flow: flow}, Authorization: &pb.EffectAuthorization{AuthorityClass: pb.EffectAuthorization_APPLICATION},
		Application: process, Executor: process, Cancelled: true, Settled: true, Command: "cmd", Dialog: "dialog", AfterACK: "hash",
		Transaction: "tx", WireHash: "hash", WireMethod: "OPTIONS", WireStatus: 200, WireSequence: 1,
		Policy: &statePolicyAdmission{Command: "cmd", Term: "term", Decision: &pb.CommandAdmission{DirectoryEpoch: "epoch", ExpiresAtMs: 1, SelectedFlow: flow}}}
	effects := []stateEngineEffect{{}, {Context: "empty", Wire: []byte{}}, fullEffect}
	for _, envelope := range []*pb.Envelope{nil, {},
		{Payload: &pb.Envelope_Request{Request: &pb.RequestPresented{Message: message}}},
		{Payload: &pb.Envelope_Egress{Egress: &pb.EgressRequest{Message: message}}},
		{Payload: &pb.Envelope_DialogEvent{DialogEvent: &pb.DialogEvent{Message: message}}}} {
		effect := fullEffect
		effect.Envelope = envelope
		effects = append(effects, effect)
	}
	for index, effect := range effects {
		want, oldErr := legacyStateEncodeEffect(effect)
		got, err := effect.MarshalJSON()
		if err != nil || oldErr != nil || !bytes.Equal(got, want) {
			t.Fatalf("effect %d changed exact encoding: %v / %v\n%s\n%s", index, err, oldErr, got, want)
		}
	}
}

func BenchmarkRetainedEffectEncoderPasses(b *testing.B) {
	value := stateEngineEffect{Context: "native-context", Native: true, Wire: bytes.Repeat([]byte{0, 255, '<', '&'}, 2048),
		Flow: &pb.FlowRef{ConnectionId: "flow", Transport: "tls"}, WireMethod: "OPTIONS", WireStatus: 200}
	b.Run("frozen", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if _, err := legacyStateEncodeEffect(value); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("current", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if _, err := value.MarshalJSON(); err != nil {
				b.Fatal(err)
			}
		}
	})
}
