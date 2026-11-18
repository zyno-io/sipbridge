package state

import (
	"bytes"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
)

// Frozen copy-based leaf codecs. The independently frozen classifier also keeps
// syntax/canonicalization decisions out of the production oracle under test.
func copiedCodecEncodeValue(data []byte) json.RawMessage {
	if structuredWithPreliminaryValidation(data) {
		return append(json.RawMessage(nil), data...)
	}
	encoded, _ := json.Marshal(data)
	return encoded
}

func copiedCodecDecodeValue(data json.RawMessage) ([]byte, error) {
	if len(data) == 0 || string(data) == "null" {
		return nil, nil
	}
	if data[0] == '{' || data[0] == '[' {
		return append([]byte(nil), data...), nil
	}
	var value []byte
	if err := json.Unmarshal(data, &value); err != nil {
		return nil, err
	}
	return value, nil
}

func copiedCodecMarshalEffect(value Effect) ([]byte, error) {
	type plain Effect
	return json.Marshal(struct {
		plain
		Payload json.RawMessage `json:"payload,omitempty"`
	}{plain: plain(value), Payload: copiedCodecEncodeValue(value.Payload)})
}

func copiedCodecMarshalPayload(value Payload) ([]byte, error) {
	type plain Payload
	return json.Marshal(struct {
		plain
		ProtocolState json.RawMessage `json:"protocolState,omitempty"`
	}{plain: plain(value), ProtocolState: copiedCodecEncodeValue(value.ProtocolState)})
}

func copiedCodecUnmarshalEffect(data []byte) (Effect, error) {
	type plain Effect
	var decoded struct {
		plain
		Payload json.RawMessage `json:"payload,omitempty"`
	}
	if err := json.Unmarshal(data, &decoded); err != nil {
		return Effect{}, err
	}
	payload, err := copiedCodecDecodeValue(decoded.Payload)
	if err != nil {
		return Effect{}, err
	}
	value := Effect(decoded.plain)
	value.Payload = payload
	return value, nil
}

func copiedCodecUnmarshalPayload(data []byte) (Payload, error) {
	type plain Payload
	var decoded struct {
		plain
		ProtocolState json.RawMessage `json:"protocolState,omitempty"`
	}
	if err := json.Unmarshal(data, &decoded); err != nil {
		return Payload{}, err
	}
	protocol, err := copiedCodecDecodeValue(decoded.ProtocolState)
	if err != nil {
		return Payload{}, err
	}
	value := Payload(decoded.plain)
	value.ProtocolState = protocol
	return value, nil
}

func assertCodecErrorEquivalent(t testing.TB, got, want error) {
	t.Helper()
	if reflect.TypeOf(got) != reflect.TypeOf(want) || errorText(got) != errorText(want) {
		t.Fatalf("codec error changed: got=%T:%v want=%T:%v", got, got, want, want)
	}
	var gotSyntax, wantSyntax *json.SyntaxError
	if errors.As(got, &gotSyntax) && errors.As(want, &wantSyntax) && gotSyntax.Offset != wantSyntax.Offset {
		t.Fatal("codec syntax error offset changed")
	}
	var gotType, wantType *json.UnmarshalTypeError
	if errors.As(got, &gotType) && errors.As(want, &wantType) &&
		(gotType.Offset != wantType.Offset || gotType.Field != wantType.Field || gotType.Struct != wantType.Struct) {
		t.Fatal("codec type error location changed")
	}
}

func assertCopiedCodecEncoding(t testing.TB, protocol, body []byte) {
	t.Helper()
	protocolBefore, bodyBefore := append([]byte(nil), protocol...), append([]byte(nil), body...)
	effect := Effect{ID: "effect", Kind: "wire", Status: EffectPending, DeadlineMS: 1, Payload: body}
	want, wantErr := copiedCodecMarshalEffect(effect)
	got, gotErr := effect.MarshalJSON()
	assertCodecErrorEquivalent(t, gotErr, wantErr)
	if !bytes.Equal(got, want) {
		t.Fatal("effect codec changed encoded bytes")
	}
	// Nested effect codecs are compared independently above; the payload oracle
	// freezes its own protocol-state handling without changing effect error types.
	payload := Payload{ProtocolState: protocol, Effects: []Effect{effect}}
	want, wantErr = copiedCodecMarshalPayload(payload)
	got, gotErr = payload.MarshalJSON()
	assertCodecErrorEquivalent(t, gotErr, wantErr)
	if !bytes.Equal(got, want) || !bytes.Equal(protocol, protocolBefore) || !bytes.Equal(body, bodyBefore) {
		t.Fatal("payload codec changed encoded bytes or its input buffers")
	}
	assertCommitPayloadEncoding(t, payload)
}

func assertCopiedCodecDecoding(t testing.TB, data []byte) {
	t.Helper()
	before := append([]byte(nil), data...)
	wantPayload, wantErr := copiedCodecUnmarshalPayload(data)
	var gotPayload Payload
	gotErr := gotPayload.UnmarshalJSON(data)
	assertCodecErrorEquivalent(t, gotErr, wantErr)
	if gotErr == nil && !reflect.DeepEqual(gotPayload, wantPayload) {
		t.Fatal("payload codec changed decoded values")
	}
	wantEffect, wantErr := copiedCodecUnmarshalEffect(data)
	var gotEffect Effect
	gotErr = gotEffect.UnmarshalJSON(data)
	assertCodecErrorEquivalent(t, gotErr, wantErr)
	if gotErr == nil && !reflect.DeepEqual(gotEffect, wantEffect) || !bytes.Equal(before, data) {
		t.Fatal("effect codec changed decoded values or source bytes")
	}
}

const ownedCodecPayloadJSON = `{"protocolState":{"value":1},"effects":[{"id":"e0","kind":"wire","status":"pending","deadlineMs":1,"payload":{"value":1}},{"id":"e1","kind":"wire","status":"pending","deadlineMs":1,"payload":{"value":1}}]}`

func TestPayloadDecodeOwnsSourceFieldsAndPreviousReceiver(t *testing.T) {
	source := []byte(ownedCodecPayloadJSON)
	var value Payload
	if err := value.UnmarshalJSON(source); err != nil {
		t.Fatal(err)
	}
	want, err := copiedCodecUnmarshalPayload([]byte(ownedCodecPayloadJSON))
	if err != nil {
		t.Fatal(err)
	}
	clear(source)
	if !reflect.DeepEqual(value, want) {
		t.Fatal("decoded fields alias the source JSON")
	}
	value.ProtocolState[0] = '!'
	if !bytes.Equal(value.Effects[0].Payload, want.Effects[0].Payload) {
		t.Fatal("protocol state aliases an effect payload")
	}
	value.Effects[0].Payload[0] = '?'
	if !bytes.Equal(value.Effects[1].Payload, want.Effects[1].Payload) {
		t.Fatal("independent effects share a reducer buffer")
	}
	if err := value.UnmarshalJSON([]byte(ownedCodecPayloadJSON)); err != nil {
		t.Fatal(err)
	}
	previous := value
	next := bytes.ReplaceAll([]byte(ownedCodecPayloadJSON), []byte(`"value":1`), []byte(`"value":2`))
	if err := value.UnmarshalJSON(next); err != nil {
		t.Fatal(err)
	}
	value.ProtocolState[0], value.Effects[0].Payload[0] = '!', '?'
	if !reflect.DeepEqual(previous, want) {
		t.Fatal("redecoding reused a previous receiver's owned buffers")
	}
}

func TestEffectDecodeOwnsSourceAndPreviousReceiver(t *testing.T) {
	source := []byte(`{"id":"effect","payload":{"value":1}}`)
	var value Effect
	if err := value.UnmarshalJSON(source); err != nil {
		t.Fatal(err)
	}
	clear(source)
	if !bytes.Equal(value.Payload, []byte(`{"value":1}`)) {
		t.Fatal("effect payload aliases source JSON")
	}
	previous := value
	if err := value.UnmarshalJSON([]byte(`{"id":"next","payload":{"value":2}}`)); err != nil {
		t.Fatal(err)
	}
	value.Payload[0] = '!'
	if previous.ID != "effect" || !bytes.Equal(previous.Payload, []byte(`{"value":1}`)) {
		t.Fatal("redecoding overwrote a previous effect value")
	}
}

func TestCodecDuplicateFieldsDoNotRetainSupersededCapacity(t *testing.T) {
	large := structuredSizedObject(2<<20, 'a')
	for _, operation := range []string{"payload", "effect"} {
		t.Run(operation, func(t *testing.T) {
			var source []byte
			var values [][]byte
			if operation == "payload" {
				source = append([]byte(`{"protocolState":`), large...)
				source = append(source, []byte(`,"protocolState":{},"effects":[{"payload":`)...)
				source = append(source, large...)
				source = append(source, []byte(`,"payload":{}}]}`)...)
				var value Payload
				if err := value.UnmarshalJSON(source); err != nil {
					t.Fatal(err)
				}
				values = [][]byte{value.ProtocolState, value.Effects[0].Payload}
			} else {
				source = append([]byte(`{"payload":`), large...)
				source = append(source, []byte(`,"payload":{}}`)...)
				var value Effect
				if err := value.UnmarshalJSON(source); err != nil {
					t.Fatal(err)
				}
				values = [][]byte{value.Payload}
			}
			clear(source)
			for _, value := range values {
				if !bytes.Equal(value, []byte(`{}`)) || cap(value)-len(value) > max(4096, len(value)/4) {
					t.Fatalf("a small final value retained superseded bytes: len=%d cap=%d", len(value), cap(value))
				}
			}
			values[0][0] = '!'
			if len(values) > 1 && !bytes.Equal(values[1], []byte(`{}`)) {
				t.Fatal("duplicate-field results alias independent fields")
			}
		})
	}
}

func TestCodecMarshalOwnsCanonicalOutput(t *testing.T) {
	for _, operation := range []string{"value", "effect", "payload"} {
		t.Run(operation, func(t *testing.T) {
			input := []byte(`{"value":1}`)
			var output []byte
			var err error
			switch operation {
			case "value":
				output = encodeValue(input)
			case "effect":
				output, err = (Effect{Payload: input}).MarshalJSON()
			case "payload":
				output, err = (Payload{ProtocolState: input, Effects: []Effect{{Payload: input}}}).MarshalJSON()
			}
			if err != nil {
				t.Fatal(err)
			}
			wantOutput := append([]byte(nil), output...)
			input[0] = '!'
			if !bytes.Equal(output, wantOutput) {
				t.Fatal("encoded output aliases the caller's reducer input")
			}
			wantInput := append([]byte(nil), input...)
			output[0] = '?'
			if !bytes.Equal(input, wantInput) {
				t.Fatal("mutating encoded output changed the caller's input")
			}
		})
	}
}

func TestCodecDecodeFailuresPreserveReceiver(t *testing.T) {
	for _, data := range []string{`{`, `[]`, `{"protocolState":"%%%"}`, `{"protocolState":42}`, `{"protocolState":{},"effects":[{"payload":{}},{"payload":"%%%"}]}`} {
		var value, want Payload
		if err := value.UnmarshalJSON([]byte(ownedCodecPayloadJSON)); err != nil {
			t.Fatal(err)
		}
		if err := want.UnmarshalJSON([]byte(ownedCodecPayloadJSON)); err != nil {
			t.Fatal(err)
		}
		if err := value.UnmarshalJSON([]byte(data)); err == nil || !reflect.DeepEqual(value, want) {
			t.Fatalf("failed payload decode changed the receiver: %v", err)
		}
	}
	for _, data := range []string{`{`, `[]`, `{"payload":"%%%"}`, `{"payload":42}`, `{"payload":{},"deadlineMs":"invalid"}`} {
		value := Effect{ID: "original", Payload: []byte(`{"old":true}`)}
		want := Effect{ID: "original", Payload: []byte(`{"old":true}`)}
		if err := value.UnmarshalJSON([]byte(data)); err == nil || !reflect.DeepEqual(value, want) {
			t.Fatalf("failed effect decode changed the receiver: %v", err)
		}
	}
}

func codecDecodeSeeds() [][]byte {
	seeds := [][]byte{nil, []byte(`null`), []byte(`{`), []byte(`[]`), []byte(`{"protocolState":"%%%","payload":42}`),
		[]byte(ownedCodecPayloadJSON),
		[]byte(`{"protocolState":{"x":1},"protocolState":[2],"effects":[{"payload":{}}],"effects":[{"payload":[]}],"payload":{},"payload":[3]}`)}
	for _, sample := range structuredValidationCases() {
		value := copiedCodecEncodeValue(sample.data)
		seeds = append(seeds, append(append([]byte(`{"protocolState":`), value...), '}'))
		seeds = append(seeds, append(append([]byte(`{"payload":`), value...), '}'))
	}
	return seeds
}

func TestCodecOwnershipMatchesCopyBasedOracle(t *testing.T) {
	for _, sample := range structuredValidationCases() {
		assertCopiedCodecEncoding(t, sample.data, sample.data)
	}
	for _, data := range codecDecodeSeeds() {
		assertCopiedCodecDecoding(t, data)
	}
}

func FuzzCodecOwnershipEncodingMatchesCopyBasedOracle(f *testing.F) {
	for _, sample := range structuredValidationCases() {
		f.Add(sample.data, sample.data)
	}
	f.Fuzz(func(t *testing.T, protocol, effect []byte) {
		if len(protocol) > 256<<10 || len(effect) > 256<<10 {
			t.Skip()
		}
		assertCopiedCodecEncoding(t, protocol, effect)
	})
}

func FuzzCodecOwnershipDecodingMatchesCopyBasedOracle(f *testing.F) {
	for _, data := range codecDecodeSeeds() {
		f.Add(data)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 256<<10 {
			t.Skip()
		}
		assertCopiedCodecDecoding(t, data)
	})
}
