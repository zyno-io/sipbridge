package state

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"testing"
)

func TestStructuredStateEncodingAvoidsSecondBase64AndPreservesOpaqueValues(t *testing.T) {
	wire := []byte("OPTIONS sip:phone@example.test SIP/2.0\r\nContent-Length: 0\r\n\r\n")
	native, _ := json.Marshal(struct {
		Wire []byte `json:"wire"`
	}{wire})
	value := Payload{ProtocolState: native, Effects: []Effect{{ID: "effect", Kind: "wire", Status: EffectPending, DeadlineMS: 1, Payload: native}}}
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Count(encoded, []byte(base64.StdEncoding.EncodeToString(wire))) != 2 {
		t.Fatal("structured wire was encoded as opaque JSON/base64 twice")
	}
	var roundtrip Payload
	if err = json.Unmarshal(encoded, &roundtrip); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(roundtrip.ProtocolState, native) || !bytes.Equal(roundtrip.Effects[0].Payload, native) {
		t.Fatal("structured reducer bytes changed")
	}
	for _, opaque := range [][]byte{[]byte("not JSON"), []byte("{ \"x\": \"<\" }"), {0, 1, 255}} {
		original := Payload{ProtocolState: opaque, Effects: []Effect{{Payload: opaque}}}
		encoded, err = json.Marshal(original)
		if err != nil {
			t.Fatal(err)
		}
		if err = json.Unmarshal(encoded, &roundtrip); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(roundtrip.ProtocolState, opaque) || !bytes.Equal(roundtrip.Effects[0].Payload, opaque) {
			t.Fatal("opaque reducer value was normalized")
		}
	}
	old, _ := json.Marshal(map[string]interface{}{"protocolState": base64.StdEncoding.EncodeToString(native), "effects": []map[string]interface{}{{"payload": base64.StdEncoding.EncodeToString(native)}}})
	if err = json.Unmarshal(old, &roundtrip); err != nil || !bytes.Equal(roundtrip.ProtocolState, native) || !bytes.Equal(roundtrip.Effects[0].Payload, native) {
		t.Fatal("legacy opaque encoding could not be read")
	}
}
