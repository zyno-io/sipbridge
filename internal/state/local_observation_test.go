package state

import (
	"bytes"
	"encoding/json"
	"testing"
)

func TestLocalObservationsNeverPersist(t *testing.T) {
	payload := Payload{LocalObservations: []LocalObservation{{ContextID: "local-only-context", OperationID: "local-only-operation", Outcome: []byte("local-only-result")}}}
	encoded, err := payload.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	var decoded Payload
	if err := decoded.UnmarshalBinary(encoded); err != nil || len(decoded.LocalObservations) != 0 || bytes.Contains(encoded, []byte("local-only")) {
		t.Fatal("local handoff became retained protocol state", err)
	}
	text, err := json.Marshal(payload)
	if err != nil || bytes.Contains(text, []byte("local-only")) {
		t.Fatal("local handoff leaked into JSON state", err)
	}
}
