package main

import (
	"bytes"
	"strings"
	"testing"

	"sipbridge/internal/state"
)

func TestOfflineInspectionIsRedacted(t *testing.T) {
	value := state.Payload{Effects: []state.Effect{{ID: "secret-context-id", Kind: "credential", Status: state.EffectPending, DeadlineMS: 1, Payload: []byte("secret-password-and-wire")}}}
	data, err := value.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	if err := inspect(bytes.NewReader(data), &output); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(output.String(), "secret-") || !strings.Contains(output.String(), "protobuf-v1") {
		t.Fatal("inspection leaked payload or omitted format")
	}
}
func TestOfflineInspectionRefusesJSONAndPartialNativeState(t *testing.T) {
	data, err := (state.Payload{ProtocolState: []byte(`{"version":1}`)}).MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	for _, sample := range [][]byte{nil, []byte("{}"), data} {
		var output bytes.Buffer
		if err := inspect(bytes.NewReader(sample), &output); err == nil || output.Len() != 0 {
			t.Fatal("invalid record produced an inspection")
		}
	}
}

func TestFixtureProjectionIsExplicitAndRefusesOldJSON(t *testing.T) {
	data, err := (state.Payload{}).MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	if err := inspectFixtureCompletion(bytes.NewReader(data), &output); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), "sipbridge.fixture.completion.v1") || !strings.Contains(output.String(), "protobuf-v1:") {
		t.Fatal("fixture projection omitted explicit provenance")
	}
	output.Reset()
	if err := inspect(bytes.NewReader(data), &output); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(output.String(), "fixture.completion") || strings.Contains(output.String(), "schemaIdentity") {
		t.Fatal("default inspection changed identifying mode")
	}
	output.Reset()
	if err := inspectFixtureCompletion(strings.NewReader(`{"protocolState":{}}`), &output); err == nil || output.Len() != 0 {
		t.Fatal("legacy JSON or partial fixture output accepted")
	}
}
