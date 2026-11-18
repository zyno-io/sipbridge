package main

import (
	"bytes"
	"strings"
	"testing"

	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/nativepb"
	"sipbridge/internal/state"
	"sipbridge/internal/storagecodec"
)

func TestOfflineInspectionIsRedacted(t *testing.T) {
	value := state.Payload{Effects: []state.Effect{{ID: "secret-context-id", Kind: "credential", Status: state.EffectPending, DeadlineMS: 1, Payload: []byte("secret-password-and-wire")}}}
	data, err := value.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	if err := inspectHistorical(bytes.NewReader(data), &output); err != nil {
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
		if err := inspectHistorical(bytes.NewReader(sample), &output); err == nil || output.Len() != 0 {
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
	if err := inspectHistorical(bytes.NewReader(data), &output); err != nil {
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

func TestDefaultSelectedNativeInspectionRedactsAndRejectsCheckpoint(t *testing.T) {
	record := &nativepb.Context{Context: &pb.ContextRef{Id: "secret-context"}, Kind: pb.ContextSnapshot_TRANSACTION, CallId: "secret-call", LocalSdp: []byte("secret-sdp"), RemoteTarget: "sip:secret-target@example.test"}
	data, err := storagecodec.Marshal(storagecodec.NativeContext, record)
	if err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err = inspect(bytes.NewReader(data), &out); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(out.String(), "secret-") || !strings.Contains(out.String(), `"class":"context"`) || !strings.Contains(out.String(), `"nativeSchemaSha256"`) {
		t.Fatal("native summary leaked identities or omitted descriptor provenance")
	}
	old, err := (state.Payload{}).MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	for _, sample := range [][]byte{nil, []byte("{}"), old, data[:len(data)-1]} {
		out.Reset()
		if err = inspect(bytes.NewReader(sample), &out); err == nil || out.Len() != 0 {
			t.Fatal("default reader accepted legacy/partial record")
		}
	}
}
