package engine

import (
	"context"
	"testing"
	"time"

	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/state"
)

func TestDefaultMemoryAdmitsOutboundNativeCompletionProfile(t *testing.T) {
	f := newNativeOwnerFixtureBackend(t, state.MemoryBackend)
	placeholder := &pb.ContextRef{Owner: f.engine.cfg.Process, Id: "memory-outbound"}
	groupRef, err := f.engine.bootstrapGroup(f.app, placeholder)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.engine.apply(context.Background(), groupRef, "memory-bootstrap", stateEngineInput{Kind: "locate"}, time.Now().Add(2*time.Second), true); err != nil {
		t.Fatal(err)
	}
	record, err := f.engine.store.Load(context.Background(), groupRef)
	if err != nil {
		t.Fatal(err)
	}
	ref := f.engine.contextRef(record.Ref, placeholder.Id)
	result := f.apply(t, ref, "memory-outbound-open", stateEngineInput{
		Kind: "command", Application: f.app, ApplicationLeaseTerm: f.term,
		Command: &pb.Command{Context: ref, Sequence: 1, Admission: &pb.CommandAdmission{DirectoryEpoch: "directory", ExpiresAtMs: uint64(time.Now().Add(10 * time.Second).UnixMilli())},
			Action: &pb.Command_OpenUac{OpenUac: &pb.OpenUac{Uri: "sip:phone@example.test", Transport: "tls", Body: []byte("v=0\r\n"), Network: &pb.NetworkTarget{Transport: "tls"}}}},
	})
	if result.Certainty != pb.OperationResult_ACCEPTED || result.Snapshot == nil {
		t.Fatalf("default memory refused supported outbound INVITE before transport: %+v", result)
	}
	record, err = f.engine.store.Load(context.Background(), groupRef)
	if err != nil {
		t.Fatal(err)
	}
	if record.Payload.ReservedCompletionBytes <= 8<<20 || len(record.Payload.Effects) == 0 {
		t.Fatal("outbound INVITE did not reserve its mandatory native completion space")
	}
}
