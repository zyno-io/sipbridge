package engine

import (
	"testing"

	"github.com/google/uuid"
	pb "sipbridge/internal/controlpb"
)

func TestNativeWorkerReferenceUsesWholeProcessLifetime(t *testing.T) {
	first := &pb.ProcessRef{InstanceId: "sipbridge-reg-0", Epoch: "epoch:one"}
	a, err := nativeWorkerReference(first)
	if err != nil {
		t.Fatal(err)
	}
	again, err := nativeWorkerReference(first)
	if err != nil || a != again {
		t.Fatal("worker reference unstable", err)
	}
	changed, err := nativeWorkerReference(&pb.ProcessRef{InstanceId: first.InstanceId, Epoch: "epoch:two"})
	if err != nil || a == changed {
		t.Fatal("restarted worker reused index", err)
	}
	other, err := nativeWorkerReference(&pb.ProcessRef{InstanceId: "sipbridge-reg-1", Epoch: first.Epoch})
	if err != nil || a == other {
		t.Fatal("distinct process reused index", err)
	}
	parsed, err := uuid.Parse(a)
	if err != nil || parsed.Version() != 8 || parsed.Variant() != uuid.RFC4122 {
		t.Fatal("worker ID is not UUIDv8", err)
	}
	if _, err = nativeWorkerReference(&pb.ProcessRef{InstanceId: "only"}); err == nil {
		t.Fatal("missing epoch accepted")
	}
}
