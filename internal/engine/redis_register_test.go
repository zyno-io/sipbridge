package engine

import (
	"github.com/google/uuid"
	pb "sipbridge/internal/controlpb"
	"testing"
)

func TestNativeApplicationReferenceBindsWholeProcess(t *testing.T) {
	key := []byte("0123456789abcdef0123456789abcdef")
	a, err := nativeApplicationReference(key, &pb.ProcessRef{InstanceId: "ab", Epoch: "c"})
	if err != nil {
		t.Fatal(err)
	}
	again, _ := nativeApplicationReference(key, &pb.ProcessRef{InstanceId: "ab", Epoch: "c"})
	b, _ := nativeApplicationReference(key, &pb.ProcessRef{InstanceId: "a", Epoch: "bc"})
	other, _ := nativeApplicationReference([]byte("0123456789abcdef0123456789abcdeg"), &pb.ProcessRef{InstanceId: "ab", Epoch: "c"})
	if a != "04766936-b1de-804f-9bf4-25412e74f456" || a != again || a == b || a == other {
		t.Fatal("application reference did not bind the complete keyed tuple")
	}
	id, err := uuid.Parse(a)
	if err != nil || id.Version() != 8 || id.Variant() != uuid.RFC4122 {
		t.Fatal("invalid UUIDv8 reference", a, err)
	}
}
