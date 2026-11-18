package rediscore

import (
	"bytes"
	"fmt"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
)

func profileFixture() ProfileConfig {
	return ProfileConfig{Root: "sipstate:test:profile:", Service: "profile", Generation: "test-v1",
		IdentityKey: bytes.Repeat([]byte{0x17}, 32), RecordBudget: 1 << 20}
}

func TestProfileBindsSchemaNamespaceAndLimits(t *testing.T) {
	cfg := profileFixture()
	want, err := BindProfile(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if len(want.Value) != 32 || want.Authority.SchemaVersion != 1 || len(want.Authority.PrimaryId) != 40 {
		t.Fatal("invalid binary profile or public authority")
	}
	explicit := cfg
	explicit.NamespaceMaxContexts = DefaultNamespaceMaxContexts
	explicit.NamespaceMaxBytes = DefaultNamespaceMaxBytes
	again, err := BindProfile(explicit)
	if err != nil || !bytes.Equal(want.Value, again.Value) || !proto.Equal(want.Authority, again.Authority) {
		t.Fatal("normalized defaults change compatibility", err)
	}
	for _, change := range []struct {
		name  string
		apply func(*ProfileConfig)
	}{
		{"root", func(c *ProfileConfig) { c.Root = "sipstate:test:other:" }},
		{"service", func(c *ProfileConfig) { c.Service = "other" }},
		{"generation", func(c *ProfileConfig) { c.Generation = "test-v2" }},
		{"key", func(c *ProfileConfig) { c.IdentityKey = bytes.Repeat([]byte{0x18}, 32) }},
		{"context-record-budget", func(c *ProfileConfig) { c.RecordBudget++ }},
		{"namespace-contexts", func(c *ProfileConfig) { c.NamespaceMaxContexts = DefaultNamespaceMaxContexts + 1 }},
		{"namespace-bytes", func(c *ProfileConfig) { c.NamespaceMaxBytes = DefaultNamespaceMaxBytes + 1 }},
	} {
		t.Run(change.name, func(t *testing.T) {
			next := cfg
			change.apply(&next)
			got, err := BindProfile(next)
			if err != nil || bytes.Equal(want.Value, got.Value) || proto.Equal(want.Authority, got.Authority) {
				t.Fatal("configuration change retained authority", err)
			}
		})
	}
	// Returned values own their data; a caller cannot alter a later binding.
	want.Value[0] ^= 0xff
	want.Authority.PrimaryId = "changed"
	last, err := BindProfile(cfg)
	if err != nil || !bytes.Equal(last.Value, again.Value) || !proto.Equal(last.Authority, again.Authority) {
		t.Fatal("profile state escaped through a returned alias", err)
	}
}

func TestProfileRejectsInvalidBindings(t *testing.T) {
	for _, change := range []func(*ProfileConfig){
		func(c *ProfileConfig) { c.IdentityKey = []byte("short") },
		func(c *ProfileConfig) { c.Root = "sipstate:*:" },
		func(c *ProfileConfig) { c.Generation = "not a reference" },
		func(c *ProfileConfig) { c.NamespaceMaxContexts = -1 },
		func(c *ProfileConfig) { c.NamespaceMaxBytes = maxExactInteger + 1 },
		func(c *ProfileConfig) { c.RecordBudget = 0 },
	} {
		cfg := profileFixture()
		change(&cfg)
		if _, err := BindProfile(cfg); err == nil {
			t.Fatal("invalid profile accepted")
		}
	}
}

func TestSelectedSchemaDigestBindsFieldsNotJustVersion(t *testing.T) {
	makeRoot := func(number int32) protoreflect.MessageDescriptor {
		t.Helper()
		file, err := protodesc.NewFile(&descriptorpb.FileDescriptorProto{Name: proto.String("test-native-profile.proto"),
			Syntax: proto.String("proto3"), Package: proto.String("native.test"), MessageType: []*descriptorpb.DescriptorProto{{Name: proto.String("Record"),
				Field: []*descriptorpb.FieldDescriptorProto{{Name: proto.String("value"), Number: proto.Int32(number), Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(), Type: descriptorpb.FieldDescriptorProto_TYPE_BYTES.Enum()}}}}}, nil)
		if err != nil {
			t.Fatal(err)
		}
		return file.Messages().Get(0)
	}
	a, err := schemaGraphDigest([]protoreflect.MessageDescriptor{makeRoot(1)})
	if err != nil {
		t.Fatal(err)
	}
	b, err := schemaGraphDigest([]protoreflect.MessageDescriptor{makeRoot(2)})
	if err != nil || a == b {
		t.Fatal("field-number drift is invisible to the schema receipt", err)
	}
	repeated, err := schemaGraphDigest([]protoreflect.MessageDescriptor{makeRoot(1), makeRoot(1)})
	if err != nil || repeated != a {
		t.Fatal("root ordering or duplicate traversal changes the schema receipt", err)
	}
}

// Re-INVITE ACK record semantics require a new layout/profile and authority,
// even though the selected-record descriptor graph is unchanged from 88dab8d.
func TestReinviteACKChangesBaselineProfile(t *testing.T) {
	binding, err := BindProfile(profileFixture())
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprintf("%x", binding.Value) == "fa91b222f6bf61dcb83c38cd1d3d775069bbd158c0e6b9559939e4905cf65fe5" ||
		binding.Authority.PrimaryId == "cc398c08bb06525ff9dedab1f9df62c281cec9b3" {
		t.Fatal("incompatible re-INVITE ACK layout retained the 88dab8d profile or authority")
	}
	for _, check := range []struct{ name, got, want string }{
		{"selected schema digest", fmt.Sprintf("%x", selectedSchemaDigest), "3f9edbced4a0fdfe3930b3f96427170172d551e1fbd8df8a8fb76c04fc795cfc"},
		{"profile", fmt.Sprintf("%x", binding.Value), "8c63ab86e1989520fe5b3602fd1a5209f39ab3d0bb6ed1833a2fc9e0a9a09a6d"},
		{"StateAuthority.PrimaryId", binding.Authority.PrimaryId, "2279d8af6b2ad961491f9ffa37d23d326104cdac"},
	} {
		if check.got != check.want {
			t.Errorf("%s changed: got %s, want %s", check.name, check.got, check.want)
		}
		t.Logf("%s: %s", check.name, check.got)
	}
}
