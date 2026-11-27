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

// An additive control field still changes the selected native descriptor.
func TestNativeCaptureBaselineProfile(t *testing.T) {
	binding, err := BindProfile(profileFixture())
	if err != nil {
		t.Fatal(err)
	}
	profile := fmt.Sprintf("%x", binding.Value)
	authority := binding.Authority.PrimaryId
	if profile == "1f43d0d1f70fa9be83678531b2c73b3d977848e7c71cad5bad2ebe95bce0206e" ||
		authority == "260c54e6aaeab75e84bcde81822fe08aecedfde7" {
		t.Fatal("would alias the generation-3 profile or authority")
	}
	for _, check := range []struct{ name, got, want string }{
		{"selected schema digest", fmt.Sprintf("%x", selectedSchemaDigest), "86b2996411161ad63704073fbfa4f978c695d0720d3954015e56fad8af009d2a"},
		{"profile", profile, "43e91a44350fd39388865c0265d927576eee9d8a516c33b619d79b37b1b360c3"},
		{"StateAuthority.PrimaryId", authority, "631f6ffecaee84f1a9b31ce2d8cf03f7e1f6cb1a"},
	} {
		if check.got != check.want {
			t.Errorf("%s changed: got %s, want %s", check.name, check.got, check.want)
		}
		t.Logf("%s: %s", check.name, check.got)
	}
}
