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

// Workers must agree on call abandonment and reverse selector encoding.
func TestParkedCallAbandonmentChangesBaselineProfile(t *testing.T) {
	binding, err := BindProfile(profileFixture())
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprintf("%x", binding.Value) == "96d1d72378690958fe4a6f0aadc0da6b55462b020bc303593ed77e72c97d1fd9" ||
		binding.Authority.PrimaryId == "9d212cd50187f9a5fd94e4fd39c8a8e7e64c31bf" {
		t.Fatal("automatic retirement retained the previous profile or authority")
	}
	for _, check := range []struct{ name, got, want string }{
		{"selected schema digest", fmt.Sprintf("%x", selectedSchemaDigest), "29e7076b73166bd7e764425a88994ff57027c6231c44b5f975708c1418f57f55"},
		{"profile", fmt.Sprintf("%x", binding.Value), "10704423a2c3edac29c5b4cd98c2f3027c52c78f090095a03dae6f3b4dea8a82"},
		{"StateAuthority.PrimaryId", binding.Authority.PrimaryId, "8d4d43b1fbbb349151e45874127fb2944b71c960"},
	} {
		if check.got != check.want {
			t.Errorf("%s changed: got %s, want %s", check.name, check.got, check.want)
		}
		t.Logf("%s: %s", check.name, check.got)
	}
}
