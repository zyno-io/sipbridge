package storagecodec

import (
	"bytes"
	"testing"

	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
	"sipbridge/internal/storagepb"
)

func TestValidatedFrameCallbackRunsOnlyAfterCompletePreflight(t *testing.T) {
	descriptor := (&storagepb.StateRetention{}).ProtoReflect().Descriptor()
	valid, err := Marshal(Payload, &storagepb.StateRetention{Terminal: true})
	if err != nil {
		t.Fatal(err)
	}
	for _, sample := range []struct {
		name  string
		data  []byte
		valid bool
	}{
		{"valid", valid, true},
		{"header", nil, false},
		{"kind", append([]byte{'S', 'I', 'P', 'B', 1, Group}, valid[6:]...), false},
		{"bool", append([]byte{'S', 'I', 'P', 'B', 1, Payload}, []byte{8, 2}...), false},
		{"duplicate", append(bytes.Clone(valid), valid[6:]...), false},
		{"size", make([]byte, MaxBytes+1), false},
	} {
		t.Run(sample.name, func(t *testing.T) {
			called := false
			err := DecodeValidatedFrame(sample.data, Payload, descriptor, func(body []byte) error {
				called = true
				if !bytes.Equal(body, sample.data[6:]) {
					t.Fatal("callback did not receive exact frame body")
				}
				return nil
			})
			if (err == nil) != sample.valid || called != sample.valid {
				t.Fatal("preflight/callback ordering changed", err, called)
			}
		})
	}
}

func TestValidatedFrameRetainsCanonicalDepthAndFieldBounds(t *testing.T) {
	file, err := protodesc.NewFile(&descriptorpb.FileDescriptorProto{
		Name: proto.String("direct-frame-bounds.proto"), Syntax: proto.String("proto3"), Package: proto.String("fixture"),
		MessageType: []*descriptorpb.DescriptorProto{{Name: proto.String("Node"), Field: []*descriptorpb.FieldDescriptorProto{{
			Name: proto.String("child"), Number: proto.Int32(1), Label: descriptorpb.FieldDescriptorProto_LABEL_REPEATED.Enum(),
			Type: descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum(), TypeName: proto.String(".fixture.Node"),
		}}}},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	descriptor := file.Messages().Get(0)
	wrap := func(body []byte) []byte {
		return protowire.AppendBytes(protowire.AppendTag(nil, 1, protowire.BytesType), body)
	}
	depth := []byte(nil)
	for n := 0; n < MaxDepth+1; n++ {
		depth = wrap(depth)
	}
	branch := bytes.Repeat(wrap(nil), MaxCollection)
	fields := bytes.Repeat(wrap(branch), MaxFields/MaxCollection+1)
	for _, sample := range []struct {
		name string
		body []byte
	}{{"depth", depth}, {"fields", fields}, {"collection", bytes.Repeat(wrap(nil), MaxCollection+1)}} {
		t.Run(sample.name, func(t *testing.T) {
			data := append([]byte{'S', 'I', 'P', 'B', 1, Group}, sample.body...)
			oldErr := Unmarshal(data, Group, dynamicpb.NewMessage(descriptor))
			called := false
			newErr := DecodeValidatedFrame(data, Group, descriptor, func([]byte) error { called = true; return nil })
			if oldErr == nil || newErr == nil || called {
				t.Fatal("bounded preflight was bypassed", oldErr, newErr, called)
			}
		})
	}
}
