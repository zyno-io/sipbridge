package engine

import (
	"bytes"
	"testing"

	"github.com/emiago/sipgo/sip/portable"
	"google.golang.org/protobuf/proto"
	"sipbridge/internal/storagepb"
)

func TestOpaqueMessageStoragePreservesExistingWireWithoutEagerSIPValidation(t *testing.T) {
	// This is the existing PortableMessage wire shape, deliberately containing
	// invalid SIP. Decoding an unrelated stored field must remain lazy.
	golden := []byte{0x0a, 0x03, 'b', 'a', 'd', 0x12, 0x03, 't', 'l', 's', 0x1a, 0x01, 's', 0x22, 0x01, 'd'}
	oracle := &storagepb.PortableMessage{Wire: []byte("bad"), Transport: "tls", Source: "s", Destination: "d"}
	fromProto := storageDecodePortableMessage(oracle)
	fromDirect, err := storageReadPortableMessage(golden)
	if err != nil {
		t.Fatal(err)
	}
	if !fromProto.Equal(fromDirect) {
		t.Fatal("direct and protobuf readers disagree")
	}
	for _, message := range []portable.Message{fromProto, fromDirect} {
		encoded, err := proto.MarshalOptions{Deterministic: true}.Marshal(storageEncodePortableMessage(message))
		if err != nil || !bytes.Equal(encoded, golden) {
			t.Fatalf("existing wire shape changed: %x, %v", encoded, err)
		}
		if message.Transport() != "tls" || message.Source() != "s" || message.Destination() != "d" {
			t.Fatal("stored observation metadata changed")
		}
	}
	// Neither decoder may retain the caller's mutable bytes.
	golden[2] = 'x'
	oracle.Wire[0] = 'y'
	if !fromProto.EqualWireBytes([]byte("bad")) || !fromDirect.EqualWireBytes([]byte("bad")) {
		t.Fatal("decoded message aliases storage input")
	}
	if _, err := fromDirect.Facts(); err == nil {
		t.Fatal("consuming invalid SIP must still fail")
	}
	if !proto.Equal(storageEncodePortableMessage(fromProto), storageEncodePortableMessage(fromDirect)) {
		t.Fatal("memoized validation changed serialized message identity")
	}
}

func TestOpaqueMessageStoragePreservesEmptyValueAndObservationFields(t *testing.T) {
	if !storageDecodePortableMessage(nil).IsZero() {
		t.Fatal("absent protobuf message must decode to the zero native value")
	}
	for _, wire := range [][]byte{nil, {}, []byte("payload")} {
		message := portable.RestoreMessage(wire, "udp", "source", "destination")
		body, err := proto.MarshalOptions{Deterministic: true}.Marshal(storageEncodePortableMessage(message))
		if err != nil {
			t.Fatal(err)
		}
		var oracle storagepb.PortableMessage
		if err := proto.Unmarshal(body, &oracle); err != nil {
			t.Fatal(err)
		}
		fromProto := storageDecodePortableMessage(&oracle)
		fromDirect, err := storageReadPortableMessage(body)
		if err != nil {
			t.Fatal(err)
		}
		if !fromProto.Equal(fromDirect) || !bytes.Equal(fromDirect.WireBytes(), wire) ||
			fromDirect.Transport() != "udp" || fromDirect.Source() != "source" || fromDirect.Destination() != "destination" {
			t.Fatal("opaque conversion changed existing bytes/observation semantics")
		}
	}
}
