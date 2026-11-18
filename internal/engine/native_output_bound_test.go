package engine

import (
	"net"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/nativepb"
)

// This producer-shape ceiling is not the native decoder's acceptance limit.
// The drain flags are deliberately all set to bound even mutually exclusive
// combinations. Context owner is a locally validated ProcessRef.
func TestNativeOutputProducerShapeSize(t *testing.T) {
	ref := strings.Repeat("r", 96)
	text := strings.Repeat("x", 1024)
	ip := "ffff:ffff:ffff:ffff:ffff:ffff:255.255.255.255"
	if net.ParseIP(ip) == nil {
		t.Fatal("maximum textual IP fixture is invalid")
	}
	process := &pb.ProcessRef{InstanceId: ref, Epoch: ref}
	authority := &pb.StateAuthority{Service: ref, Generation: ref, PrimaryId: ref, SchemaVersion: ^uint32(0)}
	context := &pb.ContextRef{Owner: process, Id: ref, State: &pb.StateRef{Authority: authority, GroupId: ref, Incarnation: ref, CallId: text, Partition: 4095}}
	flow := &pb.FlowRef{Ingress: process, ConnectionId: ref, SourceIp: ip, SourcePort: 65535, Listener: text, Transport: "udp"}
	effect := &pb.StateEffectRef{Context: context, AttemptId: ref}
	auth := &pb.EffectAuthorization{Effect: effect, AuthorityClass: pb.EffectAuthorization_RESPONSE, CompletionScope: strings.Repeat("c", 64), DeadlineMs: 1<<53 - 1, ApplicationLeaseTerm: ref}
	snapshot := &pb.ContextSnapshot{Context: context, Application: process, ApplicationLeaseTerm: ref, PrincipalId: strings.Repeat("p", 256), BindingId: strings.Repeat("b", 256), Transport: "udp"}
	fingerprint, err := SendFingerprint(snapshot, &pb.SipMessage{Method: strings.Repeat("M", 32), Status: 699, Wire: []byte("x")}, auth, flow)
	if err != nil {
		t.Fatal(err)
	}
	effect.Id = "e-" + fingerprint
	auth.Fingerprint = fingerprint
	write := &pb.FlowWrite{Flow: flow, Effect: effect, Permit: &pb.SendPermit{Authorization: auth, Flow: flow, DirectoryEpoch: ref, OrderingProof: strings.Repeat("o", 64), CompletionScope: auth.CompletionScope}}
	if _, err = SendFingerprint(snapshot, &pb.SipMessage{Method: strings.Repeat("M", 32), Status: 699, Wire: []byte("x")}, auth, flow); err != nil {
		t.Fatal(err)
	}
	field := func(prefix string) string { return "r:" + prefix + strings.Repeat("v", 191) }
	output := &nativepb.Output{Write: write, Procedure: field("p"), Result: field("r"), Dialog: field("d"), Replay: true, CleanupAfterAck: true, AckSequence: ^uint32(0), Operation: strings.Repeat("o", 160), Method: strings.Repeat("M", 32), Status: 699, Drain: &nativepb.DrainObservation{Transaction: make([]byte, 32), Ack: make([]byte, 32), Status: 65535, Request: true, Invite: true, IsAck: true, Cancel: true, Initial: true}}
	encoded, err := encodeNativeOutput(output)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("FlowWrite protobuf=%d Output protobuf=%d framed=%d", proto.Size(write), proto.Size(output), len(encoded))
	if len(encoded) != proto.Size(output)+6 {
		t.Fatal("unexpected native frame overhead")
	}
}
