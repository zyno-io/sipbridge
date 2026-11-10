package control

import (
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
)

func TestStateProfileRejectsIncompatiblePeerBeforeSessionClaim(t *testing.T) {
	pki := newPKI(t)
	authority := &pb.StateAuthority{Service: "sip", Generation: "generation", PrimaryId: "primary", SchemaVersion: 1}
	server, address := testServer(t, pki, 8, func(cfg *Config) {
		cfg.Capabilities = []string{"sip.v1", StateCapability}
		cfg.StateAuthority = authority
		cfg.OnStatus = func(*Peer, *pb.SessionStatus) error { return nil }
		cfg.HeartbeatInterval = time.Second
		cfg.HeartbeatTimeout = 6 * time.Second
	})
	compatible, err := pki.dial(address)
	if err != nil {
		t.Fatal(err)
	}
	defer compatible.CloseNow()
	send(t, compatible, &pb.Envelope{CorrelationId: "hello", Payload: &pb.Envelope_Hello{Hello: &pb.SessionHello{
		Controller: &pb.ProcessRef{InstanceId: "controller", Epoch: "epoch"}, ConnectionAttempt: 1, Major: 1,
		Capabilities: []string{"sip.v1", StateCapability}, Bounds: &pb.Bounds{MaxEnvelopeBytes: 4096},
	}}})
	reply, err := receive(compatible)
	if err != nil {
		t.Fatal(err)
	}
	ack := reply.GetHelloAck()
	if ack == nil || !proto.Equal(ack.StateAuthority, authority) {
		t.Fatal("compatible peer did not negotiate the state authority", reply)
	}
	send(t, compatible, &pb.Envelope{CorrelationId: "ready", SessionId: ack.SessionId, Payload: &pb.Envelope_Status{Status: &pb.SessionStatus{RetainedReady: true, BindingsReady: true}}})
	roundtrip(t, compatible, ack.SessionId)
	if len(server.ReadyPeers()) != 1 {
		t.Fatal("compatible controller is not ready")
	}
	for _, capabilities := range [][]string{{"sip.v1"}, {StateCapability}, nil} {
		incompatible, err := pki.dial(address)
		if err != nil {
			t.Fatal(err)
		}
		send(t, incompatible, &pb.Envelope{CorrelationId: "old", Payload: &pb.Envelope_Hello{Hello: &pb.SessionHello{
			Controller: &pb.ProcessRef{InstanceId: "controller", Epoch: "epoch"}, ConnectionAttempt: 2, Major: 1,
			Capabilities: capabilities, Bounds: &pb.Bounds{MaxEnvelopeBytes: 4096},
		}}})
		if response, err := receive(incompatible); err == nil {
			t.Fatal("incompatible peer accepted", response)
		}
		incompatible.CloseNow()
		roundtrip(t, compatible, ack.SessionId)
		if len(server.ReadyPeers()) != 1 {
			t.Fatal("incompatible hello displaced the ready controller")
		}
	}
}
