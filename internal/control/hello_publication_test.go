package control

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	pb "sipbridge/internal/controlpb"
)

func sendPublicationHello(t *testing.T, conn *websocket.Conn, server *Server, application *pb.ProcessRef, attempt uint64) {
	t.Helper()
	send(t, conn, &pb.Envelope{CorrelationId: "hello", Payload: &pb.Envelope_Hello{Hello: &pb.SessionHello{
		Controller: application, ConnectionAttempt: attempt, Major: 1, Capabilities: server.cfg.Capabilities,
		Bounds: &pb.Bounds{MaxEnvelopeBytes: 4096},
	}}})
}

func waitPublicationPeer(t *testing.T, server *Server, application *pb.ProcessRef) *Peer {
	t.Helper()
	until := time.NewTimer(time.Second)
	defer until.Stop()
	poll := time.NewTicker(time.Millisecond)
	defer poll.Stop()
	for {
		if peer, err := server.Peer(application); err == nil {
			return peer
		}
		select {
		case <-poll.C:
		case <-until.C:
			t.Fatal("acknowledged session was not published")
			return nil
		}
	}
}

func TestHelloPublicationWaitsForLeaseAndAcknowledgement(t *testing.T) {
	pki := newPKI(t)
	authority := &pb.StateAuthority{Service: "edge", Generation: "generation", PrimaryId: "primary", SchemaVersion: 1}
	entered := make(chan uint64, 2)
	releases := []chan struct{}{make(chan struct{}), make(chan struct{})}
	defer func() {
		for _, release := range releases {
			select {
			case <-release:
			default:
				close(release)
			}
		}
	}()
	var calls atomic.Uint64
	server, address := testServer(t, pki, 8, func(cfg *Config) {
		cfg.Capabilities = []string{"sip.v1", StateCapability, NativeCompletionCapability}
		cfg.StateAuthority = authority
		cfg.HelloTimeout = 2 * time.Second
		cfg.HeartbeatInterval, cfg.HeartbeatTimeout = time.Minute, 2*time.Minute
		cfg.OnStatus = func(*Peer, *pb.SessionStatus) error { return nil }
		cfg.OnApplicationLease = func(ctx context.Context, application *pb.ProcessRef, term string) (*pb.ApplicationLease, error) {
			attempt := calls.Add(1)
			if term != "" || attempt > uint64(len(releases)) {
				return nil, errors.New("unexpected publication-test lease renewal")
			}
			entered <- attempt
			select {
			case <-releases[attempt-1]:
				return &pb.ApplicationLease{Application: application, Authority: authority, Term: "native-term", CheckedAtMs: 1000, ExpiresAtMs: 31000}, nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
	})
	application := &pb.ProcessRef{InstanceId: "controller", Epoch: "same-live-process"}
	var previous *Peer
	for attempt := uint64(1); attempt <= 2; attempt++ {
		conn, err := pki.dial(address)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.CloseNow()
		sendPublicationHello(t, conn, server, application, attempt)
		select {
		case observed := <-entered:
			if observed != attempt {
				t.Fatal("unexpected lease acquisition attempt")
			}
		case <-time.After(time.Second):
			t.Fatal("hello did not reserve its claim before lease acquisition")
		}
		if previous != nil && previous.Context().Err() == nil {
			t.Fatal("higher attempt did not fence the preceding peer while its lease was pending")
		}
		if _, err := server.Peer(application); err == nil {
			t.Fatal("peer was published before lease acquisition and HelloAck")
		}
		if len(server.ReadyPeers()) != 0 {
			t.Fatal("pending hello remained eligible for ready-peer selection")
		}
		close(releases[attempt-1])
		peer := waitPublicationPeer(t, server, application)
		// Race application traffic with the client's first read. Publication must
		// prove that HelloAck already won the physical writer.
		if err := peer.Send(&pb.Envelope{CorrelationId: "published", Payload: &pb.Envelope_Ping{Ping: &pb.Heartbeat{Nonce: "published"}}}); err != nil {
			t.Fatal(err)
		}
		first, err := receive(conn)
		if err != nil {
			t.Fatal(err)
		}
		ack := first.GetHelloAck()
		if ack == nil || first.CorrelationId != "hello" || first.SessionId != ack.SessionId || !validApplicationLease(ack.ApplicationLease, application, authority) {
			t.Fatal("first wire frame was not the exact acknowledged native hello")
		}
		traffic, err := receive(conn)
		if err != nil || traffic.GetPing().GetNonce() != "published" || traffic.CorrelationId != "published" || traffic.SessionId != ack.SessionId {
			t.Fatalf("published peer did not deliver its exact application frame after HelloAck: %v", err)
		}
		send(t, conn, &pb.Envelope{SessionId: ack.SessionId, CorrelationId: "ready", Payload: &pb.Envelope_Status{Status: &pb.SessionStatus{RetainedReady: true, BindingsReady: true}}})
		send(t, conn, &pb.Envelope{SessionId: ack.SessionId, CorrelationId: "barrier", Payload: &pb.Envelope_Ping{Ping: &pb.Heartbeat{Nonce: "barrier"}}})
		barrier, err := receive(conn)
		if err != nil || barrier.GetPong().GetNonce() != "barrier" || barrier.CorrelationId != "barrier" {
			t.Fatalf("unexpected frame at ready-publication barrier: %v", err)
		}
		if ready := server.ReadyPeers(); len(ready) != 1 || ready[0].Context() != peer.Context() {
			t.Fatal("established ready peer was not selectable")
		}
		previous = peer
	}
}

func TestFailedHelloLeaseNeverPublishesPeer(t *testing.T) {
	pki := newPKI(t)
	entered, release := make(chan struct{}), make(chan struct{})
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	server, address := testServer(t, pki, 8, func(cfg *Config) {
		cfg.Capabilities = []string{"sip.v1", StateCapability, NativeCompletionCapability}
		cfg.StateAuthority = &pb.StateAuthority{Service: "edge", Generation: "generation", PrimaryId: "primary", SchemaVersion: 1}
		cfg.HelloTimeout = 2 * time.Second
		cfg.OnApplicationLease = func(ctx context.Context, _ *pb.ProcessRef, _ string) (*pb.ApplicationLease, error) {
			close(entered)
			select {
			case <-release:
				return nil, errors.New("test lease unavailable")
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
	})
	conn, err := pki.dial(address)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.CloseNow()
	application := &pb.ProcessRef{InstanceId: "controller", Epoch: "lease-refused"}
	sendPublicationHello(t, conn, server, application, 1)
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("lease acquisition did not start")
	}
	if _, err := server.Peer(application); err == nil || len(server.ReadyPeers()) != 0 {
		t.Fatal("pending lease exposed a peer")
	}
	close(release)
	if frame, err := receive(conn); err == nil {
		t.Fatalf("failed lease emitted an unexpected wire frame: %T", frame.Payload)
	} else if errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("failed lease did not close the handshake")
	}
	if _, err := server.Peer(application); err == nil || len(server.ReadyPeers()) != 0 {
		t.Fatal("failed hello left a selectable peer")
	}
}
