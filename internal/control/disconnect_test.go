package control

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	pb "sipbridge/internal/controlpb"
)

func TestDisconnectRunsOnceAfterEstablishedSessionRelease(t *testing.T) {
	pki := newPKI(t)
	closed := make(chan *Peer, 2)
	var server *Server
	var address string
	application := &pb.ProcessRef{InstanceId: "controller", Epoch: "one"}
	server, address = testServer(t, pki, 8, func(cfg *Config) {
		cfg.Capabilities = []string{"sip.v1"}
		cfg.OnDisconnect = func(peer *Peer) {
			// This also checks the callback is outside the server lock.
			if current, err := server.Peer(application); err == nil || current != nil {
				t.Error("disconnect callback ran before claim release")
			}
			if peer.Context().Err() == nil {
				t.Error("disconnect callback retained a live session context")
			}
			closed <- peer
		}
	})
	conn, err := pki.dial(address)
	if err != nil {
		t.Fatal(err)
	}
	sendPublicationHello(t, conn, server, application, 1)
	if ack, err := receive(conn); err != nil || ack.GetHelloAck() == nil {
		t.Fatal("hello", err)
	}
	conn.CloseNow()
	select {
	case peer := <-closed:
		if peer.Process().InstanceId != application.InstanceId || peer.Process().Epoch != application.Epoch {
			t.Fatal("disconnect lost authenticated controller identity")
		}
	case <-time.After(time.Second):
		t.Fatal("established disconnect callback missing")
	}
	server.Close()
	select {
	case <-closed:
		t.Fatal("disconnect callback repeated")
	default:
	}
}

func TestNativeSessionCannotReopenBeforeDisconnectFinishes(t *testing.T) {
	pki := newPKI(t)
	authority := &pb.StateAuthority{Service: "edge", Generation: "one", PrimaryId: "primary", SchemaVersion: 1}
	application := &pb.ProcessRef{InstanceId: "controller", Epoch: "one"}
	entered, release := make(chan struct{}, 1), make(chan struct{})
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	var leases atomic.Uint64
	server, address := testServer(t, pki, 8, func(cfg *Config) {
		cfg.Capabilities = []string{"sip.v1", StateCapability, NativeCompletionCapability}
		cfg.StateAuthority = authority
		cfg.OnApplicationLease = func(_ context.Context, app *pb.ProcessRef, _ string) (*pb.ApplicationLease, error) {
			return &pb.ApplicationLease{Application: app, Authority: authority, Term: fmt.Sprintf("term%d", leases.Add(1)), CheckedAtMs: 1000, ExpiresAtMs: 31000}, nil
		}
		cfg.OnDisconnect = func(*Peer) {
			entered <- struct{}{}
			<-release
		}
	})
	first, err := pki.dial(address)
	if err != nil {
		t.Fatal(err)
	}
	defer first.CloseNow()
	sendPublicationHello(t, first, server, application, 1)
	if reply, err := receive(first); err != nil || reply.GetHelloAck() == nil {
		t.Fatal("initial hello", err)
	}
	refuse := func(attempt uint64) {
		t.Helper()
		conn, err := pki.dial(address)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.CloseNow()
		sendPublicationHello(t, conn, server, application, attempt)
		if _, err := receive(conn); err == nil {
			t.Fatal("new hello reused a still-owned native control claim")
		}
		if leases.Load() != 1 {
			t.Fatal("rejected hello reached lease acquisition")
		}
	}
	refuse(2)
	peer := waitPublicationPeer(t, server, application)
	if peer.Context().Err() != nil {
		t.Fatal("rejected hello displaced the live session", peer.Context().Err())
	}
	first.CloseNow()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("disconnect callback missing")
	}
	refuse(3)
	close(release)
	deadline := time.Now().Add(time.Second)
	for {
		server.mu.Lock()
		claim := server.claims[application.InstanceId]
		free := claim == nil || claim.current == nil
		server.mu.Unlock()
		if free {
			break
		}
		if !time.Now().Before(deadline) {
			t.Fatal("disconnect did not release native claim")
		}
		time.Sleep(time.Millisecond)
	}
	next, err := pki.dial(address)
	if err != nil {
		t.Fatal(err)
	}
	defer next.CloseNow()
	sendPublicationHello(t, next, server, application, 4)
	if reply, err := receive(next); err != nil || reply.GetHelloAck().GetApplicationLease().GetTerm() != "term2" {
		t.Fatal("fresh session could not acquire a new term", err)
	}
}

func TestFailedHelloDoesNotTriggerDisconnect(t *testing.T) {
	pki := newPKI(t)
	closed := make(chan struct{}, 1)
	_, address := testServer(t, pki, 8, func(cfg *Config) {
		cfg.Capabilities = []string{"sip.v1"}
		cfg.OnDisconnect = func(*Peer) { closed <- struct{}{} }
	})
	conn, err := pki.dial(address)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.CloseNow()
	send(t, conn, &pb.Envelope{CorrelationId: "invalid", Payload: &pb.Envelope_Hello{Hello: &pb.SessionHello{
		Controller: &pb.ProcessRef{InstanceId: "controller", Epoch: "one"}, ConnectionAttempt: 1, Major: 2,
	}}})
	if _, err := receive(conn); err == nil {
		t.Fatal("invalid hello accepted")
	}
	select {
	case <-closed:
		t.Fatal("unestablished session expired application ownership")
	default:
	}
}
