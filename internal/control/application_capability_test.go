package control

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	pb "sipbridge/internal/controlpb"
)

func TestApplicationCapabilityUsesOriginalProofBound(t *testing.T) {
	app := &pb.ProcessRef{InstanceId: "application", Epoch: "one"}
	lease := &pb.ApplicationLease{Application: app, Term: "term", CheckedAtMs: 1000, ExpiresAtMs: 31000}
	started := time.Now().Add(-31 * time.Second)
	capability := newApplicationCapability(lease, started, nil)
	if !capability.until.Equal(started.Add(30*time.Second)) || !errors.Is(capability.Guard(app, "term", nil), ErrApplicationLeaseFenced) {
		t.Fatal("a slow lease reply extended its original conservative proof")
	}
	if err := capability.renew(lease, time.Now()); !errors.Is(err, ErrApplicationLeaseFenced) {
		t.Fatal("an expired local capability was revived", err)
	}
}

func TestApplicationCapabilityReleaseSerializesPublication(t *testing.T) {
	app := &pb.ProcessRef{InstanceId: "application", Epoch: "one"}
	lease := &pb.ApplicationLease{Application: app, Term: "term", CheckedAtMs: 1000, ExpiresAtMs: 31000}
	var published, released atomic.Bool
	capability := newApplicationCapability(lease, time.Now(), func(actual *ApplicationCapability) {
		if !published.Load() || actual.Term() != "term" || actual.Application().Epoch != "one" {
			t.Error("release missed the exact session's committed publication")
		}
		released.Store(true)
	})
	entered, proceed, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		if err := capability.Guard(app, "term", func(time.Time) error {
			close(entered)
			<-proceed
			published.Store(true)
			return nil
		}); err != nil {
			t.Error(err)
		}
	}()
	<-entered
	releaseDone := make(chan struct{})
	go func() { capability.release(); close(releaseDone) }()
	if released.Load() {
		t.Fatal("release passed an in-flight publication")
	}
	close(proceed)
	<-done
	<-releaseDone
	if !released.Load() || !errors.Is(capability.Guard(app, "term", nil), ErrApplicationLeaseFenced) || !errors.Is(capability.renew(lease, time.Now()), ErrApplicationLeaseFenced) {
		t.Fatal("released session admitted or renewed local authority")
	}
}

func TestApplicationCapabilityNaturalBoundUsesExistingHeartbeatRelease(t *testing.T) {
	pki := newPKI(t)
	app := &pb.ProcessRef{InstanceId: "controller", Epoch: "one"}
	authority := &pb.StateAuthority{Service: "test", Generation: "one", PrimaryId: "primary", SchemaVersion: 1}
	caps := []string{"sip.v1", StateCapability, NativeCompletionCapability}
	server, address := testServer(t, pki, 8, func(cfg *Config) {
		cfg.StateAuthority, cfg.Capabilities = authority, caps
		cfg.HeartbeatInterval, cfg.HeartbeatTimeout = 5*time.Millisecond, 6*time.Second
		cfg.OnApplicationLease = func(_ context.Context, actual *pb.ProcessRef, _ string) (*pb.ApplicationLease, error) {
			return &pb.ApplicationLease{Application: actual, Authority: authority, Term: "term", CheckedAtMs: 1000, ExpiresAtMs: 31000}, nil
		}
	})
	released := make(chan *ApplicationCapability, 1)
	if err := server.SetApplicationCapabilityRelease(func(capability *ApplicationCapability) {
		// This would deadlock if release held the publication lock.
		if !errors.Is(capability.Guard(app, "term", nil), ErrApplicationLeaseFenced) {
			t.Error("release callback preceded invalidation")
		}
		released <- capability
	}); err != nil {
		t.Fatal(err)
	}
	conn, err := pki.dial(address)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.CloseNow()
	send(t, conn, &pb.Envelope{CorrelationId: "hello", Payload: &pb.Envelope_Hello{Hello: &pb.SessionHello{Controller: app, ConnectionAttempt: 1, Major: 1, Capabilities: caps, Bounds: &pb.Bounds{MaxEnvelopeBytes: 4096}}}})
	if reply, err := receive(conn); err != nil || reply.GetHelloAck() == nil {
		t.Fatal("hello", err)
	}
	peer := waitPublicationPeer(t, server, app)
	capability := peer.ApplicationCapability(server)
	capability.mu.Lock()
	capability.until = time.Now().Add(10 * time.Millisecond)
	capability.mu.Unlock()
	select {
	case actual := <-released:
		if actual != capability {
			t.Fatal("another session released")
		}
	case <-time.After(time.Second):
		t.Fatal("natural proof expiry did not release local owners")
	}
	if peer.Context().Err() != nil {
		t.Fatal("local capability expiry invented a transport failure")
	}
}
