package control

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
)

func TestNativeLeaseReceiptAndPinnedRenewal(t *testing.T) {
	application := &pb.ProcessRef{InstanceId: "controller", Epoch: "epoch"}
	authority := &pb.StateAuthority{Service: "edge", Generation: "generation", PrimaryId: "primary", SchemaVersion: 1}
	lease := &pb.ApplicationLease{Application: application, Authority: authority, Term: "term", CheckedAtMs: 1000, ExpiresAtMs: 31000}
	for _, duration := range []uint64{30000, 30250, 30500} {
		candidate := proto.Clone(lease).(*pb.ApplicationLease)
		candidate.ExpiresAtMs = candidate.CheckedAtMs + duration
		if !validApplicationLease(candidate, application, authority) {
			t.Fatalf("valid bounded monotonic renewal rejected: %d", duration)
		}
	}
	for _, duration := range []uint64{0, 29999, 30501} {
		candidate := proto.Clone(lease).(*pb.ApplicationLease)
		candidate.ExpiresAtMs = candidate.CheckedAtMs + duration
		if validApplicationLease(candidate, application, authority) {
			t.Fatalf("invalid lease horizon accepted: %d", duration)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	calls := 0
	s := &session{ctx: ctx, process: application, applicationLease: lease, server: &Server{cfg: Config{StateAuthority: authority}}}
	s.server.cfg.OnApplicationLease = func(_ context.Context, process *pb.ProcessRef, term string) (*pb.ApplicationLease, error) {
		calls++
		if !proto.Equal(process, application) || term != "term" {
			t.Fatal("renewal attempted to acquire or change application authority")
		}
		return proto.Clone(lease).(*pb.ApplicationLease), nil
	}
	if err := s.renewApplicationLease(); err != nil || calls != 1 {
		t.Fatalf("pinned renewal failed: %v, calls=%d", err, calls)
	}
	if err := s.renewApplicationLease(); err != nil || calls != 1 {
		t.Fatalf("renewal was not coalesced: %v, calls=%d", err, calls)
	}
	s.leaseRenewed = time.Now().Add(-3 * time.Second)
	s.server.cfg.OnApplicationLease = func(context.Context, *pb.ProcessRef, string) (*pb.ApplicationLease, error) {
		calls++
		return nil, errors.New("expired pinned term")
	}
	if err := s.renewApplicationLease(); err == nil || calls != 2 || s.applicationLease.Term != "term" {
		t.Fatal("expired renewal reacquired authority")
	}
	cancel()
	if err := s.renewApplicationLease(); err == nil || calls != 2 {
		t.Fatal("closed session renewed authority")
	}
}

func TestNativeCompletionNegotiatesLeaseBeforeBusinessTraffic(t *testing.T) {
	pki := newPKI(t)
	authority := &pb.StateAuthority{Service: "edge", Generation: "generation", PrimaryId: "primary", SchemaVersion: 1}
	caps := []string{"sip.v1", StateCapability, NativeCompletionCapability}
	server, address := testServer(t, pki, 8, func(cfg *Config) {
		cfg.Capabilities, cfg.StateAuthority = caps, authority
		cfg.HeartbeatInterval, cfg.HeartbeatTimeout = time.Second, 6*time.Second
		cfg.OnApplicationLease = func(_ context.Context, application *pb.ProcessRef, term string) (*pb.ApplicationLease, error) {
			if term != "" {
				t.Error("hello tried to renew an unbound term")
			}
			return &pb.ApplicationLease{Application: application, Authority: authority, Term: "native-term", CheckedAtMs: 1000, ExpiresAtMs: 31000}, nil
		}
	})
	conn, err := pki.dial(address)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.CloseNow()
	app := &pb.ProcessRef{InstanceId: "controller", Epoch: "epoch"}
	send(t, conn, &pb.Envelope{CorrelationId: "hello", Payload: &pb.Envelope_Hello{Hello: &pb.SessionHello{Controller: app, ConnectionAttempt: 1, Major: 1, Capabilities: caps, Bounds: &pb.Bounds{MaxEnvelopeBytes: 4096}}}})
	reply, err := receive(conn)
	if err != nil {
		t.Fatal(err)
	}
	ack := reply.GetHelloAck()
	if ack == nil || !validApplicationLease(ack.ApplicationLease, app, authority) {
		t.Fatal("hello did not bind exact native lease")
	}
	roundtrip(t, conn, ack.SessionId)
	peer, err := server.Peer(app)
	if err != nil {
		t.Fatal(err)
	}
	copy := peer.ApplicationLease()
	copy.Term = "tampered"
	if peer.ApplicationLease().Term != "native-term" {
		t.Fatal("caller mutated session authority")
	}
	old, err := pki.dial(address)
	if err != nil {
		t.Fatal(err)
	}
	defer old.CloseNow()
	send(t, old, &pb.Envelope{CorrelationId: "old", Payload: &pb.Envelope_Hello{Hello: &pb.SessionHello{Controller: app, ConnectionAttempt: 2, Major: 1, Capabilities: caps[:len(caps)-1], Bounds: &pb.Bounds{MaxEnvelopeBytes: 4096}}}})
	if response, err := receive(old); err == nil {
		t.Fatalf("incompatible peer displaced owner: %v", response)
	}
	roundtrip(t, conn, ack.SessionId)
}

func TestNativeControlHeartbeatsSurviveLeaseStoreOutage(t *testing.T) {
	pki := newPKI(t)
	authority := &pb.StateAuthority{Service: "edge", Generation: "generation", PrimaryId: "logical", SchemaVersion: 1}
	caps := []string{"sip.v1", StateCapability, NativeCompletionCapability}
	var failures atomic.Uint64
	var recovered atomic.Bool
	server, address := testServer(t, pki, 8, func(cfg *Config) {
		cfg.Capabilities, cfg.StateAuthority = caps, authority
		cfg.HeartbeatInterval, cfg.HeartbeatTimeout = 50*time.Millisecond, time.Second
		cfg.OnApplicationLease = func(_ context.Context, application *pb.ProcessRef, term string) (*pb.ApplicationLease, error) {
			if term != "" && !recovered.Load() {
				failures.Add(1)
				return nil, errors.New("Redis temporarily unavailable")
			}
			return &pb.ApplicationLease{Application: application, Authority: authority, Term: "same-original-term", CheckedAtMs: 1000, ExpiresAtMs: 31000}, nil
		}
	})
	connection, err := pki.dial(address)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.CloseNow()
	application := &pb.ProcessRef{InstanceId: "controller", Epoch: "surviving-process"}
	send(t, connection, &pb.Envelope{CorrelationId: "hello", Payload: &pb.Envelope_Hello{Hello: &pb.SessionHello{Controller: application, ConnectionAttempt: 1, Major: 1, Capabilities: caps, Bounds: &pb.Bounds{MaxEnvelopeBytes: 4096}}}})
	reply, err := receive(connection)
	if err != nil {
		t.Fatal(err)
	}
	ack := reply.GetHelloAck()
	if ack == nil {
		t.Fatal("missing hello")
	}
	until := time.Now().Add(5 * time.Second)
	for failures.Load() < 3 && time.Now().Before(until) {
		envelope, err := receive(connection)
		if err != nil {
			t.Fatal("healthy control socket closed during store outage", err)
		}
		if ping := envelope.GetPing(); ping != nil {
			send(t, connection, &pb.Envelope{SessionId: ack.SessionId, CorrelationId: envelope.CorrelationId, Payload: &pb.Envelope_Pong{Pong: ping}})
		}
	}
	if failures.Load() < 3 {
		t.Fatal("test did not exercise failed lease renewal")
	}
	recovered.Store(true)
	roundtrip(t, connection, ack.SessionId)
	peer, err := server.Peer(application)
	if err != nil {
		t.Fatal(err)
	}
	if peer.ApplicationLease().Term != "same-original-term" {
		t.Fatal("outage replaced the original owner term")
	}
}
