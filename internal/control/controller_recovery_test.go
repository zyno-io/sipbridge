package control

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
)

func TestControllerRecoveryHooksBindStableOwnerAndGateFreshWork(t *testing.T) {
	pki := newPKI(t)
	stable := &pb.ProcessRef{InstanceId: "first-controller", Epoch: "first-process"}
	actual := &pb.ProcessRef{InstanceId: "replacement-controller", Epoch: "replacement-process"}
	authority := &pb.StateAuthority{Service: "edge", Generation: "generation", PrimaryId: "primary", SchemaVersion: 1}
	lease := &pb.ApplicationLease{Application: stable, Holder: actual, Authority: authority, Term: "owner-term", AttachmentGeneration: 7,
		CheckedAtMs: 1000, ExpiresAtMs: 31000, LeaseLifetimeMs: 30000, ReattachTimeoutMs: 30000, ReattachUntilMs: 50000, RecoveryToken: strings.Repeat("synthetic-secret-", 3)}
	caps := []string{"sip.v1", StateCapability, NativeCompletionCapability, ControllerReattachCapability}
	var commands, renewals atomic.Int32
	server, address := testServer(t, pki, 8, func(cfg *Config) {
		cfg.Capabilities, cfg.StateAuthority = caps, authority
		cfg.HeartbeatInterval, cfg.HeartbeatTimeout = time.Second, 6*time.Second
		cfg.OnControllerSession = func(_ context.Context, peer *Peer, hello *pb.SessionHello) (*pb.ApplicationLease, error) {
			if peer.Principal() != "spiffe://test/controller" || !proto.Equal(peer.Process(), actual) || !proto.Equal(hello.Recovery.Application, stable) {
				t.Error("recovery hook lost authenticated holder/principal")
			}
			return proto.Clone(lease).(*pb.ApplicationLease), nil
		}
		cfg.OnControllerRenew = func(_ context.Context, peer *Peer) (*pb.ApplicationLease, error) {
			if !proto.Equal(peer.Application(), stable) || !proto.Equal(peer.Process(), actual) || peer.ApplicationGeneration() != 7 {
				t.Error("renewal changed owner lineage")
			}
			renewals.Add(1)
			return proto.Clone(lease).(*pb.ApplicationLease), nil
		}
		cfg.OnStatus = func(*Peer, *pb.SessionStatus) error { return nil }
		cfg.OnMessage = func(peer *Peer, e *pb.Envelope) error {
			if e.GetCommand() != nil {
				commands.Add(1)
			}
			return peer.Send(&pb.Envelope{CorrelationId: e.CorrelationId, Payload: &pb.Envelope_Result{Result: &pb.OperationResult{Certainty: pb.OperationResult_COMPLETED}}})
		}
	})
	connection, err := pki.dial(address)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.CloseNow()
	send(t, connection, &pb.Envelope{CorrelationId: "hello", Payload: &pb.Envelope_Hello{Hello: &pb.SessionHello{Controller: actual, ConnectionAttempt: 1, Major: 1, Bounds: &pb.Bounds{MaxEnvelopeBytes: 8192}, Capabilities: caps,
		Recovery: &pb.ControllerRecovery{Application: stable, Term: lease.Term, Token: lease.RecoveryToken, AttemptId: "attach-once", AttachmentGeneration: 6}}}})
	ack, err := receive(connection)
	if err != nil || ack.GetHelloAck() == nil {
		t.Fatalf("recovery hello failed: %v", err)
	}
	if !proto.Equal(ack.GetHelloAck().ApplicationLease, lease) {
		t.Fatal("recovery receipt changed")
	}
	// The client can receive hello acknowledgement before the server finishes
	// installing its peer. An allowed inspection round trip observes that setup
	// boundary without assuming the two processes run in lockstep.
	send(t, connection, &pb.Envelope{CorrelationId: "inspect", SessionId: ack.SessionId, RemainingBudgetMs: 1000, Payload: &pb.Envelope_Inspect{Inspect: &pb.ContextInspect{Context: &pb.ContextRef{Owner: server.cfg.Process, Id: "context"}}}})
	inspection, err := receive(connection)
	if err != nil || inspection.GetResult().GetCertainty() != pb.OperationResult_COMPLETED {
		t.Fatal("inspection was blocked during recovery")
	}
	peer, err := server.Peer(stable)
	if err != nil || !proto.Equal(peer.Process(), actual) || peer.ApplicationGeneration() != 7 {
		t.Fatal("routing cannot find reattached stable application")
	}
	if err := peer.ResumeApplicationLease(); err != nil || renewals.Load() != 1 {
		t.Fatalf("renewal hook failed: %v", err)
	}
	command := &pb.Envelope{CorrelationId: "command", SessionId: ack.SessionId, RemainingBudgetMs: 1000, Payload: &pb.Envelope_Command{Command: &pb.Command{
		Context: &pb.ContextRef{Owner: server.cfg.Process, Id: "context"}, Sequence: 1, Action: &pb.Command_Respond{Respond: &pb.Respond{RequestId: "request", Status: 180}},
	}}}
	send(t, connection, command)
	refused, err := receive(connection)
	if err != nil || refused.GetResult().GetCertainty() != pb.OperationResult_NOT_ACCEPTED || commands.Load() != 0 {
		t.Fatal("fresh mutation ran before restored readiness")
	}
	send(t, connection, &pb.Envelope{CorrelationId: "ready", SessionId: ack.SessionId, Payload: &pb.Envelope_Status{Status: &pb.SessionStatus{RetainedReady: true, BindingsReady: true}}})
	send(t, connection, command)
	completed, err := receive(connection)
	if err != nil || completed.GetResult().GetCertainty() != pb.OperationResult_COMPLETED || commands.Load() != 1 {
		t.Fatal("restored controller cannot issue commands")
	}
}

func TestRecoveryCapabilityRequiresImplementedHooks(t *testing.T) {
	pki := newPKI(t)
	material, err := NewTLSContext(pki.files)
	if err != nil {
		t.Fatal(err)
	}
	_, err = NewServer(Config{Process: &pb.ProcessRef{InstanceId: "sip", Epoch: "epoch"}, TLS: material, AllowedURIs: []string{"spiffe://test/controller"}, Capabilities: []string{ControllerReattachCapability}})
	if err == nil {
		t.Fatal("server advertised recovery without implementation hooks")
	}
}

func TestControlRejectsUnsupportedMajor(t *testing.T) {
	pki := newPKI(t)
	_, address := testServer(t, pki, 8)
	connection, err := pki.dial(address)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.CloseNow()
	send(t, connection, &pb.Envelope{CorrelationId: "unsupported", Payload: &pb.Envelope_Hello{Hello: &pb.SessionHello{Controller: &pb.ProcessRef{InstanceId: "controller", Epoch: "epoch"}, ConnectionAttempt: 1, Major: 2, Bounds: &pb.Bounds{MaxEnvelopeBytes: 4096}}}})
	if _, err := receive(connection); err == nil {
		t.Fatal("unsupported control major was accepted")
	}
}

func TestRecoverableLeaseMayBeClampedToOriginalRestoreDeadline(t *testing.T) {
	process := &pb.ProcessRef{InstanceId: "controller", Epoch: "epoch"}
	authority := &pb.StateAuthority{Service: "edge", Generation: "generation", PrimaryId: "primary", SchemaVersion: 1}
	session := &session{process: process, capabilities: []string{ControllerReattachCapability}}
	lease := &pb.ApplicationLease{Application: process, Holder: process, Authority: authority, Term: "term", AttachmentGeneration: 1,
		CheckedAtMs: 1000, ExpiresAtMs: 1500, LeaseLifetimeMs: 5000, ReattachTimeoutMs: 10000, ReattachUntilMs: 1500, RecoveryToken: strings.Repeat("synthetic-secret-", 3)}
	if !session.validLease(lease, authority, true) {
		t.Fatal("valid short lease at the original restoration deadline rejected")
	}
	lease.ExpiresAtMs = lease.CheckedAtMs
	if session.validLease(lease, authority, true) {
		t.Fatal("expired restoration lease accepted")
	}
}

func TestFailedControllerRecoveryDoesNotDisplacePublishedHolder(t *testing.T) {
	pki := newPKI(t)
	application := &pb.ProcessRef{InstanceId: "controller", Epoch: "holder-process"}
	authority := &pb.StateAuthority{Service: "edge", Generation: "generation", PrimaryId: "primary", SchemaVersion: 1}
	lease := &pb.ApplicationLease{Application: application, Holder: application, Authority: authority, Term: "term", AttachmentGeneration: 1,
		CheckedAtMs: 1000, ExpiresAtMs: 6000, LeaseLifetimeMs: 5000, ReattachTimeoutMs: 10000, RecoveryToken: strings.Repeat("synthetic-secret-", 3)}
	caps := []string{"sip.v1", StateCapability, NativeCompletionCapability, ControllerReattachCapability}
	entered, release := make(chan struct{}), make(chan struct{})
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	server, address := testServer(t, pki, 8, func(cfg *Config) {
		cfg.Capabilities, cfg.StateAuthority = caps, authority
		cfg.HelloTimeout = 2 * time.Second
		cfg.HeartbeatInterval, cfg.HeartbeatTimeout = time.Minute, 2*time.Minute
		cfg.OnControllerSession = func(ctx context.Context, _ *Peer, hello *pb.SessionHello) (*pb.ApplicationLease, error) {
			if hello.Recovery != nil {
				close(entered)
				select {
				case <-release:
					return nil, errors.New("rejected private recovery material")
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}
			return proto.Clone(lease).(*pb.ApplicationLease), nil
		}
		cfg.OnControllerRenew = func(context.Context, *Peer) (*pb.ApplicationLease, error) {
			return proto.Clone(lease).(*pb.ApplicationLease), nil
		}
		cfg.OnStatus = func(*Peer, *pb.SessionStatus) error { return nil }
	})
	first, err := pki.dial(address)
	if err != nil {
		t.Fatal(err)
	}
	defer first.CloseNow()
	send(t, first, &pb.Envelope{CorrelationId: "first", Payload: &pb.Envelope_Hello{Hello: &pb.SessionHello{Controller: application, ConnectionAttempt: 1, Major: 1, Capabilities: caps, Bounds: &pb.Bounds{MaxEnvelopeBytes: 8192}}}})
	ack, err := receive(first)
	if err != nil {
		t.Fatal(err)
	}
	send(t, first, &pb.Envelope{CorrelationId: "ready", SessionId: ack.SessionId, Payload: &pb.Envelope_Status{Status: &pb.SessionStatus{RetainedReady: true, BindingsReady: true}}})
	roundtrip(t, first, ack.SessionId)
	original, err := server.Peer(application)
	if err != nil {
		t.Fatal(err)
	}
	replacement, err := pki.dial(address)
	if err != nil {
		t.Fatal(err)
	}
	defer replacement.CloseNow()
	send(t, replacement, &pb.Envelope{CorrelationId: "bad", Payload: &pb.Envelope_Hello{Hello: &pb.SessionHello{Controller: application, ConnectionAttempt: 2, Major: 1, Capabilities: caps, Bounds: &pb.Bounds{MaxEnvelopeBytes: 8192},
		Recovery: &pb.ControllerRecovery{Application: application, Term: lease.Term, Token: strings.Repeat("wrong-token-", 3), AttemptId: "bad-attach", AttachmentGeneration: 1}}}})
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("recovery hook did not run")
	}
	if original.Context().Err() != nil {
		t.Fatal("unverified recovery fenced live holder")
	}
	current, err := server.Peer(application)
	if err != nil || current.Context() != original.Context() || len(server.ReadyPeers()) != 1 {
		t.Fatal("unverified recovery displaced published peer")
	}
	close(release)
	if _, err := receive(replacement); err == nil || strings.Contains(err.Error(), "private recovery material") {
		t.Fatal("rejected proof did not close generically")
	}
	if original.Context().Err() != nil {
		t.Fatal("failed recovery disconnected current holder")
	}
	roundtrip(t, first, ack.SessionId)
}
