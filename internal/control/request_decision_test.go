package control

import (
	"context"
	"slices"
	"testing"
	"time"

	pb "sipbridge/internal/controlpb"
)

func TestRequestDecisionCapabilityIsRequiredBeforeClaim(t *testing.T) {
	pki := newPKI(t)
	authority := &pb.StateAuthority{Service: "memory", Generation: "generation", PrimaryId: "primary", SchemaVersion: 1}
	caps := append(slices.Clone(requestDecisionDependencies[:]), RequestDecisionCapability)
	server, address := testServer(t, pki, 8, func(cfg *Config) {
		cfg.Capabilities, cfg.StateAuthority = caps, authority
		cfg.OnStatus = func(*Peer, *pb.SessionStatus) error { return nil }
		cfg.OnApplicationLease = func(_ context.Context, app *pb.ProcessRef, _ string) (*pb.ApplicationLease, error) {
			now := uint64(time.Now().UnixMilli())
			return &pb.ApplicationLease{Application: app, Authority: authority, Term: "term", CheckedAtMs: now, ExpiresAtMs: now + 30000}, nil
		}
	})
	current, err := pki.dial(address)
	if err != nil {
		t.Fatal(err)
	}
	defer current.CloseNow()
	app := &pb.ProcessRef{InstanceId: "controller", Epoch: "epoch"}
	hello := func(attempt uint64, capabilities []string) *pb.Envelope {
		return &pb.Envelope{CorrelationId: "hello", Payload: &pb.Envelope_Hello{Hello: &pb.SessionHello{
			Controller: app, ConnectionAttempt: attempt, Major: 1, Capabilities: capabilities, Bounds: &pb.Bounds{MaxEnvelopeBytes: 4096},
		}}}
	}
	send(t, current, hello(1, caps))
	ackFrame, err := receive(current)
	if err != nil || ackFrame.GetHelloAck() == nil {
		t.Fatal("direct controller hello failed", err)
	}
	ack := ackFrame.GetHelloAck()
	send(t, current, &pb.Envelope{CorrelationId: "ready", SessionId: ack.SessionId, Payload: &pb.Envelope_Status{Status: &pb.SessionStatus{RetainedReady: true, BindingsReady: true}}})
	roundtrip(t, current, ack.SessionId)
	for n, missing := range []string{RequestDecisionCapability, CommandAdmissionCapability, DialogContinuityCapability} {
		candidate, err := pki.dial(address)
		if err != nil {
			t.Fatal(err)
		}
		wanted := slices.DeleteFunc(slices.Clone(caps), func(capability string) bool { return capability == missing })
		send(t, candidate, hello(uint64(n+2), wanted))
		if reply, err := receive(candidate); err == nil {
			t.Fatal("incompatible direct controller displaced current owner", missing, reply)
		}
		candidate.CloseNow()
		roundtrip(t, current, ack.SessionId)
		if len(server.ReadyPeers()) != 1 {
			t.Fatal("incompatible hello fenced the ready controller")
		}
	}
}

func TestRequestDecisionSessionDoesNotAcceptOldRequestHandshake(t *testing.T) {
	decision := &pb.Envelope_RequestDecision{RequestDecision: &pb.RequestDecision{RequestId: "request", Status: 403}}
	for _, example := range []struct {
		name    string
		direct  bool
		budget  uint32
		payload *pb.Envelope
		accept  bool
	}{
		{"decision", true, 1000, &pb.Envelope{Payload: decision}, true},
		{"unnegotiated decision", false, 1000, &pb.Envelope{Payload: decision}, false},
		{"expired decision", true, 0, &pb.Envelope{Payload: decision}, false},
		{"ingest", true, 1000, &pb.Envelope{Payload: &pb.Envelope_Ingest{Ingest: &pb.OwnerIngest{}}}, false},
		{"route reply", true, 1000, &pb.Envelope{Payload: &pb.Envelope_RouteResult{RouteResult: &pb.RouteResult{Outcome: pb.RouteResult_ACCEPTED}}}, false},
		{"reverse request", true, 1000, &pb.Envelope{Payload: &pb.Envelope_Request{Request: &pb.RequestPresented{}}}, false},
		{"later ordinary command", true, 1000, &pb.Envelope{Payload: &pb.Envelope_Command{Command: &pb.Command{Action: &pb.Command_Cancel{Cancel: &pb.Cancel{}}}}}, true},
	} {
		t.Run(example.name, func(t *testing.T) {
			called := false
			p := &session{ctx: context.Background(), id: "session", capabilities: []string{"sip.v1"}, server: &Server{cfg: Config{
				OnMessage: func(*Peer, *pb.Envelope) error { called = true; return nil },
			}}}
			if example.direct {
				p.capabilities = append(p.capabilities, RequestDecisionCapability)
			}
			example.payload.SessionId, example.payload.CorrelationId, example.payload.RemainingBudgetMs = p.id, "operation", example.budget
			if accepted := p.handle(example.payload); accepted != example.accept || called != example.accept {
				t.Fatal("incorrect direct session admission", accepted, called)
			}
		})
	}
}
