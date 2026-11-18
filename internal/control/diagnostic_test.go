package control

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	pb "sipbridge/internal/controlpb"
)

func TestControlDiagnosticProjectsOnlySafeMetadata(t *testing.T) {
	const secret = "private-peer-reason-and-wire"
	var observed Diagnostic
	p := &session{ctx: context.Background(), id: "session", process: &pb.ProcessRef{InstanceId: "controller", Epoch: "epoch"},
		server: &Server{cfg: Config{OnDiagnostic: func(d Diagnostic) { observed = d }}}}
	envelope := &pb.Envelope{CorrelationId: "operation", RemainingBudgetMs: 46,
		Payload: &pb.Envelope_Result{Result: &pb.OperationResult{Error: secret, Response: &pb.SipMessage{Wire: []byte(secret)}}}}
	p.diagnostic("read", errors.Join(context.DeadlineExceeded, websocket.CloseError{Code: websocket.StatusPolicyViolation, Reason: secret}), envelope, true)
	if observed.Stage != "read" || observed.Kind != "Result" || observed.CloseCode != 1008 || !observed.Deadline || !observed.CallerExpired || observed.SessionExpired {
		t.Fatalf("wrong diagnostic metadata: %+v", observed)
	}
	data, err := json.Marshal(observed)
	if err != nil || strings.Contains(string(data), secret) {
		t.Fatal("diagnostic exposed error text or message contents")
	}
	if observed.Peer == p.process {
		t.Fatal("diagnostic observer received the live process reference")
	}
	p.id, p.process.InstanceId, envelope.CorrelationId = "invalid ref", "invalid ref", strings.Repeat("x", 200)
	p.diagnostic("protocol", nil, envelope, false)
	if observed.Session != "" || observed.Peer != nil || observed.Correlation != "" || observed.CloseCode != 0 {
		t.Fatal("invalid references or unknown close code entered the diagnostic")
	}
}

func TestControlDiagnosticObserverPanicCannotEscape(t *testing.T) {
	p := &session{ctx: context.Background(), server: &Server{cfg: Config{OnDiagnostic: func(Diagnostic) { panic("observer failure") }}}}
	p.diagnostic("heartbeat-timeout", nil, nil, false)
	if p.ctx.Err() != nil {
		t.Fatal("diagnostic changed session lifetime")
	}
}

func TestControlWriteFailureDiagnosticPreservesCallerDeadline(t *testing.T) {
	peer, stream := blockedControlPeer(t, time.Second)
	observed := make(chan Diagnostic, 1)
	peer.session.server.cfg.OnDiagnostic = func(d Diagnostic) { observed <- d }
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := peer.Call(ctx, controlWriteEnvelope()); done <- err }()
	awaitControlWrite(t, stream)
	if err := awaitControlWriteError(t, done); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("diagnostic changed write result: %v", err)
	}
	select {
	case d := <-observed:
		if d.Stage != "write" || !d.Deadline || !d.CallerExpired || d.Kind != "Ping" {
			t.Fatalf("physical write failure lost its local cause: %+v", d)
		}
	case <-time.After(time.Second):
		t.Fatal("physical write failure was not observed")
	}
	assertControlWaiterReclaimed(t, peer)
}

func TestControlDiagnosticSocketClassesNeverExposeErrorText(t *testing.T) {
	var observed Diagnostic
	p := &session{ctx: context.Background(), server: &Server{cfg: Config{OnDiagnostic: func(d Diagnostic) { observed = d }}}}
	for _, example := range []struct {
		err  error
		want string
	}{
		{net.ErrClosed, "closed"}, {io.EOF, "eof"}, {io.ErrUnexpectedEOF, "eof"}, {errors.New("private peer contents"), ""},
	} {
		p.diagnostic("read-transport", errors.Join(example.err, errors.New("private peer contents")), nil, false)
		data, err := json.Marshal(observed)
		if err != nil || observed.IOClass != example.want || strings.Contains(string(data), "private peer contents") {
			t.Fatal("socket category leaked error text or lost its fixed classification")
		}
	}
}

func TestStatusRejectionDiagnosticsAreCompleteAndPrivate(t *testing.T) {
	for _, name := range []string{"session", "budget", "envelope", "capability", "missing-handler", "policy-capability", "policy-invalid", "policy-expired", "policy-key-overlap", "callback"} {
		t.Run(name, func(t *testing.T) {
			const private = "private-status-key-and-callback-text"
			var observed []Diagnostic
			p := &session{ctx: context.Background(), id: "session", capabilities: []string{"sip.v1"}, status: &pb.SessionStatus{},
				server: &Server{cfg: Config{OnStatus: func(*Peer, *pb.SessionStatus) error { return nil }, OnDiagnostic: func(d Diagnostic) { observed = append(observed, d) }}}}
			e := &pb.Envelope{SessionId: "session", CorrelationId: "status", Payload: &pb.Envelope_Status{Status: &pb.SessionStatus{RetainedReady: true, BindingsReady: true,
				DigestChallengePolicy: &pb.DigestChallengePolicy{SigningKey: []byte(private), Realm: private, AuthenticationEpoch: private}}}}
			want := "status-" + name
			switch name {
			case "session":
				e.SessionId = "other"
			case "budget":
				e.RemainingBudgetMs = 1
			case "envelope":
				e.Payload = &pb.Envelope_Status{}
			case "capability":
				p.capabilities = nil
			case "missing-handler":
				p.server.cfg.OnStatus = nil
				want = "status-capability"
			default:
				cause := map[string]error{"policy-capability": ErrStatusPolicyCapability, "policy-invalid": ErrStatusPolicyInvalid, "policy-expired": ErrStatusPolicyExpired, "policy-key-overlap": ErrStatusPolicyKeyOverlap}[name]
				p.server.cfg.OnStatus = func(*Peer, *pb.SessionStatus) error { return errors.Join(cause, errors.New(private)) }
			}
			if p.handle(e) || len(observed) != 1 || observed[0].Stage != want || observed[0].Kind != "Status" {
				t.Fatal("status rejection category changed", observed)
			}
			data, err := json.Marshal(observed)
			if err != nil || strings.Contains(string(data), private) || p.status.RetainedReady {
				t.Fatal("status rejection exposed data or changed readiness", err)
			}
		})
	}
}

func TestRejectedStatusStillClosesActualControlSession(t *testing.T) {
	for _, name := range []string{"budget", "policy-invalid", "policy-expired", "policy-key-overlap"} {
		t.Run(name, func(t *testing.T) {
			pki := newPKI(t)
			observed := make(chan Diagnostic, 2)
			_, address := testServer(t, pki, 2, func(cfg *Config) {
				cfg.Capabilities = []string{"sip.v1"}
				cfg.OnDiagnostic = func(d Diagnostic) { observed <- d }
				cfg.OnStatus = func(*Peer, *pb.SessionStatus) error {
					return map[string]error{"policy-invalid": ErrStatusPolicyInvalid, "policy-expired": ErrStatusPolicyExpired, "policy-key-overlap": ErrStatusPolicyKeyOverlap}[name]
				}
			})
			c, err := pki.dial(address)
			if err != nil {
				t.Fatal(err)
			}
			defer c.CloseNow()
			send(t, c, &pb.Envelope{CorrelationId: "hello", Payload: &pb.Envelope_Hello{Hello: &pb.SessionHello{Controller: &pb.ProcessRef{InstanceId: "status-controller", Epoch: "epoch"}, ConnectionAttempt: 1, Major: 1, Capabilities: []string{"sip.v1"}, Bounds: &pb.Bounds{MaxEnvelopeBytes: MaxEnvelope}}}})
			frame, err := receive(c)
			if err != nil || frame.GetHelloAck() == nil {
				t.Fatal("hello failed", err)
			}
			status := &pb.Envelope{SessionId: frame.SessionId, CorrelationId: "bad-status", Payload: &pb.Envelope_Status{Status: &pb.SessionStatus{}}}
			if name == "budget" {
				status.RemainingBudgetMs = 1
			}
			send(t, c, status)
			for {
				frame, err = receive(c)
				if err != nil {
					break
				}
				if ping := frame.GetPing(); ping != nil {
					send(t, c, &pb.Envelope{SessionId: frame.SessionId, CorrelationId: frame.CorrelationId, Payload: &pb.Envelope_Pong{Pong: ping}})
				}
			}
			if websocket.CloseStatus(err) != websocket.StatusPolicyViolation {
				t.Fatal("malformed Status or rejected policy no longer closes control", err)
			}
			select {
			case d := <-observed:
				if d.Stage != "status-"+name {
					t.Fatal("missing actual Status rejection category", d)
				}
			case <-time.After(time.Second):
				t.Fatal("Status rejection was not observed")
			}
		})
	}
}
