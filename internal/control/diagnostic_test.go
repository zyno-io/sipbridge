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
