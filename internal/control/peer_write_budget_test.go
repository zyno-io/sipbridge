package control

import (
	"context"
	"errors"
	pb "sipbridge/internal/controlpb"
	"testing"
	"time"
)

func TestPeerCallAdmissionConsumesEnvelopeAndCallerBudgets(t *testing.T) {
	for _, example := range []struct {
		name    string
		budget  uint32
		caller  time.Duration
		maximum uint32
	}{
		{"envelope-window", 200, time.Second, 150},
		{"caller-window", 1000, 300 * time.Millisecond, 250},
	} {
		t.Run(example.name, func(t *testing.T) {
			f := newPeerWriteLifetimeFixture(t, 2*time.Second)
			f.gated.release()
			hold, stop := context.WithTimeout(context.Background(), time.Second)
			defer stop()
			if err := f.peer.session.acquireWrite(hold, hold); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), example.caller)
			defer cancel()
			request := &pb.Envelope{CorrelationId: "positive-budget", RemainingBudgetMs: example.budget,
				Payload: &pb.Envelope_ProbeApplication{ProbeApplication: &pb.ApplicationProbe{
					Context: &pb.ContextRef{Id: "context"}, Application: &pb.ProcessRef{InstanceId: "controller", Epoch: "epoch"}}}}
			done := make(chan error, 1)
			go func() { _, err := f.peer.Call(ctx, request); done <- err }()
			queued := false
			until := time.Now().Add(100 * time.Millisecond)
			for time.Now().Before(until) {
				f.peer.session.mu.Lock()
				_, queued = f.peer.session.pending[request.CorrelationId]
				f.peer.session.mu.Unlock()
				if queued {
					break
				}
				time.Sleep(time.Millisecond)
			}
			if !queued {
				f.peer.session.releaseWrite()
				t.Fatal("RPC did not enter the bounded admission wait")
			}
			time.Sleep(70 * time.Millisecond)
			f.peer.session.releaseWrite()
			if err := awaitPeerWriteLifetimeError(t, done); err != nil {
				t.Fatal(err)
			}
			select {
			case frame := <-f.remoteFrames:
				if frame.RemainingBudgetMs == 0 || frame.RemainingBudgetMs > example.maximum {
					t.Fatalf("wire budget ignored admission/caller window: %d", frame.RemainingBudgetMs)
				}
			case <-time.After(time.Second):
				t.Fatal("admitted frame did not reach peer")
			}
		})
	}
}

func TestPeerCallTransportBoundIncludesWriterAdmissionWait(t *testing.T) {
	f := newPeerWriteLifetimeFixture(t, 100*time.Millisecond)
	hold, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	if err := f.peer.session.acquireWrite(hold, hold); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := f.peer.Call(ctx, peerWriteLifetimeCall("transport-wait")); done <- err }()
	if err := awaitPeerWriteLifetimeError(t, done); !errors.Is(err, context.DeadlineExceeded) || ctx.Err() != nil {
		f.peer.session.releaseWrite()
		t.Fatalf("waiting RPC lost its original transport cap: %v", err)
	}
	f.peer.session.mu.Lock()
	_, expired := f.peer.session.expired["transport-wait"]
	pending := len(f.peer.session.pending)
	f.peer.session.mu.Unlock()
	f.peer.session.releaseWrite()
	if pending != 0 || expired || f.gated.writes.Load() != 0 {
		t.Fatal("expired admission performed or retained physical work")
	}
	select {
	case <-f.gated.closed:
		t.Fatal("queued timeout closed the healthy socket")
	default:
	}
}
