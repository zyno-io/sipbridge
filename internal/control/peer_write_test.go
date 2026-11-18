package control

import (
	"context"
	"crypto/sha1"
	"encoding/base64"
	"errors"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	pb "sipbridge/internal/controlpb"
)

type controlWriteObserver struct {
	net.Conn
	entered chan struct{}
	once    sync.Once
	writes  atomic.Int64
	bytes   atomic.Int64
}

func (c *controlWriteObserver) Write(data []byte) (int, error) {
	c.writes.Add(1)
	c.once.Do(func() { close(c.entered) })
	n, err := c.Conn.Write(data)
	c.bytes.Add(int64(n))
	return n, err
}

type controlWriteRoundTripper func(*http.Request) (*http.Response, error)

func (f controlWriteRoundTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

// A real WebSocket over an unbuffered net.Pipe deterministically blocks at its
// physical Write. The remote pipe never reads, avoiding kernel-buffer guesses.
func blockedControlPeer(t *testing.T, writeTimeout time.Duration) (*Peer, *controlWriteObserver) {
	t.Helper()
	local, remote := net.Pipe()
	stream := &controlWriteObserver{Conn: local, entered: make(chan struct{})}
	client := &http.Client{Transport: controlWriteRoundTripper(func(request *http.Request) (*http.Response, error) {
		key := request.Header.Get("Sec-WebSocket-Key")
		digest := sha1.Sum([]byte(key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"))
		return &http.Response{
			StatusCode: http.StatusSwitchingProtocols,
			Header: http.Header{
				"Upgrade":              {"websocket"},
				"Connection":           {"Upgrade"},
				"Sec-Websocket-Accept": {base64.StdEncoding.EncodeToString(digest[:])},
			},
			Body: stream,
		}, nil
	})}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	connection, _, err := websocket.Dial(ctx, "ws://control.invalid/v1/control", &websocket.DialOptions{HTTPClient: client})
	cancel()
	if err != nil {
		local.Close()
		remote.Close()
		t.Fatal(err)
	}
	sessionContext, stop := context.WithCancel(context.Background())
	p := &session{server: &Server{cfg: Config{WriteTimeout: writeTimeout}}, conn: connection, ctx: sessionContext, cancel: stop,
		id: "test-session", bound: MaxEnvelope, pending: make(map[string]chan *pb.Envelope), expired: make(map[string]time.Time)}
	t.Cleanup(func() { stop(); connection.CloseNow(); local.Close(); remote.Close() })
	return &Peer{session: p}, stream
}

func controlWriteEnvelope() *pb.Envelope {
	return &pb.Envelope{CorrelationId: "bounded-call", Payload: &pb.Envelope_Ping{Ping: &pb.Heartbeat{Nonce: "bounded-call"}}}
}

func awaitControlWrite(t *testing.T, stream *controlWriteObserver) {
	t.Helper()
	select {
	case <-stream.entered:
	case <-time.After(time.Second):
		t.Fatal("WebSocket did not enter the deterministic blocked physical write")
	}
}

func awaitControlWriteError(t *testing.T, done <-chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(500 * time.Millisecond):
		t.Fatal("control write ignored caller/session cancellation in favor of the longer write timeout")
	}
	return nil
}

func assertControlWaiterReclaimed(t *testing.T, peer *Peer) {
	t.Helper()
	peer.session.mu.Lock()
	defer peer.session.mu.Unlock()
	if len(peer.session.pending) != 0 {
		t.Fatal("failed control write retained a pending RPC waiter")
	}
}

func TestPeerCallBoundsBlockedWriteByCallerDeadline(t *testing.T) {
	peer, stream := blockedControlPeer(t, 2*time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := peer.Call(ctx, controlWriteEnvelope()); done <- err }()
	awaitControlWrite(t, stream)
	if err := awaitControlWriteError(t, done); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("blocked write did not preserve the caller deadline error: %v", err)
	}
	if stream.bytes.Load() != 0 {
		t.Fatal("an unread, expired control write unexpectedly emitted bytes")
	}
	assertControlWaiterReclaimed(t, peer)
}

func TestPeerCallBlockedWriteHonorsCallerAndSessionCancellation(t *testing.T) {
	for _, source := range []string{"caller", "session"} {
		t.Run(source, func(t *testing.T) {
			peer, stream := blockedControlPeer(t, 2*time.Second)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() { _, err := peer.Call(ctx, controlWriteEnvelope()); done <- err }()
			awaitControlWrite(t, stream)
			if source == "caller" {
				cancel()
			} else {
				peer.session.cancel()
			}
			if err := awaitControlWriteError(t, done); !errors.Is(err, context.Canceled) {
				t.Fatalf("blocked write did not preserve cancellation: %v", err)
			}
			assertControlWaiterReclaimed(t, peer)
		})
	}
}

func TestPeerCallExpiredCallerNeverWritesOrPoisonsSession(t *testing.T) {
	peer, stream := blockedControlPeer(t, 2*time.Second)
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	if _, err := peer.Call(ctx, controlWriteEnvelope()); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("already expired caller was not refused: %v", err)
	}
	if stream.writes.Load() != 0 || peer.session.ctx.Err() != nil {
		t.Fatal("expired RPC emitted a frame or cancelled its healthy session")
	}
	assertControlWaiterReclaimed(t, peer)
	// The expired request did not close the connection: a later ordinary write
	// still enters the actual stream and then obeys session cancellation.
	done := make(chan error, 1)
	go func() { done <- peer.Send(controlWriteEnvelope()) }()
	awaitControlWrite(t, stream)
	peer.session.cancel()
	if err := awaitControlWriteError(t, done); !errors.Is(err, context.Canceled) {
		t.Fatalf("ordinary write no longer obeys session cancellation: %v", err)
	}
}

func TestPeerCallAndOrdinaryWriteKeepConfiguredTimeout(t *testing.T) {
	for _, mode := range []string{"call", "ordinary"} {
		t.Run(mode, func(t *testing.T) {
			peer, stream := blockedControlPeer(t, 100*time.Millisecond)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			done := make(chan error, 1)
			go func() {
				if mode == "ordinary" {
					done <- peer.Send(controlWriteEnvelope())
				} else {
					_, err := peer.Call(ctx, controlWriteEnvelope())
					done <- err
				}
			}()
			awaitControlWrite(t, stream)
			if err := awaitControlWriteError(t, done); !errors.Is(err, context.DeadlineExceeded) || ctx.Err() != nil {
				t.Fatalf("configured write timeout no longer bounds a longer caller: %v", err)
			}
			assertControlWaiterReclaimed(t, peer)
		})
	}
}
