package control

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
)

type gatedWriteConn struct {
	net.Conn
	armed         atomic.Bool
	blocked       atomic.Bool
	writes        atomic.Int64
	entered       chan struct{}
	resume        chan struct{}
	closed        chan struct{}
	writeFinished chan struct{}
	closeOnce     sync.Once
	resumeOnce    sync.Once
}

func newGatedWriteConn(conn net.Conn) *gatedWriteConn {
	return &gatedWriteConn{Conn: conn, entered: make(chan struct{}), resume: make(chan struct{}),
		closed: make(chan struct{}), writeFinished: make(chan struct{})}
}

func (c *gatedWriteConn) Write(data []byte) (int, error) {
	if !c.armed.Load() {
		return c.Conn.Write(data)
	}
	c.writes.Add(1)
	if !c.blocked.CompareAndSwap(false, true) {
		return c.Conn.Write(data)
	}
	close(c.entered)
	defer close(c.writeFinished)
	select {
	case <-c.resume:
		return c.Conn.Write(data)
	case <-c.closed:
		return 0, net.ErrClosed
	}
}

func (c *gatedWriteConn) arm() { c.armed.Store(true) }

func (c *gatedWriteConn) release() { c.resumeOnce.Do(func() { close(c.resume) }) }

func (c *gatedWriteConn) Close() error {
	var err error
	c.closeOnce.Do(func() {
		close(c.closed)
		err = c.Conn.Close()
	})
	return err
}

type peerWriteLifetimeFixture struct {
	peer          *Peer
	client        *websocket.Conn
	transport     *http.Transport
	server        *httptest.Server
	gated         *gatedWriteConn
	remoteFrames  chan *pb.Envelope
	remoteReplies chan string
	remoteErrors  chan error
	readErrors    chan error
	stop          context.CancelFunc
}

func newPeerWriteLifetimeFixture(t *testing.T, writeTimeout time.Duration) *peerWriteLifetimeFixture {
	t.Helper()
	fixture := &peerWriteLifetimeFixture{
		remoteFrames:  make(chan *pb.Envelope, 2),
		remoteReplies: make(chan string, 2),
		remoteErrors:  make(chan error, 1),
		readErrors:    make(chan error, 1),
	}
	fixture.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{CompressionMode: websocket.CompressionDisabled})
		if err != nil {
			fixture.remoteErrors <- err
			return
		}
		defer conn.CloseNow()
		for {
			readCtx, cancelRead := context.WithTimeout(context.Background(), 3*time.Second)
			kind, data, err := conn.Read(readCtx)
			cancelRead()
			if err != nil {
				return
			}
			if kind != websocket.MessageBinary {
				fixture.remoteErrors <- errors.New("client sent a non-text control frame")
				return
			}
			envelope := &pb.Envelope{}
			err = proto.Unmarshal(data, envelope)
			if err != nil {
				fixture.remoteErrors <- err
				return
			}
			fixture.remoteFrames <- envelope
			response := &pb.Envelope{
				CorrelationId: envelope.CorrelationId,
				SessionId:     envelope.SessionId,
				Payload:       &pb.Envelope_Result{Result: &pb.OperationResult{Certainty: pb.OperationResult_COMPLETED}},
			}
			encoded, err := proto.Marshal(response)
			if err != nil {
				fixture.remoteErrors <- err
				return
			}
			writeCtx, cancelWrite := context.WithTimeout(context.Background(), time.Second)
			err = conn.Write(writeCtx, websocket.MessageBinary, encoded)
			cancelWrite()
			if err != nil {
				fixture.remoteErrors <- err
				return
			}
			fixture.remoteReplies <- envelope.CorrelationId
		}
	}))
	fixture.transport = &http.Transport{DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
		conn, err := (&net.Dialer{}).DialContext(ctx, network, address)
		if err != nil {
			return nil, err
		}
		fixture.gated = newGatedWriteConn(conn)
		return fixture.gated, nil
	}}
	client := &http.Client{Transport: fixture.transport}
	dialCtx, cancelDial := context.WithTimeout(context.Background(), time.Second)
	clientConn, _, err := websocket.Dial(dialCtx, "ws"+strings.TrimPrefix(fixture.server.URL, "http"), &websocket.DialOptions{HTTPClient: client})
	cancelDial()
	if err != nil {
		fixture.server.Close()
		t.Fatal(err)
	}
	fixture.client = clientConn
	fixture.gated.arm()
	sessionCtx, stop := context.WithCancel(context.Background())
	fixture.stop = stop
	s := &session{
		server:       &Server{cfg: Config{WriteTimeout: writeTimeout}},
		conn:         clientConn,
		ctx:          sessionCtx,
		cancel:       stop,
		id:           "write-lifetime-session",
		bound:        MaxEnvelope,
		capabilities: []string{"sip.v1"},
		pending:      make(map[string]chan *pb.Envelope),
		expired:      make(map[string]time.Time),
	}
	fixture.peer = &Peer{session: s}
	go func() {
		for {
			envelope, err := s.read(s.ctx)
			if err != nil {
				fixture.readErrors <- err
				return
			}
			if !s.handle(envelope) {
				fixture.readErrors <- errors.New("peer rejected a valid operation result")
				clientConn.CloseNow()
				return
			}
		}
	}()
	t.Cleanup(func() {
		stop()
		clientConn.CloseNow()
		_ = fixture.gated.Close()
		fixture.transport.CloseIdleConnections()
		fixture.server.Close()
	})
	return fixture
}

func peerWriteLifetimeCall(id string) *pb.Envelope {
	return &pb.Envelope{CorrelationId: id, Payload: &pb.Envelope_Ping{Ping: &pb.Heartbeat{Nonce: id}}}
}

func awaitPeerWriteLifetimeError(t *testing.T, done <-chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Peer.Call did not return within its caller deadline")
		return nil
	}
}

func TestPeerCallTimeoutKeepsPhysicalWriterAndSessionUsable(t *testing.T) {
	fixture := newPeerWriteLifetimeFixture(t, 2*time.Second)
	firstCtx, cancelFirst := context.WithTimeout(context.Background(), 60*time.Millisecond)
	defer cancelFirst()
	firstDone := make(chan error, 1)
	go func() {
		_, err := fixture.peer.Call(firstCtx, peerWriteLifetimeCall("first-expired"))
		firstDone <- err
	}()
	select {
	case <-fixture.gated.entered:
	case <-time.After(time.Second):
		t.Fatal("first RPC did not reach its physical WebSocket write")
	}
	if err := awaitPeerWriteLifetimeError(t, firstDone); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("first RPC did not preserve its caller deadline: %v", err)
	}
	fixture.peer.session.mu.Lock()
	_, firstExpired := fixture.peer.session.expired["first-expired"]
	firstPending := len(fixture.peer.session.pending)
	fixture.peer.session.mu.Unlock()
	if !firstExpired || firstPending != 0 {
		t.Fatalf("timed-out admitted RPC state was not retired safely: expired=%t pending=%d", firstExpired, firstPending)
	}
	select {
	case <-fixture.gated.closed:
		t.Fatal("caller timeout closed the still-bounded physical WebSocket writer")
	default:
	}

	queuedCtx, cancelQueued := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancelQueued()
	queuedDone := make(chan error, 1)
	go func() {
		_, err := fixture.peer.Call(queuedCtx, peerWriteLifetimeCall("queued-expired"))
		queuedDone <- err
	}()
	if err := awaitPeerWriteLifetimeError(t, queuedDone); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("queued RPC did not preserve its caller deadline: %v", err)
	}
	fixture.peer.session.mu.Lock()
	_, queuedExpired := fixture.peer.session.expired["queued-expired"]
	pendingAfterQueue := len(fixture.peer.session.pending)
	fixture.peer.session.mu.Unlock()
	if queuedExpired || pendingAfterQueue != 0 || fixture.gated.writes.Load() != 1 {
		t.Fatalf("expired queued RPC started work or retained a marker: expired=%t pending=%d writes=%d", queuedExpired, pendingAfterQueue, fixture.gated.writes.Load())
	}
	select {
	case envelope := <-fixture.remoteFrames:
		t.Fatalf("blocked first physical write reached remote before release: %s", envelope.CorrelationId)
	default:
	}

	fixture.gated.release()
	select {
	case envelope := <-fixture.remoteFrames:
		if envelope.CorrelationId != "first-expired" || envelope.SessionId != fixture.peer.session.id {
			t.Fatalf("unexpected first remote frame: correlation=%q session=%q", envelope.CorrelationId, envelope.SessionId)
		}
	case <-time.After(time.Second):
		t.Fatal("released physical writer did not deliver its original frame")
	}
	select {
	case correlation := <-fixture.remoteReplies:
		if correlation != "first-expired" {
			t.Fatalf("remote replied to unexpected first correlation %q", correlation)
		}
	case <-time.After(time.Second):
		t.Fatal("remote did not send the late first response")
	}

	freshCtx, cancelFresh := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancelFresh()
	freshDone := make(chan struct {
		envelope *pb.Envelope
		err      error
	}, 1)
	go func() {
		envelope, err := fixture.peer.Call(freshCtx, peerWriteLifetimeCall("fresh-call"))
		freshDone <- struct {
			envelope *pb.Envelope
			err      error
		}{envelope: envelope, err: err}
	}()
	select {
	case result := <-freshDone:
		if result.err != nil || result.envelope.GetResult().GetCertainty() != pb.OperationResult_COMPLETED {
			t.Fatalf("fresh call did not complete on the same session: envelope=%v err=%v", result.envelope, result.err)
		}
	case <-time.After(time.Second):
		t.Fatal("fresh RPC did not complete after the original writer released its token")
	}
	select {
	case envelope := <-fixture.remoteFrames:
		if envelope.CorrelationId != "fresh-call" {
			t.Fatalf("remote received unexpected fresh correlation %q", envelope.CorrelationId)
		}
	case <-time.After(time.Second):
		t.Fatal("remote did not receive the fresh RPC")
	}
	select {
	case err := <-fixture.readErrors:
		t.Fatalf("late response or fresh result broke the session read loop: %v", err)
	default:
	}
	if fixture.peer.session.ctx.Err() != nil || fixture.gated.writes.Load() != 2 {
		t.Fatal("session was poisoned or the bounded writer did not release its token")
	}
	select {
	case err := <-fixture.remoteErrors:
		t.Fatalf("remote WebSocket failed: %v", err)
	default:
	}
}

func TestPeerCallCallerTimeoutDoesNotExtendPhysicalWriteTimeout(t *testing.T) {
	fixture := newPeerWriteLifetimeFixture(t, 150*time.Millisecond)
	callerCtx, cancelCaller := context.WithTimeout(context.Background(), 35*time.Millisecond)
	defer cancelCaller()
	done := make(chan error, 1)
	started := time.Now()
	go func() {
		_, err := fixture.peer.Call(callerCtx, peerWriteLifetimeCall("bounded-physical-stall"))
		done <- err
	}()
	select {
	case <-fixture.gated.entered:
	case <-time.After(time.Second):
		t.Fatal("RPC did not reach its physical WebSocket write")
	}
	if err := awaitPeerWriteLifetimeError(t, done); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("caller did not return at its short deadline: %v", err)
	}
	select {
	case <-fixture.gated.writeFinished:
		t.Fatal("caller deadline canceled the physical writer before the transport bound")
	default:
	}
	select {
	case <-fixture.gated.writeFinished:
		if elapsed := time.Since(started); elapsed < 100*time.Millisecond || elapsed > 500*time.Millisecond {
			t.Fatalf("physical writer did not stop at its bounded transport deadline: %s", elapsed)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("physical writer outlived its transport write deadline")
	}
	fixture.peer.session.mu.Lock()
	_, expired := fixture.peer.session.expired["bounded-physical-stall"]
	pending := len(fixture.peer.session.pending)
	fixture.peer.session.mu.Unlock()
	if !expired || pending != 0 {
		t.Fatalf("transport timeout did not retire the admitted call: expired=%t pending=%d", expired, pending)
	}
}
