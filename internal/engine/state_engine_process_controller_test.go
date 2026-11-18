package engine

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	pb "sipbridge/internal/controlpb"
)

func TestProcessControllerPendingCallUnblocksOnSessionClose(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	received := make(chan struct{})
	disconnect := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
		if _, _, err = conn.Read(ctx); err != nil {
			return
		}
		close(received)
		select {
		case <-disconnect:
		case <-ctx.Done():
		}
	}))
	defer server.Close()
	conn, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http"), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.CloseNow()
	p := &processController{connection: conn, harness: &processHarness{ctx: ctx}, pending: make(map[string]chan *pb.Envelope), done: make(chan struct{})}
	go p.read()
	result := make(chan error, 1)
	go func() {
		_, err := p.call(&pb.Envelope{Payload: &pb.Envelope_Ping{Ping: &pb.Heartbeat{}}})
		result <- err
	}()
	select {
	case <-received:
	case <-ctx.Done():
		t.Fatal("request was not sent")
	}
	close(disconnect)
	select {
	case err := <-result:
		if err == nil || !strings.Contains(err.Error(), "control session closed") {
			t.Fatal("expected closed-session error", err)
		}
	case <-time.After(time.Second):
		t.Fatal("pending call did not unblock when read loop closed")
	}
	p.mu.Lock()
	remaining := len(p.pending)
	p.mu.Unlock()
	if remaining != 0 {
		t.Fatal("closed session retained pending call", remaining)
	}
}
