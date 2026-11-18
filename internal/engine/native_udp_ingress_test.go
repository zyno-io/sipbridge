package engine

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
)

func TestNativeRetryStreamHonorsOriginalDeadlineAndPoisonsPartialWrite(t *testing.T) {
	for _, transport := range []string{"tcp", "tls"} {
		t.Run(transport, func(t *testing.T) {
			process := &pb.ProcessRef{InstanceId: "process", Epoch: "epoch"}
			ref := &pb.FlowRef{Ingress: process, ConnectionId: "connection", Transport: transport}
			conn := &framingConn{writes: []partialWrite{{n: 3, err: errors.New("partial write")}}}
			flow := &physicalFlow{ref: ref, conn: conn}
			ingress := &Ingress{process: process, flows: map[string]*physicalFlow{ref.ConnectionId: flow}}
			until := time.Now().Add(500 * time.Millisecond)
			if err := ingress.WriteNativeRetry(context.Background(), ref, []byte("complete-frame"), until); err == nil {
				t.Fatal("partial stream retry succeeded")
			}
			if !conn.lastDeadline.Equal(until) || !conn.closed || !flow.poisoned || conn.sent.String() != "com" {
				t.Fatal("stream retry lost deadline or poisoning")
			}
			if err := ingress.WriteNativeRetry(context.Background(), ref, []byte("later-frame"), until); err == nil || conn.sent.String() != "com" {
				t.Fatal("poisoned stream accepted another retry")
			}
		})
	}
}

type cancelAfterPartialRetryConn struct {
	*framingConn
	cancel context.CancelFunc
}

func (c *cancelAfterPartialRetryConn) Write(wire []byte) (int, error) {
	n, err := c.framingConn.Write(wire)
	c.cancel()
	return n, err
}

func TestNativeRetryStreamCancellationAfterShortWritePoisonsFlow(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	process := &pb.ProcessRef{InstanceId: "process", Epoch: "epoch"}
	ref := &pb.FlowRef{Ingress: process, ConnectionId: "connection", Transport: "tls"}
	conn := &cancelAfterPartialRetryConn{framingConn: &framingConn{writes: []partialWrite{{n: 3}}}, cancel: cancel}
	flow := &physicalFlow{ref: ref, conn: conn}
	ingress := &Ingress{process: process, flows: map[string]*physicalFlow{ref.ConnectionId: flow}}
	if err := ingress.WriteNativeRetry(ctx, ref, []byte("complete-frame"), time.Now().Add(time.Second)); err == nil {
		t.Fatal("cancellation after a short write succeeded")
	}
	if conn.sent.String() != "com" || !conn.closed || !flow.poisoned {
		t.Fatal("partial cancelled stream was reused or not poisoned")
	}
}

func TestNativeUDPRetryUsesOnlyExactLiveFlow(t *testing.T) {
	local, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer local.Close()
	remote, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer remote.Close()
	process := &pb.ProcessRef{InstanceId: "process", Epoch: "epoch"}
	ref := &pb.FlowRef{Ingress: process, ConnectionId: "connection", Transport: "udp"}
	flow := &physicalFlow{ref: ref, packet: local, remote: remote.LocalAddr()}
	ingress := &Ingress{process: process, flows: map[string]*physicalFlow{ref.ConnectionId: flow}}
	wire := []byte("exact UDP packet")
	if err = ingress.WriteNativeRetry(context.Background(), ref, wire, time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if err = remote.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	received := make([]byte, 64)
	n, _, err := remote.ReadFrom(received)
	if err != nil || string(received[:n]) != string(wire) {
		t.Fatalf("wrong UDP packet: %v", err)
	}
	changed := proto.Clone(ref).(*pb.FlowRef)
	changed.Ingress.Epoch = "other"
	if err = ingress.WriteNativeRetry(context.Background(), changed, wire, time.Now().Add(time.Second)); err == nil {
		t.Fatal("foreign process accepted")
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if err = ingress.WriteNativeRetry(cancelled, ref, wire, time.Now().Add(time.Second)); err == nil {
		t.Fatal("cancelled retry wrote")
	}
	if err = ingress.WriteNativeRetry(context.Background(), ref, wire, time.Now().Add(-time.Millisecond)); err == nil {
		t.Fatal("expired retry wrote")
	}
	flow.write.Lock()
	started := time.Now()
	err = ingress.WriteNativeRetry(context.Background(), ref, wire, time.Now().Add(time.Second))
	flow.write.Unlock()
	if err == nil || time.Since(started) > 100*time.Millisecond {
		t.Fatal("busy flow did not skip promptly")
	}
	ingress.mu.Lock()
	started = time.Now()
	err = ingress.WriteNativeRetry(context.Background(), ref, wire, time.Now().Add(time.Second))
	ingress.mu.Unlock()
	if err == nil || time.Since(started) > 100*time.Millisecond {
		t.Fatal("busy flow registry did not skip promptly")
	}
	if err = remote.SetReadDeadline(time.Now().Add(30 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if _, _, err = remote.ReadFrom(received); err == nil {
		t.Fatal("fenced retry emitted a packet")
	}
}

func TestNativeUDPRetrySupportsOwnedConnectedUDP(t *testing.T) {
	remote, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer remote.Close()
	conn, err := net.Dial("udp", remote.LocalAddr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	process := &pb.ProcessRef{InstanceId: "process", Epoch: "epoch"}
	ref := &pb.FlowRef{Ingress: process, ConnectionId: "connected", Transport: "udp"}
	flow := &physicalFlow{ref: ref, conn: conn}
	ingress := &Ingress{process: process, flows: map[string]*physicalFlow{ref.ConnectionId: flow}}
	wire := []byte("connected datagram")
	if err = ingress.WriteNativeRetry(context.Background(), ref, wire, time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if err = remote.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	received := make([]byte, 64)
	n, _, err := remote.ReadFrom(received)
	if err != nil || string(received[:n]) != string(wire) {
		t.Fatalf("connected UDP packet: %v", err)
	}
}

type blockingUDPPacket struct {
	started chan struct{}
	closed  chan struct{}
	once    sync.Once
}

func (p *blockingUDPPacket) ReadFrom([]byte) (int, net.Addr, error) {
	<-p.closed
	return 0, nil, net.ErrClosed
}
func (p *blockingUDPPacket) WriteTo([]byte, net.Addr) (int, error) {
	select {
	case p.started <- struct{}{}:
	default:
	}
	<-p.closed
	return 0, net.ErrClosed
}
func (p *blockingUDPPacket) Close() error { p.once.Do(func() { close(p.closed) }); return nil }
func (p *blockingUDPPacket) LocalAddr() net.Addr {
	return &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 5060}
}
func (p *blockingUDPPacket) SetDeadline(time.Time) error      { return nil }
func (p *blockingUDPPacket) SetReadDeadline(time.Time) error  { return nil }
func (p *blockingUDPPacket) SetWriteDeadline(time.Time) error { return nil }

func TestNativeUDPRetryShutdownClosesBlockedSocketBeforeJoin(t *testing.T) {
	seed := udpRetrySeed(t, udpKey("shutdown"))
	packet := &blockingUDPPacket{started: make(chan struct{}, 1), closed: make(chan struct{})}
	flow := &physicalFlow{ref: seed.Flow, packet: packet, remote: &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 5060}}
	ingress := &Ingress{process: seed.Flow.Ingress, flows: map[string]*physicalFlow{seed.Flow.ConnectionId: flow}, peers: make(map[string]*physicalFlow)}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	served := make(chan error, 1)
	go func() { served <- ingress.ServeUDP(ctx, packet) }()
	manager, _ := udpRetryManager(t, nativeUDPRetryLimits{Entries: 2, Hints: 2, Writers: 2, Bytes: 1 << 20}, ingress.WriteNativeRetry)
	if err := manager.Arm(seed); err != nil {
		t.Fatal(err)
	}
	select {
	case <-packet.started:
	case <-time.After(time.Second):
		t.Fatal("UDP write did not block")
	}
	cancel()
	select {
	case err := <-served:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("UDP socket did not close")
	}
	joined := make(chan struct{})
	go func() { manager.Close(); close(joined) }()
	select {
	case <-joined:
	case <-time.After(time.Second):
		t.Fatal("retry writer did not join after socket close")
	}
}
