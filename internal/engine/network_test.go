package engine

import (
	"context"
	"net"
	"testing"
	"time"

	"sipbridge/internal/control"
	pb "sipbridge/internal/controlpb"
)

func TestUDPNetworkTargetPreservesFlowAndDatagramBoundary(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	listener, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	remote, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer remote.Close()
	ingress := NewIngress(&pb.ProcessRef{InstanceId: "sip", Epoch: "epoch"}, new(control.Server), "test", 2)
	// Install the same listener used by ServeUDP before opening the outbound peer.
	ingress.udp = listener
	go func() { _ = ingress.ServeUDP(ctx, listener) }()
	target := &pb.NetworkTarget{Proxy: "sip:" + remote.LocalAddr().String(), Transport: "udp"}
	ref, err := ingress.OpenNetwork(ctx, "sip:callee@unresolvable.invalid", target)
	if err != nil {
		t.Fatal(err)
	}
	if ref.Transport != "udp" || ref.SourceIp != "127.0.0.1" {
		t.Fatalf("incorrect observed flow: %v", ref)
	}
	again, err := ingress.OpenNetwork(ctx, "sip:another@unresolvable.invalid", target)
	if err != nil || ref.ConnectionId != again.ConnectionId {
		t.Fatalf("peer flow changed: %v %v", again, err)
	}
	ingress.mu.Lock()
	flow := ingress.flows[ref.ConnectionId]
	ingress.mu.Unlock()
	wire := []byte("OPTIONS sip:callee@unresolvable.invalid SIP/2.0\r\nContent-Length: 0\r\n\r\n")
	flow.write.Lock()
	n, err := flow.writeWire(wire)
	flow.write.Unlock()
	if err != nil || n != len(wire) {
		t.Fatalf("write: %d %v", n, err)
	}
	_ = remote.SetReadDeadline(time.Now().Add(time.Second))
	buffer := make([]byte, 65536)
	n, source, err := remote.ReadFrom(buffer)
	if err != nil || string(buffer[:n]) != string(wire) || source.String() != listener.LocalAddr().String() {
		t.Fatalf("datagram changed: %s %v %v", buffer[:n], source, err)
	}
	for _, transport := range []string{"ws", "arbitrary"} {
		if _, err := ingress.OpenNetwork(ctx, "sip:a@example.invalid", &pb.NetworkTarget{Transport: transport}); err == nil {
			t.Fatalf("accepted %s", transport)
		}
	}
}

func TestNetworkStreamTargetsBindConfiguredSIPSource(t *testing.T) {
	for _, transport := range []string{"tcp", "tls"} {
		t.Run(transport, func(t *testing.T) {
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			accepted := make(chan net.Conn, 1)
			go func() {
				conn, err := listener.Accept()
				if err == nil {
					accepted <- conn
					if transport == "tls" {
						conn.Close()
					}
				}
			}()
			ingress := NewIngress(&pb.ProcessRef{InstanceId: "sip", Epoch: "epoch"}, new(control.Server), "test", 2, IngressOptions{SourceIP: net.ParseIP("127.0.0.2")})
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			ref, dialErr := ingress.OpenNetwork(ctx, "sip:callee@"+listener.Addr().String(), &pb.NetworkTarget{Transport: transport})
			if transport == "tcp" && (dialErr != nil || ref == nil) {
				t.Fatalf("TCP connect: %v %v", ref, dialErr)
			}
			if transport == "tls" && (dialErr == nil || ref != nil) {
				t.Fatalf("unauthenticated TLS accepted: %v %v", ref, dialErr)
			}
			select {
			case conn := <-accepted:
				defer conn.Close()
				source, _, err := net.SplitHostPort(conn.RemoteAddr().String())
				if err != nil || source != "127.0.0.2" {
					t.Fatalf("wrong source: %s %v", source, err)
				}
			case <-ctx.Done():
				t.Fatal("target did not observe the selected source")
			}
		})
	}
}

func TestUnusableConfiguredSourceCannotFallBackToDefaultRoute(t *testing.T) {
	ingress := NewIngress(&pb.ProcessRef{InstanceId: "sip", Epoch: "epoch"}, new(control.Server), "test", 1, IngressOptions{SourceIP: net.ParseIP("192.0.2.1")})
	for attempt := 0; attempt < 2; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		_, err := ingress.OpenNetwork(ctx, "sip:a@127.0.0.1:9", &pb.NetworkTarget{Transport: "tcp"})
		cancel()
		if err == nil || err.Error() == "stream capacity" {
			t.Fatalf("failed source binding leaked capacity or fell back: %v", err)
		}
	}
}
