package engine

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"math/big"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"sipbridge/internal/control"
	pb "sipbridge/internal/controlpb"
)

func outboundStreamPair(t *testing.T, transport string) (net.Conn, net.Conn) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	accepted := make(chan net.Conn, 1)
	acceptError := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			acceptError <- err
			return
		}
		accepted <- conn
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client, err := new(net.Dialer).DialContext(ctx, "tcp", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { client.Close() })
	var server net.Conn
	select {
	case server = <-accepted:
	case err := <-acceptError:
		t.Fatal(err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	t.Cleanup(func() { server.Close() })
	if transport == "tcp" {
		return client, server
	}
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	certificate := &x509.Certificate{SerialNumber: big.NewInt(1), NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}}
	der, err := x509.CreateCertificate(rand.Reader, certificate, certificate, public, private)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AddCert(parsed)
	secureClient := tls.Client(client, &tls.Config{MinVersion: tls.VersionTLS13, ServerName: "127.0.0.1", RootCAs: roots})
	secureServer := tls.Server(server, &tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: private}}})
	handshake := make(chan error, 1)
	go func() { handshake <- secureServer.HandshakeContext(ctx) }()
	if err := secureClient.HandshakeContext(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-handshake; err != nil {
		t.Fatal(err)
	}
	return secureClient, secureServer
}

// The handshake is real; only the lease duration and optional syscall barriers
// are test-selected. Reader teardown is the same path OpenNetwork starts.
type outboundStreamOptions struct {
	wrap      func(net.Conn) net.Conn
	configure func(*Ingress, *physicalFlow)
}

func leasedOutboundStream(t *testing.T, transport string, lease time.Duration, options ...outboundStreamOptions) (*Ingress, *physicalFlow, net.Conn, <-chan struct{}) {
	t.Helper()
	client, remote := outboundStreamPair(t, transport)
	var selected outboundStreamOptions
	if len(options) != 0 {
		selected = options[0]
	}
	if selected.wrap != nil {
		client = selected.wrap(client)
	}
	ingress := NewIngress(&pb.ProcessRef{InstanceId: "outbound", Epoch: "epoch"}, new(control.Server), "sip.test", 2)
	ingress.slots <- struct{}{}
	flow, err := ingress.addStream(client, "127.0.0.1", 5061, transport)
	if err != nil {
		t.Fatal(err)
	}
	if selected.configure != nil {
		selected.configure(ingress, flow)
	}
	flow.startUnusedLease(time.Now().Add(lease))
	done := make(chan struct{})
	go func() {
		defer close(done)
		ingress.readOutboundStream(flow)
	}()
	t.Cleanup(func() {
		client.Close()
		remote.Close()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("outbound reader did not release its stream")
		}
	})
	return ingress, flow, remote, done
}

func waitOutboundStream(t *testing.T, condition func() bool) {
	t.Helper()
	timeout := time.NewTimer(5 * time.Second)
	defer timeout.Stop()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for !condition() {
		select {
		case <-ticker.C:
		case <-timeout.C:
			t.Fatal("outbound stream condition did not complete")
		}
	}
}

func assertOutboundReleased(t *testing.T, ingress *Ingress, flow *physicalFlow, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("unused outbound stream retained its reader")
	}
	ingress.mu.Lock()
	retained := ingress.flows[flow.ref.ConnectionId] != nil
	ingress.mu.Unlock()
	if retained || len(ingress.slots) != 0 || !flow.closed.Load() {
		t.Fatalf("expired stream retained resources: flow=%v slots=%d closed=%v", retained, len(ingress.slots), flow.closed.Load())
	}
}

func TestOutboundStreamExpiryUsesOriginalOpenDeadline(t *testing.T) {
	entry := time.Now()
	deadline := entry.Add(10 * time.Second)
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()
	if expiry := outboundStreamExpiry(ctx, entry); !expiry.Equal(deadline.Add(2 * time.Minute)) {
		t.Fatalf("original open deadline changed: %v", expiry)
	}
	if expiry := outboundStreamExpiry(context.Background(), entry); !expiry.Equal(entry.Add(2 * time.Minute)) {
		t.Fatalf("deadline-free open did not retain its entry bound: %v", expiry)
	}
	// Even a wall-only deadline retains the entry sample's monotonic anchor.
	wallCtx, wallCancel := context.WithDeadline(context.Background(), time.UnixMilli(deadline.UnixMilli()))
	defer wallCancel()
	expiry := outboundStreamExpiry(wallCtx, entry)
	if expiry == expiry.Round(0) || !expiry.Equal(time.UnixMilli(deadline.UnixMilli()).Add(2*time.Minute)) {
		t.Fatal("wall-only operation deadline lost the local monotonic anchor")
	}
}

func TestOutboundOpenResultLossStillReclaimsUnusedTCP(t *testing.T) {
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
		}
	}()
	ingress := NewIngress(&pb.ProcessRef{InstanceId: "sip", Epoch: "epoch"}, new(control.Server), "test", 1)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	deadline, _ := ctx.Deadline()
	ref, err := ingress.OpenNetwork(ctx, "sip:callee@"+listener.Addr().String(), &pb.NetworkTarget{Transport: "tcp"})
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	var remote net.Conn
	select {
	case remote = <-accepted:
	case <-ctx.Done():
		cancel()
		t.Fatal(ctx.Err())
	}
	defer remote.Close()
	ingress.mu.Lock()
	flow := ingress.flows[ref.ConnectionId]
	ingress.mu.Unlock()
	t.Cleanup(func() { flow.conn.Close() })
	// Discard the successful result and cancel its caller: no native binding or
	// later callback is required to reclaim this physical resource.
	cancel()
	flow.write.Lock()
	if !flow.unusedUntil.Equal(deadline.Add(2*time.Minute)) || flow.sipUsed {
		flow.write.Unlock()
		t.Fatal("connect completion changed its original unused bound")
	}
	flow.expireUnused(flow.unusedUntil)
	flow.write.Unlock()
	waitOutboundStream(t, func() bool {
		ingress.mu.Lock()
		defer ingress.mu.Unlock()
		return ingress.flows[ref.ConnectionId] == nil && len(ingress.slots) == 0
	})
	_ = remote.SetReadDeadline(time.Now().Add(time.Second))
	if n, err := remote.Read(make([]byte, 1)); n != 0 || err == nil {
		t.Fatalf("unused connection remained live: %d %v", n, err)
	}
}

func TestUnusedOutboundStreamTimerReclaimsTCPAndTLS(t *testing.T) {
	for _, transport := range []string{"tcp", "tls"} {
		t.Run(transport, func(t *testing.T) {
			ingress, flow, remote, done := leasedOutboundStream(t, transport, 25*time.Millisecond)
			assertOutboundReleased(t, ingress, flow, done)
			_ = remote.SetReadDeadline(time.Now().Add(time.Second))
			if n, err := remote.Read(make([]byte, 1)); n != 0 || err == nil {
				t.Fatalf("expired %s stream still open: %d %v", transport, n, err)
			}
		})
	}
}

func TestOutboundKeepaliveAndPartialFrameDoNotAdopt(t *testing.T) {
	for _, transport := range []string{"tcp", "tls"} {
		for _, input := range []string{"keepalive", "partial"} {
			t.Run(transport+"/"+input, func(t *testing.T) {
				witness := &outboundPartialReadWitness{waiting: make(chan struct{})}
				ingress, flow, remote, done := leasedOutboundStream(t, transport, time.Minute, outboundStreamOptions{wrap: func(conn net.Conn) net.Conn { witness.Conn = conn; return witness }})
				if input == "keepalive" {
					if _, err := remote.Write([]byte("\r\n\r\n")); err != nil {
						t.Fatal(err)
					}
					_ = remote.SetReadDeadline(time.Now().Add(5 * time.Second))
					reply := make([]byte, 2)
					if _, err := io.ReadFull(remote, reply); err != nil || string(reply) != "\r\n" {
						t.Fatalf("keepalive reply: %q %v", reply, err)
					}
				} else {
					if _, err := remote.Write([]byte("OPTIONS sip:service@example.test SIP/2.0\r\nVia: ")); err != nil {
						t.Fatal(err)
					}
					select {
					case <-witness.waiting:
					case <-time.After(5 * time.Second):
						t.Fatal("parser did not consume the partial frame")
					}
				}
				flow.write.Lock()
				used := flow.sipUsed
				expired := flow.expireUnused(flow.unusedUntil)
				flow.write.Unlock()
				if used || !expired {
					t.Fatal("transport-only activity adopted an unused stream")
				}
				assertOutboundReleased(t, ingress, flow, done)
			})
		}
	}
}

// The second read begins only after the first fragment reached the parser.
type outboundPartialReadWitness struct {
	net.Conn
	reads   int
	waiting chan struct{}
}

func (c *outboundPartialReadWitness) Read(wire []byte) (int, error) {
	c.reads++
	if c.reads == 2 {
		close(c.waiting)
	}
	return c.Conn.Read(wire)
}

func TestCompleteOutboundIngressAdoptsBeforeRoutingResult(t *testing.T) {
	for _, transport := range []string{"tcp", "tls"} {
		t.Run(transport, func(t *testing.T) {
			routing := make(chan struct{})
			release := make(chan struct{})
			var once sync.Once
			ingress, flow, remote, _ := leasedOutboundStream(t, transport, time.Minute, outboundStreamOptions{configure: func(ingress *Ingress, _ *physicalFlow) {
				if err := ingress.ConfigureStateRuntime(&evidenceWriter{}, func(context.Context, *pb.SipMessage) (*pb.ContextSnapshot, error) { return nil, nil }, func(context.Context, *pb.FlowRef, *pb.SipMessage, uint32) error {
					close(routing)
					<-release
					return nil
				}); err != nil {
					t.Fatal(err)
				}
			}})
			t.Cleanup(func() { once.Do(func() { close(release) }) })
			if _, err := remote.Write(udpOrderMessage(t, "OPTIONS", "outbound-potential", 1).Wire); err != nil {
				t.Fatal(err)
			}
			select {
			case <-routing:
			case <-time.After(5 * time.Second):
				t.Fatal("complete SIP frame did not reach routing")
			}
			flow.write.Lock()
			used, until := flow.sipUsed, flow.unusedUntil
			expired := flow.expireUnused(until)
			timerStopped := flow.unusedTimer == nil
			flow.write.Unlock()
			if !used || expired || !timerStopped || !ingress.LiveFlow(flow.ref) {
				t.Fatal("delayed/rejected routing result allowed cleanup after SIP responsibility")
			}
			once.Do(func() { close(release) })
		})
	}
}

type outboundReadGate struct {
	net.Conn
	read    chan struct{}
	release chan struct{}
	once    sync.Once
	size    int
}

func (c *outboundReadGate) Read(buffer []byte) (int, error) {
	if c.size != 0 {
		n, err := io.ReadFull(c.Conn, buffer[:c.size])
		c.size = 0
		if n > 0 {
			c.once.Do(func() { close(c.read); <-c.release })
		}
		return n, err
	}
	return c.Conn.Read(buffer)
}

func TestOutboundExpiryPreventsBufferedFrameRouting(t *testing.T) {
	for _, transport := range []string{"tcp", "tls"} {
		t.Run(transport, func(t *testing.T) {
			frame := udpOrderMessage(t, "OPTIONS", "expired-buffer", 1).Wire
			gate := &outboundReadGate{read: make(chan struct{}), release: make(chan struct{}), size: len(frame)}
			var release sync.Once
			var routed atomic.Int32
			ingress, flow, remote, done := leasedOutboundStream(t, transport, time.Minute, outboundStreamOptions{wrap: func(conn net.Conn) net.Conn { gate.Conn = conn; return gate }, configure: func(ingress *Ingress, _ *physicalFlow) {
				if err := ingress.ConfigureStateRuntime(&evidenceWriter{}, func(context.Context, *pb.SipMessage) (*pb.ContextSnapshot, error) { return nil, nil }, func(context.Context, *pb.FlowRef, *pb.SipMessage, uint32) error { routed.Add(1); return nil }); err != nil {
					t.Fatal(err)
				}
			}})
			t.Cleanup(func() { release.Do(func() { close(gate.release) }) })
			if _, err := remote.Write(frame); err != nil {
				t.Fatal(err)
			}
			select {
			case <-gate.read:
			case <-time.After(5 * time.Second):
				t.Fatal("reader did not receive the buffered SIP frame")
			}
			flow.write.Lock()
			flow.expireUnused(flow.unusedUntil)
			flow.write.Unlock()
			release.Do(func() { close(gate.release) })
			assertOutboundReleased(t, ingress, flow, done)
			if routed.Load() != 0 {
				t.Fatal("expired stream routed an already buffered frame")
			}
		})
	}
}

func outboundCommittedWrite(t *testing.T, ingress *Ingress, flow *physicalFlow, writer *evidenceWriter) *pb.FlowWrite {
	t.Helper()
	if err := ingress.ConfigureStateRuntime(writer, func(context.Context, *pb.SipMessage) (*pb.ContextSnapshot, error) { return nil, nil }); err != nil {
		t.Fatal(err)
	}
	return &pb.FlowWrite{Flow: flow.ref, Effect: &pb.StateEffectRef{Id: "outbound-effect", AttemptId: "attempt"}, Permit: &pb.SendPermit{Authorization: &pb.EffectAuthorization{DeadlineMs: uint64(time.Now().Add(2 * time.Second).UnixMilli())}}}
}

func TestOutboundReservationAloneCannotAdoptExpiredStream(t *testing.T) {
	for _, transport := range []string{"tcp", "tls"} {
		t.Run(transport, func(t *testing.T) {
			writer := &evidenceWriter{reserved: &CommittedWrite{Wire: []byte("must-not-start")}, started: make(chan struct{}), release: make(chan struct{})}
			var release sync.Once
			var command *pb.FlowWrite
			ingress, flow, remote, done := leasedOutboundStream(t, transport, time.Minute, outboundStreamOptions{configure: func(ingress *Ingress, flow *physicalFlow) {
				command = outboundCommittedWrite(t, ingress, flow, writer)
			}})
			t.Cleanup(func() { release.Do(func() { close(writer.release) }) })
			result := make(chan *pb.DeliveryResult, 1)
			go func() { result <- ingress.committedResult(context.Background(), command) }()
			select {
			case <-writer.started:
			case <-time.After(5 * time.Second):
				t.Fatal("reservation did not start")
			}
			flow.write.Lock()
			used := flow.sipUsed
			flow.expireUnused(flow.unusedUntil)
			flow.write.Unlock()
			release.Do(func() { close(writer.release) })
			select {
			case outcome := <-result:
				if used || outcome.Outcome != pb.DeliveryResult_NOT_WRITTEN || !outcome.Recorded {
					t.Fatalf("reservation adopted or wrote after expiry: %v", outcome)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("expired reservation did not finish")
			}
			assertOutboundReleased(t, ingress, flow, done)
			_ = remote.SetReadDeadline(time.Now().Add(time.Second))
			if n, err := remote.Read(make([]byte, 64)); n != 0 || err == nil {
				t.Fatalf("expired stream emitted bytes: %d %v", n, err)
			}
		})
	}
}

type outboundWriteGate struct {
	net.Conn
	writing chan struct{}
	release chan struct{}
	fail    bool
	once    sync.Once
}

func (c *outboundWriteGate) Write(wire []byte) (int, error) {
	c.once.Do(func() { close(c.writing) })
	<-c.release
	if c.fail {
		n, err := c.Conn.Write(wire[:1])
		if err == nil {
			err = errors.New("injected failure after a real stream prefix")
		}
		return n, err
	}
	return c.Conn.Write(wire)
}

func TestOutboundAdmittedWriteWinsExpiryAndPreservesUnknown(t *testing.T) {
	for _, transport := range []string{"tcp", "tls"} {
		for _, partial := range []bool{false, true} {
			name := transport + "/written"
			if partial {
				name = transport + "/partial-unknown"
			}
			t.Run(name, func(t *testing.T) {
				gate := &outboundWriteGate{writing: make(chan struct{}), release: make(chan struct{}), fail: partial}
				var release sync.Once
				writer := &evidenceWriter{reserved: &CommittedWrite{Wire: []byte("selected-frame")}}
				var command *pb.FlowWrite
				ingress, flow, remote, _ := leasedOutboundStream(t, transport, time.Minute, outboundStreamOptions{wrap: func(conn net.Conn) net.Conn { gate.Conn = conn; return gate }, configure: func(ingress *Ingress, flow *physicalFlow) {
					command = outboundCommittedWrite(t, ingress, flow, writer)
				}})
				t.Cleanup(func() { release.Do(func() { close(gate.release) }) })
				result := make(chan *pb.DeliveryResult, 1)
				go func() { result <- ingress.committedResult(context.Background(), command) }()
				select {
				case <-gate.writing:
				case <-time.After(5 * time.Second):
					t.Fatal("authorized write did not enter socket admission")
				}
				expired := make(chan bool, 1)
				go func() {
					flow.write.Lock()
					defer flow.write.Unlock()
					expired <- flow.expireUnused(flow.unusedUntil)
				}()
				if flow.closed.Load() {
					t.Fatal("expiry closed an admitted write")
				}
				release.Do(func() { close(gate.release) })
				want := len(writer.reserved.Wire)
				if partial {
					want = 1
				}
				_ = remote.SetReadDeadline(time.Now().Add(5 * time.Second))
				wire := make([]byte, want)
				if _, err := io.ReadFull(remote, wire); err != nil || string(wire) != string(writer.reserved.Wire[:want]) {
					t.Fatalf("selected physical bytes changed: %q %v", wire, err)
				}
				select {
				case outcome := <-result:
					expected := pb.DeliveryResult_WRITTEN
					if partial {
						expected = pb.DeliveryResult_UNKNOWN
					}
					if outcome.Outcome != expected {
						t.Fatalf("expiry changed physical certainty: %v", outcome)
					}
					if partial {
						again := ingress.committedResult(context.Background(), command)
						if again.Outcome != pb.DeliveryResult_UNKNOWN || again.Recorded || writer.reserves != 1 {
							t.Fatalf("partial syscall was retried or reclassified: %v", again)
						}
						later := &pb.FlowWrite{Flow: flow.ref, Effect: &pb.StateEffectRef{Id: "another-dialog", AttemptId: "attempt"}, Permit: command.Permit}
						if next := ingress.committedResult(context.Background(), later); next.Outcome != pb.DeliveryResult_NOT_WRITTEN {
							t.Fatalf("another dialog appended behind a partial frame: %v", next)
						}
						if n, err := remote.Read(make([]byte, 64)); n != 0 || err == nil {
							t.Fatalf("poisoned stream emitted another frame: %d %v", n, err)
						}
					}
				case <-time.After(5 * time.Second):
					t.Fatal("admitted physical write did not finish")
				}
				select {
				case closed := <-expired:
					if closed {
						t.Fatal("timer reclaimed a stream after socket admission")
					}
				case <-time.After(5 * time.Second):
					t.Fatal("expiry did not finish after socket admission")
				}
				flow.write.Lock()
				used, stopped := flow.sipUsed, flow.unusedTimer == nil
				flow.write.Unlock()
				if !used || !stopped {
					t.Fatal("socket admission did not end its unused lease")
				}
			})
		}
	}
}

func TestOutboundUseAdmissionEnforcesExpiryWithoutTimerCallback(t *testing.T) {
	conn := new(framingConn)
	flow := &physicalFlow{conn: conn, unusedUntil: time.Now().Add(-time.Second)}
	flow.write.Lock()
	used := flow.admitSIPUse()
	n, err := flow.writeWire([]byte("late-frame"))
	flow.write.Unlock()
	if used || n != 0 || !errors.Is(err, net.ErrClosed) || !conn.closed || conn.sent.Len() != 0 {
		t.Fatalf("delayed expiry callback admitted work: used=%v n=%d err=%v", used, n, err)
	}
}
