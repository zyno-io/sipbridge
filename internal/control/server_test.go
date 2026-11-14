package control

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
)

type testPKI struct {
	files TLSFiles
	root  *x509.Certificate
	key   ed25519.PrivateKey
	roots *x509.CertPool
	cert  tls.Certificate
}

func newPKI(t *testing.T) *testPKI {
	t.Helper()
	public, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	ca := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "isolated test CA"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	der, err := x509.CreateCertificate(rand.Reader, ca, ca, public, key)
	if err != nil {
		t.Fatal(err)
	}
	root, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	p := &testPKI{files: TLSFiles{Certificate: filepath.Join(dir, "server.crt"), Key: filepath.Join(dir, "server.key"), Trust: filepath.Join(dir, "clients.crt")}, root: root, key: key, roots: x509.NewCertPool()}
	p.roots.AddCert(root)
	writeTestFile(t, p.files.Trust, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
	certPEM, keyPEM := p.issue(t, 2, false, "", time.Now().Add(time.Hour))
	writeTestFile(t, p.files.Certificate, certPEM)
	writeTestFile(t, p.files.Key, keyPEM)
	certPEM, keyPEM = p.issue(t, 3, true, "spiffe://test/controller", time.Now().Add(time.Hour))
	p.cert, err = tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func writeTestFile(t *testing.T, path string, data []byte) {
	t.Helper()
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
}

func (p *testPKI) issue(t *testing.T, serial int64, client bool, uri string, expiry time.Time) ([]byte, []byte) {
	t.Helper()
	pub, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	leaf := &x509.Certificate{SerialNumber: big.NewInt(serial), NotBefore: time.Now().Add(-time.Hour), NotAfter: expiry, KeyUsage: x509.KeyUsageDigitalSignature}
	if client {
		u, err := url.Parse(uri)
		if err != nil {
			t.Fatal(err)
		}
		leaf.URIs = []*url.URL{u}
		leaf.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}
	} else {
		leaf.DNSNames = []string{"localhost"}
		leaf.IPAddresses = []net.IP{net.ParseIP("127.0.0.1")}
		leaf.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}
	}
	der, err := x509.CreateCertificate(rand.Reader, leaf, p.root, pub, p.key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
}

func testServer(t *testing.T, p *testPKI, max int, configure ...func(*Config)) (*Server, string) {
	t.Helper()
	material, err := NewTLSContext(p.files)
	if err != nil {
		t.Fatal(err)
	}
	cfg := Config{Process: &pb.ProcessRef{InstanceId: "sip-0", Epoch: "sip-epoch"}, TLS: material, AllowedURIs: []string{"spiffe://test/controller"}, MaxSessions: max, MaxClaims: 256, HelloTimeout: 150 * time.Millisecond, WriteTimeout: 200 * time.Millisecond, HeartbeatInterval: 20 * time.Millisecond, HeartbeatTimeout: 120 * time.Millisecond}
	for _, update := range configure {
		update(&cfg)
	}
	s, err := NewServer(cfg)
	if err != nil {
		t.Fatal(err)
	}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Serve(ctx, l) }()
	t.Cleanup(func() {
		cancel()
		s.Close()
		if err := <-done; err != nil {
			t.Error(err)
		}
	})
	return s, "wss://" + l.Addr().String() + "/v1/control"
}

func (p *testPKI) dial(address string) (*websocket.Conn, error) {
	client := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: p.roots, Certificates: []tls.Certificate{p.cert}}}}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	c, _, err := websocket.Dial(ctx, address, &websocket.DialOptions{HTTPClient: client, Subprotocols: []string{Subprotocol}})
	return c, err
}

func send(t *testing.T, c *websocket.Conn, e *pb.Envelope) {
	t.Helper()
	data, err := proto.Marshal(e)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := c.Write(ctx, websocket.MessageBinary, data); err != nil {
		t.Fatal(err)
	}
}

func receive(c *websocket.Conn) (*pb.Envelope, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	kind, data, err := c.Read(ctx)
	if err != nil {
		return nil, err
	}
	if kind != websocket.MessageBinary {
		return nil, fmt.Errorf("expected binary protobuf message")
	}
	envelope := &pb.Envelope{}
	err = (proto.UnmarshalOptions{DiscardUnknown: true, RecursionLimit: 32}).Unmarshal(data, envelope)
	return envelope, err
}

func hello(t *testing.T, c *websocket.Conn, instance, epoch string, attempt uint64, bound uint32) *pb.SessionHelloAck {
	t.Helper()
	send(t, c, &pb.Envelope{CorrelationId: "hello", Payload: &pb.Envelope_Hello{Hello: &pb.SessionHello{Controller: &pb.ProcessRef{InstanceId: instance, Epoch: epoch}, ConnectionAttempt: attempt, Major: 1, Bounds: &pb.Bounds{MaxEnvelopeBytes: bound}}}})
	e, err := receive(c)
	if err != nil {
		t.Fatal(err)
	}
	if e.GetHelloAck() == nil || e.CorrelationId != "hello" || e.SessionId != e.GetHelloAck().SessionId {
		t.Fatal("invalid hello acknowledgement", e)
	}
	roundtrip(t, c, e.SessionId)
	return e.GetHelloAck()
}

func roundtrip(t *testing.T, c *websocket.Conn, id string) {
	t.Helper()
	send(t, c, &pb.Envelope{CorrelationId: "roundtrip", SessionId: id, Payload: &pb.Envelope_Ping{Ping: &pb.Heartbeat{Nonce: "roundtrip"}}})
	for {
		e, err := receive(c)
		if err != nil {
			t.Fatal(err)
		}
		if e.GetPong() != nil && e.CorrelationId == "roundtrip" {
			return
		}
		if e.GetPing() != nil {
			send(t, c, &pb.Envelope{CorrelationId: e.CorrelationId, SessionId: id, Payload: &pb.Envelope_Pong{Pong: e.GetPing()}})
		}
	}
}

func TestSessionFencingAndIsolation(t *testing.T) {
	p := newPKI(t)
	_, address := testServer(t, p, 8)
	first, err := p.dial(address)
	if err != nil {
		t.Fatal(err)
	}
	defer first.CloseNow()
	a := hello(t, first, "controller-1", "epoch-1", 9007199254740993, 4096)
	if a.Major != 1 || len(a.Capabilities) != 0 || a.Bounds.MaxEnvelopeBytes != 4096 {
		t.Fatal("invalid negotiated profile")
	}
	roundtrip(t, first, a.SessionId)
	other, err := p.dial(address)
	if err != nil {
		t.Fatal(err)
	}
	defer other.CloseNow()
	b := hello(t, other, "controller-2", "epoch-2", 1, 4096)
	replacement, err := p.dial(address)
	if err != nil {
		t.Fatal(err)
	}
	defer replacement.CloseNow()
	r := hello(t, replacement, "controller-1", "epoch-1", 9007199254740994, 4096)
	// Already-buffered pre-fence heartbeats may be read before the close.
	for {
		if _, err := receive(first); err != nil {
			break
		}
	}
	roundtrip(t, other, b.SessionId)
	roundtrip(t, replacement, r.SessionId)
	for _, epoch := range []string{"epoch-1", "different-epoch"} {
		c, err := p.dial(address)
		if err != nil {
			t.Fatal(err)
		}
		send(t, c, &pb.Envelope{CorrelationId: "rejected", Payload: &pb.Envelope_Hello{Hello: &pb.SessionHello{Controller: &pb.ProcessRef{InstanceId: "controller-1", Epoch: epoch}, ConnectionAttempt: 1, Major: 1, Bounds: &pb.Bounds{MaxEnvelopeBytes: 4096}}}})
		if _, err := receive(c); err == nil {
			t.Fatal("stale/colliding hello accepted")
		}
		c.CloseNow()
	}
	roundtrip(t, replacement, r.SessionId)
}

func TestAdmissionAndHeartbeat(t *testing.T) {
	p := newPKI(t)
	_, address := testServer(t, p, 1)
	c, err := p.dial(address)
	if err != nil {
		t.Fatal(err)
	}
	defer c.CloseNow()
	hello(t, c, "controller", "epoch", 1, 512)
	if extra, err := p.dial(address); err == nil {
		extra.CloseNow()
		t.Fatal("session bound not enforced")
	}
	start := time.Now()
	for {
		if _, err := receive(c); err != nil {
			break
		}
	}
	if time.Since(start) > time.Second {
		t.Fatal("heartbeat did not fence session")
	}
}

func TestInvalidControlFrames(t *testing.T) {
	for _, name := range []string{"session", "direction", "status", "pong", "oversize", "text", "malformed", "unknown-payload", "unknown-action", "unknown-enum"} {
		t.Run(name, func(t *testing.T) {
			p := newPKI(t)
			_, address := testServer(t, p, 4)
			c, err := p.dial(address)
			if err != nil {
				t.Fatal(err)
			}
			defer c.CloseNow()
			a := hello(t, c, "controller", "epoch", 1, 512)
			e := &pb.Envelope{CorrelationId: "bad", SessionId: a.SessionId, Payload: &pb.Envelope_Ping{Ping: &pb.Heartbeat{Nonce: "bad"}}}
			switch name {
			case "session":
				e.SessionId = "wrong"
			case "direction":
				e.Payload = &pb.Envelope_HelloAck{HelloAck: a}
			case "status":
				e.Payload = &pb.Envelope_Status{Status: &pb.SessionStatus{AdmitCalls: true}}
			case "pong":
				e.Payload = &pb.Envelope_Pong{Pong: &pb.Heartbeat{Nonce: "unsolicited"}}
			case "unknown-action":
				e.Payload = &pb.Envelope_Command{Command: &pb.Command{}}
			case "unknown-enum":
				e.Payload = &pb.Envelope_RequestDecision{RequestDecision: &pb.RequestDecision{Basis: pb.OwnerIngest_Admission(99)}}
			}
			if name == "oversize" {
				// The peer can close while this invalid write is still flushing.
				_ = c.Write(context.Background(), websocket.MessageBinary, []byte(strings.Repeat(" ", 1024)))
			} else if name == "unknown-payload" {
				frame, err := proto.Marshal(&pb.Envelope{CorrelationId: "unknown", SessionId: a.SessionId})
				if err != nil {
					t.Fatal(err)
				}
				frame = protowire.AppendTag(frame, 501, protowire.BytesType)
				frame = protowire.AppendBytes(frame, nil)
				_ = c.Write(context.Background(), websocket.MessageBinary, frame)
			} else if name == "text" || name == "malformed" {
				kind := websocket.MessageText
				data := []byte(`{"correlationId":"case","ping":{}}`)
				if name == "malformed" {
					kind = websocket.MessageBinary
					data = []byte{0xff}
				}
				c.Write(context.Background(), kind, data)
			} else {
				send(t, c, e)
			}
			for {
				if _, err := receive(c); err != nil {
					if errors.Is(err, context.DeadlineExceeded) {
						t.Fatal("invalid control message was not rejected")
					}
					break
				}
			}
		})
	}
}

func TestLegacyJSONSubprotocolIsNotAccepted(t *testing.T) {
	p := newPKI(t)
	_, address := testServer(t, p, 4)
	client := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: p.roots, Certificates: []tls.Certificate{p.cert}}}}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	conn, _, err := websocket.Dial(ctx, address, &websocket.DialOptions{HTTPClient: client, Subprotocols: []string{"sipbridge.control.json.v1"}})
	if err == nil {
		conn.CloseNow()
		t.Fatal("legacy JSON control protocol was accepted")
	}
}

func TestTLSAuthorizationAndReload(t *testing.T) {
	p := newPKI(t)
	s, address := testServer(t, p, 8)
	c, err := p.dial(address)
	if err != nil {
		t.Fatal(err)
	}
	defer c.CloseNow()
	a := hello(t, c, "controller", "epoch", 1, 4096)
	before, _ := s.cfg.TLS.Config().GetConfigForClient(nil)
	writeTestFile(t, p.files.Key, []byte("not a key"))
	if s.cfg.TLS.Reload() == nil {
		t.Fatal("bad rotation accepted")
	}
	after, _ := s.cfg.TLS.Config().GetConfigForClient(nil)
	if before != after {
		t.Fatal("invalid reload replaced context")
	}
	cert, key := p.issue(t, 42, false, "", time.Now().Add(time.Hour))
	writeTestFile(t, p.files.Certificate, cert)
	writeTestFile(t, p.files.Key, key)
	if err := s.cfg.TLS.Reload(); err != nil {
		t.Fatal(err)
	}
	addressURL, _ := url.Parse(address)
	probe, err := tls.Dial("tcp", addressURL.Host, &tls.Config{RootCAs: p.roots, Certificates: []tls.Certificate{p.cert}, MinVersion: tls.VersionTLS12})
	if err != nil {
		t.Fatal(err)
	}
	if probe.ConnectionState().PeerCertificates[0].SerialNumber.Int64() != 42 {
		t.Fatal("new handshake used old certificate")
	}
	probe.Close()
	roundtrip(t, c, a.SessionId)
	wrongCert, wrongKey := p.issue(t, 50, true, "spiffe://test/not-controller", time.Now().Add(time.Hour))
	p.cert, err = tls.X509KeyPair(wrongCert, wrongKey)
	if err != nil {
		t.Fatal(err)
	}
	if rejected, err := p.dial(address); err == nil {
		rejected.CloseNow()
		t.Fatal("wrong URI accepted")
	}
	untrusted := newPKI(t)
	untrusted.roots = p.roots // Trust the server, but present a foreign client issuer.
	if rejected, err := untrusted.dial(address); err == nil {
		rejected.CloseNow()
		t.Fatal("wrong CA accepted")
	}
}

func TestUnchangedCertificateExpirationReportsFault(t *testing.T) {
	p := newPKI(t)
	expires := time.Now().Add(2 * time.Second).Truncate(time.Second)
	cert, key := p.issue(t, 100, false, "", expires)
	writeTestFile(t, p.files.Certificate, cert)
	writeTestFile(t, p.files.Key, key)
	material, err := NewTLSContext(p.files)
	if err != nil {
		t.Fatal(err)
	}
	before, _ := material.Config().GetConfigForClient(nil)
	time.Sleep(time.Until(expires) + 20*time.Millisecond)
	if material.Reload() == nil {
		t.Fatal("unchanged expired credentials did not report a fault")
	}
	after, _ := material.Config().GetConfigForClient(nil)
	if before != after {
		t.Fatal("expiry replaced a context needed by retained sessions")
	}
}
