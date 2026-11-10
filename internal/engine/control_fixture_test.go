package engine

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"math/big"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/coder/websocket"
	"google.golang.org/protobuf/encoding/protojson"
	"sipbridge/internal/control"
	pb "sipbridge/internal/controlpb"
)

// Exercise the real authenticated control RPC and its correlated result dispatch.
func controlFixture(t *testing.T, process, application *pb.ProcessRef, status ...func(*control.Peer, *pb.SessionStatus) error) (*control.Server, *websocket.Conn, context.Context, string) {
	t.Helper()
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	uri, err := url.Parse("spiffe://test/publication-controller")
	if err != nil {
		t.Fatal(err)
	}
	certificate := &x509.Certificate{SerialNumber: big.NewInt(1), NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, URIs: []*url.URL{uri}}
	der, err := x509.CreateCertificate(rand.Reader, certificate, certificate, public, private)
	if err != nil {
		t.Fatal(err)
	}
	key, err := x509.MarshalPKCS8PrivateKey(private)
	if err != nil {
		t.Fatal(err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: key})
	directory := t.TempDir()
	files := control.TLSFiles{Certificate: filepath.Join(directory, "certificate.pem"), Key: filepath.Join(directory, "private.pem"), Trust: filepath.Join(directory, "certificate.pem")}
	if err := os.WriteFile(files.Certificate, certPEM, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(files.Key, keyPEM, 0600); err != nil {
		t.Fatal(err)
	}
	material, err := control.NewTLSContext(files)
	if err != nil {
		t.Fatal(err)
	}
	config := control.Config{Process: process, TLS: material, AllowedURIs: []string{uri.String()}, Capabilities: []string{Capability, DialogContinuityCapability}}
	if len(status) > 0 {
		config.OnStatus = status[0]
	}
	server, err := control.NewServer(config)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(func() { cancel(); server.Close(); listener.Close() })
	go server.Serve(ctx, listener)
	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM(certPEM)
	connection, _, err := websocket.Dial(ctx, "wss://"+listener.Addr().String()+"/v1/control", &websocket.DialOptions{Subprotocols: []string{control.Subprotocol}, HTTPClient: &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: roots, Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS13}}}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { connection.CloseNow() })
	writeControllerEnvelope(t, ctx, connection, &pb.Envelope{CorrelationId: "hello", Payload: &pb.Envelope_Hello{Hello: &pb.SessionHello{Controller: application, ConnectionAttempt: 1, Major: 1, Bounds: &pb.Bounds{MaxEnvelopeBytes: control.MaxEnvelope}, Capabilities: []string{Capability, DialogContinuityCapability}}}})
	hello := readControllerEnvelope(t, ctx, connection)
	if hello.GetHelloAck() == nil {
		t.Fatal("control hello was not acknowledged")
	}
	// A client can read HelloAck before its server-side write returns. Wait for
	// the read loop, which starts only after peer publication, before direct I/O.
	writeControllerEnvelope(t, ctx, connection, &pb.Envelope{SessionId: hello.GetHelloAck().SessionId, CorrelationId: "hello-ready",
		Payload: &pb.Envelope_Ping{Ping: &pb.Heartbeat{Nonce: "hello-ready"}}})
	if barrier := readControllerEnvelope(t, ctx, connection); barrier.GetPong().GetNonce() != "hello-ready" || barrier.CorrelationId != "hello-ready" {
		t.Fatal("control peer publication barrier failed")
	}
	return server, connection, ctx, hello.GetHelloAck().SessionId
}

type publicationTestController struct {
	connection *websocket.Conn
	ctx        context.Context
	session    string
}

func publicationControllerPair(t *testing.T, process, first, second *pb.ProcessRef) (*control.Server, publicationTestController, publicationTestController) {
	t.Helper()
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	uri, err := url.Parse("spiffe://test/publication-controller")
	if err != nil {
		t.Fatal(err)
	}
	certificate := &x509.Certificate{SerialNumber: big.NewInt(1), NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, URIs: []*url.URL{uri}}
	der, err := x509.CreateCertificate(rand.Reader, certificate, certificate, public, private)
	if err != nil {
		t.Fatal(err)
	}
	key, err := x509.MarshalPKCS8PrivateKey(private)
	if err != nil {
		t.Fatal(err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: key})
	directory := t.TempDir()
	files := control.TLSFiles{Certificate: filepath.Join(directory, "certificate.pem"), Key: filepath.Join(directory, "private.pem"), Trust: filepath.Join(directory, "certificate.pem")}
	if err := os.WriteFile(files.Certificate, certPEM, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(files.Key, keyPEM, 0600); err != nil {
		t.Fatal(err)
	}
	material, err := control.NewTLSContext(files)
	if err != nil {
		t.Fatal(err)
	}
	server, err := control.NewServer(control.Config{Process: process, TLS: material, AllowedURIs: []string{uri.String()}, Capabilities: []string{Capability, DialogContinuityCapability}, OnStatus: func(*control.Peer, *pb.SessionStatus) error { return nil }})
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		server.Close()
		t.Fatal(err)
	}
	serverCtx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() { cancel(); server.Close(); listener.Close() })
	go server.Serve(serverCtx, listener)
	clientCertificate, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM(certPEM)
	connect := func(process *pb.ProcessRef) publicationTestController {
		ctx, stop := context.WithTimeout(serverCtx, 5*time.Second)
		t.Cleanup(stop)
		connection, _, err := websocket.Dial(ctx, "wss://"+listener.Addr().String()+"/v1/control", &websocket.DialOptions{Subprotocols: []string{control.Subprotocol}, HTTPClient: &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: roots, Certificates: []tls.Certificate{clientCertificate}, MinVersion: tls.VersionTLS13}}}})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { connection.CloseNow() })
		writeControllerEnvelope(t, ctx, connection, &pb.Envelope{CorrelationId: "hello-" + process.InstanceId,
			Payload: &pb.Envelope_Hello{Hello: &pb.SessionHello{Controller: process, ConnectionAttempt: 1, Major: 1,
				Bounds: &pb.Bounds{MaxEnvelopeBytes: control.MaxEnvelope}, Capabilities: []string{Capability, DialogContinuityCapability}}}})
		hello := readControllerEnvelope(t, ctx, connection)
		if hello.GetHelloAck() == nil {
			t.Fatalf("control hello for %s was not acknowledged", process.InstanceId)
		}
		return publicationTestController{connection: connection, ctx: ctx, session: hello.GetHelloAck().SessionId}
	}
	firstController, secondController := connect(first), connect(second)
	return server, firstController, secondController
}

func writeControllerEnvelope(t *testing.T, ctx context.Context, connection *websocket.Conn, message *pb.Envelope) {
	t.Helper()
	encoded, err := protojson.Marshal(message)
	if err != nil {
		t.Fatal(err)
	}
	if err := connection.Write(ctx, websocket.MessageText, encoded); err != nil {
		t.Fatal(err)
	}
}

func readControllerEnvelope(t *testing.T, ctx context.Context, connection *websocket.Conn) *pb.Envelope {
	t.Helper()
	kind, wire, err := connection.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if kind != websocket.MessageText {
		t.Fatal("control returned non-text envelope")
	}
	message := new(pb.Envelope)
	if err := protojson.Unmarshal(wire, message); err != nil {
		t.Fatal(err)
	}
	return message
}
