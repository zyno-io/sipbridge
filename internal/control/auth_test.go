package control

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	pb "sipbridge/internal/controlpb"
)

func testBearer(t *testing.T) (*BearerAuthenticator, string, string) {
	t.Helper()
	token := strings.Repeat("synthetic-test-token-", 3)
	file := filepath.Join(t.TempDir(), "token")
	writeTestFile(t, file, []byte(token+"\n"))
	auth, err := NewBearerAuthenticator(file, "test-controller")
	if err != nil {
		t.Fatal(err)
	}
	return auth, token, file
}

func TestBearerRotationKeepsPrincipalAndInvalidUpdateKeepsPreviousToken(t *testing.T) {
	auth, token, file := testBearer(t)
	authenticate := func(value string) (string, error) {
		request, _ := http.NewRequest(http.MethodGet, "https://localhost/v1/control", nil)
		if value != "" {
			request.Header.Set("Authorization", value)
		}
		return auth.Authenticate(request)
	}
	principal, err := authenticate("Bearer " + token)
	if err != nil || principal != "test-controller" {
		t.Fatal("valid bearer token rejected")
	}
	for _, value := range []string{"", "Basic " + token, "Bearer invalid", "Bearer " + token + " "} {
		if _, err := authenticate(value); !errors.Is(err, ErrUnauthenticated) {
			t.Fatal("invalid authorization accepted")
		}
	}
	request, _ := http.NewRequest(http.MethodGet, "https://localhost/v1/control", nil)
	request.Header.Add("Authorization", "Bearer "+token)
	request.Header.Add("Authorization", "Bearer "+token)
	if _, err := auth.Authenticate(request); !errors.Is(err, ErrUnauthenticated) {
		t.Fatal("duplicate authorization accepted")
	}
	writeTestFile(t, file, []byte("bad\n"))
	if err := auth.Reload(); err == nil {
		t.Fatal("invalid token rotation installed")
	}
	if _, err := authenticate("Bearer " + token); err != nil {
		t.Fatal("invalid rotation revoked previous token")
	}
	replacement := strings.Repeat("synthetic-new-token-", 3)
	writeTestFile(t, file, []byte(replacement))
	if err := auth.Reload(); err != nil {
		t.Fatal(err)
	}
	if _, err := authenticate("Bearer " + token); !errors.Is(err, ErrUnauthenticated) {
		t.Fatal("old token accepted after valid rotation")
	}
	if next, err := authenticate("Bearer " + replacement); err != nil || next != principal {
		t.Fatal("rotation changed principal")
	}
	if strings.Contains(fmt.Sprintf("%v %#v", auth, auth), replacement) {
		t.Fatal("authenticator formatting exposed token")
	}
}

func TestBearerTokenFileLengthAndTerminalNewline(t *testing.T) {
	for _, test := range []struct {
		name  string
		data  string
		valid bool
	}{
		{"maximum", strings.Repeat("x", 4096), true},
		{"maximum-lf", strings.Repeat("x", 4096) + "\n", true},
		{"maximum-crlf", strings.Repeat("x", 4096) + "\r\n", true},
		{"oversized", strings.Repeat("x", 4097), false},
		{"oversized-lf", strings.Repeat("x", 4097) + "\n", false},
		{"multiple-lf", strings.Repeat("x", 32) + "\n\n", false},
		{"multiple-crlf", strings.Repeat("x", 32) + "\r\n\r\n", false},
		{"bare-cr", strings.Repeat("x", 32) + "\r", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			file := filepath.Join(t.TempDir(), "token")
			writeTestFile(t, file, []byte(test.data))
			auth, err := NewBearerAuthenticator(file, "token-boundary-test")
			if (err == nil) != test.valid {
				t.Fatalf("valid=%t: %v", test.valid, err)
			}
			if test.valid {
				request, _ := http.NewRequest(http.MethodGet, "http://localhost/v1/control", nil)
				request.Header.Set("Authorization", "Bearer "+strings.Repeat("x", 4096))
				if _, err := auth.Authenticate(request); err != nil {
					t.Fatal("maximum-length token was not preserved")
				}
			}
		})
	}
}

func bearerDial(address, token string, client *http.Client) (*websocket.Conn, *http.Response, error) {
	headers := http.Header{}
	if token != "" {
		headers.Set("Authorization", "Bearer "+token)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	return websocket.Dial(ctx, address, &websocket.DialOptions{HTTPClient: client, HTTPHeader: headers, Subprotocols: []string{Subprotocol}})
}

func TestBearerTLSDoesNotRequireClientCertificateAndStillVerifiesServer(t *testing.T) {
	pki := newPKI(t)
	auth, token, file := testBearer(t)
	material, err := NewPublicTLSContext(TLSFiles{Certificate: pki.files.Certificate, Key: pki.files.Key})
	if err != nil {
		t.Fatal(err)
	}
	server, address := testServer(t, pki, 8, func(cfg *Config) {
		cfg.TLS, cfg.Authenticator, cfg.AllowedURIs = material, auth, nil
		cfg.HeartbeatInterval, cfg.HeartbeatTimeout = time.Second, 6*time.Second
	})
	client := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: pki.roots}}}
	defer client.CloseIdleConnections()
	for _, value := range []string{"", "wrong-token"} {
		connection, response, err := bearerDial(address, value, client)
		if connection != nil {
			connection.CloseNow()
		}
		if err == nil || response == nil || response.StatusCode != http.StatusUnauthorized {
			t.Fatal("unauthenticated upgrade was not rejected before hello")
		}
	}
	connection, _, err := bearerDial(address, token, client)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.CloseNow()
	hello(t, connection, "controller", "first-process", 1, 4096)
	peer, err := server.Peer(&pb.ProcessRef{InstanceId: "controller", Epoch: "first-process"})
	if err != nil || peer.Principal() != "test-controller" {
		t.Fatal("authenticated principal was not bound to peer")
	}
	rotated := strings.Repeat("synthetic-rotated-token-", 3)
	writeTestFile(t, file, []byte(rotated))
	if err := auth.Reload(); err != nil {
		t.Fatal(err)
	}
	roundtrip(t, connection, peer.session.id)
	if old, response, err := bearerDial(address, token, client); err == nil {
		old.CloseNow()
		t.Fatal("rotated-out token admitted a new upgrade")
	} else if response == nil || response.StatusCode != http.StatusUnauthorized {
		t.Fatal("rotated token failed outside authentication")
	}
	replacement, _, err := bearerDial(address, rotated, client)
	if err != nil {
		t.Fatal(err)
	}
	defer replacement.CloseNow()
	hello(t, replacement, "another-controller", "another-process", 1, 4096)
	rotatedPeer, err := server.Peer(&pb.ProcessRef{InstanceId: "another-controller", Epoch: "another-process"})
	if err != nil || rotatedPeer.Principal() != peer.Principal() {
		t.Fatal("rotation changed the stable principal")
	}
	untrusted := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12}}}
	defer untrusted.CloseIdleConnections()
	if other, _, err := bearerDial(address, rotated, untrusted); err == nil {
		other.CloseNow()
		t.Fatal("client accepted an untrusted server certificate")
	}
}

func TestExplicitBearerPlainControlNeedsNoTLSAndRejectsNonLoopback(t *testing.T) {
	auth, token, _ := testBearer(t)
	cfg := Config{Process: &pb.ProcessRef{InstanceId: "sip", Epoch: "epoch"}, Authenticator: auth, AllowPlain: true}
	server, err := NewServer(cfg)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- server.Serve(ctx, listener) }()
	t.Cleanup(func() {
		cancel()
		server.Close()
		listener.Close()
		if err := <-done; err != nil {
			t.Error(err)
		}
	})
	connection, _, err := bearerDial("ws://"+listener.Addr().String()+"/v1/control", token, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.CloseNow()
	hello(t, connection, "controller", "plain-process", 1, 4096)
	if !server.CredentialsValid() {
		t.Fatal("plaintext bearer readiness incorrectly requires TLS")
	}
	remoteListener, err := net.Listen("tcp", "0.0.0.0:0")
	if err != nil {
		t.Fatal(err)
	}
	defer remoteListener.Close()
	other, err := NewServer(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	if err := other.Serve(context.Background(), remoteListener); err == nil {
		t.Fatal("plaintext listener exposed on a non-loopback address")
	}
}

func TestInvalidControlAuthConfigurationDoesNotEnableFallback(t *testing.T) {
	auth, _, _ := testBearer(t)
	for _, cfg := range []Config{
		{Process: &pb.ProcessRef{InstanceId: "sip", Epoch: "epoch"}, Authenticator: auth},
		{Process: &pb.ProcessRef{InstanceId: "sip", Epoch: "epoch"}, AllowedURIs: []string{"spiffe://test/controller"}, AllowPlain: true},
	} {
		if _, err := NewServer(cfg); err == nil {
			t.Fatal("invalid transport/auth configuration accepted")
		}
	}
	for _, principal := range []string{"", "contains space", "line\nbreak"} {
		if _, err := NewBearerAuthenticator("missing", principal); err == nil {
			t.Fatal("invalid bearer configuration accepted")
		}
	}
	empty := filepath.Join(t.TempDir(), "empty")
	if err := os.WriteFile(empty, nil, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := NewBearerAuthenticator(empty, "principal"); err == nil {
		t.Fatal("empty bearer token enabled authentication")
	}
}

func TestAuthenticatedPrincipalsCannotSupersedeEachOthersProcessClaim(t *testing.T) {
	pki := newPKI(t)
	server, address := testServer(t, pki, 8, func(cfg *Config) {
		cfg.AllowedURIs = []string{"spiffe://test/controller", "spiffe://test/another"}
		cfg.HeartbeatInterval, cfg.HeartbeatTimeout = time.Second, 6*time.Second
	})
	first, err := pki.dial(address)
	if err != nil {
		t.Fatal(err)
	}
	defer first.CloseNow()
	ack := hello(t, first, "same-process-id", "epoch", 1, 4096)
	original, err := server.Peer(&pb.ProcessRef{InstanceId: "same-process-id", Epoch: "epoch"})
	if err != nil {
		t.Fatal(err)
	}
	cert, key := pki.issue(t, 43, true, "spiffe://test/another", time.Now().Add(time.Hour))
	otherIdentity, err := tls.X509KeyPair(cert, key)
	if err != nil {
		t.Fatal(err)
	}
	client := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: pki.roots, Certificates: []tls.Certificate{otherIdentity}}}}
	defer client.CloseIdleConnections()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	other, _, err := websocket.Dial(ctx, address, &websocket.DialOptions{HTTPClient: client, Subprotocols: []string{Subprotocol}})
	if err != nil {
		t.Fatal(err)
	}
	defer other.CloseNow()
	send(t, other, &pb.Envelope{CorrelationId: "steal", Payload: &pb.Envelope_Hello{Hello: &pb.SessionHello{Controller: &pb.ProcessRef{InstanceId: "same-process-id", Epoch: "epoch"}, ConnectionAttempt: 2, Major: 1, Bounds: &pb.Bounds{MaxEnvelopeBytes: 4096}}}})
	if _, err := receive(other); err == nil {
		t.Fatal("different authenticated principal superseded the claim")
	}
	if original.Context().Err() != nil {
		t.Fatal("foreign claim attempt disconnected the current holder")
	}
	roundtrip(t, first, ack.SessionId)
}
