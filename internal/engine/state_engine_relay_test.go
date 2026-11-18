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
	"google.golang.org/protobuf/proto"
	"sipbridge/internal/control"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/state"
)

func stateControllerPair(t *testing.T, e *StateEngine, ready, draining *pb.ProcessRef) (*websocket.Conn, *websocket.Conn, context.Context, string) {
	t.Helper()
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	identity, err := url.Parse("spiffe://test/state-controller")
	if err != nil {
		t.Fatal(err)
	}
	certificate := &x509.Certificate{SerialNumber: big.NewInt(1), NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, URIs: []*url.URL{identity}}
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
	capabilities := []string{Capability, DialogContinuityCapability, control.StateCapability}
	configuration := control.Config{Process: e.cfg.Process, TLS: material, AllowedURIs: []string{identity.String()}, Capabilities: capabilities, StateAuthority: e.StateAuthority(), OnStatus: func(*control.Peer, *pb.SessionStatus) error { return nil }}
	if e.autonomousCompletion() {
		capabilities = append(capabilities, control.NativeCompletionCapability)
		configuration.Capabilities = capabilities
		configuration.OnApplicationLease = e.ApplicationLease
	}
	server, err := control.NewServer(configuration)
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
	dial := func(application *pb.ProcessRef, isReady bool) (*websocket.Conn, string) {
		connection, _, err := websocket.Dial(ctx, "wss://"+listener.Addr().String()+"/v1/control", &websocket.DialOptions{Subprotocols: []string{control.Subprotocol}, HTTPClient: &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: roots, Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS13}}}})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { connection.CloseNow() })
		writeControllerEnvelope(t, ctx, connection, &pb.Envelope{CorrelationId: "hello", Payload: &pb.Envelope_Hello{Hello: &pb.SessionHello{Controller: application, ConnectionAttempt: 1, Major: 1, Bounds: &pb.Bounds{MaxEnvelopeBytes: control.MaxEnvelope}, Capabilities: capabilities}}})
		hello := readControllerEnvelope(t, ctx, connection).GetHelloAck()
		if hello == nil {
			t.Fatal("state controller hello rejected")
		}
		writeControllerEnvelope(t, ctx, connection, &pb.Envelope{SessionId: hello.SessionId, CorrelationId: "status", Payload: &pb.Envelope_Status{Status: &pb.SessionStatus{RetainedReady: isReady, BindingsReady: isReady}}})
		return connection, hello.SessionId
	}
	first, _ := dial(ready, true)
	second, session := dial(draining, false)
	e.control = server
	e.dispatching = make(map[string]bool)
	until := time.Now().Add(time.Second)
	for {
		peers := server.ReadyPeers()
		if len(peers) == 1 && sameProcess(peers[0].Process(), ready) {
			break
		}
		if time.Now().After(until) {
			t.Fatal("ready peer A was not first while owner B drained")
		}
		time.Sleep(time.Millisecond)
	}
	return first, second, ctx, session
}

func TestStateApplicationGrantTargetsOriginalDrainingControllerOwner(t *testing.T) {
	a, b := historicalStateTestEngine(t)
	ready := &pb.ProcessRef{InstanceId: "controller-a", Epoch: "a"}
	owner := &pb.ProcessRef{InstanceId: "controller-b", Epoch: "b"}
	_, controller, ctx, session := stateControllerPair(t, b, ready, owner)
	ref, err := a.Locate(context.Background(), stateTestRequest("OPTIONS", 1, "", "relay"))
	if err != nil {
		t.Fatal(err)
	}
	result := stateTestApply(t, a, ref, "open-owner-b", stateEngineInput{Kind: "command", Application: owner, Command: &pb.Command{Context: ref, Sequence: 1, Action: &pb.Command_OpenUac{OpenUac: &pb.OpenUac{Uri: "sip:phone@example.test", Method: "OPTIONS", BindingId: "reg-alice", PrincipalId: "alice", Transport: "tls"}}}})
	if result.Error != "" {
		t.Fatal(result.Error)
	}
	record, _ := stateTestLoad(t, a, ref)
	var selected state.Effect
	for _, effect := range record.Payload.Effects {
		if effect.Kind == "wire" {
			selected = effect
		}
	}
	if selected.ID == "" {
		t.Fatal("application send intent missing")
	}
	done := make(chan struct{})
	go func() { b.dispatchEffect(record.Ref, selected); close(done) }()
	request := readControllerEnvelope(t, ctx, controller)
	egress := request.GetEgress()
	if egress == nil || egress.Authorization.AuthorityClass != pb.EffectAuthorization_APPLICATION || !sameProcess(egress.Snapshot.Application, owner) || !sameProcess(egress.Snapshot.Executor, b.cfg.Process) || !sameProcess(egress.Snapshot.Context.Owner, b.cfg.Process) {
		t.Fatal("application authorization went to a generic relay or retained stale executor metadata")
	}
	if !sameProcess(egress.Authorization.Effect.Context.Owner, a.cfg.Process) {
		t.Fatal("executor refresh mutated the immutable authorization")
	}
	flow := &pb.FlowRef{Ingress: b.cfg.Process, ConnectionId: "target", SourceIp: "192.0.2.10", SourcePort: 5061, Listener: "sip.example.test:5061", Transport: "tls"}
	writeControllerEnvelope(t, ctx, controller, &pb.Envelope{SessionId: session, CorrelationId: request.CorrelationId, Payload: &pb.Envelope_SendPermit{SendPermit: &pb.SendPermit{Authorization: egress.Authorization, Flow: flow, DirectoryEpoch: "directory", OrderingProof: "proof", CompletionScope: "scope"}}})
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("application grant response did not commit")
	}
	record, _ = stateTestLoad(t, a, ref)
	for _, effect := range record.Payload.Effects {
		if effect.ID != selected.ID {
			continue
		}
		var body stateEngineEffect
		if unmarshalRetainedFixture(effect.Payload, &body) != nil || effect.Kind != "deliver" || effect.Status != state.EffectAuthorized || !proto.Equal(body.Permit.Flow, flow) {
			t.Fatal("original owner grant was not durably committed")
		}
		return
	}
	t.Fatal("committed application grant missing")
}
