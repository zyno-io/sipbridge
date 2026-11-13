package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"flag"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/encoding/protojson"
	controlpkg "sipbridge/internal/control"
	pb "sipbridge/internal/controlpb"
)

func TestDegradedStartupHelper(t *testing.T) {
	if os.Getenv("SIPBRIDGE_DEGRADED_HELPER") != "true" {
		return
	}
	var arguments []string
	if err := json.Unmarshal([]byte(os.Getenv("SIPBRIDGE_DEGRADED_ARGS")), &arguments); err != nil {
		os.Exit(2)
	}
	flag.CommandLine = flag.NewFlagSet("sipbridge-degraded-helper", flag.ContinueOnError)
	os.Args = append([]string{os.Args[0]}, arguments...)
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	os.Exit(0)
}

func reserveStartupEndpoint(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	listener.Close()
	return address
}

func TestEmptyStateStartsListenersAndAdmitsControlWithoutSetup(t *testing.T) {
	testUnavailableStartup(t, false, false)
}
func TestRedisOutageStartupServes503AndRecoversWithoutRestart(t *testing.T) {
	testUnavailableStartup(t, true, false)
}
func TestObsoleteBootstrapMarkerDoesNotBlockService(t *testing.T) {
	testUnavailableStartup(t, false, true)
}
func TestRunningControlRecoversFromNamespaceLossWithoutRestart(t *testing.T) {
	testUnavailableStartup(t, false, false, true)
}
func TestMemoryDefaultStartsAndNegotiatesWithoutRedis(t *testing.T) {
	testStateStartup(t, "memory", false, false)
}
func TestBearerControlStartsWithoutClientCertificate(t *testing.T) {
	for _, transport := range []string{"tls", "plain"} {
		t.Run(transport, func(t *testing.T) {
			testStateStartupWithSecurity(t, "memory", false, false, "bearer-"+transport)
		})
	}
}
func testUnavailableStartup(t *testing.T, offline, partial bool, erase ...bool) {
	testStateStartup(t, "redis", offline, partial, erase...)
}
func testStateStartup(t *testing.T, backend string, offline, partial bool, erase ...bool) {
	testStateStartupWithSecurity(t, backend, offline, partial, "mtls", erase...)
}
func testStateStartupWithSecurity(t *testing.T, backend string, offline, partial bool, securityMode string, erase ...bool) {
	testStateStartupWithShutdown(t, backend, offline, partial, securityMode, false, erase...)
}
func testStateStartupWithShutdown(t *testing.T, backend string, offline, partial bool, securityMode string, observeDrain bool, erase ...bool) {
	testStateStartupWithHook(t, backend, offline, partial, securityMode, observeDrain, nil, erase...)
}

func testStateStartupWithHook(t *testing.T, backend string, offline, partial bool, securityMode string, observeDrain bool, hook func(*testing.T, *websocket.Conn, *pb.SessionHelloAck, string, *tls.Config), erase ...bool) {
	endpoint := os.Getenv("SIPBRIDGE_STATE_TEST_REDIS")
	if endpoint == "" && backend == "redis" {
		t.Skip("disposable Redis required")
	}
	dir := t.TempDir()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	uri, _ := url.Parse("spiffe://fixture/controller")
	cert := &x509.Certificate{URIs: []*url.URL{uri}, SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "degraded startup fixture"}, DNSNames: []string{"localhost"}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}}
	der, err := x509.CreateCertificate(rand.Reader, cert, cert, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	encodedKey, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	certificate, keyPath := filepath.Join(dir, "cert.pem"), filepath.Join(dir, "key.pem")
	if err := os.WriteFile(certificate, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: encodedKey}), 0600); err != nil {
		t.Fatal(err)
	}
	sip, public, control, admin := reserveStartupEndpoint(t), reserveStartupEndpoint(t), reserveStartupEndpoint(t), reserveStartupEndpoint(t)
	namespace := "empty-" + uuid.NewString()
	prefix := "sipstate:startup-test"
	connectEndpoint := endpoint
	if offline {
		connectEndpoint = reserveStartupEndpoint(t)
	}
	settings := map[string]string{"SIPBRIDGE_STATE_BACKEND": "redis", "SIPBRIDGE_REDIS_ADDRESS": connectEndpoint, "SIPBRIDGE_REDIS_ALLOW_PLAINTEXT": "true", "SIPBRIDGE_STATE_NAMESPACE": namespace, "SIPBRIDGE_STATE_GENERATION": "degraded-test", "SIPBRIDGE_STATE_PREFIX": prefix, "SIPBRIDGE_STATE_IDENTITY_KEY": base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{42}, 32)), "SIPBRIDGE_STATE_PARTITIONS": "2"}
	if backend == "memory" {
		delete(settings, "SIPBRIDGE_STATE_BACKEND")
		delete(settings, "SIPBRIDGE_STATE_IDENTITY_KEY")
		delete(settings, "SIPBRIDGE_REDIS_ADDRESS")
		delete(settings, "SIPBRIDGE_REDIS_ALLOW_PLAINTEXT")
	}
	fixtureRedis := redis.NewClient(&redis.Options{Addr: endpoint, MaxRetries: -1})
	t.Cleanup(func() {
		defer fixtureRedis.Close()
		if backend == "memory" {
			return
		}
		var cursor uint64
		for {
			keys, next, err := fixtureRedis.Scan(context.Background(), cursor, prefix+":"+namespace+":*", 128).Result()
			if err != nil {
				t.Error(err)
				return
			}
			if len(keys) > 0 {
				if err := fixtureRedis.Del(context.Background(), keys...).Err(); err != nil {
					t.Error(err)
					return
				}
			}
			if next == 0 {
				return
			}
			cursor = next
		}
	})
	if partial {
		if err := fixtureRedis.Set(context.Background(), prefix+":"+namespace+":{p0000}:authority", "obsolete", 0).Err(); err != nil {
			t.Fatal(err)
		}
	}
	arguments := []string{"--control-listen", control, "--sip-listen", public, "--sip-plain-listen", sip, "--admin-listen", admin, "--sip-cert", certificate, "--sip-key", keyPath, "--instance", "degraded-fixture", "--advertise", public, "--advertise-plain", sip}
	arguments = append(arguments, "--sip-withdrawal-grace", "0s", "--sip-drain-timeout", "500ms")
	if observeDrain {
		arguments = append(arguments, "--sip-withdrawal-grace", "500ms", "--sip-drain-timeout", "1500ms")
	}
	controlURL := "wss://" + control + "/v1/control"
	controlHeaders := http.Header{}
	if securityMode == "mtls" {
		arguments = append(arguments, "--control-cert", certificate, "--control-key", keyPath, "--client-ca", certificate, "--allowed-client-uri", "spiffe://fixture/controller")
	} else {
		token := strings.Repeat("startup-fixture-token-", 3)
		tokenFile := filepath.Join(dir, "control-token")
		if err := os.WriteFile(tokenFile, []byte(token+"\n"), 0600); err != nil {
			t.Fatal(err)
		}
		arguments = append(arguments, "--control-auth=bearer", "--control-bearer-token-file", tokenFile, "--control-principal", "fixture-controller")
		controlHeaders.Set("Authorization", "Bearer "+token)
		if securityMode == "bearer-plain" {
			arguments = append(arguments, "--control-transport=plain")
			controlURL = "ws://" + control + "/v1/control"
		} else {
			arguments = append(arguments, "--control-cert", certificate, "--control-key", keyPath)
		}
	}
	args, _ := json.Marshal(arguments)
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	command := exec.Command(executable, "-test.run=^TestDegradedStartupHelper$")
	for _, entry := range os.Environ() {
		if !strings.HasPrefix(entry, "SIPBRIDGE_") {
			command.Env = append(command.Env, entry)
		}
	}
	for name, value := range settings {
		command.Env = append(command.Env, name+"="+value)
	}
	command.Env = append(command.Env, "SIPBRIDGE_DEGRADED_HELPER=true", "SIPBRIDGE_DEGRADED_ARGS="+string(args))
	var logs bytes.Buffer
	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("startup process logs: %s", logs.String())
		}
	})
	command.Stdout, command.Stderr = &logs, &logs
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	exited := make(chan struct{})
	var processError error
	go func() { processError = command.Wait(); close(exited) }()
	t.Cleanup(func() {
		command.Process.Signal(syscall.SIGTERM)
		select {
		case <-exited:
		case <-time.After(3 * time.Second):
			command.Process.Kill()
			<-exited
		}
	})
	client := &http.Client{Timeout: 200 * time.Millisecond}
	until := time.Now().Add(5 * time.Second)
	healthy := false
	for time.Now().Before(until) {
		response, err := client.Get("http://" + admin + "/healthz")
		if err == nil {
			response.Body.Close()
			if response.StatusCode == 200 {
				healthy = true
				break
			}
		}
		select {
		case <-exited:
			t.Fatalf("degraded startup exited: %v, logs %s", processError, logs.String())
		default:
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !healthy {
		t.Fatal("degraded startup never opened health listener")
	}
	servingResponse, err := client.Get("http://" + admin + "/sip-servingz")
	if err != nil {
		t.Fatal(err)
	}
	servingResponse.Body.Close()
	if servingResponse.StatusCode != 200 {
		t.Fatal("listening process was removed from public traffic during Redis outage", servingResponse.StatusCode)
	}
	response, err := client.Get("http://" + admin + "/sip-readyz")
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != 503 {
		t.Fatal("uninitialized listener published readiness", response.StatusCode)
	}
	connection, err := net.DialTimeout("tcp", sip, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	connection.SetDeadline(time.Now().Add(2 * time.Second))
	wire := "REGISTER sip:fixture.test SIP/2.0\r\nVia: SIP/2.0/TCP 127.0.0.1:5060;branch=z9hG4bK-degraded;rport\r\nFrom: <sip:phone@fixture.test>;tag=phone\r\nTo: <sip:phone@fixture.test>\r\nCall-ID: degraded-register\r\nCSeq: 1 REGISTER\r\nMax-Forwards: 70\r\nContent-Length: 0\r\n\r\n"
	if _, err := connection.Write([]byte(wire)); err != nil {
		t.Fatal(err)
	}
	status, err := bufio.NewReader(connection).ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(status, "SIP/2.0 503 ") {
		t.Fatal("running unavailable listener did not reject SIP", status)
	}
	redisClient := redis.NewClient(&redis.Options{Addr: endpoint, MaxRetries: -1})
	defer redisClient.Close()
	if backend == "redis" {
		keys, _, err := redisClient.Scan(context.Background(), 0, prefix+":"+namespace+":*", 1000).Result()
		if err != nil || offline && len(keys) != 0 {
			t.Fatal("offline startup allocated state", err)
		}
	}
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
	identity, err := tls.LoadX509KeyPair(certificate, keyPath)
	if err != nil {
		t.Fatal(err)
	}
	transport := &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots, Certificates: []tls.Certificate{identity}, ServerName: "localhost"}}
	if securityMode != "mtls" {
		transport.TLSClientConfig.Certificates = nil
	}
	defer transport.CloseIdleConnections()
	// A control socket may connect while the SIP listener is degraded, but it
	// must refuse while Redis is offline, and admit without setup once available.
	attempt, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	probe, _, err := websocket.Dial(attempt, controlURL, &websocket.DialOptions{HTTPClient: &http.Client{Transport: transport}, HTTPHeader: controlHeaders, Subprotocols: []string{controlpkg.Subprotocol}})
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	capabilities := []string{"sip.v1", controlpkg.StateCapability, controlpkg.NativeCompletionCapability}
	if backend == "memory" {
		capabilities = append(capabilities, controlpkg.RequestDecisionCapability, controlpkg.CommandAdmissionCapability, controlpkg.CommandBudgetCapability, controlpkg.DialogContinuityCapability)
	}
	probeHello := &pb.Envelope{CorrelationId: "unavailable-hello", Payload: &pb.Envelope_Hello{Hello: &pb.SessionHello{Controller: &pb.ProcessRef{InstanceId: "unavailable-probe", Epoch: "epoch"}, ConnectionAttempt: 1, Major: 1, Capabilities: capabilities, Bounds: &pb.Bounds{MaxEnvelopeBytes: 4096}}}}
	probeWire, err := protojson.Marshal(probeHello)
	if err == nil {
		err = probe.Write(attempt, websocket.MessageText, probeWire)
	}
	if err != nil {
		probe.CloseNow()
		cancel()
		t.Fatal(err)
	}
	_, replyWire, probeErr := probe.Read(attempt)
	probe.CloseNow()
	cancel()
	if offline && probeErr == nil {
		reply := &pb.Envelope{}
		if protojson.Unmarshal(replyWire, reply) == nil && reply.GetHelloAck() != nil {
			t.Fatal("unavailable/partial bootstrap admitted control hello")
		}
	}
	if !offline && probeErr != nil {
		t.Fatal("empty state did not admit control hello", probeErr)
	}
	if offline {
		startStartupRedisProxy(t, connectEndpoint, endpoint)
	}
	var controller *websocket.Conn
	recoveryDeadline := time.Now().Add(8 * time.Second)
	hello := &pb.Envelope{CorrelationId: "recover-hello", Payload: &pb.Envelope_Hello{Hello: &pb.SessionHello{Controller: &pb.ProcessRef{InstanceId: "surviving-controller", Epoch: "same-process"}, ConnectionAttempt: 1, Major: 1, Capabilities: capabilities, Bounds: &pb.Bounds{MaxEnvelopeBytes: controlpkg.MaxEnvelope}}}}
	if hook != nil && backend != "memory" {
		hello.GetHello().Capabilities = append(hello.GetHello().Capabilities, controlpkg.DialogContinuityCapability)
	}
	if securityMode != "mtls" {
		hello.GetHello().Capabilities = append(hello.GetHello().Capabilities, controlpkg.ControllerReattachCapability)
	}
	var ack *pb.SessionHelloAck
	for time.Now().Before(recoveryDeadline) {
		attempt, cancel := context.WithTimeout(context.Background(), time.Second)
		candidate, _, err := websocket.Dial(attempt, controlURL, &websocket.DialOptions{HTTPClient: &http.Client{Transport: transport}, HTTPHeader: controlHeaders, Subprotocols: []string{controlpkg.Subprotocol}})
		if err == nil {
			encoded, _ := protojson.Marshal(hello)
			err = candidate.Write(attempt, websocket.MessageText, encoded)
			if err == nil {
				_, wire, readErr := candidate.Read(attempt)
				reply := &pb.Envelope{}
				if readErr == nil && protojson.Unmarshal(wire, reply) == nil && reply.GetHelloAck() != nil {
					ack = reply.GetHelloAck()
					controller = candidate
					cancel()
					break
				}
			}
			candidate.CloseNow()
		}
		cancel()
		time.Sleep(30 * time.Millisecond)
	}
	if controller == nil {
		t.Fatal("running process never recovered real authenticated control after Redis returned")
	}
	defer controller.CloseNow()
	if !slices.Contains(ack.Capabilities, controlpkg.StateCapability) || slices.Contains(ack.Capabilities, "sip.redis-state.v1") {
		t.Fatal("service must negotiate only the common state capability", ack.Capabilities)
	}
	if ack.StateAuthority == nil || ack.StateAuthority.Generation != settings["SIPBRIDGE_STATE_GENERATION"] {
		t.Fatal("offline startup changed the configuration-derived logical authority")
	}
	if backend == "redis" && securityMode != "mtls" && (ack.ApplicationLease.GetRecoveryToken() == "" || ack.ApplicationLease.GetAttachmentGeneration() != 1 || ack.ApplicationLease.GetLeaseLifetimeMs() != 5000) {
		t.Fatal("real service did not negotiate a recoverable bearer owner")
	}
	if backend == "memory" && (ack.ApplicationLease.GetTerm() == "" || ack.ApplicationLease.GetRecoveryToken() != "" || slices.Contains(ack.Capabilities, controlpkg.ControllerReattachCapability)) {
		t.Fatal("memory must negotiate an ordinary owner without checkpoint recovery")
	}
	if backend == "memory" && !slices.Contains(ack.Capabilities, controlpkg.RequestDecisionCapability) {
		t.Fatal("memory must negotiate the direct request decision contract")
	}
	frame := &pb.Envelope{SessionId: ack.SessionId, CorrelationId: "recovered-status", Payload: &pb.Envelope_Status{Status: &pb.SessionStatus{RetainedReady: true, BindingsReady: true}}}
	encoded, _ := protojson.Marshal(frame)
	attempt, cancel = context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := controller.Write(attempt, websocket.MessageText, encoded); err != nil {
		t.Fatal(err)
	}
	for time.Now().Before(recoveryDeadline) {
		ready, err := client.Get("http://" + admin + "/sip-readyz")
		if err == nil {
			ready.Body.Close()
			if ready.StatusCode == 200 {
				if len(erase) > 0 && erase[0] {
					testStartupStateLoss(t, redisClient, prefix+":"+namespace+":", controller, ack, hello, transport, control)
				}
				if hook != nil {
					hook(t, controller, ack, public, transport.TLSClientConfig)
				}
				if observeDrain {
					testStartupDrain(t, command, client, admin, public, transport.TLSClientConfig, exited, &processError)
				}
				return
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("same process did not regain operation readiness after Redis/control recovery")
}

func startStartupRedisProxy(t *testing.T, address, target string) {
	t.Helper()
	listener, err := net.Listen("tcp", address)
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	connections := make([]net.Conn, 0)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			incoming, err := listener.Accept()
			if err != nil {
				return
			}
			outgoing, err := net.DialTimeout("tcp", target, time.Second)
			if err != nil {
				incoming.Close()
				continue
			}
			mu.Lock()
			connections = append(connections, incoming, outgoing)
			mu.Unlock()
			go func() {
				defer incoming.Close()
				defer outgoing.Close()
				finished := make(chan struct{})
				go func() { io.Copy(outgoing, incoming); outgoing.Close(); close(finished) }()
				io.Copy(incoming, outgoing)
				incoming.Close()
				<-finished
			}()
		}
	}()
	t.Cleanup(func() {
		listener.Close()
		<-done
		mu.Lock()
		defer mu.Unlock()
		for _, connection := range connections {
			connection.Close()
		}
	})
}

// Exercise an already admitted control session through total namespace loss.
// Redis deletion is limited to this unique test namespace; the SIP process lives.
func testStartupStateLoss(t *testing.T, client *redis.Client, prefix string, controller *websocket.Conn, original *pb.SessionHelloAck, hello *pb.Envelope, transport *http.Transport, control string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	if original.ApplicationLease == nil {
		t.Fatal("missing initial application term")
	}
	var cursor uint64
	for {
		keys, next, err := client.Scan(ctx, cursor, prefix+"*", 128).Result()
		if err != nil {
			t.Fatal(err)
		}
		if len(keys) > 0 {
			if err := client.Del(ctx, keys...).Err(); err != nil {
				t.Fatal(err)
			}
		}
		if next == 0 {
			break
		}
		cursor = next
	}
	closed := false
	for !closed {
		_, wire, err := controller.Read(ctx)
		if err != nil {
			if ctx.Err() != nil {
				t.Fatal("missing lease did not close control session", err)
			}
			closed = true
			break
		}
		frame := &pb.Envelope{}
		if err := protojson.Unmarshal(wire, frame); err != nil {
			t.Fatal(err)
		}
		if ping := frame.GetPing(); ping != nil {
			pong := &pb.Envelope{SessionId: original.SessionId, CorrelationId: frame.CorrelationId, Payload: &pb.Envelope_Pong{Pong: ping}}
			wire, err := protojson.Marshal(pong)
			if err != nil {
				t.Fatal(err)
			}
			if err := controller.Write(ctx, websocket.MessageText, wire); err != nil {
				t.Fatal(err)
			}
		}
	}
	hello.GetHello().ConnectionAttempt = 2
	replacement, _, err := websocket.Dial(ctx, "wss://"+control+"/v1/control", &websocket.DialOptions{HTTPClient: &http.Client{Transport: transport}, Subprotocols: []string{controlpkg.Subprotocol}})
	if err != nil {
		t.Fatal("normal reconnect failed", err)
	}
	defer replacement.CloseNow()
	wire, err := protojson.Marshal(hello)
	if err != nil {
		t.Fatal(err)
	}
	if err := replacement.Write(ctx, websocket.MessageText, wire); err != nil {
		t.Fatal(err)
	}
	_, wire, err = replacement.Read(ctx)
	if err != nil {
		t.Fatal("empty Redis required setup before hello", err)
	}
	frame := &pb.Envelope{}
	if err := protojson.Unmarshal(wire, frame); err != nil {
		t.Fatal(err)
	}
	ack := frame.GetHelloAck()
	if ack == nil || ack.ApplicationLease == nil || ack.ApplicationLease.Term == original.ApplicationLease.Term {
		t.Fatal("reconnect did not acquire a fresh term")
	}
}
