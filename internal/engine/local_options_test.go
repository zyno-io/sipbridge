package engine

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/emiago/sipgo/sip"
	"google.golang.org/protobuf/proto"
	"sipbridge/internal/control"
	pb "sipbridge/internal/controlpb"
)

func localOptionsWire(transport, toTag, headers, body string) []byte {
	wire := string(methodPolicyWire("OPTIONS", transport, toTag))
	return []byte(strings.Replace(wire, "Content-Length: 0\r\n\r\n", fmt.Sprintf("%sContent-Length: %d\r\n\r\n%s", headers, len(body), body), 1))
}

func forbidOptionsState(i *Ingress) *atomic.Int64 {
	probes := new(atomic.Int64)
	i.stateWriter = &evidenceWriter{}
	i.stateCompletion = func(context.Context, *pb.FlowRef, *pb.SipMessage) (bool, error) { probes.Add(1); return false, nil }
	i.directCompletion = func(context.Context, *pb.FlowRef, *pb.SipMessage, sip.Message) (bool, error) {
		probes.Add(1)
		return false, nil
	}
	i.directRequest = func(context.Context, *control.Peer, *pb.FlowRef, *pb.SipMessage, sip.Message) error {
		probes.Add(1)
		return nil
	}
	i.stateLocate = func(context.Context, *pb.SipMessage) (*pb.ContextSnapshot, error) { probes.Add(1); return nil, nil }
	i.stateReject = func(context.Context, *pb.FlowRef, *pb.SipMessage, uint32) error { probes.Add(1); return nil }
	return probes
}

func assertLocalOptionsResponse(t *testing.T, wire []byte, status int, toTag string, policy MethodPolicy, source net.Addr) *sip.Response {
	t.Helper()
	parsed, err := parseObservedMessage(wire)
	if err != nil {
		t.Fatal(err)
	}
	response, ok := parsed.(*sip.Response)
	if !ok || response.StatusCode != status || response.CSeq().MethodName != sip.OPTIONS || response.Reason != reason(uint32(status)) {
		t.Fatalf("unexpected OPTIONS response: %s", wire)
	}
	if header(response, "Allow") != policy.allow() || len(response.Body()) != 0 || header(response, "Content-Type") != "" {
		t.Fatalf("invented or incorrect OPTIONS capabilities: %s", wire)
	}
	host, port, err := net.SplitHostPort(source.String())
	if err != nil {
		t.Fatal(err)
	}
	if response.Via().Params.GetOr("received", "") != host || response.Via().Params.GetOr("rport", "") != port {
		t.Fatalf("OPTIONS did not use observed source: %s", wire)
	}
	if value := tag(response.To().Params); toTag != "" && value != "retained" || toTag == "" && !strings.HasPrefix(value, "options-") {
		t.Fatalf("OPTIONS changed or omitted deterministic To tag: %q", value)
	}
	return response
}

func TestLocalOptionsUDPBypassesStateAndControlWithoutRetainedHistory(t *testing.T) {
	for _, toTag := range []string{"", ";tag=retained"} {
		t.Run(toTag, func(t *testing.T) {
			policy := MethodPolicy{DisableRegister: true, DisableSubscribe: true}
			var probes *atomic.Int64
			f := newUDPOrderFixture(t, func(i *Ingress) { i.methodPolicy = policy; probes = forbidOptionsState(i) })
			request := localOptionsWire("udp", toTag, "", "")
			var previous string
			for attempt := 0; attempt < 32; attempt++ {
				wireRequest := request
				if attempt > 1 {
					wireRequest = []byte(strings.Replace(string(request), "Call-ID: policy-call", fmt.Sprintf("Call-ID: options-%d", attempt), 1))
				}
				if _, err := f.remote.WriteTo(wireRequest, f.listener.LocalAddr()); err != nil {
					t.Fatal(err)
				}
				_ = f.remote.SetReadDeadline(time.Now().Add(time.Second))
				wire := make([]byte, 4096)
				n, _, err := f.remote.ReadFrom(wire)
				if err != nil {
					t.Fatal(err)
				}
				assertLocalOptionsResponse(t, wire[:n], 200, toTag, policy, f.remote.LocalAddr())
				if attempt == 1 && previous != string(wire[:n]) {
					t.Fatal("UDP retransmission changed its stateless capability response")
				}
				previous = string(wire[:n])
			}
			waitUDPOrder(t, func() bool { count, lanes := f.pending(); return count == 0 && lanes == 0 })
			f.ingress.mu.Lock()
			flows, attempts := len(f.ingress.flows), len(f.ingress.stateAttempts)
			f.ingress.mu.Unlock()
			if probes.Load() != 0 || flows != 1 || attempts != 0 || f.ingress.drainHintCount.Load() != 0 {
				t.Fatal("OPTIONS touched native state or retained per-request history", probes.Load(), flows, attempts)
			}
			select {
			case frame := <-f.received:
				t.Fatalf("OPTIONS sent control traffic: %v", frame.Payload)
			default:
			}
		})
	}
}

func TestLocalOptionsDoesNotInterceptResponseCompletion(t *testing.T) {
	i := NewIngress(nil, nil, "", 1)
	completed := false
	i.stateCompletion = func(context.Context, *pb.FlowRef, *pb.SipMessage) (bool, error) { completed = true; return true, nil }
	request, err := parseObservedMessage(localOptionsWire("udp", "", "", ""))
	if err != nil {
		t.Fatal(err)
	}
	response := sip.NewResponseFromRequest(request.(*sip.Request), 200, "OK", nil)
	message := projectIngressMessage([]byte(response.String()), response)
	i.routeParsed(context.Background(), &physicalFlow{ref: &pb.FlowRef{}}, message, response)
	if !completed {
		t.Fatal("response to an outbound OPTIONS skipped native completion")
	}
}

func TestIngressOptionsProjectionSkipsUnusedDTOWithoutChangingOtherMessages(t *testing.T) {
	wire := localOptionsWire("tls", "", "Authorization: Digest username=\"phone\", realm=\"example.test\", nonce=\"nonce\", uri=\"sip:service@example.test\", response=\"00000000000000000000000000000000\"\r\n", "")
	parsed, err := parseObservedMessage(wire)
	if err != nil {
		t.Fatal(err)
	}
	local := projectIngressMessage(wire, parsed)
	if local.Method != "OPTIONS" || !proto.Equal(local, &pb.SipMessage{Wire: wire, Method: "OPTIONS"}) {
		t.Fatal("local OPTIONS built unused header/address/digest/selectors DTO")
	}
	full := projectObservedMessage(wire, parsed, false)
	if full.Authorization == nil || len(full.Headers) == 0 || len(full.Addresses) == 0 || len(full.Selectors) == 0 {
		t.Fatal("fixture does not exercise the skipped rich projection")
	}
	business := []byte(strings.ReplaceAll(string(wire), "OPTIONS", "INFO"))
	request, err := parseObservedMessage(business)
	if err != nil {
		t.Fatal(err)
	}
	response := sip.NewResponseFromRequest(parsed.(*sip.Request), 200, "OK", nil)
	for _, input := range []struct {
		wire   []byte
		parsed sip.Message
	}{{business, request}, {[]byte(response.String()), response}} {
		if !proto.Equal(projectIngressMessage(input.wire, input.parsed), projectObservedMessage(input.wire, input.parsed, false)) {
			t.Fatal("ingress changed the full business/native response DTO")
		}
	}
}

type localOptionsStreamCounter struct {
	net.Conn
	attempts atomic.Int64
}

func (c *localOptionsStreamCounter) Write(wire []byte) (int, error) {
	c.attempts.Add(1)
	return c.Conn.Write(wire)
}

type localOptionsPacketCounter struct {
	net.PacketConn
	attempts atomic.Int64
}

func (c *localOptionsPacketCounter) WriteTo(wire []byte, remote net.Addr) (int, error) {
	c.attempts.Add(1)
	return c.PacketConn.WriteTo(wire, remote)
}

func TestLocalOptionsAlreadyParsedRejectsMalformedIdentityWithoutStateOrWrites(t *testing.T) {
	for _, transport := range []string{"udp", "tcp", "tls"} {
		for _, malformed := range []string{"cseq-method", "empty-call-id"} {
			t.Run(transport+"/"+malformed, func(t *testing.T) {
				f := newUDPOrderFixture(t)
				probes := forbidOptionsState(f.ingress)
				parsed, err := parseObservedMessage(localOptionsWire(transport, "", "", ""))
				if err != nil {
					t.Fatal(err)
				}
				request := parsed.(*sip.Request)
				if malformed == "cseq-method" {
					request.CSeq().MethodName = sip.INVITE
				} else {
					*request.CallID() = ""
				}
				// The existing observation validator accepts these parsed fields.
				// Exercise that same non-reparsed boundary, not the nil-parse fallback.
				message, err := observeFramedMessage([]byte(request.String()), request)
				if err != nil {
					t.Fatal(err)
				}
				if transport == "udp" {
					waitUDPOrder(t, func() bool { f.ingress.mu.Lock(); defer f.ingress.mu.Unlock(); return f.ingress.udp != nil })
					flow, err := f.ingress.packetFlow(f.remote.LocalAddr())
					if err != nil {
						t.Fatal(err)
					}
					counter := &localOptionsPacketCounter{PacketConn: flow.packet}
					flow.packet = counter
					if !f.ingress.routeUDPParsed(f.ctx, flow, message, request, "malformed-options", drainMessageObservation(request)) {
						t.Fatal("parsed UDP route was not scheduled")
					}
					waitUDPOrder(t, func() bool { count, lanes := f.pending(); return count == 0 && lanes == 0 })
					if counter.attempts.Load() != 0 {
						t.Fatal("malformed parsed UDP OPTIONS attempted a response")
					}
				} else {
					local, remote := outboundStreamPair(t, transport)
					counter := &localOptionsStreamCounter{Conn: local}
					flow, err := f.ingress.addStream(counter, "127.0.0.1", 5060, transport)
					if err != nil {
						t.Fatal(err)
					}
					defer remote.Close()
					f.ingress.routeParsed(f.ctx, flow, message, request, drainMessageObservation(request))
					if counter.attempts.Load() != 0 {
						t.Fatal("malformed parsed stream OPTIONS attempted a response")
					}
				}
				if f.ingress.Statistics().InvalidMessages != 1 || probes.Load() != 0 {
					t.Fatal("malformed OPTIONS was not rejected locally before native state")
				}
				select {
				case frame := <-f.received:
					t.Fatalf("malformed OPTIONS sent control traffic: %v", frame.Payload)
				default:
				}
			})
		}
	}
}

func TestLocalOptionsNearLimitRequireCannotWriteOversizedResponse(t *testing.T) {
	f := newUDPOrderFixture(t)
	probes := forbidOptionsState(f.ingress)
	local, remote := outboundStreamPair(t, "tcp")
	counter := &localOptionsStreamCounter{Conn: local}
	flow, err := f.ingress.addStream(counter, "127.0.0.1", 5060, "tcp")
	if err != nil {
		t.Fatal(err)
	}
	base := localOptionsWire("tcp", "", "Require: \r\n", "")
	request := localOptionsWire("tcp", "", "Require: "+strings.Repeat("x", 65535-len(base))+"\r\n", "")
	if len(request) != 65535 {
		t.Fatal("fixture is not a maximum-size input")
	}
	if _, err := parseObservedMessage(request); err != nil {
		t.Fatal("near-limit input is not valid SIP", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); f.ingress.readStream(ctx, flow) }()
	t.Cleanup(func() { cancel(); local.Close(); <-done })
	if _, err := remote.Write(request); err != nil {
		t.Fatal(err)
	}
	// A following small probe proves the oversized input finished processing.
	// It is the only response allowed to reach the socket.
	if _, err := remote.Write(localOptionsWire("tcp", "", "", "")); err != nil {
		t.Fatal(err)
	}
	phone := &processPhone{connection: remote, reader: bufio.NewReader(remote)}
	phone.receive(t, sip.OPTIONS, 200)
	if len(phone.received) != 1 || counter.attempts.Load() != 1 || probes.Load() != 0 {
		t.Fatal("oversized local OPTIONS response reached the socket or native state")
	}
	f.ingress.mu.Lock()
	attempts := len(f.ingress.stateAttempts)
	f.ingress.mu.Unlock()
	if attempts != 0 {
		t.Fatal("oversized local response created retained physical evidence")
	}
}

func TestLocalOptionsUDPReadinessRequireAndContent(t *testing.T) {
	for _, test := range []struct {
		name, headers, body, unsupported string
		status                           int
		unready                          bool
	}{
		{name: "ready", status: 200},
		{name: "no-controller", status: 503, unready: true},
		{name: "require", headers: "Require: 100rel, timer\r\nRequire: outbound\r\n", status: 420, unsupported: "100rel, timer, outbound"},
		{name: "required-content", headers: "Content-Type: application/sdp\r\nContent-Disposition: session;handling=required\r\n", body: "v=0\r\n", status: 415},
		{name: "default-required", headers: "Content-Type: application/unknown\r\n", body: "opaque", status: 415},
		{name: "optional-content", headers: "Content-Type: application/unknown\r\nContent-Disposition: render;handling=optional\r\n", body: "opaque", status: 200},
		{name: "ambiguous-disposition", headers: "Content-Disposition: render;handling=optional\r\nContent-Disposition: session;handling=required\r\n", body: "opaque", status: 415},
	} {
		t.Run(test.name, func(t *testing.T) {
			var probes *atomic.Int64
			f := newUDPOrderFixture(t, func(i *Ingress) {
				probes = forbidOptionsState(i)
				if test.unready {
					i.control = new(control.Server)
				}
			})
			if _, err := f.remote.WriteTo(localOptionsWire("udp", "", test.headers, test.body), f.listener.LocalAddr()); err != nil {
				t.Fatal(err)
			}
			_ = f.remote.SetReadDeadline(time.Now().Add(time.Second))
			wire := make([]byte, 4096)
			n, _, err := f.remote.ReadFrom(wire)
			if err != nil {
				t.Fatal(err)
			}
			response := assertLocalOptionsResponse(t, wire[:n], test.status, "", MethodPolicy{}, f.remote.LocalAddr())
			if header(response, "Unsupported") != test.unsupported || probes.Load() != 0 {
				t.Fatal("unsupported requirement was lost or native state was consulted")
			}
		})
	}
}

func TestLocalOptionsTCPAndTLSOnObservedFlow(t *testing.T) {
	for _, transport := range []string{"tcp", "tls"} {
		for _, ready := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/ready=%v", transport, ready), func(t *testing.T) {
				f := newUDPOrderFixture(t)
				local, remote := outboundStreamPair(t, transport)
				ingress := NewIngress(f.ingress.process, f.ingress.control, "sip.example.test:5061", 8)
				if !ready {
					ingress.control = new(control.Server)
				}
				probes := forbidOptionsState(ingress)
				host, port, _ := net.SplitHostPort(remote.LocalAddr().String())
				number, _ := strconv.Atoi(port)
				flow, err := ingress.addStream(local, host, number, transport)
				if err != nil {
					t.Fatal(err)
				}
				ctx, cancel := context.WithCancel(context.Background())
				done := make(chan struct{})
				go func() { defer close(done); ingress.readStream(ctx, flow) }()
				t.Cleanup(func() { cancel(); local.Close(); <-done })
				phone := &processPhone{connection: remote, reader: bufio.NewReader(remote)}
				status := 503
				if ready {
					status = 200
				}
				for _, toTag := range []string{"", ";tag=retained"} {
					if _, err := remote.Write(localOptionsWire(transport, toTag, "", "")); err != nil {
						t.Fatal(err)
					}
					response := phone.receive(t, sip.OPTIONS, status)
					assertLocalOptionsResponse(t, []byte(response.String()), status, toTag, MethodPolicy{}, remote.LocalAddr())
				}
				waitUDPOrder(t, func() bool {
					flow.write.Lock()
					defer flow.write.Unlock()
					return flow.drainRoutes == 0
				})
				flow.write.Lock()
				hints := len(flow.drainHints)
				flow.write.Unlock()
				if probes.Load() != 0 || hints != 0 {
					t.Fatal("stream OPTIONS retained protocol or shutdown work", probes.Load(), hints)
				}
			})
		}
	}
}

func TestLocalOptionsDuringDrainStaysStatelessAndCannotExtendDeadline(t *testing.T) {
	f := newUDPOrderFixture(t)
	i, flow, remote := drainFixture(t)
	i.control = f.ingress.control
	probes := forbidOptionsState(i)
	pending := drainMessageObservation(drainRequest(t, "INVITE", 1, ""))
	if !i.beginDrainRoute(flow, pending) {
		t.Fatal("pending transaction refused")
	}
	i.finishDrainRoute(flow, pending, true)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	readerDone := make(chan struct{})
	go func() { defer close(readerDone); i.readStream(ctx, flow) }()
	t.Cleanup(func() { flow.conn.Close(); <-readerDone })
	started := time.Now()
	done := startDrain(t, i, 120*time.Millisecond)
	phone := &processPhone{connection: remote, reader: bufio.NewReader(remote)}
	for _, toTag := range []string{"", ";tag=retained", ""} {
		if _, err := remote.Write(localOptionsWire("tls", toTag, "", "")); err != nil {
			t.Fatal(err)
		}
		phone.receive(t, sip.OPTIONS, 503)
	}
	flow.write.Lock()
	hints := len(flow.drainHints)
	flow.write.Unlock()
	requireDrainDone(t, done)
	i.remove(flow) // connection() normally releases its closed flow after readStream.
	if time.Since(started) > 500*time.Millisecond || !flow.closed.Load() || probes.Load() != 0 || hints != 1 || i.drainHintCount.Load() != 0 {
		t.Fatal("local probes extended drain or entered native arbitration")
	}
}
