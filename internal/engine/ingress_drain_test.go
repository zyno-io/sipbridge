package engine

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/emiago/sipgo/sip"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/state"
)

func drainFixture(t *testing.T) (*Ingress, *physicalFlow, net.Conn) {
	t.Helper()
	i := NewIngress(&pb.ProcessRef{InstanceId: "drain", Epoch: "one"}, nil, "sip.test:5061", 16)
	local, remote := net.Pipe()
	t.Cleanup(func() { local.Close(); remote.Close() })
	f, err := i.addStream(local, "192.0.2.1", 5061, "tls")
	if err != nil {
		t.Fatal(err)
	}
	return i, f, remote
}

func drainRequest(t *testing.T, method string, seq int, toTag string) sip.Message {
	t.Helper()
	to := "<sip:b@test>"
	if toTag != "" {
		to += ";tag=" + toTag
	}
	wire := fmt.Sprintf("%s sip:b@test SIP/2.0\r\nVia: SIP/2.0/TLS phone.test;branch=z9hG4bK-drain-%d\r\nFrom: <sip:a@test>;tag=from\r\nTo: %s\r\nCall-ID: drain@test\r\nCSeq: %d %s\r\nContent-Length: 0\r\n\r\n", method, seq, to, seq, method)
	m, err := parseObservedMessage([]byte(wire))
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func drainResponse(t *testing.T, request sip.Message, status int, tag string) sip.Message {
	t.Helper()
	r := sip.NewResponseFromRequest(request.(*sip.Request), status, "fixture", nil)
	r.To().Params.Add("tag", tag)
	return r
}

func startDrain(t *testing.T, i *Ingress, timeout time.Duration) <-chan error {
	t.Helper()
	done := make(chan error, 1)
	go func() {
		done <- i.Drain(context.Background(), DrainOptions{WithdrawalGrace: 15 * time.Millisecond, Timeout: timeout})
	}()
	until := time.Now().Add(time.Second)
	for !i.draining.Load() && time.Now().Before(until) {
		time.Sleep(time.Millisecond)
	}
	if !i.draining.Load() {
		t.Fatal("drain did not begin")
	}
	return done
}

func requireDrainDone(t *testing.T, done <-chan error) {
	t.Helper()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("drain did not finish")
	}
}

func TestIngressDrainWaitsForFinalAndACKButNotReplayRetention(t *testing.T) {
	for _, method := range []string{"INVITE", "NOTIFY"} {
		t.Run(method, func(t *testing.T) {
			i, f, _ := drainFixture(t)
			req := drainRequest(t, method, 1, "")
			obs := drainMessageObservation(req)
			if !i.beginDrainRoute(f, obs) {
				t.Fatal("request refused")
			}
			i.finishDrainRoute(f, obs, true)
			done := startDrain(t, i, 500*time.Millisecond)
			time.Sleep(30 * time.Millisecond)
			if f.closed.Load() {
				t.Fatal("closed before response")
			}
			response := drainMessageObservation(drainResponse(t, req, 200, "accepted"))
			f.write.Lock()
			i.observeDrainLocked(f, response, true, time.Now())
			f.write.Unlock()
			i.wakeDrain()
			if method == "INVITE" {
				time.Sleep(20 * time.Millisecond)
				if f.closed.Load() {
					t.Fatal("closed before success ACK")
				}
				ack := drainMessageObservation(drainRequest(t, "ACK", 1, "accepted"))
				if !i.beginDrainRoute(f, ack) {
					t.Fatal("ACK refused during drain")
				}
				i.finishDrainRoute(f, ack, true)
			}
			requireDrainDone(t, done)
			if !f.closed.Load() || i.drainHintCount.Load() != 0 {
				t.Fatal("settled flow retained a drain obligation")
			}
		})
	}
}

func TestIngressDrainUACImmediateACKAndForks(t *testing.T) {
	i, f, _ := drainFixture(t)
	req := drainRequest(t, "INVITE", 1, "")
	f.write.Lock()
	i.observeDrainLocked(f, drainMessageObservation(req), true, time.Now())
	f.write.Unlock()
	first := drainMessageObservation(drainResponse(t, req, 200, "first"))
	second := drainMessageObservation(drainResponse(t, req, 200, "second"))
	if !i.beginDrainRoute(f, first) || !i.beginDrainRoute(f, second) {
		t.Fatal("responses refused")
	}
	// First ACK is physically written inside response reduction, before route
	// returns. Its observation must not be recreated by that return.
	f.write.Lock()
	i.observeDrainLocked(f, drainMessageObservation(drainRequest(t, "ACK", 1, "first")), true, time.Now())
	f.write.Unlock()
	i.finishDrainRoute(f, first, true)
	i.finishDrainRoute(f, second, true)
	done := startDrain(t, i, 500*time.Millisecond)
	time.Sleep(25 * time.Millisecond)
	if f.closed.Load() {
		t.Fatal("other fork's ACK obligation lost")
	}
	f.write.Lock()
	i.observeDrainLocked(f, drainMessageObservation(drainRequest(t, "ACK", 1, "second")), true, time.Now())
	f.write.Unlock()
	i.wakeDrain()
	requireDrainDone(t, done)
	if i.drainHintCount.Load() != 0 {
		t.Fatal("pure accepted INVITE retention kept flow busy")
	}
}

func TestIngressDrainOutgoingNotifyNeedsAcceptedFinal(t *testing.T) {
	i, f, _ := drainFixture(t)
	req := drainRequest(t, "NOTIFY", 1, "subscription")
	f.write.Lock()
	i.observeDrainLocked(f, drainMessageObservation(req), true, time.Now())
	f.write.Unlock()
	done := startDrain(t, i, 500*time.Millisecond)
	response := drainMessageObservation(drainResponse(t, req, 200, "subscription"))
	if !i.beginDrainRoute(f, response) {
		t.Fatal("response refused")
	}
	i.finishDrainRoute(f, response, false)
	time.Sleep(25 * time.Millisecond)
	if f.closed.Load() {
		t.Fatal("unaccepted response settled transaction hint")
	}
	if !i.beginDrainRoute(f, response) {
		t.Fatal("accepted response refused")
	}
	i.finishDrainRoute(f, response, true)
	requireDrainDone(t, done)
}

func TestIngressDrainLocalRefusalSettlesRequestButKeepsInviteACK(t *testing.T) {
	for _, method := range []string{"REGISTER", "OPTIONS", "INVITE"} {
		t.Run(method, func(t *testing.T) {
			i, f, peer := drainFixture(t)
			parsed := drainRequest(t, method, 1, "")
			observation := drainMessageObservation(parsed)
			if !i.beginDrainRoute(f, observation) {
				t.Fatal("initial request refused")
			}
			message := projectObservedMessage([]byte(parsed.String()), parsed, false)
			wire := make(chan []byte, 1)
			go func() {
				reader := bufio.NewReader(peer)
				var text strings.Builder
				for {
					line, err := reader.ReadString('\n')
					if err != nil {
						wire <- nil
						return
					}
					text.WriteString(line)
					if line == "\r\n" {
						wire <- []byte(text.String())
						return
					}
				}
			}()
			i.localResponseWithin(context.Background(), f, message, 401, "drain-", nil)
			i.finishDrainRoute(f, observation, false)
			response, err := parseObservedMessage(<-wire)
			if err != nil {
				t.Fatal(err)
			}
			done := startDrain(t, i, 400*time.Millisecond)
			if method == "INVITE" {
				time.Sleep(25 * time.Millisecond)
				if f.closed.Load() {
					t.Fatal("local INVITE final dropped ACK obligation")
				}
				ack := drainMessageObservation(drainRequest(t, "ACK", 1, tag(response.To().Params)))
				if !i.beginDrainRoute(f, ack) {
					t.Fatal("local final ACK refused")
				}
				i.finishDrainRoute(f, ack, true)
			}
			requireDrainDone(t, done)
			if len(f.drainHints) != 0 {
				t.Fatal("local refusal retained completed request")
			}
		})
	}
}

func TestIngressDrainHardDeadlineDoesNotWaitForFlowLock(t *testing.T) {
	i, f, peer := drainFixture(t)
	f.write.Lock()
	done := startDrain(t, i, 50*time.Millisecond)
	requireDrainDone(t, done)
	f.write.Unlock()
	peer.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := peer.Read(make([]byte, 1)); err == nil {
		t.Fatal("hard deadline did not close transport")
	}
}

type drainWriteConn struct {
	net.Conn
	started chan struct{}
	once    sync.Once
}

func (c *drainWriteConn) Write(wire []byte) (int, error) {
	c.once.Do(func() { close(c.started) })
	return c.Conn.Write(wire)
}

func TestIngressDrainStartedWriteStaysUnknownAndUnstartedStaysNotWritten(t *testing.T) {
	for _, blocked := range []string{"write", "reserve"} {
		t.Run(blocked, func(t *testing.T) {
			i, f, _ := drainFixture(t)
			request := drainRequest(t, "NOTIFY", 1, "subscription")
			observation := drainMessageObservation(request)
			writer := &evidenceWriter{reserved: &CommittedWrite{Wire: []byte(request.String()), Drain: &observation}}
			connection := &drainWriteConn{Conn: f.conn, started: make(chan struct{})}
			f.conn = connection
			var started <-chan struct{}
			if blocked == "reserve" {
				writer.started, writer.release = make(chan struct{}), make(chan struct{})
				started = writer.started
			} else {
				started = connection.started
			}
			if err := i.ConfigureStateRuntime(writer, func(context.Context, *pb.SipMessage) (*pb.ContextSnapshot, error) { return nil, nil }); err != nil {
				t.Fatal(err)
			}
			write := &pb.FlowWrite{Flow: f.ref, Effect: &pb.StateEffectRef{Id: "drain-write", AttemptId: "one"}, Permit: &pb.SendPermit{Authorization: &pb.EffectAuthorization{DeadlineMs: uint64(time.Now().Add(time.Second).UnixMilli())}}}
			result := make(chan *pb.DeliveryResult, 1)
			go func() { result <- i.WriteCommitted(context.Background(), write) }()
			select {
			case <-started:
			case <-time.After(time.Second):
				t.Fatal("write did not reach barrier")
			}
			done := startDrain(t, i, 50*time.Millisecond)
			if blocked == "reserve" {
				until := time.Now().Add(time.Second)
				for !f.closed.Load() && time.Now().Before(until) {
					time.Sleep(time.Millisecond)
				}
				close(writer.release)
				if !f.closed.Load() {
					t.Fatal("drain did not fence pending reservation")
				}
			}
			requireDrainDone(t, done)
			select {
			case delivery := <-result:
				want := pb.DeliveryResult_UNKNOWN
				if blocked == "reserve" {
					want = pb.DeliveryResult_NOT_WRITTEN
				}
				if delivery.Outcome != want {
					t.Fatalf("%s outcome %s, want %s", blocked, delivery.Outcome, want)
				}
			case <-time.After(time.Second):
				t.Fatal("completion reporting did not finish")
			}
			if blocked == "reserve" {
				select {
				case <-connection.started:
					t.Fatal("closed flow entered syscall")
				default:
				}
			}
		})
	}
}

func TestIngressDrainClosesListenerAndFencesNewAdmission(t *testing.T) {
	i, f, _ := drainFixture(t)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { listener.Close() })
	if !i.registerDrainListener(listener) {
		t.Fatal("listener refused before drain")
	}
	done := startDrain(t, i, 200*time.Millisecond)
	if conn, err := net.DialTimeout("tcp", listener.Addr().String(), 100*time.Millisecond); err == nil {
		conn.Close()
		t.Fatal("draining listener accepted connection")
	}
	request := drainMessageObservation(drainRequest(t, "INVITE", 4, ""))
	if i.beginDrainRoute(f, request) {
		t.Fatal("new session admitted during drain")
	}
	local, remote := net.Pipe()
	defer local.Close()
	defer remote.Close()
	if _, err := i.addStream(local, "192.0.2.2", 5061, "tls"); err == nil {
		t.Fatal("handshake race admitted new stream")
	}
	if _, err := i.OpenNetwork(context.Background(), "sip:b@test", nil); err == nil || !strings.Contains(err.Error(), "draining") {
		t.Fatal("new network open not fenced")
	}
	requireDrainDone(t, done)
}

func TestIngressDrainHintBoundsHoldUntilDeadline(t *testing.T) {
	i, f, _ := drainFixture(t)
	f.write.Lock()
	for n := 0; n < drainFlowHints+1; n++ {
		i.observeDrainLocked(f, drainMessageObservation(drainRequest(t, "NOTIFY", n+1, "subscription")), true, time.Now())
	}
	if len(f.drainHints) != drainFlowHints || !f.drainUnknown {
		t.Fatal("hint bound failed")
	}
	for key := range f.drainHints {
		i.removeDrainHintLocked(f, key)
	}
	f.write.Unlock()
	done := startDrain(t, i, 80*time.Millisecond)
	time.Sleep(30 * time.Millisecond)
	if f.closed.Load() {
		t.Fatal("overflow flow declared idle")
	}
	requireDrainDone(t, done)
}

func TestIngressDrainExpiredHintDoesNotProveCompletion(t *testing.T) {
	i, f, _ := drainFixture(t)
	f.write.Lock()
	i.observeDrainLocked(f, drainMessageObservation(drainRequest(t, "NOTIFY", 1, "subscription")), true, time.Now().Add(-drainHintLifetime-time.Second))
	f.write.Unlock()
	done := startDrain(t, i, 80*time.Millisecond)
	time.Sleep(30 * time.Millisecond)
	if f.closed.Load() {
		t.Fatal("hint expiry falsely proved transaction complete")
	}
	requireDrainDone(t, done)
	if i.drainHintCount.Load() != 0 {
		t.Fatal("expired advisory metadata was retained")
	}
}

func TestIngressDrainOptions(t *testing.T) {
	for _, o := range []DrainOptions{{-1, time.Second}, {61 * time.Second, 90 * time.Second}, {time.Second, time.Second}, {0, 0}, {0, 121 * time.Second}} {
		if o.Validate() == nil {
			t.Fatalf("accepted invalid bounds %+v", o)
		}
	}
	for _, o := range []DrainOptions{{0, time.Second}, {60 * time.Second, 120 * time.Second}, {5 * time.Second, 40 * time.Second}} {
		if err := o.Validate(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestIngressDrainNativeReplayPrecedesSessionRefusal(t *testing.T) {
	for _, outcome := range []string{"retained", "new", "uncertain"} {
		t.Run(outcome, func(t *testing.T) {
			i, f, _ := drainFixture(t)
			i.draining.Store(true)
			calls, refused := 0, 0
			i.stateCompletion = func(context.Context, *pb.FlowRef, *pb.SipMessage) (bool, error) {
				calls++
				if outcome == "uncertain" {
					return false, state.ErrUnknown
				}
				return outcome == "retained", nil
			}
			parsed := drainRequest(t, "INVITE", 1, "")
			message := projectObservedMessage([]byte(parsed.String()), parsed, false)
			i.routeWithin(context.Background(), f, message, func(int) { refused++ }, func() { t.Fatal("unexpected unavailable path") }, drainMessageObservation(parsed))
			want := 0
			if outcome == "new" {
				want = 1
			}
			if calls != 1 || refused != want {
				t.Fatalf("native probes=%d refusals=%d, want1/%d", calls, refused, want)
			}
			if f.drainRoutes != 0 {
				t.Fatal("completion-only route leaked drain admission")
			}
		})
	}
}

func TestNativeReservationDrainHintIsTransientAndExact(t *testing.T) {
	f, ref, write := publicByeFixture(t, state.MemoryBackend)
	reserved, err := f.engine.Reserve(context.Background(), write)
	if err != nil || reserved == nil || reserved.Drain == nil {
		t.Fatalf("no definite reservation hint: %+v %v", reserved, err)
	}
	parsed, err := parseObservedMessage(reserved.Wire)
	if err != nil || *reserved.Drain != drainMessageObservation(parsed) {
		t.Fatal("hint differs from committed bytes", err)
	}
	record, _ := stateTestLoad(t, f.engine, ref)
	if record.Payload.LocalDrain != nil {
		t.Fatal("transient hint persisted")
	}
	again, err := f.engine.Reserve(context.Background(), write)
	if err != nil || again == nil || !again.Known || again.Drain != nil {
		t.Fatalf("repeat reservation published hint: %+v %v", again, err)
	}
}
