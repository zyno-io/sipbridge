package engine

import (
	"context"
	"fmt"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/emiago/sipgo/sip/portable"
	"google.golang.org/protobuf/encoding/protojson"
	"sipbridge/internal/control"
	pb "sipbridge/internal/controlpb"
)

type countedUDPListener struct {
	net.PacketConn
	reads atomic.Uint64
}

func (c *countedUDPListener) ReadFrom(wire []byte) (int, net.Addr, error) {
	n, remote, err := c.PacketConn.ReadFrom(wire)
	if err == nil {
		c.reads.Add(1)
	}
	return n, remote, err
}

type udpOrderFixture struct {
	ingress    *Ingress
	ctx        context.Context
	cancel     context.CancelFunc
	controller *websocket.Conn
	session    string
	listener   *countedUDPListener
	remote     net.PacketConn
	received   chan *pb.Envelope
	errors     chan error
}

func newUDPOrderFixture(t *testing.T, options ...func(*Ingress)) *udpOrderFixture {
	t.Helper()
	process := &pb.ProcessRef{InstanceId: "sip-a", Epoch: "epoch"}
	application := &pb.ProcessRef{InstanceId: "controller-a", Epoch: "epoch"}
	server, controller, controlCtx, session := controlFixture(t, process, application, func(*control.Peer, *pb.SessionStatus) error { return nil })
	writeControllerEnvelope(t, controlCtx, controller, &pb.Envelope{SessionId: session, CorrelationId: "ready", Payload: &pb.Envelope_Status{Status: &pb.SessionStatus{RetainedReady: true, BindingsReady: true}}})
	writeControllerEnvelope(t, controlCtx, controller, &pb.Envelope{SessionId: session, CorrelationId: "ready-barrier", Payload: &pb.Envelope_Ping{Ping: &pb.Heartbeat{Nonce: "ready-barrier"}}})
	if readControllerEnvelope(t, controlCtx, controller).GetPong() == nil {
		t.Fatal("controller readiness barrier failed")
	}
	listener, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	remote, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		listener.Close()
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(controlCtx)
	f := &udpOrderFixture{ingress: NewIngress(process, server, listener.LocalAddr().String(), 8), ctx: ctx, cancel: cancel,
		controller: controller, session: session, listener: &countedUDPListener{PacketConn: listener}, remote: remote,
		received: make(chan *pb.Envelope, 128), errors: make(chan error, 2)}
	t.Cleanup(func() { cancel(); listener.Close(); remote.Close() })
	for _, option := range options {
		option(f.ingress)
	}
	go func() { f.errors <- f.ingress.ServeUDP(ctx, f.listener) }()
	go func() {
		for {
			kind, wire, err := controller.Read(controlCtx)
			if err != nil {
				return
			}
			if kind != websocket.MessageText {
				continue
			}
			envelope := new(pb.Envelope)
			if err := protojson.Unmarshal(wire, envelope); err != nil {
				f.errors <- err
				return
			}
			if ping := envelope.GetPing(); ping != nil {
				pong, err := protojson.Marshal(&pb.Envelope{SessionId: session, CorrelationId: envelope.CorrelationId,
					Payload: &pb.Envelope_Pong{Pong: ping}})
				if err == nil {
					err = controller.Write(controlCtx, websocket.MessageText, pong)
				}
				if err != nil {
					f.errors <- err
					return
				}
				continue
			}
			select {
			case f.received <- envelope:
			case <-controlCtx.Done():
				return
			}
		}
	}()
	return f
}

func udpOrderMessage(t *testing.T, method, callID string, sequence int) *pb.SipMessage {
	t.Helper()
	wire := fmt.Sprintf("%s sip:service@example.test SIP/2.0\r\nVia: SIP/2.0/UDP 127.0.0.1:5060;branch=z9hG4bK-%s-%d;rport\r\nFrom: <sip:phone@example.test>;tag=phone\r\nTo: <sip:service@example.test>;tag=service\r\nCall-ID: %s\r\nCSeq: %d %s\r\nMax-Forwards: 70\r\nContent-Length: 0\r\n\r\n", method, callID, sequence, callID, sequence, method)
	message, err := Observe([]byte(wire), false)
	if err != nil {
		t.Fatal(err)
	}
	return message
}

func (f *udpOrderFixture) send(t *testing.T, method, callID string, sequence int) {
	t.Helper()
	if _, err := f.remote.WriteTo(udpOrderMessage(t, method, callID, sequence).Wire, f.listener.LocalAddr()); err != nil {
		t.Fatal(err)
	}
}

func (f *udpOrderFixture) next(t *testing.T) *pb.Envelope {
	t.Helper()
	select {
	case envelope := <-f.received:
		if envelope.GetIngress() == nil {
			t.Fatalf("unexpected control envelope: %T", envelope.Payload)
		}
		return envelope
	case err := <-f.errors:
		t.Fatalf("UDP routing fixture stopped: %v", err)
	case <-f.ctx.Done():
		t.Fatal("UDP routing fixture timed out")
	}
	return nil
}

func (f *udpOrderFixture) accept(t *testing.T, envelope *pb.Envelope) {
	t.Helper()
	writeControllerEnvelope(t, f.ctx, f.controller, &pb.Envelope{SessionId: f.session, CorrelationId: envelope.CorrelationId,
		Payload: &pb.Envelope_RouteResult{RouteResult: &pb.RouteResult{Outcome: pb.RouteResult_ACCEPTED}}})
}

func waitUDPOrder(t *testing.T, condition func() bool) {
	t.Helper()
	until := time.NewTimer(time.Second)
	defer until.Stop()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for !condition() {
		select {
		case <-ticker.C:
		case <-until.C:
			t.Fatal("UDP routing condition timed out")
		}
	}
}

func (f *udpOrderFixture) pending() (int, int) {
	f.ingress.mu.Lock()
	defer f.ingress.mu.Unlock()
	return len(f.ingress.messages), len(f.ingress.udpRoutes)
}

func TestUDPObservedACKOrderWaitsForRouteCommitAndOtherCallIDsProceed(t *testing.T) {
	f := newUDPOrderFixture(t)
	f.send(t, "ACK", "Dialog-Case", 2)
	ack := f.next(t)
	f.send(t, "INVITE", "Dialog-Case", 3)
	// Call-ID comparison is case-sensitive, even on the same observed flow.
	f.send(t, "INFO", "dialog-case", 1)
	other := f.next(t)
	if ack.GetIngress().Message.Method != "ACK" || other.GetIngress().Message.Method != "INFO" ||
		ack.GetIngress().Flow.ConnectionId != other.GetIngress().Flow.ConnectionId {
		t.Fatal("a later INVITE overtook ACK or unrelated same-flow Call-ID stalled")
	}
	f.accept(t, other)
	secondFlow, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer secondFlow.Close()
	if _, err := secondFlow.WriteTo(udpOrderMessage(t, "INFO", "Dialog-Case", 99).Wire, f.listener.LocalAddr()); err != nil {
		t.Fatal(err)
	}
	independent := f.next(t)
	if independent.GetIngress().Flow.ConnectionId == ack.GetIngress().Flow.ConnectionId {
		t.Fatal("same Call-ID on another physical UDP flow shared the blocked lane")
	}
	f.accept(t, independent)
	waitUDPOrder(t, func() bool { count, lanes := f.pending(); return count == 2 && lanes == 1 })
	select {
	case envelope := <-f.received:
		t.Fatalf("queued INVITE escaped before the ACK's native-commit route reply: %s", envelope)
	case <-time.After(30 * time.Millisecond):
	}
	// Controller sends this reply only after its exact native ingest has committed.
	f.accept(t, ack)
	invite := f.next(t)
	if invite.GetIngress().Message.Method != "INVITE" || invite.RemainingBudgetMs >= ack.RemainingBudgetMs-20 {
		t.Fatal("queued INVITE lost observed order or received a fresh route budget")
	}
	f.accept(t, invite)
	waitUDPOrder(t, func() bool { count, lanes := f.pending(); return count == 0 && lanes == 0 })
}

func TestUDPObservedACKOrderReleasesFollowingInviteAfterRedisCommit(t *testing.T) {
	a, b := historicalStateTestEngine(t)
	f := newUDPOrderFixture(t)
	f.send(t, "INFO", "seed-flow", 1)
	seed := f.next(t)
	flow := seed.GetIngress().Flow
	f.accept(t, seed)
	app := &pb.ProcessRef{InstanceId: "controller", Epoch: "one"}
	message := func(method string, sequence int, toTag, branch string) *pb.SipMessage {
		original := stateTestRequest(method, sequence, toTag, branch)
		wire := strings.ReplaceAll(string(original.Wire), "SIP/2.0/TLS", "SIP/2.0/UDP")
		wire = strings.ReplaceAll(wire, "transport=tls", "transport=udp")
		observed, err := Observe([]byte(wire), false)
		if err != nil {
			t.Fatal(err)
		}
		return observed
	}
	initial := message("INVITE", 1, "", "initial")
	ref, err := a.Locate(f.ctx, initial)
	if err != nil {
		t.Fatal(err)
	}
	ingest := func(worker *StateEngine, id string, observed *pb.SipMessage) *pb.OperationResult {
		return stateTestApply(t, worker, ref, id, stateEngineInput{Kind: "ingest", Application: app,
			Ingest: &pb.OwnerIngest{Context: ref, Application: app, Flow: flow, Wire: observed.Wire, Authenticated: true,
				PrincipalId: "phone", BindingId: "binding", Admission: pb.OwnerIngest_EXISTING_FLOW}})
	}
	if result := ingest(a, "initial-invite", initial); result.Error != "" {
		t.Fatal(result.Error)
	}
	_, group := stateTestLoad(t, a, ref)
	c := group.Contexts[ref.Id]
	var requestID string
	for _, tx := range c.Transactions {
		requestID = tx.RequestID
	}
	result := stateTestApply(t, a, ref, "initial-final", stateEngineInput{Kind: "command", Application: app,
		Command: &pb.Command{Context: ref, Sequence: 1, Action: &pb.Command_Respond{Respond: &pb.Respond{RequestId: requestID, Status: 200}}}})
	if result.Error != "" {
		t.Fatal(result.Error)
	}
	ack := message("ACK", 1, c.Tag, "ack")
	invite := message("INVITE", 2, c.Tag, "next-invite")
	if _, err := f.remote.WriteTo(ack.Wire, f.listener.LocalAddr()); err != nil {
		t.Fatal(err)
	}
	ackRoute := f.next(t)
	if _, err := f.remote.WriteTo(invite.Wire, f.listener.LocalAddr()); err != nil {
		t.Fatal(err)
	}
	waitUDPOrder(t, func() bool { count, _ := f.pending(); return count == 2 })
	// The fake Controller retains the real route RPC until another Redis executor
	// commits this ACK. The native portable dialog is still awaiting it here.
	_, group = stateTestLoad(t, b, ref)
	c = group.Contexts[ref.Id]
	if c.Dialogs[c.Dialog].RemoteInvite.State != portable.InviteAwaitingACK {
		t.Fatal("initial native ACK obligation missing")
	}
	if result := ingest(b, "committed-ack", ack); result.Error != "" {
		t.Fatal(result.Error)
	}
	_, group = stateTestLoad(t, a, ref)
	c = group.Contexts[ref.Id]
	if c.Dialogs[c.Dialog].RemoteInvite.State != portable.InviteAcked {
		t.Fatal("ACK route released without authoritative native commit")
	}
	f.accept(t, ackRoute)
	inviteRoute := f.next(t)
	if result := ingest(a, "following-invite", inviteRoute.GetIngress().Message); result.Error != "" || result.SipStatus == 491 {
		t.Fatalf("ordered UDP INVITE overtook committed ACK: %s", result)
	}
	f.accept(t, inviteRoute)
	waitUDPOrder(t, func() bool { count, lanes := f.pending(); return count == 0 && lanes == 0 })
}

func TestUDPCancelledMiddleEntryPreservesPredecessorAndReleasesCapacity(t *testing.T) {
	f := newUDPOrderFixture(t)
	f.send(t, "ACK", "cancel-chain", 2)
	ack := f.next(t)
	f.ingress.mu.Lock()
	flow := f.ingress.flows[ack.GetIngress().Flow.ConnectionId]
	f.ingress.mu.Unlock()
	ctx, cancel := context.WithCancel(f.ctx)
	if !f.ingress.routeUDP(ctx, flow, udpOrderMessage(t, "INVITE", "cancel-chain", 3), "cancel-chain") {
		t.Fatal("middle entry was not admitted")
	}
	if !f.ingress.routeUDP(f.ctx, flow, udpOrderMessage(t, "INVITE", "cancel-chain", 4), "cancel-chain") {
		t.Fatal("later entry was not admitted")
	}
	cancel()
	select {
	case envelope := <-f.received:
		t.Fatalf("cancelling a middle entry bypassed the ACK predecessor: %s", envelope)
	case <-time.After(30 * time.Millisecond):
	}
	f.accept(t, ack)
	last := f.next(t)
	if string(last.GetIngress().Message.Wire) != string(udpOrderMessage(t, "INVITE", "cancel-chain", 4).Wire) {
		t.Fatal("cancelled entry started fresh routing")
	}
	f.accept(t, last)
	waitUDPOrder(t, func() bool { count, lanes := f.pending(); return count == 0 && lanes == 0 })
}

func TestUDPOverloadDropsWithoutBlockingReaderOrAllocatingLanes(t *testing.T) {
	f := newUDPOrderFixture(t)
	f.send(t, "ACK", "full-lane", 1)
	_ = f.next(t)
	for sequence := 2; sequence <= 128; sequence++ {
		f.send(t, "INVITE", "full-lane", sequence)
	}
	waitUDPOrder(t, func() bool { count, lanes := f.pending(); return count == 128 && lanes == 1 })
	for sequence := 129; sequence <= 160; sequence++ {
		f.send(t, "INVITE", fmt.Sprintf("dropped-%d", sequence), sequence)
	}
	waitUDPOrder(t, func() bool { return f.listener.reads.Load() == 160 })
	if count, lanes := f.pending(); count != 128 || lanes != 1 {
		t.Fatal("overload allocated queue/map capacity")
	}
	f.cancel()
	waitUDPOrder(t, func() bool { count, lanes := f.pending(); return count == 0 && lanes == 0 })
	select {
	case envelope := <-f.received:
		t.Fatalf("cancelled queued datagram started fresh route/rejection work: %s", envelope)
	default:
	}
}

func TestUDPQueuedParentExpiryPreservesOrderingWithoutFreshRoute(t *testing.T) {
	f := newUDPOrderFixture(t)
	f.send(t, "INFO", "seed-flow", 1)
	seed := f.next(t)
	f.accept(t, seed)
	waitUDPOrder(t, func() bool { count, lanes := f.pending(); return count == 0 && lanes == 0 })
	f.ingress.mu.Lock()
	flow := f.ingress.flows[seed.GetIngress().Flow.ConnectionId]
	f.ingress.mu.Unlock()
	if flow == nil {
		t.Fatal("seed route did not retain its UDP flow")
	}
	entered, release := make(chan struct{}), make(chan struct{})
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	var locates atomic.Uint64
	f.ingress.stateLocate = func(ctx context.Context, message *pb.SipMessage) (*pb.ContextSnapshot, error) {
		locates.Add(1)
		if message.Method == "INFO" && strings.Contains(string(message.Wire), "natural-expiry") {
			select {
			case <-entered:
			default:
				close(entered)
			}
			<-release
			return nil, ctx.Err()
		}
		return nil, nil
	}
	parent, cancel := context.WithTimeout(f.ctx, 100*time.Millisecond)
	defer cancel()
	if !f.ingress.routeUDP(parent, flow, udpOrderMessage(t, "INFO", "natural-expiry", 1), "natural-expiry") {
		t.Fatal("predecessor was not admitted")
	}
	select {
	case <-entered:
	case <-f.ctx.Done():
		t.Fatal("predecessor did not enter its original route budget")
	}
	if !f.ingress.routeUDP(parent, flow, udpOrderMessage(t, "INVITE", "natural-expiry", 2), "natural-expiry") {
		t.Fatal("queued item was not admitted")
	}
	if !f.ingress.routeUDP(f.ctx, flow, udpOrderMessage(t, "BYE", "natural-expiry", 3), "natural-expiry") {
		t.Fatal("following item was not admitted")
	}
	waitUDPOrder(t, func() bool { count, lanes := f.pending(); return count == 3 && lanes == 1 })
	select {
	case <-parent.Done():
	case <-time.After(time.Second):
		t.Fatal("original parent route deadline did not expire")
	}
	if count, lanes := f.pending(); count != 3 || lanes != 1 {
		t.Fatal("expired middle work released its unfinished predecessor")
	}
	if locates.Load() != 1 {
		t.Fatal("queued datagram performed a fresh locate before its predecessor finished")
	}
	select {
	case envelope := <-f.received:
		t.Fatalf("queued datagram started a control route before predecessor release: %s", envelope)
	default:
	}
	close(release)
	third := f.next(t)
	if third.GetIngress().Message.Method != "BYE" || !strings.Contains(string(third.GetIngress().Message.Wire), "natural-expiry") {
		t.Fatal("work after the expired queue item did not preserve original route order")
	}
	f.accept(t, third)
	waitUDPOrder(t, func() bool { count, lanes := f.pending(); return count == 0 && lanes == 0 })
	if locates.Load() != 2 {
		t.Fatal("expired queued datagram received a fresh native lookup")
	}
	select {
	case envelope := <-f.received:
		t.Fatalf("expired queued datagram started a new control route: %s", envelope)
	default:
	}
	if err := f.remote.SetReadDeadline(time.Now().Add(20 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	buffer := make([]byte, 4096)
	if _, _, err := f.remote.ReadFrom(buffer); err == nil {
		t.Fatal("expired queued datagram received a fresh SIP rejection")
	} else if timeout, ok := err.(net.Error); !ok || !timeout.Timeout() {
		t.Fatalf("unexpected UDP read failure: %v", err)
	}
}

func TestUDPQueuedNaturalExpiryCannotStartFreshRouteOrRejection(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	defer close(release)
	var locates atomic.Uint64
	f := newUDPOrderFixture(t, func(ingress *Ingress) {
		ingress.stateLocate = func(ctx context.Context, _ *pb.SipMessage) (*pb.ContextSnapshot, error) {
			if locates.Add(1) == 1 {
				close(entered)
			}
			// Keep the predecessor's completion signal unavailable after its own
			// route budget expires, as can happen while admitted I/O unwinds.
			<-release
			return nil, ctx.Err()
		}
	})
	f.send(t, "ACK", "natural-expiry", 2)
	select {
	case <-entered:
	case <-f.ctx.Done():
		t.Fatal("ACK did not enter its original route budget")
	}
	f.send(t, "INVITE", "natural-expiry", 3)
	waitUDPOrder(t, func() bool { count, _ := f.pending(); return count == 2 })
	timer := time.NewTimer(ingressRouteTimeout + 10*time.Millisecond)
	defer timer.Stop()
	<-timer.C
	if count, lanes := f.pending(); count != 2 || lanes != 1 {
		t.Fatal("expired queued work released its unfinished predecessor")
	}
	release <- struct{}{}
	waitUDPOrder(t, func() bool { count, lanes := f.pending(); return count == 0 && lanes == 0 })
	if locates.Load() != 1 {
		t.Fatal("expired queued datagram received fresh native lookup/routing work")
	}
	select {
	case envelope := <-f.received:
		t.Fatalf("expired queued datagram started a new control route: %s", envelope)
	default:
	}
	if err := f.remote.SetReadDeadline(time.Now().Add(20 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	buffer := make([]byte, 4096)
	if _, _, err := f.remote.ReadFrom(buffer); err == nil {
		t.Fatal("expired queued datagram received a fresh SIP rejection")
	} else if timeout, ok := err.(net.Error); !ok || !timeout.Timeout() {
		t.Fatalf("unexpected UDP read failure: %v", err)
	}
}
