package engine

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
	"google.golang.org/protobuf/proto"
	"sipbridge/internal/control"
	pb "sipbridge/internal/controlpb"
)

var peerACKOffer = []byte("v=0\r\no=offer 1 1 IN IP4 192.0.2.20\r\ns=offer\r\nt=0 0\r\n")
var peerACKAnswer = []byte("v=0\r\no=answer 2 2 IN IP4 192.0.2.21\r\ns=answer\r\nt=0 0\r\n")

type peerACKScenario struct {
	optIn, requestOffer, initialLate, retry, earlyACKBody bool
}

func peerACKScenarios() map[string]peerACKScenario {
	return map[string]peerACKScenario{
		"late-offer-retry":          {optIn: true, retry: true},
		"request-offer":             {optIn: true, requestOffer: true},
		"early-offer-ack-body":      {optIn: true, requestOffer: true, earlyACKBody: true},
		"legacy-controller":         {},
		"legacy-initial-late-offer": {initialLate: true},
		"initial-late-offer":        {optIn: true, initialLate: true},
	}
}

type peerACKRuntime interface {
	ReceiveRequest(context.Context, *control.Peer, *pb.FlowRef, *pb.SipMessage) error
	TryNativeCompletion(context.Context, *pb.FlowRef, *pb.SipMessage) (bool, error)
}

func TestResidentPeerReinviteACK(t *testing.T) {
	for name, scenario := range peerACKScenarios() {
		t.Run(name, func(t *testing.T) {
			e, peer, conn, ctx, session, writes, _ := residentControlFixture(t, true, scenario.optIn)
			flow := residentDirectFlow(e)
			incoming := stateTestRequest("INVITE", 1, "", "peer-ack")
			request := residentDirectRequest(t, e, peer, conn, ctx, flow, incoming)
			decision := residentDirectDecision(request, 1, 200)
			if scenario.initialLate {
				decision.Body = peerACKOffer
				decision.Headers = []*pb.Header{{Name: "Content-Type", Value: "application/sdp"}}
			}
			result := residentSendDecision(t, ctx, conn, session, "initial-answer", decision)
			if result.Certainty != pb.OperationResult_ACCEPTED {
				t.Fatal("initial response", result)
			}
			final := peerACKFinal(t, ctx, writes)
			remote, err := portable.NewDialog(portable.UAC, portable.RestoreMessage(incoming.Wire, "tls", "", ""), portable.RestoreMessage(final, "tls", "", ""), portable.DefaultTimers())
			if err != nil {
				t.Fatal(err)
			}
			testPeerACKExchange(t, e, peer, conn, ctx, session, flow, writes, remote, request.Snapshot.Context, 2, scenario)
		})
	}
}

func TestRedisNativePeerReinviteACK(t *testing.T) {
	for name, scenario := range peerACKScenarios() {
		t.Run(name, func(t *testing.T) {
			e, peer, conn, ctx, session, flow, writes := peerACKRedisFixture(t, scenario.optIn)
			incoming := stateTestRequest("INVITE", 1, "", "redis-peer-ack")
			if err := e.ReceiveRequest(ctx, peer, flow, incoming); err != nil {
				t.Fatal(err)
			}
			request := peerACKRead(t, ctx, conn, session).GetRequest()
			if request == nil {
				t.Fatal("initial request missing")
			}
			decision := residentDirectDecision(request, 1, 200)
			if scenario.initialLate {
				decision.Body = peerACKOffer
				decision.Headers = []*pb.Header{{Name: "Content-Type", Value: "application/sdp"}}
			}
			if result := residentSendDecision(t, ctx, conn, session, "initial-answer", decision); result.Certainty != pb.OperationResult_ACCEPTED {
				t.Fatal(result)
			}
			final := peerACKFinal(t, ctx, writes)
			remote, err := portable.NewDialog(portable.UAC, portable.RestoreMessage(incoming.Wire, "tls", "", ""), portable.RestoreMessage(final, "tls", "", ""), portable.DefaultTimers())
			if err != nil {
				t.Fatal(err)
			}
			testPeerACKExchange(t, e, peer, conn, ctx, session, flow, writes, remote, request.Snapshot.Context, 2, scenario)
		})
	}
}

func peerACKFinal(t *testing.T, ctx context.Context, writes <-chan []byte) []byte {
	t.Helper()
	for {
		select {
		case wire := <-writes:
			if bytes.HasPrefix(wire, []byte("SIP/2.0 200")) {
				return wire
			}
			if !bytes.HasPrefix(wire, []byte("SIP/2.0 100")) {
				t.Fatalf("unexpected wire: %s", wire)
			}
		case <-ctx.Done():
			t.Fatal("final response missing")
			return nil
		}
	}
}

func peerACKRead(t *testing.T, ctx context.Context, conn *websocket.Conn, session string) *pb.Envelope {
	t.Helper()
	for {
		frame := readControllerEnvelope(t, ctx, conn)
		if ping := frame.GetPing(); ping != nil {
			writeControllerEnvelope(t, ctx, conn, &pb.Envelope{SessionId: session, CorrelationId: frame.CorrelationId, Payload: &pb.Envelope_Pong{Pong: ping}})
			continue
		}
		return frame
	}
}

func testPeerACKExchange(t *testing.T, runtime peerACKRuntime, peer *control.Peer, conn *websocket.Conn, ctx context.Context, session string, flow *pb.FlowRef, writes <-chan []byte, remote portable.Dialog, ref *pb.ContextRef, sequence uint64, scenario peerACKScenario) {
	t.Helper()
	build := func(method sip.RequestMethod, cseq uint32, body []byte) portable.Message {
		t.Helper()
		headers := []portable.Header{{Name: "X-Peer-ACK", Value: "retained"}}
		if len(body) > 0 {
			headers = append(headers, portable.Header{Name: "Content-Type", Value: "application/sdp"})
		}
		message, err := portable.BuildDialogRequestWithHeaders(remote, method, cseq, body, fmt.Sprintf("z9hG4bK-peer-%s-%d", method, cseq), headers)
		if err != nil {
			t.Fatal(err)
		}
		return message
	}
	complete := func(message portable.Message) <-chan error {
		t.Helper()
		done := make(chan error, 1)
		go func() {
			handled, err := runtime.TryNativeCompletion(ctx, flow, &pb.SipMessage{Wire: message.WireBytes()})
			if !handled && err == nil {
				err = fmt.Errorf("ACK not handled")
			}
			done <- err
		}()
		return done
	}
	reply := func(frame *pb.Envelope, accepted bool) {
		result := &pb.OperationResult{Certainty: pb.OperationResult_COMPLETED}
		if !accepted {
			result.Certainty, result.Error = pb.OperationResult_UNKNOWN, "synthetic retry"
		}
		writeControllerEnvelope(t, ctx, conn, &pb.Envelope{SessionId: session, CorrelationId: frame.CorrelationId, Payload: &pb.Envelope_Result{Result: result}})
	}
	initialBody := []byte(nil)
	if scenario.initialLate {
		initialBody = peerACKAnswer
	}
	done := complete(build(sip.ACK, 1, initialBody))
	initial := peerACKRead(t, ctx, conn, session)
	first := initial.GetDialogEvent()
	if first == nil || first.Kind != pb.DialogEvent_ACKNOWLEDGED || first.Message != nil || first.InviteCseq != 0 || !first.Snapshot.InitialAck {
		t.Fatal("initial ACK semantics changed", initial)
	}
	if scenario.initialLate && !bytes.Equal(first.Snapshot.RemoteSdp, peerACKAnswer) {
		t.Fatal("initial ACK SDP missing")
	}
	reply(initial, true)
	if err := <-done; err != nil {
		t.Fatal(err)
	}

	for _, cseq := range []uint32{20, 21} {
		offer := []byte(nil)
		if scenario.requestOffer {
			offer = peerACKOffer
		}
		invite := build(sip.INVITE, cseq, offer)
		if err := runtime.ReceiveRequest(ctx, peer, flow, &pb.SipMessage{Wire: invite.WireBytes()}); err != nil {
			t.Fatal(err)
		}
		request := peerACKRead(t, ctx, conn, session).GetRequest()
		if request == nil {
			t.Fatal("re-INVITE not presented")
		}
		decision := residentDirectDecision(request, sequence, 200)
		decision.Body = peerACKOffer
		if scenario.requestOffer {
			decision.Body = peerACKAnswer
		}
		decision.Headers = []*pb.Header{{Name: "Content-Type", Value: "application/sdp"}}
		accepted := residentSendDecision(t, ctx, conn, session, fmt.Sprintf("answer-%d", cseq), decision)
		if accepted.Certainty != pb.OperationResult_ACCEPTED {
			t.Fatal("re-INVITE response", accepted)
		}
		sequence++
		peerACKFinal(t, ctx, writes)
		ackBody := []byte(nil)
		if !scenario.requestOffer {
			ackBody = peerACKAnswer
			if handled, err := runtime.TryNativeCompletion(ctx, flow, &pb.SipMessage{Wire: build(sip.ACK, cseq, nil).WireBytes()}); !handled || err == nil {
				t.Fatal("late-offer ACK without an answer was accepted", err)
			}
		}
		if scenario.earlyACKBody {
			ackBody = []byte("opaque early-offer ACK body")
		}
		ack := build(sip.ACK, cseq, ackBody)
		done = complete(ack)
		if scenario.optIn {
			frame := peerACKRead(t, ctx, conn, session)
			event := frame.GetDialogEvent()
			if event == nil || event.Kind != pb.DialogEvent_ACKNOWLEDGED || event.InviteCseq != cseq || event.OperationId != "" || event.Message == nil || !bytes.Equal(event.Message.Wire, ack.WireBytes()) || !bytes.Equal(event.Message.Body, ackBody) || !proto.Equal(event.Snapshot.Context, ref) || event.Snapshot.DialogId == "" || !event.Snapshot.InitialAck || event.Snapshot.ApplicationLeaseTerm != first.Snapshot.ApplicationLeaseTerm {
				t.Fatal("wrong re-INVITE ACK event", frame)
			}
			expectedSDP := peerACKAnswer
			if scenario.requestOffer {
				expectedSDP = peerACKOffer
			}
			if !bytes.Equal(event.Snapshot.RemoteSdp, expectedSDP) {
				t.Fatal("ACK answer snapshot missing")
			}
			found := false
			for _, h := range event.Message.Headers {
				if strings.EqualFold(h.Name, "X-Peer-ACK") && h.Value == "retained" {
					found = true
				}
			}
			if !found {
				t.Fatal("ACK headers lost")
			}
			if scenario.retry && cseq == 20 {
				reply(frame, false)
				if err := <-done; err != nil {
					t.Fatal(err)
				}
				retry := peerACKRead(t, ctx, conn, session)
				if retry.CorrelationId != frame.CorrelationId || !proto.Equal(retry.GetDialogEvent(), event) {
					t.Fatal("retry changed frozen ACK", retry)
				}
				reply(retry, true)
			} else {
				reply(frame, true)
				if err := <-done; err != nil {
					t.Fatal(err)
				}
			}
		} else if err := <-done; err != nil {
			t.Fatal(err)
		}
		if handled, err := runtime.TryNativeCompletion(ctx, flow, &pb.SipMessage{Wire: ack.WireBytes()}); err != nil || !handled {
			t.Fatalf("duplicate ACK CSeq %d: %v", cseq, err)
		}
		// Inspection is also a transport barrier: any erroneous additional event
		// already queued in place of its result fails this assertion.
		writeControllerEnvelope(t, ctx, conn, &pb.Envelope{SessionId: session, CorrelationId: "ack-inspect", RemainingBudgetMs: 1000, Payload: &pb.Envelope_Inspect{Inspect: &pb.ContextInspect{Context: ref}}})
		inspection := peerACKRead(t, ctx, conn, session)
		if inspection.CorrelationId != "ack-inspect" || inspection.GetResult() == nil {
			t.Fatal("ACK was redelivered", inspection)
		}
		snapshot := inspection.GetResult().Snapshot
		expectedRemote := peerACKAnswer
		if scenario.requestOffer {
			expectedRemote = peerACKOffer
		}
		if snapshot == nil || !snapshot.InitialAck || !bytes.Equal(snapshot.RemoteSdp, expectedRemote) {
			t.Fatal("current ACK SDP snapshot", snapshot)
		}
	}
}

func TestResidentPeerReinviteACKDeliveryDrop(t *testing.T) {
	for _, outcome := range []string{"no-capability", "changed-term"} {
		t.Run(outcome, func(t *testing.T) {
			transport, peer, ws, ctx, session, _, _ := residentControlFixture(t, true, outcome == "changed-term")
			e, c, _ := residentSubscriptionFixture(t)
			e.maxBytes = 64 << 20
			e.control = transport.control
			e.ctx = ctx
			c.snapshot.Application = peer.Application()
			c.snapshot.ApplicationLeaseTerm = peer.ApplicationLease().GetTerm()
			if outcome == "changed-term" {
				c.snapshot.ApplicationLeaseTerm = "previous-controller-term"
			}
			budget, err := e.openResidentBudgetLocked(c, residentInviteBudget)
			if err != nil {
				t.Fatal(err)
			}
			c.budget = budget
			event, err := e.stageResidentEventLocked(c, pb.DialogEvent_ACKNOWLEDGED, stateTestRequest("ACK", 42, "", "delivery-drop"), "", time.Now().Add(2*time.Minute))
			if err != nil {
				t.Fatal(err)
			}
			before := e.bytes
			e.deliverResidentEvent(ctx, event)
			if len(c.events) != 0 || !event.next.IsZero() || event.backoff != 0 || e.bytes >= before {
				t.Fatal("mismatched session retained ACK debt or scheduled a retry")
			}
			writeControllerEnvelope(t, ctx, ws, &pb.Envelope{SessionId: session, CorrelationId: "drop-barrier", Payload: &pb.Envelope_Ping{Ping: &pb.Heartbeat{Nonce: "drop-barrier"}}})
			if frame := peerACKRead(t, ctx, ws, session); frame.GetPong().GetNonce() != "drop-barrier" {
				t.Fatal("mismatched session received ACK", frame)
			}
		})
	}
}
