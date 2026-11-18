package engine

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
	"google.golang.org/protobuf/proto"
	"sipbridge/internal/control"
	pb "sipbridge/internal/controlpb"
)

func TestResidentPeerReinviteACKZeroCSeq(t *testing.T) {
	for _, optIn := range []bool{false, true} {
		t.Run(fmt.Sprintf("negotiated=%t", optIn), func(t *testing.T) {
			e, peer, conn, ctx, session, writes, _ := residentControlFixture(t, true, optIn)
			testPeerReinviteACKZeroCSeq(t, e, peer, conn, ctx, session, residentDirectFlow(e), writes, optIn)
		})
	}
}

func TestRedisNativePeerReinviteACKZeroCSeq(t *testing.T) {
	for _, optIn := range []bool{false, true} {
		t.Run(fmt.Sprintf("negotiated=%t", optIn), func(t *testing.T) {
			e, peer, conn, ctx, session, flow, writes := peerACKRedisFixture(t, optIn)
			testPeerReinviteACKZeroCSeq(t, e, peer, conn, ctx, session, flow, writes, optIn)
		})
	}
}

func testPeerReinviteACKZeroCSeq(t *testing.T, runtime peerACKRuntime, peer *control.Peer, conn *websocket.Conn, ctx context.Context, session string, flow *pb.FlowRef, writes <-chan []byte, optIn bool) {
	t.Helper()
	// A UAC dialog has no remote CSeq until the callee sends its first request.
	// Establish it through the runtime so CSeq 0 reaches the pinned reducer.
	command := &pb.Command{Context: &pb.ContextRef{Owner: peer.Application(), Id: "zero-cseq"}, Sequence: 1,
		Admission: &pb.CommandAdmission{DirectoryEpoch: "directory", ExpiresAtMs: uint64(time.Now().Add(5 * time.Second).UnixMilli()), SelectedFlow: flow},
		Action: &pb.Command_OpenUac{OpenUac: &pb.OpenUac{Method: "INVITE", Uri: "sip:phone@example.test", Transport: "tls", BindingId: "phone", PrincipalId: "account",
			Headers: []*pb.Header{{Name: "From", Value: "<sip:service@example.test>"}}}}}
	writeControllerEnvelope(t, ctx, conn, &pb.Envelope{SessionId: session, CorrelationId: "open-zero-cseq", RemainingBudgetMs: 3000, Payload: &pb.Envelope_Command{Command: command}})
	opened := peerACKRead(t, ctx, conn, session).GetResult()
	if opened == nil || opened.Certainty != pb.OperationResult_ACCEPTED || opened.Error != "" {
		t.Fatal("initial UAC admission", opened)
	}
	var inviteWire []byte
	select {
	case inviteWire = <-writes:
	case <-ctx.Done():
		t.Fatal("initial INVITE missing")
	}
	invite := portable.RestoreMessage(inviteWire, "tls", "", "")
	final, err := portable.BuildResponseWithHeaders(invite, 200, "OK", nil, "callee", "sip:phone@example.test:5061;transport=tls", nil)
	if err != nil {
		t.Fatal(err)
	}
	complete := func(message portable.Message) <-chan error {
		done := make(chan error, 1)
		go func() {
			handled, err := runtime.TryNativeCompletion(ctx, flow, &pb.SipMessage{Wire: message.WireBytes()})
			if err == nil && !handled {
				err = fmt.Errorf("native completion not handled")
			}
			done <- err
		}()
		return done
	}
	reply := func(frame *pb.Envelope) {
		writeControllerEnvelope(t, ctx, conn, &pb.Envelope{SessionId: session, CorrelationId: frame.CorrelationId, Payload: &pb.Envelope_Result{Result: &pb.OperationResult{Certainty: pb.OperationResult_COMPLETED}}})
	}
	done := complete(final)
	answered := peerACKRead(t, ctx, conn, session)
	if answered.GetDialogEvent().GetKind() != pb.DialogEvent_ANSWERED {
		t.Fatal("initial ANSWERED missing", answered)
	}
	reply(answered)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	select {
	case wire := <-writes:
		parsed, err := sip.ParseMessage(wire)
		if err != nil || parsed.CSeq().MethodName != sip.ACK {
			t.Fatal("initial native ACK missing", err)
		}
	case <-ctx.Done():
		t.Fatal("initial native ACK missing")
	}
	remote, err := portable.NewDialog(portable.UAS, invite, final, portable.DefaultTimers())
	if err != nil {
		t.Fatal(err)
	}
	reinvite, err := portable.BuildDialogRequestWithHeaders(remote, sip.INVITE, 0, nil, "z9hG4bK-zero-invite", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.ReceiveRequest(ctx, peer, flow, &pb.SipMessage{Wire: reinvite.WireBytes()}); err != nil {
		t.Fatal(err)
	}
	request := peerACKRead(t, ctx, conn, session).GetRequest()
	if request == nil {
		t.Fatal("CSeq-0 re-INVITE not presented")
	}
	decision := residentDirectDecision(request, 2, 200)
	decision.Body = peerACKOffer
	decision.Headers = []*pb.Header{{Name: "Content-Type", Value: "application/sdp"}}
	if result := residentSendDecision(t, ctx, conn, session, "answer-zero-cseq", decision); result.Certainty != pb.OperationResult_ACCEPTED || result.Error != "" {
		t.Fatal("CSeq-0 re-INVITE response", result)
	}
	peerACKFinal(t, ctx, writes)
	ack, err := portable.BuildDialogRequestWithHeaders(remote, sip.ACK, 0, peerACKAnswer, "z9hG4bK-zero-ack", []portable.Header{{Name: "Content-Type", Value: "application/sdp"}})
	if err != nil {
		t.Fatal(err)
	}
	done = complete(ack)
	if optIn {
		frame := peerACKRead(t, ctx, conn, session)
		event := frame.GetDialogEvent()
		// Message presence distinguishes this from an initial ACK even when
		// inviteCseq is zero (or omitted by a JSON encoder as its default).
		if event == nil || event.Kind != pb.DialogEvent_ACKNOWLEDGED || event.Message == nil || event.InviteCseq != 0 ||
			!bytes.Equal(event.Message.Wire, ack.WireBytes()) || !bytes.Equal(event.Message.Body, peerACKAnswer) ||
			!proto.Equal(event.Snapshot.Context, opened.Snapshot.Context) || !bytes.Equal(event.Snapshot.RemoteSdp, peerACKAnswer) {
			t.Fatal("CSeq-0 re-INVITE ACK not distinguishable", frame)
		}
		reply(frame)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("CSeq-0 ACK completion blocked")
	}
	if shared, ok := runtime.(*redisNative); ok {
		// ACK delivery is asynchronous in Redis. Wait for the procedure's
		// reciprocal event debt to settle before the transport barrier, so a
		// late incorrect delivery cannot escape the negative assertion.
		waitCtx, cancel, store, err := shared.operation(ctx, time.Now().Add(3*time.Second))
		if err != nil {
			t.Fatal(err)
		}
		defer cancel()
		field := "r:procedure-" + request.RequestId
		for {
			selected, err := store.ObserveContext(waitCtx, opened.Snapshot.Context.Id, opened.Snapshot.Context.State.Incarnation, field)
			if err != nil {
				t.Fatal("ACK delivery debt did not settle", err)
			}
			owner, err := readPrivateProcedureRecord(selected.View.Records[field])
			if err != nil {
				t.Fatal(err)
			}
			if len(owner.Events) == 0 {
				break
			}
			select {
			case <-time.After(time.Millisecond):
			case <-waitCtx.Done():
				t.Fatal("ACK delivery debt did not settle")
			}
		}
	}
	// Both paths must complete native negotiation. The ordered inspection
	// also catches an ACK delivered to a controller without the capability.
	writeControllerEnvelope(t, ctx, conn, &pb.Envelope{SessionId: session, CorrelationId: "zero-inspect", RemainingBudgetMs: 1000, Payload: &pb.Envelope_Inspect{Inspect: &pb.ContextInspect{Context: opened.Snapshot.Context}}})
	inspection := peerACKRead(t, ctx, conn, session)
	if inspection.CorrelationId != "zero-inspect" || inspection.GetResult() == nil || !bytes.Equal(inspection.GetResult().GetSnapshot().GetRemoteSdp(), peerACKAnswer) {
		t.Fatal("unexpected ACK delivery or missing native completion", inspection)
	}
}
