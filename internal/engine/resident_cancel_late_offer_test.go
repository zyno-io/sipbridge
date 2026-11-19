package engine

import (
	"bytes"
	"testing"
	"time"

	"github.com/emiago/sipgo/sip/portable"
	pb "sipbridge/internal/controlpb"
)

func TestResidentCancelCrossedLateOfferCompletesACKThenBYE(t *testing.T) {
	e, peer, conn, ctx, session, writes, _ := residentControlFixture(t)
	flow := &pb.FlowRef{Ingress: e.cfg.Process, ConnectionId: "phone", SourceIp: "192.0.2.12", SourcePort: 50300, Listener: "sip.example.test:5061", Transport: "tls"}
	open := &pb.Command{Context: &pb.ContextRef{Owner: peer.Application(), Id: "cancel-crossed-offer"}, Sequence: 1,
		Admission: &pb.CommandAdmission{DirectoryEpoch: "directory", ExpiresAtMs: uint64(time.Now().Add(5 * time.Second).UnixMilli()), SelectedFlow: flow},
		Action:    &pb.Command_OpenUac{OpenUac: &pb.OpenUac{Uri: "sip:phone@example.test", Transport: "tls", BindingId: "binding", Headers: []*pb.Header{{Name: "From", Value: "<sip:service@example.test>"}}}}}
	opened, err := e.commandResidentClient(ctx, peer, open, "open", time.Now().Add(5*time.Second))
	if err != nil || opened.Certainty != pb.OperationResult_ACCEPTED {
		t.Fatal("open", err)
	}
	origin := portable.RestoreMessage(<-writes, "tls", "", "")
	ringing, err := portable.BuildResponseWithHeaders(origin, 180, "Ringing", nil, "remote", "sip:phone@example.test", nil)
	if err != nil {
		t.Fatal(err)
	}
	observed, err := Observe(ringing.WireBytes(), false)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { _, x := e.completeResidentClient(ctx, flow, observed); done <- x }()
	event := readControllerEnvelope(t, ctx, conn)
	if event.GetDialogEvent().GetKind() != pb.DialogEvent_PROVISIONAL {
		t.Fatal("missing ringing event")
	}
	writeControllerEnvelope(t, ctx, conn, &pb.Envelope{SessionId: session, CorrelationId: event.CorrelationId, Payload: &pb.Envelope_Result{Result: &pb.OperationResult{Certainty: pb.OperationResult_ACCEPTED}}})
	if err = <-done; err != nil {
		t.Fatal(err)
	}
	cancel := &pb.Command{Context: opened.Snapshot.Context, Sequence: 2, Action: &pb.Command_Cancel{Cancel: &pb.Cancel{}}}
	cancelled, err := e.commandResidentClient(ctx, peer, cancel, "cancel", time.Now().Add(5*time.Second))
	if err != nil || cancelled.Certainty != pb.OperationResult_ACCEPTED {
		t.Fatal("cancel", err)
	}
	if wire := <-writes; !bytes.HasPrefix(wire, []byte("CANCEL ")) {
		t.Fatal("missing native CANCEL")
	}
	final, err := portable.BuildResponseWithHeaders(origin, 200, "OK", []byte(nativeUnwantedOffer), "remote", "sip:phone@example.test", []portable.Header{{Name: "Content-Type", Value: "application/sdp"}})
	if err != nil {
		t.Fatal(err)
	}
	observed, err = Observe(final.WireBytes(), false)
	if err != nil {
		t.Fatal(err)
	}
	if handled, x := e.completeResidentClient(ctx, flow, observed); x != nil || !handled {
		t.Fatal("crossed final", handled, x)
	}
	for _, method := range []string{"ACK", "BYE"} {
		select {
		case wire := <-writes:
			if !bytes.HasPrefix(wire, []byte(method+" ")) {
				t.Fatal("wrong native cleanup order", method)
			}
			if method == "ACK" && (!bytes.Contains(wire, []byte("m=audio 0 RTP/AVP 0\r\n")) || !bytes.Contains(wire, []byte("m=video 0 RTP/AVP 31\r\n"))) {
				t.Fatal("ACK did not reject media")
			}
		case <-ctx.Done():
			t.Fatal("missing native cleanup", method)
		}
	}
	e.mu.Lock()
	c := e.contexts[open.Context.Id]
	e.mu.Unlock()
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.snapshot.Answered || c.selectedDialog != "" || len(c.events) != 0 {
		t.Fatal("cancelled late offer selected a winner or presented an application event")
	}
}
