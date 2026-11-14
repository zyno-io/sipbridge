package engine

import (
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
	pb "sipbridge/internal/controlpb"
)

func TestResidentSubscribeRequiresImmediateNotifyBudget(t *testing.T) {
	e, peer, conn, ctx, session, writes, _ := residentControlFixture(t)
	message := stateTestRequest("SUBSCRIBE", 1, "", "resident-subscribe")
	message.Wire = []byte(strings.Replace(string(message.Wire), "Content-Length:", "Event: message-summary\r\nExpires: 90\r\nContent-Length:", 1))
	route, err := e.LocateRoute(ctx, message)
	if err != nil {
		t.Fatal(err)
	}
	flow := &pb.FlowRef{Ingress: e.cfg.Process, ConnectionId: "phone", SourceIp: "192.0.2.12", SourcePort: 50300, Listener: "sip.example.test:5061", Transport: "tls"}
	done := make(chan error, 1)
	go func() {
		_, err := e.ingest(ctx, peer, &pb.OwnerIngest{Context: route.Context, Application: peer.Application(), Flow: flow, Wire: message.Wire}, time.Now().Add(2*time.Second))
		done <- err
	}()
	presented := readControllerEnvelope(t, ctx, conn)
	if presented.GetRequest() == nil {
		t.Fatal("SUBSCRIBE not presented")
	}
	respond := &pb.Command{Context: route.Context, Sequence: 1, Admission: &pb.CommandAdmission{DirectoryEpoch: "directory", ExpiresAtMs: uint64(time.Now().Add(2 * time.Second).UnixMilli())}, Action: &pb.Command_Respond{Respond: &pb.Respond{RequestId: presented.GetRequest().RequestId, Status: 200, Headers: []*pb.Header{{Name: "Expires", Value: "90"}}}}}
	writeControllerEnvelope(t, ctx, conn, &pb.Envelope{SessionId: session, CorrelationId: "subscribe-response", RemainingBudgetMs: 2000, Payload: &pb.Envelope_Command{Command: respond}})
	result := readControllerEnvelope(t, ctx, conn).GetResult()
	if result == nil || result.Certainty != pb.OperationResult_COMPLETED {
		t.Fatal("SUBSCRIBE response failed", result)
	}
	select {
	case <-writes:
	case <-ctx.Done():
		t.Fatal("SUBSCRIBE response missing")
	}
	writeControllerEnvelope(t, ctx, conn, &pb.Envelope{SessionId: session, CorrelationId: presented.CorrelationId, Payload: &pb.Envelope_Result{Result: &pb.OperationResult{Certainty: pb.OperationResult_ACCEPTED}}})
	if err = <-done; err != nil {
		t.Fatal(err)
	}
	c, err := e.findContext(route.Context)
	if err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	reserved := c.requiredNotifyBudget != nil
	c.mu.Unlock()
	if !reserved {
		t.Fatal("accepted subscription lacks NOTIFY budget")
	}
	notify := &pb.Command{Context: route.Context, Sequence: 2, Admission: &pb.CommandAdmission{DirectoryEpoch: "directory", ExpiresAtMs: uint64(time.Now().Add(2 * time.Second).UnixMilli())}, Action: &pb.Command_DialogRequest{DialogRequest: &pb.DialogRequest{Method: "NOTIFY"}}}
	writeControllerEnvelope(t, ctx, conn, &pb.Envelope{SessionId: session, CorrelationId: "notify", RemainingBudgetMs: 2000, Payload: &pb.Envelope_Command{Command: notify}})
	var notifyWire []byte
	select {
	case notifyWire = <-writes:
	case <-ctx.Done():
		t.Fatal("NOTIFY missing")
	}
	reply, err := portable.ResponseFor(portable.RestoreMessage(notifyWire, "tls", "", ""), 200, "OK", nil, "")
	if err != nil {
		t.Fatal(err)
	}
	handled, err := e.tryNativeCompletion(ctx, flow, &pb.SipMessage{Wire: reply.WireBytes()})
	if err != nil || !handled {
		t.Fatal("NOTIFY response not handled", handled, err)
	}
	result = readControllerEnvelope(t, ctx, conn).GetResult()
	if result == nil || result.Certainty != pb.OperationResult_COMPLETED {
		t.Fatal("NOTIFY command failed", result)
	}
	c.mu.Lock()
	reserved = c.requiredNotifyBudget != nil
	c.mu.Unlock()
	if reserved {
		t.Fatal("NOTIFY budget was not transferred")
	}
	c.mu.Lock()
	remote := portable.Dialog{Version: portable.Version, Role: portable.UAC, Phase: portable.DialogConfirmed, CallID: c.snapshot.CallId, LocalTag: c.snapshot.RemoteTag, RemoteTag: c.snapshot.LocalTag, LocalURI: c.snapshot.RemoteUri, RemoteURI: c.snapshot.LocalUri, Target: c.snapshot.LocalUri, Contact: c.snapshot.RemoteUri, SentBy: "192.0.2.12:50300", Transport: "tls"}
	c.mu.Unlock()
	refresh := func(seq uint32, reject bool) {
		request, buildErr := portable.BuildDialogRequestWithHeaders(remote, sip.SUBSCRIBE, seq, nil, "z9hG4bK-refresh-"+strconv.Itoa(int(seq)), []portable.Header{{Name: "Event", Value: "message-summary"}})
		if buildErr != nil {
			t.Fatal(buildErr)
		}
		done := make(chan error, 1)
		go func() {
			_, err := e.ingest(ctx, peer, &pb.OwnerIngest{Context: route.Context, Application: peer.Application(), Flow: flow, Wire: request.WireBytes()}, time.Now().Add(2*time.Second))
			done <- err
		}()
		presented := readControllerEnvelope(t, ctx, conn)
		if presented.GetRequest() == nil {
			t.Fatal("refresh not presented")
		}
		if reject {
			command := &pb.Command{Context: route.Context, Sequence: 3, Admission: &pb.CommandAdmission{DirectoryEpoch: "directory", ExpiresAtMs: uint64(time.Now().Add(2 * time.Second).UnixMilli())}, Action: &pb.Command_Respond{Respond: &pb.Respond{RequestId: presented.GetRequest().RequestId, Status: 403}}}
			writeControllerEnvelope(t, ctx, conn, &pb.Envelope{SessionId: session, CorrelationId: "reject-refresh", RemainingBudgetMs: 2000, Payload: &pb.Envelope_Command{Command: command}})
			if result := readControllerEnvelope(t, ctx, conn).GetResult(); result == nil || result.Certainty != pb.OperationResult_COMPLETED {
				t.Fatal("refresh rejection failed", result)
			}
			writeControllerEnvelope(t, ctx, conn, &pb.Envelope{SessionId: session, CorrelationId: presented.CorrelationId, Payload: &pb.Envelope_Result{Result: &pb.OperationResult{Certainty: pb.OperationResult_ACCEPTED}}})
		} else {
			writeControllerEnvelope(t, ctx, conn, &pb.Envelope{SessionId: session, CorrelationId: presented.CorrelationId, Payload: &pb.Envelope_Result{Result: &pb.OperationResult{Certainty: pb.OperationResult_NOT_ACCEPTED}}})
		}
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		select {
		case <-writes:
		case <-ctx.Done():
			t.Fatal("refresh response missing")
		}
		c.mu.Lock()
		pending := c.subscription.Pending
		c.mu.Unlock()
		if pending != "" {
			t.Fatal("failed refresh left subscription pending", pending)
		}
	}
	refresh(2, true)
	refresh(3, false)

}
