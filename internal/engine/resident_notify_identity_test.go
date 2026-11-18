package engine

import (
	"bytes"
	"strings"
	"testing"

	"github.com/emiago/sipgo/sip/portable"
	pb "sipbridge/internal/controlpb"
)

func TestResidentNotifyOperationReusePreservesOverlappingProcedures(t *testing.T) {
	e, peer, connection, ctx, session, writes, _ := residentControlFixture(t, true)
	flow := residentDirectFlow(e)
	subscribe := func(sequence int, localTag, branch string) *pb.SipMessage {
		message := stateTestRequest("SUBSCRIBE", sequence, localTag, branch)
		message.Wire = []byte(strings.Replace(string(message.Wire), "Content-Length:", "Event: message-summary\r\nExpires: 90\r\nContent-Length:", 1))
		return message
	}
	first := residentDirectRequest(t, e, peer, connection, ctx, flow, subscribe(1, "", "first-subscribe"))
	if result := residentSendDecision(t, ctx, connection, session, "reused-operation", residentDirectDecision(first, 1, 202)); result.Certainty != pb.OperationResult_ACCEPTED {
		t.Fatal("first subscription decision refused", result)
	}
	firstResponse, firstNotify := <-writes, <-writes
	if !bytes.HasPrefix(firstResponse, []byte("SIP/2.0")) || !bytes.HasPrefix(firstNotify, []byte("NOTIFY ")) {
		t.Fatal("first response and NOTIFY were not emitted")
	}
	firstNative, err := portable.ParseMessage(firstNotify, "tls", "", "")
	if err != nil {
		t.Fatal(err)
	}
	facts, err := firstNative.Facts()
	if err != nil {
		t.Fatal(err)
	}
	refresh := residentDirectRequest(t, e, peer, connection, ctx, flow, subscribe(2, facts.FromTag, "second-subscribe"))
	if result := residentSendDecision(t, ctx, connection, session, "reused-operation", residentDirectDecision(refresh, 2, 202)); result.Certainty != pb.OperationResult_ACCEPTED {
		t.Fatal("refresh with a reused operation ID refused", result)
	}
	secondResponse, secondNotify := <-writes, <-writes
	if !bytes.HasPrefix(secondResponse, []byte("SIP/2.0")) || !bytes.HasPrefix(secondNotify, []byte("NOTIFY ")) {
		t.Fatal("refresh response and NOTIFY were not emitted")
	}
	secondNative, err := portable.ParseMessage(secondNotify, "tls", "", "")
	if err != nil {
		t.Fatal(err)
	}
	firstKey, err := firstNative.ClientTxKey()
	if err != nil {
		t.Fatal(err)
	}
	secondKey, err := secondNative.ClientTxKey()
	if err != nil || firstKey == secondKey {
		t.Fatal("later sequence aliased an outstanding NOTIFY transaction", err)
	}
	reply, err := portable.ResponseFor(firstNative, 200, "OK", nil, "")
	if err != nil {
		t.Fatal(err)
	}
	matched, err := e.TryNativeCompletion(ctx, flow, &pb.SipMessage{Wire: reply.WireBytes()})
	if err != nil || !matched {
		t.Fatal("earlier NOTIFY response lost its native procedure", err)
	}
	c, err := e.findContext(first.Snapshot.Context)
	if err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if later := c.transactions[secondKey]; later == nil || later.native.Phase != portable.Trying {
		t.Fatal("earlier response completed the later NOTIFY")
	}
}
