package engine

import (
	"testing"

	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
)

func TestNativeInitialInviteFailureACKAndReplay(t *testing.T) {
	origin, err := portable.BuildInitialRequest(portable.InitialRequest{Method: sip.INVITE, URI: "sip:phone@example.test", From: "<sip:controller@example.test>", CallID: "uac-failure", LocalTag: "local", Branch: "z9hG4bK-original", CSeq: 1, Advertise: "192.0.2.1:5060", Contact: "<sip:controller@192.0.2.1:5060;transport=udp>", Transport: "udp"})
	if err != nil {
		t.Fatal(err)
	}
	tx, err := portable.NewTransaction(portable.InviteClient, origin, portable.DefaultTimers(), "")
	if err != nil {
		t.Fatal(err)
	}
	tx, _, err = portable.StepTransaction(tx, portable.Input{Kind: portable.Start, Now: 1000})
	if err != nil {
		t.Fatal(err)
	}
	response, err := portable.BuildResponseWithHeaders(origin, 486, "Busy", nil, "remote", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	first, err := nativeReceiveInitialInviteFailure(tx, response, 1100)
	if err != nil || first.ACK == nil || first.Replay || first.Transaction.Phase != portable.Completed {
		t.Fatalf("failed final: %v", err)
	}
	replay, err := nativeReceiveInitialInviteFailure(first.Transaction, response, 1200)
	if err != nil || replay.ACK == nil || !replay.Replay || !replay.ACK.EqualWire(*first.ACK) {
		t.Fatalf("failed ACK replay: %v", err)
	}
	if tx.Phase != portable.Calling || tx.ACK != nil {
		t.Fatal("candidate mutated input")
	}
}
