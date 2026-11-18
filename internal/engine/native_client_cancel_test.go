package engine

import (
	"testing"

	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
)

func TestNativePairedCancelDefersUntilProvisional(t *testing.T) {
	origin, err := portable.BuildInitialRequest(portable.InitialRequest{Method: sip.INVITE, URI: "sip:phone@example.test", From: "<sip:caller@example.test>", CallID: "cancel-fixture", LocalTag: "local", Branch: "z9hG4bK-cancel", CSeq: 1, Advertise: "192.0.2.1:5060", Contact: "<sip:caller@192.0.2.1:5060>", Transport: "udp"})
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
	requested, _, child, _, err := nativeRequestClientCancel(tx, 1100)
	if err != nil || !requested.Cancelled || child != nil || requested.Timers[portable.TimerCancel].Active || tx.Cancelled {
		t.Fatal("cancel was not deferred", err)
	}
	ringing, err := portable.BuildResponseWithHeaders(origin, 180, "Ringing", nil, "remote", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	proceeding, effects, err := portable.StepTransaction(requested, portable.Input{Kind: portable.ReceiveResponse, Now: 1200, Message: ringing})
	if err != nil {
		t.Fatal(err)
	}
	child, started, err := nativePairedCancel(proceeding, effects, 1200)
	if err != nil || child == nil || child.Family != portable.NonInviteClient || !proceeding.CancelSent || !proceeding.Timers[portable.TimerCancel].Active || !child.Timers[portable.TimerF].Active {
		t.Fatal("paired CANCEL was not started", err)
	}
	facts, err := child.Origin.Facts()
	if err != nil || facts.Method != sip.CANCEL || !portable.MatchRequest(origin, child.Origin) {
		t.Fatal("paired identity changed", err)
	}
	sends := 0
	for _, effect := range started {
		if effect.Kind == portable.Send && effect.Message != nil && effect.Message.Equal(child.Origin) {
			sends++
		}
	}
	if sends != 1 {
		t.Fatal("missing exact CANCEL write")
	}
	again, _, duplicate, _, err := nativeRequestClientCancel(proceeding, 1300)
	if err != nil || duplicate != nil || again.Timers[portable.TimerCancel] != proceeding.Timers[portable.TimerCancel] {
		t.Fatal("duplicate CANCEL slid native deadline", err)
	}
}

func TestNativeCancelChallengedInvitePreservesACK(t *testing.T) {
	for _, transport := range []string{"udp", "tls"} {
		t.Run(transport, func(t *testing.T) {
			origin, err := portable.BuildInitialRequest(portable.InitialRequest{Method: sip.INVITE, URI: "sip:phone@example.test", From: "<sip:caller@example.test>", CallID: "challenge-cancel", LocalTag: "local", Branch: "z9hG4bK-challenge-cancel", CSeq: 1, Advertise: "192.0.2.1:5060", Contact: "<sip:caller@192.0.2.1:5060>", Transport: transport})
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
			response, err := portable.BuildResponseWithHeaders(origin, 401, "Unauthorized", nil, "remote", "", nil)
			if err != nil {
				t.Fatal(err)
			}
			tx, _, err = portable.StepTransaction(tx, portable.Input{Kind: portable.ReceiveResponse, Now: 1100, Message: response})
			if err != nil {
				t.Fatal(err)
			}
			if tx.ACK == nil {
				t.Fatal("challenge ACK missing")
			}
			next, effects, child, _, err := nativeRequestClientCancel(tx, 1200)
			if err != nil || child != nil || len(effects) != 0 || next.ACK == nil || !next.ACK.Equal(*tx.ACK) || next.Phase != tx.Phase {
				t.Fatal("challenge cancellation changed ACK duty", err)
			}
			if transport == "udp" && (!next.Cancelled || next.Phase != portable.Completed) || transport == "tls" && (next.Cancelled || next.Phase != portable.Terminated) {
				t.Fatal("unexpected native cancellation transition")
			}
		})
	}
}
