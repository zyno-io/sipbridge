package engine

import (
	"encoding/json"
	"testing"

	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
)

func nativeEstablishedDialogFixture(t *testing.T) portable.Dialog {
	t.Helper()
	original, err := portable.BuildInitialRequest(portable.InitialRequest{Method: sip.INVITE, URI: "sip:phone@example.test", From: "<sip:controller@example.test>", CallID: "native-reinvite", LocalTag: "local-tag", Branch: "z9hG4bK-original", CSeq: 1, Advertise: "192.0.2.1:5061", Contact: "<sip:controller@192.0.2.1:5061;transport=tls>", Transport: "tls"})
	if err != nil {
		t.Fatal(err)
	}
	answer, err := portable.BuildResponseWithHeaders(original, 200, "OK", nil, "remote-tag", "sip:phone@192.0.2.2:5061;transport=tls", nil)
	if err != nil {
		t.Fatal(err)
	}
	dialog, err := portable.NewDialog(portable.UAC, original, answer, portable.DefaultTimers())
	if err != nil {
		t.Fatal(err)
	}
	dialog, _, err = portable.StepDialog(dialog, portable.DialogInput{Kind: portable.DialogLocalACK, Now: 1000, Branch: "z9hG4bK-initial-ack"})
	if err != nil {
		t.Fatal(err)
	}
	return dialog
}

func nativeReinviteFixture(t *testing.T) (portable.Dialog, portable.Message) {
	t.Helper()
	dialog := nativeEstablishedDialogFixture(t)
	dialog, _, err := portable.StepDialog(dialog, portable.DialogInput{Kind: portable.DialogLocalRequest, Now: 1100, Method: sip.INVITE, Branch: "z9hG4bK-reinvite"})
	if err != nil {
		t.Fatal(err)
	}
	return dialog, dialog.LocalInvite.Request
}

func TestNativeTimeoutReinviteAppliesBothTransitionsOrNeither(t *testing.T) {
	dialog := nativeEstablishedDialogFixture(t)
	dialog.RemoteCSeq, dialog.RemoteSet = 11, true
	peer := portable.Dialog{CallID: dialog.CallID, LocalTag: dialog.RemoteTag, RemoteTag: dialog.LocalTag, LocalURI: dialog.RemoteURI, RemoteURI: dialog.LocalURI, Target: dialog.Contact, Contact: dialog.Target, SentBy: "192.0.2.2:5061", Transport: dialog.Transport}
	request, err := portable.BuildDialogRequest(peer, sip.INVITE, 12, nil, "z9hG4bK-timeout-reinvite")
	if err != nil {
		t.Fatal(err)
	}
	response, err := portable.BuildResponseWithHeaders(request, 503, "Service Unavailable", nil, dialog.LocalTag, dialog.Contact, nil)
	if err != nil {
		t.Fatal(err)
	}
	next, effects, err := nativeTimeoutReinviteDialog(dialog, request, response, 1500)
	if err != nil || next == nil || next.RemoteCSeq != 12 || next.RemoteInvite == nil || next.RemoteInvite.State != portable.InviteRejected || next.Offer != portable.NoOffer || next.Target != dialog.Target || next.Contact != dialog.Contact || len(effects) != 1 || effects[0].Kind != portable.Respond {
		t.Fatal("timeout did not atomically reject pending re-INVITE", err)
	}
	if dialog.RemoteCSeq != 11 || dialog.RemoteInvite != nil {
		t.Fatal("candidate mutated original dialog")
	}
	stale := dialog.Clone()
	stale.RemoteCSeq = 12
	next, effects, err = nativeTimeoutReinviteDialog(stale, request, response, 1500)
	if err != nil || next != nil || len(effects) != 0 {
		t.Fatal("stale timeout changed dialog", err)
	}
}

func TestNativeReinviteResponseACKReplayAndConflict(t *testing.T) {
	dialog, request := nativeReinviteFixture(t)
	before, err := json.Marshal(dialog)
	if err != nil {
		t.Fatal(err)
	}
	final, err := portable.BuildResponseWithHeaders(request, 200, "OK", nil, "remote-tag", "sip:phone@192.0.2.3:5061;transport=tls", nil)
	if err != nil {
		t.Fatal(err)
	}
	calls := 0
	branch := func() string { calls++; return "z9hG4bK-shared-ack" }
	next, err := nativeReceiveReinviteResponse(dialog, final, 1200, branch)
	if err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatalf("ACK branch calls = %d", calls)
	}
	if next.Dialog.LocalInvite == nil || next.Dialog.LocalInvite.State != portable.InviteAcked || len(next.ACKEffects) != 1 || next.ACKEffects[0].Kind != portable.Send || next.ACKEffects[0].Message == nil || next.ACKEffects[0].Replay {
		t.Fatal("successful response did not create one exact ACK")
	}
	facts, err := next.ACKEffects[0].Message.Facts()
	if err != nil || facts.Method != sip.ACK || facts.CSeq != next.Dialog.LocalInvite.Sequence {
		t.Fatal("ACK identity changed", facts, err)
	}
	replay, err := nativeReceiveReinviteResponse(next.Dialog, final, 1300, branch)
	if calls != 1 || err != nil || len(replay.ACKEffects) != 1 || !replay.ACKEffects[0].Replay || !replay.ACKEffects[0].Message.EqualWire(*next.ACKEffects[0].Message) {
		t.Fatal("ACK replay changed", err)
	}
	conflict, err := portable.BuildResponseWithHeaders(request, 200, "changed", nil, "remote-tag", "sip:phone@192.0.2.3:5061;transport=tls", nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = nativeReceiveReinviteResponse(next.Dialog, conflict, 1400, branch); err == nil || calls != 1 {
		t.Fatal("conflicting final accepted")
	}
	after, err := json.Marshal(dialog)
	if err != nil || string(before) != string(after) {
		t.Fatal("candidate mutated original dialog", err)
	}
}

func TestNativeReinviteResponsePreservesLateOfferACKDuty(t *testing.T) {
	dialog, request := nativeReinviteFixture(t)
	final, err := portable.BuildResponseWithHeaders(request, 200, "OK", []byte("v=0\r\n"), "remote-tag", "sip:phone@192.0.2.3:5061;transport=tls", []portable.Header{{Name: "Content-Type", Value: "application/sdp"}})
	if err != nil {
		t.Fatal(err)
	}
	calls := 0
	next, err := nativeReceiveReinviteResponse(dialog, final, 1200, func() string { calls++; return "z9hG4bK-not-yet" })
	if err != nil {
		t.Fatal(err)
	}
	if calls != 0 || next.Dialog.Offer != portable.RemoteOfferInACK || next.Dialog.LocalInvite.State != portable.InviteAwaitingACK || len(next.ACKEffects) != 0 {
		t.Fatal("late offer was acknowledged without an answer")
	}
}

func TestNativeReinviteProvisionalAndForeignResponse(t *testing.T) {
	dialog, request := nativeReinviteFixture(t)
	ringing, err := portable.BuildResponseWithHeaders(request, 180, "Ringing", nil, "remote-tag", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	calls := 0
	branch := func() string { calls++; return "z9hG4bK-not-used" }
	next, err := nativeReceiveReinviteResponse(dialog, ringing, 1200, branch)
	if err != nil {
		t.Fatal(err)
	}
	if calls != 0 || len(next.ACKEffects) != 0 || next.Dialog.LocalInvite.State != portable.InvitePending {
		t.Fatal("provisional response created an ACK or completed INVITE")
	}
	foreign, err := portable.BuildInitialRequest(portable.InitialRequest{Method: sip.INVITE, URI: "sip:phone@example.test", From: "<sip:controller@example.test>", CallID: "foreign-call", LocalTag: "local-tag", Branch: "z9hG4bK-foreign", CSeq: 2, Advertise: "192.0.2.1:5061", Contact: "<sip:controller@192.0.2.1:5061;transport=tls>", Transport: "tls"})
	if err != nil {
		t.Fatal(err)
	}
	response, err := portable.BuildResponseWithHeaders(foreign, 200, "OK", nil, "remote-tag", "sip:phone@192.0.2.3:5061;transport=tls", nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = nativeReceiveReinviteResponse(dialog, response, 1300, branch); err == nil || calls != 0 {
		t.Fatal("foreign response admitted")
	}
}

func nativePeerBye(t *testing.T, dialog portable.Dialog, sequence uint32) portable.Message {
	t.Helper()
	peer := portable.Dialog{CallID: dialog.CallID, LocalTag: dialog.RemoteTag, RemoteTag: dialog.LocalTag, LocalURI: dialog.RemoteURI, RemoteURI: dialog.LocalURI, Target: dialog.Contact, Contact: dialog.Target, SentBy: "192.0.2.2:5061", Transport: dialog.Transport}
	m, err := portable.BuildDialogRequest(peer, sip.BYE, sequence, nil, "z9hG4bK-remote-bye")
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func TestNativeRemoteByeCandidateEndsDialogAndSelectsOneResponse(t *testing.T) {
	dialog := nativeEstablishedDialogFixture(t)
	before, err := json.Marshal(dialog)
	if err != nil {
		t.Fatal(err)
	}
	request := nativePeerBye(t, dialog, 12)
	candidate, err := nativeReceiveRemoteBye(dialog, request, dialog.LocalTag, 2000)
	if err != nil {
		t.Fatal(err)
	}
	if candidate.Dialog.Phase != portable.DialogEnded || candidate.Transaction.Family != portable.NonInviteServer || candidate.Transaction.LastResponse == nil {
		t.Fatal("native BYE duties incomplete")
	}
	var sends []portable.Message
	for _, effect := range candidate.TransactionEffects {
		if (effect.Kind == portable.Send || effect.Kind == portable.Respond) && effect.Message != nil {
			sends = append(sends, *effect.Message)
		}
	}
	if len(sends) != 1 || !sends[0].EqualWire(*candidate.Transaction.LastResponse) {
		t.Fatal("BYE did not select one exact final")
	}
	facts, err := sends[0].Facts()
	if err != nil || facts.IsRequest || facts.Status != 200 || facts.CSeq != 12 || facts.CSeqMethod != sip.BYE {
		t.Fatal("BYE response identity", facts, err)
	}
	after, err := json.Marshal(dialog)
	if err != nil || string(before) != string(after) {
		t.Fatal("candidate mutated original dialog", err)
	}
	if _, err = nativeReceiveRemoteBye(candidate.Dialog, request, dialog.LocalTag, 2001); err == nil {
		t.Fatal("ended dialog admitted fresh BYE")
	}
}

func TestNativeRemoteByeRejectsForeignIdentityAndStaleSequence(t *testing.T) {
	dialog := nativeEstablishedDialogFixture(t)
	request := nativePeerBye(t, dialog, 12)
	wrongDialog := dialog
	wrongDialog.RemoteTag = "foreign-tag"
	if _, err := nativeReceiveRemoteBye(wrongDialog, request, dialog.LocalTag, 2000); err == nil {
		t.Fatal("foreign tag admitted")
	}
	stale := dialog
	stale.RemoteSet = true
	stale.RemoteCSeq = 12
	if _, err := nativeReceiveRemoteBye(stale, request, dialog.LocalTag, 2000); err == nil {
		t.Fatal("stale CSeq admitted")
	}
	notBye := dialog.LocalInvite.Request
	if _, err := nativeReceiveRemoteBye(dialog, notBye, dialog.LocalTag, 2000); err == nil {
		t.Fatal("non-BYE admitted")
	}
}

func TestNativeReinviteNilBranchOnlyFailsWhenACKMustBeBuilt(t *testing.T) {
	dialog, request := nativeReinviteFixture(t)
	provisional, err := portable.BuildResponseWithHeaders(request, 180, "Ringing", nil, "remote-tag", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = nativeReceiveReinviteResponse(dialog, provisional, 1200, nil); err != nil {
		t.Fatal("nil supplier rejected provisional", err)
	}
	final, err := portable.BuildResponseWithHeaders(request, 200, "OK", nil, "remote-tag", "sip:phone@192.0.2.3:5061;transport=tls", nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = nativeReceiveReinviteResponse(dialog, final, 1200, nil); err == nil {
		t.Fatal("new ACK accepted without branch")
	}
	next, err := nativeReceiveReinviteResponse(dialog, final, 1200, func() string { return "z9hG4bK-ack" })
	if err != nil {
		t.Fatal(err)
	}
	if _, err = nativeReceiveReinviteResponse(next.Dialog, final, 1300, nil); err != nil {
		t.Fatal("nil supplier rejected stored ACK replay", err)
	}
	offer, err := portable.BuildResponseWithHeaders(request, 200, "OK", []byte("v=0\r\n"), "remote-tag", "sip:phone@192.0.2.3:5061;transport=tls", []portable.Header{{Name: "Content-Type", Value: "application/sdp"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = nativeReceiveReinviteResponse(dialog, offer, 1200, nil); err != nil {
		t.Fatal("nil supplier rejected late offer", err)
	}
}

func TestNativeInitialInviteForkResponseAndACK(t *testing.T) {
	origin, err := portable.BuildInitialRequest(portable.InitialRequest{Method: sip.INVITE, URI: "sip:phone@example.test", From: "<sip:controller@example.test>", CallID: "initial-fork", LocalTag: "local-tag", Branch: "z9hG4bK-original", CSeq: 1, Advertise: "192.0.2.1:5061", Contact: "<sip:controller@192.0.2.1:5061;transport=tls>", Transport: "tls"})
	if err != nil {
		t.Fatal(err)
	}
	early, err := portable.BuildResponseWithHeaders(origin, 180, "Ringing", nil, "remote-tag", "sip:phone@192.0.2.2:5061;transport=tls", nil)
	if err != nil {
		t.Fatal(err)
	}
	calls := 0
	branch := func() string { calls++; return "z9hG4bK-fork-ack" }
	provisional, err := nativeReceiveInitialInviteDialog(origin, early, nil, false, 1000, branch)
	if err != nil || calls != 0 || provisional.Dialog.Version == 0 || len(provisional.ACKEffects) != 0 {
		t.Fatalf("provisional fork: %v", err)
	}
	final, err := portable.BuildResponseWithHeaders(origin, 200, "OK", nil, "remote-tag", "sip:phone@192.0.2.2:5061;transport=tls", nil)
	if err != nil {
		t.Fatal(err)
	}
	candidate, err := nativeReceiveInitialInviteDialog(origin, final, &provisional.Dialog, false, 1100, branch)
	if err != nil || calls != 1 || len(candidate.ACKEffects) != 1 || candidate.ACKEffects[0].Kind != portable.Send || candidate.ACKEffects[0].Message == nil {
		t.Fatalf("fork ACK: %v calls=%d", err, calls)
	}
	facts, err := candidate.ACKEffects[0].Message.Facts()
	if err != nil || facts.Method != sip.ACK {
		t.Fatal("fork did not produce ACK", err)
	}
	if provisional.Dialog.LocalInvite == nil || provisional.Dialog.LocalInvite.State == portable.InviteAcked {
		t.Fatal("candidate mutated original fork")
	}
}
