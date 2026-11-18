package engine

import (
	"testing"

	"github.com/emiago/sipgo/sip/portable"
)

func TestNativeSuccessfulACKReplayKeepsOriginalM(t *testing.T) {
	dialog, request := nativeReinviteFixture(t)
	tx, err := portable.NewTransaction(portable.InviteClient, request, portable.DefaultTimers(), "")
	if err != nil {
		t.Fatal(err)
	}
	tx, _, err = portable.StepTransaction(tx, portable.Input{Kind: portable.Start, Now: 1100})
	if err != nil {
		t.Fatal(err)
	}
	final, err := portable.BuildResponseWithHeaders(request, 200, "OK", nil, "remote-tag", "sip:phone@192.0.2.3:5061;transport=tls", nil)
	if err != nil {
		t.Fatal(err)
	}
	tx, _, err = portable.StepTransaction(tx, portable.Input{Kind: portable.ReceiveResponse, Now: 1200, Message: final})
	if err != nil {
		t.Fatal(err)
	}
	selected, err := nativeReceiveReinviteResponse(dialog, final, 1200, func() string { return "z9hG4bK-retained-ack" })
	if err != nil || len(selected.ACKEffects) != 1 || selected.ACKEffects[0].Message == nil {
		t.Fatal("initial ACK", err)
	}
	ack := *selected.ACKEffects[0].Message
	next, effects, err := nativeSuccessfulACKReplay(tx, final, final, ack, 1300)
	if err != nil || len(effects) != 1 || !effects[0].Replay || next.Timers[portable.TimerM] != tx.Timers[portable.TimerM] {
		t.Fatal("exact replay changed original M", err)
	}
	changed, err := portable.BuildResponseWithHeaders(request, 200, "different", nil, "remote-tag", "sip:phone@192.0.2.3:5061;transport=tls", nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err = nativeSuccessfulACKReplay(tx, changed, final, ack, 1300); err == nil {
		t.Fatal("different final accepted")
	}
	if _, _, err = nativeSuccessfulACKReplay(tx, final, final, ack, tx.Timers[portable.TimerM].At); err == nil {
		t.Fatal("replay extended original M")
	}
}
