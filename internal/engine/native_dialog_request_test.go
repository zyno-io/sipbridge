package engine

import (
	"encoding/json"
	"testing"

	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
)

func TestNativeDialogRequestBYEUsesSelectedDialog(t *testing.T) {
	dialog := nativeEstablishedDialogFixture(t)
	before, err := json.Marshal(dialog)
	if err != nil {
		t.Fatal(err)
	}
	next, tx, effects, err := nativePrepareDialogRequest(dialog, portable.DialogInput{Kind: portable.DialogLocalRequest, Now: 1100, Method: sip.BYE, Branch: "z9hG4bK-app-bye"}, 1101)
	if err != nil {
		t.Fatal(err)
	}
	after, err := json.Marshal(dialog)
	if err != nil || string(before) != string(after) {
		t.Fatal("candidate mutated selected dialog", err)
	}
	if next.LocalBye == nil || next.LocalCSeq != dialog.LocalCSeq+1 || tx.Family != portable.NonInviteClient || !portable.MatchRequest(*next.LocalBye, tx.Origin) {
		t.Fatal("BYE did not preserve dialog and transaction identity")
	}
	var send, f int
	for _, effect := range effects {
		if effect.Kind == portable.Send {
			send++
		}
		if effect.Kind == portable.Schedule && effect.Timer == portable.TimerF {
			f++
		}
	}
	if send != 1 || f != 1 {
		t.Fatalf("expected one send and F deadline, got %d and %d", send, f)
	}
}
