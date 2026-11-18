package engine

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
	pb "sipbridge/internal/controlpb"
)

func TestNativeLocalAnswerACKUsesExactPendingOffer(t *testing.T) {
	dialog, request := nativeReinviteFixture(t)
	final, err := portable.BuildResponseWithHeaders(request, 200, "OK", []byte("v=0\r\n"), "remote-tag", "sip:phone@192.0.2.3:5061;transport=tls", []portable.Header{{Name: "Content-Type", Value: "application/sdp"}})
	if err != nil {
		t.Fatal(err)
	}
	offered, err := nativeReceiveReinviteResponse(dialog, final, 1200, func() string { t.Fatal("early ACK"); return "" })
	if err != nil {
		t.Fatal(err)
	}
	before, err := json.Marshal(offered.Dialog)
	if err != nil {
		t.Fatal(err)
	}
	answer := []byte("v=0\r\ns=answer\r\n")
	next, effects, err := nativePrepareLocalAnswerACK(offered.Dialog, &pb.DialogRequest{Method: "ACK", Body: answer}, "z9hG4bK-answer", 1300)
	if err != nil || next.LocalInvite == nil || next.LocalInvite.State != portable.InviteAcked || len(effects) != 1 || effects[0].Kind != portable.Send || effects[0].Message == nil {
		t.Fatal("native answer ACK", err)
	}
	facts, err := effects[0].Message.Facts()
	if err != nil || facts.Method != sip.ACK || facts.CSeq != next.LocalInvite.Sequence {
		t.Fatal("wrong ACK identity", err)
	}
	body, err := effects[0].Message.Body()
	if err != nil || !bytes.Equal(body, answer) {
		t.Fatal("wrong ACK answer", err)
	}
	after, err := json.Marshal(offered.Dialog)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("candidate changed selected dialog", err)
	}
	if _, _, err = nativePrepareLocalAnswerACK(next, &pb.DialogRequest{Method: "ACK", Body: answer}, "z9hG4bK-answer", 1400); err == nil {
		t.Fatal("answered offer accepted twice")
	}
}
