package engine

import (
	"bytes"
	"strings"
	"testing"

	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
)

const nativeUnwantedOffer = "v=0\r\no=remote 1 1 IN IP4 192.0.2.2\r\ns=-\r\nc=IN IP4 192.0.2.2\r\nt=0 0\r\na=group:BUNDLE audio video\r\na=ice-pwd:synthetic\r\nm=audio 49170 RTP/AVP 0 8\r\na=mid:audio\r\nm=video 51372 RTP/AVP 31\r\na=mid:video\r\n"

func TestNativeUnwantedLateOfferACK(t *testing.T) {
	for _, provisional := range []bool{false, true} {
		t.Run(map[bool]string{false: "new-fork", true: "ringing-fork"}[provisional], func(t *testing.T) {
			origin, err := portable.BuildInitialRequest(portable.InitialRequest{Method: sip.INVITE, URI: "sip:phone@example.test", From: "<sip:controller@example.test>", CallID: "unwanted-offer", LocalTag: "local", Branch: "z9hG4bK-invite", CSeq: 1, Advertise: "192.0.2.1:5061", Contact: "<sip:controller@192.0.2.1:5061;transport=tls>", Transport: "tls"})
			if err != nil {
				t.Fatal(err)
			}
			var previous *portable.Dialog
			branchCalls := 0
			branch := func() string { branchCalls++; return "z9hG4bK-reject-answer" }
			if provisional {
				ringing, x := portable.BuildResponseWithHeaders(origin, 180, "Ringing", nil, "remote", "sip:phone@example.test", nil)
				if x != nil {
					t.Fatal(x)
				}
				candidate, x := nativeReceiveInitialInviteDialog(origin, ringing, nil, false, 1000, branch)
				if x != nil {
					t.Fatal(x)
				}
				previous = &candidate.Dialog
			}
			final, err := portable.BuildResponseWithHeaders(origin, 200, "OK", []byte(nativeUnwantedOffer), "remote", "sip:phone@example.test", []portable.Header{{Name: "Content-Type", Value: "application/sdp"}})
			if err != nil {
				t.Fatal(err)
			}
			candidate, err := nativeReceiveInitialInviteDialog(origin, final, previous, true, 1100, branch)
			if err != nil {
				t.Fatal(err)
			}
			if branchCalls != 1 || candidate.Dialog.LocalInvite == nil || candidate.Dialog.LocalInvite.ACK == nil || candidate.Dialog.Offer != portable.NoOffer {
				t.Fatal("unwanted late offer has no rejecting ACK", branchCalls, candidate.Dialog.Offer)
			}
			ack := *candidate.Dialog.LocalInvite.ACK
			facts, err := ack.Facts()
			if err != nil || facts.Method != sip.ACK || facts.CSeq != 1 || facts.ToTag != "remote" || !facts.SDP {
				t.Fatal("rejecting ACK identity or content type", facts, err)
			}
			body, err := ack.Body()
			if err != nil || !bytes.Contains(body, []byte("m=audio 0 RTP/AVP 0\r\n")) || !bytes.Contains(body, []byte("m=video 0 RTP/AVP 31\r\n")) || bytes.Contains(body, []byte("o=remote")) || bytes.Contains(body, []byte("a=group:")) || bytes.Contains(body, []byte("ice-pwd")) {
				t.Fatal("ACK did not reject the offered media without copying offer attributes", err)
			}
			needBYE := false
			for _, effect := range candidate.ACKEffects {
				needBYE = needBYE || effect.Kind == portable.NeedBYE
			}
			if !needBYE {
				t.Fatal("rejecting ACK lost the cleanup obligation")
			}
			replay, err := nativeReceiveInitialInviteDialog(origin, final, &candidate.Dialog, true, 1200, branch)
			if err != nil || branchCalls != 1 || len(replay.ACKEffects) != 1 || !replay.ACKEffects[0].Replay || replay.ACKEffects[0].Message == nil || !replay.ACKEffects[0].Message.EqualWire(ack) {
				t.Fatal("duplicate 2xx changed the exact ACK or generated another BYE", err)
			}
			if strings.Count(string(body), "m=") != 2 {
				t.Fatal("rejected answer changed media count")
			}
		})
	}
}

func TestNativeRejectedSDPProjection(t *testing.T) {
	for _, tc := range []struct {
		name, offer, expected string
		mediaCount            int
	}{
		{name: "port-count", offer: "t=123 456\r\nm=audio 5000/2 RTP/AVP 8 0\r\n", expected: "t=123 456\r\na=inactive\r\nm=audio 0 RTP/AVP 8\r\n", mediaCount: 1},
		{name: "data-channel", offer: "t=0 0\nm=application 9 UDP/DTLS/SCTP webrtc-datachannel\n", expected: "m=application 0 UDP/DTLS/SCTP webrtc-datachannel\r\n", mediaCount: 1},
		{name: "empty-session", offer: "v=0\r\nt=0 0\r\n", expected: "t=0 0\r\na=inactive\r\n", mediaCount: 0},
		{name: "multiple-times", offer: "t=1 2\r\nt=3 4\r\nm=audio 5000 RTP/AVP 0\r\n", expected: "t=1 2\r\nt=3 4\r\na=inactive\r\n", mediaCount: 1},
		{name: "malformed-media", offer: "t=0 0\r\nm=audio\r\n", expected: "m=audio 0 RTP/AVP 0\r\n", mediaCount: 1},
		{name: "malformed-time", offer: "t=invalid\r\nm=video 10 RTP/AVP 31\r\n", expected: "t=0 0\r\na=inactive\r\nm=audio 0 RTP/AVP 0\r\n", mediaCount: 1},
		{name: "unrecognizable", offer: "not SDP", expected: "m=audio 0 RTP/AVP 0\r\n", mediaCount: 1},
		{name: "control-byte", offer: "t=0 0\r\nm=audio 5000 RTP/\x00AVP 0\r\n", expected: "m=audio 0 RTP/AVP 0\r\n", mediaCount: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			answer := string(nativeRejectedSDP([]byte(tc.offer), "z9hG4bK-reject"))
			if !strings.Contains(answer, tc.expected) || strings.Count(answer, "\r\nm=") != tc.mediaCount {
				t.Fatal("invalid all-rejected answer", answer)
			}
			for line := range strings.Lines(answer) {
				if strings.HasPrefix(line, "m=") && strings.Fields(line)[1] != "0" {
					t.Fatal("answer enabled media")
				}
			}
		})
	}
}
