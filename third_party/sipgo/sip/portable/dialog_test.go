package portable

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/emiago/sipgo/sip"
)

func dialogStep(t *testing.T, s Dialog, in DialogInput) (Dialog, []Effect) {
	t.Helper()
	before, _ := json.Marshal(s)
	next, e, err := StepDialog(hydrated(t, s), in)
	if err != nil {
		t.Fatal(err)
	}
	after, _ := json.Marshal(s)
	if !bytes.Equal(before, after) {
		t.Fatal("dialog input mutated")
	}
	return hydrated(t, next), e
}
func clientDialog(t *testing.T) Dialog {
	t.Helper()
	o := origin(t, sip.INVITE, "tls")
	d, e := NewDialog(UAC, o, reply(t, o, 200, "remote-tag"), DefaultTimers())
	if e != nil {
		t.Fatal(e)
	}
	return d
}
func serverDialog(t *testing.T) Dialog {
	t.Helper()
	o := origin(t, sip.INVITE, "tls")
	d, e := NewDialog(UAS, o, reply(t, o, 180, "remote-tag"), DefaultTimers())
	if e != nil {
		t.Fatal(e)
	}
	return d
}
func peerRequest(t *testing.T, d Dialog, method sip.RequestMethod, seq uint32, branch string) Message {
	t.Helper()
	reverse := Dialog{CallID: d.CallID, LocalTag: d.RemoteTag, RemoteTag: d.LocalTag, LocalURI: d.RemoteURI, RemoteURI: d.LocalURI, Target: d.Contact, Contact: d.Target, SentBy: "192.0.2.2:5061", Transport: d.Transport}
	m, e := BuildDialogRequest(reverse, method, seq, nil, branch)
	if e != nil {
		t.Fatal(e)
	}
	return m
}

func TestPortableDialogIndependentSequencesAndACK(t *testing.T) {
	d := clientDialog(t)
	initial := d.LocalCSeq
	d, e := dialogStep(t, d, DialogInput{Kind: DialogLocalACK, Now: 1, Branch: "z9hG4bK-ack"})
	if d.LocalCSeq != initial || count(e, Send) != 1 || d.Phase != DialogConfirmed {
		t.Fatal("ACK consumed local sequence")
	}
	info := peerRequest(t, d, sip.INFO, 2, "z9hG4bK-info")
	d, _ = dialogStep(t, d, DialogInput{Kind: DialogReceiveRequest, Now: 2, Message: info})
	if d.RemoteCSeq != 2 || d.LocalCSeq != 121787510 {
		t.Fatal("remote small sequence rejected or local changed")
	}
	_, _, err := StepDialog(d, DialogInput{Kind: DialogReceiveRequest, Now: 3, Message: peerRequest(t, d, sip.INFO, 2, "z9hG4bK-new-branch")})
	if err == nil {
		t.Fatal("new equal-CSeq transaction admitted")
	}
	_, _, err = StepDialog(d, DialogInput{Kind: DialogReceiveRequest, Now: 3, Message: mutate(t, peerRequest(t, d, sip.INFO, 3, "z9hG4bK-wrong"), func(v sip.Message) { v.To().Params.Add("tag", "borrowed") })})
	if err == nil {
		t.Fatal("directional identity bypass")
	}
}

func TestPortableServerACKAfterLaterRemoteRequest(t *testing.T) {
	d := serverDialog(t)
	o := d.RemoteInvite.Request
	final := reply(t, o, 200, d.LocalTag)
	d, _ = dialogStep(t, d, DialogInput{Kind: DialogSendResponse, Now: 1000, Message: final})
	seq := d.RemoteInvite.Sequence
	d, _ = dialogStep(t, d, DialogInput{Kind: DialogReceiveRequest, Now: 1100, Message: peerRequest(t, d, sip.INFO, seq+1, "z9hG4bK-info-after-final")})
	ack := peerRequest(t, d, sip.ACK, seq, "z9hG4bK-dialog-ack")
	d, e := dialogStep(t, d, DialogInput{Kind: DialogReceiveRequest, Now: 1200, Message: ack})
	if d.Phase != DialogConfirmed || d.Timers[TimerACK].Active || count(e, Acknowledged) != 1 {
		t.Fatal("valid ACK failed after greater INFO CSeq")
	}
	d, e = dialogStep(t, d, DialogInput{Kind: DialogReceiveRequest, Now: 1201, Message: ack})
	if count(e, Acknowledged) != 0 || d.RemoteCSeq != seq+1 {
		t.Fatal("duplicate ACK advanced state")
	}
}

func TestPortableSuccessfulFinalHasFixedDeadlineAndCappedBackoff(t *testing.T) {
	d := serverDialog(t)
	d, _ = dialogStep(t, d, DialogInput{Kind: DialogSendResponse, Now: 1000, Message: reply(t, d.RemoteInvite.Request, 200, d.LocalTag)})
	deadline := d.Timers[TimerACK].At
	if deadline != 33000 {
		t.Fatal("ACK deadline")
	}
	for _, at := range []int64{1500, 2500, 4500, 8500} {
		var e []Effect
		d, e = dialogStep(t, d, DialogInput{Kind: DialogTimer, Now: at, Timer: Timer2xx, Generation: d.Timers[Timer2xx].Generation})
		if count(e, Respond) != 1 || d.Timers[TimerACK].At != deadline || d.Timers[Timer2xx].Interval > 4000 {
			t.Fatal("TU retransmit changed overall lifetime")
		}
	}
	d, e := dialogStep(t, d, DialogInput{Kind: DialogTimer, Now: 33000, Timer: TimerACK, Generation: d.Timers[TimerACK].Generation})
	if !d.Retiring || count(e, NeedBYE) != 1 {
		t.Fatal("timeout cleanup obligation")
	}
	d, e = dialogStep(t, d, DialogInput{Kind: DialogLocalRequest, Now: 33001, Method: sip.BYE, Branch: "z9hG4bK-timeout-bye"})
	if d.LocalBye == nil || count(e, StartTransaction) != 1 {
		t.Fatal("timeout BYE could not execute")
	}
}

func TestPortableLocalByeCannotReopenOnLateInviteCompletion(t *testing.T) {
	d := clientDialog(t)
	d, _ = dialogStep(t, d, DialogInput{Kind: DialogLocalACK, Now: 1, Branch: "z9hG4bK-first-ack"})
	d, _ = dialogStep(t, d, DialogInput{Kind: DialogLocalRequest, Now: 2, Method: sip.INVITE, Branch: "z9hG4bK-reinvite"})
	pending := d.LocalInvite.Request
	d, _ = dialogStep(t, d, DialogInput{Kind: DialogLocalRequest, Now: 3, Method: sip.BYE, Branch: "z9hG4bK-local-bye"})
	bye := *d.LocalBye
	d, _ = dialogStep(t, d, DialogInput{Kind: DialogReceiveResponse, Now: 4, Message: reply(t, pending, 200, d.RemoteTag)})
	d, _ = dialogStep(t, d, DialogInput{Kind: DialogLocalACK, Now: 5, Branch: "z9hG4bK-late-ack"})
	if !d.Retiring || d.Phase != DialogTerminating {
		t.Fatal("late ACK reopened terminating dialog")
	}
	_, _, err := StepDialog(d, DialogInput{Kind: DialogLocalRequest, Now: 6, Method: sip.INFO, Branch: "z9hG4bK-after-bye"})
	if err == nil {
		t.Fatal("new request admitted during BYE")
	}
	_, _, err = StepDialog(d, DialogInput{Kind: DialogReceiveRequest, Now: 6, Message: peerRequest(t, d, sip.INVITE, 2, "z9hG4bK-peer-resume")})
	if err == nil {
		t.Fatal("peer re-INVITE admitted during BYE")
	}
	d, _ = dialogStep(t, d, DialogInput{Kind: DialogReceiveResponse, Now: 7, Message: reply(t, bye, 500, d.RemoteTag)})
	if d.Phase != DialogEnded || !d.Retiring {
		t.Fatal("failed BYE reopened dialog")
	}
}

func TestPortableCancelledSuccessfulForkACKThenBye(t *testing.T) {
	d := clientDialog(t)
	d, e := dialogStep(t, d, DialogInput{Kind: DialogCancel, Now: 1})
	if count(e, NeedACK) != 1 || !d.Retiring {
		t.Fatal("cancelled success missing ACK")
	}
	d, e = dialogStep(t, d, DialogInput{Kind: DialogLocalACK, Now: 2, Branch: "z9hG4bK-cleanup-ack"})
	if len(e) != 2 || e[0].Kind != Send || e[1].Kind != NeedBYE {
		t.Fatal("cleanup ACK/BYE ordering")
	}
	d, e = dialogStep(t, d, DialogInput{Kind: DialogLocalRequest, Now: 3, Method: sip.BYE, Branch: "z9hG4bK-cleanup-bye"})
	if count(e, StartTransaction) != 1 || d.LocalBye == nil {
		t.Fatal("cleanup BYE not allocated")
	}
	_, _, err := StepDialog(d, DialogInput{Kind: DialogLocalRequest, Now: 4, Method: sip.BYE, Branch: "z9hG4bK-second-bye"})
	if err == nil {
		t.Fatal("second logical BYE allocated")
	}
}

func TestPortableForksRetainIndependentTargetsAndACK(t *testing.T) {
	o := origin(t, sip.INVITE, "tls")
	one := reply(t, o, 200, "branch-one")
	two := mutate(t, reply(t, o, 200, "branch-two"), func(v sip.Message) { v.(*sip.Response).Contact().Address.Host = "other.example.test" })
	a, e := NewDialog(UAC, o, one, DefaultTimers())
	if e != nil {
		t.Fatal(e)
	}
	b, e := NewDialog(UAC, o, two, DefaultTimers())
	if e != nil {
		t.Fatal(e)
	}
	a, _ = dialogStep(t, a, DialogInput{Kind: DialogLocalACK, Now: 1, Branch: "z9hG4bK-one-ack"})
	b, _ = dialogStep(t, b, DialogInput{Kind: DialogCancel, Now: 2})
	if a.Target == b.Target || a.RemoteTag == b.RemoteTag || a.Phase != DialogConfirmed || !b.Retiring {
		t.Fatal("fork state leaked across children")
	}
}

func TestPortableLooseAndStrictRouting(t *testing.T) {
	d := clientDialog(t)
	d.Routes = []string{"<sip:proxy.example.test:5080;lr>", "<sip:last.example.test:5081;lr>"}
	m, e := BuildDialogRequest(d, sip.BYE, d.LocalCSeq+1, nil, "z9hG4bK-loose")
	if e != nil {
		t.Fatal(e)
	}
	r, _ := request(m)
	if r.Recipient.String() != d.Target || m.Destination != "proxy.example.test:5080" || len(r.GetHeaders("Route")) != 2 {
		t.Fatal("loose routing")
	}
	d.Routes = []string{"<sip:strict.example.test:5080>", "<sip:next.example.test:5081;lr>"}
	m, e = BuildDialogRequest(d, sip.BYE, d.LocalCSeq+1, nil, "z9hG4bK-strict")
	if e != nil {
		t.Fatal(e)
	}
	r, _ = request(m)
	routes := r.GetHeaders("Route")
	if r.Recipient.Host != "strict.example.test" || len(routes) != 2 || routes[1].Value() != "<"+d.Target+">" {
		t.Fatal("strict routing lost original target")
	}
}

func TestPortableEarlyRouteSetIsRecomputedOnlyForInitialSuccess(t *testing.T) {
	o := origin(t, sip.INVITE, "tls")
	early := mutate(t, reply(t, o, 180, "remote-tag"), func(v sip.Message) { v.AppendHeader(sip.NewHeader("Record-Route", "<sip:early.example.test:5061;lr>")) })
	d, err := NewDialog(UAC, o, early, DefaultTimers())
	if err != nil {
		t.Fatal(err)
	}
	final := mutate(t, reply(t, o, 200, "remote-tag"), func(v sip.Message) { v.AppendHeader(sip.NewHeader("Record-Route", "<sip:final.example.test:5061;lr>")) })
	d, _ = dialogStep(t, d, DialogInput{Kind: DialogReceiveResponse, Now: 1, Message: final})
	if len(d.Routes) != 1 || d.Routes[0] != "<sip:final.example.test:5061;lr>" {
		t.Fatal("early route set retained after initial 2xx")
	}
	d, _ = dialogStep(t, d, DialogInput{Kind: DialogLocalACK, Now: 2, Branch: "z9hG4bK-final-ack"})
	d, _ = dialogStep(t, d, DialogInput{Kind: DialogLocalRequest, Now: 3, Method: sip.INVITE, Branch: "z9hG4bK-refresh"})
	refresh := mutate(t, reply(t, d.LocalInvite.Request, 200, "remote-tag"), func(v sip.Message) {
		v.AppendHeader(sip.NewHeader("Record-Route", "<sip:bad-refresh.example.test:5061;lr>"))
	})
	d, _ = dialogStep(t, d, DialogInput{Kind: DialogReceiveResponse, Now: 4, Message: refresh})
	if d.Routes[0] != "<sip:final.example.test:5061;lr>" {
		t.Fatal("target refresh changed fixed route set")
	}
}

func TestPortableOfferRequiresActualACKAnswerAndEarlyAnswerDoesNotBecomeNewOffer(t *testing.T) {
	o := origin(t, sip.INVITE, "tls")
	offer, err := BuildResponseWithHeaders(o, 200, "OK", []byte("v=0\r\n"), "remote-tag", "sip:phone@192.0.2.2:5061", nil)
	if err != nil {
		t.Fatal(err)
	}
	d, err := NewDialog(UAC, o, offer, DefaultTimers())
	if err != nil {
		t.Fatal(err)
	}
	if d.Offer != RemoteOfferInACK {
		t.Fatal("remote offer not retained")
	}
	if _, _, err = StepDialog(d, DialogInput{Kind: DialogLocalACK, Now: 1, Branch: "z9hG4bK-empty", Answer: true}); err == nil {
		t.Fatal("flag substituted for missing wire answer")
	}
	d, _ = dialogStep(t, d, DialogInput{Kind: DialogLocalACK, Now: 1, Branch: "z9hG4bK-answer", Body: []byte("v=0\r\n")})
	if d.Offer != NoOffer {
		t.Fatal("answer not consumed")
	}
	o = mutate(t, o, func(v sip.Message) {
		v.SetBody([]byte("v=0\r\n"))
		v.AppendHeader(sip.NewHeader("Content-Type", "application/sdp"))
	})
	early, err := BuildResponseWithHeaders(o, 183, "Progress", []byte("v=0\r\n"), "remote-tag", "sip:phone@192.0.2.2:5061", nil)
	if err != nil {
		t.Fatal(err)
	}
	d, err = NewDialog(UAS, o, early, DefaultTimers())
	if err != nil {
		t.Fatal(err)
	}
	d, _ = dialogStep(t, d, DialogInput{Kind: DialogSendResponse, Now: 1, Message: early})
	final, err := BuildResponseWithHeaders(o, 200, "OK", []byte("v=0\r\n"), "remote-tag", "sip:phone@192.0.2.2:5061", nil)
	if err != nil {
		t.Fatal(err)
	}
	d, _ = dialogStep(t, d, DialogInput{Kind: DialogSendResponse, Now: 2, Message: final})
	if d.Offer != NoOffer {
		t.Fatal("repeated early answer became a new offer")
	}
}

func TestPortableRejectsCorruptRestoredDialog(t *testing.T) {
	s := clientDialog(t)
	for name, change := range map[string]func(*Dialog){"phase": func(v *Dialog) { v.Phase = "future" }, "offer": func(v *Dialog) { v.Offer = "future" }, "version": func(v *Dialog) { v.Version++ }, "ended-unfenced": func(v *Dialog) { v.Phase = DialogEnded; v.Retiring = false }} {
		t.Run(name, func(t *testing.T) {
			bad := s.Clone()
			change(&bad)
			if _, e, err := StepDialog(hydrated(t, bad), DialogInput{Kind: DialogLocalACK, Now: 1, Branch: "z9hG4bK-no-restore-side-effect"}); err == nil || len(e) != 0 {
				t.Fatal("invalid restored dialog emitted effects")
			}
		})
	}
}
