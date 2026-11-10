package portable

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/emiago/sipgo/sip"
)

func origin(t *testing.T, method sip.RequestMethod, transport string) Message {
	t.Helper()
	m, err := BuildInitialRequest(InitialRequest{Method: method, URI: "sip:phone@example.test;user=phone?Subject=test", From: "<sip:controller@example.test>", CallID: "portable-call", LocalTag: "local-tag", Branch: "z9hG4bK-original", CSeq: 121787510, Advertise: "192.0.2.1:5061", Contact: "<sip:controller@192.0.2.1:5061;transport=" + transport + ">", Transport: transport})
	if err != nil {
		t.Fatal(err)
	}
	m.Source = "192.0.2.1:5061"
	m.Destination = "192.0.2.2:5061"
	return m
}
func reply(t *testing.T, m Message, status int, remoteTag string) Message {
	t.Helper()
	v, err := BuildResponseWithHeaders(m, status, "test", nil, remoteTag, "<sip:phone@192.0.2.2:5061>", nil)
	if err != nil {
		t.Fatal(err)
	}
	return v
}
func hydrated[T any](t *testing.T, s T) T {
	t.Helper()
	b, e := json.Marshal(s)
	if e != nil {
		t.Fatal(e)
	}
	var v T
	if e = json.Unmarshal(b, &v); e != nil {
		t.Fatal(e)
	}
	return v
}
func step(t *testing.T, s Transaction, in Input) (Transaction, []Effect) {
	t.Helper()
	before, _ := json.Marshal(s)
	next, effects, err := StepTransaction(hydrated(t, s), in)
	if err != nil {
		t.Fatal(err)
	}
	after, _ := json.Marshal(s)
	if !bytes.Equal(before, after) {
		t.Fatal("input state mutated")
	}
	return hydrated(t, next), effects
}
func count(e []Effect, k EffectKind) int {
	n := 0
	for _, v := range e {
		if v.Kind == k {
			n++
		}
	}
	return n
}
func sent(e []Effect) []Message {
	var a []Message
	for _, v := range e {
		if v.Kind == Send && v.Message != nil {
			a = append(a, *v.Message)
		}
	}
	return a
}
func tx(t *testing.T, f Family, m Message) Transaction {
	t.Helper()
	s, e := NewTransaction(f, m, DefaultTimers(), "remote-tag")
	if e != nil {
		t.Fatal(e)
	}
	return s
}

func TestPortableInviteClientHydratesEveryTransition(t *testing.T) {
	m := origin(t, sip.INVITE, "udp")
	s := tx(t, InviteClient, m)
	s, e := step(t, s, Input{Kind: Start, Now: 1000})
	if s.Phase != Calling || s.Timers[TimerA].At != 1500 || s.Timers[TimerB].At != 33000 || count(e, Send) != 1 {
		t.Fatalf("start: %+v", s)
	}
	s, e = step(t, s, Input{Kind: TimerFired, Timer: TimerA, Generation: s.Timers[TimerA].Generation, Now: 1500})
	if count(e, Send) != 1 || s.Timers[TimerA].At != 2500 {
		t.Fatal("A retransmission lost")
	}
	s, _ = step(t, s, Input{Kind: ReceiveResponse, Message: reply(t, m, 180, "remote-tag"), Now: 1700})
	if s.Phase != Proceeding || s.Timers[TimerA].Active || s.Timers[TimerB].Active {
		t.Fatal("provisional did not end calling timers")
	}
	final := reply(t, m, 200, "remote-tag")
	s, _ = step(t, s, Input{Kind: ReceiveResponse, Message: final, Now: 1800})
	if s.Phase != Accepted || s.Timers[TimerM].At != 33800 {
		t.Fatal("accepted retention missing")
	}
	s, e = step(t, s, Input{Kind: ReceiveResponse, Message: final, Now: 1900})
	if count(e, ReportResponse) != 1 || !e[0].Replay {
		t.Fatal("successful retransmission not reported")
	}
	s, _ = step(t, s, Input{Kind: TransportFailure, Error: "socket", Now: 2000})
	if s.Phase != Accepted {
		t.Fatal("accepted state lost after delivery error")
	}
	s, _ = step(t, s, Input{Kind: TimerFired, Timer: TimerM, Generation: s.Timers[TimerM].Generation, Now: 33800})
	if s.Phase != Terminated {
		t.Fatal("M did not retire")
	}
}

func TestPortableInviteRejectedACKIsStable(t *testing.T) {
	m := origin(t, sip.INVITE, "udp")
	s, _ := step(t, tx(t, InviteClient, m), Input{Kind: Start, Now: 1000})
	final := reply(t, m, 486, "remote-tag")
	s, e := step(t, s, Input{Kind: ReceiveResponse, Message: final, Now: 1100})
	if s.Phase != Completed || s.ACK == nil || count(e, Send) != 1 {
		t.Fatal("non-2xx ACK missing")
	}
	first := bytes.Clone(s.ACK.Wire)
	s, e = step(t, s, Input{Kind: ReceiveResponse, Message: final, Now: 1101})
	if len(sent(e)) != 1 || !bytes.Equal(sent(e)[0].Wire, first) {
		t.Fatal("ACK replay allocated new identity")
	}
	_, effects, err := StepTransaction(s, Input{Kind: ReceiveResponse, Message: mutate(t, final, func(v sip.Message) { v.CSeq().SeqNo++ }), Now: 1200})
	if err == nil || len(effects) != 0 {
		t.Fatal("wrong CSeq matched")
	}
	if s.Timers[TimerD].At != 33100 {
		t.Fatal("D deadline reset")
	}
}

func TestPortableNonInviteFamiliesAndTransportProfiles(t *testing.T) {
	for _, network := range []string{"udp", "tcp", "tls"} {
		t.Run(network, func(t *testing.T) {
			m := origin(t, sip.OPTIONS, network)
			s, e := step(t, tx(t, NonInviteClient, m), Input{Kind: Start, Now: 1000})
			if count(e, Send) != 1 || !s.Timers[TimerF].Active {
				t.Fatal("non-INVITE start")
			}
			if s.Timers[TimerE].Active != (network == "udp") {
				t.Fatal("transport class lost")
			}
			if network == "udp" {
				s, _ = step(t, s, Input{Kind: ReceiveResponse, Message: reply(t, m, 100, ""), Now: 1100})
				s, e = step(t, s, Input{Kind: TimerFired, Timer: TimerE, Generation: s.Timers[TimerE].Generation, Now: 1500})
				if count(e, Send) != 1 || s.Timers[TimerE].At != 5500 || s.Timers[TimerF].At != 33000 {
					t.Fatal("E/F reset incorrectly")
				}
			}
			s, _ = step(t, s, Input{Kind: ReceiveResponse, Message: reply(t, m, 200, "remote-tag"), Now: 2000})
			if network == "udp" {
				if s.Phase != Completed || s.Timers[TimerK].At != 7000 {
					t.Fatal("K missing")
				}
			} else if s.Phase != Terminated {
				t.Fatal("reliable NICT retention")
			}
			server, e := step(t, tx(t, NonInviteServer, m), Input{Kind: Start, Now: 1000})
			if count(e, PresentRequest) != 1 {
				t.Fatal("server presentation")
			}
			server, e = step(t, server, Input{Kind: SendResponse, Message: reply(t, m, 200, "remote-tag"), Now: 1200})
			if count(e, Send) != 1 {
				t.Fatal("server final")
			}
			if network == "udp" {
				if server.Phase != Completed || server.Timers[TimerJ].At != 33200 {
					t.Fatal("J missing")
				}
				server, e = step(t, server, Input{Kind: ReceiveRequest, Message: m, Now: 1300})
				if count(e, Send) != 1 || count(e, PresentRequest) != 0 {
					t.Fatal("server replay reran policy")
				}
			} else if server.Phase != Terminated {
				t.Fatal("reliable NIST retention")
			}
		})
	}
}

func TestPortableServerTryingFinalAndACK(t *testing.T) {
	m := origin(t, sip.INVITE, "udp")
	s, e := step(t, tx(t, InviteServer, m), Input{Kind: Start, Now: 1000})
	if count(e, PresentRequest) != 1 {
		t.Fatal("presentation missing")
	}
	s, e = step(t, s, Input{Kind: TimerFired, Timer: TimerTrying, Generation: s.Timers[TimerTrying].Generation, Now: 1200})
	if len(sent(e)) != 1 {
		t.Fatal("Trying missing")
	}
	parsed, _ := response(sent(e)[0])
	if parsed.StatusCode != 100 || tag(parsed.To().Params) != "" {
		t.Fatal("100 identity")
	}
	final := reply(t, m, 486, "remote-tag")
	s, _ = step(t, s, Input{Kind: SendResponse, Message: final, Now: 1300})
	if s.Timers[TimerH].At != 33300 {
		t.Fatal("H missing")
	}
	ack, err := BuildNon2xxACK(m, final)
	if err != nil {
		t.Fatal(err)
	}
	s, e = step(t, s, Input{Kind: ReceiveRequest, Message: ack, Now: 1400})
	if s.Phase != Confirmed || s.Timers[TimerG].Active || count(e, Acknowledged) != 1 {
		t.Fatal("ACK not confirmed")
	}
	s, _ = step(t, s, Input{Kind: TimerFired, Timer: TimerI, Generation: s.Timers[TimerI].Generation, Now: 6400})
	if s.Phase != Terminated {
		t.Fatal("I not retired")
	}
}

func TestPortableCancelIsItsOwnRetainedTransaction(t *testing.T) {
	m := origin(t, sip.INVITE, "udp")
	invite, _ := step(t, tx(t, InviteServer, m), Input{Kind: Start, Now: 1000})
	cancel, err := BuildCancel(m)
	if err != nil {
		t.Fatal(err)
	}
	i, c, e, err := StepServerCancel(invite, nil, Input{Kind: ReceiveRequest, Message: cancel, Now: 1100})
	if err != nil {
		t.Fatal(err)
	}
	if i.Phase != Completed || c.Phase != Completed || !i.Cancelled || c.Key == i.Key {
		t.Fatal("cancel state not independent")
	}
	writes := sent(e)
	if len(writes) != 2 {
		t.Fatalf("want CANCEL200 then INVITE487, %+v", e)
	}
	first, _ := response(writes[0])
	second, _ := response(writes[1])
	if first.CSeq().MethodName != sip.CANCEL || first.StatusCode != 200 || second.StatusCode != 487 || tag(first.To().Params) != "remote-tag" || tag(second.To().Params) != "remote-tag" {
		t.Fatal("cancel response identity/order")
	}
	_, replay := step(t, c, Input{Kind: ReceiveRequest, Message: cancel, Now: 7000})
	if len(sent(replay)) != 1 {
		t.Fatal("CANCEL replay lost independently of INVITE")
	}
	_, _, _, err = StepServerCancel(invite, nil, Input{Kind: ReceiveRequest, Message: mutate(t, cancel, func(v sip.Message) { *v.CallID() = "other" }), Now: 1100})
	if err == nil {
		t.Fatal("borrowed CANCEL branch accepted")
	}
	accepted, _ := step(t, invite, Input{Kind: SendResponse, Message: reply(t, m, 200, "remote-tag"), Now: 1100})
	accepted, _, e, err = StepServerCancel(accepted, nil, Input{Kind: ReceiveRequest, Message: cancel, Now: 1101})
	if err != nil || accepted.Phase != Accepted || len(sent(e)) != 1 {
		t.Fatal("CANCEL replaced already-selected success")
	}
}

func TestPortableClientCancellationWaitsForProvisional(t *testing.T) {
	m := origin(t, sip.INVITE, "tls")
	s, _ := step(t, tx(t, InviteClient, m), Input{Kind: Start, Now: 1})
	s, e := step(t, s, Input{Kind: RequestCancel, Now: 2})
	if !s.Cancelled || count(e, StartTransaction) != 0 {
		t.Fatal("premature CANCEL")
	}
	s, e = step(t, s, Input{Kind: ReceiveResponse, Message: reply(t, m, 100, ""), Now: 3})
	if !s.CancelSent || count(e, StartTransaction) != 1 {
		t.Fatal("CANCEL not created")
	}
	s, e = step(t, s, Input{Kind: RequestCancel, Now: 4})
	if count(e, StartTransaction) != 0 {
		t.Fatal("duplicate CANCEL")
	}
}

func TestPortableTimerGenerationAndInputImmutability(t *testing.T) {
	m := origin(t, sip.OPTIONS, "udp")
	s, _ := step(t, tx(t, NonInviteClient, m), Input{Kind: Start, Now: 1000})
	s, e := step(t, s, Input{Kind: TimerFired, Timer: TimerE, Generation: 99, Now: 1500})
	if len(e) != 0 || s.Timers[TimerE].At != 1500 {
		t.Fatal("stale timer fired")
	}
	s, e = step(t, s, Input{Kind: TimerFired, Timer: TimerE, Generation: s.Timers[TimerE].Generation, Now: 1499})
	if len(e) != 0 {
		t.Fatal("early timer fired")
	}
	_, effects, err := StepTransaction(s, Input{Kind: ReceiveResponse, Message: mutate(t, reply(t, m, 200, "remote-tag"), func(v sip.Message) { v.From().Params.Add("tag", "attacker") }), Now: 1500})
	if err == nil || len(effects) != 0 {
		t.Fatal("mismatched identity produced effects")
	}
}

func mutate(t *testing.T, m Message, fn func(sip.Message)) Message {
	t.Helper()
	v, e := m.Parse()
	if e != nil {
		t.Fatal(e)
	}
	fn(v)
	m.Wire = []byte(v.String())
	return m
}

func TestPortableRejectsCorruptRestoredTransaction(t *testing.T) {
	s, _ := step(t, tx(t, InviteClient, origin(t, sip.INVITE, "tls")), Input{Kind: Start, Now: 1})
	for name, change := range map[string]func(*Transaction){"family": func(v *Transaction) { v.Family = "future" }, "phase": func(v *Transaction) { v.Phase = "future" }, "key": func(v *Transaction) { v.Key = "borrowed" }, "transport": func(v *Transaction) { v.Reliable = false }, "version": func(v *Transaction) { v.Version++ }} {
		t.Run(name, func(t *testing.T) {
			bad := s.Clone()
			change(&bad)
			if _, e, err := StepTransaction(hydrated(t, bad), Input{Kind: TransportFailure, Now: 2}); err == nil || len(e) != 0 {
				t.Fatal("invalid state emitted effects")
			}
		})
	}
}
