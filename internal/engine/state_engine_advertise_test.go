package engine

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
	pb "sipbridge/internal/controlpb"
)

func advertiseFixture(t *testing.T, transport string, method sip.RequestMethod, plain string) (*stateReduction, *stateContext, *stateTransaction) {
	t.Helper()
	r, c, _ := forkFixture(t)
	r.engine.cfg.AdvertiseAddress = "public.example.test:5061"
	r.engine.cfg.PlainAdvertiseAddress = plain
	r.engine.cfg.ContactURI = "sip:bridge@public.example.test:5061;Transport=tls;transport=tls;ob;profile=fixture"
	c.Snapshot.Transport = transport
	c.Snapshot.Kind = pb.ContextSnapshot_UAS
	c.Snapshot.LocalUri, c.Snapshot.RemoteUri = "sip:service@example.test", "sip:phone@example.test"
	c.Snapshot.LocalTag, c.Snapshot.RemoteTag = "local", "remote"
	c.Flow.Transport = transport
	c.Dialog, c.Dialogs = "", make(map[string]portable.Dialog)
	c.Transactions, c.Requests = make(map[string]*stateTransaction), make(map[string]string)
	c.OwnerValidated, c.OwnerDirectoryEpoch = true, "directory"
	headers := []portable.Header{}
	body := []byte("v=0\r\n")
	if method == sip.SUBSCRIBE {
		headers = append(headers, portable.Header{Name: "Event", Value: "dialog"}, portable.Header{Name: "Expires", Value: "3600"})
		body = nil
	}
	m, err := portable.BuildInitialRequest(portable.InitialRequest{Method: method, URI: c.Snapshot.LocalUri, From: "<" + c.Snapshot.RemoteUri + ">", CallID: "fork-call", LocalTag: "remote", Branch: "z9hG4bK-advertisement", CSeq: 1, Advertise: "phone.example.test:6070", Contact: "sip:phone@phone.example.test:6070;transport=" + transport, Transport: transport, Headers: headers, Body: body})
	if err != nil {
		t.Fatal(err)
	}
	c.Origin = m
	family := portable.InviteServer
	if method != sip.INVITE {
		family = portable.NonInviteServer
	}
	native, err := portable.NewTransaction(family, m, portable.DefaultTimers(), c.Tag)
	if err != nil {
		t.Fatal(err)
	}
	native, _, err = portable.StepTransaction(native, portable.Input{Kind: portable.Start, Now: r.now})
	if err != nil {
		t.Fatal(err)
	}
	tx := &stateTransaction{Native: native, Flow: c.Flow, Application: c.Snapshot.Application, RequestID: "request-advertise"}
	// This fixture constructs the original admitted server procedure directly.
	// Persist the same exact initial-flow evidence real ingest commits.
	r.deadline = r.now + 2000
	if _, err = r.admitRequestFlow(tx, c.Flow, m.Wire); err != nil {
		t.Fatal(err)
	}
	r.deadline = 0
	c.Transactions[native.Key], c.Requests[tx.RequestID] = tx, native.Key
	return r, c, tx
}

func advertiseCommand(t *testing.T, r *stateReduction, c *stateContext, cmd *pb.Command) {
	t.Helper()
	cmd.Context = c.Snapshot.Context
	r.in = stateEngineInput{Kind: "command", Context: cmd.Context, Command: cmd, Application: c.Snapshot.Application, Operation: fmt.Sprintf("advertise-%d", cmd.Sequence)}
	if err := r.command(); err != nil {
		t.Fatal(err)
	}
}

func advertiseWire(t *testing.T, r *stateReduction, method sip.RequestMethod, response bool) sip.Message {
	t.Helper()
	for i := len(r.payload.Effects) - 1; i >= 0; i-- {
		var body stateEngineEffect
		if err := json.Unmarshal(r.payload.Effects[i].Payload, &body); err != nil {
			t.Fatal(err)
		}
		if len(body.Wire) == 0 {
			continue
		}
		m, err := sip.ParseMessage(body.Wire)
		if err != nil {
			t.Fatal(err)
		}
		_, isResponse := m.(*sip.Response)
		if isResponse == response && m.CSeq().MethodName == method {
			return m
		}
	}
	t.Fatalf("wire missing: %s response=%v", method, response)
	return nil
}

func assertAdvertiseContact(t *testing.T, m sip.Message, transport, host string, port int) {
	t.Helper()
	var contact *sip.ContactHeader
	switch message := m.(type) {
	case *sip.Request:
		contact = message.Contact()
	case *sip.Response:
		contact = message.Contact()
	}
	if contact == nil {
		t.Fatal("Contact missing")
	}
	uri := contact.Address
	actualTransport, _ := uri.UriParams.Get("transport")
	profile, _ := uri.UriParams.Get("profile")
	transports := 0
	for _, param := range uri.UriParams {
		if strings.EqualFold(param.K, "transport") {
			transports++
		}
	}
	if transports != 1 {
		t.Fatal("Contact has conflicting transport parameters")
	}
	if uri.Scheme != "sip" || uri.User != "bridge" || uri.Host != host || uri.Port != port || actualTransport != transport || profile != "fixture" || !uri.UriParams.Has("ob") {
		t.Fatalf("wrong public Contact: %s", contact.Value())
	}
}

func assertAdvertiseVia(t *testing.T, m sip.Message, transport, host string, port int) {
	t.Helper()
	via := m.Via()
	if via == nil || via.Transport != strings.ToUpper(transport) || via.Host != host || via.Port != port {
		t.Fatalf("wrong Via: %+v", via)
	}
}

func TestStateUASAdvertisementMatchesActualTransportThroughBye(t *testing.T) {
	for _, transport := range []string{"udp", "tcp", "tls"} {
		for _, plain := range []string{"", "plain.example.test:6090"} {
			t.Run(transport+"/"+plain, func(t *testing.T) {
				r, c, tx := advertiseFixture(t, transport, sip.INVITE, plain)
				host, port := "public.example.test", 5061
				if transport != "tls" {
					port = 5060
					if plain != "" {
						host, port = "plain.example.test", 6090
					}
				}
				advertiseCommand(t, r, c, &pb.Command{Sequence: 1, Action: &pb.Command_Respond{Respond: &pb.Respond{RequestId: tx.RequestID, Status: 200, Body: []byte("v=0\r\n")}}})
				response := advertiseWire(t, r, sip.INVITE, true)
				assertAdvertiseContact(t, response, transport, host, port)
				// Admit the exact remote ACK before requesting native local BYE.
				parsed, err := tx.Native.Origin.Parse()
				if err != nil {
					t.Fatal(err)
				}
				ack := parsed.(*sip.Request)
				ack.Method, ack.CSeq().MethodName = sip.ACK, sip.ACK
				ack.To().Params.Add("tag", c.Tag)
				ack.SetBody(nil)
				d, _, err := portable.StepDialog(c.Dialogs[c.Dialog], portable.DialogInput{Kind: portable.DialogReceiveRequest, Now: r.now, Message: portable.Message{Wire: []byte(ack.String()), Transport: transport}})
				if err != nil {
					t.Fatal(err)
				}
				c.Dialogs[c.Dialog] = d
				advertiseCommand(t, r, c, &pb.Command{Sequence: 2, Action: &pb.Command_DialogRequest{DialogRequest: &pb.DialogRequest{Method: "BYE"}}})
				bye := advertiseWire(t, r, sip.BYE, false)
				assertAdvertiseVia(t, bye, transport, host, port)
				request := bye.(*sip.Request)
				if request.Recipient.Host != "phone.example.test" || request.Recipient.Port != 6070 {
					t.Fatalf("wrong BYE target: %s", request.Recipient.String())
				}
				if actual, _ := request.Recipient.UriParams.Get("transport"); actual != transport {
					t.Fatal("BYE target transport changed")
				}
			})
		}
	}
}

func TestStateSubscriptionAdvertisementMatchesActualTransport(t *testing.T) {
	for _, transport := range []string{"udp", "tcp", "tls"} {
		t.Run(transport, func(t *testing.T) {
			r, c, tx := advertiseFixture(t, transport, sip.SUBSCRIBE, "plain.example.test:6090")
			host, port := "public.example.test", 5061
			if transport != "tls" {
				host, port = "plain.example.test", 6090
			}
			advertiseCommand(t, r, c, &pb.Command{Sequence: 1, Action: &pb.Command_Respond{Respond: &pb.Respond{RequestId: tx.RequestID, Status: 202, Headers: []*pb.Header{{Name: "Expires", Value: "3600"}}}}})
			assertAdvertiseContact(t, advertiseWire(t, r, sip.SUBSCRIBE, true), transport, host, port)
			advertiseCommand(t, r, c, &pb.Command{Sequence: 2, Action: &pb.Command_DialogRequest{DialogRequest: &pb.DialogRequest{Method: "NOTIFY", Body: []byte("<presence/>"), Headers: []*pb.Header{{Name: "Content-Type", Value: "application/pidf+xml"}}}}})
			notify := advertiseWire(t, r, sip.NOTIFY, false)
			assertAdvertiseContact(t, notify, transport, host, port)
			assertAdvertiseVia(t, notify, transport, host, port)
		})
	}
}

func TestStateAdvertisementIPv6AndValidation(t *testing.T) {
	e := &StateEngine{cfg: Config{AdvertiseAddress: "[2001:db8::1]:5061", PlainAdvertiseAddress: "[2001:db8::2]:6090", ContactURI: "sips:bridge@[2001:db8::1]:5061;ob;profile=fixture"}}
	for _, transport := range []string{"udp", "tcp", "tls"} {
		contact, sentBy, err := e.advertisement(transport)
		if err != nil {
			t.Fatal(err)
		}
		expect := "[2001:db8::2]:6090"
		if transport == "tls" {
			expect = "[2001:db8::1]:5061"
		}
		if sentBy != expect || !strings.Contains(contact, "@"+expect+";") {
			t.Fatalf("unbracketed IPv6: %s %s", contact, sentBy)
		}
	}
	for _, endpoint := range []string{"public.example.test:0", "public.example.test:65536", ":5060", "invalid"} {
		e.cfg.PlainAdvertiseAddress = endpoint
		if _, _, err := e.advertisement("udp"); err == nil {
			t.Fatal("accepted invalid endpoint " + strconv.Quote(endpoint))
		}
	}
}

func TestStateUACAdvertisementUsesSameTransportSelection(t *testing.T) {
	for _, transport := range []string{"udp", "tcp", "tls"} {
		t.Run(transport, func(t *testing.T) {
			r, c, _ := advertiseFixture(t, transport, sip.INVITE, "plain.example.test:6090")
			engine, _ := historicalStateTestEngine(t)
			r.engine.store = engine.store
			delete(r.group.Contexts, c.Snapshot.Context.Id)
			cmd := &pb.Command{Context: c.Snapshot.Context, Sequence: 1, Action: &pb.Command_OpenUac{OpenUac: &pb.OpenUac{Uri: "sip:phone@phone.example.test:6070;transport=" + transport, BindingId: "registration", Transport: transport, Headers: []*pb.Header{{Name: "From", Value: "<sip:service@example.test>"}}, Body: []byte("v=0\r\n")}}}
			r.in = stateEngineInput{Kind: "command", Context: cmd.Context, Application: c.Snapshot.Application, Operation: "open-advertise", Command: cmd}
			if err := r.openUAC(); err != nil {
				t.Fatal(err)
			}
			host, port := "plain.example.test", 6090
			if transport == "tls" {
				host, port = "public.example.test", 5061
			}
			request := advertiseWire(t, r, sip.INVITE, false)
			assertAdvertiseContact(t, request, transport, host, port)
			assertAdvertiseVia(t, request, transport, host, port)
		})
	}
}

func TestStateUASFailureAdvertisementUsesActualTransport(t *testing.T) {
	for _, transport := range []string{"udp", "tcp", "tls"} {
		t.Run(transport, func(t *testing.T) {
			r, c, tx := advertiseFixture(t, transport, sip.INVITE, "plain.example.test:6090")
			advertiseCommand(t, r, c, &pb.Command{Sequence: 1, Action: &pb.Command_Respond{Respond: &pb.Respond{RequestId: tx.RequestID, Status: 503}}})
			host, port := "plain.example.test", 6090
			if transport == "tls" {
				host, port = "public.example.test", 5061
			}
			assertAdvertiseContact(t, advertiseWire(t, r, sip.INVITE, true), transport, host, port)
		})
	}
}

func TestStateUASIPv6AdvertisementRetainsCanonicalSentBy(t *testing.T) {
	for _, transport := range []string{"udp", "tcp", "tls"} {
		t.Run(transport, func(t *testing.T) {
			r, c, tx := advertiseFixture(t, transport, sip.INVITE, "[2001:db8::2]:6090")
			r.engine.cfg.AdvertiseAddress = "[2001:db8::1]:5061"
			r.engine.cfg.ContactURI = "sip:bridge@[2001:db8::1]:5061;ob;profile=fixture"
			advertiseCommand(t, r, c, &pb.Command{Sequence: 1, Action: &pb.Command_Respond{Respond: &pb.Respond{RequestId: tx.RequestID, Status: 200, Body: []byte("v=0\r\n")}}})
			_, expected, err := r.engine.advertisement(transport)
			if err != nil {
				t.Fatal(err)
			}
			if c.Dialogs[c.Dialog].SentBy != expected {
				t.Fatal("UAS inferred an unbracketed IPv6 sent-by")
			}
			// ACK arbitration is independent of advertisement; use the retained
			// native value to confirm a generated BYE accepts this sent-by.
			d := c.Dialogs[c.Dialog]
			request, err := portable.BuildDialogRequest(d, sip.BYE, d.LocalCSeq+1, nil, "z9hG4bK-ipv6-bye")
			if err != nil {
				t.Fatal(err)
			}
			parsed, err := request.Parse()
			if err != nil {
				t.Fatal(err)
			}
			host, port := "2001:db8::2", 6090
			if transport == "tls" {
				host, port = "2001:db8::1", 5061
			}
			assertAdvertiseVia(t, parsed, transport, host, port)
		})
	}
}
