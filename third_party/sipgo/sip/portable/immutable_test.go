package portable

import (
	"bytes"
	"encoding/json"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/emiago/sipgo/sip"
)

func TestImmutableMessageOwnsIngressAndProjections(t *testing.T) {
	original := origin(t, sip.INVITE, "tls")
	raw := original.WireBytes()
	parsed, err := sip.ParseMessage(raw)
	if err != nil {
		t.Fatal(err)
	}
	m, err := FreezeMessage(raw, parsed, "tls", "192.0.2.3:6000", "192.0.2.4:5061")
	if err != nil {
		t.Fatal(err)
	}
	before := m.WireBytes()
	facts, _ := m.Facts()
	headers, _ := m.Headers()
	vias, _ := m.Vias()
	request := parsed.(*sip.Request)
	request.To().Params.Add("tag", "injected")
	request.From().Address.Host = "changed"
	request.Via().Params.Add("branch", "changed")
	request.CSeq().SeqNo++
	request.Contact().Address.Host = "changed"
	request.Recipient.Host = "changed"
	request.SetBody([]byte("changed"))
	for i := range raw {
		raw[i] = 0
	}
	if after, _ := m.Facts(); after != facts || !bytes.Equal(before, m.WireBytes()) {
		t.Fatal("source parser or frame aliases frozen message")
	}
	copyWire := m.WireBytes()
	copyWire[0] = 0
	copyHeaders, _ := m.Headers()
	copyHeaders[0].Name = "changed"
	copyHeaders[0].Value = "changed"
	copyVias, _ := m.Vias()
	copyVias[0].Host = "changed"
	copyVias[0].Params[0].V = "changed"
	if after, _ := m.Headers(); !reflect.DeepEqual(after, headers) {
		t.Fatal("header projection aliases message")
	}
	if after, _ := m.Vias(); !reflect.DeepEqual(after, vias) {
		t.Fatal("Via projection aliases message")
	}
	if !m.EqualWireBytes(before) || m.WireHash() != original.WireHash() {
		t.Fatal("copying wire projection aliases message")
	}
	clone := m.Clone()
	if clone.data != m.data {
		t.Fatal("immutable clone copied full message")
	}
	if _, err := m.WriteWire(mutatingWriter{}); err != nil {
		t.Fatal(err)
	}
	if !m.EqualWireBytes(before) {
		t.Fatal("writer changed immutable wire")
	}
}

type mutatingWriter struct{}

func (mutatingWriter) Write(b []byte) (int, error) {
	for i := range b {
		b[i] = 0
	}
	return len(b), nil
}

func TestImmutableResponseFlowVariantPreservesSelectedFinal(t *testing.T) {
	o := origin(t, sip.INVITE, "tls")
	selected := reply(t, o, 200, "selected-tag")
	first, err := selected.WithObservedResponseFlow("192.0.2.8", 6001)
	if err != nil {
		t.Fatal(err)
	}
	repeat, err := first.WithObservedResponseFlow("192.0.2.8", 6001)
	if err != nil || repeat.data != first.data {
		t.Fatal("unchanged physical emission rerendered")
	}
	second, err := selected.WithObservedResponseFlow("192.0.2.9", 6002)
	if err != nil {
		t.Fatal(err)
	}
	if first.EqualWire(second) || selected.EqualWire(first) || selected.EqualWire(second) {
		t.Fatal("physical flow variant replaced chosen final identity")
	}
	if !MatchResponse(o, first) || !MatchResponse(o, second) {
		t.Fatal("physical Via parameters changed native response association")
	}
	r, _ := testResponse(first)
	if r.Via().Params.GetOr("received", "") != "192.0.2.8" || r.Via().Params.GetOr("rport", "") != "6001" {
		t.Fatal("flow not finalized in physical bytes")
	}
	r, _ = testResponse(second)
	if r.Via().Params.GetOr("received", "") != "192.0.2.9" || r.Via().Params.GetOr("rport", "") != "6002" {
		t.Fatal("replacement flow frozen to previous emission")
	}
	if f, _ := o.Facts(); f.ToTag != "" {
		t.Fatal("response builder mutated admitted request tag")
	}
	server, _ := NewTransaction(InviteServer, o, DefaultTimers(), "selected-tag")
	server, _, _ = StepTransaction(server, Input{Kind: Start, Now: 1})
	server, _, err = StepTransaction(server, Input{Kind: SendResponse, Now: 2, Message: selected})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := StepTransaction(server, Input{Kind: SendResponse, Now: 3, Message: second}); err == nil {
		t.Fatal("distinct physical wire accepted as selected native final replay")
	}
	if _, _, err := StepTransaction(server, Input{Kind: SendResponse, Now: 3, Message: selected}); err != nil {
		t.Fatal("selected final changed after physical variant")
	}
}

func TestImmutableLazyRestorePreservesWireShapeAndValidation(t *testing.T) {
	for _, wire := range [][]byte{nil, {}, []byte("invalid but unused stored SIP")} {
		m := RestoreMessage(wire, "tls", "source", "destination")
		if m.data.frozen != nil || m.data.err != nil {
			t.Fatal("restore eagerly validated unused SIP")
		}
		base := m.RetainedBytes()
		_ = m.WireLen()
		_ = m.WireHash()
		_ = m.Equal(m.Clone())
		_ = m.AppendWire(nil)
		encoded, err := json.Marshal(m)
		if err != nil {
			t.Fatal(err)
		}
		var shape struct {
			Wire        []byte `json:"wire"`
			Transport   string `json:"transport"`
			Source      string `json:"source"`
			Destination string `json:"destination"`
		}
		if err := json.Unmarshal(encoded, &shape); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(shape.Wire, wire) || shape.Transport != "tls" || shape.Source != "source" || shape.Destination != "destination" {
			t.Fatal("serialized byte profile changed")
		}
		var restored Message
		if err := json.Unmarshal(encoded, &restored); err != nil {
			t.Fatal(err)
		}
		if !m.Equal(restored) || m.data.frozen != nil || restored.data.frozen != nil || m.RetainedBytes() != base {
			t.Fatal("serialization/equality prepared unused message")
		}
		if _, err := restored.Facts(); err == nil {
			t.Fatal("invalid stored bytes consumed successfully")
		}
	}
	if RestoreMessage(nil, "tls", "", "").Equal(RestoreMessage([]byte{}, "tls", "", "")) {
		t.Fatal("nil/present-empty wire identity lost")
	}
	o := origin(t, sip.REGISTER, "tls")
	m := RestoreMessage(o.WireBytes(), o.Transport(), o.Source(), o.Destination())
	copy := RestoreMessage(o.WireBytes(), o.Transport(), o.Source(), o.Destination())
	base := m.RetainedBytes()
	if _, err := m.Facts(); err != nil {
		t.Fatal(err)
	}
	if !m.Equal(copy) || m.RetainedBytes() <= base {
		t.Fatal("facts affected identity or omitted accounting")
	}
	stable := m.RetainedBytes()
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := m.Facts(); err != nil {
				t.Error(err)
			}
			_ = m.RetainedBytes()
		}()
	}
	wg.Wait()
	if m.RetainedBytes() != stable {
		t.Fatal("facts charged repeatedly")
	}
}

// Private wire poisoning is a test sentinel: no external API can mutate these
// bytes. Every constructor/transition/builder must use the already-frozen facts,
// not parse this evidence again. Independently parse each generated output.
func TestImmutableNativePathsDoNotReparseFrozenWire(t *testing.T) {
	for _, transport := range []string{"udp", "tcp", "tls"} {
		t.Run(transport, func(t *testing.T) {
			for _, method := range []sip.RequestMethod{sip.REGISTER, sip.SUBSCRIBE, sip.INVITE} {
				o := origin(t, method, transport)
				if _, err := o.Facts(); err != nil {
					t.Fatal(err)
				}
				o.data.wire[0] = 0
				family := NonInviteServer
				if method == sip.INVITE {
					family = InviteServer
				}
				s, err := NewTransaction(family, o, DefaultTimers(), "chosen")
				if err != nil {
					t.Fatal(err)
				}
				s, _, err = StepTransaction(s, Input{Kind: Start, Now: 1})
				if err != nil {
					t.Fatal(err)
				}
				m, err := BuildResponseWithHeaders(o, 200, "OK", nil, "chosen", "<sip:phone@192.0.2.2:5061>", nil)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := sip.ParseMessage(m.WireBytes()); err != nil {
					t.Fatal("structured response invalid", err)
				}
				m.data.wire[0] = 0
				s, _, err = StepTransaction(s, Input{Kind: SendResponse, Now: 2, Message: m})
				if err != nil {
					t.Fatal(err)
				}
				if err := s.Validate(); err != nil {
					t.Fatal(err)
				}
				if method == sip.INVITE {
					d, err := NewDialog(UAS, o, m, DefaultTimers())
					if err != nil {
						t.Fatal(err)
					}
					d, _, err = StepDialog(d, DialogInput{Kind: DialogSendResponse, Now: 3, Message: m})
					if err != nil {
						t.Fatal(err)
					}
					deadline := d.Timers[Timer2xx]
					if _, _, err := StepDialog(d, DialogInput{Kind: DialogTimer, Now: deadline.At, Timer: Timer2xx, Generation: deadline.Generation}); err != nil {
						t.Fatal(err)
					}
					cancel, err := BuildCancel(o)
					if err != nil {
						t.Fatal(err)
					}
					if _, err := sip.ParseMessage(cancel.WireBytes()); err != nil {
						t.Fatal(err)
					}
					final, err := ResponseFor(o, 487, "Request Terminated", nil, "chosen")
					if err != nil {
						t.Fatal(err)
					}
					final.data.wire[0] = 0
					ack, err := BuildNon2xxACK(o, final)
					if err != nil {
						t.Fatal(err)
					}
					if _, err := sip.ParseMessage(ack.WireBytes()); err != nil {
						t.Fatal(err)
					}
				}
			}
		})
	}
}

func TestImmutableHeadersAndNativeKeyAdmission(t *testing.T) {
	o := origin(t, sip.INVITE, "tls")
	raw := strings.Replace(string(o.WireBytes()), "Via: ", "v: ", 1)
	raw = strings.Replace(raw, "From: ", "f: ", 1)
	raw = strings.Replace(raw, "\r\nContent-Length:", "\r\nVia: SIP/2.0/TLS second.example.test:5061;branch=z9hG4bK-second\r\nX-Test: one\r\nX-Test: two\r\nContent-Length:", 1)
	parsed, err := sip.ParseMessage([]byte(raw))
	if err != nil {
		t.Fatal(err)
	}
	m, err := FreezeMessage([]byte(raw), parsed, "tls", "", "")
	if err != nil {
		t.Fatal(err)
	}
	headers, _ := m.Headers()
	expected := parsed.(*sip.Request).Headers()
	if len(headers) != len(expected) {
		t.Fatal("ordered header count changed")
	}
	for i, h := range headers {
		if h.Name != expected[i].Name() || h.Value != expected[i].Value() {
			t.Fatal("header projection normalization changed")
		}
	}
	vias, _ := m.Vias()
	if len(vias) != 2 {
		t.Fatal("repeated Via projection omitted")
	}
	response, err := ResponseFor(m, 200, "OK", nil, "tag")
	if err != nil {
		t.Fatal(err)
	}
	responseVias, _ := response.Vias()
	if len(responseVias) != 2 {
		t.Fatal("builder lost repeated Via typed facts")
	}
	legacy := mutate(t, m, func(v sip.Message) { v.Via().Params.Add("branch", "legacy-branch") })
	if _, err := legacy.Facts(); err != nil {
		t.Fatal("client-key rejection tightened server message acceptance", err)
	}
	if _, err := legacy.ClientTxKey(); err == nil {
		t.Fatal("invalid observed branch accepted")
	}
	if _, err := legacy.ServerTxKey(); err != nil {
		t.Fatal("legacy native server-key acceptance changed", err)
	}
	cancel, err := BuildCancel(o)
	if err != nil {
		t.Fatal(err)
	}
	reply, err := ResponseFor(cancel, 200, "OK", nil, "tag")
	if err != nil {
		t.Fatal(err)
	}
	a, _ := o.ServerTxKey()
	b, _ := cancel.CancelServerTxKey()
	c, _ := reply.CancelServerTxKey()
	if a != b || a != c {
		t.Fatal("CANCEL request/response normalized selector changed")
	}
}

func TestImmutableDigestBodyOwnershipAndDerivation(t *testing.T) {
	o := origin(t, sip.INVITE, "tls")
	o = mutate(t, o, func(v sip.Message) {
		v.SetBody([]byte("unchanged digest body\x00\r\n"))
		v.AppendHeader(sip.NewHeader("Content-Type", "application/sdp"))
	})
	original := o.WireBytes()
	body, _ := o.Body()
	body[0] = 0
	if actual, _ := o.Body(); actual[0] == 0 {
		t.Fatal("body projection aliases digest input")
	}
	for _, status := range []int{401, 407} {
		for _, qop := range []string{"auth", "auth-int", ""} {
			name := "WWW-Authenticate"
			auth := "Authorization"
			if status == 407 {
				name = "Proxy-Authenticate"
				auth = "Proxy-Authorization"
			}
			challengeText := `Digest realm="example.test", nonce="fixed-nonce", opaque="fixed-opaque", algorithm=MD5`
			if qop != "" {
				challengeText += `, qop="` + qop + `"`
			}
			challenge, err := BuildResponseWithHeaders(o, status, "challenge", nil, "challenge-tag", "", []Header{{Name: name, Value: challengeText}})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := o.Facts(); err != nil {
				t.Fatal(err)
			}
			if _, err := challenge.Facts(); err != nil {
				t.Fatal(err)
			}
			// Prove retry and validation use facts, including exact URI and body.
			o.data.wire[0] = 0
			challenge.data.wire[0] = 0
			retry, err := BuildDigestRetry(o, challenge, DigestCredentials{Username: "fixture-user", Password: "fixture-password", Cnonce: "fixed-cnonce", Count: 1}, "z9hG4bK-retry")
			if err != nil {
				t.Fatal(err)
			}
			if err := ValidateDigestRetry(o, challenge, retry, "fixed-cnonce", 1, "z9hG4bK-retry"); err != nil {
				t.Fatal(err)
			}
			for _, change := range []func(sip.Message){func(v sip.Message) { v.SetBody([]byte("changed")) }, func(v sip.Message) { v.AppendHeader(sip.NewHeader("X-Injected", "unexpected")) }, func(v sip.Message) { v.Via().Host = "other.test" }, func(v sip.Message) { v.CSeq().SeqNo++ }, func(v sip.Message) {
				h := v.GetHeaders(auth)[0]
				v.(*sip.Request).RemoveHeader(auth)
				v.AppendHeader(sip.NewHeader(auth, strings.ReplaceAll(h.Value(), "fixed-nonce", "other-nonce")))
			}} {
				forged := mutate(t, retry, change)
				if err := ValidateDigestRetry(o, challenge, forged, "fixed-cnonce", 1, "z9hG4bK-retry"); err == nil {
					t.Fatal("untrusted digest changed committed native inputs")
				}
			}
			o.data.wire[0] = original[0]
			if !bytes.Equal(original, o.WireBytes()) {
				t.Fatal("digest construction changed admitted request")
			}
			facts, _ := o.Facts()
			if facts.ToTag != "" || facts.ViaBranch != "z9hG4bK-original" {
				t.Fatal("retry mutation escaped into origin facts")
			}
		}
	}
}

func TestImmutableRawValidationErrorOrder(t *testing.T) {
	o := origin(t, sip.INVITE, "tls")
	for _, tc := range []struct {
		name            string
		wire            []byte
		transport, want string
	}{
		{"transport", o.WireBytes(), "ws", "observed transport required"},
		{"cseq-method", bytes.Replace(o.WireBytes(), []byte("CSeq: 121787510 INVITE"), []byte("CSeq: 121787510 BYE"), 1), "tls", "CSeq method mismatch"},
		{"call-id-before-transport", bytes.Replace(o.WireBytes(), []byte("Call-ID: portable-call"), []byte("Call-ID: "), 1), "ws", "empty Call-ID body"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseMessage(tc.wire, tc.transport, "", "")
			if err == nil || err.Error() != tc.want {
				t.Fatalf("error order changed: %v", err)
			}
		})
	}
}

func TestImmutableStructuredHeaderProjectionMatchesParser(t *testing.T) {
	m, err := BuildInitialRequest(InitialRequest{Method: sip.INVITE, URI: "sip:phone@example.test", CallID: "construction", LocalTag: "tag", Branch: "z9hG4bK-construction", CSeq: 1, Advertise: "192.0.2.1:5061", Transport: "tls", Headers: []Header{{Name: "c", Value: " application/sdp "}, {Name: "refer-to", Value: "<sip:transfer@example.test>;x=1"}, {Name: "referred-by", Value: "<sip:caller@example.test>"}, {Name: "X-Whitespace", Value: "  value  "}}, Body: []byte("body")})
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := sip.ParseMessage(m.WireBytes())
	if err != nil {
		t.Fatal(err)
	}
	expected := parsed.(*sip.Request).Headers()
	headers, err := m.Headers()
	if err != nil {
		t.Fatal(err)
	}
	if len(headers) != len(expected) {
		t.Fatal("construction header projection length")
	}
	for i, h := range headers {
		if h.Name != expected[i].Name() || h.Value != expected[i].Value() {
			t.Fatalf("constructed header projection differs at%d: %#v != %s: %s", i, h, expected[i].Name(), expected[i].Value())
		}
	}
	facts, _ := m.Facts()
	if !facts.SDP {
		t.Fatal("compact content-type construction not classified")
	}
	if !bytes.Contains(m.WireBytes(), []byte("c:  application/sdp ")) {
		t.Fatal("projection normalization rewrote selected raw wire")
	}
}

func TestImmutablePhysicalVariantKeepsPriorParserNormalization(t *testing.T) {
	o := origin(t, sip.INVITE, "tls")
	selected, err := BuildResponseWithHeaders(o, 200, "OK", []byte("body"), "tag", "", []Header{{Name: "c", Value: " text/plain "}, {Name: "X-Padded", Value: " padded "}})
	if err != nil {
		t.Fatal(err)
	}
	facts, _ := selected.Facts()
	// The constructor already filled received/rport from observed origin.Source.
	// Legacy physical dispatch still normalized the rest of the parsed headers.
	variant, err := selected.WithObservedResponseFlow("192.0.2.1", 5061)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := sip.ParseMessage(selected.WireBytes())
	if err != nil {
		t.Fatal(err)
	}
	parsed.Via().Params.Add("received", "192.0.2.1")
	parsed.Via().Params.Add("rport", "5061")
	if !variant.EqualWireBytes([]byte(parsed.String())) {
		t.Fatal("immutable physical variant changed prior parser/render normalization")
	}
	if selected.EqualWire(variant) {
		t.Fatal("physical normalization replaced selected conflict identity")
	}
	repeat, err := variant.WithObservedResponseFlow("192.0.2.1", 5061)
	if err != nil || repeat.data != variant.data {
		t.Fatal("canonical finalized variant rerendered")
	}
	if after, _ := selected.Facts(); after != facts {
		t.Fatal("physical normalization changed selected facts")
	}
}

func TestImmutableStructuredBuilderOwnsInputBody(t *testing.T) {
	body := []byte("original body")
	headers := []Header{{Name: "X-Test", Value: "original header"}}
	message, err := BuildInitialRequest(InitialRequest{Method: sip.MESSAGE, URI: "sip:phone@example.test", CallID: "owned-builder", LocalTag: "tag", Branch: "z9hG4bK-owned-builder", CSeq: 1, Advertise: "192.0.2.1:5061", Transport: "tls", Headers: headers, Body: body})
	if err != nil {
		t.Fatal(err)
	}
	hash := message.WireHash()
	body[0] = 0
	headers[0].Value = "changed"
	actual, _ := message.Body()
	values, _ := message.HeaderValues("X-Test")
	if message.WireHash() != hash || string(actual) != "original body" || len(values) != 1 || values[0] != "original header" {
		t.Fatal("structured builder borrowed mutable input")
	}
}

func TestImmutableOrderedContactsSurviveFlowAndDigest(t *testing.T) {
	bindings := []Header{{Name: "Contact", Value: `"First" <sip:first@192.0.2.10:6001;transport=tls>;expires=90`}, {Name: "Contact", Value: `"Second" <sip:second@192.0.2.11:6002;transport=tls>;expires=180`}}
	o := origin(t, sip.REGISTER, "tls")
	response, err := BuildResponseWithHeaders(o, 200, "OK", nil, "registrar", "", bindings)
	if err != nil {
		t.Fatal(err)
	}
	selectedHash := response.WireHash()
	for _, flow := range []struct {
		ip   string
		port int
	}{{"192.0.2.20", 6100}, {"192.0.2.21", 6200}} {
		physical, err := response.WithObservedResponseFlow(flow.ip, flow.port)
		if err != nil {
			t.Fatal(err)
		}
		oracle, _ := testResponse(response)
		oracle.Via().Params.Add("received", flow.ip)
		oracle.Via().Params.Add("rport", strconv.Itoa(flow.port))
		if !physical.EqualWireBytes([]byte(oracle.String())) {
			t.Fatal("multi-binding physical response changed ordered Contacts")
		}
		actual, _ := testResponse(physical)
		contacts := actual.GetHeaders("Contact")
		if len(contacts) != 2 || contacts[0].Value() != bindings[0].Value || contacts[1].Value() != bindings[1].Value {
			t.Fatal("Contacts A,B did not remain A,B")
		}
		facts, _ := physical.Facts()
		if facts.ContactURI != actual.Contact().Address.String() {
			t.Fatal("native selected Contact semantics changed")
		}
	}
	if response.WireHash() != selectedHash {
		t.Fatal("physical variant mutated selected response")
	}
	// The independent SIP parser selects the last typed Contact. Retry must
	// retain every original binding, not put that selected binding in slot one.
	request, _ := testRequest(o)
	request.AppendHeader(&sip.ContactHeader{Address: sip.Uri{Scheme: "sip", User: "second", Host: "192.0.2.11", Port: 6002}, Params: sip.HeaderParams{{K: "expires", V: "180"}}})
	o = wire(request, "tls", o.Source(), o.Destination())
	challenge, err := BuildResponseWithHeaders(o, 401, "challenge", nil, "challenge", "", []Header{{Name: "WWW-Authenticate", Value: `Digest realm="test", nonce="nonce", algorithm=MD5, qop="auth"`}})
	if err != nil {
		t.Fatal(err)
	}
	retry, err := BuildDigestRetry(o, challenge, DigestCredentials{Username: "fixture", Password: "synthetic", Cnonce: "cnonce", Count: 1}, "z9hG4bK-multicontact")
	if err != nil {
		t.Fatal(err)
	}
	actual, _ := testRequest(retry)
	oracle, _ := testRequest(o)
	oracle.RemoveHeader("Authorization")
	oracle.AppendHeader(sip.NewHeader("Authorization", actual.GetHeader("Authorization").Value()))
	oracle.CSeq().SeqNo++
	oracle.Via().Params.Add("branch", "z9hG4bK-multicontact")
	if !retry.EqualWireBytes([]byte(oracle.String())) {
		t.Fatal("digest reconstruction changed ordered Contacts")
	}
	if err := ValidateDigestRetry(o, challenge, retry, "cnonce", 1, "z9hG4bK-multicontact"); err != nil {
		t.Fatal(err)
	}
}

func TestImmutableDuplicateSingletonsKeepOrderAndResponseSelection(t *testing.T) {
	base := origin(t, sip.REGISTER, "tls")
	request, _ := testRequest(base)
	request.AppendHeader(&sip.FromHeader{DisplayName: "Later From", Address: sip.Uri{Scheme: "sip", User: "later-from", Host: "example.test"}, Params: sip.HeaderParams{{K: "tag", V: "later-from"}}})
	request.AppendHeader(&sip.ToHeader{DisplayName: "Later To", Address: sip.Uri{Scheme: "sip", User: "later-to", Host: "example.test"}, Params: sip.HeaderParams{}})
	id := sip.CallIDHeader("later-call")
	request.AppendHeader(&id)
	request.AppendHeader(&sip.CSeqHeader{SeqNo: 22, MethodName: sip.REGISTER})
	o := wire(request, "tls", base.Source(), base.Destination())
	original, _ := testRequest(o)
	original.SetTransport("TLS")
	original.SetSource(o.Source())
	original.SetDestination(o.Destination())
	original.To().Params.Add("tag", "chosen")
	expected := sip.NewResponseFromRequest(original, 200, "OK", nil)
	response, err := ResponseFor(o, 200, "OK", nil, "chosen")
	if err != nil {
		t.Fatal(err)
	}
	if !response.EqualWireBytes([]byte(expected.String())) {
		t.Fatal("response did not copy exactly native-selected singleton fields")
	}
	for _, name := range []string{"From", "To", "Call-ID", "CSeq"} {
		values, _ := response.HeaderValues(name)
		if len(values) != 1 {
			t.Fatalf("response copied duplicate%s", name)
		}
	}
	challenge, err := BuildResponseWithHeaders(o, 401, "challenge", nil, "chosen", "", []Header{{Name: "WWW-Authenticate", Value: `Digest realm="test", nonce="nonce", algorithm=MD5, qop="auth"`}})
	if err != nil {
		t.Fatal(err)
	}
	retry, err := BuildDigestRetry(o, challenge, DigestCredentials{Username: "fixture", Password: "synthetic", Cnonce: "cnonce", Count: 1}, "z9hG4bK-duplicates")
	if err != nil {
		t.Fatal(err)
	}
	actual, _ := testRequest(retry)
	oracle, _ := testRequest(o)
	oracle.RemoveHeader("Authorization")
	oracle.AppendHeader(sip.NewHeader("Authorization", actual.GetHeader("Authorization").Value()))
	oracle.CSeq().SeqNo++
	oracle.Via().Params.Add("branch", "z9hG4bK-duplicates")
	if !retry.EqualWireBytes([]byte(oracle.String())) {
		t.Fatal("digest reconstruction reordered/substituted duplicate singleton fields")
	}
	// Physical rendering must preserve duplicates on a received response too.
	duplicate, _ := testResponse(response)
	duplicate.AppendHeader(&sip.FromHeader{Address: sip.Uri{Scheme: "sip", User: "last", Host: "example.test"}, Params: sip.HeaderParams{{K: "tag", V: "last"}}})
	duplicate.AppendHeader(&sip.ToHeader{Address: sip.Uri{Scheme: "sip", User: "last-to", Host: "example.test"}, Params: sip.HeaderParams{{K: "tag", V: "last-to"}}})
	finalID := sip.CallIDHeader("final-call")
	duplicate.AppendHeader(&finalID)
	duplicate.AppendHeader(&sip.CSeqHeader{SeqNo: 23, MethodName: sip.REGISTER})
	selected := wire(duplicate, "tls", "", base.Source())
	physical, err := selected.WithObservedResponseFlow("192.0.2.30", 6300)
	if err != nil {
		t.Fatal(err)
	}
	duplicate.Via().Params.Add("received", "192.0.2.30")
	duplicate.Via().Params.Add("rport", "6300")
	if !physical.EqualWireBytes([]byte(duplicate.String())) {
		t.Fatal("physical reconstruction substituted/reordered duplicate singleton fields")
	}
}

func TestImmutableStructuredMethodPreservesParserRejection(t *testing.T) {
	for _, method := range []sip.RequestMethod{"invite", "iNvItE", "x-extension"} {
		message, err := BuildInitialRequest(InitialRequest{Method: method, URI: "sip:phone@example.test", CallID: "lowercase", LocalTag: "tag", Branch: "z9hG4bK-lowercase", CSeq: 1, Advertise: "192.0.2.1:5061", Transport: "tls"})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := message.Facts(); err == nil || err.Error() != "CSeq method mismatch" {
			t.Fatalf("structured%s changed effective native rejection: %v", method, err)
		}
		observed, err := message.ObservedFacts()
		if err != nil || string(observed.Method) != strings.ToUpper(string(method)) || observed.CSeqMethod != method {
			t.Fatal("observed request-line normalization changed")
		}
		if _, err := NewTransaction(NonInviteClient, message, DefaultTimers(), ""); err == nil {
			t.Fatal("lowercase structured method entered native reducer")
		}
		dialog := Dialog{LocalTag: "local", RemoteTag: "remote", CallID: "lowercase", LocalURI: "sip:a@example.test", RemoteURI: "sip:b@example.test", Target: "sip:b@example.test", SentBy: "192.0.2.1:5061", Transport: "tls"}
		if _, err := BuildDialogRequestWithHeaders(dialog, method, 1, nil, "z9hG4bK-lowercase", nil); err == nil || err.Error() != "CSeq method mismatch" {
			t.Fatal("dialog header wrapper changed native rejection timing", err)
		}
	}
}

func TestImmutableDialogHeaderWrapperPreservesRouteNormalization(t *testing.T) {
	dialog := Dialog{LocalTag: "local", RemoteTag: "remote", CallID: "route-wrapper", LocalURI: "sip:a@example.test", RemoteURI: "sip:b@example.test", Target: "sip:b@example.test", SentBy: "192.0.2.1:5061", Transport: "tls", Routes: []string{"<sip:proxy1.example.test;lr>", "  <sip:proxy2.example.test;lr>, <sip:proxy3.example.test;lr>  "}}
	raw, err := BuildDialogRequest(dialog, sip.NOTIFY, 3, []byte("body"), "z9hG4bK-routes")
	if err != nil {
		t.Fatal(err)
	}
	oracle, err := testRequest(raw)
	if err != nil {
		t.Fatal(err)
	}
	headers := []Header{{Name: "Event", Value: "dialog"}, {Name: "Content-Type", Value: "application/dialog-info+xml"}}
	if err := applyHeaders(oracle, headers); err != nil {
		t.Fatal(err)
	}
	actual, err := BuildDialogRequestWithHeaders(dialog, sip.NOTIFY, 3, []byte("body"), "z9hG4bK-routes", headers)
	if err != nil {
		t.Fatal(err)
	}
	if !actual.EqualWireBytes([]byte(oracle.String())) {
		t.Fatal("dialog wrapper changed comma/padded Route normalization")
	}
}

func TestImmutableObservedAndNativeValidationRemainIndependent(t *testing.T) {
	o := origin(t, sip.INVITE, "tls")
	for _, tc := range []struct {
		name             string
		wire             []byte
		transport        string
		observed, native bool
	}{
		{"valid", o.WireBytes(), "tls", true, true},
		{"no-observed-transport", o.WireBytes(), "", true, false},
		{"unsupported-native-transport", o.WireBytes(), "ws", true, false},
		{"high-cseq", bytes.Replace(o.WireBytes(), []byte("121787510 INVITE"), []byte("2147483648 INVITE"), 1), "tls", false, false},
		{"method-mismatch", bytes.Replace(o.WireBytes(), []byte("INVITE sip:"), []byte("BYE sip:"), 1), "tls", true, false},
		{"legacy-branch", bytes.Replace(o.WireBytes(), []byte("z9hG4bK-original"), []byte("legacy-branch"), 1), "tls", false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := RestoreMessage(tc.wire, tc.transport, "", "")
			observed, err := m.ObservedFacts()
			if (err == nil) != tc.observed {
				t.Fatal("observed acceptance changed", err)
			}
			if _, err := m.Facts(); (err == nil) != tc.native {
				t.Fatal("native acceptance changed", err)
			}
			if !tc.observed {
				return
			}
			parsed, err := sip.ParseMessage(tc.wire)
			if err != nil {
				t.Fatal(err)
			}
			if observed.Method != parsed.(*sip.Request).Method || observed.CSeqMethod != parsed.CSeq().MethodName {
				t.Fatal("request-line and CSeq methods conflated")
			}
			headers, err := m.ObservedHeaders()
			if err != nil {
				t.Fatal(err)
			}
			body, err := m.ObservedBody()
			if err != nil {
				t.Fatal(err)
			}
			vias, err := m.ObservedVias()
			if err != nil {
				t.Fatal(err)
			}
			if len(headers) != len(parsed.(*sip.Request).Headers()) || !bytes.Equal(body, parsed.Body()) || len(vias) != len(parsed.GetHeaders("Via")) {
				t.Fatal("observed projection rejected/changed native-invalid message")
			}
			key, err := m.ObservedClientTxKey()
			expected, e := sip.ClientTxKeyMake(parsed)
			if err != nil || e != nil || key != expected {
				t.Fatal("observed key changed")
			}
			if !tc.native {
				if _, err := NewTransaction(NonInviteClient, m, DefaultTimers(), ""); err == nil {
					t.Fatal("observed proof bypassed strict native reducer")
				}
			}
		})
	}
}

func TestImmutableObservedFactsKeepNativeLimitAfterCustomHeaderParse(t *testing.T) {
	original := origin(t, sip.INVITE, "tls")
	raw := bytes.Replace(original.WireBytes(), []byte("121787510 INVITE"), []byte("2147483648 INVITE"), 1)
	parsers := make(map[string]sip.HeaderParser)
	for k, v := range sip.DefaultHeadersParser() {
		parsers[k] = v
	}
	parsers["cseq"] = func(_ []byte, _ string) (sip.Header, error) {
		return &sip.CSeqHeader{SeqNo: 2147483648, MethodName: sip.INVITE}, nil
	}
	parsed, err := sip.NewParser(sip.WithHeadersParsers(parsers)).ParseSIP(raw)
	if err != nil {
		t.Fatal(err)
	}
	message, err := FreezeMessage(raw, parsed, "tls", "", "")
	if err == nil || err.Error() != "invalid CSeq or Call-ID" {
		t.Fatal("native limit bypassed by supplied parser", err)
	}
	facts, err := message.ObservedFacts()
	if err != nil || facts.CSeq != 2147483648 {
		t.Fatal("observed proof incorrectly imposed portable limit", err)
	}
	if _, err := message.Facts(); err == nil {
		t.Fatal("observed projection relaxed native validation")
	}
}

func TestImmutableResponseSourceFallbackMatchesNative(t *testing.T) {
	for _, transport := range []string{"udp", "tcp", "tls"} {
		for _, tc := range []struct {
			name            string
			port            int
			received, rport string
		}{{"via-port", 5060, "", ""}, {"observed-default", 0, "", ""}, {"received", 0, "192.0.2.42", ""}, {"retained-rport", 0, "192.0.2.42", "6000"}, {"invalid-rport", 0, "", "invalid"}} {
			t.Run(transport+"/"+tc.name, func(t *testing.T) {
				initial, err := BuildInitialRequest(InitialRequest{Method: sip.INVITE, URI: "sip:phone@example.test", CallID: "source-fallback", LocalTag: "tag", Branch: "z9hG4bK-source-fallback", CSeq: 1, Advertise: "192.0.2.1:5061", Transport: transport})
				if err != nil {
					t.Fatal(err)
				}
				parsed, _ := testRequest(initial)
				parsed.Via().Port = tc.port
				parsed.Via().Transport = "TCP"
				parsed.Via().Params.Add("rport", tc.rport)
				if tc.received != "" {
					parsed.Via().Params.Add("received", tc.received)
				}
				origin := wire(parsed, transport, "", "")
				oracle, _ := testRequest(origin)
				oracle.SetTransport(strings.ToUpper(transport))
				oracle.SetSource("")
				oracle.To().Params.Add("tag", "chosen")
				expected := sip.NewResponseFromRequest(oracle, 200, "OK", nil)
				response, err := ResponseFor(origin, 200, "OK", nil, "chosen")
				if err != nil {
					t.Fatal(err)
				}
				if !response.EqualWireBytes([]byte(expected.String())) {
					t.Fatal("empty source lost native Via/default-port fallback")
				}
				if response.Destination() != "" {
					t.Fatal("effective source changed separately observed metadata")
				}
			})
		}
	}
}
