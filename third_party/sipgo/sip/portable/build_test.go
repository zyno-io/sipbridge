package portable

import (
	"bytes"
	"strings"
	"testing"

	"github.com/emiago/sipgo/sip"
	"github.com/icholy/digest"
)

func TestPortableBuildersRequireExplicitNativeIdentity(t *testing.T) {
	in := InitialRequest{Method: sip.INVITE, URI: "sip:phone@example.test", CallID: "build-call", LocalTag: "local-tag", Branch: "z9hG4bK-build", CSeq: 7, Advertise: "192.0.2.1:5061", Contact: "sip:controller@192.0.2.1:5061", Transport: "tls", Body: []byte("body"), Headers: []Header{{Name: "From", Value: "\"Caller\" <sip:alice@example.test>"}, {Name: "To", Value: "<sip:bob@example.test>"}, {Name: "Content-Type", Value: "text/plain"}, {Name: "X-Test", Value: "one"}, {Name: "X-Test", Value: "two"}}}
	m, e := BuildInitialRequest(in)
	if e != nil {
		t.Fatal(e)
	}
	r, _ := testRequest(m)
	if tag(r.From().Params) != "local-tag" || r.From().DisplayName != "Caller" || r.To().Address.User != "bob" || r.GetHeader("Content-Type").Value() != "text/plain" || len(r.GetHeaders("X-Test")) != 2 {
		t.Fatal("application headers/identity lost")
	}
	in.LocalTag = ""
	if _, e = BuildInitialRequest(in); e == nil {
		t.Fatal("native entropy silently generated")
	}
	in.LocalTag = "local-tag"
	in.Headers = append(in.Headers, Header{Name: "Call-ID", Value: "forged"})
	if _, e = BuildInitialRequest(in); e == nil {
		t.Fatal("caller overrode native Call-ID")
	}
	in.Headers = nil
	in.URI = "sips:phone@example.test"
	in.Transport = "tcp"
	if _, e = BuildInitialRequest(in); e == nil {
		t.Fatal("SIPS downgraded")
	}
	in.URI = "sip:phone@example.test"
	in.Transport = "tls"
	in.LocalTag = "bad;injected=tag"
	if _, e = BuildInitialRequest(in); e == nil {
		t.Fatal("invalid tag admitted")
	}
}

func TestPortableResponseBuilderPreservesChosenTagAndBodies(t *testing.T) {
	o := origin(t, sip.INVITE, "tls")
	before := bytes.Clone(o.WireBytes())
	m, e := BuildResponseWithHeaders(o, 200, "OK", []byte("accepted"), "chosen-tag", "sip:controller@192.0.2.1:5061", []Header{{Name: "Content-Type", Value: "text/plain"}, {Name: "X-Test", Value: "kept"}})
	if e != nil {
		t.Fatal(e)
	}
	r, _ := testResponse(m)
	if tag(r.To().Params) != "chosen-tag" || r.Contact() == nil || string(r.Body()) != "accepted" || !bytes.Equal(o.WireBytes(), before) {
		t.Fatal("response mutated identity/input")
	}
	if _, e = BuildResponseWithHeaders(o, 200, "OK", nil, "chosen-tag", "", []Header{{Name: "To", Value: "<sip:forged@example.test>;tag=other"}}); e == nil {
		t.Fatal("response identity override")
	}
	initial, err := NewTransaction(InviteServer, o, DefaultTimers(), "chosen-tag")
	if err != nil {
		t.Fatal(err)
	}
	s, _ := step(t, initial, Input{Kind: Start, Now: 1})
	s, _ = step(t, s, Input{Kind: SendResponse, Message: m, Now: 2})
	different := mutate(t, m, func(v sip.Message) { v.SetBody([]byte("different")) })
	if _, _, e = StepTransaction(s, Input{Kind: SendResponse, Message: different, Now: 3}); e == nil {
		t.Fatal("selected final changed")
	}
}

func TestPortableRegisterResponseRetainsApplicationBindingsWithoutDialogContact(t *testing.T) {
	o := origin(t, sip.REGISTER, "tls")
	headers := []Header{{Name: "Contact", Value: "<sip:alice@192.0.2.10:5061;transport=tls>;expires=90"}, {Name: "m", Value: "<sip:alice@192.0.2.11:5061;transport=tls>;expires=120"}, {Name: "Expires", Value: "90"}}
	m, err := BuildResponseWithHeaders(o, 200, "OK", nil, "registrar-tag", "sip:bridge@192.0.2.1:5061", headers)
	if err != nil {
		t.Fatal(err)
	}
	r, _ := testResponse(m)
	contacts := r.GetHeaders("Contact")
	if len(contacts) != 2 || contacts[0].Value() != headers[0].Value || contacts[1].Value() != headers[1].Value || tag(r.To().Params) != "registrar-tag" || r.CSeq().MethodName != sip.REGISTER {
		t.Fatalf("registration policy/identity lost: %s", r.String())
	}
	m, err = BuildResponseWithHeaders(o, 200, "OK", nil, "registrar-tag", "sip:bridge@192.0.2.1:5061", nil)
	if err != nil {
		t.Fatal(err)
	}
	r, _ = testResponse(m)
	if r.Contact() != nil {
		t.Fatal("registrar advertised an unrelated dialog target")
	}
	for _, invalid := range []Header{{Name: "Contact", Value: "*"}, {Name: "Contact", Value: "<sip:alice@192.0.2.10>\r\nVia: forged"}, {Name: "To", Value: "<sip:forged@example.test>;tag=other"}} {
		if _, err := BuildResponseWithHeaders(o, 200, "OK", nil, "registrar-tag", "", []Header{invalid}); err == nil {
			t.Fatalf("invalid registration header accepted: %s", invalid.Name)
		}
	}
	invite := origin(t, sip.INVITE, "tls")
	if _, err := BuildResponseWithHeaders(invite, 200, "OK", nil, "dialog-tag", "", headers); err == nil {
		t.Fatal("application replaced native dialog Contact")
	}
}

func TestPortableNativeContactPreservesBareURIParameters(t *testing.T) {
	o := origin(t, sip.INVITE, "tls")
	m, err := BuildResponseWithHeaders(o, 200, "OK", nil, "native-tag", "sip:bridge@192.0.2.1:5061;transport=tls;user=phone", nil)
	if err != nil {
		t.Fatal(err)
	}
	r, _ := testResponse(m)
	if r.Contact().Address.String() != "sip:bridge@192.0.2.1:5061;transport=tls;user=phone" || r.Contact().Params.Has("transport") {
		t.Fatalf("Contact URI parameters became header parameters: %s", r.Contact().Value())
	}
	d, err := NewDialog(UAC, o, m, DefaultTimers())
	if err != nil || d.Target != "sip:bridge@192.0.2.1:5061;transport=tls;user=phone" {
		t.Fatalf("dialog target changed URI: %+v %v", d, err)
	}
}

func TestPortableDigestRetryUsesFullURIAndCommittedIdentity(t *testing.T) {
	o := origin(t, sip.INVITE, "tls")
	challenge := reply(t, o, 407, "challenge-tag")
	challenge = mutate(t, challenge, func(v sip.Message) {
		v.AppendHeader(sip.NewHeader("Proxy-Authenticate", `Digest realm="example.test", nonce="fixed-nonce", algorithm=MD5, qop="auth"`))
	})
	creds := DigestCredentials{Username: "alice", Password: "test-only-secret", Cnonce: "fixed-cnonce", Count: 1}
	m, e := BuildDigestRetry(o, challenge, creds, "z9hG4bK-authenticated")
	if e != nil {
		t.Fatal(e)
	}
	r, _ := testRequest(m)
	original, _ := testRequest(o)
	if r.CSeq().SeqNo != original.CSeq().SeqNo+1 || r.Via().Params.GetOr("branch", "") != "z9hG4bK-authenticated" || tag(r.To().Params) != "" {
		t.Fatal("retry native identity wrong")
	}
	full := original.Recipient.String()
	if !strings.Contains(full, "user=phone") || !strings.Contains(full, "Subject=test") {
		t.Fatal("test lacks full URI")
	}
	parsed, e := digest.ParseCredentials(r.GetHeader("Proxy-Authorization").Value())
	if e != nil {
		t.Fatal(e)
	}
	if parsed.URI != full {
		t.Fatal("URI parameters/query dropped")
	}
	c, e := digest.ParseChallenge(`Digest realm="example.test", nonce="fixed-nonce", algorithm=MD5, qop="auth"`)
	if e != nil {
		t.Fatal(e)
	}
	expected, e := digest.Digest(c, digest.Options{Method: "INVITE", URI: full, Username: creds.Username, Password: creds.Password, Cnonce: creds.Cnonce, Count: 1})
	if e != nil {
		t.Fatal(e)
	}
	if parsed.Response != expected.Response {
		t.Fatal("digest response mismatch")
	}
	if bytes.Contains(m.WireBytes(), []byte(creds.Password)) {
		t.Fatal("plaintext secret in retry")
	}
	if _, e = BuildDigestRetry(o, challenge, DigestCredentials{Username: "alice", Password: creds.Password}, "z9hG4bK-auth"); e == nil {
		t.Fatal("missing entropy silently generated")
	}
}

func TestPortableResponseMatchingChecksFullNativeOrigin(t *testing.T) {
	o := origin(t, sip.INVITE, "tls")
	res := reply(t, o, 200, "remote-tag")
	if !MatchResponse(o, res) {
		t.Fatal("valid response")
	}
	for name, change := range map[string]func(sip.Message){"call-id": func(v sip.Message) { *v.CallID() = "other" }, "from-tag": func(v sip.Message) { v.From().Params.Add("tag", "other") }, "cseq": func(v sip.Message) { v.CSeq().SeqNo++ }, "branch": func(v sip.Message) { v.Via().Params.Add("branch", "z9hG4bK-other") }, "sent-by": func(v sip.Message) { v.Via().Host = "other.example.test" }} {
		t.Run(name, func(t *testing.T) {
			if MatchResponse(o, mutate(t, res, change)) {
				t.Fatal("borrowed transaction proof")
			}
		})
	}
	changed := RestoreMessage(res.WireBytes(), "tcp", res.Source(), res.Destination())
	if MatchResponse(o, changed) {
		t.Fatal("observed transport downgrade")
	}
	inDialog := mutate(t, o, func(v sip.Message) { v.To().Params.Add("tag", "fixed-remote") })
	response := reply(t, inDialog, 200, "fixed-remote")
	if !MatchResponse(inDialog, response) || MatchResponse(inDialog, mutate(t, response, func(v sip.Message) { v.To().Params.Add("tag", "other") })) {
		t.Fatal("in-dialog remote tag not pinned")
	}
}
