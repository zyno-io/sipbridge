package engine

import (
	"bytes"
	"testing"

	"github.com/emiago/sipgo/sip/portable"
	"google.golang.org/protobuf/proto"
)

func TestNativeMessageProjectionMatchesObservedAndOwnsCopies(t *testing.T) {
	wire := []byte("OPTIONS sip:service@example.test SIP/2.0\r\n" +
		"Via: SIP/2.0/TLS first.example.test:5061;branch=z9hG4bKfirst;rport\r\n" +
		"Via: SIP/2.0/TLS second.example.test:5061;branch=z9hG4bKsecond;rport\r\n" +
		"From: <sip:phone@example.test>;tag=remote\r\nTo: <sip:service@example.test>\r\n" +
		"Call-ID: projection@example.test\r\nCSeq: 1 OPTIONS\r\n" +
		"Authorization: Digest username=\"phone\", realm=\"example.test\", nonce=\"n\", uri=\"sip:service@example.test\", response=\"00000000000000000000000000000000\"\r\n" +
		"X-Repeat: one\r\nX-Repeat: two\r\nContent-Length: 4\r\n\r\nbody")
	parsed, err := parseObservedMessage(wire)
	if err != nil {
		t.Fatal(err)
	}
	native, err := portable.FreezeMessage(wire, parsed, "tls", "", "")
	if err != nil {
		t.Fatal(err)
	}
	for _, outbound := range []bool{false, true} {
		want := projectObservedMessage(wire, parsed, outbound)
		got, err := projectNativeMessage(native, outbound)
		if err != nil || !proto.Equal(got, want) {
			t.Fatalf("native projection differs for outbound=%v: %v\ngot %v\nwant %v", outbound, err, got, want)
		}
		got.Wire[0] = 'X'
		got.Body[0] = 'X'
		got.Headers[0].Value = "changed"
		got.Vias[0].Params["branch"] = "changed"
		again, err := projectNativeMessage(native, outbound)
		if err != nil || !proto.Equal(again, want) {
			t.Fatalf("a public projection mutated native evidence: %v", err)
		}
	}
	withoutBranch := bytes.ReplaceAll(wire, []byte(";branch=z9hG4bKfirst"), nil)
	missingBranch, err := portable.ParseMessage(withoutBranch, "tls", "", "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = projectNativeMessage(missingBranch, true); err == nil {
		t.Fatal("native projection accepted a request without its required Via branch")
	}
	for _, test := range []struct {
		raw       []byte
		transport string
	}{
		{bytes.Replace(wire, []byte("CSeq: 1 OPTIONS"), []byte("CSeq: 1 BYE"), 1), "tls"},
		{wire, "unsupported"},
	} {
		raw := test.raw
		observed, observeErr := Observe(raw, false)
		value := portable.RestoreMessage(raw, test.transport, "", "")
		projected, projectErr := projectObservedNativeMessage(value, false)
		if observeErr != nil || projectErr != nil || !proto.Equal(projected, observed) {
			t.Fatalf("observed and native validation were conflated: observed=%v frozen=%v\ngot %v\nwant %v", observeErr, projectErr, projected, observed)
		}
		if _, nativeErr := projectNativeMessage(value, false); nativeErr == nil {
			t.Fatal("observed projection bypassed strict native validation")
		}
	}
}
