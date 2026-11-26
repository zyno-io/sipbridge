package engine

import (
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
)

func TestNativeIdentityPreservesMemoryFormat(t *testing.T) {
	key := []byte("0123456789abcdef0123456789abcdef")
	if got := nativeIdentity(key, "ab", "c"); got != "a1a22d593090cd65b0e41a55eeed5d1f2ab13a7357116c4e299eb8740d495cb6" {
		t.Fatal("native identity changed", got)
	}
	if nativeIdentity(key, "ab", "c") == nativeIdentity(key, "a", "bc") {
		t.Fatal("identity tuple is ambiguous")
	}
}

func TestCompactNativeSIPIdentitiesKeepTheSameDigestBytes(t *testing.T) {
	key := []byte("fixture-key")
	fields := []string{"outbound-call", "controller", "application-epoch", "dialog"}
	hexDigest, err := hex.DecodeString(nativeIdentity(key, fields...))
	if err != nil {
		t.Fatal(err)
	}
	for _, check := range []struct {
		name string
		wire string
		want []byte
	}{
		{"full digest", nativeCompactIdentity(key, fields...), hexDigest},
		{"128-bit dialog tag", nativeDialogTag(key, fields...), hexDigest[:16]},
	} {
		t.Run(check.name, func(t *testing.T) {
			got, err := base64.RawURLEncoding.Strict().DecodeString(check.wire)
			if err != nil || !bytes.Equal(got, check.want) || len(check.wire) >= hex.EncodedLen(len(check.want)) {
				t.Fatalf("invalid compact encoding %q: %v", check.wire, err)
			}
		})
	}
}

func TestSelectOutboundCallID(t *testing.T) {
	const wireID = "01a219eb-7ae6-70ea-a6b7-5d192b330ab2-obl-0123456789abcdef"
	for _, tc := range []struct {
		name, method string
		proposed     *string
		want         string
	}{
		{name: "generated", want: "native@sipbridge"},
		{name: "INVITE", method: "INVITE", proposed: proto.String(wireID), want: wireID},
		{name: "host suffix", proposed: proto.String("one@talk.example"), want: "one@talk.example"},
		{name: "empty", proposed: proto.String("")},
		{name: "control bytes", proposed: proto.String("one\r\nVia: a")},
		{name: "too long", proposed: proto.String(strings.Repeat("a", 129))},
		{name: "standalone request", method: "NOTIFY", proposed: proto.String(wireID)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			open := &pb.OpenUac{Method: tc.method, OutboundCallId: tc.proposed}
			got, err := selectOutboundCallID(open, "native@sipbridge")
			if got != tc.want || (err != nil) != (tc.want == "") {
				t.Fatalf("selected %q, %v; want %q", got, err, tc.want)
			}
		})
	}
}
