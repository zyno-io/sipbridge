package engine

import (
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"testing"
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
