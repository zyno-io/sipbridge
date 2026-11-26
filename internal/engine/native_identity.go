package engine

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
)

// nativeIdentity preserves the existing Memory identity byte format across
// native execution backends.
func nativeIdentity(key []byte, parts ...string) string {
	return hex.EncodeToString(nativeIdentityDigest(key, parts...))
}

func nativeCompactIdentity(key []byte, parts ...string) string {
	return base64.RawURLEncoding.EncodeToString(nativeIdentityDigest(key, parts...))
}

func nativeDialogTag(key []byte, parts ...string) string {
	return base64.RawURLEncoding.EncodeToString(nativeIdentityDigest(key, parts...)[:16])
}

func nativeIdentityDigest(key []byte, parts ...string) []byte {
	h := hmac.New(sha256.New, key)
	for _, part := range parts {
		fmt.Fprintf(h, "%d:", len(part))
		_, _ = h.Write([]byte(part))
	}
	return h.Sum(nil)
}
