package engine

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
)

// nativeIdentity preserves the existing Memory identity byte format across
// native execution backends.
func nativeIdentity(key []byte, parts ...string) string {
	h := hmac.New(sha256.New, key)
	for _, part := range parts {
		fmt.Fprintf(h, "%d:", len(part))
		_, _ = h.Write([]byte(part))
	}
	return hex.EncodeToString(h.Sum(nil))
}
