package engine

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"

	pb "sipbridge/internal/controlpb"
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

var outboundCallIDPattern = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9._-]*(@[a-zA-Z0-9][a-zA-Z0-9.-]*)?$`)

func selectOutboundCallID(open *pb.OpenUac, generated string) (string, error) {
	if open.OutboundCallId == nil {
		return generated, nil
	}
	value := open.GetOutboundCallId()
	if (open.Method != "" && open.Method != "INVITE") || len(value) > 128 || !outboundCallIDPattern.MatchString(value) {
		return "", errors.New("invalid outbound INVITE Call-ID")
	}
	return value, nil
}
