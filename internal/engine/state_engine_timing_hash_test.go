package engine

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"

	pb "sipbridge/internal/controlpb"
)

func TestControlCallIDHashMatchesBoundedJavaScriptPolicy(t *testing.T) {
	for _, value := range []string{"call-id", strings.Repeat("x", 512)} {
		ref := &pb.ContextRef{State: &pb.StateRef{CallId: "\ufeff \t" + value + "\u00a0"}}
		sum := sha256.Sum256([]byte(value))
		if got := controlCallIDHash(ref); got != hex.EncodeToString(sum[:]) {
			t.Fatal("hash did not bind the complete trimmed Call-ID")
		}
	}
	for _, value := range []string{"", "bad value", "bad\nvalue", "\u0085call-id", strings.Repeat("x", 513), "call-\u00e9"} {
		if controlCallIDHash(&pb.ContextRef{State: &pb.StateRef{CallId: value}}) != "" {
			t.Fatal("invalid or oversized identity entered diagnostic metadata")
		}
	}
	if controlCallIDHash(nil) != "" {
		t.Fatal("bootstrap context acquired a fabricated Call-ID hash")
	}
}
