package sip

import (
	"bytes"
	"crypto/rand"
	"testing"
)

func TestGenerateTagNUsesCryptoRandomness(t *testing.T) {
	// These six-bit values exercise rejection of the two values outside the
	// SIP token alphabet. The result must depend on crypto/rand, not math/rand.
	old := rand.Reader
	rand.Reader = bytes.NewReader([]byte{62, 63, 0, 61, 1, 60})
	t.Cleanup(func() { rand.Reader = old })
	if got := GenerateTagN(4); got != "0Z1Y" {
		t.Fatalf("tag = %q, want crypto-random token 0Z1Y", got)
	}
}

func TestGenerateTagNLengthAndAlphabet(t *testing.T) {
	for _, n := range []int{0, 1, 16, 100} {
		tag := GenerateTagN(n)
		if len(tag) != n {
			t.Fatalf("len(tag) = %d, want %d", len(tag), n)
		}
		for _, ch := range tag {
			if !bytes.ContainsRune([]byte(letterBytes), ch) {
				t.Fatalf("tag contains non-token character %q", ch)
			}
		}
	}
}
