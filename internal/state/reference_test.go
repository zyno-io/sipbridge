package state

import (
	"math/rand"
	"regexp"
	"strings"
	"testing"
)

var originalReferencePattern = regexp.MustCompile(`^[A-Za-z0-9_.:@+\-]{1,96}$`)

func TestValidReferenceMatchesOriginalGrammar(t *testing.T) {
	check := func(value string) {
		t.Helper()
		if got, want := ValidReference(value), originalReferencePattern.MatchString(value); got != want {
			t.Fatalf("reference grammar differs for %q: got %t want %t", value, got, want)
		}
	}
	for length := 0; length <= 100; length++ {
		check(strings.Repeat("a", length))
	}
	for _, value := range []string{"AazZ09_.:@+-", "a/b", "a\\b", "a b", "a\nb", "a\r\nb", "a\x00b", "é", "日本語", "Ａ", "a\u200bb", "a\u2028b", string([]byte{0xff}), string([]byte{0xc0, 0xaf})} {
		check(value)
	}
	// Every byte at every position in a maximum-length identity proves the exact
	// ASCII alphabet; invalid UTF-8, controls and newline cannot pass as suffixes.
	for index := 0; index < 96; index++ {
		value := []byte(strings.Repeat("a", 96))
		for c := 0; c < 256; c++ {
			value[index] = byte(c)
			check(string(value))
		}
	}
	rng := rand.New(rand.NewSource(20261112))
	alphabet := []byte("AZaz09_.:@+-")
	for i := 0; i < 10000; i++ {
		value := make([]byte, rng.Intn(130))
		for j := range value {
			if i%2 == 0 {
				value[j] = alphabet[rng.Intn(len(alphabet))]
			} else {
				value[j] = byte(rng.Intn(256))
			}
		}
		check(string(value))
	}
}

func FuzzValidReferenceMatchesOriginalGrammar(f *testing.F) {
	for _, value := range []string{"", "a", "AazZ09_.:@+-", strings.Repeat("f", 96), strings.Repeat("f", 97), "é", "a\nb", string([]byte{0xff})} {
		f.Add(value)
	}
	f.Fuzz(func(t *testing.T, value string) {
		if ValidReference(value) != originalReferencePattern.MatchString(value) {
			t.Fatalf("reference grammar differs for %q", value)
		}
	})
}

var referenceBenchmarkResult bool

func BenchmarkReferenceValidation(b *testing.B) {
	for name, value := range map[string]string{"uuid": "7e3ffbbe-5f11-482e-9fde-1f72e407e610", "hash": strings.Repeat("abcdef09", 8), "maximum": strings.Repeat("AazZ09_.:@+-", 8)} {
		if !ValidReference(value) || !originalReferencePattern.MatchString(value) {
			b.Fatal("benchmark requires an accepted reference")
		}
		b.Run(name+"/ASCII", func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				referenceBenchmarkResult = ValidReference(value)
			}
		})
		b.Run(name+"/regexp", func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				referenceBenchmarkResult = originalReferencePattern.MatchString(value)
			}
		})
	}
}
