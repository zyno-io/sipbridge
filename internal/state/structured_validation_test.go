package state

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Freeze the preceding classifier independently of the optimized implementation.
func structuredWithPreliminaryValidation(data []byte) bool {
	if len(data) == 0 || data[0] != '{' && data[0] != '[' || !json.Valid(data) {
		return false
	}
	canonical, err := json.Marshal(json.RawMessage(data))
	return err == nil && bytes.Equal(canonical, data)
}

func structuredSizedObject(size int, fill byte) []byte {
	data := bytes.Repeat([]byte{fill}, size)
	copy(data, []byte(`{"x":"`))
	copy(data[size-2:], []byte(`"}`))
	return data
}

func structuredValidationCases() map[string]struct {
	data []byte
	want bool
} {
	cases := map[string]struct {
		data []byte
		want bool
	}{
		"nil":             {nil, false},
		"empty":           {[]byte{}, false},
		"object":          {[]byte(`{}`), true},
		"array":           {[]byte(`[]`), true},
		"nested":          {[]byte(`{"array":[true,null,1e3,-0,{"x":"y"}]}`), true},
		"duplicate-keys":  {[]byte(`{"x":1,"x":2}`), true},
		"escaped-slash":   {[]byte(`{"x":"\/"}`), true},
		"large-exponent":  {[]byte(`{"x":1e1000}`), true},
		"escaped-html":    {[]byte(`{"x":"\u003c\u003e\u0026\u2028\u2029"}`), true},
		"html":            {[]byte(`{"x":"<>&"}`), false},
		"unicode":         {[]byte(`{"x":"☃"}`), true},
		"line-separators": {[]byte("{\"x\":\"\u2028\u2029\"}"), false},
		"spaces":          {[]byte(`{ "x": 1 }`), false},
		"leading-space":   {[]byte(` {"x":1}`), false},
		"trailing-space":  {[]byte("{\"x\":1}\n"), false},
		"scalar":          {[]byte(`"opaque JSON scalar"`), false},
		"null":            {[]byte(`null`), false},
		"opaque":          {[]byte{0, 1, 255, '<', '&', '\n'}, false},
		"truncated":       {[]byte(`{"x":`), false},
		"trailing-json":   {[]byte(`{}[]`), false},
		"trailing-comma":  {[]byte(`[1,]`), false},
		"invalid-number":  {[]byte(`{"x":NaN}`), false},
		"invalid-escape":  {[]byte(`{"x":"\x00"}`), false},
		// Go's existing RawMessage/scanner behavior preserves these raw bytes.
		"invalid-utf8": {[]byte{'{', '"', 'x', '"', ':', '"', 0xff, '"', '}'}, true},
		"wide-array":   {[]byte("[" + strings.Repeat("0,", 10000) + "0]"), true},
	}
	for _, depth := range []int{9997, 9998, 9999, 10000, 10001} {
		cases[fmt.Sprintf("depth-%d", depth)] = struct {
			data []byte
			want bool
		}{[]byte(strings.Repeat("[", depth) + "0" + strings.Repeat("]", depth)), depth <= 10000}
	}
	for _, size := range []int{64<<10 - 1, 64 << 10, 64<<10 + 1} {
		valid := structuredSizedObject(size, 'a')
		earlyInvalid := structuredSizedObject(size, '<')
		earlyInvalid[1] = '!'
		lateInvalid := structuredSizedObject(size, '<')
		lateInvalid[len(lateInvalid)-1] = ']'
		for name, data := range map[string][]byte{
			"valid": valid, "valid-html": structuredSizedObject(size, '<'),
			"early-invalid": earlyInvalid, "late-invalid-html": lateInvalid,
		} {
			cases[fmt.Sprintf("threshold-%d-%s", size, name)] = struct {
				data []byte
				want bool
			}{data, name == "valid"}
		}
	}
	return cases
}

func assertStructuredValidationEquivalent(t testing.TB, data []byte) {
	t.Helper()
	want := structuredWithPreliminaryValidation(data)
	got := structured(data)
	if got != want {
		t.Fatalf("structured classification changed for %d bytes: got=%t want=%t", len(data), got, want)
	}
	var encoded []byte
	if want {
		encoded = append([]byte(nil), data...)
	} else {
		var err error
		encoded, err = json.Marshal(data)
		if err != nil {
			t.Fatal(err)
		}
	}
	if !bytes.Equal(encodeValue(data), encoded) {
		t.Fatal("structured classification changed the exact retained value encoding")
	}
}

func TestStructuredMarshalValidationMatchesPreliminaryScan(t *testing.T) {
	for name, sample := range structuredValidationCases() {
		t.Run(name, func(t *testing.T) {
			if got := structuredWithPreliminaryValidation(sample.data); got != sample.want {
				t.Fatalf("unexpected baseline classification: got=%t want=%t", got, sample.want)
			}
			assertStructuredValidationEquivalent(t, sample.data)
		})
	}
}

func TestStructuredLargeMalformedInputPreservesValidationAllocations(t *testing.T) {
	data := structuredSizedObject(1<<20, '<')
	data[len(data)-1] = ']'
	var accepted bool
	before := testing.AllocsPerRun(5, func() { accepted = structuredWithPreliminaryValidation(data) })
	if accepted {
		t.Fatal("the previous classifier must reject late-invalid HTML")
	}
	after := testing.AllocsPerRun(5, func() { accepted = structured(data) })
	if accepted || after > before+1 {
		t.Fatalf("large malformed input allocated beyond the old validation path: accepted=%t before=%g after=%g", accepted, before, after)
	}
}

func FuzzStructuredMarshalValidationMatchesPreliminaryScan(f *testing.F) {
	for _, sample := range structuredValidationCases() {
		f.Add(sample.data)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 256<<10 {
			t.Skip()
		}
		assertStructuredValidationEquivalent(t, data)
	})
}

func BenchmarkStructuredLateInvalidHTML(b *testing.B) {
	for _, size := range []int{64<<10 + 1, 1 << 20, 40 << 20} {
		data := structuredSizedObject(size, '<')
		data[len(data)-1] = ']'
		if structuredWithPreliminaryValidation(data) || structured(data) {
			b.Fatal("late-invalid HTML-heavy input must remain opaque")
		}
		for _, previous := range []bool{true, false} {
			profile, classify := "size-guard", structured
			if previous {
				profile, classify = "preliminary-scan", structuredWithPreliminaryValidation
			}
			b.Run(fmt.Sprintf("%d-bytes/%s", size, profile), func(b *testing.B) {
				b.ReportAllocs()
				b.SetBytes(int64(len(data)))
				for i := 0; i < b.N; i++ {
					structuredBenchmarkResult = classify(data)
				}
			})
		}
	}
}

var structuredBenchmarkResult bool

// Private native REGISTER captures are shared with BenchmarkCommitPayloadEncoding.
// No SIP/state content is printed; benchmark names contain only byte sizes.
func BenchmarkStructuredValidation(b *testing.B) {
	directory := os.Getenv("SIPBRIDGE_STATE_BENCH_RECORDS")
	if directory == "" {
		b.Skip("set SIPBRIDGE_STATE_BENCH_RECORDS to private registration captures")
	}
	for _, round := range []int{2, 3} {
		data, err := os.ReadFile(filepath.Join(directory, fmt.Sprintf("%d-registered-registration.json", round)))
		if err != nil {
			b.Fatal(err)
		}
		var capture struct {
			Record map[string]string `json:"record"`
		}
		if err := json.Unmarshal(data, &capture); err != nil {
			b.Fatal(err)
		}
		var payload Payload
		if err := payload.UnmarshalJSON([]byte(capture.Record["data"])); err != nil {
			b.Fatal(err)
		}
		assertStructuredValidationEquivalent(b, payload.ProtocolState)
		if !structured(payload.ProtocolState) {
			b.Fatal("registration protocol must be canonical structured JSON")
		}
		for _, previous := range []bool{true, false} {
			profile := "marshal-validator"
			classify := structured
			if previous {
				profile, classify = "preliminary-scan", structuredWithPreliminaryValidation
			}
			b.Run(fmt.Sprintf("%d-bytes/%s", len(payload.ProtocolState), profile), func(b *testing.B) {
				b.ReportAllocs()
				b.SetBytes(int64(len(payload.ProtocolState)))
				for i := 0; i < b.N; i++ {
					structuredBenchmarkResult = classify(payload.ProtocolState)
				}
			})
		}
	}
}
