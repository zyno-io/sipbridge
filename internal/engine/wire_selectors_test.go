package engine

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/emiago/sipgo/sip/portable"
)

type selectorWireCase struct {
	name     string
	wire     []byte
	outbound bool
}
type selectorGolden struct {
	ProjectionSHA256 string   `json:"projectionSHA256,omitempty"`
	Selectors        []string `json:"selectors,omitempty"`
	Error            string   `json:"error,omitempty"`
}

func selectorWireCases() []selectorWireCase {
	var cases []selectorWireCase
	for _, method := range []string{"INVITE", "CANCEL", "ACK", "REGISTER", "SUBSCRIBE", "NOTIFY", "BYE"} {
		for _, response := range []bool{false, true} {
			start := method + " sip:b@example.test SIP/2.0"
			to := "<sip:b@example.test>"
			if response {
				start = "SIP/2.0 200 OK"
				to += ";tag=remote"
			}
			if method == "NOTIFY" || method == "BYE" || method == "ACK" {
				to = "<sip:b@example.test>;tag=remote"
			}
			wire := []byte(start + "\r\nVia: SIP/2.0/TLS [2001:db8::10]:5061;branch=z9hG4bK-selector\r\nFrom: \"Fixture\" <sip:a@example.test>;tag=local\r\nTo: " + to + "\r\nCall-ID: selector@example.test\r\nCSeq: 7 " + method + "\r\nContact: <sip:a@[2001:db8::10]:5061;transport=tls>\r\nAuthorization: Digest username=\"fixture\", realm=\"example.test\", nonce=\"fixture\", uri=\"sip:b@example.test\", response=\"0123456789abcdef0123456789abcdef\", algorithm=MD5, qop=auth, nc=00000001, cnonce=\"fixture\"\r\nContent-Length: 4\r\n\r\nbody")
			for _, outbound := range []bool{false, true} {
				cases = append(cases, selectorWireCase{fmt.Sprintf("%s/response=%t/outbound=%t", method, response, outbound), wire, outbound})
			}
		}
	}
	valid := cases[0].wire
	for _, name := range []string{"Via", "From", "To", "Call-ID", "CSeq"} {
		lines := strings.Split(string(valid), "\r\n")
		for n, line := range lines {
			if strings.HasPrefix(line, name+":") {
				lines = append(lines[:n], lines[n+1:]...)
				break
			}
		}
		cases = append(cases, selectorWireCase{"missing/" + name, []byte(strings.Join(lines, "\r\n")), false})
	}
	cases = append(cases, selectorWireCase{"unsupported-branch", bytes.ReplaceAll(valid, []byte("z9hG4bK-selector"), []byte("legacy")), false})
	compact := bytes.ReplaceAll(valid, []byte("Call-ID:"), []byte("i:"))
	compact = bytes.ReplaceAll(compact, []byte("From:"), []byte("f:"))
	compact = bytes.ReplaceAll(compact, []byte("To:"), []byte("t:"))
	compact = bytes.ReplaceAll(compact, []byte("Via:"), []byte("v:"))
	cases = append(cases, selectorWireCase{"compact", compact, false})
	return cases
}

// Golden selectors and hashes were frozen from Observe at 3befc8d before the
// projection split. Hashes cover every public message field, not only selectors.
func TestSelectorProjectionPreservesObservedWire(t *testing.T) {
	data, err := os.ReadFile("testdata/selector-observe-3befc8d.json")
	if err != nil {
		t.Fatal(err)
	}
	var golden map[string]selectorGolden
	if err = json.Unmarshal(data, &golden); err != nil {
		t.Fatal(err)
	}
	for _, sample := range selectorWireCases() {
		t.Run(sample.name, func(t *testing.T) {
			expected, ok := golden[sample.name]
			if !ok {
				t.Fatal("missing pre-refactor oracle")
			}
			observed, fullErr := Observe(sample.wire, sample.outbound)
			selectors, err := observeSelectors(sample.wire, sample.outbound)
			r := new(stateReduction)
			memoized, memoErr := r.selectorsForWire(sample.wire, sample.outbound)
			if expected.Error != "" {
				if err == nil || fullErr == nil || memoErr == nil || err.Error() != expected.Error || fullErr.Error() != expected.Error || memoErr.Error() != expected.Error {
					t.Fatal("wire refusal changed", err, fullErr, memoErr)
				}
				return
			}
			if err != nil || fullErr != nil || memoErr != nil {
				t.Fatal(err, fullErr, memoErr)
			}
			if !reflect.DeepEqual(selectors, expected.Selectors) || !reflect.DeepEqual(memoized, expected.Selectors) {
				t.Fatal("selector identity changed")
			}
			encoded, err := json.Marshal(observed)
			if err != nil {
				t.Fatal(err)
			}
			sum := sha256.Sum256(encoded)
			if hex.EncodeToString(sum[:]) != expected.ProjectionSHA256 {
				t.Fatal("public message projection changed")
			}
		})
	}
}

func TestWireProjectionPreservesPortableValidation(t *testing.T) {
	cases := selectorWireCases()
	valid := cases[0].wire
	for name, wire := range map[string][]byte{
		"malformed":       []byte("not SIP"),
		"large-cseq":      bytes.ReplaceAll(valid, []byte("CSeq: 7 INVITE"), []byte("CSeq: 2147483648 INVITE")),
		"method-mismatch": bytes.ReplaceAll(valid, []byte("CSeq: 7 INVITE"), []byte("CSeq: 7 BYE")),
		"empty-call-id":   bytes.ReplaceAll(valid, []byte("selector@example.test"), nil),
		"missing-branch":  bytes.ReplaceAll(valid, []byte(";branch=z9hG4bK-selector"), nil),
	} {
		cases = append(cases, selectorWireCase{name: name, wire: wire})
	}
	for _, sample := range cases {
		t.Run(sample.name, func(t *testing.T) {
			for _, transport := range []string{"udp", "tcp", "tls", "", "TLS", "ws"} {
				message := portable.Message{Wire: sample.wire, Transport: transport, Source: "192.0.2.1:1234", Destination: "192.0.2.2:5061"}
				parsed, expectedErr := message.Parse()
				for _, selectorsFirst := range []bool{false, true} {
					r := new(stateReduction)
					if selectorsFirst {
						_, _ = r.selectorsForWire(sample.wire, sample.outbound)
					}
					for n := 0; n < 2; n++ {
						identity, err := r.portableIdentity(message)
						if expectedErr != nil {
							if err == nil || err.Error() != expectedErr.Error() {
								t.Fatal("portable refusal changed", transport, selectorsFirst, expectedErr, err)
							}
						} else if err != nil || identity.Method != parsed.CSeq().MethodName || identity.Sequence != parsed.CSeq().SeqNo || identity.ToTag != tag(parsed.To().Params) || identity.ViaBranch != parsed.Via().Params.GetOr("branch", "") || identity.SentBy != parsed.Via().SentBy() {
							t.Fatal("portable identity changed", transport, selectorsFirst, err)
						}
					}
					// The wire-only cache must not authorize a changed transport.
					message.Transport = "unsupported"
					if _, err := r.portableIdentity(message); err == nil {
						t.Fatal("cached wire bypassed transport validation")
					}
					message.Transport = transport
				}
			}
		})
	}
}

func TestSelectorMemoOwnsKeysAndSeparatesDirection(t *testing.T) {
	sample := selectorWireCases()[0]
	r := new(stateReduction)
	first, err := r.selectorsForWire(sample.wire, false)
	if err != nil {
		t.Fatal(err)
	}
	other, err := r.selectorsForWire(sample.wire, true)
	if err != nil {
		t.Fatal(err)
	}
	if reflect.DeepEqual(first, other) || len(r.selectorMemo) != 1 {
		t.Fatal("direction aliased")
	}
	identity, err := r.portableIdentity(portable.Message{Wire: sample.wire, Transport: "tls"})
	if err != nil || len(r.selectorMemo) != 1 || r.selectorMemoBytes != len(sample.wire) {
		t.Fatal("portable identity did not share the bounded wire key", err)
	}
	original := bytes.Clone(sample.wire)
	clear(sample.wire)
	again, err := r.selectorsForWire(original, false)
	if err != nil || !reflect.DeepEqual(first, again) || len(r.selectorMemo) != 1 {
		t.Fatal("memo key aliases mutable input")
	}
	got, err := r.portableIdentity(portable.Message{Wire: original, Transport: "tls"})
	if err != nil || got != identity {
		t.Fatal("scalar identity aliases mutable input", err)
	}
	r.clearSelectorMemo()
	if r.selectorMemo != nil || r.selectorMemoBytes != 0 {
		t.Fatal("memo survived rollback")
	}
}

func TestSelectorMemoBudgetFallsBackWithoutChangingKeys(t *testing.T) {
	sample := selectorWireCases()[0]
	expected, err := observeSelectors(sample.wire, false)
	if err != nil {
		t.Fatal(err)
	}
	for _, budget := range []string{"entries", "bytes", "remaining-bytes"} {
		t.Run(budget, func(t *testing.T) {
			r := new(stateReduction)
			switch budget {
			case "entries":
				r.selectorMemo = make(map[string]*wireProjection)
				for n := 0; n < stateSelectorMemoEntries; n++ {
					r.selectorMemo[fmt.Sprint(n)] = new(wireProjection)
				}
			case "bytes":
				r.selectorMemoBytes = stateSelectorMemoBytes
			case "remaining-bytes":
				r.selectorMemoBytes = stateSelectorMemoBytes - len(sample.wire) + 1
			}
			size := len(r.selectorMemo)
			retained := r.selectorMemoBytes
			for n := 0; n < 2; n++ {
				got, err := r.selectorsForWire(sample.wire, false)
				if err != nil || !reflect.DeepEqual(got, expected) {
					t.Fatal("budget changed selector semantics", err)
				}
				message := portable.Message{Wire: sample.wire, Transport: "tls"}
				identity, err := r.portableIdentity(message)
				parsed, parseErr := message.Parse()
				if err != nil || parseErr != nil || identity != projectedWireIdentity(parsed) {
					t.Fatal("budget changed portable identity", err, parseErr)
				}
			}
			if len(r.selectorMemo) != size || r.selectorMemoBytes != retained {
				t.Fatal("memo exceeded budget")
			}
		})
	}
}

var selectorBenchmarkResult []string

func BenchmarkSelectorProjection(b *testing.B) {
	wire := selectorWireCases()[0].wire
	b.Run("controller", func(b *testing.B) {
		b.ReportAllocs()
		for n := 0; n < b.N; n++ {
			message, err := Observe(wire, false)
			if err != nil {
				b.Fatal(err)
			}
			selectorBenchmarkResult = message.Selectors
		}
	})
	b.Run("selectors", func(b *testing.B) {
		b.ReportAllocs()
		for n := 0; n < b.N; n++ {
			selectors, err := observeSelectors(wire, false)
			if err != nil {
				b.Fatal(err)
			}
			selectorBenchmarkResult = selectors
		}
	})
	b.Run("reduction-rebuild", func(b *testing.B) {
		b.ReportAllocs()
		for n := 0; n < b.N; n++ {
			r := new(stateReduction)
			for count := 0; count < 4; count++ {
				selectors, err := r.selectorsForWire(wire, false)
				if err != nil {
					b.Fatal(err)
				}
				selectorBenchmarkResult = selectors
			}
		}
	})
}
