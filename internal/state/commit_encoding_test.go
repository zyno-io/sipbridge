package state

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

// Preserve the preceding commit encoder as the byte-for-byte and exact-error
// oracle, including each nested MarshalerError and SyntaxError in the chain.
func assertCommitPayloadEncoding(t testing.TB, payload Payload) []byte {
	t.Helper()
	want, wantErr := json.Marshal(payload)
	got, gotErr := marshalCommitPayload(payload)
	if !reflect.DeepEqual(gotErr, wantErr) || !bytes.Equal(got, want) {
		t.Fatalf("payload encoding changed: outer=%v direct=%v; outer-bytes=%d direct-bytes=%d", wantErr, gotErr, len(want), len(got))
	}
	if gotErr != nil {
		return nil
	}
	if !json.Valid(got) {
		t.Fatal("commit encoding returned invalid JSON")
	}
	var decoded Payload
	if err := decoded.UnmarshalJSON(got); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(decoded.ProtocolState, payload.ProtocolState) || len(decoded.Effects) != len(payload.Effects) {
		t.Fatal("encoding changed reducer values")
	}
	for i, effect := range payload.Effects {
		if !bytes.Equal(decoded.Effects[i].Payload, effect.Payload) {
			t.Fatal("encoding changed an effect's reducer bytes")
		}
	}
	return got
}

func commitEncodingCases() map[string]Payload {
	cases := payloadValidationCases()
	for name, data := range map[string][]byte{
		"array":             []byte(`[{},[],true,null,1e3,-0]`),
		"duplicate-keys":    []byte(`{"x":1,"x":2}`),
		"truncated-object":  []byte(`{"wire":`),
		"truncated-array":   []byte(`[1,`),
		"trailing-json":     []byte(`{}[]`),
		"invalid-number":    []byte(`{"x":NaN}`),
		"invalid-escape":    []byte(`{"x":"\x00"}`),
		"invalid-utf8":      []byte{'{', '"', 'x', '"', ':', '"', 0xff, '"', '}'},
		"excessive-nesting": []byte(strings.Repeat("[", 10_001) + strings.Repeat("]", 10_001)),
	} {
		payload := cases["structured"]
		payload.ProtocolState = data
		payload.Effects = append([]Effect(nil), payload.Effects...)
		payload.Effects[0].Payload = data
		cases[name] = payload
	}
	return cases
}

func TestCommitPayloadDirectEncodingMatchesOuterMarshal(t *testing.T) {
	for name, payload := range commitEncodingCases() {
		t.Run(name, func(t *testing.T) {
			assertCommitPayloadEncoding(t, payload)
		})
	}
}

func TestCommitPayloadEncodingPreservesEnclosingDepthLimit(t *testing.T) {
	for _, sample := range []struct {
		name     string
		depth    int
		effect   bool
		accepted bool
	}{
		{"protocol-at-document-limit", 9999, false, true},
		{"protocol-over-document-limit", 10000, false, false},
		{"effect-at-document-limit", 9997, true, true},
		{"effect-over-document-limit", 9998, true, false},
		{"effect-inner-marshal-error", 10000, true, false},
	} {
		t.Run(sample.name, func(t *testing.T) {
			nested := []byte(strings.Repeat("[", sample.depth) + "0" + strings.Repeat("]", sample.depth))
			if !json.Valid(nested) || !structured(nested) {
				t.Fatal("the reducer value must independently satisfy the JSON depth limit")
			}
			payload := Payload{ProtocolState: nested}
			if sample.effect {
				payload = Payload{Effects: []Effect{{ID: "effect", Kind: "wire", Status: EffectPending, DeadlineMS: 1, Payload: nested}}}
			}
			_, err := json.Marshal(payload)
			if (err == nil) != sample.accepted {
				t.Fatalf("unexpected baseline depth acceptance: %v", err)
			}
			assertCommitPayloadEncoding(t, payload)
		})
	}
}

func TestCommitPayloadDirectEncodingPreservesExactCapacity(t *testing.T) {
	for name, payload := range commitEncodingCases() {
		t.Run(name, func(t *testing.T) {
			// Lease IDs consume a second bounded projection in Redis; commit-only
			// guards/fences remain excluded from the retained payload encoding.
			payload.ApplicationLeaseIDs = []string{"owner"}
			lease := ApplicationLeaseRef{ID: "owner", Term: "11111111-1111-4111-8111-111111111111"}
			payload.CommitLeaseGuards = []ApplicationLeaseRef{lease}
			payload.CommitLeaseFences = []ApplicationLeaseRef{lease}
			encoded := assertCommitPayloadEncoding(t, payload)
			projection, err := json.Marshal(payload.ApplicationLeaseIDs)
			if err != nil {
				t.Fatal(err)
			}
			s := payloadValidationStore()
			if err := s.validatePayloadStructure(payload); err != nil {
				t.Fatal(err)
			}
			boundary := len(encoded) + len(projection) + payload.ReservedCompletionBytes + 4096
			for _, delta := range []int{-1, 0, 1} {
				s.cfg.Limits.GroupBytes = boundary + delta
				err := s.validatePayloadSize(payload, len(encoded))
				if delta < 0 && !errors.Is(err, ErrCapacity) || delta >= 0 && err != nil {
					t.Fatalf("capacity boundary changed: delta=%d error=%v", delta, err)
				}
			}
			payload.ReservedCompletionBytes = int(^uint(0) >> 1)
			encoded = assertCommitPayloadEncoding(t, payload)
			if err := s.validatePayloadSize(payload, len(encoded)); !errors.Is(err, ErrCapacity) {
				t.Fatalf("overflowing completion reserve admitted: %v", err)
			}
		})
	}
}

func TestStateCommitDirectEncodingStoresIdenticalBytes(t *testing.T) {
	s := testStore(t, nil)
	readyStore(t, s)
	for name, payload := range commitEncodingCases() {
		t.Run(name, func(t *testing.T) {
			encoded := assertCommitPayloadEncoding(t, payload)
			step := begin(t, s, group(t, s, "direct-encoding-"+name), "initial", true)
			if _, err := s.Commit(context.Background(), *step, payload, []byte("completed")); err != nil {
				t.Fatal(err)
			}
			stored, err := s.client.HGet(context.Background(), s.keys(step.Record.Ref)[0], "data").Bytes()
			if err != nil || !bytes.Equal(stored, encoded) {
				t.Fatalf("commit changed canonical stored bytes: %v", err)
			}
		})
	}
}

func TestStateCommitEncodingDepthRefusalPreservesRecordAndClaim(t *testing.T) {
	s := testStore(t, nil)
	readyStore(t, s)
	for _, sample := range []struct {
		name   string
		depth  int
		effect bool
	}{
		{"protocol", 10000, false},
		{"effect", 9998, true},
		{"inner-effect", 10000, true},
	} {
		t.Run(sample.name, func(t *testing.T) {
			nested := []byte(strings.Repeat("[", sample.depth) + "0" + strings.Repeat("]", sample.depth))
			payload := Payload{ProtocolState: nested}
			if sample.effect {
				payload = Payload{Effects: []Effect{{ID: "effect", Kind: "wire", Status: EffectPending, DeadlineMS: 1, Payload: nested}}}
			}
			_, wantErr := json.Marshal(payload)
			if wantErr == nil {
				t.Fatal("baseline must reject excess enclosing depth")
			}
			step := begin(t, s, group(t, s, "rejected-depth-"+sample.name), "initial", true)
			key := s.keys(step.Record.Ref)[0]
			before, err := s.client.HGetAll(context.Background(), key).Result()
			if err != nil {
				t.Fatal(err)
			}
			if _, err := s.Commit(context.Background(), *step, payload, []byte("must-not-commit")); !reflect.DeepEqual(err, wantErr) {
				t.Fatalf("depth refusal changed: got=%v want=%v", err, wantErr)
			}
			after, err := s.client.HGetAll(context.Background(), key).Result()
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Fatalf("refused encoding changed the retained record or claim: %v", err)
			}
		})
	}
}

func FuzzCommitPayloadDirectEncodingMatchesOuterMarshal(f *testing.F) {
	for _, payload := range commitEncodingCases() {
		f.Add(payload.ProtocolState, payload.ProtocolState, "attempt", 1, 1234)
	}
	f.Add([]byte(`{"html":"<>&"}`), []byte(`{"escaped":"\u003c\u2028"}`), "\x00<>&\u2028☃\xff", -1, -1)
	f.Add([]byte(`[]`), []byte(`{}`), "attempt", 4097, int(^uint(0)>>1))
	for _, depth := range []int{9997, 9998, 9999, 10000, 10001} {
		nested := []byte(strings.Repeat("[", depth) + "0" + strings.Repeat("]", depth))
		f.Add(nested, []byte(`{}`), "attempt", 1, 1234)
		f.Add([]byte(`{}`), nested, "attempt", 1, 1234)
	}
	f.Fuzz(func(t *testing.T, protocol, effect []byte, attempt string, transactions, reserve int) {
		if len(protocol) > 65536 || len(effect) > 65536 || len(attempt) > 16384 {
			t.Skip()
		}
		payload := Payload{ProtocolState: protocol, ActiveTransactions: transactions, ReservedCompletionBytes: reserve,
			Effects: []Effect{{ID: "effect", Kind: "wire", Status: EffectPending, DeadlineMS: 1, AttemptID: attempt, Payload: effect}},
			Timers:  []Timer{{ID: "timer", Generation: "1", DueMS: 1, DeadlineMS: 1}}, ApplicationLeaseIDs: []string{"owner"}}
		assertCommitPayloadEncoding(t, payload)
	})
}

// Like BenchmarkStateCapturedRecord, this reads private, uncommitted captures
// through SIPBRIDGE_STATE_BENCH_RECORDS. Output contains only byte sizes/timings.
// The registered snapshots retain native REGISTER request/response procedures
// and their completion metadata; rounds two/three exercise 35–56 KB aggregates.
func BenchmarkCommitPayloadEncoding(b *testing.B) {
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
		encoded := assertCommitPayloadEncoding(b, payload)
		for _, direct := range []bool{false, true} {
			profile := "outer-json-scan"
			if direct {
				profile = "validated-custom-encoder"
			}
			b.Run(fmt.Sprintf("%d-bytes/%s", len(encoded), profile), func(b *testing.B) {
				b.ReportAllocs()
				b.SetBytes(int64(len(encoded)))
				for i := 0; i < b.N; i++ {
					var result []byte
					var err error
					if direct {
						result, err = marshalCommitPayload(payload)
					} else {
						result, err = json.Marshal(payload)
					}
					if err != nil {
						b.Fatal(err)
					}
					capturedBenchmarkBytes = result
				}
			})
		}
	}
}
