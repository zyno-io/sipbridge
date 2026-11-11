package state

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

func payloadValidationStore() *Store {
	return &Store{metadata: metadata{cfg: Config{Namespace: "capacity", NamespaceKey: bytes.Repeat([]byte{9}, 32), Partitions: 8,
		Limits: Limits{GroupBytes: 64 << 20, Effects: 4096, Timers: 4096, Transactions: 4096}}}}
}

// Keep the preceding exact serialization check as a differential oracle. The
// structural checks are unchanged; malformed cases below assert their classes.
func exactPayloadValidation(s *Store, payload Payload) error {
	if err := s.validatePayloadStructure(payload); err != nil {
		return err
	}
	data, err := payload.MarshalBinary()
	if err != nil {
		return err
	}
	return s.validatePayloadSize(payload, len(data))
}

func TestRetainedRecordUsesRawEncodedByteBoundary(t *testing.T) {
	s, ref, fields := retainedDecodeFixture(t)
	payload := Payload{ProtocolState: bytes.Repeat([]byte{'x'}, 5000), ReservedCompletionBytes: 512,
		ApplicationLeaseIDs: []string{"owner"}}
	encoded, err := payload.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	projection, err := json.Marshal(payload.ApplicationLeaseIDs)
	if err != nil {
		t.Fatal(err)
	}
	fields.data = string(encoded)
	fields.receipts = "[]"
	boundary := len(encoded) + len(projection) + payload.ReservedCompletionBytes + 4096
	s.cfg.Limits.GroupBytes = boundary
	if _, err := decodeRetainedRecord(s, ref, fields); err != nil {
		t.Fatalf("exact retained boundary rejected: %v", err)
	}
	s.cfg.Limits.GroupBytes = boundary - 1
	if _, err := decodeRetainedRecord(s, ref, fields); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("one-byte-over retained record accepted: %v", err)
	}
}

func TestRetainedRecordRejectsOversizedNoncanonicalRawJSON(t *testing.T) {
	s, ref, fields := retainedDecodeFixture(t)
	canonical := `{"protocolState":{"x":1}}`
	fields.data = strings.Repeat(" ", 4097) + canonical
	s.cfg.Limits.GroupBytes = 8192
	if _, err := decodeRetainedRecord(s, ref, fields); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("oversized raw encoding accepted after normalization: %v", err)
	}
}

func payloadValidationCases() map[string]Payload {
	values := map[string][]byte{
		"nil":          nil,
		"empty":        {},
		"structured":   []byte(`{"wire":"U0lQIHdpcmU=","nested":[true,null,12]}`),
		"html":         []byte(`{"value":"<>&"}`),
		"escaped":      []byte(`{"value":"\u003c\u003e\u0026\u2028\u2029"}`),
		"noncanonical": []byte("{ \"value\": \"spaces\" }\n"),
		"base64":       []byte(`"YWxyZWFkeSBlbmNvZGVk"`),
		"null":         []byte("null"),
		"unicode":      []byte("{\"value\":\"\u2028\u2029☃\"}"),
		"opaque":       {0, 1, 255, '<', '&', '\n'},
	}
	cases := make(map[string]Payload)
	for name, value := range values {
		cases[name] = Payload{ProtocolState: value, ActiveTransactions: 1, ReservedCompletionBytes: 1234,
			Effects: []Effect{{ID: "effect", Kind: "wire", Status: EffectPending, DeadlineMS: 1, Payload: value}},
			Timers:  []Timer{{ID: "timer", Generation: "1", DueMS: 1, DeadlineMS: 1}}}
	}
	cases["empty-payload"] = Payload{}
	cases["wide-metadata"] = Payload{ProtocolState: values["structured"],
		Effects: []Effect{{ID: strings.Repeat("x", 96), Kind: strings.Repeat("y", 96), Status: EffectPending,
			DeadlineMS: 1<<53 - 1, AttemptID: strings.Repeat("<>&\u2028☃", 2048), Payload: values["html"]}},
		Timers:    []Timer{{ID: strings.Repeat("t", 96), Generation: "9223372036854775807", DueMS: 1<<53 - 1, DeadlineMS: 1<<53 - 1}},
		Retention: Retention{Terminal: true, UntilMS: 1<<53 - 1}}
	return cases
}

func TestWriterPayloadCapacityMatchesExactEncoding(t *testing.T) {
	for name, payload := range payloadValidationCases() {
		t.Run(name, func(t *testing.T) {
			s := payloadValidationStore()
			before, err := payload.MarshalBinary()
			if err != nil {
				t.Fatal(err)
			}
			boundary := len(before) + payload.ReservedCompletionBytes + 4096
			for _, limit := range []int{boundary - 1, boundary, boundary + 1, 64 << 20} {
				s.cfg.Limits.GroupBytes = limit
				got := s.validatePayloadSize(payload, len(before))
				if limit == boundary-1 && !errors.Is(got, ErrCapacity) || limit >= boundary && got != nil {
					t.Fatalf("limit=%d writer capacity result=%v", limit, got)
				}
			}
			after, err := payload.MarshalBinary()
			if err != nil || !bytes.Equal(before, after) {
				t.Fatal("capacity measurement changed the payload")
			}
		})
	}
}

func errorText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func TestPayloadCapacityRejectsReserveOverflow(t *testing.T) {
	s := payloadValidationStore()
	payload := payloadValidationCases()["structured"]
	encoded, err := payload.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	for _, reserve := range []int{int(^uint(0) >> 1), int(^uint(0)>>1) - 4096, s.cfg.Limits.GroupBytes - 4095} {
		payload.ReservedCompletionBytes = reserve
		if err := s.validatePayloadSize(payload, len(encoded)); !errors.Is(err, ErrCapacity) {
			t.Fatalf("exact accounting admitted oversized reserve: %v", err)
		}
		data, err := payload.MarshalBinary()
		if err != nil {
			t.Fatal(err)
		}
		ref, err := s.GroupForCallID("reserve-overflow")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := s.decodeRecord(ref, "incarnation", "1", string(data), "[]", "", ref.CallID, ""); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("loaded overflowing reserve: %v", err)
		}
	}
}

func TestPayloadCapacityPreservesStructuralRefusals(t *testing.T) {
	cases := []struct {
		name   string
		change func(*Payload)
		want   error
	}{
		{"negative-transactions", func(p *Payload) { p.ActiveTransactions = -1 }, ErrCapacity},
		{"transaction-limit", func(p *Payload) { p.ActiveTransactions = 4097 }, ErrCapacity},
		{"effect-limit", func(p *Payload) { p.Effects = make([]Effect, 4097) }, ErrCapacity},
		{"timer-limit", func(p *Payload) { p.Timers = make([]Timer, 4097) }, ErrCapacity},
		{"negative-reserve", func(p *Payload) { p.ReservedCompletionBytes = -1 }, ErrCapacity},
		{"negative-retention", func(p *Payload) { p.Retention.UntilMS = -1 }, ErrCapacity},
		{"retention-limit", func(p *Payload) { p.Retention.UntilMS = 1 << 53 }, ErrCapacity},
		{"effect-identity", func(p *Payload) { p.Effects[0].ID = "invalid id" }, ErrCorrupt},
		{"effect-kind", func(p *Payload) { p.Effects[0].Kind = "invalid kind" }, ErrCorrupt},
		{"duplicate-effect", func(p *Payload) { p.Effects = append(p.Effects, p.Effects[0]) }, ErrCorrupt},
		{"effect-status", func(p *Payload) { p.Effects[0].Status = "invalid" }, ErrCorrupt},
		{"effect-deadline", func(p *Payload) { p.Effects[0].DeadlineMS = 0 }, ErrCorrupt},
		{"effect-deadline-limit", func(p *Payload) { p.Effects[0].DeadlineMS = 1 << 53 }, ErrCorrupt},
		{"missing-attempt", func(p *Payload) { p.Effects[0].Status = EffectWritten }, ErrCorrupt},
		{"invalid-attempt", func(p *Payload) { p.Effects[0].Status, p.Effects[0].AttemptID = EffectUnknown, "invalid attempt" }, ErrCorrupt},
		{"timer-identity", func(p *Payload) { p.Timers[0].ID = "invalid id" }, ErrCorrupt},
		{"duplicate-timer", func(p *Payload) { p.Timers = append(p.Timers, p.Timers[0]) }, ErrCorrupt},
		{"timer-generation", func(p *Payload) { p.Timers[0].Generation = "01" }, ErrCorrupt},
		{"timer-generation-limit", func(p *Payload) { p.Timers[0].Generation = "9223372036854775808" }, ErrCorrupt},
		{"timer-due", func(p *Payload) { p.Timers[0].DueMS = 0 }, ErrCorrupt},
		{"timer-deadline", func(p *Payload) { p.Timers[0].DeadlineMS = 0 }, ErrCorrupt},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			payload := payloadValidationCases()["structured"]
			tc.change(&payload)
			s := payloadValidationStore()
			got := s.validatePayloadStructure(payload)
			if !errors.Is(got, tc.want) {
				t.Fatalf("wrong refusal: got=%v want=%v", got, tc.want)
			}
			data, err := payload.MarshalBinary()
			if err != nil {
				t.Fatal(err)
			}
			ref, err := s.GroupForCallID("capacity-test")
			if err != nil {
				t.Fatal(err)
			}
			if _, err := s.decodeRecord(ref, "incarnation", "1", string(data), "[]", "", ref.CallID, ""); !errors.Is(err, ErrCorrupt) {
				t.Fatalf("loaded malformed payload: %v", err)
			}
		})
	}
}

func FuzzRetainedRecordRawByteBoundary(f *testing.F) {
	for _, payload := range payloadValidationCases() {
		f.Add(payload.ProtocolState, "attempt")
	}
	f.Add([]byte(`{"value":"<>&"}`), "\x00<>&\u2028☃\xff")
	f.Fuzz(func(t *testing.T, value []byte, attempt string) {
		if len(value) > 65536 || len(attempt) > 65536 {
			t.Skip()
		}
		payload := Payload{ProtocolState: value, ReservedCompletionBytes: 16,
			Effects: []Effect{{ID: "effect", Kind: "wire", Status: EffectPending, DeadlineMS: 1, AttemptID: attempt, Payload: value}}}
		data, err := payload.MarshalBinary()
		if err != nil {
			return
		}
		s := payloadValidationStore()
		ref, err := s.GroupForCallID("fuzz-boundary")
		if err != nil {
			t.Fatal(err)
		}
		s.cfg.Limits.GroupBytes = len(data) + 4096 + payload.ReservedCompletionBytes
		_, gotErr := s.decodeRecord(ref, "incarnation", "1", string(data), "[]", "", ref.CallID, "")
		if structuralErr := s.validatePayloadStructure(payload); structuralErr != nil {
			if !errors.Is(gotErr, ErrCorrupt) {
				t.Fatalf("malformed retained payload accepted: %v", gotErr)
			}
			return
		}
		if gotErr != nil {
			t.Fatalf("valid exact-boundary payload refused: %v", gotErr)
		}
		_, overErr := s.decodeRecord(ref, "incarnation", "1", string(data)+" ", "[]", "", ref.CallID, "")
		if !errors.Is(overErr, ErrCorrupt) {
			t.Fatalf("over-bound retained bytes accepted: %v", overErr)
		}
	})
}

func TestStatePayloadSizingPreservesBinaryAndRefusesLegacyRecords(t *testing.T) {
	s := testStore(t, nil)
	readyStore(t, s)
	ctx := context.Background()
	ref := group(t, s, "capacity-encoding")
	step := begin(t, s, ref, "initial", true)
	payload := payloadValidationCases()["structured"]
	encoded, err := payload.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Commit(ctx, *step, payload, []byte("completed")); err != nil {
		t.Fatal(err)
	}
	stored, err := s.client.HGet(ctx, s.keys(ref)[0], "data").Result()
	if err != nil || stored != string(encoded) {
		t.Fatalf("commit changed canonical bytes: %v", err)
	}
	record, err := s.Load(ctx, ref)
	if err != nil || !bytes.Equal(record.Payload.ProtocolState, payload.ProtocolState) {
		t.Fatalf("canonical record could not be read: %v", err)
	}
	var legacy map[string]json.RawMessage
	diagnostic, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(diagnostic, &legacy); err != nil {
		t.Fatal(err)
	}
	legacy["protocolState"], err = json.Marshal(base64.StdEncoding.EncodeToString(payload.ProtocolState))
	if err != nil {
		t.Fatal(err)
	}
	legacyBytes, err := json.Marshal(legacy)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.client.HSet(ctx, s.keys(ref)[0], "data", legacyBytes).Err(); err != nil {
		t.Fatal(err)
	}
	record, err = s.Load(ctx, ref)
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("legacy JSON record accepted: %v", err)
	}
	if err := s.client.HSet(ctx, s.keys(ref)[0], "data", encoded).Err(); err != nil {
		t.Fatal(err)
	}
	step = begin(t, s, ref, "oversized-reserve", false)
	tooLarge := payload
	tooLarge.ReservedCompletionBytes = int(^uint(0) >> 1)
	if _, err := s.Commit(ctx, *step, tooLarge, nil); !errors.Is(err, ErrCapacity) {
		t.Fatalf("commit admitted overflowing reserve: %v", err)
	}
	stored, err = s.client.HGet(ctx, s.keys(ref)[0], "data").Result()
	if err != nil || stored != string(encoded) {
		t.Fatalf("refused reserve changed committed bytes: %v", err)
	}
	payload.Effects = append(payload.Effects, payload.Effects[0])
	corrupt, err := payload.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	if err := s.client.HSet(ctx, s.keys(ref)[0], "data", corrupt).Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Load(ctx, ref); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("corrupt record accepted: %v", err)
	}
}
