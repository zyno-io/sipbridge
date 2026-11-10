package state

import (
	"bytes"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
)

type retainedRecordJSON struct {
	inc, revision, data, receipts, pending, callID, proof string
}

func retainedDecodeFixture(t testing.TB) (*Store, Ref, retainedRecordJSON) {
	t.Helper()
	s := payloadValidationStore()
	ref, err := s.GroupForCallID("retained-decode@example.test")
	if err != nil {
		t.Fatal(err)
	}
	return s, ref, retainedRecordJSON{inc: "incarnation", revision: "1", data: "{}", receipts: "[]", callID: ref.CallID}
}

func decodeRetainedRecord(s *Store, ref Ref, fields retainedRecordJSON) (Record, error) {
	return s.decodeRecord(ref, fields.inc, fields.revision, fields.data, fields.receipts, fields.pending, fields.callID, fields.proof)
}

// Preserve the outer JSON scan as a differential decoder oracle.
func decodeRecordWithOuterScan(s *Store, ref Ref, fields retainedRecordJSON) (Record, error) {
	ref.Incarnation, ref.CallID = fields.inc, fields.callID
	if err := s.validateRef(ref); err != nil {
		return Record{}, ErrCorrupt
	}
	record := Record{Ref: ref, Authority: s.authority, Versions: s.cfg.Versions, Revision: fields.revision}
	if json.Unmarshal([]byte(fields.data), &record.Payload) != nil || json.Unmarshal([]byte(fields.receipts), &record.Receipts) != nil {
		return Record{}, ErrCorrupt
	}
	if fields.proof != "" {
		record.Proof = &Receipt{}
		if json.Unmarshal([]byte(fields.proof), record.Proof) != nil {
			return Record{}, ErrCorrupt
		}
	}
	if fields.pending != "" {
		record.Pending = &Input{}
		if json.Unmarshal([]byte(fields.pending), record.Pending) != nil {
			return Record{}, ErrCorrupt
		}
	}
	if err := s.validatePayloadStructure(record.Payload); err != nil {
		return Record{}, ErrCorrupt
	}
	if err := s.validatePayloadSize(record.Payload, len(fields.data)); err != nil {
		return Record{}, ErrCorrupt
	}
	return record, nil
}

func TestRetainedPayloadDirectDecodeMatchesOuterUnmarshal(t *testing.T) {
	effect := `{"id":"effect","kind":"wire","status":"pending","deadlineMs":1,"payload":{"wire":"U0lQ"}}`
	for _, sample := range []struct {
		name, data string
		valid      bool
	}{
		{"empty", "", false},
		{"whitespace-only", " \t\r\n", false},
		{"null", "null", true},
		{"surrounded-null", " \n null \t", true},
		{"empty-object", "{}", true},
		{"surrounded-object", " \r\n {} \t", true},
		{"array-root", "[]", false},
		{"string-root", `"value"`, false},
		{"number-root", "12", false},
		{"boolean-root", "true", false},
		{"truncated", `{"activeTransactions":`, false},
		{"trailing-value", "{} null", false},
		{"trailing-junk", "{}x", false},
		{"trailing-comment", "{}/*comment*/", false},
		{"structured", `{"protocolState":{"wire":"U0lQ","nested":[true,null,12]}}`, true},
		{"structured-array", `{"protocolState":[{"wire":"U0lQ"}]}`, true},
		{"noncanonical-structured", "{ \"protocolState\" : { \"value\" : \"<>&☃\" } }\n", true},
		{"noncanonical-base64", "{ \"protocolState\" : \"b2xkIGJ5dGVz\" }", true},
		{"opaque", `{"protocolState":"AAH/"}`, true},
		{"empty-opaque", `{"protocolState":""}`, true},
		{"null-protocol", `{"protocolState":null}`, true},
		{"invalid-base64", `{"protocolState":"%%%"}`, false},
		{"invalid-protocol-type", `{"protocolState":12}`, false},
		{"duplicate-protocol", `{"protocolState":12,"protocolState":{"last":true}}`, true},
		{"duplicate-count", `{"activeTransactions":1,"activeTransactions":2}`, true},
		{"duplicate-invalid-count", `{"activeTransactions":"invalid","activeTransactions":2}`, false},
		{"case-insensitive-field", `{"ActiveTransactions":2}`, true},
		{"unknown-field", `{"future":{"nested":[1,2,3]}}`, true},
		{"unicode", `{"protocolState":{"text":"☃\u003c\ud800"}}`, true},
		{"raw-invalid-utf8", "{\"protocolState\":{\"text\":\"\xff\"}}", true},
		{"unknown-invalid-utf8", "{\"future\":\"\xff\"}", true},
		{"invalid-utf8-outside-string", "{\xff}", false},
		{"byte-order-mark", "\xef\xbb\xbf{}", false},
		{"unescaped-control", "{\"protocolState\":{\"text\":\"\n\"}}", false},
		{"custom-effect", `{"effects":[` + effect + `]}`, true},
		{"null-effects", `{"effects":null}`, true},
		{"empty-effects", `{"effects":[]}`, true},
		{"duplicate-effects-field", `{"effects":[],"effects":[` + effect + `]}`, true},
		{"duplicate-effect-identity", `{"effects":[` + effect + `,` + effect + `]}`, false},
		{"null-effect", `{"effects":[null]}`, false},
		{"bad-effect-base64", `{"effects":[{"id":"e","kind":"wire","status":"pending","deadlineMs":1,"payload":"%%%"}]}`, false},
		{"duplicate-effect-payload", `{"effects":[{"id":"e","kind":"wire","status":"pending","deadlineMs":1,"payload":12,"payload":{}}]}`, true},
		{"bad-effect-field-type", `{"effects":[{"id":3}]}`, false},
		{"invalid-effect-utf8", "{\"effects\":[{\"id\":\"\xff\",\"kind\":\"wire\",\"status\":\"pending\",\"deadlineMs\":1}]}", false},
		{"missing-attempt", `{"effects":[{"id":"e","kind":"wire","status":"dispatching","deadlineMs":1}]}`, false},
		{"negative-count", `{"activeTransactions":-1}`, false},
		{"fractional-count", `{"activeTransactions":1.0}`, false},
		{"count-overflow", `{"activeTransactions":9999999999999999999999}`, false},
		{"negative-reserve", `{"reservedCompletionBytes":-1}`, false},
		{"oversized-reserve", `{"reservedCompletionBytes":67108864}`, false},
		{"invalid-timer", `{"timers":[{"id":"t","generation":"1","dueMs":2,"deadlineMs":1}]}`, false},
		{"too-deep", `{"future":` + strings.Repeat("[", 10001) + "0" + strings.Repeat("]", 10001) + "}", false},
	} {
		t.Run(sample.name, func(t *testing.T) {
			s, ref, fields := retainedDecodeFixture(t)
			fields.data = sample.data
			want, wantErr := decodeRecordWithOuterScan(s, ref, fields)
			got, gotErr := decodeRetainedRecord(s, ref, fields)
			if (wantErr == nil) != sample.valid || (gotErr == nil) != sample.valid || !reflect.DeepEqual(want, got) {
				t.Fatalf("direct decoder differs: valid=%t outer=%v direct=%v", sample.valid, wantErr, gotErr)
			}
			if !sample.valid && (!errors.Is(gotErr, ErrCorrupt) || !reflect.DeepEqual(got, Record{})) {
				t.Fatal("malformed state exposed a partial record or lost corruption classification", gotErr)
			}
		})
	}
	for name, payload := range payloadValidationCases() {
		t.Run("encoded-"+name, func(t *testing.T) {
			s, ref, fields := retainedDecodeFixture(t)
			encoded, err := json.Marshal(payload)
			if err != nil {
				t.Fatal(err)
			}
			fields.data = string(encoded)
			want, wantErr := decodeRecordWithOuterScan(s, ref, fields)
			got, gotErr := decodeRetainedRecord(s, ref, fields)
			if wantErr != nil || gotErr != nil || !reflect.DeepEqual(want, got) || !bytes.Equal(got.Payload.ProtocolState, payload.ProtocolState) {
				t.Fatalf("structured/opaque custom values changed: outer=%v direct=%v", wantErr, gotErr)
			}
		})
	}
}

func TestRetainedDirectDecodePreservesOtherRecordChecks(t *testing.T) {
	for _, field := range []string{"incarnation", "call-id", "receipts", "pending", "proof"} {
		t.Run(field, func(t *testing.T) {
			s, ref, fields := retainedDecodeFixture(t)
			switch field {
			case "incarnation":
				fields.inc = "invalid incarnation"
			case "call-id":
				fields.callID = "another-call@example.test"
			case "receipts":
				fields.receipts = "{}"
			case "pending":
				fields.pending = "{"
			case "proof":
				fields.proof = "{"
			}
			want, wantErr := decodeRecordWithOuterScan(s, ref, fields)
			got, gotErr := decodeRetainedRecord(s, ref, fields)
			if !errors.Is(wantErr, ErrCorrupt) || !errors.Is(gotErr, ErrCorrupt) || !reflect.DeepEqual(want, got) {
				t.Fatalf("auxiliary corruption check changed: outer=%v direct=%v", wantErr, gotErr)
			}
		})
	}
}

func FuzzRetainedPayloadDecodeEquivalence(f *testing.F) {
	for _, seed := range []string{"", "null", "{}", "{} null", `{"protocolState":{"wire":"U0lQ"}}`,
		`{"effects":[{"id":"effect","kind":"wire","status":"pending","deadlineMs":1,"payload":{"wire":"U0lQ"}}]}`} {
		f.Add([]byte(seed))
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		s, ref, fields := retainedDecodeFixture(t)
		fields.data = string(data)
		want, wantErr := decodeRecordWithOuterScan(s, ref, fields)
		got, gotErr := decodeRetainedRecord(s, ref, fields)
		if (wantErr == nil) != (gotErr == nil) || !reflect.DeepEqual(want, got) {
			t.Fatalf("direct decoder differs: outer=%v direct=%v", wantErr, gotErr)
		}
		if gotErr != nil && !errors.Is(gotErr, ErrCorrupt) {
			t.Fatal("retained corruption changed its error class", gotErr)
		}
	})
}
