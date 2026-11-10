package state

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

var capturedBenchmarkRecord Record
var capturedBenchmarkBytes []byte

// SIPBRIDGE_STATE_BENCH_RECORDS points to private, uncommitted captures. Only
// benchmark names, scalar timings and allocation counts appear in the output.
func BenchmarkStateCapturedRecord(b *testing.B) {
	directory := os.Getenv("SIPBRIDGE_STATE_BENCH_RECORDS")
	if directory == "" {
		b.Skip("set SIPBRIDGE_STATE_BENCH_RECORDS to private renewal captures")
	}
	for _, round := range []int{1, 12} {
		for _, kind := range []string{"registration", "subscription"} {
			name := fmt.Sprintf("%d-completed-%s.json", round, kind)
			encoded, err := os.ReadFile(filepath.Join(directory, name))
			if err != nil {
				b.Fatal(err)
			}
			var capture struct {
				Record map[string]string `json:"record"`
			}
			if err := json.Unmarshal(encoded, &capture); err != nil {
				b.Fatal(err)
			}
			fields := capture.Record
			s := payloadValidationStore()
			// Ref validation is included without importing the fixture's private
			// HMAC key. Native payload bytes and retained receipts stay unchanged.
			ref, err := s.GroupForCallID("captured-benchmark")
			if err != nil {
				b.Fatal(err)
			}
			var payload Payload
			if err := json.Unmarshal([]byte(fields["data"]), &payload); err != nil {
				b.Fatal(err)
			}
			for _, exact := range []bool{true, false} {
				profile := "actual-read"
				if exact {
					profile = "reference-reencode"
				}
				prefix := fmt.Sprintf("%s/round%d/%s", kind, round, profile)
				b.Run(prefix+"/decode-record", func(b *testing.B) {
					b.ReportAllocs()
					b.SetBytes(int64(len(fields["data"]) + len(fields["receipts"])))
					for i := 0; i < b.N; i++ {
						var record Record
						var err error
						if exact {
							record, err = decodeCapturedRecordExact(s, ref, fields)
						} else {
							record, err = s.decodeRecord(ref, fields["incarnation"], fields["revision"], fields["data"], fields["receipts"], fields["pending"], ref.CallID, fields["proof"])
						}
						if err != nil {
							b.Fatal(err)
						}
						capturedBenchmarkRecord = record
					}
				})
				b.Run(prefix+"/commit-encoding", func(b *testing.B) {
					b.ReportAllocs()
					b.SetBytes(int64(len(fields["data"])))
					for i := 0; i < b.N; i++ {
						var err error
						if exact {
							err = exactPayloadValidation(s, payload)
						} else {
							err = s.validatePayloadStructure(payload)
						}
						if err != nil {
							b.Fatal(err)
						}
						encoded, err := json.Marshal(payload)
						if err != nil {
							b.Fatal(err)
						}
						if !exact {
							if err := s.validatePayloadSize(payload, len(encoded)); err != nil {
								b.Fatal(err)
							}
						}
						capturedBenchmarkBytes = encoded
					}
				})
			}
		}
	}
}

// Reference decoder with an outer JSON scan and exact payload reserialization.
// This is not an old-release baseline; compare frozen worktrees for that claim.
func decodeCapturedRecordExact(s *Store, ref Ref, fields map[string]string) (Record, error) {
	ref.Incarnation = fields["incarnation"]
	if err := s.validateRef(ref); err != nil {
		return Record{}, ErrCorrupt
	}
	record := Record{Ref: ref, Authority: s.authority, Versions: s.cfg.Versions, Revision: fields["revision"]}
	if json.Unmarshal([]byte(fields["data"]), &record.Payload) != nil || json.Unmarshal([]byte(fields["receipts"]), &record.Receipts) != nil {
		return Record{}, ErrCorrupt
	}
	if fields["proof"] != "" {
		record.Proof = &Receipt{}
		if json.Unmarshal([]byte(fields["proof"]), record.Proof) != nil {
			return Record{}, ErrCorrupt
		}
	}
	if fields["pending"] != "" {
		record.Pending = &Input{}
		if json.Unmarshal([]byte(fields["pending"]), record.Pending) != nil {
			return Record{}, ErrCorrupt
		}
	}
	if err := exactPayloadValidation(s, record.Payload); err != nil {
		return Record{}, ErrCorrupt
	}
	return record, nil
}
