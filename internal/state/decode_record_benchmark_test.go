package state

import (
	"bytes"
	"encoding/json"
	"fmt"
	"testing"
)

// Native-shaped retained values with full-size wire copies and custom effect
// payloads exercise the enclosing aggregate scan rather than scalar JSON only.
func retainedDecodeBenchmarkPayload(b *testing.B, transactions int) Payload {
	b.Helper()
	type transaction struct {
		Key  string `json:"key"`
		Wire []byte `json:"wire"`
	}
	wire := append([]byte("INVITE sip:callee@example.test SIP/2.0\r\nContent-Type: application/sdp\r\n\r\n"), bytes.Repeat([]byte("v=0\r\na=sendrecv\r\n"), 2048)...)
	protocol := struct {
		Version      int           `json:"version"`
		Transactions []transaction `json:"transactions"`
	}{Version: 1}
	payload := Payload{ActiveTransactions: transactions, ReservedCompletionBytes: 64 << 10}
	for n := 0; n < transactions; n++ {
		tx := transaction{Key: fmt.Sprintf("tx-%d", n), Wire: wire}
		protocol.Transactions = append(protocol.Transactions, tx)
		if n < 8 {
			body, err := json.Marshal(tx)
			if err != nil {
				b.Fatal(err)
			}
			payload.Effects = append(payload.Effects, Effect{ID: fmt.Sprintf("effect-%d", n), Kind: "wire", Status: EffectPending, DeadlineMS: 1, Payload: body})
		}
	}
	encoded, err := json.Marshal(protocol)
	if err != nil {
		b.Fatal(err)
	}
	payload.ProtocolState = encoded
	return payload
}

func BenchmarkRetainedRecordPayloadDecoding(b *testing.B) {
	for _, transactions := range []int{1, 16, 64} {
		s, ref, fields := retainedDecodeFixture(b)
		payload := retainedDecodeBenchmarkPayload(b, transactions)
		encoded, err := json.Marshal(payload)
		if err != nil {
			b.Fatal(err)
		}
		fields.data = string(encoded)
		for _, direct := range []bool{false, true} {
			name := "outer-json-scan"
			if direct {
				name = "direct-custom-decoder"
			}
			b.Run(fmt.Sprintf("%d-transactions/%s", transactions, name), func(b *testing.B) {
				b.ReportAllocs()
				b.SetBytes(int64(len(encoded)))
				b.ReportMetric(float64(len(encoded)), "record-bytes")
				for n := 0; n < b.N; n++ {
					var record Record
					var err error
					if direct {
						record, err = decodeRetainedRecord(s, ref, fields)
					} else {
						record, err = decodeRecordWithOuterScan(s, ref, fields)
					}
					if err != nil {
						b.Fatal(err)
					}
					capturedBenchmarkRecord = record
				}
			})
		}
	}
}
