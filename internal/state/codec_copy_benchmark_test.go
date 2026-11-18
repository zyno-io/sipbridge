package state

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

var codecBenchmarkPayload Payload
var codecBenchmarkEffect Effect

// Capture contents remain private. Each benchmark exercises a complete production
// codec, including nested effects, rather than just the leaf byte-copy helper.
func BenchmarkCodecOwnershipCopy(b *testing.B) {
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
		encoded, err := json.Marshal(payload)
		if err != nil {
			b.Fatal(err)
		}
		var effect Effect
		for _, candidate := range payload.Effects {
			if len(candidate.Payload) > len(effect.Payload) {
				effect = candidate
			}
		}
		if len(effect.Payload) == 0 {
			b.Fatal("registration capture must contain a retained effect payload")
		}
		effectEncoded, err := json.Marshal(effect)
		if err != nil {
			b.Fatal(err)
		}
		for _, operation := range []string{"payload-marshal", "payload-unmarshal", "effect-marshal", "effect-unmarshal"} {
			size := len(encoded)
			if operation == "effect-marshal" || operation == "effect-unmarshal" {
				size = len(effectEncoded)
			}
			b.Run(fmt.Sprintf("round%d/%s/%d-bytes", round, operation, size), func(b *testing.B) {
				b.ReportAllocs()
				b.SetBytes(int64(size))
				for i := 0; i < b.N; i++ {
					switch operation {
					case "payload-marshal":
						capturedBenchmarkBytes, err = json.Marshal(payload)
					case "payload-unmarshal":
						var decoded Payload
						err = decoded.UnmarshalJSON(encoded)
						codecBenchmarkPayload = decoded
					case "effect-marshal":
						capturedBenchmarkBytes, err = json.Marshal(effect)
					case "effect-unmarshal":
						var decoded Effect
						err = decoded.UnmarshalJSON(effectEncoded)
						codecBenchmarkEffect = decoded
					}
					if err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
