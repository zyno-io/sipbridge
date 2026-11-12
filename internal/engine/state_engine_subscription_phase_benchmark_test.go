package engine

import (
	"bytes"
	"context"
	"encoding/json"
	"maps"
	"testing"

	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/execution"
	"sipbridge/internal/state"
)

type subscriptionPhaseCapture struct {
	record  state.Record
	input   state.Input
	payload []byte
	now     int64
}

var subscriptionPhaseOutcome []byte

// Redis supplies actual admitted native command/reserve/complete/response-ingest
// reads after a completed subscription has accumulated NOTIFY history. Timed
// iterations replay one immutable pure reduction at its original store time,
// including fresh outer-payload decode, protocol decode, selectors and encoding.
// No Redis I/O, receipt JSON, CAS, network I/O or setup is timed. This measures
// operation-local allocation, not live traffic or retained per-dialog memory.
func BenchmarkSubscriptionHistoryReduction(b *testing.B) {
	benchmarkSubscriptionHistoryReduction(b, 16, false)
}

// Five successful SUBSCRIBE procedures and five NOTIFY procedures model the
// observed mixed live history. Each timed phase includes owned record decoding;
// setup and Redis I/O remain excluded, as in the original phase benchmark.
func BenchmarkMixedSubscriptionHistoryReduction(b *testing.B) {
	benchmarkSubscriptionHistoryReduction(b, 6, true)
}

func benchmarkSubscriptionHistoryReduction(b *testing.B, history int, mixed bool) {
	captures := make(map[string]subscriptionPhaseCapture)
	fixture := subscriptionHistoryFixtureObserved(b, history, mixed, func(record state.Record, input state.Input, now int64) {
		switch input.Kind {
		case "command", "reserve", "complete", "ingest":
		default:
			return
		}
		payload, err := record.Payload.MarshalBinary()
		if err != nil {
			b.Fatal(err)
		}
		// Inputs/leases are immutable for replay. The serialized payload owns the
		// entire protocol/effect graph; no decoded group crosses iterations.
		record.Payload = state.Payload{}
		record.ApplicationLeases = maps.Clone(record.ApplicationLeases)
		input.Payload = bytes.Clone(input.Payload)
		input.ApplicationLeaseIDs = append([]string(nil), input.ApplicationLeaseIDs...)
		captures[input.Kind] = subscriptionPhaseCapture{record: record, input: input, payload: payload, now: now}
	}, state.RedisBackend)
	for _, phase := range []string{"command", "reserve", "complete", "ingest"} {
		b.Run(phase, func(b *testing.B) {
			capture, ok := captures[phase]
			if !ok {
				b.Fatal("missing native phase", phase)
			}
			store, err := state.NewMemory(fixture.Config)
			if err != nil {
				b.Fatal(err)
			}
			defer store.Close()
			backend := idleProfileStore{Backend: store, authority: fixture.Authority}
			e := &StateEngine{cfg: Config{Process: fixture.Process, AdvertiseAddress: "sip.example.test:5061", ContactURI: "sip:sipbridge@sip.example.test:5061;transport=tls", StateIdentityKey: fixture.Config.NamespaceKey}, store: backend, ctx: context.Background()}
			e.runtime, err = execution.NewCAS(backend, execution.ReducerFunc(e.reduce), 8)
			if err != nil {
				b.Fatal(err)
			}
			record := capture.record
			if err := record.Payload.UnmarshalBinary(capture.payload); err != nil {
				b.Fatal(err)
			}
			group, err := decodeStateGroup(record)
			if err != nil {
				b.Fatal(err)
			}
			transactions := 0
			for _, c := range group.Contexts {
				transactions += len(c.Transactions)
			}
			if transactions == 0 {
				b.Fatal("capture lacks subscription transaction")
			}
			checked, outcome, err := e.reduce(record, capture.input, capture.now)
			if err != nil {
				b.Fatal("captured native phase refused", err)
			}
			var result pb.OperationResult
			if err := json.Unmarshal(outcome, &result); err != nil || result.Error != "" || result.Certainty == pb.OperationResult_NOT_ACCEPTED {
				b.Fatal("captured phase did not execute", err, result.Error)
			}
			if bytes.Equal(checked.ProtocolState, record.Payload.ProtocolState) && len(checked.Effects) == 0 {
				b.Fatal("capture performed no native transition")
			}
			b.ReportAllocs()
			b.ResetTimer()
			for n := 0; n < b.N; n++ {
				if err := record.Payload.UnmarshalBinary(capture.payload); err != nil {
					b.Fatal(err)
				}
				payload, outcome, err := e.reduce(record, capture.input, capture.now)
				if err != nil {
					b.Fatal(err)
				}
				idleProfileResult, subscriptionPhaseOutcome = payload, outcome
			}
			b.StopTimer()
			b.ReportMetric(float64(transactions), "transactions")
			b.ReportMetric(float64(len(capture.payload)), "record-B")
			b.ReportMetric(float64(len(record.Payload.ProtocolState)), "protocol-B")
		})
	}
}
