package engine

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"

	"github.com/emiago/sipgo/sip"
	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/state"
)

func TestCompactReceiptKeepsFrozenSnapshotAndExactPublicMessages(t *testing.T) {
	request := stateTestRequest("INVITE", 1, "", "receipt-compaction")
	parsed, err := sip.ParseMessage(request.Wire)
	if err != nil {
		t.Fatal(err)
	}
	response := sip.NewResponseFromRequest(parsed.(*sip.Request), 200, "OK", []byte("v=0\r\n"))
	original := &pb.OperationResult{Certainty: pb.OperationResult_COMPLETED, SipStatus: 200,
		Snapshot: &pb.ContextSnapshot{Context: &pb.ContextRef{Id: "context"}, Revision: 17, CommandSequence: 4,
			LocalSdp: []byte("original-session"), DirectoryEpoch: "original-directory"},
		Request: mustObserve(request.Wire, true), Response: mustObserve([]byte(response.String()), false)}
	before := proto.Clone(original).(*pb.OperationResult)
	full, err := json.Marshal(original)
	if err != nil {
		t.Fatal(err)
	}
	compact, err := marshalStateReceiptResult(original)
	if err != nil || !proto.Equal(original, before) {
		t.Fatal("encoding changed receipt input", err)
	}
	var stored pb.OperationResult
	if err := json.Unmarshal(compact, &stored); err != nil {
		t.Fatal(err)
	}
	if len(compact) >= len(full) || !proto.Equal(stored.Snapshot, original.Snapshot) ||
		!proto.Equal(stored.Request, &pb.SipMessage{Wire: request.Wire}) ||
		!proto.Equal(stored.Response, &pb.SipMessage{Wire: []byte(response.String())}) {
		t.Fatal("receipt retained projections or changed historical evidence")
	}
	var replay pb.OperationResult
	if err := unmarshalStateReceiptResult(compact, &replay); err != nil || !proto.Equal(&replay, before) {
		t.Fatal("compact receipt changed exact public replay", err)
	}
}

func TestCompactReceiptDoesNotInterpretPhysicalReserveValues(t *testing.T) {
	for _, result := range []*pb.OperationResult{
		{Certainty: pb.OperationResult_COMPLETED, Request: &pb.SipMessage{Wire: []byte("arbitrary outbound bytes")}},
		{Certainty: pb.OperationResult_COMPLETED, SipStatus: uint32(pb.DeliveryResult_UNKNOWN), Response: &pb.SipMessage{}},
	} {
		data, err := marshalStateReceiptResult(result)
		if err != nil {
			t.Fatal(err)
		}
		var decoded pb.OperationResult
		if err := unmarshalStateReceiptResult(data, &decoded); err != nil || !proto.Equal(result, &decoded) {
			t.Fatal("physical reserve sentinel was parsed or changed", err)
		}
	}
}

func TestCompactReceiptReplayRemainsFrozenAfterLaterCommit(t *testing.T) {
	for _, backend := range []string{state.MemoryBackend, state.RedisBackend} {
		t.Run(backend, func(t *testing.T) {
			f := newNativeOwnerFixtureBackend(t, backend)
			request := stateTestRequest("INVITE", 1, "", "receipt-replay")
			ref, err := f.engine.Locate(context.Background(), request)
			if err != nil {
				t.Fatal(err)
			}
			input := stateEngineInput{Kind: "ingest", Application: f.app, ApplicationLeaseTerm: f.term,
				Ingest: &pb.OwnerIngest{Context: ref, Application: f.app, Flow: f.flow, Wire: request.Wire}}
			ingested := f.apply(t, ref, "original-ingest", input)
			if ingested.Error != "" {
				t.Fatal("fixture ingest failed", ingested.Error)
			}
			_, group := stateTestLoad(t, f.engine, ref)
			var requestID string
			for id := range group.Contexts[ref.Id].Requests {
				requestID = id
			}
			original := stateEngineInput{Kind: "command", Application: f.app, ApplicationLeaseTerm: f.term,
				Command: &pb.Command{Context: ref, Sequence: 1, Action: &pb.Command_Respond{Respond: &pb.Respond{RequestId: requestID, Status: 180}}}}
			accepted := f.apply(t, ref, "original-command", original)
			if accepted.Error != "" || accepted.Snapshot == nil {
				t.Fatal("fixture command failed", accepted.Error)
			}
			record, _ := stateTestLoad(t, f.engine, ref)
			if record.Payload.LocalResult != nil {
				t.Fatal("transient result DTO was persisted")
			}
			frozen := proto.Clone(accepted).(*pb.OperationResult)
			later := f.apply(t, ref, "later-command", stateEngineInput{Kind: "command", Application: f.app, ApplicationLeaseTerm: f.term,
				Command: &pb.Command{Context: ref, Sequence: 2, Action: &pb.Command_Respond{Respond: &pb.Respond{RequestId: requestID, Status: 183}}}})
			if later.Error != "" || later.Snapshot.Revision <= accepted.Snapshot.Revision {
				t.Fatal("fixture did not advance the current snapshot", later.Error)
			}
			accepted.Snapshot.Revision = 999999 // A caller owns its returned DTO.
			replay := f.apply(t, ref, "original-command", original)
			if !proto.Equal(replay, frozen) {
				t.Fatal("receipt replay substituted current snapshot")
			}
			for _, receipt := range record.Receipts {
				if receipt.OperationID == "original-command" && !bytes.Contains(receipt.Outcome, []byte("\"snapshot\"")) {
					t.Fatal("receipt did not retain frozen snapshot")
				}
			}
		})
	}
}

func BenchmarkCompactReceiptEncoding(b *testing.B) {
	fixture := idleSubscriptionOwnerFixture(b, 6)
	var payload state.Payload
	if err := payload.UnmarshalBinary(fixture.Payload); err != nil {
		b.Fatal(err)
	}
	group, err := decodeStateGroup(state.Record{Payload: payload, Versions: NativeStateVersions()})
	if err != nil {
		b.Fatal(err)
	}
	var result *pb.OperationResult
	for _, c := range group.Contexts {
		result = proto.Clone(c.Result).(*pb.OperationResult)
		result.Snapshot = stateSnapshot(c, fixture.Process)
	}
	if err := stateMaterializeResultMessages(result); err != nil {
		b.Fatal(err)
	}
	for name, encode := range map[string]func(*pb.OperationResult) ([]byte, error){"full": func(result *pb.OperationResult) ([]byte, error) { return json.Marshal(result) }, "compact": marshalStateReceiptResult} {
		b.Run(name, func(b *testing.B) {
			data, err := encode(result)
			if err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for n := 0; n < b.N; n++ {
				if _, err := encode(result); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(len(data)), "receipt-bytes")
		})
		b.Run(name+"-decode", func(b *testing.B) {
			data, err := encode(result)
			if err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for n := 0; n < b.N; n++ {
				var decoded pb.OperationResult
				if name == "full" {
					err = json.Unmarshal(data, &decoded)
				} else {
					err = unmarshalStateReceiptResult(data, &decoded)
				}
				if err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(len(data)), "receipt-bytes")
		})
	}
}
