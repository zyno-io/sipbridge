package engine

// Test-only layout experiment. This is not a persisted format or runtime path.
import (
	"fmt"
	"testing"

	"github.com/emiago/sipgo/sip/portable"
	"google.golang.org/protobuf/proto"
	"sipbridge/internal/state"
	"sipbridge/internal/storagepb"
)

type serverLayoutPrototype struct {
	live []byte
	cold [][]byte
	// Each entry keeps the same pessimistic future emission allowance as the
	// existing completionBudget until replay is no longer possible.
	futureEmissions int
	proofHeadroom   int
	wireProcedures  int
}

func prototypeServerLayout(t testing.TB, history int) (serverLayoutPrototype, []byte) {
	t.Helper()
	fixture := subscriptionHistoryFixtureObserved(t, history+1, true, nil)
	var payload state.Payload
	if err := payload.UnmarshalBinary(fixture.Payload); err != nil {
		t.Fatal(err)
	}
	var group stateGroup
	if err := group.UnmarshalBinary(payload.ProtocolState); err != nil {
		t.Fatal(err)
	}
	full, err := group.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	layout := serverLayoutPrototype{}
	for _, c := range group.Contexts {
		for key, tx := range c.Transactions {
			if tx.Native.Family != portable.NonInviteServer || !tx.Native.Reliable || tx.Native.Phase != portable.Terminated || tx.Native.LastResponse == nil || tx.FinalResponse == nil || !tx.FinalResponse.FinalWritten {
				continue
			}
			// The generated transaction preserves all adapter evidence and exact
			// wires. Only constants implied by this experimental record kind are
			// elided; profile and all identity/disposition fields remain.
			entry := storageEncodeEngineStateTransaction(*tx)
			entry.Native.Version = 0
			entry.Native.Family = ""
			entry.Native.Phase = ""
			entry.Native.Reliable = false
			data, err := proto.MarshalOptions{Deterministic: true}.Marshal(entry)
			if err != nil {
				t.Fatal(err)
			}
			layout.cold = append(layout.cold, data)
			layout.futureEmissions += max(0, stateTransactionEmissions-int(tx.Emissions))
			actual, err := tx.stateRequestDisposition.encodedBytes()
			if err != nil {
				t.Fatal(err)
			}
			layout.proofHeadroom, err = addStateProofHeadroom(layout.proofHeadroom, 1, stateRequestProofBytes-actual)
			if err != nil {
				t.Fatal(err)
			}
			layout.wireProcedures++
			delete(c.Transactions, key)
		}
	}
	layout.live, err = group.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	return layout, full
}

func TestCompletedServerLayoutPrototype(t *testing.T) {
	for _, history := range []int{1, 5, 16} {
		t.Run(fmt.Sprint(history), func(t *testing.T) {
			layout, full := prototypeServerLayout(t, history)
			if len(layout.cold) == 0 {
				t.Fatal("fixture lacks settled server history")
			}
			total := len(layout.live)
			sum := 0
			for _, data := range layout.cold {
				total += len(data)
				var entry storagepb.EngineStateTransaction
				if err := proto.Unmarshal(data, &entry); err != nil {
					t.Fatal(err)
				}
				if entry.Native == nil || entry.Native.Origin == nil || entry.Native.LastResponse == nil || entry.FinalResponse == nil {
					t.Fatal("lost required request/final proof")
				}
				sum += max(0, stateTransactionEmissions-int(entry.Emissions))
			}
			if len(layout.cold) != layout.wireProcedures || layout.proofHeadroom < 0 {
				t.Fatal("aggregate proof or wire count invalid")
			}
			if sum != layout.futureEmissions {
				t.Fatal("future emissions changed")
			}
			t.Logf("requested=%d selected=%d full=%d live=%d cold=%d total=%d future-emissions=%d proof-headroom=%d wire-procedures=%d", history, len(layout.cold), len(full), len(layout.live), total-len(layout.live), total, sum, layout.proofHeadroom, layout.wireProcedures)
		})
	}
}

func BenchmarkCompletedServerLayoutPrototype(b *testing.B) {
	for _, history := range []int{1, 5, 16} {
		b.Run(fmt.Sprint(history), func(b *testing.B) {
			layout, full := prototypeServerLayout(b, history)
			if len(layout.cold) == 0 {
				b.Fatal("fixture lacks settled server history")
			}
			for _, selected := range []bool{false, true} {
				name := "live"
				if selected {
					name = "selected"
				}
				b.Run(name, func(b *testing.B) {
					b.ReportAllocs()
					for i := 0; i < b.N; i++ {
						var group stateGroup
						if err := group.UnmarshalBinary(layout.live); err != nil {
							b.Fatal(err)
						}
						if selected {
							var entry storagepb.EngineStateTransaction
							if err := proto.Unmarshal(layout.cold[0], &entry); err != nil {
								b.Fatal(err)
							}
						}
					}
				})
			}
			b.Run("full", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					var group stateGroup
					if err := group.UnmarshalBinary(full); err != nil {
						b.Fatal(err)
					}
				}
			})
		})
	}
}
