package engine

import (
	"bytes"
	"context"
	"encoding/json"
	"maps"
	"reflect"
	"testing"

	"github.com/emiago/sipgo/sip/portable"
	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/execution"
	"sipbridge/internal/state"
)

func reserveMetadataFixture(t *testing.T, backend string) (*StateEngine, subscriptionPhaseCapture) {
	t.Helper()
	var captured subscriptionPhaseCapture
	fixture := idleSubscriptionOwnerFixtureObserved(t, 4, func(record state.Record, input state.Input, now int64) {
		if input.Kind != "reserve" {
			return
		}
		payload, err := record.Payload.MarshalBinary()
		if err != nil {
			t.Fatal(err)
		}
		record.Payload = state.Payload{}
		record.ApplicationLeases = maps.Clone(record.ApplicationLeases)
		input.Payload = bytes.Clone(input.Payload)
		captured = subscriptionPhaseCapture{record: record, input: input, payload: payload, now: now}
	}, backend)
	store, err := state.NewMemory(fixture.Config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	wrapped := idleProfileStore{Backend: store, authority: fixture.Authority}
	e := &StateEngine{cfg: Config{Process: fixture.Process, AdvertiseAddress: "sip.example.test:5061", ContactURI: "sip:sipbridge@sip.example.test:5061;transport=tls", StateIdentityKey: fixture.Config.NamespaceKey}, store: wrapped, ctx: context.Background()}
	e.runtime, err = execution.NewCAS(wrapped, execution.ReducerFunc(e.reduce), 8)
	if err != nil {
		t.Fatal(err)
	}
	if captured.input.Kind != "reserve" {
		t.Fatal("no real reservation captured")
	}
	return e, captured
}

func reserveMetadataRead(t *testing.T, capture subscriptionPhaseCapture) (state.Record, state.Input, stateEngineInput) {
	t.Helper()
	record := capture.record
	record.ApplicationLeases = maps.Clone(record.ApplicationLeases)
	if err := record.Payload.UnmarshalBinary(capture.payload); err != nil {
		t.Fatal(err)
	}
	input := capture.input
	input.Payload = bytes.Clone(input.Payload)
	var in stateEngineInput
	if err := in.UnmarshalBinary(input.Payload); err != nil {
		t.Fatal(err)
	}
	return record, input, in
}

func TestReserveMetadataPreservesProtocolAndSelectedProof(t *testing.T) {
	for _, backend := range []string{state.MemoryBackend, state.RedisBackend} {
		t.Run(backend, func(t *testing.T) {
			e, capture := reserveMetadataFixture(t, backend)
			record, input, in := reserveMetadataRead(t, capture)
			original, _ := record.Payload.MarshalBinary()
			record.Payload.CommitLeaseGuards = []state.ApplicationLeaseRef{{ID: "stale"}}
			record.Payload.CommitLeaseFences = []state.ApplicationLeaseRef{{ID: "stale"}}
			record.Payload.LocalObservations = []state.LocalObservation{{ContextID: "stale"}}
			record.Payload.LocalResult = &pb.OperationResult{Error: "stale"}
			record.Payload.LocalDrain = &state.LocalDrainObservation{Cancel: true}
			record.Payload.TimerWorker = "stale"
			got, outcome, ok := e.reserveMetadata(record, input, in, capture.now)
			if !ok {
				t.Fatal("real authorized reservation did not use metadata path")
			}
			if !bytes.Equal(got.ProtocolState, record.Payload.ProtocolState) || !reflect.DeepEqual(got.Timers, record.Payload.Timers) ||
				got.Retention != record.Payload.Retention || got.ActiveTransactions != record.Payload.ActiveTransactions || got.ReservedCompletionBytes != record.Payload.ReservedCompletionBytes ||
				!reflect.DeepEqual(got.ApplicationContexts, record.Payload.ApplicationContexts) || !reflect.DeepEqual(got.ApplicationLeaseIDs, record.Payload.ApplicationLeaseIDs) {
				t.Fatal("metadata reservation changed protocol, indexes or reserved obligations")
			}
			if len(got.CommitLeaseFences) != 0 || len(got.LocalObservations) != 0 || got.TimerWorker != e.dispatchWorker(e.cfg.Process) || got.LocalDrain == nil || got.LocalResult == nil || got.LocalResult.Error != "" {
				t.Fatal("stale transient projection")
			}
			// Disable eligibility only; normal reserve still applies the same proof
			// and policy checks and constructs the reference write/drain result.
			normalRecord, normalInput, _ := reserveMetadataRead(t, capture)
			normalInput.Proof = false
			normal, normalOutcome, err := e.reduce(normalRecord, normalInput, capture.now)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(outcome, normalOutcome) || !reflect.DeepEqual(got.LocalDrain, normal.LocalDrain) || !reflect.DeepEqual(got.CommitLeaseGuards, normal.CommitLeaseGuards) {
				t.Fatal("reservation proof/result differs from full reduction")
			}
			for index, effect := range got.Effects {
				if effect.ID != in.Effect {
					if !reflect.DeepEqual(effect, record.Payload.Effects[index]) {
						t.Fatal("unrelated effect changed")
					}
					continue
				}
				if effect.Status != state.EffectDispatching || effect.AttemptID != in.Write.Effect.AttemptId {
					t.Fatal("attempt not selected")
				}
				var body stateEngineEffect
				if body.unmarshalMetadata(effect.Payload) != nil || body.Wire != nil || body.Envelope != nil || !proto.Equal(body.Permit, in.Write.Permit) {
					t.Fatal("reserved evidence changed")
				}
				found := false
				for _, expected := range normal.Effects {
					if expected.ID == effect.ID {
						found = reflect.DeepEqual(effect, expected)
					}
				}
				if !found {
					t.Fatal("selected effect differs from normal reservation")
				}
			}
			after, _ := record.Payload.MarshalBinary()
			if !bytes.Equal(original, after) {
				t.Fatal("input record mutated")
			}
			// Hook publishes this same result through the ordinary reducer API.
			fresh, freshInput, _ := reserveMetadataRead(t, capture)
			published, publishedOutcome, err := e.reduce(fresh, freshInput, capture.now)
			if err != nil || !bytes.Equal(published.ProtocolState, fresh.Payload.ProtocolState) || !bytes.Equal(publishedOutcome, outcome) {
				t.Fatal("fast-path hook not used", err)
			}
		})
	}
}

func TestReserveMetadataFallbackDoesNotPublishPartialWork(t *testing.T) {
	e, capture := reserveMetadataFixture(t, state.MemoryBackend)
	cases := map[string]func(*state.Record, *state.Input, *stateEngineInput){
		"not-proof":     func(_ *state.Record, input *state.Input, _ *stateEngineInput) { input.Proof = false },
		"kind-mismatch": func(_ *state.Record, input *state.Input, _ *stateEngineInput) { input.Kind = "complete" },
		"due-timer": func(record *state.Record, _ *state.Input, _ *stateEngineInput) {
			record.Payload.Timers = append(record.Payload.Timers, state.Timer{ID: "due", DueMS: capture.now})
		},
		"corrupt-frame": func(record *state.Record, _ *state.Input, _ *stateEngineInput) {
			record.Payload.ProtocolState = []byte{255}
		},
		"attempt-mismatch": func(_ *state.Record, _ *state.Input, in *stateEngineInput) { in.Write.Effect.AttemptId += "-other" },
		"permit-mismatch":  func(_ *state.Record, _ *state.Input, in *stateEngineInput) { in.Write.Permit.OrderingProof += "-other" },
		"flow-mismatch":    func(_ *state.Record, _ *state.Input, in *stateEngineInput) { in.Write.Flow.ConnectionId += "-other" },
		"missing-effect": func(_ *state.Record, _ *state.Input, in *stateEngineInput) {
			in.Effect = "missing"
			in.Write.Effect.Id = "missing"
		},
		"expired-input": func(_ *state.Record, input *state.Input, _ *stateEngineInput) { input.DeadlineMS = capture.now },
	}
	cases["pending"] = func(record *state.Record, _ *state.Input, _ *stateEngineInput) { record.Pending = &state.Input{} }
	cases["missing-owner"] = func(record *state.Record, _ *state.Input, _ *stateEngineInput) { record.ApplicationLeases = nil }
	cases["missing-ACK-proof"] = func(record *state.Record, _ *state.Input, in *stateEngineInput) {
		for i := range record.Payload.Effects {
			if record.Payload.Effects[i].ID != in.Effect {
				continue
			}
			var body stateEngineEffect
			if err := body.unmarshalMetadata(record.Payload.Effects[i].Payload); err != nil {
				t.Fatal(err)
			}
			body.AfterACK, body.Dialog = "required-ACK-hash", "unwritten-dialog"
			var err error
			record.Payload.Effects[i].Payload, err = body.MarshalBinary()
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	for _, reason := range []string{"owner-lost", "owner-cleanup", "policy-expired", "policy-command"} {
		cases[reason] = func(record *state.Record, _ *state.Input, in *stateEngineInput) {
			group, err := decodeStateGroup(*record)
			if err != nil {
				t.Fatal(err)
			}
			c := group.Contexts[in.Context.Id]
			switch reason {
			case "owner-lost":
				c.OwnerLost = true
			case "owner-cleanup":
				c.OwnerCleanupAtMS = capture.now + 1000
			default:
				var body stateEngineEffect
				for _, effect := range record.Payload.Effects {
					if effect.ID == in.Effect {
						if err := body.unmarshalMetadata(effect.Payload); err != nil {
							t.Fatal(err)
						}
					}
				}
				tx := c.Transactions[body.Transaction]
				if tx == nil || tx.Policy == nil {
					t.Fatal("fixture lacks application policy")
				}
				if reason == "policy-expired" {
					tx.Policy.Decision.ExpiresAtMs = uint64(capture.now)
				} else {
					tx.Policy.Command += "-other"
				}
			}
			record.Payload.ProtocolState, err = group.MarshalBinary()
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	for _, status := range []state.EffectStatus{state.EffectDispatching, state.EffectWritten, state.EffectNotWritten, state.EffectUnknown, state.EffectCancelled} {
		cases["known-"+string(status)] = func(record *state.Record, _ *state.Input, in *stateEngineInput) {
			for i := range record.Payload.Effects {
				if record.Payload.Effects[i].ID == in.Effect {
					record.Payload.Effects[i].Status = status
				}
			}
		}
	}
	for _, reason := range []string{"stale", "expired", "sealed", "term", "generation", "attachment"} {
		cases["owner-"+reason] = func(record *state.Record, _ *state.Input, _ *stateEngineInput) {
			for id, lease := range record.ApplicationLeases {
				switch reason {
				case "stale":
					lease.CheckedAtMS--
				case "expired":
					lease.ExpiresAtMS = capture.now
				case "sealed":
					lease.Sealed = true
				case "term":
					lease.Term += "-other"
				case "generation":
					lease.Generation++
				case "attachment":
					lease.AttachmentUntilMS = capture.now + 1000
				}
				record.ApplicationLeases[id] = lease
			}
		}
	}
	for name, change := range cases {
		t.Run(name, func(t *testing.T) {
			record, input, in := reserveMetadataRead(t, capture)
			change(&record, &input, &in)
			before, _ := record.Payload.MarshalBinary()
			payload, outcome, ok := e.reserveMetadata(record, input, in, capture.now)
			if ok || outcome != nil || payload.LocalResult != nil || len(payload.ProtocolState) != 0 {
				t.Fatal("ineligible proof published")
			}
			after, _ := record.Payload.MarshalBinary()
			if !bytes.Equal(before, after) {
				t.Fatal("fallback mutated original record")
			}
			// These refusals must also fall through the public reducer without
			// returning a second socket-write payload or selecting an attempt.
			if name == "attempt-mismatch" || name == "permit-mismatch" || name == "flow-mismatch" || name == "missing-ACK-proof" || name == "known-unknown" {
				var err error
				input.Payload, err = in.MarshalBinary()
				if err != nil {
					t.Fatal(err)
				}
				_, returned, err := e.reduce(record, input, capture.now)
				if err == nil {
					var result pb.OperationResult
					if json.Unmarshal(returned, &result) != nil || len(result.GetRequest().GetWire()) != 0 {
						t.Fatal("fallback authorized a socket write")
					}
				}
			}
		})
	}
}

func TestReserveMetadataDefersUnrelatedSIPParsing(t *testing.T) {
	e, capture := reserveMetadataFixture(t, state.MemoryBackend)
	record, input, in := reserveMetadataRead(t, capture)
	group, err := decodeStateGroup(record)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range group.Contexts {
		c.Origin = portable.RestoreMessage([]byte("unrelated malformed retained SIP"), c.Origin.Transport(), c.Origin.Source(), c.Origin.Destination())
	}
	record.Payload.ProtocolState, err = group.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	payload, outcome, ok := e.reserveMetadata(record, input, in, capture.now)
	var result pb.OperationResult
	if !ok || json.Unmarshal(outcome, &result) != nil || len(result.GetRequest().GetWire()) == 0 || !bytes.Equal(payload.ProtocolState, record.Payload.ProtocolState) {
		t.Fatal("unrelated SIP was consumed by metadata reservation")
	}
	// Selected wire corruption never obtains a fresh physical reservation.
	record, input, in = reserveMetadataRead(t, capture)
	for i := range record.Payload.Effects {
		if record.Payload.Effects[i].ID != in.Effect {
			continue
		}
		var body stateEngineEffect
		if err := body.unmarshalMetadata(record.Payload.Effects[i].Payload); err != nil {
			t.Fatal(err)
		}
		body.Wire = []byte("invalid selected SIP")
		record.Payload.Effects[i].Payload, err = body.MarshalBinary()
		if err != nil {
			t.Fatal(err)
		}
	}
	if _, _, ok := e.reserveMetadata(record, input, in, capture.now); ok {
		t.Fatal("corrupt selected write accepted")
	}
}
