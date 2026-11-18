package engine

import (
	"bytes"
	"context"
	"encoding/json"
	"maps"
	"testing"

	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/execution"
	"sipbridge/internal/state"
)

func notifyRetirementFixture(t testing.TB, backend string) (*stateReduction, *stateContext, *stateTransaction) {
	t.Helper()
	var capture subscriptionPhaseCapture
	fixture := idleSubscriptionOwnerFixtureObserved(t, 2, func(record state.Record, input state.Input, now int64) {
		if input.Kind != "ingest" {
			return
		}
		payload, err := record.Payload.MarshalBinary()
		if err != nil {
			t.Fatal(err)
		}
		record.Payload = state.Payload{}
		record.ApplicationLeases = maps.Clone(record.ApplicationLeases)
		input.Payload = bytes.Clone(input.Payload)
		capture = subscriptionPhaseCapture{record: record, input: input, payload: payload, now: now}
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
	record := capture.record
	if err = record.Payload.UnmarshalBinary(capture.payload); err != nil {
		t.Fatal(err)
	}
	group, err := decodeStateGroup(record)
	if err != nil {
		t.Fatal(err)
	}
	var in stateEngineInput
	if err = in.UnmarshalBinary(capture.input.Payload); err != nil {
		t.Fatal(err)
	}
	r := &stateReduction{engine: e, record: record, group: group, payload: record.Payload, in: in, now: capture.now, deadline: capture.input.DeadlineMS, result: &pb.OperationResult{Certainty: pb.OperationResult_COMPLETED}}
	if err = r.reclaimEffects(); err != nil {
		t.Fatal(err)
	}
	c := group.Contexts[in.Ingest.Context.Id]
	var tx *stateTransaction
	for _, candidate := range c.Transactions {
		if candidate.Native.Family == portable.NonInviteClient {
			tx = candidate
		}
	}
	if tx == nil || !tx.Written || tx.Native.Phase == portable.Terminated {
		t.Fatal("fixture lacks positively written outstanding NOTIFY")
	}
	return r, c, tx
}

func TestSettledNotifyRetiresAfterMatchedSuccessfulFinal(t *testing.T) {
	for _, backend := range []string{state.MemoryBackend, state.RedisBackend} {
		t.Run(backend, func(t *testing.T) {
			r, c, tx := notifyRetirementFixture(t, backend)
			sequence, flow := c.Subscription.LocalSequence, c.Flow
			if err := r.ingest(); err != nil {
				t.Fatal(err)
			}
			if tx.Native.Phase != portable.Terminated || !r.settledNotifyRetirable(c.Snapshot.Context.Id, tx) {
				t.Fatal("successful settled reliable NOTIFY not eligible")
			}
			result, _ := json.Marshal(c.Result)
			payload, err := r.finish()
			if err != nil {
				t.Fatal(err)
			}
			if c.Transactions[tx.Native.Key] != nil || len(payload.Effects) != 0 || c.Subscription.LocalSequence != sequence || c.Flow != flow {
				t.Fatal("retirement changed subscription or retained physical work")
			}
			after, _ := json.Marshal(c.Result)
			if !bytes.Equal(result, after) {
				t.Fatal("retirement changed command result")
			}
			// Duplicate finals no longer match a client procedure and cannot recreate it,
			// publish another result, or authorize any physical effect.
			if err = r.ingest(); err == nil {
				t.Fatal("duplicate final recreated a client transaction")
			}
			after, _ = json.Marshal(c.Result)
			if len(r.payload.Effects) != 0 || c.Transactions[tx.Native.Key] != nil || !bytes.Equal(result, after) {
				t.Fatal("duplicate final changed retained authority")
			}
		})
	}
}

func TestSettledNotifyRetirementRequiresEveryPositiveProof(t *testing.T) {
	for _, name := range []string{"unwritten", "udp", "live", "missing-final", "malformed-final", "wrong-kind-final", "failure", "mismatch", "timer", "digest", "pending", "cleanup", "disposition", "unknown-effect", "controller-event", "ended", "server", "invite"} {
		t.Run(name, func(t *testing.T) {
			r, c, tx := notifyRetirementFixture(t, state.MemoryBackend)
			if err := r.ingest(); err != nil {
				t.Fatal(err)
			}
			switch name {
			case "unwritten":
				tx.Written = false
			case "udp":
				tx.Native.Reliable = false
			case "live":
				tx.Native.Phase = portable.Proceeding
			case "missing-final":
				tx.Native.LastResponse = nil
			case "malformed-final":
				tx.Native.LastResponse = stateTestPortablePtr([]byte("bad"), "tls", "", "")
			case "wrong-kind-final":
				tx.Native.LastResponse = &tx.Native.Origin
			case "failure", "mismatch":
				message, _ := sip.ParseMessage(tx.Native.LastResponse.WireBytes())
				response := message.(*sip.Response)
				if name == "failure" {
					response.StatusCode = 481
				} else {
					response.CSeq().SeqNo++
				}
				tx.Native.LastResponse = stateTestPortablePtr([]byte(response.String()), "tls", "", "")
			case "timer":
				tx.Native.Timers = map[portable.Timer]portable.Deadline{portable.TimerF: {Active: true, At: r.now + 1000, Generation: 1}}
			case "digest":
				tx.DigestAttempts = 1
			case "pending":
				tx.PendingResponse = &pb.Respond{}
			case "cleanup":
				tx.Cleanup = true
			case "disposition":
				tx.DispositionUntilMS = r.now + 120000
			case "unknown-effect", "controller-event":
				body, err := (stateEngineEffect{Context: c.Snapshot.Context.Id, Transaction: tx.Native.Key}).MarshalBinary()
				if err != nil {
					t.Fatal(err)
				}
				kind := "deliver"
				if name == "controller-event" {
					kind = "event"
				}
				r.payload.Effects = append(r.payload.Effects, state.Effect{ID: "pending", Kind: kind, Status: state.EffectUnknown, Payload: body})
			case "ended":
				c.Snapshot.Ended = true
			case "server":
				tx.Native.Family = portable.NonInviteServer
			case "invite":
				tx.Native.Family = portable.InviteClient
			}
			if r.settledNotifyRetirable(c.Snapshot.Context.Id, tx) {
				t.Fatal("missing or unresolved proof permitted early retirement")
			}
		})
	}
}

func TestNotifyFinalBeforeSettlementRetiresOnNaturalReduction(t *testing.T) {
	r, c, tx := notifyRetirementFixture(t, state.MemoryBackend)
	tx.Written = false
	body := stateEngineEffect{Context: c.Snapshot.Context.Id, Transaction: tx.Native.Key, Native: true}
	encoded, err := body.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	r.payload.Effects = []state.Effect{{ID: "notify-write", Kind: "deliver", Status: state.EffectWritten, Payload: encoded}}
	if err = r.ingest(); err != nil {
		t.Fatal(err)
	}
	if err = r.reclaimEffects(); err != nil {
		t.Fatal(err)
	}
	if tx.Written || r.settledNotifyRetirable(c.Snapshot.Context.Id, tx) {
		t.Fatal("unsettled WRITTEN was treated as settled")
	}
	body.Settled = true
	encoded, err = body.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	r.payload.Effects[0].Payload = encoded
	if err = r.reclaimEffects(); err != nil {
		t.Fatal(err)
	}
	if !tx.Written {
		t.Fatal("settlement did not transfer positive proof")
	}
	r.pruneTransactions()
	if c.Transactions[tx.Native.Key] != nil {
		t.Fatal("settled response-before-complete transaction retained")
	}
}

func TestRetiredNotifyDuplicateHasNoNativeCompletionCandidate(t *testing.T) {
	for _, backend := range []string{state.MemoryBackend, state.RedisBackend} {
		t.Run(backend, func(t *testing.T) {
			fixture := idleSubscriptionOwnerFixture(t, 2, backend)
			fixture.Engine.work = make(chan struct{}, 1)
			record := fixture.Record
			if err := record.Payload.UnmarshalBinary(fixture.Payload); err != nil {
				t.Fatal(err)
			}
			group, err := decodeStateGroup(record)
			if err != nil {
				t.Fatal(err)
			}
			for _, c := range group.Contexts {
				for _, tx := range c.Transactions {
					if tx.Native.Family == portable.NonInviteClient {
						t.Fatal("successful NOTIFY retained")
					}
				}
				if c.Result.GetResponse() == nil {
					t.Fatal("public command outcome disappeared")
				}
				response, err := Observe(c.Result.Response.Wire, false)
				if err != nil {
					t.Fatal(err)
				}
				handled, err := fixture.Engine.TryNativeCompletion(context.Background(), c.Flow, response)
				if err != nil || handled {
					t.Fatal("retired duplicate gained native completion authority", handled, err)
				}
				after, err := fixture.Engine.store.Load(context.Background(), record.Ref)
				if err != nil {
					t.Fatal(err)
				}
				if after.Revision != record.Revision || !bytes.Equal(after.Payload.ProtocolState, record.Payload.ProtocolState) || len(after.Payload.Effects) != 0 {
					t.Fatal("duplicate completion changed retained state")
				}
			}
		})
	}
}
