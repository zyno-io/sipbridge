package state

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestStateViewOwnsPayloadAndPreservesReplay(t *testing.T) {
	backendContract(t, func(t *testing.T, s Backend) {
		ctx := context.Background()
		ref := contractRef(t, s, "state-view")
		if _, _, err := s.ReadState(ctx, ref); !errors.Is(err, ErrNotFound) {
			t.Fatal("missing view allocated or accepted a group", err)
		}
		in := contractInput("original", true)
		update := contractRead(t, s, ref, in)
		deadline := time.Now().Add(time.Minute).UnixMilli()
		payload := Payload{ProtocolState: []byte("protocol"), ActiveTransactions: 1,
			Effects: []Effect{{ID: "effect", Kind: "event", Status: EffectPending, DeadlineMS: deadline, Payload: []byte("event")}},
			Timers:  []Timer{{ID: "timer", Generation: "1", DueMS: deadline, DeadlineMS: deadline}}}
		receipt, err := s.CompareAndSwap(ctx, update, payload, []byte("original-result"))
		if err != nil {
			t.Fatal(err)
		}
		view, now, err := s.ReadState(ctx, update.Record.Ref)
		if err != nil || now <= 0 || view.HasPending || view.Ref != update.Record.Ref || view.Revision != receipt.Revision ||
			view.Authority != s.Authority() || view.Versions != s.Versions() {
			t.Fatalf("view lost its validated identity/version/time: %+v %v", view, err)
		}
		full, err := s.Load(ctx, view.Ref)
		if err != nil || !reflect.DeepEqual(view.Payload, full.Payload) {
			t.Fatal("view changed the selected payload", err)
		}
		view.Payload.ProtocolState[0] = 'X'
		view.Payload.Effects[0].Payload[0] = 'X'
		view.Payload.Timers[0].ID = "changed"
		next, _, err := s.ReadState(ctx, view.Ref)
		if err != nil || !reflect.DeepEqual(next.Payload, full.Payload) {
			t.Fatal("view shared mutable state with its backend", err)
		}
		replayed, err := s.ReadUpdate(ctx, view.Ref, in)
		if err != nil || replayed.Receipt == nil || string(replayed.Receipt.Outcome) != "original-result" {
			t.Fatal("state view changed exact replay", err)
		}
		stale := view.Ref
		stale.Incarnation = "another-incarnation"
		if _, _, err := s.ReadState(ctx, stale); !errors.Is(err, ErrStale) {
			t.Fatal("state view accepted a stale incarnation", err)
		}
		borrowed := view.Ref
		borrowed.CallID = "another-call"
		if _, _, err := s.ReadState(ctx, borrowed); !errors.Is(err, ErrConflict) {
			t.Fatal("state view accepted another Call-ID", err)
		}
	})
}

func TestRedisStateViewScopesValidationWithoutCertifyingHistory(t *testing.T) {
	s := testStore(t, nil)
	readyStore(t, s)
	ctx := context.Background()
	ref := group(t, s, "state-view-history")
	update := readTestUpdate(t, s, ref, input("original", true))
	if _, err := s.CompareAndSwap(ctx, update, Payload{ProtocolState: []byte("protocol")}, []byte("result")); err != nil {
		t.Fatal(err)
	}
	key := s.keys(ref)[0]
	for _, field := range []string{"receipts", "proof", "pending"} {
		t.Run(field, func(t *testing.T) {
			original := s.client.HGet(ctx, key, field).Val()
			if err := s.client.HSet(ctx, key, field, "invalid-unselected-json").Err(); err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := s.client.HSet(ctx, key, field, original).Err(); err != nil {
					t.Error(err)
				}
			}()
			view, _, err := s.ReadState(ctx, update.Record.Ref)
			if err != nil || string(view.Payload.ProtocolState) != "protocol" || view.HasPending != (field == "pending") {
				t.Fatal("state-only projection decoded unselected history", err)
			}
			if _, err := s.Load(ctx, update.Record.Ref); err == nil || field != "pending" && !errors.Is(err, ErrCorrupt) {
				t.Fatal("full record stopped refusing corrupt history through its existing error path", err)
			}
		})
	}
	if err := s.client.HSet(ctx, key, "data", "invalid-selected-binary").Err(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.ReadState(ctx, update.Record.Ref); !errors.Is(err, ErrCorrupt) {
		t.Fatal("state view accepted corrupt selected payload", err)
	}
}

func TestRedisStateViewDoesNotInitializeOwnerRecovery(t *testing.T) {
	s := testStore(t, nil)
	readyStore(t, s)
	ctx := context.Background()
	lease, err := s.AcquireApplicationLease(ctx, "owner", uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	in := input("original", true)
	in.ApplicationLeaseIDs = []string{"owner"}
	update := readTestUpdate(t, s, group(t, s, "state-view-owner"), in)
	payload := Payload{ApplicationLeaseIDs: []string{"owner"}, CommitLeaseGuards: []ApplicationLeaseRef{{ID: "owner", Term: lease.Term}}}
	if _, err := s.CompareAndSwap(ctx, update, payload, nil); err != nil {
		t.Fatal(err)
	}
	key := s.applicationLeaseKeys("owner")[0]
	if err := s.client.HSet(ctx, key, "expiresAtMs", "1", "recoveryUntilMs", "0").Err(); err != nil {
		t.Fatal(err)
	}
	before := s.client.HGetAll(ctx, key).Val()
	if _, _, err := s.ReadState(ctx, update.Record.Ref); err != nil {
		t.Fatal(err)
	}
	if after := s.client.HGetAll(ctx, key).Val(); !reflect.DeepEqual(before, after) {
		t.Fatal("state projection initialized owner recovery")
	}
	if _, err := s.Load(ctx, update.Record.Ref); err != nil {
		t.Fatal(err)
	}
	if s.client.HGet(ctx, key, "recoveryUntilMs").Val() == "0" {
		t.Fatal("full record lost its normal lease observation")
	}
}

func TestMemoryStateViewDoesNotInitializeOwnerRecovery(t *testing.T) {
	s, clock := memoryClockStore(t, nil)
	ctx := context.Background()
	lease, err := s.AcquireApplicationLease(ctx, "owner", uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	in := contractInput("original", true)
	in.ApplicationLeaseIDs = []string{"owner"}
	update := contractRead(t, s, contractRef(t, s, "memory-state-view-owner"), in)
	payload := Payload{ApplicationLeaseIDs: []string{"owner"}, CommitLeaseGuards: []ApplicationLeaseRef{{ID: "owner", Term: lease.Term}}}
	if _, err := s.CompareAndSwap(ctx, update, payload, nil); err != nil {
		t.Fatal(err)
	}
	*clock = clock.Add(31 * time.Second)
	before := *s.leases["owner"]
	if _, _, err := s.ReadState(ctx, update.Record.Ref); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, *s.leases["owner"]) {
		t.Fatal("state projection initialized memory-owner recovery")
	}
	if _, err := s.Load(ctx, update.Record.Ref); err != nil {
		t.Fatal(err)
	}
	if reflect.DeepEqual(before, *s.leases["owner"]) {
		t.Fatal("full memory record lost its normal lease observation")
	}
}

var benchmarkStateView StateView

func BenchmarkProtocolViewWithoutReceiptDecoding(b *testing.B) {
	s, ref, fields := retainedDecodeFixture(b)
	payload, err := (Payload{ProtocolState: bytes.Repeat([]byte("p"), 1024)}).MarshalBinary()
	if err != nil {
		b.Fatal(err)
	}
	fields.data = string(payload)
	// The bounded replay-window envelope is intentionally separate from the
	// selected protocol payload. This benchmark excludes Redis and native FSMs.
	receipts := make([]Receipt, 16)
	for n := range receipts {
		in := contractInput(fmt.Sprintf("operation-%d", n), false)
		digest, _, err := digestInput(in)
		if err != nil {
			b.Fatal(err)
		}
		receipts[n] = Receipt{OperationID: in.ID, Fingerprint: in.Fingerprint, InputDigest: digest, Revision: fmt.Sprint(n + 1),
			ExpiresMS: in.ReceiptUntilMS, Outcome: bytes.Repeat([]byte("r"), 4096)}
	}
	encoded, err := json.Marshal(receipts)
	if err != nil {
		b.Fatal(err)
	}
	fields.receipts = string(encoded)
	b.Run("full-record", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			record, err := decodeRetainedRecord(s, ref, fields)
			if err != nil {
				b.Fatal(err)
			}
			capturedBenchmarkRecord = record
		}
	})
	b.Run("protocol-view", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			record, err := s.decodeProtocolRecord(ref, fields.inc, fields.revision, []byte(fields.data), fields.callID)
			if err != nil {
				b.Fatal(err)
			}
			benchmarkStateView = protocolView(record, false)
		}
	})
}
