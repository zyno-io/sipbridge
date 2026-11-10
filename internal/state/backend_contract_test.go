package state

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
)

func contractConfig() Config {
	return Config{Namespace: "contract-" + uuid.NewString(), NamespaceKey: bytes.Repeat([]byte{23}, 32),
		Versions: Versions{Schema: "2", Reducer: "9", Timers: "1"}, Partitions: 2,
		Limits: Limits{GroupBytes: 32 << 10, InputBytes: 1024, OutcomeBytes: 64, Receipts: 4}}
}
func backendContract(t *testing.T, test func(*testing.T, Backend)) {
	t.Helper()
	t.Run("memory", func(t *testing.T) {
		s, err := NewMemory(contractConfig())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { s.Close() })
		test(t, s)
	})
	t.Run("redis", func(t *testing.T) {
		s := testStore(t, func(cfg *Config) {
			cfg.Versions = Versions{Schema: "2", Reducer: "9", Timers: "1"}
			cfg.Limits = contractConfig().Limits
		})
		readyStore(t, s)
		test(t, s)
	})
}
func contractInput(id string, create bool) Input {
	now := time.Now().UnixMilli()
	return Input{ID: id, Kind: "command", Fingerprint: id, Create: create, DeadlineMS: now + 5000, ReceiptUntilMS: now + 10000}
}
func contractRead(t *testing.T, s Backend, ref Ref, in Input) Update {
	t.Helper()
	result, err := s.ReadUpdate(context.Background(), ref, in)
	if err != nil || result.Update == nil {
		t.Fatalf("read update: %+v %v", result, err)
	}
	return *result.Update
}
func contractRef(t *testing.T, s Backend, id string) Ref {
	t.Helper()
	ref, err := s.GroupForCallID(id)
	if err != nil {
		t.Fatal(err)
	}
	return ref
}

func TestBackendCASConflictReplayAndIsolation(t *testing.T) {
	backendContract(t, func(t *testing.T, s Backend) {
		ctx := context.Background()
		ref := contractRef(t, s, "cas")
		in := contractInput("create", true)
		first := contractRead(t, s, ref, in)
		loser := contractRead(t, s, ref, contractInput("loser", true))
		if _, err := s.Load(ctx, ref); !errors.Is(err, ErrNotFound) {
			t.Fatal("read allocated group", err)
		}
		next := Payload{ProtocolState: []byte("original"), Timers: []Timer{{ID: "timer", Generation: "1", DueMS: 1, DeadlineMS: 1}}}
		receipt, err := s.CompareAndSwap(ctx, first, next, []byte("result"))
		if err != nil {
			t.Fatal(err)
		}
		next.ProtocolState[0] = 'X'
		receipt.Outcome[0] = 'X'
		if _, err := s.CompareAndSwap(ctx, loser, Payload{}, nil); !errors.Is(err, ErrStale) {
			t.Fatal("losing creator committed", err)
		}
		record, err := s.Load(ctx, first.Record.Ref)
		if err != nil {
			t.Fatal(err)
		}
		if string(record.Payload.ProtocolState) != "original" {
			t.Fatal("commit retained shared payload")
		}
		record.Payload.ProtocolState[0] = 'X'
		record.Receipts[0].Outcome[0] = 'X'
		replay, err := s.ReadUpdate(ctx, first.Record.Ref, in)
		if err != nil || replay.Receipt == nil || string(replay.Receipt.Outcome) != "result" {
			t.Fatal("replay changed", err)
		}
		in.Fingerprint = "changed"
		if _, err := s.ReadUpdate(ctx, first.Record.Ref, in); !errors.Is(err, ErrConflict) {
			t.Fatal("conflicting replay accepted", err)
		}
		record, err = s.Load(ctx, first.Record.Ref)
		if err != nil || string(record.Payload.ProtocolState) != "original" {
			t.Fatal("load returned shared snapshot", err)
		}
		batch, err := s.ScheduleHints(ctx, first.Record.Ref.Partition, 1, 8)
		if err != nil || len(batch[0].Due) != 1 {
			t.Fatal("committed timer absent", err)
		}
		batch[0].Due[0].Ref.ID = "changed"
		again, err := s.ScheduleHints(ctx, first.Record.Ref.Partition, 1, 8)
		if err != nil || again[0].Due[0].Ref.ID != ref.ID {
			t.Fatal("shared hint", err)
		}
	})
}
func TestBackendCASConcurrentWinnerAndInputIntegrity(t *testing.T) {
	backendContract(t, func(t *testing.T, s Backend) {
		ctx := context.Background()
		ref := contractRef(t, s, "race")
		update := contractRead(t, s, ref, contractInput("create", true))
		var wins atomic.Int32
		var wg sync.WaitGroup
		for n := 0; n < 12; n++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, err := s.CompareAndSwap(ctx, update, Payload{}, nil)
				if err == nil {
					wins.Add(1)
				} else if !errors.Is(err, ErrStale) {
					t.Errorf("CAS race: %v", err)
				}
			}()
		}
		wg.Wait()
		if wins.Load() != 1 {
			t.Fatal("multiple winners", wins.Load())
		}
		changed := contractRead(t, s, update.Record.Ref, contractInput("changed", false))
		changed.Input.Payload = []byte("altered")
		if _, err := s.CompareAndSwap(ctx, changed, Payload{}, nil); !errors.Is(err, ErrConflict) {
			t.Fatal("mutated input accepted", err)
		}
		canceled, cancel := context.WithCancel(ctx)
		cancel()
		if _, err := s.ReadUpdate(canceled, ref, contractInput("cancelled", false)); !errors.Is(err, context.Canceled) {
			t.Fatal("cancellation ignored", err)
		}
	})
}
func TestBackendLeasesGuardAtomicCommitAndReplayCapacity(t *testing.T) {
	backendContract(t, func(t *testing.T, s Backend) {
		ctx := context.Background()
		lease, err := s.AcquireApplicationLease(ctx, "owner", uuid.NewString())
		if err != nil {
			t.Fatal(err)
		}
		same, err := s.AcquireApplicationLease(ctx, "owner", uuid.NewString())
		if err != nil || same.Term != lease.Term {
			t.Fatal("live term changed", err)
		}
		if _, err := s.RenewApplicationLease(ctx, "owner", uuid.NewString()); !errors.Is(err, ErrStale) {
			t.Fatal("wrong term renewed", err)
		}
		ref := contractRef(t, s, "guard")
		in := contractInput("guard", true)
		in.ApplicationLeaseIDs = []string{"owner"}
		update := contractRead(t, s, ref, in)
		bad := Payload{ApplicationLeaseIDs: []string{"owner"}, CommitLeaseGuards: []ApplicationLeaseRef{{ID: "owner", Term: uuid.NewString()}}}
		if _, err := s.CompareAndSwap(ctx, update, bad, nil); !errors.Is(err, ErrStale) {
			t.Fatal("wrong owner committed", err)
		}
		if _, err := s.Load(ctx, ref); !errors.Is(err, ErrNotFound) {
			t.Fatal("failed commit allocated state", err)
		}
		bad.CommitLeaseGuards[0].Term = lease.Term
		if _, err := s.CompareAndSwap(ctx, update, bad, nil); err != nil {
			t.Fatal(err)
		}
		for n := 0; n < 3; n++ {
			u := contractRead(t, s, update.Record.Ref, contractInput(fmt.Sprint("fill-", n), false))
			if _, err := s.CompareAndSwap(ctx, u, bad, nil); err != nil {
				t.Fatal(err)
			}
		}
		full := contractRead(t, s, update.Record.Ref, contractInput("full", false))
		if _, err := s.CompareAndSwap(ctx, full, bad, nil); !errors.Is(err, ErrCapacity) {
			t.Fatal("unbounded receipts", err)
		}
		cleanup := contractInput("cleanup", false)
		cleanup.Proof = true
		proof := contractRead(t, s, update.Record.Ref, cleanup)
		if _, err := s.CompareAndSwap(ctx, proof, bad, []byte("proof")); err != nil {
			t.Fatal("cleanup capacity unavailable", err)
		}
		timer := contractInput("timer", false)
		timer.Kind = "timer"
		timer.Proof = true
		tick := contractRead(t, s, update.Record.Ref, timer)
		if _, err := s.CompareAndSwap(ctx, tick, bad, nil); err != nil {
			t.Fatal(err)
		}
		inspected, err := s.InspectOperation(ctx, update.Record.Ref, "cleanup", "cleanup")
		if err != nil || inspected.Receipt == nil || string(inspected.Receipt.Outcome) != "proof" {
			t.Fatal("timer overwrote cleanup proof", err)
		}
	})
}
