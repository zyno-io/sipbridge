package state

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
)

func readTestUpdate(t *testing.T, s *Store, ref Ref, in Input) Update {
	t.Helper()
	result, err := s.ReadUpdate(context.Background(), ref, in)
	if err != nil || result.Update == nil || result.Receipt != nil {
		t.Fatalf("expected fresh update: %+v %v", result, err)
	}
	return *result.Update
}

func TestCASExpiredLeaseRefusalDoesNotAllocatePartialGroup(t *testing.T) {
	s := testStore(t, nil)
	readyStore(t, s)
	ctx := context.Background()
	lease, err := s.AcquireApplicationLease(ctx, "owner", uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	ref := group(t, s, "lease-expiry-during-reduction")
	in := input("new-context", true)
	in.ApplicationLeaseIDs = []string{"owner"}
	update := readTestUpdate(t, s, ref, in)
	expireApplicationLease(t, s, "owner")
	next := Payload{ApplicationLeaseIDs: []string{"owner"}, CommitLeaseGuards: []ApplicationLeaseRef{{ID: "owner", Term: lease.Term}}}
	if _, err := s.CompareAndSwap(ctx, update, next, nil); !errors.Is(err, ErrStale) {
		t.Fatal("expired owner accepted at commit", err)
	}
	if s.client.Exists(ctx, s.keys(ref)[0]).Val() != 0 {
		t.Fatal("lease refusal left a partially allocated group")
	}
}

func TestCASOriginalDeadlineStillAppliesAtCommit(t *testing.T) {
	s := testStore(t, nil)
	readyStore(t, s)
	ref := group(t, s, "delayed-computation")
	in := input("late", true)
	in.DeadlineMS = time.Now().Add(200 * time.Millisecond).UnixMilli()
	update := readTestUpdate(t, s, ref, in)
	time.Sleep(250 * time.Millisecond)
	if _, err := s.CompareAndSwap(context.Background(), update, Payload{}, nil); !errors.Is(err, ErrExpired) {
		t.Fatal("late computation committed after original deadline", err)
	}
}

func TestCASReadDoesNotAllocateAndCommitRetainsOnlyExternalReplay(t *testing.T) {
	s := testStore(t, nil)
	readyStore(t, s)
	ctx := context.Background()
	ref := group(t, s, "pure-computation")
	in := input("command", true)
	update := readTestUpdate(t, s, ref, in)
	if s.client.Exists(ctx, s.keys(ref)[0]).Val() != 0 {
		t.Fatal("read allocated a group")
	}
	receipt, err := s.CompareAndSwap(ctx, update, Payload{ProtocolState: []byte("committed")}, []byte("result"))
	if err != nil || receipt.Revision != "1" {
		t.Fatalf("create CAS failed: %+v %v", receipt, err)
	}
	ref = update.Record.Ref
	record, err := s.Load(ctx, ref)
	if err != nil || record.Pending != nil || record.Proof != nil || len(record.Receipts) != 1 || string(record.Payload.ProtocolState) != "committed" {
		t.Fatalf("unexpected retained computation: %+v %v", record, err)
	}
	replay, err := s.ReadUpdate(ctx, ref, in)
	if err != nil || replay.Update != nil || replay.Receipt == nil || string(replay.Receipt.Outcome) != "result" {
		t.Fatalf("exact external result was not retained: %+v %v", replay, err)
	}
	in.Fingerprint = "altered-payload"
	if _, err := s.ReadUpdate(ctx, ref, in); !errors.Is(err, ErrConflict) {
		t.Fatal("altered external replay accepted", err)
	}
	internal := input("timer", false)
	internal.Kind, internal.Proof = "timer", true
	update = readTestUpdate(t, s, ref, internal)
	if _, err := s.CompareAndSwap(ctx, update, record.Payload, nil); err != nil {
		t.Fatal(err)
	}
	record, err = s.Load(ctx, ref)
	if err != nil || record.Pending != nil || record.Proof != nil || len(record.Receipts) != 1 {
		t.Fatalf("internal computation allocated replay history: %+v %v", record, err)
	}
}

func TestCASCompetingUpdateAndLegacyBeginCannotOverwrite(t *testing.T) {
	s := testStore(t, nil)
	readyStore(t, s)
	ctx := context.Background()
	ref := group(t, s, "competing-update")
	first := readTestUpdate(t, s, ref, input("first", true))
	second := readTestUpdate(t, s, ref, input("second", true))
	if _, err := s.CompareAndSwap(ctx, first, Payload{}, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CompareAndSwap(ctx, second, Payload{}, nil); !errors.Is(err, ErrStale) {
		t.Fatal("competing creator overwrote winner", err)
	}
	ref = first.Record.Ref
	update := readTestUpdate(t, s, ref, input("new-driver", false))
	legacy := begin(t, s, ref, "old-driver", false)
	if _, err := s.CompareAndSwap(ctx, update, Payload{}, nil); !errors.Is(err, ErrStale) {
		t.Fatal("CAS overwrote legacy pending input", err)
	}
	if _, err := s.ReadUpdate(ctx, ref, input("new-driver", false)); !errors.Is(err, ErrBusy) {
		t.Fatal("read stole legacy pending input", err)
	}
	if _, err := s.Commit(ctx, *legacy, Payload{}, nil); err != nil {
		t.Fatal("legacy worker could not finish overlap", err)
	}
	update = readTestUpdate(t, s, ref, input("new-driver", false))
	if _, err := s.CompareAndSwap(ctx, update, Payload{}, nil); err != nil {
		t.Fatal("new worker could not continue retained state", err)
	}
}

func TestCASOldIncarnationCannotAffectRecreatedGroup(t *testing.T) {
	s := testStore(t, nil)
	readyStore(t, s)
	ctx := context.Background()
	ref := group(t, s, "recreated-group")
	first := readTestUpdate(t, s, ref, input("first", true))
	if _, err := s.CompareAndSwap(ctx, first, Payload{}, nil); err != nil {
		t.Fatal(err)
	}
	stale := readTestUpdate(t, s, first.Record.Ref, input("stale", true))
	if err := s.client.Del(ctx, s.keys(ref)[0]).Err(); err != nil {
		t.Fatal(err)
	}
	winner := readTestUpdate(t, s, ref, input("replacement", true))
	if _, err := s.CompareAndSwap(ctx, winner, Payload{}, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CompareAndSwap(ctx, stale, Payload{}, nil); !errors.Is(err, ErrStale) {
		t.Fatal("old incarnation changed replacement", err)
	}
}

func TestCASCleanupReplayUsesReservedProofWithoutInternalReplacement(t *testing.T) {
	s := testStore(t, func(cfg *Config) { cfg.Limits.Receipts = 1 })
	readyStore(t, s)
	ctx := context.Background()
	ref := group(t, s, "cleanup-replay")
	first := readTestUpdate(t, s, ref, input("ordinary", true))
	if _, err := s.CompareAndSwap(ctx, first, Payload{}, nil); err != nil {
		t.Fatal(err)
	}
	ref = first.Record.Ref
	cleanup := input("cleanup", false)
	cleanup.Kind, cleanup.Proof = "command", true
	update := readTestUpdate(t, s, ref, cleanup)
	if _, err := s.CompareAndSwap(ctx, update, Payload{}, []byte("cleanup-result")); err != nil {
		t.Fatal("ordinary history blocked cleanup", err)
	}
	internal := input("completion", false)
	internal.Kind, internal.Proof = "complete", true
	update = readTestUpdate(t, s, ref, internal)
	if _, err := s.CompareAndSwap(ctx, update, Payload{}, nil); err != nil {
		t.Fatal(err)
	}
	replay, err := s.ReadUpdate(ctx, ref, cleanup)
	if err != nil || replay.Receipt == nil || string(replay.Receipt.Outcome) != "cleanup-result" {
		t.Fatalf("internal work replaced cleanup proof: %+v %v", replay, err)
	}
}

func TestCASReplayRemainsAvailableDuringLegacyPendingInput(t *testing.T) {
	for _, cleanup := range []bool{false, true} {
		t.Run(fmt.Sprintf("cleanup=%t", cleanup), func(t *testing.T) {
			s := testStore(t, nil)
			readyStore(t, s)
			ctx := context.Background()
			ref := group(t, s, "replay-with-pending")
			in := input("known", true)
			in.Kind, in.Proof = "command", cleanup
			update := readTestUpdate(t, s, ref, in)
			if _, err := s.CompareAndSwap(ctx, update, Payload{}, []byte("known-result")); err != nil {
				t.Fatal(err)
			}
			ref = update.Record.Ref
			begin(t, s, ref, "unrelated-legacy-input", false)
			result, err := s.ReadUpdate(ctx, ref, in)
			if err != nil || result.Receipt == nil || string(result.Receipt.Outcome) != "known-result" {
				t.Fatalf("pending work hid a committed result: %+v %v", result, err)
			}
			in.Fingerprint = "different-payload"
			if _, err := s.ReadUpdate(ctx, ref, in); !errors.Is(err, ErrConflict) {
				t.Fatal("pending work hid an altered replay", err)
			}
		})
	}
}
