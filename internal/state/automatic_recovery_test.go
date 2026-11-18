package state

import (
	"context"
	"errors"
	"testing"
)

func TestNamespaceLossRecoversWithoutSetupAndFencesOldIncarnation(t *testing.T) {
	s := testStore(t, nil)
	ctx := context.Background()
	ref := group(t, s, "lost-call")
	first, err := s.ReadUpdate(ctx, ref, input("first", true))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.CompareAndSwap(ctx, *first.Update, Payload{ProtocolState: []byte("original")}, nil); err != nil {
		t.Fatal(err)
	}
	exact := first.Update.Record.Ref
	pending, err := s.ReadUpdate(ctx, exact, input("in-flight", false))
	if err != nil {
		t.Fatal(err)
	}

	// Delete only this test namespace, reproducing volatile Redis state loss.
	var cursor uint64
	for {
		keys, next, err := s.client.Scan(ctx, cursor, s.root+"*", 128).Result()
		if err != nil {
			t.Fatal(err)
		}
		if len(keys) > 0 {
			if err := s.client.Del(ctx, keys...).Err(); err != nil {
				t.Fatal(err)
			}
		}
		if next == 0 {
			break
		}
		cursor = next
	}
	if err := s.Ready(ctx); err != nil {
		t.Fatal("empty store requires setup", err)
	}
	if _, err := s.Load(ctx, exact); !errors.Is(err, ErrNotFound) {
		t.Fatal("lost dialog did not fail", err)
	}
	if _, err := s.CompareAndSwap(ctx, *pending.Update, Payload{ProtocolState: []byte("stale")}, nil); !errors.Is(err, ErrStale) && !errors.Is(err, ErrNotFound) {
		t.Fatal("old computation recreated state", err)
	}
	if _, err := s.ReadUpdate(ctx, exact, input("old-reference", true)); !errors.Is(err, ErrNotFound) {
		t.Fatal("old exact reference recreated state", err)
	}
	replacement, err := s.ReadUpdate(ctx, ref, input("fresh-call", true))
	if err != nil {
		t.Fatal("fresh allocation blocked", err)
	}
	if replacement.Update.Record.Ref.Incarnation == exact.Incarnation {
		t.Fatal("fresh call reused lost incarnation")
	}
	if _, err := s.CompareAndSwap(ctx, *replacement.Update, Payload{ProtocolState: []byte("replacement")}, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Load(ctx, exact); !errors.Is(err, ErrStale) {
		t.Fatal("old reference adopted replacement", err)
	}
	if _, err := s.CompareAndSwap(ctx, *pending.Update, Payload{ProtocolState: []byte("stale")}, nil); !errors.Is(err, ErrStale) {
		t.Fatal("old computation overwrote replacement", err)
	}
	restored, err := s.Load(ctx, replacement.Update.Record.Ref)
	if err != nil || string(restored.Payload.ProtocolState) != "replacement" {
		t.Fatal("replacement changed", err)
	}
}
