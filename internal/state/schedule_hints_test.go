package state

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func scheduleHintGroups(t *testing.T, s *Store, partition uint16, count int) []Ref {
	t.Helper()
	refs := make([]Ref, 0, count)
	for n := 0; len(refs) < count; n++ {
		ref := group(t, s, fmt.Sprintf("schedule-hint-%d", n))
		if ref.Partition == partition {
			refs = append(refs, ref)
		}
	}
	return refs
}

func singleScheduleHints(ctx context.Context, s *Store, partition uint16, limit int) (HintBatch, error) {
	batches, err := s.ScheduleHints(ctx, partition, 1, limit)
	if err != nil {
		return HintBatch{}, err
	}
	return batches[0], nil
}

func TestScheduleHintsPreservesIndependentLimitsOrderingAndDueTime(t *testing.T) {
	s := testStore(t, nil)
	readyStore(t, s)
	ctx := context.Background()
	refs := scheduleHintGroups(t, s, 0, 21)
	keys := s.keys(refs[0])
	now, err := s.client.Time(ctx).Result()
	if err != nil {
		t.Fatal(err)
	}
	for n := 0; n < 16; n++ {
		if err := s.client.ZAdd(ctx, keys[1], redis.Z{Score: float64(now.UnixMilli() - 1000 + int64(n)), Member: s.keys(refs[n])[0]}).Err(); err != nil {
			t.Fatal(err)
		}
		if err := s.client.ZAdd(ctx, keys[2], redis.Z{Score: float64(now.UnixMilli() - 1000 + int64(n)), Member: s.keys(refs[n+4])[0]}).Err(); err != nil {
			t.Fatal(err)
		}
	}
	future := redis.Z{Score: float64(now.Add(time.Hour).UnixMilli()), Member: s.keys(refs[20])[0]}
	for _, key := range keys[1:] {
		if err := s.client.ZAdd(ctx, key, future).Err(); err != nil {
			t.Fatal(err)
		}
	}
	batch, err := singleScheduleHints(ctx, s, 0, 8)
	if err != nil || batch.ReadyError != nil || batch.DueError != nil {
		t.Fatalf("hint poll failed: %v / %v / %v", err, batch.ReadyError, batch.DueError)
	}
	if len(batch.Ready) != 8 || len(batch.Due) != 8 {
		t.Fatalf("one set consumed the other's limit: %+v", batch)
	}
	for n := 0; n < 8; n++ {
		if batch.Ready[n].Ref.ID != refs[n].ID || batch.Due[n].Ref.ID != refs[n+4].ID ||
			batch.Ready[n].DueMS != now.UnixMilli()-1000+int64(n) || batch.Due[n].DueMS != now.UnixMilli()-1000+int64(n) {
			t.Fatalf("per-set order or original score changed: %+v", batch)
		}
	}
	ready, err := s.ReadyHints(ctx, 0, 8)
	if err != nil {
		t.Fatal(err)
	}
	due, err := s.DueHints(ctx, 0, 8)
	if err != nil || !reflect.DeepEqual(batch.Ready, ready) || !reflect.DeepEqual(batch.Due, due) {
		t.Fatalf("batch differs from independent hint queries: %+v %v", batch, err)
	}
	// A full read still excludes future work; removing/rearming an index entry
	// must be visible on the next poll without retaining a local hint snapshot.
	all, err := singleScheduleHints(ctx, s, 0, 256)
	if err != nil || all.ReadyError != nil || all.DueError != nil || len(all.Ready) != 16 || len(all.Due) != 16 {
		t.Fatalf("future timer was scheduled early: %+v %v", all, err)
	}
	if err := s.client.ZRem(ctx, keys[2], s.keys(refs[4])[0]).Err(); err != nil {
		t.Fatal(err)
	}
	if err := s.client.ZAdd(ctx, keys[2], redis.Z{Score: 1, Member: future.Member}).Err(); err != nil {
		t.Fatal(err)
	}
	again, err := singleScheduleHints(ctx, s, 0, 8)
	if err != nil || again.ReadyError != nil || again.DueError != nil || len(again.Due) != 8 || again.Due[0].Ref.ID != refs[20].ID || again.Due[0].DueMS != 1 {
		t.Fatalf("fresh due time was not observed: %+v %v", again, err)
	}
	if count := s.client.Exists(ctx, s.keys(refs[0])[0], s.keys(refs[20])[0]).Val(); count != 0 {
		t.Fatal("reading hints allocated authoritative protocol state")
	}
	if s.client.ZCard(ctx, keys[1]).Val() != 17 || s.client.ZCard(ctx, keys[2]).Val() != 16 {
		t.Fatal("reading hints consumed or rewrote scheduling entries")
	}
}

func TestScheduleHintsPreservesClaimsAndPartitionIsolation(t *testing.T) {
	s := testStore(t, func(cfg *Config) { cfg.ClaimTTL = 5 * time.Second })
	readyStore(t, s)
	ctx := context.Background()
	refs := scheduleHintGroups(t, s, 0, 1)
	step := begin(t, s, refs[0], "retained-pending", true)
	for _, key := range s.keys(refs[0])[1:] {
		if err := s.client.ZAdd(ctx, key, redis.Z{Score: 1, Member: s.keys(refs[0])[0]}).Err(); err != nil {
			t.Fatal(err)
		}
	}
	batch, err := singleScheduleHints(ctx, s, 0, 8)
	if err != nil || batch.ReadyError != nil || batch.DueError != nil || len(batch.Ready) != 1 || len(batch.Due) != 1 || batch.Ready[0].Ref.ID != batch.Due[0].Ref.ID {
		t.Fatalf("overlapping hint classes were merged or lost: %+v %v", batch, err)
	}
	if _, err := s.Resume(ctx, step.Record.Ref, "another-worker"); !errors.Is(err, ErrBusy) {
		t.Fatal("a scheduling read released or replaced the retained claim", err)
	}
	other, err := singleScheduleHints(ctx, s, 1, 8)
	if err != nil || other.ReadyError != nil || other.DueError != nil || len(other.Ready) != 0 || len(other.Due) != 0 {
		t.Fatalf("another partition received this group's hints: %+v %v", other, err)
	}
}

func TestScheduleHintsCorruptClassDoesNotSuppressHealthyClass(t *testing.T) {
	for _, badClass := range []string{"ready", "due"} {
		for _, corruption := range []string{"member", "foreign-partition", "type"} {
			t.Run(badClass+"/"+corruption, func(t *testing.T) {
				s := testStore(t, nil)
				readyStore(t, s)
				ctx := context.Background()
				ref := scheduleHintGroups(t, s, 0, 1)[0]
				keys := s.keys(ref)
				bad, good := keys[1], keys[2]
				if badClass == "due" {
					bad, good = good, bad
				}
				if err := s.client.ZAdd(ctx, good, redis.Z{Score: 1, Member: keys[0]}).Err(); err != nil {
					t.Fatal(err)
				}
				if corruption == "type" {
					if err := s.client.Set(ctx, bad, "invalid", 0).Err(); err != nil {
						t.Fatal(err)
					}
				} else {
					member := "persistent-poisoned-member"
					if corruption == "foreign-partition" {
						foreign := scheduleHintGroups(t, s, 1, 1)[0]
						member = s.keys(foreign)[0]
					}
					if err := s.client.ZAdd(ctx, bad, redis.Z{Score: 1, Member: member}, redis.Z{Score: 2, Member: keys[0]}).Err(); err != nil {
						t.Fatal(err)
					}
				}
				for n := 0; n < 2; n++ {
					batch, err := singleScheduleHints(ctx, s, 0, 8)
					if err != nil {
						t.Fatal("index corruption failed the whole authority-checked poll", err)
					}
					badHints, goodHints := batch.Ready, batch.Due
					badError, goodError := batch.ReadyError, batch.DueError
					if badClass == "due" {
						badHints, goodHints = goodHints, badHints
						badError, goodError = goodError, badError
					}
					if !errors.Is(badError, ErrCorrupt) || len(badHints) != 0 || goodError != nil || len(goodHints) != 1 || goodHints[0].Ref.ID != ref.ID {
						t.Fatalf("persistent index corruption suppressed or contaminated the healthy class: %+v", batch)
					}
				}
			})
		}
	}
}
