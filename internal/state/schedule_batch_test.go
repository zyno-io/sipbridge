package state

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/redis/go-redis/v9"
)

func TestScheduleBatchWrapsWithIndependentLimitsAndFreshScores(t *testing.T) {
	s := testStore(t, func(cfg *Config) { cfg.Partitions = 8 })
	readyStore(t, s)
	ctx := context.Background()
	for p := uint16(0); p < s.PartitionCount(); p++ {
		refs := scheduleHintGroups(t, s, p, 4)
		keys := s.keys(refs[0])
		for n, ref := range refs {
			member := s.keys(ref)[0]
			if err := s.client.ZAdd(ctx, keys[1], redis.Z{Score: float64(n + 1), Member: member}).Err(); err != nil {
				t.Fatal(err)
			}
			if err := s.client.ZAdd(ctx, keys[2], redis.Z{Score: float64(4 - n), Member: member}).Err(); err != nil {
				t.Fatal(err)
			}
		}
	}
	batches, err := s.ScheduleHints(ctx, 7, 4, 2)
	if err != nil || len(batches) != 4 {
		t.Fatalf("wrapped batch failed: %+v %v", batches, err)
	}
	for n, partition := range []uint16{7, 0, 1, 2} {
		batch := batches[n]
		if batch.ReadyError != nil || batch.DueError != nil || len(batch.Ready) != 2 || len(batch.Due) != 2 {
			t.Fatalf("partition %d lost its independent limits: %+v", partition, batch)
		}
		for _, hints := range [][]Hint{batch.Ready, batch.Due} {
			if hints[0].Ref.Partition != partition || hints[1].Ref.Partition != partition || hints[0].DueMS != 1 || hints[1].DueMS != 2 {
				t.Fatalf("partition %d order or identity changed: %+v", partition, hints)
			}
		}
		standalone, err := singleScheduleHints(ctx, s, partition, 2)
		if err != nil || !reflect.DeepEqual(batch, standalone) {
			t.Fatalf("batch differs from individual partition %d: %+v %+v %v", partition, batch, standalone, err)
		}
	}
	// The next batch observes new scores; it never consumes the hints.
	key := s.partitionPrefix(0) + "due"
	if err := s.client.ZAdd(ctx, key, redis.Z{Score: 0, Member: s.keys(batches[1].Due[1].Ref)[0]}).Err(); err != nil {
		t.Fatal(err)
	}
	again, err := s.ScheduleHints(ctx, 7, 4, 2)
	if err != nil || again[1].Due[0].DueMS != 0 || s.client.ZCard(ctx, key).Val() != 4 {
		t.Fatalf("batch used stale scores or consumed an entry: %+v %v", again, err)
	}
}

func TestScheduleBatchIsolatesCorruptionByClassAndPartition(t *testing.T) {
	for _, corrupt := range []string{"type", "member", "foreign-partition"} {
		t.Run(corrupt, func(t *testing.T) {
			s := testStore(t, func(cfg *Config) { cfg.Partitions = 4 })
			readyStore(t, s)
			ctx := context.Background()
			for p := uint16(0); p < s.PartitionCount(); p++ {
				ref := scheduleHintGroups(t, s, p, 1)[0]
				for _, key := range s.keys(ref)[1:] {
					if err := s.client.ZAdd(ctx, key, redis.Z{Score: 1, Member: s.keys(ref)[0]}).Err(); err != nil {
						t.Fatal(err)
					}
				}
			}
			bad := s.partitionPrefix(1) + "ready"
			if err := s.client.Del(ctx, bad).Err(); err != nil {
				t.Fatal(err)
			}
			if corrupt == "type" {
				if err := s.client.Set(ctx, bad, "invalid-index-type", 0).Err(); err != nil {
					t.Fatal(err)
				}
			} else {
				member := "invalid-member"
				if corrupt == "foreign-partition" {
					member = s.keys(scheduleHintGroups(t, s, 2, 1)[0])[0]
				}
				if err := s.client.ZAdd(ctx, bad, redis.Z{Score: 1, Member: member}).Err(); err != nil {
					t.Fatal(err)
				}
			}
			batches, err := s.ScheduleHints(ctx, 0, 4, 8)
			if err != nil {
				t.Fatal(err)
			}
			for p, batch := range batches {
				if batch.DueError != nil || len(batch.Due) != 1 {
					t.Fatalf("partition %d due hints suppressed: %+v", p, batch)
				}
				if p == 1 {
					if !errors.Is(batch.ReadyError, ErrCorrupt) || len(batch.Ready) != 0 {
						t.Fatalf("bad class was accepted: %+v", batch)
					}
				} else if batch.ReadyError != nil || len(batch.Ready) != 1 {
					t.Fatalf("partition %d ready hints suppressed: %+v", p, batch)
				}
			}
		})
	}
}

func TestScheduleBatchRejectsInvalidBounds(t *testing.T) {
	s := testStore(t, func(cfg *Config) { cfg.Partitions = 16 })
	for _, sample := range []struct {
		first        uint16
		count, limit int
	}{
		{16, 1, 8}, {0, 0, 8}, {0, 9, 8}, {0, 1, 0}, {0, 1, 257},
	} {
		if _, err := s.ScheduleHints(context.Background(), sample.first, sample.count, sample.limit); err == nil {
			t.Fatalf("invalid bounds accepted: %+v", sample)
		}
	}
	small := testStore(t, nil)
	if _, err := small.ScheduleHints(context.Background(), 0, 3, 8); err == nil {
		t.Fatal("same partition could be selected twice")
	}
}
