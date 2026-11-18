package rediscore

import (
	"bytes"
	"context"
	"errors"
	"maps"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

func checkNamespace(t *testing.T, s *Store, c *redis.Client, ids ...Identity) {
	t.Helper()
	ctx := context.Background()
	var bytes int64
	for _, id := range ids {
		h, err := c.HGetAll(ctx, s.contextKey(id.Context)).Result()
		if err != nil {
			t.Fatal(err)
		}
		if len(h) == 0 {
			continue
		}
		bytes += int64(len(s.contextKey(id.Context)))
		for k, v := range h {
			bytes += int64(len(k) + len(v))
		}
		reserved, e := strconv.ParseInt(h["reserved"], 10, 64)
		if e != nil {
			t.Fatal(e)
		}
		bytes += reserved
		// Index projections are independently calculated from the actual structures.
		var indexes int64
		if h["owner"] != "" {
			member, e := c.SIsMember(ctx, s.applicationOwnedKey(h["term"]), id.Context).Result()
			if e != nil || !member {
				t.Fatalf("missing owner membership: %v", e)
			}
			indexes += int64(len(id.Context))
		}
		deadlines, err := c.ZRangeWithScores(ctx, s.contextDeadlineKey(id.Context), 0, -1).Result()
		if err != nil {
			t.Fatal(err)
		}
		if len(deadlines) > 0 {
			indexes += int64(len(s.contextDeadlineKey(id.Context))) + int64(len(id.Context)+8)
		}
		for _, v := range deadlines {
			member, ok := v.Member.(string)
			if !ok {
				t.Fatal("invalid deadline member")
			}
			indexes += int64(len(member) + 8)
		}
		for _, worker := range []string{"worker1", "worker2"} {
			ready, err := c.ZRange(ctx, s.workerReadyKey(worker), 0, -1).Result()
			if err != nil {
				t.Fatal(err)
			}
			for _, member := range ready {
				if len(member) > len(id.Context) && member[:len(id.Context)+1] == id.Context+"/" {
					indexes += int64(len(member) + 8)
				}
			}
		}
		selectorEntries, err := c.HGetAll(ctx, s.selectorKey()).Result()
		if err != nil {
			t.Fatal(err)
		}
		for field, value := range selectorEntries {
			target, e := parseSelectorTarget(value)
			if e != nil {
				t.Fatal(e)
			}
			if target.Context == id.Context && target.Incarnation == id.Incarnation {
				indexes += int64(len(field) + len(value))
			}
		}
		encoded, e := strconv.ParseInt(h["index_bytes"], 10, 64)
		if e != nil || encoded != indexes {
			t.Fatalf("index accounting got %d want %d (%v)", encoded, indexes, e)
		}
		bytes += indexes
	}
	cap, err := c.HGetAll(ctx, s.capacityKey()).Result()
	if err != nil {
		t.Fatal(err)
	}
	got, err := strconv.ParseInt(cap["bytes"], 10, 64)
	if err != nil || got != bytes {
		t.Fatalf("namespace bytes got %d want %d: %v", got, bytes, err)
	}
}

func TestNamespaceCapacityRaceAndRetire(t *testing.T) {
	s, c, id := fixture(t)
	ctx := context.Background()
	s.namespaceContexts = 1
	peer, err := New(c, Config{Root: s.root, Profile: s.profile, RecordBudget: s.maxBytes, PrimaryRunID: s.primary, NamespaceMaxContexts: 1})
	if err != nil {
		t.Fatal(err)
	}
	other := Identity{Context: "ctx-" + uuid.NewString(), Incarnation: uuid.NewString()}
	t.Cleanup(func() {
		if e := c.Del(ctx, s.contextKey(other.Context)).Err(); e != nil {
			t.Error(e)
		}
	})
	var wg sync.WaitGroup
	var mu sync.Mutex
	success, capacity := 0, 0
	var winner Identity
	for i, item := range []Identity{id, other} {
		wg.Add(1)
		go func(n int, x Identity) {
			defer wg.Done()
			node := s
			if n == 1 {
				node = peer
			}
			_, e := node.Commit(ctx, Publication{Identity: x, Create: true, Deadline: time.Now().Add(time.Minute), Changes: []Change{{Field: "r:duty", Value: []byte("work")}}})
			mu.Lock()
			defer mu.Unlock()
			if e == nil {
				success++
				winner = x
			} else if errors.Is(e, ErrCapacity) {
				capacity++
			} else {
				t.Errorf("commit: %v", e)
			}
		}(i, item)
	}
	wg.Wait()
	if success != 1 || capacity != 1 {
		t.Fatalf("success=%d capacity=%d", success, capacity)
	}
	checkNamespace(t, s, c, id, other)
	if e := s.RetireContext(ctx, winner, 1, time.Now().Add(time.Minute), true); !errors.Is(e, ErrConflict) {
		t.Fatal("duty retired", e)
	}
	_, err = s.Commit(ctx, Publication{Identity: winner, Revision: 1, Deadline: time.Now().Add(time.Minute), Changes: []Change{{Field: "r:duty", Delete: true}}})
	if err != nil {
		t.Fatal(err)
	}
	checkNamespace(t, s, c, id, other)
	if err = s.RetireContext(ctx, winner, 2, time.Now().Add(time.Minute), true); err != nil {
		t.Fatal(err)
	}
	cap, err := c.HGetAll(ctx, s.capacityKey()).Result()
	if err != nil || cap["contexts"] != "0" || cap["bytes"] != "0" {
		t.Fatal("retirement accounting", cap, err)
	}
	if err = s.RetireContext(ctx, winner, 2, time.Now().Add(time.Minute), true); !errors.Is(err, ErrAbsent) {
		t.Fatal("absence inferred a result", err)
	}
}
func TestNamespaceOutputAndDeadlineAccounting(t *testing.T) {
	s, c, id, out, data := outputFixture(t)
	ctx := context.Background()
	now := time.Now()
	assignment := TimerAssignment{Worker: out.Worker, Epoch: out.Epoch, Term: "timer-term"}
	t.Cleanup(func() {
		if e := c.Del(ctx, s.contextDeadlineKey(id.Context), s.workerTimerKey(out.Worker)).Err(); e != nil {
			t.Error(e)
		}
	})
	p := Publication{Identity: id, Create: true, Deadline: now.Add(time.Minute), Reserved: 111, Changes: []Change{{Field: out.MessageField, Value: data}, {Field: out.MetadataField, Value: []byte("metadata")}}, NewOutputs: []OutputIntent{out}, Timers: &TimerMutation{Target: assignment, Changes: []TimerChange{{Member: "notify", Due: now.Add(time.Minute)}}}}
	if _, err := s.Commit(ctx, p); err != nil {
		t.Fatal(err)
	}
	checkNamespace(t, s, c, id)
	ref := outputRef(id, out)
	if _, err := s.ReserveOutput(ctx, ref); err != nil {
		t.Fatal(err)
	}
	checkNamespace(t, s, c, id)
	if err := s.CompleteOutput(ctx, ref, "WRITTEN"); err != nil {
		t.Fatal(err)
	}
	checkNamespace(t, s, c, id)
	if err := s.SettleOutput(ctx, ref, true, true); err != nil {
		t.Fatal(err)
	}
	checkNamespace(t, s, c, id)
	p = Publication{Identity: id, Revision: 1, Deadline: now.Add(time.Minute), Changes: []Change{{Field: out.MessageField, Delete: true}, {Field: out.MetadataField, Delete: true}}, Timers: &TimerMutation{Expected: assignment, Target: assignment, Changes: []TimerChange{{Member: "notify", Delete: true}}}}
	if _, err := s.Commit(ctx, p); err != nil {
		t.Fatal(err)
	}
	checkNamespace(t, s, c, id)
	if err := s.RetireContext(ctx, id, 2, now.Add(time.Minute), true); err != nil {
		t.Fatal(err)
	}
	checkNamespace(t, s, c, id)
}

func TestNamespaceByteLimitRefusesWholePublication(t *testing.T) {
	s, c, id, out, data := outputFixture(t)
	ctx := context.Background()
	now := time.Now()
	publishOutput(t, s, id, out, data)
	checkNamespace(t, s, c, id)
	current, err := c.HGet(ctx, s.capacityKey(), "bytes").Int64()
	if err != nil {
		t.Fatal(err)
	}
	s.namespaceBytes = current + 4
	before, err := c.HGetAll(ctx, s.contextKey(id.Context)).Result()
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.Commit(ctx, Publication{Identity: id, Revision: 1, Deadline: now.Add(time.Minute), Changes: []Change{{Field: "r:large", Value: []byte("must refuse")}}})
	if !errors.Is(err, ErrCapacity) {
		t.Fatal("byte ceiling not enforced", err)
	}
	after, err := c.HGetAll(ctx, s.contextKey(id.Context)).Result()
	if err != nil {
		t.Fatal(err)
	}
	if !maps.Equal(before, after) {
		t.Fatal("capacity refusal changed context")
	}
	checkNamespace(t, s, c, id)
}

func TestNamespaceUnknownOutputCannotRetire(t *testing.T) {
	s, c, id, out, data := outputFixture(t)
	ctx := context.Background()
	publishOutput(t, s, id, out, data)
	ref := outputRef(id, out)
	if err := s.RetireContext(ctx, id, 1, time.Now().Add(time.Minute), true); !errors.Is(err, ErrConflict) {
		t.Fatal("pending output retired", err)
	}
	if _, err := s.ReserveOutput(ctx, ref); err != nil {
		t.Fatal(err)
	}
	if err := s.CompleteOutput(ctx, ref, "UNKNOWN"); err != nil {
		t.Fatal(err)
	}
	checkNamespace(t, s, c, id)
	if err := s.SettleOutput(ctx, ref, true, true); !errors.Is(err, ErrConflict) {
		t.Fatal("unknown proof settled", err)
	}
	if err := s.RetireContext(ctx, id, 1, time.Now().Add(time.Minute), true); !errors.Is(err, ErrConflict) {
		t.Fatal("unknown output retired", err)
	}
	if err := s.CompleteOutput(ctx, ref, "WRITTEN"); err != nil {
		t.Fatal(err)
	}
	if err := s.SettleOutput(ctx, ref, true, true); err != nil {
		t.Fatal(err)
	}
	checkNamespace(t, s, c, id)
}
func TestNamespaceUnstartedNoWriteRefundsReady(t *testing.T) {
	s, c, id, out, data := outputFixture(t)
	ctx := context.Background()
	publishOutput(t, s, id, out, data)
	checkNamespace(t, s, c, id)
	if err := s.CompleteOutput(ctx, outputRef(id, out), "NOT_WRITTEN"); err != nil {
		t.Fatal(err)
	}
	checkNamespace(t, s, c, id)
	if err := s.SettleOutput(ctx, outputRef(id, out), true, true); err != nil {
		t.Fatal(err)
	}
	checkNamespace(t, s, c, id)
}

func TestNamespaceTwoClientsCompeteForBytes(t *testing.T) {
	s, c, id := fixture(t)
	ctx := context.Background()
	other := Identity{Context: "ctx-" + uuid.NewString(), Incarnation: uuid.NewString()}
	t.Cleanup(func() {
		if err := c.Del(ctx, s.contextKey(other.Context)).Err(); err != nil {
			t.Error(err)
		}
	})
	publish(t, s, id)
	publish(t, s, other)
	current, err := c.HGet(ctx, s.capacityKey(), "bytes").Int64()
	if err != nil {
		t.Fatal(err)
	}
	s.namespaceBytes = current + 100
	peer, err := New(c, Config{Root: s.root, Profile: s.profile, RecordBudget: s.maxBytes, PrimaryRunID: s.primary, NamespaceMaxBytes: s.namespaceBytes})
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	var mu sync.Mutex
	accepted, refused := 0, 0
	for n, item := range []Identity{id, other} {
		wg.Add(1)
		go func(i int, x Identity) {
			defer wg.Done()
			node := s
			if i == 1 {
				node = peer
			}
			_, e := node.Commit(ctx, Publication{Identity: x, Revision: 1, Deadline: time.Now().Add(time.Minute), Changes: []Change{{Field: "r:one", Value: bytes.Repeat([]byte{'x'}, 70)}}})
			mu.Lock()
			defer mu.Unlock()
			if e == nil {
				accepted++
			} else if errors.Is(e, ErrCapacity) {
				refused++
			} else {
				t.Errorf("commit: %v", e)
			}
		}(n, item)
	}
	wg.Wait()
	if accepted != 1 || refused != 1 {
		t.Fatalf("accepted=%d refused=%d", accepted, refused)
	}
	checkNamespace(t, s, c, id, other)
}
