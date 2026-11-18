package rediscore

import (
	"context"
	"errors"
	"maps"
	"testing"
	"time"
)

func TestTerminalPublicationRemovesLastRecordsAndIndexesAtomically(t *testing.T) {
	s, client, id, output, _ := outputFixture(t)
	ctx := context.Background()
	a := TimerAssignment{Worker: output.Worker, Epoch: output.Epoch, Term: output.Epoch}
	selector := Selector{Kind: SelectorServerTransaction, Digest: [32]byte{1}}
	t.Cleanup(func() { _ = client.Del(ctx, s.contextDeadlineKey(id.Context), s.workerTimerKey(a.Worker)).Err() })
	_, err := s.Commit(ctx, Publication{Identity: id, Create: true, Deadline: time.Now().Add(time.Minute), Reserved: 100,
		Changes:   []Change{{Field: "r:context", Value: []byte("context")}, {Field: "r:procedure", Value: []byte("procedure")}},
		Selectors: []SelectorChange{{Selector: selector, Record: "r:procedure"}},
		Timers:    &TimerMutation{Target: a, Changes: []TimerChange{{Member: "retire", Due: time.Now().Add(time.Minute)}}}})
	if err != nil {
		t.Fatal(err)
	}
	before, err := client.HGetAll(ctx, s.contextKey(id.Context)).Result()
	if err != nil {
		t.Fatal(err)
	}
	p := Publication{Identity: id, Revision: 1, Deadline: time.Now().Add(time.Minute), Terminal: true,
		Changes:   []Change{{Field: "r:context", Delete: true}},
		Selectors: []SelectorChange{{Selector: selector, Record: "r:procedure", Delete: true}},
		Timers:    &TimerMutation{Expected: a, Target: a, RequireLease: true, Changes: []TimerChange{{Member: "retire", Delete: true}}}}
	if _, err = s.Commit(ctx, p); !errors.Is(err, ErrConflict) {
		t.Fatal("terminal publication forgot an unlisted record", err)
	}
	after, err := client.HGetAll(ctx, s.contextKey(id.Context)).Result()
	if err != nil || !maps.Equal(before, after) {
		t.Fatal("refused terminal publication partially removed state", err)
	}
	p.Changes = append(p.Changes, Change{Field: "r:procedure", Delete: true})
	if revision, err := s.Commit(ctx, p); err != nil || revision != 2 {
		t.Fatal("atomic terminal publication failed", revision, err)
	}
	if count, err := client.Exists(ctx, s.contextKey(id.Context), s.contextDeadlineKey(id.Context), s.workerTimerKey(a.Worker), s.selectorKey()).Result(); err != nil || count != 0 {
		t.Fatal("terminal publication left an unindexed context or index", count, err)
	}
	checkNamespace(t, s, client, id)
	if _, err = s.Read(ctx, id); !errors.Is(err, ErrAbsent) {
		t.Fatal("retired context claimed an available historical result", err)
	}
}

func TestTerminalPublicationCannotEraseUnsettledPhysicalProof(t *testing.T) {
	s, client, id, output, data := outputFixture(t)
	ctx := context.Background()
	publishOutput(t, s, id, output, data)
	p := Publication{Identity: id, Revision: 1, Deadline: time.Now().Add(time.Minute), Terminal: true,
		Changes: []Change{{Field: output.MessageField, Delete: true}, {Field: output.MetadataField, Delete: true}}}
	if _, err := s.Commit(ctx, p); !errors.Is(err, ErrConflict) {
		t.Fatal("terminal publication erased a physical attempt", err)
	}
	ref := outputRef(id, output)
	if _, err := s.ReserveOutput(ctx, ref); err != nil {
		t.Fatal(err)
	}
	if err := s.CompleteOutput(ctx, ref, "WRITTEN"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Commit(ctx, p); !errors.Is(err, ErrConflict) {
		t.Fatal("terminal publication erased an unacknowledged proof", err)
	}
	if err := s.SettleOutput(ctx, ref, true, true); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Commit(ctx, p); err != nil {
		t.Fatal("native terminal proof could not retire settled context", err)
	}
	checkNamespace(t, s, client, id)
}
