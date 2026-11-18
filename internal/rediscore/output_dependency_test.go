package rediscore

import (
	"context"
	"errors"
	"maps"
	"sync"
	"testing"
	"time"
)

func TestOutputMutableDependenciesPinDeletionUntilSettlement(t *testing.T) {
	s, c, id, out, data := outputFixture(t)
	ctx := context.Background()
	out.Dependencies = []string{"r:procedure", "r:result", "r:dialog"}
	changes := []Change{{Field: out.MessageField, Value: data}, {Field: out.MetadataField, Value: []byte("metadata")}, {Field: "r:procedure", Value: []byte("old")}, {Field: "r:result", Value: []byte("pending")}, {Field: "r:dialog", Value: []byte("dialog")}}
	if _, err := s.Commit(ctx, Publication{Identity: id, Create: true, Deadline: time.Now().Add(time.Minute), Changes: changes, NewOutputs: []OutputIntent{out}}); err != nil {
		t.Fatal(err)
	}
	checkNamespace(t, s, c, id)
	if _, err := s.Commit(ctx, Publication{Identity: id, Revision: 1, Deadline: time.Now().Add(time.Minute), Changes: []Change{{Field: "r:procedure", Value: []byte("updated")}}}); err != nil {
		t.Fatal("mutable dependency update fenced", err)
	}
	checkNamespace(t, s, c, id)
	for _, field := range out.Dependencies {
		if _, err := s.Commit(ctx, Publication{Identity: id, Revision: 2, Deadline: time.Now().Add(time.Minute), Changes: []Change{{Field: field, Delete: true}}}); !errors.Is(err, ErrConflict) {
			t.Fatal("dependent record deleted", field, err)
		}
	}
	ref := outputRef(id, out)
	if err := s.CompleteOutput(ctx, ref, "NOT_WRITTEN"); err != nil {
		t.Fatal(err)
	}
	if err := s.SettleOutput(ctx, ref, true, false); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Commit(ctx, Publication{Identity: id, Revision: 2, Deadline: time.Now().Add(time.Minute), Changes: []Change{{Field: "r:procedure", Delete: true}}}); !errors.Is(err, ErrConflict) {
		t.Fatal("partial settlement released dependency", err)
	}
	if err := s.SettleOutput(ctx, ref, false, true); err != nil {
		t.Fatal(err)
	}
	checkNamespace(t, s, c, id)
	if _, err := s.Commit(ctx, Publication{Identity: id, Revision: 2, Deadline: time.Now().Add(time.Minute), Changes: []Change{{Field: "r:procedure", Delete: true}, {Field: "r:result", Delete: true}, {Field: "r:dialog", Delete: true}}}); err != nil {
		t.Fatal("final settlement kept dependencies", err)
	}
	checkNamespace(t, s, c, id)
}
func TestOutputDependencyCapacityRefusalIsAtomic(t *testing.T) {
	s, c, id, out, data := outputFixture(t)
	ctx := context.Background()
	out.Dependencies = []string{"r:procedure"}
	publish(t, s, id, Change{Field: "r:procedure", Value: []byte("old")}, Change{Field: out.MessageField, Value: data}, Change{Field: out.MetadataField, Value: []byte("metadata")})
	before, err := c.HGetAll(ctx, s.contextKey(id.Context)).Result()
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.Commit(ctx, Publication{Identity: id, Revision: 1, Deadline: time.Now().Add(time.Minute), Reserved: s.maxBytes, Changes: []Change{{Field: "r:procedure", Value: []byte("new")}}, NewOutputs: []OutputIntent{out}})
	if !errors.Is(err, ErrCapacity) {
		t.Fatal("dependency capacity accepted", err)
	}
	after, err := c.HGetAll(ctx, s.contextKey(id.Context)).Result()
	if err != nil || !maps.Equal(before, after) {
		t.Fatal("capacity refusal mutated context", err)
	}
	checkNamespace(t, s, c, id)
}
func TestOutputSharedDependencyConcurrentSettlement(t *testing.T) {
	s, c, id, out, data := outputFixture(t)
	ctx := context.Background()
	out.Dependencies = []string{"r:procedure"}
	other := out
	other.ID = "other"
	other.Attempt = "other-attempt"
	p := Publication{Identity: id, Create: true, Deadline: time.Now().Add(time.Minute), Changes: []Change{{Field: out.MessageField, Value: data}, {Field: out.MetadataField, Value: []byte("metadata")}, {Field: "r:procedure", Value: []byte("old")}}, NewOutputs: []OutputIntent{out, other}}
	if _, err := s.Commit(ctx, p); err != nil {
		t.Fatal(err)
	}
	for _, v := range []OutputIntent{out, other} {
		if err := s.CompleteOutput(ctx, outputRef(id, v), "NOT_WRITTEN"); err != nil {
			t.Fatal(err)
		}
	}
	var wg sync.WaitGroup
	for _, v := range []OutputIntent{out, other} {
		wg.Add(1)
		go func(item OutputIntent) {
			defer wg.Done()
			if err := s.SettleOutput(ctx, outputRef(id, item), true, true); err != nil {
				t.Error(err)
			}
		}(v)
	}
	wg.Wait()
	if n, err := c.HExists(ctx, s.contextKey(id.Context), "dref:r:procedure").Result(); err != nil || n {
		t.Fatal("dependent count leaked", n, err)
	}
	checkNamespace(t, s, c, id)
	if _, err := s.Commit(ctx, Publication{Identity: id, Revision: 1, Deadline: time.Now().Add(time.Minute), Changes: []Change{{Field: "r:procedure", Delete: true}}}); err != nil {
		t.Fatal("dependent remained pinned", err)
	}
}
