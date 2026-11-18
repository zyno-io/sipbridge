package rediscore

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

type selectorCommandCount struct{ count atomic.Int32 }

func (h *selectorCommandCount) DialHook(next redis.DialHook) redis.DialHook { return next }
func (h *selectorCommandCount) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}
func (h *selectorCommandCount) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		h.count.Add(1)
		return next(ctx, cmd)
	}
}

func TestSelectorLookupOneAtomicRPC(t *testing.T) {
	s, client, id := fixture(t)
	ctx := context.Background()
	selector := Selector{Kind: SelectorDialog, Digest: [32]byte{0xff, 1}}
	_, err := s.Commit(ctx, Publication{Identity: id, Create: true, Deadline: time.Now().Add(time.Minute),
		Changes:   []Change{{Field: "r:context", Value: []byte("context")}, {Field: "r:dialog", Value: []byte("dialog")}},
		Selectors: []SelectorChange{{Selector: selector, Record: "r:dialog"}}})
	if err != nil {
		t.Fatal(err)
	}
	// Exclude the ordinary one-time script load from steady-state accounting.
	if err = selectorLookupScript.Load(ctx, client).Err(); err != nil {
		t.Fatal(err)
	}
	hook := new(selectorCommandCount)
	client.AddHook(hook)
	got, err := s.LookupSelector(ctx, selector, "r:context")
	if err != nil || got == nil || got.Record != "r:dialog" || string(got.View.Records["r:dialog"]) != "dialog" || string(got.View.Records["r:context"]) != "context" {
		t.Fatal("native selector lookup failed", err)
	}
	if n := hook.count.Load(); n != 1 {
		t.Fatalf("native selector lookup used %d Redis commands, want one", n)
	}
}

func TestSelectorLookupRevalidatesAndReadsOnlySelected(t *testing.T) {
	s, c, id := fixture(t)
	ctx := context.Background()
	t.Cleanup(func() {
		if err := c.Del(ctx, s.selectorKey()).Err(); err != nil {
			t.Error(err)
		}
	})
	publish(t, s, id, Change{Field: "r:procedure", Value: []byte("selected")}, Change{Field: "r:other", Value: []byte("other")})
	selector := Selector{Kind: SelectorServerTransaction, Digest: [32]byte{0, 1, 2, 255}}
	target := SelectorTarget{Context: id.Context, Incarnation: id.Incarnation, Record: "r:procedure"}
	if err := c.HSet(ctx, s.selectorKey(), selector.field(), target.value()).Err(); err != nil {
		t.Fatal(err)
	}
	got, err := s.LookupSelector(ctx, selector, "r:procedure", "r:missing")
	if err != nil {
		t.Fatal(err)
	}
	if got.Identity != id || got.Record != target.Record || got.View.Revision != 1 || !bytes.Equal(got.View.Records["r:procedure"], []byte("selected")) || got.View.Records["r:missing"] != nil || len(got.View.Records) != 2 {
		t.Fatal("wrong selected result", got)
	}
	auto, err := s.LookupSelector(ctx, selector, "r:missing")
	if err != nil || auto == nil || len(auto.View.Records) != 2 || !bytes.Equal(auto.View.Records[target.Record], []byte("selected")) || auto.View.Records["r:other"] != nil {
		t.Fatal("selector target required another read or loaded unrelated state", err)
	}
	if _, err = s.LookupSelector(ctx, selector, "r:procedure", "r:procedure"); !errors.Is(err, ErrConflict) {
		t.Fatal("duplicate field accepted", err)
	}
	if err = c.HSet(ctx, s.selectorKey(), selector.field(), SelectorTarget{Context: id.Context, Incarnation: "replacement", Record: target.Record}.value()).Err(); err != nil {
		t.Fatal(err)
	}
	if _, err = s.LookupSelector(ctx, selector); !errors.Is(err, ErrConflict) {
		t.Fatal("mapping to a different incarnation was followed", err)
	}
	target.Incarnation = "other"
	if err = c.HSet(ctx, s.selectorKey(), selector.field(), target.value()).Err(); err != nil {
		t.Fatal(err)
	}
	if _, err = s.LookupSelector(ctx, selector, "r:procedure"); !errors.Is(err, ErrConflict) {
		t.Fatal("stale incarnation selected", err)
	}
	if err = c.HSet(ctx, s.selectorKey(), selector.field(), "invalid").Err(); err != nil {
		t.Fatal(err)
	}
	if _, err = s.LookupSelector(ctx, selector); !errors.Is(err, ErrCorrupt) {
		t.Fatal("malformed selector accepted", err)
	}
}

func TestSelectorPublicationIsConditionalAndAccounted(t *testing.T) {
	s, c, id := fixture(t)
	ctx := context.Background()
	t.Cleanup(func() {
		if err := c.Del(ctx, s.selectorKey()).Err(); err != nil {
			t.Error(err)
		}
	})
	a := Selector{Kind: SelectorServerTransaction, Digest: [32]byte{1}}
	b := Selector{Kind: SelectorClientTransaction, Digest: [32]byte{2}}
	d := Selector{Kind: SelectorDialog, Digest: [32]byte{3}}
	p := Publication{Identity: id, Create: true, Deadline: time.Now().Add(time.Minute), Changes: []Change{{Field: "r:procedure", Value: []byte("tx")}, {Field: "r:dialog", Value: []byte("dialog")}}, Selectors: []SelectorChange{{Selector: a, Record: "r:procedure"}, {Selector: b, Record: "r:procedure"}, {Selector: d, Record: "r:dialog"}}}
	if _, err := s.Commit(ctx, p); err != nil {
		t.Fatal(err)
	}
	checkNamespace(t, s, c, id)
	for _, v := range []struct {
		selector Selector
		field    string
	}{{a, "r:procedure"}, {b, "r:procedure"}, {d, "r:dialog"}} {
		got, err := s.LookupSelector(ctx, v.selector, v.field)
		if err != nil || got.Record != v.field || got.View.Revision != 1 {
			t.Fatal("missing selector", err)
		}
	}
	for _, changes := range [][]SelectorChange{nil, {{Selector: a, Record: "r:procedure", Delete: true}}} {
		_, err := s.Commit(ctx, Publication{Identity: id, Revision: 1, Deadline: time.Now().Add(time.Minute), Changes: []Change{{Field: "r:procedure", Delete: true}}, Selectors: changes})
		if !errors.Is(err, ErrConflict) {
			t.Fatal("orphaned selector", err)
		}
		checkNamespace(t, s, c, id)
	}
	other := Identity{Context: "other", Incarnation: "other-incarnation"}
	t.Cleanup(func() {
		if err := c.Del(ctx, s.contextKey(other.Context)).Err(); err != nil {
			t.Error(err)
		}
	})
	_, err := s.Commit(ctx, Publication{Identity: other, Create: true, Deadline: time.Now().Add(time.Minute), Changes: []Change{{Field: "r:procedure", Value: []byte("wrong")}}, Selectors: []SelectorChange{{Selector: a, Record: "r:procedure"}}})
	if !errors.Is(err, ErrConflict) {
		t.Fatal("selector stolen", err)
	}
	if n, _ := c.Exists(ctx, s.contextKey(other.Context)).Result(); n != 0 {
		t.Fatal("collision published context")
	}
	checkNamespace(t, s, c, id)
	_, err = s.Commit(ctx, Publication{Identity: id, Revision: 1, Deadline: time.Now().Add(time.Minute), Changes: []Change{{Field: "r:procedure", Delete: true}}, Selectors: []SelectorChange{{Selector: a, Record: "r:procedure", Delete: true}, {Selector: b, Record: "r:procedure", Delete: true}}})
	if err != nil {
		t.Fatal(err)
	}
	checkNamespace(t, s, c, id)
	if _, err = s.LookupSelector(ctx, a); !errors.Is(err, ErrAbsent) {
		t.Fatal("deleted selector remained", err)
	}
	_, err = s.Commit(ctx, Publication{Identity: id, Revision: 2, Deadline: time.Now().Add(time.Minute), Changes: []Change{{Field: "r:dialog", Delete: true}}, Selectors: []SelectorChange{{Selector: d, Record: "r:dialog", Delete: true}}})
	if err != nil {
		t.Fatal(err)
	}
	checkNamespace(t, s, c, id)
	if err = s.RetireContext(ctx, id, 3, time.Now().Add(time.Minute), true); err != nil {
		t.Fatal("empty context not retired", err)
	}
	checkNamespace(t, s, c, id)
}

func TestSelectorTwoContextRaceChoosesOne(t *testing.T) {
	s, c, id := fixture(t)
	ctx := context.Background()
	other := Identity{Context: "other", Incarnation: "other-incarnation"}
	t.Cleanup(func() {
		if err := c.Del(ctx, s.selectorKey(), s.contextKey(other.Context)).Err(); err != nil {
			t.Error(err)
		}
	})
	peer, err := New(c, Config{Root: s.root, Profile: s.profile, RecordBudget: s.maxBytes, PrimaryRunID: s.primary})
	if err != nil {
		t.Fatal(err)
	}
	selector := Selector{Kind: SelectorDialog, Digest: [32]byte{7}}
	var wg sync.WaitGroup
	var mu sync.Mutex
	accepted, conflicts := 0, 0
	for i, x := range []Identity{id, other} {
		wg.Add(1)
		go func(n int, who Identity) {
			defer wg.Done()
			node := s
			if n == 1 {
				node = peer
			}
			_, e := node.Commit(ctx, Publication{Identity: who, Create: true, Deadline: time.Now().Add(time.Minute), Changes: []Change{{Field: "r:dialog", Value: []byte("d")}}, Selectors: []SelectorChange{{Selector: selector, Record: "r:dialog"}}})
			mu.Lock()
			defer mu.Unlock()
			if e == nil {
				accepted++
			} else if errors.Is(e, ErrConflict) {
				conflicts++
			} else {
				t.Errorf("commit: %v", e)
			}
		}(i, x)
	}
	wg.Wait()
	if accepted != 1 || conflicts != 1 {
		t.Fatalf("accepted %d conflicts %d", accepted, conflicts)
	}
	got, err := s.LookupSelector(ctx, selector, "r:dialog")
	if err != nil || got.View.Revision != 1 {
		t.Fatal("winner absent", err)
	}
	checkNamespace(t, s, c, id, other)
}
