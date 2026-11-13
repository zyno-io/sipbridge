package engine

import (
	"testing"
	"time"
)

func TestResidentDeadlinesReplaceRemoveAndOrder(t *testing.T) {
	var d residentDeadlines
	now := time.Unix(1000, 0)
	a, b := residentDeadlineKey{record: "a"}, residentDeadlineKey{record: "b"}
	d.set(a, 1, now.Add(time.Second))
	d.set(b, 1, now.Add(2*time.Second))
	for n := uint64(2); n < 100; n++ {
		d.set(b, n, now.Add(time.Duration(n)*time.Second))
	}
	if len(d.heap) != 2 || len(d.byKey) != 2 {
		t.Fatalf("updates retained stale entries: heap=%d index=%d", len(d.heap), len(d.byKey))
	}
	d.set(b, 100, now)
	if _, ok := d.popDue(now.Add(-time.Nanosecond)); ok {
		t.Fatal("popped before deadline")
	}
	item, ok := d.popDue(now)
	if !ok || item.key != b || item.generation != 100 {
		t.Fatalf("unexpected due deadline: %+v, %v", item, ok)
	}
	d.remove(a)
	d.remove(a)
	if _, ok := d.next(); ok || len(d.byKey) != 0 {
		t.Fatal("removed deadline still indexed")
	}
}
