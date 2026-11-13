package engine

import (
	"container/heap"
	"time"

	"github.com/emiago/sipgo/sip/portable"
)

// residentDeadline identifies one actual protocol or retirement deadline. The
// generation belongs to the record and fences work already removed from the heap.
type residentDeadlineKind uint8

const (
	residentControllerDeadline residentDeadlineKind = iota + 1
	residentOwnerCleanupDeadline
	residentAdmissionDeadline
	residentTransactionDeadline
	residentTransactionRetirementDeadline
	residentDialogDeadline
	residentOutputDeadline
	residentEventDeadline
	residentSubscriptionDeadline
	residentContextRetirementDeadline
)

// A native key is opaque data, never a delimited string to decode. These fields
// reuse resident identity strings; scheduling allocates no encoded key payload.
type residentDeadlineKey struct {
	kind      residentDeadlineKind
	contextID string
	record    string
	timer     portable.Timer
}

type residentDeadline struct {
	key        residentDeadlineKey
	generation uint64
	at         time.Time
	index      int
}

type residentDeadlineHeap []*residentDeadline

func (h residentDeadlineHeap) Len() int { return len(h) }
func (h residentDeadlineHeap) Less(i, j int) bool {
	if h[i].at.Equal(h[j].at) {
		a, b := h[i].key, h[j].key
		if a.kind != b.kind {
			return a.kind < b.kind
		}
		if a.contextID != b.contextID {
			return a.contextID < b.contextID
		}
		if a.record != b.record {
			return a.record < b.record
		}
		return a.timer < b.timer
	}
	return h[i].at.Before(h[j].at)
}
func (h residentDeadlineHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index, h[j].index = i, j
}
func (h *residentDeadlineHeap) Push(value any) {
	item := value.(*residentDeadline)
	item.index = len(*h)
	*h = append(*h, item)
}
func (h *residentDeadlineHeap) Pop() any {
	old := *h
	item := old[len(old)-1]
	old[len(old)-1] = nil
	item.index = -1
	*h = old[:len(old)-1]
	return item
}

// residentDeadlines is guarded by its owner's mutex. Updating an existing key
// changes one heap entry; repeated updates cannot retain stale heap entries.
type residentDeadlines struct {
	heap  residentDeadlineHeap
	byKey map[residentDeadlineKey]*residentDeadline
}

func (d *residentDeadlines) set(key residentDeadlineKey, generation uint64, at time.Time) {
	if d.byKey == nil {
		d.byKey = make(map[residentDeadlineKey]*residentDeadline)
	}
	if item := d.byKey[key]; item != nil {
		item.generation, item.at = generation, at
		heap.Fix(&d.heap, item.index)
		return
	}
	item := &residentDeadline{key: key, generation: generation, at: at}
	d.byKey[key] = item
	heap.Push(&d.heap, item)
}

func (d *residentDeadlines) remove(key residentDeadlineKey) {
	item := d.byKey[key]
	if item == nil {
		return
	}
	heap.Remove(&d.heap, item.index)
	delete(d.byKey, key)
}

func (d *residentDeadlines) next() (residentDeadline, bool) {
	if len(d.heap) == 0 {
		return residentDeadline{}, false
	}
	return *d.heap[0], true
}

func (d *residentDeadlines) popDue(now time.Time) (residentDeadline, bool) {
	if len(d.heap) == 0 || d.heap[0].at.After(now) {
		return residentDeadline{}, false
	}
	item := heap.Pop(&d.heap).(*residentDeadline)
	delete(d.byKey, item.key)
	return *item, true
}
