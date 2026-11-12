package state

import (
	"context"
	"sort"
	"strconv"
)

type memoryWorkerWake struct {
	cursor  uint64
	changed chan struct{}
}

func (m *MemoryStore) forgetDispatchWake(worker string) {
	if m.dispatchQueues[worker] == nil || m.dispatchQueues[worker].Len() == 0 {
		if wake := m.dispatchWakes[worker]; wake != nil {
			close(wake.changed)
			delete(m.dispatchWakes, worker)
		}
	}
}

func (m *MemoryStore) dispatchWake(worker string) *memoryWorkerWake {
	if m.dispatchWakes == nil {
		m.dispatchWakes = make(map[string]*memoryWorkerWake)
	}
	wake := m.dispatchWakes[worker]
	if wake == nil {
		wake = &memoryWorkerWake{changed: make(chan struct{})}
		m.dispatchWakes[worker] = wake
	}
	return wake
}

func (m *MemoryStore) wakeDispatch(worker string) {
	wake := m.dispatchWake(worker)
	wake.cursor++
	close(wake.changed)
	wake.changed = make(chan struct{})
}

func (m *MemoryStore) WaitDispatch(ctx context.Context, worker, cursor string) (string, error) {
	if !dispatchWorkerPattern.MatchString(worker) || !validScheduleCursor(cursor) {
		return "", ErrConflict
	}
	if _, err := m.lock(ctx); err != nil {
		return "", err
	}
	wake := m.dispatchWake(worker)
	current := strconv.FormatUint(wake.cursor, 10) + "-0"
	changed := wake.changed
	m.unlock()
	if current != cursor {
		return current, nil
	}
	select {
	case <-ctx.Done():
		return "", ctx.Err()
	case <-changed:
		if _, err := m.lock(ctx); err != nil {
			return "", err
		}
		current = strconv.FormatUint(m.dispatchWake(worker).cursor, 10) + "-0"
		m.unlock()
		return current, nil
	}
}

func (m *MemoryStore) workerHints(worker string, now int64, limit int) []Hint {
	byID := make(map[string]Hint, 2*limit)
	for _, queue := range []*hintHeap{m.dispatchQueues[worker], m.timerQueues[worker]} {
		if queue == nil {
			continue
		}
		for _, hint := range queue.hints(now, limit) {
			prior, exists := byID[hint.Ref.ID]
			if !exists || hint.DueMS < prior.DueMS {
				byID[hint.Ref.ID] = hint
			}
		}
	}
	result := make([]Hint, 0, len(byID))
	for _, hint := range byID {
		result = append(result, hint)
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].DueMS < result[j].DueMS || result[i].DueMS == result[j].DueMS && result[i].Ref.ID < result[j].Ref.ID
	})
	return result[:min(limit, len(result))]
}

func (m *MemoryStore) scheduleOwnedTimer(g *memoryGroup) {
	if g.due > 0 {
		if m.timerWorkers == nil {
			m.timerWorkers = make(map[string]int64)
		}
		if m.timerWorkers[g.timerWorker] == 0 {
			m.timerWorkers[g.timerWorker] = m.lastClock.UnixMilli() + DispatchWorkerLifetime.Milliseconds()
		}
	}
	if m.timerQueues == nil {
		m.timerQueues = make(map[string]*hintHeap)
	}
	queue := m.timerQueues[g.timerWorker]
	if queue == nil {
		if g.due == 0 {
			return
		}
		queue = &hintHeap{}
		m.timerQueues[g.timerWorker] = queue
	}
	var previous int64
	if queue.Len() > 0 {
		previous = queue.items[0].due
	}
	queue.set(g.ref, g.due)
	if queue.Len() == 0 {
		delete(m.timerQueues, g.timerWorker)
	} else if previous == 0 || queue.items[0].due < previous {
		m.wakeDispatch(g.timerWorker)
	}
}

func (m *MemoryStore) TakeoverTimers(ctx context.Context, token, worker string, limit int) (bool, error) {
	if !workerPattern.MatchString(token) || !dispatchWorkerPattern.MatchString(worker) || limit < 1 || limit > 128 {
		return false, ErrConflict
	}
	now, err := m.lock(ctx)
	if err != nil {
		return false, err
	}
	defer m.unlock()
	if !m.schedulerCurrent(token, now) || m.timerWorkers[worker] <= now {
		return false, ErrStale
	}
	var expired []string
	for id, until := range m.timerWorkers {
		if until <= now {
			expired = append(expired, id)
		}
	}
	sort.Strings(expired)
	if len(expired) == 0 {
		return false, nil
	}
	old := expired[0]
	queue := m.timerQueues[old]
	for n := 0; queue != nil && queue.Len() > 0 && n < limit; n++ {
		ref := queue.items[0].ref
		queue.set(ref, 0)
		group := m.groups[ref.ID]
		if group != nil && group.timerWorker == old {
			group.timerWorker = worker
			m.scheduleOwnedTimer(group)
		}
	}
	if queue == nil || queue.Len() == 0 {
		delete(m.timerQueues, old)
		delete(m.timerWorkers, old)
		m.forgetDispatchWake(old)
	}
	return len(expired) > 1 || queue != nil && queue.Len() > 0, nil
}
