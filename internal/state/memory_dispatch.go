package state

import (
	"context"
	"sort"
	"strconv"
)

func (m *MemoryStore) RegisterDispatchWorker(ctx context.Context, worker string) error {
	if !dispatchWorkerPattern.MatchString(worker) {
		return ErrConflict
	}
	now, err := m.lock(ctx)
	if err != nil {
		return err
	}
	defer m.unlock()
	if m.dispatchWorkers == nil {
		m.dispatchWorkers = make(map[string]int64)
	}
	for id, until := range m.dispatchWorkers {
		if until <= now {
			delete(m.dispatchWorkers, id)
		}
	}
	if len(m.dispatchWorkers) >= 256 && m.dispatchWorkers[worker] == 0 {
		return ErrCapacity
	}
	m.dispatchWorkers[worker] = now + 2000
	if m.timerWorkers == nil {
		m.timerWorkers = make(map[string]int64)
	}
	for id, until := range m.timerWorkers {
		if until <= now && (m.timerQueues[id] == nil || m.timerQueues[id].Len() == 0) {
			delete(m.timerWorkers, id)
			m.forgetDispatchWake(id)
		}
	}
	m.timerWorkers[worker] = now + DispatchWorkerLifetime.Milliseconds()
	return nil
}

func (m *MemoryStore) resolveDispatchWorkers(requested []string, now int64) []string {
	workers := make(map[string]bool)
	for _, worker := range requested {
		if worker != AnyDispatchWorker {
			workers[worker] = true
			continue
		}
		for id, until := range m.dispatchWorkers {
			if until > now {
				workers[id] = true
			}
		}
	}
	result := make([]string, 0, len(workers))
	for worker := range workers {
		result = append(result, worker)
	}
	sort.Strings(result)
	return result
}

func (m *MemoryStore) wakeSchedule() {
	m.schedulerCursor++
	close(m.schedulerWake)
	m.schedulerWake = make(chan struct{})
}

func (m *MemoryStore) scheduleDispatch(g *memoryGroup, workers []string, now int64) {
	for _, worker := range g.workers {
		if queue := m.dispatchQueues[worker]; queue != nil {
			queue.set(g.ref, 0)
			if queue.Len() == 0 {
				delete(m.dispatchQueues, worker)
			}
		}
	}
	g.workers = workers
	for _, worker := range workers {
		if m.dispatchQueues == nil {
			m.dispatchQueues = make(map[string]*hintHeap)
		}
		queue := m.dispatchQueues[worker]
		if queue == nil {
			queue = &hintHeap{}
			m.dispatchQueues[worker] = queue
		}
		var previous int64
		if queue.Len() > 0 {
			previous = queue.items[0].due
		}
		queue.set(g.ref, max(1, now))
		if previous == 0 || queue.items[0].due < previous {
			m.wakeDispatch(worker)
		}
	}
	if len(workers) > 0 && g.timerWorker == "" {
		m.wakeSchedule()
	}
}

func (m *MemoryStore) DispatchHead(ctx context.Context, worker string) (ScheduleHead, error) {
	if !dispatchWorkerPattern.MatchString(worker) {
		return ScheduleHead{}, ErrConflict
	}
	now, err := m.lock(ctx)
	if err != nil {
		return ScheduleHead{}, err
	}
	defer m.unlock()
	var due int64
	if queue := m.dispatchQueues[worker]; queue != nil && queue.Len() > 0 {
		due = queue.items[0].due
	}
	if queue := m.timerQueues[worker]; queue != nil && queue.Len() > 0 && (due == 0 || queue.items[0].due < due) {
		due = queue.items[0].due
	}
	wake := m.dispatchWake(worker)
	return ScheduleHead{DueMS: due, After: scheduleDelay(now, due), Cursor: strconv.FormatUint(wake.cursor, 10) + "-0"}, nil
}

func (m *MemoryStore) DispatchHints(ctx context.Context, worker string, limit int) ([]Hint, error) {
	if !dispatchWorkerPattern.MatchString(worker) || limit < 1 || limit > 256 {
		return nil, ErrConflict
	}
	now, err := m.lock(ctx)
	if err != nil {
		return nil, err
	}
	defer m.unlock()
	return m.workerHints(worker, now, limit), nil
}

func (m *MemoryStore) AckDispatch(ctx context.Context, worker string, ref Ref, revision string) error {
	if !dispatchWorkerPattern.MatchString(worker) || !decimal(revision) {
		return ErrConflict
	}
	if err := m.validateRef(ref); err != nil {
		return err
	}
	_, err := m.lock(ctx)
	if err != nil {
		return err
	}
	defer m.unlock()
	g := m.groups[ref.ID]
	if g != nil && ref.Incarnation != "" && g.ref.Incarnation != ref.Incarnation {
		return ErrStale
	}
	if g != nil && g.revision != revision {
		return nil
	}
	if queue := m.dispatchQueues[worker]; queue != nil {
		queue.set(ref, 0)
		if queue.Len() == 0 {
			delete(m.dispatchQueues, worker)
		}
	}
	return nil
}
