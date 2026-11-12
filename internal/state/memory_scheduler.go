package state

import (
	"context"
	"strconv"
	"time"
)

func (m *MemoryStore) schedulerCurrent(token string, now int64) bool {
	return m.schedulerHolder == token && m.schedulerUntil > now
}

func (m *MemoryStore) LeaseScheduler(ctx context.Context, token string, acquire bool) (time.Time, error) {
	if !workerPattern.MatchString(token) {
		return time.Time{}, ErrConflict
	}
	start := time.Now()
	now, err := m.lock(ctx)
	if err != nil {
		return time.Time{}, err
	}
	defer m.unlock()
	if acquire {
		if m.schedulerHolder != token && m.schedulerUntil > now {
			return time.Time{}, ErrBusy
		}
	} else if !m.schedulerCurrent(token, now) {
		return time.Time{}, ErrStale
	}
	m.schedulerHolder, m.schedulerUntil = token, now+SchedulerLeaseLifetime.Milliseconds()
	return start.Add(SchedulerLeaseLifetime), nil
}

func (m *MemoryStore) ReleaseScheduler(ctx context.Context, token string) error {
	if !workerPattern.MatchString(token) {
		return ErrConflict
	}
	_, err := m.lock(ctx)
	if err != nil {
		return err
	}
	defer m.unlock()
	if m.schedulerHolder == token {
		m.schedulerHolder, m.schedulerUntil = "", 0
	}
	return nil
}

func (m *MemoryStore) scheduleDeadline() int64 {
	due, _ := m.schedulePartition()
	return due
}

func (m *MemoryStore) schedulePartition() (int64, uint16) {
	var due int64
	var partition uint16
	for n := range m.schedules {
		for _, index := range []*hintHeap{&m.schedules[n].ready, &m.schedules[n].due} {
			if index.Len() > 0 && (due == 0 || index.items[0].due < due) {
				due = index.items[0].due
				partition = uint16(n)
			}
		}
	}
	return due, partition
}

func (m *MemoryStore) ScheduleHead(ctx context.Context, token string, _ bool) (ScheduleHead, error) {
	if !workerPattern.MatchString(token) {
		return ScheduleHead{}, ErrConflict
	}
	now, err := m.lock(ctx)
	if err != nil {
		return ScheduleHead{}, err
	}
	defer m.unlock()
	if !m.schedulerCurrent(token, now) {
		return ScheduleHead{}, ErrStale
	}
	due, partition := m.schedulePartition()
	applicationExpiry := false
	if m.applicationExpiry.Len() > 0 && (due == 0 || m.applicationExpiry.items[0].due < due) {
		due = m.applicationExpiry.items[0].due
		applicationExpiry = true
	}
	return ScheduleHead{DueMS: due, After: scheduleDelay(now, due), Cursor: strconv.FormatUint(m.schedulerCursor, 10) + "-0", Partition: partition, ApplicationExpiry: applicationExpiry}, nil
}

func (m *MemoryStore) WaitSchedule(ctx context.Context, cursor string) (string, error) {
	if !validScheduleCursor(cursor) {
		return "", ErrConflict
	}
	_, err := m.lock(ctx)
	if err != nil {
		return "", err
	}
	wake := m.schedulerWake
	current := strconv.FormatUint(m.schedulerCursor, 10) + "-0"
	m.unlock()
	if current != cursor {
		return current, nil
	}
	select {
	case <-ctx.Done():
		return "", ctx.Err()
	case <-wake:
		_, err := m.lock(ctx)
		if err != nil {
			return "", err
		}
		current = strconv.FormatUint(m.schedulerCursor, 10) + "-0"
		m.unlock()
		return current, nil
	}
}

func (m *MemoryStore) RepairScheduler(ctx context.Context, token string, _ uint64, count int) (uint64, int, error) {
	if !workerPattern.MatchString(token) || count < 1 || count > 256 {
		return 0, 0, ErrConflict
	}
	now, err := m.lock(ctx)
	if err != nil {
		return 0, 0, err
	}
	defer m.unlock()
	if !m.schedulerCurrent(token, now) {
		return 0, 0, ErrStale
	}
	return 0, 0, nil
}
