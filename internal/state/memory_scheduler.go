package state

import (
	"context"
	"strconv"
	"time"
)

func (m *MemoryStore) schedulerCurrent(token string, now int64) bool {
	return m.schedulerHolder == token && m.schedulerUntil > now
}

func (m *MemoryStore) wakeSchedulerLease() {
	m.schedulerLeaseCursor++
	close(m.schedulerLeaseWake)
	m.schedulerLeaseWake = make(chan struct{})
}
func (m *MemoryStore) LeaseScheduler(ctx context.Context, token string, acquire bool) (SchedulerLease, error) {
	if !workerPattern.MatchString(token) {
		return SchedulerLease{}, ErrConflict
	}
	start := time.Now()
	now, err := m.lock(ctx)
	if err != nil {
		return SchedulerLease{}, err
	}
	defer m.unlock()
	held := true
	if acquire {
		if m.schedulerHolder != token && m.schedulerUntil > now {
			held = false
		} else if !m.schedulerCurrent(token, now) {
			m.wakeSchedulerLease()
		}
	} else if !m.schedulerCurrent(token, now) {
		return SchedulerLease{}, ErrStale
	}
	if held {
		m.schedulerHolder, m.schedulerUntil = token, now+SchedulerLeaseLifetime.Milliseconds()
	}
	return SchedulerLease{Held: held, Until: start.Add(scheduleDelay(now, m.schedulerUntil)), Cursor: strconv.FormatUint(m.schedulerLeaseCursor, 10) + "-0"}, nil
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
		m.wakeSchedulerLease()
	}
	return nil
}
func (m *MemoryStore) WaitSchedulerLease(ctx context.Context, cursor string) (string, error) {
	if !validScheduleCursor(cursor) {
		return "", ErrConflict
	}
	_, err := m.lock(ctx)
	if err != nil {
		return "", err
	}
	wake := m.schedulerLeaseWake
	current := strconv.FormatUint(m.schedulerLeaseCursor, 10) + "-0"
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
		current = strconv.FormatUint(m.schedulerLeaseCursor, 10) + "-0"
		m.unlock()
		return current, nil
	}
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

func (m *MemoryStore) ScheduleHead(ctx context.Context, token string, _ bool, prefer ScheduleClass) (ScheduleHead, error) {
	if !workerPattern.MatchString(token) || prefer >= ScheduleClassCount {
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
	protocol, partition := m.schedulePartition()
	due := [3]int64{protocol, 0, 0}
	if m.applicationExpiry.Len() > 0 {
		due[1] = m.applicationExpiry.items[0].due
	}
	for _, v := range m.timerWorkers {
		if v > 0 && (due[2] == 0 || v < due[2]) {
			due[2] = v
		}
	}
	selected := ScheduleClassCount
	for i := ScheduleClass(0); i < ScheduleClassCount; i++ {
		n := (prefer + i) % ScheduleClassCount
		if due[n] > 0 && due[n] <= now {
			selected = n
			break
		}
	}
	if selected == ScheduleClassCount {
		for n := ScheduleClass(0); n < ScheduleClassCount; n++ {
			if due[n] > 0 && (selected == ScheduleClassCount || due[n] < due[selected]) {
				selected = n
			}
		}
	}
	if selected == ScheduleClassCount {
		selected = ScheduleProtocol
	}
	return ScheduleHead{DueMS: due[selected], After: scheduleDelay(now, due[selected]), Cursor: strconv.FormatUint(m.schedulerCursor, 10) + "-0", Partition: partition, Class: selected}, nil
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
