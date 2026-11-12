package state

import (
	"context"
	"sort"
	"strconv"

	"github.com/google/uuid"
)

type memoryApplicationExpiry struct {
	token, cursor string
	started       int64
}

func (m *MemoryStore) setApplicationExpiry(id string, due int64) {
	var previous int64
	if m.applicationExpiry.Len() > 0 {
		previous = m.applicationExpiry.items[0].due
	}
	m.applicationExpiry.set(Ref{ID: id}, due)
	if m.applicationExpiry.Len() > 0 && (previous == 0 || m.applicationExpiry.items[0].due < previous) {
		m.wakeSchedule()
	}
}

func (m *MemoryStore) forgetApplicationExpiry(id string) {
	m.setApplicationExpiry(id, 0)
}

func (m *MemoryStore) scheduleApplicationExpiry(lease *memoryLease, now int64, reset bool) {
	if reset {
		lease.expiry = &memoryApplicationExpiry{token: uuid.NewString(), started: now}
	}
	due := lease.ExpiresAtMS
	if lease.expiry != nil {
		due = now
	}
	m.setApplicationExpiry(lease.ID, due)
}

func (m *MemoryStore) retainApplicationExpiry(lease *memoryLease, now int64, firstReference bool) {
	if firstReference || lease.expiry != nil {
		m.scheduleApplicationExpiry(lease, now, false)
	}
}

func (m *MemoryStore) ApplicationExpiryHead(ctx context.Context) (ScheduleHead, error) {
	now, err := m.lock(ctx)
	if err != nil {
		return ScheduleHead{}, err
	}
	defer m.unlock()
	var due int64
	if m.applicationExpiry.Len() > 0 {
		due = m.applicationExpiry.items[0].due
	}
	return ScheduleHead{DueMS: due, After: scheduleDelay(now, due), Cursor: strconv.FormatUint(m.schedulerCursor, 10) + "-0"}, nil
}

func (m *MemoryStore) ApplicationExpiryPage(ctx context.Context, limit int) (ApplicationExpiryPage, error) {
	if limit < 1 || limit > ApplicationContextPageLimit {
		return ApplicationExpiryPage{}, ErrConflict
	}
	now, err := m.lock(ctx)
	if err != nil {
		return ApplicationExpiryPage{}, err
	}
	defer m.unlock()
	if m.applicationExpiry.Len() == 0 || m.applicationExpiry.items[0].due > now {
		return ApplicationExpiryPage{}, nil
	}
	id := m.applicationExpiry.items[0].ref.ID
	lease := m.liveLease(id, now)
	if lease == nil {
		m.forgetApplicationExpiry(id)
		return ApplicationExpiryPage{}, nil
	}
	if lease.expiry == nil {
		observed := m.observeLease(id, now)
		if !observed.Sealed {
			due := observed.RecoveryUntilMS
			if observed.ExpiresAtMS > now {
				due = observed.ExpiresAtMS
			}
			if due > now {
				m.setApplicationExpiry(id, due)
				return ApplicationExpiryPage{}, nil
			}
		}
		lease.expiry = &memoryApplicationExpiry{token: uuid.NewString(), started: now}
	}
	page := ApplicationExpiryPage{OwnerID: id, SweepToken: lease.expiry.token, Cursor: lease.expiry.cursor, NextCursor: lease.expiry.cursor, Complete: true}
	entries := m.applicationContexts[id]
	if len(entries) > ApplicationContextLimit {
		return ApplicationExpiryPage{}, ErrCorrupt
	}
	members := make([]string, 0, len(entries))
	for member := range entries {
		if member > page.Cursor {
			members = append(members, member)
		}
	}
	sort.Strings(members)
	if len(members) > limit {
		page.Complete = false
		members = members[:limit]
	}
	for _, member := range members {
		page.Entries = append(page.Entries, entries[member])
		page.NextCursor = member
	}
	m.setApplicationExpiry(id, now)
	return page, nil
}

func (m *MemoryStore) AckApplicationExpiryPage(ctx context.Context, page ApplicationExpiryPage) error {
	if !validApplicationLeaseRef(ApplicationLeaseRef{ID: page.OwnerID, Term: page.SweepToken}) || len(page.Cursor) > applicationContextMemberLimit || len(page.NextCursor) > applicationContextMemberLimit || page.NextCursor < page.Cursor || !page.Complete && page.NextCursor == page.Cursor {
		return ErrConflict
	}
	now, err := m.lock(ctx)
	if err != nil {
		return err
	}
	defer m.unlock()
	lease := m.liveLease(page.OwnerID, now)
	if lease == nil || lease.expiry == nil || lease.expiry.token != page.SweepToken || lease.expiry.cursor != page.Cursor {
		return ErrStale
	}
	lease.expiry.cursor = page.NextCursor
	due := now
	if page.Complete {
		started := lease.expiry.started
		lease.expiry = nil
		due = 0
		if !lease.sealed {
			if lease.ExpiresAtMS > now {
				due = lease.ExpiresAtMS
			} else if lease.RecoveryUntilMS > now {
				due = lease.RecoveryUntilMS
			} else if lease.ExpiresAtMS > started || lease.RecoveryUntilMS > started {
				due = now
			}
		}
	}
	m.setApplicationExpiry(page.OwnerID, due)
	return nil
}
