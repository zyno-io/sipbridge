package state

import (
	"context"
	"crypto/subtle"
)

func leaseHashEqual(a, b string) bool {
	return len(a) == 64 && len(b) == 64 && subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1
}
func (m *MemoryStore) AcquireRecoverableApplicationLease(ctx context.Context, id, term string, owner ApplicationLeaseOwner) (ApplicationLease, error) {
	timeout, err := normalizeReattachTimeout(owner.ReattachTimeoutMS)
	if err != nil {
		return ApplicationLease{}, err
	}
	return m.recoverableApplicationLease(ctx, id, term, "create", ApplicationLeaseAttach{ApplicationLeaseHolder: owner.ApplicationLeaseHolder, TokenHash: owner.TokenHash}, 0, timeout)
}
func (m *MemoryStore) RenewApplicationAttachment(ctx context.Context, ref ApplicationLeaseRef, holder ApplicationLeaseHolder) (ApplicationLease, error) {
	return m.recoverableApplicationLease(ctx, ref.ID, ref.Term, "renew", ApplicationLeaseAttach{ApplicationLeaseHolder: holder}, ref.Generation, 0)
}
func (m *MemoryStore) CompleteApplicationAttachment(ctx context.Context, ref ApplicationLeaseRef, holder ApplicationLeaseHolder) (ApplicationLease, error) {
	return m.recoverableApplicationLease(ctx, ref.ID, ref.Term, "complete", ApplicationLeaseAttach{ApplicationLeaseHolder: holder}, ref.Generation, 0)
}
func (m *MemoryStore) AttachApplicationLease(ctx context.Context, id, term string, generation uint64, a ApplicationLeaseAttach) (ApplicationLease, error) {
	return m.recoverableApplicationLease(ctx, id, term, "attach", a, generation, 0)
}
func (m *MemoryStore) recoverableApplicationLease(ctx context.Context, id, term, mode string, a ApplicationLeaseAttach, generation uint64, timeout int64) (ApplicationLease, error) {
	if err := validateLeaseIdentity(id, term, false); err != nil {
		return ApplicationLease{}, err
	}
	if !validLeaseHolder(a.ApplicationLeaseHolder) || generation > 1<<53-1 || mode != "create" && generation == 0 || (mode == "create" || mode == "attach") && !validLeaseHash(a.TokenHash) || mode == "attach" && (!validLeaseHash(a.NextTokenHash) || !ValidReference(a.AttemptID)) {
		return ApplicationLease{}, ErrConflict
	}
	now, err := m.lock(ctx)
	if err != nil {
		return ApplicationLease{}, err
	}
	defer m.unlock()
	lease := m.liveLease(id, now)
	completing := lease != nil && mode == "complete" && lease.AttachmentUntilMS > 0
	if mode == "create" {
		if lease != nil {
			return ApplicationLease{}, ErrConflict
		}
		// Incremental fair pruning, without pruning a recoverable orphan early.
		for n, count := 0, min(applicationLeasePrune, m.leaseOrder.Len()); n < count; n++ {
			element := m.leaseOrder.Front()
			candidate := element.Value.(string)
			m.leaseOrder.MoveToBack(element)
			saved := m.liveLease(candidate, now)
			if saved != nil && saved.references == 0 && saved.ExpiresAtMS <= now && (!saved.Recoverable || saved.ExpiresAtMS+saved.ReattachTimeoutMS <= now) {
				m.forgetApplicationExpiry(candidate)
				delete(m.leases, candidate)
				m.leaseOrder.Remove(element)
			}
		}
		if len(m.leases) >= applicationLeaseLimit {
			return ApplicationLease{}, ErrCapacity
		}
		lease = &memoryLease{ApplicationLease: ApplicationLease{ID: id, Term: term, Recoverable: true, Generation: 1,
			PrincipalHash: a.PrincipalHash, HolderID: a.HolderID, HolderEpoch: a.HolderEpoch, ReattachTimeoutMS: timeout}, tokenHash: a.TokenHash, element: m.leaseOrder.PushBack(id)}
	} else {
		if lease == nil || !lease.Recoverable || lease.Term != term || !leaseHashEqual(lease.PrincipalHash, a.PrincipalHash) {
			return ApplicationLease{}, ErrStale
		}
		if mode == "attach" && lease.Generation == generation+1 && lease.lastAttachGeneration == generation && lease.LastAttachID == a.AttemptID &&
			lease.HolderID == a.HolderID && lease.HolderEpoch == a.HolderEpoch && leaseHashEqual(lease.lastTokenHash, a.TokenHash) && leaseHashEqual(lease.lastNextTokenHash, a.NextTokenHash) {
			if lease.sealed {
				return ApplicationLease{}, ErrExpired
			}
			if lease.ExpiresAtMS <= now {
				if lease.AttachmentUntilMS == 0 || now >= lease.AttachmentUntilMS {
					return ApplicationLease{}, ErrExpired
				}
				lease.ExpiresAtMS = min(now+RecoverableApplicationLeaseLifetime.Milliseconds(), lease.AttachmentUntilMS)
				lease.RecoveryUntilMS = 0
				if lease.references == 0 {
					lease.idleUntil = now + applicationLeaseIdleTTL.Milliseconds()
				}
				m.scheduleApplicationExpiry(lease, now, false)
			}
			result := lease.ApplicationLease
			result.CheckedAtMS = now
			result.Sealed = lease.sealed
			return result, nil
		}
		deadline := lease.ExpiresAtMS + lease.ReattachTimeoutMS
		if lease.AttachmentUntilMS > 0 {
			deadline = lease.AttachmentUntilMS
		}
		if lease.sealed || now >= deadline {
			return ApplicationLease{}, ErrExpired
		}
		if lease.Generation != generation {
			return ApplicationLease{}, ErrStale
		}
		switch mode {
		case "renew", "complete":
			if lease.HolderID != a.HolderID || lease.HolderEpoch != a.HolderEpoch {
				return ApplicationLease{}, ErrStale
			}
			if lease.ExpiresAtMS <= now {
				return ApplicationLease{}, ErrExpired
			}
			if mode == "complete" {
				lease.AttachmentUntilMS = 0
			}
		case "attach":
			if !leaseHashEqual(lease.tokenHash, a.TokenHash) {
				return ApplicationLease{}, ErrStale
			}
			if lease.HolderID != a.HolderID || lease.HolderEpoch != a.HolderEpoch {
				if lease.ExpiresAtMS > now {
					return ApplicationLease{}, ErrBusy
				}
				if lease.Generation >= 1<<53-1 {
					return ApplicationLease{}, ErrCapacity
				}
				if leaseHashEqual(a.TokenHash, a.NextTokenHash) {
					return ApplicationLease{}, ErrConflict
				}
				lease.AttachmentUntilMS = deadline
				lease.lastAttachGeneration = lease.Generation
				lease.Generation++
				lease.LastAttachID = a.AttemptID
				lease.lastTokenHash = a.TokenHash
				lease.lastNextTokenHash = a.NextTokenHash
				lease.tokenHash = a.NextTokenHash
				lease.HolderID = a.HolderID
				lease.HolderEpoch = a.HolderEpoch
			} else if lease.ExpiresAtMS <= now {
				lease.AttachmentUntilMS = deadline
			}
		default:
			return ApplicationLease{}, ErrCorrupt
		}
	}
	lease.ExpiresAtMS = max(lease.ExpiresAtMS, now+RecoverableApplicationLeaseLifetime.Milliseconds())
	if lease.AttachmentUntilMS > 0 {
		lease.ExpiresAtMS = min(lease.ExpiresAtMS, lease.AttachmentUntilMS)
	}
	lease.CheckedAtMS = now
	lease.RecoveryUntilMS = 0
	if lease.references == 0 {
		lease.idleUntil = now + applicationLeaseIdleTTL.Milliseconds()
	}
	m.leases[id] = lease
	m.scheduleApplicationExpiry(lease, now, completing)
	return lease.ApplicationLease, nil
}
