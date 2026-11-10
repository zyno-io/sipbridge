package engine

import (
	"context"
	"errors"

	"sipbridge/internal/control"
	"sipbridge/internal/state"
)

// Historical claimed-driver preflight uses observations from claim acquisition; the
// normal live-owner path does no additional Redis read. Pure reducers never do
// I/O. A successful owner renewal refreshes the same claim's observations before
// reduction, while ordinary claim/revision/transport checks remain in force.
func (e *StateEngine) resumeConnectedOwners(ctx context.Context, step state.Step) (state.Step, error) {
	if !e.autonomousCompletion() || e.control == nil {
		return step, nil
	}
	expired := false
	for _, lease := range step.Record.ApplicationLeases {
		if lease.Term != "" && lease.ExpiresAtMS <= lease.CheckedAtMS {
			expired = true
			break
		}
	}
	if !expired {
		return step, nil
	}
	group, err := decodeStateGroup(step.Record)
	if err != nil {
		return step, err
	}
	seen := make(map[string]bool)
	resumed := false
	for _, c := range group.Contexts {
		if c.Rejection || c.OwnerLost {
			continue
		}
		id := e.applicationLeaseID(c.Snapshot.Application)
		lease, ok := step.Record.ApplicationLeases[id]
		if !ok || seen[id] || lease.Term != c.Snapshot.ApplicationLeaseTerm || lease.ExpiresAtMS > lease.CheckedAtMS {
			continue
		}
		seen[id] = true
		peer, err := e.control.Peer(c.Snapshot.Application)
		if err != nil {
			continue
		}
		cached := peer.ApplicationLease()
		if cached == nil || cached.Term != lease.Term {
			continue
		}
		err = peer.ResumeApplicationLease()
		if err != nil {
			if !errors.Is(err, control.ErrApplicationLeaseFenced) {
				return step, err
			}
		} else {
			resumed = true
		}
	}
	if !resumed {
		return step, nil
	}
	return e.store.(*state.Store).RefreshClaim(ctx, step)
}
