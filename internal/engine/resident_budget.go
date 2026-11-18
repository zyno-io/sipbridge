package engine

import "sipbridge/internal/state"

// One procedure owns its promised continuation capacity. Exact proofs may hold
// this small owner after closure; freeing them cannot revive a closed reserve.
// Its fields are protected by the owning context lock.
type residentBudget struct {
	remaining   int64
	open        bool
	outputs     int
	events      int
	provisional *residentEvent
	acks        map[residentACKIdentity]uint16
}

type residentACKIdentity struct {
	local, remote string
	sequence      uint32
}

const (
	residentSmallBudget         = int64(4*stateWireLimit + stateTransactionEmissions*stateProofMetadataBytes)
	residentSubscriptionBudget  = residentSmallBudget + 2*stateProofMetadataBytes
	residentAuthenticatedBudget = 3 * residentSmallBudget
	residentServerInviteBudget  = int64(8*stateWireLimit + 4*stateTransactionEmissions*stateProofMetadataBytes)
	// Initial INVITE admits the existing sixteen-branch profile, including
	// bounded ACK/cleanup/peer-BYE emissions, native wire and physical proofs.
	// This is admission headroom, never allocated per quiet dialog or REGISTER.
	residentInviteBudget = int64(stateForkDialogs * (8*stateWireLimit + 4*stateTransactionEmissions*stateProofMetadataBytes))
)

func (e *ResidentMemory) openResidentBudgetLocked(c *residentContext, limit int64) (*residentBudget, error) {
	if limit <= 0 {
		return nil, state.ErrCapacity
	}
	slots := stateTransactionEmissions
	if limit == residentInviteBudget {
		slots = stateForkDialogs*(3*stateTransactionEmissions+stateProfileTransmissions) + 2*stateTransactionEmissions
	}
	if limit == residentAuthenticatedBudget {
		slots *= 3
	}
	if limit == residentServerInviteBudget {
		slots = 4 * stateTransactionEmissions
	}
	if len(c.outputs)+c.reservedOutputs+slots > stateNativeProcedures*stateTransactionEmissions {
		return nil, state.ErrCapacity
	}
	events := 4
	if limit == residentInviteBudget {
		events = 4 * stateForkDialogs
	}
	if len(c.events)+c.reservedEvents+events > 256 {
		return nil, state.ErrCapacity
	}
	if err := e.accountResidentLocked(c, 0, limit); err != nil {
		return nil, err
	}
	c.reservedOutputs += slots
	c.reservedEvents += events
	return &residentBudget{remaining: limit, open: true, outputs: slots, events: events}, nil
}

func (e *ResidentMemory) chargeResidentBudgetLocked(c *residentContext, budget *residentBudget, bytes int64) (int64, error) {
	if bytes < 0 {
		return 0, state.ErrCorrupt
	}
	if bytes == 0 {
		return 0, nil
	}
	if budget == nil || !budget.open {
		return 0, state.ErrExpired
	}
	borrowed := min(budget.remaining, bytes)
	if err := e.accountResidentLocked(c, bytes, -borrowed); err != nil {
		return 0, err
	}
	budget.remaining -= borrowed
	return borrowed, nil
}

func (e *ResidentMemory) refundResidentBudgetLocked(c *residentContext, budget *residentBudget, bytes, borrowed int64) error {
	if bytes < 0 || borrowed < 0 || borrowed > bytes {
		return state.ErrCorrupt
	}
	restored := int64(0)
	if budget != nil && budget.open {
		restored = borrowed
	}
	if err := e.accountResidentLocked(c, -bytes, restored); err != nil {
		return err
	}
	if restored != 0 {
		budget.remaining += restored
	}
	return nil
}

func (e *ResidentMemory) closeResidentBudgetLocked(c *residentContext, budget *residentBudget) {
	if budget == nil || !budget.open {
		return
	}
	_ = e.accountResidentLocked(c, 0, -budget.remaining)
	c.reservedOutputs -= budget.outputs
	c.reservedEvents -= budget.events
	budget.outputs = 0
	budget.events = 0
	budget.remaining, budget.open = 0, false
}
