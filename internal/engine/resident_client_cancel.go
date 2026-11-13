package engine

import (
	"errors"
	"time"

	"github.com/emiago/sipgo/sip/portable"
	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
)

// residentPairedCancelLocked consumes only the reducer's StartTransaction
// effect; it starts the paired native CANCEL under the INVITE's existing
// completion budget. The caller publishes the INVITE candidate immediately
// after this helper returns and must not perform another fallible stage.
func (e *ResidentMemory) residentPairedCancelLocked(c *residentContext, invite *residentTransaction, effects []portable.Effect, deadline time.Time) ([]*pb.FlowWrite, error) {
	var message *portable.Message
	for _, effect := range effects {
		if effect.Kind == portable.StartTransaction {
			if effect.Message == nil || message != nil {
				return nil, errors.New("invalid paired CANCEL effects")
			}
			message = effect.Message
		} else if effect.Kind == portable.Send || effect.Kind == portable.Respond {
			return nil, errors.New("mixed CANCEL and direct outputs")
		}
	}
	if message == nil {
		return nil, nil
	}
	native, err := portable.NewTransaction(portable.NonInviteClient, *message, portable.DefaultTimers(), "")
	if err != nil {
		return nil, err
	}
	if c.transactions[native.Key] != nil {
		return nil, errors.New("paired CANCEL already exists")
	}
	next, started, err := portable.StepTransaction(native, portable.Input{Kind: portable.Start, Now: time.Now().UnixMilli()})
	if err != nil {
		return nil, err
	}
	observed, err := Observe(message.Wire, true)
	if err != nil {
		return nil, err
	}
	tx := &residentTransaction{native: next, flow: proto.Clone(invite.flow).(*pb.FlowRef), application: proto.Clone(invite.application).(*pb.ProcessRef),
		cleanup: true, budget: invite.budget, selectors: append([]string(nil), observed.Selectors...)}
	tx.charged = residentTransactionBytes(tx)
	tx.borrowed, err = e.chargeResidentBudgetLocked(c, tx.budget, tx.charged)
	if err != nil {
		return nil, err
	}
	writes, err := e.stageResidentOutputsLocked(c, tx, started, nil, "", deadline)
	if err != nil {
		_ = e.refundResidentBudgetLocked(c, tx.budget, tx.charged, tx.borrowed)
		return nil, err
	}
	c.transactions[tx.native.Key] = tx
	e.mu.Lock()
	for _, selector := range tx.selectors {
		e.transactions[selector] = c
	}
	e.mu.Unlock()
	_ = e.scheduleResidentTransactionLocked(c, tx, started)
	return writes, nil
}

// residentCancelInviteLocked is also used by owner-loss cleanup. It never
// performs IO while holding the context lock.
func (e *ResidentMemory) residentCancelInviteLocked(c *residentContext, invite *residentTransaction, deadline time.Time) ([]*pb.FlowWrite, error) {
	next, effects, err := portable.StepTransaction(invite.native, portable.Input{Kind: portable.RequestCancel, Now: time.Now().UnixMilli()})
	if err != nil {
		return nil, err
	}
	delta := residentClientTransactionDelta(invite, next)
	growth, err := e.prepareResidentClientGrowth(c, invite.budget, delta)
	if err != nil {
		return nil, err
	}
	writes, err := e.residentPairedCancelLocked(c, invite, effects, deadline)
	if err != nil {
		e.rollbackResidentClientGrowth(c, growth)
		return nil, err
	}
	invite.native = next
	invite.charged += delta
	e.commitResidentClientGrowth(c, growth, &invite.borrowed)
	_ = e.scheduleResidentTransactionLocked(c, invite, effects)
	return writes, nil
}
