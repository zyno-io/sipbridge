package engine

import (
	"context"
	"time"

	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"

	"sipbridge/internal/control"
	pb "sipbridge/internal/controlpb"
)

// presentResident sends one fresh request with its original bound. A failed
// callback competes with response selection under the same context lock.
func (e *ResidentMemory) presentResident(parent context.Context, peer *control.Peer, c *residentContext, requestID string, deadline time.Time, presentation *pb.RequestPresented) error {
	ctx, cancel := context.WithDeadline(parent, deadline)
	accepted := false
	if ctx.Err() == nil {
		remaining := time.Until(deadline).Milliseconds()
		if remaining > 0 {
			reply, err := peer.Call(ctx, &pb.Envelope{RemainingBudgetMs: uint32(min(2000, remaining)), Payload: &pb.Envelope_Request{Request: presentation}})
			if err == nil {
				result := reply.GetResult()
				accepted = result != nil && result.Error == "" && (result.Certainty == pb.OperationResult_ACCEPTED || result.Certainty == pb.OperationResult_COMPLETED)
			}
		}
	}
	cancel()
	if accepted {
		return nil
	}
	return e.failResidentPresentation(c, requestID)
}

// A presentation failure competes with a controller response under the same
// context lock; it may select a 503 only while the original transaction remains open.
func (e *ResidentMemory) failResidentPresentation(c *residentContext, requestID string) error {
	c.mu.Lock()
	tx := c.transactions[c.requests[requestID]]
	if tx == nil || tx.command != nil || tx.native.Phase == portable.Completed || tx.native.Phase == portable.Confirmed || tx.native.Phase == portable.Terminated {
		c.mu.Unlock()
		return nil
	}
	contact, _, err := advertisement(e.cfg, tx.native.Origin.Transport)
	if err != nil {
		c.mu.Unlock()
		return err
	}
	message, err := portable.BuildResponseWithHeaders(tx.native.Origin, 503, reason(503), nil, c.snapshot.LocalTag, contact, nil)
	if err != nil {
		c.mu.Unlock()
		return err
	}
	next, effects, err := portable.StepTransaction(tx.native, portable.Input{Kind: portable.SendResponse, Now: time.Now().UnixMilli(), Message: message})
	if err != nil {
		c.mu.Unlock()
		return err
	}
	delta, borrowed, err := e.stageResidentTransactionBytesLocked(c, tx, next)
	if err != nil {
		c.mu.Unlock()
		return err
	}
	var dialog *residentDialog
	var dialogKey string
	var dialogNext portable.Dialog
	var dialogEffects []portable.Effect
	var growth residentClientGrowth
	origin, parseErr := tx.native.Origin.Parse()
	if request, ok := origin.(*sip.Request); parseErr == nil && ok && request.IsInvite() && request.To() != nil && request.From() != nil {
		dialogKey = tag(request.To().Params) + "/" + tag(request.From().Params)
		dialog = c.dialogs[dialogKey]
		if dialog != nil {
			dialogNext, dialogEffects, err = portable.StepDialog(dialog.native, portable.DialogInput{Kind: portable.DialogSendResponse, Now: time.Now().UnixMilli(), Message: message})
			if err == nil {
				growth, err = e.prepareResidentClientGrowth(c, dialog.budget, residentDialogBytes(dialogNext)-residentDialogBytes(dialog.native))
			}
			if err != nil {
				e.abortResidentTransactionBytesLocked(c, tx, delta, borrowed)
				c.mu.Unlock()
				return err
			}
		}
	}
	writes, err := e.stageResidentOutputsLocked(c, tx, effects, nil, "", time.Now().Add(2*time.Second))
	if err != nil {
		if dialog != nil {
			e.rollbackResidentClientGrowth(c, growth)
		}
		e.abortResidentTransactionBytesLocked(c, tx, delta, borrowed)
		c.mu.Unlock()
		return err
	}
	tx.native = next
	if c.subscription != nil && c.subscription.Pending == tx.native.Key {
		c.subscription.Pending = ""
	}
	if dialog != nil {
		dialog.native = dialogNext
		e.commitResidentClientGrowth(c, growth, &dialog.borrowed)
		_ = e.scheduleResidentDialogLocked(c, dialogKey, dialogEffects)
	}
	e.commitResidentTransactionBytesLocked(c, tx, delta, borrowed)
	_ = e.scheduleResidentTransactionLocked(c, tx, effects)
	e.retireResidentTransactionLocked(c, tx)
	c.mu.Unlock()
	e.dispatchResidentTimerOutputs(writes)
	return nil
}
