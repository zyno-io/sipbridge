package engine

import (
	"context"
	"crypto/sha256"
	"time"

	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
)

// tryNativeCompletion handles exact retained server transaction ACKs locally.
// Business requests remain subject to controller routing and admission.
func (e *ResidentMemory) tryNativeCompletion(ctx context.Context, flow *pb.FlowRef, received *pb.SipMessage) (bool, error) {
	return e.TryNativeCompletionParsed(ctx, flow, received, nil)
}

// TryNativeCompletionParsed reuses only the parsed value from this ingress
// operation. A caller without a framing parse supplies nil; nothing is retained.
func (e *ResidentMemory) TryNativeCompletionParsed(ctx context.Context, flow *pb.FlowRef, received *pb.SipMessage, parsed sip.Message, frozen ...portable.Message) (bool, error) {
	if flow == nil || received == nil {
		return false, nil
	}
	if parsed == nil {
		var err error
		parsed, err = parseObservedMessage(received.Wire)
		if err != nil {
			return false, err
		}
		received = &pb.SipMessage{Wire: received.Wire, Selectors: observedMessageSelectors(parsed, false)}
	}
	if response, ok := parsed.(*sip.Response); ok {
		return e.completeResidentClientParsed(ctx, flow, received.Wire, response, received.Selectors, frozen...)
	}
	request, ok := parsed.(*sip.Request)
	if !ok {
		return false, nil
	}
	if !request.IsAck() && !request.IsCancel() && request.Method != sip.BYE {
		return false, nil
	}
	observed := received
	if request.Method == sip.BYE {
		return e.completeResidentRemoteByeParsed(ctx, flow, received, observed, request, frozen...)
	}
	if request.IsCancel() {
		return e.completeResidentCancelParsed(ctx, flow, received, observed, request, frozen...)
	}
	key, err := sip.ServerTxKeyMake(request)
	if err != nil {
		return false, err
	}
	e.mu.Lock()
	var dialogContext *residentContext
	for _, selector := range observed.Selectors {
		if selected := e.dialogs[selector]; selected != nil {
			dialogContext = selected
			break
		}
	}
	e.mu.Unlock()
	if dialogContext != nil {
		c := dialogContext
		c.mu.Lock()
		if c.snapshot == nil || !e.residentContextCurrentLocked(c, c.snapshot.Context) {
			c.mu.Unlock()
			return false, nil
		}
		dialogKey := tag(request.To().Params) + "/" + tag(request.From().Params)
		dialog := c.dialogs[dialogKey]
		// A non-2xx ACK belongs to its INVITE transaction even when the
		// request also carries an established dialog tuple.
		if transaction := c.transactions[key]; transaction != nil && transaction.native.Family == portable.InviteServer && transaction.native.LastResponse != nil {
			final, parseErr := transaction.native.LastResponse.Facts()
			if parseErr == nil && !final.IsRequest && final.Status >= 300 && request.To() != nil && final.ToTag == tag(request.To().Params) {
				dialog = nil
			}
		}
		if dialog != nil {
			tx := c.transactions[dialog.transaction]
			if tx == nil || !validPolicyFlow(flow) || !sameProcess(flow.Ingress, e.cfg.Process) || (!proto.Equal(tx.flow, flow) && (flow.Transport != "tls" || c.snapshot.Transport != "tls")) {
				c.mu.Unlock()
				return true, rejectIngest(481, "ACK flow mismatch")
			}
			message, freezeErr := residentObservedNative(received.Wire, parsed, flow, frozen...)
			if freezeErr != nil {
				c.mu.Unlock()
				return true, freezeErr
			}
			next, effects, stepErr := portable.StepDialog(dialog.native, portable.DialogInput{Kind: portable.DialogReceiveRequest, Message: message, Now: time.Now().UnixMilli()})
			if stepErr != nil {
				c.mu.Unlock()
				return true, stepErr
			}
			acknowledged := false
			for _, effect := range effects {
				if effect.Kind == portable.Acknowledged {
					acknowledged = true
					break
				}
			}
			if !acknowledged {
				c.mu.Unlock()
				return true, nil
			}
			flowDelta := residentFlowBytes(flow) - residentFlowBytes(dialog.completionFlow)
			var flowBorrowed int64
			if flowDelta > 0 {
				flowBorrowed, stepErr = e.chargeResidentBudgetLocked(c, dialog.budget, flowDelta)
			}
			if stepErr != nil {
				c.mu.Unlock()
				return true, stepErr
			}
			delta := residentDialogBytes(next) - residentDialogBytes(dialog.native)
			var borrowed int64
			if delta > 0 {
				borrowed, stepErr = e.chargeResidentBudgetLocked(c, dialog.budget, delta)
			}
			if stepErr != nil {
				if flowDelta > 0 {
					_ = e.refundResidentBudgetLocked(c, dialog.budget, flowDelta, flowBorrowed)
				}
				c.mu.Unlock()
				return true, stepErr
			}
			var event *residentEvent
			for _, effect := range effects {
				if effect.Kind == portable.Acknowledged && !c.snapshot.InitialAck && dialogKey == c.selectedDialog {
					event, stepErr = e.stageResidentEventLocked(c, pb.DialogEvent_ACKNOWLEDGED, nil, "", time.Now().Add(2*time.Minute), dialog.budget)
					break
				}
			}
			if stepErr != nil {
				if delta > 0 {
					_ = e.refundResidentBudgetLocked(c, dialog.budget, delta, borrowed)
				}
				if flowDelta > 0 {
					_ = e.refundResidentBudgetLocked(c, dialog.budget, flowDelta, flowBorrowed)
				}
				c.mu.Unlock()
				return true, stepErr
			}
			originalBudget := tx.budget
			tx.budget = dialog.budget
			writes, stepErr := e.stageResidentOutputsLocked(c, tx, effects, nil, "", time.Now().Add(2*time.Second))
			tx.budget = originalBudget
			if stepErr != nil {
				if event != nil {
					e.releaseResidentEventLocked(c, event)
				}
				if delta > 0 {
					_ = e.refundResidentBudgetLocked(c, dialog.budget, delta, borrowed)
				}
				if flowDelta > 0 {
					_ = e.refundResidentBudgetLocked(c, dialog.budget, flowDelta, flowBorrowed)
				}
				c.mu.Unlock()
				return true, stepErr
			}
			if delta < 0 {
				returned := min(dialog.borrowed, -delta)
				_ = e.refundResidentBudgetLocked(c, dialog.budget, -delta, returned)
				dialog.borrowed -= returned
			}
			dialog.borrowed += borrowed
			if flowDelta < 0 {
				returned := min(dialog.completionBorrowed, -flowDelta)
				_ = e.refundResidentBudgetLocked(c, dialog.budget, -flowDelta, returned)
				dialog.completionBorrowed -= returned
			}
			dialog.completionBorrowed += flowBorrowed
			dialog.completionFlow = proto.Clone(flow).(*pb.FlowRef)
			dialog.native = next
			if event != nil {
				c.snapshot.InitialAck = true
				c.revision++
				c.snapshot.Revision++
				event.message.Snapshot.InitialAck = true
				event.message.Snapshot.Revision = c.snapshot.Revision
			}
			_ = e.scheduleResidentDialogLocked(c, dialogKey, effects)
			if c.ownerLost {
				cleanup, cleanupErr := e.residentOwnerByeLocked(c, dialogKey, dialog, false)
				if cleanupErr == nil {
					writes = append(writes, cleanup...)
				}
			}
			c.mu.Unlock()
			e.dispatchResident(ctx, writes)
			if event != nil {
				e.dispatchResidentEvents(ctx, []*residentEvent{event})
			}
			e.scheduleResidentAckFlowPublication(c, dialogKey, flow)
			return true, nil
		}
		c.mu.Unlock()
	}
	e.mu.Lock()
	var c *residentContext
	for _, selector := range observed.Selectors {
		if selected := e.transactions[selector]; selected != nil {
			c = selected
			break
		}
	}
	e.mu.Unlock()
	if c == nil {
		return false, nil
	}
	c.mu.Lock()
	if c.snapshot == nil || !e.residentContextCurrentLocked(c, c.snapshot.Context) {
		c.mu.Unlock()
		return false, nil
	}
	tx := c.transactions[key]
	if tx == nil || tx.native.Family != portable.InviteServer {
		c.mu.Unlock()
		return false, nil
	}
	if !proto.Equal(tx.flow, flow) {
		valid := validPolicyFlow(flow) && sameProcess(flow.Ingress, e.cfg.Process) && flow.Transport == "tls" && c.snapshot.Transport == "tls" && tx.native.LastResponse != nil
		if valid {
			final, parseErr := tx.native.LastResponse.Facts()
			valid = parseErr == nil && !final.IsRequest && final.Status >= 300 && request.To() != nil && final.ToTag == tag(request.To().Params)
		}
		if !valid {
			c.mu.Unlock()
			return false, nil
		}
	}
	message, err := residentObservedNative(received.Wire, request, flow, frozen...)
	if err != nil {
		c.mu.Unlock()
		return true, err
	}
	next, effects, err := portable.StepTransaction(tx.native, portable.Input{Kind: portable.ReceiveRequest, Message: message, Now: time.Now().UnixMilli()})
	if err != nil {
		c.mu.Unlock()
		return true, err
	}
	delta, borrowed, err := e.stageResidentTransactionBytesLocked(c, tx, next)
	if err != nil {
		c.mu.Unlock()
		return true, err
	}
	writes, err := e.stageResidentOutputsLocked(c, tx, effects, nil, "", time.Now().Add(2*time.Second))
	if err != nil {
		e.abortResidentTransactionBytesLocked(c, tx, delta, borrowed)
		c.mu.Unlock()
		return true, err
	}
	tx.native = next
	e.commitResidentTransactionBytesLocked(c, tx, delta, borrowed)
	_ = e.scheduleResidentTransactionLocked(c, tx, effects)
	e.retireResidentTransactionLocked(c, tx)
	c.mu.Unlock()
	e.dispatchResident(ctx, writes)
	return true, nil
}

func (e *ResidentMemory) completeResidentCancel(ctx context.Context, flow *pb.FlowRef, message *pb.SipMessage, observed *pb.SipMessage) (bool, error) {
	parsed, err := parseObservedMessage(message.Wire)
	if err != nil {
		return false, err
	}
	return e.completeResidentCancelParsed(ctx, flow, message, observed, parsed)
}

func (e *ResidentMemory) completeResidentCancelParsed(ctx context.Context, flow *pb.FlowRef, message *pb.SipMessage, observed *pb.SipMessage, parsed sip.Message, frozen ...portable.Message) (bool, error) {
	e.mu.Lock()
	var c *residentContext
	for _, selector := range observed.Selectors {
		if found := e.transactions[selector]; found != nil {
			c = found
			break
		}
	}
	e.mu.Unlock()
	if c == nil {
		return false, nil
	}
	input, err := residentObservedNative(message.Wire, parsed, flow, frozen...)
	if err != nil {
		return true, err
	}
	cancelKey, err := sip.ServerTxKeyMake(parsed)
	if err != nil {
		return true, err
	}
	c.mu.Lock()
	if c.snapshot == nil || !e.residentContextCurrentLocked(c, c.snapshot.Context) {
		c.mu.Unlock()
		return false, nil
	}
	cancel := c.transactions[cancelKey]
	if cancel != nil && cancel.retired {
		matched := cancel.wireHash == sha256.Sum256(message.Wire) && (proto.Equal(cancel.flow, flow) || validPolicyFlow(flow) && sameProcess(flow.Ingress, e.cfg.Process) && flow.Transport == "tls" && c.snapshot.Transport == "tls")
		c.mu.Unlock()
		if !matched {
			return true, rejectIngest(481, "CANCEL transaction mismatch")
		}
		return true, nil
	}
	var invite *residentTransaction
	for _, tx := range c.transactions {
		if tx.native.Family == portable.InviteServer && portable.MatchRequest(tx.native.Origin, input) {
			invite = tx
			break
		}
	}
	if invite == nil || !validPolicyFlow(flow) || !sameProcess(flow.Ingress, e.cfg.Process) || (!proto.Equal(invite.flow, flow) && (flow.Transport != "tls" || c.snapshot.Transport != "tls")) {
		c.mu.Unlock()
		return false, nil
	}
	var prior *portable.Transaction
	if cancel != nil {
		if cancel.retired && cancel.wireHash == sha256.Sum256(message.Wire) && proto.Equal(cancel.flow, flow) {
			c.mu.Unlock()
			return true, nil
		}
		if cancel.retired || (!proto.Equal(cancel.flow, flow) && (flow.Transport != "tls" || c.snapshot.Transport != "tls")) {
			c.mu.Unlock()
			return true, rejectIngest(481, "CANCEL transaction mismatch")
		}
		v := cancel.native
		prior = &v
	}
	nextInvite, nextCancel, effects, err := portable.StepServerCancel(invite.native, prior, portable.Input{Kind: portable.ReceiveRequest, Message: input, Now: time.Now().UnixMilli()})
	if err != nil {
		c.mu.Unlock()
		return true, err
	}
	var event *residentEvent
	for _, effect := range effects {
		if effect.Kind == portable.Cancelled && !c.directUnbound {
			event, err = e.stageResidentEventLocked(c, pb.DialogEvent_CANCELLED, nil, "", time.Now().Add(2*time.Minute), invite.budget)
			break
		}
	}
	if err != nil {
		c.mu.Unlock()
		return true, err
	}
	created := cancel == nil
	if created {
		cancel = &residentTransaction{native: nextCancel, flow: proto.Clone(flow).(*pb.FlowRef), application: proto.Clone(invite.application).(*pb.ProcessRef)}
		cancel.budget = invite.budget
		cancel.charged = residentTransactionBytes(cancel)
		cancel.borrowed, err = e.chargeResidentBudgetLocked(c, cancel.budget, cancel.charged)
		if err != nil {
			if event != nil {
				e.releaseResidentEventLocked(c, event)
			}
			c.mu.Unlock()
			return true, err
		}
	}
	inviteDelta, inviteBorrowed, err := e.stageResidentTransactionBytesLocked(c, invite, nextInvite)
	if err != nil {
		if created {
			_ = e.refundResidentBudgetLocked(c, cancel.budget, cancel.charged, cancel.borrowed)
		}
		if event != nil {
			e.releaseResidentEventLocked(c, event)
		}
		c.mu.Unlock()
		return true, err
	}
	var cancelDelta, cancelBorrowed int64
	if !created {
		cancelDelta, cancelBorrowed, err = e.stageResidentTransactionBytesLocked(c, cancel, nextCancel)
		if err != nil {
			e.abortResidentTransactionBytesLocked(c, invite, inviteDelta, inviteBorrowed)
			if event != nil {
				e.releaseResidentEventLocked(c, event)
			}
			c.mu.Unlock()
			return true, err
		}
	}
	effects = residentSuppressUncertainReplayLocked(c, invite, effects)
	selectedCancel := *cancel
	selectedCancel.native = nextCancel
	selectedCancel.flow = proto.Clone(flow).(*pb.FlowRef)
	flowDelta := int64(0)
	var flowBorrowed int64
	if !created {
		flowDelta = residentFlowBytes(selectedCancel.flow) - residentFlowBytes(cancel.flow)
		if flowDelta > 0 {
			flowBorrowed, err = e.chargeResidentBudgetLocked(c, cancel.budget, flowDelta)
		}
	}
	if err != nil {
		e.abortResidentTransactionBytesLocked(c, invite, inviteDelta, inviteBorrowed)
		if !created {
			e.abortResidentTransactionBytesLocked(c, cancel, cancelDelta, cancelBorrowed)
		}
		if event != nil {
			e.releaseResidentEventLocked(c, event)
		}
		c.mu.Unlock()
		return true, err
	}
	// A CANCEL-generated final for a re-INVITE must also clear the exact
	// dialog offer in the same publication as both transaction responses.
	var dialog *residentDialog
	var dialogKey string
	var dialogNext portable.Dialog
	var dialogEffects []portable.Effect
	var dialogGrowth residentClientGrowth
	origin, parseErr := invite.native.Origin.Facts()
	if parseErr == nil && origin.IsRequest && origin.ToTag != "" {
		dialogKey = origin.ToTag + "/" + origin.FromTag
		dialog = c.dialogs[dialogKey]
		if dialog != nil {
			var final *portable.Message
			for _, effect := range effects {
				if effect.Message == nil || !portable.MatchResponse(invite.native.Origin, *effect.Message) {
					continue
				}
				response, responseErr := effect.Message.Facts()
				if responseErr == nil && !response.IsRequest && response.Status == 487 {
					final = effect.Message
					break
				}
			}
			if final != nil && !invite.decisionPending {
				dialogNext, dialogEffects, err = portable.StepDialog(dialog.native, portable.DialogInput{Kind: portable.DialogSendResponse, Now: time.Now().UnixMilli(), Message: *final})
				if err == nil {
					dialogGrowth, err = e.prepareResidentClientGrowth(c, dialog.budget, residentDialogBytes(dialogNext)-residentDialogBytes(dialog.native))
				}
			} else {
				dialog = nil
			}
		}
	}
	if err != nil {
		if flowDelta > 0 {
			_ = e.refundResidentBudgetLocked(c, cancel.budget, flowDelta, flowBorrowed)
		}
		e.abortResidentTransactionBytesLocked(c, invite, inviteDelta, inviteBorrowed)
		if !created {
			e.abortResidentTransactionBytesLocked(c, cancel, cancelDelta, cancelBorrowed)
		} else {
			_ = e.refundResidentBudgetLocked(c, cancel.budget, cancel.charged, cancel.borrowed)
		}
		if event != nil {
			e.releaseResidentEventLocked(c, event)
		}
		c.mu.Unlock()
		return true, err
	}
	previousCommand := invite.command
	invite.command = nil
	writes, err := e.stageResidentMixedOutputsLocked(c, invite, effects, nil, "", time.Now().Add(2*time.Second), func(effect portable.Effect) *residentTransaction {
		if effect.Message != nil && portable.MatchResponse(nextCancel.Origin, *effect.Message) {
			return &selectedCancel
		}
		return invite
	})
	invite.command = previousCommand
	if err != nil {
		if dialog != nil {
			e.rollbackResidentClientGrowth(c, dialogGrowth)
		}
		if flowDelta > 0 {
			_ = e.refundResidentBudgetLocked(c, cancel.budget, flowDelta, flowBorrowed)
		}
		e.abortResidentTransactionBytesLocked(c, invite, inviteDelta, inviteBorrowed)
		if !created {
			e.abortResidentTransactionBytesLocked(c, cancel, cancelDelta, cancelBorrowed)
		}
		if created {
			_ = e.refundResidentBudgetLocked(c, cancel.budget, cancel.charged, cancel.borrowed)
		}
		if event != nil {
			e.releaseResidentEventLocked(c, event)
		}
		c.mu.Unlock()
		return true, err
	}
	oldInvite, oldCancel := invite.native, cancel.native
	invite.native = nextInvite
	if invite.decisionPending {
		e.clearResidentDecisionLocked(c, invite)
	}
	if dialog != nil {
		dialog.native = dialogNext
		e.commitResidentClientGrowth(c, dialogGrowth, &dialog.borrowed)
		_ = e.scheduleResidentDialogLocked(c, dialogKey, dialogEffects)
	}
	e.commitResidentTransactionBytesLocked(c, invite, inviteDelta, inviteBorrowed)
	cancel.native = nextCancel
	if flowDelta < 0 {
		returned := min(cancel.borrowed, -flowDelta)
		_ = e.refundResidentBudgetLocked(c, cancel.budget, -flowDelta, returned)
		cancel.borrowed -= returned
	}
	cancel.borrowed += flowBorrowed
	cancel.charged += flowDelta
	cancel.flow = selectedCancel.flow
	cancel.emissions = selectedCancel.emissions
	cancel.provisionalEmissions = selectedCancel.provisionalEmissions
	if !created {
		e.commitResidentTransactionBytesLocked(c, cancel, cancelDelta, cancelBorrowed)
	}
	c.transactions[cancelKey] = cancel
	e.syncResidentTransactionTimersLocked(c, invite, oldInvite)
	e.syncResidentTransactionTimersLocked(c, cancel, oldCancel)
	e.retireResidentTransactionLocked(c, cancel)
	c.revision++
	c.snapshot.Revision++
	if event != nil {
		event.message.Snapshot.Revision = c.snapshot.Revision
	}
	c.mu.Unlock()
	e.dispatchResident(ctx, writes)
	if event != nil {
		e.dispatchResidentEvents(ctx, []*residentEvent{event})
	}
	return true, nil
}
