package engine

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
	"google.golang.org/protobuf/proto"
	"sipbridge/internal/control"
	pb "sipbridge/internal/controlpb"
)

// command selects an application response under the context lock. Physical
// completion settles the particular command receipt, independently of later commands.
func (e *ResidentMemory) command(ctx context.Context, peer *control.Peer, cmd *pb.Command, operation string, deadline time.Time) (*pb.OperationResult, error) {
	if cmd == nil || cmd.Context == nil {
		return nil, errors.New("command context required")
	}
	if cmd.GetRespond() == nil {
		return e.commandResidentClient(ctx, peer, cmd, operation, deadline)
	}
	c, err := e.findContext(cmd.Context)
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	if !e.residentContextCurrentLocked(c, cmd.Context) {
		c.mu.Unlock()
		return nil, errors.New("resident context changed")
	}
	receipt, policy, replay, err := e.admitResidentCommandLocked(c, peer, cmd, operation, deadline)
	if err != nil {
		c.mu.Unlock()
		return nil, err
	}
	if replay {
		c.mu.Unlock()
		return awaitResidentReplay(ctx, c, receipt)
	}
	var expiredOwnerKey string
	var requiredNotify *residentBudget
	refuse := func(err error) (*pb.OperationResult, error) {
		if requiredNotify != nil {
			e.closeResidentBudgetLocked(c, requiredNotify)
			requiredNotify = nil
		}
		finishResidentCommandLocked(c, receipt, &pb.OperationResult{Certainty: pb.OperationResult_NOT_ACCEPTED, Error: err.Error()})
		result := residentCommandResultLocked(c, receipt)
		if receipt.originalPending && receipt.registered {
			c.receiptPins--
		}
		receipt.originalPending = false
		releaseResidentCommandWireLocked(c, receipt)
		releaseResidentCommandLocked(c, receipt)
		if c.retireReady {
			e.afterResidentOutputReleaseLocked(c)
		}
		c.mu.Unlock()
		return result, nil
	}
	response := cmd.GetRespond()
	if response == nil {
		return refuse(errors.New("unsupported resident command"))
	}
	tx := c.transactions[c.requests[response.RequestId]]
	if tx != nil {
		receipt.budget = tx.budget
	}
	if tx == nil || tx.native.Phase == portable.Terminated || tx.native.Phase == portable.Completed || tx.native.Phase == portable.Confirmed {
		return refuse(errors.New("server transaction cannot select response"))
	}
	original, err := tx.native.Origin.Parse()
	if err != nil {
		return refuse(err)
	}
	method := original.CSeq().MethodName
	if response.PrincipalId != "" && c.snapshot.PrincipalId != "" && c.snapshot.PrincipalId != response.PrincipalId {
		return refuse(errors.New("principal mismatch"))
	}
	var message portable.Message
	var subscription *stateSubscription
	var snapshot *pb.ContextSnapshot
	if method == sip.SUBSCRIBE && response.Status >= 200 && response.Status < 300 {
		needsAdoption := c.subscription != nil || c.snapshot.Delivery != nil || !proto.Equal(c.flow, tx.flow) || !sameProcess(c.snapshot.Application, tx.application)
		var delivery *pb.ContextDelivery
		if needsAdoption {
			revision := c.revision
			proposal := proto.Clone(c.snapshot).(*pb.ContextSnapshot)
			if response.PrincipalId != "" {
				proposal.PrincipalId = response.PrincipalId
				proposal.BindingId = response.BindingId
			}
			flow := proto.Clone(tx.flow).(*pb.FlowRef)
			application := proto.Clone(tx.application).(*pb.ProcessRef)
			c.mu.Unlock()
			delivery, err = e.adoptResidentFlow(ctx, proposal, flow, application, true)
			c.mu.Lock()
			if err != nil {
				return refuse(err)
			}
			e.mu.Lock()
			lease := e.leases[residentControllerKey(peer.Application())]
			live := lease != nil && peer.ApplicationLease() != nil && lease.term == peer.ApplicationLease().Term && time.Now().Before(lease.expires)
			e.mu.Unlock()
			if !live || c.revision != revision || c.transactions[tx.native.Key] != tx || c.command != receipt || !time.Now().Before(deadline) || time.Now().UnixMilli() >= int64(policy.ExpiresAtMs) || !proto.Equal(c.snapshot.Delivery, policy.ExpectedDelivery) {
				return refuse(errors.New("subscription authority changed during flow adoption"))
			}
		}
		subscription, snapshot, message, err = e.prepareResidentSubscription(c, tx, response, time.Now())
		if err == nil && delivery != nil {
			snapshot.Delivery = delivery
			snapshot.ApplicationLeaseTerm = peer.ApplicationLease().Term
		}
	} else {
		var contact string
		contact, _, err = advertisement(e.cfg, tx.native.Origin.Transport)
		if err == nil && method == sip.REFER && response.Status >= 200 && response.Status < 300 {
			request, ok := original.(*sip.Request)
			if !ok || request.To() == nil || request.From() == nil {
				err = errors.New("REFER identity unavailable")
			} else if tag(request.To().Params) != "" {
				dialog := c.dialogs[tag(request.To().Params)+"/"+tag(request.From().Params)]
				if dialog == nil || !portable.MatchDialogRequest(dialog.native, tx.native.Origin) || dialog.native.Contact == "" {
					err = errors.New("successful in-dialog REFER requires retained native Contact")
				} else {
					contact = dialog.native.Contact
				}
			}
		}
		if err == nil {
			message, err = portable.BuildResponseWithHeaders(tx.native.Origin, int(response.Status), reason(response.Status), response.Body, c.snapshot.LocalTag, contact, stateHeaders(response.Headers))
		}
	}
	if err != nil {
		return refuse(err)
	}
	if subscription != nil {
		if c.requiredNotifyBudget != nil {
			return refuse(errors.New("required subscription NOTIFY is pending"))
		}
		requiredNotify, err = e.openResidentBudgetLocked(c, residentSmallBudget)
		if err != nil {
			return refuse(err)
		}
	}
	var dialog *residentDialog
	var dialogEffects []portable.Effect
	var dialogKey string
	var inviteSnapshot *pb.ContextSnapshot
	if method == sip.INVITE && response.Status >= 200 {
		var candidate portable.Dialog
		var dialogErr error
		if c.selectedDialog != "" {
			previous := c.dialogs[c.selectedDialog]
			if previous == nil {
				return refuse(errors.New("re-INVITE dialog unavailable"))
			}
			candidate = previous.native
			dialogKey = c.selectedDialog
		} else if response.Status < 300 {
			candidate, dialogErr = portable.NewDialog(portable.UAS, tx.native.Origin, message, portable.DefaultTimers())
			if dialogErr != nil {
				return refuse(dialogErr)
			}
			_, candidate.SentBy, dialogErr = advertisement(e.cfg, tx.native.Origin.Transport)
			if dialogErr != nil {
				return refuse(dialogErr)
			}
			dialogKey = candidate.LocalTag + "/" + candidate.RemoteTag
		}
		if dialogKey != "" {
			candidate, dialogEffects, dialogErr = portable.StepDialog(candidate, portable.DialogInput{Kind: portable.DialogSendResponse, Now: time.Now().UnixMilli(), Message: message, Answer: len(response.Body) > 0})
			if dialogErr != nil {
				return refuse(dialogErr)
			}
			dialog = &residentDialog{native: candidate, transaction: tx.native.Key, budget: tx.budget}
			if previous := c.dialogs[dialogKey]; previous != nil {
				copy := *previous
				copy.native = candidate
				if response.Status < 300 {
					copy.transaction = tx.native.Key
				}
				dialog = &copy
			}
			inviteSnapshot = proto.Clone(c.snapshot).(*pb.ContextSnapshot)
			if response.Status < 300 {
				inviteSnapshot.Answered = true
				inviteSnapshot.LocalSdp = append([]byte(nil), response.Body...)
				inviteSnapshot.RemoteSdp = append([]byte(nil), original.Body()...)
				inviteSnapshot.DialogId = sip.DialogIDMake(candidate.CallID, candidate.LocalTag, candidate.RemoteTag)
				inviteSnapshot.LocalTag, inviteSnapshot.RemoteTag, inviteSnapshot.RemoteTarget = candidate.LocalTag, candidate.RemoteTag, candidate.Target
			}
		}
	}
	next, effects, err := portable.StepTransaction(tx.native, portable.Input{Kind: portable.SendResponse, Now: time.Now().UnixMilli(), Message: message})
	if err != nil {
		return refuse(err)
	}
	txDelta, txBorrowed, err := e.stageResidentTransactionBytesLocked(c, tx, next)
	if err != nil {
		return refuse(err)
	}
	var subscriptionDelta, subscriptionBorrowed int64
	if subscription != nil {
		oldView := &residentContext{snapshot: c.snapshot, flow: c.flow}
		newFlow := c.flow
		if snapshot.Delivery != nil {
			newFlow = tx.flow
		}
		newView := &residentContext{snapshot: snapshot, flow: newFlow}
		subscriptionDelta = residentContextBytes(newView) - residentContextBytes(oldView) + residentSubscriptionBytes(subscription) - residentSubscriptionBytes(c.subscription)
		if subscriptionDelta > 0 {
			subscriptionBorrowed, err = e.chargeResidentBudgetLocked(c, c.subscriptionBudget, subscriptionDelta)
		}
		if err != nil {
			e.abortResidentTransactionBytesLocked(c, tx, txDelta, txBorrowed)
			return refuse(err)
		}
	}
	var dialogDelta, dialogBorrowed int64
	if dialog != nil {
		dialogDelta = residentDialogBytes(dialog.native) + int64(len(dialog.transaction)+len(dialog.localTransaction)) + residentContextBytes(&residentContext{snapshot: inviteSnapshot, flow: c.flow}) - residentContextBytes(&residentContext{snapshot: c.snapshot, flow: c.flow})
		if previous := c.dialogs[dialogKey]; previous != nil {
			dialogDelta -= residentDialogBytes(previous.native) + int64(len(previous.transaction)+len(previous.localTransaction))
		}
		if dialogDelta > 0 {
			dialogBorrowed, err = e.chargeResidentBudgetLocked(c, dialog.budget, dialogDelta)
		}
		if err != nil {
			if subscriptionDelta > 0 {
				_ = e.refundResidentBudgetLocked(c, c.subscriptionBudget, subscriptionDelta, subscriptionBorrowed)
			}
			e.abortResidentTransactionBytesLocked(c, tx, txDelta, txBorrowed)
			return refuse(err)
		}
	}
	previous := tx.command
	tx.command = receipt
	writes, err := e.stageResidentOutputsLocked(c, tx, effects, policy, operation, deadline)
	if err != nil {
		tx.command = previous
		e.abortResidentTransactionBytesLocked(c, tx, txDelta, txBorrowed)
		if subscriptionDelta > 0 {
			_ = e.refundResidentBudgetLocked(c, c.subscriptionBudget, subscriptionDelta, subscriptionBorrowed)
		}
		if dialogDelta > 0 {
			_ = e.refundResidentBudgetLocked(c, dialog.budget, dialogDelta, dialogBorrowed)
		}
		return refuse(err)
	}
	if len(writes) == 0 {
		tx.command = previous
		e.abortResidentTransactionBytesLocked(c, tx, txDelta, txBorrowed)
		if subscriptionDelta > 0 {
			_ = e.refundResidentBudgetLocked(c, c.subscriptionBudget, subscriptionDelta, subscriptionBorrowed)
		}
		if dialogDelta > 0 {
			_ = e.refundResidentBudgetLocked(c, dialog.budget, dialogDelta, dialogBorrowed)
		}
		return refuse(errors.New("response produced no output"))
	}
	retainResidentCommandLocked(receipt)
	releaseResidentCommandLocked(c, previous)
	tx.native = next
	if response.Status >= 300 && c.subscription != nil && c.subscription.Pending == tx.native.Key {
		c.subscription.Pending = ""
	}
	e.commitResidentTransactionBytesLocked(c, tx, txDelta, txBorrowed)
	if subscriptionDelta < 0 {
		returned := min(c.subscriptionBorrowed, -subscriptionDelta)
		_ = e.refundResidentBudgetLocked(c, c.subscriptionBudget, -subscriptionDelta, returned)
		c.subscriptionBorrowed -= returned
	}
	c.subscriptionBorrowed += subscriptionBorrowed
	if dialogDelta < 0 {
		returned := min(dialog.borrowed, -dialogDelta)
		_ = e.refundResidentBudgetLocked(c, dialog.budget, -dialogDelta, returned)
		dialog.borrowed -= returned
	}
	if dialog != nil {
		dialog.borrowed += dialogBorrowed
	}
	if dialog != nil {
		previous := c.dialogs[dialogKey]
		c.snapshot = inviteSnapshot
		c.dialogs[dialogKey] = dialog
		if previous != nil && previous.transaction != dialog.transaction {
			if old := c.transactions[previous.transaction]; old != nil {
				e.retireResidentTransactionLocked(c, old)
			}
		}
		c.selectedDialog = dialogKey
		c.revision++
		c.snapshot.Revision++
		observed, observeErr := Observe(message.Wire, true)
		if observeErr == nil {
			e.mu.Lock()
			for _, selector := range observed.Selectors {
				if strings.HasPrefix(selector, "dlg:") {
					e.dialogs[selector] = c
				}
			}
			e.mu.Unlock()
		}
		_ = e.scheduleResidentDialogLocked(c, dialogKey, dialogEffects)
	}
	if subscription != nil {
		c.requiredNotifyBudget = requiredNotify
		requiredNotify = nil
		c.subscription = subscription
		c.snapshot = snapshot
		if snapshot.Delivery != nil {
			c.flow = proto.Clone(tx.flow).(*pb.FlowRef)
			c.snapshot.Application = proto.Clone(tx.application).(*pb.ProcessRef)
			c.ownerLost = false
			e.mu.Lock()
			e.indexResidentOwnerUnderEngineLock(c)
			lease := e.leases[residentControllerKey(c.snapshot.Application)]
			if lease == nil || lease.term != c.snapshot.ApplicationLeaseTerm || !time.Now().Before(lease.expires) {
				expiredOwnerKey = c.ownerKey
			}
			e.mu.Unlock()
		}
		c.revision++
		c.snapshot.Revision++
		e.scheduleResidentSubscriptionLocked(c)
		observed, observeErr := Observe(message.Wire, true)
		if observeErr == nil {
			e.mu.Lock()
			for _, selector := range observed.Selectors {
				if strings.HasPrefix(selector, "dlg:") {
					e.dialogs[selector] = c
				}
			}
			e.mu.Unlock()
		}
	}
	if response.PrincipalId != "" {
		c.snapshot.PrincipalId = response.PrincipalId
		c.snapshot.BindingId = response.BindingId
	}
	_ = e.scheduleResidentTransactionLocked(c, tx, effects)
	e.retireResidentTransactionLocked(c, tx)
	c.mu.Unlock()
	if expiredOwnerKey != "" {
		e.loseResidentOwner(c, expiredOwnerKey)
	}
	e.dispatchResident(ctx, writes)
	return awaitResidentCommand(ctx, c, receipt)
}

func (e *ResidentMemory) residentOutputCompleted(c *residentContext, output *residentOutput, result *pb.DeliveryResult) ([]*pb.FlowWrite, error) {
	writes, handled, err := e.residentClientOutputCompletedLocked(c, output, result)
	if err != nil || handled {
		return writes, err
	}
	if output.command == nil {
		return nil, nil
	}
	// A written client request is only transport evidence; its transaction
	// remains pending until the native response establishes the result.
	if output.status == 0 && result.Outcome == pb.DeliveryResult_WRITTEN {
		if output.method == "BYE" {
			if tx := c.transactions[output.transaction]; tx != nil && !tx.cleanup {
				request, err := Observe(tx.native.Origin.Wire, true)
				if err != nil {
					return nil, err
				}
				finishResidentCommandLocked(c, output.command, &pb.OperationResult{Certainty: pb.OperationResult_ACCEPTED, Request: request})
			}
		}
		return nil, nil
	}
	certainty := pb.OperationResult_UNKNOWN
	message := "SIP delivery unavailable or uncertain"
	if result.Outcome == pb.DeliveryResult_WRITTEN {
		certainty = pb.OperationResult_COMPLETED
		message = ""
	}
	finishResidentCommandLocked(c, output.command, &pb.OperationResult{Certainty: certainty, SipStatus: output.status, Error: message})
	return nil, nil
}
