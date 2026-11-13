package engine

import (
	"context"
	"errors"
	"time"

	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
)

func (e *ResidentMemory) completeResidentClient(ctx context.Context, flow *pb.FlowRef, received *pb.SipMessage) (bool, error) {
	if flow == nil || received == nil {
		return false, nil
	}
	parsed, err := parseObservedMessage(received.Wire)
	if err != nil {
		return false, err
	}
	response, ok := parsed.(*sip.Response)
	if !ok {
		return false, nil
	}
	return e.completeResidentClientParsed(ctx, flow, received.Wire, response)
}

func (e *ResidentMemory) completeResidentClientParsed(ctx context.Context, flow *pb.FlowRef, wire []byte, response *sip.Response, projected ...[]string) (bool, error) {
	key, err := sip.ClientTxKeyMake(response)
	if err != nil {
		return false, nil
	}
	var selectors []string
	if len(projected) > 0 {
		selectors = projected[0]
	} else {
		selectors = observedMessageSelectors(response, false)
	}
	e.mu.Lock()
	var c *residentContext
	for _, selector := range selectors {
		if c = e.transactions[selector]; c != nil {
			break
		}
	}
	e.mu.Unlock()
	if c == nil {
		return false, nil
	}
	message := portable.Message{Wire: wire, Transport: flow.Transport}
	c.mu.Lock()
	if c.snapshot == nil || !e.residentContextCurrentLocked(c, c.snapshot.Context) {
		c.mu.Unlock()
		return false, nil
	}
	tx := c.transactions[key]
	if tx == nil || tx.native.Family != portable.NonInviteClient && tx.native.Family != portable.InviteClient || !proto.Equal(tx.flow, flow) || !portable.MatchResponse(tx.native.Origin, message) {
		c.mu.Unlock()
		return false, nil
	}
	handled, err := e.residentDigest(ctx, c, tx, message, response)
	if handled || err != nil {
		c.mu.Unlock()
		return handled, err
	}
	if tx.native.Family == portable.InviteClient {
		observed := &pb.SipMessage{Wire: wire, Selectors: selectors}
		writes, event, err := e.residentInviteResponseLocked(c, tx, message, observed, response)
		c.mu.Unlock()
		if err != nil {
			return true, err
		}
		e.dispatchResident(ctx, writes)
		if event != nil {
			e.dispatchResidentEvents(ctx, []*residentEvent{event})
		}
		return true, nil
	}
	next, effects, err := portable.StepTransaction(tx.native, portable.Input{Kind: portable.ReceiveResponse, Now: time.Now().UnixMilli(), Message: message})
	if err != nil {
		c.mu.Unlock()
		return true, err
	}
	var dialog *residentDialog
	var nextDialog portable.Dialog
	var dialogEffects []portable.Effect
	var dialogKey string
	var event *residentEvent
	txDelta := residentClientTransactionDelta(tx, next)
	var dialogDelta int64
	oldSnapshot := c.snapshot
	if oldSnapshot.Revision >= 1<<53-1 {
		c.mu.Unlock()
		return true, errors.New("observation revision exhausted")
	}
	if response.CSeq().MethodName == sip.BYE {
		dialogKey = tag(response.From().Params) + "/" + tag(response.To().Params)
		dialog = c.dialogs[dialogKey]
		if dialog != nil && portable.MatchDialogResponse(dialog.native, message) {
			nextDialog, dialogEffects, err = portable.StepDialog(dialog.native, portable.DialogInput{Kind: portable.DialogReceiveResponse, Now: time.Now().UnixMilli(), Message: message})
			if err != nil {
				c.mu.Unlock()
				return true, err
			}
			dialogDelta = residentDialogBytes(nextDialog) - residentDialogBytes(dialog.native)

		} else {
			dialog = nil
		}
	}
	txGrowth, err := e.prepareResidentClientGrowth(c, tx.budget, txDelta)
	if err != nil {
		c.mu.Unlock()
		return true, err
	}
	var dialogGrowth residentClientGrowth
	if dialog != nil {
		dialogGrowth, err = e.prepareResidentClientGrowth(c, dialog.budget, dialogDelta)
		if err != nil {
			e.rollbackResidentClientGrowth(c, txGrowth)
			c.mu.Unlock()
			return true, err
		}
	}
	rollback := func() {
		if dialog != nil {
			e.rollbackResidentClientGrowth(c, dialogGrowth)
		}
		e.rollbackResidentClientGrowth(c, txGrowth)
	}
	if response.CSeq().MethodName == sip.NOTIFY && response.StatusCode >= 200 && c.subscription != nil && !c.snapshot.Ended && (c.subscription.Final || response.StatusCode == 481) {
		c.snapshot = proto.Clone(c.snapshot).(*pb.ContextSnapshot)
		c.snapshot.Ended = true
		event, err = e.stageResidentEventLocked(c, pb.DialogEvent_ENDED, nil, "", time.Now().Add(2*time.Minute), c.subscriptionBudget)
		if err != nil {
			c.snapshot = oldSnapshot
			rollback()
			c.mu.Unlock()
			return true, err
		}
	}
	if dialog != nil {
		for _, effect := range dialogEffects {
			if effect.Kind == portable.Ended && dialogKey == c.selectedDialog {
				c.snapshot = proto.Clone(c.snapshot).(*pb.ContextSnapshot)
				c.snapshot.Ended = true
				observed := &pb.SipMessage{Wire: wire, Selectors: selectors}
				event, err = e.stageResidentEventLocked(c, pb.DialogEvent_ENDED, observed, "", time.Now().Add(2*time.Minute), dialog.budget)
				if err != nil {
					c.snapshot = oldSnapshot
					rollback()
					c.mu.Unlock()
					return true, err
				}
			}
		}
	}
	writes, err := e.stageResidentOutputsLocked(c, tx, effects, nil, "", time.Now().Add(2*time.Second))
	if err != nil {
		if event != nil {
			e.releaseResidentEventLocked(c, event)
		}
		c.snapshot = oldSnapshot
		rollback()
		c.mu.Unlock()
		return true, err
	}
	if c.snapshot == oldSnapshot {
		c.snapshot = proto.Clone(c.snapshot).(*pb.ContextSnapshot)
	}
	c.snapshot.Revision++
	if event != nil {
		event.message.Snapshot.Revision = c.snapshot.Revision
	}
	c.revision++
	tx.native = next
	tx.charged += txDelta
	e.commitResidentClientGrowth(c, txGrowth, &tx.borrowed)
	if dialog != nil {
		e.commitResidentClientGrowth(c, dialogGrowth, &dialog.borrowed)
	}
	if c.snapshot.Ended && !oldSnapshot.Ended {
		e.scheduleResidentContextRetirementLocked(c)
	}
	if dialog != nil {
		dialog.native = nextDialog
		_ = e.scheduleResidentDialogLocked(c, dialogKey, dialogEffects)
	}
	_ = e.scheduleResidentTransactionLocked(c, tx, effects)
	if response.StatusCode >= 200 {
		finishResidentCommandWireLocked(c, tx.command, pb.OperationResult_COMPLETED, uint32(response.StatusCode), "", tx.native.Origin.Wire, wire)
	}
	e.retireResidentTransactionLocked(c, tx)
	c.mu.Unlock()
	e.dispatchResident(ctx, writes)
	if event != nil {
		e.dispatchResidentEvents(ctx, []*residentEvent{event})
	}
	return true, nil
}
