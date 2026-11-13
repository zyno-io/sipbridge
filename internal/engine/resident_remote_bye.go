package engine

import (
	"context"
	"crypto/sha256"
	"strings"
	"time"

	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
)

// completeResidentRemoteBye accepts only the retained dialog's exact admitted
// flow. Its native 200 and terminal event do not require a live controller.
func (e *ResidentMemory) completeResidentRemoteBye(ctx context.Context, flow *pb.FlowRef, received, observed *pb.SipMessage) (bool, error) {
	parsed, err := parseObservedMessage(received.Wire)
	if err != nil {
		return false, err
	}
	return e.completeResidentRemoteByeParsed(ctx, flow, received, observed, parsed)
}

func (e *ResidentMemory) completeResidentRemoteByeParsed(ctx context.Context, flow *pb.FlowRef, received, observed *pb.SipMessage, parsed sip.Message) (bool, error) {
	e.mu.Lock()
	var c *residentContext
	for _, selector := range observed.Selectors {
		if found := e.dialogs[selector]; found != nil {
			c = found
			break
		}
	}
	e.mu.Unlock()
	if c == nil {
		return false, nil
	}
	message := portable.Message{Wire: received.Wire, Transport: flow.Transport}
	request, ok := parsed.(*sip.Request)
	if !ok || request.To() == nil || request.From() == nil {
		return false, nil
	}
	key, err := sip.ServerTxKeyMake(request)
	if err != nil {
		return true, err
	}
	c.mu.Lock()
	if c.snapshot == nil || !e.residentContextCurrentLocked(c, c.snapshot.Context) {
		c.mu.Unlock()
		return false, nil
	}
	dialogKey := tag(request.To().Params) + "/" + tag(request.From().Params)
	dialog := c.dialogs[dialogKey]
	if dialog == nil || !validPolicyFlow(flow) || !sameProcess(flow.Ingress, e.cfg.Process) || (!proto.Equal(c.flow, flow) && (flow.Transport != "tls" || c.snapshot.Transport != "tls")) {
		c.mu.Unlock()
		return false, nil
	}
	if previous := c.transactions[key]; previous != nil {
		if previous.retired {
			matched := previous.wireHash == sha256.Sum256(received.Wire) && (proto.Equal(previous.flow, flow) || flow.Transport == "tls" && c.snapshot.Transport == "tls")
			c.mu.Unlock()
			if !matched {
				return true, rejectIngest(481, "BYE transaction mismatch")
			}
			return true, nil
		}
		if !portable.MatchRequest(previous.native.Origin, message) {
			c.mu.Unlock()
			return true, rejectIngest(481, "BYE transaction mismatch")
		}
		next, effects, stepErr := portable.StepTransaction(previous.native, portable.Input{Kind: portable.ReceiveRequest, Message: message, Now: time.Now().UnixMilli()})
		if stepErr != nil {
			c.mu.Unlock()
			return true, stepErr
		}
		effects = residentSuppressUncertainReplayLocked(c, previous, effects)
		delta, borrowed, stepErr := e.stageResidentTransactionBytesLocked(c, previous, next)
		if stepErr != nil {
			c.mu.Unlock()
			return true, stepErr
		}
		flowDelta := residentFlowBytes(flow) - residentFlowBytes(previous.flow)
		var flowBorrowed int64
		if flowDelta > 0 {
			flowBorrowed, stepErr = e.chargeResidentBudgetLocked(c, previous.budget, flowDelta)
		}
		if stepErr != nil {
			e.abortResidentTransactionBytesLocked(c, previous, delta, borrowed)
			c.mu.Unlock()
			return true, stepErr
		}
		candidate := *previous
		candidate.flow = flow
		writes, stepErr := e.stageResidentOutputsLocked(c, &candidate, effects, nil, "", time.Now().Add(2*time.Second))
		if stepErr != nil {
			e.abortResidentTransactionBytesLocked(c, previous, delta, borrowed)
			if flowDelta > 0 {
				_ = e.refundResidentBudgetLocked(c, previous.budget, flowDelta, flowBorrowed)
			}
			c.mu.Unlock()
			return true, stepErr
		}
		if flowDelta < 0 {
			returned := min(previous.borrowed, -flowDelta)
			_ = e.refundResidentBudgetLocked(c, previous.budget, -flowDelta, returned)
			previous.borrowed -= returned
		}
		previous.borrowed += flowBorrowed
		previous.charged += flowDelta
		previous.flow = proto.Clone(flow).(*pb.FlowRef)
		previous.emissions = candidate.emissions
		previous.provisionalEmissions = candidate.provisionalEmissions
		previous.native = next
		e.commitResidentTransactionBytesLocked(c, previous, delta, borrowed)
		_ = e.scheduleResidentTransactionLocked(c, previous, effects)
		c.mu.Unlock()
		e.dispatchResident(ctx, writes)
		return true, nil
	}
	nextDialog, dialogEffects, err := portable.StepDialog(dialog.native, portable.DialogInput{Kind: portable.DialogReceiveRequest, Message: message, Now: time.Now().UnixMilli()})
	if err != nil {
		c.mu.Unlock()
		return true, err
	}
	tx, effects, err := residentStartServer(message, flow, c.snapshot.Application, c.snapshot.LocalTag, "r-"+e.id("remote-bye", c.snapshot.Context.Id, string(message.Wire))[:48], time.Now().Add(2*time.Second), time.Now().UnixMilli())
	if err != nil {
		c.mu.Unlock()
		return true, err
	}
	for _, effect := range dialogEffects {
		if effect.Kind == portable.Respond && effect.Message != nil {
			var more []portable.Effect
			tx.native, more, err = portable.StepTransaction(tx.native, portable.Input{Kind: portable.SendResponse, Message: *effect.Message, Now: time.Now().UnixMilli()})
			effects = append(effects, more...)
			break
		}
	}
	if err != nil {
		c.mu.Unlock()
		return true, err
	}
	tx.budget = dialog.budget
	tx.selectors = append([]string(nil), observed.Selectors...)
	tx.charged = residentTransactionBytes(tx)
	tx.borrowed, err = e.chargeResidentBudgetLocked(c, tx.budget, tx.charged)
	if err != nil {
		c.mu.Unlock()
		return true, err
	}
	delta := residentDialogBytes(nextDialog) - residentDialogBytes(dialog.native)
	var borrowed int64
	if delta > 0 {
		borrowed, err = e.chargeResidentBudgetLocked(c, dialog.budget, delta)
	}
	if err != nil {
		_ = e.refundResidentBudgetLocked(c, tx.budget, tx.charged, tx.borrowed)
		c.mu.Unlock()
		return true, err
	}
	var event *residentEvent
	if dialogKey == c.selectedDialog {
		event, err = e.stageResidentEventLocked(c, pb.DialogEvent_ENDED, nil, "", time.Now().Add(2*time.Minute), dialog.budget)
	}
	if err != nil {
		if delta > 0 {
			_ = e.refundResidentBudgetLocked(c, dialog.budget, delta, borrowed)
		}
		_ = e.refundResidentBudgetLocked(c, tx.budget, tx.charged, tx.borrowed)
		c.mu.Unlock()
		return true, err
	}
	writes, err := e.stageResidentOutputsLocked(c, tx, effects, nil, "", time.Now().Add(2*time.Second))
	if err != nil {
		if event != nil {
			e.releaseResidentEventLocked(c, event)
		}
		if delta > 0 {
			_ = e.refundResidentBudgetLocked(c, dialog.budget, delta, borrowed)
		}
		_ = e.refundResidentBudgetLocked(c, tx.budget, tx.charged, tx.borrowed)
		c.mu.Unlock()
		return true, err
	}
	if delta < 0 {
		returned := min(dialog.borrowed, -delta)
		_ = e.refundResidentBudgetLocked(c, dialog.budget, -delta, returned)
		dialog.borrowed -= returned
	}
	dialog.borrowed += borrowed
	dialog.native = nextDialog
	c.transactions[key] = tx
	if event != nil {
		c.snapshot.Ended = true
	}
	c.revision++
	c.snapshot.Revision++
	if event != nil {
		event.message.Snapshot.Ended = true
		event.message.Snapshot.Revision = c.snapshot.Revision
	}
	e.mu.Lock()
	for _, selector := range observed.Selectors {
		if strings.HasPrefix(selector, "stx:") {
			e.transactions[selector] = c
		}
	}
	e.mu.Unlock()
	_ = e.scheduleResidentTransactionLocked(c, tx, effects)
	e.retireResidentTransactionLocked(c, tx)
	_ = e.scheduleResidentDialogLocked(c, dialogKey, dialogEffects)
	if event != nil {
		e.scheduleResidentContextRetirementLocked(c)
	}
	c.mu.Unlock()
	e.dispatchResident(ctx, writes)
	if event != nil {
		e.dispatchResidentEvents(ctx, []*residentEvent{event})
	}
	return true, nil
}
