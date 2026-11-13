package engine

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"time"

	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
)

// locate selects only derived indexes. A fresh initial request receives a
// short-lived, bounded exact-wire admission that ingest must consume.
func (e *ResidentMemory) locate(ctx context.Context, message *pb.SipMessage) (*pb.ContextSnapshot, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if message == nil {
		return nil, errors.New("SIP message required")
	}
	parsed, err := parseObservedMessage(message.Wire)
	if err != nil {
		return nil, err
	}
	selectors := observedMessageSelectors(parsed, false)
	callID := parsed.CallID().Value()
	now := time.Now()
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return nil, errors.New("resident engine closed")
	}
	var selected *residentContext
	for _, key := range selectors {
		selected = e.transactions[key]
		if selected == nil {
			selected = e.dialogs[key]
		}
		if selected != nil {
			break
		}
	}
	if selected != nil {
		e.mu.Unlock()
		selected.mu.Lock()
		defer selected.mu.Unlock()
		if selected.snapshot == nil || !e.residentContextCurrentLocked(selected, selected.snapshot.Context) || selected.snapshot.Ended || selected.snapshot.CallId != callID {
			return nil, rejectIngest(481, "native context unavailable")
		}
		return proto.Clone(selected.snapshot).(*pb.ContextSnapshot), nil
	}
	request, ok := parsed.(*sip.Request)
	if !ok || request.To() == nil || tag(request.To().Params) != "" || request.IsAck() || request.IsCancel() {
		e.mu.Unlock()
		return nil, rejectIngest(481, "unknown native context")
	}
	hash := sha256.Sum256(message.Wire)
	if id := e.admissionByWire[hash]; id != "" {
		entry := e.admissions[id]
		if entry.callID == callID && now.Before(entry.expires) {
			e.mu.Unlock()
			return &pb.ContextSnapshot{Context: e.contextRef(callID, id)}, nil
		}
	}
	if len(e.contexts)+len(e.admissions) >= e.maxContexts || e.admissionCounts[callID] >= stateAdmissionLimit {
		e.mu.Unlock()
		return nil, rejectIngest(503, "native admission capacity")
	}
	if e.admissions == nil {
		e.admissions = make(map[string]residentAdmission)
		e.admissionByWire = make(map[[32]byte]string)
		e.admissionCounts = make(map[string]int)
	}
	e.admissionSerial++
	serial := fmt.Sprintf("%016x", e.admissionSerial)
	id := "in-" + serial + "-" + e.id("resident-inbound", e.authority.PrimaryId, serial)[:48]
	expires := now.Add(time.Duration(stateAdmissionMS) * time.Millisecond)
	e.admissions[id] = residentAdmission{callID: callID, wireHash: hash, expires: expires}
	e.admissionByWire[hash] = id
	e.admissionCounts[callID]++
	e.deadlines.set(residentDeadlineKey{kind: residentAdmissionDeadline, contextID: id}, e.admissionSerial, expires)
	e.wakeDeadline()
	e.mu.Unlock()
	return &pb.ContextSnapshot{Context: e.contextRef(callID, id)}, nil
}

func (e *ResidentMemory) fireResidentDeadline(item residentDeadline) {
	if e.fireResidentOutputDeadline(item) || e.fireResidentEventDeadline(item) || e.fireResidentSubscriptionDeadline(item) || e.fireResidentTransactionRetirement(item) || e.fireResidentOwnerCleanup(item) {
		return
	}
	if item.key.kind == residentContextRetirementDeadline {
		e.mu.Lock()
		c := e.contexts[item.key.contextID]
		e.mu.Unlock()
		if c != nil {
			c.mu.Lock()
			if c.snapshot.GetContext().GetState().GetIncarnation() == item.key.record && e.residentContextCurrentLocked(c, c.snapshot.Context) && c.snapshot.Ended && c.retireGeneration == item.generation && !time.Now().Before(item.at) {
				c.retireReady = true
				e.afterResidentOutputReleaseLocked(c)
			}
			c.mu.Unlock()
		}
		return
	}
	if item.key.kind == residentDialogDeadline {
		e.mu.Lock()
		c := e.contexts[item.key.contextID]
		e.mu.Unlock()
		if c == nil {
			return
		}
		c.mu.Lock()
		d := c.dialogs[item.key.record]
		if d == nil {
			c.mu.Unlock()
			return
		}
		due := d.native.Timers[item.key.timer]
		if !due.Active || due.Generation != item.generation || time.Now().UnixMilli() < due.At {
			c.mu.Unlock()
			return
		}
		tx := c.transactions[d.transaction]
		if tx == nil {
			c.mu.Unlock()
			return
		}
		next, effects, err := portable.StepDialog(d.native, portable.DialogInput{Kind: portable.DialogTimer, Timer: item.key.timer, Generation: item.generation, Now: time.Now().UnixMilli()})
		if err != nil {
			c.mu.Unlock()
			return
		}
		delta := residentDialogBytes(next) - residentDialogBytes(d.native)
		var borrowed int64
		if delta > 0 {
			borrowed, err = e.chargeResidentBudgetLocked(c, d.budget, delta)
		}
		if err != nil {
			c.mu.Unlock()
			return
		}
		needBye := false
		for _, effect := range effects {
			if effect.Kind == portable.NeedBYE {
				needBye = true
				break
			}
		}
		if needBye {
			previous := d.native
			d.native = next
			writes, cleanupErr := e.residentOwnerByeLocked(c, item.key.record, d, true)
			if cleanupErr != nil || len(writes) == 0 {
				d.native = previous
				if delta > 0 {
					_ = e.refundResidentBudgetLocked(c, d.budget, delta, borrowed)
				}
				c.mu.Unlock()
				return
			}
			if delta < 0 {
				returned := min(d.borrowed, -delta)
				_ = e.refundResidentBudgetLocked(c, d.budget, -delta, returned)
				d.borrowed -= returned
			}
			d.borrowed += borrowed
			_ = e.scheduleResidentDialogLocked(c, item.key.record, effects)
			c.mu.Unlock()
			e.dispatchResidentTimerOutputs(writes)
			return
		}
		var event *residentEvent
		for _, effect := range effects {
			if effect.Kind == portable.Ended {
				event, err = e.stageResidentEventLocked(c, pb.DialogEvent_ENDED, nil, "", time.Now().Add(2*time.Minute), d.budget)
				break
			}
		}
		if err != nil {
			if delta > 0 {
				_ = e.refundResidentBudgetLocked(c, d.budget, delta, borrowed)
			}
			c.mu.Unlock()
			return
		}
		effects = residentSuppressUncertainReplayLocked(c, tx, effects)
		originalBudget := tx.budget
		tx.budget = d.budget
		writes, err := e.stageResidentOutputsLocked(c, tx, effects, nil, "", time.Time{})
		tx.budget = originalBudget
		if err != nil {
			if event != nil {
				e.releaseResidentEventLocked(c, event)
			}
			if delta > 0 {
				_ = e.refundResidentBudgetLocked(c, d.budget, delta, borrowed)
			}
			c.mu.Unlock()
			return
		}
		if delta < 0 {
			returned := min(d.borrowed, -delta)
			_ = e.refundResidentBudgetLocked(c, d.budget, -delta, returned)
			d.borrowed -= returned
		}
		d.borrowed += borrowed
		d.native = next
		if event != nil {
			c.snapshot.Ended = true
			e.scheduleResidentContextRetirementLocked(c)
			c.revision++
			c.snapshot.Revision++
			event.message.Snapshot.Ended = true
			event.message.Snapshot.Revision = c.snapshot.Revision
		}
		_ = e.scheduleResidentDialogLocked(c, item.key.record, effects)
		e.finishResidentOwnerCleanupLocked(c)
		e.afterResidentOutputReleaseLocked(c)
		if event != nil {
			e.scheduleResidentEventsLocked(c, []*residentEvent{event})
		}
		c.mu.Unlock()
		e.dispatchResidentTimerOutputs(writes)
		return
	}
	if item.key.kind == residentTransactionDeadline {
		contextID, txKey, timer := item.key.contextID, item.key.record, item.key.timer
		e.mu.Lock()
		c := e.contexts[contextID]
		e.mu.Unlock()
		if c == nil {
			return
		}
		c.mu.Lock()
		tx := c.transactions[txKey]
		if tx == nil {
			c.mu.Unlock()
			return
		}
		due := tx.native.Timers[timer]
		if !due.Active || due.Generation != item.generation || time.Now().UnixMilli() < due.At {
			c.mu.Unlock()
			return
		}
		next, effects, err := portable.StepTransaction(tx.native, portable.Input{Kind: portable.TimerFired, Timer: timer, Generation: item.generation, Now: time.Now().UnixMilli()})
		if err != nil {
			c.mu.Unlock()
			return
		}
		effects = residentSuppressUncertainReplayLocked(c, tx, effects)
		delta, borrowed, err := e.stageResidentTransactionBytesLocked(c, tx, next)
		if err != nil {
			c.snapshot.Ended = true
			finishResidentCommandLocked(c, tx.command, &pb.OperationResult{Certainty: pb.OperationResult_UNKNOWN, Error: "native timer capacity unavailable"})
			c.mu.Unlock()
			return
		}
		var timeoutDialog *residentDialog
		var timeoutDialogKey string
		var timeoutNext portable.Dialog
		var timeoutEffects []portable.Effect
		var timeoutGrowth residentClientGrowth
		var timeoutEvent *residentEvent
		failure := ""
		for _, effect := range effects {
			if effect.Kind == portable.Ended && effect.Error != "" {
				failure = effect.Error
				break
			}
		}
		if failure != "" && next.Family == portable.NonInviteClient {
			origin, parseErr := next.Origin.Parse()
			if parseErr == nil && origin.CSeq().MethodName == sip.BYE && origin.From() != nil && origin.To() != nil {
				timeoutDialogKey = tag(origin.From().Params) + "/" + tag(origin.To().Params)
				timeoutDialog = c.dialogs[timeoutDialogKey]
				if timeoutDialog != nil && timeoutDialog.native.LocalBye != nil && portable.MatchRequest(*timeoutDialog.native.LocalBye, next.Origin) {
					timeoutNext, timeoutEffects, err = portable.StepDialog(timeoutDialog.native, portable.DialogInput{Kind: portable.DialogTerminate, Now: time.Now().UnixMilli()})
					if err == nil {
						timeoutGrowth, err = e.prepareResidentClientGrowth(c, timeoutDialog.budget, residentDialogBytes(timeoutNext)-residentDialogBytes(timeoutDialog.native))
					}
					if err == nil && timeoutDialogKey == c.selectedDialog {
						timeoutEvent, err = e.stageResidentEventLocked(c, pb.DialogEvent_ENDED, nil, "", time.Now().Add(2*time.Minute), timeoutDialog.budget)
					}
					if err != nil {
						e.rollbackResidentClientGrowth(c, timeoutGrowth)
						e.abortResidentTransactionBytesLocked(c, tx, delta, borrowed)
						c.mu.Unlock()
						return
					}
				} else {
					timeoutDialog = nil
				}
			}
		}
		timeoutInitialInvite := false
		if failure != "" && next.Family == portable.InviteClient && !c.snapshot.Answered && !c.snapshot.Ended && c.selectedDialog == "" {
			origin, parseErr := next.Origin.Parse()
			if parseErr == nil && origin.To() != nil && tag(origin.To().Params) == "" {
				timeoutInitialInvite = true
				timeoutEvent, err = e.stageResidentEventLocked(c, pb.DialogEvent_FAILED, nil, "", time.Now().Add(2*time.Minute), tx.budget)
				if err != nil {
					e.abortResidentTransactionBytesLocked(c, tx, delta, borrowed)
					c.mu.Unlock()
					return
				}
			}
		}
		writes, err := e.stageResidentOutputsLocked(c, tx, effects, nil, "", time.Time{})
		if err != nil {
			if timeoutEvent != nil {
				e.releaseResidentEventLocked(c, timeoutEvent)
			}
			if timeoutDialog != nil {
				e.rollbackResidentClientGrowth(c, timeoutGrowth)
			}
			e.abortResidentTransactionBytesLocked(c, tx, delta, borrowed)
			// Reserved completion capacity should make this unreachable. Fence
			// the affected procedure instead of polling on a failed obligation.
			c.snapshot.Ended = true
			finishResidentCommandLocked(c, tx.command, &pb.OperationResult{Certainty: pb.OperationResult_UNKNOWN, Error: "native timer output capacity unavailable"})
			c.mu.Unlock()
			return
		}
		tx.native = next
		e.commitResidentTransactionBytesLocked(c, tx, delta, borrowed)
		if timeoutDialog != nil {
			timeoutDialog.native = timeoutNext
			e.commitResidentClientGrowth(c, timeoutGrowth, &timeoutDialog.borrowed)
			_ = e.scheduleResidentDialogLocked(c, timeoutDialogKey, timeoutEffects)
			if timeoutEvent != nil {
				c.snapshot.Ended = true
				c.revision++
				c.snapshot.Revision++
				timeoutEvent.message.Snapshot.Ended = true
				timeoutEvent.message.Snapshot.Revision = c.snapshot.Revision
				e.scheduleResidentContextRetirementLocked(c)
				e.scheduleResidentEventsLocked(c, []*residentEvent{timeoutEvent})
			}
		}
		if timeoutInitialInvite {
			c.snapshot.Ended = true
			c.revision++
			c.snapshot.Revision++
			timeoutEvent.message.Snapshot.Ended = true
			timeoutEvent.message.Snapshot.Revision = c.snapshot.Revision
			e.scheduleResidentContextRetirementLocked(c)
			e.scheduleResidentEventsLocked(c, []*residentEvent{timeoutEvent})
		}
		if failure != "" && (next.Family == portable.NonInviteClient || timeoutInitialInvite) && tx.command != nil {
			finishResidentCommandLocked(c, tx.command, &pb.OperationResult{Certainty: pb.OperationResult_COMPLETED, Error: failure, Request: mustObserve(next.Origin.Wire, true)})
		}

		_ = e.scheduleResidentTransactionLocked(c, tx, effects)
		e.retireResidentTransactionLocked(c, tx)
		e.finishResidentOwnerCleanupLocked(c)
		e.afterResidentOutputReleaseLocked(c)
		c.mu.Unlock()
		e.dispatchResidentTimerOutputs(writes)
		return
	}
	if item.key.kind == residentAdmissionDeadline {
		id := item.key.contextID
		e.mu.Lock()
		admission, ok := e.admissions[id]
		if ok && !time.Now().Before(admission.expires) {
			delete(e.admissions, id)
			if e.admissionByWire[admission.wireHash] == id {
				delete(e.admissionByWire, admission.wireHash)
			}
			e.admissionCounts[admission.callID]--
			if e.admissionCounts[admission.callID] == 0 {
				delete(e.admissionCounts, admission.callID)
			}
		}
		e.mu.Unlock()
	}
}
