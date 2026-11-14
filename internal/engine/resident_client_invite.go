package engine

import (
	"errors"
	"strings"
	"time"
	"unsafe"

	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
)

func residentDialogBytes(dialog portable.Dialog) int64 {
	size := int64(unsafe.Sizeof(residentDialog{})) + int64(len(dialog.CallID)+len(dialog.LocalTag)+len(dialog.RemoteTag)+len(dialog.LocalURI)+len(dialog.RemoteURI)+len(dialog.Contact)+len(dialog.SentBy)+len(dialog.Target)+len(dialog.Transport))
	for _, route := range dialog.Routes {
		size += int64(len(route)) + 16
	}
	for _, invite := range []*portable.InviteProcedure{dialog.LocalInvite, dialog.RemoteInvite} {
		if invite == nil {
			continue
		}
		size += int64(unsafe.Sizeof(*invite)) + residentMessageBytes(invite.Request)
		for _, message := range []*portable.Message{invite.Final, invite.ACK} {
			if message != nil {
				size += int64(unsafe.Sizeof(*message)) + residentMessageBytes(*message)
			}
		}
	}
	if dialog.LocalBye != nil {
		size += int64(unsafe.Sizeof(*dialog.LocalBye)) + residentMessageBytes(*dialog.LocalBye)
	}
	return size + int64(len(dialog.Timers))*128 + 256
}

// residentInviteResponseLocked stages a fork and its native ACK in the same
// publication. Losing branches defer their cleanup BYE until ACK is WRITTEN.
func (e *ResidentMemory) residentInviteResponseLocked(c *residentContext, tx *residentTransaction, message portable.Message, observed *pb.SipMessage, response *sip.Response) ([]*pb.FlowWrite, *residentEvent, error) {
	now := time.Now()
	nextTx, txEffects, err := portable.StepTransaction(tx.native, portable.Input{Kind: portable.ReceiveResponse, Now: now.UnixMilli(), Message: message})
	if err != nil {
		return nil, nil, err
	}
	origin, err := tx.native.Origin.Facts()
	if err != nil {
		return nil, nil, err
	}
	if origin.ToTag != "" {
		return e.residentReinviteResponseLocked(c, tx, nextTx, txEffects, message, observed, response, now)
	}
	key := tag(response.From().Params) + "/" + tag(response.To().Params)
	var dialog portable.Dialog
	var previous *residentDialog
	var dialogDelta int64
	var ackEffects, dialogEffects []portable.Effect
	winning := c.selectedDialog == key
	selected := c.selectedDialog
	if c.snapshot.Revision >= 1<<53-1 {
		return nil, nil, errors.New("observation revision exhausted")
	}
	snapshot := proto.Clone(c.snapshot).(*pb.ContextSnapshot)
	snapshot.Revision++
	kind := pb.DialogEvent_PROVISIONAL
	if response.StatusCode >= 300 {
		if selected == "" && !snapshot.Ended {
			snapshot.Ended = true
			kind = pb.DialogEvent_FAILED
		} else {
			kind = pb.DialogEvent_UNSPECIFIED
		}
	} else if response.StatusCode > 100 && tag(response.To().Params) != "" && response.Contact() != nil {
		previous = c.dialogs[key]
		if previous == nil {
			if len(c.dialogs) >= 16 {
				return nil, nil, errors.New("native dialog branch capacity")
			}
			dialog, err = portable.NewDialog(portable.UAC, tx.native.Origin, message, portable.DefaultTimers())
		} else {
			dialog = previous.native
		}
		if err != nil {
			return nil, nil, err
		}
		if response.IsSuccess() {
			if selected == "" && !c.cancelled && !snapshot.Ended {
				selected, winning = key, true
			}
			var effects []portable.Effect
			if previous == nil {
				dialog, effects, err = portable.StepDialog(dialog, portable.DialogInput{Kind: portable.DialogStart, Now: now.UnixMilli()})
			} else {
				dialog, effects, err = portable.StepDialog(dialog, portable.DialogInput{Kind: portable.DialogReceiveResponse, Now: now.UnixMilli(), Message: message})
			}
			if err != nil {
				return nil, nil, err
			}
			dialogEffects = append(dialogEffects, effects...)
			if (!winning || c.cancelled || snapshot.Ended) && dialog.LocalInvite != nil && !dialog.LocalInvite.CancelRequested {
				dialog, effects, err = portable.StepDialog(dialog, portable.DialogInput{Kind: portable.DialogCancel, Now: now.UnixMilli()})
				if err != nil {
					return nil, nil, err
				}
			}
			dialogEffects = append(dialogEffects, effects...)
			for _, effect := range effects {
				if effect.Kind == portable.Send {
					ackEffects = append(ackEffects, effect)
				}
				if effect.Kind == portable.NeedACK && dialog.Offer != portable.RemoteOfferInACK {
					dialog, ackEffects, err = portable.StepDialog(dialog, portable.DialogInput{Kind: portable.DialogLocalACK, Now: now.UnixMilli(), Branch: sip.RFC3261BranchMagicCookie + e.id("ack", c.snapshot.Context.State.Incarnation, key, tx.command.operation)[:32], Answer: true})
					if err != nil {
						return nil, nil, err
					}
				}
			}
			if winning {
				if snapshot.Answered {
					kind = pb.DialogEvent_UNSPECIFIED
				} else {
					kind = pb.DialogEvent_ANSWERED
				}
				snapshot.Answered, snapshot.DialogId = true, sip.DialogIDMake(dialog.CallID, dialog.LocalTag, dialog.RemoteTag)
				snapshot.LocalTag, snapshot.RemoteTag, snapshot.RemoteTarget = dialog.LocalTag, dialog.RemoteTag, dialog.Target
				snapshot.RemoteSdp = append([]byte(nil), response.Body()...)
			} else {
				kind = pb.DialogEvent_UNSPECIFIED
			}
		}
		dialogDelta = residentDialogBytes(dialog) + int64(2*len(tx.native.Key))
		if previous != nil {
			dialogDelta -= residentDialogBytes(previous.native) + int64(len(previous.transaction)+len(previous.localTransaction))
		}
	} else if response.IsSuccess() {
		return nil, nil, errors.New("successful INVITE requires remote tag and Contact")
	}
	contextDelta := residentContextBytes(&residentContext{snapshot: snapshot, flow: c.flow}) - residentContextBytes(&residentContext{snapshot: c.snapshot, flow: c.flow})
	txDelta := residentClientTransactionDelta(tx, nextTx)
	dialogBudget := tx.budget
	var dialogBorrowed int64
	if previous != nil {
		dialogBudget, dialogBorrowed = previous.budget, previous.borrowed
	}
	txGrowth, err := e.prepareResidentClientGrowth(c, tx.budget, txDelta)
	if err != nil {
		return nil, nil, err
	}
	contextGrowth, err := e.prepareResidentClientGrowth(c, c.budget, contextDelta)
	if err != nil {
		e.rollbackResidentClientGrowth(c, txGrowth)
		return nil, nil, err
	}
	dialogGrowth, err := e.prepareResidentClientGrowth(c, dialogBudget, dialogDelta)
	if err != nil {
		e.rollbackResidentClientGrowth(c, contextGrowth)
		e.rollbackResidentClientGrowth(c, txGrowth)
		return nil, nil, err
	}
	rollback := func() {
		e.rollbackResidentClientGrowth(c, dialogGrowth)
		e.rollbackResidentClientGrowth(c, contextGrowth)
		e.rollbackResidentClientGrowth(c, txGrowth)
	}
	oldSnapshot := c.snapshot
	c.snapshot = snapshot
	var event *residentEvent
	if kind != pb.DialogEvent_UNSPECIFIED {
		event, err = e.stageResidentEventNativeLocked(c, kind, observed, message, tx.command.operation, now.Add(2*time.Minute), tx.budget)
		if err != nil {
			c.snapshot = oldSnapshot
			rollback()
			return nil, nil, err
		}
	}
	effects := append(txEffects, ackEffects...)
	pairedCancel := false
	for _, effect := range txEffects {
		if effect.Kind == portable.StartTransaction {
			pairedCancel = true
		}
	}
	var writes []*pb.FlowWrite
	if pairedCancel {
		if len(ackEffects) != 0 {
			err = errors.New("mixed ACK and paired CANCEL")
		} else {
			writes, err = e.residentPairedCancelLocked(c, tx, txEffects, now.Add(2*time.Second))
		}
	} else {
		writes, err = e.stageResidentOutputsLocked(c, tx, effects, nil, tx.command.operation, now.Add(2*time.Second))
	}
	if err != nil {
		if event != nil {
			e.releaseResidentEventLocked(c, event)
		}
		c.snapshot = oldSnapshot
		rollback()
		return nil, nil, err
	}
	tx.native = nextTx
	c.revision++
	tx.charged += txDelta
	e.commitResidentClientGrowth(c, txGrowth, &tx.borrowed)
	e.commitResidentClientGrowth(c, contextGrowth, &c.borrowed)
	e.commitResidentClientGrowth(c, dialogGrowth, &dialogBorrowed)
	if snapshot.Ended && !oldSnapshot.Ended {
		e.scheduleResidentContextRetirementLocked(c)
	}
	c.selectedDialog = selected
	if dialog.Version != 0 {
		record := &residentDialog{}
		if previous != nil {
			*record = *previous
		}
		record.native, record.transaction, record.localTransaction = dialog, tx.native.Key, tx.native.Key
		record.budget, record.borrowed = dialogBudget, dialogBorrowed
		c.dialogs[key] = record
		e.mu.Lock()
		for _, selector := range observed.Selectors {
			if strings.HasPrefix(selector, "dlg:") {
				e.dialogs[selector] = c
			}
		}
		e.mu.Unlock()
		_ = e.scheduleResidentDialogLocked(c, key, dialogEffects)
	}
	for _, write := range writes {
		if output := c.outputs[write.Effect.Id]; output != nil && output.method == "ACK" && response.IsSuccess() && dialog.Version != 0 {
			output.dialog = key
			output.cleanupAfterACK = !winning || c.cancelled || snapshot.Ended
		}
	}
	_ = e.scheduleResidentTransactionLocked(c, tx, txEffects)
	return writes, event, nil
}

// residentClientOutputCompletedLocked continues a losing fork only after the
// exact ACK was positively written. Unknown and not-written outcomes do not
// authorize a cleanup attempt.
func (e *ResidentMemory) residentClientOutputCompletedLocked(c *residentContext, output *residentOutput, result *pb.DeliveryResult) ([]*pb.FlowWrite, bool, error) {
	if output.method == "INVITE" && !output.replay && result.Outcome == pb.DeliveryResult_NOT_WRITTEN {
		tx := c.transactions[output.transaction]
		if tx != nil && tx.command != nil && tx.native.Family == portable.InviteClient && tx.native.LastResponse == nil {
			origin, err := tx.native.Origin.Facts()
			if err != nil {
				return nil, true, err
			}
			if origin.ToTag != "" {
				return nil, false, nil
			}
			next, effects, err := portable.StepTransaction(tx.native, portable.Input{Kind: portable.TransportFailure, Now: time.Now().UnixMilli(), Error: "SIP write did not start"})
			if err != nil {
				return nil, true, err
			}
			for _, effect := range effects {
				if effect.Kind == portable.Send || effect.Kind == portable.Respond {
					return nil, true, errors.New("unexpected transport failure output")
				}
			}
			delta := residentClientTransactionDelta(tx, next)
			growth, err := e.prepareResidentClientGrowth(c, tx.budget, delta)
			if err != nil {
				return nil, true, err
			}
			oldSnapshot := c.snapshot
			if oldSnapshot.Revision >= 1<<53-1 {
				e.rollbackResidentClientGrowth(c, growth)
				return nil, true, errors.New("observation revision exhausted")
			}
			c.snapshot = proto.Clone(c.snapshot).(*pb.ContextSnapshot)
			c.snapshot.Ended = true
			c.snapshot.Revision++
			event, err := e.stageResidentEventLocked(c, pb.DialogEvent_FAILED, nil, tx.command.operation, time.Now().Add(2*time.Minute), tx.budget)
			if err != nil {
				c.snapshot = oldSnapshot
				e.rollbackResidentClientGrowth(c, growth)
				return nil, true, err
			}
			tx.native = next
			c.revision++
			tx.charged += delta
			e.commitResidentClientGrowth(c, growth, &tx.borrowed)
			_ = e.scheduleResidentTransactionLocked(c, tx, effects)
			e.scheduleResidentContextRetirementLocked(c)
			e.scheduleResidentEventsLocked(c, []*residentEvent{event})
			finishResidentCommandLocked(c, tx.command, &pb.OperationResult{Certainty: pb.OperationResult_COMPLETED, Error: "SIP write did not start"})
			return nil, true, nil
		}
	}
	if output.method != "ACK" || output.dialog == "" {
		return nil, false, nil
	}
	dialog := c.dialogs[output.dialog]
	if dialog == nil {
		return nil, true, errors.New("ACK dialog unavailable")
	}
	if dialog.native.LocalInvite == nil || output.ackSequence != dialog.native.LocalInvite.Sequence {
		return nil, true, nil
	}
	if result.Outcome != pb.DeliveryResult_WRITTEN {
		if tx := c.transactions[output.transaction]; output.command != nil && tx != nil && output.command != tx.command {
			finishResidentCommandLocked(c, output.command, &pb.OperationResult{Certainty: pb.OperationResult_UNKNOWN, Error: "ACK delivery unavailable or uncertain"})
		}
		return nil, true, nil
	}
	if err := e.replaceResidentCompletionFlowLocked(c, dialog, output.write.Flow); err != nil {
		return nil, true, err
	}
	dialog.ackWritten = true
	if output.dialog == c.selectedDialog && c.snapshot.Kind == pb.ContextSnapshot_UAC && dialog.native.LocalInvite.Initial && !c.snapshot.InitialAck {
		if c.snapshot.Revision >= 1<<53-1 {
			return nil, true, errors.New("observation revision exhausted")
		}
		c.snapshot.InitialAck = true
		c.snapshot.Revision++
		c.revision++
	}
	if output.dialog == c.selectedDialog && c.snapshot.Kind == pb.ContextSnapshot_UAC && dialog.native.LocalInvite.Initial && c.snapshot.Answered && !c.snapshot.Ended && !c.cancelled && !c.ownerLost && dialog.native.LocalInvite.Final != nil {
		if tx := c.transactions[output.transaction]; tx != nil && tx.command != nil && tx.native.Family == portable.InviteClient {
			finishResidentCommandMessageLocked(c, tx.command, pb.OperationResult_COMPLETED, 0, "", tx.native.Origin, *dialog.native.LocalInvite.Final)
		}
	}
	if tx := c.transactions[output.transaction]; output.command != nil && tx != nil && output.command != tx.command {
		var request portable.Message
		if dialog.native.LocalInvite.ACK != nil {
			request = *dialog.native.LocalInvite.ACK
		}
		finishResidentCommandMessageLocked(c, output.command, pb.OperationResult_COMPLETED, 0, "", request, portable.Message{})
	}
	if !output.cleanupAfterACK || dialog.native.Phase == portable.DialogEnded || dialog.native.LocalBye != nil {
		return nil, true, nil
	}
	now := time.Now()
	next, effects, err := portable.StepDialog(dialog.native, portable.DialogInput{Kind: portable.DialogLocalRequest, Now: now.UnixMilli(), Method: sip.BYE,
		Branch: sip.RFC3261BranchMagicCookie + e.id("cleanup-bye", c.snapshot.Context.State.Incarnation, output.dialog)[:32]})
	if err != nil {
		return nil, true, err
	}
	var origin *portable.Message
	for _, effect := range effects {
		if effect.Kind == portable.StartTransaction {
			origin = effect.Message
		}
	}
	if origin == nil {
		return nil, true, errors.New("cleanup BYE unavailable")
	}
	native, err := portable.NewTransaction(portable.NonInviteClient, *origin, portable.DefaultTimers(), "")
	if err != nil {
		return nil, true, err
	}
	started, effects, err := portable.StepTransaction(native, portable.Input{Kind: portable.Start, Now: now.UnixMilli()})
	if err != nil {
		return nil, true, err
	}
	selectors, err := nativeMessageSelectors(*origin, true)
	if err != nil {
		return nil, true, err
	}
	tx := &residentTransaction{native: started, flow: proto.Clone(output.write.Flow).(*pb.FlowRef), application: proto.Clone(c.snapshot.Application).(*pb.ProcessRef), cleanup: true, budget: dialog.budget, selectors: append([]string(nil), selectors...)}
	tx.charged = residentTransactionBytes(tx)
	delta := residentDialogBytes(next) - residentDialogBytes(dialog.native)
	tx.borrowed, err = e.chargeResidentBudgetLocked(c, tx.budget, tx.charged)
	if err != nil {
		return nil, true, err
	}
	growth, err := e.prepareResidentClientGrowth(c, dialog.budget, delta)
	if err != nil {
		_ = e.refundResidentBudgetLocked(c, tx.budget, tx.charged, tx.borrowed)
		return nil, true, err
	}
	writes, err := e.stageResidentOutputsLocked(c, tx, effects, nil, "", now.Add(2*time.Second))
	if err != nil {
		e.rollbackResidentClientGrowth(c, growth)
		_ = e.refundResidentBudgetLocked(c, tx.budget, tx.charged, tx.borrowed)
		return nil, true, err
	}
	dialog.native = next
	e.commitResidentClientGrowth(c, growth, &dialog.borrowed)
	c.transactions[tx.native.Key] = tx
	e.mu.Lock()
	for _, selector := range tx.selectors {
		e.transactions[selector] = c
	}
	e.mu.Unlock()
	_ = e.scheduleResidentTransactionLocked(c, tx, effects)
	return writes, true, nil
}

func (e *ResidentMemory) residentReinviteResponseLocked(c *residentContext, tx *residentTransaction, nextTx portable.Transaction, txEffects []portable.Effect, message portable.Message, observed *pb.SipMessage, response *sip.Response, now time.Time) ([]*pb.FlowWrite, *residentEvent, error) {
	key := tag(response.From().Params) + "/" + tag(response.To().Params)
	current := c.dialogs[key]
	if current == nil || current.localTransaction != tx.native.Key {
		return nil, nil, errors.New("response dialog transaction unavailable")
	}
	dialog, dialogEffects, err := portable.StepDialog(current.native, portable.DialogInput{Kind: portable.DialogReceiveResponse, Now: now.UnixMilli(), Message: message})
	if err != nil {
		return nil, nil, err
	}
	var ackEffects []portable.Effect
	for _, effect := range dialogEffects {
		if effect.Kind == portable.Send {
			ackEffects = append(ackEffects, effect)
		}
		if effect.Kind == portable.NeedACK && dialog.Offer != portable.RemoteOfferInACK {
			dialog, ackEffects, err = portable.StepDialog(dialog, portable.DialogInput{Kind: portable.DialogLocalACK, Now: now.UnixMilli(),
				Branch: sip.RFC3261BranchMagicCookie + e.id("ack", c.snapshot.Context.State.Incarnation, key, tx.command.operation)[:32], Answer: true})
			if err != nil {
				return nil, nil, err
			}
		}
	}
	if c.snapshot.Revision >= 1<<53-1 {
		return nil, nil, errors.New("observation revision exhausted")
	}
	snapshot := proto.Clone(c.snapshot).(*pb.ContextSnapshot)
	snapshot.Revision++
	if response.IsSuccess() && key == c.selectedDialog {
		body, parseErr := tx.native.Origin.Body()
		if parseErr != nil {
			return nil, nil, parseErr
		}
		snapshot.LocalSdp = body
		snapshot.RemoteSdp = append([]byte(nil), response.Body()...)
		snapshot.RemoteTarget = dialog.Target
	}
	txDelta := residentClientTransactionDelta(tx, nextTx)
	dialogDelta := residentDialogBytes(dialog) - residentDialogBytes(current.native)
	contextDelta := residentContextBytes(&residentContext{snapshot: snapshot, flow: c.flow}) - residentContextBytes(&residentContext{snapshot: c.snapshot, flow: c.flow})
	txGrowth, err := e.prepareResidentClientGrowth(c, tx.budget, txDelta)
	if err != nil {
		return nil, nil, err
	}
	dialogGrowth, err := e.prepareResidentClientGrowth(c, current.budget, dialogDelta)
	if err != nil {
		e.rollbackResidentClientGrowth(c, txGrowth)
		return nil, nil, err
	}
	contextGrowth, err := e.prepareResidentClientGrowth(c, c.budget, contextDelta)
	if err != nil {
		e.rollbackResidentClientGrowth(c, dialogGrowth)
		e.rollbackResidentClientGrowth(c, txGrowth)
		return nil, nil, err
	}
	effects := append(txEffects, ackEffects...)
	writes, err := e.stageResidentOutputsLocked(c, tx, effects, nil, tx.command.operation, now.Add(2*time.Second))
	if err != nil {
		e.rollbackResidentClientGrowth(c, contextGrowth)
		e.rollbackResidentClientGrowth(c, dialogGrowth)
		e.rollbackResidentClientGrowth(c, txGrowth)
		return nil, nil, err
	}
	tx.native, current.native, c.snapshot = nextTx, dialog, snapshot
	c.revision++
	tx.charged += txDelta
	e.commitResidentClientGrowth(c, txGrowth, &tx.borrowed)
	e.commitResidentClientGrowth(c, dialogGrowth, &current.borrowed)
	e.commitResidentClientGrowth(c, contextGrowth, &c.borrowed)
	for _, write := range writes {
		if output := c.outputs[write.Effect.Id]; output != nil && output.method == "ACK" && response.IsSuccess() {
			output.dialog = key
			output.cleanupAfterACK = c.cancelled
		}
	}
	_ = e.scheduleResidentTransactionLocked(c, tx, txEffects)
	_ = e.scheduleResidentDialogLocked(c, key, append(dialogEffects, ackEffects...))
	if response.StatusCode >= 200 {
		finishResidentCommandMessageLocked(c, tx.command, pb.OperationResult_COMPLETED, uint32(response.StatusCode), "", tx.native.Origin, message)
	}
	return writes, nil, nil
}
