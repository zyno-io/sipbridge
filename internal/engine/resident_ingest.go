package engine

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"time"

	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
	"google.golang.org/protobuf/proto"
	"sipbridge/internal/control"
	pb "sipbridge/internal/controlpb"
)

// ingest validates the exact admission before publishing the initial native
// transaction. Presentation happens after publication and outside state locks.
func (e *ResidentMemory) ingest(ctx context.Context, peer *control.Peer, in *pb.OwnerIngest, deadline time.Time) (*pb.OperationResult, error) {
	if in == nil || in.Context == nil || in.Flow == nil || !sameProcess(in.Application, peer.Application()) || !sameProcess(in.Flow.Ingress, e.cfg.Process) {
		return nil, errors.New("invalid ingest authority")
	}
	if err := e.validateRef(in.Context); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	now := time.Now()
	e.mu.Lock()
	c := e.contexts[in.Context.Id]
	admission, admitted := e.admissions[in.Context.Id]
	e.mu.Unlock()
	if c != nil {
		return e.ingestResidentExisting(ctx, c, in, deadline)
	}
	if !admitted || !proto.Equal(in.Context.State, e.contextRef(admission.callID, in.Context.Id).State) || admission.callID != in.Context.State.CallId || admission.wireHash != sha256.Sum256(in.Wire) || !now.Before(admission.expires) {
		return &pb.OperationResult{Certainty: pb.OperationResult_NOT_ACCEPTED, SipStatus: 481, Error: "native admission unavailable"}, nil
	}
	candidate, tx, effects, err := e.prepareResidentInitial(in, deadline, now)
	if err != nil {
		return nil, err
	}
	lease := peer.ApplicationLease()
	if lease == nil {
		return nil, control.ErrApplicationLeaseFenced
	}
	candidate.snapshot.ApplicationLeaseTerm = lease.Term
	candidate.snapshot.Executor = proto.Clone(e.cfg.Process).(*pb.ProcessRef)
	observed, err := Observe(in.Wire, false)
	if err != nil {
		return nil, err
	}
	candidate.snapshot.Selectors = append([]string(nil), observed.Selectors...)
	tx.selectors = append([]string(nil), observed.Selectors...)
	tx.charged = residentTransactionBytes(tx)
	candidate.mu.Lock()
	if err = e.accountResidentLocked(candidate, residentContextBytes(candidate)+tx.charged, 0); err != nil {
		candidate.mu.Unlock()
		return &pb.OperationResult{Certainty: pb.OperationResult_NOT_ACCEPTED, SipStatus: 503, Error: "resident capacity"}, nil
	}
	limit := residentSmallBudget
	requestMessage, parseErr := tx.native.Origin.Parse()
	if parseErr != nil {
		_ = e.accountResidentLocked(candidate, -candidate.bytes, 0)
		candidate.mu.Unlock()
		return nil, parseErr
	}
	request := requestMessage.(*sip.Request)
	if request.IsInvite() {
		limit = residentServerInviteBudget
	} else if request.Method == sip.SUBSCRIBE {
		limit = residentSubscriptionBudget
	}
	tx.budget, err = e.openResidentBudgetLocked(candidate, limit)
	if err != nil {
		_ = e.accountResidentLocked(candidate, -candidate.bytes, 0)
		candidate.mu.Unlock()
		return &pb.OperationResult{Certainty: pb.OperationResult_NOT_ACCEPTED, SipStatus: 503, Error: "resident capacity"}, nil
	}
	candidate.budget = tx.budget
	if request.Method == sip.SUBSCRIBE {
		candidate.subscriptionBudget = tx.budget
	}
	writes, err := e.stageResidentOutputsLocked(candidate, tx, effects, nil, "", deadline)
	if err != nil {
		_ = e.accountResidentLocked(candidate, -candidate.bytes, -candidate.reserved)
		candidate.mu.Unlock()
		return nil, err
	}
	e.mu.Lock()
	current, exists := e.admissions[in.Context.Id]
	liveLease := e.leases[residentControllerKey(in.Application)]
	valid := exists && current == admission && time.Now().Before(current.expires) && e.contexts[in.Context.Id] == nil && !e.closed && liveLease != nil && liveLease.term == lease.Term && time.Now().Before(liveLease.expires)
	if valid {
		delete(e.admissions, in.Context.Id)
		e.admissionCounts[current.callID]--
		if e.admissionCounts[current.callID] == 0 {
			delete(e.admissionCounts, current.callID)
		}
		if e.admissionByWire[current.wireHash] == in.Context.Id {
			delete(e.admissionByWire, current.wireHash)
		}
		e.deadlines.remove(residentDeadlineKey{kind: residentAdmissionDeadline, contextID: in.Context.Id})
		e.contexts[in.Context.Id] = candidate
		e.indexResidentOwnerUnderEngineLock(candidate)
		for _, selector := range observed.Selectors {
			e.transactions[selector] = candidate
		}
	}
	e.mu.Unlock()
	if !valid {
		_ = e.accountResidentLocked(candidate, -candidate.bytes, -candidate.reserved)
		candidate.mu.Unlock()
		return &pb.OperationResult{Certainty: pb.OperationResult_NOT_ACCEPTED, SipStatus: 481, Error: "native admission changed"}, nil
	}
	e.scheduleResidentOutputDeadlinesLocked(candidate, writes)
	if err = e.scheduleResidentTransactionLocked(candidate, tx, effects); err != nil {
		candidate.mu.Unlock()
		return nil, err
	}
	presentation := &pb.RequestPresented{Snapshot: proto.Clone(candidate.snapshot).(*pb.ContextSnapshot), RequestId: tx.requestID, Message: observed, Flow: proto.Clone(tx.flow).(*pb.FlowRef)}
	candidate.mu.Unlock()
	e.dispatchResident(ctx, writes)
	if err = e.presentResident(ctx, peer, candidate, tx.requestID, tx.presentDeadline, presentation); err != nil {
		return nil, err
	}
	return &pb.OperationResult{Certainty: pb.OperationResult_ACCEPTED, Snapshot: proto.Clone(presentation.Snapshot).(*pb.ContextSnapshot)}, nil
}

func (e *ResidentMemory) ingestResidentExisting(ctx context.Context, c *residentContext, in *pb.OwnerIngest, deadline time.Time) (*pb.OperationResult, error) {
	parsed, err := (portable.Message{Wire: in.Wire, Transport: in.Flow.Transport}).Parse()
	if err != nil {
		return nil, err
	}
	return e.ingestResidentExistingParsed(ctx, c, in, deadline, parsed, nil)
}

func (e *ResidentMemory) ingestResidentExistingParsed(ctx context.Context, c *residentContext, in *pb.OwnerIngest, deadline time.Time, parsed sip.Message, observed *pb.SipMessage) (*pb.OperationResult, error) {
	c.mu.Lock()
	if c.snapshot == nil || !proto.Equal(c.snapshot.Context.State, in.Context.State) || (!sameProcess(c.snapshot.Application, in.Application) && c.subscription == nil) || c.snapshot.Transport != in.Flow.Transport {
		c.mu.Unlock()
		return nil, errors.New("resident context authority mismatch")
	}
	message := portable.Message{Wire: in.Wire, Transport: in.Flow.Transport}
	request, ok := parsed.(*sip.Request)
	if ok {
		key, keyErr := sip.ServerTxKeyMake(request)
		tx := c.transactions[key]
		if keyErr == nil && tx != nil && tx.retired && tx.wireHash == sha256.Sum256(in.Wire) && residentReplayFlowAllowed(c, tx, in, request) {
			c.mu.Unlock()
			return &pb.OperationResult{Certainty: pb.OperationResult_COMPLETED}, nil
		}
		if keyErr == nil && tx != nil && !tx.retired && portable.MatchRequest(tx.native.Origin, message) && bytes.Equal(tx.native.Origin.Wire, in.Wire) && residentReplayFlowAllowed(c, tx, in, request) {
			if tx.decisionPending {
				c.mu.Unlock()
				return &pb.OperationResult{Certainty: pb.OperationResult_ACCEPTED}, nil
			}
			next, effects, err := portable.StepTransaction(tx.native, portable.Input{Kind: portable.ReceiveRequest, Now: time.Now().UnixMilli(), Message: message})
			if err != nil {
				c.mu.Unlock()
				return nil, err
			}
			effects = residentSuppressUncertainReplayLocked(c, tx, effects)
			delta, borrowed, err := e.stageResidentTransactionBytesLocked(c, tx, next)
			if err != nil {
				c.mu.Unlock()
				return nil, err
			}
			flowDelta := residentFlowBytes(in.Flow) - residentFlowBytes(tx.flow)
			var flowBorrowed int64
			if flowDelta > 0 {
				flowBorrowed, err = e.chargeResidentBudgetLocked(c, tx.budget, flowDelta)
			}
			if err != nil {
				e.abortResidentTransactionBytesLocked(c, tx, delta, borrowed)
				c.mu.Unlock()
				return nil, err
			}
			candidate := *tx
			candidate.flow = in.Flow
			writes, err := e.stageResidentOutputsLocked(c, &candidate, effects, nil, "", deadline)
			if err != nil {
				e.abortResidentTransactionBytesLocked(c, tx, delta, borrowed)
				if flowDelta > 0 {
					_ = e.refundResidentBudgetLocked(c, tx.budget, flowDelta, flowBorrowed)
				}
				c.mu.Unlock()
				return nil, err
			}
			if flowDelta < 0 {
				returned := min(tx.borrowed, -flowDelta)
				_ = e.refundResidentBudgetLocked(c, tx.budget, -flowDelta, returned)
				tx.borrowed -= returned
			}
			tx.borrowed += flowBorrowed
			tx.charged += flowDelta
			tx.flow = proto.Clone(in.Flow).(*pb.FlowRef)
			tx.emissions = candidate.emissions
			tx.provisionalEmissions = candidate.provisionalEmissions
			tx.native = next
			e.commitResidentTransactionBytesLocked(c, tx, delta, borrowed)
			_ = e.scheduleResidentTransactionLocked(c, tx, effects)
			c.mu.Unlock()
			e.dispatchResident(ctx, writes)
			return &pb.OperationResult{Certainty: pb.OperationResult_ACCEPTED}, nil
		}
	}
	c.mu.Unlock()
	if observed != nil {
		return e.receiveResidentExisting(ctx, c, in, deadline, observed, request)
	}
	return e.ingestResidentNewRequest(ctx, c, in, deadline)
}

// ingestResidentNewRequest admits a fresh subscription renewal only after the
// retained subscription identity and remote CSeq have been checked.
func (e *ResidentMemory) ingestResidentNewRequest(ctx context.Context, c *residentContext, in *pb.OwnerIngest, deadline time.Time) (*pb.OperationResult, error) {
	message := portable.Message{Wire: in.Wire, Transport: in.Flow.Transport}
	parsed, err := message.Parse()
	if err != nil {
		return nil, err
	}
	request, ok := parsed.(*sip.Request)
	if !ok || request.IsAck() || request.IsCancel() {
		return nil, rejectIngest(481, "unknown resident request")
	}
	observed, err := Observe(in.Wire, false)
	if err != nil {
		return nil, err
	}
	peer, err := e.control.Peer(in.Application)
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	if !e.residentContextCurrentLocked(c, in.Context) || c.snapshot.Ended || c.snapshot.Transport != in.Flow.Transport {
		c.mu.Unlock()
		return nil, rejectIngest(481, "resident context changed")
	}
	var dialog *residentDialog
	var dialogNext portable.Dialog
	var dialogEffects []portable.Effect
	var dialogKey string
	var adopted *pb.ContextDelivery
	if c.subscription != nil {
		if err = residentValidateSubscription(c, request, in); err != nil {
			c.mu.Unlock()
			return nil, err
		}
	} else {
		if !sameProcess(c.snapshot.Application, in.Application) || request.To() == nil || request.From() == nil {
			c.mu.Unlock()
			return nil, rejectIngest(481, "dialog owner mismatch")
		}
		dialogKey = tag(request.To().Params) + "/" + tag(request.From().Params)
		dialog = c.dialogs[dialogKey]
		if dialog == nil {
			c.mu.Unlock()
			return nil, rejectIngest(481, "dialog unavailable")
		}
		if !proto.Equal(c.flow, in.Flow) {
			valid := in.Admission == pb.OwnerIngest_TLS_DIALOG_CONTINUITY && c.snapshot.Transport == "tls" && in.Flow.Transport == "tls"
			if in.Admission == pb.OwnerIngest_AUTHENTICATED_BINDING && c.snapshot.PrincipalId != "" && c.snapshot.BindingId != "" && c.snapshot.PrincipalId == in.PrincipalId && c.snapshot.BindingId == in.BindingId {
				valid = true
			}
			if !valid {
				c.mu.Unlock()
				return nil, rejectIngest(481, "replacement flow admission required")
			}
		}
		dialogNext, dialogEffects, err = portable.StepDialog(dialog.native, portable.DialogInput{Kind: portable.DialogReceiveRequest, Message: message, Now: time.Now().UnixMilli(), Offer: request.IsInvite() && len(request.Body()) > 0})
		if err != nil {
			c.mu.Unlock()
			return nil, err
		}
		if !proto.Equal(c.flow, in.Flow) {
			revision := c.revision
			proposal := proto.Clone(c.snapshot).(*pb.ContextSnapshot)
			proposedFlow := proto.Clone(in.Flow).(*pb.FlowRef)
			application := proto.Clone(in.Application).(*pb.ProcessRef)
			c.mu.Unlock()
			adopted, err = e.adoptResidentFlow(ctx, proposal, proposedFlow, application, false)
			c.mu.Lock()
			if err != nil {
				c.mu.Unlock()
				return nil, err
			}
			e.mu.Lock()
			lease := e.leases[residentControllerKey(in.Application)]
			live := lease != nil && lease.term == c.snapshot.ApplicationLeaseTerm && time.Now().Before(lease.expires)
			e.mu.Unlock()
			if !live || c.ownerLost || c.revision != revision || c.dialogs[dialogKey] != dialog || !time.Now().Before(deadline) {
				c.mu.Unlock()
				return nil, rejectIngest(481, "dialog authority changed during flow adoption")
			}
		}
	}
	requestID := "r-" + e.id("resident-request", in.Context.State.Incarnation, in.Context.Id, string(in.Wire))[:48]
	until := time.Now().Add(2 * time.Second)
	if deadline.Before(until) {
		until = deadline
	}
	tx, effects, err := residentStartServer(message, in.Flow, in.Application, c.snapshot.LocalTag, requestID, until, time.Now().UnixMilli())
	if err != nil {
		c.mu.Unlock()
		return nil, err
	}
	if c.transactions[tx.native.Key] != nil {
		c.mu.Unlock()
		return nil, rejectIngest(481, "transaction key conflict")
	}
	var event *residentEvent
	nativeBye := dialog != nil && request.Method == sip.BYE
	if nativeBye {
		for _, effect := range dialogEffects {
			if effect.Kind == portable.Respond && effect.Message != nil {
				var replyEffects []portable.Effect
				tx.native, replyEffects, err = portable.StepTransaction(tx.native, portable.Input{Kind: portable.SendResponse, Message: *effect.Message, Now: time.Now().UnixMilli()})
				if err != nil {
					c.mu.Unlock()
					return nil, err
				}
				effects = append(effects, replyEffects...)
			}
		}
		if dialogKey == c.selectedDialog {
			event, err = e.stageResidentEventLocked(c, pb.DialogEvent_ENDED, nil, "", time.Now().Add(2*time.Minute), dialog.budget)
		}
		if err != nil {
			c.mu.Unlock()
			return nil, err
		}
	}
	tx.selectors = append([]string(nil), observed.Selectors...)
	tx.charged = residentTransactionBytes(tx)
	dialogDelta := int64(0)
	if dialog != nil {
		dialogDelta = residentDialogBytes(dialogNext) - residentDialogBytes(dialog.native)
		if adopted != nil {
			candidateSnapshot := proto.Clone(c.snapshot).(*pb.ContextSnapshot)
			candidateSnapshot.Delivery = adopted
			dialogDelta += residentContextBytes(&residentContext{snapshot: candidateSnapshot, flow: in.Flow}) - residentContextBytes(&residentContext{snapshot: c.snapshot, flow: c.flow})
		}
	}
	ownedBudget := request.Method != sip.BYE
	if ownedBudget {
		limit := residentSmallBudget
		if request.IsInvite() {
			limit = residentServerInviteBudget
		}
		tx.budget, err = e.openResidentBudgetLocked(c, limit)
	} else {
		tx.budget = dialog.budget
	}
	if err != nil {
		if event != nil {
			e.releaseResidentEventLocked(c, event)
		}
		c.mu.Unlock()
		return nil, err
	}
	tx.borrowed, err = e.chargeResidentBudgetLocked(c, tx.budget, tx.charged)
	if err != nil {
		if ownedBudget {
			e.closeResidentBudgetLocked(c, tx.budget)
		}
		if event != nil {
			e.releaseResidentEventLocked(c, event)
		}
		c.mu.Unlock()
		return nil, err
	}
	var dialogBorrowed int64
	if dialogDelta > 0 {
		dialogBorrowed, err = e.chargeResidentBudgetLocked(c, dialog.budget, dialogDelta)
	}
	if err != nil {
		_ = e.refundResidentBudgetLocked(c, tx.budget, tx.charged, tx.borrowed)
		if ownedBudget {
			e.closeResidentBudgetLocked(c, tx.budget)
		}
		if event != nil {
			e.releaseResidentEventLocked(c, event)
		}
		c.mu.Unlock()
		return nil, err
	}
	writes, err := e.stageResidentOutputsLocked(c, tx, effects, nil, "", deadline)
	if err != nil {
		if dialogDelta > 0 {
			_ = e.refundResidentBudgetLocked(c, dialog.budget, dialogDelta, dialogBorrowed)
		}
		_ = e.refundResidentBudgetLocked(c, tx.budget, tx.charged, tx.borrowed)
		if ownedBudget {
			e.closeResidentBudgetLocked(c, tx.budget)
		}
		if event != nil {
			e.releaseResidentEventLocked(c, event)
		}
		c.mu.Unlock()
		return nil, err
	}
	if dialogDelta < 0 {
		returned := min(dialog.borrowed, -dialogDelta)
		_ = e.refundResidentBudgetLocked(c, dialog.budget, -dialogDelta, returned)
		dialog.borrowed -= returned
	}
	if dialog != nil {
		dialog.borrowed += dialogBorrowed
	}
	c.transactions[tx.native.Key] = tx
	c.requests[requestID] = tx.native.Key
	if c.subscription != nil {
		c.subscription.Pending = tx.native.Key
	}
	if dialog != nil {
		if adopted != nil {
			c.snapshot.Delivery = adopted
			c.flow = proto.Clone(in.Flow).(*pb.FlowRef)
			if dialog.completionFlow != nil {
				_ = e.refundResidentBudgetLocked(c, dialog.budget, residentFlowBytes(dialog.completionFlow), dialog.completionBorrowed)
				dialog.completionFlow = nil
				dialog.completionBorrowed = 0
			}
		}
		dialog.native = dialogNext
		_ = e.scheduleResidentDialogLocked(c, dialogKey, dialogEffects)
		if event != nil {
			c.snapshot.Ended = true
			e.scheduleResidentContextRetirementLocked(c)
			event.message.Snapshot.Ended = true
		}
	}
	c.revision++
	c.snapshot.Revision++
	if event != nil {
		event.message.Snapshot.Revision = c.snapshot.Revision
	}
	e.mu.Lock()
	for _, selector := range observed.Selectors {
		e.transactions[selector] = c
	}
	e.mu.Unlock()
	_ = e.scheduleResidentTransactionLocked(c, tx, effects)
	if nativeBye {
		e.retireResidentTransactionLocked(c, tx)
	}
	presentation := &pb.RequestPresented{Snapshot: proto.Clone(c.snapshot).(*pb.ContextSnapshot), RequestId: requestID, Message: observed, Flow: proto.Clone(tx.flow).(*pb.FlowRef)}
	c.mu.Unlock()
	e.dispatchResident(ctx, writes)
	if nativeBye {
		if event != nil {
			e.dispatchResidentEvents(ctx, []*residentEvent{event})
		}
		return &pb.OperationResult{Certainty: pb.OperationResult_ACCEPTED, Snapshot: presentation.Snapshot}, nil
	}
	if err = e.presentResident(ctx, peer, c, requestID, until, presentation); err != nil {
		return nil, err
	}
	return &pb.OperationResult{Certainty: pb.OperationResult_ACCEPTED, Snapshot: presentation.Snapshot}, nil
}
