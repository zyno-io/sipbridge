package engine

import (
	"context"
	"crypto/sha256"
	"errors"
	"time"

	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
	"sipbridge/internal/control"
	pb "sipbridge/internal/controlpb"
)

// ReceiveRequest is the Memory ingress boundary after local policy and native
// completion. A business request is notified once; only a decision accepts it.
func (e *ResidentMemory) ReceiveRequest(ctx context.Context, peer *control.Peer, flow *pb.FlowRef, message *pb.SipMessage) error {
	return e.ReceiveRequestParsed(ctx, peer, flow, message, nil)
}

// ReceiveRequestParsed consumes the owned projection and parse from the same
// framing operation. The parse is transient and is never stored in native state.
func (e *ResidentMemory) ReceiveRequestParsed(ctx context.Context, peer *control.Peer, flow *pb.FlowRef, message *pb.SipMessage, parsed sip.Message) (receiveErr error) {
	defer func() {
		var protocol *portable.ProtocolError
		if errors.As(receiveErr, &protocol) {
			receiveErr = rejectIngest(uint32(protocol.Status), protocol.Reason)
		}
	}()
	if peer == nil || !peer.HasCapability(control.RequestDecisionCapability) || flow == nil || message == nil || !validPolicyFlow(flow) || !sameProcess(flow.Ingress, e.cfg.Process) {
		return errors.New("direct request authority required")
	}
	if parsed == nil {
		var err error
		parsed, err = parseObservedMessage(message.Wire)
		if err != nil {
			return err
		}
		message = projectObservedMessage(message.Wire, parsed, false)
	}
	request, ok := parsed.(*sip.Request)
	if !ok {
		return errors.New("direct request requires a SIP request")
	}
	route, err := e.locateResidentParsed(ctx, message.Wire, parsed, message.Selectors, true)
	if err != nil {
		return err
	}
	if route.Application != nil {
		selected, selectErr := e.control.Peer(route.Application)
		if selectErr != nil && route.Kind != pb.ContextSnapshot_SUBSCRIPTION {
			return selectErr
		}
		if selectErr == nil {
			peer = selected
		}
	}
	if err = e.validateController(ctx, peer); err != nil {
		return err
	}
	deadline := deadlineOr(ctx, time.Now().Add(2*time.Second))
	result, err := e.receiveResidentInitial(ctx, peer, &pb.OwnerIngest{Context: route.Context, Application: peer.Application(), Flow: flow, Wire: message.Wire}, deadline, message, request)
	if err != nil {
		return err
	}
	if result.Certainty == pb.OperationResult_NOT_ACCEPTED {
		return rejectIngest(result.SipStatus, result.Error)
	}
	return nil
}

func residentWithoutSends(effects []portable.Effect) []portable.Effect {
	selected := make([]portable.Effect, 0, len(effects))
	for _, effect := range effects {
		if effect.Kind != portable.Send && effect.Kind != portable.Respond {
			selected = append(selected, effect)
		}
	}
	return selected
}

func (e *ResidentMemory) notifyResidentRequest(ctx context.Context, peer *control.Peer, c *residentContext, tx *residentTransaction, request *pb.RequestPresented) error {
	if err := ctx.Err(); err != nil {
		return e.failResidentPresentation(c, tx.requestID)
	}
	remaining := time.Until(tx.presentDeadline).Milliseconds()
	if remaining <= 0 {
		return e.failResidentPresentation(c, tx.requestID)
	}
	if err := peer.Send(&pb.Envelope{CorrelationId: uuid.NewString(), RemainingBudgetMs: uint32(min(2000, remaining)), Payload: &pb.Envelope_Request{Request: request}}); err != nil {
		return e.failResidentPresentation(c, tx.requestID)
	}
	return nil
}

func (e *ResidentMemory) scheduleResidentDecisionLocked(c *residentContext, tx *residentTransaction) {
	e.mu.Lock()
	e.deadlines.set(residentDeadlineKey{kind: residentRequestDecisionDeadline, contextID: c.snapshot.Context.Id, record: tx.native.Key}, uint64(tx.presentDeadline.UnixMilli()), tx.presentDeadline)
	e.mu.Unlock()
	e.wakeDeadline()
}

func (e *ResidentMemory) clearResidentDecisionLocked(c *residentContext, tx *residentTransaction) {
	before := residentTransactionBytes(tx)
	tx.decisionPending = false
	tx.decisionProposal = nil
	tx.requestDialog = ""
	delta := residentTransactionBytes(tx) - before
	if delta < 0 {
		returned := min(tx.borrowed, -delta)
		_ = e.refundResidentBudgetLocked(c, tx.budget, -delta, returned)
		tx.borrowed -= returned
		tx.charged += delta
	}
	e.mu.Lock()
	e.deadlines.remove(residentDeadlineKey{kind: residentRequestDecisionDeadline, contextID: c.snapshot.Context.Id, record: tx.native.Key})
	e.mu.Unlock()
}

func (e *ResidentMemory) fireResidentDecisionDeadline(item residentDeadline) bool {
	if item.key.kind != residentRequestDecisionDeadline {
		return false
	}
	e.mu.Lock()
	c := e.contexts[item.key.contextID]
	e.mu.Unlock()
	if c == nil {
		return true
	}
	c.mu.Lock()
	tx := c.transactions[item.key.record]
	valid := tx != nil && tx.decisionPending && uint64(tx.presentDeadline.UnixMilli()) == item.generation && !time.Now().Before(tx.presentDeadline)
	c.mu.Unlock()
	if valid {
		_ = e.failResidentPresentation(c, tx.requestID)
	}
	return true
}

func (e *ResidentMemory) receiveResidentInitial(ctx context.Context, peer *control.Peer, in *pb.OwnerIngest, deadline time.Time, observed *pb.SipMessage, request *sip.Request) (*pb.OperationResult, error) {
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
		return e.ingestResidentExistingParsed(ctx, c, in, deadline, request, observed)
	}
	if !admitted || !proto.Equal(in.Context.State, e.contextRef(admission.callID, in.Context.Id).State) || admission.callID != in.Context.State.CallId || admission.wireHash != sha256.Sum256(in.Wire) || !now.Before(admission.expires) {
		return &pb.OperationResult{Certainty: pb.OperationResult_NOT_ACCEPTED, SipStatus: 481, Error: "native admission unavailable"}, nil
	}
	candidate, tx, effects, err := e.prepareResidentInitialParsed(in, deadline, now, request)
	if err != nil {
		return nil, err
	}
	lease := peer.ApplicationLease()
	if lease == nil {
		return nil, control.ErrApplicationLeaseFenced
	}
	candidate.snapshot.ApplicationLeaseTerm = lease.Term
	candidate.directUnbound = true
	candidate.snapshot.Executor = proto.Clone(e.cfg.Process).(*pb.ProcessRef)
	candidate.snapshot.Selectors = append([]string(nil), observed.Selectors...)
	tx.selectors = append([]string(nil), observed.Selectors...)
	tx.direct, tx.decisionPending = true, true
	tx.charged = residentTransactionBytes(tx)
	candidate.mu.Lock()
	if err = e.accountResidentLocked(candidate, residentContextBytes(candidate)+tx.charged, 0); err != nil {
		candidate.mu.Unlock()
		return &pb.OperationResult{Certainty: pb.OperationResult_NOT_ACCEPTED, SipStatus: 503, Error: "resident capacity"}, nil
	}
	limit := residentSmallBudget
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
	effects = residentWithoutSends(effects)
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
	e.scheduleResidentDecisionLocked(candidate, tx)
	candidate.mu.Unlock()
	e.dispatchResident(ctx, writes)
	if err = e.notifyResidentRequest(ctx, peer, candidate, tx, presentation); err != nil {
		return nil, err
	}
	return &pb.OperationResult{Certainty: pb.OperationResult_ACCEPTED}, nil
}

// Only a pending transaction is published before policy. Dialog input and route
// publication are recomputed from current native values when its decision arrives.
func (e *ResidentMemory) receiveResidentExisting(ctx context.Context, c *residentContext, in *pb.OwnerIngest, deadline time.Time, observed *pb.SipMessage, request *sip.Request) (*pb.OperationResult, error) {
	if request == nil || request.IsAck() || request.IsCancel() {
		return nil, rejectIngest(481, "unknown native request")
	}
	peer, err := e.control.Peer(in.Application)
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	if !e.residentContextCurrentLocked(c, in.Context) || c.snapshot.Ended || c.snapshot.Transport != in.Flow.Transport || !sameProcess(c.snapshot.Application, in.Application) && c.subscription == nil {
		c.mu.Unlock()
		return nil, rejectIngest(481, "native request authority changed")
	}
	var dialogKey string
	if c.subscription != nil {
		if err = residentValidateSubscription(c, request, in); err != nil {
			c.mu.Unlock()
			return nil, err
		}
	} else {
		if request.To() == nil || request.From() == nil {
			c.mu.Unlock()
			return nil, rejectIngest(481, "dialog identity required")
		}
		dialogKey = tag(request.To().Params) + "/" + tag(request.From().Params)
		dialog := c.dialogs[dialogKey]
		if dialog == nil {
			c.mu.Unlock()
			return nil, rejectIngest(481, "dialog unavailable")
		}
		_, _, err = portable.StepDialog(dialog.native, portable.DialogInput{Kind: portable.DialogReceiveRequest, Message: portable.Message{Wire: in.Wire, Transport: in.Flow.Transport}, Now: time.Now().UnixMilli(), Offer: request.IsInvite() && len(request.Body()) > 0})
		if err != nil {
			c.mu.Unlock()
			return nil, err
		}
	}
	now := time.Now()
	until := minTime(deadline, now.Add(2*time.Second))
	requestID := "r-" + e.id("resident-request", in.Context.State.Incarnation, in.Context.Id, string(in.Wire))[:48]
	tx, effects, err := residentStartServerParsed(portable.Message{Wire: in.Wire, Transport: in.Flow.Transport}, in.Flow, in.Application, c.snapshot.LocalTag, requestID, until, now.UnixMilli(), request)
	if err != nil {
		c.mu.Unlock()
		return nil, err
	}
	if c.transactions[tx.native.Key] != nil {
		c.mu.Unlock()
		return nil, rejectIngest(481, "transaction key conflict")
	}
	tx.direct, tx.decisionPending, tx.requestDialog = true, true, dialogKey
	tx.selectors = append([]string(nil), observed.Selectors...)
	if c.subscription != nil || !proto.Equal(c.flow, in.Flow) {
		tx.decisionProposal = &pb.FlowAdoptionRequest{Context: c.snapshot.Context, Application: proto.Clone(in.Application).(*pb.ProcessRef), PrincipalId: c.snapshot.PrincipalId, BindingId: c.snapshot.BindingId, Flow: proto.Clone(in.Flow).(*pb.FlowRef), NativeRevision: c.snapshot.GetDelivery().GetNativeRevision() + 1, Subscription: c.subscription != nil}
		if tx.decisionProposal.NativeRevision == 0 {
			c.mu.Unlock()
			return nil, errors.New("flow publication sequence exhausted")
		}
	}
	limit := residentSmallBudget
	if request.IsInvite() {
		limit = residentServerInviteBudget
	}
	tx.budget, err = e.openResidentBudgetLocked(c, limit)
	if err != nil {
		c.mu.Unlock()
		return nil, err
	}
	tx.charged = residentTransactionBytes(tx)
	tx.borrowed, err = e.chargeResidentBudgetLocked(c, tx.budget, tx.charged)
	if err != nil {
		e.closeResidentBudgetLocked(c, tx.budget)
		c.mu.Unlock()
		return nil, err
	}
	pendingBytes := int64(0)
	if c.subscription != nil {
		pendingBytes = int64(len(tx.native.Key) - len(c.subscription.Pending))
	}
	var pendingBorrowed int64
	if pendingBytes > 0 {
		pendingBorrowed, err = e.chargeResidentBudgetLocked(c, c.subscriptionBudget, pendingBytes)
	}
	if err != nil {
		_ = e.refundResidentBudgetLocked(c, tx.budget, tx.charged, tx.borrowed)
		e.closeResidentBudgetLocked(c, tx.budget)
		c.mu.Unlock()
		return nil, err
	}
	c.transactions[tx.native.Key] = tx
	c.requests[requestID] = tx.native.Key
	if c.subscription != nil {
		c.subscription.Pending = tx.native.Key
		c.subscriptionBorrowed += pendingBorrowed
	}
	c.revision++
	c.snapshot.Revision++
	e.mu.Lock()
	for _, selector := range tx.selectors {
		e.transactions[selector] = c
	}
	e.mu.Unlock()
	_ = e.scheduleResidentTransactionLocked(c, tx, residentWithoutSends(effects))
	e.scheduleResidentDecisionLocked(c, tx)
	notification := &pb.RequestPresented{Snapshot: proto.Clone(c.snapshot).(*pb.ContextSnapshot), RequestId: requestID, Message: observed, Flow: proto.Clone(tx.flow).(*pb.FlowRef)}
	if tx.decisionProposal != nil {
		notification.FlowAdoption = proto.Clone(tx.decisionProposal).(*pb.FlowAdoptionRequest)
	}
	c.mu.Unlock()
	if err = e.notifyResidentRequest(ctx, peer, c, tx, notification); err != nil {
		return nil, err
	}
	return &pb.OperationResult{Certainty: pb.OperationResult_ACCEPTED}, nil
}
