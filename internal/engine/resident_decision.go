package engine

import (
	"context"
	"errors"
	"time"

	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
	"google.golang.org/protobuf/proto"
	"sipbridge/internal/control"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/state"
)

// residentDecision contains only operation-local validation values. Native
// records never retain the controller decision or parsed SIP graph.
type residentDecision struct {
	value       *pb.RequestDecision
	request     portable.MessageFacts
	policy      *pb.CommandAdmission
	application *pb.ProcessRef
	term        string
	bindOwner   bool
	capability  *control.ApplicationCapability
}

func (e *ResidentMemory) decideResidentRequest(ctx context.Context, peer *control.Peer, d *pb.RequestDecision, operation string, deadline time.Time) (*pb.OperationResult, error) {
	if d == nil || d.Context == nil || !peer.HasCapability(control.RequestDecisionCapability) {
		return nil, errors.New("direct decision authority required")
	}
	response := &pb.Respond{RequestId: d.RequestId, Status: d.Status, Headers: d.Headers, Body: d.Body}
	if d.Status < 300 {
		response.PrincipalId, response.BindingId = d.PrincipalId, d.BindingId
	}
	cmd := &pb.Command{Context: d.Context, Sequence: d.Sequence, Admission: d.Admission, Action: &pb.Command_Respond{Respond: response}}
	result, err := e.respondResident(ctx, peer, cmd, operation, deadline, &residentDecision{value: d})
	if err != nil {
		if errors.Is(err, state.ErrNotFound) {
			return &pb.OperationResult{Certainty: pb.OperationResult_UNKNOWN, Error: "historical decision outcome unavailable"}, nil
		}
		return &pb.OperationResult{Certainty: pb.OperationResult_NOT_ACCEPTED, Error: err.Error()}, nil
	}
	return result, nil
}

func (e *ResidentMemory) validateResidentDecisionLocked(c *residentContext, peer *control.Peer, decision *residentDecision, operation string, deadline time.Time) (*pb.CommandAdmission, error) {
	d := decision.value
	tx := c.transactions[c.requests[d.RequestId]]
	if tx == nil {
		return nil, errors.New("pending native decision unavailable")
	}
	candidate, err := nativePrepareDecision(nativeDecisionInput{
		decision: d, transaction: tx.native, direct: tx.direct, decisionPending: tx.decisionPending,
		unbound: c.directUnbound, hasSubscription: c.subscription != nil, presentDeadline: tx.presentDeadline,
		now: time.Now(), deadline: deadline, transactionApplication: tx.application, application: peer.Application(),
		leaseTerm: peer.ApplicationLease().GetTerm(), snapshot: c.snapshot, currentFlow: c.flow,
		transactionFlow: tx.flow, requestDialog: tx.requestDialog, proposal: tx.decisionProposal, operation: operation,
	})
	if err != nil {
		return nil, err
	}
	decision.request, decision.policy = candidate.request, candidate.policy
	decision.application, decision.term, decision.bindOwner = candidate.application, candidate.term, candidate.bindOwner
	return candidate.policy, nil
}

// finishResidentDecisionLocked freezes the small native-commit receipt before
// socket I/O. Physical completion never replaces this fact with a write result.
func finishResidentDecisionLocked(c *residentContext, receipt *residentCommand, status uint32) *pb.OperationResult {
	finishResidentCommandWireLocked(c, receipt, pb.OperationResult_ACCEPTED, status, "", nil, nil)
	result := residentCommandResultLocked(c, receipt)
	if receipt.originalPending && receipt.registered {
		c.receiptPins--
	}
	receipt.originalPending = false
	releaseResidentCommandWireLocked(c, receipt)
	releaseResidentCommandLocked(c, receipt)
	return result
}

func (e *ResidentMemory) receiveDecisionDialogLocked(c *residentContext, tx *residentTransaction, request portable.MessageFacts) (*residentDialog, []portable.Effect, error) {
	if tx.requestDialog == "" {
		return nil, nil, nil
	}
	previous := c.dialogs[tx.requestDialog]
	if previous == nil {
		return nil, nil, errors.New("decision dialog unavailable")
	}
	next, effects, err := portable.StepDialog(previous.native, portable.DialogInput{Kind: portable.DialogReceiveRequest, Message: tx.native.Origin, Now: time.Now().UnixMilli(), Offer: request.Method == sip.INVITE && request.BodyLength > 0})
	if err != nil {
		return nil, nil, err
	}
	copy := *previous
	copy.native = next
	return &copy, effects, nil
}

func cloneResidentDelivery(value *pb.ContextDelivery) *pb.ContextDelivery {
	if value == nil {
		return nil
	}
	return proto.Clone(value).(*pb.ContextDelivery)
}

func (e *ResidentMemory) commitResidentDeferredLocked(ctx context.Context, c *residentContext, tx *residentTransaction, receipt *residentCommand, direct *residentDecision, dialog *residentDialog, dialogEffects []portable.Effect, deadline time.Time) (*pb.OperationResult, error) {
	fail := func(err error) (*pb.OperationResult, error) {
		finishResidentCommandLocked(c, receipt, &pb.OperationResult{Certainty: pb.OperationResult_NOT_ACCEPTED, Error: err.Error()})
		result := residentCommandResultLocked(c, receipt)
		if receipt.originalPending && receipt.registered {
			c.receiptPins--
		}
		receipt.originalPending = false
		releaseResidentCommandWireLocked(c, receipt)
		releaseResidentCommandLocked(c, receipt)
		c.mu.Unlock()
		return result, nil
	}
	snapshot := proto.Clone(c.snapshot).(*pb.ContextSnapshot)
	bindResidentDecisionSnapshot(snapshot, direct)
	snapshot.PrincipalId, snapshot.BindingId = direct.value.PrincipalId, direct.value.BindingId
	flow := c.flow
	if direct.value.Delivery != nil {
		snapshot.Delivery = cloneResidentDelivery(direct.value.Delivery)
		flow = tx.flow
	}
	contextDelta := residentContextBytes(&residentContext{snapshot: snapshot, flow: flow}) - residentContextBytes(c)
	contextGrowth, err := e.prepareResidentClientGrowth(c, c.budget, contextDelta)
	if err != nil {
		return fail(err)
	}
	var dialogGrowth residentClientGrowth
	if dialog != nil {
		previous := c.dialogs[tx.requestDialog]
		dialogGrowth, err = e.prepareResidentClientGrowth(c, dialog.budget, residentDialogBytes(dialog.native)-residentDialogBytes(previous.native))
		if err != nil {
			e.rollbackResidentClientGrowth(c, contextGrowth)
			return fail(err)
		}
	}
	var effects []portable.Effect
	if tx.native.LastResponse != nil && tx.emissions == 0 {
		effects = []portable.Effect{{Kind: portable.Send, Message: tx.native.LastResponse}}
	}
	oldSnapshot, oldFlow := c.snapshot, c.flow
	c.snapshot, c.flow = snapshot, flow
	publish := func() error {
		return e.publishResidentDecisionAuthorityLocked(c, direct, minTime(deadline, tx.presentDeadline))
	}
	writes, err := e.stageResidentOutputGroupsLocked(c, []residentOutputGroup{{tx: tx, effects: effects, policy: direct.policy}}, receipt.operation, deadline, publish)
	if err == nil && len(writes) == 0 {
		err = publish()
	}
	c.snapshot, c.flow = oldSnapshot, oldFlow
	if err != nil {
		if dialog != nil {
			e.rollbackResidentClientGrowth(c, dialogGrowth)
		}
		e.rollbackResidentClientGrowth(c, contextGrowth)
		return fail(err)
	}
	e.commitResidentClientGrowth(c, contextGrowth, &c.borrowed)
	c.snapshot = snapshot
	c.flow = proto.Clone(flow).(*pb.FlowRef)
	if dialog != nil {
		key := tx.requestDialog
		e.commitResidentClientGrowth(c, dialogGrowth, &dialog.borrowed)
		c.dialogs[key] = dialog
		_ = e.scheduleResidentDialogLocked(c, key, dialogEffects)
	}
	e.clearResidentDecisionLocked(c, tx)
	c.revision++
	c.snapshot.Revision++
	result := finishResidentDecisionLocked(c, receipt, 0)
	c.mu.Unlock()
	e.dispatchResident(ctx, writes)
	return result, nil
}

func bindResidentDecisionSnapshot(snapshot *pb.ContextSnapshot, decision *residentDecision) {
	if decision.bindOwner {
		snapshot.Application = proto.Clone(decision.application).(*pb.ProcessRef)
		snapshot.ApplicationLeaseTerm = decision.term
	}
}

// Called after all fallible native/output staging, with the candidate snapshot
// installed under c.mu. Lease expiry and owner publication share e.mu, so an
// expiring selected controller cannot miss its newly accepted context.
func (e *ResidentMemory) publishResidentDecisionAuthorityLocked(c *residentContext, decision *residentDecision, deadline time.Time) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	lease := e.leases[residentControllerKey(decision.application)]
	now := time.Now()
	if e.closed || (!e.phoneDomain && (lease == nil || lease.term != decision.term || !now.Before(lease.expires))) || !now.Before(deadline) {
		return errors.New("decision controller or original deadline expired")
	}
	if decision.bindOwner {
		c.directUnbound = false
		if e.phoneDomain {
			c.applicationCapability = decision.capability
		}
		c.ownerLost = false
		e.indexResidentOwnerUnderEngineLock(c)
	}
	return nil
}
