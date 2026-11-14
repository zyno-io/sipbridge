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
		return &pb.OperationResult{Certainty: pb.OperationResult_NOT_ACCEPTED, Error: err.Error()}, nil
	}
	return result, nil
}

func (e *ResidentMemory) validateResidentDecisionLocked(c *residentContext, peer *control.Peer, decision *residentDecision, operation string, deadline time.Time) (*pb.CommandAdmission, error) {
	d := decision.value
	tx := c.transactions[c.requests[d.RequestId]]
	if tx == nil || !tx.direct || !tx.decisionPending || tx.native.Phase == portable.Completed || tx.native.Phase == portable.Confirmed || tx.native.Phase == portable.Terminated || !time.Now().Before(tx.presentDeadline) || !time.Now().Before(deadline) || !c.directUnbound && !sameProcess(tx.application, peer.Application()) {
		return nil, errors.New("pending native decision unavailable")
	}
	decision.application, decision.term = peer.Application(), peer.ApplicationLease().GetTerm()
	decision.bindOwner = c.directUnbound || d.Delivery != nil
	request, err := tx.native.Origin.Facts()
	if err != nil {
		return nil, err
	}
	if !request.IsRequest {
		return nil, errors.New("native decision request required")
	}
	decision.request = request
	if d.Status != 0 && (d.Status < 200 || d.Status > 699) {
		return nil, errors.New("decision requires a final response or deferred handling")
	}
	if d.Status == 0 && (request.Method == sip.REGISTER || request.Method == sip.SUBSCRIBE) {
		return nil, errors.New("REGISTER and SUBSCRIBE require a final decision")
	}
	if d.Status == 0 && (len(d.Headers) != 0 || len(d.Body) != 0) {
		return nil, errors.New("deferred decision cannot select response content")
	}
	acceptedSub := request.Method == sip.SUBSCRIBE && d.Status >= 200 && d.Status < 300
	if acceptedSub != (d.Notify != nil) || d.Notify != nil && d.Notify.Method != "NOTIFY" {
		return nil, errors.New("accepted SUBSCRIBE requires exactly one NOTIFY template")
	}
	if d.Status >= 300 {
		if d.Delivery != nil || d.PrincipalId != "" || d.BindingId != "" {
			return nil, errors.New("denial cannot publish account or flow authority")
		}
		return nil, nil
	}
	if c.snapshot.PrincipalId != "" && c.snapshot.PrincipalId != d.PrincipalId || c.snapshot.BindingId != "" && c.snapshot.BindingId != d.BindingId {
		return nil, errors.New("decision account changed")
	}
	switch d.Basis {
	case pb.OwnerIngest_EXISTING_FLOW:
		if !proto.Equal(c.flow, tx.flow) {
			return nil, errors.New("existing flow changed")
		}
	case pb.OwnerIngest_AUTHENTICATED_BINDING:
		if d.PrincipalId == "" || d.BindingId == "" {
			return nil, errors.New("authenticated binding required")
		}
	case pb.OwnerIngest_TLS_DIALOG_CONTINUITY:
		if c.snapshot.Transport != "tls" || tx.flow.Transport != "tls" || c.subscription == nil && tx.requestDialog == "" {
			return nil, errors.New("TLS context continuity required")
		}
	default:
		return nil, errors.New("invalid request admission basis")
	}
	if proposal := tx.decisionProposal; proposal != nil {
		receipt := d.Delivery
		if receipt == nil || !proto.Equal(receipt.Flow, proposal.Flow) || receipt.NativeRevision != proposal.NativeRevision || receipt.DirectoryEpoch == "" || receipt.RegistrationTicket == "" {
			return nil, errors.New("ordered flow receipt required")
		}
		if d.Admission == nil || receipt.DirectoryEpoch != d.Admission.DirectoryEpoch {
			return nil, errors.New("flow receipt directory changed")
		}
	} else if d.Delivery != nil {
		return nil, errors.New("unsolicited flow receipt")
	}
	if d.Admission == nil {
		return nil, errors.New("decision policy required")
	}
	policy := proto.Clone(d.Admission).(*pb.CommandAdmission)
	if policy.SelectedFlow != nil {
		target := c.flow
		if d.Delivery != nil {
			target = d.Delivery.Flow
		}
		if d.Notify == nil || !proto.Equal(policy.SelectedFlow, target) {
			return nil, errors.New("decision NOTIFY flow mismatch")
		}
		policy.SelectedFlow = nil
	}
	command := &pb.Command{Context: d.Context, Sequence: d.Sequence, Admission: policy, Action: &pb.Command_Respond{Respond: &pb.Respond{RequestId: d.RequestId, Status: d.Status}}}
	until := minTime(deadline, tx.presentDeadline)
	if err = validateResidentCommandPolicy(c, command, operation, until); err != nil {
		return nil, err
	}
	policy.ExpiresAtMs = min(policy.ExpiresAtMs, uint64(until.UnixMilli()))
	decision.policy = policy
	return policy, nil
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
	if e.closed || lease == nil || lease.term != decision.term || !now.Before(lease.expires) || !now.Before(deadline) {
		return errors.New("decision controller or original deadline expired")
	}
	if decision.bindOwner {
		c.directUnbound = false
		c.ownerLost = false
		e.indexResidentOwnerUnderEngineLock(c)
	}
	return nil
}
