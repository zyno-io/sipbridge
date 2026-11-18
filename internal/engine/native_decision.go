package engine

import (
	"errors"
	"time"

	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
)

// nativeDecisionInput is a bounded selected-record view. The caller has
// already checked the live owner lease and sequence and still owns publication.
type nativeDecisionInput struct {
	decision                                          *pb.RequestDecision
	transaction                                       portable.Transaction
	direct, decisionPending, unbound, hasSubscription bool
	presentDeadline, now, deadline                    time.Time
	transactionApplication, application               *pb.ProcessRef
	leaseTerm                                         string
	snapshot                                          *pb.ContextSnapshot
	currentFlow, transactionFlow                      *pb.FlowRef
	requestDialog                                     string
	proposal                                          *pb.FlowAdoptionRequest
	operation                                         string
}
type nativeDecisionCandidate struct {
	request     portable.MessageFacts
	policy      *pb.CommandAdmission
	application *pb.ProcessRef
	term        string
	bindOwner   bool
}

func nativePrepareDecision(in nativeDecisionInput) (nativeDecisionCandidate, error) {
	d := in.decision
	candidate := nativeDecisionCandidate{}
	if d == nil || in.snapshot == nil {
		return nativeDecisionCandidate{}, errors.New("pending native decision unavailable")
	}
	if !in.direct || !in.decisionPending || in.transaction.Phase == portable.Completed || in.transaction.Phase == portable.Confirmed || in.transaction.Phase == portable.Terminated || !in.now.Before(in.presentDeadline) || !in.now.Before(in.deadline) || !in.unbound && !sameProcess(in.transactionApplication, in.application) {
		return nativeDecisionCandidate{}, errors.New("pending native decision unavailable")
	}
	candidate.application, candidate.term = in.application, in.leaseTerm
	candidate.bindOwner = in.unbound || d.Delivery != nil
	request, err := in.transaction.Origin.Facts()
	if err != nil {
		return nativeDecisionCandidate{}, err
	}
	if !request.IsRequest {
		return nativeDecisionCandidate{}, errors.New("native decision request required")
	}
	candidate.request = request
	if d.Status != 0 && (d.Status < 200 || d.Status > 699) {
		return nativeDecisionCandidate{}, errors.New("decision requires a final response or deferred handling")
	}
	if d.Status == 0 && (request.Method == sip.REGISTER || request.Method == sip.SUBSCRIBE) {
		return nativeDecisionCandidate{}, errors.New("REGISTER and SUBSCRIBE require a final decision")
	}
	if d.Status == 0 && (len(d.Headers) != 0 || len(d.Body) != 0) {
		return nativeDecisionCandidate{}, errors.New("deferred decision cannot select response content")
	}
	acceptedSub := request.Method == sip.SUBSCRIBE && d.Status >= 200 && d.Status < 300
	if acceptedSub && in.hasSubscription {
		candidate.bindOwner = true
	}
	if acceptedSub != (d.Notify != nil) || d.Notify != nil && d.Notify.Method != "NOTIFY" {
		return nativeDecisionCandidate{}, errors.New("accepted SUBSCRIBE requires exactly one NOTIFY template")
	}
	if d.Status >= 300 {
		if d.Delivery != nil || d.PrincipalId != "" || d.BindingId != "" {
			return nativeDecisionCandidate{}, errors.New("denial cannot publish account or flow authority")
		}
		return candidate, nil
	}
	if in.snapshot.PrincipalId != "" && in.snapshot.PrincipalId != d.PrincipalId || in.snapshot.BindingId != "" && in.snapshot.BindingId != d.BindingId {
		return nativeDecisionCandidate{}, errors.New("decision account changed")
	}
	switch d.Basis {
	case pb.OwnerIngest_EXISTING_FLOW:
		if !proto.Equal(in.currentFlow, in.transactionFlow) {
			return nativeDecisionCandidate{}, errors.New("existing flow changed")
		}
	case pb.OwnerIngest_AUTHENTICATED_BINDING:
		if d.PrincipalId == "" || d.BindingId == "" {
			return nativeDecisionCandidate{}, errors.New("authenticated binding required")
		}
	case pb.OwnerIngest_TLS_DIALOG_CONTINUITY:
		if in.snapshot.Transport != "tls" || in.transactionFlow.GetTransport() != "tls" || !in.hasSubscription && in.requestDialog == "" {
			return nativeDecisionCandidate{}, errors.New("TLS context continuity required")
		}
	default:
		return nativeDecisionCandidate{}, errors.New("invalid request admission basis")
	}
	if proposal := in.proposal; proposal != nil {
		receipt := d.Delivery
		if receipt == nil || !proto.Equal(receipt.Flow, proposal.Flow) || receipt.NativeRevision != proposal.NativeRevision || receipt.DirectoryEpoch == "" || receipt.RegistrationTicket == "" {
			return nativeDecisionCandidate{}, errors.New("ordered flow receipt required")
		}
		if d.Admission == nil || receipt.DirectoryEpoch != d.Admission.DirectoryEpoch {
			return nativeDecisionCandidate{}, errors.New("flow receipt directory changed")
		}
	} else if d.Delivery != nil {
		return nativeDecisionCandidate{}, errors.New("unsolicited flow receipt")
	}
	if d.Admission == nil {
		return nativeDecisionCandidate{}, errors.New("decision policy required")
	}
	policy := proto.Clone(d.Admission).(*pb.CommandAdmission)
	if policy.SelectedFlow != nil {
		target := in.currentFlow
		if d.Delivery != nil {
			target = d.Delivery.Flow
		}
		if d.Notify == nil || !proto.Equal(policy.SelectedFlow, target) {
			return nativeDecisionCandidate{}, errors.New("decision NOTIFY flow mismatch")
		}
		policy.SelectedFlow = nil
	}
	command := &pb.Command{Context: d.Context, Sequence: d.Sequence, Admission: policy, Action: &pb.Command_Respond{Respond: &pb.Respond{RequestId: d.RequestId, Status: d.Status}}}
	until := minTime(in.deadline, in.presentDeadline)
	if err = validateNativeCommandPolicy(in.snapshot, command, in.operation, in.now, until); err != nil {
		return nativeDecisionCandidate{}, err
	}
	policy.ExpiresAtMs = min(policy.ExpiresAtMs, uint64(until.UnixMilli()))
	candidate.policy = policy
	return candidate, nil
}
