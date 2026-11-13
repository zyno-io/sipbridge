package engine

import (
	"errors"
	"time"

	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
)

// prepareResidentClientStart constructs the initial client transaction without
// publishing it or performing IO. The caller must account and publish the
// candidate and its output together under the context lock.
func (e *ResidentMemory) prepareResidentClientStart(open *pb.OpenUac, ref *pb.ContextRef, application *pb.ProcessRef, flow *pb.FlowRef, operation string, now time.Time) (*residentTransaction, []portable.Effect, error) {
	if open == nil || ref == nil || ref.State == nil || application == nil || flow == nil || operation == "" {
		return nil, nil, errors.New("client transaction authority required")
	}
	transport := uacTransport(open)
	if transport != "udp" && transport != "tcp" && transport != "tls" || flow.Transport != transport {
		return nil, nil, errors.New("unsupported or mismatched transport")
	}
	method := sip.RequestMethod(open.Method)
	if method == "" {
		method = sip.INVITE
	}
	contact, sentBy, err := e.advertisement(transport)
	if err != nil {
		return nil, nil, err
	}
	message, err := portable.BuildInitialRequest(portable.InitialRequest{Method: method, URI: open.Uri, CallID: ref.State.CallId,
		LocalTag: e.id("tag", ref.State.Incarnation, ref.Id)[:32], Branch: sip.RFC3261BranchMagicCookie + e.id("branch", ref.State.Incarnation, operation)[:32],
		CSeq: 1, Advertise: sentBy, Contact: contact, Transport: transport, Headers: stateHeaders(open.Headers), Body: open.Body})
	if err != nil {
		return nil, nil, err
	}
	family := portable.NonInviteClient
	if method == sip.INVITE {
		family = portable.InviteClient
	}
	native, err := portable.NewTransaction(family, message, portable.DefaultTimers(), "")
	if err != nil {
		return nil, nil, err
	}
	next, effects, err := portable.StepTransaction(native, portable.Input{Kind: portable.Start, Now: now.UnixMilli()})
	if err != nil {
		return nil, nil, err
	}
	return &residentTransaction{native: next, flow: proto.Clone(flow).(*pb.FlowRef), application: proto.Clone(application).(*pb.ProcessRef)}, effects, nil
}

// residentClientTransactionDelta measures retained native growth against the
// charge already attributed to this transaction; the caller commits the delta
// together with the candidate transition.
func residentClientTransactionDelta(tx *residentTransaction, next portable.Transaction) int64 {
	candidate := *tx
	candidate.native = next
	return residentTransactionBytes(&candidate) - tx.charged
}

// residentClientGrowth stages one touched record's owned-byte delta. Shrinkage
// is applied only at publication, so rollback never requires fresh capacity.
type residentClientGrowth struct {
	budget   *residentBudget
	delta    int64
	borrowed int64
}

func (e *ResidentMemory) prepareResidentClientGrowth(c *residentContext, budget *residentBudget, delta int64) (residentClientGrowth, error) {
	growth := residentClientGrowth{budget: budget, delta: delta}
	var err error
	growth.borrowed, err = e.chargeResidentBudgetLocked(c, budget, max(int64(0), delta))
	if err != nil {
		return residentClientGrowth{}, err
	}
	return growth, err
}
func (e *ResidentMemory) rollbackResidentClientGrowth(c *residentContext, growth residentClientGrowth) {
	_ = e.refundResidentBudgetLocked(c, growth.budget, max(int64(0), growth.delta), growth.borrowed)
}
func (e *ResidentMemory) commitResidentClientGrowth(c *residentContext, growth residentClientGrowth, owned *int64) {
	*owned += growth.borrowed
	if growth.delta < 0 {
		released := -growth.delta
		borrowed := min(*owned, released)
		_ = e.refundResidentBudgetLocked(c, growth.budget, released, borrowed)
		*owned -= borrowed
	}
}
