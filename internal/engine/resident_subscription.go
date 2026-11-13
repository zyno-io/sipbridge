package engine

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
)

func residentValidateSubscription(c *residentContext, request *sip.Request, in *pb.OwnerIngest) error {
	sub := c.subscription
	if sub == nil {
		return nil
	}
	event, err := eventKey(request)
	if request.Method != sip.SUBSCRIBE || err != nil || event != sub.Event || request.Contact() == nil || request.To() == nil || request.From() == nil || request.CSeq() == nil ||
		tag(request.To().Params) != c.snapshot.LocalTag || tag(request.From().Params) != c.snapshot.RemoteTag ||
		request.From().Address.String() != c.snapshot.RemoteUri || request.To().Address.String() != c.snapshot.LocalUri || sub.Terminating {
		return &portable.ProtocolError{Status: 481, Reason: "subscription identity mismatch"}
	}
	if c.snapshot.PrincipalId != "" && in.PrincipalId != "" && c.snapshot.PrincipalId != in.PrincipalId ||
		c.snapshot.BindingId != "" && in.BindingId != "" && c.snapshot.BindingId != in.BindingId {
		return &portable.ProtocolError{Status: 481, Reason: "subscription binding mismatch"}
	}
	if request.CSeq().SeqNo <= sub.RemoteSequence {
		return &portable.ProtocolError{Status: 500, Reason: "stale subscription sequence"}
	}
	if sub.Pending != "" {
		return &portable.ProtocolError{Status: 503, Reason: "subscription renewal pending"}
	}
	return nil
}

// prepareResidentSubscription returns only candidate values. Native response,
// output capacity, indexes and these values must be published together by the
// caller after any required flow-publication receipt has been checked.
func (e *ResidentMemory) prepareResidentSubscription(c *residentContext, tx *residentTransaction, response *pb.Respond, now time.Time) (*stateSubscription, *pb.ContextSnapshot, portable.Message, error) {
	parsed, err := tx.native.Origin.Parse()
	if err != nil {
		return nil, nil, portable.Message{}, err
	}
	request, ok := parsed.(*sip.Request)
	if !ok || request.Method != sip.SUBSCRIBE || request.Contact() == nil || request.CSeq() == nil || request.From() == nil || response == nil || response.Status < 200 || response.Status >= 300 {
		return nil, nil, portable.Message{}, errors.New("successful SUBSCRIBE with Contact required")
	}
	event, err := eventKey(request)
	if err != nil {
		return nil, nil, portable.Message{}, err
	}
	expires := 3600
	for _, h := range response.Headers {
		if strings.EqualFold(h.Name, "Expires") {
			expires, err = strconv.Atoi(h.Value)
			if err != nil || expires < 0 || expires > 10800 {
				return nil, nil, portable.Message{}, errors.New("invalid granted subscription expiry")
			}
		}
	}
	sub := &stateSubscription{Event: event}
	if c.subscription != nil {
		if c.subscription.Event != event {
			return nil, nil, portable.Message{}, errors.New("subscription event mismatch")
		}
		*sub = *c.subscription
		sub.Routes = append([]string(nil), c.subscription.Routes...)
	} else {
		for _, h := range request.GetHeaders("Record-Route") {
			sub.Routes = append(sub.Routes, h.Value())
		}
	}
	sub.Target = request.Contact().Address.String()
	sub.RemoteSequence = request.CSeq().SeqNo
	sub.Pending = ""
	sub.ExpiresMS = now.Add(time.Duration(expires) * time.Second).UnixMilli()
	sub.Terminating = expires == 0
	sub.TerminalMS = 0
	if sub.Terminating {
		sub.TerminalMS = now.Add(5 * time.Second).UnixMilli()
	}
	snapshot := proto.Clone(c.snapshot).(*pb.ContextSnapshot)
	snapshot.Kind = pb.ContextSnapshot_SUBSCRIPTION
	snapshot.RemoteTag = tag(request.From().Params)
	snapshot.DialogId = sip.DialogIDMake(snapshot.CallId, snapshot.LocalTag, snapshot.RemoteTag)
	snapshot.RemoteTarget = sub.Target
	contact, _, err := e.advertisement(tx.native.Origin.Transport)
	if err != nil {
		return nil, nil, portable.Message{}, err
	}
	message, err := portable.BuildResponseWithHeaders(tx.native.Origin, int(response.Status), reason(response.Status), response.Body, snapshot.LocalTag, contact, stateHeaders(response.Headers))
	return sub, snapshot, message, err
}

func (e *ResidentMemory) prepareResidentNotify(c *residentContext, command *pb.DialogRequest, operation string, now time.Time) (*stateSubscription, *residentTransaction, []portable.Effect, error) {
	if c.subscription == nil || command == nil || command.Method != "NOTIFY" {
		return nil, nil, nil, errors.New("subscription only supports NOTIFY")
	}
	sub := *c.subscription
	stateValue := "active"
	for _, h := range command.Headers {
		if strings.EqualFold(h.Name, "Subscription-State") {
			stateValue = h.Value
		}
	}
	final := strings.HasPrefix(stateValue, "terminated")
	if c.snapshot.Ended || sub.Final || sub.Terminating && !final {
		return nil, nil, nil, errSubscriptionTerminating
	}
	if !final {
		remaining := (sub.ExpiresMS - now.UnixMilli() + 999) / 1000
		if remaining < 1 {
			return nil, nil, nil, errors.New("subscription expired")
		}
		stateValue = fmt.Sprintf("active;expires=%d", remaining)
	}
	if sub.LocalSequence >= math.MaxInt32 {
		return nil, nil, nil, errors.New("subscription CSeq exhausted")
	}
	sub.LocalSequence++
	headers := []portable.Header{{Name: "Event", Value: sub.Event}, {Name: "Subscription-State", Value: stateValue}}
	for _, h := range command.Headers {
		if strings.EqualFold(h.Name, "Content-Type") {
			headers = append(headers, portable.Header{Name: h.Name, Value: h.Value})
		}
	}
	contact, sentBy, err := e.advertisement(c.snapshot.Transport)
	if err != nil {
		return nil, nil, nil, err
	}
	dialog := portable.Dialog{Version: portable.Version, Role: portable.UAS, Phase: portable.DialogConfirmed, CallID: c.snapshot.CallId,
		LocalTag: c.snapshot.LocalTag, RemoteTag: c.snapshot.RemoteTag, LocalURI: c.snapshot.LocalUri, RemoteURI: c.snapshot.RemoteUri,
		Target: sub.Target, Routes: sub.Routes, Contact: contact, SentBy: sentBy, Transport: c.snapshot.Transport}
	message, err := portable.BuildDialogRequestWithHeaders(dialog, sip.NOTIFY, sub.LocalSequence, command.Body,
		sip.RFC3261BranchMagicCookie+e.id("notify", e.authority.PrimaryId, operation)[:32], headers)
	if err != nil {
		return nil, nil, nil, err
	}
	native, err := portable.NewTransaction(portable.NonInviteClient, message, portable.DefaultTimers(), "")
	if err != nil {
		return nil, nil, nil, err
	}
	next, effects, err := portable.StepTransaction(native, portable.Input{Kind: portable.Start, Now: now.UnixMilli()})
	if err != nil {
		return nil, nil, nil, err
	}
	if final {
		sub.Terminating, sub.Final, sub.TerminalMS = true, true, now.Add(5*time.Second).UnixMilli()
	}
	return &sub, &residentTransaction{native: next, flow: proto.Clone(c.flow).(*pb.FlowRef), application: proto.Clone(c.snapshot.Application).(*pb.ProcessRef)}, effects, nil
}

// adoptResidentFlow performs the direct publication RPC with no state lock.
// The caller must revalidate revision/request identity before publishing the
// returned receipt together with its accepted subscription/response transition.
func (e *ResidentMemory) adoptResidentFlow(ctx context.Context, snapshot *pb.ContextSnapshot, flow *pb.FlowRef, application *pb.ProcessRef, subscription bool) (*pb.ContextDelivery, error) {
	if snapshot == nil || snapshot.Context == nil || flow == nil || application == nil || snapshot.GetDelivery().GetNativeRevision() == math.MaxUint64 {
		return nil, errors.New("invalid flow publication proposal")
	}
	peer, err := e.control.Peer(application)
	if err != nil {
		return nil, err
	}
	proposal := &pb.FlowAdoptionRequest{Context: snapshot.Context, Application: application, PrincipalId: snapshot.PrincipalId, BindingId: snapshot.BindingId,
		Flow: flow, NativeRevision: snapshot.GetDelivery().GetNativeRevision() + 1, Subscription: subscription}
	reply, err := peer.Call(ctx, &pb.Envelope{CorrelationId: uuid.NewString(), RemainingBudgetMs: uint32(min(2000, max(1, time.Until(deadlineOr(ctx, time.Now().Add(2*time.Second))).Milliseconds()))),
		Payload: &pb.Envelope_FlowAdoption{FlowAdoption: proposal}})
	if err != nil {
		return nil, err
	}
	result := reply.GetFlowAdoptionResult()
	receipt := result.GetReceipt()
	if result == nil || result.Error != "" || receipt == nil || receipt.NativeRevision != proposal.NativeRevision || receipt.DirectoryEpoch == "" || !proto.Equal(receipt.Flow, flow) {
		return nil, errors.New("flow publication refused or invalid receipt")
	}
	if subscription && snapshot.DirectoryEpoch != "" && receipt.DirectoryEpoch != snapshot.DirectoryEpoch {
		return nil, errors.New("subscription owner directory epoch changed")
	}
	return proto.Clone(receipt).(*pb.ContextDelivery), nil
}

func deadlineOr(ctx context.Context, fallback time.Time) time.Time {
	if deadline, ok := ctx.Deadline(); ok {
		return deadline
	}
	return fallback
}
