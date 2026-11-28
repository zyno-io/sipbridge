package engine

import (
	"context"
	"errors"
	"math"
	"strconv"
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
	if !proto.Equal(c.flow, in.Flow) {
		return &portable.ProtocolError{Status: 481, Reason: "subscription connection changed"}
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

// prepareResidentSubscription creates candidates without publishing them.
func (e *ResidentMemory) prepareResidentSubscription(c *residentContext, tx *residentTransaction, response *pb.Respond, now time.Time) (*stateSubscription, *pb.ContextSnapshot, portable.Message, error) {
	if tx.flow != nil && tx.flow.Transport != "udp" && e.cfg.LiveFlow != nil && !e.cfg.LiveFlow(tx.flow) {
		return nil, nil, portable.Message{}, &portable.ProtocolError{Status: 481, Reason: "subscription connection closed"}
	}
	return nativePrepareSubscription(c.subscription, c.snapshot, tx.native, response, e.nativeAdvertisement(), now)
}

func (e *ResidentMemory) prepareResidentNotify(c *residentContext, command *pb.DialogRequest, operation string, now time.Time) (*stateSubscription, *residentTransaction, []portable.Effect, error) {
	if c == nil || c.snapshot == nil || c.snapshot.Context == nil || c.snapshot.Context.State == nil {
		return nil, nil, nil, errors.New("subscription context identity required")
	}
	attribution, valid := notifyCapture(command)
	if !valid {
		return nil, nil, nil, errors.New("invalid NOTIFY capture delivery ID")
	}
	ref := c.snapshot.Context
	branch := sip.RFC3261BranchMagicCookie + e.id("notify", e.authority.PrimaryId, ref.State.Incarnation, ref.Id, strconv.FormatUint(c.snapshot.CommandSequence, 10), operation)[:32]
	sub, native, effects, err := nativePrepareNotify(c.subscription, c.snapshot, command, e.nativeAdvertisement(), branch, now)
	if err != nil {
		return nil, nil, nil, err
	}
	return sub, &residentTransaction{native: native, attribution: attribution, flow: proto.Clone(c.flow).(*pb.FlowRef), application: proto.Clone(c.snapshot.Application).(*pb.ProcessRef)}, effects, nil
}

func (e *ResidentMemory) nativeAdvertisement() nativeSIPAdvertisement {
	return nativeSIPAdvertisement{contactURI: e.cfg.ContactURI, address: e.cfg.AdvertiseAddress, plainAddress: e.cfg.PlainAdvertiseAddress}
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
