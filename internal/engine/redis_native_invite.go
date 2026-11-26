package engine

import (
	"context"
	"errors"
	"time"

	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
	"sipbridge/internal/control"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/rediscore"
)

// receiveInitialInvite is a private qualification boundary; the serving
// selector remains unchanged until the complete UAS lifecycle qualifies.
func (e *redisNative) receiveInitialInvite(parent context.Context, peer *control.Peer, flow *pb.FlowRef, message portable.Message) error {
	if !validPolicyFlow(flow) || !sameProcess(flow.Ingress, e.cfg.Process) {
		return errors.New("invalid initial INVITE flow")
	}
	witness, err := e.controllerWitness(peer)
	if err != nil {
		return err
	}
	facts, err := message.Facts()
	if err != nil || !facts.IsRequest || facts.Method != sip.INVITE || facts.ToTag != "" {
		return errors.New("fresh initial INVITE required")
	}
	deadline := minTime(deadlineOr(parent, time.Now().Add(2*time.Second)), time.Now().Add(2*time.Second))
	ctx, cancel, store, err := e.operation(parent, deadline)
	if err != nil {
		return err
	}
	defer cancel()
	id, incarnation, requestID := uuid.NewString(), uuid.NewString(), uuid.NewString()
	ref := &pb.ContextRef{Id: id, Owner: proto.Clone(e.cfg.Process).(*pb.ProcessRef), State: &pb.StateRef{Authority: e.StateAuthority(), GroupId: id, Incarnation: incarnation, CallId: facts.CallID}}
	localTag := nativeCompactIdentity(e.cfg.StateIdentityKey, "native-invite-tag", id, incarnation)
	presentation, err := receiveRedisInvite(ctx, store, redisRegisterReceive{IdentityKey: e.cfg.StateIdentityKey, Context: ref, Authority: e.StateAuthority(), Application: peer.Application(), Executor: e.cfg.Process, Lease: witness, Flow: flow, Message: message, RequestID: requestID, LocalTag: localTag, Timer: rediscore.TimerAssignment{Worker: e.worker, Epoch: e.cfg.Process.Epoch, Term: e.cfg.Process.Epoch}, Deadline: deadline})
	if err != nil {
		return err
	}
	remaining := time.Until(deadline).Milliseconds()
	if ctx.Err() != nil || remaining <= 0 {
		return rediscore.ErrUnknown
	}
	return peer.Send(&pb.Envelope{CorrelationId: uuid.NewString(), RemainingBudgetMs: uint32(min(2000, remaining)), Payload: &pb.Envelope_Request{Request: presentation}})
}
