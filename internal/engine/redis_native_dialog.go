package engine

import (
	"context"
	"errors"
	"time"

	"github.com/emiago/sipgo/sip/portable"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
	"sipbridge/internal/control"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/rediscore"
)

// receiveDialog is a private direct-presentation seam. Decision/response,
// duplicate and timeout handling must qualify before ingress can route here.
func (e *redisNative) receiveDialog(parent context.Context, peer *control.Peer, flow *pb.FlowRef, message portable.Message, route *pb.ContextRef, principalID, bindingID string) error {
	if route == nil || route.State == nil || !proto.Equal(route.State.Authority, e.StateAuthority()) || !validPolicyFlow(flow) || !sameProcess(flow.Ingress, e.cfg.Process) {
		return errNativeRecord
	}
	witness, err := e.controllerWitness(peer)
	if err != nil {
		return err
	}
	deadline := minTime(deadlineOr(parent, time.Now().Add(2*time.Second)), time.Now().Add(2*time.Second))
	ctx, cancel, store, err := e.operation(parent, deadline)
	if err != nil {
		return err
	}
	defer cancel()
	in := redisDialogReceive{IdentityKey: e.cfg.StateIdentityKey, Context: route, Application: peer.Application(), Executor: e.cfg.Process, Lease: witness, Flow: flow, Message: message, PrincipalID: principalID, BindingID: bindingID,
		RequestID: uuid.NewString(), Timer: rediscore.TimerAssignment{Worker: e.worker, Epoch: e.cfg.Process.Epoch, Term: e.cfg.Process.Epoch}, Deadline: deadline}
	var presentation *pb.RequestPresented
	for attempts := 0; attempts < 3; attempts++ {
		presentation, err = receiveRedisDialogRequest(ctx, store, in)
		if !errors.Is(err, rediscore.ErrConflict) || ctx.Err() != nil || !time.Now().Before(deadline) {
			break
		}
	}
	if err != nil {
		return err
	}
	remaining := time.Until(deadline).Milliseconds()
	if ctx.Err() != nil || remaining <= 0 {
		return rediscore.ErrUnknown
	}
	return peer.Send(&pb.Envelope{CorrelationId: uuid.NewString(), RemainingBudgetMs: uint32(min(2000, remaining)), Payload: &pb.Envelope_Request{Request: presentation}})
}
