package engine

import (
	"context"
	"errors"
	"time"

	"google.golang.org/protobuf/proto"
	"sipbridge/internal/control"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/rediscore"
	"sipbridge/internal/state"
)

// inspectNativeContext observes current metadata only. It never allocates,
// adopts an application, materializes SIP or replays an old command result.
func (e *redisNative) inspectNativeContext(parent context.Context, peer *control.Peer, ref *pb.ContextRef, deadline time.Time) (*pb.OperationResult, error) {
	if ref == nil || ref.State == nil || !state.ValidReference(ref.Id) || ref.State.GroupId != ref.Id ||
		ref.State.Partition != 0 || !state.ValidReference(ref.State.Incarnation) || !proto.Equal(ref.State.Authority, e.StateAuthority()) {
		return &pb.OperationResult{Certainty: pb.OperationResult_NOT_ACCEPTED, Error: "invalid native inspection reference"}, nil
	}
	witness, err := e.controllerWitness(peer)
	if err != nil {
		return &pb.OperationResult{Certainty: pb.OperationResult_NOT_ACCEPTED, Error: "controller attachment unavailable"}, nil
	}
	ctx, cancel, store, err := e.operation(parent, deadline)
	if err != nil {
		return nil, err
	}
	defer cancel()
	selected, err := store.ObserveApplicationWitness(ctx, ref.Id, ref.State.Incarnation, witness, "r:context")
	if errors.Is(err, rediscore.ErrAbsent) {
		return &pb.OperationResult{Certainty: pb.OperationResult_COMPLETED, RetirementProof: &pb.ContextRetirementProof{
			Context: proto.Clone(ref).(*pb.ContextRef), Reason: pb.ContextRetirementProof_CONTEXT_ABSENT}}, nil
	}
	if err != nil {
		return nil, err
	}
	current, err := decodeNativeContext(selected.View.Records["r:context"])
	if err != nil {
		return nil, err
	}
	if current.Context == nil || !proto.Equal(current.Context.State, ref.State) || current.Context.Id != ref.Id {
		return nil, rediscore.ErrAuthority
	}
	if current.Kind == pb.ContextSnapshot_SUBSCRIPTION || !sameProcess(current.Application, peer.Application()) || current.ApplicationLeaseTerm != witness.Term || selected.Identity.Owner != witness.Application || selected.Identity.Term != witness.Term {
		return nil, rediscore.ErrAuthority
	}
	snapshot, err := nativeProjectSnapshot(current, nil, nil, nil)
	if err != nil {
		return nil, err
	}
	snapshot.Executor = proto.Clone(e.cfg.Process).(*pb.ProcessRef)
	return &pb.OperationResult{Certainty: pb.OperationResult_COMPLETED, Snapshot: snapshot}, nil
}
