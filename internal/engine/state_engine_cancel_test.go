package engine

import (
	"testing"

	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/state"
)

func TestStateCancellationCannotStartLateAuthorizedInvite(t *testing.T) {
	r, c, tx := forkFixture(t)
	r.result = &pb.OperationResult{Certainty: pb.OperationResult_COMPLETED}
	if err := r.send(c, tx, c.Origin, false); err != nil {
		t.Fatal(err)
	}
	effect := r.payload.Effects[0]
	var body stateEngineEffect
	if err := unmarshalRetainedFixture(effect.Payload, &body); err != nil {
		t.Fatal(err)
	}
	r.in = stateEngineInput{Effect: effect.ID}
	if err := r.startEffect(); err != nil {
		t.Fatal(err)
	}
	c.Cancelled = true
	if err := r.cancelUnstartedEffects(c); err != nil {
		t.Fatal(err)
	}
	_, cancelled, cancelledBody, err := r.findEffect(effect.ID)
	if err != nil {
		t.Fatal(err)
	}
	if cancelled.Status != state.EffectUnknown || !cancelledBody.Cancelled {
		t.Fatal("in-flight authorization lost cancellation evidence")
	}
	permit := &pb.SendPermit{Authorization: body.Authorization, Flow: body.Flow, DirectoryEpoch: "directory", OrderingProof: "proof"}
	r.in = stateEngineInput{Effect: effect.ID, Executor: r.engine.cfg.Process, Reply: &pb.Envelope{Payload: &pb.Envelope_SendPermit{SendPermit: permit}}}
	if err = r.effectResult(); err != nil {
		t.Fatal(err)
	}
	_, completed, completedBody, err := r.findEffect(effect.ID)
	if err != nil {
		t.Fatal(err)
	}
	if completed.Kind != "deliver" || completed.Status != state.EffectNotWritten || !proto.Equal(completedBody.Permit, permit) {
		t.Fatal("late authorization became a fresh physical start")
	}
	r.in = stateEngineInput{Write: &pb.FlowWrite{Flow: body.Flow, Effect: body.Authorization.Effect, Permit: permit}, Executor: body.Flow.Ingress}
	r.result = &pb.OperationResult{}
	if err = r.reserve(); err != nil || r.result.Response == nil || pb.DeliveryResult_Outcome(r.result.SipStatus) != pb.DeliveryResult_NOT_WRITTEN || r.result.Request != nil {
		t.Fatalf("cancelled grant returned physical bytes: %v", err)
	}
}
func TestStateCancellationPreservesAlreadyReservedPhysicalAttempt(t *testing.T) {
	r, c, tx := forkFixture(t)
	r.result = &pb.OperationResult{}
	if err := r.send(c, tx, c.Origin, false); err != nil {
		t.Fatal(err)
	}
	effect := r.payload.Effects[0]
	var body stateEngineEffect
	if err := unmarshalRetainedFixture(effect.Payload, &body); err != nil {
		t.Fatal(err)
	}
	r.in = stateEngineInput{Effect: effect.ID}
	if err := r.startEffect(); err != nil {
		t.Fatal(err)
	}
	permit := &pb.SendPermit{Authorization: body.Authorization, Flow: body.Flow, DirectoryEpoch: "directory", OrderingProof: "proof"}
	r.in = stateEngineInput{Effect: effect.ID, Executor: r.engine.cfg.Process, Reply: &pb.Envelope{Payload: &pb.Envelope_SendPermit{SendPermit: permit}}}
	if err := r.effectResult(); err != nil {
		t.Fatal(err)
	}
	r.in = stateEngineInput{Write: &pb.FlowWrite{Flow: body.Flow, Effect: body.Authorization.Effect, Permit: permit}, Executor: body.Flow.Ingress}
	if err := r.reserve(); err != nil {
		t.Fatal(err)
	}
	c.Cancelled = true
	if err := r.cancelUnstartedEffects(c); err != nil {
		t.Fatal(err)
	}
	_, after, _, err := r.findEffect(effect.ID)
	if err != nil {
		t.Fatal(err)
	}
	if after.Status != state.EffectDispatching || after.AttemptID != body.Authorization.Effect.AttemptId {
		t.Fatal("cancellation reclassified an uncertain physical start")
	}
}
