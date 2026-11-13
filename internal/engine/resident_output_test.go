package engine

import (
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
)

func TestResidentOutputAttemptEvidence(t *testing.T) {
	now := time.Unix(1000, 0)
	effect := &pb.StateEffectRef{Context: &pb.ContextRef{Id: "ctx"}, Id: "effect", AttemptId: "attempt"}
	flow := &pb.FlowRef{ConnectionId: "connection"}
	write := &pb.FlowWrite{Effect: effect, Flow: flow, Permit: &pb.SendPermit{Flow: flow, Authorization: &pb.EffectAuthorization{Effect: effect, DeadlineMs: uint64(now.Add(time.Second).UnixMilli())}}}
	wire := []byte("wire")
	o, err := newResidentOutput(write, wire, now.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	wire[0] = 'x'
	write.Flow.ConnectionId = "changed"
	correct := proto.Clone(o.write).(*pb.FlowWrite)
	if _, err := o.reserve(write, now); err == nil {
		t.Fatal("accepted different flow")
	}
	reserved, err := o.reserve(correct, now)
	if err != nil || string(reserved.Wire) != "wire" || reserved.Known {
		t.Fatalf("reserve: %+v %v", reserved, err)
	}
	again, err := o.reserve(correct, now)
	if err != nil || !again.Known || again.Outcome != pb.DeliveryResult_UNKNOWN {
		t.Fatalf("second reserve: %+v %v", again, err)
	}
	if err := o.complete(correct, &pb.DeliveryResult{Outcome: pb.DeliveryResult_UNKNOWN, Effect: correct.Effect}); err != nil {
		t.Fatal(err)
	}
	if err := o.complete(correct, &pb.DeliveryResult{Outcome: pb.DeliveryResult_WRITTEN, Effect: correct.Effect}); err != nil {
		t.Fatal(err)
	}
	if err := o.complete(correct, &pb.DeliveryResult{Outcome: pb.DeliveryResult_NOT_WRITTEN, Effect: correct.Effect}); err == nil {
		t.Fatal("accepted conflicting terminal evidence")
	}
	again, err = o.reserve(correct, now.Add(2*time.Second))
	if err != nil || !again.Known || again.Outcome != pb.DeliveryResult_WRITTEN {
		t.Fatalf("known evidence: %+v %v", again, err)
	}
}

func TestResidentOutputUnreservedNoStartRequiresExactProof(t *testing.T) {
	now := time.Unix(1000, 0)
	effect := &pb.StateEffectRef{Context: &pb.ContextRef{Id: "ctx"}, Id: "e", AttemptId: "a"}
	flow := &pb.FlowRef{ConnectionId: "flow"}
	write := &pb.FlowWrite{Effect: effect, Flow: flow, Permit: &pb.SendPermit{Flow: flow, Authorization: &pb.EffectAuthorization{Effect: effect, DeadlineMs: uint64(now.Add(time.Second).UnixMilli())}}}
	o, err := newResidentOutput(write, []byte("wire"), now.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if err := o.complete(write, &pb.DeliveryResult{Outcome: pb.DeliveryResult_NOT_WRITTEN}); err == nil {
		t.Fatal("accepted missing absence proof")
	}
	if err := o.complete(write, &pb.DeliveryResult{Outcome: pb.DeliveryResult_NOT_WRITTEN, Effect: effect}); err != nil {
		t.Fatal(err)
	}
}
