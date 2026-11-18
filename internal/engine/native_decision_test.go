package engine

import (
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
)

func TestNativeDecisionCandidateDoesNotMutateInputs(t *testing.T) {
	_, c, tx := residentSubscriptionFixture(t)
	now := time.Unix(1700000000, 0)
	c.snapshot.ApplicationLeaseTerm = "lease"
	c.snapshot.DirectoryEpoch = "directory"
	flow := c.flow
	decision := &pb.RequestDecision{Status: 202, Basis: pb.OwnerIngest_EXISTING_FLOW, Notify: &pb.DialogRequest{Method: "NOTIFY"}, Admission: &pb.CommandAdmission{DirectoryEpoch: "directory", ExpiresAtMs: uint64(now.Add(time.Minute).UnixMilli()), SelectedFlow: flow}}
	snapshotBefore := proto.Clone(c.snapshot)
	decisionBefore := proto.Clone(decision)
	flowBefore := proto.Clone(flow)
	input := nativeDecisionInput{decision: decision, transaction: tx.native, direct: true, decisionPending: true, unbound: true, hasSubscription: true, presentDeadline: now.Add(30 * time.Second), now: now, deadline: now.Add(20 * time.Second), application: c.snapshot.Application, leaseTerm: "lease", snapshot: c.snapshot, currentFlow: flow, transactionFlow: flow, operation: "operation"}
	candidate, err := nativePrepareDecision(input)
	if err != nil {
		t.Fatal(err)
	}
	if candidate.policy == nil || candidate.policy.SelectedFlow != nil || candidate.policy.ExpiresAtMs != uint64(input.deadline.UnixMilli()) || !candidate.bindOwner {
		t.Fatal("missing normalized candidate")
	}
	if !proto.Equal(snapshotBefore, c.snapshot) || !proto.Equal(decisionBefore, decision) || !proto.Equal(flowBefore, flow) {
		t.Fatal("candidate mutated caller input")
	}
	bad := proto.Clone(decision).(*pb.RequestDecision)
	bad.Notify = nil
	input.decision = bad
	if _, err = nativePrepareDecision(input); err == nil {
		t.Fatal("accepted subscription without required NOTIFY")
	}
	if !proto.Equal(snapshotBefore, c.snapshot) || !proto.Equal(decisionBefore, decision) || !proto.Equal(flowBefore, flow) {
		t.Fatal("refused candidate mutated caller input")
	}
	bad.Status = 403
	bad.PrincipalId = "account"
	if _, err = nativePrepareDecision(input); err == nil {
		t.Fatal("denial published account authority")
	}
	input.decision = decision
	input.now = input.deadline
	if _, err = nativePrepareDecision(input); err == nil {
		t.Fatal("expired decision accepted")
	}
}
