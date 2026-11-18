package engine

import (
	"context"
	"testing"
	"time"

	"github.com/emiago/sipgo/sip/portable"
	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
)

func residentOutputLifetimeFixture(t *testing.T) (*ResidentMemory, *residentContext, *pb.FlowWrite, *residentCommand) {
	t.Helper()
	e, c, tx := residentSubscriptionFixture(t)
	e.ctx, e.maxBytes, e.deadlineWake = context.Background(), 64<<20, make(chan struct{}, 1)
	c.transactions = map[string]*residentTransaction{tx.native.Key: tx}
	c.requests = map[string]string{tx.requestID: tx.native.Key}
	c.dialogs, c.outputs = make(map[string]*residentDialog), make(map[string]*residentOutput)
	c.subscription = &stateSubscription{Event: "message-summary", ExpiresMS: time.Now().Add(time.Hour).UnixMilli()}
	e.contexts = map[string]*residentContext{c.snapshot.Context.Id: c}
	if err := e.accountResidentLocked(c, residentContextBytes(c)+residentSubscriptionBytes(c.subscription), 0); err != nil {
		t.Fatal(err)
	}
	var err error
	c.budget, err = e.openResidentBudgetLocked(c, residentSubscriptionBudget)
	if err != nil {
		t.Fatal(err)
	}
	c.subscriptionBudget, tx.budget = c.budget, c.budget
	tx.charged = residentTransactionBytes(tx)
	tx.borrowed, err = e.chargeResidentBudgetLocked(c, tx.budget, tx.charged)
	if err != nil {
		t.Fatal(err)
	}
	receipt := newResidentCommand("response", 1)
	c.command, tx.command = receipt, receipt
	response, err := portable.BuildResponseWithHeaders(tx.native.Origin, 202, "Accepted", nil, c.snapshot.LocalTag, e.cfg.ContactURI, nil)
	if err != nil {
		t.Fatal(err)
	}
	writes, err := e.stageResidentOutputsLocked(c, tx, []portable.Effect{{Kind: portable.Send, Message: &response}}, nil, "response", time.Now().Add(time.Second))
	if err != nil || len(writes) != 1 {
		t.Fatal("output staging failed", err)
	}
	return e, c, writes[0], receipt
}

func assertResidentForgottenOutput(t *testing.T, e *ResidentMemory, write *pb.FlowWrite) {
	t.Helper()
	known, err := e.Reserve(context.Background(), write)
	if err != nil || known == nil || !known.Known || known.Outcome != pb.DeliveryResult_UNKNOWN || len(known.Wire) != 0 {
		t.Fatal("forgotten output granted wire or a proven absence", known, err)
	}
}

func popResidentOutputStart(t *testing.T, e *ResidentMemory, c *residentContext, write *pb.FlowWrite) residentDeadline {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	output := c.outputs[write.Effect.Id]
	output.deadline = time.Now().Add(-time.Second)
	e.mu.Lock()
	defer e.mu.Unlock()
	key := residentDeadlineKey{kind: residentOutputDeadline, contextID: c.snapshot.Context.Id, record: write.Effect.Id}
	e.deadlines.set(key, 1, output.deadline)
	item, exists := e.deadlines.popDue(time.Now())
	if !exists || item.key != key {
		t.Fatal("expected exact output deadline")
	}
	return item
}

func TestResidentOutputAcknowledgmentReleasesOnlyAfterSettlement(t *testing.T) {
	for _, outcome := range []pb.DeliveryResult_Outcome{pb.DeliveryResult_WRITTEN, pb.DeliveryResult_NOT_WRITTEN} {
		t.Run(outcome.String(), func(t *testing.T) {
			e, c, write, receipt := residentOutputLifetimeFixture(t)
			settled, release, done := make(chan struct{}), make(chan struct{}, 1), make(chan struct{})
			t.Cleanup(func() {
				select {
				case release <- struct{}{}:
				default:
				}
			})
			e.cfg.WriteCommitted = func(ctx context.Context, write *pb.FlowWrite) *pb.DeliveryResult {
				if outcome == pb.DeliveryResult_WRITTEN {
					if reserved, err := e.Reserve(ctx, write); err != nil || reserved.Known || len(reserved.Wire) == 0 {
						t.Error("first physical attempt not authorized", reserved, err)
					}
				}
				result := &pb.DeliveryResult{Effect: write.Effect, Outcome: outcome, Recorded: true}
				if err := e.Complete(ctx, write, result); err != nil {
					t.Error(err)
				}
				close(settled)
				<-release
				return result
			}
			go func() { e.dispatchResident(context.Background(), []*pb.FlowWrite{write}); close(done) }()
			<-settled
			c.mu.Lock()
			output := c.outputs[write.Effect.Id]
			retained := output != nil && output.settled
			c.mu.Unlock()
			if !retained {
				t.Fatal("settlement discarded an unacknowledged dispatcher instruction")
			}
			e.mu.Lock()
			key := residentDeadlineKey{kind: residentOutputDeadline, contextID: c.snapshot.Context.Id, record: write.Effect.Id}
			original := e.deadlines.byKey[key]
			startRetained := original != nil && original.generation == 1
			e.mu.Unlock()
			if !startRetained {
				t.Fatal("ordinary settlement replaced its original deadline unnecessarily")
			}
			release <- struct{}{}
			<-done
			c.mu.Lock()
			present := c.outputs[write.Effect.Id] != nil
			c.command = newResidentCommand("newer", 2)
			c.mu.Unlock()
			if present {
				t.Fatal("acknowledged definite output retained its proof")
			}
			assertResidentForgottenOutput(t, e, write)
			result, err := awaitResidentCommand(context.Background(), c, receipt)
			if err != nil || !receipt.finished || result.SipStatus != 202 || outcome == pb.DeliveryResult_WRITTEN && result.Certainty != pb.OperationResult_COMPLETED {
				t.Fatal("proof release discarded the original command waiter's result", result, err)
			}
		})
	}
}

func TestResidentOutputAcknowledgmentFallbackAndNoWriter(t *testing.T) {
	for _, missingWriter := range []bool{false, true} {
		e, c, write, _ := residentOutputLifetimeFixture(t)
		if !missingWriter {
			e.cfg.WriteCommitted = func(context.Context, *pb.FlowWrite) *pb.DeliveryResult {
				return &pb.DeliveryResult{Effect: write.Effect, Outcome: pb.DeliveryResult_NOT_WRITTEN}
			}
		}
		e.dispatchResident(context.Background(), []*pb.FlowWrite{write})
		if len(c.outputs) != 0 {
			t.Fatal("local definite no-start was not settled and acknowledged")
		}
		assertResidentForgottenOutput(t, e, write)
	}
}

func TestResidentOutputUnknownAndFailedSettlementStay(t *testing.T) {
	for _, failed := range []bool{false, true} {
		e, c, write, _ := residentOutputLifetimeFixture(t)
		if failed {
			c.outputs[write.Effect.Id].method, c.outputs[write.Effect.Id].dialog = "ACK", "missing-dialog"
		}
		e.cfg.WriteCommitted = func(ctx context.Context, write *pb.FlowWrite) *pb.DeliveryResult {
			if _, err := e.Reserve(ctx, write); err != nil {
				t.Error(err)
			}
			outcome := pb.DeliveryResult_UNKNOWN
			if failed {
				outcome = pb.DeliveryResult_WRITTEN
			}
			result := &pb.DeliveryResult{Effect: write.Effect, Outcome: outcome, Recorded: true}
			err := e.Complete(ctx, write, result)
			if (err != nil) != failed {
				t.Error("unexpected settlement result", err)
			}
			return result
		}
		e.dispatchResident(context.Background(), []*pb.FlowWrite{write})
		output := c.outputs[write.Effect.Id]
		if output == nil || output.settled {
			t.Fatal("uncertain or unsuccessfully settled output was forgotten")
		}
		if !failed {
			assertResidentForgottenOutput(t, e, write)
		}
	}
}

func TestResidentOutputNoAckAndLateSettlementKeepBoundedCutoff(t *testing.T) {
	for _, late := range []bool{false, true} {
		e, c, write, _ := residentOutputLifetimeFixture(t)
		result := &pb.DeliveryResult{Effect: write.Effect, Outcome: pb.DeliveryResult_WRITTEN, Recorded: true}
		e.cfg.WriteCommitted = func(ctx context.Context, write *pb.FlowWrite) *pb.DeliveryResult {
			if _, err := e.Reserve(ctx, write); err != nil {
				t.Error(err)
			}
			if !late {
				if err := e.Complete(ctx, write, result); err != nil {
					t.Error(err)
				}
			}
			return nil // no dispatcher acknowledgment, even if Complete succeeded
		}
		e.dispatchResident(context.Background(), []*pb.FlowWrite{write})
		if c.outputs[write.Effect.Id] == nil {
			t.Fatal("nil dispatcher return acknowledged an output")
		}
		item := popResidentOutputStart(t, e, c, write)
		e.fireResidentOutputDeadline(item)
		if late {
			if c.outputs[write.Effect.Id].state != residentOutputUnknown {
				t.Fatal("missed started completion lost its uncertainty")
			}
			if err := e.Complete(context.Background(), write, result); err != nil {
				t.Fatal(err)
			}
		}
		e.mu.Lock()
		cutoff := e.deadlines.byKey[item.key]
		bounded := cutoff != nil && cutoff.generation == 2 && cutoff.at.Equal(c.outputs[write.Effect.Id].deadline.Add(2*time.Minute))
		e.mu.Unlock()
		if !bounded {
			t.Fatal("unacknowledged or late definite completion lost its original cutoff")
		}
	}
}

func TestResidentPoppedOutputDeadlineCannotResurrectReleasedProof(t *testing.T) {
	e, c, write, _ := residentOutputLifetimeFixture(t)
	if _, err := e.Reserve(context.Background(), write); err != nil {
		t.Fatal(err)
	}
	item := popResidentOutputStart(t, e, c, write)
	if err := e.Complete(context.Background(), write, &pb.DeliveryResult{Effect: write.Effect, Outcome: pb.DeliveryResult_WRITTEN}); err != nil {
		t.Fatal(err)
	}
	e.acknowledgeResidentOutput(write)
	e.fireResidentOutputDeadline(item)
	e.mu.Lock()
	resurrected := e.deadlines.byKey[item.key] != nil
	e.mu.Unlock()
	if resurrected || len(c.outputs) != 0 {
		t.Fatal("popped deadline resurrected a released physical proof")
	}
	assertResidentForgottenOutput(t, e, write)
}

func TestResidentForgottenProofRefusesLiveAbsentAndRecycledContexts(t *testing.T) {
	e, c, write, _ := residentOutputLifetimeFixture(t)
	extant := proto.Clone(write).(*pb.FlowWrite)
	extant.Flow.ConnectionId = "altered-flow"
	if _, err := e.Reserve(context.Background(), extant); err == nil {
		t.Fatal("altered extant flow accepted")
	}
	extant = proto.Clone(write).(*pb.FlowWrite)
	extant.Effect.AttemptId = "altered-attempt"
	if _, err := e.Reserve(context.Background(), extant); err == nil {
		t.Fatal("altered extant attempt accepted")
	}
	extant = proto.Clone(write).(*pb.FlowWrite)
	extant.Permit.OrderingProof = "altered-permit"
	if _, err := e.Reserve(context.Background(), extant); err == nil {
		t.Fatal("altered extant permit accepted")
	}
	e.cfg.WriteCommitted = nil
	e.dispatchResident(context.Background(), []*pb.FlowWrite{write})
	assertResidentForgottenOutput(t, e, write) // live context, forgotten output
	e.mu.Lock()
	delete(e.contexts, c.snapshot.Context.Id)
	e.mu.Unlock()
	assertResidentForgottenOutput(t, e, write) // valid context itself absent
	c.mu.Lock()
	known, err := e.reserveResidentOutputLocked(c, write) // pointer selected before retirement
	c.mu.Unlock()
	if err != nil || !known.Known || known.Outcome != pb.DeliveryResult_UNKNOWN || len(known.Wire) != 0 {
		t.Fatal("retired pointer granted new permission", known, err)
	}
	replacement := &residentContext{snapshot: proto.Clone(c.snapshot).(*pb.ContextSnapshot), outputs: make(map[string]*residentOutput)}
	replacement.snapshot.Context.State.Incarnation = "replacement-incarnation"
	e.mu.Lock()
	e.contexts[c.snapshot.Context.Id] = replacement
	e.mu.Unlock()
	assertResidentForgottenOutput(t, e, write) // recycled bare ID, old valid incarnation
	// An extant effect under that ID still requires its exact current proof;
	// an old incarnation cannot turn a colliding current effect into absence.
	replacement.outputs[write.Effect.Id] = &residentOutput{}
	if _, err := e.Reserve(context.Background(), write); err == nil {
		t.Fatal("old incarnation accepted against an extant replacement effect")
	}
	foreign := proto.Clone(write).(*pb.FlowWrite)
	foreign.Effect.Context.State.Authority.PrimaryId = "foreign-authority"
	if _, err := e.Reserve(context.Background(), foreign); err == nil {
		t.Fatal("foreign authority accepted")
	}
	malformed := proto.Clone(write).(*pb.FlowWrite)
	malformed.Effect.Context.State = nil
	if _, err := e.Reserve(context.Background(), malformed); err == nil {
		t.Fatal("malformed authority accepted")
	}
}
