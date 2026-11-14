package engine

import (
	"context"
	"errors"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/state"
)

func TestResidentReliableRegisterReleasesAfterAcknowledgmentAndLastWaiter(t *testing.T) {
	for _, scenario := range []struct {
		name              string
		pinned, ownerLost bool
	}{{name: "no-waiter"}, {name: "replay-waiter", pinned: true}, {name: "owner-lost", ownerLost: true}} {
		t.Run(scenario.name, func(t *testing.T) {
			e, peer, conn, ctx, session, _, count := residentControlFixture(t, true)
			flow := residentDirectFlow(e)
			request := residentDirectRequest(t, e, peer, conn, ctx, flow, stateTestRequest("REGISTER", 1, "", "release-register"))
			c, err := e.findContext(request.Snapshot.Context)
			if err != nil {
				t.Fatal(err)
			}
			settled, acknowledge := make(chan *pb.FlowWrite, 1), make(chan struct{})
			t.Cleanup(func() {
				select {
				case <-acknowledge:
				default:
					close(acknowledge)
				}
			})
			e.cfg.WriteCommitted = func(ctx context.Context, write *pb.FlowWrite) *pb.DeliveryResult {
				reserved, err := e.Reserve(ctx, write)
				if err != nil || reserved == nil || reserved.Known || len(reserved.Wire) == 0 {
					t.Error("selected final was not authorized exactly once", reserved, err)
					return nil
				}
				count.Add(1)
				result := &pb.DeliveryResult{Effect: write.Effect, Outcome: pb.DeliveryResult_WRITTEN, Recorded: true}
				if err = e.Complete(ctx, write, result); err != nil {
					t.Error(err)
					return nil
				}
				settled <- write
				<-acknowledge
				return result
			}
			decision := residentDirectDecision(request, 1, 200)
			type outcome struct {
				result *pb.OperationResult
				err    error
			}
			results := make(chan outcome, 1)
			go func() {
				result, err := e.decideResidentRequest(ctx, peer, decision, "completed-register", time.Now().Add(2*time.Second))
				results <- outcome{result, err}
			}()
			var write *pb.FlowWrite
			select {
			case write = <-settled:
			case <-ctx.Done():
				t.Fatal("REGISTER did not settle", ctx.Err())
			}
			c.mu.Lock()
			output := c.outputs[write.Effect.Id]
			if output == nil || !output.settled || c.snapshot.Ended {
				c.mu.Unlock()
				t.Fatal("unacknowledged final lost its context or output")
			}
			receipt := c.command
			ownerKey := c.ownerKey
			if scenario.pinned {
				receipt.replayWaiters++
				c.receiptPins++
				retainResidentCommandLocked(receipt)
			}
			c.mu.Unlock()
			if scenario.ownerLost {
				e.loseResidentOwner(c, ownerKey)
			}
			close(acknowledge)
			completed := <-results
			result := completed.result
			if completed.err != nil || result == nil || result.Certainty != pb.OperationResult_ACCEPTED || result.SipStatus != 200 || result.Snapshot == nil || result.Snapshot.Ended {
				t.Fatal("retirement replaced the original detached commit result", result, completed.err)
			}
			if scenario.pinned {
				if _, err = e.findContext(request.Snapshot.Context); err != nil {
					t.Fatal("active replay waiter lost its context", err)
				}
				replay, replayErr := awaitResidentReplay(ctx, c, receipt)
				if replayErr != nil || replay.Certainty != pb.OperationResult_ACCEPTED || replay.Snapshot == nil || replay.Snapshot.Ended {
					t.Fatal("last waiter lost its detached historical result", replay, replayErr)
				}
			}
			if _, err = e.findContext(request.Snapshot.Context); !errors.Is(err, state.ErrNotFound) {
				t.Fatal("completed reliable REGISTER retained its context", err)
			}
			// An owner-expiry callback may already have copied this context before
			// retirement. It cannot recreate work for the removed incarnation.
			e.loseResidentOwner(c, ownerKey)
			e.mu.Lock()
			for key := range e.deadlines.byKey {
				if key.contextID == request.Snapshot.Context.Id {
					t.Error("completed REGISTER retained a deadline", key)
				}
			}
			for selector, indexed := range e.transactions {
				if indexed == c {
					t.Error("completed REGISTER retained a transaction selector", selector)
				}
			}
			e.mu.Unlock()
			forgotten := residentSendDecision(t, ctx, conn, session, "completed-register", decision)
			if forgotten.Certainty != pb.OperationResult_UNKNOWN || forgotten.Snapshot != nil || count.Load() != 1 {
				t.Fatal("forgotten decision claimed non-acceptance or repeated its write", forgotten)
			}
			assertResidentForgottenOutput(t, e, write)
			inspection, err := e.inspectResident(ctx, peer.Application(), request.Snapshot.Context)
			if err != nil || inspection.RetirementProof == nil || inspection.Snapshot != nil {
				t.Fatal("inspection did not prove only current absence", inspection, err)
			}
			fresh := residentDirectRequest(t, e, peer, conn, ctx, flow, stateTestRequest("REGISTER", 1, "", "release-register"))
			if proto.Equal(fresh.Snapshot.Context, request.Snapshot.Context) {
				t.Fatal("a fresh REGISTER reused the old command authority")
			}
		})
	}
}
