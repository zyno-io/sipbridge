package engine

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/emiago/sipgo/sip/portable"
	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/state"
)

// Drive the actual client transaction/dialog to its selected successful final,
// retaining an authorized but not-yet-written initial ACK. Earlier INVITE and
// ANSWERED delivery are settled fixture preconditions, not ACK write proof.
func initialUACACKFixture(t *testing.T, r *stateReduction, c *stateContext, tx *stateTransaction) *pb.FlowWrite {
	t.Helper()
	r.group.Version = 1
	r.group.Retired = make(map[string]int64)
	r.result = &pb.OperationResult{Certainty: pb.OperationResult_COMPLETED}
	c.Snapshot.CommandSequence = 1
	c.Snapshot.Answered, c.Snapshot.InitialAck, c.Snapshot.Ended = false, false, false
	c.OwnerValidated, c.OwnerDirectoryEpoch = true, "old-directory"
	c.OwnerProbeAtMS = r.now + 5000
	c.Result = &pb.OperationResult{Certainty: pb.OperationResult_ACCEPTED, Request: mustObserve(tx.Native.Origin.Wire, true)}
	origin := tx.Native.Origin.Clone()
	c.ScopeOrigin, tx.CompletionScope, tx.Application = &origin, c.CompletionScope, c.Snapshot.Application
	if err := r.stepTransaction(c, tx, portable.Input{Kind: portable.Start, Now: r.now}); err != nil {
		t.Fatal(err)
	}
	index, effect, body := causalEffect(t, r, "INVITE", "")
	completeResponseEffectAt(t, r, index, pb.DeliveryResult_WRITTEN)
	index, effect, body, err := r.findEffect(effect.ID)
	if err != nil {
		t.Fatal(err)
	}
	body.Settled = true
	r.setEffect(index, effect, body)
	if err := r.reclaimEffects(); err != nil {
		t.Fatal(err)
	}
	r.in = stateEngineInput{Kind: "ingest"}
	if err := r.stepTransaction(c, tx, portable.Input{Kind: portable.ReceiveResponse, Now: r.now + 1, Message: forkReply(t, tx, 200, "winner")}); err != nil {
		t.Fatal(err)
	}
	for index, effect := range r.payload.Effects {
		if effect.Kind == "event" {
			effect.Status, effect.AttemptID = state.EffectWritten, "fixture-answer-received"
			r.payload.Effects[index] = effect
		}
	}
	if err := r.reclaimEffects(); err != nil {
		t.Fatal(err)
	}
	index, effect, body = causalEffect(t, r, "ACK", "winner")
	return causalPermit(r, index, effect, body)
}

func writeInitialACKFixture(t *testing.T, r *stateReduction, write *pb.FlowWrite) {
	t.Helper()
	r.in = stateEngineInput{Write: write, Executor: write.Flow.Ingress}
	if err := r.reserve(); err != nil {
		t.Fatal(err)
	}
	recordResponseWrite(t, r, write, pb.DeliveryResult_WRITTEN)
}

func TestStateInitialUACWrittenACKCompletesInspectionBeforeTimerM(t *testing.T) {
	engine, _ := historicalStateTestEngine(t)
	r, c, tx := forkFixture(t)
	r.engine, r.now = engine, time.Now().UnixMilli()
	ref, err := engine.store.GroupForCallID(r.record.Ref.CallID)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	begun, err := engine.store.(*state.Store).Begin(ctx, ref, "seed", state.Input{ID: "seed", Kind: "locate", Fingerprint: "seed", Create: true, DeadlineMS: r.now + 2000, ReceiptUntilMS: r.now + 120000})
	if err != nil {
		t.Fatal(err)
	}
	r.record = begun.Step.Record
	c.Snapshot.Context = engine.contextRef(r.record.Ref, c.Snapshot.Context.Id)
	c.Flow.Ingress = proto.Clone(engine.cfg.Process).(*pb.ProcessRef)
	write := initialUACACKFixture(t, r, c, tx)
	m := tx.Native.Timers[portable.TimerM]
	requestWire := append([]byte(nil), tx.Native.Origin.Wire...)
	finalWire := append([]byte(nil), c.Dialogs[c.Dialog].LocalInvite.Final.Wire...)
	payload, err := r.finish()
	if err != nil {
		t.Fatal(err)
	}
	if _, err = engine.store.(*state.Store).Commit(ctx, *begun.Step, payload, nil); err != nil {
		t.Fatal(err)
	}
	inspect := func() *pb.OperationResult {
		t.Helper()
		result, err := engine.inspectContext(ctx, c.Snapshot.Application, c.Snapshot.Context)
		if err != nil {
			t.Fatal(err)
		}
		return result
	}
	if got := inspect(); got.Certainty != pb.OperationResult_ACCEPTED || got.Snapshot.InitialAck || !m.Active {
		t.Fatal("selecting/authorizing the initial ACK acknowledged OpenUAC")
	}
	reserved, err := engine.Reserve(ctx, write)
	if err != nil || reserved.Known || len(reserved.Wire) == 0 {
		t.Fatalf("reserve exact initial ACK: %v", err)
	}
	if got := inspect(); got.Certainty != pb.OperationResult_ACCEPTED || got.Snapshot.InitialAck {
		t.Fatal("physical ACK reservation acknowledged OpenUAC")
	}
	if err := engine.Complete(ctx, write, &pb.DeliveryResult{Effect: write.Effect, Outcome: pb.DeliveryResult_WRITTEN}); err != nil {
		t.Fatal(err)
	}
	completed := inspect()
	if completed.Certainty != pb.OperationResult_COMPLETED || completed.Error != "" || !completed.Snapshot.InitialAck ||
		completed.Snapshot.CommandSequence != 1 || !bytes.Equal(completed.GetRequest().GetWire(), requestWire) ||
		!bytes.Equal(completed.GetResponse().GetWire(), finalWire) {
		t.Fatal("written winning initial ACK did not settle its exact creation operation with selected request/final")
	}
	record, group := stateTestLoad(t, engine, c.Snapshot.Context)
	current := group.Contexts[c.Snapshot.Context.Id]
	retained := current.Transactions[tx.Native.Key]
	if retained.Native.Phase != portable.Accepted || retained.Native.Timers[portable.TimerM] != m || time.Now().UnixMilli() >= m.At ||
		current.Operation != c.Operation || current.Snapshot.CommandSequence != 1 {
		t.Fatal("OpenUAC completion waited for/restarted Timer M or allocated another command")
	}
	// Result completion does not hide the distinct trusted settlement obligation.
	settlement := stateEngineInput{Kind: "effect-result", Effect: write.Effect.Id,
		Reply:    &pb.Envelope{Payload: &pb.Envelope_Result{Result: &pb.OperationResult{Certainty: pb.OperationResult_COMPLETED}}},
		Delivery: &pb.DeliveryResult{Effect: write.Effect, Outcome: pb.DeliveryResult_WRITTEN, Recorded: true}}
	result, err := engine.apply(ctx, record.Ref, "initial-ack-settled", settlement, time.Now().Add(2*time.Second), false)
	if err != nil || result.Error != "" {
		t.Fatalf("settle exact initial ACK: %v", err)
	}
	record, group = stateTestLoad(t, engine, c.Snapshot.Context)
	current = group.Contexts[c.Snapshot.Context.Id]
	if len(record.Payload.Effects) != 0 {
		t.Fatal("settled initial ACK did not compact to bounded native proof")
	}

}

func TestStateInitialUACACKCannotOverwriteAnotherResult(t *testing.T) {
	for _, condition := range []string{"unknown", "later-accepted", "later-completed", "cancelled", "ended"} {
		t.Run(condition, func(t *testing.T) {
			r, c, tx := forkFixture(t)
			write := initialUACACKFixture(t, r, c, tx)
			switch condition {
			case "unknown":
				c.Result.Certainty, c.Result.Error = pb.OperationResult_UNKNOWN, "original physical uncertainty"
			case "later-accepted":
				c.Operation = "later-command"
				c.Result = &pb.OperationResult{Certainty: pb.OperationResult_ACCEPTED, Error: "later command metadata"}
			case "later-completed":
				c.Operation = "later-command"
				c.Result = &pb.OperationResult{Certainty: pb.OperationResult_COMPLETED, SipStatus: 487, Error: "later conclusive result"}
			case "cancelled":
				c.Cancelled = true
			case "ended":
				c.Snapshot.Ended = true
			}
			before := proto.Clone(c.Result)
			writeInitialACKFixture(t, r, write)
			if !proto.Equal(c.Result, before) {
				t.Fatal("late initial ACK replaced an unrelated/uncertain/cancelled operation result")
			}
		})
	}
}

func TestStateInitialUACOnlyWinnerACKCompletesAndDuplicateProofStaysBounded(t *testing.T) {
	r, c, tx := forkFixture(t)
	winner := initialUACACKFixture(t, r, c, tx)
	if _, err := r.forkResponse(c, tx, forkReply(t, tx, 200, "loser")); err != nil {
		t.Fatal(err)
	}
	index, effect, body := causalEffect(t, r, "ACK", "loser")
	loser := causalPermit(r, index, effect, body)
	writeInitialACKFixture(t, r, loser)
	if c.Result.Certainty != pb.OperationResult_ACCEPTED || c.Snapshot.InitialAck {
		t.Fatal("a late fork ACK completed the selected initial operation")
	}
	writeInitialACKFixture(t, r, winner)
	if c.Result.Certainty != pb.OperationResult_COMPLETED || c.Result.GetResponse().GetStatus() != 200 {
		t.Fatal("winner ACK did not retain its successful final")
	}
	completed, sequence := proto.Clone(c.Result), c.Snapshot.CommandSequence
	for attempt := 0; attempt < 3; attempt++ {
		if _, err := r.forkResponse(c, tx, forkReply(t, tx, 200, "winner")); err != nil {
			t.Fatal(err)
		}
		index, effect, body := causalEffect(t, r, "ACK", "winner")
		replay := causalPermit(r, index, effect, body)
		writeInitialACKFixture(t, r, replay)
		if !proto.Equal(c.Result, completed) || c.Snapshot.CommandSequence != sequence || len(c.ACKWritten) != 2 ||
			c.ACKEmissions[c.Dialog].Count > stateProfileTransmissions || len(c.Transactions) != 2 {
			t.Fatal("duplicate final/ACK replaced the operation or escaped native proof/procedure bounds")
		}
	}
	// A new in-dialog INVITE has its own result; writing its ACK cannot replace
	// that result with the original creation request/final.
	r.in = stateEngineInput{Kind: "command", Operation: "reinvite", Application: c.Snapshot.Application,
		Command: &pb.Command{Context: c.Snapshot.Context, Sequence: 2, Action: &pb.Command_DialogRequest{DialogRequest: &pb.DialogRequest{Method: "INVITE", Body: []byte("v=0\r\n")}}}}
	if err := r.command(); err != nil {
		t.Fatal(err)
	}
	var reinvite *stateTransaction
	for _, candidate := range c.Transactions {
		if candidate.Command == "reinvite" {
			reinvite = candidate
		}
	}
	if reinvite == nil {
		t.Fatal("native re-INVITE fixture missing")
	}
	if err := r.stepTransaction(c, reinvite, portable.Input{Kind: portable.ReceiveResponse, Now: r.now + 1, Message: forkReply(t, reinvite, 200, "winner")}); err != nil {
		t.Fatal(err)
	}
	reinviteResult := proto.Clone(c.Result)
	index, effect, body = causalEffect(t, r, "ACK", "winner")
	reinviteACK := causalPermit(r, index, effect, body)
	writeInitialACKFixture(t, r, reinviteACK)
	if !proto.Equal(c.Result, reinviteResult) || c.Operation != "reinvite" || c.Snapshot.CommandSequence != 2 {
		t.Fatal("re-INVITE ACK overwrote its operation result")
	}
}
