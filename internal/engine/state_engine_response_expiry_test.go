package engine

import (
	"context"
	"testing"
	"time"

	"github.com/emiago/sipgo/sip"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/state"
)

func TestStateRespondPublicationExpiryRequiresPhysicalFailureResponse(t *testing.T) {
	for _, outcome := range []pb.DeliveryResult_Outcome{pb.DeliveryResult_WRITTEN, pb.DeliveryResult_UNKNOWN} {
		t.Run(outcome.String(), func(t *testing.T) {
			r, c, tx := serverFinalFixture(t, sip.SUBSCRIBE)
			c.Subscription = &stateSubscription{Event: "dialog", Target: "sip:phone@accepted.example.test", ExpiresMS: r.now + 60000}
			c.Snapshot.Kind = pb.ContextSnapshot_SUBSCRIPTION
			c.Delivery = &pb.ContextDelivery{Flow: c.Flow, NativeRevision: 1, DirectoryEpoch: "directory"}
			c.DeliveryRevision = 1
			advertiseCommand(t, r, c, &pb.Command{Sequence: 1, Action: &pb.Command_Respond{Respond: &pb.Respond{RequestId: tx.RequestID, Status: 202}}})
			effect := r.payload.Effects[len(r.payload.Effects)-1]
			if effect.Kind != "adopt" {
				t.Fatal("fixture must wait on adoption")
			}
			r.now = effect.DeadlineMS + 1
			r.in = stateEngineInput{Timer: r.timerID("group", "effects", "recovery", "")}
			if handled, err := r.lifecycleTimer(); !handled || err != nil {
				t.Fatalf("recovery: %v %v", handled, err)
			}
			if c.Result.Certainty != pb.OperationResult_ACCEPTED || c.Result.SipStatus != 503 || c.Result.Error == "" {
				t.Fatalf("publication expiry guessed response delivery: %s", c.Result)
			}
			selectedError := c.Result.Error
			completeLatestResponse(t, r, outcome)
			want := pb.OperationResult_COMPLETED
			if outcome == pb.DeliveryResult_UNKNOWN {
				want = pb.OperationResult_UNKNOWN
			}
			if c.Result.Certainty != want || c.Result.SipStatus != 503 || c.Result.Error != selectedError {
				t.Fatalf("physical result lost selected refusal: %s", c.Result)
			}
		})
	}
}

func TestStateRespondWaitExpiryPreservesLatestPersistedRefusal(t *testing.T) {
	engine, _ := historicalStateTestEngine(t)
	r, c, _ := serverFinalFixture(t, sip.OPTIONS)
	r.engine = engine
	r.group.Version = 1
	r.group.Retired = make(map[string]int64)
	r.now = time.Now().UnixMilli()
	c.Operation = "response-operation"
	c.Result = &pb.OperationResult{Certainty: pb.OperationResult_ACCEPTED, SipStatus: 503, Error: "context publication refused"}
	ref, err := engine.store.GroupForCallID("latest-respond-refusal")
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
	payload, err := r.finish()
	if err != nil {
		t.Fatal(err)
	}
	if _, err = engine.store.(*state.Store).Commit(ctx, *begun.Step, payload, nil); err != nil {
		t.Fatal(err)
	}
	wait, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	result, err := engine.awaitOperation(wait, r.record.Ref, c.Snapshot.Context.Id, c.Operation, &pb.OperationResult{Certainty: pb.OperationResult_ACCEPTED}, true)
	if err != nil || result.Certainty != pb.OperationResult_UNKNOWN || result.SipStatus != 503 || result.Error != "context publication refused" {
		t.Fatalf("wait expiry lost latest selected refusal: %s %v", result, err)
	}
}
