package engine

import (
	"encoding/json"
	"strconv"
	"testing"

	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/state"
)

func completeLatestResponse(t *testing.T, r *stateReduction, outcome pb.DeliveryResult_Outcome) {
	t.Helper()
	for index := len(r.payload.Effects) - 1; index >= 0; index-- {
		effect := r.payload.Effects[index]
		if effect.Kind != "wire" || effect.Status != state.EffectPending {
			continue
		}
		var body stateEngineEffect
		if err := json.Unmarshal(effect.Payload, &body); err != nil {
			t.Fatal(err)
		}
		write := causalPermit(r, index, effect, body)
		r.result = &pb.OperationResult{Certainty: pb.OperationResult_COMPLETED}
		r.in = stateEngineInput{Write: write, Executor: write.Flow.Ingress}
		if err := r.reserve(); err != nil {
			t.Fatal(err)
		}
		r.in.Delivery = &pb.DeliveryResult{Effect: write.Effect, Outcome: outcome}
		if err := r.complete(); err != nil {
			t.Fatal(err)
		}
		return
	}
	t.Fatal("response intent missing")
}

// A retransmission is another physical attempt for an already chosen final.
// Its failure cannot revoke a prior successful write of that same final.
func TestStateWrittenFinalResponseCannotBeDowngradedByLaterReplayDelivery(t *testing.T) {
	for _, outcome := range []pb.DeliveryResult_Outcome{pb.DeliveryResult_NOT_WRITTEN, pb.DeliveryResult_UNKNOWN} {
		t.Run(outcome.String(), func(t *testing.T) {
			r, c, tx := advertiseFixture(t, "tls", sip.INVITE, "")
			advertiseCommand(t, r, c, &pb.Command{Sequence: 1, Action: &pb.Command_Respond{Respond: &pb.Respond{RequestId: tx.RequestID, Status: 200, Body: []byte("v=0\r\n")}}})
			completeLatestResponse(t, r, pb.DeliveryResult_WRITTEN)
			if c.Result.Certainty != pb.OperationResult_COMPLETED || c.Result.Error != "" {
				t.Fatal("first written final did not complete Respond")
			}
			if err := r.send(c, tx, *tx.Native.LastResponse, true); err != nil {
				t.Fatal(err)
			}
			completeLatestResponse(t, r, outcome)
			if c.Result.Certainty != pb.OperationResult_COMPLETED || c.Result.Error != "" {
				t.Fatalf("later retransmission revoked written final: %+v", c.Result)
			}
		})
	}
}

func TestStateSubscriptionPublicationRefusalRemainsFailureAfterResponseWrite(t *testing.T) {
	for _, expires := range []int{180, 0} {
		t.Run(strconv.Itoa(expires), func(t *testing.T) {
			r, c, tx := advertiseFixture(t, "tls", sip.SUBSCRIBE, "")
			c.Subscription = &stateSubscription{Event: "dialog", Target: "sip:phone@accepted.example.test", ExpiresMS: r.now + 60000}
			c.Snapshot.Kind = pb.ContextSnapshot_SUBSCRIPTION
			c.Delivery = &pb.ContextDelivery{Flow: c.Flow, NativeRevision: 1, DirectoryEpoch: "directory"}
			c.DeliveryRevision = 1
			previous := proto.Clone(c.Delivery).(*pb.ContextDelivery)
			previousExpiry := c.Subscription.ExpiresMS
			advertiseCommand(t, r, c, &pb.Command{Sequence: 1, Action: &pb.Command_Respond{Respond: &pb.Respond{RequestId: tx.RequestID, Status: 202, Headers: []*pb.Header{{Name: "Expires", Value: strconv.Itoa(expires)}}}}})
			index := len(r.payload.Effects) - 1
			effect := r.payload.Effects[index]
			var body stateEngineEffect
			if err := json.Unmarshal(effect.Payload, &body); err != nil || effect.Kind != "adopt" {
				t.Fatalf("refresh did not wait for publication: %s %v", effect.Kind, err)
			}
			r.in = stateEngineInput{Reply: &pb.Envelope{Payload: &pb.Envelope_FlowAdoptionResult{FlowAdoptionResult: &pb.FlowAdoptionResult{Error: "receipt superseded"}}}}
			if err := r.finishFlowAdoption(index, effect, body, c); err != nil {
				t.Fatal(err)
			}
			if response := advertiseWire(t, r, sip.SUBSCRIBE, true).(*sip.Response); response.StatusCode != 503 {
				t.Fatalf("publication refusal selected %d", response.StatusCode)
			}
			if c.Result.Certainty != pb.OperationResult_ACCEPTED || c.Result.Error == "" {
				t.Fatalf("native reliable transaction termination guessed delivery: %+v", c.Result)
			}
			completeLatestResponse(t, r, pb.DeliveryResult_WRITTEN)
			if c.Result.Certainty != pb.OperationResult_COMPLETED || c.Result.SipStatus != 503 || c.Result.Error == "" {
				t.Fatalf("writing refusal acknowledged requested202: %+v", c.Result)
			}
			if !proto.Equal(c.Delivery, previous) || c.Subscription.ExpiresMS != previousExpiry || c.Subscription.Terminating || tx.PendingResponse != nil {
				t.Fatal("refused refresh changed the prior accepted subscription or receipt")
			}
		})
	}
}

func TestStateMatchedClientFinalCannotBeDowngradedByLateWriteOutcome(t *testing.T) {
	for _, outcome := range []pb.DeliveryResult_Outcome{pb.DeliveryResult_NOT_WRITTEN, pb.DeliveryResult_UNKNOWN} {
		t.Run(outcome.String(), func(t *testing.T) {
			r, c, tx := advertiseFixture(t, "tls", sip.OPTIONS, "")
			native, err := portable.NewTransaction(portable.NonInviteClient, tx.Native.Origin, portable.DefaultTimers(), "")
			if err != nil {
				t.Fatal(err)
			}
			tx.Native, tx.Command, c.Operation = native, "options", "options"
			r.result = &pb.OperationResult{Certainty: pb.OperationResult_COMPLETED}
			c.Result = &pb.OperationResult{Certainty: pb.OperationResult_ACCEPTED}
			if err := r.stepTransaction(c, tx, portable.Input{Kind: portable.Start, Now: r.now}); err != nil {
				t.Fatal(err)
			}
			index, effect, body := causalEffect(t, r, "OPTIONS", "")
			write := causalPermit(r, index, effect, body)
			r.in = stateEngineInput{Write: write, Executor: write.Flow.Ingress}
			if err := r.reserve(); err != nil {
				t.Fatal(err)
			}
			if err := r.receivedResponse(c, tx, forkReply(t, tx, 200, "remote")); err != nil {
				t.Fatal(err)
			}
			if c.Result.Certainty != pb.OperationResult_COMPLETED || c.Result.GetResponse().GetStatus() != 200 {
				t.Fatal("matched successful final did not complete the client command")
			}
			r.in = stateEngineInput{Write: write, Executor: write.Flow.Ingress, Delivery: &pb.DeliveryResult{Effect: write.Effect, Outcome: outcome}}
			if err := r.complete(); err != nil {
				t.Fatal(err)
			}
			if c.Result.Certainty != pb.OperationResult_COMPLETED || c.Result.Error != "" || c.Result.GetResponse().GetStatus() != 200 {
				t.Fatalf("late write outcome revoked matched final: %+v", c.Result)
			}
		})
	}
}
