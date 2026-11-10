package engine

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"

	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/state"
)

func serverFinalFixture(t *testing.T, method sip.RequestMethod) (*stateReduction, *stateContext, *stateTransaction) {
	t.Helper()
	r, c, tx := advertiseFixture(t, "tls", method, "")
	r.deadline = r.now + 20000
	r.result = &pb.OperationResult{Certainty: pb.OperationResult_COMPLETED}
	parsed, err := tx.Native.Origin.Parse()
	if err != nil {
		t.Fatal(err)
	}
	parsed.Via().Params.Add("rport", "")
	tx.Native.Origin.Wire = []byte(parsed.String())
	c.Origin = tx.Native.Origin.Clone()
	return r, c, tx
}

func latestFinalEffect(t *testing.T, r *stateReduction) (int, state.Effect, stateEngineEffect) {
	t.Helper()
	for index := len(r.payload.Effects) - 1; index >= 0; index-- {
		effect := r.payload.Effects[index]
		var body stateEngineEffect
		if json.Unmarshal(effect.Payload, &body) == nil && body.WireStatus >= 200 {
			return index, effect, body
		}
	}
	t.Fatal("final response effect missing")
	return 0, state.Effect{}, stateEngineEffect{}
}

func completeResponseEffectAt(t *testing.T, r *stateReduction, index int, outcome pb.DeliveryResult_Outcome) {
	t.Helper()
	effect := r.payload.Effects[index]
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
}

func replayRequestOnFlow(t *testing.T, r *stateReduction, c *stateContext, tx *stateTransaction, flow *pb.FlowRef) {
	t.Helper()
	r.in = stateEngineInput{Kind: "ingest", Context: c.Snapshot.Context, Application: c.Snapshot.Application,
		Ingest: &pb.OwnerIngest{Context: c.Snapshot.Context, Application: c.Snapshot.Application, Flow: flow, Wire: tx.Native.Origin.Wire,
			Admission: pb.OwnerIngest_AUTHENTICATED_BINDING, PrincipalId: c.Snapshot.PrincipalId, BindingId: c.Snapshot.BindingId}}
	if err := r.ingest(); err != nil {
		t.Fatal(err)
	}
}

func selectFinal(t *testing.T, r *stateReduction, c *stateContext, tx *stateTransaction, sequence uint64) {
	t.Helper()
	respond := &pb.Respond{RequestId: tx.RequestID, Status: 200}
	if tx.Native.Family == portable.InviteServer {
		respond.Body = []byte("v=0\r\n")
	}
	advertiseCommand(t, r, c, &pb.Command{Sequence: sequence, Action: &pb.Command_Respond{Respond: respond}})
}

func TestStateReliableFinalNoStartRetriesExactRenderedFlowAndWaitsForWrite(t *testing.T) {
	r, c, tx := serverFinalFixture(t, sip.OPTIONS)
	selectFinal(t, r, c, tx, 1)
	_, original, body := latestFinalEffect(t, r)
	retire, deadline := tx.RetireMS, tx.RespondDeadlineMS
	completeLatestResponse(t, r, pb.DeliveryResult_NOT_WRITTEN)
	if tx.Native.Phase != portable.Terminated || tx.FinalResponse.RetryAtMS != r.now+tx.Native.Profile.T1 || c.Result.Certainty != pb.OperationResult_ACCEPTED {
		t.Fatal("positive no-start did not retain the final delivery obligation")
	}
	// A later application route cannot move this already selected response.
	replacement := proto.Clone(tx.Flow).(*pb.FlowRef)
	replacement.ConnectionId, replacement.SourcePort = "replacement-flow", replacement.SourcePort+1
	tx.Flow, c.Flow = replacement, replacement
	r.now = tx.FinalResponse.RetryAtMS - 1
	before := len(r.payload.Effects)
	if err := r.recoverServerResponses(); err != nil || len(r.payload.Effects) != before {
		t.Fatalf("retry ran before its T1 spacing: %v", err)
	}
	r.now++
	if err := r.recoverServerResponses(); err != nil {
		t.Fatal(err)
	}
	_, retry, retried := latestFinalEffect(t, r)
	if retry.ID == original.ID || !bytes.Equal(body.Wire, retried.Wire) || !proto.Equal(body.Flow, retried.Flow) ||
		!bytes.Contains(retried.Wire, []byte("rport=")) || tx.RetireMS != retire || tx.RespondDeadlineMS != deadline || tx.Emissions != 2 {
		t.Fatal("recovery changed final bytes, received/rport, flow, deadlines or finite emission identity")
	}
	if c.Result.Certainty != pb.OperationResult_ACCEPTED {
		t.Fatal("response selection/termination acknowledged delivery")
	}
	completeLatestResponse(t, r, pb.DeliveryResult_WRITTEN)
	if c.Result.Certainty != pb.OperationResult_COMPLETED || !tx.FinalResponse.Written {
		t.Fatal("actual final write did not complete Respond")
	}
}

func TestStateReliableFinalProvisionalWriteCannotSuppressRecovery(t *testing.T) {
	r, c, tx := serverFinalFixture(t, sip.OPTIONS)
	advertiseCommand(t, r, c, &pb.Command{Sequence: 1, Action: &pb.Command_Respond{Respond: &pb.Respond{RequestId: tx.RequestID, Status: 180}}})
	completeLatestResponse(t, r, pb.DeliveryResult_WRITTEN)
	for index, effect := range r.payload.Effects {
		var body stateEngineEffect
		if json.Unmarshal(effect.Payload, &body) == nil {
			body.Settled = true
			r.setEffect(index, effect, body)
		}
	}
	if err := r.reclaimEffects(); err != nil || !tx.Written {
		t.Fatalf("provisional written proof fixture failed: %v", err)
	}
	selectFinal(t, r, c, tx, 2)
	completeLatestResponse(t, r, pb.DeliveryResult_NOT_WRITTEN)
	r.now = tx.FinalResponse.RetryAtMS
	if err := r.recoverServerResponses(); err != nil || tx.Emissions != 3 || tx.FinalResponse.Written {
		t.Fatalf("earlier provisional suppressed final recovery: %v", err)
	}
}

func TestStateReliableFinalGenericWrittenCannotReplaceSelectedProof(t *testing.T) {
	r, c, tx := serverFinalFixture(t, sip.OPTIONS)
	// Reducer4 never treats an untyped transaction bit as proof of this selected response.
	tx.Written = true
	selectFinal(t, r, c, tx, 1)
	completeLatestResponse(t, r, pb.DeliveryResult_NOT_WRITTEN)
	r.now = tx.FinalResponse.RetryAtMS
	if err := r.recoverServerResponses(); err != nil || tx.Emissions != 2 || tx.FinalResponse.Written || c.Result.Certainty != pb.OperationResult_ACCEPTED {
		t.Fatalf("generic written proof suppressed exact selected-response recovery: %v", err)
	}
}

func TestStateReliableFinalUnstartedReplayOnDifferentFlowDoesNotCoalesce(t *testing.T) {
	r, c, tx := serverFinalFixture(t, sip.OPTIONS)
	c.Snapshot.PrincipalId, c.Snapshot.BindingId = "principal", "binding"
	selectFinal(t, r, c, tx, 1)
	firstIndex, first, original := latestFinalEffect(t, r)
	flow := proto.Clone(original.Flow).(*pb.FlowRef)
	flow.ConnectionId = "same-wire-new-flow"
	replayRequestOnFlow(t, r, c, tx, flow)
	secondIndex, second, replay := latestFinalEffect(t, r)
	if first.ID == second.ID || firstIndex == secondIndex || !bytes.Equal(original.Wire, replay.Wire) || proto.Equal(original.Flow, replay.Flow) || !proto.Equal(replay.Flow, flow) {
		t.Fatal("same-wire replay on a different flow coalesced with the original unstarted effect")
	}
	if tx.Emissions != 2 {
		t.Fatal("different-flow replay did not reserve its own bounded native emission")
	}
}

func TestStateReliableFinalPendingReplacementProtectsAcceptedRespondFromOldNoStart(t *testing.T) {
	r, c, tx := serverFinalFixture(t, sip.OPTIONS)
	c.Snapshot.PrincipalId, c.Snapshot.BindingId = "principal", "binding"
	tx.Emissions = stateTransactionEmissions - 2
	tx.ProvisionalEmissions = stateProfileTransmissions
	selectFinal(t, r, c, tx, 1)
	firstIndex, _, _ := latestFinalEffect(t, r)
	flow := proto.Clone(tx.Flow).(*pb.FlowRef)
	flow.ConnectionId = "replacement-pending-flow"
	replayRequestOnFlow(t, r, c, tx, flow)
	_, latest, pending := latestFinalEffect(t, r)
	if !proto.Equal(pending.Flow, flow) || latest.Status != state.EffectPending {
		t.Fatal("replacement final was not left pending on its selected flow")
	}
	completeResponseEffectAt(t, r, firstIndex, pb.DeliveryResult_NOT_WRITTEN)
	if c.Result.Certainty != pb.OperationResult_ACCEPTED || tx.Emissions != stateTransactionEmissions {
		t.Fatalf("late old-flow no-start ended Respond while its last allowed replacement emission remained pending: %+v", c.Result)
	}
	completeLatestResponse(t, r, pb.DeliveryResult_WRITTEN)
	if c.Result.Certainty != pb.OperationResult_COMPLETED {
		t.Fatal("last allowed pending emission did not complete Respond on actual write")
	}
}

func TestStateReliableFinalBlockedRetrySurvivesOldUnknownUntilNoStartResolves(t *testing.T) {
	r, c, tx := serverFinalFixture(t, sip.OPTIONS)
	c.Snapshot.PrincipalId, c.Snapshot.BindingId = "principal", "binding"
	selectFinal(t, r, c, tx, 1)
	oldIndex, _, oldBody := latestFinalEffect(t, r)
	completeResponseEffectAt(t, r, oldIndex, pb.DeliveryResult_UNKNOWN)
	var oldPhysical stateEngineEffect
	if err := json.Unmarshal(r.payload.Effects[oldIndex].Payload, &oldPhysical); err != nil {
		t.Fatal(err)
	}
	flowB := proto.Clone(oldBody.Flow).(*pb.FlowRef)
	flowB.ConnectionId = "proven-no-start-flow-B"
	replayRequestOnFlow(t, r, c, tx, flowB)
	bIndex, bEffect, bBody := latestFinalEffect(t, r)
	if bIndex == oldIndex || !proto.Equal(bBody.Flow, flowB) {
		t.Fatal("explicit replay did not select flow B for the new final emission")
	}
	completeResponseEffectAt(t, r, bIndex, pb.DeliveryResult_NOT_WRITTEN)
	if tx.FinalResponse.RetryAtMS == 0 || !proto.Equal(tx.FinalResponse.Flow, flowB) {
		t.Fatal("flow B positive no-start did not retain its retry permission")
	}
	// A remains physically unknown despite B's independently proven no-start.
	// Keep B's retry obligation until A's exact writer proves its no-start.
	due := tx.FinalResponse.RetryAtMS
	r.now = due
	if err := r.recoverServerResponses(); err != nil {
		t.Fatal(err)
	}
	if tx.Emissions != 2 || tx.FinalResponse.RetryAtMS <= r.now {
		t.Fatal("old A physical UNKNOWN erased flow B's blocked retry schedule")
	}
	r.in = stateEngineInput{Write: &pb.FlowWrite{Effect: oldPhysical.Authorization.Effect, Permit: oldPhysical.Permit, Flow: oldPhysical.Permit.Flow},
		Executor: oldPhysical.Permit.Flow.Ingress, Delivery: &pb.DeliveryResult{Effect: oldPhysical.Authorization.Effect, Outcome: pb.DeliveryResult_NOT_WRITTEN}}
	if err := r.complete(); err != nil {
		t.Fatal(err)
	}
	r.now = tx.FinalResponse.RetryAtMS
	if err := r.recoverServerResponses(); err != nil {
		t.Fatal(err)
	}
	_, retry, retryBody := latestFinalEffect(t, r)
	if retry.ID == bEffect.ID || !proto.Equal(retryBody.Flow, flowB) || tx.Emissions != 3 {
		t.Fatal("resolved A no-start did not resume the already-proven flow B retry")
	}
}

func TestStateReliableFinalInvite2xxNoStartKeepsOriginalNativeRetry(t *testing.T) {
	for _, exhausted := range []bool{false, true} {
		t.Run(map[bool]string{false: "native-retry-live", true: "emission-budget-exhausted"}[exhausted], func(t *testing.T) {
			r, c, tx := serverFinalFixture(t, sip.INVITE)
			if exhausted {
				tx.Emissions = stateTransactionEmissions - 1
				tx.ProvisionalEmissions = stateProfileTransmissions
			}
			selectFinal(t, r, c, tx, 1)
			if tx.RetireMS != 0 {
				t.Fatal("active INVITE fixture unexpectedly retired")
			}
			completeLatestResponse(t, r, pb.DeliveryResult_NOT_WRITTEN)
			want := pb.OperationResult_ACCEPTED
			if exhausted {
				want = pb.OperationResult_UNKNOWN
			}
			if c.Result.Certainty != want || r.serverFinalRetryLive(c, tx) == exhausted {
				t.Fatalf("INVITE positive no-start ignored native retry/budget: %s", c.Result)
			}
		})
	}
}

func TestStateReliableFinalUnknownAndWrittenAnyFlowNeverRetryAutomatically(t *testing.T) {
	for _, status := range []state.EffectStatus{state.EffectDispatching, state.EffectUnknown, state.EffectWritten} {
		for _, legacy := range []bool{false, true} {
			t.Run(string(status)+map[bool]string{true: "/legacy", false: "/typed"}[legacy], func(t *testing.T) {
				r, c, tx := serverFinalFixture(t, sip.OPTIONS)
				selectFinal(t, r, c, tx, 1)
				completeLatestResponse(t, r, pb.DeliveryResult_NOT_WRITTEN)
				_, _, original := latestFinalEffect(t, r)
				other := original
				other.Flow = proto.Clone(original.Flow).(*pb.FlowRef)
				other.Flow.ConnectionId, other.Flow.SourcePort = "explicit-replay-flow", original.Flow.SourcePort+1
				wire, err := stateRenderResponse(*tx.Native.LastResponse, other.Flow)
				if err != nil {
					t.Fatal(err)
				}
				other.Wire, other.Envelope, other.WireHash = nil, nil, stateACKHash(wire.Wire)
				if legacy {
					other.WireStatus = 0
				}
				data, _ := json.Marshal(other)
				r.payload.Effects = append(r.payload.Effects, state.Effect{ID: "other-final", Kind: "deliver", Status: status, DeadlineMS: r.now - 1, Payload: data})
				r.now = tx.FinalResponse.RetryAtMS
				if err := r.recoverServerResponses(); err != nil || tx.Emissions != 1 {
					t.Fatalf("automatic retry crossed another flow's physical evidence: %v", err)
				}
			})
		}
	}
}

func TestStateReliableFinalExplicitReplayMayUseNewFlowDespiteOldUnknown(t *testing.T) {
	r, c, tx := serverFinalFixture(t, sip.OPTIONS)
	c.Snapshot.PrincipalId, c.Snapshot.BindingId = "principal", "binding"
	selectFinal(t, r, c, tx, 1)
	_, original, old := latestFinalEffect(t, r)
	completeLatestResponse(t, r, pb.DeliveryResult_UNKNOWN)
	flow := proto.Clone(tx.Flow).(*pb.FlowRef)
	flow.ConnectionId, flow.SourcePort = "admitted-new-TLS", flow.SourcePort+1
	r.in = stateEngineInput{Kind: "ingest", Context: c.Snapshot.Context, Application: c.Snapshot.Application,
		Ingest: &pb.OwnerIngest{Context: c.Snapshot.Context, Application: c.Snapshot.Application, Flow: flow, Wire: tx.Native.Origin.Wire,
			Admission: pb.OwnerIngest_AUTHENTICATED_BINDING, PrincipalId: c.Snapshot.PrincipalId, BindingId: c.Snapshot.BindingId}}
	if err := r.ingest(); err != nil {
		t.Fatal(err)
	}
	_, replay, body := latestFinalEffect(t, r)
	if replay.ID == original.ID || !proto.Equal(body.Flow, flow) || bytes.Equal(body.Wire, old.Wire) ||
		!proto.Equal(tx.FinalResponse.Flow, flow) || !proto.Equal(c.Flow, old.Flow) {
		t.Fatal("explicit validated replay lost its new return flow or automatically adopted an application route")
	}
	_, retained, _, err := r.findEffect(original.ID)
	if err != nil || retained.Status != state.EffectUnknown || c.Result.Certainty != pb.OperationResult_UNKNOWN {
		t.Fatal("new response replay rewrote the old physical/command uncertainty")
	}
	completeLatestResponse(t, r, pb.DeliveryResult_NOT_WRITTEN)
	r.now = tx.FinalResponse.RetryAtMS
	if err := r.recoverServerResponses(); err != nil || tx.Emissions != 2 {
		t.Fatalf("automatic recovery crossed old-flow UNKNOWN: %v", err)
	}
}

func TestStateReliableFinalSettlementCompactsOnlyFinalWriteProof(t *testing.T) {
	r, c, tx := serverFinalFixture(t, sip.OPTIONS)
	selectFinal(t, r, c, tx, 1)
	completeLatestResponse(t, r, pb.DeliveryResult_WRITTEN)
	index, effect, body := latestFinalEffect(t, r)
	body.Settled = true
	r.setEffect(index, effect, body)
	if err := r.reclaimEffects(); err != nil || len(r.payload.Effects) != 0 || !tx.FinalResponse.Written {
		t.Fatalf("final write proof did not survive settlement compaction: %v", err)
	}
	tx.FinalResponse.RetryAtMS = r.now
	if err := r.recoverServerResponses(); err != nil || tx.Emissions != 1 {
		t.Fatalf("compaction opened automatic final retry: %v", err)
	}
}

func TestStateReliableFinalNoStartEmissionRetirementAndCommandBounds(t *testing.T) {
	for _, bound := range []string{"emissions", "retirement", "deadline"} {
		t.Run(bound, func(t *testing.T) {
			r, c, tx := serverFinalFixture(t, sip.OPTIONS)
			selectFinal(t, r, c, tx, 1)
			retire := tx.RetireMS
			if bound == "emissions" {
				tx.Emissions = stateTransactionEmissions
			}
			completeLatestResponse(t, r, pb.DeliveryResult_NOT_WRITTEN)
			if bound == "retirement" {
				r.now = tx.RetireMS
			} else if bound == "deadline" {
				r.now = tx.RespondDeadlineMS
			} else {
				r.now += tx.Native.Profile.T1
			}
			if err := r.recoverServerResponses(); err != nil || tx.RetireMS != retire {
				t.Fatalf("recovery changed native retirement bound: %v", err)
			}
			if bound != "deadline" && len(r.payload.Effects) != 1 {
				t.Fatal("recovery escaped emission/retirement bound")
			}
			if bound != "retirement" && c.Result.Certainty != pb.OperationResult_UNKNOWN {
				t.Fatal("exhausted command/emission budget acknowledged an unwritten response")
			}
			if err := r.transactionEnded(c, tx, "native ended"); err != nil || c.Result.Certainty == pb.OperationResult_COMPLETED {
				t.Fatalf("native termination guessed physical delivery: %v", err)
			}
		})
	}
}

func TestStateRespondWaitExpiryReturnsUnknownWithoutUsingAcceptedAsSuccess(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	accepted := &pb.OperationResult{Certainty: pb.OperationResult_ACCEPTED, SipStatus: 503, Error: "publication refused"}
	result, err := (&StateEngine{}).awaitOperation(ctx, state.Ref{}, "context", "operation", accepted, true)
	if err != nil || result.Certainty != pb.OperationResult_UNKNOWN || result.SipStatus != 503 || result.Error != accepted.Error || accepted.Certainty != pb.OperationResult_ACCEPTED {
		t.Fatalf("control expiry acknowledged an unwritten response or erased refusal: %s %v", result, err)
	}
}
