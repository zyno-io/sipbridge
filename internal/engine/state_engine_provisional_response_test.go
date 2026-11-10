package engine

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/state"
)

func provisionalResponseFixture(t *testing.T) (*stateReduction, *stateContext, *stateTransaction) {
	t.Helper()
	r, c, tx := advertiseFixture(t, "udp", sip.INVITE, "")
	r.deadline = r.now + 20000
	r.result = &pb.OperationResult{Certainty: pb.OperationResult_COMPLETED}
	request, err := tx.Native.Origin.Parse()
	if err != nil {
		t.Fatal(err)
	}
	request.Via().Params.Add("rport", "")
	tx.Native.Origin.Wire = []byte(request.String())
	c.Origin = tx.Native.Origin.Clone()
	return r, c, tx
}

func responseEffectForStatus(t *testing.T, r *stateReduction, status uint32) (int, state.Effect, stateEngineEffect) {
	t.Helper()
	for index := len(r.payload.Effects) - 1; index >= 0; index-- {
		effect := r.payload.Effects[index]
		var body stateEngineEffect
		if json.Unmarshal(effect.Payload, &body) == nil && body.WireStatus == status && (effect.Kind == "wire" || effect.Kind == "deliver") {
			return index, effect, body
		}
	}
	t.Fatalf("response effect %d missing", status)
	return 0, state.Effect{}, stateEngineEffect{}
}

func recordResponseWrite(t *testing.T, r *stateReduction, write *pb.FlowWrite, outcome pb.DeliveryResult_Outcome) {
	t.Helper()
	r.result = &pb.OperationResult{Certainty: pb.OperationResult_COMPLETED}
	r.in = stateEngineInput{Write: write, Executor: write.Flow.Ingress, Delivery: &pb.DeliveryResult{Effect: write.Effect, Outcome: outcome}}
	if err := r.complete(); err != nil {
		t.Fatal(err)
	}
}

func TestStateProvisionalNoStartRecoversExact180ThenAllows487(t *testing.T) {
	for _, tryingWrittenAfterSelection := range []bool{false, true} {
		name := "automatic100-written-before180"
		if tryingWrittenAfterSelection {
			name = "automatic100-written-after180-selection"
		}
		t.Run(name, func(t *testing.T) {
			r, c, tx := provisionalResponseFixture(t)
			context, application, flow := proto.Clone(c.Snapshot.Context), proto.Clone(c.Snapshot.Application), proto.Clone(tx.Flow)
			trying := tx.Native.Timers[portable.TimerTrying]
			r.now = trying.At
			if !trying.Active {
				t.Fatal("automatic Trying timer fixture is not active")
			}
			if err := r.stepTransaction(c, tx, portable.Input{Kind: portable.TimerFired, Now: r.now, Timer: portable.TimerTrying, Generation: trying.Generation}); err != nil {
				t.Fatal(err)
			}
			tryingIndex, _, tryingBody := responseEffectForStatus(t, r, 100)
			if tryingBody.Command != "" {
				t.Fatal("automatic100 unexpectedly belongs to an application command")
			}
			writeTrying := func() {
				completeResponseEffectAt(t, r, tryingIndex, pb.DeliveryResult_WRITTEN)
				index, effect, body := responseEffectForStatus(t, r, 100)
				body.Settled = true
				r.setEffect(index, effect, body)
				if err := r.reclaimEffects(); err != nil || !tx.Written {
					t.Fatalf("automatic100 compacted written proof fixture failed: %v", err)
				}
			}
			if !tryingWrittenAfterSelection {
				writeTrying()
			}
			advertiseCommand(t, r, c, &pb.Command{Sequence: 1, Action: &pb.Command_Respond{Respond: &pb.Respond{RequestId: tx.RequestID, Status: 180}}})
			if tryingWrittenAfterSelection {
				writeTrying()
			}
			if c.Result.Certainty != pb.OperationResult_ACCEPTED {
				t.Fatalf("automatic100/tx.Written acknowledged the selected180: %s", c.Result)
			}
			firstIndex, first, selected := responseEffectForStatus(t, r, 180)
			deadline, command, emissions := tx.RespondDeadlineMS, c.Operation, tx.Emissions
			firstWrite := causalPermit(r, firstIndex, first, selected)
			// The physical writer positively proves it never started this attempt,
			// after its two-second grant expired but within Respond's original budget.
			r.now = first.DeadlineMS + 1
			recordResponseWrite(t, r, firstWrite, pb.DeliveryResult_NOT_WRITTEN)
			if r.now >= deadline || c.Result.Certainty != pb.OperationResult_ACCEPTED {
				t.Fatalf("positive no-start before the original Respond deadline permanently fenced selected180: %s", c.Result)
			}
			if tx.Native.Reliable || tx.Native.Phase != portable.Proceeding || tx.RetireMS != 0 {
				t.Fatal("regression must retain the actual UDP proceeding INVITE server transaction")
			}
			r.now += tx.Native.Profile.T1 - 1
			if err := r.recoverServerResponses(); err != nil || tx.Emissions != emissions {
				t.Fatalf("provisional retry ran before T1: %v", err)
			}
			r.now++
			if err := r.recoverServerResponses(); err != nil {
				t.Fatal(err)
			}
			retryIndex, retry, retried := responseEffectForStatus(t, r, 180)
			if retry.ID == first.ID || retryIndex == firstIndex || tx.Emissions != emissions+1 || tx.Emissions > stateTransactionEmissions ||
				!bytes.Equal(selected.Wire, retried.Wire) || !bytes.Contains(retried.Wire, []byte("rport=")) ||
				!proto.Equal(selected.Flow, retried.Flow) || selected.WireHash != retried.WireHash || retried.Command != command ||
				tx.RespondDeadlineMS != deadline || c.Snapshot.CommandSequence != 1 || c.Result.Certainty != pb.OperationResult_ACCEPTED ||
				!proto.Equal(c.Snapshot.Context, context) || !proto.Equal(c.Snapshot.Application, application) || !proto.Equal(tx.Flow, flow) {
				t.Fatal("recovery changed selected180 bytes/flow, command identity, original deadline or finite emission bound")
			}
			if err := r.recoverServerResponses(); err != nil || tx.Emissions != emissions+1 {
				t.Fatalf("pending selected180 retry was not coalesced: %v", err)
			}
			retryWrite := causalPermit(r, retryIndex, retry, retried)
			r.in = stateEngineInput{Write: retryWrite, Executor: retryWrite.Flow.Ingress}
			if err := r.reserve(); err != nil {
				t.Fatal(err)
			}
			recordResponseWrite(t, r, retryWrite, pb.DeliveryResult_WRITTEN)
			if c.Result.Certainty != pb.OperationResult_COMPLETED || c.Result.Error != "" || tx.RespondDeadlineMS != deadline {
				t.Fatalf("exact selected180 write failed to complete its original command: %s", c.Result)
			}

			r.deadline = r.now + 20000
			advertiseCommand(t, r, c, &pb.Command{Sequence: 2, Action: &pb.Command_Respond{Respond: &pb.Respond{RequestId: tx.RequestID, Status: 487}}})
			_, _, final := responseEffectForStatus(t, r, 487)
			finalCommand, finalDeadline := c.Operation, tx.RespondDeadlineMS
			acceptedFinal := proto.Clone(c.Result)
			for _, old := range []struct {
				write   *pb.FlowWrite
				outcome pb.DeliveryResult_Outcome
			}{{firstWrite, pb.DeliveryResult_NOT_WRITTEN}, {retryWrite, pb.DeliveryResult_WRITTEN}} {
				recordResponseWrite(t, r, old.write, old.outcome)
				if !proto.Equal(c.Result, acceptedFinal) || c.Operation != finalCommand || tx.RespondDeadlineMS != finalDeadline {
					t.Fatal("stale/duplicate180 result changed the pending487 command")
				}
			}
			completeLatestResponse(t, r, pb.DeliveryResult_WRITTEN)
			if c.Snapshot.CommandSequence != 2 || c.Result.Certainty != pb.OperationResult_COMPLETED || final.Command != finalCommand {
				t.Fatal("next-sequence487 failed to select and physically complete")
			}
			completedFinal, finalWire, finalEmissions := proto.Clone(c.Result), append([]byte(nil), tx.Native.LastResponse.Wire...), tx.Emissions
			recordResponseWrite(t, r, firstWrite, pb.DeliveryResult_NOT_WRITTEN)
			r.now += tx.Native.Profile.T1
			if err := r.recoverServerResponses(); err != nil || !proto.Equal(c.Result, completedFinal) ||
				!bytes.Equal(tx.Native.LastResponse.Wire, finalWire) || tx.Emissions != finalEmissions || tx.Emissions > stateTransactionEmissions {
				t.Fatalf("late180 no-start rearmed or rolled back the written487: %v", err)
			}
		})
	}
}

func TestStateProvisionalNoStartRecoveryKeepsOriginalBounds(t *testing.T) {
	for _, bound := range []string{"command-deadline", "emission-cap"} {
		t.Run(bound, func(t *testing.T) {
			r, c, tx := provisionalResponseFixture(t)
			advertiseCommand(t, r, c, &pb.Command{Sequence: 1, Action: &pb.Command_Respond{Respond: &pb.Respond{RequestId: tx.RequestID, Status: 180}}})
			index, effect, body := responseEffectForStatus(t, r, 180)
			write := causalPermit(r, index, effect, body)
			recordResponseWrite(t, r, write, pb.DeliveryResult_NOT_WRITTEN)
			deadline := tx.RespondDeadlineMS
			if bound == "emission-cap" {
				tx.Emissions, tx.ProvisionalEmissions = stateProfileTransmissions, stateProfileTransmissions
				r.now += tx.Native.Profile.T1
			} else {
				r.now = deadline
			}
			emissions, count := tx.Emissions, len(r.payload.Effects)
			if err := r.recoverServerResponses(); err != nil || tx.Emissions != emissions || len(r.payload.Effects) != count ||
				tx.RespondDeadlineMS != deadline || tx.RetireMS != 0 || c.Snapshot.CommandSequence != 1 || c.Result.Certainty != pb.OperationResult_COMPLETED || c.Result.Error == "" {
				t.Fatalf("selected180 recovery escaped the original %s: result=%s error=%v", bound, c.Result, err)
			}
		})
	}
}

func TestStateProvisionalPhysicalUnknownNeverAuthorizesAutomaticRetry(t *testing.T) {
	r, c, tx := provisionalResponseFixture(t)
	advertiseCommand(t, r, c, &pb.Command{Sequence: 1, Action: &pb.Command_Respond{Respond: &pb.Respond{RequestId: tx.RequestID, Status: 180}}})
	index, _, _ := responseEffectForStatus(t, r, 180)
	completeResponseEffectAt(t, r, index, pb.DeliveryResult_UNKNOWN)
	unknown := r.payload.Effects[index]
	deadline, emissions := tx.RespondDeadlineMS, tx.Emissions
	if c.Result.Certainty != pb.OperationResult_UNKNOWN || unknown.Status != state.EffectUnknown {
		t.Fatal("actual physical uncertainty was not retained")
	}
	for _, now := range []int64{r.now + tx.Native.Profile.T1, deadline - 1, deadline} {
		r.now = now
		if err := r.recoverServerResponses(); err != nil || tx.Emissions != emissions || len(r.payload.Effects) != 1 ||
			!bytes.Equal(r.payload.Effects[index].Payload, unknown.Payload) || r.payload.Effects[index].Status != state.EffectUnknown ||
			c.Result.Certainty != pb.OperationResult_UNKNOWN || tx.RespondDeadlineMS != deadline {
			t.Fatalf("provisional recovery retried or rewrote genuine physical UNKNOWN: %v", err)
		}
	}
}
