package engine

import (
	"bytes"
	"testing"

	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/state"
)

func TestStateServerProvisionalQuotaCannotConsumeFinalBudget(t *testing.T) {
	engine, _ := historicalStateTestEngine(t)
	r, c, tx := provisionalResponseFixture(t)
	r.engine.store = engine.store
	r.group.Version = 1
	r.group.Retired = make(map[string]int64)
	advertiseCommand(t, r, c, &pb.Command{Sequence: 1, Action: &pb.Command_Respond{Respond: &pb.Respond{RequestId: tx.RequestID, Status: 180}}})
	completeLatestResponse(t, r, pb.DeliveryResult_WRITTEN)
	for tx.ProvisionalEmissions < stateProfileTransmissions {
		r.now += 2001
		r.in = stateEngineInput{Kind: "ingest"}
		if err := r.send(c, tx, *tx.Native.LastResponse, true); err != nil {
			t.Fatal(err)
		}
	}
	before := len(r.payload.Effects)
	if err := r.send(c, tx, *tx.Native.LastResponse, true); err != nil || tx.Emissions != stateProfileTransmissions || len(r.payload.Effects) != before {
		t.Fatal("excess provisional replay consumed the final class")
	}
	// Exercise the actual reducer refusal wrapper, including its established
	// consumed-sequence contract, rather than calling command in isolation.
	record := r.record
	record.Payload = r.payload
	record.Payload.ProtocolState, _ = marshalRetainedFixture(r.group)
	input := stateEngineInput{Kind: "command", Context: c.Snapshot.Context, Application: c.Snapshot.Application, Operation: "quota-refusal",
		Command: &pb.Command{Context: c.Snapshot.Context, Sequence: 2, Action: &pb.Command_Respond{Respond: &pb.Respond{RequestId: tx.RequestID, Status: 180}}}}
	encoded, _ := marshalRetainedFixture(input)
	deadline := r.now + 20000
	next, outcome, err := r.engine.reduce(record, state.Input{ID: input.Operation, Kind: input.Kind, Payload: encoded, DeadlineMS: deadline, ReceiptUntilMS: deadline + 120000}, r.now)
	if err != nil {
		t.Fatal(err)
	}
	var refused pb.OperationResult
	if unmarshalRetainedFixture(outcome, &refused) != nil || refused.Certainty != pb.OperationResult_NOT_ACCEPTED || refused.Error == "" {
		t.Fatal("application provisional quota was silently accepted or uncertain")
	}
	record.Payload = next
	group, err := decodeStateGroup(record)
	if err != nil {
		t.Fatal(err)
	}
	current := group.Contexts[c.Snapshot.Context.Id]
	retained := current.Transactions[tx.Native.Key]
	if current.Snapshot.CommandSequence != 2 || retained.ProvisionalEmissions != stateProfileTransmissions || retained.Emissions != stateProfileTransmissions {
		t.Fatal("definite refusal changed quotas or lost its consumed command sequence")
	}
	r.group, r.payload = group, next
	r.deadline = deadline
	advertiseCommand(t, r, current, &pb.Command{Sequence: 3, Action: &pb.Command_Respond{Respond: &pb.Respond{RequestId: retained.RequestID, Status: 487}}})
	if current.Result.Certainty != pb.OperationResult_ACCEPTED || retained.FinalResponse.Status != 487 || retained.ProvisionalEmissions != stateProfileTransmissions {
		t.Fatal("exhausted provisional class stranded the next mandatory final")
	}
	for retained.Emissions-retained.ProvisionalEmissions < stateProfileTransmissions {
		r.now += 2001
		r.in = stateEngineInput{Kind: "timer"}
		if err := r.send(current, retained, *retained.Native.LastResponse, true); err != nil {
			t.Fatal(err)
		}
	}
	count := len(r.payload.Effects)
	if err := r.send(current, retained, *retained.Native.LastResponse, true); err != nil || retained.Emissions != stateTransactionEmissions || len(r.payload.Effects) != count {
		t.Fatal("server response classes escaped the unchanged total22 budget")
	}
}

func TestStateNonInviteFinalNoStartRecoversUDPAndTLS(t *testing.T) {
	for _, transport := range []string{"udp", "tls"} {
		for _, status := range []uint32{401, 200} {
			t.Run(transport+"/"+reason(status), func(t *testing.T) {
				r, c, tx := advertiseFixture(t, transport, sip.OPTIONS, "")
				r.deadline = r.now + 20000
				r.result = &pb.OperationResult{Certainty: pb.OperationResult_COMPLETED}
				// Generic compact history is never the selected response's proof.
				tx.Written = true
				advertiseCommand(t, r, c, &pb.Command{Sequence: 1, Action: &pb.Command_Respond{Respond: &pb.Respond{RequestId: tx.RequestID, Status: status}}})
				_, original, body := responseEffectForStatus(t, r, status)
				originalNative, _ := marshalRetainedFixture(tx.Native)
				deadline, until, retire := tx.RespondDeadlineMS, tx.FinalResponse.UntilMS, tx.RetireMS
				if until > deadline || until <= r.now {
					t.Fatal("selected non-INVITE final lost its original command/native bound")
				}
				completeLatestResponse(t, r, pb.DeliveryResult_NOT_WRITTEN)
				if c.Result.Certainty != pb.OperationResult_ACCEPTED || tx.FinalResponse.RetryAtMS == 0 || tx.FinalResponse.Written {
					t.Fatal("positive no-start final became uncertain or inherited generic written proof")
				}
				replacement := proto.Clone(tx.Flow).(*pb.FlowRef)
				replacement.ConnectionId = "ordinary-route-moved"
				tx.Flow, c.Flow = replacement, replacement
				r.now = tx.FinalResponse.RetryAtMS
				if err := r.recoverServerResponses(); err != nil {
					t.Fatal(err)
				}
				_, retry, replay := responseEffectForStatus(t, r, status)
				if retry.ID == original.ID || !bytes.Equal(body.Wire, replay.Wire) || !proto.Equal(body.Flow, replay.Flow) ||
					tx.RespondDeadlineMS != deadline || tx.FinalResponse.UntilMS != until || tx.RetireMS != retire {
					t.Fatal("non-INVITE retry changed response, flow, command or native lifetime")
				}
				afterNative, _ := marshalRetainedFixture(tx.Native)
				if !bytes.Equal(originalNative, afterNative) {
					t.Fatal("non-INVITE response recovery restarted native timers/state")
				}
				completeLatestResponse(t, r, pb.DeliveryResult_WRITTEN)
				emissions := tx.Emissions
				r.now += 2 * tx.Native.Profile.T1
				if err := r.recoverServerResponses(); err != nil || c.Result.Certainty != pb.OperationResult_COMPLETED || tx.Emissions != emissions || !tx.FinalResponse.Written {
					t.Fatal("written non-INVITE final continued recovery or failed to complete")
				}
			})
		}
	}
}

func TestStateProvisionalRecoveryAppliesToBothServerFamiliesAndAllTransports(t *testing.T) {
	for _, transport := range []string{"udp", "tcp", "tls"} {
		for _, method := range []sip.RequestMethod{sip.INVITE, sip.OPTIONS} {
			t.Run(transport+"/"+method.String(), func(t *testing.T) {
				r, c, tx := advertiseFixture(t, transport, method, "")
				r.deadline = r.now + 20000
				r.result = &pb.OperationResult{Certainty: pb.OperationResult_COMPLETED}
				advertiseCommand(t, r, c, &pb.Command{Sequence: 1, Action: &pb.Command_Respond{Respond: &pb.Respond{RequestId: tx.RequestID, Status: 183}}})
				_, first, body := responseEffectForStatus(t, r, 183)
				native, _ := marshalRetainedFixture(tx.Native)
				completeLatestResponse(t, r, pb.DeliveryResult_NOT_WRITTEN)
				r.now = tx.FinalResponse.RetryAtMS
				if r.now == 0 || c.Result.Certainty != pb.OperationResult_ACCEPTED {
					t.Fatal("selected provisional has no bounded retry")
				}
				if err := r.recoverServerResponses(); err != nil {
					t.Fatal(err)
				}
				_, retry, retried := responseEffectForStatus(t, r, 183)
				afterNative, _ := marshalRetainedFixture(tx.Native)
				if first.ID == retry.ID || !bytes.Equal(body.Wire, retried.Wire) || !proto.Equal(body.Flow, retried.Flow) ||
					!bytes.Equal(native, afterNative) || tx.RespondDeadlineMS != r.deadline || tx.ProvisionalEmissions != 2 {
					t.Fatal("provisional recovery changed response identity or native lifetime")
				}
				completeLatestResponse(t, r, pb.DeliveryResult_WRITTEN)
				if c.Result.Certainty != pb.OperationResult_COMPLETED || c.Result.Error != "" {
					t.Fatal("exact selected provisional write did not complete its command")
				}
			})
		}
	}
}

func TestStateProvisionalWrittenOtherFlowSurvivesCompactionWithoutCompletingSelection(t *testing.T) {
	r, c, tx := provisionalResponseFixture(t)
	advertiseCommand(t, r, c, &pb.Command{Sequence: 1, Action: &pb.Command_Respond{Respond: &pb.Respond{RequestId: tx.RequestID, Status: 180}}})
	firstIndex, _, _ := responseEffectForStatus(t, r, 180)
	flow := proto.Clone(tx.Flow).(*pb.FlowRef)
	flow.ConnectionId, flow.SourcePort = "selected-second-flow", flow.SourcePort+1
	tx.Flow = flow
	r.in = stateEngineInput{Kind: "ingest"}
	if err := r.send(c, tx, *tx.Native.LastResponse, true); err != nil {
		t.Fatal(err)
	}
	completeLatestResponse(t, r, pb.DeliveryResult_NOT_WRITTEN)
	completeResponseEffectAt(t, r, firstIndex, pb.DeliveryResult_WRITTEN)
	first := r.payload.Effects[firstIndex]
	var firstBody stateEngineEffect
	if unmarshalRetainedFixture(first.Payload, &firstBody) != nil {
		t.Fatal("invalid fixture effect")
	}
	firstBody.Settled = true
	r.setEffect(firstIndex, first, firstBody)
	if err := r.reclaimEffects(); err != nil || tx.FinalResponse.Written || !tx.FinalResponse.AnyWritten {
		t.Fatal("old-flow write was lost or substituted for exact selected-flow proof")
	}
	emissions := tx.Emissions
	r.now = tx.FinalResponse.RetryAtMS
	if err := r.recoverServerResponses(); err != nil || tx.Emissions != emissions {
		t.Fatal("compaction of an old-flow written response allowed automatic recovery")
	}
	r.now = tx.RespondDeadlineMS
	if err := r.recoverServerResponses(); err != nil || c.Result.Certainty != pb.OperationResult_UNKNOWN || tx.Emissions != emissions {
		t.Fatal("other-flow write became exact selection success or definite no-start failure")
	}
}

func TestStateProvisionalLatePositiveNoStartCanConcludeDefiniteFailure(t *testing.T) {
	r, c, tx := provisionalResponseFixture(t)
	advertiseCommand(t, r, c, &pb.Command{Sequence: 1, Action: &pb.Command_Respond{Respond: &pb.Respond{RequestId: tx.RequestID, Status: 180}}})
	index, effect, body := responseEffectForStatus(t, r, 180)
	write := causalPermit(r, index, effect, body)
	r.now = tx.RespondDeadlineMS
	recordResponseWrite(t, r, write, pb.DeliveryResult_NOT_WRITTEN)
	if c.Result.Certainty != pb.OperationResult_COMPLETED || c.Result.Error == "" || tx.FinalResponse.Written {
		t.Fatal("late positive no-start was called ambiguous or claimed a response write")
	}
	r.deadline = r.now + 20000
	advertiseCommand(t, r, c, &pb.Command{Sequence: 2, Action: &pb.Command_Respond{Respond: &pb.Respond{RequestId: tx.RequestID, Status: 487}}})
	completeLatestResponse(t, r, pb.DeliveryResult_WRITTEN)
	if c.Result.Certainty != pb.OperationResult_COMPLETED || c.Result.Error != "" || tx.FinalResponse.Status != 487 {
		t.Fatal("definite provisional failure stranded the following final")
	}
}

func TestStateReliableInviteNon2xxNoStartRetriesWithoutCompetingNativeTimer(t *testing.T) {
	for _, transport := range []string{"tcp", "tls"} {
		for _, status := range []uint32{486, 487} {
			t.Run(transport+"/"+reason(status), func(t *testing.T) {
				r, c, tx := advertiseFixture(t, transport, sip.INVITE, "")
				r.deadline = r.now + 60000
				r.result = &pb.OperationResult{Certainty: pb.OperationResult_COMPLETED}
				advertiseCommand(t, r, c, &pb.Command{Sequence: 1, Action: &pb.Command_Respond{Respond: &pb.Respond{RequestId: tx.RequestID, Status: status}}})
				_, first, body := responseEffectForStatus(t, r, status)
				h := tx.Native.Timers[portable.TimerH]
				if !h.Active || tx.Native.Timers[portable.TimerG].Active || tx.FinalResponse.UntilMS != h.At {
					t.Fatal("reliable non-2xx final lost its immutable native H bound")
				}
				completeLatestResponse(t, r, pb.DeliveryResult_NOT_WRITTEN)
				r.now = tx.FinalResponse.RetryAtMS
				if r.now == 0 || c.Result.Certainty != pb.OperationResult_ACCEPTED {
					t.Fatal("reliable non-2xx final no-start permanently fenced Respond")
				}
				if err := r.recoverServerResponses(); err != nil {
					t.Fatal(err)
				}
				_, retry, retried := responseEffectForStatus(t, r, status)
				if retry.ID == first.ID || !bytes.Equal(body.Wire, retried.Wire) || !proto.Equal(body.Flow, retried.Flow) ||
					tx.Native.Timers[portable.TimerH] != h || tx.Native.Timers[portable.TimerG].Active || tx.Native.Phase != portable.Completed {
					t.Fatal("reliable non-2xx recovery changed the selected response/native procedure")
				}
				completeLatestResponse(t, r, pb.DeliveryResult_NOT_WRITTEN)
				// A native ACK ends the helper without changing the original Timer H.
				ack, err := portable.BuildNon2xxACK(tx.Native.Origin, *tx.Native.LastResponse)
				if err != nil {
					t.Fatal(err)
				}
				if err := r.stepTransaction(c, tx, portable.Input{Kind: portable.ReceiveRequest, Now: r.now, Message: ack}); err != nil {
					t.Fatal(err)
				}
				emissions := tx.Emissions
				r.now += tx.Native.Profile.T1
				if err := r.recoverServerResponses(); err != nil || tx.Emissions != emissions || tx.FinalResponse.RetryAtMS != 0 {
					t.Fatal("ACK/termination left a reliable final helper live")
				}
			})
		}
	}
}

func TestStateProvisionalExhaustionInvalidatesOnlyExpiredUnstartedWork(t *testing.T) {
	r, c, tx := provisionalResponseFixture(t)
	r.deadline = r.now + 60000
	advertiseCommand(t, r, c, &pb.Command{Sequence: 1, Action: &pb.Command_Respond{Respond: &pb.Respond{RequestId: tx.RequestID, Status: 180}}})
	for tx.ProvisionalEmissions < stateProfileTransmissions {
		r.now += 2001
		r.in = stateEngineInput{Kind: "ingest"}
		if err := r.send(c, tx, *tx.Native.LastResponse, true); err != nil {
			t.Fatal(err)
		}
	}
	// Prior pending intents are expired, but still retained. The last selected
	// physical no-start must atomically invalidate those unstarted intents before
	// reporting the aggregate definite failure.
	completeLatestResponse(t, r, pb.DeliveryResult_NOT_WRITTEN)
	if c.Result.Certainty != pb.OperationResult_COMPLETED || c.Result.Error == "" || tx.FinalResponse.Written || tx.ProvisionalEmissions != stateProfileTransmissions {
		t.Fatal("expired unstarted intents turned proven quota exhaustion into physical ambiguity")
	}
	for _, effect := range r.payload.Effects {
		if effect.Kind == "wire" && effect.Status != state.EffectCancelled {
			t.Fatal("definite failure left an old unstarted wire intent live")
		}
	}
	r.deadline = r.now + 20000
	advertiseCommand(t, r, c, &pb.Command{Sequence: 2, Action: &pb.Command_Respond{Respond: &pb.Respond{RequestId: tx.RequestID, Status: 487}}})
	completeLatestResponse(t, r, pb.DeliveryResult_WRITTEN)
	if c.Result.Certainty != pb.OperationResult_COMPLETED || c.Result.Error != "" || tx.FinalResponse.Status != 487 {
		t.Fatal("proven quota exhaustion stranded the later final class")
	}
}

func TestStateProvisionalAutomaticTryingCannotAcquireRetryBudget(t *testing.T) {
	r, c, tx := provisionalResponseFixture(t)
	trying := tx.Native.Timers[portable.TimerTrying]
	r.now = trying.At
	if !trying.Active {
		t.Fatal("automatic Trying timer fixture missing")
	}
	if err := r.stepTransaction(c, tx, portable.Input{Kind: portable.TimerFired, Now: r.now, Timer: portable.TimerTrying, Generation: trying.Generation}); err != nil {
		t.Fatal(err)
	}
	completeLatestResponse(t, r, pb.DeliveryResult_NOT_WRITTEN)
	emissions := tx.Emissions
	r.now += tx.Native.Profile.T1
	if err := r.recoverServerResponses(); err != nil || tx.Emissions != emissions || tx.FinalResponse.Command != "" ||
		tx.FinalResponse.UntilMS != 0 || tx.FinalResponse.RetryAtMS != 0 || tx.RespondDeadlineMS != 0 {
		t.Fatal("automatic100 without Respond acquired a retry/deadline obligation")
	}
}

func TestStateRespondRefusesNativeTerminalTransactionWithoutAnEmission(t *testing.T) {
	engine, _ := historicalStateTestEngine(t)
	for _, transport := range []string{"udp", "tls"} {
		for _, method := range []sip.RequestMethod{sip.OPTIONS, sip.SUBSCRIBE} {
			t.Run(transport+"/"+method.String(), func(t *testing.T) {
				r, c, tx := advertiseFixture(t, transport, method, "")
				r.engine.store = engine.store
				r.group.Version = 1
				r.group.Retired = make(map[string]int64)
				if err := r.stepTransaction(c, tx, portable.Input{Kind: portable.Terminate, Now: r.now}); err != nil {
					t.Fatal(err)
				}
				r.record.Payload = r.payload
				r.record.Payload.ProtocolState, _ = marshalRetainedFixture(r.group)
				beforeNative, _ := marshalRetainedFixture(tx.Native)
				input := stateEngineInput{Kind: "command", Context: c.Snapshot.Context, Application: c.Snapshot.Application, Operation: "terminated-response",
					Command: &pb.Command{Context: c.Snapshot.Context, Sequence: 1, Action: &pb.Command_Respond{Respond: &pb.Respond{RequestId: tx.RequestID, Status: 200}}}}
				encoded, _ := marshalRetainedFixture(input)
				payload, outcome, err := r.engine.reduce(r.record, state.Input{ID: input.Operation, Kind: input.Kind, Payload: encoded, DeadlineMS: r.now + 20000, ReceiptUntilMS: r.now + 140000}, r.now)
				if err != nil {
					t.Fatal(err)
				}
				var result pb.OperationResult
				if unmarshalRetainedFixture(outcome, &result) != nil || result.Certainty != pb.OperationResult_NOT_ACCEPTED || result.Error == "" {
					t.Fatal("native ignored response silently committed ACCEPTED")
				}
				r.record.Payload = payload
				group, err := decodeStateGroup(r.record)
				if err != nil {
					t.Fatal(err)
				}
				current := group.Contexts[c.Snapshot.Context.Id]
				retained := current.Transactions[tx.Native.Key]
				afterNative, _ := marshalRetainedFixture(retained.Native)
				if !bytes.Equal(beforeNative, afterNative) || retained.Emissions != 0 || retained.RespondDeadlineMS != 0 || current.Snapshot.CommandSequence != 1 {
					t.Fatal("definite response refusal mutated native state/budget or lost consumed sequence")
				}
			})
		}
	}
}

func TestStateInviteNon2xxNoStartKeepsNativeTimerGObligation(t *testing.T) {
	r, c, tx := provisionalResponseFixture(t)
	advertiseCommand(t, r, c, &pb.Command{Sequence: 1, Action: &pb.Command_Respond{Respond: &pb.Respond{RequestId: tx.RequestID, Status: 487}}})
	g, h := tx.Native.Timers[portable.TimerG], tx.Native.Timers[portable.TimerH]
	completeLatestResponse(t, r, pb.DeliveryResult_NOT_WRITTEN)
	if !g.Active || !h.Active || c.Result.Certainty != pb.OperationResult_ACCEPTED || tx.FinalResponse.RetryAtMS != 0 {
		t.Fatal("non-2xx INVITE lost its native G/H retry or acquired a competing helper")
	}
	r.now = g.At
	if err := r.recoverServerResponses(); err != nil || tx.Emissions != 1 {
		t.Fatal("generic helper competed with the native INVITE final timer")
	}
	if err := r.stepTransaction(c, tx, portable.Input{Kind: portable.TimerFired, Now: r.now, Timer: portable.TimerG, Generation: g.Generation}); err != nil {
		t.Fatal(err)
	}
	if tx.Emissions != 2 || tx.Native.Timers[portable.TimerH].At != h.At {
		t.Fatal("native final retransmission changed TimerH or did not emit")
	}
	completeLatestResponse(t, r, pb.DeliveryResult_WRITTEN)
	if c.Result.Certainty != pb.OperationResult_COMPLETED {
		t.Fatal("exact native487 write did not settle Respond")
	}
}

func TestStateSelectedProvisionalUnknownOnOtherFlowBlocksAutomaticRecovery(t *testing.T) {
	r, c, tx := provisionalResponseFixture(t)
	advertiseCommand(t, r, c, &pb.Command{Sequence: 1, Action: &pb.Command_Respond{Respond: &pb.Respond{RequestId: tx.RequestID, Status: 180}}})
	index, original, body := responseEffectForStatus(t, r, 180)
	oldWrite := causalPermit(r, index, original, body)
	r.in = stateEngineInput{Write: oldWrite, Executor: oldWrite.Flow.Ingress}
	if err := r.reserve(); err != nil {
		t.Fatal(err)
	}
	replacement := proto.Clone(tx.Flow).(*pb.FlowRef)
	replacement.ConnectionId = "explicit-request-replay"
	replacement.SourcePort++
	tx.Flow = replacement
	r.in = stateEngineInput{Kind: "ingest"}
	if err := r.send(c, tx, *tx.Native.LastResponse, true); err != nil {
		t.Fatal(err)
	}
	completeLatestResponse(t, r, pb.DeliveryResult_NOT_WRITTEN)
	recordResponseWrite(t, r, oldWrite, pb.DeliveryResult_UNKNOWN)
	count, emissions := len(r.payload.Effects), tx.Emissions
	r.now = tx.FinalResponse.RetryAtMS
	if err := r.recoverServerResponses(); err != nil || len(r.payload.Effects) != count || tx.Emissions != emissions {
		t.Fatal("other-flow physical uncertainty allowed an automatic response attempt")
	}
	r.now = tx.RespondDeadlineMS
	if err := r.recoverServerResponses(); err != nil || c.Result.Certainty != pb.OperationResult_UNKNOWN || tx.Emissions != emissions {
		t.Fatal("uncertain other-flow response became a conclusive no-start failure")
	}
	_, retained, _, err := r.findEffect(original.ID)
	if err != nil || retained.Status != state.EffectUnknown {
		t.Fatal("selected-response recovery rewrote the old unknown attempt")
	}
}
