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

func causalEffect(t *testing.T, r *stateReduction, method, remote string) (int, state.Effect, stateEngineEffect) {
	t.Helper()
	for index := len(r.payload.Effects) - 1; index >= 0; index-- {
		effect := r.payload.Effects[index]
		var body stateEngineEffect
		if json.Unmarshal(effect.Payload, &body) != nil || len(body.Wire) == 0 {
			continue
		}
		parsed, err := sip.ParseMessage(body.Wire)
		request, ok := parsed.(*sip.Request)
		if err == nil && ok && string(request.Method) == method && tag(request.To().Params) == remote {
			return index, effect, body
		}
	}
	t.Fatalf("%s effect for %s missing", method, remote)
	return 0, state.Effect{}, stateEngineEffect{}
}

func causalPermit(r *stateReduction, index int, effect state.Effect, body stateEngineEffect) *pb.FlowWrite {
	body.Permit = &pb.SendPermit{Authorization: body.Authorization, Flow: body.Flow, DirectoryEpoch: "directory", OrderingProof: "proof"}
	effect.Kind = "deliver"
	effect.Status = state.EffectAuthorized
	effect.AttemptID = ""
	r.setEffect(index, effect, body)
	return &pb.FlowWrite{Flow: body.Flow, Effect: body.Authorization.Effect, Permit: body.Permit}
}

func TestStateCleanupBYERequiresWrittenACKProcedure(t *testing.T) {
	r, c, tx := forkFixture(t)
	r.group = stateGroup{Version: 1, Contexts: map[string]*stateContext{c.Snapshot.Context.Id: c}, Selectors: make(map[string]string)}
	r.result = &pb.OperationResult{Certainty: pb.OperationResult_COMPLETED}
	if _, err := r.forkResponse(c, tx, forkReply(t, tx, 200, "winner")); err != nil {
		t.Fatal(err)
	}
	loser := forkReply(t, tx, 200, "loser")
	if _, err := r.forkResponse(c, tx, loser); err != nil {
		t.Fatal(err)
	}
	ackIndex, ackEffect, ackBody := causalEffect(t, r, "ACK", "loser")
	byeIndex, byeEffect, byeBody := causalEffect(t, r, "BYE", "loser")
	if ackBody.Dialog != "local/loser" || byeBody.AfterACK != stateACKHash(ackBody.Wire) || stateACKDependencyReady(c, byeBody) {
		t.Fatal("cleanup BYE has no exact pending ACK dependency")
	}
	ackWrite := causalPermit(r, ackIndex, ackEffect, ackBody)
	byeWrite := causalPermit(r, byeIndex, byeEffect, byeBody)
	r.in = stateEngineInput{Write: byeWrite, Executor: byeWrite.Flow.Ingress}
	if err := r.reserve(); err == nil {
		t.Fatal("BYE reserved before ACK write")
	}
	r.in = stateEngineInput{Write: ackWrite, Executor: ackWrite.Flow.Ingress}
	if err := r.reserve(); err != nil {
		t.Fatal(err)
	}
	r.in.Delivery = &pb.DeliveryResult{Outcome: pb.DeliveryResult_UNKNOWN, Effect: ackWrite.Effect}
	if err := r.complete(); err != nil || stateACKDependencyReady(c, byeBody) {
		t.Fatalf("unknown ACK opened cleanup gate: %v", err)
	}
	// A native retransmission gets another effect, preserving the exact ACK wire.
	if _, err := r.forkResponse(c, tx, loser); err != nil {
		t.Fatal(err)
	}
	retryIndex, retryEffect, retryBody := causalEffect(t, r, "ACK", "loser")
	if retryEffect.ID == ackEffect.ID || !bytes.Equal(retryBody.Wire, ackBody.Wire) {
		t.Fatal("native ACK retransmission identity lost")
	}
	retryWrite := causalPermit(r, retryIndex, retryEffect, retryBody)
	r.in = stateEngineInput{Write: retryWrite, Executor: retryWrite.Flow.Ingress}
	if err := r.reserve(); err != nil {
		t.Fatal(err)
	}
	r.in.Delivery = &pb.DeliveryResult{Outcome: pb.DeliveryResult_WRITTEN, Effect: retryWrite.Effect}
	if err := r.complete(); err != nil || !stateACKDependencyReady(c, byeBody) {
		t.Fatalf("written identical ACK did not open gate: %v", err)
	}
	_, written, compacted, err := r.findEffect(retryEffect.ID)
	if err != nil {
		t.Fatal(err)
	}
	compacted.Wire, compacted.Envelope = nil, nil
	r.setEffect(retryIndex, written, compacted)
	if err := r.complete(); err != nil || !stateACKDependencyReady(c, byeBody) {
		t.Fatalf("compacted ACK completion replay lost proof: %v", err)
	}
	r.in = stateEngineInput{Write: byeWrite, Executor: byeWrite.Flow.Ingress}
	if err := r.reserve(); err != nil {
		t.Fatalf("BYE did not reserve after proven ACK: %v", err)
	}
}

func TestStateUASACKTimeoutUsesRetainedCleanupScope(t *testing.T) {
	r, c, tx := forkFixture(t)
	r.group = stateGroup{Version: 1, Contexts: map[string]*stateContext{c.Snapshot.Context.Id: c}, Selectors: make(map[string]string)}
	r.result = &pb.OperationResult{Certainty: pb.OperationResult_COMPLETED}
	response := forkReply(t, tx, 200, "server-tag")
	dialog, err := portable.NewDialog(portable.UAS, tx.Native.Origin, response, portable.DefaultTimers())
	if err != nil {
		t.Fatal(err)
	}
	c.Dialog = dialog.LocalTag + "/" + dialog.RemoteTag
	c.Dialogs = map[string]portable.Dialog{c.Dialog: dialog}
	c.DialogScopes = map[string]string{c.Dialog: "uas-initial-scope"}
	c.Snapshot.LocalTag, c.Snapshot.RemoteTag = dialog.LocalTag, dialog.RemoteTag
	if err := r.dialogEffectsFor(c, c.Dialog, []portable.Effect{{Kind: portable.NeedBYE}}); err != nil {
		t.Fatal(err)
	}
	_, _, body := causalEffect(t, r, "BYE", dialog.RemoteTag)
	if body.Authorization.AuthorityClass != pb.EffectAuthorization_TERMINAL_CLEANUP || body.Authorization.CompletionScope != "uas-initial-scope" || body.AfterACK != "" {
		t.Fatal("UAS timeout cleanup requires application owner or an impossible local ACK")
	}
}

func TestStateSlowACKReemitsOnlyUnstartedCleanupWithinOriginalTimeout(t *testing.T) {
	for _, scenario := range []string{"pending", "authorized", "authorization-unknown", "physical-unknown", "written", "timeout", "clamped"} {
		t.Run(scenario, func(t *testing.T) {
			r, c, tx := forkFixture(t)
			r.group = stateGroup{Version: 1, Contexts: map[string]*stateContext{c.Snapshot.Context.Id: c}, Selectors: make(map[string]string)}
			r.result = &pb.OperationResult{Certainty: pb.OperationResult_COMPLETED}
			if _, err := r.forkResponse(c, tx, forkReply(t, tx, 200, "winner")); err != nil {
				t.Fatal(err)
			}
			if _, err := r.forkResponse(c, tx, forkReply(t, tx, 200, "loser")); err != nil {
				t.Fatal(err)
			}
			_, _, ack := causalEffect(t, r, "ACK", "loser")
			index, original, body := causalEffect(t, r, "BYE", "loser")
			cleanup := c.Transactions[body.Transaction]
			deadline := cleanup.Native.Timers[portable.TimerF].At
			switch scenario {
			case "authorized", "physical-unknown", "written":
				write := causalPermit(r, index, original, body)
				_, original, body, _ = r.findEffect(original.ID)
				if scenario == "physical-unknown" {
					original.Status, original.AttemptID = state.EffectUnknown, write.Effect.AttemptId
				} else if scenario == "written" {
					original.Status, original.AttemptID = state.EffectWritten, write.Effect.AttemptId
				}
				r.setEffect(index, original, body)
			case "authorization-unknown":
				original.Status, original.AttemptID = state.EffectUnknown, "rpc-attempt"
				r.setEffect(index, original, body)
			}
			r.now = original.DeadlineMS + 1000
			if scenario == "timeout" {
				r.now = deadline
			} else if scenario == "clamped" {
				r.now = deadline - 500
			}
			if err := stateRecordWrittenACK(c, ack); err != nil {
				t.Fatal(err)
			}
			before := len(r.payload.Effects)
			if err := r.releaseACKCleanup(c, ack); err != nil {
				t.Fatal(err)
			}
			if cleanup.Native.Timers[portable.TimerF].At != deadline {
				t.Fatal("cleanup transaction timeout restarted")
			}
			if scenario == "physical-unknown" || scenario == "written" || scenario == "timeout" {
				if len(r.payload.Effects) != before {
					t.Fatal("cleanup replayed a started attempt or extended native timeout")
				}
				return
			}
			_, emitted, next := causalEffect(t, r, "BYE", "loser")
			if emitted.ID == original.ID || !bytes.Equal(next.Wire, body.Wire) || emitted.DeadlineMS != min(r.now+2000, deadline) || !stateACKDependencyReady(c, next) {
				t.Fatal("fresh finite cleanup emission changed cached SIP identity or deadline")
			}
			_, invalidated, old, _ := r.findEffect(original.ID)
			if !old.Cancelled || invalidated.Status == state.EffectAuthorized {
				t.Fatal("old unreserved cleanup can still dispatch")
			}
			if old.Permit != nil {
				write := &pb.FlowWrite{Flow: old.Permit.Flow, Effect: old.Authorization.Effect, Permit: old.Permit}
				r.in = stateEngineInput{Write: write, Executor: write.Flow.Ingress}
				if err := r.reserve(); err != nil {
					t.Fatal(err)
				}
				if r.result.Response == nil || r.result.Request != nil {
					t.Fatal("old invalidated effect still returned bytes")
				}
			}
			if c.Snapshot.RemoteTag != "winner" || next.Envelope.GetEgress().Snapshot.RemoteTag != "loser" {
				t.Fatal("cleanup emission changed application winner or lost branch identity")
			}
		})
	}
}

func TestStateTimelyACKReleasesLaterExpiredUnstartedCleanup(t *testing.T) {
	for _, scenario := range []string{"pending", "authorized", "late-permit", "grant-absent", "no-start-complete", "no-start-replay", "early-no-start-complete", "early-no-start-replay", "physical-unknown", "written", "timeout", "no-ACK-proof"} {
		t.Run(scenario, func(t *testing.T) {
			r, c, tx := forkFixture(t)
			r.result = &pb.OperationResult{Certainty: pb.OperationResult_COMPLETED}
			c.Cancelled = true
			if _, err := r.forkResponse(c, tx, forkReply(t, tx, 200, "late")); err != nil {
				t.Fatal(err)
			}
			_, _, ack := causalEffect(t, r, "ACK", "late")
			index, original, body := causalEffect(t, r, "BYE", "late")
			cleanup := c.Transactions[body.Transaction]
			deadline := cleanup.Native.Timers[portable.TimerF].At
			if scenario != "no-ACK-proof" {
				if err := stateRecordWrittenACK(c, ack); err != nil {
					t.Fatal(err)
				}
			}
			// The ACK was completed while the first BYE emission remained valid.
			before := len(r.payload.Effects)
			if err := r.releaseACKCleanup(c, ack); err != nil || len(r.payload.Effects) != before {
				t.Fatalf("timely ACK replaced a still-valid cleanup emission: %v", err)
			}
			var write *pb.FlowWrite
			switch scenario {
			case "authorized", "no-start-complete", "no-start-replay", "early-no-start-complete", "early-no-start-replay", "physical-unknown", "written":
				write = causalPermit(r, index, original, body)
				_, original, body, _ = r.findEffect(original.ID)
				if scenario == "physical-unknown" {
					original.Status, original.AttemptID = state.EffectUnknown, write.Effect.AttemptId
				} else if scenario == "written" {
					original.Status, original.AttemptID = state.EffectWritten, write.Effect.AttemptId
				} else if scenario == "no-start-replay" || scenario == "early-no-start-replay" {
					original.Status, original.AttemptID = state.EffectNotWritten, write.Effect.AttemptId
				}
				r.setEffect(index, original, body)
			case "late-permit", "grant-absent":
				original.Status, original.AttemptID = state.EffectUnknown, "rpc-attempt"
				r.setEffect(index, original, body)
			}
			r.now = original.DeadlineMS + 1
			if scenario == "timeout" {
				r.now = deadline
			} else if scenario == "early-no-start-complete" || scenario == "early-no-start-replay" {
				r.now = original.DeadlineMS - 1
			}
			if scenario == "no-start-complete" || scenario == "no-start-replay" || scenario == "early-no-start-complete" || scenario == "early-no-start-replay" {
				r.in = stateEngineInput{Write: write, Executor: write.Flow.Ingress, Delivery: &pb.DeliveryResult{Effect: write.Effect, Outcome: pb.DeliveryResult_NOT_WRITTEN}}
				if err := r.complete(); err != nil {
					t.Fatal(err)
				}
			} else if scenario == "late-permit" || scenario == "grant-absent" {
				reply := &pb.Envelope{Payload: &pb.Envelope_Result{Result: &pb.OperationResult{Certainty: pb.OperationResult_NOT_ACCEPTED, Error: "send permit not found"}}}
				if scenario == "late-permit" {
					reply = &pb.Envelope{Payload: &pb.Envelope_SendPermit{SendPermit: &pb.SendPermit{Authorization: body.Authorization, Flow: body.Flow, DirectoryEpoch: "directory", OrderingProof: "proof"}}}
				}
				r.in = stateEngineInput{Effect: original.ID, Reply: reply}
				if err := r.effectResult(); err != nil {
					t.Fatal(err)
				}
			} else {
				r.in = stateEngineInput{Timer: r.timerID("group", "effects", "recovery", "")}
				if handled, err := r.lifecycleTimer(); !handled || err != nil {
					t.Fatalf("effect expiry failed: %v", err)
				}
			}
			if cleanup.Native.Timers[portable.TimerF].At != deadline {
				t.Fatal("cleanup transaction timeout restarted")
			}
			if scenario == "physical-unknown" || scenario == "written" || scenario == "timeout" || scenario == "no-ACK-proof" {
				if len(r.payload.Effects) != before {
					t.Fatal("cleanup replayed without a live, unstarted procedure and written ACK")
				}
				return
			}
			_, replacement, next := causalEffect(t, r, "BYE", "late")
			if replacement.ID == original.ID || !bytes.Equal(next.Wire, body.Wire) || replacement.DeadlineMS != min(r.now+2000, deadline) || !stateACKDependencyReady(c, next) {
				t.Fatal("expired cleanup replacement changed native wire, ACK proof or deadline")
			}
			if cleanup.Emissions != 2 {
				t.Fatal("cleanup replacement did not consume the finite native emission budget")
			}
		})
	}
}

func TestStateRepeatedExpiredCleanupGrantsRemainBounded(t *testing.T) {
	r, c, tx := forkFixture(t)
	r.result = &pb.OperationResult{Certainty: pb.OperationResult_COMPLETED}
	c.Cancelled = true
	if _, err := r.forkResponse(c, tx, forkReply(t, tx, 200, "late")); err != nil {
		t.Fatal(err)
	}
	_, _, ack := causalEffect(t, r, "ACK", "late")
	if err := stateRecordWrittenACK(c, ack); err != nil {
		t.Fatal(err)
	}
	_, _, initial := causalEffect(t, r, "BYE", "late")
	cleanup := c.Transactions[initial.Transaction]
	deadline := cleanup.Native.Timers[portable.TimerF].At
	for index := 0; index < stateTransactionEmissions+2; index++ {
		_, latest, _ := causalEffect(t, r, "BYE", "late")
		r.now = latest.DeadlineMS + 1
		r.in = stateEngineInput{Timer: r.timerID("group", "effects", "recovery", "")}
		if handled, err := r.lifecycleTimer(); !handled || err != nil {
			t.Fatalf("effect expiry failed: %v", err)
		}
	}
	if cleanup.Emissions <= 1 || cleanup.Emissions > stateTransactionEmissions || cleanup.Native.Timers[portable.TimerF].At != deadline || r.now < deadline {
		t.Fatalf("repeated cleanup emission escaped its finite budget or original timeout: %d", cleanup.Emissions)
	}
}

func TestStateEarlyCleanupNoStartReplaysAndSettlementStayBounded(t *testing.T) {
	r, c, tx := forkFixture(t)
	r.result = &pb.OperationResult{Certainty: pb.OperationResult_COMPLETED}
	c.Cancelled = true
	if _, err := r.forkResponse(c, tx, forkReply(t, tx, 200, "late")); err != nil {
		t.Fatal(err)
	}
	_, _, ack := causalEffect(t, r, "ACK", "late")
	if err := stateRecordWrittenACK(c, ack); err != nil {
		t.Fatal(err)
	}
	_, _, first := causalEffect(t, r, "BYE", "late")
	cleanup := c.Transactions[first.Transaction]
	deadline := cleanup.Native.Timers[portable.TimerF].At
	for emission := 0; emission < stateTransactionEmissions; emission++ {
		index, effect, body := causalEffect(t, r, "BYE", "late")
		if !bytes.Equal(body.Wire, first.Wire) {
			t.Fatal("early no-start retry changed native BYE bytes or CSeq")
		}
		write := causalPermit(r, index, effect, body)
		r.now++
		if r.now >= effect.DeadlineMS {
			t.Fatal("fixture did not exercise early no-start completion")
		}
		r.in = stateEngineInput{Write: write, Executor: write.Flow.Ingress, Delivery: &pb.DeliveryResult{Effect: write.Effect, Outcome: pb.DeliveryResult_NOT_WRITTEN}}
		if err := r.complete(); err != nil {
			t.Fatal(err)
		}
		count := len(r.payload.Effects)
		if err := r.complete(); err != nil || len(r.payload.Effects) != count {
			t.Fatalf("same no-start completion duplicated a pending cleanup emission: %v", err)
		}
		index, known, terminal, err := r.findEffect(effect.ID)
		if err != nil {
			t.Fatal(err)
		}
		terminal.Settled = true
		r.setEffect(index, known, terminal)
		if err := r.reclaimEffects(); err != nil {
			t.Fatal(err)
		}
	}
	if cleanup.Emissions != stateTransactionEmissions || cleanup.Native.Timers[portable.TimerF].At != deadline || r.now >= deadline {
		t.Fatalf("early known failures escaped the fixed emission/procedure bounds: %d", cleanup.Emissions)
	}
	for _, effect := range r.payload.Effects {
		var body stateEngineEffect
		if json.Unmarshal(effect.Payload, &body) == nil && body.Transaction == cleanup.Native.Key {
			t.Fatal("settled old cleanup receipts remained or emitted beyond the cap")
		}
	}
}

func TestStateRetirementRetainsUnknownEffectsAndNativeReplay(t *testing.T) {
	for _, obligation := range []string{"unknown", "dispatching", "replay", "timer"} {
		t.Run(obligation, func(t *testing.T) {
			r, c, tx := forkFixture(t)
			id := c.Snapshot.Context.Id
			r.now = 200000
			r.group = stateGroup{Version: 1, Contexts: map[string]*stateContext{id: c}, Selectors: make(map[string]string)}
			c.Snapshot.Ended, c.RetireMS = true, 1000
			c.Transactions = make(map[string]*stateTransaction)
			switch obligation {
			case "unknown", "dispatching":
				body, _ := json.Marshal(stateEngineEffect{Context: id})
				status := state.EffectUnknown
				if obligation == "dispatching" {
					status = state.EffectDispatching
				}
				r.payload.Effects = []state.Effect{{ID: "unresolved", Kind: "deliver", Status: status, DeadlineMS: 1000, AttemptID: "attempt", Payload: body}}
			case "replay":
				tx.Native.Phase, tx.RetireMS = portable.Terminated, r.now+1000
				c.Transactions[tx.Native.Key] = tx
			case "timer":
				c.Dialogs["retained"] = portable.Dialog{Timers: map[portable.Timer]portable.Deadline{portable.TimerACK: {Active: true, At: r.now + 1000, Generation: 1}}}
			}
			if err := r.appendLifecycleTimers(); err != nil || r.group.Contexts[id] == nil {
				t.Fatalf("retired native obligation %s: %v", obligation, err)
			}
			if obligation == "unknown" || obligation == "dispatching" {
				found := false
				for _, timer := range r.payload.Timers {
					found = found || timer.ID == r.timerID("group", "effects", "recovery", "")
				}
				if !found {
					t.Fatal("historical physical evidence is no longer inspected")
				}
			}
			c.Transactions = make(map[string]*stateTransaction)
			c.Dialogs = make(map[string]portable.Dialog)
			r.payload.Effects, r.payload.Timers = nil, nil
			if err := r.appendLifecycleTimers(); err != nil || r.group.Contexts[id] != nil {
				t.Fatalf("resolved native context could not retire: %v", err)
			}
		})
	}
}

func TestStateScopeGrantRetainedForUASAndNonInvite(t *testing.T) {
	for _, family := range []portable.Family{portable.InviteServer, portable.NonInviteClient} {
		t.Run(string(family), func(t *testing.T) {
			r, c, tx := forkFixture(t)
			c.CompletionScope = ""
			c.Dialog = "local/remote"
			tx.Native.Family = family
			r.group = stateGroup{Version: 1, Contexts: map[string]*stateContext{c.Snapshot.Context.Id: c}}
			r.result = &pb.OperationResult{Certainty: pb.OperationResult_COMPLETED}
			auth := &pb.EffectAuthorization{Effect: &pb.StateEffectRef{Context: c.Snapshot.Context, Id: "effect", AttemptId: "attempt"}}
			body := stateEngineEffect{Context: c.Snapshot.Context.Id, Transaction: tx.Native.Key, Authorization: auth, Executor: r.engine.cfg.Process}
			encoded, _ := json.Marshal(body)
			r.payload.Effects = []state.Effect{{ID: "effect", Kind: "wire", Status: state.EffectDispatching, AttemptID: "rpc-attempt", DeadlineMS: r.now + 2000, Payload: encoded}}
			permit := &pb.SendPermit{Authorization: auth, Flow: c.Flow, CompletionScope: "retained-scope", DirectoryEpoch: "directory", OrderingProof: "proof"}
			r.in = stateEngineInput{Effect: "effect", Executor: r.engine.cfg.Process, Reply: &pb.Envelope{Payload: &pb.Envelope_SendPermit{SendPermit: permit}}}
			if err := r.effectResult(); err != nil || tx.CompletionScope != permit.CompletionScope {
				t.Fatalf("native transaction scope lost: %v", err)
			}
			if family == portable.InviteServer && (c.CompletionScope != permit.CompletionScope || c.DialogScopes[c.Dialog] != permit.CompletionScope) {
				t.Fatal("initial UAS scope did not reach native dialog cleanup")
			}
		})
	}
}

func TestStateCompleteRejectsUnreservedNativeOutcome(t *testing.T) {
	a, _ := historicalStateTestEngine(t)
	message := stateTestRequest("OPTIONS", 1, "", "completion-reject")
	ref, err := a.Locate(context.Background(), message)
	if err != nil {
		t.Fatal(err)
	}
	application := &pb.ProcessRef{InstanceId: "controller", Epoch: "epoch"}
	flow := &pb.FlowRef{Ingress: a.cfg.Process, ConnectionId: "flow", SourceIp: "192.0.2.30", SourcePort: 50001, Listener: "sip.example.test:5061", Transport: "tls"}
	result := stateTestApply(t, a, ref, "ingest-completion", stateEngineInput{Kind: "ingest", Application: application, Ingest: &pb.OwnerIngest{Context: ref, Application: application, Flow: flow, Wire: message.Wire}})
	if result.Error != "" {
		t.Fatal(result.Error)
	}
	_, group := stateTestLoad(t, a, ref)
	var requestID string
	for id := range group.Contexts[ref.Id].Requests {
		requestID = id
	}
	stateTestApply(t, a, ref, "respond-completion", stateEngineInput{Kind: "command", Application: application, Command: &pb.Command{Context: ref, Sequence: 1, Action: &pb.Command_Respond{Respond: &pb.Respond{RequestId: requestID, Status: 200}}}})
	record, _ := stateTestLoad(t, a, ref)
	for _, effect := range record.Payload.Effects {
		if effect.Kind != "wire" {
			continue
		}
		var body stateEngineEffect
		if json.Unmarshal(effect.Payload, &body) != nil {
			t.Fatal("bad effect")
		}
		permit := &pb.SendPermit{Authorization: body.Authorization, Flow: flow, DirectoryEpoch: "directory", OrderingProof: "proof"}
		stateTestApply(t, a, ref, "start-grant-completion", stateEngineInput{Kind: "effect-start", Effect: effect.ID})
		stateTestApply(t, a, ref, "grant-completion", stateEngineInput{Kind: "effect-result", Effect: effect.ID, Executor: a.cfg.Process, Reply: &pb.Envelope{Payload: &pb.Envelope_SendPermit{SendPermit: permit}}})
		write := &pb.FlowWrite{Flow: flow, Effect: proto.Clone(body.Authorization.Effect).(*pb.StateEffectRef), Permit: permit}
		if err := a.Complete(context.Background(), write, &pb.DeliveryResult{Outcome: pb.DeliveryResult_WRITTEN, Effect: write.Effect}); err == nil {
			t.Fatal("physical writer would discard evidence after rejected native completion")
		}
		return
	}
	t.Fatal("response effect missing")
}
