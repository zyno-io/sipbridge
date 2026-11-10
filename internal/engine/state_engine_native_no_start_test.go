package engine

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/execution"
	executionfixture "sipbridge/internal/execution/fixture"
	"sipbridge/internal/state"
)

func nativeUnreservedRegisterResponse(t *testing.T) (nativeOwnerFixture, *pb.ContextRef, state.Effect, stateEngineEffect) {
	t.Helper()
	f := newNativeOwnerFixture(t)
	message := stateTestRequest("REGISTER", 1, "", "native-no-start-first")
	ref, err := f.engine.Locate(context.Background(), message)
	if err != nil {
		t.Fatal(err)
	}
	result := f.apply(t, ref, "first-register", stateEngineInput{Kind: "ingest", Application: f.app, ApplicationLeaseTerm: f.term,
		Ingest: &pb.OwnerIngest{Context: ref, Application: f.app, Flow: f.flow, Wire: message.Wire}})
	if result.Error != "" {
		t.Fatal(result.Error)
	}
	record, group := stateTestLoad(t, f.engine, ref)
	var presented string
	for _, effect := range record.Payload.Effects {
		if effect.Kind == "present" {
			presented = effect.ID
		}
	}
	if presented == "" {
		t.Fatal("REGISTER was not presented to its application")
	}
	// Admit the actual receiver acknowledgement before selecting its response.
	// The only unstarted work left in this fixture is the native response write.
	if result = f.apply(t, ref, "start-register-presentation", stateEngineInput{Kind: "effect-start", Effect: presented}); result.Error != "" {
		t.Fatal(result.Error)
	}
	result = f.apply(t, ref, "accept-register-presentation", stateEngineInput{Kind: "effect-result", Effect: presented, Executor: f.engine.cfg.Process,
		Reply: &pb.Envelope{Payload: &pb.Envelope_Result{Result: &pb.OperationResult{Certainty: pb.OperationResult_ACCEPTED}}}})
	if result.Error != "" {
		t.Fatal(result.Error)
	}
	var requestID string
	for id := range group.Contexts[ref.Id].Requests {
		requestID = id
	}
	result = f.apply(t, ref, "select-register-challenge", stateEngineInput{Kind: "command", Application: f.app, ApplicationLeaseTerm: f.term,
		Command: &pb.Command{Context: ref, Sequence: 1, Action: &pb.Command_Respond{Respond: &pb.Respond{
			RequestId: requestID, Status: 401, Authority: f.responseAuthority(t, ref, requestID)}}}})
	if result.Error != "" {
		t.Fatal(result.Error)
	}
	record, _ = stateTestLoad(t, f.engine, ref)
	for _, effect := range record.Payload.Effects {
		var body stateEngineEffect
		if err = json.Unmarshal(effect.Payload, &body); err != nil {
			t.Fatal(err)
		}
		if effect.Kind == "deliver" && body.Native && body.WireMethod == "REGISTER" && body.WireStatus == 401 {
			if effect.Status != state.EffectAuthorized || effect.AttemptID != "" || body.Authorization.GetEffect().GetAttemptId() == "" ||
				body.Permit == nil || effect.DeadlineMS <= time.Now().UnixMilli() {
				t.Fatal("fixture did not retain a live, unreserved native response")
			}
			return f, ref, effect, body
		}
	}
	t.Fatal("native REGISTER response missing")
	return f, ref, state.Effect{}, stateEngineEffect{}
}

func waitNativeResponseExpiry(effect state.Effect) {
	if delay := time.Until(time.UnixMilli(effect.DeadlineMS + 1)); delay > 0 {
		time.Sleep(delay)
	}
}

func TestLegacyHistoricalExpiredUnreservedResponseResumeAdmitsLaterRegister(t *testing.T) {
	f, ref, original, authorization := nativeUnreservedRegisterResponse(t)
	// Explicit historical claimed-input fixture; production never resumes it.
	fixture, err := executionfixture.New(f.engine.store.(*state.Store), execution.ReducerFunc(f.engine.reduce), "historical-no-start", 8)
	if err != nil {
		t.Fatal(err)
	}
	f.engine.runtime = fixture
	ctx := context.Background()
	groupRef, err := f.engine.group(ref)
	if err != nil {
		t.Fatal(err)
	}
	input := stateEngineInput{Kind: "locate", Context: ref, Operation: "admitted-before-response-expiry"}
	encoded, err := json.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}
	immutable := state.Input{ID: input.Operation, Kind: input.Kind, Fingerprint: input.Operation, Payload: encoded,
		DeadlineMS: original.DeadlineMS, ReceiptUntilMS: original.DeadlineMS + 120000}
	admitted, err := f.engine.store.(*state.Store).Begin(ctx, groupRef, "executor-paused-before-expiry", immutable)
	if err != nil || admitted.Step == nil {
		t.Fatalf("could not admit the immutable recovery input: %v", err)
	}
	// No writer reserves the response. The original computation claim and input
	// deadline expire while its immutable bytes remain admitted in real Redis.
	waitNativeResponseExpiry(original)
	failed, err := f.engine.store.(*state.Store).Resume(ctx, groupRef, "executor-with-invalid-outcome")
	if err != nil || failed.Step == nil {
		t.Fatalf("could not reclaim the expired immutable input: %v", err)
	}
	broken, outcome, err := f.engine.reduce(failed.Step.Record, failed.Step.Input, failed.Step.NowMS)
	if err != nil {
		t.Fatal(err)
	}
	// Reproduce the formerly emitted outcome without weakening Store validation:
	// one missing metadata field makes Commit fail while retaining the input.
	for index, effect := range broken.Effects {
		if effect.ID == original.ID {
			effect.AttemptID = ""
			broken.Effects[index] = effect
		}
	}
	if _, err = f.engine.store.(*state.Store).Commit(ctx, *failed.Step, broken, outcome); !errors.Is(err, state.ErrCorrupt) {
		t.Fatalf("missing attempt metadata did not reproduce the rejected commit: %v", err)
	}
	stranded, err := f.engine.store.Load(ctx, groupRef)
	if err != nil || stranded.Pending == nil || stranded.Pending.ID != immutable.ID ||
		!bytes.Equal(stranded.Pending.Payload, immutable.Payload) || stranded.Revision != admitted.Step.Record.Revision {
		t.Fatalf("rejected commit changed or lost the original admitted input: %v", err)
	}
	time.Sleep(time.Until(time.UnixMilli(failed.Step.Claim.UntilMS + 1)))
	recovered, err := legacyFixtureRuntime(t, f.engine).Resume(ctx, groupRef)
	if err != nil {
		t.Fatalf("expired native delivery froze the recovered input instead of committing NOT_WRITTEN: %v", err)
	}
	if recovered.OperationID != immutable.ID || recovered.Fingerprint != immutable.Fingerprint || recovered.InputDigest != admitted.Step.Claim.InputDigest {
		t.Fatal("recovery replaced the admitted operation instead of completing it")
	}
	record, _ := stateTestLoad(t, f.engine, ref)
	if record.Pending != nil || record.Revision == admitted.Step.Record.Revision {
		t.Fatal("recovery did not clear the pending input and advance its original group")
	}
	found := false
	for _, effect := range record.Payload.Effects {
		if effect.ID != original.ID {
			continue
		}
		var body stateEngineEffect
		if err = json.Unmarshal(effect.Payload, &body); err != nil {
			t.Fatal(err)
		}
		found = true
		if effect.Status != state.EffectNotWritten || effect.AttemptID != authorization.Authorization.Effect.AttemptId ||
			effect.DeadlineMS != original.DeadlineMS || !body.Settled || !body.Native ||
			!proto.Equal(body.Authorization, authorization.Authorization) || !proto.Equal(body.Permit, authorization.Permit) ||
			!bytes.Equal(body.Wire, authorization.Wire) {
			t.Fatal("no-start completion changed the retained attempt, authority, wire or deadline")
		}
	}
	if !found {
		t.Fatal("recovery omitted the exact no-start outcome")
	}
	batches, err := f.engine.store.ScheduleHints(ctx, groupRef.Partition, 1, 32)
	if err != nil || len(batches) != 1 {
		t.Fatalf("schedule hints: %+v %v", batches, err)
	}
	batch := batches[0]
	if batch.ReadyError != nil || batch.DueError != nil {
		t.Fatalf("schedule hint errors: %v %v", batch.ReadyError, batch.DueError)
	}
	for _, hint := range append(batch.Ready, batch.Due...) {
		if hint.Ref.ID == groupRef.ID {
			t.Fatal("completed recovery left the group in an immediately due scheduler loop")
		}
	}
	later := stateTestRequest("REGISTER", 2, "", "native-no-start-later")
	next, err := f.engine.Locate(ctx, later)
	if err != nil {
		t.Fatalf("recovered group could not admit the later REGISTER: %v", err)
	}
	if next.Id == ref.Id || next.State.GroupId != ref.State.GroupId || next.State.Incarnation != ref.State.Incarnation {
		t.Fatal("later REGISTER replaced the group instead of using its existing incarnation")
	}
	result := f.apply(t, next, "later-register", stateEngineInput{Kind: "ingest", Application: f.app, ApplicationLeaseTerm: f.term,
		Ingest: &pb.OwnerIngest{Context: next, Application: f.app, Flow: f.flow, Wire: later.Wire}})
	if result.Error != "" {
		t.Fatalf("later REGISTER remained blocked by the expired response: %s", result.Error)
	}
	current, group := stateTestLoad(t, f.engine, next)
	if current.Pending != nil || group.Contexts[next.Id] == nil || group.Contexts[next.Id].Snapshot.CallId != ref.State.CallId {
		t.Fatal("later REGISTER did not commit in the recovered group")
	}
}

func TestNativeExpiredReservedResponsePreservesDispatchingAndUnknown(t *testing.T) {
	for _, status := range []state.EffectStatus{state.EffectDispatching, state.EffectUnknown} {
		t.Run(string(status), func(t *testing.T) {
			f, ref, original, body := nativeUnreservedRegisterResponse(t)
			write := &pb.FlowWrite{Flow: body.Permit.Flow, Effect: body.Authorization.Effect, Permit: body.Permit}
			reservation, err := f.engine.Reserve(context.Background(), write)
			if err != nil || reservation.Known || !bytes.Equal(reservation.Wire, body.Wire) {
				t.Fatalf("real Redis reservation did not retain the exact response: %v", err)
			}
			if status == state.EffectUnknown {
				if err = f.engine.Complete(context.Background(), write, &pb.DeliveryResult{Effect: write.Effect, Outcome: pb.DeliveryResult_UNKNOWN}); err != nil {
					t.Fatal(err)
				}
			}
			waitNativeResponseExpiry(original)
			if result := f.apply(t, ref, "inspect-after-reserved-expiry", stateEngineInput{Kind: "locate"}); result.Error != "" {
				t.Fatal(result.Error)
			}
			record, _ := stateTestLoad(t, f.engine, ref)
			for _, effect := range record.Payload.Effects {
				if effect.ID != original.ID {
					continue
				}
				var retained stateEngineEffect
				if err = json.Unmarshal(effect.Payload, &retained); err != nil {
					t.Fatal(err)
				}
				if effect.Status != status || effect.AttemptID != body.Authorization.Effect.AttemptId || effect.DeadlineMS != original.DeadlineMS ||
					retained.Settled || !proto.Equal(retained.Authorization, body.Authorization) || !proto.Equal(retained.Permit, body.Permit) {
					t.Fatal("unreserved expiry logic erased or changed a reserved physical outcome")
				}
				return
			}
			t.Fatal("reserved physical evidence disappeared at its start deadline")
		})
	}
}

func TestNativeExpiredUnreservedResponseRejectsMalformedAttempt(t *testing.T) {
	for _, missing := range []bool{true, false} {
		name := "invalid"
		if missing {
			name = "missing"
		}
		t.Run(name, func(t *testing.T) {
			f, ref, original, _ := nativeUnreservedRegisterResponse(t)
			record, group := stateTestLoad(t, f.engine, ref)
			next := record.Payload
			next.Effects = append([]state.Effect(nil), next.Effects...)
			for index, effect := range next.Effects {
				if effect.ID != original.ID {
					continue
				}
				var body stateEngineEffect
				if err := json.Unmarshal(effect.Payload, &body); err != nil {
					t.Fatal(err)
				}
				if missing {
					body.Authorization.Effect = nil
				} else {
					body.Authorization.Effect.AttemptId = "invalid/attempt"
				}
				effect.Payload, _ = json.Marshal(body)
				next.Effects[index] = effect
			}
			r := &stateReduction{engine: f.engine, record: record, group: group, payload: next, now: original.DeadlineMS}
			// Corrupt authorization must produce no valid physical outcome and
			// must not panic while recovering its expired unreserved delivery.
			if err := r.prepareAutonomousEffects(); err != nil {
				t.Fatal(err)
			}
			input := state.Input{ID: "malformed-no-start", Kind: "fixture", Fingerprint: "malformed-no-start",
				DeadlineMS: time.Now().Add(time.Second).UnixMilli(), ReceiptUntilMS: time.Now().Add(time.Minute).UnixMilli()}
			begun, err := f.engine.store.(*state.Store).Begin(context.Background(), record.Ref, "malformed-writer", input)
			if err != nil || begun.Step == nil {
				t.Fatalf("could not admit malformed-outcome validation input: %v", err)
			}
			if _, err = f.engine.store.(*state.Store).Commit(context.Background(), *begun.Step, r.payload, nil); !errors.Is(err, state.ErrCorrupt) {
				t.Fatalf("malformed retained attempt became a committed no-start outcome: %v", err)
			}
			current, err := f.engine.store.Load(context.Background(), record.Ref)
			if err != nil || current.Pending == nil || current.Pending.ID != input.ID || current.Revision != record.Revision {
				t.Fatalf("invalid no-start metadata changed the authoritative group: %v", err)
			}
		})
	}
}
