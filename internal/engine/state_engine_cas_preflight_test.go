package engine

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/execution"
	"sipbridge/internal/state"
)

func TestStateCASControlUsesComputationReadWithoutOrdinaryPreflight(t *testing.T) {
	f, ref, in := preflightFixture(t, "REGISTER")
	var err error
	f.engine.runtime, err = execution.NewCAS(f.engine.store, execution.ReducerFunc(f.engine.reduce), 8)
	if err != nil {
		t.Fatal(err)
	}
	before := preflightEvalCalls(t, f)
	if result := f.apply(t, ref, "initial", in); result.Error != "" {
		t.Fatal(result.Error)
	}
	if got := preflightEvalCalls(t, f) - before; got != 2 {
		t.Fatalf("ordinary ingest used %d Redis scripts, want one read and one CAS", got)
	}
	_, group := stateTestLoad(t, f.engine, ref)
	var request string
	for id := range group.Contexts[ref.Id].Requests {
		request = id
	}
	authority := f.responseAuthority(t, ref, request)
	admission := &pb.CommandAdmission{DirectoryEpoch: authority.DirectoryEpoch, ExpiresAtMs: uint64(time.Now().Add(10 * time.Second).UnixMilli())}
	before = preflightEvalCalls(t, f)
	result := f.apply(t, ref, "respond", stateEngineInput{Kind: "command", Application: f.app, ApplicationLeaseTerm: f.term,
		Command: &pb.Command{Context: ref, Sequence: 1, Admission: admission, Action: &pb.Command_Respond{Respond: &pb.Respond{RequestId: request, Status: 200, Authority: authority}}}})
	if result.Error != "" {
		t.Fatal(result.Error)
	}
	if got := preflightEvalCalls(t, f) - before; got != 2 {
		t.Fatalf("ordinary Respond used %d Redis scripts, want one read and one CAS", got)
	}
}

func TestStateCASReservationReadsOnceAndCannotReplayLegacyWireProof(t *testing.T) {
	t.Setenv("SIPBRIDGE_STATE_TEST_REDIS", preflightRedisProxy(t))
	f, _, _ := casOptionsFixture(t)
	write := casOptionsResponse(t, f, "cas-reserve-read")
	before := preflightEvalCalls(t, f)
	reserved, err := f.engine.Reserve(context.Background(), write)
	if err != nil || reserved.Known || len(reserved.Wire) == 0 {
		t.Fatalf("first reservation failed: %+v %v", reserved, err)
	}
	if got := preflightEvalCalls(t, f) - before; got != 2 {
		t.Fatalf("reservation used %d Redis scripts, want one read and one CAS", got)
	}

	// Model an expanded legacy writer's old pure-computation proof. Reusing its
	// operation identity would return old wire despite the consumed attempt.
	logical, err := f.engine.group(write.Effect.Context)
	if err != nil {
		t.Fatal(err)
	}
	copy := proto.Clone(write).(*pb.FlowWrite)
	copy.Wire = nil
	input := stateEngineInput{Kind: "reserve", Context: write.Effect.Context, Executor: f.engine.cfg.Process, Write: copy, Effect: write.Effect.Id}
	data, err := json.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(data)
	deadline := time.Now().Add(time.Second).UnixMilli()
	started, err := f.engine.store.(*state.Store).Begin(context.Background(), logical, "legacy-reservation-proof", state.Input{
		ID: f.engine.id("reserve", write.Effect.Id, write.Effect.AttemptId), Kind: "reserve", Fingerprint: hex.EncodeToString(sum[:]),
		Payload: data, Proof: true, DeadlineMS: deadline, ReceiptUntilMS: deadline})
	if err != nil || started.Step == nil {
		t.Fatalf("legacy proof fixture failed: %v", err)
	}
	outcome, err := json.Marshal(&pb.OperationResult{Certainty: pb.OperationResult_COMPLETED, Request: &pb.SipMessage{Wire: reserved.Wire}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.engine.store.(*state.Store).Commit(context.Background(), *started.Step, started.Step.Record.Payload, outcome); err != nil {
		t.Fatal(err)
	}
	before = preflightEvalCalls(t, f)
	duplicate, err := f.engine.Reserve(context.Background(), write)
	if err != nil || !duplicate.Known || duplicate.Outcome != pb.DeliveryResult_UNKNOWN || len(duplicate.Wire) != 0 {
		t.Fatalf("consumed reservation replayed old physical wire: %+v %v", duplicate, err)
	}
	if got := preflightEvalCalls(t, f) - before; got != 2 {
		t.Fatalf("consumed reservation used %d scripts, want one read and one CAS", got)
	}
}

func TestStateCASPreflightRefusesLegacyPendingWithoutResume(t *testing.T) {
	f, ref, in := preflightFixture(t, "REGISTER")
	if result := f.apply(t, ref, "initial", in); result.Error != "" {
		t.Fatal(result.Error)
	}
	logical, err := f.engine.group(ref)
	if err != nil {
		t.Fatal(err)
	}
	in.Operation = "retained-before-switch"
	data, err := json.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(data)
	deadline := time.Now().Add(time.Second).UnixMilli()
	begun, err := f.engine.store.(*state.Store).Begin(context.Background(), logical, "old-worker", state.Input{ID: in.Operation, Kind: in.Kind,
		Fingerprint: hex.EncodeToString(sum[:]), Payload: data, DeadlineMS: deadline, ReceiptUntilMS: deadline + 120000,
		ApplicationLeaseIDs: []string{f.engine.applicationLeaseID(f.app)}})
	if err != nil || begun.Step == nil {
		t.Fatalf("legacy pending fixture failed: %v", err)
	}
	key := f.prefix + "{p0000}:group:" + logical.ID
	if err = f.client.HSet(context.Background(), key, "claimUntilMs", "0").Err(); err != nil {
		t.Fatal(err)
	}
	f.engine.runtime, err = execution.NewCAS(f.engine.store, execution.ReducerFunc(f.engine.reduce), 8)
	if err != nil {
		t.Fatal(err)
	}
	assertCASPendingRefused(t, f.engine, logical)
}

func TestStateCASCollectedEffectResultCannotChangeStateOrWriteAgain(t *testing.T) {
	f, ingress, conn := casOptionsFixture(t)
	write := casOptionsResponse(t, f, "collected-callback")
	if delivery := ingress.committedResult(context.Background(), write); !delivery.Recorded || delivery.Outcome != pb.DeliveryResult_WRITTEN {
		t.Fatal(delivery)
	}
	f.apply(t, write.Effect.Context, "collect", stateEngineInput{Kind: "locate"})
	before, _ := stateTestLoad(t, f.engine, write.Effect.Context)
	for _, e := range before.Payload.Effects {
		if e.ID == write.Effect.Id {
			t.Fatal("fixture retained the completed effect")
		}
	}
	result := f.apply(t, write.Effect.Context, "late-callback", stateEngineInput{Kind: "effect-result", Effect: write.Effect.Id,
		Reply: &pb.Envelope{Payload: &pb.Envelope_Result{Result: &pb.OperationResult{Certainty: pb.OperationResult_COMPLETED}}}})
	after, _ := stateTestLoad(t, f.engine, write.Effect.Context)
	if result.Certainty != pb.OperationResult_NOT_ACCEPTED || result.Snapshot != nil || result.Error != "effect not found" ||
		after.Revision != before.Revision || !bytes.Equal(after.Payload.ProtocolState, before.Payload.ProtocolState) || conn.deadlines != 1 {
		t.Fatal("collected callback changed a revision, protocol state or physical action")
	}
}

func TestStateCASCollectedEffectRefusesLegacyPendingWithoutCommit(t *testing.T) {
	f, ref, in := preflightFixture(t, "REGISTER")
	if result := f.apply(t, ref, "initial", in); result.Error != "" {
		t.Fatal(result.Error)
	}
	logical, err := f.engine.group(ref)
	if err != nil {
		t.Fatal(err)
	}
	in = stateEngineInput{Kind: "effect-result", Context: ref, Effect: "already-collected", Operation: "legacy-callback"}
	data, err := json.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(data)
	deadline := time.Now().Add(time.Second).UnixMilli()
	begun, err := f.engine.store.(*state.Store).Begin(context.Background(), logical, "old-worker", state.Input{ID: in.Operation, Kind: in.Kind, Fingerprint: hex.EncodeToString(sum[:]),
		Payload: data, Proof: true, DeadlineMS: deadline, ReceiptUntilMS: deadline})
	if err != nil || begun.Step == nil {
		t.Fatalf("legacy pending fixture failed: %v", err)
	}
	key := f.prefix + "{p0000}:group:" + logical.ID
	if err = f.client.HSet(context.Background(), key, "claimUntilMs", "0").Err(); err != nil {
		t.Fatal(err)
	}
	f.engine.runtime, err = execution.NewCAS(f.engine.store, execution.ReducerFunc(f.engine.reduce), 8)
	if err != nil {
		t.Fatal(err)
	}
	assertCASPendingRefused(t, f.engine, logical)
}
