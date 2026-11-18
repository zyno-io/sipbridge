package engine

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/execution"
	"sipbridge/internal/state"
)

type lostCASCompletion struct {
	*StateEngine
	lose bool
}

func (w *lostCASCompletion) Complete(ctx context.Context, write *pb.FlowWrite, result *pb.DeliveryResult) error {
	if err := w.StateEngine.Complete(ctx, write, result); err != nil {
		return err
	}
	if w.lose {
		return errors.New("simulated lost completion reply")
	}
	return nil
}

func casOptionsFixture(t *testing.T, backends ...string) (nativeOwnerFixture, *Ingress, *framingConn) {
	t.Helper()
	backend := state.RedisBackend
	if len(backends) > 0 {
		backend = backends[0]
	}
	f := newNativeOwnerFixtureBackend(t, backend)
	var err error
	f.engine.runtime, err = execution.NewCAS(f.engine.store, execution.ReducerFunc(f.engine.reduce), 8)
	if err != nil {
		t.Fatal(err)
	}
	ingress, initial, conn := stateWriterFixture(t, &evidenceWriter{})
	ingress.stateWriter = f.engine
	f.flow = initial.Flow
	return f, ingress, conn
}

func casOptionsResponse(t *testing.T, f nativeOwnerFixture, callID string) *pb.FlowWrite {
	t.Helper()
	message := stateTestRequest("OPTIONS", 1, "", callID)
	message, err := Observe(bytes.ReplaceAll(message.Wire, []byte("redis-native-call@example.test"), []byte(callID+"@example.test")), false)
	if err != nil {
		t.Fatal(err)
	}
	ref, err := f.engine.Locate(context.Background(), message)
	if err != nil {
		t.Fatal(err)
	}
	result := f.apply(t, ref, "ingest-"+callID, stateEngineInput{Kind: "ingest", Application: f.app, ApplicationLeaseTerm: f.term,
		Ingest: &pb.OwnerIngest{Context: ref, Application: f.app, Flow: f.flow, Wire: message.Wire, PrincipalId: "phone", BindingId: "binding"}})
	if result.Error != "" {
		t.Fatal(result.Error)
	}
	_, group := stateTestLoad(t, f.engine, ref)
	var request string
	for id := range group.Contexts[ref.Id].Requests {
		request = id
	}
	result = f.apply(t, ref, "respond-"+callID, stateEngineInput{Kind: "command", Application: f.app, ApplicationLeaseTerm: f.term,
		Command: &pb.Command{Context: ref, Sequence: 1, Action: &pb.Command_Respond{Respond: &pb.Respond{RequestId: request, Status: 200, Authority: f.responseAuthority(t, ref, request)}}}})
	if result.Error != "" {
		t.Fatal(result.Error)
	}
	record, _ := stateTestLoad(t, f.engine, ref)
	for _, effect := range record.Payload.Effects {
		var body stateEngineEffect
		if effect.Kind == "deliver" && unmarshalRetainedFixture(effect.Payload, &body) == nil && body.Native {
			return &pb.FlowWrite{Flow: body.Permit.Flow, Effect: body.Authorization.Effect, Permit: body.Permit}
		}
	}
	t.Fatal("CAS response did not produce a native delivery")
	return nil
}

func TestStateCASNativeResponseKeepsNoPureComputation(t *testing.T) {
	f, ingress, conn := casOptionsFixture(t)
	write := casOptionsResponse(t, f, "cas-options")
	result := ingress.committedResult(context.Background(), write)
	if result.Outcome != pb.DeliveryResult_WRITTEN || !result.Recorded {
		t.Fatalf("CAS response did not write and record: %+v", result)
	}
	if conn.deadlines != 1 || !bytes.HasPrefix(conn.sent.Bytes(), []byte("SIP/2.0 200")) {
		t.Fatal("CAS produced an incorrect physical response")
	}
	record, group := stateTestLoad(t, f.engine, write.Effect.Context)
	if record.Pending != nil || record.Proof != nil || len(group.Contexts[write.Effect.Context.Id].CommandFingerprint) != 64 {
		t.Fatal("CAS retained pure work or lost command replay identity")
	}
	if retry := ingress.committedResult(context.Background(), write); retry.Outcome == pb.DeliveryResult_NOT_WRITTEN || conn.deadlines != 1 {
		t.Fatalf("known write was physically repeated: %+v", retry)
	}
}

func TestStateCASLostCompletionCannotLeakCapacityOrResend(t *testing.T) {
	f, ingress, conn := casOptionsFixture(t)
	writer := &lostCASCompletion{StateEngine: f.engine, lose: true}
	ingress.stateWriter = writer
	write := casOptionsResponse(t, f, "lost-completion")
	result := ingress.committedResult(context.Background(), write)
	if result.Outcome != pb.DeliveryResult_WRITTEN || result.Recorded || len(ingress.stateAttempts) != 1 {
		t.Fatalf("fixture did not lose the committed acknowledgement: %+v", result)
	}
	f.apply(t, write.Effect.Context, "unrelated-progress", stateEngineInput{Kind: "locate"})
	record, _ := stateTestLoad(t, f.engine, write.Effect.Context)
	for _, effect := range record.Payload.Effects {
		if effect.ID == write.Effect.Id {
			t.Fatal("fixture failed to reclaim settled effect")
		}
	}
	key := write.Effect.Id + "/" + write.Effect.AttemptId
	ingress.mu.Lock()
	ingress.stateAttempts[key].until = time.Now().Add(-time.Millisecond)
	ingress.mu.Unlock()
	inspected := ingress.inspectCommitted(context.Background(), write)
	if inspected.Outcome != pb.DeliveryResult_UNKNOWN || inspected.Recorded || len(ingress.stateAttempts) != 0 || conn.deadlines != 1 {
		t.Fatalf("expired evidence invented certainty or kept capacity: %+v", inspected)
	}
	writer.lose = false
	next := casOptionsResponse(t, f, "healthy-after-loss")
	if healthy := ingress.committedResult(context.Background(), next); healthy.Outcome != pb.DeliveryResult_WRITTEN || !healthy.Recorded || conn.deadlines != 2 {
		t.Fatalf("lost acknowledgement blocked unrelated writing: %+v", healthy)
	}
	if replay := ingress.committedResult(context.Background(), write); replay.Outcome != pb.DeliveryResult_UNKNOWN || conn.deadlines != 2 {
		t.Fatalf("evicted memo allowed another physical send: %+v", replay)
	}
}

func TestStateCASCommandFingerprintGuardsReplayAfterReceiptPruning(t *testing.T) {
	f, _, _ := casOptionsFixture(t)
	write := casOptionsResponse(t, f, "fingerprint")
	ref := write.Effect.Context
	_, group := stateTestLoad(t, f.engine, ref)
	var request string
	for id := range group.Contexts[ref.Id].Requests {
		request = id
	}
	// Prove the current command independently guards payload identity after its
	// bounded generic replay receipt has expired/pruned.
	key := f.prefix + "{p0000}:group:" + ref.State.GroupId
	if err := f.client.HSet(context.Background(), key, "receipts", "[]", "proof", "").Err(); err != nil {
		t.Fatal(err)
	}
	result := f.apply(t, ref, "respond-fingerprint", stateEngineInput{Kind: "command", Application: f.app, ApplicationLeaseTerm: f.term,
		Command: &pb.Command{Context: proto.Clone(ref).(*pb.ContextRef), Sequence: 1, Action: &pb.Command_Respond{Respond: &pb.Respond{
			RequestId: request, Status: 202, Authority: f.responseAuthority(t, ref, request)}}}})
	if result.Certainty != pb.OperationResult_NOT_ACCEPTED || !strings.Contains(result.Error, "payload conflict") {
		t.Fatalf("changed equal-sequence command was accepted: %+v", result)
	}
}

func TestStateCASProcessControlReconnectPreservesDialog(t *testing.T) {
	t.Setenv("SIPBRIDGE_TEST_EXECUTION_MODE", "cas")
	TestStateNativeCurrentProcessControlReconnectPreservesDialog(t)
}

func TestStateCASProcessOwnerLossCompletesUDPAndTLS(t *testing.T) {
	t.Setenv("SIPBRIDGE_TEST_EXECUTION_MODE", "cas")
	TestStateNativeCurrentProcessAllControllerGoneCompletesUDPAndTLS(t)
}

func TestStateCASDuplicateRPCStartDoesNotChurnRevisionOrResumeLegacyInput(t *testing.T) {
	for _, legacyPending := range []bool{false, true} {
		t.Run(fmt.Sprintf("legacyPending=%t", legacyPending), func(t *testing.T) {
			f, _, _ := casOptionsFixture(t)
			ref := f.admit(t)
			effect, _ := receiverEvent(t, f, ref)
			effectID := effect.ID
			in := stateEngineInput{Kind: "effect-start", Effect: effectID}
			if result := f.apply(t, ref, "start-present", in); result.Snapshot == nil {
				t.Fatal("original RPC did not start")
			}
			before, _ := stateTestLoad(t, f.engine, ref)
			var result *pb.OperationResult
			if legacyPending {
				group, err := f.engine.group(ref)
				if err != nil {
					t.Fatal(err)
				}
				in.Context, in.Operation = ref, "legacy-duplicate"
				payload, err := marshalRetainedFixture(in)
				if err != nil {
					t.Fatal(err)
				}
				sum := sha256.Sum256(payload)
				deadline := time.Now().Add(2 * time.Second).UnixMilli()
				_, err = f.engine.store.(*state.Store).Begin(context.Background(), group, "old-worker", state.Input{ID: in.Operation, Kind: in.Kind,
					Fingerprint: hex.EncodeToString(sum[:]), Payload: payload, Proof: true, DeadlineMS: deadline, ReceiptUntilMS: deadline})
				if err != nil {
					t.Fatal(err)
				}
				key := f.prefix + "{p0000}:group:" + group.ID
				if err := f.client.HSet(context.Background(), key, "claimUntilMs", "1").Err(); err != nil {
					t.Fatal(err)
				}
				assertCASPendingRefused(t, f.engine, group)
				return
			} else {
				result = f.apply(t, ref, "start-present", in)
			}
			after, _ := stateTestLoad(t, f.engine, ref)
			if result.Certainty != pb.OperationResult_NOT_ACCEPTED || result.Snapshot != nil || after.Pending != nil || len(after.Payload.Effects) != len(before.Payload.Effects) {
				t.Fatal("duplicate start granted permission, allocated effects or stranded pending input")
			}
			if !legacyPending && after.Revision != before.Revision {
				t.Fatal("read-only start refusal consumed another revision")
			}
		})
	}
}

// A negative dispatch hint needs only effect metadata. It cannot expose a
// snapshot or permission from an unread protocol aggregate; positive starts and
// legacy pending recovery must still decode/validate that aggregate.
func TestStateCASNegativeEffectChecksMetadataBeforeNativeDecode(t *testing.T) {
	for _, kind := range []string{"effect-start", "effect-result"} {
		t.Run(kind, func(t *testing.T) {
			f, _, _ := casOptionsFixture(t)
			payload, err := marshalRetainedFixture(stateEngineInput{Kind: kind, Effect: "absent"})
			if err != nil {
				t.Fatal(err)
			}
			input := state.Input{Proof: true, Kind: kind, Payload: payload}
			record := state.Record{Payload: state.Payload{ProtocolState: []byte("invalid-native-payload")}}
			_, outcome, err := f.engine.reduce(record, input, time.Now().UnixMilli())
			var result pb.OperationResult
			if !errors.Is(err, execution.ErrReadOnly) || unmarshalRetainedFixture(outcome, &result) != nil ||
				result.Certainty != pb.OperationResult_NOT_ACCEPTED || result.Snapshot != nil {
				t.Fatalf("metadata refusal decoded native state or granted permission: %+v, %v", &result, err)
			}
			record.Pending = &input
			if _, _, err = f.engine.reduce(record, input, time.Now().UnixMilli()); !errors.Is(err, state.ErrCorrupt) {
				t.Fatalf("legacy pending recovery bypassed native validation: %v", err)
			}
			record.Pending = nil
			record.Payload.Effects = []state.Effect{{ID: "absent", Kind: "present", Status: state.EffectPending}}
			if _, _, err = f.engine.reduce(record, input, time.Now().UnixMilli()); !errors.Is(err, state.ErrCorrupt) {
				t.Fatalf("existing effect bypassed native validation: %v", err)
			}
		})
	}
}
