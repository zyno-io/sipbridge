package engine

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/state"
)

type evidenceWriter struct {
	mu                            sync.Mutex
	reserves, completions         int
	reserved                      *CommittedWrite
	reserveError, completionError error
	started, release              chan struct{}
}

type recordedInspector struct {
	*evidenceWriter
	lookups int
	stored  *pb.DeliveryResult
}

type isolatedReservationWriter struct{ started, release chan struct{} }

type failingNativeReservation struct {
	engine                           *StateEngine
	failReserve, commitBeforeError   bool
	nilReservation                   bool
	pendingAdmission                 bool
	failCompletion                   bool
	reserves, completions, inspected int
}

func (w *failingNativeReservation) Reserve(ctx context.Context, command *pb.FlowWrite) (*CommittedWrite, error) {
	w.reserves++
	if w.nilReservation {
		return nil, nil
	}
	if !w.failReserve {
		return w.engine.Reserve(ctx, command)
	}
	if w.pendingAdmission {
		ref, err := w.engine.group(command.Effect.Context)
		if err != nil {
			return nil, err
		}
		input := stateEngineInput{Kind: "reserve", Context: command.Effect.Context, Executor: w.engine.cfg.Process, Write: command, Effect: command.Effect.Id}
		data, err := json.Marshal(input)
		if err != nil {
			return nil, err
		}
		sum := sha256.Sum256(data)
		deadline := int64(command.Permit.Authorization.DeadlineMs)
		begun, err := w.engine.store.(*state.Store).Begin(ctx, ref, "paused-reserve-admission", state.Input{ID: w.engine.id("reserve", command.Effect.Id, command.Effect.AttemptId), Kind: "reserve", Fingerprint: hex.EncodeToString(sum[:]), Payload: data, DeadlineMS: deadline, ReceiptUntilMS: deadline, Proof: true})
		if err != nil || begun.Step == nil {
			return nil, errors.New("pending reservation fixture could not admit")
		}
		return nil, errors.New("simulated reservation admission reply loss")
	}
	if w.commitBeforeError {
		if _, err := w.engine.Reserve(ctx, command); err != nil {
			return nil, err
		}
	}
	return nil, errors.New("simulated reservation reply loss")
}

func (w *failingNativeReservation) Complete(ctx context.Context, command *pb.FlowWrite, result *pb.DeliveryResult) error {
	w.completions++
	if w.failCompletion {
		return errors.New("simulated completion outage")
	}
	return w.engine.Complete(ctx, command, result)
}

func (w *failingNativeReservation) InspectWrite(ctx context.Context, command *pb.FlowWrite) (*pb.DeliveryResult, error) {
	w.inspected++
	return w.engine.InspectWrite(ctx, command)
}

func nativeStateWriterFixture(t *testing.T) (*StateEngine, *Ingress, *pb.FlowWrite, *framingConn) {
	t.Helper()
	engine, _ := historicalStateTestEngine(t)
	ingress, initial, conn := stateWriterFixture(t, &evidenceWriter{})
	message := stateTestRequest("OPTIONS", 1, "", "native-writer")
	ref, err := engine.Locate(context.Background(), message)
	if err != nil {
		t.Fatal(err)
	}
	application := &pb.ProcessRef{InstanceId: "controller", Epoch: "owner"}
	if result := stateTestApply(t, engine, ref, "writer-ingest", stateEngineInput{Kind: "ingest", Application: application, Ingest: &pb.OwnerIngest{Context: ref, Application: application, Flow: initial.Flow, Wire: message.Wire}}); result.Error != "" {
		t.Fatal(result.Error)
	}
	_, group := stateTestLoad(t, engine, ref)
	var request string
	for id := range group.Contexts[ref.Id].Requests {
		request = id
	}
	if result := stateTestApply(t, engine, ref, "writer-response", stateEngineInput{Kind: "command", Application: application, Command: &pb.Command{Context: ref, Sequence: 1, Action: &pb.Command_Respond{Respond: &pb.Respond{RequestId: request, Status: 200}}}}); result.Error != "" {
		t.Fatal(result.Error)
	}
	record, _ := stateTestLoad(t, engine, ref)
	for _, effect := range record.Payload.Effects {
		if effect.Kind != "wire" {
			continue
		}
		var body stateEngineEffect
		if err := json.Unmarshal(effect.Payload, &body); err != nil {
			t.Fatal(err)
		}
		permit := &pb.SendPermit{Authorization: body.Authorization, Flow: initial.Flow, DirectoryEpoch: "directory", OrderingProof: "proof"}
		stateTestApply(t, engine, ref, "writer-authority-start", stateEngineInput{Kind: "effect-start", Effect: effect.ID})
		if result := stateTestApply(t, engine, ref, "writer-authority-result", stateEngineInput{Kind: "effect-result", Effect: effect.ID, Executor: engine.cfg.Process, Reply: &pb.Envelope{Payload: &pb.Envelope_SendPermit{SendPermit: permit}}}); result.Error != "" {
			t.Fatal(result.Error)
		}
		command := &pb.FlowWrite{Flow: initial.Flow, Effect: body.Authorization.Effect, Permit: permit}
		return engine, ingress, command, conn
	}
	t.Fatal("native fixture has no authorized delivery")
	return nil, nil, nil, nil
}

func (w *isolatedReservationWriter) Reserve(_ context.Context, cmd *pb.FlowWrite) (*CommittedWrite, error) {
	if cmd.Effect.Id == "blocked" {
		close(w.started)
		<-w.release
	}
	return &CommittedWrite{Wire: []byte(cmd.Effect.Id)}, nil
}
func (w *isolatedReservationWriter) Complete(context.Context, *pb.FlowWrite, *pb.DeliveryResult) error {
	return nil
}

func (w *recordedInspector) InspectWrite(context.Context, *pb.FlowWrite) (*pb.DeliveryResult, error) {
	w.lookups++
	return w.stored, nil
}

func (w *evidenceWriter) Reserve(context.Context, *pb.FlowWrite) (*CommittedWrite, error) {
	w.mu.Lock()
	w.reserves++
	w.mu.Unlock()
	if w.started != nil {
		close(w.started)
		<-w.release
	}
	return w.reserved, w.reserveError
}
func (w *evidenceWriter) Complete(context.Context, *pb.FlowWrite, *pb.DeliveryResult) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.completions++
	return w.completionError
}

func stateWriterFixture(t *testing.T, w *evidenceWriter) (*Ingress, *pb.FlowWrite, *framingConn) {
	t.Helper()
	process := &pb.ProcessRef{InstanceId: "sip-a", Epoch: "epoch"}
	ingress := NewIngress(process, nil, "sip.test:5061", 16)
	err := ingress.ConfigureStateRuntime(w, func(context.Context, *pb.SipMessage) (*pb.ContextSnapshot, error) { return nil, nil })
	if err != nil {
		t.Fatal(err)
	}
	conn := &framingConn{}
	flow, err := ingress.addStream(conn, "192.0.2.1", 5061, "tls")
	if err != nil {
		t.Fatal(err)
	}
	cmd := &pb.FlowWrite{Flow: flow.ref, Effect: &pb.StateEffectRef{Id: "effect", AttemptId: "attempt"}, Permit: &pb.SendPermit{Authorization: &pb.EffectAuthorization{DeadlineMs: uint64(time.Now().Add(time.Minute).UnixMilli())}}}
	return ingress, cmd, conn
}

func TestCommittedWriteCannotStartAfterOriginalPermitDeadline(t *testing.T) {
	writer := &evidenceWriter{reserved: &CommittedWrite{Wire: []byte("one-frame")}, completionError: errors.New("retain")}
	ingress, cmd, conn := stateWriterFixture(t, writer)
	cmd.Permit.Authorization.DeadlineMs = uint64(time.Now().Add(-time.Second).UnixMilli())
	if result := ingress.committedResult(context.Background(), cmd); result.Outcome != pb.DeliveryResult_NOT_WRITTEN || conn.sent.Len() != 0 || writer.reserves != 1 || writer.completions != 1 {
		t.Fatalf("expired permit wrote: %v", result)
	}
}

func TestCommittedWriteLostReceiptCannotSendTwice(t *testing.T) {
	writer := &evidenceWriter{reserved: &CommittedWrite{Wire: []byte("one-frame")}, completionError: errors.New("lost result acknowledgement")}
	ingress, cmd, conn := stateWriterFixture(t, writer)
	first := ingress.committedResult(context.Background(), cmd)
	second := ingress.committedResult(context.Background(), cmd)
	if first.Outcome != pb.DeliveryResult_WRITTEN || second.Outcome != pb.DeliveryResult_WRITTEN || first.Recorded || second.Recorded || writer.reserves != 1 || conn.sent.String() != "one-frame" {
		t.Fatalf("duplicate physical start: first=%v second=%v reserves=%d wire=%q", first, second, writer.reserves, conn.sent.String())
	}
}

func TestCommittedKnownOutcomeIsRecordedOnlyAfterValidatedCompletion(t *testing.T) {
	writer := &evidenceWriter{reserved: &CommittedWrite{Wire: []byte("one-frame")}}
	ingress, cmd, _ := stateWriterFixture(t, writer)
	if result := ingress.committedResult(context.Background(), cmd); !result.Recorded || result.Outcome != pb.DeliveryResult_WRITTEN {
		t.Fatalf("known committed outcome lacks proof: %v", result)
	}
}

func TestCommittedInspectionCanUseStoredTerminalWithoutPhysicalMemo(t *testing.T) {
	writer := &evidenceWriter{}
	ingress, cmd, conn := stateWriterFixture(t, writer)
	lookup := &recordedInspector{evidenceWriter: writer, stored: &pb.DeliveryResult{Effect: cmd.Effect, Outcome: pb.DeliveryResult_WRITTEN, Recorded: true}}
	ingress.stateWriter = lookup
	result := ingress.inspectCommitted(context.Background(), cmd)
	if !result.Recorded || result.Outcome != pb.DeliveryResult_WRITTEN || lookup.lookups != 1 || writer.reserves != 0 || writer.completions != 0 || conn.sent.Len() != 0 {
		t.Fatalf("stored inspection started physical work: %v", result)
	}
}

func TestCommittedReservationWaitCannotBlockAnotherDialogOnSameStream(t *testing.T) {
	ingress, blocked, conn := stateWriterFixture(t, &evidenceWriter{})
	writer := &isolatedReservationWriter{started: make(chan struct{}), release: make(chan struct{})}
	ingress.stateWriter = writer
	blocked.Effect.Id = "blocked"
	other := proto.Clone(blocked).(*pb.FlowWrite)
	other.Effect.Id = "other"
	first := make(chan *pb.DeliveryResult, 1)
	go func() { first <- ingress.committedResult(context.Background(), blocked) }()
	<-writer.started
	second := make(chan *pb.DeliveryResult, 1)
	go func() { second <- ingress.committedResult(context.Background(), other) }()
	select {
	case result := <-second:
		if result.Outcome != pb.DeliveryResult_WRITTEN || conn.sent.String() != "other" {
			t.Fatalf("unrelated dialog failed: %v", result)
		}
	case <-time.After(250 * time.Millisecond):
		close(writer.release)
		<-first
		<-second
		t.Fatal("Redis reservation held another dialog's stream")
	}
	close(writer.release)
	if result := <-first; result.Outcome != pb.DeliveryResult_WRITTEN || conn.sent.String() != "otherblocked" {
		t.Fatalf("blocked reservation did not resume safely: %v", result)
	}
}

func TestCommittedWriteConcurrentRetryIsUncertainWithoutSecondAttempt(t *testing.T) {
	writer := &evidenceWriter{reserved: &CommittedWrite{Wire: []byte("one-frame")}, completionError: errors.New("retain evidence"), started: make(chan struct{}), release: make(chan struct{})}
	ingress, cmd, conn := stateWriterFixture(t, writer)
	done := make(chan *pb.DeliveryResult, 1)
	go func() { done <- ingress.committedResult(context.Background(), cmd) }()
	<-writer.started
	retry := ingress.committedResult(context.Background(), cmd)
	if retry.Outcome != pb.DeliveryResult_UNKNOWN {
		t.Fatalf("pending write guessed: %v", retry)
	}
	close(writer.release)
	if result := <-done; result.Outcome != pb.DeliveryResult_WRITTEN {
		t.Fatal(result)
	}
	if writer.reserves != 1 || conn.sent.String() != "one-frame" {
		t.Fatalf("concurrent replay: reserves=%d wire=%q", writer.reserves, conn.sent.String())
	}
}

func TestCommittedWriteUsesStoredOutcomeWithoutAnotherSocketWrite(t *testing.T) {
	writer := &evidenceWriter{reserved: &CommittedWrite{Known: true, Outcome: pb.DeliveryResult_WRITTEN}}
	ingress, cmd, conn := stateWriterFixture(t, writer)
	if result := ingress.committedResult(context.Background(), cmd); result.Outcome != pb.DeliveryResult_WRITTEN || conn.sent.Len() != 0 || writer.reserves != 1 {
		t.Fatalf("known effect wrote: %v %q", result, conn.sent.String())
	}
}

func TestPhysicalWriteRequiresCommittedStateWriterAndExactIdentities(t *testing.T) {
	for _, test := range []struct {
		name       string
		invalidate func(*Ingress, *pb.FlowWrite)
	}{
		{"writer", func(ingress *Ingress, _ *pb.FlowWrite) { ingress.stateWriter = nil }},
		{"effect", func(_ *Ingress, cmd *pb.FlowWrite) { cmd.Effect = nil }},
		{"permit", func(_ *Ingress, cmd *pb.FlowWrite) { cmd.Permit = nil }},
		{"foreign-ingress", func(_ *Ingress, cmd *pb.FlowWrite) {
			cmd.Flow.Ingress = &pb.ProcessRef{InstanceId: cmd.Flow.Ingress.InstanceId, Epoch: "another-process"}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			writer := &evidenceWriter{reserved: &CommittedWrite{Wire: []byte("committed-frame")}}
			ingress, cmd, conn := stateWriterFixture(t, writer)
			test.invalidate(ingress, cmd)
			if result := ingress.committedResult(context.Background(), cmd); result.Outcome != pb.DeliveryResult_UNKNOWN || result.Error == "" || writer.reserves != 0 || conn.sent.Len() != 0 {
				t.Fatalf("unvalidated physical request reached reservation/socket: %v", result)
			}
		})
	}
}

func TestCommittedWriteUncertainReservationRetainsPositiveLocalNoStart(t *testing.T) {
	writer := &evidenceWriter{reserveError: errors.New("reservation outcome unknown"), completionError: errors.New("retain evidence")}
	ingress, cmd, conn := stateWriterFixture(t, writer)
	for count := 0; count < 2; count++ {
		if result := ingress.committedResult(context.Background(), cmd); result.Outcome != pb.DeliveryResult_NOT_WRITTEN || result.Recorded {
			t.Fatal(result)
		}
	}
	if writer.reserves != 1 || writer.completions != 2 || conn.sent.Len() != 0 {
		t.Fatalf("uncertain reservation replay: reserves=%d bytes=%q", writer.reserves, conn.sent.String())
	}
}

func TestCommittedFailedReservationMemoCapacityRecoversWithoutNewReserve(t *testing.T) {
	writer := &evidenceWriter{reserveError: errors.New("Redis unavailable"), completionError: errors.New("Redis unavailable")}
	ingress, template, conn := stateWriterFixture(t, writer)
	commands := make([]*pb.FlowWrite, 1024)
	for n := range commands {
		command := proto.Clone(template).(*pb.FlowWrite)
		command.Effect.Id = fmt.Sprintf("failed-reserve-%d", n)
		commands[n] = command
		if result := ingress.committedResult(context.Background(), command); result.Outcome != pb.DeliveryResult_NOT_WRITTEN || result.Recorded {
			t.Fatalf("failed reservation %d lost no-start evidence: %v", n, result)
		}
	}
	if len(ingress.stateAttempts) != 1024 || writer.reserves != 1024 || conn.sent.Len() != 0 {
		t.Fatal("failed reservation fixture did not retain bounded no-start evidence")
	}
	extra := proto.Clone(template).(*pb.FlowWrite)
	extra.Effect.Id = "blocked-by-evidence-capacity"
	if result := ingress.committedResult(context.Background(), extra); result.Outcome != pb.DeliveryResult_UNKNOWN || writer.reserves != 1024 {
		t.Fatal("full memo pool admitted another reservation")
	}
	writer.completionError = nil
	for _, command := range commands {
		if result := ingress.inspectCommitted(context.Background(), command); result.Outcome != pb.DeliveryResult_NOT_WRITTEN || !result.Recorded {
			t.Fatalf("read-only no-start completion could not recover: %v", result)
		}
	}
	if len(ingress.stateAttempts) != 0 || writer.reserves != 1024 || writer.completions != 2048 || conn.sent.Len() != 0 {
		t.Fatal("no-start recovery repeated Reserve/write or retained acknowledged evidence")
	}
	if result := ingress.committedResult(context.Background(), extra); result.Outcome != pb.DeliveryResult_NOT_WRITTEN || !result.Recorded || writer.reserves != 1025 {
		t.Fatal("reconciled evidence capacity did not reopen")
	}
}

func TestCommittedFailedReserveReconcilesLostCommitAndNeverCommittedInRedis(t *testing.T) {
	for _, mode := range []string{"never-committed", "lost-commit-reply", "pending-admission", "nil-reservation"} {
		t.Run(mode, func(t *testing.T) {
			engine, ingress, command, conn := nativeStateWriterFixture(t)
			writer := &failingNativeReservation{engine: engine, failReserve: true, commitBeforeError: mode == "lost-commit-reply", pendingAdmission: mode == "pending-admission", nilReservation: mode == "nil-reservation", failCompletion: true}
			ingress.stateWriter = writer
			first := ingress.committedResult(context.Background(), command)
			if first.Outcome != pb.DeliveryResult_NOT_WRITTEN || first.Recorded || conn.sent.Len() != 0 || writer.reserves != 1 || len(ingress.stateAttempts) != 1 {
				t.Fatalf("failed Reserve did not retain exact no-start evidence: %v", first)
			}
			writer.failCompletion = false
			second := ingress.inspectCommitted(context.Background(), command)
			if second.Outcome != pb.DeliveryResult_NOT_WRITTEN || !second.Recorded || conn.sent.Len() != 0 || writer.reserves != 1 || writer.completions != 2 || len(ingress.stateAttempts) != 0 {
				t.Fatalf("inspection could not commit exact no-start evidence: %v", second)
			}
			stored, err := engine.InspectWrite(context.Background(), command)
			if err != nil || !stored.Recorded || stored.Outcome != pb.DeliveryResult_NOT_WRITTEN {
				t.Fatalf("native no-start outcome not retained: %v %v", stored, err)
			}
			reserved, err := engine.Reserve(context.Background(), command)
			if err != nil || !reserved.Known || reserved.Outcome != pb.DeliveryResult_NOT_WRITTEN || len(reserved.Wire) != 0 {
				t.Fatalf("completed no-start attempt could be reserved again: %v %v", reserved, err)
			}
		})
	}
}

func TestCommittedDuplicateReadErrorRecoversEarlierRecordedWrittenFact(t *testing.T) {
	engine, ingress, command, conn := nativeStateWriterFixture(t)
	writer := &failingNativeReservation{engine: engine}
	ingress.stateWriter = writer
	first := ingress.committedResult(context.Background(), command)
	if first.Outcome != pb.DeliveryResult_WRITTEN || !first.Recorded || len(ingress.stateAttempts) != 0 || conn.sent.Len() == 0 {
		t.Fatalf("original write did not record and release its memo: %v", first)
	}
	written := conn.sent.String()
	writer.failReserve = true
	duplicate := ingress.committedResult(context.Background(), command)
	if duplicate.Outcome != pb.DeliveryResult_WRITTEN || !duplicate.Recorded || conn.sent.String() != written || writer.reserves != 2 || writer.completions != 2 || writer.inspected != 1 || len(ingress.stateAttempts) != 0 {
		t.Fatalf("duplicate read error contradicted earlier terminal fact or repeated socket write: %v", duplicate)
	}
}

func TestCommittedKnownNativeUnknownIsNeverReplacedByLocalNoStart(t *testing.T) {
	writer := &evidenceWriter{reserved: &CommittedWrite{Known: true, Outcome: pb.DeliveryResult_UNKNOWN}}
	ingress, command, conn := stateWriterFixture(t, writer)
	result := ingress.committedResult(context.Background(), command)
	if result.Outcome != pb.DeliveryResult_UNKNOWN || result.Recorded || conn.sent.Len() != 0 || writer.reserves != 1 || writer.completions != 0 || len(ingress.stateAttempts) != 0 {
		t.Fatalf("known native physical uncertainty became local no-start: %v", result)
	}
	if inspected := ingress.inspectCommitted(context.Background(), command); inspected.Outcome != pb.DeliveryResult_UNKNOWN || writer.reserves != 1 || writer.completions != 0 {
		t.Fatal("inspection reclassified or retried a native physical UNKNOWN")
	}
}

func TestCommittedKnownUnknownWithoutLocalStartCannotFillMemoCapacity(t *testing.T) {
	writer := &evidenceWriter{reserved: &CommittedWrite{Known: true, Outcome: pb.DeliveryResult_UNKNOWN}}
	ingress, template, conn := stateWriterFixture(t, writer)
	for n := 0; n < 1100; n++ {
		command := proto.Clone(template).(*pb.FlowWrite)
		command.Effect.Id = fmt.Sprintf("native-unknown-%d", n)
		if result := ingress.committedResult(context.Background(), command); result.Outcome != pb.DeliveryResult_UNKNOWN || result.Recorded {
			t.Fatalf("native UNKNOWN supplied a write or terminal fact: %v", result)
		}
	}
	if len(ingress.stateAttempts) != 0 || writer.reserves != 1100 || writer.completions != 0 || conn.sent.Len() != 0 {
		t.Fatal("native no-permission responses retained local syscall memos or wrote")
	}
}

func TestCommittedPartialSyscallUnknownNeverUsesNoStartReconciliation(t *testing.T) {
	writer := &evidenceWriter{reserved: &CommittedWrite{Wire: []byte("one-frame")}}
	ingress, command, conn := stateWriterFixture(t, writer)
	conn.writes = []partialWrite{{n: 3, err: errors.New("partial socket write")}}
	lookup := &recordedInspector{evidenceWriter: writer, stored: &pb.DeliveryResult{Effect: command.Effect, Outcome: pb.DeliveryResult_NOT_WRITTEN, Recorded: true}}
	ingress.stateWriter = lookup
	result := ingress.committedResult(context.Background(), command)
	if result.Outcome != pb.DeliveryResult_UNKNOWN || result.Recorded || conn.sent.String() != "one" || lookup.lookups != 0 || len(ingress.stateAttempts) != 1 {
		t.Fatalf("partial syscall was reclassified as no-start: %v", result)
	}
	if inspected := ingress.inspectCommitted(context.Background(), command); inspected.Outcome != pb.DeliveryResult_UNKNOWN || lookup.lookups != 0 || writer.reserves != 1 || writer.completions != 1 {
		t.Fatal("read-only inspection replaced genuine physical uncertainty")
	}
}

func TestCommittedUnreservedNoStartRequiresExactNativeProof(t *testing.T) {
	engine, _, command, _ := nativeStateWriterFixture(t)
	for _, mismatch := range []string{"physical-process", "flow", "attempt", "permit", "delivery-effect", "written", "unknown"} {
		t.Run(mismatch, func(t *testing.T) {
			copy := proto.Clone(command).(*pb.FlowWrite)
			physical := proto.Clone(engine.cfg.Process).(*pb.ProcessRef)
			delivery := &pb.DeliveryResult{Effect: copy.Effect, Outcome: pb.DeliveryResult_NOT_WRITTEN}
			switch mismatch {
			case "physical-process":
				physical.Epoch = "other-epoch"
			case "flow":
				copy.Flow.ConnectionId = "other-flow"
			case "attempt":
				copy.Effect.AttemptId = "other-attempt"
			case "permit":
				copy.Permit.OrderingProof = "other-proof"
			case "delivery-effect":
				delivery.Effect = proto.Clone(copy.Effect).(*pb.StateEffectRef)
				delivery.Effect.Id = "other-effect"
			case "written":
				delivery.Outcome = pb.DeliveryResult_WRITTEN
			case "unknown":
				delivery.Outcome = pb.DeliveryResult_UNKNOWN
			}
			result := stateTestApply(t, engine, command.Effect.Context, "bad-no-start-"+mismatch, stateEngineInput{Kind: "complete", Context: command.Effect.Context, Executor: physical, Write: copy, Delivery: delivery})
			if result.Certainty != pb.OperationResult_NOT_ACCEPTED {
				t.Fatalf("unreserved completion admitted mismatched proof: %v", result)
			}
		})
	}
	record, _ := stateTestLoad(t, engine, command.Effect.Context)
	for _, effect := range record.Payload.Effects {
		if effect.ID == command.Effect.Id && (effect.Status != state.EffectAuthorized || effect.AttemptID != "") {
			t.Fatal("rejected absence proof mutated the physical attempt")
		}
	}
}

func TestCommittedInspectionNeverReservesOrWritesAndRepairsKnownReceipt(t *testing.T) {
	writer := &evidenceWriter{reserved: &CommittedWrite{Wire: []byte("one-frame")}, completionError: errors.New("lost receipt")}
	ingress, cmd, conn := stateWriterFixture(t, writer)
	if result := ingress.inspectCommitted(context.Background(), cmd); result.Outcome != pb.DeliveryResult_UNKNOWN || writer.reserves != 0 || writer.completions != 0 || conn.sent.Len() != 0 {
		t.Fatalf("inspection started work: %v", result)
	}
	if result := ingress.committedResult(context.Background(), cmd); result.Outcome != pb.DeliveryResult_WRITTEN {
		t.Fatal(result)
	}
	ingress.mu.Lock()
	delete(ingress.flows, cmd.Flow.ConnectionId)
	ingress.mu.Unlock()
	writer.completionError = nil
	if result := ingress.inspectCommitted(context.Background(), cmd); result.Outcome != pb.DeliveryResult_WRITTEN || writer.reserves != 1 || writer.completions != 2 || conn.sent.String() != "one-frame" {
		t.Fatalf("inspection failed to reconcile original physical evidence: %v", result)
	}
	if len(ingress.stateAttempts) != 0 {
		t.Fatal("acknowledged known evidence retained")
	}
}

func TestCommittedInspectionOfInFlightAttemptCannotWriteOrGuess(t *testing.T) {
	writer := &evidenceWriter{reserved: &CommittedWrite{Wire: []byte("one-frame")}, completionError: errors.New("retain"), started: make(chan struct{}), release: make(chan struct{})}
	ingress, cmd, conn := stateWriterFixture(t, writer)
	done := make(chan *pb.DeliveryResult, 1)
	go func() { done <- ingress.committedResult(context.Background(), cmd) }()
	<-writer.started
	if result := ingress.inspectCommitted(context.Background(), cmd); result.Outcome != pb.DeliveryResult_UNKNOWN {
		t.Fatal(result)
	}
	close(writer.release)
	<-done
	if writer.reserves != 1 || conn.sent.String() != "one-frame" {
		t.Fatal("inspection duplicated physical attempt")
	}
}
