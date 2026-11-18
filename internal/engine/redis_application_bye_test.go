package engine

import (
	"bytes"
	"context"
	"errors"
	"net"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/nativepb"
	"sipbridge/internal/rediscore"
	"sipbridge/internal/storagecodec"
)

// testRedisApplicationByeAdmission runs after the real UAC winner and ACK
// have been selected and physically completed by the enclosing fixture.
func testRedisApplicationByeAdmission(t *testing.T, ctx context.Context, store *rediscore.Store, client *redis.Client, root string, input redisUACQualification, workerID, primary string, timeout, refusal bool, runtimeCase int) {
	t.Helper()
	if _, err := store.RenewApplication(ctx, input.Lease); err != nil {
		t.Fatal(err)
	}
	if _, err := store.RenewWorker(ctx, rediscore.WorkerLeaseRequest{Worker: workerID, Epoch: input.Executor.Epoch, PrimaryRunID: primary}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	command := &pb.Command{Context: input.Context, Sequence: 2, Admission: &pb.CommandAdmission{DirectoryEpoch: "directory", ExpiresAtMs: uint64(deadline.UnixMilli()), SelectedFlow: input.Flow}, Action: &pb.Command_DialogRequest{DialogRequest: &pb.DialogRequest{Method: "BYE"}}}
	capture, err := captureRedisNotifyWait(command)
	if err != nil {
		t.Fatal(err)
	}
	identity, err := publishRedisApplicationBye(ctx, store, input.Key, input.Application, input.Lease, command, capture, "bye-operation", input.ExecutorTimer, deadline)
	if err != nil {
		t.Fatal("publish BYE", err)
	}
	view, err := store.Read(ctx, identity, "r:context", "r:result-2")
	if err != nil {
		t.Fatal(err)
	}
	current, err := decodeNativeContext(view.Records["r:context"])
	if err != nil || current.CommandSequence != 2 || len(current.NoninitialProcedures) != 1 {
		t.Fatal("missing BYE ownership", err)
	}
	result, err := decodeNativeResult(view.Records["r:result-2"])
	if err != nil || !result.Pending || result.Output == "" || result.CutoffMs != deadline.UnixMilli() {
		t.Fatal("missing pending exact receipt", err)
	}
	selected, err := store.Read(ctx, identity, current.NoninitialProcedures[0], result.Output)
	if err != nil {
		t.Fatal(err)
	}
	procedure, err := readPrivateProcedureRecord(selected.Records[current.NoninitialProcedures[0]])
	if err != nil || procedure.Native.Family != string(portable.NonInviteClient) || procedure.CommandResult != "r:result-2" || procedure.CreditContribution != uint64(residentSmallBudget) {
		t.Fatal("missing independently funded native client", err)
	}
	output, err := decodeNativeOutput(selected.Records[result.Output])
	if err != nil || output.Method != string(sip.BYE) || output.Procedure != current.NoninitialProcedures[0] || output.Result != "r:result-2" || output.Write.Permit.Authorization.AuthorityClass != pb.EffectAuthorization_APPLICATION {
		t.Fatal("missing exact application BYE intent", err)
	}
	dialogView, err := store.Read(ctx, identity, output.Dialog)
	if err != nil {
		t.Fatal(err)
	}
	dialog, err := readPrivateDialogRecord(dialogView.Records[output.Dialog])
	if err != nil || dialog.LocalBye != procedure.Origin || dialog.LocalTransaction != procedure.Native.Key || !slices.Contains(dialog.Outputs, result.Output) || !slices.Contains(procedure.Outputs, result.Output) {
		t.Fatal("missing reciprocal BYE ownership", err)
	}
	timer := procedure.Native.Timers.Values[string(portable.TimerF)]
	if timer == nil || !timer.Active {
		t.Fatal("missing native F timer")
	}
	member := "app-bye-" + strings.TrimPrefix(current.NoninitialProcedures[0], "r:app-bye-") + "-F"
	score, err := client.ZScore(ctx, root+"core:deadlines:uac", member).Result()
	if err != nil || int64(score) != timer.At {
		t.Fatal("missing exact indexed F deadline", err)
	}
	if runtimeCase >= 32 && runtimeCase <= 34 {
		testRedisApplicationByeRuntime(t, ctx, store, input, workerID, primary, identity, capture, output, result.Output, deadline, runtimeCase)
		return
	}
	reservation, err := (&nativeRedisWriter{store: store}).Reserve(ctx, output.Write)
	if err != nil || reservation == nil || len(reservation.Wire) == 0 {
		t.Fatal("reserve actual BYE", err)
	}
	message, err := portable.ParseMessage(reservation.Wire, input.Flow.Transport, "", "")
	if err != nil {
		t.Fatal(err)
	}
	facts, err := message.Facts()
	if err != nil || !facts.IsRequest || facts.Method != sip.BYE {
		t.Fatal("reserved wire is not BYE", err)
	}
	writer := nativeRedisWriter{store: store}
	if err = writer.Complete(ctx, output.Write, &pb.DeliveryResult{Effect: output.Write.Effect, Outcome: pb.DeliveryResult_UNKNOWN}); err != nil {
		t.Fatal(err)
	}
	ref, err := nativeOutputRef(output.Write)
	if err != nil {
		t.Fatal(err)
	}
	proof, err := store.InspectOutput(ctx, ref)
	if err != nil || proof.State != rediscore.OutputUnknown {
		t.Fatal("missing UNKNOWN proof", err)
	}
	cap := &nativeOutputCapture{ref: ref, proof: proof, metadata: output}
	if err = settleRedisApplicationByeOutput(ctx, store, output.Write, cap, input.ExecutorTimer, deadline); err != nil {
		t.Fatal(err)
	}
	unknown, err := store.Read(ctx, identity, "r:result-2")
	if err != nil {
		t.Fatal(err)
	}
	unknownResult, err := decodeNativeResult(unknown.Records["r:result-2"])
	if err != nil || unknownResult.Pending || unknownResult.Certainty != pb.OperationResult_UNKNOWN {
		t.Fatal("uncertain proof did not finish UNKNOWN receipt", err)
	}
	if err = writer.Complete(ctx, output.Write, &pb.DeliveryResult{Effect: output.Write.Effect, Outcome: pb.DeliveryResult_WRITTEN}); err != nil {
		t.Fatal(err)
	}
	proof, err = store.InspectOutput(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	cap.proof = proof
	if err = settleRedisApplicationByeOutput(ctx, store, output.Write, cap, input.ExecutorTimer, deadline); err != nil {
		t.Fatal(err)
	}
	if err = acknowledgeRedisApplicationByeOutput(ctx, store, output.Write, deadline); err != nil {
		t.Fatal(err)
	}
	after, err := store.Read(ctx, identity, "r:result-2", current.NoninitialProcedures[0])
	if err != nil {
		t.Fatal(err)
	}
	result, err = decodeNativeResult(after.Records["r:result-2"])
	if err != nil || result.Pending || result.Certainty != pb.OperationResult_ACCEPTED || result.Request != procedure.Origin {
		t.Fatal("late written proof did not upgrade exact BYE receipt", err)
	}
	procedure, err = readPrivateProcedureRecord(after.Records[current.NoninitialProcedures[0]])
	if err != nil || !procedure.OutputConsumed {
		t.Fatal("definite proof released without consumption", err)
	}
	if refusal {
		before, e := store.Read(ctx, identity, "r:context", output.Dialog, output.Procedure)
		if e != nil {
			t.Fatal(e)
		}
		second := &pb.Command{Context: input.Context, Sequence: 3, Admission: &pb.CommandAdmission{DirectoryEpoch: "directory", ExpiresAtMs: uint64(deadline.UnixMilli()), SelectedFlow: input.Flow}, Action: &pb.Command_DialogRequest{DialogRequest: &pb.DialogRequest{Method: "BYE"}}}
		secondCapture, e := captureRedisNotifyWait(second)
		if e != nil {
			t.Fatal(e)
		}
		if _, e = publishRedisApplicationBye(ctx, store, input.Key, input.Application, input.Lease, second, secondCapture, "second-bye", input.ExecutorTimer, deadline); e != nil {
			t.Fatal("record semantic refusal", e)
		}
		after, e := store.Read(ctx, identity, "r:context", output.Dialog, output.Procedure, "r:result-3")
		if e != nil {
			t.Fatal(e)
		}
		rejected, e := decodeNativeResult(after.Records["r:result-3"])
		if e != nil || rejected.Certainty != pb.OperationResult_NOT_ACCEPTED || rejected.Output != "" {
			t.Fatal("BYE refusal receipt", e)
		}
		if !bytes.Equal(before.Records[output.Dialog], after.Records[output.Dialog]) || !bytes.Equal(before.Records[output.Procedure], after.Records[output.Procedure]) || before.Reserved != after.Reserved {
			t.Fatal("BYE refusal mutated native state or credit")
		}
		return
	}
	if timeout {
		// Advance the existing F deadline and its matching indexed projection;
		// the actual SDK timer handler, not a fabricated phase, ends the call.
		now, e := client.Time(ctx).Result()
		if e != nil {
			t.Fatal(e)
		}
		v, e := store.ObserveContext(ctx, identity.Context, identity.Incarnation, current.NoninitialProcedures[0])
		if e != nil {
			t.Fatal(e)
		}
		p, e := readPrivateProcedureRecord(v.View.Records[current.NoninitialProcedures[0]])
		if e != nil {
			t.Fatal(e)
		}
		f := p.Native.Timers.Values[string(portable.TimerF)]
		if f == nil || !f.Active {
			t.Fatal("BYE F no longer active")
		}
		f.At = now.Add(-time.Millisecond).UnixMilli()
		encoded, e := marshalNativeRecord(storagecodec.NativeProcedure, p)
		if e != nil {
			t.Fatal(e)
		}
		_, e = store.Commit(ctx, rediscore.Publication{Identity: v.Identity, Revision: v.View.Revision, Deadline: deadline, SpendReservation: true, Changes: []rediscore.Change{{Field: current.NoninitialProcedures[0], Value: encoded}}, Timers: &rediscore.TimerMutation{Expected: v.View.Timer, Target: v.View.Timer, RequireLease: true, Changes: []rediscore.TimerChange{{Member: member, Due: time.UnixMilli(f.At)}}}})
		if e != nil {
			t.Fatal(e)
		}
		v, e = store.ObserveContext(ctx, identity.Context, identity.Incarnation, "r:context")
		if e != nil {
			t.Fatal(e)
		}
		due := &rediscore.DueContext{Identity: v.Identity, Revision: v.View.Revision, Assignment: v.View.Timer, Member: member, DeadlineMS: f.At, CheckedAtMS: now.UnixMilli(), Observed: time.Now()}
		if e = fireRedisCleanupByeTimer(ctx, store, due, deadline); e != nil {
			t.Fatal("BYE F transition", e)
		}
		finished, e := store.Read(ctx, identity, "r:context", "r:result-2")
		if e != nil {
			t.Fatal(e)
		}
		result, e = decodeNativeResult(finished.Records["r:result-2"])
		if e != nil || result.Certainty != pb.OperationResult_COMPLETED || result.Error == "" || result.Request != procedure.Origin || result.Response != "" {
			t.Fatal("BYE timeout receipt", e)
		}
		current, e = decodeNativeContext(finished.Records["r:context"])
		if e != nil || !current.Ended {
			t.Fatal("BYE timeout did not end call", e)
		}
		testRedisApplicationByeRetirement(t, ctx, store, client, root, input, workerID, primary, identity, output, result, false)
		return
	}
	response, err := portable.BuildResponseWithHeaders(message, 200, "OK", nil, "", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	response = portable.RestoreMessage(response.WireBytes(), input.Flow.Transport, net.JoinHostPort(input.Flow.SourceIp, strconv.Itoa(int(input.Flow.SourcePort))), "")
	handled, err := completeRedisCleanupByeResponse(ctx, store, input.Flow, response, nil, deadline)
	if err != nil || !handled {
		t.Fatal("matched BYE final", err)
	}
	finished, err := store.Read(ctx, identity, "r:context", "r:result-2")
	if err != nil {
		t.Fatal(err)
	}
	result, err = decodeNativeResult(finished.Records["r:result-2"])
	if err != nil || result.Certainty != pb.OperationResult_COMPLETED || result.SipStatus != 200 || result.Request != procedure.Origin || result.Response == "" {
		t.Fatal("final did not upgrade exact BYE receipt", err)
	}
	current, err = decodeNativeContext(finished.Records["r:context"])
	if err != nil || !current.Ended || current.CallRetireAtMs == 0 {
		t.Fatal("BYE final did not end selected call", err)
	}
	testRedisApplicationByeRetirement(t, ctx, store, client, root, input, workerID, primary, identity, output, result, true)
}

func testRedisApplicationByeRuntime(t *testing.T, ctx context.Context, store *rediscore.Store, input redisUACQualification, workerID, primary string, identity rediscore.Identity, capture redisNotifyWaitCapture, output *nativepb.Output, outputField string, deadline time.Time, runtimeCase int) {
	t.Helper()
	lease, err := store.RenewWorker(ctx, rediscore.WorkerLeaseRequest{Worker: workerID, Epoch: input.Executor.Epoch, PrimaryRunID: primary})
	if err != nil {
		t.Fatal(err)
	}
	e := &redisNative{cfg: Config{Process: input.Executor, StateIdentityKey: input.Key}, worker: workerID, resultWaiters: make(map[rediscore.ResultHint]*nativeResultWaiter)}
	var wire []byte
	called := false
	outcome := pb.DeliveryResult_WRITTEN
	if runtimeCase == 33 {
		outcome = pb.DeliveryResult_NOT_WRITTEN
	} else if runtimeCase == 34 {
		outcome = pb.DeliveryResult_UNKNOWN
	}
	e.cfg.WriteCommitted = func(ctx context.Context, write *pb.FlowWrite) *pb.DeliveryResult {
		called = true
		reservation, reserveErr := (nativeRedisWriter{store: store}).Reserve(ctx, write)
		if reserveErr != nil {
			return &pb.DeliveryResult{Effect: write.Effect, Outcome: pb.DeliveryResult_UNKNOWN, Error: reserveErr.Error()}
		}
		wire = bytes.Clone(reservation.Wire)
		return &pb.DeliveryResult{Effect: write.Effect, Outcome: outcome}
	}
	hint := rediscore.ResultHint{Context: identity.Context, Incarnation: identity.Incarnation, Sequence: capture.sequence}
	waiter, err := e.registerResultWaiter(hint)
	if err != nil {
		t.Fatal(err)
	}
	defer e.releaseResultWaiter(hint, waiter)
	ready, err := store.PeekReady(ctx, workerID, input.Executor.Epoch, rediscore.MaxReadyPeek)
	if err != nil {
		t.Fatal(err)
	}
	var selected *rediscore.ReadyOutput
	for i := range ready {
		if ready[i].MetadataField == outputField {
			selected = &ready[i]
			break
		}
	}
	if selected == nil {
		t.Fatal("application BYE was not indexed for its physical worker")
	}
	badMetadata := proto.Clone(output).(*nativepb.Output)
	badMetadata.Procedure = "r:app-bye-00000000000000000000000000000000"
	badBytes, err := encodeNativeOutput(badMetadata)
	if err != nil {
		t.Fatal(err)
	}
	badReady := *selected
	badReady.Metadata = badBytes
	if err = e.readyOutput(ctx, store, badReady); err == nil || called {
		t.Fatal("mismatched BYE metadata reached the physical writer")
	}
	if err = e.readyOutput(ctx, store, *selected); err != nil {
		t.Fatalf("route application BYE output: %v", err)
	}
	answer, err := e.awaitRedisApplicationByeResult(ctx, store, identity, input.Lease, capture, "bye-operation", deadline, lease.Until, waiter, "BYE")
	if err != nil || answer == nil {
		t.Fatalf("original BYE waiter: %+v %v", answer, err)
	}
	if outcome == pb.DeliveryResult_WRITTEN && (answer.Certainty != pb.OperationResult_ACCEPTED || answer.Request == nil || !bytes.Equal(answer.Request.Wire, wire)) || outcome != pb.DeliveryResult_WRITTEN && (answer.Certainty != pb.OperationResult_UNKNOWN || answer.Request != nil) {
		t.Fatalf("original BYE waiter outcome: %+v", answer)
	}
	ref, err := nativeOutputRef(output.Write)
	if err != nil {
		t.Fatal(err)
	}
	proof, err := store.InspectOutput(ctx, ref)
	if outcome == pb.DeliveryResult_UNKNOWN {
		if err != nil || proof.State != rediscore.OutputUnknown || proof.Settled {
			t.Fatalf("unknown BYE proof was not retained: %v", err)
		}
	} else if !errors.Is(err, rediscore.ErrAbsent) {
		t.Fatalf("definite BYE proof was not consumed: %v", err)
	}
}

func testRedisApplicationDialogRequest(t *testing.T, ctx context.Context, store *rediscore.Store, client *redis.Client, root string, input redisUACQualification, workerID, primary, method string, timeout bool, outcome pb.DeliveryResult_Outcome, terminal ...portable.Message) {
	t.Helper()
	if _, err := store.RenewApplication(ctx, input.Lease); err != nil {
		t.Fatal(err)
	}
	workerLease, err := store.RenewWorker(ctx, rediscore.WorkerLeaseRequest{Worker: workerID, Epoch: input.Executor.Epoch, PrimaryRunID: primary})
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	command := &pb.Command{Context: input.Context, Sequence: 2, Admission: &pb.CommandAdmission{DirectoryEpoch: "directory", ExpiresAtMs: uint64(deadline.UnixMilli()), SelectedFlow: input.Flow}, Action: &pb.Command_DialogRequest{DialogRequest: &pb.DialogRequest{Method: method}}}
	capture, err := captureRedisNotifyWait(command)
	if err != nil {
		t.Fatal(err)
	}
	identity, err := publishRedisApplicationBye(ctx, store, input.Key, input.Application, input.Lease, command, capture, "dialog-operation", input.ExecutorTimer, deadline)
	if err != nil {
		t.Fatal("publish dialog request", err)
	}
	view, err := store.Read(ctx, identity, "r:context", "r:result-2")
	if err != nil {
		t.Fatal(err)
	}
	result, err := decodeNativeResult(view.Records["r:result-2"])
	if err != nil || !result.Pending || result.Output == "" {
		t.Fatal("missing pending dialog result", err)
	}
	selected, err := store.Read(ctx, identity, result.Output)
	if err != nil {
		t.Fatal(err)
	}
	output, err := decodeNativeOutput(selected.Records[result.Output])
	if err != nil || output.Method != method {
		t.Fatal("wrong selected dialog output", err)
	}
	e := &redisNative{cfg: Config{Process: input.Executor, StateIdentityKey: input.Key}, worker: workerID, resultWaiters: make(map[rediscore.ResultHint]*nativeResultWaiter)}
	var wire []byte
	e.cfg.WriteCommitted = func(ctx context.Context, write *pb.FlowWrite) *pb.DeliveryResult {
		reservation, reserveErr := (nativeRedisWriter{store: store}).Reserve(ctx, write)
		if reserveErr != nil {
			return &pb.DeliveryResult{Effect: write.Effect, Outcome: pb.DeliveryResult_UNKNOWN, Error: reserveErr.Error()}
		}
		wire = bytes.Clone(reservation.Wire)
		return &pb.DeliveryResult{Effect: write.Effect, Outcome: outcome}
	}
	hint := rediscore.ResultHint{Context: identity.Context, Incarnation: identity.Incarnation, Sequence: 2}
	waiter, err := e.registerResultWaiter(hint)
	if err != nil {
		t.Fatal(err)
	}
	defer e.releaseResultWaiter(hint, waiter)
	ready, err := store.PeekReady(ctx, workerID, input.Executor.Epoch, rediscore.MaxReadyPeek)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, item := range ready {
		if item.MetadataField == result.Output {
			found = true
			if err = e.readyOutput(ctx, store, item); err != nil {
				t.Fatal("route dialog request", err)
			}
			break
		}
	}
	if !found || len(wire) == 0 {
		t.Fatal("dialog request was not written")
	}
	view, err = store.Read(ctx, identity, "r:result-2")
	if err != nil {
		t.Fatal(err)
	}
	result, err = decodeNativeResult(view.Records["r:result-2"])
	if outcome != pb.DeliveryResult_WRITTEN {
		if err != nil || result.Pending || result.Certainty != pb.OperationResult_UNKNOWN || result.Request != "" {
			t.Fatal("uncertain dialog write did not return UNKNOWN", err)
		}
		answer, x := e.awaitRedisApplicationByeResult(ctx, store, identity, input.Lease, capture, "dialog-operation", deadline, workerLease.Until, waiter, method)
		if x != nil || answer == nil || answer.Certainty != pb.OperationResult_UNKNOWN || answer.Request != nil {
			t.Fatalf("uncertain dialog waiter: %+v %v", answer, x)
		}
		ref, x := nativeOutputRef(output.Write)
		if x != nil {
			t.Fatal(x)
		}
		proof, x := store.InspectOutput(ctx, ref)
		if outcome == pb.DeliveryResult_UNKNOWN && (x != nil || proof.State != rediscore.OutputUnknown) || outcome == pb.DeliveryResult_NOT_WRITTEN && !errors.Is(x, rediscore.ErrAbsent) {
			t.Fatal("uncertain/definite proof lifetime mismatch", x)
		}
		if len(terminal) == 2 {
			request := portable.RestoreMessage(wire, input.Flow.Transport, "", "")
			response, responseErr := portable.BuildResponseWithHeaders(request, 200, "OK", nil, "", "", nil)
			if responseErr != nil {
				t.Fatal(responseErr)
			}
			response = portable.RestoreMessage(response.WireBytes(), input.Flow.Transport, net.JoinHostPort(input.Flow.SourceIp, strconv.Itoa(int(input.Flow.SourcePort))), "")
			if handled, responseErr := completeRedisCleanupByeResponse(ctx, store, input.Flow, response, nil, deadline); responseErr != nil || !handled {
				t.Fatal("matched uncertain dialog response", responseErr)
			}
			testWinningAckUnknownTerminal(t, ctx, client, store, root, input, workerID, terminal[0], terminal[1], output, result.Output, true)
		}
		return
	}
	if err != nil || !result.Pending {
		t.Fatal("written non-BYE command finished before a final response", err)
	}
	if timeout {
		selected, x := store.ObserveContext(ctx, identity.Context, identity.Incarnation, "r:context", output.Procedure)
		if x != nil {
			t.Fatal(x)
		}
		procedure, x := readPrivateProcedureRecord(selected.View.Records[output.Procedure])
		if x != nil {
			t.Fatal(x)
		}
		f := procedure.Native.Timers.Values[string(portable.TimerF)]
		if f == nil || !f.Active {
			t.Fatal("missing original dialog F")
		}
		now, x := client.Time(ctx).Result()
		if x != nil {
			t.Fatal(x)
		}
		f.At = now.Add(-time.Millisecond).UnixMilli()
		encoded, x := marshalNativeRecord(storagecodec.NativeProcedure, procedure)
		if x != nil {
			t.Fatal(x)
		}
		member := "app-client-" + strings.TrimPrefix(output.Procedure, "r:app-client-") + "-F"
		_, x = store.Commit(ctx, rediscore.Publication{Identity: selected.Identity, Revision: selected.View.Revision, Deadline: deadline, SpendReservation: true, Changes: []rediscore.Change{{Field: output.Procedure, Value: encoded}}, Timers: &rediscore.TimerMutation{Expected: selected.View.Timer, Target: selected.View.Timer, RequireLease: true, Changes: []rediscore.TimerChange{{Member: member, Due: time.UnixMilli(f.At)}}}})
		if x != nil {
			t.Fatal(x)
		}
		selected, x = store.ObserveContext(ctx, identity.Context, identity.Incarnation, "r:context")
		if x != nil {
			t.Fatal(x)
		}
		due := &rediscore.DueContext{Identity: selected.Identity, Revision: selected.View.Revision, Assignment: selected.View.Timer, Member: member, DeadlineMS: f.At, CheckedAtMS: now.UnixMilli(), Observed: time.Now()}
		if x = fireRedisCleanupByeTimer(ctx, store, due, deadline); x != nil {
			t.Fatal("dialog F", x)
		}
		answer, x := e.awaitRedisApplicationByeResult(ctx, store, identity, input.Lease, capture, "dialog-operation", deadline, workerLease.Until, waiter, method)
		if x != nil || answer == nil || answer.Certainty != pb.OperationResult_COMPLETED || answer.Error == "" || answer.Request == nil || !bytes.Equal(answer.Request.Wire, wire) || answer.Snapshot.Ended {
			t.Fatalf("dialog timeout result: %+v %v", answer, x)
		}
		view, x = store.Read(ctx, identity, "r:result-2")
		if x != nil {
			t.Fatal(x)
		}
		result, x = decodeNativeResult(view.Records["r:result-2"])
		if x != nil {
			t.Fatal(x)
		}
		testRedisApplicationByeRetirement(t, ctx, store, client, root, input, workerID, primary, identity, output, result, false)
		return
	}
	message, err := portable.ParseMessage(wire, input.Flow.Transport, "", "")
	if err != nil {
		t.Fatal(err)
	}
	response, err := portable.BuildResponseWithHeaders(message, 200, "OK", nil, "", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	response = portable.RestoreMessage(response.WireBytes(), input.Flow.Transport, net.JoinHostPort(input.Flow.SourceIp, strconv.Itoa(int(input.Flow.SourcePort))), "")
	if handled, responseErr := completeRedisCleanupByeResponse(ctx, store, input.Flow, response, nil, deadline); responseErr != nil || !handled {
		t.Fatal("matched dialog response", responseErr)
	}
	answer, err := e.awaitRedisApplicationByeResult(ctx, store, identity, input.Lease, capture, "dialog-operation", deadline, workerLease.Until, waiter, method)
	if err != nil || answer == nil || answer.Certainty != pb.OperationResult_COMPLETED || answer.SipStatus != 200 || answer.Request == nil || !bytes.Equal(answer.Request.Wire, wire) || answer.Snapshot.Ended {
		t.Fatalf("dialog result: %+v %v", answer, err)
	}
	view, err = store.Read(ctx, identity, "r:result-2")
	if err != nil {
		t.Fatal(err)
	}
	result, err = decodeNativeResult(view.Records["r:result-2"])
	if err != nil {
		t.Fatal(err)
	}
	testRedisApplicationByeRetirement(t, ctx, store, client, root, input, workerID, primary, identity, output, result, true)
}

// The actual result remains visible while its native transaction/output
// retire. Timer K's elapsed timestamp and projection are advanced only by the
// fixture; the real SDK handler performs the transition.
func testRedisApplicationByeRetirement(t *testing.T, ctx context.Context, store *rediscore.Store, client *redis.Client, root string, input redisUACQualification, workerID, primary string, identity rediscore.Identity, output *nativepb.Output, result *nativepb.Result, fireK bool) {
	t.Helper()
	deadline := func() time.Time { return time.Now().Add(time.Second) }
	if fireK {
		view, err := store.ObserveContext(ctx, identity.Context, identity.Incarnation, output.Procedure)
		if err != nil {
			t.Fatal(err)
		}
		p, err := readPrivateProcedureRecord(view.View.Records[output.Procedure])
		if err != nil {
			t.Fatal(err)
		}
		k := p.Native.Timers.Values[string(portable.TimerK)]
		if k == nil || !k.Active {
			t.Fatal("BYE K missing")
		}
		now, err := client.Time(ctx).Result()
		if err != nil {
			t.Fatal(err)
		}
		k.At = now.Add(-time.Millisecond).UnixMilli()
		value, err := marshalNativeRecord(storagecodec.NativeProcedure, p)
		if err != nil {
			t.Fatal(err)
		}
		prefix := redisApplicationClientPrefix(output.Method)
		member := prefix + strings.TrimPrefix(output.Procedure, "r:"+prefix) + "-K"
		_, err = store.Commit(ctx, rediscore.Publication{Identity: view.Identity, Revision: view.View.Revision, Deadline: deadline(), SpendReservation: true, Changes: []rediscore.Change{{Field: output.Procedure, Value: value}}, Timers: &rediscore.TimerMutation{Expected: view.View.Timer, Target: view.View.Timer, RequireLease: true, Changes: []rediscore.TimerChange{{Member: member, Due: time.UnixMilli(k.At)}}}})
		if err != nil {
			t.Fatal(err)
		}
		view, err = store.ObserveContext(ctx, identity.Context, identity.Incarnation, "r:context")
		if err != nil {
			t.Fatal(err)
		}
		due := &rediscore.DueContext{Identity: view.Identity, Revision: view.View.Revision, Assignment: view.View.Timer, Member: member, DeadlineMS: k.At, CheckedAtMS: now.UnixMilli(), Observed: time.Now()}
		if err = fireRedisCleanupByeTimer(ctx, store, due, deadline()); err != nil {
			t.Fatal("BYE K transition", err)
		}
	}
	physicalDeadline := time.UnixMilli(int64(output.Write.Permit.Authorization.DeadlineMs))
	waitUntil := time.Now().Add(3 * time.Second)
	nextRenewal := time.Now().Add(500 * time.Millisecond)
	var checked time.Time
	var err error
	for {
		if !time.Now().Before(nextRenewal) {
			if _, err = store.RenewWorker(ctx, rediscore.WorkerLeaseRequest{Worker: workerID, Epoch: input.Executor.Epoch, PrimaryRunID: primary}); err != nil {
				t.Fatal("renew live worker while waiting for physical cutoff", err)
			}
			nextRenewal = time.Now().Add(500 * time.Millisecond)
		}
		checked, err = client.Time(ctx).Result()
		if err != nil {
			t.Fatal(err)
		}
		if !checked.Before(physicalDeadline) {
			break
		}
		if !time.Now().Before(waitUntil) {
			t.Fatal("physical output deadline did not mature")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if _, err := store.RenewWorker(ctx, rediscore.WorkerLeaseRequest{Worker: workerID, Epoch: input.Executor.Epoch, PrimaryRunID: primary}); err != nil {
		t.Fatal(err)
	}
	view, err := store.ObserveContext(ctx, identity.Context, identity.Incarnation, "r:context")
	if err != nil {
		t.Fatal(err)
	}
	prefix := redisApplicationClientPrefix(output.Method)
	member := "out-" + prefix + "output-" + strings.TrimPrefix(output.Procedure, "r:"+prefix)
	due := &rediscore.DueContext{Identity: view.Identity, Revision: view.View.Revision, Assignment: view.View.Timer, Member: member, DeadlineMS: int64(output.Write.Permit.Authorization.DeadlineMs), CheckedAtMS: checked.UnixMilli(), Observed: time.Now()}
	if err = dispatchRedisCleanupByeCutoff(ctx, store, due, deadline()); err != nil {
		t.Fatal("BYE exact output retirement", err)
	}
	fields := []string{"r:context", "r:result-2", output.Procedure, result.Output, result.Request}
	if result.Response != "" {
		fields = append(fields, result.Response)
	}
	after, err := store.Read(ctx, identity, fields...)
	if err != nil {
		t.Fatal(err)
	}
	current, err := decodeNativeContext(after.Records["r:context"])
	if err != nil || slices.Contains(current.NoninitialProcedures, output.Procedure) {
		t.Fatal("BYE procedure retained", err)
	}
	retained, err := decodeNativeResult(after.Records["r:result-2"])
	if err != nil || retained.Output != "" || retained.Certainty != pb.OperationResult_COMPLETED || retained.Request != result.Request || retained.Response != result.Response {
		t.Fatal("current BYE receipt not retained", err)
	}
	if len(after.Records[output.Procedure]) != 0 || len(after.Records[result.Output]) != 0 || len(after.Records[result.Request]) == 0 || result.Response != "" && len(after.Records[result.Response]) == 0 {
		t.Fatal("BYE retirement lost retained wire or left native owner")
	}
}
