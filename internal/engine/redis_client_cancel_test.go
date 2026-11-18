package engine

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
	"github.com/redis/go-redis/v9"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/rediscore"
	"sipbridge/internal/selectedmessage"
)

func testRedisInitialClientCancel(t *testing.T, ctx context.Context, store *rediscore.Store, client *redis.Client, root string, input redisUACQualification, parentField string, originData []byte, worker, primary string, proceeding, unknownTerminal bool) {
	t.Helper()
	origin, err := selectedmessage.DecodeOpaque(originData)
	if err != nil {
		t.Fatal(err)
	}
	ringing, err := portable.BuildResponseWithHeaders(origin, 180, "Ringing", nil, "remote-tag", "sip:phone@192.0.2.1:5060;transport=udp", nil)
	if err != nil {
		t.Fatal(err)
	}
	ringing = portable.RestoreMessage(ringing.WireBytes(), "udp", "192.0.2.1:5060", "")
	if proceeding {
		if handled, e := receiveRedisUACProvisional(ctx, store, input.Flow, ringing, input.Key, input.ExecutorTimer, time.Now().Add(time.Second)); e != nil || !handled {
			t.Fatal("ringing", e)
		}
	}
	command := &pb.Command{Context: input.Context, Sequence: 2, Action: &pb.Command_Cancel{Cancel: &pb.Cancel{}}}
	capture, err := captureRedisNotifyWait(command)
	if err != nil {
		t.Fatal(err)
	}
	result, err := publishRedisClientCancel(ctx, store, input.Key, input.Application, input.Lease, command, capture, "cancel", input.ExecutorTimer, time.Now().Add(2*time.Second))
	if err != nil || result == nil || result.Certainty != pb.OperationResult_ACCEPTED {
		t.Fatal("cancel command", err)
	}
	selected, err := store.ObserveContext(ctx, input.Context.Id, input.Context.State.Incarnation, "r:context", parentField)
	if err != nil {
		t.Fatal(err)
	}
	current, err := decodeNativeContext(selected.View.Records["r:context"])
	if err != nil {
		t.Fatal(err)
	}
	parent, err := readPrivateProcedureRecord(selected.View.Records[parentField])
	if err != nil || !parent.Native.Cancelled {
		t.Fatal("cancel flag", err)
	}
	if !proceeding {
		if parent.Native.CancelSent || len(current.NoninitialProcedures) != 0 {
			t.Fatal("early CANCEL created child")
		}
		if handled, e := receiveRedisUACProvisional(ctx, store, input.Flow, ringing, input.Key, input.ExecutorTimer, time.Now().Add(time.Second)); e != nil || !handled {
			t.Fatal("deferred paired CANCEL", e)
		}
		selected, err = store.ObserveContext(ctx, input.Context.Id, input.Context.State.Incarnation, "r:context", parentField)
		if err != nil {
			t.Fatal(err)
		}
		current, err = decodeNativeContext(selected.View.Records["r:context"])
		if err != nil {
			t.Fatal(err)
		}
		parent, err = readPrivateProcedureRecord(selected.View.Records[parentField])
		if err != nil {
			t.Fatal(err)
		}
	}
	if !parent.Native.CancelSent || parent.Native.Timers.Values[string(portable.TimerCancel)] == nil || !parent.Native.Timers.Values[string(portable.TimerCancel)].Active || len(current.NoninitialProcedures) != 1 {
		t.Fatal("paired CANCEL or watchdog missing")
	}
	childField := current.NoninitialProcedures[0]
	childView, err := store.Read(ctx, selected.Identity, childField)
	if err != nil {
		t.Fatal(err)
	}
	child, err := readPrivateProcedureRecord(childView.Records[childField])
	if err != nil || child.Native.Family != string(portable.NonInviteClient) || !child.Cleanup || len(child.Outputs) != 1 || child.CreditContribution != 0 {
		t.Fatal("paired CANCEL owner", err)
	}
	outputField := child.Outputs[0]
	outView, err := store.Read(ctx, selected.Identity, outputField, child.Origin)
	if err != nil {
		t.Fatal(err)
	}
	output, err := decodeNativeOutput(outView.Records[outputField])
	if err != nil || output.Method != "CANCEL" {
		t.Fatal("CANCEL output", err)
	}
	cancel, err := selectedmessage.DecodeOpaque(outView.Records[child.Origin])
	if err != nil {
		t.Fatal(err)
	}
	facts, err := cancel.Facts()
	if err != nil || facts.Method != sip.CANCEL || !portable.MatchRequest(origin, cancel) {
		t.Fatal("CANCEL identity", err)
	}
	if unknownTerminal {
		testClientCancelUnknownTerminal(t, ctx, client, store, root, input, output, outputField, childField, parentField)
		return
	}
	writer := nativeRedisWriter{store: store}
	ref, err := nativeOutputRef(output.Write)
	if err != nil {
		t.Fatal(err)
	}
	proof, err := store.InspectOutput(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	if proceeding {
		runtime := &redisNative{worker: worker, cfg: Config{Process: input.Executor, StateIdentityKey: input.Key}}
		runtime.cfg.WriteCommitted = func(ctx context.Context, write *pb.FlowWrite) *pb.DeliveryResult {
			if _, e := writer.Reserve(ctx, write); e != nil {
				return nil
			}
			return &pb.DeliveryResult{Effect: write.Effect, Outcome: pb.DeliveryResult_WRITTEN}
		}
		ready := rediscore.ReadyOutput{Ref: ref, Metadata: proof.Metadata, MetadataField: outputField, DeadlineMS: proof.DeadlineMS}
		if err = runtime.readyOutput(ctx, store, ready); err != nil {
			t.Fatal("runtime CANCEL output", err)
		}
	} else {
		if _, err = writer.Reserve(ctx, output.Write); err != nil {
			t.Fatal("reserve CANCEL", err)
		}
		if err = writer.Complete(ctx, output.Write, &pb.DeliveryResult{Effect: output.Write.Effect, Outcome: pb.DeliveryResult_WRITTEN}); err != nil {
			t.Fatal("record CANCEL", err)
		}
		proof, err = store.InspectOutput(ctx, ref)
		if err != nil {
			t.Fatal(err)
		}
		physical := &nativeOutputCapture{ref: ref, proof: proof, metadata: output}
		if err = settleRedisClientCancelOutput(ctx, store, output.Write, physical, input.ExecutorTimer, time.Now().Add(time.Second)); err != nil {
			t.Fatal("settle CANCEL", err)
		}
		if err = acknowledgeRedisClientCancelOutput(ctx, store, output.Write, time.Now().Add(time.Second)); err != nil {
			t.Fatal("acknowledge CANCEL", err)
		}
	}
	response, err := portable.ResponseFor(cancel, 200, "OK", nil, "remote-tag")
	if err != nil {
		t.Fatal(err)
	}
	response = portable.RestoreMessage(response.WireBytes(), "udp", "192.0.2.1:5060", "")
	if handled, e := receiveRedisClientCancelResponse(ctx, store, input.Flow, response, input.Key, input.ExecutorTimer, time.Now().Add(time.Second)); e != nil || !handled {
		t.Fatal("CANCEL response", e)
	}
	after, err := store.ObserveContext(ctx, input.Context.Id, input.Context.State.Incarnation, childField)
	if err != nil {
		t.Fatal(err)
	}
	native, err := decodeNativeProcedure(after.View.Records[childField])
	if err != nil || native.Native.Phase != portable.Completed || !native.Native.Timers[portable.TimerK].Active {
		t.Fatal("CANCEL K", err)
	}
	// Advance the existing K deadline in this bounded fixture.
	wireFields := []string{native.Refs.Origin, native.Refs.LastResponse}
	wires, err := store.Read(ctx, after.Identity, wireFields...)
	if err != nil {
		t.Fatal(err)
	}
	memo, err := selectedmessage.NewMemo(2)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := native.attach(&nativeSelectedMessages{records: wires.Records, memo: memo, provenance: "admitted"}, true, true, false)
	if err != nil {
		t.Fatal(err)
	}
	k := tx.Timers[portable.TimerK]
	k.At = time.Now().Add(-time.Second).UnixMilli()
	tx.Timers[portable.TimerK] = k
	data, err := encodeNativeProcedure(tx, native.Refs, native.Metadata)
	if err != nil {
		t.Fatal(err)
	}
	id := strings.TrimPrefix(childField, "r:cancel-client-")
	member := "cancel-client-" + id + "-" + string(portable.TimerK)
	_, err = store.Commit(ctx, rediscore.Publication{Identity: after.Identity, Revision: after.View.Revision, Deadline: time.Now().Add(time.Second), SpendReservation: true, Changes: []rediscore.Change{{Field: childField, Value: data}}, Timers: &rediscore.TimerMutation{Expected: after.View.Timer, Target: after.View.Timer, RequireLease: true, Changes: []rediscore.TimerChange{{Member: member, Due: time.UnixMilli(k.At)}}}})
	if err != nil {
		t.Fatal(err)
	}
	due, err := store.PeekDue(ctx, worker, input.Executor.Epoch)
	if err != nil || due == nil || due.Member != member {
		t.Fatal("CANCEL K due", due, err)
	}
	if err = fireRedisClientCancelTimer(ctx, store, due, input.Key, time.Now().Add(time.Second)); err != nil {
		t.Fatal("CANCEL K firing", err)
	}
	for time.Now().UnixMilli() <= proof.DeadlineMS {
		wait := min(time.Until(time.UnixMilli(proof.DeadlineMS))+time.Millisecond, 500*time.Millisecond)
		if wait > 0 {
			time.Sleep(wait)
		}
		if _, err = store.RenewWorker(ctx, rediscore.WorkerLeaseRequest{Worker: worker, Epoch: input.Executor.Epoch, PrimaryRunID: primary}); err != nil {
			t.Fatal("renew executor", err)
		}
	}
	latest, err := store.Read(ctx, after.Identity, "r:context")
	if err != nil {
		t.Fatal(err)
	}
	outputDue := &rediscore.DueContext{Identity: after.Identity, Revision: latest.Revision, Assignment: latest.Timer, Member: "out-" + outputField[2:], DeadlineMS: proof.DeadlineMS, CheckedAtMS: proof.DeadlineMS}
	if err = retireRedisClientCancelOutput(ctx, store, outputDue, time.Now().Add(time.Second)); err != nil {
		t.Fatal("CANCEL output retirement", err)
	}
	retireMember := "cancel-client-retire-" + id
	score, err := client.ZScore(ctx, root+"core:deadlines:uac", retireMember).Result()
	if err != nil || int64(score) > time.Now().UnixMilli() {
		t.Fatal("CANCEL retirement not indexed", err)
	}
	latest, err = store.Read(ctx, after.Identity, "r:context")
	if err != nil {
		t.Fatal(err)
	}
	due = &rediscore.DueContext{Identity: after.Identity, Revision: latest.Revision, Assignment: latest.Timer, Member: retireMember, DeadlineMS: int64(score), CheckedAtMS: time.Now().UnixMilli()}
	if err = retireRedisClientCancel(ctx, store, due, input.Key, time.Now().Add(time.Second)); err != nil {
		t.Fatal("CANCEL retirement", err)
	}
	closed, err := store.ObserveContext(ctx, input.Context.Id, input.Context.State.Incarnation, "r:context", childField, parentField)
	if err != nil {
		t.Fatal(err)
	}
	current, err = decodeNativeContext(closed.View.Records["r:context"])
	if err != nil || slices.Contains(current.NoninitialProcedures, childField) || closed.View.Records[childField] != nil {
		t.Fatal("CANCEL child retained", err)
	}
	parent, err = readPrivateProcedureRecord(closed.View.Records[parentField])
	if err != nil || !parent.Native.Cancelled {
		t.Fatal("parent cancellation lost", err)
	}
}

func testRedisCompletedInviteCancel(t *testing.T, ctx context.Context, store *rediscore.Store, input redisUACQualification, dialogField, answeredField string, kind int) {
	t.Helper()
	command := &pb.Command{Context: input.Context, Sequence: 2, Action: &pb.Command_Cancel{Cancel: &pb.Cancel{}}}
	capture, err := captureRedisNotifyWait(command)
	if err != nil {
		t.Fatal(err)
	}
	result, err := publishRedisClientCancel(ctx, store, input.Key, input.Application, input.Lease, command, capture, "cancel-after-success", input.ExecutorTimer, time.Now().Add(2*time.Second))
	if err != nil || result == nil || result.Certainty != pb.OperationResult_ACCEPTED {
		t.Fatal("cancel successful INVITE", err)
	}
	selected, err := store.ObserveContext(ctx, input.Context.Id, input.Context.State.Incarnation, "r:context", dialogField)
	if err != nil {
		t.Fatal(err)
	}
	current, err := decodeNativeContext(selected.View.Records["r:context"])
	if err != nil {
		t.Fatal(err)
	}
	dialog, err := readPrivateDialogRecord(selected.View.Records[dialogField])
	if err != nil || dialog.Native.LocalInvite == nil || !dialog.Native.LocalInvite.CancelRequested || current.Ended {
		t.Fatal("native CANCEL transition", err)
	}
	if kind != 50 && (dialog.LocalBye != "" || len(current.NoninitialProcedures) != 0) {
		t.Fatal("cleanup started before ACK W")
	}
	if kind == 52 {
		testRedisApplicationAnswerACK(t, ctx, store, input, dialogField, answeredField, 52, nil, "")
	} else if kind == 51 {
		ackField := ""
		for _, field := range dialog.Outputs {
			if strings.HasPrefix(field, "r:uac-dialog-ack-output-") {
				ackField = field
			}
		}
		if ackField == "" {
			t.Fatal("pending ACK missing")
		}
		view, e := store.Read(ctx, selected.Identity, ackField)
		if e != nil {
			t.Fatal(e)
		}
		output, e := decodeNativeOutput(view.Records[ackField])
		if e != nil {
			t.Fatal(e)
		}
		writer := nativeRedisWriter{store: store}
		if _, e = writer.Reserve(ctx, output.Write); e != nil {
			t.Fatal("reserve ACK", e)
		}
		if e = writer.Complete(ctx, output.Write, &pb.DeliveryResult{Effect: output.Write.Effect, Outcome: pb.DeliveryResult_WRITTEN}); e != nil {
			t.Fatal("record ACK", e)
		}
		ref, e := nativeOutputRef(output.Write)
		if e != nil {
			t.Fatal(e)
		}
		proof, e := store.InspectOutput(ctx, ref)
		if e != nil {
			t.Fatal(e)
		}
		physical := &nativeOutputCapture{ref: ref, proof: proof, metadata: output}
		if e = settleRedisUACSuccessACK(ctx, store, output.Write, physical, input.Key, input.ExecutorTimer, time.Now().Add(time.Second)); e != nil {
			t.Fatal("settle ACK after CANCEL", e)
		}
		proof, e = store.InspectOutput(ctx, ref)
		if e != nil {
			t.Fatal(e)
		}
		physical.proof = proof
		if e = acknowledgeRedisUACSuccessACK(ctx, store, output.Write, physical); e != nil {
			t.Fatal("consume ACK after CANCEL", e)
		}
	}
	after, err := store.ObserveContext(ctx, input.Context.Id, input.Context.State.Incarnation, "r:context", dialogField)
	if err != nil {
		t.Fatal(err)
	}
	current, err = decodeNativeContext(after.View.Records["r:context"])
	if err != nil {
		t.Fatal(err)
	}
	dialog, err = readPrivateDialogRecord(after.View.Records[dialogField])
	if err != nil {
		t.Fatal(err)
	}
	if !dialog.AckWritten || dialog.LocalBye == "" || len(current.NoninitialProcedures) != 1 || current.Ended {
		t.Fatal("CANCEL did not start cleanup after ACK W")
	}
	child, err := store.Read(ctx, after.Identity, current.NoninitialProcedures[0])
	if err != nil {
		t.Fatal(err)
	}
	procedure, err := readPrivateProcedureRecord(child.Records[current.NoninitialProcedures[0]])
	if err != nil || !procedure.Cleanup || procedure.Native.Family != string(portable.NonInviteClient) || procedure.Origin != dialog.LocalBye {
		t.Fatal("missing cleanup BYE owner", err)
	}
}
