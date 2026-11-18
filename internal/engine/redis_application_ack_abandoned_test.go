package engine

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/emiago/sipgo/sip/portable"
	"github.com/redis/go-redis/v9"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/nativepb"
	"sipbridge/internal/rediscore"
	"sipbridge/internal/storagecodec"
)

func testAbandonedApplicationACK(t *testing.T, ctx context.Context, client *redis.Client, store *rediscore.Store, root string, input redisUACQualification, output *nativepb.Output, field, eventField string, lateWrite bool) {
	t.Helper()
	deadline := func() time.Time { return time.Now().Add(2 * time.Second) }
	initial, err := store.ObserveContext(ctx, "uac", "incarnation", "r:result-1")
	if err != nil {
		t.Fatal(err)
	}
	initialResult, err := decodeNativeResult(initial.View.Records["r:result-1"])
	if err != nil {
		t.Fatal(err)
	}
	initialView, err := store.Read(ctx, initial.Identity, initialResult.Output)
	if err != nil {
		t.Fatal(err)
	}
	initialOutput, err := decodeNativeOutput(initialView.Records[initialResult.Output])
	if err != nil {
		t.Fatal(err)
	}
	writer := nativeRedisWriter{store: store}
	if _, err = writer.Reserve(ctx, initialOutput.Write); err != nil {
		t.Fatal("reserve initial INVITE", err)
	}
	if err = writer.Complete(ctx, initialOutput.Write, &pb.DeliveryResult{Effect: initialOutput.Write.Effect, Outcome: pb.DeliveryResult_WRITTEN}); err != nil {
		t.Fatal("complete initial INVITE", err)
	}
	initialRef, err := nativeOutputRef(initialOutput.Write)
	if err != nil {
		t.Fatal(err)
	}
	initialProof, err := store.InspectOutput(ctx, initialRef)
	if err != nil {
		t.Fatal(err)
	}
	initialCapture := &nativeOutputCapture{ref: initialRef, proof: initialProof, metadata: initialOutput}
	if err = settleRedisUACInitialOutput(ctx, store, initialOutput.Write, initialCapture, input.ExecutorTimer, deadline()); err != nil {
		t.Fatal("settle initial INVITE", err)
	}
	initialCapture.proof, err = store.InspectOutput(ctx, initialRef)
	if err != nil {
		t.Fatal(err)
	}
	if err = acknowledgeRedisUACInitialOutput(ctx, store, initialCapture, deadline()); err != nil {
		t.Fatal("ack initial INVITE", err)
	}
	initial, err = store.ObserveContext(ctx, "uac", "incarnation", "r:context")
	if err != nil {
		t.Fatal(err)
	}
	initialDue := &rediscore.DueContext{Identity: initial.Identity, Revision: initial.View.Revision, Assignment: initial.View.Timer, Member: "out-" + strings.TrimPrefix(initialResult.Output, "r:"), DeadlineMS: initialProof.DeadlineMS, CheckedAtMS: initialProof.DeadlineMS}
	if err = retireRedisUACInitialOutput(ctx, store, initialDue, deadline()); err != nil {
		t.Fatal("retire initial INVITE", err)
	}
	if err := store.ExpireApplication(ctx, input.Lease); err != nil {
		t.Fatal(err)
	}
	selected, err := store.ObserveContext(ctx, "uac", "incarnation", "r:context")
	if err != nil {
		t.Fatal(err)
	}
	current, err := decodeNativeContext(selected.View.Records["r:context"])
	if err != nil {
		t.Fatal(err)
	}
	item := rediscore.ExpiredApplicationContext{Identity: selected.Identity, Revision: selected.View.Revision}
	capture := &redisExpiredOwnerSelection{identity: selected.Identity, view: selected.View, context: current}
	if err = releaseExpiredRedisCall(ctx, store, item, input.Key, input.ExecutorTimer, deadline(), capture); err != nil {
		t.Fatal("expire owner", err)
	}
	selected, err = store.ObserveContext(ctx, "uac", "incarnation", eventField)
	if err != nil {
		t.Fatal(err)
	}
	event, err := decodeNativeLifecycle(selected.View.Records[eventField])
	if err != nil {
		t.Fatal(err)
	}
	checked := waitRedisFixtureTime(t, ctx, client, time.UnixMilli(event.NextMs))
	eventDue := &rediscore.DueContext{Identity: selected.Identity, Revision: selected.View.Revision, Assignment: selected.View.Timer, Member: strings.TrimPrefix(eventField, "r:"), DeadlineMS: event.NextMs, CheckedAtMS: checked.UnixMilli(), Observed: time.Now()}
	eventCapture, _, err := selectRedisNativeLifecycle(ctx, store, eventDue)
	if err != nil {
		t.Fatal("select answered", err)
	}
	if err = completeRedisNativeLifecycle(ctx, store, eventCapture, true, deadline()); err != nil {
		t.Fatal("complete answered", err)
	}
	selected, err = store.ObserveContext(ctx, "uac", "incarnation", "r:context", output.Procedure, output.Result, "r:result-1", field)
	if err != nil {
		t.Fatal(err)
	}
	current, err = decodeNativeContext(selected.View.Records["r:context"])
	if err != nil || !current.Ended || selected.Identity.Owner != "" {
		t.Fatal("owner remained", err)
	}
	proc, err := readPrivateProcedureRecord(selected.View.Records[output.Procedure])
	if err != nil {
		t.Fatal(err)
	}
	now, err := client.Time(ctx).Result()
	if err != nil {
		t.Fatal(err)
	}
	m := proc.Native.Timers.Values[string(portable.TimerM)]
	if m == nil || !m.Active {
		t.Fatal("M missing")
	}
	m.At = now.Add(-time.Second).UnixMilli()
	data, err := marshalNativeRecord(storagecodec.NativeProcedure, proc)
	if err != nil {
		t.Fatal(err)
	}
	member := "call-tx-" + strings.TrimPrefix(output.Procedure, "r:uac-procedure-") + "-M"
	_, err = store.Commit(ctx, rediscore.Publication{Identity: selected.Identity, Revision: selected.View.Revision, Deadline: deadline(), SpendReservation: true, Changes: []rediscore.Change{{Field: output.Procedure, Value: data}}, Timers: &rediscore.TimerMutation{Expected: selected.View.Timer, Target: selected.View.Timer, RequireLease: true, Changes: []rediscore.TimerChange{{Member: member, Due: time.UnixMilli(m.At)}}}})
	if err != nil {
		t.Fatal(err)
	}
	selected, err = store.ObserveContext(ctx, "uac", "incarnation", "r:context")
	if err != nil {
		t.Fatal(err)
	}
	duty := &rediscore.DueContext{Identity: selected.Identity, Revision: selected.View.Revision, Assignment: selected.View.Timer, Member: member, DeadlineMS: m.At, CheckedAtMS: now.UnixMilli(), Observed: time.Now()}
	if err = fireRedisUACDeadline(ctx, store, duty, deadline()); err != nil {
		t.Fatal("fire M", err)
	}
	selected, err = store.ObserveContext(ctx, "uac", "incarnation", "r:context", output.Result, "r:result-1", field)
	if err != nil {
		t.Fatal(err)
	}
	current, err = decodeNativeContext(selected.View.Records["r:context"])
	if err != nil {
		t.Fatal(err)
	}
	now, err = client.Time(ctx).Result()
	if err != nil {
		t.Fatal(err)
	}
	old := now.Add(-rediscore.OutputObsoleteGrace - time.Second).UnixMilli()
	current.CallRetireAtMs = now.Add(-time.Second).UnixMilli()
	contextData, err := encodeNativeContext(current)
	if err != nil {
		t.Fatal(err)
	}
	changes := []rediscore.Change{{Field: "r:context", Value: contextData}}
	for _, f := range []string{"r:result-1", output.Result} {
		result, e := decodeNativeResult(selected.View.Records[f])
		if e != nil {
			t.Fatal(e)
		}
		result.CutoffMs = current.CallRetireAtMs
		data, e := encodeNativeResult(result)
		if e != nil {
			t.Fatal(e)
		}
		changes = append(changes, rediscore.Change{Field: f, Value: data})
	}
	output, err = decodeNativeOutput(selected.View.Records[field])
	if err != nil {
		t.Fatal(err)
	}
	output.Write.Permit.Authorization.DeadlineMs = uint64(old)
	encoded, err := encodeNativeOutput(output)
	if err != nil {
		t.Fatal(err)
	}
	hash, err := nativeFlowWriteHash(output.Write)
	if err != nil {
		t.Fatal(err)
	}
	prefix := "o:" + output.Write.Effect.Id + ":"
	if err = client.HSet(ctx, root+"core:context:uac", field, encoded, prefix+"p", hash[:], prefix+"d", old).Err(); err != nil {
		t.Fatal(err)
	}
	cutoff := max(current.CallRetireAtMs, old+rediscore.OutputObsoleteGrace.Milliseconds())
	_, err = store.Commit(ctx, rediscore.Publication{Identity: selected.Identity, Revision: selected.View.Revision, Deadline: deadline(), SpendReservation: true, Changes: changes, Timers: &rediscore.TimerMutation{Expected: selected.View.Timer, Target: selected.View.Timer, RequireLease: true, Changes: []rediscore.TimerChange{{Member: "call-retire", Due: time.UnixMilli(cutoff)}}}})
	if err != nil {
		t.Fatal(err)
	}
	ref, err := nativeOutputRef(output.Write)
	if err != nil {
		t.Fatal(err)
	}
	if lateWrite {
		writer := nativeRedisWriter{store: store}
		if err = writer.Complete(ctx, output.Write, &pb.DeliveryResult{Effect: output.Write.Effect, Outcome: pb.DeliveryResult_WRITTEN}); err != nil {
			t.Fatal("late W", err)
		}
	}
	selected, err = store.ObserveContext(ctx, "uac", "incarnation", "r:context")
	if err != nil {
		t.Fatal(err)
	}
	duty = &rediscore.DueContext{Identity: selected.Identity, Revision: selected.View.Revision, Assignment: selected.View.Timer, Member: "call-retire", DeadlineMS: cutoff, CheckedAtMS: now.UnixMilli(), Observed: time.Now()}
	current, err = decodeNativeContext(selected.View.Records["r:context"])
	if err != nil {
		t.Fatal(err)
	}
	owners, err := selectRedisTerminalOwners(ctx, store, duty, current, deadline())
	if err != nil || owners == nil {
		t.Fatal("terminal owners", err)
	}
	_, err = reconcileRedisTerminalOutput(ctx, store, duty, owners, deadline())
	if err != nil {
		t.Fatal("reconcile proof", err)
	}
	proof, err := store.InspectOutput(ctx, ref)
	if lateWrite {
		if err != nil || proof.State != rediscore.OutputWritten {
			t.Fatal("late W was forgotten", err)
		}
		capture := &nativeOutputCapture{ref: ref, proof: proof, metadata: output}
		if err = settleRedisApplicationACK(ctx, store, output.Write, capture, input.Key, input.ExecutorTimer, deadline()); err != nil {
			t.Fatal("settle competing W", err)
		}
		selected, err = store.ObserveContext(ctx, "uac", "incarnation", "r:context", output.Dialog)
		if err != nil {
			t.Fatal(err)
		}
		dialog, e := readPrivateDialogRecord(selected.View.Records[output.Dialog])
		if e != nil || !dialog.AckWritten || dialog.LocalBye == "" {
			t.Fatal("late W did not authorize its cleanup BYE", e)
		}
		if err = acknowledgeRedisApplicationACK(ctx, store, output.Write, deadline()); err != nil {
			t.Fatal("acknowledge competing W", err)
		}
		if _, err = store.InspectOutput(ctx, ref); !errors.Is(err, rediscore.ErrAbsent) {
			t.Fatal("competing W was not consumed", err)
		}
		return
	}
	if !errors.Is(err, rediscore.ErrAbsent) {
		t.Fatal("U not forgotten", err)
	}
	selected, err = store.ObserveContext(ctx, "uac", "incarnation", "r:context")
	if err != nil {
		t.Fatal(err)
	}
	duty.Identity, duty.Revision, duty.Assignment = selected.Identity, selected.View.Revision, selected.View.Timer
	current, err = decodeNativeContext(selected.View.Records["r:context"])
	if err != nil {
		t.Fatal(err)
	}
	owners, err = selectRedisTerminalOwners(ctx, store, duty, current, deadline())
	if err != nil || owners == nil {
		t.Fatal("owners after forget", err)
	}
	_, err = reconcileRedisTerminalOutput(ctx, store, duty, owners, deadline())
	if err != nil {
		t.Fatal("terminate after forget", err)
	}
	selected, err = store.ObserveContext(ctx, "uac", "incarnation", "r:context", output.Dialog, output.Procedure, output.Result, field)
	if err != nil {
		t.Fatal(err)
	}
	dialog, e := readPrivateDialogRecord(selected.View.Records[output.Dialog])
	if e != nil {
		t.Fatal(e)
	}
	proc, e = readPrivateProcedureRecord(selected.View.Records[output.Procedure])
	if e != nil {
		t.Fatal(e)
	}
	result, e := decodeNativeResult(selected.View.Records[output.Result])
	if e != nil {
		t.Fatal(e)
	}
	if dialog.Native.Phase != string(portable.DialogEnded) || dialog.AckWritten || dialog.LocalBye != "" || slices.Contains(dialog.Outputs, field) || slices.Contains(proc.Outputs, field) || result.Output != "" || result.Certainty != pb.OperationResult_UNKNOWN || len(selected.View.Records[field]) != 0 {
		t.Fatal("invalid abandoned ACK termination")
	}
}
