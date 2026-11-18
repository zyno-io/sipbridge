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

func testWinningAckUnknownTerminal(t *testing.T, ctx context.Context, client *redis.Client, store *rediscore.Store, root string, input redisUACQualification, workerID string, origin, final portable.Message, output *nativepb.Output, outputField string, application ...bool) {
	t.Helper()
	appClient := len(application) == 1 && application[0]
	deadline := func() time.Time { return time.Now().Add(time.Second) }
	now, err := client.Time(ctx).Result()
	if err != nil {
		t.Fatal(err)
	}
	dialog, err := portable.NewDialog(portable.UAC, origin, final, portable.DefaultTimers())
	if err != nil {
		t.Fatal(err)
	}
	dialog, _, err = portable.StepDialog(dialog, portable.DialogInput{Kind: portable.DialogLocalACK, Now: now.UnixMilli(), Branch: "z9hG4bK-test-peer-ack"})
	if err != nil {
		t.Fatal(err)
	}
	bye := nativePeerBye(t, dialog, 12)
	incoming := portable.RestoreMessage(bye.WireBytes(), "tls", "192.0.2.1:5060", "")
	selected, err := selectRedisCallDialog(ctx, store, input.Executor, input.Flow, incoming, deadline())
	if err != nil {
		t.Fatal("select actual BYE", err)
	}
	if err = publishRedisCallRemoteBye(ctx, store, selected, input.Flow, input.Key, input.ExecutorTimer, deadline()); err != nil {
		t.Fatal("publish actual BYE", err)
	}
	writes, err := nativeReadyWrites(ctx, store, workerID, input.Executor.Epoch, 8)
	if err != nil {
		t.Fatal(err)
	}
	var byeWrite *pb.FlowWrite
	for _, w := range writes {
		ref, e := nativeOutputRef(w)
		if e != nil {
			t.Fatal(e)
		}
		proof, e := store.InspectOutput(ctx, ref)
		if e != nil {
			t.Fatal(e)
		}
		metadata, e := decodeNativeOutput(proof.Metadata)
		if e != nil {
			t.Fatal(e)
		}
		if metadata.Method == "BYE" && metadata.Status == 200 {
			byeWrite = w
		}
	}
	if byeWrite == nil {
		t.Fatal("actual BYE response missing")
	}
	writer := nativeRedisWriter{store: store}
	if _, err = writer.Reserve(ctx, byeWrite); err != nil {
		t.Fatal(err)
	}
	byeRef, err := nativeOutputRef(byeWrite)
	if err != nil {
		t.Fatal(err)
	}
	byeProof, err := store.InspectOutput(ctx, byeRef)
	if err != nil {
		t.Fatal(err)
	}
	if err = completeRedisCallByeOutput(ctx, store, byeWrite, &pb.DeliveryResult{Effect: byeWrite.Effect, Outcome: pb.DeliveryResult_WRITTEN}); err != nil {
		t.Fatal(err)
	}
	if err = acknowledgeRedisCallByeOutput(ctx, store, byeWrite, deadline()); err != nil {
		t.Fatal(err)
	}
	// The response was consumed and acknowledged; advance its indexed retention only.
	v, err := store.ObserveContext(ctx, "uac", "incarnation", "r:context")
	if err != nil {
		t.Fatal(err)
	}
	now, err = client.Time(ctx).Result()
	if err != nil {
		t.Fatal(err)
	}
	dueAt := now.Add(-time.Millisecond).UnixMilli()
	byeMember := "out-" + strings.TrimPrefix(byeProof.MetadataField, "r:")
	_, err = store.Commit(ctx, rediscore.Publication{Identity: v.Identity, Revision: v.View.Revision, Deadline: deadline(), Timers: &rediscore.TimerMutation{Expected: v.View.Timer, Target: v.View.Timer, RequireLease: true, Changes: []rediscore.TimerChange{{Member: byeMember, Due: time.UnixMilli(dueAt)}}}})
	if err != nil {
		t.Fatal(err)
	}
	v, err = store.ObserveContext(ctx, "uac", "incarnation", "r:context")
	if err != nil {
		t.Fatal(err)
	}
	due := &rediscore.DueContext{Identity: v.Identity, Revision: v.View.Revision, Assignment: v.View.Timer, Member: byeMember, DeadlineMS: dueAt, CheckedAtMS: now.UnixMilli(), Observed: time.Now()}
	if err = retireRedisCallByeKnown(ctx, store, due, input.ExecutorTimer, deadline()); err != nil {
		t.Fatal("retire actual BYE", err)
	}
	// Complete the actual ANSWERED and ENDED lifecycle calls independently.
	members, err := client.ZRange(ctx, root+"core:deadlines:uac", 0, -1).Result()
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, member := range members {
		if !strings.HasPrefix(member, "event-") {
			continue
		}
		v, e := store.ObserveContext(ctx, "uac", "incarnation", "r:"+member)
		if e != nil {
			t.Fatal(e)
		}
		event, e := decodeNativeLifecycle(v.View.Records["r:"+member])
		if e != nil {
			t.Fatal(e)
		}
		checked := waitRedisFixtureTime(t, ctx, client, time.UnixMilli(event.NextMs))
		d := &rediscore.DueContext{Identity: v.Identity, Revision: v.View.Revision, Assignment: v.View.Timer, Member: member, DeadlineMS: event.NextMs, CheckedAtMS: checked.UnixMilli(), Observed: time.Now()}
		capture, public, e := selectRedisNativeLifecycle(ctx, store, d)
		if e != nil || public == nil {
			t.Fatal("select lifecycle", e)
		}
		if e = completeRedisNativeLifecycle(ctx, store, capture, true, deadline()); e != nil {
			t.Fatal("complete lifecycle", e)
		}
		count++
	}
	if count != 2 {
		t.Fatalf("expected ANSWERED and ENDED, got %d", count)
	}
	// Advance only the real Timer M deadline and its matching Redis projection.
	anchorField := output.Procedure
	if appClient {
		selected, e := store.ObserveContext(ctx, "uac", "incarnation", "r:context")
		if e != nil {
			t.Fatal(e)
		}
		context, e := decodeNativeContext(selected.View.Records["r:context"])
		if e != nil || len(context.InitialProcedures) != 1 {
			t.Fatal("initial anchor missing", e)
		}
		anchorField = context.InitialProcedures[0]
	}
	v, err = store.ObserveContext(ctx, "uac", "incarnation", "r:context", anchorField)
	if err != nil {
		t.Fatal(err)
	}
	p, err := readPrivateProcedureRecord(v.View.Records[anchorField])
	if err != nil {
		t.Fatal(err)
	}
	timer := p.Native.Timers.Values[string(portable.TimerM)]
	if timer == nil || !timer.Active {
		t.Fatal("Timer M missing")
	}
	now, err = client.Time(ctx).Result()
	if err != nil {
		t.Fatal(err)
	}
	timer.At = now.Add(-time.Millisecond).UnixMilli()
	pdata, err := marshalNativeRecord(storagecodec.NativeProcedure, p)
	if err != nil {
		t.Fatal(err)
	}
	member := "call-tx-" + strings.TrimPrefix(anchorField, "r:uac-procedure-") + "-M"
	_, err = store.Commit(ctx, rediscore.Publication{Identity: v.Identity, Revision: v.View.Revision, Deadline: deadline(), SpendReservation: true, Changes: []rediscore.Change{{Field: anchorField, Value: pdata}}, Timers: &rediscore.TimerMutation{Expected: v.View.Timer, Target: v.View.Timer, RequireLease: true, Changes: []rediscore.TimerChange{{Member: member, Due: time.UnixMilli(timer.At)}}}})
	if err != nil {
		t.Fatal(err)
	}
	v, err = store.ObserveContext(ctx, "uac", "incarnation", "r:context")
	if err != nil {
		t.Fatal(err)
	}
	due = &rediscore.DueContext{Identity: v.Identity, Revision: v.View.Revision, Assignment: v.View.Timer, Member: member, DeadlineMS: timer.At, CheckedAtMS: now.UnixMilli(), Observed: time.Now()}
	if err = fireRedisUACDeadline(ctx, store, due, deadline()); err != nil {
		t.Fatal("real M transition", err)
	}
	// Mature only the immutable terminal and physical horizons; retain U.
	resultField := "r:result-1"
	fields := []string{"r:context", resultField, outputField, output.Dialog}
	if appClient {
		resultField = "r:result-2"
		fields = append(fields, resultField)
	}
	v, err = store.ObserveContext(ctx, "uac", "incarnation", fields...)
	if err != nil {
		t.Fatal(err)
	}
	current, err := decodeNativeContext(v.View.Records["r:context"])
	if err != nil || !current.Ended {
		t.Fatal("actual BYE did not end call", err)
	}
	d, err := readPrivateDialogRecord(v.View.Records[output.Dialog])
	if err != nil || d.AckWritten != appClient {
		t.Fatal("ACK evidence changed", err)
	}
	result, err := decodeNativeResult(v.View.Records[resultField])
	if err != nil {
		t.Fatal(err)
	}
	output, err = decodeNativeOutput(v.View.Records[outputField])
	if err != nil {
		t.Fatal(err)
	}
	now, err = client.Time(ctx).Result()
	if err != nil {
		t.Fatal(err)
	}
	old := now.Add(-rediscore.OutputObsoleteGrace - time.Second).UnixMilli()
	current.CallRetireAtMs = now.Add(-time.Second).UnixMilli()
	result.CutoffMs = current.CallRetireAtMs
	output.Write.Permit.Authorization.DeadlineMs = uint64(old)
	cdata, err := encodeNativeContext(current)
	if err != nil {
		t.Fatal(err)
	}
	rdata, err := encodeNativeResult(result)
	if err != nil {
		t.Fatal(err)
	}
	odata, err := encodeNativeOutput(output)
	if err != nil {
		t.Fatal(err)
	}
	hash, err := nativeFlowWriteHash(output.Write)
	if err != nil {
		t.Fatal(err)
	}
	prefix := "o:" + output.Write.Effect.Id + ":"
	if err = client.HSet(ctx, root+"core:context:uac", outputField, odata, prefix+"p", hash[:], prefix+"d", old).Err(); err != nil {
		t.Fatal(err)
	}
	cutoff := max(current.CallRetireAtMs, old+rediscore.OutputObsoleteGrace.Milliseconds())
	changes := []rediscore.Change{{Field: "r:context", Value: cdata}, {Field: resultField, Value: rdata}}
	timers := []rediscore.TimerChange{{Member: "call-retire", Due: time.UnixMilli(cutoff)}}
	if appClient {
		initial, e := decodeNativeResult(v.View.Records["r:result-1"])
		if e != nil {
			t.Fatal(e)
		}
		initial.CutoffMs = current.CallRetireAtMs
		value, e := encodeNativeResult(initial)
		if e != nil {
			t.Fatal(e)
		}
		changes = append(changes, rediscore.Change{Field: "r:result-1", Value: value})
		resultMember, e := uacInitialResultTimer(anchorField)
		if e != nil {
			t.Fatal(e)
		}
		timers = append(timers, rediscore.TimerChange{Member: resultMember, Due: time.UnixMilli(initial.CutoffMs)}, rediscore.TimerChange{Member: "out-" + strings.TrimPrefix(outputField, "r:"), Due: time.UnixMilli(old)})
	}
	_, err = store.Commit(ctx, rediscore.Publication{Identity: v.Identity, Revision: v.View.Revision, Deadline: deadline(), SpendReservation: true, Changes: changes, Timers: &rediscore.TimerMutation{Expected: v.View.Timer, Target: v.View.Timer, RequireLease: true, Changes: timers}})
	if err != nil {
		t.Fatal(err)
	}
	ref, err := nativeOutputRef(output.Write)
	if err != nil {
		t.Fatal(err)
	}
	proof, err := store.InspectOutput(ctx, ref)
	if err != nil || proof.State != rediscore.OutputUnknown {
		t.Fatal("lost U proof", err)
	}
	forgot := false
	for i := 0; i < 8; i++ {
		due, e := store.PeekDue(ctx, workerID, input.Executor.Epoch)
		if e != nil || due == nil {
			t.Fatal("terminal duty", e)
		}
		if due.Member == "call-retire" {
			_, e = dispatchRedisCallRetirement(ctx, store, due, deadline())
		} else if appClient && strings.HasPrefix(due.Member, "uac-result-") {
			e = expireRedisUACInitialResult(ctx, store, due, input.Key, deadline())
		} else if appClient && due.Member == "out-"+strings.TrimPrefix(outputField, "r:") {
			e = dispatchRedisCleanupByeCutoff(ctx, store, due, deadline())
		} else if due.Member == "out-"+strings.TrimPrefix(outputField, "r:") {
			e = retireRedisUACSuccessACK(ctx, store, due, input.Key, deadline())
		} else {
			t.Fatalf("unexpected duty %s", due.Member)
		}
		if e != nil {
			t.Fatal("terminal ACK cleanup", e)
		}
		after, e := store.ObserveContext(ctx, "uac", "incarnation", outputField, output.Dialog, output.Procedure)
		if e != nil {
			t.Fatal(e)
		}
		if len(after.View.Records[outputField]) != 0 {
			if _, e = store.InspectOutput(ctx, ref); errors.Is(e, rediscore.ErrAbsent) {
				owner, ownerErr := readPrivateProcedureRecord(after.View.Records[output.Procedure])
				dialog, dialogErr := readPrivateDialogRecord(after.View.Records[output.Dialog])
				if ownerErr != nil || dialogErr != nil || !slices.Contains(owner.Outputs, outputField) || !slices.Contains(dialog.Outputs, outputField) || dialog.AckWritten != appClient {
					t.Fatal("forget lost retained owners or invented ACK write", ownerErr, dialogErr)
				}
				forgot = true
			} else if e != nil {
				t.Fatal("inspect retained ACK proof", e)
			}
		}
		if len(after.View.Records[outputField]) == 0 {
			if !forgot {
				t.Fatal("metadata removed before proof-only forget was observed")
			}
			if _, e = store.InspectOutput(ctx, ref); !errors.Is(e, rediscore.ErrAbsent) {
				t.Fatal("U proof retained", e)
			}
			d, e = readPrivateDialogRecord(after.View.Records[output.Dialog])
			if e != nil || d.AckWritten != appClient {
				t.Fatal("terminal cleanup invented ACK write", e)
			}
			return
		}
	}
	t.Fatal("terminal ACK did not reconcile")
}
