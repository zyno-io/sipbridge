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

func testClientCancelUnknownTerminal(t *testing.T, ctx context.Context, client *redis.Client, store *rediscore.Store, root string, input redisUACQualification, output *nativepb.Output, field, childField, parentField string) {
	t.Helper()
	deadline := func() time.Time { return time.Now().Add(2 * time.Second) }
	writer := nativeRedisWriter{store: store}
	if _, err := writer.Reserve(ctx, output.Write); err != nil {
		t.Fatal(err)
	}
	if err := writer.Complete(ctx, output.Write, &pb.DeliveryResult{Effect: output.Write.Effect, Outcome: pb.DeliveryResult_UNKNOWN}); err != nil {
		t.Fatal(err)
	}
	ref, err := nativeOutputRef(output.Write)
	if err != nil {
		t.Fatal(err)
	}
	proof, err := store.InspectOutput(ctx, ref)
	if err != nil || proof.State != rediscore.OutputUnknown {
		t.Fatal("U missing", err)
	}
	selected, err := store.ObserveContext(ctx, "uac", "incarnation", "r:context", "r:result-1", parentField, childField)
	if err != nil {
		t.Fatal(err)
	}
	initialResult, err := decodeNativeResult(selected.View.Records["r:result-1"])
	if err != nil {
		t.Fatal(err)
	}
	initialView, err := store.Read(ctx, selected.Identity, initialResult.Output)
	if err != nil {
		t.Fatal(err)
	}
	initialOutput, err := decodeNativeOutput(initialView.Records[initialResult.Output])
	if err != nil {
		t.Fatal(err)
	}
	if _, err = writer.Reserve(ctx, initialOutput.Write); err != nil {
		t.Fatal(err)
	}
	if err = writer.Complete(ctx, initialOutput.Write, &pb.DeliveryResult{Effect: initialOutput.Write.Effect, Outcome: pb.DeliveryResult_WRITTEN}); err != nil {
		t.Fatal(err)
	}
	initialRef, err := nativeOutputRef(initialOutput.Write)
	if err != nil {
		t.Fatal(err)
	}
	initialProof, err := store.InspectOutput(ctx, initialRef)
	if err != nil {
		t.Fatal(err)
	}
	cap := &nativeOutputCapture{ref: initialRef, proof: initialProof, metadata: initialOutput}
	if err = settleRedisUACInitialOutput(ctx, store, initialOutput.Write, cap, input.ExecutorTimer, deadline()); err != nil {
		t.Fatal(err)
	}
	cap.proof, err = store.InspectOutput(ctx, initialRef)
	if err != nil {
		t.Fatal(err)
	}
	if err = acknowledgeRedisUACInitialOutput(ctx, store, cap, deadline()); err != nil {
		t.Fatal(err)
	}
	selected, err = store.ObserveContext(ctx, "uac", "incarnation", "r:context")
	if err != nil {
		t.Fatal(err)
	}
	due := &rediscore.DueContext{Identity: selected.Identity, Revision: selected.View.Revision, Assignment: selected.View.Timer, Member: "out-" + strings.TrimPrefix(initialResult.Output, "r:"), DeadlineMS: initialProof.DeadlineMS, CheckedAtMS: initialProof.DeadlineMS}
	if err = retireRedisUACInitialOutput(ctx, store, due, deadline()); err != nil {
		t.Fatal(err)
	}
	if err = store.ExpireApplication(ctx, input.Lease); err != nil {
		t.Fatal(err)
	}
	selected, err = store.ObserveContext(ctx, "uac", "incarnation", "r:context", parentField, childField)
	if err != nil {
		t.Fatal(err)
	}
	current, err := decodeNativeContext(selected.View.Records["r:context"])
	if err != nil {
		t.Fatal(err)
	}
	capOwner := &redisExpiredOwnerSelection{identity: selected.Identity, view: selected.View, context: current}
	if err = releaseExpiredRedisCall(ctx, store, rediscore.ExpiredApplicationContext{Identity: selected.Identity, Revision: selected.View.Revision}, input.Key, input.ExecutorTimer, deadline(), capOwner); err != nil {
		t.Fatal(err)
	}
	// Fire the actual child F and original INVITE Cancel watchdog at their fixture-aged deadlines.
	for _, spec := range []struct {
		field, member string
		timer         portable.Timer
		child         bool
	}{{childField, "cancel-client-" + strings.TrimPrefix(childField, "r:cancel-client-") + "-F", portable.TimerF, true}, {parentField, "call-tx-" + strings.TrimPrefix(parentField, "r:uac-procedure-") + "-" + string(portable.TimerCancel), portable.TimerCancel, false}} {
		selected, err = store.ObserveContext(ctx, "uac", "incarnation", spec.field)
		if err != nil {
			t.Fatal(err)
		}
		p, e := readPrivateProcedureRecord(selected.View.Records[spec.field])
		if e != nil {
			t.Fatal(e)
		}
		timer := p.Native.Timers.Values[string(spec.timer)]
		if timer == nil || !timer.Active {
			t.Fatalf("%s missing", spec.timer)
		}
		timer.At = time.Now().Add(-time.Second).UnixMilli()
		data, e := marshalNativeRecord(storagecodec.NativeProcedure, p)
		if e != nil {
			t.Fatal(e)
		}
		_, e = store.Commit(ctx, rediscore.Publication{Identity: selected.Identity, Revision: selected.View.Revision, Deadline: deadline(), SpendReservation: true, Changes: []rediscore.Change{{Field: spec.field, Value: data}}, Timers: &rediscore.TimerMutation{Expected: selected.View.Timer, Target: selected.View.Timer, RequireLease: true, Changes: []rediscore.TimerChange{{Member: spec.member, Due: time.UnixMilli(timer.At)}}}})
		if e != nil {
			t.Fatal(e)
		}
		selected, e = store.ObserveContext(ctx, "uac", "incarnation", "r:context")
		if e != nil {
			t.Fatal(e)
		}
		due = &rediscore.DueContext{Identity: selected.Identity, Revision: selected.View.Revision, Assignment: selected.View.Timer, Member: spec.member, DeadlineMS: timer.At, CheckedAtMS: time.Now().UnixMilli()}
		if spec.child {
			e = fireRedisClientCancelTimer(ctx, store, due, input.Key, deadline())
		} else {
			e = fireRedisUACDeadline(ctx, store, due, deadline())
		}
		if e != nil {
			t.Fatalf("fire %s: %v", spec.timer, e)
		}
	}
	// Remove the frozen provisional callback through its real lifecycle consumer.
	members, err := client.ZRange(ctx, root+"core:deadlines:uac", 0, -1).Result()
	if err != nil {
		t.Fatal(err)
	}
	for _, member := range members {
		if !strings.HasPrefix(member, "event-") {
			continue
		}
		selected, err = store.ObserveContext(ctx, "uac", "incarnation", "r:"+member)
		if err != nil {
			t.Fatal(err)
		}
		event, e := decodeNativeLifecycle(selected.View.Records["r:"+member])
		if e != nil {
			t.Fatal(e)
		}
		checked := waitRedisFixtureTime(t, ctx, client, time.UnixMilli(event.NextMs))
		d := &rediscore.DueContext{Identity: selected.Identity, Revision: selected.View.Revision, Assignment: selected.View.Timer, Member: member, DeadlineMS: event.NextMs, CheckedAtMS: checked.UnixMilli(), Observed: time.Now()}
		c, _, e := selectRedisNativeLifecycle(ctx, store, d)
		if e != nil {
			t.Fatal(e)
		}
		if e = completeRedisNativeLifecycle(ctx, store, c, true, deadline()); e != nil {
			t.Fatal(e)
		}
	}
	selected, err = store.ObserveContext(ctx, "uac", "incarnation", "r:context", "r:result-1", "r:result-2", field)
	if err != nil {
		t.Fatal(err)
	}
	current, err = decodeNativeContext(selected.View.Records["r:context"])
	if err != nil {
		t.Fatal(err)
	}
	now, err := client.Time(ctx).Result()
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
	for _, f := range []string{"r:result-1", "r:result-2"} {
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
	selected, err = store.ObserveContext(ctx, "uac", "incarnation", "r:context")
	if err != nil {
		t.Fatal(err)
	}
	current, err = decodeNativeContext(selected.View.Records["r:context"])
	if err != nil {
		t.Fatal(err)
	}
	due = &rediscore.DueContext{Identity: selected.Identity, Revision: selected.View.Revision, Assignment: selected.View.Timer, Member: "call-retire", DeadlineMS: cutoff, CheckedAtMS: now.UnixMilli(), Observed: time.Now()}
	owners, err := selectRedisTerminalOwners(ctx, store, due, current, deadline())
	if err != nil || owners == nil {
		t.Fatal("terminal ownership", err)
	}
	if owners.early != "" {
		if err = terminateRedisTerminalEarlyDialog(ctx, store, due, owners, deadline()); err != nil {
			t.Fatal(err)
		}
		selected, err = store.ObserveContext(ctx, "uac", "incarnation", "r:context")
		if err != nil {
			t.Fatal(err)
		}
		due.Revision, due.Assignment = selected.View.Revision, selected.View.Timer
		current, err = decodeNativeContext(selected.View.Records["r:context"])
		if err != nil {
			t.Fatal(err)
		}
		owners, err = selectRedisTerminalOwners(ctx, store, due, current, deadline())
		if err != nil || owners == nil {
			t.Fatal("ended ownership", err)
		}
	}
	if _, err = reconcileRedisTerminalOutput(ctx, store, due, owners, deadline()); err != nil {
		t.Fatal("forget CANCEL U", err)
	}
	if _, err = store.InspectOutput(ctx, ref); !errors.Is(err, rediscore.ErrAbsent) {
		t.Fatal("U not forgotten", err)
	}
	selected, err = store.ObserveContext(ctx, "uac", "incarnation", "r:context")
	if err != nil {
		t.Fatal(err)
	}
	due.Revision, due.Assignment = selected.View.Revision, selected.View.Timer
	current, err = decodeNativeContext(selected.View.Records["r:context"])
	if err != nil {
		t.Fatal(err)
	}
	owners, err = selectRedisTerminalOwners(ctx, store, due, current, deadline())
	if err != nil || owners == nil {
		t.Fatal("owners after forget", err)
	}
	if _, err = reconcileRedisTerminalOutput(ctx, store, due, owners, deadline()); err != nil {
		t.Fatal("remove forgotten CANCEL", err)
	}
	selected, err = store.ObserveContext(ctx, "uac", "incarnation", childField, field)
	if err != nil {
		t.Fatal(err)
	}
	child, err := readPrivateProcedureRecord(selected.View.Records[childField])
	if err != nil || slices.Contains(child.Outputs, field) || child.OutputConsumed || len(selected.View.Records[field]) != 0 {
		t.Fatal("CANCEL outcome inferred or ownership retained", err)
	}
}
