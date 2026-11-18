package engine

import (
	"bytes"
	"context"
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

func testRedisInviteWorkerAnchor(t *testing.T, ctx context.Context, store *rediscore.Store, client *redis.Client, input redisUACQualification) time.Time {
	t.Helper()
	info, err := client.Info(ctx, "server").Result()
	if err != nil {
		t.Fatal(err)
	}
	primary := ""
	for _, line := range strings.Split(info, "\r\n") {
		if value, ok := strings.CutPrefix(line, "run_id:"); ok {
			primary = value
		}
	}
	lease, err := store.RenewWorker(ctx, rediscore.WorkerLeaseRequest{Worker: input.ExecutorTimer.Worker, Epoch: input.Executor.Epoch, PrimaryRunID: primary})
	if err != nil {
		t.Fatal(err)
	}
	return lease.Until
}

// The local derivative uses the exact WRITTEN attempt and the existing one
// manager. Its timer tick must not consult Redis.
func testRedisApplicationInviteUDPArm(t *testing.T, ctx context.Context, store *rediscore.Store, client *redis.Client, input redisUACQualification, output *nativepb.Output, wire []byte) <-chan struct{} {
	t.Helper()
	counter := &retryCommandCounter{}
	client.AddHook(counter)
	writes := make(chan []byte, 1)
	manager, _ := udpRetryManager(t, nativeUDPRetryLimits{Entries: 2, Hints: 2, Writers: 2, Bytes: 1 << 20}, func(ctx context.Context, _ *pb.FlowRef, data []byte, _ time.Time) error {
		select {
		case writes <- bytes.Clone(data):
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	engine := &redisNative{cfg: Config{Process: input.Executor}, worker: input.ExecutorTimer.Worker, udp: manager}
	stopped := make(chan struct{}, 1)
	wake, err := store.SubscribeWakeResults(ctx, engine.worker, func(rediscore.ResultHint) {}, func(h rediscore.UDPHint) {
		engine.udpHint(h)
		if h.Stop && h.Procedure == output.Procedure {
			select {
			case stopped <- struct{}{}:
			default:
			}
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for wake.Receive(ctx) == nil {
		}
	}()
	t.Cleanup(func() { _ = wake.Close(); <-done })
	ref, err := nativeOutputRef(output.Write)
	if err != nil {
		t.Fatal(err)
	}
	if err = engine.armRedisApplicationInviteUDP(ctx, store, ref, output); err != nil {
		t.Fatal("arm re-INVITE A", err)
	}
	baseline := counter.calls.Load()
	select {
	case retry := <-writes:
		if !bytes.Equal(retry, wire) || counter.calls.Load() != baseline {
			t.Fatal("re-INVITE A changed wire or read Redis")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("re-INVITE A retry missing")
	}
	return stopped
}

// testRedisApplicationInviteRetirement advances only the original native M
// or D deadline/index. It waits for the real physical cutoffs on Redis TIME.
func testRedisApplicationInviteRetirement(t *testing.T, ctx context.Context, store *rediscore.Store, client *redis.Client, root string, input redisUACQualification, identity rediscore.Identity, dialogField string, output, ack *nativepb.Output, ackField string, timer portable.Timer) {
	t.Helper()
	info, err := client.Info(ctx, "server").Result()
	if err != nil {
		t.Fatal(err)
	}
	primary := ""
	for _, line := range strings.Split(info, "\r\n") {
		if value, ok := strings.CutPrefix(line, "run_id:"); ok {
			primary = value
		}
	}
	physical := max(int64(output.Write.Permit.Authorization.DeadlineMs), int64(ack.Write.Permit.Authorization.DeadlineMs))
	limit := time.Now().Add(4 * time.Second)
	var checked time.Time
	for {
		if _, err = store.RenewWorker(ctx, rediscore.WorkerLeaseRequest{Worker: input.ExecutorTimer.Worker, Epoch: input.Executor.Epoch, PrimaryRunID: primary}); err != nil {
			t.Fatal(err)
		}
		checked, err = client.Time(ctx).Result()
		if err != nil {
			t.Fatal(err)
		}
		if checked.UnixMilli() >= physical {
			break
		}
		if !time.Now().Before(limit) {
			t.Fatal("re-INVITE physical cutoff did not become due")
		}
		time.Sleep(25 * time.Millisecond)
	}
	view, err := store.Read(ctx, identity, "r:context")
	if err != nil {
		t.Fatal(err)
	}
	ackDue := &rediscore.DueContext{Identity: identity, Revision: view.Revision, Assignment: view.Timer, Member: "out-" + strings.TrimPrefix(ackField, "r:"), DeadlineMS: int64(ack.Write.Permit.Authorization.DeadlineMs), CheckedAtMS: checked.UnixMilli()}
	if err = retireRedisApplicationInviteACK(ctx, store, ackDue, input.Key, time.Now().Add(time.Second)); err != nil {
		t.Fatal("retire re-INVITE ACK", err)
	}
	view, err = store.Read(ctx, identity, output.Procedure)
	if err != nil {
		t.Fatal(err)
	}
	proc, err := readPrivateProcedureRecord(view.Records[output.Procedure])
	if err != nil || proc.Native.Timers == nil || proc.Native.Timers.Values[string(timer)] == nil {
		t.Fatal("native re-INVITE terminal timer missing", err)
	}
	d := proc.Native.Timers.Values[string(timer)]
	d.At = checked.Add(-time.Second).UnixMilli()
	data, err := marshalNativeRecord(storagecodec.NativeProcedure, proc)
	if err != nil {
		t.Fatal(err)
	}
	id := strings.TrimPrefix(output.Procedure, "r:app-invite-")
	member := "app-invite-" + id + "-" + string(timer)
	_, err = store.Commit(ctx, rediscore.Publication{Identity: identity, Revision: view.Revision, Deadline: time.Now().Add(time.Second), SpendReservation: true, Changes: []rediscore.Change{{Field: output.Procedure, Value: data}}, Timers: &rediscore.TimerMutation{Expected: view.Timer, Target: view.Timer, RequireLease: true, Changes: []rediscore.TimerChange{{Member: member, Due: time.UnixMilli(d.At)}}}})
	if err != nil {
		t.Fatal("age re-INVITE terminal timer", err)
	}
	view, err = store.Read(ctx, identity, "r:context")
	if err != nil {
		t.Fatal(err)
	}
	due := &rediscore.DueContext{Identity: identity, Revision: view.Revision, Assignment: view.Timer, Member: member, DeadlineMS: d.At, CheckedAtMS: checked.UnixMilli()}
	if err = fireRedisApplicationInviteDeadline(ctx, store, due, time.Now().Add(time.Second)); err != nil {
		t.Fatal("fire re-INVITE terminal timer", err)
	}
	view, err = store.Read(ctx, identity, "r:context")
	if err != nil {
		t.Fatal(err)
	}
	outputDue := &rediscore.DueContext{Identity: identity, Revision: view.Revision, Assignment: view.Timer, Member: "out-app-invite-output-" + id, DeadlineMS: int64(output.Write.Permit.Authorization.DeadlineMs), CheckedAtMS: checked.UnixMilli()}
	if err = dispatchRedisApplicationInviteCutoff(ctx, store, outputDue, time.Now().Add(time.Second)); err != nil {
		t.Fatal("retire re-INVITE output", err)
	}
	view, err = store.Read(ctx, identity, "r:context")
	if err != nil {
		t.Fatal(err)
	}
	retireMember := "app-invite-retire-" + id
	retireScore, err := client.ZScore(ctx, root+"core:deadlines:"+identity.Context, retireMember).Result()
	if err != nil || int64(retireScore) > checked.UnixMilli() {
		t.Fatal("selected retirement not due", err)
	}
	retireDue := &rediscore.DueContext{Identity: identity, Revision: view.Revision, Assignment: view.Timer, Member: retireMember, DeadlineMS: int64(retireScore), CheckedAtMS: checked.UnixMilli()}
	if err = retireRedisApplicationInvite(ctx, store, retireDue, input.Key, time.Now().Add(time.Second)); err != nil {
		t.Fatal("retain selected re-INVITE", err)
	}
	view, err = store.Read(ctx, identity, output.Procedure, dialogField, output.Result)
	if err != nil {
		t.Fatal(err)
	}
	proc, err = readPrivateProcedureRecord(view.Records[output.Procedure])
	dialog, dialogErr := readPrivateDialogRecord(view.Records[dialogField])
	result, resultErr := decodeNativeResult(view.Records[output.Result])
	if err != nil || dialogErr != nil || resultErr != nil || proc.Native.Phase != string(portable.Terminated) || len(proc.Outputs) != 0 || dialog.LocalInvite == nil || dialog.LocalInvite.Request != proc.Origin || result.Output != "" || result.Certainty != pb.OperationResult_COMPLETED {
		t.Fatal("selected re-INVITE did not retain exact native/result owners", err, dialogErr, resultErr)
	}
	if timer != portable.TimerM {
		return
	}
	oldRequest, oldResponse, oldACK := result.Request, result.Response, dialog.LocalInvite.Ack
	nextDeadline := time.Now().Add(2 * time.Second)
	next := &pb.Command{Context: input.Context, Sequence: result.Sequence + 1, Admission: &pb.CommandAdmission{DirectoryEpoch: "directory", ExpiresAtMs: uint64(nextDeadline.UnixMilli()), SelectedFlow: input.Flow}, Action: &pb.Command_DialogRequest{DialogRequest: &pb.DialogRequest{Method: "INVITE", Body: []byte("v=0\r\ns=next\r\n"), Headers: []*pb.Header{{Name: "Content-Type", Value: "application/sdp"}}}}}
	capture, err := captureRedisNotifyWait(next)
	if err != nil {
		t.Fatal(err)
	}
	leaseUntil := testRedisInviteWorkerAnchor(t, ctx, store, client, input)
	if _, err = publishRedisApplicationInvite(ctx, store, input.Key, input.Application, input.Lease, next, capture, "next-reinvite", input.ExecutorTimer, nextDeadline, leaseUntil); err != nil {
		t.Fatal("replace selected re-INVITE", err)
	}
	view, err = store.Read(ctx, identity, "r:context")
	if err != nil {
		t.Fatal(err)
	}
	checked, err = client.Time(ctx).Result()
	if err != nil {
		t.Fatal(err)
	}
	retireScore, err = client.ZScore(ctx, root+"core:deadlines:"+identity.Context, retireMember).Result()
	if err != nil {
		t.Fatal(err)
	}
	if int64(retireScore) > checked.UnixMilli() {
		checked = waitRedisFixtureTime(t, ctx, client, time.UnixMilli(int64(retireScore)))
	}
	retireDue = &rediscore.DueContext{Identity: identity, Revision: view.Revision, Assignment: view.Timer, Member: retireMember, DeadlineMS: int64(retireScore), CheckedAtMS: checked.UnixMilli()}
	if err = retireRedisApplicationInvite(ctx, store, retireDue, input.Key, time.Now().Add(time.Second)); err != nil {
		t.Fatal("retire replaced re-INVITE", err)
	}
	view, err = store.Read(ctx, identity, output.Procedure, output.Result, oldRequest, oldResponse, oldACK)
	if err != nil {
		t.Fatal(err)
	}
	previous, err := decodeNativeResult(view.Records[output.Result])
	if err != nil || len(view.Records[output.Procedure]) != 0 || previous.Request != oldRequest || previous.Response != oldResponse || len(view.Records[oldRequest]) == 0 || len(view.Records[oldResponse]) == 0 || len(view.Records[oldACK]) != 0 {
		t.Fatal("replaced INVITE did not release only its exact owners", err)
	}
}
