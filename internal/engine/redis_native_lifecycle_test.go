package engine

import (
	"bytes"
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/nativepb"
	"sipbridge/internal/redisclient"
	"sipbridge/internal/rediscore"
)

func TestRedisNativeLifecycleCallLeavesSIPDeadlineRunnable(t *testing.T) {
	for _, test := range []struct {
		name  string
		kind  pb.ContextSnapshot_Kind
		event pb.DialogEvent_Kind
		ended bool
	}{
		{"call-acknowledged", pb.ContextSnapshot_UAS, pb.DialogEvent_ACKNOWLEDGED, false},
		{"call-ended", pb.ContextSnapshot_UAC, pb.DialogEvent_ENDED, true},
		{"call-failed", pb.ContextSnapshot_UAC, pb.DialogEvent_FAILED, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			testRedisLifecycleProgress(t, test.kind, test.event, test.ended)
		})
	}
}

func testRedisLifecycleProgress(t *testing.T, kind pb.ContextSnapshot_Kind, eventKind pb.DialogEvent_Kind, ended bool) {
	address := os.Getenv("SIPBRIDGE_CORE_TEST_REDIS")
	if address == "" {
		t.Skip("isolated Redis fixture required")
	}
	// Reuse only this fixture's authenticated JSON transport. The event and
	// scheduling authority under test belong exclusively to the native store.
	transport, peer, websocket, ctx, session, _, _ := residentControlFixture(t)
	root := "sipstate:test:lifecycle-" + uuid.NewString() + ":"
	connection, err := rediscore.NewConnection(rediscore.ConnectionConfig{Redis: redisclient.Options{Address: address},
		Profile: rediscore.ProfileConfig{Root: root, Service: "test", Generation: "one", IdentityKey: bytes.Repeat([]byte{7}, 32), RecordBudget: 1 << 20}})
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	store, err := connection.Refresh(ctx)
	if err != nil {
		t.Fatal(err)
	}
	client := redis.NewClient(&redis.Options{Addr: address, MaxRetries: -1})
	defer client.Close()
	t.Cleanup(func() {
		cleanup := redis.NewClient(&redis.Options{Addr: address, MaxRetries: -1})
		defer cleanup.Close()
		for _, name := range []string{"profile", "capacity", "selectors", "applications", "owner-expiry", "coordinator", "worker:worker", "timers:worker", "ready:worker", "context:context", "deadlines:context"} {
			if e := cleanup.Del(context.Background(), root+"core:"+name).Err(); e != nil {
				t.Error(e)
			}
		}
	})
	info, err := client.Info(ctx, "server").Result()
	if err != nil {
		t.Fatal(err)
	}
	primary := ""
	for _, line := range strings.Split(info, "\r\n") {
		if value, found := strings.CutPrefix(line, "run_id:"); found {
			primary = value
		}
	}
	_, err = store.RenewWorker(ctx, rediscore.WorkerLeaseRequest{Worker: "worker", Epoch: "epoch", PrimaryRunID: primary, Acquire: true})
	if err != nil {
		t.Fatal(err)
	}
	id := rediscore.Identity{Context: "context", Incarnation: "incarnation"}
	assignment := rediscore.TimerAssignment{Worker: "worker", Epoch: "epoch", Term: "epoch"}
	ref := &pb.ContextRef{Id: id.Context, State: &pb.StateRef{Authority: connection.Authority(), GroupId: id.Context, Incarnation: id.Incarnation}}
	retireAt := int64(0)
	if ended {
		retireAt = time.Now().Add(2 * time.Minute).UnixMilli()
	}
	current, err := encodeNativeContext(&nativepb.Context{Context: ref, Application: peer.Application(), Kind: kind, Ended: ended, CallRetireAtMs: retireAt})
	if err != nil {
		t.Fatal(err)
	}
	subscription, err := encodeNativeSubscription(nativeSubscriptionRecord{Native: stateSubscription{Event: "dialog"}, BaseCredit: 4096})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	operation := ""
	if eventKind == pb.DialogEvent_FAILED {
		operation = "start-call"
	}
	event, err := encodeNativeLifecycle(&nativepb.Lifecycle{Kind: eventKind, OperationId: operation,
		Snapshot: &pb.ContextSnapshot{Context: ref, Application: peer.Application(), Kind: kind, Ended: ended},
		NextMs:   now.Add(-time.Second).UnixMilli(), DeadlineMs: now.Add(time.Minute).UnixMilli()})
	if err != nil {
		t.Fatal(err)
	}
	changes := []rediscore.Change{{Field: "r:context", Value: current}, {Field: "r:event-one", Value: event}}
	if kind == pb.ContextSnapshot_SUBSCRIPTION {
		changes = append(changes, rediscore.Change{Field: "r:subscription", Value: subscription})
	}
	_, err = store.Commit(ctx, rediscore.Publication{Identity: id, Create: true, Deadline: now.Add(time.Second), CreditDelta: 4096,
		Changes: changes,
		Timers: &rediscore.TimerMutation{Target: assignment, Changes: []rediscore.TimerChange{
			{Member: "event-one", Due: now.Add(-time.Second)}, {Member: "native-duty", Due: now.Add(-500 * time.Millisecond)}}}})
	if err != nil {
		t.Fatal(err)
	}
	due, err := store.PeekDue(ctx, "worker", "epoch")
	if err != nil || due == nil || due.Member != "event-one" {
		t.Fatal("event head missing", due, err)
	}
	e := &redisNative{control: transport.control, eventWork: make(chan struct{}, 8), events: make(map[redisNativeEventKey]struct{}, 8)}
	if err = e.dispatchLifecycle(ctx, store, due); err != nil {
		t.Fatal(err)
	}
	notification := readControllerEnvelope(t, ctx, websocket)
	if notification.GetDialogEvent() == nil || notification.CorrelationId != "event-one" {
		t.Fatal("frozen event was not delivered over JSON")
	}
	publicEvent := notification.GetDialogEvent()
	if publicEvent.Kind != eventKind || publicEvent.OperationId != operation || publicEvent.Message != nil || publicEvent.Error != "" || publicEvent.Snapshot.Ended != ended {
		t.Fatal("snapshot-only lifecycle shape changed")
	}
	// The controller has not replied. Native work in the same context must
	// already be selectable, and winning this revision must not resend the event.
	native, err := store.PeekDue(ctx, "worker", "epoch")
	if err != nil || native == nil || native.Member != "native-duty" {
		t.Fatal("blocked controller hid the SIP deadline", native, err)
	}
	timers := []rediscore.TimerChange{{Member: native.Member, Delete: true}}
	if kind == pb.ContextSnapshot_SUBSCRIPTION {
		timers = append(timers, rediscore.TimerChange{Member: "subscription-expiry", Due: now.Add(time.Hour)})
	}
	_, err = store.Commit(ctx, rediscore.Publication{Identity: id, Revision: native.Revision, Deadline: time.Now().Add(time.Second),
		Timers: &rediscore.TimerMutation{Expected: assignment, Target: assignment, RequireLease: true, Changes: timers}})
	if err != nil {
		t.Fatal(err)
	}
	writeControllerEnvelope(t, ctx, websocket, &pb.Envelope{SessionId: session, CorrelationId: notification.CorrelationId,
		Payload: &pb.Envelope_Result{Result: &pb.OperationResult{Certainty: pb.OperationResult_ACCEPTED}}})
	finished := make(chan struct{})
	go func() { e.wg.Wait(); close(finished) }()
	select {
	case <-finished:
	case <-ctx.Done():
		t.Fatal("event acknowledgement did not finish")
	}
	view, err := store.Read(ctx, id, "r:event-one")
	if err != nil || len(view.Records["r:event-one"]) != 0 || len(e.eventWork) != 0 {
		t.Fatal("accepted event was not consumed after unrelated native update", err)
	}
	if kind != pb.ContextSnapshot_SUBSCRIPTION {
		if !ended && view.Timer != (rediscore.TimerAssignment{}) {
			t.Fatal("quiet active call retained a timer assignment")
		}
		if ended {
			next, e := store.PeekContextDeadline(ctx, id, view.Revision, "")
			if e != nil || next == nil || next.Member != "call-retire" || next.DeadlineMS != retireAt {
				t.Fatal("ended call lost or extended its original retirement duty", next, e)
			}
		}
	}
}

func TestRedisLifecycleCutoffUsesObservedClock(t *testing.T) {
	observed := time.Now()
	// Simulate a different wall-clock epoch at capture without changing the
	// machine clock. Only the verified Redis interval may map onto Observed.
	checked := observed.Add(-time.Hour).UnixMilli()
	due := &rediscore.DueContext{Observed: observed, CheckedAtMS: checked}
	cutoff, err := redisLifecycleCutoff(due, checked+1500)
	if err != nil || cutoff.Sub(observed) != 1500*time.Millisecond || cutoff == cutoff.Round(0) {
		t.Fatal("event cutoff depended on the current wall epoch", cutoff, err)
	}
	wallOnly := *due
	wallOnly.Observed = wallOnly.Observed.Round(0)
	if _, err = redisLifecycleCutoff(&wallOnly, checked+1500); err == nil {
		t.Fatal("wall-only anchor admitted")
	}
	if _, err = redisLifecycleCutoff(due, int64(1<<63-1)); err == nil {
		t.Fatal("overflowing retained deadline admitted")
	}
	if _, err = redisLifecycleCutoff(due, checked); err == nil {
		t.Fatal("expired debt mapped to a future callback cutoff")
	}
}
