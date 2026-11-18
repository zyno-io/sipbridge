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
	"sipbridge/internal/rediscore"
)

// The result and its cutoff are real Redis records. Advancing the cutoff is
// test-only; no native procedure or physical attempt is fabricated as closed.
func TestRedisSupersededResultExpiry(t *testing.T) {
	t.Run("unknown", func(t *testing.T) { testRedisSupersededResultExpiry(t, pb.OperationResult_UNKNOWN) })
	t.Run("refusal", func(t *testing.T) { testRedisSupersededResultExpiry(t, pb.OperationResult_NOT_ACCEPTED) })
}

func testRedisSupersededResultExpiry(t *testing.T, certainty pb.OperationResult_Certainty) {
	address := os.Getenv("SIPBRIDGE_CORE_TEST_REDIS")
	if address == "" {
		t.Skip("isolated Redis fixture required")
	}
	ctx := context.Background()
	client := redis.NewClient(&redis.Options{Addr: address, MaxRetries: -1})
	root := "sipstate:test:bye-result-" + uuid.NewString() + ":"
	worker := "worker"
	t.Cleanup(func() {
		for _, name := range []string{"profile", "capacity", "selectors", "context:call", "deadlines:call", "worker:" + worker, "timers:" + worker, "ready:" + worker, "owner-expiry", "coordinator"} {
			if e := client.Del(ctx, root+"core:"+name).Err(); e != nil {
				t.Error(e)
			}
		}
		_ = client.Close()
	})
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
	profile := []byte{6, 3}
	if err = client.Set(ctx, root+"core:profile", profile, 0).Err(); err != nil {
		t.Fatal(err)
	}
	store, err := rediscore.New(client, rediscore.Config{Root: root, Profile: profile, PrimaryRunID: primary, RecordBudget: 4 << 20})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = store.RenewWorker(ctx, rediscore.WorkerLeaseRequest{Worker: worker, Epoch: "epoch", PrimaryRunID: primary, Acquire: true}); err != nil {
		t.Fatal(err)
	}
	now, err := client.Time(ctx).Result()
	if err != nil {
		t.Fatal(err)
	}
	cutoff := now.Add(-time.Second).UnixMilli()
	fingerprint := bytes.Repeat([]byte{9}, 32)
	result := &nativepb.Result{Certainty: certainty, Error: "SIP delivery unavailable or uncertain", Sequence: 2, OperationId: "bye", Fingerprint: fingerprint, DecidingLeaseTerm: "term", CutoffMs: cutoff}
	resultData, err := encodeNativeResult(result)
	if err != nil {
		t.Fatal(err)
	}
	next := &nativepb.Result{Certainty: pb.OperationResult_NOT_ACCEPTED, Sequence: 3, OperationId: "later", Fingerprint: bytes.Repeat([]byte{3}, 32), DecidingLeaseTerm: "term", CutoffMs: now.Add(time.Minute).UnixMilli()}
	nextData, err := encodeNativeResult(next)
	if err != nil {
		t.Fatal(err)
	}
	current := &nativepb.Context{Context: &pb.ContextRef{Id: "call", State: &pb.StateRef{Incarnation: "inc"}}, Kind: pb.ContextSnapshot_UAC, CallId: "expiry@test", CommandSequence: 3}
	currentData, err := encodeNativeContext(current)
	if err != nil {
		t.Fatal(err)
	}
	identity := rediscore.Identity{Context: "call", Incarnation: "inc"}
	assignment := rediscore.TimerAssignment{Worker: worker, Epoch: "epoch", Term: "epoch"}
	member := "orig-result-2"
	_, err = store.Commit(ctx, rediscore.Publication{Identity: identity, Create: true, Deadline: time.Now().Add(time.Second), Changes: []rediscore.Change{{Field: "r:context", Value: currentData}, {Field: "r:result-2", Value: resultData}, {Field: "r:result-3", Value: nextData}}, Timers: &rediscore.TimerMutation{Target: assignment, Changes: []rediscore.TimerChange{{Member: member, Due: time.UnixMilli(cutoff)}}}})
	if err != nil {
		t.Fatal(err)
	}
	due, err := store.PeekDue(ctx, worker, "epoch")
	if err != nil || due == nil || due.Member != member {
		t.Fatal("result duty missing", err)
	}
	stale := *due
	stale.Revision++
	if err = expireRedisSupersededResult(ctx, store, &stale, time.Now().Add(time.Second)); err == nil {
		t.Fatal("stale revision accepted")
	}
	if err = expireRedisSupersededResult(ctx, store, due, time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	after, err := store.Read(ctx, identity, "r:result-2", "r:result-3")
	if err != nil || len(after.Records["r:result-2"]) != 0 || !bytes.Equal(after.Records["r:result-3"], nextData) {
		t.Fatal("expiry changed current result or retained old result", err)
	}
}
