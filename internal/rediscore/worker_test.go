package rediscore

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestWorkerCancelledOrLateRenewalReplyReturnsNoAuthority(t *testing.T) {
	for _, name := range []string{"cancelled", "deadline"} {
		t.Run(name, func(t *testing.T) {
			s, admin, primary := workerFixture(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			hook := &delayReserveReply{armed: true}
			if name == "cancelled" {
				hook.cancel = cancel
			} else {
				hook.delay = workerRedisTimeout + 25*time.Millisecond
			}
			client := redis.NewClient(&redis.Options{Addr: admin.Options().Addr, MaxRetries: -1})
			defer client.Close()
			client.AddHook(hook)
			peer, err := New(client, Config{Root: s.root, Profile: s.profile, RecordBudget: s.maxBytes, PrimaryRunID: primary})
			if err != nil {
				t.Fatal(err)
			}
			lease, err := peer.RenewWorker(ctx, WorkerLeaseRequest{Worker: "worker1", Epoch: "epoch1", PrimaryRunID: primary, Acquire: true})
			if !errors.Is(err, ErrUnknown) || !lease.Until.IsZero() || lease.Coordinator {
				t.Fatal("cancelled or late committed reply returned worker authority", lease, err)
			}
			if epoch, err := admin.HGet(context.Background(), s.workerKey("worker1"), "epoch").Result(); err != nil || epoch != "epoch1" {
				t.Fatal("qualification did not lose an already committed lease reply", epoch, err)
			}
		})
	}
}

func workerFixture(t *testing.T) (*Store, *redis.Client, string) {
	t.Helper()
	s, c, _ := fixture(t)
	info, err := c.Info(context.Background(), "server").Result()
	if err != nil {
		t.Fatal(err)
	}
	var runID string
	for _, line := range strings.Split(info, "\n") {
		if strings.HasPrefix(line, "run_id:") {
			runID = strings.TrimSpace(strings.TrimPrefix(line, "run_id:"))
		}
	}
	if !validPrimary(runID) {
		t.Fatal("invalid Redis run ID")
	}
	t.Cleanup(func() {
		keys := []string{s.workerKey("worker1"), s.workerKey("worker2"), s.workerTimerKey("worker1"), s.workerTimerKey("worker2"), s.workerReadyKey("worker1"), s.workerReadyKey("worker2"), s.coordinatorKey(), s.ownerExpiryKey()}
		if err := c.Del(context.Background(), keys...).Err(); err != nil {
			t.Error(err)
		}
	})
	return s, c, runID
}

func TestWorkerExplicitReacquirePreservesPhysicalUncertainty(t *testing.T) {
	s, client, id, output, data := outputFixture(t)
	ctx := context.Background()
	req := WorkerLeaseRequest{Worker: output.Worker, Epoch: output.Epoch, PrimaryRunID: s.primary, Acquire: true}
	if _, err := s.RenewWorker(ctx, req); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Del(ctx, s.ownerExpiryKey(), s.coordinatorKey()).Err() })
	publishOutput(t, s, id, output, data)
	ref := outputRef(id, output)
	if _, err := s.ReserveOutput(ctx, ref); err != nil {
		t.Fatal(err)
	}
	past := time.Now().Add(-time.Second).UnixMilli()
	if err := client.HSet(ctx, s.workerKey(output.Worker), "expires_ms", past).Err(); err != nil {
		t.Fatal(err)
	}
	if err := client.ZAdd(ctx, s.ownerExpiryKey(), redis.Z{Score: float64(past), Member: "w-" + output.Worker}).Err(); err != nil {
		t.Fatal(err)
	}
	req.Acquire = false
	if _, err := s.RenewWorker(ctx, req); !errors.Is(err, ErrExpired) {
		t.Fatal("ordinary renewal revived an expired process lease", err)
	}
	req.Reacquire = true
	lease, err := s.RenewWorker(ctx, req)
	if err != nil || !lease.Coordinator || !time.Now().Before(lease.Until) || lease.OwnerExpiry.DeadlineMS != lease.RedisTimeMS+WorkerLeaseLifetime.Milliseconds() {
		t.Fatal("explicit recovery did not refresh authoritative heads", lease, err)
	}
	if _, err = s.ReserveOutput(ctx, ref); !errors.Is(err, ErrUnknown) {
		t.Fatal("reacquisition replayed an uncertain physical attempt", err)
	}
	if err = s.CompleteOutput(ctx, ref, "WRITTEN"); err != nil {
		t.Fatal("reacquisition blocked exact late physical proof", err)
	}
	wrong := req
	wrong.Epoch = "different-process"
	if _, err = s.RenewWorker(ctx, wrong); !errors.Is(err, ErrConflict) {
		t.Fatal("different process inherited old physical authority", err)
	}
}

func TestWorkerLostExplicitReacquireReturnsNoAuthority(t *testing.T) {
	s, admin, primary := workerFixture(t)
	ctx := context.Background()
	client := redis.NewClient(&redis.Options{Addr: admin.Options().Addr, MaxRetries: -1})
	defer client.Close()
	client.AddHook(&loseReply{armed: true})
	peer, err := New(client, Config{Root: s.root, Profile: s.profile, RecordBudget: s.maxBytes, PrimaryRunID: primary})
	if err != nil {
		t.Fatal(err)
	}
	lease, err := peer.RenewWorker(ctx, WorkerLeaseRequest{Worker: "worker1", Epoch: "epoch1", PrimaryRunID: primary, Reacquire: true})
	if !errors.Is(err, ErrUnknown) || !lease.Until.IsZero() || lease.Coordinator {
		t.Fatal("lost recovery reply returned authority", lease, err)
	}
}

func TestWorkerLeaseHeadsAndEpochFencing(t *testing.T) {
	s, c, primary := workerFixture(t)
	ctx := context.Background()
	req := WorkerLeaseRequest{Worker: "worker1", Epoch: "epoch1", PrimaryRunID: primary, Acquire: true}
	before := time.Now()
	lease, err := s.RenewWorker(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	if lease.Head.Context != "" || lease.Ready || !lease.Coordinator || lease.Until.After(before.Add(2100*time.Millisecond)) {
		t.Fatal("invalid empty lease", lease)
	}
	now := lease.RedisTimeMS
	if err = c.ZAdd(ctx, s.workerTimerKey(req.Worker), redis.Z{Score: float64(now + 900), Member: "context1"}, redis.Z{Score: float64(now + 1400), Member: "context2"}).Err(); err != nil {
		t.Fatal(err)
	}
	if err = c.ZAdd(ctx, s.workerReadyKey(req.Worker), redis.Z{Score: float64(now + 1000), Member: "context1/output1"}).Err(); err != nil {
		t.Fatal(err)
	}
	if err = c.ZAdd(ctx, s.ownerExpiryKey(), redis.Z{Score: float64(now + 1200), Member: "a-owner1"}).Err(); err != nil {
		t.Fatal(err)
	}
	if err = c.HSet(ctx, s.coordinatorKey(), "worker", req.Worker, "epoch", req.Epoch, "expires_ms", now+1800).Err(); err != nil {
		t.Fatal(err)
	}
	req.Acquire = false
	lease, err = s.RenewWorker(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	if lease.Head.Context != "context1" || lease.Head.DeadlineMS != now+900 || !lease.Ready || !lease.Coordinator || lease.OwnerExpiry.Context != "a-owner1" {
		t.Fatal("atomic heads/ready missing", lease)
	}
	n, err := c.ZCard(ctx, s.workerTimerKey(req.Worker)).Result()
	if err != nil || n != 2 {
		t.Fatal("head peek removed duty", n, err)
	}
	if err = c.ZAdd(ctx, s.workerReadyKey("worker2"), redis.Z{Score: float64(now - 1), Member: "context2/expired"}).Err(); err != nil {
		t.Fatal(err)
	}
	other := WorkerLeaseRequest{Worker: "worker2", Epoch: "epoch2", PrimaryRunID: primary, Acquire: true}
	separate, err := s.RenewWorker(ctx, other)
	if err != nil {
		t.Fatal(err)
	}
	if separate.Ready || separate.Head.Context != "" || separate.Coordinator {
		t.Fatal("another worker saw private heads", separate)
	}
	competing := req
	competing.Epoch = "epoch2"
	competing.Acquire = true
	if _, err = s.RenewWorker(ctx, competing); !errors.Is(err, ErrConflict) {
		t.Fatal("live epoch not fenced", err)
	}
	if err = c.HSet(ctx, s.workerKey(req.Worker), "expires_ms", now-1).Err(); err != nil {
		t.Fatal(err)
	}
	if _, err = s.RenewWorker(ctx, req); !errors.Is(err, ErrExpired) {
		t.Fatal("expired epoch renewed", err)
	}
	reacquire := req
	reacquire.Acquire = true
	if _, err = s.RenewWorker(ctx, reacquire); !errors.Is(err, ErrExpired) {
		t.Fatal("expired epoch reacquired", err)
	}
	if _, err = s.RenewWorker(ctx, competing); !errors.Is(err, ErrConflict) {
		t.Fatal("worker reference reused", err)
	}
	if _, err = s.RenewWorker(ctx, req); !errors.Is(err, ErrExpired) {
		t.Fatal("old epoch was not fenced", err)
	}
}

func TestWorkerLeasePreflightDoesNotMutate(t *testing.T) {
	s, c, primary := workerFixture(t)
	ctx := context.Background()
	req := WorkerLeaseRequest{Worker: "worker1", Epoch: "epoch1", PrimaryRunID: primary, Acquire: true}
	if err := c.Set(ctx, s.workerTimerKey(req.Worker), "bad", 0).Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.RenewWorker(ctx, req); !errors.Is(err, ErrCorrupt) {
		t.Fatal("wrong type accepted", err)
	}
	exists, err := c.Exists(ctx, s.workerKey(req.Worker)).Result()
	if err != nil || exists != 0 {
		t.Fatal("preflight changed lease", exists, err)
	}
	if err = c.Del(ctx, s.workerTimerKey(req.Worker)).Err(); err != nil {
		t.Fatal(err)
	}
	req.PrimaryRunID = strings.Repeat("a", 40)
	if _, err = s.RenewWorker(ctx, req); !errors.Is(err, ErrAuthority) {
		t.Fatal("wrong primary accepted", err)
	}
	exists, err = c.Exists(ctx, s.workerKey(req.Worker)).Result()
	if err != nil || exists != 0 {
		t.Fatal("wrong primary changed lease", exists, err)
	}
	wrongStore := *s
	wrongStore.primary = req.PrimaryRunID
	if _, err = wrongStore.RenewWorker(ctx, req); !errors.Is(err, ErrAuthority) {
		t.Fatal("Redis primary check accepted wrong generation", err)
	}
	exists, err = c.Exists(ctx, s.workerKey(req.Worker)).Result()
	if err != nil || exists != 0 {
		t.Fatal("Redis primary refusal changed lease", exists, err)
	}
}

func TestWakeGenerationSurvivesReadRace(t *testing.T) {
	s, c, _ := workerFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	wake, err := s.SubscribeWake(ctx, "worker1")
	if err != nil {
		t.Fatal(err)
	}
	defer wake.Close()
	before, ch, live := wake.Snapshot()
	if !live {
		t.Fatal("not subscribed")
	}
	if err = c.Publish(ctx, s.root+"core:wake:worker1", "wake").Err(); err != nil {
		t.Fatal(err)
	}
	if err = wake.Receive(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case <-ch:
	default:
		t.Fatal("wake lost during read")
	}
	after, next, live := wake.Snapshot()
	if !live || after == before {
		t.Fatal("generation not advanced")
	}
	select {
	case <-next:
		t.Fatal("new latch already invalidated")
	default:
	}
	if err = wake.Close(); err != nil {
		t.Fatal(err)
	}
	_, _, live = wake.Snapshot()
	if live {
		t.Fatal("closed wake still live")
	}
}
