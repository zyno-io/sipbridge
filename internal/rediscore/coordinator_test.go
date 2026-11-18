package rediscore

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

func TestCoordinatorTransfersExpiredWorkerInBoundedBatches(t *testing.T) {
	s, c, id := fixture(t)
	ctx := context.Background()
	old := WorkerLeaseRequest{Worker: "worker1", Epoch: "old-epoch", PrimaryRunID: s.primary, Acquire: true}
	target := WorkerLeaseRequest{Worker: "worker2", Epoch: "target-epoch-long", PrimaryRunID: s.primary, Acquire: true}
	first, err := s.RenewWorker(ctx, old)
	if err != nil || !first.Coordinator {
		t.Fatal("first worker not elected", err)
	}
	second, err := s.RenewWorker(ctx, target)
	if err != nil || second.Coordinator {
		t.Fatal("two coordinators", err)
	}
	ids := []Identity{id, {Context: "ctx-" + uuid.NewString(), Incarnation: uuid.NewString()}, {Context: "ctx-" + uuid.NewString(), Incarnation: uuid.NewString()}}
	keys := []string{s.workerKey(old.Worker), s.workerKey(target.Worker), s.workerTimerKey(old.Worker), s.workerTimerKey(target.Worker), s.ownerExpiryKey(), s.coordinatorKey()}
	for _, v := range ids {
		keys = append(keys, s.contextDeadlineKey(v.Context))
		if v.Context != id.Context {
			keys = append(keys, s.contextKey(v.Context))
		}
	}
	t.Cleanup(func() {
		if err := c.Del(ctx, keys...).Err(); err != nil {
			t.Error(err)
		}
	})
	for i, v := range ids {
		_, err = s.Commit(ctx, Publication{Identity: v, Create: true, Deadline: time.Now().Add(time.Minute), Changes: []Change{{Field: "r:procedure", Value: []byte(fmt.Sprintf("record-%d", i))}}, Timers: &TimerMutation{Target: TimerAssignment{Worker: old.Worker, Epoch: old.Epoch, Term: "old-assignment"}, Changes: []TimerChange{{Member: "procedure", Due: time.Now().Add(time.Minute)}}}})
		if err != nil {
			t.Fatal(err)
		}
	}
	// Cross a decimal revision-width boundary without changing its prior charge.
	if err = c.HSet(ctx, s.contextKey(ids[0].Context), "revision", "9").Err(); err != nil {
		t.Fatal(err)
	}
	checkNamespace(t, s, c, ids...)
	req := WorkerTransfer{Coordinator: target.Worker, CoordinatorEpoch: target.Epoch, ExpiredWorker: old.Worker, Target: target.Worker, TargetEpoch: target.Epoch, Limit: 2}
	if _, err = s.TransferExpiredWorker(ctx, req); !errors.Is(err, ErrAuthority) {
		t.Fatal("unelected coordinator transferred", err)
	}
	expired := time.Now().Add(-time.Second).UnixMilli()
	if err = c.HSet(ctx, s.workerKey(old.Worker), "expires_ms", expired).Err(); err != nil {
		t.Fatal(err)
	}
	if err = c.ZAdd(ctx, s.ownerExpiryKey(), redis.Z{Score: float64(expired), Member: "w-" + old.Worker}).Err(); err != nil {
		t.Fatal(err)
	}
	if err = c.HSet(ctx, s.coordinatorKey(), "expires_ms", expired).Err(); err != nil {
		t.Fatal(err)
	}
	target.Acquire = false
	lease, err := s.RenewWorker(ctx, target)
	if err != nil || !lease.Coordinator || lease.OwnerExpiry.Context != "w-"+old.Worker {
		t.Fatal("successor did not see orphan", lease, err)
	}
	firstHead, err := c.ZRange(ctx, s.workerTimerKey(old.Worker), 0, 0).Result()
	if err != nil || len(firstHead) != 1 {
		t.Fatal("source head missing", err)
	}
	if err = c.ZAdd(ctx, s.workerTimerKey(target.Worker), redis.Z{Score: float64(time.Now().Add(time.Minute).UnixMilli()), Member: firstHead[0]}).Err(); err != nil {
		t.Fatal(err)
	}
	if _, err = s.TransferExpiredWorker(ctx, req); !errors.Is(err, ErrConflict) {
		t.Fatal("colliding target accepted", err)
	}
	checkNamespace(t, s, c, ids...)
	if err = c.ZRem(ctx, s.workerTimerKey(target.Worker), firstHead[0]).Err(); err != nil {
		t.Fatal(err)
	}
	budget, err := c.HGet(ctx, s.capacityKey(), "bytes").Int64()
	if err != nil {
		t.Fatal(err)
	}
	originalLimit := s.namespaceBytes
	s.namespaceBytes = budget
	if _, err = s.TransferExpiredWorker(ctx, req); !errors.Is(err, ErrCapacity) {
		t.Fatal("transfer exceeded namespace capacity", err)
	}
	checkNamespace(t, s, c, ids...)
	s.namespaceBytes = originalLimit
	result, err := s.TransferExpiredWorker(ctx, req)
	if err != nil || result.Moved != 2 || result.Drained {
		t.Fatal("first batch", result, err)
	}
	checkNamespace(t, s, c, ids...)
	n, err := c.ZCard(ctx, s.workerTimerKey(old.Worker)).Result()
	if err != nil || n != 1 {
		t.Fatal("orphan dropped early", n, err)
	}
	result, err = s.TransferExpiredWorker(ctx, req)
	if err != nil || result.Moved != 1 || !result.Drained {
		t.Fatal("final batch", result, err)
	}
	checkNamespace(t, s, c, ids...)
	for i, v := range ids {
		view, err := s.Read(ctx, v, "r:procedure")
		expectedRevision := uint64(2)
		if i == 0 {
			expectedRevision = 10
		}
		if err != nil || view.Revision != expectedRevision || view.Timer != (TimerAssignment{Worker: target.Worker, Epoch: target.Epoch, Term: target.Epoch}) {
			t.Fatal("transferred assignment", view, err)
		}
	}
	if _, err = c.ZScore(ctx, s.ownerExpiryKey(), "w-"+old.Worker).Result(); !errors.Is(err, redis.Nil) {
		t.Fatal("drained orphan retained", err)
	}
}

func TestCoordinatorEmptyExpiredWorkerDoesNotTouchReadyProofs(t *testing.T) {
	s, c, id, out, data := outputFixture(t)
	ctx := context.Background()
	old := WorkerLeaseRequest{Worker: out.Worker, Epoch: out.Epoch, PrimaryRunID: s.primary, Acquire: true}
	target := WorkerLeaseRequest{Worker: "worker2", Epoch: "epoch2", PrimaryRunID: s.primary, Acquire: true}
	keys := []string{s.workerKey(target.Worker), s.workerTimerKey(target.Worker), s.ownerExpiryKey(), s.coordinatorKey()}
	t.Cleanup(func() {
		if err := c.Del(ctx, keys...).Err(); err != nil {
			t.Error(err)
		}
	})
	if _, err := s.RenewWorker(ctx, old); err != nil {
		t.Fatal(err)
	}
	if _, err := s.RenewWorker(ctx, target); err != nil {
		t.Fatal(err)
	}
	publishOutput(t, s, id, out, data)
	expired := time.Now().Add(-time.Second).UnixMilli()
	if err := c.HSet(ctx, s.workerKey(old.Worker), "expires_ms", expired).Err(); err != nil {
		t.Fatal(err)
	}
	if err := c.ZAdd(ctx, s.ownerExpiryKey(), redis.Z{Score: float64(expired), Member: "w-" + old.Worker}).Err(); err != nil {
		t.Fatal(err)
	}
	if err := c.HSet(ctx, s.coordinatorKey(), "expires_ms", expired).Err(); err != nil {
		t.Fatal(err)
	}
	target.Acquire = false
	if _, err := s.RenewWorker(ctx, target); err != nil {
		t.Fatal(err)
	}
	result, err := s.TransferExpiredWorker(ctx, WorkerTransfer{Coordinator: target.Worker, CoordinatorEpoch: target.Epoch, ExpiredWorker: old.Worker, Target: target.Worker, TargetEpoch: target.Epoch, Limit: 1})
	if err != nil || result.Moved != 0 || !result.Drained {
		t.Fatal("empty orphan", result, err)
	}
	if n, err := c.ZCard(ctx, s.workerReadyKey(old.Worker)).Result(); err != nil || n != 1 {
		t.Fatal("ready proof removed", n, err)
	}
	checkNamespace(t, s, c, id)
	if err := s.CompleteOutput(ctx, outputRef(id, out), "NOT_WRITTEN"); err != nil {
		t.Fatal("late exact proof fenced", err)
	}
	checkNamespace(t, s, c, id)
}

func TestCoordinatorDrainsQuietWorkerWithoutNamespace(t *testing.T) {
	s, c, _ := fixture(t)
	ctx := context.Background()
	old := WorkerLeaseRequest{Worker: "worker1", Epoch: "epoch1", PrimaryRunID: s.primary, Acquire: true}
	target := WorkerLeaseRequest{Worker: "worker2", Epoch: "epoch2", PrimaryRunID: s.primary, Acquire: true}
	t.Cleanup(func() {
		if err := c.Del(ctx, s.workerKey(old.Worker), s.workerKey(target.Worker), s.ownerExpiryKey(), s.coordinatorKey()).Err(); err != nil {
			t.Error(err)
		}
	})
	if _, err := s.RenewWorker(ctx, old); err != nil {
		t.Fatal(err)
	}
	if _, err := s.RenewWorker(ctx, target); err != nil {
		t.Fatal(err)
	}
	expired := time.Now().Add(-time.Second).UnixMilli()
	if err := c.HSet(ctx, s.workerKey(old.Worker), "expires_ms", expired).Err(); err != nil {
		t.Fatal(err)
	}
	if err := c.ZAdd(ctx, s.ownerExpiryKey(), redis.Z{Score: float64(expired), Member: "w-" + old.Worker}).Err(); err != nil {
		t.Fatal(err)
	}
	if err := c.HSet(ctx, s.coordinatorKey(), "expires_ms", expired).Err(); err != nil {
		t.Fatal(err)
	}
	target.Acquire = false
	if _, err := s.RenewWorker(ctx, target); err != nil {
		t.Fatal(err)
	}
	result, err := s.TransferExpiredWorker(ctx, WorkerTransfer{Coordinator: target.Worker, CoordinatorEpoch: target.Epoch, ExpiredWorker: old.Worker, Target: target.Worker, TargetEpoch: target.Epoch, Limit: 1})
	if err != nil || result.Moved != 0 || !result.Drained {
		t.Fatal("empty worker did not drain", result, err)
	}
	if n, err := c.Exists(ctx, s.capacityKey()).Result(); err != nil || n != 0 {
		t.Fatal("quiet worker created namespace", n, err)
	}
}

func TestCoordinatorTransferLostReplyIsUnknown(t *testing.T) {
	s, c, id := fixture(t)
	ctx := context.Background()
	old := WorkerLeaseRequest{Worker: "worker1", Epoch: "epoch1", PrimaryRunID: s.primary, Acquire: true}
	target := WorkerLeaseRequest{Worker: "worker2", Epoch: "epoch2", PrimaryRunID: s.primary, Acquire: true}
	keys := []string{s.workerKey(old.Worker), s.workerKey(target.Worker), s.workerTimerKey(old.Worker), s.workerTimerKey(target.Worker), s.contextDeadlineKey(id.Context), s.ownerExpiryKey(), s.coordinatorKey()}
	t.Cleanup(func() {
		if err := c.Del(ctx, keys...).Err(); err != nil {
			t.Error(err)
		}
	})
	if _, err := s.RenewWorker(ctx, old); err != nil {
		t.Fatal(err)
	}
	if _, err := s.RenewWorker(ctx, target); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Commit(ctx, Publication{Identity: id, Create: true, Deadline: time.Now().Add(time.Minute), Timers: &TimerMutation{Target: TimerAssignment{Worker: old.Worker, Epoch: old.Epoch, Term: "old-term"}, Changes: []TimerChange{{Member: "procedure", Due: time.Now().Add(time.Minute)}}}}); err != nil {
		t.Fatal(err)
	}
	expired := time.Now().Add(-time.Second).UnixMilli()
	if err := c.HSet(ctx, s.workerKey(old.Worker), "expires_ms", expired).Err(); err != nil {
		t.Fatal(err)
	}
	if err := c.ZAdd(ctx, s.ownerExpiryKey(), redis.Z{Score: float64(expired), Member: "w-" + old.Worker}).Err(); err != nil {
		t.Fatal(err)
	}
	if err := c.HSet(ctx, s.coordinatorKey(), "expires_ms", expired).Err(); err != nil {
		t.Fatal(err)
	}
	target.Acquire = false
	if _, err := s.RenewWorker(ctx, target); err != nil {
		t.Fatal(err)
	}
	// The idle lease hash may have expired; the non-reused worker reference
	// and orphan index still fence this exact source.
	if err := c.Del(ctx, s.workerKey(old.Worker)).Err(); err != nil {
		t.Fatal(err)
	}
	client := redis.NewClient(&redis.Options{Addr: c.Options().Addr, MaxRetries: -1})
	defer client.Close()
	client.AddHook(&loseReply{armed: true})
	peer, err := New(client, Config{Root: s.root, Profile: s.profile, RecordBudget: s.maxBytes, PrimaryRunID: s.primary})
	if err != nil {
		t.Fatal(err)
	}
	req := WorkerTransfer{Coordinator: target.Worker, CoordinatorEpoch: target.Epoch, ExpiredWorker: old.Worker, Target: target.Worker, TargetEpoch: target.Epoch, Limit: 1}
	if _, err = peer.TransferExpiredWorker(ctx, req); !errors.Is(err, ErrUnknown) {
		t.Fatal("lost transfer claimed definite outcome", err)
	}
	view, err := s.Read(ctx, id)
	if err != nil || view.Timer.Worker != target.Worker || view.Revision != 2 {
		t.Fatal("committed transfer not observable", view, err)
	}
	checkNamespace(t, s, c, id)
	if _, err = s.TransferExpiredWorker(ctx, req); !errors.Is(err, ErrAbsent) {
		t.Fatal("drained worker not absent", err)
	}
}

func TestCoordinatorElectionConcurrent(t *testing.T) {
	s, c, _ := fixture(t)
	ctx := context.Background()
	keys := []string{s.workerKey("worker1"), s.workerKey("worker2"), s.coordinatorKey(), s.ownerExpiryKey()}
	t.Cleanup(func() {
		if err := c.Del(ctx, keys...).Err(); err != nil {
			t.Error(err)
		}
	})
	peerClient := redis.NewClient(&redis.Options{Addr: c.Options().Addr, MaxRetries: -1})
	defer peerClient.Close()
	peer, err := New(peerClient, Config{Root: s.root, Profile: s.profile, RecordBudget: s.maxBytes, PrimaryRunID: s.primary})
	if err != nil {
		t.Fatal(err)
	}
	requests := []WorkerLeaseRequest{{Worker: "worker1", Epoch: "epoch1", PrimaryRunID: s.primary, Acquire: true}, {Worker: "worker2", Epoch: "epoch2", PrimaryRunID: s.primary, Acquire: true}}
	var results [2]WorkerLease
	var errs [2]error
	var wg sync.WaitGroup
	for i, node := range []*Store{s, peer} {
		wg.Add(1)
		go func(index int, store *Store) {
			defer wg.Done()
			results[index], errs[index] = store.RenewWorker(ctx, requests[index])
		}(i, node)
	}
	wg.Wait()
	if errs[0] != nil || errs[1] != nil || results[0].Coordinator == results[1].Coordinator {
		t.Fatal("coordinator election raced", results, errs)
	}
	for _, req := range requests {
		ttl, e := c.PTTL(ctx, s.workerKey(req.Worker)).Result()
		if e != nil || ttl <= 0 || ttl > WorkerIdleLifetime {
			t.Fatal("worker idle TTL", ttl, e)
		}
	}
}
