package rediscore

import (
	"bytes"
	"context"
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestCreditAdmissionSpendRefillAndClose(t *testing.T) {
	s, c, id := fixture(t)
	ctx := context.Background()
	deadline := time.Now().Add(time.Minute)
	revision, err := s.Commit(ctx, Publication{Identity: id, Create: true, Deadline: deadline, CreditDelta: 1000})
	if err != nil {
		t.Fatal(err)
	}
	if got := c.HGet(ctx, s.contextKey(id.Context), "reserved").Val(); got != "1000" {
		t.Fatal("admission credit", got)
	}
	revision, err = s.Commit(ctx, Publication{Identity: id, Revision: revision, Deadline: deadline, SpendReservation: true, Changes: []Change{{Field: "r:body", Value: bytes.Repeat([]byte{'x'}, 200)}}})
	if err != nil {
		t.Fatal(err)
	}
	before, err := strconv.ParseInt(c.HGet(ctx, s.contextKey(id.Context), "reserved").Val(), 10, 64)
	if err != nil || before >= 1000 {
		t.Fatal("spend", before, err)
	}
	checkNamespace(t, s, c, id)
	revision, err = s.Commit(ctx, Publication{Identity: id, Revision: revision, Deadline: deadline, Changes: []Change{{Field: "r:body", Delete: true}}})
	if err != nil {
		t.Fatal(err)
	}
	after, err := strconv.ParseInt(c.HGet(ctx, s.contextKey(id.Context), "reserved").Val(), 10, 64)
	if err != nil || after <= before || after > 1000 {
		t.Fatal("refill", after, err)
	}
	checkNamespace(t, s, c, id)
	revision, err = s.Commit(ctx, Publication{Identity: id, Revision: revision, Deadline: deadline, CreditDelta: -1000})
	if err != nil {
		t.Fatal(err)
	}
	if v := c.HGet(ctx, s.contextKey(id.Context), "reserved").Val(); v != "0" {
		t.Fatal("closed credit retained", v)
	}
	if n := c.HExists(ctx, s.contextKey(id.Context), "credit_limit").Val(); n {
		t.Fatal("closed ceiling retained")
	}
	if _, err = s.Commit(ctx, Publication{Identity: id, Revision: revision, Deadline: deadline, Changes: []Change{{Field: "r:other", Value: []byte("x")}}}); err != nil {
		t.Fatal(err)
	}
	checkNamespace(t, s, c, id)
}
func TestCreditCompoundOldResponseAndNewNotify(t *testing.T) {
	s, c, id := fixture(t)
	ctx := context.Background()
	deadline := time.Now().Add(time.Minute)
	revision, err := s.Commit(ctx, Publication{Identity: id, Create: true, Deadline: deadline, CreditDelta: 1000})
	if err != nil {
		t.Fatal(err)
	}
	before, credit := retainedWithoutCredit(t, s, id)
	capacity, err := c.HGet(ctx, s.capacityKey(), "bytes").Int64()
	if err != nil {
		t.Fatal(err)
	}
	s.namespaceBytes = capacity + 24
	revision, err = s.Commit(ctx, Publication{Identity: id, Revision: revision, Deadline: deadline, CreditDelta: 20, SpendReservation: true, Changes: []Change{{Field: "r:server-response", Value: bytes.Repeat([]byte{'x'}, 100)}}})
	if err != nil {
		t.Fatal("compound publication could not use old server completion credit", err)
	}
	after, remaining := retainedWithoutCredit(t, s, id)
	growth := after - before
	if revision != 2 || growth <= 20 || remaining != credit+20-growth {
		t.Fatal("incorrect combined accounting", revision, growth, credit, remaining)
	}
	checkNamespace(t, s, c, id)
}

func TestCreditFreshAdmissionDoesNotSpendExistingObligation(t *testing.T) {
	s, c, id := fixture(t)
	ctx := context.Background()
	deadline := time.Now().Add(time.Minute)
	revision, err := s.Commit(ctx, Publication{Identity: id, Create: true, Deadline: deadline, CreditDelta: 1000})
	if err != nil {
		t.Fatal(err)
	}
	capacity, err := c.HGet(ctx, s.capacityKey(), "bytes").Int64()
	if err != nil {
		t.Fatal(err)
	}
	s.namespaceBytes = capacity
	p := Publication{Identity: id, Revision: revision, Deadline: deadline, CreditDelta: 20, Changes: []Change{{Field: "r:new-business", Value: bytes.Repeat([]byte{'x'}, 100)}}}
	if _, err = s.Commit(ctx, p); !errors.Is(err, ErrCapacity) {
		t.Fatal("fresh admission consumed old credit at full namespace", err)
	}
	h, err := c.HMGet(ctx, s.contextKey(id.Context), "revision", "reserved", "credit_limit", "r:new-business").Result()
	if err != nil || h[0] != "1" || h[1] != "1000" || h[2] != "1000" || h[3] != nil {
		t.Fatal("refusal mutated state", h, err)
	}
	s.namespaceBytes = capacity + 1000
	if _, err = s.Commit(ctx, p); err != nil {
		t.Fatal("funded fresh admission rejected", err)
	}
	remaining, err := c.HGet(ctx, s.contextKey(id.Context), "reserved").Int64()
	if err != nil || remaining != 1020 {
		t.Fatal("fresh admission spent old reserve", remaining, err)
	}
	checkNamespace(t, s, c, id)
}

func TestCreditPhysicalReleaseAtFullNamespaceAndNoLateRevival(t *testing.T) {
	s, c, id, out, wire := outputFixture(t)
	ctx := context.Background()
	deadline := time.Now().Add(time.Minute)
	second := out
	second.ID = "output2"
	second.Attempt = "attempt2"
	revision, err := s.Commit(ctx, Publication{Identity: id, Create: true, Deadline: deadline, CreditDelta: 20000, Changes: []Change{{Field: out.MessageField, Value: wire}, {Field: out.MetadataField, Value: []byte("metadata")}}, NewOutputs: []OutputIntent{out, second}})
	if err != nil {
		t.Fatal(err)
	}
	revision, err = s.Commit(ctx, Publication{Identity: id, Revision: revision, Deadline: deadline, SpendReservation: true, Changes: []Change{{Field: "r:extra", Value: bytes.Repeat([]byte{'x'}, 200)}}})
	if err != nil {
		t.Fatal(err)
	}
	before, err := strconv.ParseInt(c.HGet(ctx, s.contextKey(id.Context), "reserved").Val(), 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	full, err := strconv.ParseInt(c.HGet(ctx, s.capacityKey(), "bytes").Val(), 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	s.namespaceBytes = full
	ref := outputRef(id, out)
	if _, err = s.ReserveOutput(ctx, ref); err != nil {
		t.Fatal(err)
	}
	after, err := strconv.ParseInt(c.HGet(ctx, s.contextKey(id.Context), "reserved").Val(), 10, 64)
	if err != nil || after <= before {
		t.Fatal("ready release did not replenish", after, before, err)
	}
	if err = s.CompleteOutput(ctx, ref, "WRITTEN"); err != nil {
		t.Fatal(err)
	}
	if err = s.SettleOutput(ctx, ref, true, true); err != nil {
		t.Fatal(err)
	}
	if got, err := strconv.ParseInt(c.HGet(ctx, s.capacityKey(), "bytes").Val(), 10, 64); err != nil || got > full {
		t.Fatal("recycling increased charged bytes at full namespace", got, full, err)
	}
	checkNamespace(t, s, c, id)
	revision, err = s.Commit(ctx, Publication{Identity: id, Revision: revision, Deadline: deadline, CreditDelta: -20000})
	if err != nil {
		t.Fatal(err)
	}
	secondRef := outputRef(id, second)
	if _, err = s.ReserveOutput(ctx, secondRef); err != nil {
		t.Fatal(err)
	}
	if err = s.CompleteOutput(ctx, secondRef, "WRITTEN"); err != nil {
		t.Fatal(err)
	}
	if err = s.SettleOutput(ctx, secondRef, true, true); err != nil {
		t.Fatal(err)
	}
	if got := c.HGet(ctx, s.contextKey(id.Context), "reserved").Val(); got != "0" {
		t.Fatal("late proof revived closed credit", got)
	}
	checkNamespace(t, s, c, id)
}

func TestCreditRefundAcrossDecimalBoundaryAtFullCapacity(t *testing.T) {
	s, c, id := fixture(t)
	ctx := context.Background()
	deadline := time.Now().Add(time.Minute)
	rev, err := s.Commit(ctx, Publication{Identity: id, Create: true, Deadline: deadline, CreditDelta: 100})
	if err != nil {
		t.Fatal(err)
	}
	rev, err = s.Commit(ctx, Publication{Identity: id, Revision: rev, Deadline: deadline, SpendReservation: true, Changes: []Change{{Field: "r:x", Value: bytes.Repeat([]byte{'x'}, 87)}}})
	if err != nil {
		t.Fatal(err)
	}
	if got := c.HGet(ctx, s.contextKey(id.Context), "reserved").Val(); got != "9" {
		t.Fatal("fixture did not cross expected digit boundary", got)
	}
	full, err := strconv.ParseInt(c.HGet(ctx, s.capacityKey(), "bytes").Val(), 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	s.namespaceBytes = full
	if _, err = s.Commit(ctx, Publication{Identity: id, Revision: rev, Deadline: deadline, Changes: []Change{{Field: "r:x", Delete: true}}}); err != nil {
		t.Fatal("full namespace prevented refill", err)
	}
	got, err := strconv.ParseInt(c.HGet(ctx, s.contextKey(id.Context), "reserved").Val(), 10, 64)
	if err != nil || got != 99 {
		t.Fatal("incorrect bounded refill", got, err)
	}
	used, err := strconv.ParseInt(c.HGet(ctx, s.capacityKey(), "bytes").Val(), 10, 64)
	if err != nil || used > full {
		t.Fatal("refund increased namespace", used, full, err)
	}
	checkNamespace(t, s, c, id)
}

func TestCreditTransferRecyclesActualAssignmentShrink(t *testing.T) { testCreditTransfer(t, true) }
func TestCreditTransferSpendsActualAssignmentGrowth(t *testing.T)   { testCreditTransfer(t, false) }
func testCreditTransfer(t *testing.T, shrink bool) {
	t.Helper()
	s, c, id := fixture(t)
	ctx := context.Background()
	deadline := time.Now().Add(time.Minute)
	oldEpoch, targetEpoch := "old-epoch-very-long", "new"
	if !shrink {
		oldEpoch, targetEpoch = "old", "new-epoch-very-long"
	}
	old := WorkerLeaseRequest{Worker: "worker1", Epoch: oldEpoch, PrimaryRunID: s.primary, Acquire: true}
	target := WorkerLeaseRequest{Worker: "worker2", Epoch: targetEpoch, PrimaryRunID: s.primary, Acquire: true}
	if _, err := s.RenewWorker(ctx, old); err != nil {
		t.Fatal(err)
	}
	if _, err := s.RenewWorker(ctx, target); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if e := c.Del(ctx, s.workerKey(old.Worker), s.workerKey(target.Worker), s.workerTimerKey(old.Worker), s.workerTimerKey(target.Worker), s.ownerExpiryKey(), s.coordinatorKey(), s.contextDeadlineKey(id.Context)).Err(); e != nil {
			t.Error(e)
		}
	})
	assignment := TimerAssignment{Worker: old.Worker, Epoch: old.Epoch, Term: old.Epoch}
	rev, err := s.Commit(ctx, Publication{Identity: id, Create: true, Deadline: deadline, CreditDelta: 1000, Timers: &TimerMutation{Target: assignment, Changes: []TimerChange{{Member: "duty", Due: deadline}}}})
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.Commit(ctx, Publication{Identity: id, Revision: rev, Deadline: deadline, SpendReservation: true, Changes: []Change{{Field: "r:body", Value: bytes.Repeat([]byte{'x'}, 200)}}})
	if err != nil {
		t.Fatal(err)
	}
	before, err := strconv.ParseInt(c.HGet(ctx, s.contextKey(id.Context), "reserved").Val(), 10, 64)
	if err != nil {
		t.Fatal(err)
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
	if _, err = s.RenewWorker(ctx, target); err != nil {
		t.Fatal(err)
	}
	full, err := c.HGet(ctx, s.capacityKey(), "bytes").Int64()
	if err != nil {
		t.Fatal(err)
	}
	s.namespaceBytes = full
	result, err := s.TransferExpiredWorker(ctx, WorkerTransfer{Coordinator: target.Worker, CoordinatorEpoch: target.Epoch, ExpiredWorker: old.Worker, Target: target.Worker, TargetEpoch: target.Epoch, Limit: 1})
	if err != nil || result.Moved != 1 {
		t.Fatal("transfer", result, err)
	}
	after, err := c.HGet(ctx, s.contextKey(id.Context), "reserved").Int64()
	if err != nil || shrink && after <= before || !shrink && after >= before {
		t.Fatal("assignment delta was not recycled or spent", before, after, err)
	}
	checkNamespace(t, s, c, id)
}

func TestCreditConcurrentAdmissionAndPhysicalRelease(t *testing.T) {
	s, c, id, out, wire := outputFixture(t)
	ctx := context.Background()
	deadline := time.Now().Add(time.Minute)
	rev, err := s.Commit(ctx, Publication{Identity: id, Create: true, Deadline: deadline, CreditDelta: 10000, Changes: []Change{{Field: out.MessageField, Value: wire}, {Field: out.MetadataField, Value: []byte("metadata")}}, NewOutputs: []OutputIntent{out}})
	if err != nil {
		t.Fatal(err)
	}
	rev, err = s.Commit(ctx, Publication{Identity: id, Revision: rev, Deadline: deadline, SpendReservation: true, Changes: []Change{{Field: "r:extra", Value: bytes.Repeat([]byte{'x'}, 200)}}})
	if err != nil {
		t.Fatal(err)
	}
	before, err := c.HGet(ctx, s.contextKey(id.Context), "reserved").Int64()
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	var errs [2]error
	wg.Add(2)
	go func() { defer wg.Done(); _, errs[0] = s.ReserveOutput(ctx, outputRef(id, out)) }()
	go func() {
		defer wg.Done()
		_, errs[1] = s.Commit(ctx, Publication{Identity: id, Revision: rev, Deadline: deadline, CreditDelta: 1000})
	}()
	wg.Wait()
	if errs[0] != nil || errs[1] != nil {
		t.Fatal("concurrent operations", errs)
	}
	limit, err := c.HGet(ctx, s.contextKey(id.Context), "credit_limit").Int64()
	if err != nil || limit != 11000 {
		t.Fatal("wrong combined ceiling", limit, err)
	}
	remaining, err := c.HGet(ctx, s.contextKey(id.Context), "reserved").Int64()
	if err != nil || remaining <= before+1000 || remaining > limit {
		t.Fatal("release was overwritten by admission", before, remaining, err)
	}
	checkNamespace(t, s, c, id)
}

func TestCreditPartialClosureClampsAndRejectsUnderflow(t *testing.T) {
	s, c, id := fixture(t)
	ctx := context.Background()
	deadline := time.Now().Add(time.Minute)
	rev, err := s.Commit(ctx, Publication{Identity: id, Create: true, Deadline: deadline, CreditDelta: 1000})
	if err != nil {
		t.Fatal(err)
	}
	rev, err = s.Commit(ctx, Publication{Identity: id, Revision: rev, Deadline: deadline, CreditDelta: 500})
	if err != nil {
		t.Fatal(err)
	}
	rev, err = s.Commit(ctx, Publication{Identity: id, Revision: rev, Deadline: deadline, SpendReservation: true, Changes: []Change{{Field: "r:body", Value: bytes.Repeat([]byte{'x'}, 700)}}})
	if err != nil {
		t.Fatal(err)
	}
	before, err := c.HGet(ctx, s.contextKey(id.Context), "reserved").Int64()
	if err != nil {
		t.Fatal(err)
	}
	rev, err = s.Commit(ctx, Publication{Identity: id, Revision: rev, Deadline: deadline, CreditDelta: -500})
	if err != nil {
		t.Fatal(err)
	}
	limit, err := c.HGet(ctx, s.contextKey(id.Context), "credit_limit").Int64()
	if err != nil || limit != 1000 {
		t.Fatal("partial ceiling", limit, err)
	}
	after, err := c.HGet(ctx, s.contextKey(id.Context), "reserved").Int64()
	if err != nil || after < before || after > limit {
		t.Fatal("closure did not preserve remaining obligations", before, after, err)
	}
	if _, err = s.Commit(ctx, Publication{Identity: id, Revision: rev, Deadline: deadline, CreditDelta: -1001}); !errors.Is(err, ErrConflict) {
		t.Fatal("overclosure accepted", err)
	}
	checkNamespace(t, s, c, id)
}

func TestCreditIndexReleaseRespectsContextBudget(t *testing.T) {
	s, c, id, out, wire := outputFixture(t)
	ctx := context.Background()
	deadline := time.Now().Add(time.Minute)
	rev, err := s.Commit(ctx, Publication{Identity: id, Create: true, Deadline: deadline, CreditDelta: 20000, Changes: []Change{{Field: out.MessageField, Value: wire}, {Field: out.MetadataField, Value: []byte("metadata")}}, NewOutputs: []OutputIntent{out}})
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.Commit(ctx, Publication{Identity: id, Revision: rev, Deadline: deadline, SpendReservation: true, Changes: []Change{{Field: "r:extra", Value: bytes.Repeat([]byte{'x'}, 200)}}})
	if err != nil {
		t.Fatal(err)
	}
	current, err := c.HMGet(ctx, s.contextKey(id.Context), "bytes", "reserved").Result()
	if err != nil {
		t.Fatal(err)
	}
	record, err := strconv.ParseInt(current[0].(string), 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	reserved, err := strconv.ParseInt(current[1].(string), 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	s.maxBytes = record + reserved
	if _, err = s.ReserveOutput(ctx, outputRef(id, out)); err != nil {
		t.Fatal(err)
	}
	after, err := c.HGet(ctx, s.contextKey(id.Context), "reserved").Int64()
	if err != nil || after != reserved {
		t.Fatal("index refund exceeded record budget", reserved, after, err)
	}
	checkNamespace(t, s, c, id)
}
