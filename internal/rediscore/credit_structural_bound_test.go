package rediscore

import (
	"bytes"
	"context"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

// structuralBoundFixture owns only the exact keys below. The deliberately
// long root and identities exercise producer maxima, not a new runtime limit.
func structuralBoundFixture(t *testing.T) (*Store, *redis.Client, Identity, TimerAssignment) {
	t.Helper()
	address := os.Getenv("SIPBRIDGE_CORE_TEST_REDIS")
	if address == "" {
		t.Skip("isolated qualification Redis required")
	}
	c := redis.NewClient(&redis.Options{Addr: address, MaxRetries: -1})
	info, err := c.Info(context.Background(), "server").Result()
	if err != nil {
		t.Fatal(err)
	}
	primary := ""
	for _, line := range strings.Split(info, "\r\n") {
		if strings.HasPrefix(line, "run_id:") {
			primary = strings.TrimPrefix(line, "run_id:")
		}
	}
	prefix := "sipstate:test:shape-" + uuid.NewString() + ":"
	root := prefix + strings.Repeat("r", 512-len(prefix)-1) + ":"
	s, err := New(c, Config{Root: root, Profile: []byte{1, 2, 3}, RecordBudget: 16 << 20, PrimaryRunID: primary})
	if err != nil {
		t.Fatal(err)
	}
	id := Identity{Context: strings.Repeat("c", 96), Incarnation: strings.Repeat("i", 96)}
	assignment := TimerAssignment{Worker: uuid.NewString(), Epoch: strings.Repeat("e", 96), Term: strings.Repeat("t", 96)}
	if err = c.Set(context.Background(), s.profileKey(), s.profile, 0).Err(); err != nil {
		t.Fatal(err)
	}
	req := WorkerLeaseRequest{Worker: assignment.Worker, Epoch: assignment.Epoch, PrimaryRunID: primary, Acquire: true}
	if _, err = s.RenewWorker(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		keys := []string{s.profileKey(), s.capacityKey(), s.selectorKey(), s.contextKey(id.Context), s.contextDeadlineKey(id.Context), s.workerKey(assignment.Worker), s.workerTimerKey(assignment.Worker), s.workerReadyKey(assignment.Worker), s.coordinatorKey(), s.ownerExpiryKey()}
		if err := c.Del(context.Background(), keys...).Err(); err != nil {
			t.Error(err)
		}
		if err := c.Close(); err != nil {
			t.Error(err)
		}
	})
	return s, c, id, assignment
}
func structuralBytes(t *testing.T, c *redis.Client, s *Store) int64 {
	t.Helper()
	n, err := strconv.ParseInt(c.HGet(context.Background(), s.capacityKey(), "bytes").Val(), 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	return n
}
func shapeField(letter string) string { return "r:" + strings.Repeat(letter, 192) }

func TestNotifyCreditMaximumProducerStructuralAllowance(t *testing.T) {
	s, c, id, assignment := structuralBoundFixture(t)
	ctx := context.Background()
	deadline := time.UnixMilli(maxExactInteger)
	lease, err := s.AcquireApplication(ctx, strings.Repeat("a", 96), strings.Repeat("b", 96))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		keys := []string{s.applicationKey(lease.Application), s.applicationOwnerKey(lease.Term), s.applicationOwnedKey(lease.Term), s.applicationAdmissionKey()}
		if err := c.Del(context.Background(), keys...).Err(); err != nil {
			t.Error(err)
		}
	})
	id.Owner, id.Term = lease.Application, lease.Term
	procedure, dep2, dep3 := shapeField("p"), shapeField("q"), shapeField("r")
	if _, err := s.Commit(ctx, Publication{Identity: id, Create: true, Deadline: deadline, Application: &lease.ApplicationLeaseRef, Changes: []Change{{Field: procedure, Value: []byte{1}}, {Field: dep3, Value: []byte{1}}}}); err != nil {
		t.Fatal(err)
	}
	if err := c.HSet(ctx, s.contextKey(id.Context), "revision", "9").Err(); err != nil {
		t.Fatal(err)
	}
	before := structuralBytes(t, c, s)
	packetField, metadataField := shapeField("m"), shapeField("o")
	packet := bytes.Repeat([]byte{1}, 65606)
	metadata := bytes.Repeat([]byte{2}, 8139)
	procedurePayload := bytes.Repeat([]byte{3}, 2048)
	resultPayload := bytes.Repeat([]byte{4}, 2048)
	deliveryTag := [32]byte{3}
	out := OutputIntent{ID: strings.Repeat("a", 66), Attempt: strings.Repeat("b", 96), Worker: assignment.Worker, Epoch: assignment.Epoch, Connection: strings.Repeat("f", 96), MessageField: packetField, MetadataField: metadataField, Dependencies: []string{procedure, dep2, dep3}, WireHash: [32]byte{1}, ProofHash: [32]byte{2}, Application: &lease.ApplicationLeaseRef, DeliveryTag: deliveryTag, Deadline: deadline}
	timers := []TimerChange{{Member: strings.Repeat("j", 192), Due: deadline}, {Member: strings.Repeat("k", 192), Due: deadline}, {Member: strings.Repeat("l", 192), Due: deadline}}
	selectors := []SelectorChange{{Selector: Selector{Kind: SelectorClientTransaction, Digest: [32]byte{1}}, Record: procedure}, {Selector: Selector{Kind: SelectorDialog, Digest: [32]byte{2}}, Record: procedure}}
	const virtualCredit = 10 << 20
	_, err = s.Commit(ctx, Publication{Identity: id, Revision: 9, Deadline: deadline, CreditDelta: virtualCredit, Application: &lease.ApplicationLeaseRef, Changes: []Change{{Field: procedure, Value: procedurePayload}, {Field: dep2, Value: resultPayload}, {Field: packetField, Value: packet}, {Field: metadataField, Value: metadata}, {Field: "r:delivery-tag", Value: deliveryTag[:]}}, NewOutputs: []OutputIntent{out}, Timers: &TimerMutation{Target: assignment, Changes: timers}, Selectors: selectors})
	if err != nil {
		t.Fatal(err)
	}
	delta := structuralBytes(t, c, s) - before
	structural := delta - virtualCredit - int64(len(packet)+len(packetField)) - int64(len(procedurePayload)-1) - int64(len(resultPayload)+len(dep2)) - int64(len(metadata))
	if structural < 0 || structural+int64(len(metadata)) > 16<<10 {
		t.Fatalf("output structural=%d metadata=%d total=%d exceeds 16 KiB", structural, len(metadata), structural+int64(len(metadata)))
	}
	t.Logf("logical delta=%d virtual_credit=%d packet_and_field=%d procedure_delta=%d result_and_field=%d metadata=%d structural_excluding_metadata=%d structural_including_metadata=%d", delta, virtualCredit, len(packet)+len(packetField), len(procedurePayload)-1, len(resultPayload)+len(dep2), len(metadata), structural, structural+int64(len(metadata)))
}
func TestSubscriptionLifecycleMaximumProducerIndexAllowance(t *testing.T) {
	s, c, id, assignment := structuralBoundFixture(t)
	ctx := context.Background()
	deadline := time.UnixMilli(maxExactInteger)
	if _, err := s.Commit(ctx, Publication{Identity: id, Create: true, Deadline: deadline}); err != nil {
		t.Fatal(err)
	}
	if err := c.HSet(ctx, s.contextKey(id.Context), "revision", "9").Err(); err != nil {
		t.Fatal(err)
	}
	before := structuralBytes(t, c, s)
	first, second := shapeField("x"), shapeField("y")
	payload := bytes.Repeat([]byte{1}, 4096)
	timers := []TimerChange{{Member: strings.Repeat("u", 192), Due: deadline}, {Member: strings.Repeat("v", 192), Due: deadline}}
	const virtualCredit = 10 << 20
	_, err := s.Commit(ctx, Publication{Identity: id, Revision: 9, Deadline: deadline, CreditDelta: virtualCredit, Changes: []Change{{Field: first, Value: payload}, {Field: second, Value: payload}}, Timers: &TimerMutation{Target: assignment, Changes: timers}})
	if err != nil {
		t.Fatal(err)
	}
	delta := structuralBytes(t, c, s) - before
	allowance := delta - virtualCredit - int64(2*len(payload)+len(first)+len(second))
	if allowance < 0 || allowance > 4096 {
		t.Fatalf("lifecycle index/envelope overhead %d exceeds 4 KiB", allowance)
	}
	t.Logf("logical delta=%d virtual_credit=%d lifecycle_payloads=%d separately_budgeted_field_names=%d index_envelope=%d", delta, virtualCredit, 2*len(payload), len(first)+len(second), allowance)
}
