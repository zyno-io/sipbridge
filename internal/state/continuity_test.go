package state

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
)

func pinContinuityLease(t *testing.T, s *Store, id string) (ApplicationLease, Ref) {
	t.Helper()
	lease, err := s.AcquireApplicationLease(context.Background(), id, uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	ref := group(t, s, "lease-"+id)
	step := begin(t, s, ref, "pin", true)
	if _, err = s.Commit(context.Background(), *step, Payload{ApplicationLeaseIDs: []string{id}}, nil); err != nil {
		t.Fatal(err)
	}
	return lease, step.Record.Ref
}

func TestReferencedLeaseSurvivesIdleExpiryAndResumesUntilOwnerLossWins(t *testing.T) {
	s := testStore(t, nil)
	readyStore(t, s)
	ctx := context.Background()
	lease, ref := pinContinuityLease(t, s, "owner")
	key := s.applicationLeaseKeys("owner")[0]
	if ttl := s.client.PTTL(ctx, key).Val(); ttl != -time.Nanosecond {
		t.Fatalf("referenced owner expires: %v", ttl)
	}
	if refs := s.client.HGet(ctx, key, "references").Val(); refs != "1" {
		t.Fatal("wrong reference count", refs)
	}
	expireApplicationLease(t, s, "owner")
	restored, err := s.ResumeApplicationLease(ctx, "owner", lease.Term)
	if err != nil || restored.Term != lease.Term || restored.ExpiresAtMS <= restored.CheckedAtMS {
		t.Fatalf("live owner cannot resume expired retained term: %+v %v", restored, err)
	}
	expireApplicationLease(t, s, "owner")
	step := begin(t, s, ref, "owner-loss", false)
	next := step.Record.Payload
	next.CommitLeaseFences = []ApplicationLeaseRef{{ID: "owner", Term: lease.Term}}
	if _, err = s.Commit(ctx, *step, next, nil); err != nil {
		t.Fatal("owner loss could not seal expired term", err)
	}
	if _, err = s.ResumeApplicationLease(ctx, "owner", lease.Term); !errors.Is(err, ErrExpired) {
		t.Fatal("sealed owner revived", err)
	}
	successor, err := s.AcquireApplicationLease(ctx, "owner", uuid.NewString())
	if err != nil || successor.Term == lease.Term {
		t.Fatal("fresh term not acquired after sealed loss", err)
	}
	if _, err = s.ResumeApplicationLease(ctx, "owner", lease.Term); !errors.Is(err, ErrStale) {
		t.Fatal("old term displaced successor", err)
	}
	release := begin(t, s, ref, "release-owner", false)
	if _, err = s.Commit(ctx, *release, Payload{}, nil); err != nil {
		t.Fatal(err)
	}
	if refs := s.client.HGet(ctx, key, "references").Val(); refs != "0" {
		t.Fatal("last owner reference leaked", refs)
	}
	if ttl := s.client.PTTL(ctx, key).Val(); ttl <= 0 || ttl > applicationLeaseIdleTTL {
		t.Fatal("unreferenced term became immortal", ttl)
	}
}

func TestOwnerResumeWinsAgainstComputedExpiredOwnerLoss(t *testing.T) {
	s := testStore(t, nil)
	readyStore(t, s)
	ctx := context.Background()
	lease, ref := pinContinuityLease(t, s, "owner")
	expireApplicationLease(t, s, "owner")
	step := begin(t, s, ref, "computed-loss", false)
	next := step.Record.Payload
	next.CommitLeaseFences = []ApplicationLeaseRef{{ID: "owner", Term: lease.Term}}
	if _, err := s.ResumeApplicationLease(ctx, "owner", lease.Term); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Commit(ctx, *step, next, nil); !errors.Is(err, ErrStale) {
		t.Fatalf("stale owner-loss computation erased resumed owner: %v", err)
	}
	if sealed := s.client.HGet(ctx, s.applicationLeaseKeys("owner")[0], "sealed").Val(); sealed != "0" {
		t.Fatal("resumed term was sealed", sealed)
	}
}

func TestReferencedLeasePruningMakesProgressBeyondOneBatch(t *testing.T) {
	s := testStore(t, nil)
	readyStore(t, s)
	ctx := context.Background()
	for n := 0; n < applicationLeasePrune+3; n++ {
		_, _ = pinContinuityLease(t, s, fmt.Sprintf("retained-%03d", n))
	}
	for n := 0; n < 7; n++ {
		if _, err := s.AcquireApplicationLease(ctx, fmt.Sprintf("unreferenced-%d", n), uuid.NewString()); err != nil {
			t.Fatal(err)
		}
	}
	// Place more than one prune batch of retained entries before the reclaimable
	// entries. Keeping their expired scores would starve the trailing records.
	for n := 0; n < applicationLeasePrune+3; n++ {
		expireApplicationLease(t, s, fmt.Sprintf("retained-%03d", n))
	}
	for n := 0; n < 7; n++ {
		expireApplicationLease(t, s, fmt.Sprintf("unreferenced-%d", n))
	}
	for n := 0; n < 4; n++ {
		if _, err := s.AcquireApplicationLease(ctx, fmt.Sprintf("pruner-%d", n), uuid.NewString()); err != nil {
			t.Fatal(err)
		}
	}
	for n := 0; n < 7; n++ {
		if exists := s.client.Exists(ctx, s.applicationLeaseKeys(fmt.Sprintf("unreferenced-%d", n))[0]).Val(); exists != 0 {
			t.Fatal("referenced entries starved expired-record pruning", n)
		}
	}
	for n := 0; n < applicationLeasePrune+3; n++ {
		if exists := s.client.Exists(ctx, s.applicationLeaseKeys(fmt.Sprintf("retained-%03d", n))[0]).Val(); exists != 1 {
			t.Fatal("pruning discarded referenced owner", n)
		}
	}
}

func TestDetectedTransportChangeFencesOldComputation(t *testing.T) {
	s := testStore(t, nil)
	readyStore(t, s)
	ctx := context.Background()
	step := begin(t, s, group(t, s, "transport-claim"), "first", true)
	original := s.Authority()
	s.transport.Store(&primaryObservation{runID: s.primaryID()})
	if _, err := s.Commit(ctx, *step, Payload{}, nil); !errors.Is(err, ErrStale) {
		t.Fatal("old-transport computation committed", err)
	}
	if s.Authority() != original {
		t.Fatal("transport change rewrote logical authority")
	}
}

func TestRetiringLastReferencedGroupRestoresLeaseExpiry(t *testing.T) {
	s := testStore(t, nil)
	readyStore(t, s)
	ctx := context.Background()
	_, ref := pinContinuityLease(t, s, "retiring-owner")
	step := begin(t, s, ref, "terminal", false)
	next := step.Record.Payload
	next.Retention = Retention{Terminal: true, UntilMS: time.Now().Add(-time.Second).UnixMilli()}
	if _, err := s.Commit(ctx, *step, next, nil); err != nil {
		t.Fatal(err)
	}
	// Elapse replay retention deterministically without a long timing sleep.
	if err := s.client.HSet(ctx, s.keys(ref)[0], "receipts", "[]", "retireAtMs", time.Now().Add(-time.Second).UnixMilli()).Err(); err != nil {
		t.Fatal(err)
	}
	if err := s.Retire(ctx, ref); err != nil {
		t.Fatal(err)
	}
	key := s.applicationLeaseKeys("retiring-owner")[0]
	if refs := s.client.HGet(ctx, key, "references").Val(); refs != "0" {
		t.Fatal("retirement leaked owner reference", refs)
	}
	if ttl := s.client.PTTL(ctx, key).Val(); ttl <= 0 || ttl > applicationLeaseIdleTTL {
		t.Fatal("retired owner's term remains immortal", ttl)
	}
}

func TestEmptyStateAndObsoleteMarkersDoNotRequireSetup(t *testing.T) {
	s := testStore(t, nil)
	ctx := context.Background()
	if err := s.Ready(ctx); err != nil {
		t.Fatal("empty state unavailable", err)
	}
	if err := s.client.Set(ctx, s.partitionPrefix(0)+"authority", "obsolete", 0).Err(); err != nil {
		t.Fatal(err)
	}
	if err := s.Ready(ctx); err != nil {
		t.Fatal("obsolete marker blocked service", err)
	}
	begin(t, s, group(t, s, "new-call"), "new", true)
}

func TestEmptyStoreAcquiresLeaseWithoutSetupAndDoesNotReviveLostTerm(t *testing.T) {
	s := testStore(t, nil)
	ctx := context.Background()
	lease, err := s.AcquireApplicationLease(ctx, "owner", uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	if err := s.client.Del(ctx, s.applicationLeaseKeys("owner")[0]).Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ResumeApplicationLease(ctx, "owner", lease.Term); !errors.Is(err, ErrExpired) {
		t.Fatal("lost term revived", err)
	}
	replacement, err := s.AcquireApplicationLease(ctx, "owner", uuid.NewString())
	if err != nil || replacement.Term == lease.Term {
		t.Fatal("fresh lease unavailable", err)
	}
}

func TestExpiredLeaseRecoveryDeadlineIsSharedAndCannotBeExtended(t *testing.T) {
	s := testStore(t, nil)
	readyStore(t, s)
	ctx := context.Background()
	lease, ref := pinContinuityLease(t, s, "grace-owner")
	key := s.applicationLeaseKeys(lease.ID)[0]
	// Expire without pre-observing the episode. The first worker must start the
	// shared window; prior elapsed wall time must not consume its opportunity.
	if err := s.client.HSet(ctx, key, "expiresAtMs", time.Now().Add(-time.Minute).UnixMilli(), "recoveryUntilMs", "0").Err(); err != nil {
		t.Fatal(err)
	}
	peer, err := Open(ctx, s.cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer peer.Close()
	first, err := peer.ReadApplicationLease(ctx, lease.ID)
	if err != nil || first.RecoveryUntilMS-first.CheckedAtMS != ApplicationLeaseRecoveryGrace.Milliseconds() {
		t.Fatalf("first expiration did not start a full bounded window: %+v %v", first, err)
	}
	second, err := s.ReadApplicationLease(ctx, lease.ID)
	if err != nil || second.RecoveryUntilMS != first.RecoveryUntilMS {
		t.Fatalf("another reader moved recovery deadline: %+v %v", second, err)
	}
	step := begin(t, peer, ref, "premature-loss", false)
	next := step.Record.Payload
	next.CommitLeaseFences = []ApplicationLeaseRef{{ID: lease.ID, Term: lease.Term}}
	if _, err = peer.Commit(ctx, *step, next, nil); !errors.Is(err, ErrStale) {
		t.Fatal("commit bypassed shared recovery window", err)
	}
	if _, err = s.ResumeApplicationLease(ctx, lease.ID, lease.Term); err != nil {
		t.Fatal(err)
	}
	if deadline := s.client.HGet(ctx, key, "recoveryUntilMs").Val(); deadline != "0" {
		t.Fatal("successful renewal retained old expired episode", deadline)
	}
	if err := s.client.HSet(ctx, key, "expiresAtMs", time.Now().Add(-time.Millisecond).UnixMilli()).Err(); err != nil {
		t.Fatal(err)
	}
	again, err := peer.ReadApplicationLease(ctx, lease.ID)
	if err != nil || again.RecoveryUntilMS-again.CheckedAtMS != ApplicationLeaseRecoveryGrace.Milliseconds() {
		t.Fatalf("later independent outage lost its recovery window: %+v %v", again, err)
	}
}
