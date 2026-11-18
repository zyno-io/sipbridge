package state

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
)

type recoveryStateFixture struct {
	backend Backend
	memory  *MemoryStore
	now     *time.Time
	redis   *Store
}

func recoveryStateContract(t *testing.T, test func(*testing.T, *recoveryStateFixture)) {
	t.Helper()
	t.Run("memory", func(t *testing.T) {
		cfg := contractConfig()
		cfg.Versions = Versions{Schema: "1", Reducer: "1", Timers: "1"}
		cfg.Limits.GroupBytes = 4 << 20
		now := time.Now()
		m, err := newMemory(cfg, func() time.Time { return now })
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { m.Close() })
		test(t, &recoveryStateFixture{backend: m, memory: m, now: &now})
	})
	t.Run("redis", func(t *testing.T) {
		s := testStore(t, func(cfg *Config) {
			cfg.Versions = Versions{Schema: "1", Reducer: "1", Timers: "1"}
			cfg.Limits = contractConfig().Limits
			cfg.Limits.GroupBytes = 4 << 20
		})
		readyStore(t, s)
		test(t, &recoveryStateFixture{backend: s, redis: s})
	})
}
func recoveryHash(value string) string {
	s := sha256.Sum256([]byte(value))
	return hex.EncodeToString(s[:])
}
func recoveryOwner() ApplicationLeaseOwner {
	return ApplicationLeaseOwner{ApplicationLeaseHolder: ApplicationLeaseHolder{PrincipalHash: recoveryHash("application-principal"), HolderID: "controller-a", HolderEpoch: "epoch-a"}, TokenHash: recoveryHash("initial-recovery-secret")}
}
func recoveryRef(lease ApplicationLease) ApplicationLeaseRef {
	return ApplicationLeaseRef{ID: lease.ID, Term: lease.Term, Generation: lease.Generation}
}
func (f *recoveryStateFixture) clockMS() int64 {
	if f.now != nil {
		return f.now.UnixMilli()
	}
	return time.Now().UnixMilli()
}
func (f *recoveryStateFixture) expire(t *testing.T, id string, offset time.Duration) int64 {
	t.Helper()
	if f.memory != nil {
		lease := f.memory.leases[id]
		lease.ExpiresAtMS = f.clockMS() + offset.Milliseconds()
		lease.RecoveryUntilMS = 0
		return lease.ExpiresAtMS
	}
	value, err := f.redis.client.Eval(context.Background(), `
local t=redis.call('TIME');local expiry=tonumber(t[1])*1000+math.floor(tonumber(t[2])/1000)+tonumber(ARGV[1])
redis.call('HSET',KEYS[1],'expiresAtMs',string.format('%.0f',expiry),'recoveryUntilMs','0')
return expiry`, []string{f.redis.applicationLeaseKeys(id)[0]}, offset.Milliseconds()).Int64()
	if err != nil {
		t.Fatal(err)
	}
	return value
}
func (f *recoveryStateFixture) pendingDeadline(t *testing.T, id string, offset time.Duration) int64 {
	t.Helper()
	if f.memory != nil {
		lease := f.memory.leases[id]
		lease.AttachmentUntilMS = f.clockMS() + offset.Milliseconds()
		lease.ExpiresAtMS = lease.AttachmentUntilMS - 1
		lease.RecoveryUntilMS = 0
		return lease.AttachmentUntilMS
	}
	value, err := f.redis.client.Eval(context.Background(), `
local t=redis.call('TIME');local deadline=tonumber(t[1])*1000+math.floor(tonumber(t[2])/1000)+tonumber(ARGV[1])
redis.call('HSET',KEYS[1],'attachmentUntilMs',string.format('%.0f',deadline),'expiresAtMs',string.format('%.0f',deadline-1),'recoveryUntilMs','0')
return deadline`, []string{f.redis.applicationLeaseKeys(id)[0]}, offset.Milliseconds()).Int64()
	if err != nil {
		t.Fatal(err)
	}
	return value
}
func recoveryAttempt(owner ApplicationLeaseOwner, holder string) ApplicationLeaseAttach {
	return ApplicationLeaseAttach{ApplicationLeaseHolder: ApplicationLeaseHolder{PrincipalHash: owner.PrincipalHash, HolderID: holder, HolderEpoch: holder + "-epoch"}, TokenHash: owner.TokenHash, NextTokenHash: recoveryHash(holder + "-rotated-secret"), AttemptID: uuid.NewString()}
}
func (f *recoveryStateFixture) input(id string, create bool) Input {
	now := f.clockMS()
	return Input{ID: id, Kind: "command", Fingerprint: id, Create: create, DeadlineMS: now + 5000, ReceiptUntilMS: now + 10000}
}

func TestRecoverableApplicationLeaseProofDeadlineAndReplay(t *testing.T) {
	recoveryStateContract(t, func(t *testing.T, f *recoveryStateFixture) {
		ctx := context.Background()
		owner := recoveryOwner()
		lease, err := f.backend.AcquireRecoverableApplicationLease(ctx, "recoverable-owner", uuid.NewString(), owner)
		if err != nil || lease.Generation != 1 || !lease.Recoverable || lease.ExpiresAtMS-lease.CheckedAtMS != 5000 || lease.ReattachTimeoutMS != 10000 {
			t.Fatalf("create: %+v %v", lease, err)
		}
		if _, err = f.backend.AcquireRecoverableApplicationLease(ctx, lease.ID, uuid.NewString(), owner); !errors.Is(err, ErrConflict) {
			t.Fatal("asserted holder recreated owner", err)
		}
		for _, fn := range []func(context.Context, string, string) (ApplicationLease, error){f.backend.AcquireApplicationLease, f.backend.RenewApplicationLease, f.backend.ResumeApplicationLease} {
			if _, err = fn(ctx, lease.ID, lease.Term); !errors.Is(err, ErrStale) {
				t.Fatal("legacy generation bypass", err)
			}
		}
		same := ApplicationLeaseAttach{ApplicationLeaseHolder: owner.ApplicationLeaseHolder, TokenHash: owner.TokenHash, NextTokenHash: recoveryHash("same-holder-must-not-rotate"), AttemptID: "same-process"}
		observed, err := f.backend.AttachApplicationLease(ctx, lease.ID, lease.Term, 1, same)
		if err != nil || observed.Generation != 1 || observed.HolderID != owner.HolderID {
			t.Fatalf("multi-worker reconnect churned ownership: %+v %v", observed, err)
		}
		attempt := recoveryAttempt(owner, "controller-b")
		if _, err = f.backend.AttachApplicationLease(ctx, lease.ID, lease.Term, 1, attempt); !errors.Is(err, ErrBusy) {
			t.Fatal("live owner stolen", err)
		}
		expiry := f.expire(t, lease.ID, -2000*time.Millisecond)
		observed, err = f.backend.ReadApplicationLease(ctx, lease.ID)
		if err != nil || observed.RecoveryUntilMS != expiry+10000 {
			t.Fatalf("deadline anchored to read instead of expiry: %+v %v", observed, err)
		}
		wrong := attempt
		wrong.PrincipalHash = recoveryHash("other-principal")
		if _, err = f.backend.AttachApplicationLease(ctx, lease.ID, lease.Term, 1, wrong); !errors.Is(err, ErrStale) {
			t.Fatal("wrong principal admitted", err)
		}
		wrong = attempt
		wrong.TokenHash = recoveryHash("wrong-secret")
		if _, err = f.backend.AttachApplicationLease(ctx, lease.ID, lease.Term, 1, wrong); !errors.Is(err, ErrStale) {
			t.Fatal("wrong token admitted", err)
		}
		reread, err := f.backend.ReadApplicationLease(ctx, lease.ID)
		if err != nil || reread.ExpiresAtMS != expiry || reread.RecoveryUntilMS != observed.RecoveryUntilMS {
			t.Fatal("failed attach/read extended deadline", reread, err)
		}
		attached, err := f.backend.AttachApplicationLease(ctx, lease.ID, lease.Term, 1, attempt)
		if err != nil || attached.Generation != 2 || attached.HolderID != attempt.HolderID || attached.AttachmentUntilMS != expiry+10000 {
			t.Fatalf("claim: %+v %v", attached, err)
		}
		retry, err := f.backend.AttachApplicationLease(ctx, lease.ID, lease.Term, 1, attempt)
		if err != nil || retry.Generation != 2 || retry.ExpiresAtMS != attached.ExpiresAtMS || retry.LastAttachID != attempt.AttemptID || retry.AttachmentUntilMS != attached.AttachmentUntilMS {
			t.Fatalf("lost reply replay changed claim: %+v %v", retry, err)
		}
		if _, err = f.backend.RenewApplicationAttachment(ctx, recoveryRef(lease), owner.ApplicationLeaseHolder); !errors.Is(err, ErrStale) {
			t.Fatal("old generation renewed", err)
		}
		ready, err := f.backend.CompleteApplicationAttachment(ctx, recoveryRef(attached), attempt.ApplicationLeaseHolder)
		if err != nil || ready.AttachmentUntilMS != 0 || ready.Generation != 2 {
			t.Fatal("restoration complete failed", ready, err)
		}
	})
}

func TestRecoverableApplicationLeaseClaimRace(t *testing.T) {
	recoveryStateContract(t, func(t *testing.T, f *recoveryStateFixture) {
		ctx := context.Background()
		owner := recoveryOwner()
		lease, err := f.backend.AcquireRecoverableApplicationLease(ctx, "racing-owner", uuid.NewString(), owner)
		if err != nil {
			t.Fatal(err)
		}
		f.expire(t, lease.ID, -time.Millisecond)
		attempts := []ApplicationLeaseAttach{recoveryAttempt(owner, "candidate-a"), recoveryAttempt(owner, "candidate-b")}
		start := make(chan struct{})
		results := make(chan struct {
			lease ApplicationLease
			err   error
		}, 2)
		var wait sync.WaitGroup
		for _, attempt := range attempts {
			wait.Add(1)
			go func(a ApplicationLeaseAttach) {
				defer wait.Done()
				<-start
				l, e := f.backend.AttachApplicationLease(ctx, lease.ID, lease.Term, 1, a)
				results <- struct {
					lease ApplicationLease
					err   error
				}{l, e}
			}(attempt)
		}
		close(start)
		wait.Wait()
		close(results)
		winners := 0
		for result := range results {
			if result.err == nil {
				winners++
				if result.lease.Generation != 2 {
					t.Fatal("double generation bump", result.lease)
				}
			} else if !errors.Is(result.err, ErrStale) {
				t.Fatal(result.err)
			}
		}
		if winners != 1 {
			t.Fatalf("want one owner, got %d", winners)
		}
	})
}

func TestRecoverableApplicationLeaseExpiredAndPendingDeadlines(t *testing.T) {
	recoveryStateContract(t, func(t *testing.T, f *recoveryStateFixture) {
		ctx := context.Background()
		owner := recoveryOwner()
		lease, err := f.backend.AcquireRecoverableApplicationLease(ctx, "expired-owner", uuid.NewString(), owner)
		if err != nil {
			t.Fatal(err)
		}
		expiry := f.expire(t, lease.ID, -10001*time.Millisecond)
		observed, err := f.backend.ReadApplicationLease(ctx, lease.ID)
		if err != nil || observed.RecoveryUntilMS != expiry+10000 {
			t.Fatal(observed, err)
		}
		if _, err = f.backend.AttachApplicationLease(ctx, lease.ID, lease.Term, 1, recoveryAttempt(owner, "late-holder")); !errors.Is(err, ErrExpired) {
			t.Fatal("deadline revived", err)
		}
		lease, err = f.backend.AcquireRecoverableApplicationLease(ctx, "pending-owner", uuid.NewString(), owner)
		if err != nil {
			t.Fatal(err)
		}
		f.expire(t, lease.ID, -time.Millisecond)
		attempt := recoveryAttempt(owner, "pending-holder")
		attached, err := f.backend.AttachApplicationLease(ctx, lease.ID, lease.Term, 1, attempt)
		if err != nil {
			t.Fatal(err)
		}
		originalDeadline := attached.AttachmentUntilMS
		f.expire(t, lease.ID, -time.Millisecond)
		nextAttempt := recoveryAttempt(owner, "second-pending-holder")
		nextAttempt.TokenHash = attempt.NextTokenHash
		next, err := f.backend.AttachApplicationLease(ctx, lease.ID, lease.Term, attached.Generation, nextAttempt)
		if err != nil || next.Generation != attached.Generation+1 || next.AttachmentUntilMS != originalDeadline {
			t.Fatal("second recovery reset original deadline", next, err)
		}
		attached, attempt = next, nextAttempt
		deadline := f.pendingDeadline(t, lease.ID, 1000*time.Millisecond)
		for n := 0; n < 3; n++ {
			renewed, err := f.backend.RenewApplicationAttachment(ctx, recoveryRef(attached), attempt.ApplicationLeaseHolder)
			if err != nil || renewed.ExpiresAtMS != deadline || renewed.AttachmentUntilMS != deadline {
				t.Fatal("heartbeat extended original restoration deadline", renewed, err)
			}
		}
		f.pendingDeadline(t, lease.ID, -time.Millisecond)
		if _, err = f.backend.RenewApplicationAttachment(ctx, recoveryRef(attached), attempt.ApplicationLeaseHolder); !errors.Is(err, ErrExpired) {
			t.Fatal("pending heartbeat revived", err)
		}
		if _, err = f.backend.CompleteApplicationAttachment(ctx, recoveryRef(attached), attempt.ApplicationLeaseHolder); !errors.Is(err, ErrExpired) {
			t.Fatal("late ready revived", err)
		}
		same := attempt
		same.TokenHash = attempt.NextTokenHash
		same.AttemptID = "expired-reconnect"
		if _, err = f.backend.AttachApplicationLease(ctx, lease.ID, lease.Term, attached.Generation, same); !errors.Is(err, ErrExpired) {
			t.Fatal("same holder extended failed restoration", err)
		}
	})
}

func TestRecoverableApplicationGenerationGuardsAndInventory(t *testing.T) {
	recoveryStateContract(t, func(t *testing.T, f *recoveryStateFixture) {
		ctx := context.Background()
		owner := recoveryOwner()
		lease, err := f.backend.AcquireRecoverableApplicationLease(ctx, "indexed-owner", uuid.NewString(), owner)
		if err != nil {
			t.Fatal(err)
		}
		ref := contractRef(t, f.backend, "indexed-call")
		update := contractRead(t, f.backend, ref, f.input("create-indexed", true))
		payload := Payload{ApplicationLeaseIDs: []string{lease.ID}, ApplicationContexts: []ApplicationContextIndex{{LeaseID: lease.ID, LeaseTerm: lease.Term, ContextID: "dialog-b"}, {LeaseID: lease.ID, LeaseTerm: lease.Term, ContextID: "dialog-a"}}, CommitLeaseGuards: []ApplicationLeaseRef{recoveryRef(lease)}}
		if _, err = f.backend.CompareAndSwap(ctx, update, payload, nil); err != nil {
			t.Fatal(err)
		}
		record, err := f.backend.Load(ctx, ref)
		if err != nil {
			t.Fatal(err)
		}
		ref = record.Ref
		page, err := f.backend.ListApplicationContexts(ctx, recoveryRef(lease), "", 1)
		if err != nil || len(page.Entries) != 1 || page.Entries[0].ContextID != "dialog-a" || page.Entries[0].Ref != ref || page.NextCursor == "" {
			t.Fatalf("first inventory page: %+v %v", page, err)
		}
		last, err := f.backend.ListApplicationContexts(ctx, recoveryRef(lease), page.NextCursor, 1)
		if err != nil || len(last.Entries) != 1 || last.Entries[0].ContextID != "dialog-b" || last.NextCursor != "" {
			t.Fatal(last, err)
		}
		f.expire(t, lease.ID, -time.Millisecond)
		stale := contractRead(t, f.backend, ref, f.input("old-fence", false))
		payload.CommitLeaseGuards = nil
		payload.CommitLeaseFences = []ApplicationLeaseRef{recoveryRef(lease)}
		attempt := recoveryAttempt(owner, "inventory-holder")
		attached, err := f.backend.AttachApplicationLease(ctx, lease.ID, lease.Term, 1, attempt)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = f.backend.CompareAndSwap(ctx, stale, payload, nil); !errors.Is(err, ErrStale) {
			t.Fatal("old generation sealed new owner", err)
		}
		payload.CommitLeaseFences = nil
		for _, generation := range []uint64{0, 1, 2} {
			payload.CommitLeaseGuards = []ApplicationLeaseRef{{ID: lease.ID, Term: lease.Term, Generation: generation}}
			current := contractRead(t, f.backend, ref, f.input(fmt.Sprintf("pending-guard-%d", generation), false))
			if _, err = f.backend.CompareAndSwap(ctx, current, payload, nil); !errors.Is(err, ErrStale) {
				t.Fatal("pending/stale generation mutated", generation, err)
			}
		}
		if _, err = f.backend.ListApplicationContexts(ctx, recoveryRef(lease), "", 64); !errors.Is(err, ErrStale) {
			t.Fatal("old owner listed recovered contexts", err)
		}
		if _, err = f.backend.ListApplicationContexts(ctx, recoveryRef(attached), page.NextCursor, 64); !errors.Is(err, ErrConflict) {
			t.Fatal("cursor from old attachment generation accepted", err)
		}
		list, err := f.backend.ListApplicationContexts(ctx, recoveryRef(attached), "", 64)
		if err != nil || len(list.Entries) != 2 {
			t.Fatal("reattach lost inventory", list, err)
		}
		if _, err = f.backend.CompleteApplicationAttachment(ctx, recoveryRef(attached), attempt.ApplicationLeaseHolder); err != nil {
			t.Fatal(err)
		}
		for _, staleGeneration := range []uint64{0, 1} {
			payload.CommitLeaseGuards = []ApplicationLeaseRef{{ID: lease.ID, Term: lease.Term, Generation: staleGeneration}}
			staleUpdate := contractRead(t, f.backend, ref, f.input(fmt.Sprintf("active-old-guard-%d", staleGeneration), false))
			if _, err = f.backend.CompareAndSwap(ctx, staleUpdate, payload, nil); !errors.Is(err, ErrStale) {
				t.Fatal("old generation mutated restored owner", staleGeneration, err)
			}
		}
		payload.CommitLeaseGuards = []ApplicationLeaseRef{recoveryRef(attached)}
		current := contractRead(t, f.backend, ref, f.input("new-owner-update", false))
		if _, err = f.backend.CompareAndSwap(ctx, current, payload, nil); err != nil {
			t.Fatal("new owner guard refused", err)
		}
	})
}

func TestRecoverableApplicationEffectReservationFencesAndStartedSettlement(t *testing.T) {
	recoveryStateContract(t, func(t *testing.T, f *recoveryStateFixture) {
		ctx := context.Background()
		owner := recoveryOwner()
		lease, err := f.backend.AcquireRecoverableApplicationLease(ctx, "effect-owner", uuid.NewString(), owner)
		if err != nil {
			t.Fatal(err)
		}
		oldRef := recoveryRef(lease)
		pendingRef := contractRef(t, f.backend, "unstarted-application-write")
		pendingPayload := Payload{ApplicationLeaseIDs: []string{lease.ID}, Effects: []Effect{{ID: "unstarted-effect", Kind: "wire", Status: EffectPending, DeadlineMS: f.clockMS() + 20000, ApplicationLease: &oldRef}}}
		if _, err = f.backend.CompareAndSwap(ctx, contractRead(t, f.backend, pendingRef, f.input("create-pending", true)), pendingPayload, nil); err != nil {
			t.Fatal(err)
		}
		physicalRef := contractRef(t, f.backend, "started-application-write")
		physicalPayload := Payload{ApplicationLeaseIDs: []string{lease.ID}, Effects: []Effect{{ID: "started-effect", Kind: "wire", Status: EffectDispatching, AttemptID: "original-write-attempt", DeadlineMS: f.clockMS() + 20000, ApplicationLease: &oldRef}}}
		if _, err = f.backend.CompareAndSwap(ctx, contractRead(t, f.backend, physicalRef, f.input("reserve-started", true)), physicalPayload, nil); err != nil {
			t.Fatal(err)
		}
		f.expire(t, lease.ID, -time.Millisecond)
		attempt := recoveryAttempt(owner, "effect-successor")
		attached, err := f.backend.AttachApplicationLease(ctx, lease.ID, lease.Term, 1, attempt)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = f.backend.CompleteApplicationAttachment(ctx, recoveryRef(attached), attempt.ApplicationLeaseHolder); err != nil {
			t.Fatal(err)
		}
		pendingPayload.Effects[0].Status = EffectDispatching
		pendingPayload.Effects[0].AttemptID = "late-old-generation-write"
		if _, err = f.backend.CompareAndSwap(ctx, contractRead(t, f.backend, pendingRef, f.input("reserve-old-effect", false)), pendingPayload, nil); !errors.Is(err, ErrStale) {
			t.Fatal("old application reservation started after claim", err)
		}
		physicalPayload.Effects[0].Status = EffectWritten
		if _, err = f.backend.CompareAndSwap(ctx, contractRead(t, f.backend, physicalRef, f.input("settle-original-write", false)), physicalPayload, nil); err != nil {
			t.Fatal("known started physical attempt could not settle", err)
		}
		record, err := f.backend.Load(ctx, physicalRef)
		if err != nil || record.Payload.Effects[0].AttemptID != "original-write-attempt" || record.Payload.Effects[0].ApplicationLease.Generation != 1 {
			t.Fatal("physical evidence identity changed", record, err)
		}
	})
}

func TestApplicationContextInventoryQuotaAtomicityAndRetirement(t *testing.T) {
	recoveryStateContract(t, func(t *testing.T, f *recoveryStateFixture) {
		ctx := context.Background()
		owner := recoveryOwner()
		lease, err := f.backend.AcquireRecoverableApplicationLease(ctx, "quota-owner", uuid.NewString(), owner)
		if err != nil {
			t.Fatal(err)
		}
		var refs []Ref
		for group := 0; group < 4; group++ {
			if _, err = f.backend.RenewApplicationAttachment(ctx, recoveryRef(lease), owner.ApplicationLeaseHolder); err != nil {
				t.Fatal(err)
			}
			ref := contractRef(t, f.backend, fmt.Sprintf("index-quota-%d", group))
			payload := Payload{ApplicationLeaseIDs: []string{lease.ID}}
			for n := 0; n < applicationContextGroupLimit; n++ {
				payload.ApplicationContexts = append(payload.ApplicationContexts, ApplicationContextIndex{LeaseID: lease.ID, LeaseTerm: lease.Term, ContextID: fmt.Sprintf("dialog-%04d", n)})
			}
			if _, err = f.backend.CompareAndSwap(ctx, contractRead(t, f.backend, ref, f.input(fmt.Sprintf("index-group-%d", group), true)), payload, nil); err != nil {
				t.Fatal(err)
			}
			record, err := f.backend.Load(ctx, ref)
			if err != nil {
				t.Fatal(err)
			}
			refs = append(refs, record.Ref)
		}
		extra := contractRef(t, f.backend, "extra-index-group")
		extraPayload := Payload{ApplicationLeaseIDs: []string{lease.ID}, ApplicationContexts: []ApplicationContextIndex{{LeaseID: lease.ID, LeaseTerm: lease.Term, ContextID: "extra-context"}}}
		if _, err = f.backend.CompareAndSwap(ctx, contractRead(t, f.backend, extra, f.input("extra-index", true)), extraPayload, nil); !errors.Is(err, ErrCapacity) {
			t.Fatal("owner context quota not enforced", err)
		}
		if _, err = f.backend.Load(ctx, extra); !errors.Is(err, ErrNotFound) {
			t.Fatal("failed index commit allocated group", err)
		}
		cursor := ""
		count := 0
		for {
			if _, err = f.backend.RenewApplicationAttachment(ctx, recoveryRef(lease), owner.ApplicationLeaseHolder); err != nil {
				t.Fatal(err)
			}
			page, err := f.backend.ListApplicationContexts(ctx, recoveryRef(lease), cursor, 64)
			if err != nil {
				t.Fatal(err)
			}
			count += len(page.Entries)
			if page.NextCursor == "" {
				break
			}
			cursor = page.NextCursor
		}
		if count != ApplicationContextLimit {
			t.Fatalf("inventory has %d entries", count)
		}
		record, err := f.backend.Load(ctx, refs[0])
		if err != nil {
			t.Fatal(err)
		}
		terminal := record.Payload
		terminal.Retention.Terminal = true
		if _, err = f.backend.CompareAndSwap(ctx, contractRead(t, f.backend, refs[0], f.input("terminal-indexed-group", false)), terminal, nil); err != nil {
			t.Fatal(err)
		}
		// Move only synthetic receipt/scheduling barriers to permit immediate retirement.
		if f.memory != nil {
			g := f.memory.groups[refs[0].ID]
			g.retire = f.clockMS() - 1
			g.receipts = nil
			g.proof = nil
		} else {
			if err = f.redis.client.HSet(ctx, f.redis.keys(refs[0])[0], "retireAtMs", time.Now().UnixMilli()-1, "receipts", "[]", "proof", "").Err(); err != nil {
				t.Fatal(err)
			}
		}
		if err = f.backend.Retire(ctx, refs[0]); err != nil {
			t.Fatal(err)
		}
		if _, err = f.backend.CompareAndSwap(ctx, contractRead(t, f.backend, extra, f.input("index-after-retirement", true)), extraPayload, nil); err != nil {
			t.Fatal("retirement did not release index quota", err)
		}
		cursor = ""
		count = 0
		for {
			page, err := f.backend.ListApplicationContexts(ctx, recoveryRef(lease), cursor, 64)
			if err != nil {
				t.Fatal(err)
			}
			for _, entry := range page.Entries {
				if entry.Ref.ID == refs[0].ID {
					t.Fatal("retired context still listed")
				}
			}
			count += len(page.Entries)
			if page.NextCursor == "" {
				break
			}
			cursor = page.NextCursor
		}
		if count != ApplicationContextLimit-applicationContextGroupLimit+1 {
			t.Fatal("retirement index count", count)
		}
	})
}

func TestRecoverableExpiredSameHolderReconnectKeepsOriginalDeadline(t *testing.T) {
	recoveryStateContract(t, func(t *testing.T, f *recoveryStateFixture) {
		ctx := context.Background()
		owner := recoveryOwner()
		lease, err := f.backend.AcquireRecoverableApplicationLease(ctx, "same-holder-expired", uuid.NewString(), owner)
		if err != nil {
			t.Fatal(err)
		}
		expiry := f.expire(t, lease.ID, -time.Millisecond)
		same := ApplicationLeaseAttach{ApplicationLeaseHolder: owner.ApplicationLeaseHolder, TokenHash: owner.TokenHash, NextTokenHash: recoveryHash("unused-same-holder-rotation"), AttemptID: "same-holder-reconnect"}
		attached, err := f.backend.AttachApplicationLease(ctx, lease.ID, lease.Term, 1, same)
		if err != nil || attached.Generation != 1 || attached.AttachmentUntilMS != expiry+10000 || attached.LastAttachID != "" {
			t.Fatal("expired same holder escaped restoration phase", attached, err)
		}
		deadline := attached.AttachmentUntilMS
		for n := 0; n < 3; n++ {
			f.expire(t, lease.ID, -time.Millisecond)
			repeated, err := f.backend.AttachApplicationLease(ctx, lease.ID, lease.Term, 1, same)
			if err != nil || repeated.AttachmentUntilMS != deadline || repeated.Generation != 1 {
				t.Fatal("same-holder retry restarted budget", repeated, err)
			}
		}
		f.pendingDeadline(t, lease.ID, -time.Millisecond)
		if _, err = f.backend.AttachApplicationLease(ctx, lease.ID, lease.Term, 1, same); !errors.Is(err, ErrExpired) {
			t.Fatal("expired same holder reset hard deadline", err)
		}
	})
}

func TestRecoverableLostReplyAfterLeaseExpiryRemainsBounded(t *testing.T) {
	recoveryStateContract(t, func(t *testing.T, f *recoveryStateFixture) {
		ctx := context.Background()
		owner := recoveryOwner()
		lease, err := f.backend.AcquireRecoverableApplicationLease(ctx, "lost-reply-owner", uuid.NewString(), owner)
		if err != nil {
			t.Fatal(err)
		}
		f.expire(t, lease.ID, -time.Millisecond)
		attempt := recoveryAttempt(owner, "lost-reply-holder")
		attached, err := f.backend.AttachApplicationLease(ctx, lease.ID, lease.Term, 1, attempt)
		if err != nil {
			t.Fatal(err)
		}
		deadline := attached.AttachmentUntilMS
		f.expire(t, lease.ID, -time.Millisecond)
		retry, err := f.backend.AttachApplicationLease(ctx, lease.ID, lease.Term, 1, attempt)
		if err != nil || retry.Generation != attached.Generation || retry.HolderID != attached.HolderID || retry.AttachmentUntilMS != deadline || retry.ExpiresAtMS <= retry.CheckedAtMS {
			t.Fatal("delayed exact retry lost claim or extended restoration", retry, err)
		}
		if _, err = f.backend.CompleteApplicationAttachment(ctx, recoveryRef(retry), attempt.ApplicationLeaseHolder); err != nil {
			t.Fatal(err)
		}
		f.expire(t, lease.ID, -time.Millisecond)
		if _, err = f.backend.AttachApplicationLease(ctx, lease.ID, lease.Term, 1, attempt); !errors.Is(err, ErrExpired) {
			t.Fatal("completed old-proof replay revived lease", err)
		}
		current := attempt
		current.TokenHash = attempt.NextTokenHash
		current.AttemptID = "current-proof-reconnect"
		expiry := f.expire(t, lease.ID, -time.Millisecond)
		reclaimed, err := f.backend.AttachApplicationLease(ctx, lease.ID, lease.Term, retry.Generation, current)
		if err != nil || reclaimed.Generation != retry.Generation || reclaimed.AttachmentUntilMS != expiry+10000 {
			t.Fatal("current proof could not open bounded same-holder recovery", reclaimed, err)
		}
		f.pendingDeadline(t, lease.ID, -time.Millisecond)
		if _, err = f.backend.AttachApplicationLease(ctx, lease.ID, lease.Term, 1, attempt); !errors.Is(err, ErrExpired) {
			t.Fatal("exact retry crossed restoration deadline", err)
		}
	})
}
