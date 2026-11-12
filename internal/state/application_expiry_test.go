package state

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"
)

type applicationExpiryBackend interface {
	ApplicationExpiryHead(context.Context) (ScheduleHead, error)
	ApplicationExpiryPage(context.Context, int) (ApplicationExpiryPage, error)
	AckApplicationExpiryPage(context.Context, ApplicationExpiryPage) error
}

func expiryGroup(t *testing.T, f *recoveryStateFixture, lease ApplicationLease, id string) Ref {
	t.Helper()
	ref := contractRef(t, f.backend, id)
	update := contractRead(t, f.backend, ref, f.input(id, true))
	payload := Payload{ApplicationLeaseIDs: []string{lease.ID},
		ApplicationContexts: []ApplicationContextIndex{{LeaseID: lease.ID, LeaseTerm: lease.Term, ContextID: id}},
		CommitLeaseGuards:   []ApplicationLeaseRef{recoveryRef(lease)}}
	if _, err := f.backend.CompareAndSwap(context.Background(), update, payload, nil); err != nil {
		t.Fatal(err)
	}
	return update.Record.Ref
}

// Move only fixture deadlines, never wait for the 30-second production lease.
func expiryDue(t *testing.T, f *recoveryStateFixture, lease ApplicationLease, graceElapsed bool) {
	t.Helper()
	if f.memory != nil {
		saved := f.memory.leases[lease.ID]
		saved.ExpiresAtMS = f.clockMS() - 2
		saved.RecoveryUntilMS = 0
		if graceElapsed {
			saved.RecoveryUntilMS = f.clockMS() - 1
		}
		f.memory.setApplicationExpiry(lease.ID, f.clockMS()-1)
		return
	}
	_, err := f.redis.client.Eval(context.Background(), `
local t=redis.call('TIME'); local now=tonumber(t[1])*1000+math.floor(tonumber(t[2])/1000)
redis.call('HSET',KEYS[1],'expiresAtMs',string.format('%.0f',now-2),'recoveryUntilMs',ARGV[2]=='1' and string.format('%.0f',now-1) or '0')
redis.call('ZADD',KEYS[2],now-1,ARGV[1]); return 1`, []string{f.redis.applicationLeaseKeys(lease.ID)[0], f.redis.applicationExpiryKeys(lease.ID)[0]}, lease.ID, graceElapsed).Result()
	if err != nil {
		t.Fatal(err)
	}
}

func TestApplicationExpiryHealthyRenewalDoesNotTouchGroups(t *testing.T) {
	recoveryStateContract(t, func(t *testing.T, f *recoveryStateFixture) {
		ctx := context.Background()
		api := f.backend.(applicationExpiryBackend)
		lease, err := f.backend.AcquireApplicationLease(ctx, "expiry-healthy", uuid.NewString())
		if err != nil {
			t.Fatal(err)
		}
		// A lease is scheduled even before its first context is attached.
		head, err := api.ApplicationExpiryHead(ctx)
		if err != nil || head.DueMS != lease.ExpiresAtMS {
			t.Fatalf("initial deadline: %+v %v", head, err)
		}
		ref := expiryGroup(t, f, lease, "healthy-context")
		before, err := f.backend.Load(ctx, ref)
		if err != nil {
			t.Fatal(err)
		}
		for n := 0; n < 4; n++ {
			lease, err = f.backend.RenewApplicationLease(ctx, lease.ID, lease.Term)
			if err != nil {
				t.Fatal(err)
			}
		}
		after, err := f.backend.Load(ctx, ref)
		if err != nil || after.Revision != before.Revision || !bytes.Equal(after.Payload.ProtocolState, before.Payload.ProtocolState) || !reflect.DeepEqual(after.Payload, before.Payload) {
			t.Fatal("healthy renewal rewrote context state", err)
		}
		head, err = api.ApplicationExpiryHead(ctx)
		if err != nil || head.DueMS != lease.ExpiresAtMS {
			t.Fatalf("renewed deadline: %+v %v", head, err)
		}
		page, err := api.ApplicationExpiryPage(ctx, 64)
		if err != nil || page.OwnerID != "" {
			t.Fatal("healthy owner was enumerated", page, err)
		}
	})
}

func TestApplicationExpiryGraceAndResumableKeysetSweep(t *testing.T) {
	recoveryStateContract(t, func(t *testing.T, f *recoveryStateFixture) {
		ctx := context.Background()
		api := f.backend.(applicationExpiryBackend)
		lease, err := f.backend.AcquireApplicationLease(ctx, "expiry-sweep", uuid.NewString())
		if err != nil {
			t.Fatal(err)
		}
		expiryGroup(t, f, lease, "context-a")
		expiryGroup(t, f, lease, "context-b")
		expiryDue(t, f, lease, false)
		page, err := api.ApplicationExpiryPage(ctx, 1)
		if err != nil || page.OwnerID != "" {
			t.Fatal("sweep bypassed fixed recovery grace", page, err)
		}
		observed, err := f.backend.ReadApplicationLease(ctx, lease.ID)
		if err != nil || observed.RecoveryUntilMS <= observed.CheckedAtMS {
			t.Fatal("recovery was not observed", observed, err)
		}
		head, err := api.ApplicationExpiryHead(ctx)
		if err != nil || head.DueMS != observed.RecoveryUntilMS {
			t.Fatal("grace deadline not scheduled", head, err)
		}
		expiryDue(t, f, lease, true)
		page, err = api.ApplicationExpiryPage(ctx, 1)
		if err != nil || len(page.Entries) != 1 || page.Complete {
			t.Fatal("first bounded page", page, err)
		}
		repeat, err := api.ApplicationExpiryPage(ctx, 1)
		if err != nil || !reflect.DeepEqual(page, repeat) {
			t.Fatal("unacknowledged page was consumed", repeat, err)
		}
		if err := api.AckApplicationExpiryPage(ctx, page); err != nil {
			t.Fatal(err)
		}
		// Removal of an earlier member must not make keyset pagination skip the
		// next context, as offset-based pagination would.
		removed := contractRead(t, f.backend, page.Entries[0].Ref, f.input("remove-first-context", false))
		if _, err := f.backend.CompareAndSwap(ctx, removed, Payload{}, nil); err != nil {
			t.Fatal(err)
		}
		if err := api.AckApplicationExpiryPage(ctx, page); !errors.Is(err, ErrStale) {
			t.Fatal("stale page acknowledgement advanced sweep", err)
		}
		next, err := api.ApplicationExpiryPage(ctx, 1)
		if err != nil || len(next.Entries) != 1 || !next.Complete || next.Entries[0] == page.Entries[0] || next.Cursor != page.NextCursor || next.SweepToken != page.SweepToken {
			t.Fatal("resumed keyset page", next, err)
		}
		if _, err := f.backend.ListApplicationContexts(ctx, recoveryRef(lease), "", 1); !errors.Is(err, ErrExpired) {
			t.Fatal("internal sweep weakened public live-owner listing", err)
		}
		if err := api.AckApplicationExpiryPage(ctx, next); err != nil {
			t.Fatal(err)
		}
		head, err = api.ApplicationExpiryHead(ctx)
		if err != nil || head.DueMS != 0 {
			t.Fatal("completed expired owner kept polling", head, err)
		}
	})
}

func TestApplicationExpirySealedOwnerAndEffectOnlyReferencesQuiesce(t *testing.T) {
	recoveryStateContract(t, func(t *testing.T, f *recoveryStateFixture) {
		ctx := context.Background()
		api := f.backend.(applicationExpiryBackend)
		lease, err := f.backend.AcquireApplicationLease(ctx, "expiry-sealed", uuid.NewString())
		if err != nil {
			t.Fatal(err)
		}
		ref := expiryGroup(t, f, lease, "sealed-context")
		expiryDue(t, f, lease, true)
		update := contractRead(t, f.backend, ref, f.input("seal-context", false))
		next := update.Record.Payload
		next.CommitLeaseFences = []ApplicationLeaseRef{recoveryRef(lease)}
		if _, err := f.backend.CompareAndSwap(ctx, update, next, nil); err != nil {
			t.Fatal(err)
		}
		page, err := api.ApplicationExpiryPage(ctx, 64)
		if err != nil || len(page.Entries) != 1 {
			t.Fatal("sealed owner was not enumerated", page, err)
		}
		if err := api.AckApplicationExpiryPage(ctx, page); err != nil {
			t.Fatal(err)
		}
		// A remaining lease reference without a context can retain physical
		// evidence, but must not restart a completed owner sweep.
		update = contractRead(t, f.backend, ref, f.input("retain-effect-owner", false))
		if _, err := f.backend.CompareAndSwap(ctx, update, Payload{ApplicationLeaseIDs: []string{lease.ID}}, nil); err != nil {
			t.Fatal(err)
		}
		head, err := api.ApplicationExpiryHead(ctx)
		if err != nil || head.DueMS != 0 {
			t.Fatal("retained physical owner reference restarted sweep", head, err)
		}
	})
}

// This isolates owner-deadline maintenance; it is not a SIP capacity benchmark.
func BenchmarkApplicationExpiryHealthyOwner(b *testing.B) {
	for _, count := range []int{1, 32, 128} {
		b.Run(fmt.Sprintf("groups-%d", count), func(b *testing.B) {
			ctx := context.Background()
			store, err := NewMemory(contractConfig())
			if err != nil {
				b.Fatal(err)
			}
			defer store.Close()
			lease, err := store.AcquireApplicationLease(ctx, "benchmark-owner", uuid.NewString())
			if err != nil {
				b.Fatal(err)
			}
			for n := 0; n < count; n++ {
				id := fmt.Sprintf("context-%04d", n)
				ref, err := store.GroupForCallID(id)
				if err != nil {
					b.Fatal(err)
				}
				read, err := store.ReadUpdate(ctx, ref, contractInput(id, true))
				if err != nil || read.Update == nil {
					b.Fatal(err)
				}
				payload := Payload{ApplicationLeaseIDs: []string{lease.ID}, ApplicationContexts: []ApplicationContextIndex{{LeaseID: lease.ID, LeaseTerm: lease.Term, ContextID: id}}, CommitLeaseGuards: []ApplicationLeaseRef{recoveryRef(lease)}}
				if _, err := store.CompareAndSwap(ctx, *read.Update, payload, nil); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for n := 0; n < b.N; n++ {
				if _, err := store.RenewApplicationLease(ctx, lease.ID, lease.Term); err != nil {
					b.Fatal(err)
				}
				if _, err := store.ApplicationExpiryHead(ctx); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			for _, group := range store.groups {
				if group.revision != "1" {
					b.Fatal("healthy owner maintenance rewrote group", group.revision)
				}
			}
			b.ReportMetric(0, "group_commits/op")
		})
	}
}

func TestApplicationExpirySuccessorResetsSweepAndRenewalPreservesIt(t *testing.T) {
	recoveryStateContract(t, func(t *testing.T, f *recoveryStateFixture) {
		ctx := context.Background()
		api := f.backend.(applicationExpiryBackend)
		lease, err := f.backend.AcquireApplicationLease(ctx, "expiry-successor", uuid.NewString())
		if err != nil {
			t.Fatal(err)
		}
		expiryGroup(t, f, lease, "old-context")
		expiryDue(t, f, lease, true)
		old, err := api.ApplicationExpiryPage(ctx, 1)
		if err != nil || len(old.Entries) != 1 {
			t.Fatal(old, err)
		}
		successor, err := f.backend.AcquireApplicationLease(ctx, lease.ID, uuid.NewString())
		if err != nil || successor.Term == lease.Term {
			t.Fatal(successor, err)
		}
		if err := api.AckApplicationExpiryPage(ctx, old); !errors.Is(err, ErrStale) {
			t.Fatal("predecessor ack discarded successor sweep", err)
		}
		pending, err := api.ApplicationExpiryPage(ctx, 64)
		if err != nil || len(pending.Entries) != 1 || pending.SweepToken == old.SweepToken {
			t.Fatal("old-term contexts not swept under live successor", pending, err)
		}
		successor, err = f.backend.RenewApplicationLease(ctx, successor.ID, successor.Term)
		if err != nil {
			t.Fatal(err)
		}
		repeat, err := api.ApplicationExpiryPage(ctx, 64)
		if err != nil || !reflect.DeepEqual(pending, repeat) {
			t.Fatal("renewal postponed or replaced predecessor sweep", repeat, err)
		}
		if err := api.AckApplicationExpiryPage(ctx, repeat); err != nil {
			t.Fatal(err)
		}
		head, err := api.ApplicationExpiryHead(ctx)
		if err != nil || head.DueMS != successor.ExpiresAtMS {
			t.Fatal("completed sweep did not restore live deadline", head, err)
		}
	})
}

func TestApplicationExpiryNoContextOwnerCompletesOnce(t *testing.T) {
	recoveryStateContract(t, func(t *testing.T, f *recoveryStateFixture) {
		ctx := context.Background()
		api := f.backend.(applicationExpiryBackend)
		lease, err := f.backend.AcquireApplicationLease(ctx, "expiry-empty", uuid.NewString())
		if err != nil {
			t.Fatal(err)
		}
		expiryDue(t, f, lease, true)
		page, err := api.ApplicationExpiryPage(ctx, 64)
		if err != nil || page.OwnerID == "" || len(page.Entries) != 0 || !page.Complete {
			t.Fatal("empty owner sweep", page, err)
		}
		if err := api.AckApplicationExpiryPage(ctx, page); err != nil {
			t.Fatal(err)
		}
		head, err := api.ApplicationExpiryHead(ctx)
		if err != nil || head.DueMS != 0 {
			t.Fatal("empty owner spins", head, err)
		}
	})
}

func TestApplicationExpiryCompletedAttachmentReconcilesOldGeneration(t *testing.T) {
	recoveryStateContract(t, func(t *testing.T, f *recoveryStateFixture) {
		ctx := context.Background()
		api := f.backend.(applicationExpiryBackend)
		owner := recoveryOwner()
		lease, err := f.backend.AcquireRecoverableApplicationLease(ctx, "expiry-attach", uuid.NewString(), owner)
		if err != nil {
			t.Fatal(err)
		}
		expiryGroup(t, f, lease, "attached-context")
		f.expire(t, lease.ID, -time.Millisecond)
		attempt := recoveryAttempt(owner, "replacement")
		attached, err := f.backend.AttachApplicationLease(ctx, lease.ID, lease.Term, lease.Generation, attempt)
		if err != nil {
			t.Fatal(err)
		}
		completed, err := f.backend.CompleteApplicationAttachment(ctx, recoveryRef(attached), attempt.ApplicationLeaseHolder)
		if err != nil || completed.AttachmentUntilMS != 0 {
			t.Fatal(completed, err)
		}
		page, err := api.ApplicationExpiryPage(ctx, 64)
		if err != nil || len(page.Entries) != 1 {
			t.Fatal("ready reattachment did not trigger old-generation sweep", page, err)
		}
		if err := api.AckApplicationExpiryPage(ctx, page); err != nil {
			t.Fatal(err)
		}
		head, err := api.ApplicationExpiryHead(ctx)
		if err != nil || head.DueMS != completed.ExpiresAtMS {
			t.Fatal(head, err)
		}
	})
}

func TestApplicationExpiryRenewedOwnerCanExpireAgainDuringSweep(t *testing.T) {
	recoveryStateContract(t, func(t *testing.T, f *recoveryStateFixture) {
		ctx := context.Background()
		api := f.backend.(applicationExpiryBackend)
		lease, err := f.backend.AcquireApplicationLease(ctx, "expiry-second-episode", uuid.NewString())
		if err != nil {
			t.Fatal(err)
		}
		expiryGroup(t, f, lease, "resumed-context")
		expiryDue(t, f, lease, true)
		page, err := api.ApplicationExpiryPage(ctx, 64)
		if err != nil || len(page.Entries) != 1 {
			t.Fatal(page, err)
		}
		lease, err = f.backend.ResumeApplicationLease(ctx, lease.ID, lease.Term)
		if err != nil {
			t.Fatal(err)
		}
		// Model the original sweep taking longer than the renewed lease. The
		// caller has reconciled its groups while the owner was healthy.
		if f.memory != nil {
			f.memory.leases[lease.ID].expiry.started = f.clockMS() - 60000
		} else {
			_, err := f.redis.client.HSet(ctx, f.redis.applicationExpiryKeys(lease.ID)[1], "started", time.Now().Add(-time.Minute).UnixMilli()).Result()
			if err != nil {
				t.Fatal(err)
			}
		}
		expiryDue(t, f, lease, false)
		if err := api.AckApplicationExpiryPage(ctx, page); err != nil {
			t.Fatal(err)
		}
		head, err := api.ApplicationExpiryHead(ctx)
		if err != nil || head.DueMS == 0 || head.After > 0 {
			t.Fatal("final ack swallowed the new expiry episode", head, err)
		}
		next, err := api.ApplicationExpiryPage(ctx, 64)
		if err != nil || next.OwnerID != "" {
			t.Fatal("new episode skipped its recovery grace", next, err)
		}
		observed, err := f.backend.ReadApplicationLease(ctx, lease.ID)
		if err != nil || observed.RecoveryUntilMS <= observed.CheckedAtMS {
			t.Fatal("new recovery episode not scheduled", observed, err)
		}
	})
}
