package state

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

func expireApplicationLease(t *testing.T, s *Store, id string) {
	t.Helper()
	keys := s.applicationLeaseKeys(id)
	if err := s.client.Eval(context.Background(), `
local clock=redis.call('TIME'); local now=tonumber(clock[1])*1000+math.floor(tonumber(clock[2])/1000)
redis.call('HSET',KEYS[1],'expiresAtMs',string.format('%.0f',now),'recoveryUntilMs',string.format('%.0f',now-1))
redis.call('ZADD',KEYS[2],now,ARGV[1])
return now
`, []string{keys[0], keys[1]}, id).Err(); err != nil {
		t.Fatal(err)
	}
}

func TestApplicationLeaseAbsentAndValidatedIdentity(t *testing.T) {
	s := testStore(t, nil)
	readyStore(t, s)
	ctx := context.Background()
	lease, err := s.ReadApplicationLease(ctx, "owner")
	if err != nil || lease.ID != "owner" || lease.Term != "" || lease.ExpiresAtMS != 0 || lease.CheckedAtMS <= 0 {
		t.Fatalf("missing lease was not an authoritative absence: %+v %v", lease, err)
	}
	if _, err = s.RenewApplicationLease(ctx, "owner", uuid.NewString()); !errors.Is(err, ErrExpired) {
		t.Fatalf("renewal created a missing lease: %v", err)
	}
	for _, id := range []string{"", "owner\n", "owner/other", string(make([]byte, 97))} {
		if _, err = s.ReadApplicationLease(ctx, id); err == nil {
			t.Fatalf("invalid identity accepted: %q", id)
		}
	}
	for _, term := range []string{"", "term", uuid.Nil.String(), "URN:UUID:" + uuid.NewString()} {
		if _, err = s.AcquireApplicationLease(ctx, "owner", term); err == nil {
			t.Fatalf("invalid term accepted: %q", term)
		}
	}
	if count := s.client.Exists(ctx, s.applicationLeaseKeys("owner")[0], s.applicationLeaseKeys("owner")[1]).Val(); count != 0 {
		t.Fatal("refused admission created lease state")
	}
}

func TestApplicationLeaseSharedLiveAcquireAndRenewal(t *testing.T) {
	s := testStore(t, nil)
	readyStore(t, s)
	ctx := context.Background()
	peer, err := Open(ctx, s.cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer peer.Close()
	firstTerm := uuid.NewString()
	first, err := s.AcquireApplicationLease(ctx, "owner", firstTerm)
	if err != nil || first.Term != firstTerm || first.ExpiresAtMS-first.CheckedAtMS != 30_000 {
		t.Fatalf("initial lease: %+v %v", first, err)
	}
	second, err := peer.AcquireApplicationLease(ctx, "owner", uuid.NewString())
	if err != nil || second.Term != firstTerm || second.ExpiresAtMS-second.CheckedAtMS != 30_000 || second.ExpiresAtMS < first.ExpiresAtMS {
		t.Fatalf("another core changed the live term: %+v %v", second, err)
	}
	renewed, err := peer.RenewApplicationLease(ctx, "owner", firstTerm)
	if err != nil || renewed.Term != firstTerm || renewed.ExpiresAtMS-renewed.CheckedAtMS != 30_000 || renewed.ExpiresAtMS < second.ExpiresAtMS {
		t.Fatalf("another core could not renew the exact live term: %+v %v", renewed, err)
	}
	keys := s.applicationLeaseKeys("owner")
	ttl := s.client.PTTL(ctx, keys[0]).Val()
	if ttl <= 119*time.Second || ttl > 120*time.Second {
		t.Fatalf("lease idle retention is not 120 seconds: %v", ttl)
	}
	observed, err := s.ReadApplicationLease(ctx, "owner")
	if err != nil || observed.Term != firstTerm || observed.ExpiresAtMS != renewed.ExpiresAtMS || s.client.PTTL(ctx, keys[0]).Val() > ttl {
		t.Fatalf("read extended the lease or its idle retention: %+v %v", observed, err)
	}
	if count := s.client.ZCard(ctx, keys[1]).Val(); count != 1 {
		t.Fatalf("same process created %d indexed leases", count)
	}
}

func TestApplicationLeaseConcurrentCoreAcquisitionHasOneTerm(t *testing.T) {
	s := testStore(t, nil)
	readyStore(t, s)
	ctx := context.Background()
	peer, err := Open(ctx, s.cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer peer.Close()
	type observation struct {
		lease ApplicationLease
		err   error
	}
	observations := make(chan observation, 16)
	var workers sync.WaitGroup
	for i := 0; i < cap(observations); i++ {
		client := s
		if i%2 == 1 {
			client = peer
		}
		workers.Add(1)
		go func() {
			defer workers.Done()
			lease, err := client.AcquireApplicationLease(ctx, "owner", uuid.NewString())
			observations <- observation{lease, err}
		}()
	}
	workers.Wait()
	close(observations)
	var term string
	for observed := range observations {
		if observed.err != nil {
			t.Fatal(observed.err)
		}
		if term == "" {
			term = observed.lease.Term
		}
		if observed.lease.Term != term {
			t.Fatal("concurrent cores allocated distinct live authority terms")
		}
	}
}

func TestApplicationLeaseExpiryCannotRenewOrReacquireSameTerm(t *testing.T) {
	s := testStore(t, nil)
	readyStore(t, s)
	ctx := context.Background()
	oldTerm := uuid.NewString()
	if _, err := s.AcquireApplicationLease(ctx, "owner", oldTerm); err != nil {
		t.Fatal(err)
	}
	expireApplicationLease(t, s, "owner")
	expired, err := s.ReadApplicationLease(ctx, "owner")
	if err != nil || expired.Term != oldTerm || expired.ExpiresAtMS > expired.CheckedAtMS {
		t.Fatalf("expired observation: %+v %v", expired, err)
	}
	if _, err = s.RenewApplicationLease(ctx, "owner", oldTerm); !errors.Is(err, ErrExpired) {
		t.Fatalf("renewal revived an expired term: %v", err)
	}
	if _, err = s.AcquireApplicationLease(ctx, "owner", oldTerm); !errors.Is(err, ErrConflict) {
		t.Fatalf("handshake reused its expired candidate term: %v", err)
	}
	newTerm := uuid.NewString()
	successor, err := s.AcquireApplicationLease(ctx, "owner", newTerm)
	if err != nil || successor.Term != newTerm || successor.ExpiresAtMS-successor.CheckedAtMS != 30_000 {
		t.Fatalf("new handshake did not allocate successor term: %+v %v", successor, err)
	}
	if _, err = s.RenewApplicationLease(ctx, "owner", oldTerm); !errors.Is(err, ErrStale) {
		t.Fatalf("old session renewed its successor: %v", err)
	}
	current, err := s.ReadApplicationLease(ctx, "owner")
	if err != nil || current.Term != newTerm || current.ExpiresAtMS != successor.ExpiresAtMS {
		t.Fatalf("stale renewal changed successor state: %+v %v", current, err)
	}
}

func TestApplicationLeaseIdleExpiryIsAuthoritativeAbsence(t *testing.T) {
	s := testStore(t, nil)
	readyStore(t, s)
	ctx := context.Background()
	if _, err := s.AcquireApplicationLease(ctx, "owner", uuid.NewString()); err != nil {
		t.Fatal(err)
	}
	expireApplicationLease(t, s, "owner")
	if err := s.client.PExpireAt(ctx, s.applicationLeaseKeys("owner")[0], time.UnixMilli(1)).Err(); err != nil {
		t.Fatal(err)
	}
	absent, err := s.ReadApplicationLease(ctx, "owner")
	if err != nil || absent.Term != "" || absent.ExpiresAtMS != 0 || absent.CheckedAtMS <= 0 {
		t.Fatalf("idle expiry was not an observed absence: %+v %v", absent, err)
	}
	if _, err = s.RenewApplicationLease(ctx, "owner", uuid.NewString()); !errors.Is(err, ErrExpired) {
		t.Fatalf("idle-expired state was revived: %v", err)
	}
}

func TestApplicationLeaseCancelledReadIsNotAbsence(t *testing.T) {
	s := testStore(t, nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.ReadApplicationLease(ctx, "owner"); !errors.Is(err, context.Canceled) {
		t.Fatal("cancelled read became absence", err)
	}
}

func TestApplicationLeaseCapacityPreservesLiveOwnersAndPrunesOnlyExpired(t *testing.T) {
	s := testStore(t, nil)
	readyStore(t, s)
	ctx := context.Background()
	for i := 0; i < 1024; i++ {
		if _, err := s.AcquireApplicationLease(ctx, fmt.Sprintf("owner-%d", i), uuid.NewString()); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.AcquireApplicationLease(ctx, "extra", uuid.NewString()); !errors.Is(err, ErrCapacity) {
		t.Fatalf("live owner capacity was exceeded: %v", err)
	}
	kept, err := s.AcquireApplicationLease(ctx, "owner-1", uuid.NewString())
	if err != nil {
		t.Fatalf("full capacity refused an existing live owner: %v", err)
	}
	if _, err = s.RenewApplicationLease(ctx, "owner-1", kept.Term); err != nil {
		t.Fatalf("full capacity refused exact renewal: %v", err)
	}
	keys := s.applicationLeaseKeys("owner-0")
	if err = s.client.ZAdd(ctx, keys[1], redis.Z{Score: 1, Member: "owner-0"}).Err(); err != nil {
		t.Fatal(err)
	}
	if _, err = s.AcquireApplicationLease(ctx, "extra", uuid.NewString()); !errors.Is(err, ErrCapacity) {
		t.Fatalf("a stale index erased a live lease: %v", err)
	}
	if live, err := s.ReadApplicationLease(ctx, "owner-0"); err != nil || live.ExpiresAtMS <= live.CheckedAtMS {
		t.Fatalf("index pruning removed live authority: %+v %v", live, err)
	}
	expireApplicationLease(t, s, "owner-0")
	if _, err = s.AcquireApplicationLease(ctx, "extra", uuid.NewString()); err != nil {
		t.Fatalf("expired owner blocked a new acquisition: %v", err)
	}
	if count := s.client.ZCard(ctx, keys[1]).Val(); count != 1024 || s.client.Exists(ctx, keys[0]).Val() != 0 {
		t.Fatalf("expired pruning did not preserve the live bound: %d", count)
	}
	if live, err := s.ReadApplicationLease(ctx, "owner-1"); err != nil || live.Term != kept.Term {
		t.Fatalf("pruning changed another owner: %+v %v", live, err)
	}
}

func TestApplicationLeasePruningHasAFixedWorkBound(t *testing.T) {
	s := testStore(t, nil)
	readyStore(t, s)
	ctx := context.Background()
	index := s.applicationLeaseKeys("owner")[1]
	if err := s.client.Eval(ctx, `
local clock=redis.call('TIME'); local now=tonumber(clock[1])*1000+math.floor(tonumber(clock[2])/1000)
for i=1,1024 do
 local id='expired-'..i; local key=ARGV[1]..id
 redis.call('HSET',key,'id',id,'term',ARGV[2],'expiresAtMs',string.format('%.0f',now),'references','0','sealed','0','recoveryUntilMs','0')
 redis.call('PEXPIRE',key,120000); redis.call('ZADD',KEYS[1],now,id)
end
return now
`, []string{index}, s.partitionPrefix(0)+"application-lease:", uuid.NewString()).Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.AcquireApplicationLease(ctx, "new-owner", uuid.NewString()); err != nil {
		t.Fatal(err)
	}
	if count := s.client.ZCard(ctx, index).Val(); count != 1024-64+1 {
		t.Fatalf("one acquisition did unbounded or insufficient pruning: %d", count)
	}
}

func TestApplicationLeaseCorruptStateIsNotLiveOrAbsent(t *testing.T) {
	for _, name := range []string{"lease-type", "index-type", "term", "expiry", "unbounded-retention"} {
		t.Run(name, func(t *testing.T) {
			s := testStore(t, nil)
			readyStore(t, s)
			ctx := context.Background()
			term := uuid.NewString()
			if _, err := s.AcquireApplicationLease(ctx, "owner", term); err != nil {
				t.Fatal(err)
			}
			keys := s.applicationLeaseKeys("owner")
			switch name {
			case "lease-type":
				if err := s.client.Set(ctx, keys[0], "other", 0).Err(); err != nil {
					t.Fatal(err)
				}
			case "index-type":
				if err := s.client.Set(ctx, keys[1], "other", 0).Err(); err != nil {
					t.Fatal(err)
				}
			case "term", "expiry":
				field, value := "term", "malformed"
				if name == "expiry" {
					field, value = "expiresAtMs", "9007199254740992"
				}
				if err := s.client.HSet(ctx, keys[0], field, value).Err(); err != nil {
					t.Fatal(err)
				}
			case "unbounded-retention":
				if err := s.client.Persist(ctx, keys[0]).Err(); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := s.ReadApplicationLease(ctx, "owner"); !errors.Is(err, ErrCorrupt) {
				t.Fatalf("corrupt lease became live or absent: %v", err)
			}
			if _, err := s.RenewApplicationLease(ctx, "owner", term); !errors.Is(err, ErrCorrupt) {
				t.Fatalf("corrupt lease renewed: %v", err)
			}
		})
	}
}
