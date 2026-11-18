package rediscore

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func applicationFixture(t *testing.T) (*Store, *redis.Client, string) {
	t.Helper()
	s, c, _ := fixture(t)
	app := "application1"
	t.Cleanup(func() {
		keys := []string{s.applicationKey(app), s.applicationOwnerKey("term1"), s.applicationOwnerKey("term2"), s.applicationOwnerKey("term3"), s.applicationOwnedKey("term1"), s.applicationOwnedKey("term2"), s.applicationOwnedKey("term3"), s.ownerExpiryKey(), s.applicationAdmissionKey()}
		if err := c.Del(context.Background(), keys...).Err(); err != nil {
			t.Error(err)
		}
	})
	return s, c, app
}

func TestApplicationHelloSharesLiveTermAcrossClients(t *testing.T) {
	s, c, app := applicationFixture(t)
	peerClient := redis.NewClient(&redis.Options{Addr: c.Options().Addr, MaxRetries: -1})
	defer peerClient.Close()
	peer, err := New(peerClient, Config{Root: s.root, Profile: s.profile, RecordBudget: s.maxBytes, PrimaryRunID: s.primary})
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	var results [2]ApplicationLease
	var errs [2]error
	for i, store := range []*Store{s, peer} {
		wg.Add(1)
		go func(i int, store *Store) {
			defer wg.Done()
			term := "term1"
			if i == 1 {
				term = "term2"
			}
			results[i], errs[i] = store.AcquireApplication(context.Background(), app, term)
		}(i, store)
	}
	wg.Wait()
	if errs[0] != nil || errs[1] != nil || results[0].Term != results[1].Term {
		t.Fatal("live hello forked", results, errs)
	}
	for _, key := range []string{s.applicationKey(app), s.applicationOwnerKey(results[0].Term)} {
		ttl, e := c.PTTL(context.Background(), key).Result()
		if e != nil || ttl <= 0 || ttl > ApplicationIdleLifetime {
			t.Fatal("idle TTL", key, ttl, e)
		}
	}
	if n, e := c.ZCard(context.Background(), s.ownerExpiryKey()).Result(); e != nil || n != 0 {
		t.Fatal("quiet scope indexed", n, e)
	}
}

func TestApplicationRenewAndExpiredScopeIsolation(t *testing.T) {
	s, c, app := applicationFixture(t)
	ctx := context.Background()
	first, err := s.AcquireApplication(ctx, app, "term1")
	if err != nil {
		t.Fatal(err)
	}
	if err = c.SAdd(ctx, s.applicationOwnedKey(first.Term), "context1").Err(); err != nil {
		t.Fatal(err)
	}
	if err = c.ZAdd(ctx, s.ownerExpiryKey(), redis.Z{Score: float64(first.ExpiresAtMS), Member: "a-" + first.Term}).Err(); err != nil {
		t.Fatal(err)
	}
	next, err := s.RenewApplication(ctx, first.ApplicationLeaseRef)
	if err != nil {
		t.Fatal(err)
	}
	score, err := c.ZScore(ctx, s.ownerExpiryKey(), "a-"+first.Term).Result()
	if err != nil || int64(score) != next.ExpiresAtMS {
		t.Fatal("owner expiry not refreshed", score, err)
	}
	if n, e := c.ZCard(ctx, s.ownerExpiryKey()).Result(); e != nil || n != 1 {
		t.Fatal("expiry duplicate", n, e)
	}
	for _, key := range []string{s.applicationKey(app), s.applicationOwnerKey(first.Term)} {
		ttl, e := c.PTTL(ctx, key).Result()
		if e != nil || ttl != -1 {
			t.Fatal("owned scope did not persist", key, ttl, e)
		}
	}
	if err = c.HSet(ctx, s.applicationOwnerKey(first.Term), "expires_ms", time.Now().Add(-time.Second).UnixMilli()).Err(); err != nil {
		t.Fatal(err)
	}
	if _, err = s.RenewApplication(ctx, first.ApplicationLeaseRef); !errors.Is(err, ErrExpired) {
		t.Fatal("expired term renewed", err)
	}
	if _, err = s.AcquireApplication(ctx, app, first.Term); !errors.Is(err, ErrConflict) {
		t.Fatal("expired term reused", err)
	}
	replacement, err := s.AcquireApplication(ctx, app, "term2")
	if err != nil {
		t.Fatal(err)
	}
	if replacement.Term == first.Term {
		t.Fatal("expired owner revived")
	}
	if _, err = s.RenewApplication(ctx, first.ApplicationLeaseRef); !errors.Is(err, ErrAuthority) {
		t.Fatal("old term not fenced", err)
	}
	if n, e := c.SCard(ctx, s.applicationOwnedKey(first.Term)).Result(); e != nil || n != 1 {
		t.Fatal("old scope lost contexts", n, e)
	}
	if _, e := c.ZScore(ctx, s.ownerExpiryKey(), "a-"+first.Term).Result(); e != nil {
		t.Fatal("old expiry lost", e)
	}
	observed, err := s.ObserveApplication(ctx, app)
	if err != nil || observed.Term != replacement.Term {
		t.Fatal("observe wrong term", err)
	}
}

func TestApplicationLostReplyAndPreflightFences(t *testing.T) {
	s, c, app := applicationFixture(t)
	ctx := context.Background()
	client := redis.NewClient(&redis.Options{Addr: c.Options().Addr, MaxRetries: -1})
	defer client.Close()
	client.AddHook(&loseReply{armed: true})
	peer, err := New(client, Config{Root: s.root, Profile: s.profile, RecordBudget: s.maxBytes, PrimaryRunID: s.primary})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = peer.AcquireApplication(ctx, app, "term1"); !errors.Is(err, ErrUnknown) {
		t.Fatal("lost acquisition not UNKNOWN", err)
	}
	got, err := s.ObserveApplication(ctx, app)
	if err != nil || got.Term != "term1" {
		t.Fatal("lost acquisition not observable", err)
	}
	wrong := *s
	wrong.primary = strings.Repeat("a", 40)
	if _, err = wrong.AcquireApplication(ctx, app, "term2"); !errors.Is(err, ErrAuthority) {
		t.Fatal("wrong primary accepted", err)
	}
	after, err := s.ObserveApplication(ctx, app)
	if err != nil || after.Term != got.Term || after.ExpiresAtMS != got.ExpiresAtMS {
		t.Fatal("primary refusal changed lease", err)
	}
	now := time.Now().UnixMilli()
	v, err := applicationLeaseScript.Run(ctx, c, []string{s.profileKey(), s.applicationKey(app), s.ownerExpiryKey(), s.applicationAdmissionKey()}, s.profile, app, "acquire", "", "term2", now+60000, now+61000, ApplicationMaxGroups, ApplicationIdleLifetime.Milliseconds(), s.primary).Slice()
	if err != nil {
		t.Fatal(err)
	}
	if err = status(v); !errors.Is(err, ErrExpired) {
		t.Fatal("outside clock window accepted", err)
	}
	after, err = s.ObserveApplication(ctx, app)
	if err != nil || after.Term != got.Term || after.ExpiresAtMS != got.ExpiresAtMS {
		t.Fatal("clock refusal changed lease", err)
	}
}

func TestApplicationPublicationGuard(t *testing.T) {
	s, c, app := applicationFixture(t)
	ctx := context.Background()
	lease, err := s.AcquireApplication(ctx, app, "term1")
	if err != nil {
		t.Fatal(err)
	}
	script := redis.NewScript(applicationGuardScript + `
local problem=guardApplication(ARGV[1],ARGV[2],ARGV[3],tonumber(ARGV[4])); return problem or 'OK'`)
	check := func(ref ApplicationLeaseRef, now int64) string {
		t.Helper()
		v, e := script.Run(ctx, c, nil, s.root, ref.Application, ref.Term, now).Text()
		if e != nil {
			t.Fatal(e)
		}
		return v
	}
	if v := check(lease.ApplicationLeaseRef, lease.CheckedAtMS); v != "OK" {
		t.Fatal(v)
	}
	wrong := lease.ApplicationLeaseRef
	wrong.Term = "old-term"
	if v := check(wrong, lease.CheckedAtMS); v != "AUTHORITY" {
		t.Fatal("term guard", v)
	}
	if v := check(lease.ApplicationLeaseRef, lease.ExpiresAtMS); v != "EXPIRED" {
		t.Fatal("expiry guard", v)
	}
	if err := c.HSet(ctx, s.applicationOwnerKey(lease.Term), "expires_ms", time.Now().Add(-time.Second).UnixMilli()).Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.AcquireApplication(ctx, app, "term2"); err != nil {
		t.Fatal(err)
	}
	if v := check(lease.ApplicationLeaseRef, time.Now().UnixMilli()); v != "AUTHORITY" {
		t.Fatal("old scope guard", v)
	}
}

func TestApplicationRejectsOversizedScopeAndWrongType(t *testing.T) {
	s, c, app := applicationFixture(t)
	ctx := context.Background()
	if _, err := s.AcquireApplication(ctx, app, strings.Repeat("x", 191)); !errors.Is(err, ErrConflict) {
		t.Fatal("unbounded scope admitted", err)
	}
	if err := c.HSet(ctx, s.applicationKey(app), "bad", "wrong-type").Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.AcquireApplication(ctx, app, "term1"); !errors.Is(err, ErrCorrupt) {
		t.Fatal("wrong key type admitted", err)
	}
	if n, err := c.Exists(ctx, s.applicationOwnerKey("term1"), s.applicationOwnedKey("term1"), s.ownerExpiryKey()).Result(); err != nil || n != 0 {
		t.Fatal("refusal left partial state", n, err)
	}
}

func TestApplicationRenewDoesNotShortenPriorDeadline(t *testing.T) {
	s, c, app := applicationFixture(t)
	ctx := context.Background()
	lease, err := s.AcquireApplication(ctx, app, "term1")
	if err != nil {
		t.Fatal(err)
	}
	prior := time.Now().Add(30100 * time.Millisecond).UnixMilli()
	if err = c.HSet(ctx, s.applicationOwnerKey(lease.Term), "expires_ms", prior).Err(); err != nil {
		t.Fatal(err)
	}
	renewed, err := s.RenewApplication(ctx, lease.ApplicationLeaseRef)
	if err != nil {
		t.Fatal(err)
	}
	if renewed.ExpiresAtMS < prior {
		t.Fatalf("lease shortened: %d < %d", renewed.ExpiresAtMS, prior)
	}
}

func TestApplicationExpiredEmptyOwnerRemovedAtomically(t *testing.T) {
	s, c, app := applicationFixture(t)
	ctx := context.Background()
	first, err := s.AcquireApplication(ctx, app, "term1")
	if err != nil {
		t.Fatal(err)
	}
	if err = c.HSet(ctx, s.applicationOwnerKey(first.Term), "expires_ms", time.Now().Add(-time.Second).UnixMilli()).Err(); err != nil {
		t.Fatal(err)
	}
	if err = c.Set(ctx, s.applicationOwnerKey("term2"), "occupied", 0).Err(); err != nil {
		t.Fatal(err)
	}
	if _, err = s.AcquireApplication(ctx, app, "term2"); !errors.Is(err, ErrConflict) {
		t.Fatal("term collision accepted", err)
	}
	if n, err := c.Exists(ctx, s.applicationOwnerKey("term1")).Result(); err != nil || n != 1 {
		t.Fatal("refusal deleted old owner", n, err)
	}
	if err = c.Del(ctx, s.applicationOwnerKey("term2")).Err(); err != nil {
		t.Fatal(err)
	}
	next, err := s.AcquireApplication(ctx, app, "term2")
	if err != nil || next.Term != "term2" {
		t.Fatal("replacement failed", err)
	}
	if n, err := c.Exists(ctx, s.applicationOwnerKey("term1")).Result(); err != nil || n != 0 {
		t.Fatal("expired empty owner retained", n, err)
	}
}

func TestApplicationTermCollisionAcrossApplications(t *testing.T) {
	s, c, app := applicationFixture(t)
	ctx := context.Background()
	if _, err := s.AcquireApplication(ctx, app, "term1"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.AcquireApplication(ctx, "application2", "term1"); !errors.Is(err, ErrConflict) {
		t.Fatal("cross-application term collision accepted", err)
	}
	if n, err := c.Exists(ctx, s.applicationKey("application2")).Result(); err != nil || n != 0 {
		t.Fatal("term collision left current pointer", n, err)
	}
}

func TestApplicationAdmissionCapAndOwnedTermPreservation(t *testing.T) {
	s, c, _ := applicationFixture(t)
	ctx := context.Background()
	now := time.Now()
	entries := make([]redis.Z, ApplicationMaxGroups)
	for i := range entries {
		entries[i] = redis.Z{Member: fmt.Sprintf("app-%04d", i), Score: float64(now.Add(time.Minute).UnixMilli())}
	}
	entries[0] = redis.Z{Member: "expired-app", Score: float64(now.Add(-time.Second).UnixMilli())}
	if err := c.ZAdd(ctx, s.applicationAdmissionKey(), entries...).Err(); err != nil {
		t.Fatal(err)
	}
	owned := []string{s.applicationKey("expired-app"), s.applicationOwnerKey("old-term"), s.applicationOwnedKey("old-term"), s.applicationKey("new-app"), s.applicationOwnerKey("new-term"), s.applicationKey("blocked-app"), s.applicationOwnerKey("blocked-term")}
	t.Cleanup(func() {
		if err := c.Del(ctx, owned...).Err(); err != nil {
			t.Error(err)
		}
	})
	if err := c.Set(ctx, s.applicationKey("expired-app"), "old-term", 0).Err(); err != nil {
		t.Fatal(err)
	}
	if err := c.HSet(ctx, s.applicationOwnerKey("old-term"), "application", "expired-app", "expires_ms", now.Add(-time.Second).UnixMilli()).Err(); err != nil {
		t.Fatal(err)
	}
	if err := c.SAdd(ctx, s.applicationOwnedKey("old-term"), "old-context").Err(); err != nil {
		t.Fatal(err)
	}
	lease, err := s.AcquireApplication(ctx, "new-app", "new-term")
	if err != nil {
		t.Fatal(err)
	}
	if lease.Term != "new-term" {
		t.Fatal("new group not admitted")
	}
	n, err := c.ZCard(ctx, s.applicationAdmissionKey()).Result()
	if err != nil || n != ApplicationMaxGroups {
		t.Fatal("wrong bounded count", n, err)
	}
	if current, err := c.Get(ctx, s.applicationKey("expired-app")).Result(); err != nil || current != "old-term" {
		t.Fatal("expired owned pointer erased", err)
	}
	if n, err = c.SCard(ctx, s.applicationOwnedKey("old-term")).Result(); err != nil || n != 1 {
		t.Fatal("expired owned scope erased", err)
	}
	if _, err = s.AcquireApplication(ctx, "blocked-app", "blocked-term"); !errors.Is(err, ErrCapacity) {
		t.Fatal("full cap accepted new application", err)
	}
	if n, err = c.Exists(ctx, s.applicationKey("blocked-app"), s.applicationOwnerKey("blocked-term")).Result(); err != nil || n != 0 {
		t.Fatal("refused admission wrote state", n, err)
	}
	before, err := c.ZScore(ctx, s.applicationAdmissionKey(), "new-app").Result()
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.RenewApplication(ctx, lease.ApplicationLeaseRef); err != nil {
		t.Fatal(err)
	}
	after, err := c.ZScore(ctx, s.applicationAdmissionKey(), "new-app").Result()
	if err != nil || after < before {
		t.Fatal("renewal failed to refresh group", after, err)
	}
}
func TestApplicationAdmissionCapConcurrentClients(t *testing.T) {
	s, c, _ := applicationFixture(t)
	ctx := context.Background()
	entries := make([]redis.Z, ApplicationMaxGroups-1)
	due := float64(time.Now().Add(time.Minute).UnixMilli())
	for i := range entries {
		entries[i] = redis.Z{Member: fmt.Sprintf("app-%04d", i), Score: due}
	}
	if err := c.ZAdd(ctx, s.applicationAdmissionKey(), entries...).Err(); err != nil {
		t.Fatal(err)
	}
	keys := []string{s.applicationKey("racer-one"), s.applicationOwnerKey("race-term-one"), s.applicationKey("racer-two"), s.applicationOwnerKey("race-term-two")}
	t.Cleanup(func() {
		if err := c.Del(ctx, keys...).Err(); err != nil {
			t.Error(err)
		}
	})
	peer, err := New(c, Config{Root: s.root, Profile: s.profile, RecordBudget: s.maxBytes, PrimaryRunID: s.primary})
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	var mu sync.Mutex
	accepted, refused := 0, 0
	for i, item := range []struct{ app, term string }{{"racer-one", "race-term-one"}, {"racer-two", "race-term-two"}} {
		wg.Add(1)
		go func(n int, v struct{ app, term string }) {
			defer wg.Done()
			store := s
			if n == 1 {
				store = peer
			}
			_, e := store.AcquireApplication(ctx, v.app, v.term)
			mu.Lock()
			defer mu.Unlock()
			if e == nil {
				accepted++
			} else if errors.Is(e, ErrCapacity) {
				refused++
			} else {
				t.Errorf("acquire %v", e)
			}
		}(i, item)
	}
	wg.Wait()
	if accepted != 1 || refused != 1 {
		t.Fatalf("accepted=%d refused=%d", accepted, refused)
	}
	n, err := c.ZCard(ctx, s.applicationAdmissionKey()).Result()
	if err != nil || n != ApplicationMaxGroups {
		t.Fatal("cap raced", n, err)
	}
}
