package rediscore

import (
	"bytes"
	"context"
	"errors"
	"net"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"sipbridge/internal/redisclient"
)

func generationFixture(t *testing.T) (string, *redis.Client, func(string) *Connection) {
	t.Helper()
	cfg, client := connectionFixture(t)
	pool := cfg.Profile.Root
	t.Cleanup(func() {
		ctx := context.Background()
		iter := client.Scan(ctx, 0, pool+"*", 64).Iterator()
		for iter.Next(ctx) {
			if err := client.Del(ctx, iter.Val()).Err(); err != nil {
				t.Error(err)
			}
		}
		if err := iter.Err(); err != nil {
			t.Error(err)
		}
	})
	return pool, client, func(id string) *Connection {
		t.Helper()
		cfg := cfg
		cfg.Profile.Root, cfg.Profile.Generation = pool+id+":", id
		conn, err := NewConnection(cfg)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = conn.Close() })
		return conn
	}
}

func openGenerationFixture(t *testing.T, c *Connection) *Store {
	t.Helper()
	s, err := c.Refresh(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestGenerationClaimAndCleanup(t *testing.T) {
	ctx := context.Background()
	pool, client, connection := generationFixture(t)
	old, current := connection("1"), connection("2")
	oldStore := openGenerationFixture(t, old)
	s := openGenerationFixture(t, current)
	future := openGenerationFixture(t, connection("10"))
	nested := openGenerationFixture(t, connection("1:core:side"))
	if _, err := old.Refresh(ctx); err != nil {
		t.Fatal("an existing older profile must remain compatible while live", err)
	}
	if _, err := connection("3").Refresh(ctx); !errors.Is(err, ErrAuthority) {
		t.Fatal("generation below the newest ID opened a missing profile", err)
	}
	renew := func(store *Store) {
		t.Helper()
		lease, err := store.RenewWorker(ctx, WorkerLeaseRequest{Worker: "worker", Epoch: "epoch", PrimaryRunID: store.primary, Acquire: true})
		if err != nil || !lease.Coordinator {
			t.Fatal("worker did not acquire coordinator", lease, err)
		}
	}
	renew(oldStore)
	renew(s)
	g := current.generation
	expired := time.Now().Add(-2 * generationRetainFor).UnixMilli()
	if err := client.HSet(ctx, g.registry(), "1", expired).Err(); err != nil {
		t.Fatal(err)
	}
	claim := func(token string) bool {
		t.Helper()
		claimed, err := s.claimGeneration(ctx, g, "worker", "epoch", "1", token)
		if err != nil {
			t.Fatal(err)
		}
		return claimed
	}
	if claim("first") {
		t.Fatal("claimed past a live old coordinator")
	}
	if err := oldStore.recordGenerationHeartbeat(ctx, old.generation, "worker", "epoch"); err != nil {
		t.Fatal(err)
	}
	if err := client.Del(ctx, oldStore.coordinatorKey()).Err(); err != nil {
		t.Fatal(err)
	}
	if claim("first") {
		t.Fatal("claimed a recently refreshed generation")
	}
	for i := range 100 {
		if err := client.Set(ctx, oldStore.contextKey("fixture-"+strconv.Itoa(i)), []byte{1}, 0).Err(); err != nil {
			t.Fatal(err)
		}
	}
	if err := client.HSet(ctx, g.registry(), "1", expired, "10", expired).Err(); err != nil {
		t.Fatal(err)
	}
	if !claim("first") || claim("other") {
		t.Fatal("cleanup claim was not exclusive")
	}
	if _, err := old.Refresh(ctx); !errors.Is(err, ErrAuthority) {
		t.Fatal("retirement let the old process reopen its profile", err)
	}
	if _, err := oldStore.RenewWorker(ctx, WorkerLeaseRequest{Worker: "worker", Epoch: "epoch", PrimaryRunID: oldStore.primary}); !errors.Is(err, ErrAuthority) {
		t.Fatal("retirement left an existing old worker lease renewable", err)
	}
	oldKey := oldStore.contextKey("fixture-0")
	if _, err := s.sweepGenerationBatch(ctx, g, "1", "incorrect", []string{oldKey}, false); !errors.Is(err, ErrConflict) {
		t.Fatal("wrong cleanup claim unlinked data", err)
	}
	if _, err := s.sweepGenerationBatch(ctx, g, "1", "first", []string{future.profileKey()}, false); !errors.Is(err, ErrConflict) {
		t.Fatal("cleanup accepted a key from a newer generation", err)
	}
	previousPrimary := *s
	previousPrimary.primary = strings.Repeat("0", 40)
	if _, err := previousPrimary.sweepGenerationBatch(ctx, g, "1", "first", []string{oldKey}, false); !errors.Is(err, ErrAuthority) {
		t.Fatal("a sweep bound to a previous primary performed a deletion", err)
	}
	if removed, err := s.sweepGenerationBatch(ctx, g, "1", "first", []string{oldKey}, false); err != nil || removed != 1 {
		t.Fatal("first partial sweep failed", removed, err)
	}
	if err := client.PExpireAt(ctx, g.claim("1"), time.Now().Add(-time.Second)).Err(); err != nil {
		t.Fatal(err)
	}
	if !claim("replacement") {
		t.Fatal("replacement could not resume an interrupted cleanup")
	}
	ticks := time.NewTicker(time.Millisecond)
	defer ticks.Stop()
	removed, err := s.sweepGeneration(ctx, g, "1", "replacement", ticks.C)
	if err != nil || removed < 99 {
		t.Fatal("replacement did not finish the sweep", removed, err)
	}
	if _, err := old.Refresh(ctx); !errors.Is(err, ErrAuthority) {
		t.Fatal("cleaned generation could be recreated", err)
	}
	if _, err := client.HGet(ctx, g.registry(), "1").Result(); !errors.Is(err, redis.Nil) {
		t.Fatal("completed generation retained its registry entry", err)
	}
	if newest, err := client.HGet(ctx, g.registry(), "newest").Result(); err != nil || newest != "10" {
		t.Fatal("cleanup changed the newest generation", newest, err)
	}
	logged := 0
	if err := s.sweepGenerations(ctx, g, "worker", "epoch", func(string, int64) { logged++ }); err != nil || logged != 0 {
		t.Fatal("worker selected a later generation", logged, err)
	}
	for _, key := range []string{pool + "1:core:profile", oldKey, g.claim("1")} {
		if n, err := client.Exists(ctx, key).Result(); err != nil || n != 0 {
			t.Fatal("completed cleanup left state", n, err)
		}
	}
	if n, err := client.Exists(ctx, nested.profileKey(), future.profileKey(), s.profileKey()).Result(); err != nil || n != 3 {
		t.Fatal("cleanup altered nested, current or later state", n, err)
	}
	keys := client.Scan(ctx, 0, g.nativePrefix("1")+"*", 64).Iterator()
	for keys.Next(ctx) {
		if keys.Val() != nested.profileKey() {
			t.Fatalf("cleanup left a retired native key %q", keys.Val())
		}
	}
	if err := keys.Err(); err != nil {
		t.Fatal(err)
	}
}

func TestDriverSweepsOlderGeneration(t *testing.T) {
	_, client, connection := generationFixture(t)
	old := openGenerationFixture(t, connection("1"))
	current := connection("2")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Set(ctx, old.contextKey("old-record"), []byte{1}, 0).Err(); err != nil {
		t.Fatal(err)
	}
	if err := client.HSet(ctx, current.generation.registry(), "1", time.Now().Add(-2*generationRetainFor).UnixMilli()).Err(); err != nil {
		t.Fatal(err)
	}
	events := make(chan DriverDiagnostic, 4)
	driver, err := NewDriver(DriverConfig{Connection: current, Worker: "worker", Epoch: "epoch",
		Due:          func(context.Context, *Store, *DueContext) error { return nil },
		ReadyOutput:  func(context.Context, *Store, ReadyOutput) error { return nil },
		OwnerExpired: func(context.Context, *Store, ExpiredApplicationContext) error { return nil },
		OnDiagnostic: func(event DriverDiagnostic) {
			if event.Stage == "generation-cleanup" {
				select {
				case events <- event:
				default:
				}
			}
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- driver.Run(ctx) }()
	defer func() { cancel(); <-done }()
	select {
	case event := <-events:
		if event.Category != "complete" || event.Pool != current.generation.pool || event.Generation != "1" || event.KeysRemoved != 1 || driver.Ready() != nil {
			t.Fatal("automatic retirement did not complete with a serving worker", event)
		}
	case <-ctx.Done():
		t.Fatal("missing automatic retirement")
	}
}

func TestGenerationCleanupAfterRedisPromotion(t *testing.T) {
	address := os.Getenv("SIPBRIDGE_GENERATION_TEST_REPLICA")
	if address == "" {
		t.Skip("requires a disposable replica of the qualification Redis")
	}
	_, client, connection := generationFixture(t)
	old := openGenerationFixture(t, connection("1"))
	current := connection("2")
	prior := openGenerationFixture(t, current)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	replica := redis.NewClient(&redis.Options{Addr: address, MaxRetries: -1})
	defer replica.Close()
	g := current.generation
	const worker, epoch = "worker", "epoch"
	cut := time.Now().Add(-2 * generationRetainFor).UnixMilli()
	oldKey := old.contextKey("fixture")
	if err := client.Set(ctx, oldKey, []byte{1}, 0).Err(); err != nil {
		t.Fatal(err)
	}
	if err := client.HSet(ctx, g.registry(), "1", cut).Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := prior.RenewWorker(ctx, WorkerLeaseRequest{Worker: worker, Epoch: epoch, PrimaryRunID: prior.primary, Acquire: true}); err != nil {
		t.Fatal(err)
	}
	if claimed, err := prior.claimGeneration(ctx, g, worker, epoch, "1", "prior"); err != nil || !claimed {
		t.Fatal("initial coordinator failed to claim generation", claimed, err)
	}
	for {
		lock, err := replica.Get(ctx, g.claim("1")).Result()
		if err == nil && lock == "prior" {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("claim did not replicate", err)
		case <-time.After(10 * time.Millisecond):
		}
	}
	if err := replica.Do(ctx, "REPLICAOF", "NO", "ONE").Err(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		host, port, err := net.SplitHostPort(client.Options().Addr)
		if err != nil {
			t.Error(err)
			return
		}
		restore, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := replica.Do(restore, "REPLICAOF", host, port).Err(); err != nil {
			t.Error(err)
		}
	}()
	recovery, err := NewConnection(ConnectionConfig{
		Redis: redisclient.Options{Address: address},
		Profile: ProfileConfig{Root: current.root, Service: "sipbridge", Generation: "2",
			IdentityKey: bytes.Repeat([]byte{7}, 32), RecordBudget: 1 << 20},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer recovery.Close()
	next := openGenerationFixture(t, recovery)
	if next.primary == prior.primary {
		t.Fatal("fixture did not switch Redis primaries")
	}
	stale := *next
	stale.primary = prior.primary
	if _, err := stale.sweepGenerationBatch(ctx, g, "1", "prior", []string{oldKey}, false); !errors.Is(err, ErrAuthority) {
		t.Fatal("old binding could unlink on the promoted primary", err)
	}
	if err := replica.PExpireAt(ctx, g.claim("1"), time.Now().Add(-time.Second)).Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := next.RenewWorker(ctx, WorkerLeaseRequest{Worker: worker, Epoch: epoch, PrimaryRunID: next.primary, Reacquire: true}); err != nil {
		t.Fatal(err)
	}
	if claimed, err := next.claimGeneration(ctx, g, worker, epoch, "1", "next"); err != nil || !claimed {
		t.Fatal("new coordinator did not reclaim sweep", claimed, err)
	}
	ticks := time.NewTicker(time.Millisecond)
	defer ticks.Stop()
	if n, err := next.sweepGeneration(ctx, g, "1", "next", ticks.C); err != nil || n != 1 {
		t.Fatal("cleanup did not finish on the promoted primary", n, err)
	}
}
