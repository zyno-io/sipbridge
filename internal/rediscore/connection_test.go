package rediscore

import (
	"bytes"
	"context"
	"errors"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"sipbridge/internal/redisclient"
)

func connectionFixture(t *testing.T) (ConnectionConfig, *redis.Client) {
	t.Helper()
	address := os.Getenv("SIPBRIDGE_CORE_TEST_REDIS")
	if address == "" {
		t.Skip("isolated qualification Redis required")
	}
	client := redis.NewClient(&redis.Options{Addr: address, MaxRetries: -1})
	if err := client.Ping(context.Background()).Err(); err != nil {
		t.Fatal(err)
	}
	root := "sipstate:test:connection-" + uuid.NewString() + ":"
	cfg := ConnectionConfig{Redis: redisclient.Options{Address: address, DialTimeout: time.Second, IOTimeout: time.Second, PoolSize: 2}, Profile: ProfileConfig{Root: root, Service: "sipbridge", Generation: "test", IdentityKey: bytes.Repeat([]byte{7}, 32), RecordBudget: 1 << 20}}
	t.Cleanup(func() {
		keys := []string{root + "core:profile", root + "core:capacity", root + "core:selectors", root + "core:applications", root + "core:owner-expiry", root + "core:coordinator"}
		if err := client.Del(context.Background(), keys...).Err(); err != nil {
			t.Error(err)
		}
		if err := client.Close(); err != nil {
			t.Error(err)
		}
	})
	return cfg, client
}
func TestConnectionDeferredAndOwnedAuthority(t *testing.T) {
	cfg := ConnectionConfig{Redis: redisclient.Options{Address: "127.0.0.1:1", DialTimeout: 50 * time.Millisecond, IOTimeout: 50 * time.Millisecond}, Profile: ProfileConfig{Root: "sipstate:test:deferred:", Service: "sipbridge", Generation: "test", IdentityKey: bytes.Repeat([]byte{1}, 32), RecordBudget: 1 << 20}}
	c, err := NewConnection(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	before := c.Authority()
	before.PrimaryId = "changed"
	if c.Authority().PrimaryId == "changed" {
		t.Fatal("authority not independently owned")
	}
	cfg.Profile.IdentityKey[0] ^= 1
	if _, err = c.Refresh(context.Background()); !errors.Is(err, ErrUnknown) {
		t.Fatal("unavailable primary did not fail closed", err)
	}
}
func TestConnectionRefreshInitializesAndBindsPrimary(t *testing.T) {
	cfg, client := connectionFixture(t)
	conn, err := NewConnection(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ctx := context.Background()
	binding, err := BindProfile(cfg.Profile)
	if err != nil {
		t.Fatal(err)
	}
	cfg.Profile.IdentityKey[0] ^= 1
	if n, err := client.Exists(ctx, cfg.Profile.Root+"core:profile").Result(); err != nil || n != 0 {
		t.Fatal("deferred constructor dialed or initialized", n, err)
	}
	authority := conn.Authority()
	store, err := conn.Refresh(ctx)
	if err != nil {
		t.Fatal(err)
	}
	info, err := client.Info(ctx, "server").Result()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(info, "run_id:"+store.primary) {
		t.Fatal("store not bound to actual primary")
	}
	raw, err := client.Get(ctx, cfg.Profile.Root+"core:profile").Bytes()
	if err != nil || !bytes.Equal(raw, binding.Value) {
		t.Fatal("wrong persisted profile", err)
	}
	other, err := conn.Refresh(ctx)
	if err != nil || other.primary != store.primary || conn.Authority().PrimaryId != authority.PrimaryId {
		t.Fatal("refresh changed logical authority", err)
	}
}
func TestConnectionRefusesMismatchAndPartialNamespace(t *testing.T) {
	cfg, c := connectionFixture(t)
	ctx := context.Background()
	conn, err := NewConnection(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	key := cfg.Profile.Root + "core:profile"
	wrong := []byte("other-profile")
	if err = c.Set(ctx, key, wrong, 0).Err(); err != nil {
		t.Fatal(err)
	}
	if _, err = conn.Refresh(ctx); !errors.Is(err, ErrAuthority) {
		t.Fatal("wrong profile accepted", err)
	}
	after, err := c.Get(ctx, key).Bytes()
	if err != nil || !bytes.Equal(after, wrong) {
		t.Fatal("wrong profile overwritten", err)
	}
	if err = c.Del(ctx, key).Err(); err != nil {
		t.Fatal(err)
	}
	if err = c.HSet(ctx, cfg.Profile.Root+"core:capacity", "contexts", 1, "bytes", 1).Err(); err != nil {
		t.Fatal(err)
	}
	if _, err = conn.Refresh(ctx); !errors.Is(err, ErrAuthority) {
		t.Fatal("partial namespace accepted", err)
	}
	if n, err := c.Exists(ctx, key).Result(); err != nil || n != 0 {
		t.Fatal("partial namespace initialized", err)
	}
}
func TestConnectionConcurrentIncompatibleInitializers(t *testing.T) {
	cfg, c := connectionFixture(t)
	other := cfg
	other.Profile.IdentityKey = bytes.Repeat([]byte{9}, 32)
	a, err := NewConnection(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	b, err := NewConnection(other)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	var wg sync.WaitGroup
	var errs [2]error
	for i, conn := range []*Connection{a, b} {
		wg.Add(1)
		go func(n int, v *Connection) { defer wg.Done(); _, errs[n] = v.Refresh(context.Background()) }(i, conn)
	}
	wg.Wait()
	if !((errs[0] == nil && errors.Is(errs[1], ErrAuthority)) || (errs[1] == nil && errors.Is(errs[0], ErrAuthority))) {
		t.Fatal("incompatible bootstrap race", errs)
	}
	raw, err := c.Get(context.Background(), cfg.Profile.Root+"core:profile").Bytes()
	if err != nil {
		t.Fatal(err)
	}
	expected := a.profile
	if errs[1] == nil {
		expected = b.profile
	}
	if !bytes.Equal(raw, expected) {
		t.Fatal("winner profile overwritten")
	}
}
func TestConnectionLostInitializationReplyIsUnknown(t *testing.T) {
	cfg, c := connectionFixture(t)
	conn, err := NewConnection(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	conn.client.AddHook(&loseReply{armed: true})
	if _, err = conn.Refresh(context.Background()); !errors.Is(err, ErrUnknown) {
		t.Fatal("lost mutation reply claimed failure", err)
	}
	raw, err := c.Get(context.Background(), cfg.Profile.Root+"core:profile").Bytes()
	if err != nil || !bytes.Equal(raw, conn.profile) {
		t.Fatal("profile not observable", err)
	}
	if _, err = conn.Refresh(context.Background()); err != nil {
		t.Fatal("same profile did not reconcile", err)
	}
}
