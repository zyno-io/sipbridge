//go:build redis_restart

package state

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/redis/go-redis/v9"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
)

func continuityEventually(t *testing.T, description string, condition func() bool) {
	t.Helper()
	until := time.Now().Add(20 * time.Second)
	for time.Now().Before(until) {
		if condition() {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatal("timed out: " + description)
}

func continuitySentinel(t *testing.T, program, primaryPort string) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	_, port, _ := net.SplitHostPort(address)
	listener.Close()
	dir := t.TempDir()
	config := filepath.Join(dir, "sentinel.conf")
	contents := fmt.Sprintf("bind 127.0.0.1\nport %s\nprotected-mode yes\ndir %s\nsentinel monitor fixture 127.0.0.1 %s 2\nsentinel down-after-milliseconds fixture 500\nsentinel failover-timeout fixture 2000\nsentinel parallel-syncs fixture 1\n", port, dir, primaryPort)
	if err := os.WriteFile(config, []byte(contents), 0600); err != nil {
		t.Fatal(err)
	}
	command := exec.Command(program, config, "--sentinel")
	command.Stdout, command.Stderr = io.Discard, io.Discard
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	exited := make(chan error, 1)
	go func() { exited <- command.Wait() }()
	t.Cleanup(func() {
		if err := command.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
			t.Error(err)
		}
		select {
		case <-exited:
		case <-time.After(5 * time.Second):
			t.Error("Sentinel process not reaped")
		}
	})
	admin := standaloneRestartAdmin(t, address)
	continuityEventually(t, "Sentinel listener", func() bool { return admin.Ping(context.Background()).Err() == nil })
	return address
}

func TestStateRetainedAuthoritySurvivesRealSentinelPromotion(t *testing.T) {
	primary := newStandaloneRestartRedis(t)
	replicas := []*standaloneRestartRedis{newStandaloneRestartRedis(t), newStandaloneRestartRedis(t)}
	ctx := context.Background()
	for _, replica := range replicas {
		admin := standaloneRestartAdmin(t, replica.address)
		if err := admin.Do(ctx, "REPLICAOF", "127.0.0.1", primary.port).Err(); err != nil {
			t.Fatal(err)
		}
	}
	sentinels := make([]string, 3)
	for n := range sentinels {
		sentinels[n] = continuitySentinel(t, primary.program, primary.port)
	}
	for _, endpoint := range sentinels {
		admin := standaloneRestartAdmin(t, endpoint)
		continuityEventually(t, "complete Sentinel voter discovery", func() bool {
			raw, err := admin.Do(ctx, "SENTINEL", "SENTINELS", "fixture").Slice()
			return err == nil && len(raw) == 2
		})
	}
	cfg := Config{Redis: RedisOptions{SentinelAddresses: sentinels, SentinelMaster: "fixture", DialTimeout: 200 * time.Millisecond, IOTimeout: 30 * time.Second, PoolSize: 2}, Prefix: "sipstate:sentinel-continuity", Namespace: "test-" + uuid.NewString(), NamespaceKey: bytes.Repeat([]byte{81}, 32), Partitions: 2, Generation: "retained", Versions: Versions{Schema: "1", Reducer: "1", Timers: "1"}, ClaimTTL: 100 * time.Millisecond}
	store, err := Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.Ready(ctx); err != nil {
		t.Fatal(err)
	}
	original := store.Authority()
	actual := store.primaryID()
	lease, ref := pinContinuityLease(t, store, "controller-owner")
	pending := begin(t, store, ref, "pending-across-promotion", false)
	for _, replica := range replicas {
		admin := standaloneRestartAdmin(t, replica.address)
		continuityEventually(t, "retained claim/namespace replicated", func() bool {
			return admin.HGet(ctx, store.keys(ref)[0], "claimId").Val() == pending.Claim.ID
		})
	}
	// Hold this test claim beyond promotion so rejecting its old computation is
	// specifically a transport/actual-primary check, not incidental TTL expiry.
	heldUntil := time.Now().Add(time.Minute).UnixMilli()
	primaryAdmin := standaloneRestartAdmin(t, primary.address)
	if err := primaryAdmin.HSet(ctx, store.keys(ref)[0], "claimUntilMs", heldUntil).Err(); err != nil {
		t.Fatal(err)
	}
	for _, replica := range replicas {
		admin := standaloneRestartAdmin(t, replica.address)
		continuityEventually(t, "held test claim replicated", func() bool { return admin.HGet(ctx, store.keys(ref)[0], "claimUntilMs").Val() == fmt.Sprint(heldUntil) })
	}
	hook := &continuitySwitchHook{change: func() {
		primary.stop(t, true)
		continuityEventually(t, "existing Store reconnects to promoted retained primary", func() bool { return store.Ready(ctx) == nil && store.primaryID() != actual })
	}}
	store.client.AddHook(hook)
	if _, err := store.Commit(ctx, *pending, Payload{ProtocolState: []byte("old-computation-must-not-cross")}, nil); !errors.Is(err, ErrStale) {
		t.Fatal("old computation crossed check/execution promotion boundary", err)
	}
	if !hook.fired.Load() {
		t.Fatal("promotion did not occur inside the commit boundary")
	}
	// Let normal recovery reclaim this same immutable pending input immediately.
	if err := store.client.HSet(ctx, store.keys(ref)[0], "claimUntilMs", time.Now().UnixMilli()).Err(); err != nil {
		t.Fatal(err)
	}
	if store.Authority() != original {
		t.Fatal("promotion rewrote logical SIP authority")
	}
	record, err := store.Load(ctx, ref)
	if err != nil || record.Ref.Incarnation != ref.Incarnation || record.ApplicationLeases["controller-owner"].Term != lease.Term {
		t.Fatalf("retained context/lease lost: %+v %v", record, err)
	}
	if _, err := store.Commit(ctx, *pending, Payload{}, nil); !errors.Is(err, ErrStale) {
		t.Fatal("old-primary computation crossed promotion", err)
	}
	continuityEventually(t, "retained pending input reclaimable", func() bool {
		result, err := store.Resume(ctx, ref, "successor-worker")
		if err != nil || result.Step == nil {
			return false
		}
		if result.Step.Input.ID != pending.Input.ID {
			t.Fatal("promotion replaced admitted input")
		}
		next := result.Step.Record.Payload
		next.ProtocolState = []byte("resumed-same-dialog")
		_, err = store.Commit(ctx, *result.Step, next, []byte("retained-result"))
		return err == nil
	})
	fresh, err := Open(ctx, cfg)
	if err != nil {
		t.Fatal("new worker could not read retained logical bootstrap", err)
	}
	defer fresh.Close()
	if fresh.Authority() != original {
		t.Fatal("new worker invented another authority after promotion")
	}
	loaded, err := fresh.Load(ctx, ref)
	if err != nil || string(loaded.Payload.ProtocolState) != "resumed-same-dialog" {
		t.Fatal("new worker did not continue retained protocol state", err)
	}
	inspected, err := fresh.InspectOperation(ctx, ref, pending.Input.ID, pending.Input.Fingerprint)
	if err != nil || inspected.Receipt == nil {
		t.Fatal("exact operation receipt not retained through promotion", err)
	}
	expireApplicationLease(t, fresh, "controller-owner")
	if resumed, err := fresh.ResumeApplicationLease(ctx, "controller-owner", lease.Term); err != nil || resumed.Term != lease.Term {
		t.Fatal("original owner could not resume after primary switch", err)
	}
	if err := fresh.client.Del(ctx, fresh.keys(ref)[0]).Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Load(ctx, ref); !errors.Is(err, ErrNotFound) {
		t.Fatal("missing promoted dialog became generic authority fence or invented state", err)
	}
}

// The hook changes the real master after Commit/runKeys captured their expected
// observation, but before the command is sent. Captured Lua arguments must still
// name the old physical process, so the new primary cannot accept the old work.
type continuitySwitchHook struct {
	fired  atomic.Bool
	change func()
}

func (h *continuitySwitchHook) DialHook(next redis.DialHook) redis.DialHook { return next }
func (h *continuitySwitchHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}
func (h *continuitySwitchHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, command redis.Cmder) error {
		if (command.Name() == "evalsha" || command.Name() == "eval") && len(command.Args()) > 1 && command.Args()[1] == commitScript.Hash() && h.fired.CompareAndSwap(false, true) {
			h.change()
		}
		return next(ctx, command)
	}
}
