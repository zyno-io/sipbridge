//go:build redis_restart

package state

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

// This tagged fixture never reads SIPBRIDGE_STATE_TEST_REDIS. It owns the
// executable, private loopback endpoint, tempdir and every process it kills.
type standaloneRestartRedis struct {
	program            string
	address            string
	port               string
	dir                string
	extra              []string
	tls                *tls.Config
	username, password string
	command            *exec.Cmd
	exited             chan error
}

func newStandaloneRestartRedis(t *testing.T) *standaloneRestartRedis {
	t.Helper()
	program, err := exec.LookPath("redis-server")
	if err != nil {
		t.Fatal("standalone restart qualification requires redis-server; this tagged fixture cannot skip")
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal("reserve private standalone Redis port")
	}
	address := listener.Addr().String()
	_, port, err := net.SplitHostPort(address)
	listener.Close()
	if err != nil {
		t.Fatal("read private standalone Redis port")
	}
	fixture := &standaloneRestartRedis{program: program, address: address, port: port, dir: t.TempDir()}
	t.Cleanup(func() { fixture.stop(t, false) })
	fixture.start(t)
	return fixture
}

func (f *standaloneRestartRedis) start(t *testing.T) {
	t.Helper()
	if f.command != nil {
		t.Fatal("standalone Redis process was not reaped before restart")
	}
	arguments := []string{"--bind", "127.0.0.1", "--port", f.port, "--protected-mode", "yes",
		"--save", "", "--appendonly", "no", "--daemonize", "no", "--dir", f.dir, "--loglevel", "warning",
		"--maxmemory-policy", "noeviction"}
	command := exec.Command(f.program, append(arguments, f.extra...)...)
	command.Stdout, command.Stderr = io.Discard, io.Discard
	if err := command.Start(); err != nil {
		t.Fatal("start owned standalone Redis process")
	}
	exited := make(chan error, 1)
	f.command, f.exited = command, exited
	go func() { exited <- command.Wait() }()
	probe := redis.NewClient(&redis.Options{Addr: f.address, TLSConfig: f.tls, Username: f.username, Password: f.password, MaxRetries: -1, DialTimeout: 100 * time.Millisecond,
		ReadTimeout: 100 * time.Millisecond, WriteTimeout: 100 * time.Millisecond, ContextTimeoutEnabled: true})
	defer probe.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		if err := probe.Ping(ctx).Err(); err == nil {
			return
		}
		select {
		case <-exited:
			f.command, f.exited = nil, nil
			t.Fatal("owned standalone Redis exited before readiness")
		case <-ctx.Done():
			t.Fatal("owned standalone Redis did not become ready")
		case <-ticker.C:
		}
	}
}

func (f *standaloneRestartRedis) stop(t *testing.T, required bool) {
	t.Helper()
	if f.command == nil {
		if required {
			t.Fatal("owned standalone Redis was not running at the crash boundary")
		}
		return
	}
	command, exited := f.command, f.exited
	f.command, f.exited = nil, nil
	if err := command.Process.Kill(); err != nil && (required || !errors.Is(err, os.ErrProcessDone)) {
		t.Error("could not kill the owned standalone Redis process")
	}
	select {
	case <-exited:
	case <-time.After(5 * time.Second):
		t.Fatal("owned standalone Redis process was not reaped")
	}
}

func standaloneRestartAdmin(t *testing.T, address string) *redis.Client {
	t.Helper()
	client := redis.NewClient(&redis.Options{Addr: address, MaxRetries: -1, DialTimeout: time.Second,
		ReadTimeout: time.Second, WriteTimeout: time.Second, ContextTimeoutEnabled: true})
	t.Cleanup(func() { client.Close() })
	return client
}

func standaloneRestartRunID(t *testing.T, client *redis.Client) string {
	t.Helper()
	info, err := client.Info(context.Background(), "server").Result()
	if err != nil {
		t.Fatal("read actual standalone Redis process identity")
	}
	match := runIDPattern.FindStringSubmatch(info)
	if len(match) != 2 {
		t.Fatal("standalone Redis did not report a valid process identity")
	}
	return match[1]
}

func standaloneRestartInput(id string, create bool) Input {
	now := time.Now()
	return Input{ID: id, Kind: "request", Fingerprint: "fingerprint-" + id, Payload: []byte("fixture-" + id),
		DeadlineMS: now.Add(30 * time.Second).UnixMilli(), ReceiptUntilMS: now.Add(time.Minute).UnixMilli(), Create: create}
}

func standaloneRestartAuthorityRefusal(t *testing.T, operation string, err error) {
	t.Helper()
	if err == nil || errors.Is(err, ErrNotFound) {
		t.Fatalf("%s did not fail as an authority loss", operation)
	}
	// A reconnect's positive OnConnect authority refusal is wrapped as UNKNOWN
	// by mutation I/O; it must not become a guessed missing-context decision.
	if !errors.Is(err, ErrAuthority) && !(errors.Is(err, ErrUnknown) && strings.Contains(err.Error(), ErrAuthority.Error())) {
		t.Fatalf("%s did not preserve its authority refusal", operation)
	}
}

func TestStateSentinelDiscoveryAuthority(t *testing.T) {
	fixture := newStandaloneRestartRedis(t)
	admin := standaloneRestartAdmin(t, fixture.address)
	primaryRunID := standaloneRestartRunID(t, admin)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal("reserve owned Sentinel endpoint")
	}
	address := listener.Addr().String()
	_, port, err := net.SplitHostPort(address)
	listener.Close()
	if err != nil {
		t.Fatal("read owned Sentinel port")
	}
	dir := t.TempDir()
	config := filepath.Join(dir, "sentinel.conf")
	contents := "bind 127.0.0.1\nport " + port + "\nprotected-mode yes\ndir " + dir +
		"\nsentinel monitor fixture 127.0.0.1 " + fixture.port + " 1\n"
	if err := os.WriteFile(config, []byte(contents), 0600); err != nil {
		t.Fatal("write owned Sentinel configuration")
	}
	command := exec.Command(fixture.program, config, "--sentinel")
	command.Stdout, command.Stderr = io.Discard, io.Discard
	if err := command.Start(); err != nil {
		t.Fatal("start owned Sentinel process")
	}
	exited := make(chan error, 1)
	go func() { exited <- command.Wait() }()
	t.Cleanup(func() {
		if err := command.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
			t.Error("kill owned Sentinel process")
		}
		select {
		case <-exited:
		case <-time.After(5 * time.Second):
			t.Error("owned Sentinel process was not reaped")
		}
	})
	probe := standaloneRestartAdmin(t, address)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for {
		if endpoint, err := probe.Do(ctx, "SENTINEL", "GET-MASTER-ADDR-BY-NAME", "fixture").StringSlice(); err == nil && len(endpoint) == 2 && net.JoinHostPort(endpoint[0], endpoint[1]) == fixture.address {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("owned Sentinel did not discover the owned Redis primary")
		case <-time.After(10 * time.Millisecond):
		}
	}
	cfg := Config{Redis: RedisOptions{SentinelAddresses: []string{address}, SentinelMaster: "fixture",
		DialTimeout: time.Second, IOTimeout: time.Second, PoolSize: 2}, Prefix: "sipstate:sentinel-test",
		Namespace: "test-" + uuid.NewString(), NamespaceKey: bytes.Repeat([]byte{84}, 32), Partitions: 1,
		Generation: "sentinel-generation", Versions: Versions{Schema: "1", Reducer: "1", Timers: "1"}}
	store, err := Open(ctx, cfg)
	if err != nil {
		t.Fatalf("open state through real Sentinel discovery: %v", err)
	}
	t.Cleanup(func() { store.Close() })
	if store.primaryID() != primaryRunID {
		t.Fatal("Sentinel discovery replaced the actual data primary identity")
	}
	if err := store.Ready(ctx); err != nil {
		t.Fatal("bootstrap actual data authority through Sentinel")
	}
	group, err := store.GroupForCallID("sentinel-discovery@fixture.test")
	if err != nil {
		t.Fatal(err)
	}
	step, err := store.Begin(ctx, group, "sentinel-worker", standaloneRestartInput("sentinel-commit", true))
	if err != nil || step.Step == nil {
		t.Fatal("admit state transition through Sentinel")
	}
	if _, err := store.Commit(ctx, *step.Step, Payload{ProtocolState: []byte("retained-via-sentinel")}, []byte("result")); err != nil {
		t.Fatal("commit state transition through Sentinel")
	}
	record, err := store.Load(ctx, step.Step.Record.Ref)
	if err != nil || string(record.Payload.ProtocolState) != "retained-via-sentinel" {
		t.Fatal("read actual retained state through Sentinel")
	}

	// The metadata exception must never apply to a configured direct data endpoint.
	cfg.Redis = RedisOptions{Address: address, DialTimeout: time.Second, IOTimeout: time.Second}
	direct, err := Open(ctx, cfg)
	if direct != nil {
		direct.Close()
	}
	standaloneRestartAuthorityRefusal(t, "Sentinel as direct data store", err)
}

func TestStateStandaloneRedisRestartTotalLoss(t *testing.T) {
	fixture := newStandaloneRestartRedis(t)
	admin := standaloneRestartAdmin(t, fixture.address)
	originalRunID := standaloneRestartRunID(t, admin)
	cfg := Config{Redis: RedisOptions{Address: fixture.address, DialTimeout: time.Second, IOTimeout: time.Second, PoolSize: 2},
		Prefix: "sipstate:restart-test", Namespace: "test-" + uuid.NewString(), NamespaceKey: bytes.Repeat([]byte{83}, 32), Partitions: 1,
		Versions: Versions{Schema: "2", Reducer: "9", Timers: "1"}}
	ctx := context.Background()
	original, err := Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer original.Close()
	group, err := original.GroupForCallID("standalone-restart@fixture.test")
	if err != nil {
		t.Fatal(err)
	}
	first, err := original.ReadUpdate(ctx, group, standaloneRestartInput("first", true))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := original.CompareAndSwap(ctx, *first.Update, Payload{ProtocolState: []byte("original")}, nil); err != nil {
		t.Fatal(err)
	}
	exact := first.Update.Record.Ref
	pending, err := original.ReadUpdate(ctx, exact, standaloneRestartInput("pending", false))
	if err != nil {
		t.Fatal(err)
	}
	fixture.stop(t, true)
	fixture.start(t)
	restarted := standaloneRestartAdmin(t, fixture.address)
	if standaloneRestartRunID(t, restarted) == originalRunID {
		t.Fatal("Redis process did not restart")
	}
	if size, err := restarted.DBSize(ctx).Result(); err != nil || size != 0 {
		t.Fatal("volatile Redis retained state", err)
	}
	if err := original.Ready(ctx); err != nil {
		t.Fatal("surviving store requires manual recovery", err)
	}
	if _, err := original.Load(ctx, exact); !errors.Is(err, ErrNotFound) {
		t.Fatal("lost context did not fail", err)
	}
	if _, err := original.CompareAndSwap(ctx, *pending.Update, Payload{}, nil); !errors.Is(err, ErrStale) {
		t.Fatal("old computation survived transport switch", err)
	}
	fresh, err := Open(ctx, cfg)
	if err != nil {
		t.Fatal("same configuration could not start", err)
	}
	defer fresh.Close()
	if fresh.Authority() != original.Authority() {
		t.Fatal("restart changed configuration identity")
	}
	created, err := fresh.ReadUpdate(ctx, group, standaloneRestartInput("new", true))
	if err != nil {
		t.Fatal(err)
	}
	if created.Update.Record.Ref.Incarnation == exact.Incarnation {
		t.Fatal("lost incarnation reused")
	}
	if _, err := fresh.CompareAndSwap(ctx, *created.Update, Payload{ProtocolState: []byte("replacement")}, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := original.Load(ctx, exact); !errors.Is(err, ErrStale) {
		t.Fatal("old context adopted replacement", err)
	}
	if _, err := original.CompareAndSwap(ctx, *pending.Update, Payload{}, nil); !errors.Is(err, ErrStale) {
		t.Fatal("stale computation changed replacement", err)
	}
	after, err := original.Load(ctx, created.Update.Record.Ref)
	if err != nil || string(after.Payload.ProtocolState) != "replacement" {
		t.Fatal("replacement unavailable", err)
	}
	if files, err := filepath.Glob(filepath.Join(fixture.dir, "*.rdb")); err != nil || len(files) != 0 {
		t.Fatal("volatile fixture persisted data")
	}
}
