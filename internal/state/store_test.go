package state

import (
	"bytes"
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

func testStore(t *testing.T, configure func(*Config)) *Store {
	t.Helper()
	address := os.Getenv("SIPBRIDGE_STATE_TEST_REDIS")
	if address == "" {
		t.Skip("set SIPBRIDGE_STATE_TEST_REDIS for real Redis qualification")
	}
	cfg := Config{Redis: RedisOptions{Address: address}, Prefix: "sipstate:test", Namespace: "test-" + uuid.NewString(), NamespaceKey: bytes.Repeat([]byte{9}, 32), Generation: "generation", Versions: Versions{Schema: "1", Reducer: "1", Timers: "1"}, Partitions: 2, ClaimTTL: time.Second}
	if configure != nil {
		configure(&cfg)
	}
	s, err := Open(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		var cursor uint64
		for {
			keys, next, err := s.client.Scan(context.Background(), cursor, s.root+"*", 128).Result()
			if err != nil {
				t.Error(err)
				break
			}
			if len(keys) > 0 {
				if err = s.client.Del(context.Background(), keys...).Err(); err != nil {
					t.Error(err)
				}
			}
			cursor = next
			if cursor == 0 {
				break
			}
		}
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	return s
}
func readyStore(t *testing.T, s *Store) {
	t.Helper()
	if err := s.Ready(context.Background()); err != nil {
		t.Fatal(err)
	}
}
func input(id string, create bool) Input {
	return Input{ID: id, Kind: "request", Fingerprint: "fingerprint-" + id, Payload: []byte(id), DeadlineMS: time.Now().Add(5 * time.Second).UnixMilli(), ReceiptUntilMS: time.Now().Add(10 * time.Second).UnixMilli(), Create: create}
}
func group(t *testing.T, s *Store, callID string) Ref {
	t.Helper()
	ref, err := s.GroupForCallID(callID)
	if err != nil {
		t.Fatal(err)
	}
	return ref
}
func begin(t *testing.T, s *Store, ref Ref, id string, create bool) *Step {
	t.Helper()
	result, err := s.Begin(context.Background(), ref, "worker-a", input(id, create))
	if err != nil {
		t.Fatal(err)
	}
	if result.Step == nil {
		t.Fatal("expected a computation claim")
	}
	return result.Step
}

func TestStateConfigOnlyStartupAndNamespace(t *testing.T) {
	s := testStore(t, nil)
	ctx := context.Background()
	ref := group(t, s, "Call-ID@EXAMPLE")
	readyStore(t, s)
	peer, err := Open(ctx, s.cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer peer.Close()
	if peer.Authority() != s.Authority() {
		t.Fatal("same authority differs across clients")
	}
	otherCase := group(t, s, "call-id@EXAMPLE")
	if otherCase.ID == ref.ID {
		t.Fatal("Call-ID was case folded")
	}
	if _, err := s.Begin(ctx, Ref{ID: ref.ID, Partition: ref.Partition, CallID: "borrowed"}, "worker", input("bad", true)); !errors.Is(err, ErrConflict) {
		t.Fatalf("borrowed key: %v", err)
	}
	step := begin(t, s, ref, "initial", true)
	if err = s.Ready(ctx); err != nil {
		t.Fatalf("idempotent complete bootstrap: %v", err)
	}
	if _, err = peer.Begin(ctx, ref, "worker-b", input("concurrent", true)); !errors.Is(err, ErrBusy) {
		t.Fatalf("parallel writer: %v", err)
	}
	if _, err = s.Commit(ctx, *step, Payload{ProtocolState: []byte("accepted")}, []byte("result")); err != nil {
		t.Fatal(err)
	}
	record, err := peer.Load(ctx, step.Record.Ref)
	if err != nil {
		t.Fatal(err)
	}
	if string(record.Payload.ProtocolState) != "accepted" || record.Revision != step.Claim.Token {
		t.Fatalf("wrong committed state: %+v", record)
	}
	if ttl, err := s.client.PTTL(ctx, s.keys(ref)[0]).Result(); err != nil || ttl != -time.Nanosecond {
		t.Fatalf("active group must not expire: %v %v", ttl, err)
	}
}

func TestStateRecoveryAndExactFence(t *testing.T) {
	s := testStore(t, nil)
	readyStore(t, s)
	ctx := context.Background()
	ref := group(t, s, "recovery")
	first := begin(t, s, ref, "original", true)
	if err := s.client.HSet(ctx, s.keys(ref)[0], "claimUntilMs", "0", "claimCounter", "9007199254740992").Err(); err != nil {
		t.Fatal(err)
	}
	candidate := input("next-arrival", false)
	result, err := s.Begin(ctx, ref, "worker-b", candidate)
	if err != nil {
		t.Fatal(err)
	}
	if !result.Step.Recovered || result.Step.Input.ID != "original" || result.Step.Claim.Token != "9007199254740993" {
		t.Fatalf("lost input or rounded fence: %+v", result.Step)
	}
	if _, err = s.Commit(ctx, *first, Payload{ProtocolState: []byte("stale")}, nil); !errors.Is(err, ErrStale) {
		t.Fatalf("stale commit accepted: %v", err)
	}
	if _, err = s.Commit(ctx, *result.Step, Payload{ProtocolState: []byte("recovered")}, []byte("done")); err != nil {
		t.Fatal(err)
	}
	record, err := s.Load(ctx, result.Step.Record.Ref)
	if err != nil {
		t.Fatal(err)
	}
	if record.Revision != "9007199254740993" || record.Pending != nil {
		t.Fatalf("bad recovered commit: %+v", record)
	}
	if _, err = s.Resume(ctx, record.Ref, "worker-b"); !errors.Is(err, ErrIdle) {
		t.Fatalf("idle resume admitted work: %v", err)
	}
	if err = s.client.HSet(ctx, s.keys(ref)[0], "claimCounter", "9223372036854775807").Err(); err != nil {
		t.Fatal(err)
	}
	if _, err = s.Begin(ctx, record.Ref, "worker-b", candidate); !errors.Is(err, ErrCapacity) {
		t.Fatalf("counter wrapped: %v", err)
	}
	if got := s.client.HGet(ctx, s.keys(ref)[0], "revision").Val(); got != "9007199254740993" {
		t.Fatalf("rejected admission changed revision: %s", got)
	}
}

func TestStateLostCommitReplyAndReceiptConflict(t *testing.T) {
	s := testStore(t, nil)
	readyStore(t, s)
	ctx := context.Background()
	ref := group(t, s, "commit-reply")
	request := input("op", true)
	started, err := s.Begin(ctx, ref, "worker", request)
	if err != nil {
		t.Fatal(err)
	}
	// Discarding the returned receipt models a successful commit whose control
	// response is lost. Inspection/repeated Begin must use retained evidence.
	if _, err = s.Commit(ctx, *started.Step, Payload{ProtocolState: []byte("once")}, []byte("known")); err != nil {
		t.Fatal(err)
	}
	inspection, err := s.InspectOperation(ctx, started.Step.Record.Ref, request.ID, request.Fingerprint)
	if err != nil {
		t.Fatal(err)
	}
	if inspection.Receipt == nil || string(inspection.Receipt.Outcome) != "known" {
		t.Fatal("lost commit cannot be inspected")
	}
	before := s.client.HGet(ctx, s.keys(ref)[0], "claimCounter").Val()
	if err = s.client.ScriptFlush(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	retry, err := s.Begin(ctx, ref, "another", request)
	if err != nil {
		t.Fatal(err)
	}
	if retry.Receipt == nil || retry.Step != nil || retry.Receipt.Revision != inspection.Receipt.Revision {
		t.Fatal("retry recomputed committed input")
	}
	if after := s.client.HGet(ctx, s.keys(ref)[0], "claimCounter").Val(); after != before {
		t.Fatal("receipt lookup allocated a new claim")
	}
	request.Payload = []byte("changed")
	if _, err = s.Begin(ctx, ref, "another", request); !errors.Is(err, ErrConflict) {
		t.Fatalf("changed same-ID input accepted: %v", err)
	}
	if _, err = s.InspectOperation(ctx, ref, "op", "different"); !errors.Is(err, ErrConflict) {
		t.Fatalf("fingerprint mismatch accepted: %v", err)
	}
}

func TestStateNamespaceACL(t *testing.T) {
	s := testStore(t, func(cfg *Config) { cfg.Versions = Versions{Schema: "1", Reducer: "1", Timers: "1"} })
	ctx := context.Background()
	user := "state-" + uuid.NewString()
	password := uuid.NewString()
	commands := []interface{}{"ACL", "SETUSER", user, "reset", "on", ">" + password, "~" + s.root + "*",
		"+hello", "+ping", "+client|setinfo", "+client|setname", "+info", "+mget", "+scan",
		"+get", "+set", "+time", "+type", "+exists", "+pttl", "+hget", "+hset", "+hincrby",
		"+hlen", "+hmget", "+zadd", "+zrem", "+zrange", "+zrangebyscore", "+xadd", "+xread", "+xrevrange", "+zcard", "+zscore", "+pexpire", "+persist", "+del", "+eval", "+evalsha"}
	if err := s.client.Do(ctx, commands...).Err(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.client.Do(ctx, "ACL", "DELUSER", user).Err(); err != nil {
			t.Error(err)
		}
	})
	cfg := s.cfg
	cfg.Redis.Username, cfg.Redis.Password = user, password
	restricted, err := Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer restricted.Close()
	readyStore(t, restricted)
	const leaseID = "original-controller"
	if absent, err := restricted.ReadApplicationLease(ctx, leaseID); err != nil || absent.Term != "" {
		t.Fatalf("restricted client could not prove lease absence: %+v %v", absent, err)
	}
	lease, err := restricted.AcquireApplicationLease(ctx, leaseID, uuid.NewString())
	if err != nil {
		t.Fatalf("restricted client could not acquire its lease: %v", err)
	}
	reconnected, err := restricted.AcquireApplicationLease(ctx, leaseID, uuid.NewString())
	if err != nil || reconnected.Term != lease.Term {
		t.Fatalf("restricted reconnect replaced the live term: %+v %v", reconnected, err)
	}
	renewed, err := restricted.RenewApplicationLease(ctx, leaseID, lease.Term)
	if err != nil || renewed.Term != lease.Term || renewed.ExpiresAtMS < reconnected.ExpiresAtMS {
		t.Fatalf("restricted client could not renew its live term: %+v %v", renewed, err)
	}
	observed, err := restricted.ReadApplicationLease(ctx, leaseID)
	if err != nil || observed.Term != lease.Term || observed.ExpiresAtMS != renewed.ExpiresAtMS {
		t.Fatalf("restricted client could not read its lease: %+v %v", observed, err)
	}
	ref := group(t, restricted, "acl")
	request := input("allowed", true)
	request.ApplicationLeaseIDs = []string{leaseID}
	admitted, err := restricted.Begin(ctx, ref, "worker", request)
	if err != nil || admitted.Step == nil || admitted.Step.Record.ApplicationLeases[leaseID].Term != lease.Term {
		t.Fatalf("restricted admission lost its atomic lease observation: %+v %v", admitted, err)
	}
	step := admitted.Step
	payload := Payload{ProtocolState: []byte("live-owner"), ApplicationLeaseIDs: []string{leaseID},
		CommitLeaseGuards: []ApplicationLeaseRef{{ID: leaseID, Term: lease.Term}}}
	if _, err = restricted.Commit(ctx, *step, payload, []byte("ok")); err != nil {
		t.Fatal(err)
	}
	loaded, err := restricted.Load(ctx, step.Record.Ref)
	if err != nil || loaded.ApplicationLeases[leaseID].Term != lease.Term || string(loaded.Payload.ProtocolState) != "live-owner" {
		t.Fatalf("restricted load lost the lease projection or guarded commit: %+v %v", loaded, err)
	}
	paused := begin(t, restricted, step.Record.Ref, "owner-expired-before-commit", false)
	expireApplicationLease(t, restricted, leaseID)
	if _, err = restricted.RenewApplicationLease(ctx, leaseID, lease.Term); !errors.Is(err, ErrExpired) {
		t.Fatalf("restricted client revived an expired term: %v", err)
	}
	if _, err = restricted.Commit(ctx, *paused, payload, []byte("must-not-commit")); !errors.Is(err, ErrStale) {
		t.Fatalf("restricted commit did not atomically reject the expired lease: %v", err)
	}
	successor, err := restricted.AcquireApplicationLease(ctx, leaseID, uuid.NewString())
	if err != nil || successor.Term == lease.Term {
		t.Fatalf("restricted client could not prune and replace its expired lease: %+v %v", successor, err)
	}
	resumed, err := restricted.Resume(ctx, step.Record.Ref, "replacement-worker")
	if err != nil || resumed.Step == nil || resumed.Step.Input.ID != paused.Input.ID || resumed.Step.Record.Revision != loaded.Revision ||
		resumed.Step.Record.ApplicationLeases[leaseID].Term != successor.Term {
		t.Fatalf("restricted recovery lost the pending input or fresh lease observation: %+v %v", resumed, err)
	}
	if _, err = restricted.Commit(ctx, *resumed.Step, payload, []byte("must-not-substitute-term")); !errors.Is(err, ErrStale) {
		t.Fatalf("restricted commit accepted the predecessor term after reacquisition: %v", err)
	}
	current, err := restricted.Load(ctx, step.Record.Ref)
	if err != nil || current.Pending == nil || current.Pending.ID != paused.Input.ID || current.Revision != loaded.Revision ||
		string(current.Payload.ProtocolState) != "live-owner" {
		t.Fatalf("restricted guard refusal changed or lost the pending operation: %+v %v", current, err)
	}
	for _, command := range [][]interface{}{
		{"GET", "zt:fw:ban"}, {"DEL", "zt:fw:ban"}, {"FLUSHDB"}, {"CONFIG", "SET", "maxmemory", "0"},
		{"EVAL", "return redis.call('GET',KEYS[1])", 1, "zt:fw:ban"},
		{"EVAL", "return redis.call('GET',KEYS[1])", 1, s.cfg.Prefix + ":foreign-namespace:authority"},
		{"HGETALL", s.root + "unused"}, {"HDEL", s.root + "unused", "field"},
	} {
		if err = restricted.client.Do(ctx, command...).Err(); err == nil || !strings.Contains(err.Error(), "NOPERM") {
			t.Fatalf("namespace/admin boundary not enforced for %s: %v", command[0], err)
		}
	}
}

func TestStateAdmissionExpiryDoesNotDropPending(t *testing.T) {
	s := testStore(t, nil)
	readyStore(t, s)
	ctx := context.Background()
	ref := group(t, s, "expired-input")
	request := input("expired", true)
	request.DeadlineMS = time.Now().Add(-time.Second).UnixMilli()
	if _, err := s.Begin(ctx, ref, "worker", request); !errors.Is(err, ErrExpired) {
		t.Fatalf("fresh expired input admitted: %v", err)
	}
	first := begin(t, s, ref, "pending", true)
	// Keep the pending bytes but simulate elapsed caller and computation budgets.
	first.Input.DeadlineMS = time.Now().Add(-time.Second).UnixMilli()
	digest, data, _ := digestInput(first.Input)
	if err := s.client.HSet(ctx, s.keys(ref)[0], "pending", data, "pendingDigest", digest, "claimUntilMs", "0").Err(); err != nil {
		t.Fatal(err)
	}
	resumed, err := s.Resume(ctx, ref, "reclaimer")
	if err != nil {
		t.Fatal(err)
	}
	if resumed.Step.Input.ID != "pending" || resumed.Step.Input.DeadlineMS >= resumed.Step.NowMS {
		t.Fatal("recovery erased the admitted late input")
	}
	if _, err = s.Commit(ctx, *resumed.Step, Payload{}, []byte("late completion")); err != nil {
		t.Fatal(err)
	}
}

func TestStateReceiptClockEncoding(t *testing.T) {
	s := testStore(t, nil)
	readyStore(t, s)
	ctx := context.Background()
	ref := group(t, s, "exact-expiry")
	request := input("exact-expiry", true)
	// Both values are exactly representable below 2^53. Redis Lua's cjson
	// renderer has a smaller default number precision, so receipt expiry must
	// retain its decimal string when it is re-encoded with the receipt list.
	request.DeadlineMS = 9000000000000000
	request.ReceiptUntilMS = 9000000000000001
	started, err := s.Begin(ctx, ref, "worker", request)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.Commit(ctx, *started.Step, Payload{}, nil); err != nil {
		t.Fatal(err)
	}
	inspection, err := s.InspectOperation(ctx, ref, request.ID, request.Fingerprint)
	if err != nil {
		t.Fatal(err)
	}
	if inspection.Receipt == nil || inspection.Receipt.ExpiresMS != request.ReceiptUntilMS {
		t.Fatal("receipt expiry rounded during Lua encoding")
	}
	retry, err := s.Begin(ctx, ref, "another", request)
	if err != nil || retry.Receipt == nil || retry.Receipt.ExpiresMS != request.ReceiptUntilMS {
		t.Fatalf("receipt replay lost expiry precision: %+v %v", retry, err)
	}
}

func TestStateCapacityVersionAndAuthority(t *testing.T) {
	s := testStore(t, func(cfg *Config) {
		cfg.Limits = Limits{GroupBytes: 16384, InputBytes: 1024, OutcomeBytes: 1024, Effects: 2, Timers: 2, Receipts: 2, Transactions: 2}
	})
	readyStore(t, s)
	ctx := context.Background()
	ref := group(t, s, "bounds")
	step := begin(t, s, ref, "one", true)
	if _, err := s.Commit(ctx, *step, Payload{ProtocolState: bytes.Repeat([]byte{1}, 20000)}, nil); !errors.Is(err, ErrCapacity) {
		t.Fatalf("oversized state accepted: %v", err)
	}
	if _, err := s.Commit(ctx, *step, Payload{ReservedCompletionBytes: 15000}, nil); !errors.Is(err, ErrCapacity) {
		t.Fatalf("completion reserve unaccounted: %v", err)
	}
	if _, err := s.Commit(ctx, *step, Payload{}, nil); err != nil {
		t.Fatal(err)
	}
	step = begin(t, s, step.Record.Ref, "two", false)
	if _, err := s.Commit(ctx, *step, Payload{}, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Begin(ctx, step.Record.Ref, "worker", input("three", false)); !errors.Is(err, ErrCapacity) {
		t.Fatalf("receipt cap: %v", err)
	}
	if err := s.client.HSet(ctx, s.keys(ref)[0], "versions", `{"schema":"other"}`).Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Load(ctx, ref); !errors.Is(err, ErrVersion) {
		t.Fatalf("incompatible state decoded: %v", err)
	}
	if err := s.client.HSet(ctx, s.keys(ref)[0], "authority", "different-authority").Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Load(ctx, ref); !errors.Is(err, ErrAuthority) {
		t.Fatalf("marker change ignored: %v", err)
	}
}

func TestStateHintRepairAndABARetirement(t *testing.T) {
	s := testStore(t, nil)
	readyStore(t, s)
	ctx := context.Background()
	ref := group(t, s, "retention")
	request := input("pending", true)
	request.DeadlineMS = time.Now().Add(time.Second).UnixMilli()
	request.ReceiptUntilMS = time.Now().Add(1100 * time.Millisecond).UnixMilli()
	started, err := s.Begin(ctx, ref, "worker", request)
	if err != nil {
		t.Fatal(err)
	}
	next := Payload{Timers: []Timer{{ID: "timeout", Generation: "1", DueMS: started.Step.NowMS, DeadlineMS: started.Step.NowMS + 1000}}}
	if _, err = s.Commit(ctx, *started.Step, next, nil); err != nil {
		t.Fatal(err)
	}
	if err = s.client.Del(ctx, s.keys(ref)[2]).Err(); err != nil {
		t.Fatal(err)
	}
	var cursor uint64
	for {
		cursor, _, err = s.RepairSchedule(ctx, cursor, 16)
		if err != nil {
			t.Fatal(err)
		}
		if cursor == 0 {
			break
		}
	}
	hints, err := s.DueHints(ctx, ref.Partition, 16)
	if err != nil || len(hints) != 1 || hints[0].Ref.ID != ref.ID {
		t.Fatalf("timer hint not repaired: %+v %v", hints, err)
	}
	if err = s.Retire(ctx, started.Step.Record.Ref); !errors.Is(err, ErrNotReady) {
		t.Fatalf("active timer retired: %v", err)
	}
	terminalInput := input("terminal", false)
	terminalInput.DeadlineMS = time.Now().Add(time.Second).UnixMilli()
	terminalInput.ReceiptUntilMS = terminalInput.DeadlineMS
	terminal, err := s.Begin(ctx, started.Step.Record.Ref, "worker", terminalInput)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.Commit(ctx, *terminal.Step, Payload{Retention: Retention{Terminal: true, UntilMS: time.Now().UnixMilli()}}, nil); err != nil {
		t.Fatal(err)
	}
	if err = s.Retire(ctx, terminal.Step.Record.Ref); !errors.Is(err, ErrNotReady) {
		t.Fatalf("unexpired receipt discarded: %v", err)
	}
	if hints, err := s.DueHints(ctx, ref.Partition, 16); err != nil || len(hints) != 0 {
		t.Fatalf("terminal group scheduled before receipt retention closes: %+v %v", hints, err)
	}
	// Expiry is store time; bounded polling avoids assuming an exact scheduler tick.
	deadline := time.Now().Add(3 * time.Second)
	for {
		err = s.Retire(ctx, terminal.Step.Record.Ref)
		if err == nil {
			break
		}
		if !errors.Is(err, ErrNotReady) || time.Now().After(deadline) {
			t.Fatal(err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	reused := begin(t, s, ref, "new-incarnation", true)
	if reused.Record.Ref.Incarnation == terminal.Step.Record.Ref.Incarnation {
		t.Fatal("incarnation reused after deletion")
	}
	if _, err = s.Load(ctx, terminal.Step.Record.Ref); !errors.Is(err, ErrStale) {
		t.Fatalf("old incarnation accepted: %v", err)
	}
	if _, err = s.Commit(ctx, *terminal.Step, Payload{}, nil); !errors.Is(err, ErrStale) {
		t.Fatalf("ABA commit accepted: %v", err)
	}
}

func TestStateActualPrimaryAndWrongHintTypes(t *testing.T) {
	s := testStore(t, nil)
	readyStore(t, s)
	ctx := context.Background()
	ref := group(t, s, "primary-identity")
	step := begin(t, s, ref, "one", true)
	if _, err := s.Commit(ctx, *step, Payload{}, nil); err != nil {
		t.Fatal(err)
	}
	if err := s.client.Set(ctx, s.keys(ref)[1], "wrong-type", 0).Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Begin(ctx, ref, "worker", input("two", false)); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("hint type not preflighted: %v", err)
	}
	if err := s.client.Del(ctx, s.keys(ref)[1]).Err(); err != nil {
		t.Fatal(err)
	}
	// Alter persisted evidence and the local expected identity together. The real
	// primary process check must still reject this apparent matching checkpoint.
	s.transport.Store(&primaryObservation{runID: strings.Repeat("0", 40)})
	if err := s.client.HSet(ctx, s.keys(ref)[0], "authority", s.authority.identity()).Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Load(ctx, ref); !errors.Is(err, ErrAuthority) {
		t.Fatalf("actual primary identity ignored: %v", err)
	}
}

func TestStateProofCompletionBypassesFullOrdinaryHistory(t *testing.T) {
	s := testStore(t, func(cfg *Config) { cfg.Limits.Receipts = 2 })
	readyStore(t, s)
	ctx := context.Background()
	ref := group(t, s, "proof-capacity")
	first := begin(t, s, ref, "ordinary-one", true)
	payload := Payload{ProtocolState: []byte("authoritative-native-state"), ReservedCompletionBytes: 4096}
	if _, err := s.Commit(ctx, *first, payload, []byte("ordinary-result")); err != nil {
		t.Fatal(err)
	}
	ref = first.Record.Ref
	second := begin(t, s, ref, "ordinary-two", false)
	if _, err := s.Commit(ctx, *second, payload, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Begin(ctx, ref, "worker", input("ordinary-three", false)); !errors.Is(err, ErrCapacity) {
		t.Fatalf("fresh work entered full history: %v", err)
	}
	completion := input("complete-attempt", false)
	completion.Kind = "complete"
	completion.Proof = true
	begun, err := s.Begin(ctx, ref, "sink", completion)
	if err != nil {
		t.Fatal(err)
	}
	payload.Effects = []Effect{{ID: "effect", Kind: "deliver", Status: EffectWritten, AttemptID: "same-attempt", DeadlineMS: completion.DeadlineMS}}
	if _, err = s.Commit(ctx, *begun.Step, payload, []byte("written")); err != nil {
		t.Fatal(err)
	}
	inspected, err := s.InspectOperation(ctx, ref, completion.ID, completion.Fingerprint)
	if err != nil || inspected.Receipt == nil {
		t.Fatalf("latest proof cannot reconcile lost ack: %+v %v", inspected, err)
	}
	record, err := s.Load(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	if len(record.Receipts) != 2 || record.Proof == nil || record.Proof.OperationID != completion.ID {
		t.Fatal("internal completion consumed ordinary history")
	}
	next := input("another-timer", false)
	next.Kind = "timer"
	next.Proof = true
	begun, err = s.Begin(ctx, ref, "scheduler", next)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.Commit(ctx, *begun.Step, payload, nil); err != nil {
		t.Fatal(err)
	}
	inspected, err = s.InspectOperation(ctx, ref, completion.ID, completion.Fingerprint)
	if err != nil || inspected.Receipt != nil {
		t.Fatalf("replaced proof invented operation history: %+v %v", inspected, err)
	}
	record, err = s.Load(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	if record.Payload.Effects[0].Status != EffectWritten || record.Payload.Effects[0].AttemptID != "same-attempt" {
		t.Fatal("replaced proof erased physical outcome authority")
	}
	if _, err = s.Begin(ctx, ref, "worker", input("ordinary-three", false)); !errors.Is(err, ErrCapacity) {
		t.Fatalf("internal proof evicted external reconciliation history: %v", err)
	}
}
