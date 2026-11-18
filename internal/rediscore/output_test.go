package rediscore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"maps"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"sipbridge/internal/selectedmessage"
)

func outputFixture(t *testing.T) (*Store, *redis.Client, Identity, OutputIntent, []byte) {
	t.Helper()
	s, c, id := fixture(t)
	m, err := portable.BuildInitialRequest(portable.InitialRequest{Method: sip.NOTIFY, URI: "sip:phone@example.test", From: "<sip:alice@example.test>", CallID: "test-call", LocalTag: "tag", Branch: "z9hG4bK-test", CSeq: 1, Advertise: "192.0.2.1:5061", Transport: "tls"})
	if err != nil {
		t.Fatal(err)
	}
	data, err := selectedmessage.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	out := OutputIntent{ID: "output1", Attempt: "attempt1", Worker: "worker1", Epoch: "epoch1", Connection: "connection1", MessageField: "r:message", MetadataField: "r:metadata", WireHash: m.WireHash(), ProofHash: sha256.Sum256([]byte("test-proof")), Deadline: time.Now().Add(time.Minute)}
	ctx := context.Background()
	if err = c.HSet(ctx, s.workerKey(out.Worker), "epoch", out.Epoch, "expires_ms", time.Now().Add(time.Minute).UnixMilli()).Err(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := c.Del(ctx, s.workerKey(out.Worker), s.workerReadyKey(out.Worker)).Err(); err != nil {
			t.Error(err)
		}
	})
	return s, c, id, out, data
}
func mustOutputWire(t *testing.T, data []byte) []byte {
	t.Helper()
	m, err := selectedmessage.DecodeOpaque(data)
	if err != nil {
		t.Fatal(err)
	}
	return selectedmessage.ExactWire(m)
}
func outputRef(id Identity, o OutputIntent) OutputRef {
	return OutputRef{Context: id.Context, Incarnation: id.Incarnation, ID: o.ID, Attempt: o.Attempt, Worker: o.Worker, Epoch: o.Epoch, Connection: o.Connection, ProofHash: o.ProofHash, RequireApplication: o.Application != nil}
}
func publishOutput(t *testing.T, s *Store, id Identity, o OutputIntent, data []byte) {
	t.Helper()
	_, err := s.Commit(context.Background(), Publication{Identity: id, Create: true, Deadline: time.Now().Add(time.Minute), Changes: []Change{{Field: o.MessageField, Value: data}, {Field: o.MetadataField, Value: []byte("metadata")}}, NewOutputs: []OutputIntent{o}})
	if err != nil {
		t.Fatal(err)
	}
}

func TestOutputAtomicReservationAndRelease(t *testing.T) {
	s, c, id, o, data := outputFixture(t)
	ctx := context.Background()
	publishOutput(t, s, id, o, data)
	ref := outputRef(id, o)
	if _, err := s.Commit(ctx, Publication{Identity: id, Revision: 1, Deadline: time.Now().Add(time.Minute), Changes: []Change{{Field: o.MessageField, Delete: true}}}); !errors.Is(err, ErrConflict) {
		t.Fatal("referenced wire deleted", err)
	}
	wrong := ref
	wrong.Worker = "worker2"
	if got, err := s.ReserveOutput(ctx, wrong); !errors.Is(err, ErrConflict) || got.Wire != nil {
		t.Fatal("wrong worker received bytes", err)
	}
	wrong = ref
	wrong.Connection = "other-connection"
	if got, err := s.ReserveOutput(ctx, wrong); !errors.Is(err, ErrConflict) || got.Wire != nil {
		t.Fatal("wrong connection received bytes", err)
	}
	if err := s.CompleteOutput(ctx, wrong, "NOT_WRITTEN"); !errors.Is(err, ErrConflict) {
		t.Fatal("wrong connection completed attempt", err)
	}
	client := redis.NewClient(&redis.Options{Addr: c.Options().Addr, MaxRetries: -1})
	defer client.Close()
	peer, err := New(client, Config{Root: s.root, Profile: s.profile, RecordBudget: s.maxBytes, PrimaryRunID: s.primary})
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	var mu sync.Mutex
	writes, unknown := 0, 0
	for _, node := range []*Store{s, peer} {
		wg.Add(1)
		go func(node *Store) {
			defer wg.Done()
			got, err := node.ReserveOutput(ctx, ref)
			mu.Lock()
			defer mu.Unlock()
			if err == nil && got.Connection == ref.Connection && got.DeadlineMS == o.Deadline.UnixMilli() && got.LeaseExpiresMS > 0 && !got.Until.IsZero() && time.Now().Before(got.Until) && bytes.Equal(got.Wire, mustOutputWire(t, data)) {
				writes++
			} else if errors.Is(err, ErrUnknown) {
				unknown++
			} else {
				t.Errorf("reserve: %v", err)
			}
		}(node)
	}
	wg.Wait()
	if writes != 1 || unknown != 1 {
		t.Fatalf("writes=%d unknown=%d", writes, unknown)
	}
	if n, _ := c.ZCard(ctx, s.workerReadyKey(o.Worker)).Result(); n != 0 {
		t.Fatal("reserved output remained ready")
	}
	if _, err := s.Commit(ctx, Publication{Identity: id, Revision: 1, Deadline: time.Now().Add(time.Minute), Changes: []Change{{Field: "r:other", Value: []byte("changed")}}}); err != nil {
		t.Fatal(err)
	}
	if err := c.HSet(ctx, s.workerKey(o.Worker), "epoch", "replacement", "expires_ms", time.Now().Add(time.Minute).UnixMilli()).Err(); err != nil {
		t.Fatal(err)
	}
	if err := s.CompleteOutput(ctx, ref, "WRITTEN"); err != nil {
		t.Fatal("late exact completion", err)
	}
	if err := s.CompleteOutput(ctx, ref, "NOT_WRITTEN"); !errors.Is(err, ErrConflict) {
		t.Fatal("conflicting proof accepted", err)
	}
	if err := s.SettleOutput(ctx, ref, true, false); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Commit(ctx, Publication{Identity: id, Revision: 2, Deadline: time.Now().Add(time.Minute), Changes: []Change{{Field: o.MessageField, Delete: true}}}); !errors.Is(err, ErrConflict) {
		t.Fatal("unsettled body released", err)
	}
	if err := s.SettleOutput(ctx, ref, false, true); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Commit(ctx, Publication{Identity: id, Revision: 2, Deadline: time.Now().Add(time.Minute), Changes: []Change{{Field: o.MessageField, Delete: true}}}); err != nil {
		t.Fatal("released body cannot be deleted", err)
	}
}

func TestOutputLostReserveRemainsUnknown(t *testing.T) {
	s, c, id, o, data := outputFixture(t)
	publishOutput(t, s, id, o, data)
	ref := outputRef(id, o)
	client := redis.NewClient(&redis.Options{Addr: c.Options().Addr, MaxRetries: -1})
	defer client.Close()
	client.AddHook(&loseReply{armed: true})
	peer, err := New(client, Config{Root: s.root, Profile: s.profile, RecordBudget: s.maxBytes, PrimaryRunID: s.primary})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = peer.ReserveOutput(context.Background(), ref); !errors.Is(err, ErrUnknown) {
		t.Fatal("lost reserve was not unknown", err)
	}
	if got, err := s.ReserveOutput(context.Background(), ref); !errors.Is(err, ErrUnknown) || got.Wire != nil {
		t.Fatal("unknown attempt returned bytes", err)
	}
	if err = s.CompleteOutput(context.Background(), ref, "UNKNOWN"); err != nil {
		t.Fatal(err)
	}
	if err = s.SettleOutput(context.Background(), ref, true, true); !errors.Is(err, ErrConflict) {
		t.Fatal("unknown retired", err)
	}
}

func TestOutputAtomicCapacityAndLastACL(t *testing.T) {
	s, c, id, o, data := outputFixture(t)
	ctx := context.Background()
	second := o
	second.ID = "output2"
	second.Attempt = "attempt2"
	p := Publication{Identity: id, Create: true, Deadline: time.Now().Add(time.Minute), Changes: []Change{{Field: o.MessageField, Value: data}, {Field: o.MetadataField, Value: []byte("metadata")}}, NewOutputs: []OutputIntent{o, second}, Reserved: s.maxBytes}
	if _, err := s.Commit(ctx, p); !errors.Is(err, ErrCapacity) {
		t.Fatal("capacity accepted", err)
	}
	if exists, _ := c.Exists(ctx, s.contextKey(id.Context)).Result(); exists != 0 {
		t.Fatal("partial capacity publication")
	}
	user := "core-" + uuid.NewString()
	const password = "disposable-output-test"
	if err := c.ACLSetUser(ctx, user, "reset", "on", ">"+password, "~"+s.root+"*", "+hello", "+ping", "+client|setinfo", "+client|setname", "+eval", "+evalsha", "+get", "+type", "+exists", "+hget", "+hmget", "+hlen", "+hexists", "+time", "+hset", "+info", "+zadd", "+zscore").Err(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := c.ACLDelUser(ctx, user).Err(); err != nil {
			t.Error(err)
		}
	})
	client := redis.NewClient(&redis.Options{Addr: c.Options().Addr, Username: user, Password: password, MaxRetries: -1})
	defer client.Close()
	restricted, err := New(client, Config{Root: s.root, Profile: s.profile, RecordBudget: s.maxBytes, PrimaryRunID: s.primary})
	if err != nil {
		t.Fatal(err)
	}
	p.Reserved = 0
	if _, err = restricted.Commit(ctx, p); !errors.Is(err, ErrAuthority) {
		t.Fatal("denied last PUBLISH accepted", err)
	}
	if !strings.HasSuffix(err.Error(), "commit output-acl") {
		t.Fatal("output ACL refusal not identified", err)
	}
	if exists, _ := c.Exists(ctx, s.contextKey(id.Context)).Result(); exists != 0 {
		t.Fatal("ACL refusal partially committed")
	}
	before, _ := c.HGetAll(ctx, s.contextKey(id.Context)).Result()
	if !maps.Equal(before, map[string]string{}) {
		t.Fatal("context unexpectedly mutated")
	}
	if n, _ := c.ZCard(ctx, s.workerReadyKey(o.Worker)).Result(); n != 0 {
		t.Fatal("ACL refusal published ready entries")
	}
	if _, err = s.Commit(ctx, p); err != nil {
		t.Fatal(err)
	}
	if n, _ := c.ZCard(ctx, s.workerReadyKey(o.Worker)).Result(); n != 2 {
		t.Fatal("required outputs not published together", n)
	}
}

func TestOutputStaleEpochAndLostCompletion(t *testing.T) {
	s, c, id, o, data := outputFixture(t)
	ctx := context.Background()
	publishOutput(t, s, id, o, data)
	ref := outputRef(id, o)
	if err := c.HSet(ctx, s.workerKey(o.Worker), "epoch", "replacement").Err(); err != nil {
		t.Fatal(err)
	}
	if got, err := s.ReserveOutput(ctx, ref); !errors.Is(err, ErrAuthority) || got.Wire != nil {
		t.Fatal("stale epoch received bytes", err)
	}
	if n, _ := c.ZCard(ctx, s.workerReadyKey(o.Worker)).Result(); n != 1 {
		t.Fatal("failed reserve removed eligible output")
	}
	if err := c.HSet(ctx, s.workerKey(o.Worker), "epoch", o.Epoch).Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ReserveOutput(ctx, ref); err != nil {
		t.Fatal(err)
	}
	if err := c.HSet(ctx, s.contextKey(id.Context), "owner", "replacement-owner", "term", "replacement-term", "revision", "9").Err(); err != nil {
		t.Fatal(err)
	}
	client := redis.NewClient(&redis.Options{Addr: c.Options().Addr, MaxRetries: -1})
	defer client.Close()
	client.AddHook(&loseReply{armed: true})
	peer, err := New(client, Config{Root: s.root, Profile: s.profile, RecordBudget: s.maxBytes, PrimaryRunID: s.primary})
	if err != nil {
		t.Fatal(err)
	}
	if err = peer.CompleteOutput(ctx, ref, "WRITTEN"); !errors.Is(err, ErrUnknown) {
		t.Fatal("lost completion reply not unknown", err)
	}
	if err = s.CompleteOutput(ctx, ref, "WRITTEN"); err != nil {
		t.Fatal("exact repeated completion failed", err)
	}
	got, err := s.ReserveOutput(ctx, ref)
	if err != nil || got.Outcome != "WRITTEN" || got.Wire != nil {
		t.Fatal("known outcome replay", got, err)
	}
}

func TestOutputSharedWireAccountingAndWake(t *testing.T) {
	s, c, id, o, data := outputFixture(t)
	ctx := context.Background()
	other := o
	other.ID = "output2"
	other.Attempt = "attempt2"
	wake, err := s.SubscribeWake(ctx, o.Worker)
	if err != nil {
		t.Fatal(err)
	}
	defer wake.Close()
	_, ch, _ := wake.Snapshot()
	_, err = s.Commit(ctx, Publication{Identity: id, Create: true, Deadline: time.Now().Add(time.Minute), Changes: []Change{{Field: o.MessageField, Value: data}, {Field: o.MetadataField, Value: []byte("metadata")}}, NewOutputs: []OutputIntent{o, other}})
	if err != nil {
		t.Fatal(err)
	}
	if err = wake.Receive(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case <-ch:
	default:
		t.Fatal("output publication did not wake")
	}
	checkBytes := func() {
		t.Helper()
		m, e := c.HGetAll(ctx, s.contextKey(id.Context)).Result()
		if e != nil {
			t.Fatal(e)
		}
		total := 0
		for k, v := range m {
			if strings.HasPrefix(k, "r:") || strings.HasPrefix(k, "o:") || strings.HasPrefix(k, "ref:") {
				total += len(k) + len(v)
			}
		}
		if m["bytes"] != strconv.Itoa(total) {
			t.Fatalf("accounted=%s actual=%d", m["bytes"], total)
		}
	}
	checkBytes()
	r1, r2 := outputRef(id, o), outputRef(id, other)
	if err = s.CompleteOutput(ctx, r1, "NOT_WRITTEN"); err != nil {
		t.Fatal(err)
	}
	if err = s.SettleOutput(ctx, r1, true, true); err != nil {
		t.Fatal(err)
	}
	checkBytes()
	if _, err = s.Commit(ctx, Publication{Identity: id, Revision: 1, Deadline: time.Now().Add(time.Minute), Changes: []Change{{Field: o.MessageField, Delete: true}}}); !errors.Is(err, ErrConflict) {
		t.Fatal("second output lost shared wire", err)
	}
	if err = s.CompleteOutput(ctx, r2, "NOT_WRITTEN"); err != nil {
		t.Fatal(err)
	}
	if err = s.SettleOutput(ctx, r2, true, true); err != nil {
		t.Fatal(err)
	}
	checkBytes()
	if _, err = s.Commit(ctx, Publication{Identity: id, Revision: 1, Deadline: time.Now().Add(time.Minute), Changes: []Change{{Field: o.MessageField, Delete: true}}}); err != nil {
		t.Fatal(err)
	}
	checkBytes()
}

func TestOutputSettleDeniedDeleteDoesNotPartiallyMutate(t *testing.T) {
	s, c, id, o, data := outputFixture(t)
	ctx := context.Background()
	publishOutput(t, s, id, o, data)
	ref := outputRef(id, o)
	if err := s.CompleteOutput(ctx, ref, "NOT_WRITTEN"); err != nil {
		t.Fatal(err)
	}
	before, err := c.HGetAll(ctx, s.contextKey(id.Context)).Result()
	if err != nil {
		t.Fatal(err)
	}
	user := "core-" + uuid.NewString()
	const password = "disposable-settle-test"
	if err = c.ACLSetUser(ctx, user, "reset", "on", ">"+password, "~"+s.root+"*", "+hello", "+ping", "+client|setinfo", "+client|setname", "+eval", "+evalsha", "+get", "+type", "+hget", "+hmget", "+hlen", "+info", "+hset").Err(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := c.ACLDelUser(ctx, user).Err(); err != nil {
			t.Error(err)
		}
	})
	client := redis.NewClient(&redis.Options{Addr: c.Options().Addr, Username: user, Password: password, MaxRetries: -1})
	defer client.Close()
	peer, err := New(client, Config{Root: s.root, Profile: s.profile, RecordBudget: s.maxBytes, PrimaryRunID: s.primary})
	if err != nil {
		t.Fatal(err)
	}
	if err = peer.SettleOutput(ctx, ref, true, true); !errors.Is(err, ErrAuthority) {
		t.Fatal("denied delete accepted", err)
	}
	after, err := c.HGetAll(ctx, s.contextKey(id.Context)).Result()
	if err != nil || !maps.Equal(before, after) {
		t.Fatal("denied settle partially mutated context", err)
	}
}

func TestOutputLostPublicationReplyKeepsExactIntent(t *testing.T) {
	s, c, id, o, data := outputFixture(t)
	ctx := context.Background()
	client := redis.NewClient(&redis.Options{Addr: c.Options().Addr, MaxRetries: -1})
	defer client.Close()
	client.AddHook(&loseReply{armed: true})
	peer, err := New(client, Config{Root: s.root, Profile: s.profile, RecordBudget: s.maxBytes, PrimaryRunID: s.primary})
	if err != nil {
		t.Fatal(err)
	}
	p := Publication{Identity: id, Create: true, Deadline: time.Now().Add(time.Minute), Changes: []Change{{Field: o.MessageField, Value: data}, {Field: o.MetadataField, Value: []byte("metadata")}}, NewOutputs: []OutputIntent{o}}
	if _, err = peer.Commit(ctx, p); !errors.Is(err, ErrUnknown) {
		t.Fatal("lost commit response not unknown", err)
	}
	if _, err = s.Commit(ctx, p); !errors.Is(err, ErrConflict) {
		t.Fatal("uncertain commit could be admitted again", err)
	}
	got, err := s.ReserveOutput(ctx, outputRef(id, o))
	if err != nil || !bytes.Equal(got.Wire, mustOutputWire(t, data)) {
		t.Fatal("exact committed attempt lost", err)
	}
}

func TestOutputConflictingDigestNeverReturnsWire(t *testing.T) {
	s, c, id, o, data := outputFixture(t)
	ctx := context.Background()
	publishOutput(t, s, id, o, data)
	ref := outputRef(id, o)
	wrong := ref
	wrong.ProofHash[0] ^= 1
	if got, err := s.ReserveOutput(ctx, wrong); !errors.Is(err, ErrConflict) || got.Wire != nil {
		t.Fatal("different digest admitted", err)
	}
	if n, _ := c.ZCard(ctx, s.workerReadyKey(o.Worker)).Result(); n != 1 {
		t.Fatal("digest refusal changed ready index")
	}
}

func TestOutputRefusesOrphanReadyCollision(t *testing.T) {
	s, c, id, o, data := outputFixture(t)
	ctx := context.Background()
	if err := c.ZAdd(ctx, s.workerReadyKey(o.Worker), redis.Z{Score: float64(o.Deadline.UnixMilli()), Member: id.Context + "/" + o.ID}).Err(); err != nil {
		t.Fatal(err)
	}
	_, err := s.Commit(ctx, Publication{Identity: id, Create: true, Deadline: time.Now().Add(time.Minute), Changes: []Change{{Field: o.MessageField, Value: data}, {Field: o.MetadataField, Value: []byte("metadata")}}, NewOutputs: []OutputIntent{o}})
	if !errors.Is(err, ErrCorrupt) {
		t.Fatal("orphan ready collision accepted", err)
	}
	if n, _ := c.Exists(ctx, s.contextKey(id.Context)).Result(); n != 0 {
		t.Fatal("collision partially published context")
	}
}

type delayReserveReply struct {
	delay  time.Duration
	cancel context.CancelFunc
	armed  bool
}

func (h *delayReserveReply) DialHook(next redis.DialHook) redis.DialHook { return next }
func (h *delayReserveReply) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}
func (h *delayReserveReply) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		err := next(ctx, cmd)
		if err == nil && h.armed && (cmd.Name() == "evalsha" || cmd.Name() == "eval") {
			h.armed = false
			if h.cancel != nil {
				h.cancel()
			}
			if h.delay > 0 {
				time.Sleep(h.delay)
			}
		}
		return err
	}
}

func TestOutputLateOrCancelledReserveReplyNeverReturnsWire(t *testing.T) {
	for _, name := range []string{"deadline", "cancelled", "lease"} {
		t.Run(name, func(t *testing.T) {
			s, c, id, o, data := outputFixture(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			hook := &delayReserveReply{armed: true}
			if name == "deadline" {
				o.Deadline = time.Now().Add(150 * time.Millisecond)
				hook.delay = 250 * time.Millisecond
			} else if name == "cancelled" {
				hook.cancel = cancel
			} else {
				hook.delay = 80 * time.Millisecond
			}
			publishOutput(t, s, id, o, data)
			ref := outputRef(id, o)
			client := redis.NewClient(&redis.Options{Addr: c.Options().Addr, MaxRetries: -1})
			defer client.Close()
			client.AddHook(hook)
			peer, err := New(client, Config{Root: s.root, Profile: s.profile, RecordBudget: s.maxBytes, PrimaryRunID: s.primary})
			if err != nil {
				t.Fatal(err)
			}
			if name == "lease" {
				// Establish the connection before shortening the lease. A cold
				// dial under host contention must not turn this post-commit lost
				// authority case into a correct pre-write lease refusal.
				if err := client.Ping(ctx).Err(); err != nil {
					t.Fatal(err)
				}
				if err := c.HSet(ctx, s.workerKey(o.Worker), "expires_ms", time.Now().Add(60*time.Millisecond).UnixMilli()).Err(); err != nil {
					t.Fatal(err)
				}
			}
			result, err := peer.ReserveOutput(ctx, ref)
			if !errors.Is(err, ErrUnknown) || result.Wire != nil || !result.Until.IsZero() {
				t.Fatal("late/cancelled reserve returned bytes", err)
			}
			phase, err := c.HGet(context.Background(), s.contextKey(id.Context), "o:"+o.ID+":s").Result()
			if err != nil || phase != "S" {
				t.Fatal("fixture did not commit STARTED", phase, err)
			}
			result, err = s.ReserveOutput(context.Background(), ref)
			if !errors.Is(err, ErrUnknown) || result.Wire != nil {
				t.Fatal("second reserve returned bytes", err)
			}
		})
	}
}

func TestOutputReserveCutoffRemainsBoundToObservedLease(t *testing.T) {
	s, c, id, o, data := outputFixture(t)
	ctx := context.Background()
	publishOutput(t, s, id, o, data)
	leaseMS := time.Now().Add(3 * time.Second).UnixMilli()
	if err := c.HSet(ctx, s.workerKey(o.Worker), "expires_ms", leaseMS).Err(); err != nil {
		t.Fatal(err)
	}
	result, err := s.ReserveOutput(ctx, outputRef(id, o))
	if err != nil {
		t.Fatal(err)
	}
	if result.LeaseExpiresMS != leaseMS || result.DeadlineMS != o.Deadline.UnixMilli() || result.Until.After(time.UnixMilli(leaseMS).Add(2*time.Millisecond)) {
		t.Fatal("reservation cutoff exceeded observed lease", result)
	}
	original := result.Until
	if err = c.HSet(ctx, s.workerKey(o.Worker), "expires_ms", leaseMS+10000).Err(); err != nil {
		t.Fatal(err)
	}
	if !result.Until.Equal(original) {
		t.Fatal("renewal extended existing permit")
	}
}

func TestOutputApplicationAuthorityAndMetadataPin(t *testing.T) {
	s, c, id, out, data := outputFixture(t)
	ctx := context.Background()
	app, err := s.AcquireApplication(ctx, "application1", "term1")
	if err != nil {
		t.Fatal(err)
	}
	appKeys := []string{s.applicationKey("application1"), s.applicationOwnerKey("term1"), s.applicationOwnedKey("term1"), s.applicationAdmissionKey(), s.ownerExpiryKey()}
	t.Cleanup(func() {
		if e := c.Del(ctx, appKeys...).Err(); e != nil {
			t.Error(e)
		}
	})
	id.Owner, id.Term = app.Application, app.Term
	out.Application = &app.ApplicationLeaseRef
	out.DeliveryTag = sha256.Sum256([]byte("delivery"))
	p := Publication{Identity: id, Create: true, Application: &app.ApplicationLeaseRef, Deadline: time.Now().Add(time.Minute), Changes: []Change{{Field: out.MessageField, Value: data}, {Field: out.MetadataField, Value: []byte("metadata")}, {Field: "r:delivery-tag", Value: out.DeliveryTag[:]}}, NewOutputs: []OutputIntent{out}}
	if _, err = s.Commit(ctx, p); err != nil {
		t.Fatal(err)
	}
	ref := outputRef(id, out)
	wrong := ref
	wrong.ProofHash[0] ^= 1
	if _, err = s.ReserveOutput(ctx, wrong); !errors.Is(err, ErrConflict) {
		t.Fatal("altered proof reserved", err)
	}
	wrong = ref
	wrong.RequireApplication = false
	if _, err = s.ReserveOutput(ctx, wrong); !errors.Is(err, ErrConflict) {
		t.Fatal("wrong authority class reserved", err)
	}
	if _, err = s.Commit(ctx, Publication{Identity: id, Revision: 1, Application: &app.ApplicationLeaseRef, Deadline: time.Now().Add(time.Minute), Changes: []Change{{Field: out.MetadataField, Delete: true}}}); !errors.Is(err, ErrConflict) {
		t.Fatal("pinned metadata deleted", err)
	}
	changed := sha256.Sum256([]byte("changed"))
	if _, err = s.Commit(ctx, Publication{Identity: id, Revision: 1, Application: &app.ApplicationLeaseRef, Deadline: time.Now().Add(time.Minute), Changes: []Change{{Field: "r:delivery-tag", Value: changed[:]}}}); err != nil {
		t.Fatal(err)
	}
	if _, err = s.ReserveOutput(ctx, ref); !errors.Is(err, ErrAuthority) {
		t.Fatal("changed delivery reserved", err)
	}
	if _, err = s.Commit(ctx, Publication{Identity: id, Revision: 2, Application: &app.ApplicationLeaseRef, Deadline: time.Now().Add(time.Minute), Changes: []Change{{Field: "r:delivery-tag", Value: out.DeliveryTag[:]}}}); err != nil {
		t.Fatal(err)
	}
	if err = c.HSet(ctx, s.applicationOwnerKey(app.Term), "expires_ms", time.Now().Add(-time.Second).UnixMilli()).Err(); err != nil {
		t.Fatal(err)
	}
	if _, err = s.ReserveOutput(ctx, ref); !errors.Is(err, ErrExpired) {
		t.Fatal("expired app term reserved", err)
	}
	if n, err := c.ZCard(ctx, s.workerReadyKey(out.Worker)).Result(); err != nil || n != 1 {
		t.Fatal("refusal consumed output", n, err)
	}
}
func TestOutputReadyPeekDoesNotReserveOrReadWire(t *testing.T) {
	s, c, id, out, data := outputFixture(t)
	ctx := context.Background()
	publishOutput(t, s, id, out, data)
	ready, err := s.PeekReady(ctx, out.Worker, out.Epoch, 1)
	if err != nil || len(ready) != 1 || !bytes.Equal(ready[0].Metadata, []byte("metadata")) || ready[0].MetadataField != out.MetadataField || ready[0].Ref != outputRef(id, out) {
		t.Fatal("ready metadata mismatch", ready, err)
	}
	if n, err := c.ZCard(ctx, s.workerReadyKey(out.Worker)).Result(); err != nil || n != 1 {
		t.Fatal("peek reserved output", n, err)
	}
	if _, err = s.ReserveOutput(ctx, ready[0].Ref); err != nil {
		t.Fatal(err)
	}
	ready, err = s.PeekReady(ctx, out.Worker, out.Epoch, 1)
	if err != nil || len(ready) != 0 {
		t.Fatal("reserved output still visible", ready, err)
	}
}

func TestOutputApplicationOwnerTransferFencesPending(t *testing.T) {
	s, c, id, out, data := outputFixture(t)
	ctx := context.Background()
	first, err := s.AcquireApplication(ctx, "application1", "term1")
	if err != nil {
		t.Fatal(err)
	}
	second, err := s.AcquireApplication(ctx, "application2", "term2")
	if err != nil {
		t.Fatal(err)
	}
	keys := []string{s.applicationKey("application1"), s.applicationKey("application2"), s.applicationOwnerKey("term1"), s.applicationOwnerKey("term2"), s.applicationOwnedKey("term1"), s.applicationOwnedKey("term2"), s.applicationAdmissionKey(), s.ownerExpiryKey()}
	t.Cleanup(func() {
		if e := c.Del(ctx, keys...).Err(); e != nil {
			t.Error(e)
		}
	})
	id.Owner, id.Term = first.Application, first.Term
	out.Application = &first.ApplicationLeaseRef
	out.DeliveryTag = sha256.Sum256([]byte("delivery"))
	if _, err = s.Commit(ctx, Publication{Identity: id, Create: true, Application: &first.ApplicationLeaseRef, Deadline: time.Now().Add(time.Minute), Changes: []Change{{Field: out.MessageField, Value: data}, {Field: out.MetadataField, Value: []byte("metadata")}, {Field: "r:delivery-tag", Value: out.DeliveryTag[:]}}, NewOutputs: []OutputIntent{out}}); err != nil {
		t.Fatal(err)
	}
	ref := outputRef(id, out)
	if _, err = s.Commit(ctx, Publication{Identity: id, Revision: 1, Application: &second.ApplicationLeaseRef, BindApplication: true, Deadline: time.Now().Add(time.Minute)}); err != nil {
		t.Fatal(err)
	}
	if _, err = s.ReserveOutput(ctx, ref); !errors.Is(err, ErrAuthority) {
		t.Fatal("old owner output reserved", err)
	}
	if err = s.CompleteOutput(ctx, ref, "NOT_WRITTEN"); err != nil {
		t.Fatal("independent exact completion failed", err)
	}
}
func TestOutputReadyPeekIsBoundedAndSkipsExpired(t *testing.T) {
	s, c, id, out, data := outputFixture(t)
	ctx := context.Background()
	other := out
	other.ID = "output2"
	other.Attempt = "attempt2"
	other.Deadline = out.Deadline.Add(time.Second)
	if _, err := s.Commit(ctx, Publication{Identity: id, Create: true, Deadline: time.Now().Add(time.Minute), Changes: []Change{{Field: out.MessageField, Value: data}, {Field: out.MetadataField, Value: []byte("metadata")}}, NewOutputs: []OutputIntent{out, other}}); err != nil {
		t.Fatal(err)
	}
	first, err := s.PeekReady(ctx, out.Worker, out.Epoch, 1)
	if err != nil || len(first) != 1 || first[0].Ref.ID != out.ID {
		t.Fatal("bounded peek", first, err)
	}
	again, err := s.PeekReady(ctx, out.Worker, out.Epoch, 1)
	if err != nil || len(again) != 1 || again[0].Ref != first[0].Ref {
		t.Fatal("peek consumed output", again, err)
	}
	if err = c.ZAdd(ctx, s.workerReadyKey(out.Worker), redis.Z{Member: id.Context + "/" + out.ID, Score: float64(time.Now().Add(-time.Second).UnixMilli())}).Err(); err != nil {
		t.Fatal(err)
	}
	remaining, err := s.PeekReady(ctx, out.Worker, out.Epoch, 2)
	if err != nil || len(remaining) != 1 || remaining[0].Ref.ID != other.ID {
		t.Fatal("expired entry eligible", remaining, err)
	}
}

func TestOutputApplicationRequiresCurrentDeliveryTag(t *testing.T) {
	s, c, id, out, data := outputFixture(t)
	ctx := context.Background()
	lease, err := s.AcquireApplication(ctx, "application1", "term1")
	if err != nil {
		t.Fatal(err)
	}
	keys := []string{s.applicationKey("application1"), s.applicationOwnerKey("term1"), s.applicationOwnedKey("term1"), s.applicationAdmissionKey(), s.ownerExpiryKey()}
	t.Cleanup(func() {
		if e := c.Del(ctx, keys...).Err(); e != nil {
			t.Error(e)
		}
	})
	id.Owner, id.Term = lease.Application, lease.Term
	out.Application = &lease.ApplicationLeaseRef
	out.DeliveryTag = sha256.Sum256([]byte("delivery"))
	_, err = s.Commit(ctx, Publication{Identity: id, Create: true, Application: &lease.ApplicationLeaseRef, Deadline: time.Now().Add(time.Minute), Changes: []Change{{Field: out.MessageField, Value: data}, {Field: out.MetadataField, Value: []byte("metadata")}}, NewOutputs: []OutputIntent{out}})
	if !errors.Is(err, ErrConflict) {
		t.Fatal("missing delivery tag accepted", err)
	}
	if n, err := c.Exists(ctx, s.contextKey(id.Context), s.workerReadyKey(out.Worker), s.capacityKey()).Result(); err != nil || n != 0 {
		t.Fatal("refused output published partial state", n, err)
	}
}

func TestOutputApplicationLeaseCapsReservationUntil(t *testing.T) {
	s, c, id, out, data := outputFixture(t)
	ctx := context.Background()
	lease, err := s.AcquireApplication(ctx, "application1", "term1")
	if err != nil {
		t.Fatal(err)
	}
	keys := []string{s.applicationKey("application1"), s.applicationOwnerKey("term1"), s.applicationOwnedKey("term1"), s.applicationAdmissionKey(), s.ownerExpiryKey()}
	t.Cleanup(func() {
		if e := c.Del(ctx, keys...).Err(); e != nil {
			t.Error(e)
		}
	})
	id.Owner, id.Term = lease.Application, lease.Term
	out.Application = &lease.ApplicationLeaseRef
	out.DeliveryTag = sha256.Sum256([]byte("delivery"))
	if _, err = s.Commit(ctx, Publication{Identity: id, Create: true, Application: &lease.ApplicationLeaseRef, Deadline: time.Now().Add(time.Minute), Changes: []Change{{Field: out.MessageField, Value: data}, {Field: out.MetadataField, Value: []byte("metadata")}, {Field: "r:delivery-tag", Value: out.DeliveryTag[:]}}, NewOutputs: []OutputIntent{out}}); err != nil {
		t.Fatal(err)
	}
	appExpiry := time.Now().Add(1200 * time.Millisecond).UnixMilli()
	if err = c.HSet(ctx, s.applicationOwnerKey(lease.Term), "expires_ms", appExpiry).Err(); err != nil {
		t.Fatal(err)
	}
	result, err := s.ReserveOutput(ctx, outputRef(id, out))
	if err != nil {
		t.Fatal(err)
	}
	if result.ApplicationExpiresMS != appExpiry || result.Until.After(time.UnixMilli(appExpiry).Add(2*time.Millisecond)) {
		t.Fatal("app lease did not bound socket start", result)
	}
}
