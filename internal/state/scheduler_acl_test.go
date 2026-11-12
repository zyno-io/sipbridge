package state

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

func TestSchedulerRequiredPermissionsFailBeforeProtocolMutation(t *testing.T) {
	for _, denied := range []string{"zrange", "xadd", "xread", "xrevrange"} {
		t.Run(denied, func(t *testing.T) {
			s := testStore(t, nil)
			ctx := context.Background()
			user := "scheduler-" + uuid.NewString()
			if err := s.client.ACLSetUser(ctx, user, "reset", "on", ">fixture-only", "~"+s.root+"*", "+@all", "-"+denied).Err(); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { s.client.ACLDelUser(ctx, user) })
			cfg := s.cfg
			cfg.Redis.Username, cfg.Redis.Password = user, "fixture-only"
			restricted, err := Open(ctx, cfg)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { restricted.Close() })
			ref := group(t, restricted, "no-partial-write")
			read, err := restricted.ReadUpdate(ctx, ref, input("create", true))
			if err != nil || read.Update == nil {
				t.Fatal(err)
			}
			if _, err := restricted.CompareAndSwap(ctx, *read.Update, Payload{Timers: []Timer{{ID: "timer", Generation: "1", DueMS: 1, DeadlineMS: 1}}}, nil); !errors.Is(err, ErrAuthority) {
				t.Fatal("missing scheduler permission did not refuse CAS", err)
			}
			keys := append(s.keys(ref), s.schedulerKeys()...)
			if exists := s.client.Exists(ctx, keys...).Val(); exists != 0 {
				t.Fatal("permission error left a partial protocol/index mutation", exists)
			}
		})
	}
}

func TestOwnedSchedulingWorksWithBoundedStateACL(t *testing.T) {
	admin := testStore(t, nil)
	ctx := context.Background()
	user := "owned-" + uuid.NewString()
	// Fixed supported command vocabulary, scoped to one SIP namespace. The
	// newer BYLEX mode reuses ZRANGE; no lex/count-specific grant is needed.
	permissions := strings.Fields("+hello +ping +quit +client|setinfo +client|setname +info +mget +scan +get +set +time +type +exists +pttl +hget +hstrlen +hset +hincrby +hlen +hmget +pexpire +persist +zcard +zscore +zadd +zrem +zrange +zrangebyscore +xadd +xread +xrevrange +del +eval +evalsha")
	rules := append([]string{"reset", "on", ">fixture-only", "~" + admin.root + "*"}, permissions...)
	if err := admin.client.ACLSetUser(ctx, user, rules...).Err(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { admin.client.ACLDelUser(ctx, user) })
	cfg := admin.cfg
	cfg.Redis.Username, cfg.Redis.Password = user, "fixture-only"
	s, err := Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	a, b := strings.Repeat("a", 64), strings.Repeat("b", 64)
	before, err := s.DispatchHead(ctx, a)
	if err != nil {
		t.Fatal(err)
	}
	for _, worker := range []string{a, b} {
		if err := s.RegisterDispatchWorker(ctx, worker); err != nil {
			t.Fatal(err)
		}
	}
	lease, err := s.AcquireApplicationLease(ctx, "acl-owner", uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	ref := group(t, s, "owned-acl")
	read, err := s.ReadUpdate(ctx, ref, input("create", true))
	if err != nil || read.Update == nil {
		t.Fatal(err)
	}
	due := read.Update.NowMS - 1
	payload := Payload{TimerWorker: a, ApplicationLeaseIDs: []string{lease.ID},
		ApplicationContexts: []ApplicationContextIndex{{LeaseID: lease.ID, LeaseTerm: lease.Term, ContextID: "context"}},
		CommitLeaseGuards:   []ApplicationLeaseRef{{ID: lease.ID, Term: lease.Term}},
		Timers:              []Timer{{ID: "timer", Generation: "1", DueMS: due, DeadlineMS: due}}}
	if _, err := s.CompareAndSwap(ctx, *read.Update, payload, nil); err != nil {
		t.Fatal(err)
	}
	ref = read.Update.Record.Ref
	listed, err := s.ListApplicationContexts(ctx, ApplicationLeaseRef{ID: lease.ID, Term: lease.Term}, "", 1)
	if err != nil || len(listed.Entries) != 1 {
		t.Fatal("bounded context listing needs an unexpected command", err)
	}
	wait, cancel := context.WithTimeout(ctx, time.Second)
	cursor, err := s.WaitDispatch(wait, a, before.Cursor)
	cancel()
	if err != nil || cursor == before.Cursor {
		t.Fatal("targeted notification needs an unexpected command", err)
	}
	if hints, err := s.DispatchHints(ctx, a, 1); err != nil || len(hints) != 1 {
		t.Fatal("owned timer lookup needs an unexpected command", err)
	}
	if err := admin.client.ZAdd(ctx, s.root+"timer:workers", redis.Z{Score: 1, Member: a}).Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.LeaseScheduler(ctx, "acl-coordinator", true); err != nil {
		t.Fatal(err)
	}
	if _, err := s.TakeoverTimers(ctx, "acl-coordinator", b, 1); err != nil {
		t.Fatal("timer takeover needs an unexpected command", err)
	}
	if err := s.AckDispatch(ctx, a, ref, "1"); err != nil {
		t.Fatal(err)
	}
	if err := admin.client.HSet(ctx, s.applicationLeaseKeys(lease.ID)[0], "expiresAtMs", due-2, "recoveryUntilMs", due-1).Err(); err != nil {
		t.Fatal(err)
	}
	if err := admin.client.ZAdd(ctx, s.applicationExpiryKeys(lease.ID)[0], redis.Z{Score: 1, Member: lease.ID}).Err(); err != nil {
		t.Fatal(err)
	}
	page, err := s.ApplicationExpiryPage(ctx, 1)
	if err != nil || len(page.Entries) != 1 {
		t.Fatal("owner expiry needs an unexpected command", err)
	}
	if err := s.AckApplicationExpiryPage(ctx, page); err != nil {
		t.Fatal(err)
	}
	for _, command := range []string{"ZCOUNT", "ZRANGEBYLEX"} {
		if err := s.client.Do(ctx, command, s.root+"probe", "-", "+").Err(); err == nil || !strings.Contains(err.Error(), "NOPERM") {
			t.Fatal("fixture accidentally granted a new command")
		}
	}
	if err := s.client.Get(ctx, cfg.Prefix+":foreign:probe").Err(); err == nil || !strings.Contains(err.Error(), "NOPERM") {
		t.Fatal("owned scheduling widened the namespace key scope")
	}
}

func TestSchedulerCorruptProjectionFailsBeforeProtocolMutation(t *testing.T) {
	for _, keyIndex := range []int{0, 1} {
		s := testStore(t, nil)
		ctx := context.Background()
		if err := s.client.Set(ctx, s.schedulerKeys()[keyIndex], "wrong-type", 0).Err(); err != nil {
			t.Fatal(err)
		}
		ref := group(t, s, uuid.NewString())
		read, err := s.ReadUpdate(ctx, ref, input("create", true))
		if err != nil || read.Update == nil {
			t.Fatal(err)
		}
		if _, err := s.CompareAndSwap(ctx, *read.Update, Payload{}, nil); !errors.Is(err, ErrCorrupt) {
			t.Fatal("wrong projection type did not refuse CAS", err)
		}
		if exists := s.client.Exists(ctx, s.keys(ref)...).Val(); exists != 0 {
			t.Fatal("projection error partially mutated protocol/index state", exists)
		}
	}
}
