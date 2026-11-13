package state

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func dispatchTestInput(id string, create bool) Input {
	in := contractInput(id, create)
	in.Kind, in.Proof = "timer", true
	return in
}

func dispatchScore(t *testing.T, backend Backend, worker string, ref Ref) (int64, bool) {
	t.Helper()
	switch store := backend.(type) {
	case *Store:
		score, err := store.client.ZScore(context.Background(), store.dispatchKey(worker), store.keys(ref)[0]).Result()
		if errors.Is(err, redis.Nil) {
			return 0, false
		}
		if err != nil {
			t.Fatal(err)
		}
		return int64(score), true
	case *MemoryStore:
		_, err := store.lock(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		defer store.unlock()
		q := store.dispatchQueues[worker]
		if q == nil {
			return 0, false
		}
		item := q.byID[ref.ID]
		if item == nil {
			return 0, false
		}
		return item.due, true
	default:
		t.Fatal("unknown store")
		return 0, false
	}
}

func TestDispatchFreshCommitGraceAndContinuingTargetScores(t *testing.T) {
	backendContract(t, func(t *testing.T, s Backend) {
		ctx := context.Background()
		a, b, c := strings.Repeat("a", 64), strings.Repeat("b", 64), strings.Repeat("c", 64)
		deadline := time.Now().Add(time.Minute).UnixMilli()
		other := contractRef(t, s, "earlier-other-group")
		otherUpdate := contractRead(t, s, other, dispatchTestInput("other", true))
		otherPayload := Payload{Effects: []Effect{{ID: "other", Kind: "deliver", Status: EffectAuthorized, DeadlineMS: deadline, Worker: a}}}
		if _, err := s.CompareAndSwap(ctx, otherUpdate, otherPayload, nil); err != nil {
			t.Fatal(err)
		}
		other = otherUpdate.Record.Ref
		otherScore, _ := dispatchScore(t, s, a, other)
		ref := contractRef(t, s, "local-grace")
		update := contractRead(t, s, ref, dispatchTestInput("first", true))
		payload := Payload{TimerWorker: a, Effects: []Effect{{ID: "local", Kind: "deliver", Status: EffectAuthorized, DeadlineMS: deadline, Worker: a}, {ID: "remote", Kind: "deliver", Status: EffectAuthorized, DeadlineMS: deadline, Worker: b}}}
		if _, err := s.CompareAndSwap(ctx, update, payload, nil); err != nil {
			t.Fatal(err)
		}
		ref = update.Record.Ref
		local, localOK := dispatchScore(t, s, a, ref)
		remote, remoteOK := dispatchScore(t, s, b, ref)
		if !localOK || !remoteOK || local != remote+100 {
			t.Fatalf("local and remote scores: %d %d", local, remote)
		}
		if otherScore >= local {
			t.Fatalf("test did not establish an earlier different group: %d >= %d", otherScore, local)
		}
		for i := 0; i < 3; i++ {
			time.Sleep(time.Millisecond)
			update = contractRead(t, s, ref, dispatchTestInput(fmt.Sprintf("repeat-%d", i), false))
			if _, err := s.CompareAndSwap(ctx, update, payload, nil); err != nil {
				t.Fatal(err)
			}
			got, _ := dispatchScore(t, s, a, ref)
			if got != local {
				t.Fatalf("continuing group score postponed or replaced by another group: %d != %d", got, local)
			}
		}
		if err := s.AckDispatch(ctx, a, ref, "1"); err != nil {
			t.Fatal(err)
		}
		if got, ok := dispatchScore(t, s, a, ref); !ok || got != local {
			t.Fatal("stale acknowledgement erased later revision")
		}
		update = contractRead(t, s, ref, dispatchTestInput("new-executor", false))
		payload.TimerWorker = c
		payload.Effects = append(payload.Effects, Effect{ID: "new-local", Kind: "deliver", Status: EffectAuthorized, DeadlineMS: deadline, Worker: c})
		if _, err := s.CompareAndSwap(ctx, update, payload, nil); err != nil {
			t.Fatal(err)
		}
		newLocal, ok := dispatchScore(t, s, c, ref)
		newRemote, _ := dispatchScore(t, s, a, ref)
		if !ok || newLocal < newRemote+100 {
			t.Fatalf("fresh executor did not receive its own grace: local=%d remote=%d", newLocal, newRemote)
		}
		view, _, err := s.ReadState(ctx, ref)
		if err != nil || view.TimerWorker != a {
			t.Fatal("fixture lost original timer owner", err)
		}
		update = contractRead(t, s, ref, dispatchTestInput("settled", false))
		if _, err := s.CompareAndSwap(ctx, update, Payload{TimerWorker: c}, nil); err != nil {
			t.Fatal(err)
		}
		for _, worker := range []string{a, b, c} {
			if _, ok := dispatchScore(t, s, worker, ref); ok {
				t.Fatal("settlement retained group backup", worker)
			}
		}
		if _, ok := dispatchScore(t, s, a, other); !ok {
			t.Fatal("settlement removed another group's index")
		}
	})
}

func TestMemoryDispatchGraceHonorsNativeTimerAndNeverPostpones(t *testing.T) {
	m, now := memoryClockStore(t, nil)
	ctx := context.Background()
	worker := strings.Repeat("d", 64)
	ref := contractRef(t, m, "memory-grace-exact")
	update := contractRead(t, m, ref, dispatchTestInput("first", true))
	due := now.UnixMilli() + 25
	payload := Payload{TimerWorker: worker, Effects: []Effect{{ID: "delivery", Kind: "deliver", Status: EffectAuthorized, DeadlineMS: now.Add(time.Minute).UnixMilli(), Worker: worker}}, Timers: []Timer{{ID: "native", Generation: "1", DueMS: due, DeadlineMS: due}}}
	if _, err := m.CompareAndSwap(ctx, update, payload, nil); err != nil {
		t.Fatal(err)
	}
	ref = update.Record.Ref
	score, ok := dispatchScore(t, m, worker, ref)
	if !ok || score != now.UnixMilli()+100 {
		t.Fatal("local grace is not exactly 100ms")
	}
	head, err := m.DispatchHead(ctx, worker)
	if err != nil || head.DueMS != due {
		t.Fatal("native timer did not precede backup", head, err)
	}
	*now = now.Add(30 * time.Millisecond)
	for i := 0; i < 3; i++ {
		u := contractRead(t, m, ref, dispatchTestInput(fmt.Sprintf("retry-%d", i), false))
		if _, err := m.CompareAndSwap(ctx, u, payload, nil); err != nil {
			t.Fatal(err)
		}
		*now = now.Add(20 * time.Millisecond)
	}
	if current, _ := dispatchScore(t, m, worker, ref); current != score {
		t.Fatal("repeated commits postponed backup")
	}
	hints, err := m.DispatchHints(ctx, worker, 8)
	if err != nil || len(hints) != 1 {
		t.Fatal("native timer should wake the worker", err)
	}
	update = contractRead(t, m, ref, dispatchTestInput("remove-timer", false))
	payload.Timers = nil
	if _, err := m.CompareAndSwap(ctx, update, payload, nil); err != nil {
		t.Fatal(err)
	}
	*now = now.Add(9 * time.Millisecond)
	hints, err = m.DispatchHints(ctx, worker, 8)
	if err != nil || len(hints) != 0 {
		t.Fatal("backup ran before its original deadline", err)
	}
	*now = now.Add(time.Millisecond)
	hints, err = m.DispatchHints(ctx, worker, 8)
	if err != nil || len(hints) != 1 || hints[0].DueMS != score {
		t.Fatal("backup did not become eligible on time", hints, err)
	}
	if err = m.AckDispatch(ctx, worker, ref, "1"); err != nil {
		t.Fatal(err)
	}
	if _, ok := dispatchScore(t, m, worker, ref); !ok {
		t.Fatal("stale ack removed backup")
	}
	update = contractRead(t, m, ref, dispatchTestInput("settled", false))
	if _, err = m.CompareAndSwap(ctx, update, Payload{TimerWorker: worker}, nil); err != nil {
		t.Fatal(err)
	}
	if _, ok := dispatchScore(t, m, worker, ref); ok {
		t.Fatal("settlement did not remove backup")
	}
}

func TestRedisDispatchRepairIsImmediateAndCorruptScoresFailBeforeCommit(t *testing.T) {
	s := testStore(t, nil)
	readyStore(t, s)
	ctx := context.Background()
	worker := strings.Repeat("e", 64)
	ref := group(t, s, "repair-grace")
	update := contractRead(t, s, ref, dispatchTestInput("create", true))
	payload := Payload{TimerWorker: worker, Effects: []Effect{{ID: "delivery", Kind: "deliver", Status: EffectAuthorized, DeadlineMS: time.Now().Add(time.Minute).UnixMilli(), Worker: worker}}}
	if _, err := s.CompareAndSwap(ctx, update, payload, nil); err != nil {
		t.Fatal(err)
	}
	ref = update.Record.Ref
	original, _ := dispatchScore(t, s, worker, ref)
	_, beforeNow, err := s.LoadAt(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.run(ctx, repairScript, ref, []interface{}{""}, true)
	if err != nil {
		t.Fatal(err)
	}
	repaired, _ := dispatchScore(t, s, worker, ref)
	_, afterNow, err := s.LoadAt(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	if repaired > original || repaired > afterNow || repaired < beforeNow && repaired != original {
		t.Fatalf("repair delayed work: %d vs %d", repaired, original)
	}
	key := s.keys(ref)[0]
	if err = s.client.ZAdd(ctx, s.dispatchKey(worker), redis.Z{Score: 1.5, Member: key}).Err(); err != nil {
		t.Fatal(err)
	}
	before, err := s.Load(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	update = contractRead(t, s, ref, dispatchTestInput("must-refuse", false))
	if _, err = s.CompareAndSwap(ctx, update, payload, nil); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("invalid dispatch score accepted: %v", err)
	}
	after, err := s.Load(ctx, ref)
	if err != nil || before.Revision != after.Revision {
		t.Fatal("invalid score partially committed protocol state", err)
	}
	score, err := s.client.ZScore(ctx, s.dispatchKey(worker), key).Result()
	if err != nil || score != 1.5 {
		t.Fatal("invalid score was mutated before refusal", score, err)
	}
}

func TestMemoryDispatchSettlementBeforeGraceRemovesOnlyItsTarget(t *testing.T) {
	m, now := memoryClockStore(t, nil)
	ctx := context.Background()
	worker := strings.Repeat("f", 64)
	ref := contractRef(t, m, "settled-before-grace")
	update := contractRead(t, m, ref, dispatchTestInput("first", true))
	payload := Payload{TimerWorker: worker, Effects: []Effect{{ID: "delivery", Kind: "deliver", Status: EffectAuthorized, DeadlineMS: now.Add(time.Minute).UnixMilli(), Worker: worker}}}
	if _, err := m.CompareAndSwap(ctx, update, payload, nil); err != nil {
		t.Fatal(err)
	}
	ref = update.Record.Ref
	score, ok := dispatchScore(t, m, worker, ref)
	if !ok || score != now.UnixMilli()+100 {
		t.Fatal("missing initial grace")
	}
	update = contractRead(t, m, ref, dispatchTestInput("settled-before-due", false))
	if _, err := m.CompareAndSwap(ctx, update, Payload{TimerWorker: worker}, nil); err != nil {
		t.Fatal(err)
	}
	if _, ok := dispatchScore(t, m, worker, ref); ok {
		t.Fatal("settled effect retained a backup")
	}
	*now = now.Add(100 * time.Millisecond)
	hints, err := m.DispatchHints(ctx, worker, 8)
	if err != nil || len(hints) != 0 {
		t.Fatal("settled effect reappeared", hints, err)
	}
}

type dropCommittedCASReply struct{ triggered atomic.Bool }

func (h *dropCommittedCASReply) DialHook(next redis.DialHook) redis.DialHook { return next }
func (h *dropCommittedCASReply) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}
func (h *dropCommittedCASReply) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if cmd.Name() != "evalsha" || !h.triggered.CompareAndSwap(false, true) {
			return next(ctx, cmd)
		}
		if err := next(ctx, cmd); err != nil {
			return err
		}
		return context.DeadlineExceeded
	}
}

func TestRedisLostCASReplyRetainsBoundedLocalBackup(t *testing.T) {
	s := testStore(t, nil)
	readyStore(t, s)
	ctx := context.Background()
	worker := strings.Repeat("a", 64)
	warm := contractRead(t, s, group(t, s, "warm-backup"), dispatchTestInput("warm", true))
	if _, err := s.CompareAndSwap(ctx, warm, Payload{}, nil); err != nil {
		t.Fatal(err)
	}
	ref := group(t, s, "lost-cas-backup")
	update := contractRead(t, s, ref, dispatchTestInput("lost-reply", true))
	payload := Payload{TimerWorker: worker, Effects: []Effect{{ID: "delivery", Kind: "deliver", Status: EffectAuthorized, DeadlineMS: time.Now().Add(time.Minute).UnixMilli(), Worker: worker}}}
	hook := new(dropCommittedCASReply)
	s.client.AddHook(hook)
	if _, err := s.CompareAndSwap(ctx, update, payload, nil); !errors.Is(err, ErrUnknown) {
		t.Fatalf("lost CAS reply was not uncertain: %v", err)
	}
	ref = update.Record.Ref
	record, now, err := s.LoadAt(ctx, ref)
	if err != nil || record.Revision != "1" || !hook.triggered.Load() {
		t.Fatal("fixture did not commit before losing the reply", err)
	}
	score, ok := dispatchScore(t, s, worker, ref)
	if !ok || score < update.NowMS+100 || score > now+100 {
		t.Fatalf("lost reply did not retain original backup score: score=%d read=%d now=%d", score, update.NowMS, now)
	}
	if wait := time.Until(time.UnixMilli(score)); wait > 0 {
		time.Sleep(wait + time.Millisecond)
	}
	hints, err := s.DispatchHints(ctx, worker, 8)
	if err != nil || len(hints) != 1 || hints[0].Ref.ID != ref.ID {
		t.Fatal("lost reply stranded local backup", hints, err)
	}
	record, err = s.Load(ctx, ref)
	if err != nil || record.Payload.Effects[0].Status != EffectAuthorized {
		t.Fatal("backup discovery changed physical evidence", err)
	}
}

func TestDispatchWildcardKeepsRemoteWorkerImmediate(t *testing.T) {
	backendContract(t, func(t *testing.T, s Backend) {
		ctx := context.Background()
		local, remote := strings.Repeat("a", 64), strings.Repeat("b", 64)
		for _, worker := range []string{local, remote} {
			if err := s.RegisterDispatchWorker(ctx, worker); err != nil {
				t.Fatal(err)
			}
		}
		ref := contractRef(t, s, "wildcard-grace")
		update := contractRead(t, s, ref, dispatchTestInput("wildcard", true))
		payload := Payload{TimerWorker: local, Effects: []Effect{{ID: "callback", Kind: "event", Status: EffectPending, DeadlineMS: time.Now().Add(time.Minute).UnixMilli(), Worker: AnyDispatchWorker}}}
		if _, err := s.CompareAndSwap(ctx, update, payload, nil); err != nil {
			t.Fatal(err)
		}
		ref = update.Record.Ref
		a, aOK := dispatchScore(t, s, local, ref)
		b, bOK := dispatchScore(t, s, remote, ref)
		if !aOK || !bOK || a != b+100 {
			t.Fatalf("wildcard expansion lost executor locality: %d %d", a, b)
		}
		hints, err := s.DispatchHints(ctx, remote, 8)
		if err != nil || len(hints) != 1 || hints[0].Ref.ID != ref.ID {
			t.Fatal("remote wildcard work was delayed", hints, err)
		}
	})
}
