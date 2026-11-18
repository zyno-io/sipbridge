package rediscore

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func expiringOutputFixture(t *testing.T) (*Store, *redis.Client, Identity, OutputIntent, *DueContext) {
	t.Helper()
	s, c, id, out, data := outputFixture(t)
	ctx := context.Background()
	timer := TimerAssignment{Worker: "worker2", Epoch: "timer-epoch", Term: "timer-term"}
	out.Deadline = time.Now().Add(200 * time.Millisecond)
	keys := []string{s.workerKey(timer.Worker), s.workerTimerKey(timer.Worker), s.contextDeadlineKey(id.Context)}
	t.Cleanup(func() {
		if e := c.Del(ctx, keys...).Err(); e != nil {
			t.Error(e)
		}
	})
	if err := c.HSet(ctx, s.workerKey(timer.Worker), "epoch", timer.Epoch, "expires_ms", time.Now().Add(time.Minute).UnixMilli()).Err(); err != nil {
		t.Fatal(err)
	}
	_, err := s.Commit(ctx, Publication{Identity: id, Create: true, Deadline: time.Now().Add(time.Minute), Changes: []Change{{Field: out.MessageField, Value: data}, {Field: out.MetadataField, Value: []byte("metadata")}}, NewOutputs: []OutputIntent{out}, Timers: &TimerMutation{Target: timer, Changes: []TimerChange{{Member: "out-metadata", Due: out.Deadline}}}})
	if err != nil {
		t.Fatal(err)
	}
	wait := time.Until(out.Deadline.Add(20 * time.Millisecond))
	if wait > 0 {
		time.Sleep(wait)
	}
	due, err := s.PeekDue(ctx, timer.Worker, timer.Epoch)
	if err != nil || due == nil {
		t.Fatal("due output unavailable", due, err)
	}
	return s, c, id, out, due
}
func TestExpireOutputPendingAndStartedProofs(t *testing.T) {
	t.Run("pending", func(t *testing.T) {
		s, c, id, out, due := expiringOutputFixture(t)
		ctx := context.Background()
		ref := outputRef(id, out)
		state, err := s.ExpireOutput(ctx, due, ref)
		if err != nil || state != OutputNotWritten {
			t.Fatal("pending cutoff", state, err)
		}
		if state, err = s.ExpireOutput(ctx, due, ref); err != nil || state != OutputNotWritten {
			t.Fatal("NOT_WRITTEN changed on repeat", state, err)
		}
		if n, err := c.ZCard(ctx, s.workerReadyKey(out.Worker)).Result(); err != nil || n != 0 {
			t.Fatal("pending ready retained", n, err)
		}
		checkNamespace(t, s, c, id)
		again, err := s.PeekDue(ctx, due.Assignment.Worker, due.Assignment.Epoch)
		if err != nil || again == nil || again.Member != due.Member {
			t.Fatal("expiry popped timer", again, err)
		}
		if err = s.CompleteOutput(ctx, ref, "WRITTEN"); !errors.Is(err, ErrConflict) {
			t.Fatal("late written contradicted absence proof", err)
		}
	})
	t.Run("started", func(t *testing.T) {
		s, c, id, out, data := outputFixture(t)
		ctx := context.Background()
		timer := TimerAssignment{Worker: "worker2", Epoch: "timer-epoch", Term: "timer-term"}
		out.Deadline = time.Now().Add(200 * time.Millisecond)
		keys := []string{s.workerKey(timer.Worker), s.workerTimerKey(timer.Worker), s.contextDeadlineKey(id.Context)}
		t.Cleanup(func() {
			if e := c.Del(ctx, keys...).Err(); e != nil {
				t.Error(e)
			}
		})
		if err := c.HSet(ctx, s.workerKey(timer.Worker), "epoch", timer.Epoch, "expires_ms", time.Now().Add(time.Minute).UnixMilli()).Err(); err != nil {
			t.Fatal(err)
		}
		if _, err := s.Commit(ctx, Publication{Identity: id, Create: true, Deadline: time.Now().Add(time.Minute), Changes: []Change{{Field: out.MessageField, Value: data}, {Field: out.MetadataField, Value: []byte("metadata")}}, NewOutputs: []OutputIntent{out}, Timers: &TimerMutation{Target: timer, Changes: []TimerChange{{Member: "out-metadata", Due: out.Deadline}}}}); err != nil {
			t.Fatal(err)
		}
		ref := outputRef(id, out)
		if _, err := s.ReserveOutput(ctx, ref); err != nil {
			t.Fatal(err)
		}
		wait := time.Until(out.Deadline.Add(20 * time.Millisecond))
		if wait > 0 {
			time.Sleep(wait)
		}
		due, err := s.PeekDue(ctx, timer.Worker, timer.Epoch)
		if err != nil || due == nil {
			t.Fatal("missing due", err)
		}
		state, err := s.ExpireOutput(ctx, due, ref)
		if err != nil || state != OutputUnknown {
			t.Fatal("started cutoff invented no-write", state, err)
		}
		if state, err = s.ExpireOutput(ctx, due, ref); err != nil || state != OutputUnknown {
			t.Fatal("UNKNOWN changed on repeat", state, err)
		}
		if err = s.CompleteOutput(ctx, ref, "WRITTEN"); err != nil {
			t.Fatal("late exact proof rejected", err)
		}
		state, err = s.ExpireOutput(ctx, due, ref)
		if err != nil || state != OutputWritten {
			t.Fatal("late written state changed", state, err)
		}
		checkNamespace(t, s, c, id)
	})
}
func TestExpireOutputFencesStaleDutyAndLease(t *testing.T) {
	s, c, id, out, due := expiringOutputFixture(t)
	ctx := context.Background()
	ref := outputRef(id, out)
	wrong := *due
	wrong.Member = "out-wrong"
	if _, err := s.ExpireOutput(ctx, &wrong, ref); !errors.Is(err, ErrConflict) {
		t.Fatal("wrong timer duty accepted", err)
	}
	wrong = *due
	wrong.DeadlineMS--
	if _, err := s.ExpireOutput(ctx, &wrong, ref); !errors.Is(err, ErrConflict) {
		t.Fatal("wrong deadline accepted", err)
	}
	wrong = *due
	wrong.Assignment.Worker = out.Worker
	wrong.Assignment.Epoch = out.Epoch
	if _, err := s.ExpireOutput(ctx, &wrong, ref); !errors.Is(err, ErrConflict) {
		t.Fatal("physical worker substituted executor", err)
	}
	if err := c.HSet(ctx, s.workerKey(due.Assignment.Worker), "expires_ms", time.Now().Add(-time.Second).UnixMilli()).Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ExpireOutput(ctx, due, ref); !errors.Is(err, ErrAuthority) {
		t.Fatal("expired executor accepted", err)
	}
	if err := c.HSet(ctx, s.workerKey(due.Assignment.Worker), "expires_ms", time.Now().Add(time.Minute).UnixMilli()).Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Commit(ctx, Publication{Identity: id, Revision: due.Revision, Deadline: time.Now().Add(time.Minute), Changes: []Change{{Field: "r:other", Value: []byte("new")}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ExpireOutput(ctx, due, ref); !errors.Is(err, ErrConflict) {
		t.Fatal("stale revision accepted", err)
	}
	inspection, err := s.InspectOutput(ctx, ref)
	if err != nil || inspection.State != OutputPending {
		t.Fatal("refusal changed output", inspection, err)
	}
	checkNamespace(t, s, c, id)
}
func TestExpireOutputLostReplyRemainsUnknown(t *testing.T) {
	s, c, id, out, due := expiringOutputFixture(t)
	ctx := context.Background()
	client := redis.NewClient(&redis.Options{Addr: c.Options().Addr, MaxRetries: -1})
	defer client.Close()
	client.AddHook(&loseReply{armed: true})
	peer, err := New(client, Config{Root: s.root, Profile: s.profile, RecordBudget: s.maxBytes, PrimaryRunID: s.primary})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = peer.ExpireOutput(ctx, due, outputRef(id, out)); !errors.Is(err, ErrUnknown) {
		t.Fatal("lost cutoff reply claimed outcome", err)
	}
	inspected, err := s.InspectOutput(ctx, outputRef(id, out))
	if err != nil || inspected.State != OutputNotWritten {
		t.Fatal("exact state not observable", inspected, err)
	}
	checkNamespace(t, s, c, id)
}
