package rediscore

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestDuePeekKeepsWorkUntilNativeCommit(t *testing.T) {
	s, client, id, out, _ := outputFixture(t)
	ctx := context.Background()
	a := TimerAssignment{Worker: out.Worker, Epoch: out.Epoch, Term: "timer-term"}
	t.Cleanup(func() {
		if err := client.Del(ctx, s.contextDeadlineKey(id.Context), s.workerTimerKey(out.Worker)).Err(); err != nil {
			t.Error(err)
		}
	})
	past := time.Now().Add(-time.Second)
	future := time.Now().Add(time.Hour)
	_, err := s.Commit(ctx, Publication{Identity: id, Create: true, Deadline: time.Now().Add(time.Minute),
		Timers: &TimerMutation{Target: a, Changes: []TimerChange{{Member: "procedure", Due: past}, {Member: "subscription", Due: future}}}})
	if err != nil {
		t.Fatal(err)
	}
	first, err := s.PeekDue(ctx, a.Worker, a.Epoch)
	if err != nil || first == nil || first.Identity != id || first.Revision != 1 || first.Assignment != a || first.Member != "procedure" || first.DeadlineMS != past.UnixMilli() || first.CheckedAtMS < first.DeadlineMS {
		t.Fatal("incorrect exact due context", first, err)
	}
	if first.Observed.IsZero() || first.Observed == first.Observed.Round(0) || first.Observed.After(time.Now()) {
		t.Fatal("due observation lost its monotonic request-start anchor")
	}
	next, err := s.PeekContextDeadline(ctx, id, 1, "procedure")
	if err != nil || next == nil || next.Member != "subscription" || next.DeadlineMS != future.UnixMilli() {
		t.Fatal("bounded next-record head did not skip current duty", next, err)
	}
	second, err := s.PeekDue(ctx, a.Worker, a.Epoch)
	if err != nil || second == nil || second.Revision != first.Revision || second.Member != first.Member {
		t.Fatal("peek lost uncommitted native work", second, err)
	}
	// An intervening transition wins the same revision. The old timer observer
	// cannot publish, and the still-current minimum remains indexed for retry.
	_, err = s.Commit(ctx, Publication{Identity: id, Revision: 1, Deadline: time.Now().Add(time.Minute), Changes: []Change{{Field: "r:context", Value: []byte{0x08, 1}}}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.PeekContextDeadline(ctx, id, 1, "procedure"); !errors.Is(err, ErrConflict) {
		t.Fatal("stale next-record observation accepted", err)
	}
	remove := Publication{Identity: first.Identity, Revision: first.Revision, Deadline: time.Now().Add(time.Minute),
		Timers: &TimerMutation{Expected: first.Assignment, Target: first.Assignment, RequireLease: true, Changes: []TimerChange{{Member: first.Member, Delete: true}}}}
	if _, err = s.Commit(ctx, remove); !errors.Is(err, ErrConflict) {
		t.Fatal("stale native transition committed", err)
	}
	current, err := s.PeekDue(ctx, a.Worker, a.Epoch)
	if err != nil || current == nil || current.Revision != 2 || current.Member != "procedure" {
		t.Fatal("CAS conflict lost duty", current, err)
	}
	remove.Revision = current.Revision
	if _, err = s.Commit(ctx, remove); err != nil {
		t.Fatal(err)
	}
	if due, err := s.PeekDue(ctx, a.Worker, a.Epoch); err != nil || due != nil {
		t.Fatal("future deadline fired early", due, err)
	}
	view, err := s.Read(ctx, id)
	if err != nil || view.Timer != a {
		t.Fatal("future duty lost assignment", err)
	}
}

func TestDuePeekFencesDeadOrReplacedWorker(t *testing.T) {
	s, client, _, out, _ := outputFixture(t)
	ctx := context.Background()
	if due, err := s.PeekDue(ctx, out.Worker, out.Epoch); err != nil || due != nil {
		t.Fatal("quiet worker has work", due, err)
	}
	if _, err := s.PeekDue(ctx, out.Worker, "other-epoch"); !errors.Is(err, ErrAuthority) {
		t.Fatal("replacement process used old worker lease", err)
	}
	if err := client.HSet(ctx, s.workerKey(out.Worker), "expires_ms", time.Now().Add(-time.Second).UnixMilli()).Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.PeekDue(ctx, out.Worker, out.Epoch); !errors.Is(err, ErrExpired) {
		t.Fatal("expired worker observed executable work", err)
	}
}
