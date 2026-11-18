package rediscore

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestForgetObsoleteCallOutputRequiresExactLaterTerminalHead(t *testing.T) {
	s, c, id, out, old := obsoleteOutputFixture(t, "U", true)
	ctx := context.Background()
	firstEnd := old.DeadlineMS + 500
	_, err := s.Commit(ctx, Publication{Identity: id, Revision: old.Revision, Deadline: time.Now().Add(time.Second), Timers: &TimerMutation{Expected: old.Assignment, Target: old.Assignment, RequireLease: true, Changes: []TimerChange{{Member: old.Member, Delete: true}, {Member: "call-retire", Due: time.UnixMilli(firstEnd)}}}})
	if err != nil {
		t.Fatal(err)
	}
	due, err := s.PeekDue(ctx, old.Assignment.Worker, old.Assignment.Epoch)
	if err != nil || due == nil || due.Member != "call-retire" {
		t.Fatal("terminal head unavailable", due, err)
	}
	ref := outputRef(id, out)
	if err = s.ForgetObsoleteCallOutput(ctx, due, ref, firstEnd, false); !errors.Is(err, ErrConflict) {
		t.Fatal("missing native witness accepted", err)
	}
	if err = s.ForgetObsoleteCallOutput(ctx, due, ref, firstEnd-1, true); !errors.Is(err, ErrConflict) {
		t.Fatal("different original cutoff accepted", err)
	}
	stale := *due
	stale.Revision++
	if err = s.ForgetObsoleteCallOutput(ctx, &stale, ref, firstEnd, true); !errors.Is(err, ErrConflict) {
		t.Fatal("stale revision accepted", err)
	}
	if err = s.ForgetObsoleteCallOutput(ctx, due, ref, firstEnd, true); err != nil {
		t.Fatal(err)
	}
	if _, err = s.InspectOutput(ctx, ref); !errors.Is(err, ErrAbsent) {
		t.Fatal("unknown proof not released", err)
	}
	checkNamespace(t, s, c, id)
	retained, err := s.ObserveContext(ctx, id.Context, id.Incarnation, "r:context", "r:procedure", out.MetadataField, out.MessageField)
	if err != nil || retained.View.Records[out.MetadataField] == nil || retained.View.Records[out.MessageField] == nil || string(retained.View.Records["r:context"]) != "ended" {
		t.Fatal("terminal records changed", err)
	}
	next, err := s.PeekDue(ctx, due.Assignment.Worker, due.Assignment.Epoch)
	if err != nil || next == nil || next.Member != "call-retire" || next.DeadlineMS != firstEnd {
		t.Fatal("terminal head changed", next, err)
	}
}

func TestForgetObsoleteCallOutputFencesPhysicalCutoffAndPhase(t *testing.T) {
	for _, phase := range []string{"P", "S", "W", "N", "U"} {
		t.Run(phase, func(t *testing.T) {
			s, _, id, out, old := obsoleteOutputFixture(t, phase, true)
			ctx := context.Background()
			firstEnd := old.DeadlineMS - 500
			_, err := s.Commit(ctx, Publication{Identity: id, Revision: old.Revision, Deadline: time.Now().Add(time.Second), Timers: &TimerMutation{Expected: old.Assignment, Target: old.Assignment, RequireLease: true, Changes: []TimerChange{{Member: old.Member, Delete: true}, {Member: "call-retire", Due: time.UnixMilli(old.DeadlineMS)}}}})
			if err != nil {
				t.Fatal(err)
			}
			due, err := s.PeekDue(ctx, old.Assignment.Worker, old.Assignment.Epoch)
			if err != nil || due == nil {
				t.Fatal("terminal head unavailable", err)
			}
			ref := outputRef(id, out)
			err = s.ForgetObsoleteCallOutput(ctx, due, ref, firstEnd, true)
			if phase == "U" {
				if err != nil {
					t.Fatal(err)
				}
			} else if !errors.Is(err, ErrConflict) {
				t.Fatal("non-unknown proof accepted", phase, err)
			}
		})
	}
}

func TestForgetObsoleteCallOutputLostReplyRetainsTerminalWitness(t *testing.T) {
	s, c, id, out, old := obsoleteOutputFixture(t, "U", true)
	ctx := context.Background()
	cutoff := old.DeadlineMS
	_, err := s.Commit(ctx, Publication{Identity: id, Revision: old.Revision, Deadline: time.Now().Add(time.Second), Timers: &TimerMutation{Expected: old.Assignment, Target: old.Assignment, RequireLease: true, Changes: []TimerChange{{Member: old.Member, Delete: true}, {Member: "call-retire", Due: time.UnixMilli(cutoff)}}}})
	if err != nil {
		t.Fatal(err)
	}
	due, err := s.PeekDue(ctx, old.Assignment.Worker, old.Assignment.Epoch)
	if err != nil || due == nil {
		t.Fatal(err)
	}
	client := redis.NewClient(&redis.Options{Addr: c.Options().Addr, MaxRetries: -1})
	defer client.Close()
	client.AddHook(&loseReply{armed: true})
	peer, err := New(client, Config{Root: s.root, Profile: s.profile, RecordBudget: s.maxBytes, PrimaryRunID: s.primary})
	if err != nil {
		t.Fatal(err)
	}
	ref := outputRef(id, out)
	if err = peer.ForgetObsoleteCallOutput(ctx, due, ref, cutoff, true); !errors.Is(err, ErrUnknown) {
		t.Fatal("lost reply invented outcome", err)
	}
	if _, err = s.InspectOutput(ctx, ref); !errors.Is(err, ErrAbsent) {
		t.Fatal("proof release not observable", err)
	}
	view, err := s.ObserveContext(ctx, id.Context, id.Incarnation, "r:context", "r:result", out.MetadataField)
	if err != nil || string(view.View.Records["r:context"]) != "ended" || string(view.View.Records["r:result"]) != "ACCEPTED" || view.View.Records[out.MetadataField] == nil {
		t.Fatal("lost reply changed terminal witness or accepted result", err)
	}
	next, err := s.PeekDue(ctx, due.Assignment.Worker, due.Assignment.Epoch)
	if err != nil || next == nil || next.Member != "call-retire" {
		t.Fatal("terminal duty lost", next, err)
	}
	checkNamespace(t, s, c, id)
}
