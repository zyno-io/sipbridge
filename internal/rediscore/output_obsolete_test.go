package rediscore

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// The local fixture rewrites the immutable test timestamp by the same width to
// model elapsed time without waiting two minutes; no service data is touched.
func obsoleteOutputFixture(t *testing.T, phase string, mature bool) (*Store, *redis.Client, Identity, OutputIntent, *DueContext) {
	t.Helper()
	s, c, id, out, data := outputFixture(t)
	ctx := context.Background()
	timer := TimerAssignment{Worker: "worker2", Epoch: "timer-epoch", Term: "timer-term"}
	out.Deadline = time.Now().Add(time.Minute)
	out.Dependencies = []string{"r:procedure", "r:result"}
	keys := []string{s.workerKey(timer.Worker), s.workerTimerKey(timer.Worker), s.contextDeadlineKey(id.Context)}
	t.Cleanup(func() {
		if e := c.Del(ctx, keys...).Err(); e != nil {
			t.Error(e)
		}
	})
	if err := c.HSet(ctx, s.workerKey(timer.Worker), "epoch", timer.Epoch, "expires_ms", time.Now().Add(time.Minute).UnixMilli()).Err(); err != nil {
		t.Fatal(err)
	}
	changes := []Change{{Field: out.MessageField, Value: data}, {Field: out.MetadataField, Value: []byte("metadata")}, {Field: "r:context", Value: []byte("ended")}, {Field: "r:procedure", Value: []byte("terminated")}, {Field: "r:result", Value: []byte("ACCEPTED")}}
	if _, err := s.Commit(ctx, Publication{Identity: id, Create: true, Deadline: time.Now().Add(time.Minute), Changes: changes, NewOutputs: []OutputIntent{out}, Timers: &TimerMutation{Target: timer, Changes: []TimerChange{{Member: "out-metadata", Due: out.Deadline}}}}); err != nil {
		t.Fatal(err)
	}
	ref := outputRef(id, out)
	if phase != "P" {
		if _, err := s.ReserveOutput(ctx, ref); err != nil {
			t.Fatal(err)
		}
	}
	if phase == "U" || phase == "W" || phase == "N" {
		outcome := "UNKNOWN"
		if phase == "W" {
			outcome = "WRITTEN"
		} else if phase == "N" {
			outcome = "NOT_WRITTEN"
		}
		if err := s.CompleteOutput(ctx, ref, outcome); err != nil {
			t.Fatal(err)
		}
	}
	physical := time.Now().Add(-OutputObsoleteGrace + time.Second)
	if mature {
		physical = time.Now().Add(-OutputObsoleteGrace - time.Second)
	}
	if err := c.HSet(ctx, s.contextKey(id.Context), "o:"+out.ID+":d", physical.UnixMilli()).Err(); err != nil {
		t.Fatal(err)
	}
	cutoff := physical.Add(OutputObsoleteGrace)
	if _, err := s.Commit(ctx, Publication{Identity: id, Revision: 1, Deadline: time.Now().Add(time.Minute), Timers: &TimerMutation{Expected: timer, Target: timer, Changes: []TimerChange{{Member: "out-metadata", Due: cutoff}}}}); err != nil {
		t.Fatal(err)
	}
	if !mature {
		return s, c, id, out, &DueContext{Identity: id, Revision: 2, Assignment: timer, Member: "out-metadata", DeadlineMS: cutoff.UnixMilli(), CheckedAtMS: cutoff.UnixMilli()}
	}
	due, err := s.PeekDue(ctx, timer.Worker, timer.Epoch)
	if err != nil || due == nil {
		t.Fatal("obsolete timer unavailable", due, err)
	}
	return s, c, id, out, due
}
func TestForgetObsoleteOutputReleasesOnlyUnknownProof(t *testing.T) {
	s, c, id, out, due := obsoleteOutputFixture(t, "U", true)
	ctx := context.Background()
	ref := outputRef(id, out)
	if err := s.ForgetObsoleteOutput(ctx, due, ref, false); !errors.Is(err, ErrConflict) {
		t.Fatal("missing native witness accepted", err)
	}
	if err := s.ForgetObsoleteOutput(ctx, due, ref, true); err != nil {
		t.Fatal(err)
	}
	checkNamespace(t, s, c, id)
	if _, err := s.InspectOutput(ctx, ref); !errors.Is(err, ErrAbsent) {
		t.Fatal("obsolete proof retained", err)
	}
	view, err := s.ObserveContext(ctx, id.Context, id.Incarnation, "r:context", "r:procedure", "r:result", out.MessageField, out.MetadataField)
	if err != nil {
		t.Fatal(err)
	}
	if string(view.View.Records["r:context"]) != "ended" || string(view.View.Records["r:result"]) != "ACCEPTED" || view.View.Records[out.MessageField] == nil || view.View.Records[out.MetadataField] == nil {
		t.Fatal("obsolete cleanup changed selected evidence")
	}
	for _, field := range out.Dependencies {
		if n, err := c.HExists(ctx, s.contextKey(id.Context), "dref:"+field).Result(); err != nil || n {
			t.Fatal("dependency count retained", field, err)
		}
	}
	if next, err := s.PeekDue(ctx, due.Assignment.Worker, due.Assignment.Epoch); err != nil || next == nil || next.Member != due.Member {
		t.Fatal("obsolete cleanup popped timer", next, err)
	}
	if err = s.ForgetObsoleteOutput(ctx, due, ref, true); !errors.Is(err, ErrAbsent) {
		t.Fatal("missing proof did not remain unknown", err)
	}
}
func TestForgetObsoleteOutputFencesDeadlinePhaseAndDuty(t *testing.T) {
	t.Run("early", func(t *testing.T) {
		s, _, id, out, due := obsoleteOutputFixture(t, "U", false)
		if err := s.ForgetObsoleteOutput(context.Background(), due, outputRef(id, out), true); !errors.Is(err, ErrConflict) {
			t.Fatal("early cutoff accepted", err)
		}
	})
	for _, phase := range []string{"P", "S", "W", "N"} {
		t.Run(phase, func(t *testing.T) {
			s, _, id, out, due := obsoleteOutputFixture(t, phase, true)
			if err := s.ForgetObsoleteOutput(context.Background(), due, outputRef(id, out), true); !errors.Is(err, ErrConflict) {
				t.Fatal("non-UNKNOWN output forgotten", phase, err)
			}
		})
	}
	t.Run("stale-or-dead", func(t *testing.T) {
		s, c, id, out, due := obsoleteOutputFixture(t, "U", true)
		ctx := context.Background()
		ref := outputRef(id, out)
		wrong := *due
		wrong.Member = "out-other"
		if err := s.ForgetObsoleteOutput(ctx, &wrong, ref, true); !errors.Is(err, ErrConflict) {
			t.Fatal("wrong duty accepted", err)
		}
		wrong = *due
		wrong.DeadlineMS--
		if err := s.ForgetObsoleteOutput(ctx, &wrong, ref, true); !errors.Is(err, ErrConflict) {
			t.Fatal("sliding cutoff accepted", err)
		}
		if err := c.HSet(ctx, s.workerKey(due.Assignment.Worker), "expires_ms", time.Now().Add(-time.Second).UnixMilli()).Err(); err != nil {
			t.Fatal(err)
		}
		if err := s.ForgetObsoleteOutput(ctx, due, ref, true); !errors.Is(err, ErrAuthority) {
			t.Fatal("dead executor accepted", err)
		}
		if err := c.HSet(ctx, s.workerKey(due.Assignment.Worker), "expires_ms", time.Now().Add(time.Minute).UnixMilli()).Err(); err != nil {
			t.Fatal(err)
		}
		if _, err := s.Commit(ctx, Publication{Identity: id, Revision: due.Revision, Deadline: time.Now().Add(time.Minute), Changes: []Change{{Field: "r:procedure", Value: []byte("changed")}}}); err != nil {
			t.Fatal(err)
		}
		if err := s.ForgetObsoleteOutput(ctx, due, ref, true); !errors.Is(err, ErrConflict) {
			t.Fatal("stale revision accepted", err)
		}
		if got, err := s.InspectOutput(ctx, ref); err != nil || got.State != OutputUnknown {
			t.Fatal("refusal changed unknown proof", got, err)
		}
	})
}
func TestForgetObsoleteOutputLostReplyReconcilesMarker(t *testing.T) {
	s, c, id, out, due := obsoleteOutputFixture(t, "U", true)
	ctx := context.Background()
	client := redis.NewClient(&redis.Options{Addr: c.Options().Addr, MaxRetries: -1})
	defer client.Close()
	client.AddHook(&loseReply{armed: true})
	peer, err := New(client, Config{Root: s.root, Profile: s.profile, RecordBudget: s.maxBytes, PrimaryRunID: s.primary})
	if err != nil {
		t.Fatal(err)
	}
	if err = peer.ForgetObsoleteOutput(ctx, due, outputRef(id, out), true); !errors.Is(err, ErrUnknown) {
		t.Fatal("lost reply invented result", err)
	}
	if _, err = s.InspectOutput(ctx, outputRef(id, out)); !errors.Is(err, ErrAbsent) {
		t.Fatal("exact proof unexpectedly retained", err)
	}
	view, err := s.ObserveContext(ctx, id.Context, id.Incarnation, "r:context", "r:result")
	if err != nil || string(view.View.Records["r:context"]) != "ended" || string(view.View.Records["r:result"]) != "ACCEPTED" {
		t.Fatal("native marker or accepted receipt lost", view, err)
	}
	checkNamespace(t, s, c, id)
}
