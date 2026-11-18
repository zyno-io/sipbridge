package rediscore

import (
	"context"
	"errors"
	"maps"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

func TestPublicationCombinesRecordsOutputsAndNextDeadline(t *testing.T) {
	s, client, id, out, data := outputFixture(t)
	ctx := context.Background()
	assignment := TimerAssignment{Worker: out.Worker, Epoch: out.Epoch, Term: "18446744073709551615"}
	deadline := time.Now().Add(20 * time.Second)
	t.Cleanup(func() {
		if err := client.Del(ctx, s.contextDeadlineKey(id.Context), s.workerTimerKey(out.Worker)).Err(); err != nil {
			t.Error(err)
		}
	})
	second := out
	second.ID, second.Attempt = "second-output", "second-attempt"
	p := Publication{Identity: id, Create: true, Deadline: time.Now().Add(time.Minute),
		Changes:    []Change{{Field: out.MessageField, Value: data}, {Field: out.MetadataField, Value: []byte("metadata")}, {Field: "r:state", Value: []byte{0x08, 1}}},
		NewOutputs: []OutputIntent{out, second}, Timers: &TimerMutation{Target: assignment, Changes: []TimerChange{{Member: "notify", Due: deadline}}}}
	if revision, err := s.Commit(ctx, p); err != nil || revision != 1 {
		t.Fatalf("combined publication: %d %v", revision, err)
	}
	view, err := s.Read(ctx, id, "r:state")
	if err != nil || view.Timer != assignment || view.Revision != 1 {
		t.Fatalf("assignment/read projection: %v", err)
	}
	if head, err := client.ZScore(ctx, s.workerTimerKey(out.Worker), id.Context).Result(); err != nil || int64(head) != deadline.UnixMilli() {
		t.Fatalf("owner head: %v", err)
	}
	if count, err := client.ZCard(ctx, s.workerReadyKey(out.Worker)).Result(); err != nil || count != 2 {
		t.Fatalf("required output pair: %d %v", count, err)
	}
	before, err := client.HGetAll(ctx, s.contextKey(id.Context)).Result()
	if err != nil {
		t.Fatal(err)
	}
	// The timer plan is valid, but the combined record capacity is not. Neither
	// the new output nor either index may escape the rejected native transition.
	third := out
	third.ID, third.Attempt = "third-output", "third-attempt"
	reject := Publication{Identity: id, Revision: 1, Deadline: time.Now().Add(time.Minute), Reserved: s.maxBytes,
		Changes: []Change{{Field: "r:state", Value: []byte{0x08, 2}}}, NewOutputs: []OutputIntent{third},
		Timers: &TimerMutation{Expected: assignment, Target: assignment, Changes: []TimerChange{{Member: "notify", Due: deadline.Add(-time.Second)}}}}
	if _, err = s.Commit(ctx, reject); !errors.Is(err, ErrCapacity) {
		t.Fatalf("combined capacity: %v", err)
	}
	after, err := client.HGetAll(ctx, s.contextKey(id.Context)).Result()
	if err != nil || !maps.Equal(before, after) {
		t.Fatal("capacity refusal partially published native records")
	}
	if count, err := client.ZCard(ctx, s.workerReadyKey(out.Worker)).Result(); err != nil || count != 2 {
		t.Fatal("capacity refusal published an output")
	}
	if head, err := client.ZScore(ctx, s.workerTimerKey(out.Worker), id.Context).Result(); err != nil || int64(head) != deadline.UnixMilli() {
		t.Fatal("capacity refusal moved owner head")
	}
	for _, intent := range []OutputIntent{out, second} {
		ref := outputRef(id, intent)
		if _, err = s.ReserveOutput(ctx, ref); err != nil {
			t.Fatal(err)
		}
		if err = s.CompleteOutput(ctx, ref, "WRITTEN"); err != nil {
			t.Fatal(err)
		}
		if err = s.SettleOutput(ctx, ref, true, true); err != nil {
			t.Fatal(err)
		}
	}
	// After physical acknowledgement and native settlement, the last real timer
	// duty and the now-unneeded message can leave in one native publication.
	_, err = s.Commit(ctx, Publication{Identity: id, Revision: 1, Deadline: time.Now().Add(time.Minute),
		Changes: []Change{{Field: out.MessageField, Delete: true}},
		Timers:  &TimerMutation{Expected: assignment, Target: assignment, Changes: []TimerChange{{Member: "notify", Delete: true}}}})
	if err != nil {
		t.Fatal(err)
	}
	view, err = s.Read(ctx, id, "r:state")
	if err != nil || !view.Timer.empty() || view.Revision != 2 {
		t.Fatal("quiet context retained assignment", err)
	}
	if n, err := client.Exists(ctx, s.contextDeadlineKey(id.Context), s.workerTimerKey(out.Worker)).Result(); err != nil || n != 0 {
		t.Fatal("quiet context retained timer indexes", err)
	}
	fields, err := client.HGetAll(ctx, s.contextKey(id.Context)).Result()
	if err != nil {
		t.Fatal(err)
	}
	total := 0
	for key, value := range fields {
		if key != "incarnation" && key != "owner" && key != "term" && key != "revision" && key != "bytes" && key != "reserved" && key != "index_bytes" {
			total += len(key) + len(value)
		}
	}
	if fields["bytes"] != strconv.Itoa(total) {
		t.Fatalf("combined field accounting: got %s want %d", fields["bytes"], total)
	}
}

func TestPublicationLastTimerChannelDenialPublishesNothing(t *testing.T) {
	s, admin, id, out, data := outputFixture(t)
	ctx := context.Background()
	assignment := TimerAssignment{Worker: "timer-worker", Epoch: "timer-epoch", Term: "timer-term"}
	if err := admin.HSet(ctx, s.workerKey(assignment.Worker), "epoch", assignment.Epoch, "expires_ms", time.Now().Add(time.Minute).UnixMilli()).Err(); err != nil {
		t.Fatal(err)
	}
	user := "core-" + uuid.NewString()
	const password = "disposable-publication-qualification"
	if err := admin.ACLSetUser(ctx, user, "reset", "on", ">"+password, "+@all", "~"+s.root+"*", "&"+s.root+"core:wake:"+out.Worker).Err(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := admin.ACLDelUser(ctx, user).Err(); err != nil {
			t.Error(err)
		}
		if err := admin.Del(ctx, s.workerKey(assignment.Worker), s.workerTimerKey(assignment.Worker), s.contextDeadlineKey(id.Context)).Err(); err != nil {
			t.Error(err)
		}
	})
	client := redis.NewClient(&redis.Options{Addr: admin.Options().Addr, Username: user, Password: password, MaxRetries: -1})
	defer client.Close()
	restricted, err := New(client, Config{Root: s.root, Profile: s.profile, RecordBudget: s.maxBytes, PrimaryRunID: s.primary})
	if err != nil {
		t.Fatal(err)
	}
	_, err = restricted.Commit(ctx, Publication{Identity: id, Create: true, Deadline: time.Now().Add(time.Minute),
		Changes: []Change{{Field: out.MessageField, Value: data}, {Field: out.MetadataField, Value: []byte("metadata")}}, NewOutputs: []OutputIntent{out},
		Timers: &TimerMutation{Target: assignment, Changes: []TimerChange{{Member: "procedure", Due: time.Now().Add(20 * time.Second)}}}})
	if !errors.Is(err, ErrAuthority) {
		t.Fatalf("timer channel denial: %v", err)
	}
	if n, err := admin.Exists(ctx, s.contextKey(id.Context), s.contextDeadlineKey(id.Context), s.workerReadyKey(out.Worker), s.workerTimerKey(assignment.Worker)).Result(); err != nil || n != 0 {
		t.Fatal("last channel refusal left a partial native publication", n, err)
	}
}
