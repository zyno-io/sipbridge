package rediscore

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// Test-only composition demonstrates the preflight/apply contract. Production
// publication must compose these functions with every record and output check.
var timerHarness = redis.NewScript(commonScript + timerScript + `
local problem=guard();if problem then return fail(problem) end
local v;v,problem=identity();if problem then return fail(problem) end
if v[4]~=ARGV[5] then return fail('CONFLICT') end
local count=number(ARGV[13]);if not count or #ARGV~=15+count*3 then return fail('CORRUPT') end
local changes={};for i=1,count do local p=14+(i-1)*3;changes[i]={ARGV[p],ARGV[p+1],ARGV[p+2]} end
local clock=redis.call('TIME');local now=tonumber(clock[1])*1000+math.floor(tonumber(clock[2])/1000)
local expected={ARGV[6],ARGV[7],ARGV[8]};local target={ARGV[9],ARGV[10],ARGV[11]};local context=ARGV[14+count*3];local root=string.sub(KEYS[1],1,#KEYS[1]-#'core:profile')
local plan,err=timerPrepare(root,KEYS[2],KEYS[3],KEYS[4],KEYS[5],KEYS[6],KEYS[7],root..'core:wake:'..target[1],context,expected,target,changes,ARGV[12]=='1',now)
if err then return fail(err) end
if not redis.acl_check_cmd('HSET',KEYS[2],'r:marker','new') then return fail('AUTHORITY') end
redis.call('HSET',KEYS[2],'r:marker','new');timerApply(plan)
return {'OK',tostring(plan.byteDelta),tostring(plan.fieldDelta),plan.score}
`)

func runTimer(t *testing.T, s *Store, id Identity, change TimerMutation) ([]interface{}, error) {
	t.Helper()
	if !change.valid() {
		return nil, ErrConflict
	}
	args := []interface{}{s.profile, id.Incarnation, id.Owner, id.Term, "1", change.Expected.Worker, change.Expected.Epoch, change.Expected.Term, change.Target.Worker, change.Target.Epoch, change.Target.Term, "0", len(change.Changes)}
	if change.RequireLease {
		args[11] = "1"
	}
	for _, c := range change.Changes {
		if c.Delete {
			args = append(args, c.Member, "delete", "")
		} else {
			args = append(args, c.Member, "set", strconv.FormatInt(c.Due.UnixMilli(), 10))
		}
	}
	args = append(args, id.Context, s.primary)
	keys := []string{s.profileKey(), s.contextKey(id.Context), s.contextDeadlineKey(id.Context), s.workerKey(change.Expected.Worker), s.workerTimerKey(change.Expected.Worker), s.workerKey(change.Target.Worker), s.workerTimerKey(change.Target.Worker)}
	value, err := timerHarness.Run(context.Background(), s.client, keys, args...).Slice()
	if err != nil {
		return nil, err
	}
	return value, status(value)
}
func TestTimerPublicationPreflightAndMinimum(t *testing.T) {
	s, c, id := fixture(t)
	ctx := context.Background()
	publish(t, s, id, Change{Field: "r:marker", Value: []byte("old")})
	now := time.Now()
	a := TimerAssignment{"worker1", "epoch1", "18446744073709551615"}
	b := TimerAssignment{"worker2", "epoch2", "18446744073709551614"}
	keys := []string{s.contextDeadlineKey(id.Context), s.workerKey(a.Worker), s.workerKey(b.Worker), s.workerTimerKey(a.Worker), s.workerTimerKey(b.Worker)}
	t.Cleanup(func() {
		if err := c.Del(ctx, keys...).Err(); err != nil {
			t.Error(err)
		}
	})
	for _, w := range []TimerAssignment{a, b} {
		if err := c.HSet(ctx, s.workerKey(w.Worker), "epoch", w.Epoch, "expires_ms", now.Add(time.Minute).UnixMilli()).Err(); err != nil {
			t.Fatal(err)
		}
	}
	change := func(member string, ms int64) TimerChange {
		return TimerChange{Member: member, Due: now.Add(time.Duration(ms) * time.Millisecond)}
	}
	check := func(worker string, expected int64) {
		t.Helper()
		score, err := c.ZScore(ctx, s.workerTimerKey(worker), id.Context).Result()
		if err != nil || int64(score) != now.Add(time.Duration(expected)*time.Millisecond).UnixMilli() {
			t.Fatalf("wrong head %s %v %v", worker, score, err)
		}
	}
	pub, err := runTimer(t, s, id, TimerMutation{Target: a, Changes: []TimerChange{change("one", 900), change("two", 800), change("three", 700)}})
	if err != nil {
		t.Fatal(err)
	}
	if pub[2] != "3" {
		t.Fatal("assignment accounting", pub)
	}
	check(a.Worker, 700)
	_, err = runTimer(t, s, id, TimerMutation{Expected: a, Target: a, Changes: []TimerChange{change("three", 1200), change("two", 1300)}})
	if err != nil {
		t.Fatal(err)
	}
	check(a.Worker, 900)
	_, err = runTimer(t, s, id, TimerMutation{Expected: a, Target: a, Changes: []TimerChange{change("two", 500)}})
	if err != nil {
		t.Fatal(err)
	}
	check(a.Worker, 500)
	if err = c.HSet(ctx, s.contextKey(id.Context), "r:marker", "unchanged").Err(); err != nil {
		t.Fatal(err)
	}
	_, err = runTimer(t, s, id, TimerMutation{Expected: a, Target: b, Changes: []TimerChange{change("four", 400)}})
	if !errors.Is(err, ErrConflict) {
		t.Fatal("live owner transferred", err)
	}
	marker, _ := c.HGet(ctx, s.contextKey(id.Context), "r:marker").Result()
	if marker != "unchanged" {
		t.Fatal("refusal mutated records")
	}
	check(a.Worker, 500)
	stale := a
	stale.Term = "18446744073709551614"
	_, err = runTimer(t, s, id, TimerMutation{Expected: stale, Target: stale, Changes: []TimerChange{change("two", 200)}, RequireLease: true})
	if !errors.Is(err, ErrConflict) {
		t.Fatal("wide stale term accepted", err)
	}
	if err = c.HSet(ctx, s.workerKey(a.Worker), "expires_ms", now.Add(-time.Second).UnixMilli()).Err(); err != nil {
		t.Fatal(err)
	}
	_, err = runTimer(t, s, id, TimerMutation{Expected: a, Target: a, Changes: []TimerChange{change("two", 200)}, RequireLease: true})
	if !errors.Is(err, ErrExpired) {
		t.Fatal("expired execution accepted", err)
	}
	_, err = runTimer(t, s, id, TimerMutation{Expected: a, Target: a, Changes: []TimerChange{change("new-duty", 300)}})
	if !errors.Is(err, ErrExpired) {
		t.Fatal("new duty retained expired worker", err)
	}
	if err = c.ZAdd(ctx, s.workerTimerKey(b.Worker), redis.Z{Score: float64(now.Add(time.Second).UnixMilli()), Member: id.Context}).Err(); err != nil {
		t.Fatal(err)
	}
	_, err = runTimer(t, s, id, TimerMutation{Expected: a, Target: b, Changes: []TimerChange{change("four", 400)}})
	if !errors.Is(err, ErrConflict) {
		t.Fatal("conflicting target accepted", err)
	}
	marker, _ = c.HGet(ctx, s.contextKey(id.Context), "r:marker").Result()
	if marker != "unchanged" {
		t.Fatal("target conflict mutated record")
	}
	if err = c.ZRem(ctx, s.workerTimerKey(b.Worker), id.Context).Err(); err != nil {
		t.Fatal(err)
	}
	_, err = runTimer(t, s, id, TimerMutation{Expected: a, Target: b, Changes: []TimerChange{change("four", 400)}})
	if err != nil {
		t.Fatal(err)
	}
	check(b.Worker, 400)
	if _, err = c.ZScore(ctx, s.workerTimerKey(a.Worker), id.Context).Result(); !errors.Is(err, redis.Nil) {
		t.Fatal("old assignment retained", err)
	}
	_, err = runTimer(t, s, id, TimerMutation{Expected: b, Target: b, Changes: []TimerChange{{Member: "one", Delete: true}, {Member: "two", Delete: true}, {Member: "three", Delete: true}, {Member: "four", Delete: true}}})
	if err != nil {
		t.Fatal(err)
	}
	n, err := c.Exists(ctx, s.contextDeadlineKey(id.Context)).Result()
	if err != nil || n != 0 {
		t.Fatal("quiet context retained timer set", n, err)
	}
	present, err := c.HExists(ctx, s.contextKey(id.Context), "timer:term").Result()
	if err != nil || present {
		t.Fatal("quiet assignment retained", present, err)
	}
}

func TestTimerEarlierHeadWakesWorker(t *testing.T) {
	s, c, id := fixture(t)
	ctx := context.Background()
	publish(t, s, id, Change{Field: "r:marker", Value: []byte("old")})
	a := TimerAssignment{Worker: "worker1", Epoch: "epoch1", Term: "term1"}
	now := time.Now()
	keys := []string{s.contextDeadlineKey(id.Context), s.workerKey(a.Worker), s.workerTimerKey(a.Worker)}
	t.Cleanup(func() {
		if err := c.Del(ctx, keys...).Err(); err != nil {
			t.Error(err)
		}
	})
	if err := c.HSet(ctx, s.workerKey(a.Worker), "epoch", a.Epoch, "expires_ms", now.Add(time.Minute).UnixMilli()).Err(); err != nil {
		t.Fatal(err)
	}
	sub := c.Subscribe(ctx, s.root+"core:wake:"+a.Worker)
	defer sub.Close()
	if _, err := sub.Receive(ctx); err != nil {
		t.Fatal(err)
	}
	events := sub.Channel()
	change := func(delta time.Duration) TimerChange { return TimerChange{Member: "one", Due: now.Add(delta)} }
	if _, err := runTimer(t, s, id, TimerMutation{Target: a, Changes: []TimerChange{change(5 * time.Second)}}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-events:
	case <-time.After(time.Second):
		t.Fatal("first timer did not wake worker")
	}
	if _, err := runTimer(t, s, id, TimerMutation{Expected: a, Target: a, Changes: []TimerChange{change(10 * time.Second)}}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-events:
		t.Fatal("later head caused unnecessary wake")
	case <-time.After(100 * time.Millisecond):
	}
	if _, err := runTimer(t, s, id, TimerMutation{Expected: a, Target: a, Changes: []TimerChange{change(time.Second)}}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-events:
	case <-time.After(time.Second):
		t.Fatal("earlier head did not wake worker")
	}
}
