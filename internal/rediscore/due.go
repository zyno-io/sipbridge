package rediscore

import (
	"context"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

type RecordDeadline struct {
	Member     string
	DeadlineMS int64
}

// PeekContextDeadline observes the next selected context duty while optionally
// excluding the one currently being handled. It reads at most two sorted heads;
// a subsequent publication still fences this exact context revision. This is
// used for real retirement work, never a periodic head poll.
func (s *Store) PeekContextDeadline(ctx context.Context, id Identity, revision uint64, exclude string) (*RecordDeadline, error) {
	if !id.valid() || revision == 0 || exclude != "" && !reference(exclude) {
		return nil, ErrConflict
	}
	v, err := contextDeadlineScript.Run(ctx, s.client, []string{s.profileKey(), s.contextKey(id.Context), s.contextDeadlineKey(id.Context)},
		s.profile, id.Incarnation, id.Owner, id.Term, strconv.FormatUint(revision, 10), exclude, s.primary).Slice()
	if err != nil {
		return nil, err
	}
	if err = status(v); err != nil {
		return nil, err
	}
	if len(v) != 3 {
		return nil, ErrCorrupt
	}
	member, ok := v[1].(string)
	due, err := responseInt(v[2])
	if !ok || err != nil || (member == "") != (due == 0) || member != "" && !reference(member) || member != "" && member == exclude {
		return nil, ErrCorrupt
	}
	if member == "" {
		return nil, nil
	}
	return &RecordDeadline{Member: member, DeadlineMS: due}, nil
}

var contextDeadlineScript = redis.NewScript(commonScript + `
local problem=guard();if problem then return fail(problem) end
local v;v,problem=identity();if problem then return fail(problem) end
if v[4]~=ARGV[5] then return fail('CONFLICT') end
if not kind(KEYS[3],'zset') then return fail('CORRUPT') end
local heads=redis.call('ZRANGE',KEYS[3],0,ARGV[6]=='' and 0 or 1,'WITHSCORES')
for i=1,#heads,2 do
 local member,due=heads[i],number(heads[i+1])
 if #member>192 or not string.match(member,'^[A-Za-z0-9_-]+$') or not due or due==0 then return fail('CORRUPT') end
 if member~=ARGV[6] then return {'OK',member,heads[i+1]} end
end
return {'OK','','0'}
`)

// DueContext is a peek at one actual native duty, never a claim or a popped
// work item. The subsequent native publication must fence Identity, Revision
// and Assignment with TimerMutation.RequireLease. Another committed transition
// can supersede this observation without losing the indexed work.
type DueContext struct {
	Identity    Identity
	Revision    uint64
	Assignment  TimerAssignment
	Member      string
	DeadlineMS  int64
	CheckedAtMS int64
	// Observed is the monotonic request-start anchor for CheckedAtMS. It is
	// operation-local and never stored or used in place of publication fences.
	Observed time.Time
}

// PeekDue reads only this live worker's first due context and that context's
// earliest active record. No SIP record is read or parsed, and no key is changed.
// A nil result means the worker currently has no due native work.
func (s *Store) PeekDue(ctx context.Context, worker, epoch string) (*DueContext, error) {
	due, _, _, err := s.peekDuePage(ctx, worker, epoch, 0)
	return due, err
}

func (s *Store) peekDuePage(ctx context.Context, worker, epoch string, offset int) (*DueContext, int, time.Time, error) {
	if !reference(worker) || !reference(epoch) {
		return nil, 0, time.Time{}, ErrConflict
	}
	start := time.Now()
	deadline := start.Add(workerRedisTimeout)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	callCtx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	low := start.UnixMilli() - workerClockTolerance.Milliseconds()
	high := start.UnixMilli() + max(int64(0), deadline.Sub(start).Milliseconds()) + workerClockTolerance.Milliseconds()
	limit := 1
	if offset != 0 {
		limit = nativeWorkPage
	}
	v, err := peekDueScript.Run(callCtx, s.client, []string{s.profileKey(), s.workerKey(worker), s.workerTimerKey(worker)},
		s.profile, worker, epoch, low, high, offset, limit, s.primary).Slice()
	if err != nil {
		return nil, 0, time.Time{}, err
	}
	if err = status(v); err != nil {
		return nil, 0, time.Time{}, err
	}
	if len(v) != 3 && len(v) != 4 && len(v) != 11 {
		return nil, 0, time.Time{}, ErrCorrupt
	}
	now, err := responseInt(v[1])
	if err != nil {
		return nil, 0, time.Time{}, err
	}
	finish := time.Now()
	elapsed := finish.Sub(start)
	skew := finish.UnixMilli() - start.UnixMilli() - elapsed.Milliseconds()
	if elapsed < 0 || skew < -workerClockTolerance.Milliseconds() || skew > workerClockTolerance.Milliseconds() || now < low || now > finish.UnixMilli()+workerClockTolerance.Milliseconds() {
		return nil, 0, time.Time{}, ErrClock
	}
	if err = callCtx.Err(); err != nil {
		return nil, 0, time.Time{}, err
	}
	next, err := responseInt(v[2])
	if err != nil || next != 0 && (next < int64(offset) || next > int64(offset+limit)) {
		return nil, 0, time.Time{}, ErrCorrupt
	}
	if len(v) == 3 {
		return nil, int(next), time.Time{}, nil
	}
	if len(v) == 4 {
		future, err := responseInt(v[3])
		if err != nil || future <= now || next != 0 {
			return nil, 0, time.Time{}, ErrCorrupt
		}
		return nil, 0, observedDeadline(start, future-now), nil
	}
	var values [8]string
	for i := range values {
		value, ok := v[i+3].(string)
		if !ok {
			return nil, 0, time.Time{}, ErrCorrupt
		}
		values[i] = value
	}
	identity := Identity{Context: values[0], Incarnation: values[1], Owner: values[2], Term: values[3]}
	revision, err := strconv.ParseUint(values[4], 10, 64)
	if err != nil || revision == 0 || !identity.valid() || !reference(values[5]) || !reference(values[6]) {
		return nil, 0, time.Time{}, ErrCorrupt
	}
	due, err := responseInt(values[7])
	if err != nil || due == 0 || due > now {
		return nil, 0, time.Time{}, ErrCorrupt
	}
	return &DueContext{Identity: identity, Revision: revision, Assignment: TimerAssignment{Worker: worker, Epoch: epoch, Term: values[5]},
		Member: values[6], DeadlineMS: due, CheckedAtMS: now, Observed: start}, int(next), time.Time{}, nil
}

var peekDueScript = redis.NewScript(commonScript + parkedScript + `
local problem=guard();if problem then return fail(problem) end
if not kind(KEYS[3],'zset') then return fail('CORRUPT') end
local lo,hi=number(ARGV[4]),number(ARGV[5]);if not lo or not hi or hi<lo then return fail('CORRUPT') end
local clock=redis.call('TIME');local now=tonumber(clock[1])*1000+math.floor(tonumber(clock[2])/1000)
if now<lo or now>hi then return fail('EXPIRED') end
local lease=redis.call('HMGET',KEYS[2],'epoch','expires_ms')
if not lease[1] then return fail('ABSENT') end
local expiry=number(lease[2]);if not expiry then return fail('CORRUPT') end
if lease[1]~=ARGV[3] then return fail('AUTHORITY') end
if expiry<=now then return fail('EXPIRED') end
local offset,limit=number(ARGV[6]),number(ARGV[7])
if not offset or not limit or limit==0 then return fail('CORRUPT') end
local heads=redis.call('ZRANGE',KEYS[3],offset,offset+limit-1,'WITHSCORES')
local nowText=string.format('%.0f',now)
local function ref(v) return type(v)=='string' and #v>0 and #v<=192 and string.match(v,'^[A-Za-z0-9_-]+$') end
local root=string.sub(KEYS[1],1,#KEYS[1]-#'core:profile')
local function selectItem(id,due)
 if not ref(id) or not due or due==0 then return nil end
 local context=root..'core:context:'..id;local deadlines=root..'core:deadlines:'..id
 if not kind(context,'hash') or not kind(deadlines,'zset') then return nil end
 local v=redis.call('HMGET',context,'incarnation','owner','term','revision','timer:worker','timer:epoch','timer:term')
 if not ref(v[1]) or type(v[2])~='string' or type(v[3])~='string' or (v[2]=='')~=(v[3]=='') or (v[2]~='' and (not ref(v[2]) or not ref(v[3]) or #v[3]>190)) or not decimal(v[4]) or v[4]=='0' or not ref(v[7]) or v[5]~=ARGV[2] or v[6]~=ARGV[3] or parked(root,id,v[1]) then return nil end
 local record=redis.call('ZRANGE',deadlines,0,0,'WITHSCORES')
 if #record~=2 or not ref(record[1]) or number(record[2])~=due then return nil end
 return {v[1],v[2],v[3],v[4],v[7],record[1]}
end
for i=1,#heads,2 do
 local due=number(heads[i+1])
 if due and due>now then return {'OK',nowText,'0',heads[i+1]} end
 local item=selectItem(heads[i],due)
 if item then return {'OK',nowText,tostring(offset+math.floor(i/2)),heads[i],item[1],item[2],item[3],item[4],item[5],item[6],heads[i+1]} end
end
return {'OK',nowText,tostring(#heads==2*limit and offset+limit or 0)}
`)
