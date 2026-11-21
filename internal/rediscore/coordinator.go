package rediscore

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

const MaxWorkerTransferBatch = 32

type WorkerTransfer struct {
	Coordinator, CoordinatorEpoch string
	ExpiredWorker                 string
	Target, TargetEpoch           string
	Limit                         int
	offset                        int
	isolate                       bool
}
type WorkerTransferResult struct {
	Moved   int
	Drained bool
	next    int
	more    bool
}

// TransferExpiredWorker atomically moves a bounded prefix of one expired
// worker's indexed timed contexts. Lost replies remain UNKNOWN. Neither native
// records nor physical output/proof ownership are read or changed.
func (s *Store) TransferExpiredWorker(ctx context.Context, req WorkerTransfer) (WorkerTransferResult, error) {
	if !reference(req.Coordinator) || !reference(req.CoordinatorEpoch) || !reference(req.ExpiredWorker) || !reference(req.Target) || !reference(req.TargetEpoch) || req.ExpiredWorker == req.Target || len(req.Coordinator) > maxWorkerReferenceBytes || len(req.ExpiredWorker) > maxWorkerReferenceBytes || len(req.Target) > maxWorkerReferenceBytes || req.Limit < 1 || req.Limit > MaxWorkerTransferBatch {
		return WorkerTransferResult{}, ErrConflict
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
	keys := []string{s.profileKey(), s.coordinatorKey(), s.ownerExpiryKey(), s.workerKey(req.ExpiredWorker), s.workerTimerKey(req.ExpiredWorker), s.workerKey(req.Target), s.workerTimerKey(req.Target), s.capacityKey(), s.root + "core:wake:" + req.Target, s.workerKey(req.Coordinator)}
	isolate := "0"
	if req.isolate {
		isolate = "1"
	}
	v, err := transferWorkerScript.Run(callCtx, s.client, keys, s.profile, req.Coordinator, req.CoordinatorEpoch, req.ExpiredWorker, req.Target, req.TargetEpoch, req.Limit, low, high, s.namespaceBytes, s.maxBytes, s.namespaceContexts, req.offset, isolate, s.primary).Slice()
	finish := time.Now()
	if err != nil {
		return WorkerTransferResult{}, fmt.Errorf("%w: %w", ErrUnknown, err)
	}
	if err = status(v); err != nil {
		return WorkerTransferResult{}, err
	}
	if len(v) != 6 {
		return WorkerTransferResult{}, errors.Join(ErrUnknown, ErrCorrupt)
	}
	now, e1 := responseInt(v[1])
	moved, e2 := responseInt(v[2])
	drained, ok := v[3].(string)
	next, e3 := responseInt(v[4])
	more, moreOK := v[5].(string)
	elapsed := finish.Sub(start)
	skew := finish.UnixMilli() - start.UnixMilli() - elapsed.Milliseconds()
	if e1 != nil || e2 != nil || e3 != nil || moved > int64(req.Limit) || !ok || (drained != "0" && drained != "1") || !moreOK || (more != "0" && more != "1") || next > int64(req.offset+req.Limit) || elapsed < 0 || skew < -workerClockTolerance.Milliseconds() || skew > workerClockTolerance.Milliseconds() || now < low || now > finish.UnixMilli()+workerClockTolerance.Milliseconds() || ctx.Err() != nil || callCtx.Err() != nil {
		return WorkerTransferResult{}, errors.Join(ErrUnknown, ErrCorrupt)
	}
	return WorkerTransferResult{Moved: int(moved), Drained: drained == "1", next: int(next), more: more == "1"}, nil
}

// ownerPage skips malformed, untrusted index members without deriving keys
// from them. The actual owner operation revalidates the expired term.
func (s *Store) ownerPage(ctx context.Context, worker, epoch string, offset int) (string, int, time.Time, error) {
	start := time.Now()
	ctx, cancel := context.WithTimeout(ctx, workerRedisTimeout)
	defer cancel()
	v, err := ownerPageScript.Run(ctx, s.client, []string{s.profileKey(), s.coordinatorKey(), s.workerKey(worker), s.ownerExpiryKey()}, s.profile, worker, epoch, offset, nativeWorkPage, s.primary).Slice()
	if err != nil {
		return "", 0, time.Time{}, err
	}
	if err = status(v); err != nil {
		return "", 0, time.Time{}, err
	}
	if len(v) != 3 && len(v) != 4 {
		return "", 0, time.Time{}, ErrCorrupt
	}
	member, ok := v[1].(string)
	n, err := responseInt(v[2])
	if !ok || err != nil || n != 0 && (n < int64(offset) || n > int64(offset+nativeWorkPage)) || member != "" &&
		(len(member) < 3 || !reference(member[2:]) ||
			!strings.HasPrefix(member, "p-") && (len(member) > 192 || !strings.HasPrefix(member, "a-") && !strings.HasPrefix(member, "w-"))) {
		return "", 0, time.Time{}, ErrCorrupt
	}
	if len(v) == 4 {
		delta, err := responseInt(v[3])
		if err != nil || delta <= 0 || member != "" || n != 0 {
			return "", 0, time.Time{}, ErrCorrupt
		}
		return "", 0, observedDeadline(start, delta), nil
	}
	return member, int(n), time.Time{}, nil
}

var ownerPageScript = redis.NewScript(commonScript + `
local problem=guard();if problem then return fail(problem) end
if not kind(KEYS[3],'hash') or not kind(KEYS[4],'zset') then return fail('CORRUPT') end
local clock=redis.call('TIME');local now=tonumber(clock[1])*1000+math.floor(tonumber(clock[2])/1000)
local c=redis.call('HMGET',KEYS[2],'worker','epoch','expires_ms')
local w=redis.call('HMGET',KEYS[3],'epoch','expires_ms')
if c[1]~=ARGV[2] or c[2]~=ARGV[3] or not number(c[3]) or number(c[3])<=now or w[1]~=ARGV[3] or not number(w[2]) or number(w[2])<=now then return fail('AUTHORITY') end
local offset,limit=number(ARGV[4]),number(ARGV[5]);if not offset or not limit or limit==0 then return fail('CORRUPT') end
local rows=redis.call('ZRANGE',KEYS[4],offset,offset+limit-1,'WITHSCORES')
for i=1,#rows,2 do
 local member,due=rows[i],number(rows[i+1])
 if due and due>now then return {'OK','','0',string.format('%.0f',due-now)} end
 if due and due>0 and ((#member<=192 and string.match(member,'^[aw]%-[A-Za-z0-9_-]+$')) or (#member<=194 and string.match(member,'^p%-[A-Za-z0-9_-]+$'))) then
  return {'OK',member,tostring(offset+math.floor(i/2))}
 end
end
return {'OK','',tostring(#rows==2*limit and offset+limit or 0)}
`)

var transferWorkerScript = redis.NewScript(commonScript + namespaceScript + `
local problem=guard();if problem then return fail(problem) end
local function ref(v) return type(v)=='string' and #v>0 and #v<=192 and string.match(v,'^[A-Za-z0-9_-]+$') end
local root=string.sub(KEYS[1],1,#KEYS[1]-#'core:profile')
local coordinator,coordEpoch,source,target,targetEpoch=ARGV[2],ARGV[3],ARGV[4],ARGV[5],ARGV[6]
local limit,low,high,maxNamespace,maxRecord,maxContexts=number(ARGV[7]),number(ARGV[8]),number(ARGV[9]),number(ARGV[10]),number(ARGV[11]),number(ARGV[12])
local offset,isolate=number(ARGV[13]),ARGV[14]=='1'
if not ref(coordinator) or not ref(coordEpoch) or not ref(source) or not ref(target) or not ref(targetEpoch) or source==target or not limit or limit<1 or limit>32 or not low or not high or high<low or not maxNamespace or not maxRecord or not maxContexts then return fail('CORRUPT') end
if KEYS[2]~=root..'core:coordinator' or KEYS[3]~=root..'core:owner-expiry' or KEYS[4]~=root..'core:worker:'..source or KEYS[5]~=root..'core:timers:'..source or KEYS[6]~=root..'core:worker:'..target or KEYS[7]~=root..'core:timers:'..target or KEYS[8]~=root..'core:capacity' or KEYS[9]~=root..'core:wake:'..target or KEYS[10]~=root..'core:worker:'..coordinator then return fail('CORRUPT') end
for _,k in ipairs({KEYS[3],KEYS[5],KEYS[7]}) do if not kind(k,'zset') then return fail('CORRUPT') end end
for _,k in ipairs({KEYS[4],KEYS[6],KEYS[8],KEYS[10]}) do if not kind(k,'hash') then return fail('CORRUPT') end end
local clock=redis.call('TIME');local now=tonumber(clock[1])*1000+math.floor(tonumber(clock[2])/1000);if now<low or now>high then return fail('EXPIRED') end
local coord=redis.call('HMGET',KEYS[2],'worker','epoch','expires_ms');if coord[1]~=coordinator or coord[2]~=coordEpoch or not number(coord[3]) or number(coord[3])<=now then return fail('AUTHORITY') end
local leader=redis.call('HMGET',KEYS[10],'epoch','expires_ms');if leader[1]~=coordEpoch or not number(leader[2]) or number(leader[2])<=now then return fail('AUTHORITY') end
local dest=redis.call('HMGET',KEYS[6],'epoch','expires_ms');if dest[1]~=targetEpoch or not number(dest[2]) or number(dest[2])<=now then return fail('AUTHORITY') end
local sourceDue=redis.call('ZSCORE',KEYS[3],'w-'..source);if not sourceDue or not number(sourceDue) then return fail('ABSENT') end;if number(sourceDue)>now then return fail('CONFLICT') end
local old=redis.call('HMGET',KEYS[4],'epoch','expires_ms');if (old[1] and not old[2]) or (old[2] and not old[1]) then return fail('CORRUPT') end
if old[1] and (not number(old[2]) or number(old[2])>now or number(old[2])~=number(sourceDue)) then return fail('CONFLICT') end
if not offset then return fail('CORRUPT') end
local heads=redis.call('ZRANGE',KEYS[5],offset,offset+limit-1,'WITHSCORES');local count=#heads/2;local remain=redis.call('ZCARD',KEYS[5]);local plans={};local delta=0
if count==0 then
 if remain~=0 then return {'OK',string.format('%.0f',now),'0','0','0','0'} end
 if not redis.acl_check_cmd('ZREM',KEYS[3],'w-'..source) or not redis.acl_check_cmd('DEL',KEYS[4]) then return fail('AUTHORITY') end
 redis.call('ZREM',KEYS[3],'w-'..source);redis.call('DEL',KEYS[4]);return {'OK',string.format('%.0f',now),'0','1','0','0'}
end
if not redis.acl_check_cmd('HLEN',KEYS[8]) then return fail('AUTHORITY') end
local cap=redis.call('HMGET',KEYS[8],'contexts','bytes');if not number(cap[1]) or not number(cap[2]) then return fail('CORRUPT') end
local function planItem(context,score)
 local due=number(score);if not ref(context) or not due or due==0 then return nil,'CORRUPT' end
 local contextKey=root..'core:context:'..context;local deadlines=root..'core:deadlines:'..context
 if not kind(contextKey,'hash') or not kind(deadlines,'zset') then return nil,'CORRUPT' end
 local v=redis.call('HMGET',contextKey,'incarnation','owner','term','revision','bytes','reserved','index_bytes','timer:worker','timer:epoch','timer:term')
 if not ref(v[1]) or type(v[2])~='string' or type(v[3])~='string' or (v[2]=='')~=(v[3]=='') or (v[2]~='' and (not ref(v[2]) or not ref(v[3]) or #v[3]>190)) or not decimal(v[4]) or v[4]=='0' or v[4]=='18446744073709551615' or not number(v[5]) or not number(v[6]) or not number(v[7]) or v[8]~=source or not ref(v[9]) or not ref(v[10]) then return nil,'CORRUPT' end
 if old[1] and v[9]~=old[1] or v[10]==targetEpoch then return nil,'CONFLICT' end
 local head=redis.call('ZRANGE',deadlines,0,0,'WITHSCORES');if #head~=2 or not ref(head[1]) or number(head[2])~=due then return nil,'CORRUPT' end
 if redis.call('ZSCORE',KEYS[7],context) then return nil,'CONFLICT' end
 local nextRevision=increment(v[4]);local nextBytes=number(v[5])+#target-#v[8]+#targetEpoch-#v[9]+#targetEpoch-#v[10]
 if nextBytes<0 or nextBytes>maxRecord then return nil,'CAPACITY' end
 local bytesText=string.format('%.0f',nextBytes)
 local pooled=redis.call('HEXISTS',contextKey,'credit_limit')==1
 local ns,problem=nsPlan(KEYS[8],contextKey,false,false,v[1],v[2],v[3],nextRevision,nextBytes,number(v[6]),number(v[7]),maxContexts,9007199254740991,pooled,0,true,maxRecord);if problem then return nil,problem end
 if number(ns.reserved)>maxRecord-nextBytes then return nil,'CAPACITY' end
 if not redis.acl_check_cmd('HSET',contextKey,'timer:worker',target,'timer:epoch',targetEpoch,'timer:term',targetEpoch,'revision',nextRevision,'bytes',bytesText) or not redis.acl_check_cmd('ZREM',KEYS[5],context) or not redis.acl_check_cmd('ZADD',KEYS[7],score,context) then return nil,'AUTHORITY' end
 return {key=contextKey,context=context,due=score,revision=nextRevision,bytes=bytesText,ns=ns}
end
for i=1,#heads,2 do
 local plan,problem=planItem(heads[i],heads[i+1])
 if not plan then
  if not isolate or problem=='AUTHORITY' then return fail(problem) end
 else
  local nextDelta=delta+number(plan.ns.bytes)-number(cap[2])
  if not isolate or (number(cap[2])+nextDelta>=0 and number(cap[2])+nextDelta<=maxNamespace) then
   plans[#plans+1]=plan;delta=nextDelta
  end
 end
end
local nextGlobal=number(cap[2])+delta;if nextGlobal<0 or nextGlobal>maxNamespace or nextGlobal>9007199254740991 then return fail('CAPACITY') end
local globalText=string.format('%.0f',nextGlobal);if not redis.acl_check_cmd('HSET',KEYS[8],'bytes',globalText) then return fail('AUTHORITY') end
local moved=#plans
if moved>0 and not redis.acl_check_cmd('PUBLISH',KEYS[9],'1') then return fail('AUTHORITY') end
local drained=remain==moved
if drained and (not redis.acl_check_cmd('ZREM',KEYS[3],'w-'..source) or not redis.acl_check_cmd('DEL',KEYS[4])) then return fail('AUTHORITY') end
for _,p in ipairs(plans) do redis.call('HSET',p.key,'timer:worker',target,'timer:epoch',targetEpoch,'timer:term',targetEpoch,'revision',p.revision,'bytes',p.bytes);redis.call('ZREM',KEYS[5],p.context);redis.call('ZADD',KEYS[7],p.due,p.context);nsApplyContext(p.ns) end
redis.call('HSET',KEYS[8],'bytes',globalText)
if drained then redis.call('ZREM',KEYS[3],'w-'..source);redis.call('DEL',KEYS[4]) end
if moved>0 then redis.call('PUBLISH',KEYS[9],'1') end
return {'OK',string.format('%.0f',now),tostring(moved),drained and '1' or '0',tostring(offset+count-moved),remain>offset+count and '1' or '0'}
`)
