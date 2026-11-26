package rediscore

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// ExpireOutput applies only the exact output deadline proof. It does not pop
// the native timer or perform the subsequent receipt/semantic transition.
func (s *Store) ExpireOutput(ctx context.Context, due *DueContext, ref OutputRef) (OutputState, error) {
	if due == nil || !due.Identity.valid() || due.Identity.Context != ref.Context || due.Identity.Incarnation != ref.Incarnation || due.Revision == 0 || !due.Assignment.valid() || !reference(due.Member) || !strings.HasPrefix(due.Member, "out-") || due.DeadlineMS <= 0 || due.DeadlineMS > maxExactInteger || due.CheckedAtMS < due.DeadlineMS || !ref.valid() {
		return "", ErrConflict
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
	args := s.outputArgs(ref)
	args = append(args, due.Identity.Owner, due.Identity.Term, strconv.FormatUint(due.Revision, 10), due.Assignment.Worker, due.Assignment.Epoch, due.Assignment.Term, due.Member, due.DeadlineMS, low, high, outputClass(ref), s.maxBytes, s.namespaceContexts, s.namespaceBytes, s.primary)
	keys := []string{s.profileKey(), s.contextKey(ref.Context), s.workerKey(due.Assignment.Worker), s.workerTimerKey(due.Assignment.Worker), s.contextDeadlineKey(ref.Context), s.workerReadyKey(ref.Worker), s.capacityKey()}
	result, err := expireOutputScript.Run(callCtx, s.client, keys, args...).Slice()
	finish := time.Now()
	if err != nil {
		return "", fmt.Errorf("%w: %w", ErrUnknown, err)
	}
	if err = status(result); err != nil {
		return "", err
	}
	if len(result) != 3 {
		return "", errors.Join(ErrUnknown, ErrCorrupt)
	}
	now, err := responseInt(result[1])
	phase, ok := result[2].(string)
	elapsed := finish.Sub(start)
	skew := finish.UnixMilli() - start.UnixMilli() - elapsed.Milliseconds()
	if err != nil || !ok || elapsed < 0 || skew < -workerClockTolerance.Milliseconds() || skew > workerClockTolerance.Milliseconds() || now < low || now > finish.UnixMilli()+workerClockTolerance.Milliseconds() || ctx.Err() != nil || callCtx.Err() != nil {
		return "", errors.Join(ErrUnknown, ErrCorrupt)
	}
	switch OutputState(phase) {
	case OutputUnknown, OutputWritten, OutputNotWritten:
		return OutputState(phase), nil
	default:
		return "", errors.Join(ErrUnknown, ErrCorrupt)
	}
}

var expireOutputScript = redis.NewScript(outputScript + `
local problem=guard();if problem then return fail(problem) end
local root=string.sub(KEYS[1],1,#KEYS[1]-#'core:profile');local context=string.sub(KEYS[2],#root+#'core:context:'+1)
if KEYS[3]~=root..'core:worker:'..ARGV[12] or KEYS[4]~=root..'core:timers:'..ARGV[12] or KEYS[5]~=root..'core:deadlines:'..context or KEYS[6]~=root..'core:ready:'..ARGV[5] or KEYS[7]~=root..'core:capacity' then return fail('CORRUPT') end
if not kind(KEYS[3],'hash') or not kind(KEYS[4],'zset') or not kind(KEYS[5],'zset') or not kind(KEYS[6],'zset') then return fail('CORRUPT') end
local out;out,problem=output();if problem then return fail(problem) end;local a=out.values
local owner=redis.call('HMGET',KEYS[2],'incarnation','owner','term','revision','timer:worker','timer:epoch','timer:term')
if owner[1]~=ARGV[2] or owner[2]~=ARGV[9] or owner[3]~=ARGV[10] or owner[4]~=ARGV[11] or owner[5]~=ARGV[12] or owner[6]~=ARGV[13] or owner[7]~=ARGV[14] then return fail('CONFLICT') end
local due,lo,hi=number(ARGV[16]),number(ARGV[17]),number(ARGV[18]);if not due or due==0 or not lo or not hi or hi<lo or due~=number(a[5]) or ARGV[15]~='out-'..string.sub(a[11],3) then return fail('CONFLICT') end
local clock=redis.call('TIME');local now=tonumber(clock[1])*1000+math.floor(tonumber(clock[2])/1000);if now<lo or now>hi then return fail('EXPIRED') end;if now<due then return fail('CONFLICT') end
local lease=redis.call('HMGET',KEYS[3],'epoch','expires_ms');if lease[1]~=ARGV[13] or not number(lease[2]) or number(lease[2])<=now then return fail('AUTHORITY') end
local indexed=redis.call('ZSCORE',KEYS[4],context);local semantic=redis.call('ZSCORE',KEYS[5],ARGV[15]);if not indexed or number(indexed)~=due or not semantic or number(semantic)~=due then return fail('CONFLICT') end
local phase=a[8];local next=phase;local ns=nil;local member=context..'/'..ARGV[3]
if phase=='P' then
 local score=redis.call('ZSCORE',KEYS[6],member);if not score or number(score)~=due then return fail('CONFLICT') end
 ns,problem=nsCurrent(KEYS[7],KEYS[2],0,-#member-8,number(ARGV[#ARGV-2]),number(ARGV[#ARGV-1]),number(ARGV[#ARGV-3]));if problem then return fail(problem) end
 if not redis.acl_check_cmd('ZREM',KEYS[6],member) then return fail('AUTHORITY') end
 next='N'
elseif phase=='R' then
 if redis.call('ZSCORE',KEYS[6],member) then return fail('CORRUPT') end
 next='N'
elseif phase=='S' then next='U' end
if next~=phase and not redis.acl_check_cmd('HSET',KEYS[2],out.prefix..'s',next) then return fail('AUTHORITY') end
if phase=='P' then redis.call('ZREM',KEYS[6],member) end
if next~=phase then redis.call('HSET',KEYS[2],out.prefix..'s',next) end
if ns then nsApply(ns) end
return {'OK',string.format('%.0f',now),next}
`)
