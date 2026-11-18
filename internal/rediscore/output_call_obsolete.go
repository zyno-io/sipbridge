package rediscore

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// ForgetObsoleteCallOutput releases one exact UNKNOWN proof only after the native
// caller proves the applicable terminal graph and its immutable terminal cutoff. The
// indexed terminal head and all native ownership remain for reconciliation.
func (s *Store) ForgetObsoleteCallOutput(ctx context.Context, due *DueContext, ref OutputRef, terminalCutoffMS int64, nativeObsolete bool) error {
	if !nativeObsolete || due == nil || !due.Identity.valid() || due.Identity.Context != ref.Context || due.Identity.Incarnation != ref.Incarnation || due.Revision == 0 || !due.Assignment.valid() || !reference(due.Member) || due.Member != "call-retire" || due.DeadlineMS <= 0 || due.DeadlineMS > maxExactInteger || due.CheckedAtMS < due.DeadlineMS || !ref.valid() || terminalCutoffMS <= 0 || terminalCutoffMS > maxExactInteger {
		return ErrConflict
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
	args = append(args, due.Identity.Owner, due.Identity.Term, strconv.FormatUint(due.Revision, 10), due.Assignment.Worker, due.Assignment.Epoch, due.Assignment.Term, due.Member, due.DeadlineMS, low, high, OutputObsoleteGrace.Milliseconds(), "1", terminalCutoffMS, outputClass(ref), s.maxBytes, s.namespaceContexts, s.namespaceBytes, s.primary)
	keys := []string{s.profileKey(), s.contextKey(ref.Context), s.workerKey(due.Assignment.Worker), s.workerTimerKey(due.Assignment.Worker), s.contextDeadlineKey(ref.Context), s.workerReadyKey(ref.Worker), s.capacityKey()}
	v, err := forgetObsoleteCallOutputScript.Run(callCtx, s.client, keys, args...).Slice()
	finish := time.Now()
	if err != nil {
		return fmt.Errorf("%w: %w", ErrUnknown, err)
	}
	if err = status(v); err != nil {
		return err
	}
	if len(v) != 2 {
		return errors.Join(ErrUnknown, ErrCorrupt)
	}
	now, err := responseInt(v[1])
	elapsed := finish.Sub(start)
	skew := finish.UnixMilli() - start.UnixMilli() - elapsed.Milliseconds()
	if err != nil || elapsed < 0 || skew < -workerClockTolerance.Milliseconds() || skew > workerClockTolerance.Milliseconds() || now < low || now > finish.UnixMilli()+workerClockTolerance.Milliseconds() || ctx.Err() != nil || callCtx.Err() != nil {
		return errors.Join(ErrUnknown, ErrCorrupt)
	}
	return nil
}

var forgetObsoleteCallOutputScript = redis.NewScript(outputScript + outputReleaseScript + `
local problem=guard();if problem then return fail(problem) end
local root=string.sub(KEYS[1],1,#KEYS[1]-#'core:profile');local context=string.sub(KEYS[2],#root+#'core:context:'+1)
if KEYS[3]~=root..'core:worker:'..ARGV[12] or KEYS[4]~=root..'core:timers:'..ARGV[12] or KEYS[5]~=root..'core:deadlines:'..context or KEYS[6]~=root..'core:ready:'..ARGV[5] or KEYS[7]~=root..'core:capacity' then return fail('CORRUPT') end
if not kind(KEYS[3],'hash') or not kind(KEYS[4],'zset') or not kind(KEYS[5],'zset') or not kind(KEYS[6],'zset') then return fail('CORRUPT') end
local out;out,problem=output();if problem then return fail(problem) end;local a=out.values
if a[8]~='U' or ARGV[20]~='1' then return fail('CONFLICT') end
local owner=redis.call('HMGET',KEYS[2],'incarnation','owner','term','revision','timer:worker','timer:epoch','timer:term')
if owner[1]~=ARGV[2] or owner[2]~=ARGV[9] or owner[3]~=ARGV[10] or owner[4]~=ARGV[11] or owner[5]~=ARGV[12] or owner[6]~=ARGV[13] or owner[7]~=ARGV[14] then return fail('CONFLICT') end
local physical,due,lo,hi,grace,ended=number(a[5]),number(ARGV[16]),number(ARGV[17]),number(ARGV[18]),number(ARGV[19]),number(ARGV[21])
if not physical or physical==0 or not due or not lo or not hi or hi<lo or grace~=120000 or physical>9007199254740991-grace or not ended or ended==0 or due~=math.max(ended,physical+grace) or ARGV[15]~='call-retire' then return fail('CONFLICT') end
local clock=redis.call('TIME');local now=tonumber(clock[1])*1000+math.floor(tonumber(clock[2])/1000);if now<lo or now>hi then return fail('EXPIRED') end;if now<due then return fail('CONFLICT') end
local lease=redis.call('HMGET',KEYS[3],'epoch','expires_ms');if lease[1]~=ARGV[13] or not number(lease[2]) or number(lease[2])<=now then return fail('AUTHORITY') end
local indexed=redis.call('ZSCORE',KEYS[4],context);local semantic=redis.call('ZSCORE',KEYS[5],ARGV[15]);if not indexed or number(indexed)~=due or not semantic or number(semantic)~=due then return fail('CONFLICT') end
if redis.call('ZSCORE',KEYS[6],context..'/'..ARGV[3]) then return fail('CORRUPT') end
local release;release,problem=outputReleasePlan(out,KEYS[7]);if problem then return fail(problem) end
outputReleaseApply(release)
return {'OK',string.format('%.0f',now)}
`)
