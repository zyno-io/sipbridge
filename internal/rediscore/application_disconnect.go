package rediscore

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// ExpireApplication ends the exact shared control term. A delayed disconnect
// cannot expire a newer term, and the old owned-context set remains available
// to the existing owner-expiry scheduler. This never touches SIP output proof.
func (s *Store) ExpireApplication(ctx context.Context, ref ApplicationLeaseRef) error {
	if !ref.valid() {
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
	keys := []string{s.profileKey(), s.applicationKey(ref.Application), s.ownerExpiryKey(), s.coordinatorKey()}
	values, err := expireApplicationScript.Run(callCtx, s.client, keys, s.profile, ref.Application, ref.Term, low, high, s.primary).Slice()
	if err != nil {
		return fmt.Errorf("%w: %w", ErrUnknown, err)
	}
	if err = status(values); err != nil {
		return err
	}
	if len(values) != 2 {
		return errors.Join(ErrUnknown, ErrCorrupt)
	}
	now, err := responseInt(values[1])
	finish := time.Now()
	elapsed := finish.Sub(start)
	skew := finish.UnixMilli() - start.UnixMilli() - elapsed.Milliseconds()
	if err != nil || elapsed < 0 || skew < -workerClockTolerance.Milliseconds() || skew > workerClockTolerance.Milliseconds() || now < low || now > finish.UnixMilli()+workerClockTolerance.Milliseconds() || ctx.Err() != nil || callCtx.Err() != nil {
		return errors.Join(ErrUnknown, ErrClock)
	}
	return nil
}

var expireApplicationScript = redis.NewScript(primaryScript + applicationGuardScript + `
local function fail(s) return {s} end
if not primary(ARGV[6]) then return fail('AUTHORITY') end
if not appKind(KEYS[1],'string') or redis.call('GET',KEYS[1])~=ARGV[1] then return fail('AUTHORITY') end
local app,term=ARGV[2],ARGV[3];local low,high=appNumber(ARGV[4]),appNumber(ARGV[5])
if not appRef(app) or not appRef(term) or #term>190 or not low or not high or high<low then return fail('CORRUPT') end
local root=string.sub(KEYS[1],1,#KEYS[1]-#'core:profile')
if KEYS[2]~=root..'core:application:'..app or KEYS[3]~=root..'core:owner-expiry' or KEYS[4]~=root..'core:coordinator' then return fail('CORRUPT') end
if not appKind(KEYS[2],'string') or not appKind(KEYS[3],'zset') or not appKind(KEYS[4],'hash') then return fail('CORRUPT') end
local clock=redis.call('TIME');local now=tonumber(clock[1])*1000+math.floor(tonumber(clock[2])/1000)
if now<low or now>high then return fail('EXPIRED') end
local result={'OK',string.format('%.0f',now)}
if redis.call('GET',KEYS[2])~=term then return result end
local owner=root..'core:owner:'..term;local owned=root..'core:owned:'..term
if not appKind(owner,'hash') or not appKind(owned,'set') then return fail('CORRUPT') end
local v=redis.call('HMGET',owner,'application','expires_ms');local expires=appNumber(v[2])
if v[1]~=app or not expires or expires==0 then return fail('CORRUPT') end
if expires<=now then return result end
local refs=redis.call('SCARD',owned);local member='a-'..term;local indexed=redis.call('ZSCORE',KEYS[3],member)
if refs>0 and (not indexed or appNumber(indexed)~=expires) or refs==0 and indexed then return fail('CORRUPT') end
local wake
if refs>0 then
 local c=redis.call('HMGET',KEYS[4],'worker','epoch','expires_ms')
 if c[1] or c[2] or c[3] then
  if not appRef(c[1]) or not appRef(c[2]) or not appNumber(c[3]) then return fail('CORRUPT') end
  if appNumber(c[3])>now then wake=root..'core:wake:'..c[1] end
 end
end
local due=string.format('%.0f',now)
if not redis.acl_check_cmd('HSET',owner,'expires_ms',due) then return fail('AUTHORITY') end
if refs>0 and not redis.acl_check_cmd('ZADD',KEYS[3],due,member) then return fail('AUTHORITY') end
if wake and not redis.acl_check_cmd('PUBLISH',wake,'1') then return fail('AUTHORITY') end
redis.call('HSET',owner,'expires_ms',due)
if refs>0 then redis.call('ZADD',KEYS[3],due,member) end
if wake then redis.call('PUBLISH',wake,'1') end
return result
`)
