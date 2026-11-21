package rediscore

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	WorkerLeaseLifetime     = 2 * time.Second
	WorkerRenewInterval     = time.Second
	WorkerIdleLifetime      = 120 * time.Second
	maxWorkerReferenceBytes = 190
	workerRedisTimeout      = 100 * time.Millisecond
	workerClockTolerance    = 250 * time.Millisecond
)

// WorkerLeaseRequest identifies a physical process and the validated primary.
// Both Worker and Epoch must be fresh, non-reused references for that process
// lifetime. Acquire is attempted only at startup; an uncertain acquisition
// fences that process. Expired rows need no permanent reuse tombstones because
// callers never allocate an old process reference again.
type WorkerLeaseRequest struct {
	Worker, Epoch, PrimaryRunID string
	Acquire                     bool
	// Reacquire is explicit recovery by the same unique physical process,
	// after cancelling/fencing its old local work. It never resets outputs.
	Reacquire bool
}

// WorkerHead is a hint returned atomically with the lease, not native authority.
// DeadlineMS is Redis wall time; After and Until carry the local monotonic
// request-start anchor, so network latency never extends the original deadline.
type WorkerHead struct {
	Context    string
	DeadlineMS int64
	After      time.Duration
}
type WorkerLease struct {
	Until       time.Time
	RedisTimeMS int64
	Head        WorkerHead
	Coordinator bool
	OwnerExpiry WorkerHead
	Ready       bool
}

func (s *Store) workerKey(worker string) string      { return s.root + "core:worker:" + worker }
func (s *Store) workerTimerKey(worker string) string { return s.root + "core:timers:" + worker }
func (s *Store) workerReadyKey(worker string) string { return s.root + "core:ready:" + worker }
func (s *Store) coordinatorKey() string              { return s.root + "core:coordinator" }
func (s *Store) ownerExpiryKey() string              { return s.root + "core:owner-expiry" }

func observedDeadline(start time.Time, deltaMS int64) time.Time {
	return start.Add(time.Duration(min(deltaMS, int64((1<<63-1)/time.Millisecond))) * time.Millisecond)
}

// RenewWorker renews a live epoch or acquires a fresh one. A timeout, lost
// response, malformed response, or post-mutation clock failure is UNKNOWN.
// The caller supplies its independently validated current primary run ID and
// must fence native work on any error; this primitive starts no polling loop.
func (s *Store) RenewWorker(ctx context.Context, req WorkerLeaseRequest) (WorkerLease, error) {
	if !reference(req.Worker) || len(req.Worker) > maxWorkerReferenceBytes || !reference(req.Epoch) || !validPrimary(req.PrimaryRunID) || req.Acquire && req.Reacquire {
		return WorkerLease{}, ErrConflict
	}
	if req.PrimaryRunID != s.primary {
		return WorkerLease{}, ErrAuthority
	}
	start := time.Now()
	deadline := start.Add(workerRedisTimeout)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	callCtx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	acquire := "0"
	if req.Acquire {
		acquire = "1"
	}
	if req.Reacquire {
		acquire = "2"
	}
	low := start.UnixMilli() - workerClockTolerance.Milliseconds()
	high := start.UnixMilli() + max(int64(0), deadline.Sub(start).Milliseconds()) + workerClockTolerance.Milliseconds()
	values, err := workerLeaseScript.Run(callCtx, s.client, []string{s.profileKey(), s.workerKey(req.Worker), s.workerTimerKey(req.Worker), s.workerReadyKey(req.Worker), s.coordinatorKey(), s.ownerExpiryKey()}, s.profile, req.Worker, req.Epoch, req.PrimaryRunID, acquire, low, high).Slice()
	finish := time.Now()
	if err != nil {
		return WorkerLease{}, fmt.Errorf("%w: %w", ErrUnknown, err)
	}
	if err = status(values); err != nil {
		return WorkerLease{}, err
	}
	if len(values) != 9 {
		return WorkerLease{}, errors.Join(ErrUnknown, ErrCorrupt)
	}
	number := func(i int) (int64, error) { return responseInt(values[i]) }
	now, e1 := number(1)
	until, e2 := number(2)
	due, e3 := number(4)
	ownerDue, e4 := number(7)
	contextID, ok1 := values[3].(string)
	coordinator, ok2 := values[5].(string)
	ownerID, ok3 := values[6].(string)
	ready, ok4 := values[8].(string)
	if e1 != nil || e2 != nil || e3 != nil || e4 != nil || !ok1 || !ok2 || !ok3 || !ok4 || (coordinator != "0" && coordinator != "1") || (ready != "0" && ready != "1") || (contextID != "" && !reference(contextID)) || (ownerID != "" && !reference(ownerID)) || (contextID == "") != (due == 0) || (ownerID == "") != (ownerDue == 0) || (coordinator == "0" && ownerID != "") || until != now+WorkerLeaseLifetime.Milliseconds() {
		return WorkerLease{}, errors.Join(ErrUnknown, ErrCorrupt)
	}
	elapsed := finish.Sub(start)
	wallDelta := finish.UnixMilli() - start.UnixMilli() - elapsed.Milliseconds()
	if elapsed < 0 || wallDelta < -workerClockTolerance.Milliseconds() || wallDelta > workerClockTolerance.Milliseconds() || now < low || now > finish.UnixMilli()+workerClockTolerance.Milliseconds() {
		return WorkerLease{}, errors.Join(ErrUnknown, ErrClock)
	}
	if ctx.Err() != nil || callCtx.Err() != nil || !time.Now().Before(start.Add(WorkerLeaseLifetime)) {
		return WorkerLease{}, errors.Join(ErrUnknown, ErrExpired)
	}
	after := func(due int64) time.Duration {
		if due == 0 || due <= now {
			return 0
		}
		delta := due - now
		if delta > int64((1<<63-1)/time.Millisecond) {
			return time.Duration(1<<63 - 1)
		}
		return max(0, start.Add(time.Duration(delta)*time.Millisecond).Sub(finish))
	}
	return WorkerLease{Until: start.Add(WorkerLeaseLifetime), RedisTimeMS: now, Head: WorkerHead{Context: contextID, DeadlineMS: due, After: after(due)}, Coordinator: coordinator == "1", OwnerExpiry: WorkerHead{Context: ownerID, DeadlineMS: ownerDue, After: after(ownerDue)}, Ready: ready == "1"}, nil
}

// ErrClock fences operations whose local/Redis clock evidence is unacceptable.
var ErrClock = errors.New("native Redis clock skew")

// Redis returns decimal scores as strings, avoiding Lua number round trips for
// revision identities. Deadline scores must be exact nonzero safe integers.
var workerLeaseScript = redis.NewScript(primaryScript + `
local function fail(s) return {s} end
local function kind(k,t) local v=redis.call('TYPE',k).ok; return v=='none' or v==t end
local function uint(s)
 if type(s)~='string' or not string.match(s,'^[0-9]+$') or (#s>1 and string.sub(s,1,1)=='0') or #s>16 then return nil end
 local n=tonumber(s); if not n or n>9007199254740991 or n~=math.floor(n) then return nil end; return n
end
local function head(k)
 local v=redis.call('ZRANGE',k,0,0,'WITHSCORES'); if #v==0 then return '', '0' end
 local d=uint(v[2]); if not d or d==0 or #v[1]>192 or not string.match(v[1],'^[A-Za-z0-9_-]+$') then return 'inspect', '1' end
 return v[1],v[2]
end
local function ownerHeadAfterRenew(k,member,due)
 local rows=redis.call('ZRANGE',k,0,1,'WITHSCORES');local selected,score=member,due
 for i=1,#rows,2 do
  if rows[i]~=member then
   local n=uint(rows[i+1]);if not n or n==0 or #rows[i]>192 or not string.match(rows[i],'^[A-Za-z0-9_-]+$') then return 'inspect','1' end
   if n<score or (n==score and rows[i]<selected) then selected,score=rows[i],n end
   break
  end
 end
 return selected,string.format('%.0f',score)
end
if not kind(KEYS[1],'string') or redis.call('GET',KEYS[1])~=ARGV[1] then return fail('AUTHORITY') end
if not kind(KEYS[2],'hash') or not kind(KEYS[3],'zset') or not kind(KEYS[4],'zset') or not kind(KEYS[5],'hash') or not kind(KEYS[6],'zset') then return fail('CORRUPT') end
if not primary(ARGV[4]) then return fail('AUTHORITY') end
local lo=uint(ARGV[6]); local hi=uint(ARGV[7]); if not lo or not hi or hi<lo then return fail('CORRUPT') end
if ARGV[5]~='0' and ARGV[5]~='1' and ARGV[5]~='2' then return fail('CORRUPT') end
local clock=redis.call('TIME'); local now=tonumber(clock[1])*1000+math.floor(tonumber(clock[2])/1000)
if now<lo or now>hi or now>9007199254738991 then return fail('EXPIRED') end
local fields=redis.call('HMGET',KEYS[2],'epoch','expires_ms'); local epoch=fields[1]; local expires=fields[2]
if (epoch and not expires) or (expires and not epoch) then return fail('CORRUPT') end
if epoch then
 expires=uint(expires); if not expires then return fail('CORRUPT') end
 if epoch~=ARGV[3] then return fail('CONFLICT') end
 if expires<=now and ARGV[5]~='2' then return fail('EXPIRED') end
elseif ARGV[5]~='1' and ARGV[5]~='2' then return fail('ABSENT') end
local context,due=head(KEYS[3])
local c=redis.call('HMGET',KEYS[5],'worker','epoch','expires_ms'); local coordinator='0'; local owner,ownerDue='','0'
local coordExpiry=0
if c[1] or c[2] or c[3] then
 if not c[1] or not c[2] or not c[3] then return fail('CORRUPT') end
 coordExpiry=uint(c[3]); if not coordExpiry then return fail('CORRUPT') end
end
if coordExpiry<=now or (c[1]==ARGV[2] and c[2]==ARGV[3]) then coordinator='1';owner,ownerDue=ownerHeadAfterRenew(KEYS[6],'w-'..ARGV[2],now+2000) end
local ready='0'; if redis.call('ZCOUNT',KEYS[4],'('..string.format('%.0f',now),'+inf')>0 then ready='1' end
local untilms=now+2000
local untilText=string.format('%.0f',untilms)
if not redis.acl_check_cmd('HSET',KEYS[2],'epoch',ARGV[3],'expires_ms',untilText) or not redis.acl_check_cmd('PEXPIRE',KEYS[2],120000) or not redis.acl_check_cmd('ZADD',KEYS[6],untilText,'w-'..ARGV[2]) then return fail('AUTHORITY') end
if coordinator=='1' and (not redis.acl_check_cmd('HSET',KEYS[5],'worker',ARGV[2],'epoch',ARGV[3],'expires_ms',untilText) or not redis.acl_check_cmd('PEXPIRE',KEYS[5],2000)) then return fail('AUTHORITY') end
redis.call('HSET',KEYS[2],'epoch',ARGV[3],'expires_ms',untilText);redis.call('PEXPIRE',KEYS[2],120000);redis.call('ZADD',KEYS[6],untilText,'w-'..ARGV[2])
if coordinator=='1' then redis.call('HSET',KEYS[5],'worker',ARGV[2],'epoch',ARGV[3],'expires_ms',untilText);redis.call('PEXPIRE',KEYS[5],2000) end
return {'OK',string.format('%.0f',now),untilText,context,due,coordinator,owner,ownerDue,ready}
`)
