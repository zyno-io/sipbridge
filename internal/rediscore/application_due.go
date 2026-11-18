package rediscore

import (
	"context"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

const MaxExpiredApplicationBatch = 32

// ExpiredApplicationContext is an atomic observation of one still-owned
// context. The later native release must fence this exact identity and revision.
type ExpiredApplicationContext struct {
	Identity Identity
	Revision uint64
}
type ExpiredApplicationBatch struct {
	Contexts    []ExpiredApplicationContext
	CheckedAtMS int64
}

// PeekExpiredApplication samples at most limit members without removing them.
// It requires the current live coordinator and the expired exact owner term;
// there is no whole-set read or durable work claim.
func (s *Store) PeekExpiredApplication(ctx context.Context, coordinator, epoch, term string, limit int) (ExpiredApplicationBatch, error) {
	if !reference(coordinator) || len(coordinator) > maxWorkerReferenceBytes || !reference(epoch) || !validApplicationTerm(term) || limit < 1 || limit > MaxExpiredApplicationBatch {
		return ExpiredApplicationBatch{}, ErrConflict
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
	keys := []string{s.profileKey(), s.coordinatorKey(), s.workerKey(coordinator), s.ownerExpiryKey(), s.applicationOwnerKey(term), s.applicationOwnedKey(term)}
	values, err := peekExpiredApplicationScript.Run(callCtx, s.client, keys, s.profile, coordinator, epoch, term, limit, low, high, s.primary).Slice()
	if err != nil {
		return ExpiredApplicationBatch{}, err
	}
	if err = status(values); err != nil {
		return ExpiredApplicationBatch{}, err
	}
	if len(values) < 2 || (len(values)-2)%4 != 0 || (len(values)-2)/4 > limit {
		return ExpiredApplicationBatch{}, ErrCorrupt
	}
	now, err := responseInt(values[1])
	finish := time.Now()
	elapsed := finish.Sub(start)
	skew := finish.UnixMilli() - start.UnixMilli() - elapsed.Milliseconds()
	if err != nil || elapsed < 0 || skew < -workerClockTolerance.Milliseconds() || skew > workerClockTolerance.Milliseconds() || now < low || now > finish.UnixMilli()+workerClockTolerance.Milliseconds() || ctx.Err() != nil || callCtx.Err() != nil {
		return ExpiredApplicationBatch{}, ErrClock
	}
	out := ExpiredApplicationBatch{CheckedAtMS: now, Contexts: make([]ExpiredApplicationContext, 0, (len(values)-2)/4)}
	for i := 2; i < len(values); i += 4 {
		contextID, ok1 := values[i].(string)
		inc, ok2 := values[i+1].(string)
		owner, ok3 := values[i+2].(string)
		revisionText, ok4 := values[i+3].(string)
		revision, e := strconv.ParseUint(revisionText, 10, 64)
		id := Identity{Context: contextID, Incarnation: inc, Owner: owner, Term: term}
		if !ok1 || !ok2 || !ok3 || !ok4 || e != nil || revision == 0 || !id.valid() {
			return ExpiredApplicationBatch{}, ErrCorrupt
		}
		out.Contexts = append(out.Contexts, ExpiredApplicationContext{Identity: id, Revision: revision})
	}
	return out, nil
}

var peekExpiredApplicationScript = redis.NewScript(commonScript + `
local problem=guard();if problem then return fail(problem) end
local function ref(v) return type(v)=='string' and #v>0 and #v<=192 and string.match(v,'^[A-Za-z0-9_-]+$') end
local root=string.sub(KEYS[1],1,#KEYS[1]-#'core:profile')
local coordinator,epoch,term=ARGV[2],ARGV[3],ARGV[4];local limit,low,high=number(ARGV[5]),number(ARGV[6]),number(ARGV[7])
if not ref(coordinator) or not ref(epoch) or not ref(term) or #term>190 or not limit or limit<1 or limit>32 or not low or not high or high<low then return fail('CORRUPT') end
if KEYS[2]~=root..'core:coordinator' or KEYS[3]~=root..'core:worker:'..coordinator or KEYS[4]~=root..'core:owner-expiry' or KEYS[5]~=root..'core:owner:'..term or KEYS[6]~=root..'core:owned:'..term then return fail('CORRUPT') end
if not kind(KEYS[2],'hash') or not kind(KEYS[3],'hash') or not kind(KEYS[4],'zset') or not kind(KEYS[5],'hash') or not kind(KEYS[6],'set') then return fail('CORRUPT') end
local clock=redis.call('TIME');local now=tonumber(clock[1])*1000+math.floor(tonumber(clock[2])/1000);if now<low or now>high then return fail('EXPIRED') end
local c=redis.call('HMGET',KEYS[2],'worker','epoch','expires_ms');if c[1]~=coordinator or c[2]~=epoch or not number(c[3]) or number(c[3])<=now then return fail('AUTHORITY') end
local w=redis.call('HMGET',KEYS[3],'epoch','expires_ms');if w[1]~=epoch or not number(w[2]) or number(w[2])<=now then return fail('AUTHORITY') end
local expiry=redis.call('ZSCORE',KEYS[4],'a-'..term);if not expiry then return fail('ABSENT') end
local owner=redis.call('HMGET',KEYS[5],'application','expires_ms');local due=number(owner[2]);if not ref(owner[1]) or not due or number(expiry)~=due then return fail('CORRUPT') end
if due>now then return fail('CONFLICT') end
local contexts=redis.call('SRANDMEMBER',KEYS[6],limit);if #contexts==0 then return fail('CORRUPT') end
local out={'OK',string.format('%.0f',now)}
for _,id in ipairs(contexts) do
 if not ref(id) then return fail('CORRUPT') end
 local key=root..'core:context:'..id;if not kind(key,'hash') then return fail('CORRUPT') end
 local v=redis.call('HMGET',key,'incarnation','owner','term','revision')
 if not ref(v[1]) or v[2]~=owner[1] or v[3]~=term or not decimal(v[4]) or v[4]=='0' then return fail('CORRUPT') end
 out[#out+1]=id;out[#out+1]=v[1];out[#out+1]=v[2];out[#out+1]=v[4]
end
return out
`)
