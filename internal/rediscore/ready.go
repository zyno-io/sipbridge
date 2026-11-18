package rediscore

import (
	"context"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
	"sipbridge/internal/storagecodec"
)

const MaxReadyPeek = 32

type ReadyOutput struct {
	Ref           OutputRef
	Metadata      []byte
	MetadataField string
	DeadlineMS    int64
}

// PeekReady returns bounded metadata for still-unreserved attempts assigned to
// this live worker. It never reads wire and does not grant write authority.
func (s *Store) PeekReady(ctx context.Context, worker, epoch string, limit int) ([]ReadyOutput, error) {
	if !reference(worker) || !reference(epoch) || limit < 1 || limit > MaxReadyPeek {
		return nil, ErrConflict
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
	v, err := peekReadyScript.Run(callCtx, s.client, []string{s.profileKey(), s.workerKey(worker), s.workerReadyKey(worker)}, s.profile, worker, epoch, limit, low, high, s.primary).Slice()
	if err != nil {
		return nil, err
	}
	if err = status(v); err != nil {
		return nil, err
	}
	if len(v) < 2 || (len(v)-2)%10 != 0 || (len(v)-2)/10 > limit {
		return nil, ErrCorrupt
	}
	now, err := responseInt(v[1])
	if err != nil {
		return nil, err
	}
	finish := time.Now()
	elapsed := finish.Sub(start)
	skew := finish.UnixMilli() - start.UnixMilli() - elapsed.Milliseconds()
	if elapsed < 0 || skew < -workerClockTolerance.Milliseconds() || skew > workerClockTolerance.Milliseconds() || now < low || now > finish.UnixMilli()+workerClockTolerance.Milliseconds() {
		return nil, ErrClock
	}
	if err = callCtx.Err(); err != nil {
		return nil, err
	}
	out := make([]ReadyOutput, 0, (len(v)-2)/10)
	for i := 2; i < len(v); i += 10 {
		var x [10]string
		for j := range x {
			value, ok := v[i+j].(string)
			if !ok {
				return nil, ErrCorrupt
			}
			x[j] = value
		}
		due, e := strconv.ParseInt(x[8], 10, 64)
		if e != nil || due <= now || due > maxExactInteger || len(x[5]) != 32 || (x[6] != "A" && x[6] != "N") || len(x[7]) == 0 || len(x[7]) > storagecodec.MaxNativeOutputBytes || !recordField(x[9]) {
			return nil, ErrCorrupt
		}
		ref := OutputRef{Context: x[0], Incarnation: x[1], ID: x[2], Attempt: x[3], Worker: worker, Epoch: epoch, Connection: x[4], RequireApplication: x[6] == "A"}
		copy(ref.ProofHash[:], x[5])
		if !ref.valid() {
			return nil, ErrCorrupt
		}
		out = append(out, ReadyOutput{Ref: ref, Metadata: []byte(x[7]), MetadataField: x[9], DeadlineMS: due})
	}
	return out, nil
}

var peekReadyScript = redis.NewScript(commonScript + `
local problem=guard();if problem then return fail(problem) end
if not kind(KEYS[3],'zset') then return fail('CORRUPT') end
local worker,epoch=ARGV[2],ARGV[3];local limit,lo,hi=number(ARGV[4]),number(ARGV[5]),number(ARGV[6]);if not limit or limit<1 or limit>32 or not lo or not hi or hi<lo then return fail('CORRUPT') end
local clock=redis.call('TIME');local now=tonumber(clock[1])*1000+math.floor(tonumber(clock[2])/1000);if now<lo or now>hi then return fail('EXPIRED') end
local lease=redis.call('HMGET',KEYS[2],'epoch','expires_ms');if lease[1]~=epoch or not number(lease[2]) or number(lease[2])<=now then return fail('AUTHORITY') end
local root=string.sub(KEYS[1],1,#KEYS[1]-#'core:profile');local members=redis.call('ZRANGEBYSCORE',KEYS[3],'('..string.format('%.0f',now),'+inf','WITHSCORES','LIMIT',0,limit);local out={'OK',string.format('%.0f',now)}
for i=1,#members,2 do
 local context,id=string.match(members[i],'^([A-Za-z0-9_-]+)/([A-Za-z0-9_-]+)$');local due=number(members[i+1]);if not context or #context>192 or not id or #id>192 or not due or due<=now then return fail('CORRUPT') end
 local key=root..'core:context:'..context;if not kind(key,'hash') then return fail('CORRUPT') end
 local p='o:'..id..':';local v=redis.call('HMGET',key,'incarnation',p..'a',p..'w',p..'e',p..'c',p..'d',p..'x',p..'p',p..'s',p..'q')
 for j=1,10 do if not v[j] then return fail('CORRUPT') end end
 if v[3]~=worker or v[4]~=epoch or v[9]~='P' or number(v[6])~=due or #v[8]~=32 or (v[10]~='A' and v[10]~='N') or not string.match(v[7],'^r:[A-Za-z0-9_-]+$') then return fail('CORRUPT') end
 local size=redis.call('HSTRLEN',key,v[7]);if size==0 or size>65536 then return fail('CORRUPT') end
 local meta=redis.call('HGET',key,v[7]);out[#out+1]=context;out[#out+1]=v[1];out[#out+1]=id;out[#out+1]=v[2];out[#out+1]=v[5];out[#out+1]=v[8];out[#out+1]=v[10];out[#out+1]=meta;out[#out+1]=members[i+1];out[#out+1]=v[7]
end
return out
`)
