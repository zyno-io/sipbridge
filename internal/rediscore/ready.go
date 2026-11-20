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
	identity      Identity
	revision      uint64
	rank          int
}

// PeekReady returns bounded metadata for still-unreserved attempts assigned to
// this live worker. It never reads wire and does not grant write authority.
func (s *Store) PeekReady(ctx context.Context, worker, epoch string, limit int) ([]ReadyOutput, error) {
	items, _, err := s.peekReadyPage(ctx, worker, epoch, limit, 0)
	return items, err
}

func (s *Store) peekReadyPage(ctx context.Context, worker, epoch string, limit, offset int) ([]ReadyOutput, int, error) {
	if !reference(worker) || !reference(epoch) || limit < 1 || limit > MaxReadyPeek {
		return nil, 0, ErrConflict
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
	v, err := peekReadyScript.Run(callCtx, s.client, []string{s.profileKey(), s.workerKey(worker), s.workerReadyKey(worker)}, s.profile, worker, epoch, limit, low, high, offset, s.primary).Slice()
	if err != nil {
		return nil, 0, err
	}
	if err = status(v); err != nil {
		return nil, 0, err
	}
	const width = 14
	if len(v) < 3 || (len(v)-3)%width != 0 || (len(v)-3)/width > limit {
		return nil, 0, ErrCorrupt
	}
	now, err := responseInt(v[1])
	if err != nil {
		return nil, 0, err
	}
	finish := time.Now()
	elapsed := finish.Sub(start)
	skew := finish.UnixMilli() - start.UnixMilli() - elapsed.Milliseconds()
	if elapsed < 0 || skew < -workerClockTolerance.Milliseconds() || skew > workerClockTolerance.Milliseconds() || now < low || now > finish.UnixMilli()+workerClockTolerance.Milliseconds() {
		return nil, 0, ErrClock
	}
	if err = callCtx.Err(); err != nil {
		return nil, 0, err
	}
	next, err := responseInt(v[2])
	if err != nil || next != 0 && next != int64(offset+limit) {
		return nil, 0, ErrCorrupt
	}
	out := make([]ReadyOutput, 0, (len(v)-3)/width)
	for i := 3; i < len(v); i += width {
		var x [width]string
		for j := range x {
			value, ok := v[i+j].(string)
			if !ok {
				return nil, 0, ErrCorrupt
			}
			x[j] = value
		}
		due, e := strconv.ParseInt(x[8], 10, 64)
		revision, revErr := strconv.ParseUint(x[10], 10, 64)
		id := Identity{Context: x[0], Incarnation: x[1], Owner: x[11], Term: x[12]}
		rank, rankErr := strconv.Atoi(x[13])
		if e != nil || due <= now || due > maxExactInteger || len(x[5]) != 32 || (x[6] != "A" && x[6] != "N") || len(x[7]) == 0 || len(x[7]) > storagecodec.MaxNativeOutputBytes || !recordField(x[9]) || revErr != nil || revision == 0 || !id.valid() || rankErr != nil || rank < offset || rank >= offset+limit {
			continue
		}
		ref := OutputRef{Context: x[0], Incarnation: x[1], ID: x[2], Attempt: x[3], Worker: worker, Epoch: epoch, Connection: x[4], RequireApplication: x[6] == "A"}
		copy(ref.ProofHash[:], x[5])
		if !ref.valid() {
			continue
		}
		out = append(out, ReadyOutput{Ref: ref, Metadata: []byte(x[7]), MetadataField: x[9], DeadlineMS: due, identity: id, revision: revision, rank: rank})
	}
	return out, int(next), nil
}

var peekReadyScript = redis.NewScript(commonScript + parkedScript + `
local problem=guard();if problem then return fail(problem) end
if not kind(KEYS[3],'zset') then return fail('CORRUPT') end
local worker,epoch=ARGV[2],ARGV[3];local limit,lo,hi,offset=number(ARGV[4]),number(ARGV[5]),number(ARGV[6]),number(ARGV[7]);if not limit or limit<1 or limit>32 or not lo or not hi or hi<lo or not offset then return fail('CORRUPT') end
local clock=redis.call('TIME');local now=tonumber(clock[1])*1000+math.floor(tonumber(clock[2])/1000);if now<lo or now>hi then return fail('EXPIRED') end
local lease=redis.call('HMGET',KEYS[2],'epoch','expires_ms');if lease[1]~=epoch or not number(lease[2]) or number(lease[2])<=now then return fail('AUTHORITY') end
local root=string.sub(KEYS[1],1,#KEYS[1]-#'core:profile')
local start=offset+redis.call('ZCOUNT',KEYS[3],'-inf',string.format('%.0f',now))
local members=redis.call('ZRANGE',KEYS[3],start,start+limit-1,'WITHSCORES')
local out={'OK',string.format('%.0f',now),tostring(#members==2*limit and offset+limit or 0)}
local function selectItem(member,score,rank)
 local context,id=string.match(member,'^([A-Za-z0-9_-]+)/([A-Za-z0-9_-]+)$');local due=number(score);if not context or #context>192 or not id or #id>192 or not due or due<=now then return end
 local key=root..'core:context:'..context;if not kind(key,'hash') then return end
 local p='o:'..id..':';local v=redis.call('HMGET',key,'incarnation',p..'a',p..'w',p..'e',p..'c',p..'d',p..'x',p..'p',p..'s',p..'q','revision','owner','term')
 for j=1,13 do if not v[j] then return end end
 if v[3]~=worker or v[4]~=epoch or v[9]~='P' or number(v[6])~=due or #v[8]~=32 or (v[10]~='A' and v[10]~='N') or not string.match(v[7],'^r:[A-Za-z0-9_-]+$') or parked(root,context,v[1]) then return end
 local size=redis.call('HSTRLEN',key,v[7]);if size==0 or size>65536 then return end
 local meta=redis.call('HGET',key,v[7])
 for _,value in ipairs({context,v[1],id,v[2],v[5],v[8],v[10],meta,score,v[7],v[11],v[12],v[13],tostring(rank)}) do out[#out+1]=value end
end
for i=1,#members,2 do
 selectItem(members[i],members[i+1],offset+math.floor(i/2))
end
return out
`)
