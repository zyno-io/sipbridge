package rediscore

import (
	"context"
	"errors"
	"strconv"

	"github.com/redis/go-redis/v9"
)

var ErrParked = errors.New("native context parked")

const nativeWorkPage = 16

// Parking isolates one incarnation after an exact selected revision fails.
// It deliberately preserves canonical index membership and physical proof.
func (s *Store) park(ctx context.Context, worker, epoch string, id Identity, revision uint64, output OutputRef) error {
	if !id.valid() || revision == 0 {
		return ErrConflict
	}
	if output != (OutputRef{}) && (!output.valid() || output.Context != id.Context || output.Incarnation != id.Incarnation || output.Worker != worker || output.Epoch != epoch) {
		return ErrConflict
	}
	ctx, cancel := context.WithTimeout(ctx, workerRedisTimeout)
	defer cancel()
	key := s.root + "core:parked:" + id.Context
	reply, err := parkScript.Run(ctx, s.client, []string{s.profileKey(), s.contextKey(id.Context), s.workerKey(worker), key},
		s.profile, id.Incarnation, id.Owner, id.Term, strconv.FormatUint(revision, 10), epoch, output.ID, output.Attempt, output.ProofHash[:], s.primary).Slice()
	if err != nil {
		return err
	}
	return status(reply)
}

const parkedScript = `
local function parked(root,context,inc)
 local key=root..'core:parked:'..context
 return not kind(key,'string') or redis.call('GET',key)==inc
end
`

var parkScript = redis.NewScript(commonScript + `
local problem=guard();if problem then return fail(problem) end
if not kind(KEYS[3],'hash') or not kind(KEYS[4],'string') then return fail('CORRUPT') end
local w=redis.call('HMGET',KEYS[3],'epoch','expires_ms')
local clock=redis.call('TIME');local now=tonumber(clock[1])*1000+math.floor(tonumber(clock[2])/1000)
if w[1]~=ARGV[6] or not number(w[2]) or number(w[2])<=now then return fail('AUTHORITY') end
local v;v,problem=identity();if problem then return fail(problem) end
if v[4]~=ARGV[5] then return fail('CONFLICT') end
if ARGV[7]~='' then
 local p='o:'..ARGV[7]..':';local a=redis.call('HMGET',KEYS[2],p..'a',p..'s',p..'p')
 if a[1]~=ARGV[8] or a[2]~='P' or a[3]~=ARGV[9] then return fail('CONFLICT') end
end
local old=redis.call('GET',KEYS[4]);if old==ARGV[2] then return {'OK'} end
if old then return fail('CONFLICT') end
if not redis.acl_check_cmd('SET',KEYS[4],ARGV[2]) then return fail('AUTHORITY') end
redis.call('SET',KEYS[4],ARGV[2])
return {'OK'}
`)
