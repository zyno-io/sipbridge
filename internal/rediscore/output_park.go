package rediscore

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/redis/go-redis/v9"
)

// PrimaryRunID is the actual Redis primary pinned by this store. A controller
// must bind any transferable parked-output proof to this identity.
func (s *Store) PrimaryRunID() string { return s.primary }

// ParkPendingApplicationOutput serializes parking of one selected
// application write against physical reservation. Success proves that the
// exact attempt could never have received bytes from ReserveOutput. Existing
// parked records and ambiguous replies do not establish a fresh transition.
func (s *Store) ParkPendingApplicationOutput(ctx context.Context, id Identity, revision uint64, ref OutputRef) error {
	if !id.valid() || id.Owner == "" || id.Context != ref.Context || id.Incarnation != ref.Incarnation || revision == 0 || !ref.valid() || !ref.RequireApplication {
		return ErrConflict
	}
	callCtx, cancel := context.WithTimeout(ctx, workerRedisTimeout)
	defer cancel()
	args := s.outputArgs(ref)
	args = append(args, ref.Context, id.Owner, id.Term, strconv.FormatUint(revision, 10), outputClass(ref), s.maxBytes, s.namespaceContexts, s.namespaceBytes, s.primary)
	keys := []string{s.profileKey(), s.contextKey(ref.Context), s.workerReadyKey(ref.Worker), s.capacityKey()}
	reply, err := parkPendingApplicationOutputScript.Run(callCtx, s.client, keys, args...).Slice()
	if err != nil {
		return fmt.Errorf("%w: %w", ErrUnknown, err)
	}
	if err = status(reply); err != nil {
		return err
	}
	if len(reply) != 1 {
		return errors.Join(ErrUnknown, ErrCorrupt)
	}
	if callCtx.Err() != nil {
		return errors.Join(ErrUnknown, callCtx.Err())
	}
	return nil
}

var parkPendingApplicationOutputScript = redis.NewScript(outputScript + parkedScript + `
local problem=guard();if problem then return fail(problem) end
local root=string.sub(KEYS[1],1,#KEYS[1]-#'core:profile')
if KEYS[2]~=root..'core:context:'..ARGV[9] or KEYS[3]~=root..'core:ready:'..ARGV[5] or KEYS[4]~=root..'core:capacity' or not kind(KEYS[3],'zset') then return fail('CORRUPT') end
local out;out,problem=output();if problem then return fail(problem) end
local a=out.values
local owner=redis.call('HMGET',KEYS[2],'owner','term','revision','r:delivery-tag')
if a[13]~='A' or a[14]~=ARGV[10] or a[15]~=ARGV[11] or owner[1]~=ARGV[10] or owner[2]~=ARGV[11] or owner[4]~=a[16] then return fail('AUTHORITY') end
if owner[3]~=ARGV[12] or a[8]~='P' or a[9]~='0' or a[10]~='0' then return fail('CONFLICT') end
if parked(root,ARGV[9],ARGV[2]) then return fail('PARKED') end
local clock=redis.call('TIME');local now=tonumber(clock[1])*1000+math.floor(tonumber(clock[2])/1000)
if number(a[5])<=now then return fail('EXPIRED') end
problem=guardApplication(root,a[14],a[15],now);if problem then return fail(problem) end
problem=guardApplicationAttachment(root,a[14],a[15],a[18],now);if problem then return fail(problem) end
local member=ARGV[9]..'/'..ARGV[3];local score=redis.call('ZSCORE',KEYS[3],member)
if not score or number(score)~=number(a[5]) then return fail('CONFLICT') end
local ns;ns,problem=nsCurrent(KEYS[4],KEYS[2],0,-#member-8,number(ARGV[#ARGV-2]),number(ARGV[#ARGV-1]),number(ARGV[#ARGV-3]));if problem then return fail(problem) end
if not redis.acl_check_cmd('HSET',KEYS[2],out.prefix..'s','R') or not redis.acl_check_cmd('ZREM',KEYS[3],member) then return fail('AUTHORITY') end
redis.call('HSET',KEYS[2],out.prefix..'s','R');redis.call('ZREM',KEYS[3],member);nsApply(ns)
return {'OK'}
`)
