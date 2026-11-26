package rediscore

import (
	"context"
	"errors"
	"strconv"

	"github.com/redis/go-redis/v9"
)

var ErrParked = errors.New("native context parked")

const nativeWorkPage = 16
const parkedCallGrace = OutputObsoleteGrace

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
	reply, err := parkScript.Run(ctx, s.client, []string{s.profileKey(), s.contextKey(id.Context), s.workerKey(worker), key, s.ownerExpiryKey()},
		s.profile, id.Incarnation, id.Owner, id.Term, strconv.FormatUint(revision, 10), epoch, output.ID, output.Attempt, output.ProofHash[:],
		"p-"+id.Context, parkedCallGrace.Milliseconds(), s.primary).Slice()
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
if not kind(KEYS[3],'hash') or not kind(KEYS[4],'string') or not kind(KEYS[5],'zset') then return fail('CORRUPT') end
local w=redis.call('HMGET',KEYS[3],'epoch','expires_ms')
local clock=redis.call('TIME');local now=tonumber(clock[1])*1000+math.floor(tonumber(clock[2])/1000)
if w[1]~=ARGV[6] or not number(w[2]) or number(w[2])<=now then return fail('AUTHORITY') end
local v;v,problem=identity();if problem then return fail(problem) end
if v[4]~=ARGV[5] then return fail('CONFLICT') end
if ARGV[7]~='' then
 local p='o:'..ARGV[7]..':';local a=redis.call('HMGET',KEYS[2],p..'a',p..'s',p..'p')
 if a[1]~=ARGV[8] or a[2]~='P' or a[3]~=ARGV[9] then return fail('CONFLICT') end
end
if redis.call('GET',KEYS[4]) then return fail('CONFLICT') end
local grace=number(ARGV[11]);if not grace or now+grace>9007199254740991 then return fail('CORRUPT') end
local due=string.format('%.0f',now+grace)
if not redis.acl_check_cmd('SET',KEYS[4],ARGV[2]) or not redis.acl_check_cmd('ZADD',KEYS[5],due,ARGV[10]) then return fail('AUTHORITY') end
redis.call('SET',KEYS[4],ARGV[2]);redis.call('ZADD',KEYS[5],due,ARGV[10])
return {'OK'}
`)

// abandonParked releases a quarantined incarnation under the live coordinator
// lease. Only the selected context's bounded Redis structure is inspected.
func (s *Store) abandonParked(ctx context.Context, worker, epoch, id string) (DriverDiagnostic, error) {
	if !reference(id) {
		return DriverDiagnostic{}, ErrConflict
	}
	ctx, cancel := context.WithTimeout(ctx, workerRedisTimeout)
	defer cancel()
	result, err := abandonParkedScript.Run(ctx, s.client, []string{s.profileKey(), s.contextKey(id), s.coordinatorKey(), s.workerKey(worker),
		s.ownerExpiryKey(), s.root + "core:parked:" + id, s.contextDeadlineKey(id), s.selectorKey(), s.capacityKey()},
		s.profile, worker, epoch, id, s.namespaceContexts, s.namespaceBytes, parkedCallGrace.Milliseconds(), s.primary).Slice()
	if err != nil {
		return DriverDiagnostic{}, errors.Join(ErrUnknown, err)
	}
	if err = status(result); err != nil {
		return DriverDiagnostic{}, err
	}
	if len(result) == 1 {
		return DriverDiagnostic{}, nil
	}
	if len(result) != 3 {
		return DriverDiagnostic{}, ErrCorrupt
	}
	incarnation, ok := result[1].(string)
	revision, revOK := result[2].(string)
	if !ok || !revOK || !reference(incarnation) {
		return DriverDiagnostic{}, ErrCorrupt
	}
	return DriverDiagnostic{Stage: "context-abandoned", Category: "unrecoverable", Context: id, Incarnation: incarnation, Revision: revision}, nil
}

var abandonParkedScript = redis.NewScript(commonScript + namespaceScript + applicationGuardScript + applicationBindingScript + `
local problem=guard();if problem then return fail(problem) end
if not kind(KEYS[3],'hash') or not kind(KEYS[4],'hash') or not kind(KEYS[5],'zset') or not kind(KEYS[6],'string') or not kind(KEYS[8],'hash') then return fail('CORRUPT') end
local clock=redis.call('TIME');local now=tonumber(clock[1])*1000+math.floor(tonumber(clock[2])/1000)
local coord=redis.call('HMGET',KEYS[3],'worker','epoch','expires_ms')
local lease=redis.call('HMGET',KEYS[4],'epoch','expires_ms')
if coord[1]~=ARGV[2] or coord[2]~=ARGV[3] or not number(coord[3]) or number(coord[3])<=now or lease[1]~=ARGV[3] or not number(lease[2]) or number(lease[2])<=now then return fail('AUTHORITY') end
local context=ARGV[4];local member='p-'..context
local due=number(redis.call('ZSCORE',KEYS[5],member));if not due then return fail('ABSENT') end
if due>now then return fail('CONFLICT') end
local v=redis.call('HMGET',KEYS[2],'incarnation','owner','term','revision','timer:worker','credit_limit')
local inc=redis.call('GET',KEYS[6])
if not redis.acl_check_cmd('ZREM',KEYS[5],member) or not redis.acl_check_cmd('DEL',KEYS[6]) then return fail('AUTHORITY') end
if not inc or v[1]~=inc then
 redis.call('ZREM',KEYS[5],member);redis.call('DEL',KEYS[6]);return {'OK'}
end
if not appRef(inc) or not decimal(v[4]) or v[4]=='0' or redis.call('HLEN',KEYS[2])>4105 then return fail('CORRUPT') end
local grace=number(ARGV[7]);if not grace then return fail('CORRUPT') end
local root=string.sub(KEYS[1],1,#KEYS[1]-#'core:profile')
local prefix=context..'\0'..inc..'\0'
local fields=redis.call('HKEYS',KEYS[2])
local ready={};local seenOutputs={};local cutoff=0
for _,field in ipairs(fields) do
 if string.sub(field,1,2)=='o:' then
  local output=string.match(field,'^o:([A-Za-z0-9_-]+):[a-z]$')
  if not output or #output>192 then return fail('CORRUPT') end
  if not seenOutputs[output] then
   seenOutputs[output]=true
   local base='o:'..output..':'
   local expiry=number(redis.call('HGET',KEYS[2],base..'d'))
   local phase=redis.call('HGET',KEYS[2],base..'s')
   if not expiry or expiry==0 or not phase or not string.match(phase,'^[PSUWNR]$') then return fail('CORRUPT') end
   cutoff=math.max(cutoff,expiry+grace)
   local worker=redis.call('HGET',KEYS[2],base..'w')
   if not appRef(worker) then return fail('CORRUPT') end
   local key=root..'core:ready:'..worker;local ref=context..'/'..output
   if not kind(key,'zset') then return fail('CORRUPT') end
   if not redis.acl_check_cmd('ZREM',key,ref) then return fail('AUTHORITY') end
   ready[#ready+1]={key,ref}
  end
 end
end
if cutoff>now then
 if cutoff>9007199254740991 then return fail('CORRUPT') end
 local next=string.format('%.0f',cutoff)
 if not redis.acl_check_cmd('ZADD',KEYS[5],next,member) then return fail('AUTHORITY') end
 redis.call('ZADD',KEYS[5],next,member);return {'OK'}
end
local selectors={};local count=0
for _,field in ipairs(fields) do
 if string.sub(field,1,5)=='sref:' then
  local record=string.sub(field,6)
  local refs=redis.call('HGET',KEYS[2],field)
  if #refs==0 or #refs%33~=0 or count+#refs/33>512 then return fail('CORRUPT') end
  count=count+#refs/33
  for i=1,#refs,33 do
   local selector=string.sub(refs,i,i+32)
   if redis.call('HGET',KEYS[8],selector)==prefix..record then
    if not redis.acl_check_cmd('HDEL',KEYS[8],selector) then return fail('AUTHORITY') end
    selectors[#selectors+1]=selector
   end
  end
 end
end
local selectorCount=redis.call('HGET',KEYS[2],'selector_count')
if (selectorCount and number(selectorCount) or 0)~=count then return fail('CORRUPT') end
local binding;binding,problem=bindingPrepare(root,context,v[2],v[3],'','',now,false);if problem then return fail(problem) end
local limit=v[6] and number(v[6]) or 0
local plan;plan,problem=nsPlan(KEYS[9],KEYS[2],false,true,inc,v[2],v[3],v[4],0,0,0,number(ARGV[5]),number(ARGV[6]),false,-limit,true)
if problem then return fail(problem) end
local timers=nil
if v[5] then
 if not appRef(v[5]) then return fail('CORRUPT') end
 timers=root..'core:timers:'..v[5]
 if not kind(timers,'zset') then return fail('CORRUPT') end
 if not redis.acl_check_cmd('ZREM',timers,context) then return fail('AUTHORITY') end
end
if not redis.acl_check_cmd('UNLINK',KEYS[2],KEYS[7]) then return fail('AUTHORITY') end
for _,selector in ipairs(selectors) do redis.call('HDEL',KEYS[8],selector) end
for _,ref in ipairs(ready) do redis.call('ZREM',ref[1],ref[2]) end
if timers then redis.call('ZREM',timers,context) end
bindingApply(binding);nsApply(plan)
redis.call('UNLINK',KEYS[2],KEYS[7]);redis.call('DEL',KEYS[6]);redis.call('ZREM',KEYS[5],member)
return {'OK',inc,v[4]}
`)
