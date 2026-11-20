package rediscore

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

const DefaultNamespaceMaxContexts int64 = 131072
const DefaultNamespaceMaxBytes int64 = 1 << 30

func (s *Store) capacityKey() string { return s.root + "core:capacity" }

// RetireContext removes an empty context only after the trusted native caller
// has established its terminal boundary. No opaque record is interpreted here.
func (s *Store) RetireContext(ctx context.Context, id Identity, revision uint64, deadline time.Time, terminal bool) error {
	if !terminal || !id.valid() || revision == 0 || deadline.IsZero() || deadline.UnixMilli() <= 0 || deadline.UnixMilli() > maxExactInteger {
		return ErrConflict
	}
	v, err := retireContextScript.Run(ctx, s.client, []string{s.profileKey(), s.contextKey(id.Context), s.capacityKey(), s.contextDeadlineKey(id.Context)}, s.profile, id.Incarnation, id.Owner, id.Term, strconv.FormatUint(revision, 10), deadline.UnixMilli(), id.Context, s.namespaceContexts, s.namespaceBytes, s.primary).Slice()
	if err != nil {
		return fmt.Errorf("%w: %w", ErrUnknown, err)
	}
	if err = status(v); err != nil {
		return err
	}
	if len(v) != 1 {
		return errors.Join(ErrUnknown, ErrCorrupt)
	}
	return nil
}

// namespaceScript is composed into the same Lua transaction as each mutation.
// It charges logical string bytes and fixed eight-byte sorted-set scores; it is
// not a Redis allocator measurement. All checks precede the caller's writes.
const namespaceScript = admissionScript + `
local function nsPlan(capacity,context,creating,retiring,inc,owner,term,revision,total,reserved,indexes,maxContexts,maxBytes,spend,creditDelta,automatic,recordMaximum,instance,class,source)
 if not redis.acl_check_cmd('HLEN',capacity) then return nil,'AUTHORITY' end
 if not kind(capacity,'hash') then return nil,'CORRUPT' end
 local cap=redis.call('HMGET',capacity,'contexts','bytes');local exists=cap[1] or cap[2]
 if exists and (not cap[1] or not cap[2]) or not exists and redis.call('HLEN',capacity)~=0 then return nil,'CORRUPT' end
 if not exists and not creating then return nil,'CORRUPT' end
 local contexts=exists and number(cap[1]) or 0;local global=exists and number(cap[2]) or 0
 if not contexts or not global or not maxContexts or maxContexts==0 or not maxBytes or maxBytes==0 then return nil,'CORRUPT' end
 local names={'incarnation','owner','term','revision','bytes','reserved','index_bytes'}
 local old=redis.call('HMGET',context,unpack(names));local oldLimitText=redis.call('HGET',context,'credit_limit');local oldLimit=oldLimitText and number(oldLimitText) or 0;local oldCharge=0
 local oldAdmission=redis.call('HGET',context,'admission')
 local admission,problem=admissionPlan(capacity,oldAdmission,creating,retiring,instance,class,source)
 if problem then return nil,problem end
 if admission.value and admission.value~=oldAdmission and not redis.acl_check_cmd('HSET',context,'admission',admission.value) then return nil,'AUTHORITY' end
 if oldLimitText and (not oldLimit or oldLimit==0) then return nil,'CORRUPT' end
 creditDelta=creditDelta or 0
 if not creating then
  for i=1,#names do if not old[i] then return nil,'CORRUPT' end end
  local t=number(old[5]);local r=number(old[6]);local x=number(old[7]);if not t or not r or not x then return nil,'CORRUPT' end
  oldCharge=#context+t+r+x;for i=1,#names do oldCharge=oldCharge+#names[i]+#old[i] end
  if oldAdmission then oldCharge=oldCharge+#'admission'+#oldAdmission end
  if oldLimitText then oldCharge=oldCharge+#'credit_limit'+#oldLimitText;if r>oldLimit then return nil,'CORRUPT' end end
  if oldCharge>global or contexts==0 then return nil,'CORRUPT' end
 elseif redis.call('EXISTS',context)~=0 then return nil,'CONFLICT' end
 local nextContexts=contexts+(creating and 1 or 0)-(retiring and 1 or 0)
 if nextContexts<0 or nextContexts>maxContexts then return nil,'CAPACITY' end
 local limit=oldLimit+creditDelta;if limit<0 or limit>9007199254740991 then return nil,'CONFLICT' end
 if creditDelta~=0 and reserved~=0 then return nil,'CONFLICT' end
 if oldLimit==0 and creditDelta>0 and not creating and number(old[6])~=0 then return nil,'CONFLICT' end
 if retiring and limit~=0 then return nil,'CONFLICT' end
 local pooled=oldLimit>0 or creditDelta~=0
 recordMaximum=recordMaximum or 9007199254740991
 if not recordMaximum or total>recordMaximum then return nil,'CAPACITY' end
 if pooled and not automatic and reserved~=0 then return nil,'CONFLICT' end
 local nextCharge=0;local totalText=string.format('%.0f',total);local reservedText=string.format('%.0f',reserved);local indexText=string.format('%.0f',indexes)
 if not retiring then
  local vals={inc,owner,term,revision,totalText,reservedText,indexText}
  nextCharge=#context+total+reserved+indexes
  for i=1,#names do nextCharge=nextCharge+#names[i]+#vals[i] end
  if admission.value then nextCharge=nextCharge+#'admission'+#admission.value end
  if limit>0 then nextCharge=nextCharge+#'credit_limit'+#string.format('%.0f',limit) end
  if nextCharge>9007199254740991 then return nil,'CAPACITY' end
 end
 if pooled then
  local previous=creating and 0 or number(old[6]);local remaining=previous+math.max(0,creditDelta)
  local oldBase=creating and 0 or oldCharge-previous-#old[6]
  local growth=(nextCharge-reserved-#reservedText)-oldBase
  if spend and growth>0 then
   remaining=remaining-growth;if remaining<0 then return nil,'CAPACITY' end
  end
  remaining=math.min(remaining,limit)
  if remaining>recordMaximum-total then return nil,'CAPACITY' end
  if growth<0 then
   local refund=math.min(limit-remaining,recordMaximum-total-remaining,-growth)
   local oldDigits=#string.format('%.0f',remaining)
   -- Refund only actual freed bytes, including decimal text growth.
   for i=1,16 do
    local extra=math.max(0,#string.format('%.0f',remaining+refund)-oldDigits)
    if refund+extra<=-growth then break end
    refund=refund-1
   end
   remaining=remaining+refund
  end
  local nextText=string.format('%.0f',remaining);nextCharge=nextCharge-reserved-#reservedText+remaining+#nextText;reservedText=nextText
 elseif spend then
  if creating or retiring or reserved~=0 then return nil,'CORRUPT' end
  local oldReserved=number(old[6]);local growth=(nextCharge-#reservedText)-(oldCharge-oldReserved-#old[6])
  local remaining=oldReserved-math.max(0,growth);if remaining<0 then return nil,'CAPACITY' end
  local nextText=string.format('%.0f',remaining);nextCharge=nextCharge-#reservedText+remaining+#nextText;reservedText=nextText
 end
 local remaining=global-oldCharge;if remaining>maxBytes or nextCharge>maxBytes-remaining then return nil,'CAPACITY' end
 local nextGlobal=remaining+nextCharge
 local ctxText=string.format('%.0f',nextContexts);local globalText=string.format('%.0f',nextGlobal)
 if not redis.acl_check_cmd('HSET',capacity,'contexts',ctxText,'bytes',globalText) then return nil,'AUTHORITY' end
 if not retiring and not redis.acl_check_cmd('HSET',context,'index_bytes',indexText,'reserved',reservedText) then return nil,'AUTHORITY' end
 if not retiring and limit>0 and not redis.acl_check_cmd('HSET',context,'credit_limit',string.format('%.0f',limit)) then return nil,'AUTHORITY' end
 if not retiring and limit==0 and oldLimitText and not redis.acl_check_cmd('HDEL',context,'credit_limit') then return nil,'AUTHORITY' end
 return {capacity=capacity,context=context,contexts=ctxText,bytes=globalText,index=indexText,reserved=reservedText,limit=limit,hadLimit=oldLimitText~=false,retiring=retiring,admission=admission,oldAdmission=oldAdmission},nil
end
local function nsCurrent(capacity,context,recordDelta,indexDelta,maxContexts,maxBytes,recordMaximum)
 local v=redis.call('HMGET',context,'incarnation','owner','term','revision','bytes','reserved','index_bytes')
 for i=1,7 do if not v[i] then return nil,'CORRUPT' end end
 local total=number(v[5]);local reserved=number(v[6]);local index=number(v[7]);if not total or not reserved or not index then return nil,'CORRUPT' end
 total=total+recordDelta;index=index+indexDelta
 if total<0 or index<0 or total>9007199254740991 or index>9007199254740991 then return nil,'CORRUPT' end
 return nsPlan(capacity,context,false,false,v[1],v[2],v[3],v[4],total,reserved,index,maxContexts,maxBytes,false,0,true,recordMaximum)
end
local function nsApplyContext(p)
 if not p.retiring then redis.call('HSET',p.context,'index_bytes',p.index,'reserved',p.reserved);if p.limit>0 then redis.call('HSET',p.context,'credit_limit',string.format('%.0f',p.limit)) elseif p.hadLimit then redis.call('HDEL',p.context,'credit_limit') end end
end
local function nsApply(p)
 nsApplyContext(p)
 if p.admission.value and p.admission.value~=p.oldAdmission then redis.call('HSET',p.context,'admission',p.admission.value) end
 for field,value in pairs(p.admission.changes) do
  if value then redis.call('HSET',p.capacity,field,value) else redis.call('HDEL',p.capacity,field) end
 end
 redis.call('HSET',p.capacity,'contexts',p.contexts,'bytes',p.bytes)
end
`

var retireContextScript = redis.NewScript(commonScript + namespaceScript + applicationGuardScript + applicationBindingScript + `
local problem=guard();if problem then return fail(problem) end
local v;v,problem=identity();if problem then return fail(problem) end
if v[4]~=ARGV[5] then return fail('CONFLICT') end
local due=number(ARGV[6]);local maxContexts=number(ARGV[8]);local maxBytes=number(ARGV[9]);local clock=redis.call('TIME');local now=tonumber(clock[1])*1000+math.floor(tonumber(clock[2])/1000)
if not due or due<=now then return fail('EXPIRED') end
if not kind(KEYS[4],'zset') then return fail('CORRUPT') end
local root=string.sub(KEYS[1],1,#KEYS[1]-#'core:profile');local context=ARGV[7]
if not appRef(context) or KEYS[2]~=root..'core:context:'..context then return fail('CORRUPT') end
local binding;binding,problem=bindingPrepare(root,context,v[2],v[3],'','',now,false);if problem then return fail(problem) end
local expectedIndex=(v[2]~='' and #context or 0)
if redis.call('HLEN',KEYS[2])~=7+redis.call('HEXISTS',KEYS[2],'admission') or v[5]~='0' or v[6]~='0' or number(redis.call('HGET',KEYS[2],'index_bytes'))~=expectedIndex or redis.call('ZCARD',KEYS[4])~=0 then return fail('CONFLICT') end
local plan;plan,problem=nsPlan(KEYS[3],KEYS[2],false,true,v[1],v[2],v[3],v[4],0,0,0,maxContexts,maxBytes);if problem then return fail(problem) end
if not redis.acl_check_cmd('DEL',KEYS[2],root..'core:parked:'..context) then return fail('AUTHORITY') end
bindingApply(binding);redis.call('DEL',KEYS[2],root..'core:parked:'..context);nsApply(plan);return {'OK'}
`)
