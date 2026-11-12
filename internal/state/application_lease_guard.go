package state

import (
	"encoding/json"

	"github.com/google/uuid"
)

const applicationLeaseRecordLua = `
local function validApplicationLeaseID(value)
 return type(value)=='string' and #value>0 and #value<=96 and string.match(value,'^[A-Za-z0-9_.:@+%-]+$')~=nil
end
local function validApplicationLeaseTerm(value)
 if type(value)~='string' or #value~=36 or value=='00000000-0000-0000-0000-000000000000' then return false end
 if string.sub(value,9,9)~='-' or string.sub(value,14,14)~='-' or string.sub(value,19,19)~='-' or string.sub(value,24,24)~='-' then return false end
 local hex=string.gsub(value,'-','')
 return #hex==32 and string.match(hex,'^[0-9a-f]+$')~=nil
end
local function validLeaseHash(value)
 return type(value)=='string' and #value==64 and string.match(value,'^[0-9a-f]+$')~=nil
end
local function constantHashEqual(a,b)
 if not validLeaseHash(a) or not validLeaseHash(b) then return false end
 local difference=0
 for n=1,64 do difference=bit.bor(difference,bit.bxor(string.byte(a,n),string.byte(b,n))) end
 return difference==0
end
local function leaseInteger(value,minimum)
 local n=value and tonumber(value)
 if not n or n<minimum or n>9007199254740991 or n~=math.floor(n) or value~=integer(n) then return nil end
 return n
end
local function readApplicationLeaseRecord(key,expected,idle)
 if not keytype(key,'hash') then return nil,'CORRUPT' end
 if redis.call('EXISTS',key)==0 then return nil,nil end
 local fields=redis.call('HLEN',key)
 if fields~=6 and fields~=18 then return nil,'CORRUPT' end
 local v=redis.call('HMGET',key,'id','term','expiresAtMs','references','sealed','recoveryUntilMs',
  'recoverable','generation','principalHash','holderId','holderEpoch','tokenHash','reattachTimeoutMs',
  'lastAttachId','lastAttachTokenHash','lastAttachNextTokenHash','lastAttachExpectedGeneration','attachmentUntilMs')
 local expiry,references,recovery=leaseInteger(v[3],1),leaseInteger(v[4],0),leaseInteger(v[6],0)
 if v[1]~=expected or not validApplicationLeaseTerm(v[2]) or not expiry or not references or not recovery or v[5]~='0' and v[5]~='1' then return nil,'CORRUPT' end
 local ttl=redis.call('PTTL',key)
 if references>0 and ttl~=-1 or references==0 and (ttl<0 or ttl>idle) then return nil,'CORRUPT' end
 local saved={term=v[2],expires=expiry,references=references,sealed=v[5]=='1',recoveryUntil=recovery,generation=0,recoverable=false}
 if fields==18 then
  local generation,timeout,lastGeneration=leaseInteger(v[8],1),leaseInteger(v[13],1),leaseInteger(v[17],0)
  local attachmentUntil=leaseInteger(v[18],0)
  if v[7]~='1' or not attachmentUntil or attachmentUntil>0 and expiry>attachmentUntil or not generation or not timeout or timeout>10000 or not validLeaseHash(v[9]) or
     not validApplicationLeaseID(v[10]) or not validApplicationLeaseID(v[11]) or not validLeaseHash(v[12]) or not lastGeneration or
     recovery~=0 and recovery~=(attachmentUntil>0 and attachmentUntil or expiry+timeout) then return nil,'CORRUPT' end
  if generation==1 then
   if v[14]~='' or v[15]~='' or v[16]~='' or lastGeneration~=0 then return nil,'CORRUPT' end
  elseif not validApplicationLeaseID(v[14]) or not validLeaseHash(v[15]) or not validLeaseHash(v[16]) or lastGeneration~=generation-1 or not constantHashEqual(v[16],v[12]) then return nil,'CORRUPT' end
  saved.recoverable=true; saved.generation=generation; saved.principal=v[9]; saved.holder=v[10]; saved.epoch=v[11]; saved.token=v[12]
  saved.reattachTimeout=timeout; saved.lastAttach=v[14]; saved.lastToken=v[15]; saved.lastNextToken=v[16]; saved.lastGeneration=lastGeneration; saved.attachmentUntil=attachmentUntil
 end
 return saved,nil
end
local function applicationLeaseObservation(id,saved,now)
 return {id=id,term=saved and saved.term or '',expiresAtMs=saved and saved.expires or 0,checkedAtMs=now,
  recoveryUntilMs=saved and saved.recoveryUntil or 0,recoverable=saved and saved.recoverable or false,
  generation=saved and saved.generation or 0,principalHash=saved and saved.principal or '',
  holderId=saved and saved.holder or '',holderEpoch=saved and saved.epoch or '',
  reattachTimeoutMs=saved and saved.reattachTimeout or 0,attachmentUntilMs=saved and saved.attachmentUntil or 0,sealed=saved and saved.sealed or false,lastAttachId=saved and saved.lastAttach or ''}
end
-- Recoverable owners freeze their deadline at lease expiry, never first read.
-- Legacy owners retain the original observed five-second recovery semantics.
local function observeApplicationLeaseRecovery(key,saved,now)
 if saved and not saved.sealed and saved.expires<=now and saved.recoveryUntil==0 then
  saved.recoveryUntil=saved.recoverable and (saved.attachmentUntil>0 and saved.attachmentUntil or saved.expires+saved.reattachTimeout) or now+5000
  redis.call('HSET',key,'recoveryUntilMs',integer(saved.recoveryUntil))
 end
end
`

const applicationLeaseGuardLua = `
local function applicationLeaseObservations(encoded,input,now)
 local ids={}; local seen={}
 local function append(values)
  if values==nil then return nil end
  if type(values)~='table' or #values>1024 then return 'CORRUPT' end
  for key,id in pairs(values) do
   if type(key)~='number' or key<1 or key>#values or key~=math.floor(key) or not validApplicationLeaseID(id) then return 'CORRUPT' end
  end
  for _,id in ipairs(values) do if not seen[id] then seen[id]=true; table.insert(ids,id) end end
  if #ids>1024 then return 'CAPACITY' end
  return nil
 end
 local err=append(cjson.decode(encoded)); if err then return nil,err end
 if input~='' then err=append(cjson.decode(input).applicationLeaseIds); if err then return nil,err end end
 local result={}
 if #ids>0 then
  local prefix=ARGV[#ARGV-2]
  for _,id in ipairs(ids) do
   local saved,problem=readApplicationLeaseRecord(prefix..id,id,120000); if problem then return nil,problem end
   observeApplicationLeaseRecovery(prefix..id,saved,now)
   result[id]=applicationLeaseObservation(id,saved,now)
  end
 end
 return cjson.encode(result),nil
end
local function applicationLeaseGuards(encoded,now)
 local guards=cjson.decode(encoded)
 if type(guards)~='table' or #guards>1024 then return 'CORRUPT' end
 if #guards==0 then return nil end
 local prefix=ARGV[#ARGV-2]
 for _,guard in ipairs(guards) do
  if type(guard)~='table' or not validApplicationLeaseID(guard.id) or not validApplicationLeaseTerm(guard.term) then return 'CORRUPT' end
  local lease,err=readApplicationLeaseRecord(prefix..guard.id,guard.id,120000)
  if err then return err end
  if not lease or lease.term~=guard.term or lease.generation~=(guard.generation or 0) or lease.expires<=now or lease.sealed or lease.recoverable and lease.attachmentUntil>0 then return 'STALE' end
 end
 return nil
end
-- Validate every affected record before any mutation; Lua errors do not roll
-- back earlier writes. References retain owner terms across arbitrary outages.
local function planApplicationLeases(before,after,fences,now)
 local prefix=ARGV[#ARGV-2]
 local changes={}; local seen={}
 local function add(id,delta)
  if not validApplicationLeaseID(id) then return 'CORRUPT' end
  if not changes[id] then
   local saved,err=readApplicationLeaseRecord(prefix..id,id,120000)
   if err then return err end
   changes[id]={saved=saved,delta=0,seal=false}; table.insert(seen,id)
  end
  changes[id].delta=changes[id].delta+delta
  return nil
 end
 local old,new={},{}
 for _,id in ipairs(cjson.decode(before)) do old[id]=true end
 for _,id in ipairs(cjson.decode(after)) do new[id]=true end
 for id in pairs(old) do if not new[id] then local err=add(id,-1); if err then return nil,err end end end
 for id in pairs(new) do if not old[id] then local err=add(id,1); if err then return nil,err end end end
 for _,fence in ipairs(cjson.decode(fences)) do
  if not validApplicationLeaseTerm(fence.term) then return nil,'CORRUPT' end
  local err=add(fence.id,0); if err then return nil,err end
  local change=changes[fence.id]; local saved=change.saved
  if saved and saved.term==fence.term and saved.generation~=(fence.generation or 0) then return nil,'STALE' end
  if saved and saved.term==fence.term then
   if not saved.sealed and (saved.expires>now or saved.recoveryUntil==0 or saved.recoveryUntil>now) then return nil,'STALE' end
   change.seal=true
  end
 end
 for _,id in ipairs(seen) do
  local change=changes[id]
  if not change.saved then
   if change.delta>0 then return nil,'CORRUPT' end
  else
   change.references=change.saved.references+change.delta
   if change.references<0 or change.references>9007199254740991 then return nil,'CORRUPT' end
   if change.references>0 and not redis.acl_check_cmd('PERSIST',prefix..id) then return nil,'AUTHORITY' end
   local expiry,problem=planApplicationExpiry(id,change.saved.expires,change.references,'',change.saved.references==0 and change.references>0)
   if problem then return nil,problem end
   change.expiry=expiry
  end
 end
 return {changes=changes,ids=seen,prefix=prefix},nil
end
local function applyApplicationLeases(plan)
 for _,id in ipairs(plan.ids) do
  local change=plan.changes[id]
  if change.saved then
   local key=plan.prefix..id
   redis.call('HSET',key,'references',integer(change.references),'sealed',(change.seal or change.saved.sealed) and '1' or '0')
   if change.references>0 then redis.call('PERSIST',key) else redis.call('PEXPIRE',key,120000) end
   applyApplicationExpiry(change.expiry)
  end
 end
end

`

func validateApplicationLeaseIDs(ids []string) error {
	if len(ids) > applicationLeaseLimit {
		return ErrCapacity
	}
	seen := make(map[string]bool, len(ids))
	for _, id := range ids {
		if !ValidReference(id) || seen[id] {
			return ErrCorrupt
		}
		seen[id] = true
	}
	return nil
}

func validApplicationLeaseRef(ref ApplicationLeaseRef) bool {
	term, err := uuid.Parse(ref.Term)
	return ref.Generation <= 1<<53-1 && ValidReference(ref.ID) && err == nil && term != uuid.Nil && term.String() == ref.Term
}

func decodeApplicationLeases(record *Record, encoded string, now int64, inputIDs []string) error {
	if json.Unmarshal([]byte(encoded), &record.ApplicationLeases) != nil {
		return ErrCorrupt
	}
	ids := make(map[string]bool)
	for _, id := range append(append([]string(nil), record.Payload.ApplicationLeaseIDs...), inputIDs...) {
		ids[id] = true
	}
	if len(ids) != len(record.ApplicationLeases) {
		return ErrCorrupt
	}
	for id := range ids {
		lease, ok := record.ApplicationLeases[id]
		if !ok || lease.ID != id || lease.CheckedAtMS != now || lease.ExpiresAtMS < 0 || lease.ExpiresAtMS > 1<<53-1 || lease.RecoveryUntilMS < 0 || lease.RecoveryUntilMS > 1<<53-1 || lease.AttachmentUntilMS < 0 || lease.AttachmentUntilMS > 1<<53-1 || lease.AttachmentUntilMS > 0 && (!lease.Recoverable || lease.ExpiresAtMS > lease.AttachmentUntilMS) ||
			lease.Term == "" && (lease.RecoveryUntilMS != 0 || lease.Generation != 0 || lease.Recoverable) ||
			lease.Recoverable && (lease.Generation == 0 || !validLeaseHolder(ApplicationLeaseHolder{lease.PrincipalHash, lease.HolderID, lease.HolderEpoch}) || lease.ReattachTimeoutMS < 1 || lease.ReattachTimeoutMS > ApplicationReattachTimeout.Milliseconds()) ||
			!lease.Recoverable && lease.Generation != 0 ||
			(lease.Term == "") != (lease.ExpiresAtMS == 0) || lease.Term != "" && !validApplicationLeaseRef(ApplicationLeaseRef{ID: id, Term: lease.Term, Generation: lease.Generation}) {
			return ErrCorrupt
		}
	}
	return nil
}

type applicationLeaseEffect struct {
	Status EffectStatus
	Lease  ApplicationLeaseRef
}

func snapshotApplicationEffects(effects []Effect) map[string]applicationLeaseEffect {
	snapshot := make(map[string]applicationLeaseEffect)
	for _, effect := range effects {
		if effect.ApplicationLease != nil {
			snapshot[effect.ID] = applicationLeaseEffect{Status: effect.Status, Lease: *effect.ApplicationLease}
		}
	}
	return snapshot
}

func commitApplicationLeaseGuards(previous Payload, next Payload) ([]ApplicationLeaseRef, error) {
	return commitApplicationLeaseGuardsForEffects(snapshotApplicationEffects(previous.Effects), next)
}

func commitApplicationLeaseGuardsForEffects(previous map[string]applicationLeaseEffect, next Payload) ([]ApplicationLeaseRef, error) {
	guards := make([]ApplicationLeaseRef, 0, len(next.CommitLeaseGuards))
	terms := make(map[string]ApplicationLeaseRef)
	add := func(ref ApplicationLeaseRef) error {
		if !validApplicationLeaseRef(ref) {
			return ErrCorrupt
		}
		if term, exists := terms[ref.ID]; exists {
			if term != ref {
				return ErrConflict
			}
			return nil
		}
		if len(terms) >= applicationLeaseLimit {
			return ErrCapacity
		}
		terms[ref.ID] = ref
		guards = append(guards, ref)
		return nil
	}
	for _, guard := range next.CommitLeaseGuards {
		if err := add(guard); err != nil {
			return nil, err
		}
	}
	for _, effect := range next.Effects {
		before, exists := previous[effect.ID]
		if exists && (effect.ApplicationLease == nil || before.Lease != *effect.ApplicationLease) {
			return nil, ErrConflict
		}
		if effect.ApplicationLease != nil && effect.Status == EffectDispatching && (!exists || before.Status != EffectDispatching) {
			if err := add(*effect.ApplicationLease); err != nil {
				return nil, err
			}
		}
	}
	return guards, nil
}
