package state

import "github.com/redis/go-redis/v9"

func stateScript(body string) *redis.Script {
	return redis.NewScript(`
local started=redis.call('TIME')
local started_ms=tonumber(started[1])*1000+math.floor(tonumber(started[2])/1000)
local observed_low=started_ms; local observed_high=started_ms
local function execute()
` + commonLua + applicationLeaseRecordLua + applicationLeaseGuardLua + applicationContextIndexLua + dispatchIndexLua + body + `
end
local result=execute()
local finished=redis.call('TIME')
local finished_ms=tonumber(finished[1])*1000+math.floor(tonumber(finished[2])/1000)
observed_low=math.min(observed_low,finished_ms); observed_high=math.max(observed_high,finished_ms)
return {'SIPSTATE-CLOCK-V1',string.format('%.0f',observed_low),string.format('%.0f',observed_high),result}
`)
}

// Group operations share one partition hash tag; scheduler batches span selected
// partitions on the supported single primary. INFO is checked on the same Redis
// execution that reads/mutates state; a reconnect cannot transplant a claim.
const commonLua = `
local function fail(code) return { 'ERROR', code } end
local function nowms()
 local t=redis.call('TIME'); local now=tonumber(t[1])*1000+math.floor(tonumber(t[2])/1000)
 observed_low=math.min(observed_low,now); observed_high=math.max(observed_high,now); return now
end
local function integer(n) return string.format('%.0f',n) end
local clock_now=nowms()
if clock_now<tonumber(ARGV[#ARGV-1]) or clock_now>tonumber(ARGV[#ARGV]) then return fail('CLOCK') end
local function primary()
 return string.match(redis.call('INFO','server'),'run_id:([a-f0-9]+)')==ARGV[2] and
        string.match(redis.call('INFO','replication'),'role:([^\r\n]+)')=='master'
end
local function keytype(key, expected)
 local t=redis.call('TYPE',key).ok; return t=='none' or t==expected
end
local function hintsafe()
 return keytype(KEYS[2],'zset') and keytype(KEYS[3],'zset')
end
local function schedulerGuard(key,holder)
 if not keytype(key,'hash') then return 'CORRUPT' end
 local untilms=tonumber(redis.call('HGET',key,'until') or '0')
 if not untilms or untilms<0 or untilms>9007199254740991 then return 'CORRUPT' end
 if redis.call('HGET',key,'holder')~=holder or redis.call('HGET',key,'authority')~=ARGV[1] or
  redis.call('HGET',key,'primary')~=ARGV[2] or untilms<=nowms() then return 'STALE' end
 return nil
end
local function scheduleProblem()
 if not keytype(KEYS[4],'zset') or not keytype(KEYS[5],'stream') then return 'CORRUPT' end
 if not redis.acl_check_cmd('ZRANGE',KEYS[2],0,0,'WITHSCORES') or
  not redis.acl_check_cmd('ZRANGE',KEYS[3],0,0,'WITHSCORES') or
  not redis.acl_check_cmd('ZADD',KEYS[4],1,KEYS[2]) or not redis.acl_check_cmd('ZREM',KEYS[4],KEYS[2]) or
  not redis.acl_check_cmd('ZSCORE',KEYS[4],KEYS[2]) or
  not redis.acl_check_cmd('XADD',KEYS[5],'MAXLEN','=',1,'*','changed','1') or
  not redis.acl_check_cmd('XREAD','COUNT',1,'STREAMS',KEYS[5],'0-0') or
  not redis.acl_check_cmd('XREVRANGE',KEYS[5],'+','-','COUNT',1) then return 'AUTHORITY' end
 local wake=redis.call('XREVRANGE',KEYS[5],'+','-','COUNT',1)
 if #wake>0 then
  local ms,seq=string.match(wake[1][1],'^(%d+)%-(%d+)$')
  if not ms or #ms>16 or #seq>16 or tonumber(ms)>9007199254740991 or tonumber(seq)>9007199254740991 then return 'CORRUPT' end
 end
 return nil
end
local function scheduleScore(value)
 local score=tonumber(value)
 if not score or score>9007199254740991 or score~=math.floor(score) then return 1 end
 return math.max(1,score)
end
local function checkgroup()
 if redis.call('EXISTS',KEYS[1])==0 then return 'NOT_FOUND' end
 if redis.call('HGET',KEYS[1],'authority')~=ARGV[1] then return 'AUTHORITY' end
 if redis.call('HGET',KEYS[1],'namespace')~=ARGV[3] then return 'CONFLICT' end
 if ARGV[4]~='' and redis.call('HGET',KEYS[1],'callId')~=ARGV[4] then return 'CONFLICT' end
 if redis.call('HGET',KEYS[1],'versions')~=ARGV[5] then return 'VERSION' end
 if ARGV[6]~='' and redis.call('HGET',KEYS[1],'incarnation')~=ARGV[6] then return 'STALE' end
 if redis.call('PTTL',KEYS[1])~=-1 then return 'CORRUPT' end
 if not redis.call('HGET',KEYS[1],'revision') or not redis.call('HGET',KEYS[1],'data') then return 'CORRUPT' end
 return nil
end
local function schedule(member, ready, due, forceWake)
 if ready and ready>0 then redis.call('ZADD',KEYS[2],ready,member) else redis.call('ZREM',KEYS[2],member) end
 if due and due>0 then redis.call('ZADD',KEYS[3],due,member) else redis.call('ZREM',KEYS[3],member) end
 local changed=false
 for offset=2,3 do
  local previous=redis.call('ZSCORE',KEYS[4],KEYS[offset])
  local head=redis.call('ZRANGE',KEYS[offset],0,0,'WITHSCORES')
  if #head==0 then redis.call('ZREM',KEYS[4],KEYS[offset]) else
   local score=scheduleScore(head[2])
   redis.call('ZADD',KEYS[4],score,KEYS[offset])
   if not previous or score<tonumber(previous) then changed=true end
  end
 end
 if changed or forceWake then redis.call('XADD',KEYS[5],'MAXLEN','=',1,'*','changed','1') end
end
`

var readScript = stateScript(`
if not primary() then return fail('AUTHORITY') end
local err=checkgroup(); if err then return fail(err) end
local now=nowms()
local leases,problem=applicationLeaseObservations(redis.call('HGET',KEYS[1],'applicationLeaseIds') or '[]',redis.call('HGET',KEYS[1],'pending') or '',now)
if problem then return fail(problem) end
return {'RECORD',integer(now),redis.call('HGET',KEYS[1],'incarnation'),redis.call('HGET',KEYS[1],'revision'),
 redis.call('HGET',KEYS[1],'data'),redis.call('HGET',KEYS[1],'receipts') or '[]',redis.call('HGET',KEYS[1],'pending') or '',
 redis.call('HGET',KEYS[1],'callId'),redis.call('HGET',KEYS[1],'proof') or '',leases}
`)

var beginScript = stateScript(`
if not primary() then return fail('AUTHORITY') end
if not hintsafe() or not keytype(KEYS[1],'hash') then return fail('CORRUPT') end
local scheduleError=scheduleProblem(); if scheduleError then return fail(scheduleError) end
local now=nowms(); local exists=redis.call('EXISTS',KEYS[1])==1
local inc=ARGV[7]; local rev='0'; local data=ARGV[11]; local receipts='[]'; local pending=''; local digest=''; local reserve=0
if exists then
 local err=checkgroup(); if err then return fail(err) end
 inc=redis.call('HGET',KEYS[1],'incarnation'); rev=redis.call('HGET',KEYS[1],'revision'); data=redis.call('HGET',KEYS[1],'data')
 receipts=redis.call('HGET',KEYS[1],'receipts') or '[]'; pending=redis.call('HGET',KEYS[1],'pending') or ''
 digest=redis.call('HGET',KEYS[1],'pendingDigest') or ''; reserve=tonumber(redis.call('HGET',KEYS[1],'reservedBytes') or '0')
else
 if ARGV[12]=='' then return fail('NOT_FOUND') end
 local input=cjson.decode(ARGV[12]); if not input.create or ARGV[6]~='' or ARGV[4]=='' then return fail('NOT_FOUND') end
end
local proof=redis.call('HGET',KEYS[1],'proof') or ''
if proof~='' then
 local r=cjson.decode(proof)
 if tonumber(r.expiresMs)>now and r.operationId==ARGV[14] then
  if r.inputDigest~=ARGV[13] then return fail('CONFLICT') end
  return {'RECEIPT',proof}
 end
end
local live=0
for _,r in ipairs(cjson.decode(receipts)) do
 if tonumber(r.expiresMs)>now then
  live=live+1
  if r.operationId==ARGV[14] then
   if r.inputDigest~=ARGV[13] then return fail('CONFLICT') end
   return {'RECEIPT',cjson.encode(r)}
  end
 end
end
if pending~='' then
 local input=cjson.decode(pending)
 if input.id==ARGV[14] and digest~=ARGV[13] then return fail('CONFLICT') end
 if tonumber(redis.call('HGET',KEYS[1],'claimUntilMs') or '0')>now then return fail('BUSY') end
else
 if ARGV[12]=='' then return fail('IDLE') end
 if tonumber(ARGV[15])<=now then return fail('EXPIRED') end
 local candidate=cjson.decode(ARGV[12])
 if live>=tonumber(ARGV[19]) and not candidate.proof then return fail('CAPACITY') end
 pending=ARGV[12]; digest=ARGV[13]
end
local admitted=cjson.decode(pending)
local leases,problem=applicationLeaseObservations(redis.call('HGET',KEYS[1],'applicationLeaseIds') or '[]',pending,now)
if problem then return fail(problem) end
local outcomeReserve=tonumber(ARGV[20]); if not admitted.proof then outcomeReserve=outcomeReserve*2 end
local projected=redis.call('HGET',KEYS[1],'applicationLeaseIds') or '[]'; local projectionBytes=projected=='[]' and 0 or #projected
local contexts=redis.call('HGET',KEYS[1],'applicationContexts') or '[]'; if contexts~='[]' then projectionBytes=projectionBytes+#contexts end
if #pending>tonumber(ARGV[18]) or #data+#receipts+#pending+4096+reserve+outcomeReserve+projectionBytes>tonumber(ARGV[17]) then return fail('CAPACITY') end
local counter=redis.call('HGET',KEYS[1],'claimCounter') or '0'
if not string.match(counter,'^%d+$') or (#counter>1 and string.sub(counter,1,1)=='0') or #counter>19 or (#counter==19 and counter>='9223372036854775807') then return fail('CAPACITY') end
redis.call('HINCRBY',KEYS[1],'claimCounter',1)
local token=redis.call('HGET',KEYS[1],'claimCounter'); local untilms=now+tonumber(ARGV[10])
redis.call('HSET',KEYS[1],'authority',ARGV[1],'namespace',ARGV[3],'callId',ARGV[4]~='' and ARGV[4] or redis.call('HGET',KEYS[1],'callId'),
 'versions',ARGV[5],'incarnation',inc,'revision',rev,'data',data,'receipts',receipts,'pending',pending,'pendingDigest',digest,
 'claimId',ARGV[8],'claimant',ARGV[9],'claimUntilMs',integer(untilms),'reservedBytes',integer(reserve))
schedule(KEYS[1],untilms,tonumber(redis.call('HGET',KEYS[1],'dueAtMs') or '0'))
return {'STEP',integer(now),inc,rev,data,receipts,pending,digest,token,integer(untilms),redis.call('HGET',KEYS[1],'callId'),proof,leases}
`)

const claimedCommitLua = `
if not primary() then return fail('AUTHORITY') end
local err=checkgroup(); if err then return fail(err) end
if not hintsafe() then return fail('CORRUPT') end
local scheduleError=scheduleProblem(); if scheduleError then return fail(scheduleError) end
local now=nowms()
if redis.call('HGET',KEYS[1],'claimId')~=ARGV[7] or redis.call('HGET',KEYS[1],'claimCounter')~=ARGV[8] or
 redis.call('HGET',KEYS[1],'claimant')~=ARGV[9] or redis.call('HGET',KEYS[1],'revision')~=ARGV[10] or
 redis.call('HGET',KEYS[1],'pendingDigest')~=ARGV[11] or (redis.call('HGET',KEYS[1],'pending') or '')=='' or
 tonumber(redis.call('HGET',KEYS[1],'claimUntilMs') or '0')<=now then return fail('STALE') end
local oldPending=cjson.decode(redis.call('HGET',KEYS[1],'pending'))
local keepProof=true
local isCAS=false
`

const casCommitLua = `
if not primary() then return fail('AUTHORITY') end
if not hintsafe() or not keytype(KEYS[1],'hash') then return fail('CORRUPT') end
local scheduleError=scheduleProblem(); if scheduleError then return fail(scheduleError) end
local now=nowms()
local oldPending=cjson.decode(ARGV[25])
local keepProof=oldPending.kind=='command'
local isCAS=true
if tonumber(oldPending.deadlineMs)<=now then return fail('EXPIRED') end
if redis.call('EXISTS',KEYS[1])==1 then
 local err=checkgroup(); if err then return fail(err) end
 if redis.call('HGET',KEYS[1],'revision')~=ARGV[10] or
    (redis.call('HGET',KEYS[1],'claimCounter') or '0')~=ARGV[26] or
    (redis.call('HGET',KEYS[1],'pending') or '')~='' then return fail('STALE') end
else
 if not oldPending.create or ARGV[4]=='' or ARGV[10]~='0' or ARGV[26]~='0' then return fail('STALE') end
end
`

const commitResultLua = `
local receipt=cjson.decode(ARGV[14])
if receipt.operationId~=oldPending.id or receipt.inputDigest~=ARGV[11] or receipt.fingerprint~=oldPending.fingerprint or
 receipt.revision~=ARGV[8] or tonumber(receipt.expiresMs)~=oldPending.receiptUntilMs then return fail('CONFLICT') end
local leaseError=applicationLeaseGuards(ARGV[22],now)
if leaseError then
 if not isCAS and leaseError=='STALE' then
  redis.call('HSET',KEYS[1],'claimId','','claimant','','claimUntilMs','0')
  schedule(KEYS[1],now,tonumber(redis.call('HGET',KEYS[1],'dueAtMs') or '0'))
 end
 return fail(leaseError)
end
local leasePlan,leaseProblem=planApplicationLeases(redis.call('HGET',KEYS[1],'applicationLeaseIds') or '[]',ARGV[23],ARGV[24],now)
if leaseProblem then
 if not isCAS and leaseProblem=='STALE' then
  redis.call('HSET',KEYS[1],'claimId','','claimant','','claimUntilMs','0')
  schedule(KEYS[1],now,tonumber(redis.call('HGET',KEYS[1],'dueAtMs') or '0'))
 end
 return fail(leaseProblem)
end
local contextPlan,contextProblem=planApplicationContexts(redis.call('HGET',KEYS[1],'applicationContexts') or '[]',ARGV[#ARGV-4])
if contextProblem then return fail(contextProblem) end
local dispatchPlan,dispatchProblem=planDispatch(redis.call('HGET',KEYS[1],'dispatchWorkers') or '[]',ARGV[#ARGV-3])
if dispatchProblem then return fail(dispatchProblem) end
local retained={}
for _,r in ipairs(cjson.decode(redis.call('HGET',KEYS[1],'receipts') or '[]')) do
 if tonumber(r.expiresMs)>now then table.insert(retained,r) end
end
local proof=redis.call('HGET',KEYS[1],'proof') or ''
if oldPending.proof then
 if keepProof then proof=ARGV[14] end
elseif tonumber(receipt.expiresMs)>now then table.insert(retained,receipt) end
if #retained>tonumber(ARGV[20]) then return fail('CAPACITY') end
local encodedReceipts=cjson.encode(retained)
if #retained==0 then encodedReceipts='[]' end
local projectionBytes=(ARGV[23]=='[]' and 0 or #ARGV[23])+(ARGV[#ARGV-4]=='[]' and 0 or #ARGV[#ARGV-4])+(ARGV[#ARGV-3]=='[]' and 0 or #ARGV[#ARGV-3])+(dispatchPlan.encoded=='[]' and 0 or #dispatchPlan.encoded)
if #ARGV[12]+#encodedReceipts+4096+tonumber(ARGV[18])+tonumber(ARGV[21])+projectionBytes>tonumber(ARGV[19]) then return fail('CAPACITY') end
local retire=tonumber(ARGV[17]); local due=tonumber(ARGV[16])
if retire>0 then
 for _,r in ipairs(retained) do retire=math.max(retire,tonumber(r.expiresMs)) end
 if proof~='' then retire=math.max(retire,tonumber(cjson.decode(proof).expiresMs)) end
 due=retire
end
applyApplicationLeases(leasePlan)
applyApplicationContexts(contextPlan)
if isCAS then
 redis.call('HSET',KEYS[1],'authority',ARGV[1],'namespace',ARGV[3],'callId',ARGV[4],
  'versions',ARGV[5],'incarnation',ARGV[6],'claimCounter',ARGV[8])
end
redis.call('HSET',KEYS[1],'revision',ARGV[8],'data',ARGV[12],'receipts',encodedReceipts,'proof',proof,
 'pending','','pendingDigest','','claimId','','claimant','','claimUntilMs','0',
 'readyAtMs',ARGV[15],'dueAtMs',integer(due),'retireAtMs',integer(retire),'reservedBytes',ARGV[18],'applicationLeaseIds',ARGV[23],'applicationContexts',ARGV[#ARGV-4],'dispatchWorkers',dispatchPlan.encoded,'dispatchTargets',ARGV[#ARGV-3])
local physical=applyDispatch(dispatchPlan,now)
schedule(KEYS[1],tonumber(ARGV[15]),due,physical)
return {'RECEIPT',ARGV[14]}
`

var commitScript = stateScript(claimedCommitLua + commitResultLua)
var compareAndSwapScript = stateScript(casCommitLua + commitResultLua)

var readUpdateScript = stateScript(`
if not primary() then return fail('AUTHORITY') end
if not keytype(KEYS[1],'hash') then return fail('CORRUPT') end
local now=nowms()
local leases,problem=applicationLeaseObservations(redis.call('HGET',KEYS[1],'applicationLeaseIds') or '[]',ARGV[7],now)
if problem then return fail(problem) end
if redis.call('EXISTS',KEYS[1])==0 then return {'ABSENT',integer(now),leases} end
local err=checkgroup(); if err then return fail(err) end
return {'RECORD',integer(now),redis.call('HGET',KEYS[1],'incarnation'),redis.call('HGET',KEYS[1],'revision'),
 redis.call('HGET',KEYS[1],'data'),redis.call('HGET',KEYS[1],'receipts') or '[]',redis.call('HGET',KEYS[1],'callId'),
 redis.call('HGET',KEYS[1],'proof') or '',redis.call('HGET',KEYS[1],'claimCounter') or '0',leases,
 redis.call('HGET',KEYS[1],'pending') or ''}
`)

var repairScript = stateScript(`
if not primary() then return fail('AUTHORITY') end
if ARGV[7]~='' then local problem=schedulerGuard(KEYS[6],ARGV[7]); if problem then return fail(problem) end end
local err=checkgroup(); if err then return fail(err) end
if not hintsafe() then return fail('CORRUPT') end
local scheduleError=scheduleProblem(); if scheduleError then return fail(scheduleError) end
local ready=tonumber(redis.call('HGET',KEYS[1],'readyAtMs') or '0')
if (redis.call('HGET',KEYS[1],'pending') or '')~='' then ready=math.max(nowms(),tonumber(redis.call('HGET',KEYS[1],'claimUntilMs') or '0')) end
local saved=redis.call('HGET',KEYS[1],'dispatchWorkers') or '[]'
local targets=redis.call('HGET',KEYS[1],'dispatchTargets') or '[]'
local plan,problem=planDispatch(saved,targets); if problem then return fail(problem) end
local physical=applyDispatch(plan,nowms())
redis.call('HSET',KEYS[1],'dispatchWorkers',plan.encoded)
schedule(KEYS[1],ready,tonumber(redis.call('HGET',KEYS[1],'dueAtMs') or '0'),physical)
return {'OK'}
`)

var retireScript = stateScript(`
if not primary() then return fail('AUTHORITY') end
local err=checkgroup(); if err then return fail(err) end
if not hintsafe() then return fail('CORRUPT') end
local scheduleError=scheduleProblem(); if scheduleError then return fail(scheduleError) end
if redis.call('HGET',KEYS[1],'revision')~=ARGV[7] then return fail('STALE') end
local untilms=tonumber(redis.call('HGET',KEYS[1],'retireAtMs') or '0')
if untilms<=0 or untilms>nowms() or (redis.call('HGET',KEYS[1],'pending') or '')~='' then return fail('NOT_READY') end
local plan,problem=planApplicationLeases(redis.call('HGET',KEYS[1],'applicationLeaseIds') or '[]','[]','[]',nowms())
if problem then return fail(problem) end
local contexts,problem=planApplicationContexts(redis.call('HGET',KEYS[1],'applicationContexts') or '[]','[]')
if problem then return fail(problem) end
local dispatch,problem=planDispatch(redis.call('HGET',KEYS[1],'dispatchWorkers') or '[]','[]')
if problem then return fail(problem) end
applyApplicationLeases(plan)
applyApplicationContexts(contexts)
applyDispatch(dispatch,nowms())
redis.call('DEL',KEYS[1]); schedule(KEYS[1],0,0); return {'OK'}
`)

var hintsScript = stateScript(`
if not primary() then return fail('AUTHORITY') end
if not hintsafe() then return fail('CORRUPT') end
local key=ARGV[7]=='ready' and KEYS[2] or KEYS[3]
local entries=redis.call('ZRANGEBYSCORE',key,'-inf',integer(nowms()),'WITHSCORES','LIMIT',0,ARGV[8])
local result={'HINTS'}; for _,entry in ipairs(entries) do table.insert(result,entry) end; return result
`)

var scheduleHintsScript = stateScript(`
if not primary() then return fail('AUTHORITY') end
if ARGV[8]~='' then local problem=schedulerGuard(KEYS[#KEYS],ARGV[8]); if problem then return fail(problem) end end
local cutoff=integer(nowms())
local function selectHints(key)
 if not keytype(key,'zset') then return fail('CORRUPT') end
 local entries=redis.call('ZRANGEBYSCORE',key,'-inf',cutoff,'WITHSCORES','LIMIT',0,ARGV[7])
 local result={'HINTS'}; for _,entry in ipairs(entries) do table.insert(result,entry) end; return result
end
local result={'SCHEDULE_HINTS'}
for offset=1,#KEYS-1,2 do
 table.insert(result,{selectHints(KEYS[offset]),selectHints(KEYS[offset+1])})
end
return result
`)
