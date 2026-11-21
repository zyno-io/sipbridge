package rediscore

import "github.com/redis/go-redis/v9"

const generationLeaseScript = `
local function generationNow()
 local clock=redis.call('TIME');return tonumber(clock[1])*1000+math.floor(tonumber(clock[2])/1000)
end
local function liveGenerationWorker(key,epoch,now)
 if not kind(key,'hash') then return false end
 local w=redis.call('HMGET',key,'epoch','expires_ms');local expiry=number(w[2])
 return w[1]==epoch and expiry and expiry>now
end
`

var generationHeartbeatScript = redis.NewScript(commonScript + generationLeaseScript + `
local problem=guard();if problem then return fail(problem) end
local now=generationNow()
if not liveGenerationWorker(KEYS[3],ARGV[2],now) then return fail('AUTHORITY') end
local last=number(redis.call('HGET',KEYS[2],ARGV[3]));if not last then return fail('AUTHORITY') end
redis.call('HSET',KEYS[2],ARGV[3],string.format('%.0f',math.max(now,last)));return {'OK'}
`)

var claimGenerationScript = redis.NewScript(commonScript + generationLeaseScript + `
local problem=guard();if problem then return fail(problem) end
local now=generationNow()
if not liveGenerationWorker(KEYS[3],ARGV[3],now) then return fail('AUTHORITY') end
if not kind(KEYS[4],'hash') or not kind(KEYS[5],'string') or not kind(KEYS[6],'hash') then return fail('CORRUPT') end
local coordinator=redis.call('HMGET',KEYS[4],'worker','epoch','expires_ms')
local expiry=number(coordinator[3])
if coordinator[1]~=ARGV[2] or coordinator[2]~=ARGV[3] or not expiry or expiry<=now then return {'OK','0'} end
local selfId,targetId,age,ttl=number(ARGV[4]),number(ARGV[5]),number(ARGV[7]),number(ARGV[8])
local newest=number(redis.call('HGET',KEYS[2],'newest'))
if not selfId or not targetId or targetId==0 or targetId>=selfId or not newest or newest<selfId or not age or not ttl then return fail('CORRUPT') end
local last=redis.call('HGET',KEYS[2],ARGV[5]);if not last then return {'OK','0'} end
last=number(last);if not last or last==0 then return fail('CORRUPT') end
local previous=redis.call('HGET',KEYS[6],'expires_ms');local previousExpiry=number(previous)
if previous and not previousExpiry then return fail('CORRUPT') end
if last>now-age or (previousExpiry and previousExpiry>now) then return {'OK','0'} end
if not redis.acl_check_cmd('SET',KEYS[7],ARGV[6],'NX','EX',ttl) or not redis.acl_check_cmd('DEL',KEYS[5]) then return fail('AUTHORITY') end
if not redis.call('SET',KEYS[7],ARGV[6],'NX','EX',ttl) then return {'OK','0'} end
redis.call('DEL',KEYS[5]);return {'OK','1'}
`)

var sweepGenerationScript = redis.NewScript(commonScript + `
local problem=guard();if problem then return fail(problem) end
if redis.call('GET',KEYS[3])~=ARGV[2] or redis.call('EXISTS',KEYS[4])~=0 then return fail('CONFLICT') end
if ARGV[5]=='1' then
 if #KEYS~=4 then return fail('CORRUPT') end
 if not redis.acl_check_cmd('HDEL',KEYS[2],ARGV[4]) or not redis.acl_check_cmd('DEL',KEYS[3]) then return fail('AUTHORITY') end
 redis.call('HDEL',KEYS[2],ARGV[4]);redis.call('DEL',KEYS[3]);return {'OK','0'}
end
local prefix=ARGV[3];local ttl=number(ARGV[6]);local removed=0
if ARGV[5]~='0' or not ttl or ttl==0 or #KEYS>68 then return fail('CORRUPT') end
if not redis.acl_check_cmd('PEXPIRE',KEYS[3],ttl) then return fail('AUTHORITY') end
local singleton={applications=true,capacity=true,coordinator=true,['owner-expiry']=true,selectors=true}
local family={application=true,context=true,deadlines=true,owned=true,owner=true,parked=true,ready=true,timers=true,worker=true}
local retired={}
for i=5,#KEYS do
 if string.sub(KEYS[i],1,#prefix)~=prefix or KEYS[i]==KEYS[4] then return fail('CONFLICT') end
 local tail=string.sub(KEYS[i],#prefix+1)
 local label,ref=string.match(tail,'^([a-z]+):([A-Za-z0-9_-]+)$')
 if singleton[tail] or (label and family[label] and #ref<=192) then retired[#retired+1]=KEYS[i] end
end
if #retired>0 then removed=redis.call('UNLINK',unpack(retired)) end
redis.call('PEXPIRE',KEYS[3],ttl);return {'OK',tostring(removed)}
`)
