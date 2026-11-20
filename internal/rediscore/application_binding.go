package rediscore

// applicationBindingScript prepares exact ownership membership/index updates
// without mutating. The enclosing publication must call bindingApply only after
// every record, timer, output, capacity and ACL check succeeds.
const applicationBindingScript = `
local function bindingPrepare(root, context, oldApp, oldTerm, newApp, newTerm, now, expiredOnly)
 local function validPair(app,term) return (app=='' and term=='') or (appRef(app) and appRef(term) and #term<=190) end
 if not appRef(context) or not validPair(oldApp,oldTerm) or not validPair(newApp,newTerm) then return nil,'CORRUPT' end
 if oldTerm~='' and oldTerm==newTerm and oldApp~=newApp then return nil,'CONFLICT' end
 local index=root..'core:owner-expiry'; if not appKind(index,'zset') then return nil,'CORRUPT' end
 local plan={context=context,index=index,oldApp=oldApp,oldTerm=oldTerm,newApp=newApp,newTerm=newTerm,indexDelta=0}
 local oldCount,oldExpiry
 if oldTerm~='' then
  local owner=root..'core:owner:'..oldTerm; local owned=root..'core:owned:'..oldTerm; local current=root..'core:application:'..oldApp
  if not appKind(owner,'hash') or not appKind(owned,'set') or not appKind(current,'string') then return nil,'CORRUPT' end
  local v=redis.call('HMGET',owner,'application','expires_ms');oldExpiry=appNumber(v[2])
  if v[1]~=oldApp or not oldExpiry or redis.call('SISMEMBER',owned,context)~=1 then return nil,'CORRUPT' end
  oldCount=redis.call('SCARD',owned);local score=redis.call('ZSCORE',index,'a-'..oldTerm)
  if oldCount<1 or not score or tonumber(score)~=oldExpiry then return nil,'CORRUPT' end
  plan.oldOwner=owner;plan.oldOwned=owned;plan.oldCurrent=current;plan.oldExpiry=oldExpiry
 end
 if expiredOnly and (oldTerm=='' or oldExpiry>now) then return nil,'CONFLICT' end
 if oldApp==newApp and oldTerm==newTerm then return plan,nil end
 if oldTerm~='' then
  plan.indexDelta=plan.indexDelta-#context
  if not redis.acl_check_cmd('SREM',plan.oldOwned,context) then return nil,'AUTHORITY' end
  if oldCount==1 then
   plan.lastOld=true;plan.oldIsCurrent=redis.call('GET',plan.oldCurrent)==oldTerm
   if not redis.acl_check_cmd('ZREM',index,'a-'..oldTerm) then return nil,'AUTHORITY' end
   if plan.oldIsCurrent then
    if not redis.acl_check_cmd('PEXPIRE',plan.oldOwner,120000) or not redis.acl_check_cmd('PEXPIRE',plan.oldCurrent,120000) then return nil,'AUTHORITY' end
   elseif not redis.acl_check_cmd('DEL',plan.oldOwner) then return nil,'AUTHORITY' end
  end
 end
 if newTerm~='' then
  local owner=root..'core:owner:'..newTerm;local owned=root..'core:owned:'..newTerm;local current=root..'core:application:'..newApp
  if not appKind(owner,'hash') or not appKind(owned,'set') or not appKind(current,'string') then return nil,'CORRUPT' end
  local v=redis.call('HMGET',owner,'application','expires_ms');local expires=appNumber(v[2]);if v[1]~=newApp or not expires then return nil,'CORRUPT' end
  if redis.call('GET',current)~=newTerm or expires<=now then return nil,'AUTHORITY' end
  if redis.call('SISMEMBER',owned,context)~=0 then return nil,'CONFLICT' end
  local count=redis.call('SCARD',owned);local member='a-'..newTerm;local existing=redis.call('ZSCORE',index,member)
  if (count==0 and existing) or (count>0 and (not existing or tonumber(existing)~=expires)) then return nil,'CORRUPT' end
  if not redis.acl_check_cmd('SADD',owned,context) or not redis.acl_check_cmd('PERSIST',owner) or not redis.acl_check_cmd('PERSIST',current) or not redis.acl_check_cmd('ZADD',index,expires,member) then return nil,'AUTHORITY' end
  plan.indexDelta=plan.indexDelta+#context;plan.newOwner=owner;plan.newOwned=owned;plan.newCurrent=current;plan.newExpiry=expires
  local head=redis.call('ZRANGE',index,0,0,'WITHSCORES');local headDue=#head>0 and appNumber(head[2]) or nil
  local earlier=not headDue or not appRef(head[1]) or expires<headDue
  if earlier then
   local coordinator=root..'core:coordinator';if not appKind(coordinator,'hash') then return nil,'CORRUPT' end
   local c=redis.call('HMGET',coordinator,'worker','epoch','expires_ms')
   if (c[1] or c[2] or c[3]) and (not c[1] or not c[2] or not appNumber(c[3]) or not appRef(c[1]) or not appRef(c[2])) then return nil,'CORRUPT' end
   if c[1] and appNumber(c[3])>now then plan.wake=root..'core:wake:'..c[1];if not redis.acl_check_cmd('PUBLISH',plan.wake,'1') then return nil,'AUTHORITY' end end
  end
 end
 return plan,nil
end
local function bindingApply(p)
 if p.oldApp==p.newApp and p.oldTerm==p.newTerm then return end
 if p.oldTerm~='' then
  redis.call('SREM',p.oldOwned,p.context)
  if p.lastOld then
   redis.call('ZREM',p.index,'a-'..p.oldTerm)
   if p.oldIsCurrent then redis.call('PEXPIRE',p.oldOwner,120000);redis.call('PEXPIRE',p.oldCurrent,120000) else redis.call('DEL',p.oldOwner) end
  end
 end
 if p.newTerm~='' then
  redis.call('SADD',p.newOwned,p.context);redis.call('PERSIST',p.newOwner);redis.call('PERSIST',p.newCurrent);redis.call('ZADD',p.index,p.newExpiry,'a-'..p.newTerm)
  if p.wake then redis.call('PUBLISH',p.wake,'1') end
 end
end
`
