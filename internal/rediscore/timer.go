package rediscore

import "time"

// TimerAssignment is the exact, non-reused derivative executor identity. Term
// is an opaque string: never round-trip it through a Lua number.
type TimerAssignment struct{ Worker, Epoch, Term string }

func (a TimerAssignment) empty() bool { return a.Worker == "" && a.Epoch == "" && a.Term == "" }
func (a TimerAssignment) valid() bool {
	return reference(a.Worker) && reference(a.Epoch) && reference(a.Term)
}

// TimerChange touches one active semantic record deadline, not an output
// deadline. Delete removes the duty; zero deadlines are never stored.
type TimerChange struct {
	Member string
	Due    time.Time
	Delete bool
}
type TimerMutation struct {
	Expected, Target TimerAssignment
	Changes          []TimerChange
	// RequireLease fences due execution, independent of ordinary ingress updates.
	RequireLease bool
}

func (m TimerMutation) valid() bool {
	if (!m.Expected.empty() && !m.Expected.valid()) || (!m.Target.empty() && !m.Target.valid()) || len(m.Changes) == 0 || len(m.Changes) > maxFields {
		return false
	}
	seen := make(map[string]bool, len(m.Changes))
	for _, c := range m.Changes {
		if !reference(c.Member) || seen[c.Member] || c.Delete && !c.Due.IsZero() || !c.Delete && (c.Due.IsZero() || c.Due.UnixMilli() <= 0 || c.Due.UnixMilli() > maxExactInteger) {
			return false
		}
		seen[c.Member] = true
	}
	return !m.RequireLease || m.Expected.valid() && m.Expected == m.Target
}
func (s *Store) contextDeadlineKey(context string) string {
	return s.root + "core:deadlines:" + context
}

// timerScript defines preflight and apply separately so a caller may combine
// it with record/output/capacity checks in one Redis script. It assumes the
// enclosing script has already fenced the primary, profile and context identity.
// All arguments are explicit; no writes occur in timerPrepare.
const timerScript = `
local function timerPrepare(root, contextKey, deadlines, oldWorkerKey, oldIndex, targetWorkerKey, targetIndex, wake, context, expected, target, changes, requireLease, now)
 local function failTimer(s) return nil,s end
 local function ref(s) return type(s)=='string' and #s>0 and #s<=192 and string.match(s,'^[A-Za-z0-9_-]+$') end
 local function empty(a) return a[1]=='' and a[2]=='' and a[3]=='' end
 local function valid(a) return ref(a[1]) and ref(a[2]) and ref(a[3]) end
 local function same(a,b) return a[1]==b[1] and a[2]==b[2] and a[3]==b[3] end
 local function live(key,assignment)
  if not kind(key,'hash') then return nil,'CORRUPT' end
  local v=redis.call('HMGET',key,'epoch','expires_ms')
  if (v[1] and not v[2]) or (v[2] and not v[1]) then return nil,'CORRUPT' end
  if not v[1] then return false end
  local expires=number(v[2]); if not expires then return nil,'CORRUPT' end
  return v[1]==assignment[2] and expires>now
 end
 if not ref(context) or (#changes<1 or #changes>128) or (not empty(expected) and not valid(expected)) or (not empty(target) and not valid(target)) then return failTimer('CONFLICT') end
 if contextKey~=root..'core:context:'..context or deadlines~=root..'core:deadlines:'..context then return failTimer('CORRUPT') end
 if not empty(expected) and (oldWorkerKey~=root..'core:worker:'..expected[1] or oldIndex~=root..'core:timers:'..expected[1]) then return failTimer('CORRUPT') end
 if not empty(target) and (targetWorkerKey~=root..'core:worker:'..target[1] or targetIndex~=root..'core:timers:'..target[1] or wake~=root..'core:wake:'..target[1]) then return failTimer('CORRUPT') end
 if not kind(contextKey,'hash') or not kind(deadlines,'zset') then return failTimer('CORRUPT') end
 if not empty(expected) and (not kind(oldIndex,'zset') or not kind(oldWorkerKey,'hash')) then return failTimer('CORRUPT') end
 if not empty(target) and (not kind(targetIndex,'zset') or not kind(targetWorkerKey,'hash')) then return failTimer('CORRUPT') end
 local fields=redis.call('HMGET',contextKey,'timer:worker','timer:epoch','timer:term')
 local present=fields[1] or fields[2] or fields[3]
 if present and (not fields[1] or not fields[2] or not fields[3]) then return failTimer('CORRUPT') end
 local actual={fields[1] or '',fields[2] or '',fields[3] or ''}
 if not empty(actual) and not valid(actual) then return failTimer('CORRUPT') end
 if not same(actual,expected) then return failTimer('CONFLICT') end
 local touched={}; local earliest=nil; local creates=false; local indexDelta=0
 for _,c in ipairs(changes) do
  local member,action,due=c[1],c[2],c[3]
  if not ref(member) or touched[member] or (action~='set' and action~='delete') then return failTimer('CONFLICT') end
  touched[member]=true
  local existing=redis.call('ZSCORE',deadlines,member)
  if action=='set' then
   local n=number(due);if not n or n==0 then return failTimer('CONFLICT') end
   if not earliest or n<earliest then earliest=n end
   if not existing then creates=true;indexDelta=indexDelta+#member+8 end
  elseif due~='' then return failTimer('CONFLICT') elseif existing then indexDelta=indexDelta-#member-8 end
 end
 local head=redis.call('ZRANGE',deadlines,0,#changes,'WITHSCORES')
 local original=nil
 if #head>0 then
  original=number(head[2]); if not original or original==0 or not ref(head[1]) then return failTimer('CORRUPT') end
  if empty(actual) then return failTimer('CORRUPT') end
 elseif not empty(actual) then return failTimer('CORRUPT') end
 if original then
  local indexed=redis.call('ZSCORE',oldIndex,context)
  if not indexed or number(indexed)~=original then return failTimer('CORRUPT') end
 end
 for i=1,#head,2 do
  local member,due=head[i],number(head[i+1]); if not ref(member) or not due or due==0 then return failTimer('CORRUPT') end
  if not touched[member] then if not earliest or due<earliest then earliest=due end; break end
 end
 if requireLease then
  if empty(actual) or not same(actual,target) then return failTimer('CONFLICT') end
  local isLive,err=live(oldWorkerKey,actual);if err then return failTimer(err) end;if not isLive then return failTimer('EXPIRED') end
 end
 if earliest and empty(target) then return failTimer('CONFLICT') end
 local changing=not same(actual,target)
 if earliest and not changing and creates and not requireLease then
  local isLive,err=live(oldWorkerKey,actual);if err then return failTimer(err) end
  if not isLive then return failTimer('EXPIRED') end
 end
 if earliest and changing then
  if not empty(actual) then
   local isLive,err=live(oldWorkerKey,actual);if err then return failTimer(err) end
   if isLive or not creates or actual[3]==target[3] then return failTimer('CONFLICT') end
  end
  local isLive,err=live(targetWorkerKey,target);if err then return failTimer(err) end;if not isLive then return failTimer('EXPIRED') end
  if targetIndex~=oldIndex and redis.call('ZSCORE',targetIndex,context) then return failTimer('CONFLICT') end
 end
 if not original and earliest then indexDelta=indexDelta+#deadlines+#context+8 elseif original and not earliest then indexDelta=indexDelta-#deadlines-#context-8 end
 local nextAssignment=earliest and target or {'','',''}
 local wakeNeeded=earliest and (not original or changing or earliest<original)
 local newScore=earliest and string.format('%.0f',earliest) or ''
 local byteDelta, fieldDelta=0,0
 local names={'timer:worker','timer:epoch','timer:term'}
 for i=1,3 do
  if fields[i] then byteDelta=byteDelta-#names[i]-#fields[i];fieldDelta=fieldDelta-1 end
  if earliest then byteDelta=byteDelta+#names[i]+#nextAssignment[i];fieldDelta=fieldDelta+1 end
 end
 for _,c in ipairs(changes) do
  if c[2]=='set' then if not redis.acl_check_cmd('ZADD',deadlines,c[3],c[1]) then return failTimer('AUTHORITY') end
  elseif not redis.acl_check_cmd('ZREM',deadlines,c[1]) then return failTimer('AUTHORITY') end
 end
 if original and (not earliest or changing) and not redis.acl_check_cmd('ZREM',oldIndex,context) then return failTimer('AUTHORITY') end
 if earliest and not redis.acl_check_cmd('ZADD',targetIndex,newScore,context) then return failTimer('AUTHORITY') end
 if earliest then
  if not redis.acl_check_cmd('HSET',contextKey,names[1],nextAssignment[1],names[2],nextAssignment[2],names[3],nextAssignment[3]) then return failTimer('AUTHORITY') end
 elseif present and not redis.acl_check_cmd('HDEL',contextKey,unpack(names)) then return failTimer('AUTHORITY') end
 if wakeNeeded and not redis.acl_check_cmd('PUBLISH',wake,'1') then return failTimer('AUTHORITY') end
 return {contextKey=contextKey,deadlines=deadlines,oldIndex=oldIndex,targetIndex=targetIndex,wake=wake,context=context,changes=changes,old=original,next=earliest,score=newScore,changing=changing,assignment=nextAssignment,hadAssignment=not not present,wakeNeeded=wakeNeeded,byteDelta=byteDelta,fieldDelta=fieldDelta,indexDelta=indexDelta},nil
end
local function timerApply(p)
 for _,c in ipairs(p.changes) do if c[2]=='set' then redis.call('ZADD',p.deadlines,c[3],c[1]) else redis.call('ZREM',p.deadlines,c[1]) end end
 if p.old and (not p.next or p.changing) then redis.call('ZREM',p.oldIndex,p.context) end
 if p.next then
  redis.call('ZADD',p.targetIndex,p.score,p.context)
  redis.call('HSET',p.contextKey,'timer:worker',p.assignment[1],'timer:epoch',p.assignment[2],'timer:term',p.assignment[3])
 elseif p.hadAssignment then redis.call('HDEL',p.contextKey,'timer:worker','timer:epoch','timer:term') end
 if p.wakeNeeded then redis.call('PUBLISH',p.wake,'1') end
end
`
