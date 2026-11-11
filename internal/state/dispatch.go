package state

import (
	"context"
	"regexp"
	"sort"
	"time"
)

const anySchedulePartition = uint16(0xffff)
const AnyDispatchWorker = "*"

var dispatchWorkerPattern = regexp.MustCompile(`^[a-f0-9]{64}$`)

const dispatchIndexLua = `
local function planDispatch(before,after)
 local prefix=string.sub(KEYS[4],1,#KEYS[4]-#'scheduler:heads')..'dispatch:'
 local function decode(value,wildcard)
  if #value>280000 or string.sub(value,1,1)~='[' then return nil,'CORRUPT' end
  local ok,items=pcall(cjson.decode,value)
  if not ok or type(items)~='table' or #items>4096 then return nil,'CORRUPT' end
  local result={}; local count=0
  for index,worker in pairs(items) do
   if type(index)~='number' or index<1 or index>#items or type(worker)~='string' or
    (not (wildcard and worker=='*') and (#worker~=64 or not string.match(worker,'^[a-f0-9]+$'))) or result[worker] then return nil,'CORRUPT' end
   result[worker]=prefix..worker; count=count+1
  end
  if count~=#items then return nil,'CORRUPT' end
  return result,nil
 end
 local prior,problem=decode(before,false); if problem then return nil,problem end
 local next,problem=decode(after,true); if problem then return nil,problem end
 if next['*'] then
  next['*']=nil
  local registry=string.sub(KEYS[4],1,#KEYS[4]-#'scheduler:heads')..'scheduler:workers'
  if not keytype(registry,'zset') then return nil,'CORRUPT' end
  if not redis.acl_check_cmd('ZRANGEBYSCORE',registry,'('..integer(nowms()),'+inf','LIMIT',0,256) then return nil,'AUTHORITY' end
  local live=redis.call('ZRANGEBYSCORE',registry,'('..integer(nowms()),'+inf','LIMIT',0,256)
  for _,worker in ipairs(live) do
   if #worker~=64 or not string.match(worker,'^[a-f0-9]+$') then return nil,'CORRUPT' end
   next[worker]=prefix..worker
  end
 end
 local workers={}; for worker,_ in pairs(next) do table.insert(workers,worker) end
 if #workers>4096 then return nil,'CAPACITY' end
 table.sort(workers)
 local encoded=#workers==0 and '[]' or cjson.encode(workers)
 for _,key in pairs(prior) do
  if not keytype(key,'zset') then return nil,'CORRUPT' end
  if not redis.acl_check_cmd('ZREM',key,KEYS[1]) then return nil,'AUTHORITY' end
 end
 for _,key in pairs(next) do
  if not keytype(key,'zset') then return nil,'CORRUPT' end
  if not redis.acl_check_cmd('ZADD',key,1,KEYS[1]) then return nil,'AUTHORITY' end
 end
 return {prior=prior,next=next,encoded=encoded},nil
end
local function applyDispatch(plan,now)
 for worker,key in pairs(plan.prior) do if not plan.next[worker] then redis.call('ZREM',key,KEYS[1]) end end
 local changed=false
 for _,key in pairs(plan.next) do redis.call('ZADD',key,now,KEYS[1]); changed=true end
 return changed
end
`

func dispatchWorkers(effects []Effect) []string {
	workers := make(map[string]bool)
	for _, effect := range effects {
		if effect.Worker != "" {
			workers[effect.Worker] = true
		}
	}
	result := make([]string, 0, len(workers))
	for worker := range workers {
		result = append(result, worker)
	}
	sort.Strings(result)
	return result
}

func (s *Store) dispatchKey(worker string) string { return s.root + "dispatch:" + worker }

func (s *Store) RegisterDispatchWorker(ctx context.Context, worker string) error {
	if !dispatchWorkerPattern.MatchString(worker) {
		return ErrConflict
	}
	_, err := s.runKeys(ctx, dispatchRegisterScript, Ref{}, []string{s.root + "scheduler:workers"}, []interface{}{worker}, true)
	return err
}

var dispatchRegisterScript = stateScript(`
if not primary() then return fail('AUTHORITY') end
if not keytype(KEYS[1],'zset') then return fail('CORRUPT') end
local now=nowms()
local expired=redis.call('ZRANGEBYSCORE',KEYS[1],'-inf',integer(now),'LIMIT',0,256)
for _,worker in ipairs(expired) do redis.call('ZREM',KEYS[1],worker) end
if redis.call('ZCARD',KEYS[1])>=256 and not redis.call('ZSCORE',KEYS[1],ARGV[7]) then return fail('CAPACITY') end
redis.call('ZADD',KEYS[1],now+2000,ARGV[7])
return {'OK'}
`)

func (s *Store) DispatchHead(ctx context.Context, worker string) (ScheduleHead, error) {
	if !dispatchWorkerPattern.MatchString(worker) {
		return ScheduleHead{}, ErrConflict
	}
	start := time.Now()
	values, err := s.runKeys(ctx, dispatchHeadScript, Ref{}, []string{s.dispatchKey(worker), s.schedulerKeys()[1]}, nil, false)
	if err != nil {
		return ScheduleHead{}, err
	}
	if len(values) != 4 || values[0] != "SCHEDULE_HEAD" {
		return ScheduleHead{}, ErrCorrupt
	}
	now, nowErr := millisAt(values, 1)
	due, dueErr := millisAt(values, 2)
	cursor, cursorErr := textAt(values, 3)
	if nowErr != nil || dueErr != nil || cursorErr != nil || !validScheduleCursor(cursor) {
		return ScheduleHead{}, ErrCorrupt
	}
	return ScheduleHead{DueMS: due, After: max(0, scheduleDelay(now, due)-time.Since(start)), Cursor: cursor}, nil
}

func (s *Store) DispatchHints(ctx context.Context, worker string, limit int) ([]Hint, error) {
	if !dispatchWorkerPattern.MatchString(worker) || limit < 1 || limit > 256 {
		return nil, ErrConflict
	}
	values, err := s.runKeys(ctx, dispatchHintsScript, Ref{}, []string{s.dispatchKey(worker)}, []interface{}{limit}, false)
	if err != nil {
		return nil, err
	}
	if values[0] != "HINTS" {
		return nil, ErrCorrupt
	}
	return s.decodeHints(values[1:], anySchedulePartition, limit)
}

func (s *Store) AckDispatch(ctx context.Context, worker string, ref Ref, revision string) error {
	if !dispatchWorkerPattern.MatchString(worker) || !decimal(revision) {
		return ErrConflict
	}
	if err := s.validateRef(ref); err != nil {
		return err
	}
	_, err := s.runKeys(ctx, dispatchAckScript, ref, []string{s.keys(ref)[0], s.dispatchKey(worker)}, []interface{}{revision}, true)
	return err
}

var dispatchHeadScript = stateScript(`
if not primary() then return fail('AUTHORITY') end
if not keytype(KEYS[1],'zset') or not keytype(KEYS[2],'stream') then return fail('CORRUPT') end
local head=redis.call('ZRANGE',KEYS[1],0,0,'WITHSCORES')
local wake=redis.call('XREVRANGE',KEYS[2],'+','-','COUNT',1)
return {'SCHEDULE_HEAD',integer(nowms()),#head>0 and head[2] or '0',#wake>0 and wake[1][1] or '0-0'}
`)

var dispatchHintsScript = stateScript(`
if not primary() then return fail('AUTHORITY') end
if not keytype(KEYS[1],'zset') then return fail('CORRUPT') end
local entries=redis.call('ZRANGEBYSCORE',KEYS[1],'-inf',integer(nowms()),'WITHSCORES','LIMIT',0,ARGV[7])
local result={'HINTS'}; for _,entry in ipairs(entries) do table.insert(result,entry) end; return result
`)

var dispatchAckScript = stateScript(`
if not primary() then return fail('AUTHORITY') end
if not keytype(KEYS[2],'zset') then return fail('CORRUPT') end
if redis.call('EXISTS',KEYS[1])==0 then redis.call('ZREM',KEYS[2],KEYS[1]); return {'OK'} end
local problem=checkgroup(); if problem then return fail(problem) end
if redis.call('HGET',KEYS[1],'revision')==ARGV[7] then redis.call('ZREM',KEYS[2],KEYS[1]) end
return {'OK'}
`)
