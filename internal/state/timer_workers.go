package state

import (
	"context"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"
)

const DispatchWorkerLifetime = 2 * time.Second

func (s *Store) timerKey(worker string) string        { return s.root + "timer:" + worker }
func (s *Store) dispatchWakeKey(worker string) string { return s.dispatchKey(worker) + ":wake" }

// WaitDispatch watches only this worker's queue changes. It retains neither SIP
// state nor messages, and an old connection cannot consume another's wake.
func (s *Store) WaitDispatch(ctx context.Context, worker, cursor string) (string, error) {
	if !dispatchWorkerPattern.MatchString(worker) || !validScheduleCursor(cursor) {
		return "", ErrConflict
	}
	observation, err := s.ensurePrimary(ctx)
	if err != nil {
		return "", err
	}
	ioCtx, cancel := context.WithTimeout(ctx, 1500*time.Millisecond)
	defer cancel()
	key := s.dispatchWakeKey(worker)
	streams, err := s.notifications.XRead(ioCtx, &redis.XReadArgs{Streams: []string{key, cursor}, Count: 1, Block: time.Second}).Result()
	if observation != s.transport.Load() {
		return "", ErrStale
	}
	if errors.Is(err, redis.Nil) {
		return cursor, nil
	}
	if err != nil {
		return "", err
	}
	if len(streams) != 1 || streams[0].Stream != key || len(streams[0].Messages) != 1 {
		return "", ErrCorrupt
	}
	message := streams[0].Messages[0]
	if !validScheduleCursor(message.ID) || len(message.Values) != 1 || message.Values["changed"] != "1" {
		return "", ErrCorrupt
	}
	return message.ID, nil
}

// TakeoverTimers moves derivative timer hints only. It never adopts sockets or
// application ownership and never changes protocol revisions or physical proof.
func (s *Store) TakeoverTimers(ctx context.Context, token, worker string, limit int) (bool, error) {
	if !workerPattern.MatchString(token) || !dispatchWorkerPattern.MatchString(worker) || limit < 1 || limit > 128 {
		return false, ErrConflict
	}
	values, err := s.runKeys(ctx, takeoverTimersScript, Ref{}, []string{s.schedulerKeys()[2], s.root + "timer:workers"},
		[]interface{}{token, worker, limit, s.root, s.versions}, true)
	if err != nil {
		return false, err
	}
	if len(values) != 2 || values[0] != "TAKEOVER" || values[1] != "0" && values[1] != "1" {
		return false, ErrCorrupt
	}
	return values[1] == "1", nil
}

var takeoverTimersScript = stateScript(`
if not primary() then return fail('AUTHORITY') end
local problem=schedulerGuard(KEYS[1],ARGV[7]); if problem then return fail(problem) end
if not keytype(KEYS[2],'zset') then return fail('CORRUPT') end
local now=nowms(); local target=ARGV[8]; local prefix=ARGV[10]
local alive=tonumber(redis.call('ZSCORE',KEYS[2],target) or '0')
if alive<=now then return fail('STALE') end
local expired=redis.call('ZRANGEBYSCORE',KEYS[2],'-inf',integer(now),'LIMIT',0,1)
if #expired==0 then return {'TAKEOVER','0'} end
local old=expired[1]
if #old~=64 or not string.match(old,'^[a-f0-9]+$') then return fail('CORRUPT') end
local source=prefix..'timer:'..old; local destination=prefix..'timer:'..target
local wake=prefix..'dispatch:'..target..':wake'
if not keytype(source,'zset') or not keytype(destination,'zset') or not keytype(wake,'stream') then return fail('CORRUPT') end
local entries=redis.call('ZRANGE',source,0,tonumber(ARGV[9])-1)
local changes={}
for _,key in ipairs(entries) do
 if not string.match(key,'^'..string.gsub(prefix,'([^%w])','%%%1')..'{p%d%d%d%d}:group:[A-Za-z0-9_-]+$') or not keytype(key,'hash') then return fail('CORRUPT') end
 local exists=redis.call('EXISTS',key)>0
 local owner=exists and (redis.call('HGET',key,'timerOwner') or '') or ''
 if exists and (redis.call('HGET',key,'authority')~=ARGV[1] or redis.call('HGET',key,'namespace')~=ARGV[3] or redis.call('HGET',key,'versions')~=ARGV[11]) then return fail('AUTHORITY') end
 local due=exists and tonumber(redis.call('HGET',key,'dueAtMs') or '0') or 0
 if not due or due<0 or due>9007199254740991 then return fail('CORRUPT') end
 if not redis.acl_check_cmd('ZREM',source,key) or owner==old and
  (not redis.acl_check_cmd('HSET',key,'timerOwner',target) or not redis.acl_check_cmd('ZADD',destination,math.max(1,due),key)) then return fail('AUTHORITY') end
 table.insert(changes,{key=key,owned=owner==old,due=due})
end
if not redis.acl_check_cmd('ZREM',KEYS[2],old) or not redis.acl_check_cmd('XADD',wake,'MAXLEN','=',1,'*','changed','1') or not redis.acl_check_cmd('PEXPIRE',wake,120000) then return fail('AUTHORITY') end
local previous=redis.call('ZRANGE',destination,0,0,'WITHSCORES')
for _,change in ipairs(changes) do
 redis.call('ZREM',source,change.key)
 if change.owned then
  redis.call('HSET',change.key,'timerOwner',target)
  if change.due>0 then redis.call('ZADD',destination,change.due,change.key) end
 end
end
if redis.call('ZCARD',source)==0 then redis.call('ZREM',KEYS[2],old) end
local head=redis.call('ZRANGE',destination,0,0,'WITHSCORES')
if #head>0 and (#previous==0 or tonumber(head[2])<tonumber(previous[2])) then
 redis.call('XADD',wake,'MAXLEN','=',1,'*','changed','1'); redis.call('PEXPIRE',wake,120000)
end
return {'TAKEOVER',#redis.call('ZRANGEBYSCORE',KEYS[2],'-inf',integer(now),'LIMIT',0,1)>0 and '1' or '0'}
`)
