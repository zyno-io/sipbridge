package state

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

const SchedulerLeaseLifetime = 2 * time.Second

type ScheduleClass uint8

const (
	ScheduleProtocol ScheduleClass = iota
	ScheduleApplicationExpiry
	ScheduleWorkerExpiry
	ScheduleClassCount
)

type SchedulerLease struct {
	Held   bool
	Until  time.Time
	Cursor string
}

// ScheduleHead is a scheduling hint, never a cached protocol record. After is
// relative to the validated read's Redis clock and adjusted for elapsed I/O.
type ScheduleHead struct {
	DueMS     int64
	After     time.Duration
	Cursor    string
	Partition uint16
	Class     ScheduleClass
}

func scheduleDelay(now, due int64) time.Duration {
	const longest = time.Duration(1<<63 - 1)
	delta := max(0, due-now)
	if delta > longest.Milliseconds() {
		return longest
	}
	return time.Duration(delta) * time.Millisecond
}

func (s *Store) schedulerKeys() []string {
	return []string{s.root + "scheduler:heads", s.root + "scheduler:wake", s.root + "scheduler:lease"}
}

func (s *Store) LeaseScheduler(ctx context.Context, token string, acquire bool) (SchedulerLease, error) {
	if !workerPattern.MatchString(token) {
		return SchedulerLease{}, ErrConflict
	}
	start := time.Now()
	values, err := s.runKeys(ctx, schedulerLeaseScript, Ref{}, []string{s.schedulerKeys()[2], s.root + "scheduler:lease:wake"}, []interface{}{token, acquire, SchedulerLeaseLifetime.Milliseconds()}, true)
	if err != nil {
		return SchedulerLease{}, err
	}
	if len(values) != 5 || values[0] != "LEASE" || (values[1] != "0" && values[1] != "1") {
		return SchedulerLease{}, ErrCorrupt
	}
	now, e1 := millisAt(values, 2)
	expiry, e2 := millisAt(values, 3)
	cursor, e3 := textAt(values, 4)
	if e1 != nil || e2 != nil || e3 != nil || !validScheduleCursor(cursor) {
		return SchedulerLease{}, ErrCorrupt
	}
	// Cursor and lease are returned from one Redis execution.
	return SchedulerLease{Held: values[1] == "1", Until: start.Add(scheduleDelay(now, expiry)), Cursor: cursor}, nil
}

func (s *Store) ReleaseScheduler(ctx context.Context, token string) error {
	if !workerPattern.MatchString(token) {
		return ErrConflict
	}
	values, err := s.runKeys(ctx, schedulerReleaseScript, Ref{}, []string{s.schedulerKeys()[2], s.root + "scheduler:lease:wake"}, []interface{}{token}, true)
	if err == nil && (len(values) != 1 || values[0] != "OK") {
		err = ErrCorrupt
	}
	return err
}

func (s *Store) ScheduleHead(ctx context.Context, token string, rebuild bool, prefer ScheduleClass) (ScheduleHead, error) {
	if !workerPattern.MatchString(token) || prefer >= ScheduleClassCount {
		return ScheduleHead{}, ErrConflict
	}
	scheduler := s.schedulerKeys()
	keys := []string{scheduler[2], scheduler[0], scheduler[1]}
	if rebuild {
		for partition := uint16(0); partition < s.cfg.Partitions; partition++ {
			prefix := s.partitionPrefix(partition)
			keys = append(keys, prefix+"ready", prefix+"due")
		}
	}
	keys = append(keys, s.root+"application-expiry", s.root+"timer:workers")
	start := time.Now()
	values, err := s.runKeys(ctx, schedulerHeadScript, Ref{}, keys, []interface{}{token, rebuild, int(prefer)}, rebuild)
	if err != nil {
		return ScheduleHead{}, err
	}
	if len(values) != 5 || values[0] != "SCHEDULE_HEAD" {
		return ScheduleHead{}, ErrCorrupt
	}
	now, nowErr := millisAt(values, 1)
	due, dueErr := millisAt(values, 2)
	cursor, cursorErr := textAt(values, 3)
	if nowErr != nil || dueErr != nil || cursorErr != nil || !validScheduleCursor(cursor) {
		return ScheduleHead{}, ErrCorrupt
	}
	var partition uint16
	class := ScheduleProtocol
	if due != 0 {
		queue, err := textAt(values, 4)
		if err == nil && queue == s.root+"application-expiry" {
			return ScheduleHead{DueMS: due, After: max(0, scheduleDelay(now, due)-time.Since(start)), Cursor: cursor, Class: ScheduleApplicationExpiry}, nil
		}
		if err == nil && queue == s.root+"timer:workers" {
			return ScheduleHead{DueMS: due, After: max(0, scheduleDelay(now, due)-time.Since(start)), Cursor: cursor, Class: ScheduleWorkerExpiry}, nil
		}
		if err != nil || !strings.HasPrefix(queue, s.root+"{p") {
			return ScheduleHead{}, ErrCorrupt
		}
		suffix := strings.TrimPrefix(queue, s.root+"{p")
		if len(suffix) < 9 || suffix[4:6] != "}:" || suffix[6:] != "ready" && suffix[6:] != "due" {
			return ScheduleHead{}, ErrCorrupt
		}
		parsed, err := strconv.ParseUint(suffix[:4], 10, 16)
		if err != nil || parsed >= uint64(s.cfg.Partitions) {
			return ScheduleHead{}, ErrCorrupt
		}
		partition = uint16(parsed)
	}
	return ScheduleHead{DueMS: due, After: max(0, scheduleDelay(now, due)-time.Since(start)), Cursor: cursor, Partition: partition, Class: class}, nil
}

func validScheduleCursor(cursor string) bool {
	ms, sequence, ok := strings.Cut(cursor, "-")
	if !ok || !decimal(ms) || !decimal(sequence) {
		return false
	}
	_, msErr := strconv.ParseUint(ms, 10, 64)
	_, sequenceErr := strconv.ParseUint(sequence, 10, 64)
	return msErr == nil && sequenceErr == nil
}

// WaitSchedule uses a separate one-connection pool while blocked. It consumes no
// messages; old leaders cannot steal their successor's wakeup. A bounded timeout
// permits cancellation/primary validation without polling protocol state.
func (s *Store) WaitSchedule(ctx context.Context, cursor string) (string, error) {
	return s.waitSchedulerStream(ctx, s.schedulerKeys()[1], cursor)
}

func (s *Store) WaitSchedulerLease(ctx context.Context, cursor string) (string, error) {
	return s.waitSchedulerStream(ctx, s.root+"scheduler:lease:wake", cursor)
}

func (s *Store) waitSchedulerStream(ctx context.Context, key, cursor string) (string, error) {
	if !validScheduleCursor(cursor) {
		return "", ErrConflict
	}
	observation, err := s.ensurePrimary(ctx)
	if err != nil {
		return "", err
	}
	ioCtx, cancel := context.WithTimeout(ctx, 1500*time.Millisecond)
	defer cancel()
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

var schedulerLeaseScript = stateScript(`
if not primary() then return fail('AUTHORITY') end
if not keytype(KEYS[1],'hash') or not keytype(KEYS[2],'stream') then return fail('CORRUPT') end
if not redis.acl_check_cmd('HSET',KEYS[1],'holder','probe') or not redis.acl_check_cmd('XREVRANGE',KEYS[2],'+','-','COUNT',1) or
 not redis.acl_check_cmd('XADD',KEYS[2],'MAXLEN','=',1,'*','changed','1') or not redis.acl_check_cmd('PEXPIRE',KEYS[2],120000) or not redis.acl_check_cmd('XREAD','COUNT',1,'STREAMS',KEYS[2],'0-0') then return fail('AUTHORITY') end
local existingWake=redis.call('XREVRANGE',KEYS[2],'+','-','COUNT',1)
if #existingWake>0 and (#existingWake[1][2]~=2 or existingWake[1][2][1]~='changed' or existingWake[1][2][2]~='1') then return fail('CORRUPT') end
local now=nowms(); local holder=redis.call('HGET',KEYS[1],'holder') or ''
local untilms=tonumber(redis.call('HGET',KEYS[1],'until') or '0')
if not untilms or untilms<0 or untilms>9007199254740991 or untilms~=math.floor(untilms) then return fail('CORRUPT') end
local same=redis.call('HGET',KEYS[1],'primary')==ARGV[2] and redis.call('HGET',KEYS[1],'authority')==ARGV[1]
local held='1'; local changed=false
if ARGV[8]=='1' then
 if same and untilms>now and holder~=ARGV[7] then held='0' else changed=not(same and untilms>now and holder==ARGV[7]) end
else
 local problem=schedulerGuard(KEYS[1],ARGV[7]); if problem then return fail(problem) end
end
if held=='1' then
 untilms=now+tonumber(ARGV[9]); redis.call('HSET',KEYS[1],'holder',ARGV[7],'until',integer(untilms),'primary',ARGV[2],'authority',ARGV[1])
 if changed then redis.call('XADD',KEYS[2],'MAXLEN','=',1,'*','changed','1'); redis.call('PEXPIRE',KEYS[2],120000) end
end
local wake=redis.call('XREVRANGE',KEYS[2],'+','-','COUNT',1)
return {'LEASE',held,integer(now),integer(untilms),#wake>0 and wake[1][1] or '0-0'}
`)

var schedulerReleaseScript = stateScript(`
if not primary() then return fail('AUTHORITY') end
if not keytype(KEYS[1],'hash') or not keytype(KEYS[2],'stream') then return fail('CORRUPT') end
if redis.call('HGET',KEYS[1],'holder')==ARGV[7] and redis.call('HGET',KEYS[1],'primary')==ARGV[2] and redis.call('HGET',KEYS[1],'authority')==ARGV[1] then
 if not redis.acl_check_cmd('DEL',KEYS[1]) or not redis.acl_check_cmd('XADD',KEYS[2],'MAXLEN','=',1,'*','changed','1') or not redis.acl_check_cmd('PEXPIRE',KEYS[2],120000) then return fail('AUTHORITY') end
 redis.call('DEL',KEYS[1]); redis.call('XADD',KEYS[2],'MAXLEN','=',1,'*','changed','1'); redis.call('PEXPIRE',KEYS[2],120000)
end
return {'OK'}
`)

var schedulerValidateScript = stateScript(`
if not primary() then return fail('AUTHORITY') end
local problem=schedulerGuard(KEYS[1],ARGV[7]); if problem then return fail(problem) end
return {'OK'}
`)

var schedulerHeadScript = stateScript(`
if not primary() then return fail('AUTHORITY') end
local problem=schedulerGuard(KEYS[1],ARGV[7]); if problem then return fail(problem) end
if not keytype(KEYS[2],'zset') or not keytype(KEYS[3],'stream') then return fail('CORRUPT') end
if not redis.acl_check_cmd('ZRANGE',KEYS[2],0,0,'WITHSCORES') or
 not redis.acl_check_cmd('XREAD','COUNT',1,'STREAMS',KEYS[3],'0-0') or
 not redis.acl_check_cmd('XREVRANGE',KEYS[3],'+','-','COUNT',1) then return fail('AUTHORITY') end
if not keytype(KEYS[#KEYS-1],'zset') or not keytype(KEYS[#KEYS],'zset') then return fail('CORRUPT') end
if not redis.acl_check_cmd('ZRANGE',KEYS[#KEYS-1],0,0,'WITHSCORES') or not redis.acl_check_cmd('ZRANGE',KEYS[#KEYS],0,0,'WITHSCORES') then return fail('AUTHORITY') end
for _,key in ipairs({KEYS[#KEYS-1],KEYS[#KEYS]}) do
 local h=redis.call('ZRANGE',key,0,0,'WITHSCORES'); if #h>0 then local n=tonumber(h[2]); if not n or n<1 or n>9007199254740991 or n~=math.floor(n) then return fail('CORRUPT') end end
end
if ARGV[8]=='1' then
 if not redis.acl_check_cmd('DEL',KEYS[2]) or not redis.acl_check_cmd('ZADD',KEYS[2],1,'probe') then return fail('AUTHORITY') end
 for offset=4,#KEYS-2 do
  if not keytype(KEYS[offset],'zset') then return fail('CORRUPT') end
  if not redis.acl_check_cmd('ZRANGE',KEYS[offset],0,0,'WITHSCORES') then return fail('AUTHORITY') end
  local h=redis.call('ZRANGE',KEYS[offset],0,0,'WITHSCORES'); if #h>0 then local n=tonumber(h[2]); if not n or n<1 or n>9007199254740991 or n~=math.floor(n) then return fail('CORRUPT') end end
 end
 redis.call('DEL',KEYS[2])
 for offset=4,#KEYS-2 do
  if keytype(KEYS[offset],'zset') then
   local head=redis.call('ZRANGE',KEYS[offset],0,0,'WITHSCORES')
   if #head>0 then redis.call('ZADD',KEYS[2],scheduleScore(head[2]),KEYS[offset]) end
  end
 end
end
local now=nowms()
local heads={redis.call('ZRANGE',KEYS[2],0,0,'WITHSCORES'),redis.call('ZRANGE',KEYS[#KEYS-1],0,0,'WITHSCORES'),redis.call('ZRANGE',KEYS[#KEYS],0,0,'WITHSCORES')}
if not keytype(KEYS[#KEYS-1],'zset') or not keytype(KEYS[#KEYS],'zset') then return fail('CORRUPT') end
local selected=nil; local prefer=tonumber(ARGV[9])
for i=0,2 do local n=(prefer+i)%3+1; local h=heads[n]; if #h>0 then
 local score=tonumber(h[2]); if not score or score<1 or score>9007199254740991 or score~=math.floor(score) then return fail('CORRUPT') end
 if score<=now then selected=n; break end
end end
if not selected then for n=1,3 do local h=heads[n]; if #h>0 and (not selected or tonumber(h[2])<tonumber(heads[selected][2])) then selected=n end end end
local head=selected and heads[selected] or {}
local queue=selected==2 and KEYS[#KEYS-1] or selected==3 and KEYS[#KEYS] or (#head>0 and head[1] or '')
local wake=redis.call('XREVRANGE',KEYS[3],'+','-','COUNT',1)
return {'SCHEDULE_HEAD',integer(now),#head>0 and head[2] or '0',#wake>0 and wake[1][1] or '0-0',queue}

`)
