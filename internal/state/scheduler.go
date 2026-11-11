package state

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

const SchedulerLeaseLifetime = 250 * time.Millisecond

// ScheduleHead is a scheduling hint, never a cached protocol record. After is
// relative to the validated read's Redis clock and adjusted for elapsed I/O.
type ScheduleHead struct {
	DueMS     int64
	After     time.Duration
	Cursor    string
	Partition uint16
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

func (s *Store) LeaseScheduler(ctx context.Context, token string, acquire bool) (time.Time, error) {
	if !workerPattern.MatchString(token) {
		return time.Time{}, ErrConflict
	}
	start := time.Now()
	values, err := s.runKeys(ctx, schedulerLeaseScript, Ref{}, []string{s.schedulerKeys()[2]},
		[]interface{}{token, acquire, SchedulerLeaseLifetime.Milliseconds()}, true)
	if err != nil {
		return time.Time{}, err
	}
	if len(values) != 1 || values[0] != "OK" {
		return time.Time{}, ErrCorrupt
	}
	// Starting before the Redis operation is conservative despite clock offset.
	until := start.Add(SchedulerLeaseLifetime)
	if !time.Now().Before(until) {
		return time.Time{}, ErrStale
	}
	return until, nil
}

func (s *Store) ReleaseScheduler(ctx context.Context, token string) error {
	if !workerPattern.MatchString(token) {
		return ErrConflict
	}
	values, err := s.runKeys(ctx, schedulerReleaseScript, Ref{}, []string{s.schedulerKeys()[2]}, []interface{}{token}, true)
	if err == nil && (len(values) != 1 || values[0] != "OK") {
		err = ErrCorrupt
	}
	return err
}

func (s *Store) ScheduleHead(ctx context.Context, token string, rebuild bool) (ScheduleHead, error) {
	if !workerPattern.MatchString(token) {
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
	start := time.Now()
	values, err := s.runKeys(ctx, schedulerHeadScript, Ref{}, keys, []interface{}{token, rebuild}, rebuild)
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
	if due != 0 {
		queue, err := textAt(values, 4)
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
	return ScheduleHead{DueMS: due, After: max(0, scheduleDelay(now, due)-time.Since(start)), Cursor: cursor, Partition: partition}, nil
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
	if !validScheduleCursor(cursor) {
		return "", ErrConflict
	}
	observation, err := s.ensurePrimary(ctx)
	if err != nil {
		return "", err
	}
	ioCtx, cancel := context.WithTimeout(ctx, 1500*time.Millisecond)
	defer cancel()
	streams, err := s.notifications.XRead(ioCtx, &redis.XReadArgs{Streams: []string{s.schedulerKeys()[1], cursor}, Count: 1, Block: time.Second}).Result()
	if observation != s.transport.Load() {
		return "", ErrStale
	}
	if errors.Is(err, redis.Nil) {
		return cursor, nil
	}
	if err != nil {
		return "", err
	}
	if len(streams) != 1 || streams[0].Stream != s.schedulerKeys()[1] || len(streams[0].Messages) != 1 {
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
if not keytype(KEYS[1],'hash') then return fail('CORRUPT') end
local now=nowms()
local holder=redis.call('HGET',KEYS[1],'holder') or ''
local untilms=tonumber(redis.call('HGET',KEYS[1],'until') or '0')
local samePrimary=redis.call('HGET',KEYS[1],'primary')==ARGV[2]
if ARGV[8]=='1' then
 if samePrimary and untilms>now and holder~=ARGV[7] then return fail('BUSY') end
else
 local problem=schedulerGuard(KEYS[1],ARGV[7]); if problem then return fail(problem) end
end
redis.call('HSET',KEYS[1],'holder',ARGV[7],'until',integer(now+tonumber(ARGV[9])), 'primary',ARGV[2],'authority',ARGV[1])
return {'OK'}
`)

var schedulerReleaseScript = stateScript(`
if not primary() then return fail('AUTHORITY') end
if not keytype(KEYS[1],'hash') then return fail('CORRUPT') end
if redis.call('HGET',KEYS[1],'holder')==ARGV[7] and redis.call('HGET',KEYS[1],'primary')==ARGV[2] and
 redis.call('HGET',KEYS[1],'authority')==ARGV[1] then redis.call('DEL',KEYS[1]) end
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
if ARGV[8]=='1' then
 if not redis.acl_check_cmd('DEL',KEYS[2]) or not redis.acl_check_cmd('ZADD',KEYS[2],1,'probe') then return fail('AUTHORITY') end
 for offset=4,#KEYS do
  if not redis.acl_check_cmd('ZRANGE',KEYS[offset],0,0,'WITHSCORES') then return fail('AUTHORITY') end
 end
 redis.call('DEL',KEYS[2])
 for offset=4,#KEYS do
  if keytype(KEYS[offset],'zset') then
   local head=redis.call('ZRANGE',KEYS[offset],0,0,'WITHSCORES')
   if #head>0 then redis.call('ZADD',KEYS[2],scheduleScore(head[2]),KEYS[offset]) end
  end
 end
end
local head=redis.call('ZRANGE',KEYS[2],0,0,'WITHSCORES')
local wake=redis.call('XREVRANGE',KEYS[3],'+','-','COUNT',1)
local due=#head>0 and head[2] or '0'
return {'SCHEDULE_HEAD',integer(nowms()),due,#wake>0 and wake[1][1] or '0-0',#head>0 and head[1] or ''}
`)
