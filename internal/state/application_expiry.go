package state

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/google/uuid"
)

// ApplicationExpiryPage is an internal, resumable owner sweep. It grants no
// application authority. The caller acknowledges only after every entry has
// been reconciled through ordinary native state admission.
type ApplicationExpiryPage struct {
	OwnerID    string
	SweepToken string
	Cursor     string
	NextCursor string
	Entries    []ApplicationContextEntry
	Complete   bool
}

func (s *Store) applicationExpiryKeys(id string) []string {
	return []string{s.root + "application-expiry", s.root + "application-expiry:" + id, s.schedulerKeys()[1]}
}

// One deadline per lease, and at most one token/cursor/start sidecar during a sweep.
// The sidecar follows the lease's referenced/idle lifetime; the sorted entry is
// consumed at expiry even if an unreferenced lease has already disappeared.
const applicationExpiryLua = `
local function applicationExpiryKeys(id)
 local prefix=ARGV[#ARGV-2]
 local root=string.sub(prefix,1,#prefix-#'{p0000}:application-lease:')
 return root..'application-expiry',root..'application-expiry:'..id,root..'scheduler:wake'
end
local function applicationExpiryState(id)
 local index,key,wake=applicationExpiryKeys(id)
 if not keytype(index,'zset') or not keytype(key,'hash') or not keytype(wake,'stream') then return nil,'CORRUPT' end
 local token=redis.call('HGET',key,'token') or ''
 local cursor=redis.call('HGET',key,'cursor') or ''
 local started=tonumber(redis.call('HGET',key,'started') or '0')
 if not started or started<0 or started>9007199254740991 or started~=math.floor(started) then return nil,'CORRUPT' end
 if redis.call('EXISTS',key)==1 and (redis.call('HLEN',key)~=3 or not validApplicationLeaseTerm(token) or #cursor>8192 or started==0) then return nil,'CORRUPT' end
 local score=redis.call('ZSCORE',index,id)
 if score and (not tonumber(score) or tonumber(score)<1 or tonumber(score)>9007199254740991) then return nil,'CORRUPT' end
 if not redis.acl_check_cmd('ZRANGE',index,0,0,'WITHSCORES') or
    not redis.acl_check_cmd('ZADD',index,1,id) or not redis.acl_check_cmd('ZREM',index,id) or
    not redis.acl_check_cmd('HSET',key,'token','probe','cursor','') or not redis.acl_check_cmd('DEL',key) or
    not redis.acl_check_cmd('PERSIST',key) or not redis.acl_check_cmd('PEXPIRE',key,120000) or
    not redis.acl_check_cmd('XADD',wake,'MAXLEN','=',1,'*','changed','1') then return nil,'AUTHORITY' end
 return {index=index,key=key,wake=wake,id=id,token=token,cursor=cursor,started=started,score=score and tonumber(score) or 0},nil
end
local function planApplicationExpiry(id,expiry,references,reset,ensure)
 local plan,err=applicationExpiryState(id); if err then return nil,err end
 local now=nowms()
 if reset~='' then plan.token=reset; plan.cursor=''; plan.started=now end
 plan.references=references
 if plan.token~='' then plan.due=plan.score>0 and math.min(plan.score,now) or now
 elseif ensure or plan.score>0 then plan.due=expiry
 else plan.due=0 end
 return plan,nil
end
local function applyApplicationExpiry(plan)
 local before=redis.call('ZRANGE',plan.index,0,0,'WITHSCORES')
 if plan.due>0 then redis.call('ZADD',plan.index,plan.due,plan.id) else redis.call('ZREM',plan.index,plan.id) end
 if plan.token~='' then
  redis.call('HSET',plan.key,'token',plan.token,'cursor',plan.cursor,'started',integer(plan.started))
  if plan.references>0 then redis.call('PERSIST',plan.key) else redis.call('PEXPIRE',plan.key,120000) end
 else redis.call('DEL',plan.key) end
 local after=redis.call('ZRANGE',plan.index,0,0,'WITHSCORES')
 if #after>0 and (#before==0 or tonumber(after[2])<tonumber(before[2])) then
  redis.call('XADD',plan.wake,'MAXLEN','=',1,'*','changed','1')
 end
end
local function forgetApplicationExpiry(id)
 local plan,err=applicationExpiryState(id); if err then return err end
 plan.token=''; plan.cursor=''; plan.due=0; plan.references=0
 applyApplicationExpiry(plan)
 return nil
end
`

func (s *Store) ApplicationExpiryHead(ctx context.Context) (ScheduleHead, error) {
	start := time.Now()
	values, err := s.runKeys(ctx, applicationExpiryHeadScript, Ref{}, s.applicationExpiryKeys(""), nil, false)
	if err != nil {
		return ScheduleHead{}, err
	}
	if len(values) != 4 || values[0] != "APPLICATION_EXPIRY_HEAD" {
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

var applicationExpiryHeadScript = stateScript(`
if not primary() then return fail('AUTHORITY') end
if not keytype(KEYS[1],'zset') or not keytype(KEYS[3],'stream') then return fail('CORRUPT') end
local head=redis.call('ZRANGE',KEYS[1],0,0,'WITHSCORES')
local wake=redis.call('XREVRANGE',KEYS[3],'+','-','COUNT',1)
return {'APPLICATION_EXPIRY_HEAD',integer(nowms()),#head>0 and head[2] or '0',#wake>0 and wake[1][1] or '0-0'}
`)

func (s *Store) ApplicationExpiryPage(ctx context.Context, limit int) (ApplicationExpiryPage, error) {
	if limit < 1 || limit > ApplicationContextPageLimit {
		return ApplicationExpiryPage{}, ErrConflict
	}
	values, err := s.runKeys(ctx, applicationExpiryPageScript, Ref{}, s.applicationExpiryKeys(""), []interface{}{limit, uuid.NewString()}, true)
	if err != nil {
		return ApplicationExpiryPage{}, err
	}
	if len(values) == 1 && values[0] == "IDLE" {
		return ApplicationExpiryPage{}, nil
	}
	if len(values) != 6 || values[0] != "APPLICATION_EXPIRY_PAGE" {
		return ApplicationExpiryPage{}, ErrCorrupt
	}
	id, idErr := textAt(values, 1)
	token, tokenErr := textAt(values, 2)
	cursor, cursorErr := textAt(values, 3)
	encoded, encodedErr := textAt(values, 4)
	complete, completeErr := textAt(values, 5)
	if idErr != nil || tokenErr != nil || cursorErr != nil || encodedErr != nil || completeErr != nil || !referencePattern.MatchString(id) || !validApplicationLeaseRef(ApplicationLeaseRef{ID: id, Term: token}) || len(cursor) > applicationContextMemberLimit || complete != "0" && complete != "1" {
		return ApplicationExpiryPage{}, ErrCorrupt
	}
	var members []string
	if json.Unmarshal([]byte(encoded), &members) != nil || len(members) > limit {
		return ApplicationExpiryPage{}, ErrCorrupt
	}
	page := ApplicationExpiryPage{OwnerID: id, SweepToken: token, Cursor: cursor, NextCursor: cursor, Complete: complete == "1"}
	for _, member := range members {
		term, data, ok := strings.Cut(member, "|")
		var entry ApplicationContextEntry
		if !ok || len(member) > applicationContextMemberLimit || member <= page.NextCursor || !validApplicationLeaseRef(ApplicationLeaseRef{ID: id, Term: term}) || json.Unmarshal([]byte(data), &entry) != nil || s.validateRef(entry.Ref) != nil || !referencePattern.MatchString(entry.ContextID) {
			return ApplicationExpiryPage{}, ErrCorrupt
		}
		page.Entries = append(page.Entries, entry)
		page.NextCursor = member
	}
	return page, nil
}

var applicationExpiryPageScript = stateScript(`
if not primary() then return fail('AUTHORITY') end
if not keytype(KEYS[1],'zset') then return fail('CORRUPT') end
local now=nowms(); local head=redis.call('ZRANGE',KEYS[1],0,0,'WITHSCORES')
if #head==0 or tonumber(head[2])>now then return {'IDLE'} end
local id=head[1]; if not validApplicationLeaseID(id) then return fail('CORRUPT') end
local plan,err=applicationExpiryState(id); if err then return fail(err) end
local prefix=ARGV[#ARGV-2]; local leaseKey=prefix..id; local contexts=leaseKey..':contexts'
local saved,problem=readApplicationLeaseRecord(leaseKey,id,120000); if problem then return fail(problem) end
if not keytype(contexts,'zset') or redis.call('ZCARD',contexts)>4096 then return fail('CORRUPT') end
if not redis.acl_check_cmd('ZRANGE',contexts,'-','+','BYLEX','LIMIT',0,65) then return fail('AUTHORITY') end
plan.references=saved and saved.references or 0
if plan.token=='' then
 if saved and not saved.sealed then
  observeApplicationLeaseRecovery(leaseKey,saved,now)
  local deadline=saved.expires>now and saved.expires or saved.recoveryUntil
  if deadline>now then plan.due=deadline; applyApplicationExpiry(plan); return {'IDLE'} end
 end
 plan.token=ARGV[8]; plan.cursor=''; plan.started=now
end
local lower=plan.cursor=='' and '-' or '('..plan.cursor
local members=redis.call('ZRANGE',contexts,lower,'+','BYLEX','LIMIT',0,tonumber(ARGV[7])+1)
local complete=#members<=tonumber(ARGV[7]); if not complete then table.remove(members) end
for _,member in ipairs(members) do
 if #member>8192 or not validApplicationLeaseTerm(string.sub(member,1,36)) or string.sub(member,37,37)~='|' or redis.call('ZSCORE',contexts,member)~='0' then return fail('CORRUPT') end
end
plan.due=now; applyApplicationExpiry(plan)
return {'APPLICATION_EXPIRY_PAGE',id,plan.token,plan.cursor,#members==0 and '[]' or cjson.encode(members),complete and '1' or '0'}
`)

func (s *Store) AckApplicationExpiryPage(ctx context.Context, page ApplicationExpiryPage) error {
	if !validApplicationLeaseRef(ApplicationLeaseRef{ID: page.OwnerID, Term: page.SweepToken}) || len(page.Cursor) > applicationContextMemberLimit || len(page.NextCursor) > applicationContextMemberLimit || page.NextCursor < page.Cursor || !page.Complete && page.NextCursor == page.Cursor {
		return ErrConflict
	}
	_, err := s.runKeys(ctx, applicationExpiryAckScript, Ref{}, s.applicationExpiryKeys(page.OwnerID), []interface{}{page.OwnerID, page.SweepToken, page.Cursor, page.NextCursor, page.Complete}, true)
	return err
}

var applicationExpiryAckScript = stateScript(`
if not primary() then return fail('AUTHORITY') end
local id=ARGV[7]; local plan,err=applicationExpiryState(id); if err then return fail(err) end
if plan.token~=ARGV[8] or plan.cursor~=ARGV[9] then return fail('STALE') end
local now=nowms(); local saved,problem=readApplicationLeaseRecord(ARGV[#ARGV-2]..id,id,120000)
if problem then return fail(problem) end
plan.references=saved and saved.references or 0
plan.cursor=ARGV[10]; plan.due=now
if ARGV[11]=='1' then
 plan.token=''; plan.cursor=''; plan.due=0
 if saved and not saved.sealed then
  if saved.expires>now then plan.due=saved.expires
  elseif saved.recoveryUntil>now then plan.due=saved.recoveryUntil
  elseif saved.expires>plan.started or saved.recoveryUntil>plan.started then plan.due=now end
 end
end
applyApplicationExpiry(plan)
return {'OK'}
`)
