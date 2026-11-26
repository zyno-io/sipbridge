package rediscore

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	ApplicationLeaseLifetime = 30 * time.Second
	ApplicationIdleLifetime  = 120 * time.Second
	ApplicationMaxGroups     = 1024
)

// ApplicationLeaseRef identifies a non-recoverable process-bound term.
// The same non-reused term names its private owned-context scope.
type ApplicationLeaseRef struct {
	Application, Term string
	Attachment        string
}

func (r ApplicationLeaseRef) valid() bool {
	return reference(r.Application) && validApplicationTerm(r.Term) && (r.Attachment == "" || reference(r.Attachment))
}
func validApplicationTerm(s string) bool { return len(s) <= 190 && reference(s) }

type ApplicationLease struct {
	ApplicationLeaseRef
	CheckedAtMS, ExpiresAtMS int64
	Until                    time.Time
}

func (s *Store) applicationKey(id string) string        { return s.root + "core:application:" + id }
func (s *Store) applicationOwnerKey(term string) string { return s.root + "core:owner:" + term }
func (s *Store) applicationOwnedKey(term string) string { return s.root + "core:owned:" + term }
func (s *Store) applicationAdmissionKey() string        { return s.root + "core:applications" }
func (s *Store) applicationAttachmentKey(id string) string {
	return s.root + "core:attachment:" + id
}

// AcquireApplication shares a live current term, or atomically creates a new
// candidate term after expiry. Candidates must be fresh non-reused values.
// A lost response is UNKNOWN; observe current authority before any retry.
func (s *Store) AcquireApplication(ctx context.Context, application, candidateTerm string) (ApplicationLease, error) {
	return s.AcquireAttachedApplication(ctx, application, candidateTerm, "")
}

// AcquireAttachedApplication publishes the exact server-issued control attachment
// with the shared application term. Another attachment can use the same live term.
func (s *Store) AcquireAttachedApplication(ctx context.Context, application, candidateTerm, attachment string) (ApplicationLease, error) {
	if !reference(application) || !validApplicationTerm(candidateTerm) {
		return ApplicationLease{}, ErrConflict
	}
	return s.applicationLease(ctx, application, "acquire", "", candidateTerm, attachment)
}

// RenewApplication extends only the same currently live term.
func (s *Store) RenewApplication(ctx context.Context, ref ApplicationLeaseRef) (ApplicationLease, error) {
	if !ref.valid() {
		return ApplicationLease{}, ErrConflict
	}
	return s.applicationLease(ctx, ref.Application, "renew", ref.Term, "", ref.Attachment)
}

// ObserveApplication reads the live current term without creating or renewing
// it. Absence or expiry is not evidence that an earlier acquisition failed.
func (s *Store) ObserveApplication(ctx context.Context, application string) (ApplicationLease, error) {
	if !reference(application) {
		return ApplicationLease{}, ErrConflict
	}
	return s.applicationLease(ctx, application, "observe", "", "", "")
}

func (s *Store) ObserveAttachedApplication(ctx context.Context, ref ApplicationLeaseRef) (ApplicationLease, error) {
	if !ref.valid() {
		return ApplicationLease{}, ErrConflict
	}
	return s.applicationLease(ctx, ref.Application, "observe", ref.Term, "", ref.Attachment)
}

func (s *Store) applicationLease(ctx context.Context, app, mode, term, candidateTerm, attachment string) (ApplicationLease, error) {
	if attachment != "" && !reference(attachment) {
		return ApplicationLease{}, ErrConflict
	}
	start := time.Now()
	deadline := start.Add(workerRedisTimeout)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	callCtx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	low := start.UnixMilli() - workerClockTolerance.Milliseconds()
	high := start.UnixMilli() + max(int64(0), deadline.Sub(start).Milliseconds()) + workerClockTolerance.Milliseconds()
	keys := []string{s.profileKey(), s.applicationKey(app), s.ownerExpiryKey(), s.applicationAdmissionKey()}
	if attachment != "" {
		keys = append(keys, s.applicationAttachmentKey(attachment))
	}
	v, err := applicationLeaseScript.Run(callCtx, s.client, keys, s.profile, app, mode, term, candidateTerm, low, high, ApplicationMaxGroups, ApplicationIdleLifetime.Milliseconds(), s.primary, attachment).Slice()
	finish := time.Now()
	if err != nil {
		if mode == "observe" {
			return ApplicationLease{}, err
		}
		return ApplicationLease{}, fmt.Errorf("%w: %w", ErrUnknown, err)
	}
	if err = status(v); err != nil {
		return ApplicationLease{}, err
	}
	invalid := func(err error) (ApplicationLease, error) {
		if mode != "observe" {
			return ApplicationLease{}, errors.Join(ErrUnknown, err)
		}
		return ApplicationLease{}, err
	}
	if len(v) != 4 {
		return invalid(ErrCorrupt)
	}
	gotTerm, ok := v[1].(string)
	now, e1 := responseInt(v[2])
	expiry, e2 := responseInt(v[3])
	if !ok || !validApplicationTerm(gotTerm) || e1 != nil || e2 != nil || expiry <= now || expiry-now > ApplicationLeaseLifetime.Milliseconds()+2*workerClockTolerance.Milliseconds() {
		return invalid(ErrCorrupt)
	}
	elapsed := finish.Sub(start)
	skew := finish.UnixMilli() - start.UnixMilli() - elapsed.Milliseconds()
	if elapsed < 0 || skew < -workerClockTolerance.Milliseconds() || skew > workerClockTolerance.Milliseconds() || now < low || now > finish.UnixMilli()+workerClockTolerance.Milliseconds() {
		return invalid(ErrClock)
	}
	until := start.Add(time.Duration(expiry-now) * time.Millisecond)
	if ctx.Err() != nil || callCtx.Err() != nil || !time.Now().Before(until) {
		return invalid(ErrExpired)
	}
	return ApplicationLease{ApplicationLeaseRef: ApplicationLeaseRef{Application: app, Term: gotTerm, Attachment: attachment}, CheckedAtMS: now, ExpiresAtMS: expiry, Until: until}, nil
}

// applicationGuardScript is a read-only preflight for a future atomic native
// publication. Its caller checks primary/profile and obtains Redis TIME first.
// It does not fence controller-independent native completion or output results.
const applicationGuardScript = `
local function appKind(k,t) local v=redis.call('TYPE',k).ok; return v=='none' or v==t end
local function appRef(v) return type(v)=='string' and #v>0 and #v<=192 and string.match(v,'^[A-Za-z0-9_-]+$') end
local function appNumber(v)
 if type(v)~='string' or #v==0 or #v>16 or not string.match(v,'^[0-9]+$') or (#v>1 and string.sub(v,1,1)=='0') then return nil end
 local n=tonumber(v); if not n or n>9007199254740991 or n~=math.floor(n) then return nil end; return n
end
local function guardApplication(root,app,term,now)
 if not appRef(app) or not appRef(term) or #term>190 then return 'CORRUPT' end
 local current=root..'core:application:'..app; local owner=root..'core:owner:'..term
 if not appKind(current,'string') or not appKind(owner,'hash') then return 'CORRUPT' end
 if redis.call('GET',current)~=term then return 'AUTHORITY','application-term' end
 local v=redis.call('HMGET',owner,'application','expires_ms')
 if not v[1] or not v[2] or not appNumber(v[2]) then return 'CORRUPT' end
 if v[1]~=app then return 'AUTHORITY','application-owner' end
 if appNumber(v[2])<=now then return 'EXPIRED' end
 return nil
end
local function attachmentValue(root,attachment)
 local key=root..'core:attachment:'..attachment
 if not appKind(key,'string') then return nil,'CORRUPT' end
 local value=redis.call('GET',key)
 if not value then return nil,'AUTHORITY' end
 return value,nil
end
local function guardApplicationAttachment(root,app,term,attachment,now)
 if not attachment or attachment=='' then return nil end
 if not appRef(attachment) then return 'CORRUPT' end
 local value,problem=attachmentValue(root,attachment)
 if problem then return problem,'application-attachment' end
 local owner,saved,expiry=string.match(value,'^([A-Za-z0-9_-]+)/([A-Za-z0-9_-]+)/([0-9]+)$')
 if not owner or not appNumber(expiry) then return 'CORRUPT' end
 if owner~=app or saved~=term then return 'AUTHORITY','application-attachment' end
 if appNumber(expiry)<=now then return 'EXPIRED','application-attachment' end
 return nil,nil,appNumber(expiry)
end
`

var applicationLeaseScript = redis.NewScript(primaryScript + applicationGuardScript + `
local function fail(s) return {s} end
if not primary(ARGV[10]) then return fail('AUTHORITY') end
local maxGroups=appNumber(ARGV[8]);local idleMS=appNumber(ARGV[9]);if maxGroups~=1024 or idleMS~=120000 then return fail('CORRUPT') end
if not appKind(KEYS[1],'string') or redis.call('GET',KEYS[1])~=ARGV[1] then return fail('AUTHORITY') end
if not appKind(KEYS[2],'string') or not appKind(KEYS[3],'zset') or not appKind(KEYS[4],'zset') then return fail('CORRUPT') end
local app,mode=ARGV[2],ARGV[3]; local low,high=appNumber(ARGV[6]),appNumber(ARGV[7])
if not appRef(app) or not low or not high or high<low then return fail('CORRUPT') end
local clock=redis.call('TIME'); local now=tonumber(clock[1])*1000+math.floor(tonumber(clock[2])/1000)
if now<low or now>high or now>9007199254620991 then return fail('EXPIRED') end
local root=string.sub(KEYS[1],1,#KEYS[1]-#'core:profile')
local attachment=ARGV[11]
if attachment~='' then
 if not appRef(attachment) or #KEYS~=5 or KEYS[5]~=root..'core:attachment:'..attachment or not appKind(KEYS[5],'string') then return fail('CORRUPT') end
elseif #KEYS~=4 then return fail('CORRUPT') end
local current=redis.call('GET',KEYS[2]); local savedExpiry,savedRefs
if current then
 if not appRef(current) or #current>190 then return fail('CORRUPT') end
 local owner=root..'core:owner:'..current; local owned=root..'core:owned:'..current
 if not appKind(owner,'hash') or not appKind(owned,'set') then return fail('CORRUPT') end
 local v=redis.call('HMGET',owner,'application','expires_ms')
 if v[1]~=app or not appNumber(v[2]) then return fail('CORRUPT') end
 savedExpiry=appNumber(v[2]); savedRefs=redis.call('SCARD',owned)
 if savedExpiry>now+30500 then return fail('CORRUPT') end
end
if mode=='observe' then
 if not current then return fail('ABSENT') end
 if ARGV[4]~='' and current~=ARGV[4] then return fail('AUTHORITY') end
 if savedExpiry<=now then return fail('EXPIRED') end
 local problem,_,attachmentExpiry=guardApplicationAttachment(root,app,current,attachment,now);if problem then return fail(problem) end
 if attachmentExpiry then savedExpiry=math.min(savedExpiry,attachmentExpiry) end
 return {'OK',current,string.format('%.0f',now),string.format('%.0f',savedExpiry)}
end
local count=redis.call('ZCARD',KEYS[4]);if count>maxGroups then return fail('CORRUPT') end
local admission=redis.call('ZSCORE',KEYS[4],app);local admissionDue=admission and appNumber(admission) or nil
if admission and (not admissionDue or admissionDue==0) then return fail('CORRUPT') end
if current and savedExpiry>now and (not admissionDue or admissionDue<=now) then return fail('CORRUPT') end
local expired=0
if mode=='acquire' then
 expired=redis.call('ZCOUNT',KEYS[4],'-inf',now)
 local live=count-expired
 if (not admissionDue or admissionDue<=now) and live>=maxGroups then return fail('CAPACITY') end
elseif mode=='renew' and (not admissionDue or admissionDue<=now) then return fail('CORRUPT') end
local chosen,refs
if mode=='renew' then
 if not current or current~=ARGV[4] then return fail('AUTHORITY') end
 if savedExpiry<=now then return fail('EXPIRED') end
 local problem=guardApplicationAttachment(root,app,current,attachment,now);if problem then return fail(problem) end
 chosen,refs=current,savedRefs
elseif mode=='acquire' then
 if attachment~='' and redis.call('EXISTS',KEYS[5])~=0 then return fail('CONFLICT') end
 if current and savedExpiry>now then chosen,refs=current,savedRefs
 else
  chosen,refs=ARGV[5],0
  if not appRef(chosen) or #chosen>190 or chosen==current then return fail('CONFLICT') end
  local candidateOwner=root..'core:owner:'..chosen; local candidateOwned=root..'core:owned:'..chosen
  if redis.call('EXISTS',candidateOwner,candidateOwned)~=0 or redis.call('ZSCORE',KEYS[3],'a-'..chosen) then return fail('CONFLICT') end
 end
else return fail('CORRUPT') end
local owner=root..'core:owner:'..chosen; local owned=root..'core:owned:'..chosen
if not appKind(owner,'hash') or not appKind(owned,'set') then return fail('CORRUPT') end
local expires=now+30000; if chosen==current and savedExpiry>expires then expires=savedExpiry end; local member='a-'..chosen
local attachmentExpires=string.format('%.0f',now+30000)
local attachmentRecord=app..'/'..chosen..'/'..attachmentExpires
if attachment~='' and not redis.acl_check_cmd('SET',KEYS[5],attachmentRecord,'PXAT',attachmentExpires) then return fail('AUTHORITY') end
local oldEmpty=current and current~=chosen and savedRefs==0
if oldEmpty and redis.call('ZSCORE',KEYS[3],'a-'..current) then return fail('CORRUPT') end
if oldEmpty and not redis.acl_check_cmd('DEL',root..'core:owner:'..current) then return fail('AUTHORITY') end
if not redis.acl_check_cmd('SET',KEYS[2],chosen) or not redis.acl_check_cmd('HSET',owner,'application',app,'expires_ms',string.format('%.0f',expires)) then return fail('AUTHORITY') end
if refs>0 then
 if not redis.acl_check_cmd('PERSIST',owner) or not redis.acl_check_cmd('ZADD',KEYS[3],expires,member) then return fail('AUTHORITY') end
else
 if not redis.acl_check_cmd('PEXPIRE',owner,idleMS) or not redis.acl_check_cmd('PEXPIRE',KEYS[2],idleMS) or not redis.acl_check_cmd('ZREM',KEYS[3],member) then return fail('AUTHORITY') end
end
local admissionScore=string.format('%.0f',now+idleMS)
if not redis.acl_check_cmd('ZADD',KEYS[4],admissionScore,app) then return fail('AUTHORITY') end
if expired>0 and not redis.acl_check_cmd('ZREMRANGEBYSCORE',KEYS[4],'-inf',now) then return fail('AUTHORITY') end
if expired>0 then redis.call('ZREMRANGEBYSCORE',KEYS[4],'-inf',now) end
redis.call('ZADD',KEYS[4],admissionScore,app)
if oldEmpty then redis.call('DEL',root..'core:owner:'..current) end
redis.call('SET',KEYS[2],chosen)
redis.call('HSET',owner,'application',app,'expires_ms',string.format('%.0f',expires))
if attachment~='' then redis.call('SET',KEYS[5],attachmentRecord,'PXAT',attachmentExpires) end
if refs>0 then redis.call('PERSIST',owner); redis.call('ZADD',KEYS[3],expires,member)
else redis.call('PEXPIRE',owner,idleMS); redis.call('PEXPIRE',KEYS[2],idleMS); redis.call('ZREM',KEYS[3],member) end
return {'OK',chosen,string.format('%.0f',now),attachment~='' and attachmentExpires or string.format('%.0f',expires)}
`)
