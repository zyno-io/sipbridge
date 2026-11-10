package state

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"
)

const (
	ApplicationLeaseLifetime            = 30 * time.Second
	ApplicationLeaseRecoveryGrace       = 5 * time.Second
	RecoverableApplicationLeaseLifetime = 5 * time.Second
	ApplicationReattachTimeout          = 10 * time.Second
	applicationLeaseIdleTTL             = 120 * time.Second
	applicationLeaseLimit               = 1024
	applicationLeasePrune               = 64
)

// ApplicationLease is an authoritative observation, never a recovery credential.
// Generation zero denotes a legacy process-bound owner. Recovery secrets are
// retained only as hashes in the adapter's private lease record.
type ApplicationLease struct {
	ID                string `json:"id"`
	Term              string `json:"term"`
	ExpiresAtMS       int64  `json:"expiresAtMs"`
	CheckedAtMS       int64  `json:"checkedAtMs"`
	RecoveryUntilMS   int64  `json:"recoveryUntilMs"`
	Recoverable       bool   `json:"recoverable,omitempty"`
	Generation        uint64 `json:"generation,omitempty"`
	PrincipalHash     string `json:"principalHash,omitempty"`
	HolderID          string `json:"holderId,omitempty"`
	HolderEpoch       string `json:"holderEpoch,omitempty"`
	ReattachTimeoutMS int64  `json:"reattachTimeoutMs,omitempty"`
	AttachmentUntilMS int64  `json:"attachmentUntilMs,omitempty"`
	Sealed            bool   `json:"sealed,omitempty"`
	LastAttachID      string `json:"lastAttachId,omitempty"`
}

type ApplicationLeaseRef struct {
	ID         string `json:"id"`
	Term       string `json:"term"`
	Generation uint64 `json:"generation,omitempty"`
}

type ApplicationLeaseHolder struct {
	PrincipalHash string
	HolderID      string
	HolderEpoch   string
}
type ApplicationLeaseOwner struct {
	ApplicationLeaseHolder
	TokenHash         string
	ReattachTimeoutMS int64
}
type ApplicationLeaseAttach struct {
	ApplicationLeaseHolder
	TokenHash     string
	NextTokenHash string
	AttemptID     string
}

const applicationLeaseLua = `
if not primary() then return fail('AUTHORITY') end
if not keytype(KEYS[1],'hash') or not keytype(KEYS[2],'zset') then return fail('CORRUPT') end
local id,term,mode=ARGV[7],ARGV[8],ARGV[9]
local lifetime,idle,capacity,prune=tonumber(ARGV[10]),tonumber(ARGV[11]),tonumber(ARGV[12]),tonumber(ARGV[13])
local prefix=ARGV[14]
local now=nowms()
if now<=0 or now+idle>9007199254740991 then return fail('CLOCK') end
if redis.call('ZCARD',KEYS[2])>capacity then return fail('CAPACITY') end
local saved,err=readApplicationLeaseRecord(KEYS[1],id,idle)
if err then return fail(err) end
if mode=='read' then
 observeApplicationLeaseRecovery(KEYS[1],saved,now)
 return {'LEASE',cjson.encode(applicationLeaseObservation(id,saved,now))}
end
if saved and saved.recoverable then return fail('STALE') end
if mode=='renew' or mode=='resume' then
 if not saved or saved.sealed or mode=='renew' and saved.expires<=now then return fail('EXPIRED') end
 if saved.term~=term then return fail('STALE') end
elseif mode=='acquire' then
 if saved and not saved.sealed and saved.expires>now then term=saved.term
 elseif saved and saved.term==term then return fail('CONFLICT') end
else return fail('CORRUPT') end
local expired=redis.call('ZRANGEBYSCORE',KEYS[2],'-inf',now,'LIMIT',0,prune)
for _,member in ipairs(expired) do
 if not validApplicationLeaseID(member) then return fail('CORRUPT') end
 local key=prefix..member
 local previous,problem=readApplicationLeaseRecord(key,member,idle)
 if problem then return fail(problem) end
 if not previous or previous.expires<=now and previous.references==0 and
    (not previous.recoverable or previous.expires+previous.reattachTimeout<=now) then
  redis.call('DEL',key); redis.call('ZREM',KEYS[2],member)
 else
  redis.call('ZADD',KEYS[2],previous.references>0 and math.max(previous.expires,now+idle) or previous.expires,member)
 end
end
if not redis.call('ZSCORE',KEYS[2],id) and redis.call('ZCARD',KEYS[2])>=capacity then return fail('CAPACITY') end
local expiry=now+lifetime
if saved and saved.term==term and saved.expires>expiry then expiry=saved.expires end
local references=saved and saved.references or 0
if references>0 and not redis.acl_check_cmd('PERSIST',KEYS[1]) then return fail('AUTHORITY') end
redis.call('HSET',KEYS[1],'id',id,'term',term,'expiresAtMs',integer(expiry),'references',integer(references),'sealed','0','recoveryUntilMs','0')
if references>0 then redis.call('PERSIST',KEYS[1]) else redis.call('PEXPIRE',KEYS[1],idle) end
redis.call('ZADD',KEYS[2],expiry,id)
return {'LEASE',cjson.encode(applicationLeaseObservation(id,{term=term,expires=expiry,recoveryUntil=0},now))}
`

const recoverableApplicationLeaseLua = `
if not primary() then return fail('AUTHORITY') end
if not keytype(KEYS[1],'hash') or not keytype(KEYS[2],'zset') then return fail('CORRUPT') end
local id,term,mode=ARGV[7],ARGV[8],ARGV[9]
local principal,holder,epoch,token,nextToken,attempt=ARGV[10],ARGV[11],ARGV[12],ARGV[13],ARGV[14],ARGV[15]
local generation,timeout=tonumber(ARGV[16]),tonumber(ARGV[17])
local now=nowms()
local saved,err=readApplicationLeaseRecord(KEYS[1],id,120000)
if err then return fail(err) end
if mode=='create' then
 if saved then return fail('CONFLICT') end
 local prefix=ARGV[#ARGV-2]
 for _,member in ipairs(redis.call('ZRANGEBYSCORE',KEYS[2],'-inf',now,'LIMIT',0,64)) do
  if not validApplicationLeaseID(member) then return fail('CORRUPT') end
  local key=prefix..member
  local old,problem=readApplicationLeaseRecord(key,member,120000)
  if problem then return fail(problem) end
  if not old or old.references==0 and old.expires<=now and (not old.recoverable or old.expires+old.reattachTimeout<=now) then
   redis.call('DEL',key); redis.call('ZREM',KEYS[2],member)
  else redis.call('ZADD',KEYS[2],math.max(old.expires,now+120000),member) end
 end
 if redis.call('ZCARD',KEYS[2])>=1024 then return fail('CAPACITY') end
 saved={term=term,references=0,sealed=false,recoveryUntil=0,recoverable=true,generation=1,
  principal=principal,holder=holder,epoch=epoch,token=token,reattachTimeout=timeout,
  lastAttach='',lastToken='',lastNextToken='',lastGeneration=0,attachmentUntil=0}
elseif not saved or not saved.recoverable or saved.term~=term then return fail('STALE')
elseif not constantHashEqual(saved.principal,principal) then return fail('STALE')
elseif mode=='attach' and saved.generation==generation+1 and saved.lastGeneration==generation and
 saved.lastAttach==attempt and saved.holder==holder and saved.epoch==epoch and
 constantHashEqual(saved.lastToken,token) and constantHashEqual(saved.lastNextToken,nextToken) then
 -- An unexpired exact replay is read-only. If the reply was lost for longer
 -- than the short lease, only this same pending attachment may be renewed,
 -- and never beyond its original restoration deadline.
 if saved.sealed then return fail('EXPIRED') end
 if saved.expires>now then return {'LEASE',cjson.encode(applicationLeaseObservation(id,saved,now))} end
 if saved.attachmentUntil==0 or now>=saved.attachmentUntil then return fail('EXPIRED') end
else
 local deadline=saved.attachmentUntil>0 and saved.attachmentUntil or saved.expires+saved.reattachTimeout
 if saved.sealed or now>=deadline then return fail('EXPIRED') end
 if saved.generation~=generation then return fail('STALE') end
 if mode=='renew' or mode=='complete' then
  if saved.holder~=holder or saved.epoch~=epoch then return fail('STALE') end
  if saved.expires<=now then return fail('EXPIRED') end
  if mode=='complete' then saved.attachmentUntil=0 end
 elseif mode=='attach' then
  if not constantHashEqual(saved.token,token) then return fail('STALE') end
  if saved.holder~=holder or saved.epoch~=epoch then
   if saved.expires>now then return fail('BUSY') end
   if saved.generation>=9007199254740991 then return fail('CAPACITY') end
   if constantHashEqual(token,nextToken) then return fail('CONFLICT') end
   saved.attachmentUntil=deadline
   saved.lastGeneration=saved.generation; saved.generation=saved.generation+1
   saved.lastAttach=attempt; saved.lastToken=token; saved.lastNextToken=nextToken
   saved.token=nextToken; saved.holder=holder; saved.epoch=epoch
  elseif saved.expires<=now then
   saved.attachmentUntil=deadline
  end
 else return fail('CORRUPT') end
end
local expiry=math.max(saved.expires or 0,now+5000)
if saved.attachmentUntil>0 then expiry=math.min(expiry,saved.attachmentUntil) end
saved.expires=expiry; saved.recoveryUntil=0
if saved.references>0 and not redis.acl_check_cmd('PERSIST',KEYS[1]) then return fail('AUTHORITY') end
redis.call('HSET',KEYS[1],'id',id,'term',term,'expiresAtMs',integer(expiry),
 'references',integer(saved.references),'sealed','0','recoveryUntilMs','0','recoverable','1',
 'generation',integer(saved.generation),'principalHash',saved.principal,'holderId',saved.holder,
 'holderEpoch',saved.epoch,'tokenHash',saved.token,'reattachTimeoutMs',integer(saved.reattachTimeout),
 'lastAttachId',saved.lastAttach,'lastAttachTokenHash',saved.lastToken,
 'lastAttachNextTokenHash',saved.lastNextToken,'lastAttachExpectedGeneration',integer(saved.lastGeneration),
 'attachmentUntilMs',integer(saved.attachmentUntil))
if saved.references>0 then redis.call('PERSIST',KEYS[1]) else redis.call('PEXPIRE',KEYS[1],120000) end
redis.call('ZADD',KEYS[2],expiry,id)
return {'LEASE',cjson.encode(applicationLeaseObservation(id,saved,now))}
`

var applicationLeaseScript = stateScript(applicationLeaseLua)
var recoverableApplicationLeaseScript = stateScript(recoverableApplicationLeaseLua)

func (s *Store) applicationLeaseKeys(id string) []string {
	prefix := s.partitionPrefix(0)
	return []string{prefix + "application-lease:" + id, prefix + "application-leases"}
}
func (s *Store) AcquireApplicationLease(ctx context.Context, id, term string) (ApplicationLease, error) {
	return s.applicationLease(ctx, id, term, "acquire")
}
func (s *Store) RenewApplicationLease(ctx context.Context, id, term string) (ApplicationLease, error) {
	return s.applicationLease(ctx, id, term, "renew")
}
func (s *Store) ResumeApplicationLease(ctx context.Context, id, term string) (ApplicationLease, error) {
	return s.applicationLease(ctx, id, term, "resume")
}
func (s *Store) ReadApplicationLease(ctx context.Context, id string) (ApplicationLease, error) {
	return s.applicationLease(ctx, id, "", "read")
}

func validateLeaseIdentity(id, term string, read bool) error {
	if !referencePattern.MatchString(id) {
		return errors.New("invalid SIP application lease identity")
	}
	if !read {
		parsed, err := uuid.Parse(term)
		if err != nil || parsed == uuid.Nil || parsed.String() != term {
			return errors.New("invalid SIP application lease term")
		}
	}
	return nil
}
func decodeApplicationLease(values []interface{}) (ApplicationLease, error) {
	var lease ApplicationLease
	if len(values) != 2 || values[0] != "LEASE" {
		return lease, ErrCorrupt
	}
	encoded, err := textAt(values, 1)
	if err != nil || json.Unmarshal([]byte(encoded), &lease) != nil {
		return lease, ErrCorrupt
	}
	if !referencePattern.MatchString(lease.ID) || lease.CheckedAtMS <= 0 || lease.CheckedAtMS > 1<<53-1 ||
		lease.ExpiresAtMS < 0 || lease.ExpiresAtMS > 1<<53-1 || lease.RecoveryUntilMS < 0 || lease.RecoveryUntilMS > 1<<53-1 || lease.AttachmentUntilMS < 0 || lease.AttachmentUntilMS > 1<<53-1 || lease.AttachmentUntilMS > 0 && (!lease.Recoverable || lease.ExpiresAtMS > lease.AttachmentUntilMS) ||
		(lease.Term == "") != (lease.ExpiresAtMS == 0) || lease.Term == "" && (lease.Generation != 0 || lease.Recoverable || lease.RecoveryUntilMS != 0) {
		return lease, ErrCorrupt
	}
	if lease.Term != "" && !validApplicationLeaseRef(ApplicationLeaseRef{ID: lease.ID, Term: lease.Term, Generation: lease.Generation}) {
		return lease, ErrCorrupt
	}
	if lease.Recoverable {
		if lease.Generation == 0 || !validLeaseHolder(ApplicationLeaseHolder{lease.PrincipalHash, lease.HolderID, lease.HolderEpoch}) || lease.ReattachTimeoutMS < 1 || lease.ReattachTimeoutMS > ApplicationReattachTimeout.Milliseconds() {
			return lease, ErrCorrupt
		}
	} else if lease.Generation != 0 {
		return lease, ErrCorrupt
	}
	return lease, nil
}
func (s *Store) applicationLease(ctx context.Context, id, term, mode string) (ApplicationLease, error) {
	if err := validateLeaseIdentity(id, term, mode == "read"); err != nil {
		return ApplicationLease{}, err
	}
	values, err := s.runKeys(ctx, applicationLeaseScript, Ref{Partition: 0}, s.applicationLeaseKeys(id), []interface{}{id, term, mode, ApplicationLeaseLifetime.Milliseconds(), applicationLeaseIdleTTL.Milliseconds(), applicationLeaseLimit, applicationLeasePrune, s.partitionPrefix(0) + "application-lease:"}, mode != "read")
	if err != nil {
		return ApplicationLease{}, err
	}
	return decodeApplicationLease(values)
}
func validLeaseHash(value string) bool {
	if len(value) != 64 {
		return false
	}
	for _, c := range value {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}
func validLeaseHolder(holder ApplicationLeaseHolder) bool {
	return validLeaseHash(holder.PrincipalHash) && referencePattern.MatchString(holder.HolderID) && referencePattern.MatchString(holder.HolderEpoch)
}
func normalizeReattachTimeout(value int64) (int64, error) {
	if value == 0 {
		value = ApplicationReattachTimeout.Milliseconds()
	}
	if value < 1 || value > ApplicationReattachTimeout.Milliseconds() {
		return 0, ErrConflict
	}
	return value, nil
}
func (s *Store) AcquireRecoverableApplicationLease(ctx context.Context, id, term string, owner ApplicationLeaseOwner) (ApplicationLease, error) {
	timeout, err := normalizeReattachTimeout(owner.ReattachTimeoutMS)
	if err != nil {
		return ApplicationLease{}, err
	}
	return s.recoverableApplicationLease(ctx, id, term, "create", ApplicationLeaseAttach{ApplicationLeaseHolder: owner.ApplicationLeaseHolder, TokenHash: owner.TokenHash}, 0, timeout)
}
func (s *Store) RenewApplicationAttachment(ctx context.Context, ref ApplicationLeaseRef, holder ApplicationLeaseHolder) (ApplicationLease, error) {
	return s.recoverableApplicationLease(ctx, ref.ID, ref.Term, "renew", ApplicationLeaseAttach{ApplicationLeaseHolder: holder}, ref.Generation, 0)
}
func (s *Store) CompleteApplicationAttachment(ctx context.Context, ref ApplicationLeaseRef, holder ApplicationLeaseHolder) (ApplicationLease, error) {
	return s.recoverableApplicationLease(ctx, ref.ID, ref.Term, "complete", ApplicationLeaseAttach{ApplicationLeaseHolder: holder}, ref.Generation, 0)
}
func (s *Store) AttachApplicationLease(ctx context.Context, id, term string, generation uint64, attempt ApplicationLeaseAttach) (ApplicationLease, error) {
	return s.recoverableApplicationLease(ctx, id, term, "attach", attempt, generation, 0)
}
func (s *Store) recoverableApplicationLease(ctx context.Context, id, term, mode string, a ApplicationLeaseAttach, generation uint64, timeout int64) (ApplicationLease, error) {
	if err := validateLeaseIdentity(id, term, false); err != nil {
		return ApplicationLease{}, err
	}
	if !validLeaseHolder(a.ApplicationLeaseHolder) || generation > 1<<53-1 || mode != "create" && generation == 0 || (mode == "create" || mode == "attach") && !validLeaseHash(a.TokenHash) || mode == "attach" && (!validLeaseHash(a.NextTokenHash) || !referencePattern.MatchString(a.AttemptID)) {
		return ApplicationLease{}, ErrConflict
	}
	values, err := s.runKeys(ctx, recoverableApplicationLeaseScript, Ref{Partition: 0}, s.applicationLeaseKeys(id), []interface{}{id, term, mode, a.PrincipalHash, a.HolderID, a.HolderEpoch, a.TokenHash, a.NextTokenHash, a.AttemptID, generation, timeout}, true)
	if err != nil {
		return ApplicationLease{}, err
	}
	return decodeApplicationLease(values)
}
