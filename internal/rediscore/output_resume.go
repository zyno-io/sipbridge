package rediscore

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/redis/go-redis/v9"
	"sipbridge/internal/storagecodec"
)

// ResumeParkedApplicationOutput exchanges one exact parked output for a fresh
// physical attempt without changing its SIP request, original expiration, or
// timers. The native caller validates and updates the procedure's response flow.
// PrimaryRunID must be obtained when parking and authenticated by the caller as
// part of the resume handle. A different primary cannot prove attempt history.
func (s *Store) ResumeParkedApplicationOutput(ctx context.Context, primaryRunID string, id Identity, revision uint64, old OutputRef, next OutputIntent, procedure Change, metadata []byte) (uint64, error) {
	if primaryRunID == "" || primaryRunID != s.primary {
		return 0, ErrAuthority
	}
	if !id.valid() || id.Context != old.Context || id.Incarnation != old.Incarnation || revision == 0 || revision == ^uint64(0) || !old.valid() || !old.RequireApplication || !next.valid() || next.Application == nil || next.Application.Application != id.Owner || next.Application.Term != id.Term || old.ID == next.ID || old.Attempt == next.Attempt || old.ProofHash == next.ProofHash || old.Worker == next.Worker && old.Epoch == next.Epoch && old.Connection == next.Connection || procedure.Delete || !recordField(procedure.Field) || len(procedure.Value) == 0 || len(procedure.Value) > maxRecordBytes || len(metadata) == 0 || len(metadata) > storagecodec.MaxNativeOutputBytes {
		return 0, ErrConflict
	}
	args := s.outputArgs(old)
	args = append(args, id.Context, id.Owner, id.Term, strconv.FormatUint(revision, 10), next.ID, next.Attempt, next.Worker, next.Epoch, next.Connection, next.ProofHash[:], next.DeliveryTag[:], next.Application.Attachment, procedure.Field, procedure.Value, metadata, next.MessageField, next.WireHash[:], next.MetadataField, strings.Join(next.Dependencies, "\x00"), next.Deadline.UnixMilli(), outputClass(old), s.maxBytes, s.namespaceContexts, s.namespaceBytes, s.primary)
	keys := []string{s.profileKey(), s.contextKey(id.Context), s.workerReadyKey(old.Worker), s.workerReadyKey(next.Worker), s.workerKey(next.Worker), s.contextDeadlineKey(id.Context), s.capacityKey()}
	callCtx, cancel := context.WithTimeout(ctx, workerRedisTimeout)
	defer cancel()
	reply, err := resumeParkedApplicationOutputScript.Run(callCtx, s.client, keys, args...).Slice()
	if err != nil {
		return 0, fmt.Errorf("%w: %w", ErrUnknown, err)
	}
	if err = status(reply); err != nil {
		return 0, err
	}
	if len(reply) != 2 || reply[1] != strconv.FormatUint(revision+1, 10) {
		return 0, errors.Join(ErrUnknown, ErrCorrupt)
	}
	if callCtx.Err() != nil {
		return 0, errors.Join(ErrUnknown, callCtx.Err())
	}
	return revision + 1, nil
}

var resumeParkedApplicationOutputScript = redis.NewScript(outputScript + parkedScript + `
local problem=guard();if problem then return fail(problem) end
local root=string.sub(KEYS[1],1,#KEYS[1]-#'core:profile')
local context,app,term,revision=ARGV[9],ARGV[10],ARGV[11],ARGV[12]
local id,attempt,worker,epoch,connection,proof,tag,attachment=unpack(ARGV,13,20)
local procedure,procedureBytes,metadata=ARGV[21],ARGV[22],ARGV[23]
if KEYS[2]~=root..'core:context:'..context or KEYS[3]~=root..'core:ready:'..ARGV[5] or KEYS[4]~=root..'core:ready:'..worker or KEYS[5]~=root..'core:worker:'..worker or KEYS[6]~=root..'core:deadlines:'..context or KEYS[7]~=root..'core:capacity' or not kind(KEYS[3],'zset') or not kind(KEYS[4],'zset') or not kind(KEYS[5],'hash') or not kind(KEYS[6],'zset') then return fail('CORRUPT') end
if not appRef(id) or not appRef(attempt) or not appRef(worker) or not appRef(epoch) or not appRef(connection) or #proof~=32 or #tag~=32 or #metadata==0 or #metadata>65536 or #procedureBytes==0 or #procedureBytes>67108864 or id==ARGV[3] or attempt==ARGV[4] or proof==ARGV[8] or worker==ARGV[5] and epoch==ARGV[6] and connection==ARGV[7] then return fail('CONFLICT') end
local out;out,problem=output();if problem then return fail(problem) end
local a=out.values
if a[8]~='R' or a[13]~='A' or a[6]~=ARGV[24] or a[7]~=ARGV[25] or a[11]~=ARGV[26] or a[17]~=ARGV[27] or a[5]~=ARGV[28] or out.refs[a[11]]~=1 then return fail('CONFLICT') end
if a[14]~=app or a[15]~=term then return fail('AUTHORITY') end
local owner=redis.call('HMGET',KEYS[2],'owner','term','revision','r:delivery-tag','bytes','index_bytes')
if owner[1]~=app or owner[2]~=term or owner[4]~=tag then return fail('AUTHORITY') end
if owner[3]~=revision or not decimal(revision) or revision=='0' or revision=='18446744073709551615' then return fail('CONFLICT') end
if parked(root,context,ARGV[2]) then return fail('PARKED') end
local hasProcedure=false;for _,dependency in ipairs(out.dependencies) do if dependency.field==procedure then hasProcedure=true end end
if not hasProcedure or redis.call('HEXISTS',KEYS[2],'ref:'..procedure)~=0 then return fail('CONFLICT') end
local oldMember=context..'/'..ARGV[3];local member=context..'/'..id
if redis.call('ZSCORE',KEYS[3],oldMember) or redis.call('ZSCORE',KEYS[4],member) then return fail('CONFLICT') end
local due=number(a[5]);local timer=redis.call('ZSCORE',KEYS[6],'out-'..string.sub(a[11],3));if not timer or number(timer)~=due then return fail('CONFLICT') end
local clock=redis.call('TIME');local now=tonumber(clock[1])*1000+math.floor(tonumber(clock[2])/1000)
if due<=now then return fail('EXPIRED') end
local workerLease=redis.call('HMGET',KEYS[5],'epoch','expires_ms');if workerLease[1]~=epoch or not number(workerLease[2]) or number(workerLease[2])<=now then return fail('AUTHORITY') end
problem=guardApplication(root,app,term,now);if problem then return fail(problem) end
problem=guardApplicationAttachment(root,app,term,attachment,now);if problem then return fail(problem) end
local oldRecords=redis.call('HMGET',KEYS[2],procedure,a[11]);if not oldRecords[1] or not oldRecords[2] then return fail('CORRUPT') end
local total,indexes=number(owner[5]),number(owner[6]);if not total or not indexes then return fail('CORRUPT') end
total=total+#procedureBytes-#oldRecords[1]+#metadata-#oldRecords[2];indexes=indexes+#member+8
local suffixes={'a','w','e','c','d','m','h','s','k','n','x','p','q','u','t','g','l','z'};local oldNames={}
local nextPrefix='o:'..id..':'
local parts={attempt,worker,epoch,connection,a[5],a[6],a[7],'P','0','0',a[11],proof,'A',app,term,tag,a[17],attachment~='' and attachment or false}
for i,suffix in ipairs(suffixes) do
 local name=nextPrefix..suffix
 if redis.call('HEXISTS',KEYS[2],name)~=0 then return fail('CONFLICT') end
 if a[i] then local oldName=out.prefix..suffix;oldNames[#oldNames+1]=oldName;total=total-#oldName-#a[i] end
 if parts[i] then
  total=total+#name+#parts[i]
  if not redis.acl_check_cmd('HSET',KEYS[2],name,parts[i]) then return fail('AUTHORITY') end
 end
end
if total<0 or indexes>9007199254740991 then return fail('CORRUPT') end
local nextRevision=increment(revision)
local ns;ns,problem=nsPlan(KEYS[7],KEYS[2],false,false,ARGV[2],app,term,nextRevision,total,0,indexes,number(ARGV[#ARGV-2]),number(ARGV[#ARGV-1]),true,0,false,number(ARGV[#ARGV-3]),'','','');if problem then return fail(problem) end
local wake=root..'core:wake:'..worker
local totalText=string.format('%.0f',total)
if not redis.acl_check_cmd('HDEL',KEYS[2],unpack(oldNames)) or not redis.acl_check_cmd('HSET',KEYS[2],procedure,procedureBytes,a[11],metadata,'bytes',totalText,'revision',nextRevision) or not redis.acl_check_cmd('ZADD',KEYS[4],a[5],member) or not redis.acl_check_cmd('PUBLISH',wake,'1') then return fail('AUTHORITY') end
redis.call('HDEL',KEYS[2],unpack(oldNames))
for i,suffix in ipairs(suffixes) do if parts[i] then redis.call('HSET',KEYS[2],nextPrefix..suffix,parts[i]) end end
redis.call('HSET',KEYS[2],procedure,procedureBytes,a[11],metadata,'bytes',totalText,'revision',nextRevision)
redis.call('ZADD',KEYS[4],a[5],member);nsApply(ns);redis.call('PUBLISH',wake,'1')
return {'OK',nextRevision}
`)
