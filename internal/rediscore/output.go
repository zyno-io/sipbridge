package rediscore

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"sipbridge/internal/selectedmessage"
	"sipbridge/internal/storagecodec"
)

const maxOutputs = 32
const maxOutputDependencies = 3

// OutputIntent binds one selected message record to one physical attempt.
// The trusted admission writer supplies the SHA-256 of the exact SIP wire;
// consumers must verify the selected record and digest before a socket write.
// The immutable physical deadline is the ready-index score.
type OutputIntent struct {
	ID, Attempt, Worker, Epoch, Connection string
	MessageField, MetadataField            string
	Dependencies                           []string
	WireHash, ProofHash                    [32]byte
	Application                            *ApplicationLeaseRef
	DeliveryTag                            [32]byte
	Deadline                               time.Time
}

func (o OutputIntent) valid() bool {
	if len(o.Dependencies) > maxOutputDependencies {
		return false
	}
	for i, field := range o.Dependencies {
		if !recordField(field) || field == o.MessageField || field == o.MetadataField {
			return false
		}
		for _, previous := range o.Dependencies[:i] {
			if field == previous {
				return false
			}
		}
	}
	return reference(o.ID) && reference(o.Attempt) && reference(o.Worker) && reference(o.Epoch) && reference(o.Connection) && recordField(o.MessageField) && recordField(o.MetadataField) && o.MetadataField != o.MessageField && o.WireHash != ([32]byte{}) && o.ProofHash != ([32]byte{}) && (o.Application == nil && o.DeliveryTag == ([32]byte{}) || o.Application != nil && o.Application.valid() && o.DeliveryTag != ([32]byte{})) && !o.Deadline.IsZero() && o.Deadline.UnixMilli() > 0 && o.Deadline.UnixMilli() <= maxExactInteger
}

// OutputRef is the exact previously admitted physical attempt. Context and
// incarnation fence reuse; worker/epoch and connection identify the original socket.
type OutputRef struct {
	Context, Incarnation, ID, Attempt, Worker, Epoch, Connection string
	ProofHash                                                    [32]byte
	RequireApplication                                           bool
}

func (o OutputRef) valid() bool {
	return reference(o.Context) && reference(o.Incarnation) && reference(o.ID) && reference(o.Attempt) && reference(o.Worker) && reference(o.Epoch) && reference(o.Connection) && o.ProofHash != ([32]byte{})
}

// OutputResult contains bytes only for the one successful new reservation.
// Existing STARTED/UNKNOWN attempts return ErrUnknown and never return bytes.
// Until is a conservative monotonic cutoff capped by the original output
// deadline and the worker lease observed by reservation. The writer must check
// it and its context immediately before socket start; renewal never extends it.
type OutputResult struct {
	Wire                 []byte
	Metadata             []byte
	WireHash             [32]byte
	Connection           string
	Attachment           string
	DeadlineMS           int64
	LeaseExpiresMS       int64
	ApplicationExpiresMS int64
	Until                time.Time
	Outcome              string
}

func outputClass(o OutputRef) string {
	if o.RequireApplication {
		return "A"
	}
	return "N"
}
func (s *Store) outputArgs(o OutputRef) []interface{} {
	return []interface{}{s.profile, o.Incarnation, o.ID, o.Attempt, o.Worker, o.Epoch, o.Connection, o.ProofHash[:]}
}

// ReserveOutput atomically marks STARTED and removes ready eligibility before
// returning a selected record. Retrying after an uncertain reply cannot write.
func (s *Store) ReserveOutput(ctx context.Context, o OutputRef) (OutputResult, error) {
	if !o.valid() {
		return OutputResult{}, ErrConflict
	}
	start := time.Now()
	deadline := start.Add(workerRedisTimeout)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	callCtx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	args := s.outputArgs(o)
	args = append(args, o.Context, start.UnixMilli()-workerClockTolerance.Milliseconds(), start.UnixMilli()+max(int64(0), deadline.Sub(start).Milliseconds())+workerClockTolerance.Milliseconds(), outputClass(o), s.maxBytes, s.namespaceContexts, s.namespaceBytes, s.primary)
	v, err := reserveOutputScript.Run(callCtx, s.client, []string{s.profileKey(), s.contextKey(o.Context), s.workerKey(o.Worker), s.workerReadyKey(o.Worker), s.capacityKey()}, args...).Slice()
	finish := time.Now()
	if err != nil {
		return OutputResult{}, fmt.Errorf("%w: %w", ErrUnknown, err)
	}
	if len(v) == 0 {
		return OutputResult{}, errors.Join(ErrUnknown, ErrCorrupt)
	}
	code, ok := v[0].(string)
	if !ok {
		return OutputResult{}, errors.Join(ErrUnknown, ErrCorrupt)
	}
	switch code {
	case "UNKNOWN":
		return OutputResult{}, ErrUnknown
	case "DONE":
		if len(v) != 2 {
			return OutputResult{}, errors.Join(ErrUnknown, ErrCorrupt)
		}
		value, ok := v[1].(string)
		if !ok || (value != "WRITTEN" && value != "NOT_WRITTEN") {
			return OutputResult{}, errors.Join(ErrUnknown, ErrCorrupt)
		}
		return OutputResult{Outcome: value}, nil
	case "OK":
		if len(v) != 9 {
			return OutputResult{}, errors.Join(ErrUnknown, ErrCorrupt)
		}
		now, e := responseInt(v[1])
		due, dueErr := responseInt(v[2])
		leaseExpiry, leaseErr := responseInt(v[3])
		record, ok := v[4].(string)
		metadata, metadataOK := v[5].(string)
		wireHash, hashOK := v[6].(string)
		appExpiry, appErr := responseInt(v[7])
		attachment, attachmentOK := v[8].(string)
		elapsed := finish.Sub(start)
		skew := finish.UnixMilli() - start.UnixMilli() - elapsed.Milliseconds()
		if e != nil || dueErr != nil || leaseErr != nil || appErr != nil || (o.RequireApplication && appExpiry <= now) || (!o.RequireApplication && appExpiry != 0) || !attachmentOK || attachment != "" && (!o.RequireApplication || !reference(attachment)) || due <= now || leaseExpiry <= now || !ok || len(record) == 0 || len(record) > maxRecordBytes || !metadataOK || len(metadata) == 0 || len(metadata) > storagecodec.MaxNativeOutputBytes || !hashOK || len(wireHash) != 32 || elapsed < 0 || skew < -workerClockTolerance.Milliseconds() || skew > workerClockTolerance.Milliseconds() || now < start.UnixMilli()-workerClockTolerance.Milliseconds() || now > finish.UnixMilli()+workerClockTolerance.Milliseconds() {
			return OutputResult{}, errors.Join(ErrUnknown, ErrCorrupt)
		}
		if ctx.Err() != nil || callCtx.Err() != nil {
			return OutputResult{}, ErrUnknown
		}
		data := []byte(record)
		wire, e := selectedmessage.DecodeWire(data)
		var expectedWire [32]byte
		copy(expectedWire[:], wireHash)
		if e != nil || sha256.Sum256(wire) != expectedWire {
			return OutputResult{}, errors.Join(ErrUnknown, ErrCorrupt)
		}
		limit := min(due, leaseExpiry)
		if o.RequireApplication {
			limit = min(limit, appExpiry)
		}
		delta := limit - now
		if delta > int64((1<<63-1)/time.Millisecond) {
			return OutputResult{}, errors.Join(ErrUnknown, ErrCorrupt)
		}
		until := start.Add(time.Duration(delta) * time.Millisecond)
		if ctx.Err() != nil || callCtx.Err() != nil || !time.Now().Before(until) {
			return OutputResult{}, ErrUnknown
		}
		return OutputResult{Wire: wire, Metadata: []byte(metadata), WireHash: expectedWire, Connection: o.Connection, Attachment: attachment, DeadlineMS: due, LeaseExpiresMS: leaseExpiry, ApplicationExpiresMS: appExpiry, Until: until}, nil
	default:
		return OutputResult{}, status(v)
	}
}

// CompleteOutput records the exact physical proof. It intentionally ignores
// current context revision, application owner and current worker lease.
func (s *Store) CompleteOutput(ctx context.Context, o OutputRef, outcome string) error {
	if !o.valid() || (outcome != "WRITTEN" && outcome != "NOT_WRITTEN" && outcome != "UNKNOWN") {
		return ErrConflict
	}
	args := s.outputArgs(o)
	args = append(args, o.Context, outcome, outputClass(o), s.maxBytes, s.namespaceContexts, s.namespaceBytes, s.primary)
	v, err := completeOutputScript.Run(ctx, s.client, []string{s.profileKey(), s.contextKey(o.Context), s.workerReadyKey(o.Worker), s.capacityKey()}, args...).Slice()
	if err != nil {
		return fmt.Errorf("%w: %w", ErrUnknown, err)
	}
	if err = status(v); err != nil {
		return err
	}
	if len(v) != 1 {
		return errors.Join(ErrUnknown, ErrCorrupt)
	}
	return nil
}

// SettleOutput records two independent completion boundaries. The reference
// and metadata are released only when both are true and outcome is definite.
func (s *Store) SettleOutput(ctx context.Context, o OutputRef, acknowledge, settle bool) error {
	if !o.valid() || (!acknowledge && !settle) {
		return ErrConflict
	}
	ack, sem := "0", "0"
	if acknowledge {
		ack = "1"
	}
	if settle {
		sem = "1"
	}
	args := s.outputArgs(o)
	args = append(args, ack, sem, outputClass(o), s.maxBytes, s.namespaceContexts, s.namespaceBytes, s.primary)
	v, err := settleOutputScript.Run(ctx, s.client, []string{s.profileKey(), s.contextKey(o.Context), s.capacityKey()}, args...).Slice()
	if err != nil {
		return fmt.Errorf("%w: %w", ErrUnknown, err)
	}
	if err = status(v); err != nil {
		return err
	}
	if len(v) != 1 {
		return errors.Join(ErrUnknown, ErrCorrupt)
	}
	return nil
}

const outputScript = commonScript + namespaceScript + applicationGuardScript + `
local function output()
 if redis.call('HGET',KEYS[2],'incarnation')~=ARGV[2] then return nil,'AUTHORITY' end
 local prefix='o:'..ARGV[3]..':'
 local a=redis.call('HMGET',KEYS[2],prefix..'a',prefix..'w',prefix..'e',prefix..'c',prefix..'d',prefix..'m',prefix..'h',prefix..'s',prefix..'k',prefix..'n',prefix..'x',prefix..'p',prefix..'q',prefix..'u',prefix..'t',prefix..'g',prefix..'l',prefix..'z')
 if not a[1] then return nil,'ABSENT' end
 for i=1,17 do if not a[i] then return nil,'CORRUPT' end end
 if a[18] and (a[13]~='A' or not appRef(a[18])) then return nil,'CORRUPT' end
 if a[1]~=ARGV[4] or a[2]~=ARGV[5] or a[3]~=ARGV[6] or a[4]~=ARGV[7] or a[12]~=ARGV[8] or a[13]~=ARGV[#ARGV-4] then return nil,'CONFLICT' end
 if not number(a[5]) or number(a[5])==0 or #a[7]~=32 or not string.match(a[6],'^r:[A-Za-z0-9_-]+$') or (a[8]~='P' and a[8]~='S' and a[8]~='U' and a[8]~='W' and a[8]~='N' and a[8]~='R') or (a[9]~='0' and a[9]~='1') or (a[10]~='0' and a[10]~='1') or not string.match(a[11],'^r:[A-Za-z0-9_-]+$') or a[11]==a[6] or #a[12]~=32 or (a[13]~='A' and a[13]~='N') or (a[13]=='A' and (not appRef(a[14]) or not appRef(a[15]) or #a[16]~=32)) or (a[13]=='N' and (a[14]~='' or a[15]~='' or a[16]~='')) then return nil,'CORRUPT' end
 if a[8]=='R' and (a[13]~='A' or a[9]~='0' or a[10]~='0') then return nil,'CORRUPT' end
 local refs={}
 for _,field in ipairs({a[6],a[11]}) do local n=redis.call('HGET',KEYS[2],'ref:'..field);if not n or not number(n) or number(n)<1 then return nil,'CORRUPT' end;refs[field]=number(n) end
 local dependencies={};local dependencyNames={};local seen={}
 for field in string.gmatch(a[17],'[^%z]+') do
  if #dependencies>=3 or #field<3 or #field>194 or not string.match(field,'^r:[A-Za-z0-9_-]+$') or field==a[6] or field==a[11] or seen[field] then return nil,'CORRUPT' end
  local n=redis.call('HGET',KEYS[2],'dref:'..field);if not n or not number(n) or number(n)<1 then return nil,'CORRUPT' end
  seen[field]=true;dependencyNames[#dependencyNames+1]=field;dependencies[#dependencies+1]={field=field,count=number(n)}
 end
 if table.concat(dependencyNames,'\0')~=a[17] then return nil,'CORRUPT' end
 return {prefix=prefix,values=a,refs=refs,dependencies=dependencies},nil
end
`

var reserveOutputScript = redis.NewScript(outputScript + parkedScript + `
local problem=guard(); if problem then return fail(problem) end
if not kind(KEYS[3],'hash') or not kind(KEYS[4],'zset') then return fail('CORRUPT') end
local out; out,problem=output(); if problem then return fail(problem) end
local a=out.values
if a[8]=='S' or a[8]=='U' or a[8]=='R' then return {'UNKNOWN'} end
if a[8]=='W' then return {'DONE','WRITTEN'} elseif a[8]=='N' then return {'DONE','NOT_WRITTEN'} end
if parked(string.sub(KEYS[1],1,#KEYS[1]-#'core:profile'),ARGV[9],ARGV[2]) then return fail('PARKED') end
local lo=number(ARGV[10]); local hi=number(ARGV[11]); if not lo or not hi or hi<lo then return fail('CORRUPT') end
local clock=redis.call('TIME'); local now=tonumber(clock[1])*1000+math.floor(tonumber(clock[2])/1000)
if now<lo or now>hi then return fail('EXPIRED') end
if number(a[5])<=now then return fail('EXPIRED') end
local lease=redis.call('HMGET',KEYS[3],'epoch','expires_ms'); if not lease[1] or not lease[2] or not number(lease[2]) or lease[1]~=ARGV[6] or number(lease[2])<=now then return fail('AUTHORITY') end
local appExpiry=0
if a[13]=='A' then
 local identity=redis.call('HMGET',KEYS[2],'owner','term');if identity[1]~=a[14] or identity[2]~=a[15] then return fail('AUTHORITY') end
 local root=string.sub(KEYS[1],1,#KEYS[1]-#'core:profile');local appProblem=guardApplication(root,a[14],a[15],now);if appProblem then return fail(appProblem) end
 appExpiry=number(redis.call('HGET',root..'core:owner:'..a[15],'expires_ms'));if not appExpiry or appExpiry<=now then return fail('AUTHORITY') end
 local attachmentProblem,_,attachmentExpiry=guardApplicationAttachment(root,a[14],a[15],a[18],now);if attachmentProblem then return fail(attachmentProblem) end
 if attachmentExpiry then appExpiry=math.min(appExpiry,attachmentExpiry) end
 if redis.call('HGET',KEYS[2],'r:delivery-tag')~=a[16] then return fail('AUTHORITY') end
end
local member=ARGV[9]..'/'..ARGV[3]; local score=redis.call('ZSCORE',KEYS[4],member)
if not score or tonumber(score)~=number(a[5]) then return fail('CONFLICT') end
local record=redis.call('HGET',KEYS[2],a[6]); if not record or #record==0 or #record>67108864 then return fail('CORRUPT') end
local metadata=redis.call('HGET',KEYS[2],a[11]);if not metadata or #metadata==0 or #metadata>65536 then return fail('CORRUPT') end
local ns;ns,problem=nsCurrent(KEYS[5],KEYS[2],0,-#member-8,number(ARGV[#ARGV-2]),number(ARGV[#ARGV-1]),number(ARGV[#ARGV-3]));if problem then return fail(problem) end
if not redis.acl_check_cmd('HSET',KEYS[2],out.prefix..'s','S') or not redis.acl_check_cmd('ZREM',KEYS[4],member) then return fail('AUTHORITY') end
redis.call('HSET',KEYS[2],out.prefix..'s','S'); redis.call('ZREM',KEYS[4],member);nsApply(ns)
return {'OK',string.format('%.0f',now),a[5],lease[2],record,metadata,a[7],string.format('%.0f',appExpiry),a[18] or ''}
`)

var completeOutputScript = redis.NewScript(outputScript + `
local problem=guard(); if problem then return fail(problem) end
if not kind(KEYS[3],'zset') then return fail('CORRUPT') end
local out; out,problem=output(); if problem then return fail(problem) end
local phase=out.values[8]; local requested=ARGV[10]; local next
if requested=='WRITTEN' then next='W' elseif requested=='NOT_WRITTEN' then next='N' elseif requested=='UNKNOWN' then next='U' else return fail('CORRUPT') end
if phase=='W' or phase=='N' then if phase==next then return {'OK'} else return fail('CONFLICT') end end
if phase=='R' then return fail('CONFLICT') end
if phase=='P' and next~='N' then return fail('CONFLICT') end
if phase=='U' and next=='U' then return {'OK'} end
local member=ARGV[9]..'/'..ARGV[3]
local ns=nil
if phase=='P' then local score=redis.call('ZSCORE',KEYS[3],member);if not score or number(score)~=number(out.values[5]) then return fail('CONFLICT') end;ns,problem=nsCurrent(KEYS[4],KEYS[2],0,-#member-8,number(ARGV[#ARGV-2]),number(ARGV[#ARGV-1]),number(ARGV[#ARGV-3]));if problem then return fail(problem) end end
if not redis.acl_check_cmd('HSET',KEYS[2],out.prefix..'s',next) or (phase=='P' and not redis.acl_check_cmd('ZREM',KEYS[3],member)) then return fail('AUTHORITY') end
redis.call('HSET',KEYS[2],out.prefix..'s',next)
if phase=='P' then redis.call('ZREM',KEYS[3],member);nsApply(ns) end
return {'OK'}
`)

const outputReleaseScript = `
local function outputReleasePlan(out,capacity)
local a=out.values
local current=number(redis.call('HGET',KEYS[2],'bytes')); if not current then return nil,'CORRUPT' end
local suffixes={'a','w','e','c','d','m','h','s','k','n','x','p','q','u','t','g','l'}; local names={}; local removed=0
if a[18] then suffixes[18]='z' end
for i,suffix in ipairs(suffixes) do local field=out.prefix..suffix; names[i]=field; removed=removed+#field+#a[i] end
local refChanges={}
for _,field in ipairs({a[6],a[11]}) do
 local ref='ref:'..field;local old=tostring(out.refs[field]);local next=out.refs[field]-1
 if next==0 then removed=removed+#ref+#old else removed=removed+#old-#tostring(next) end
 refChanges[#refChanges+1]={key=ref,next=next}
end
for _,dependency in ipairs(out.dependencies) do
 local ref='dref:'..dependency.field;local old=tostring(dependency.count);local next=dependency.count-1
 if next==0 then removed=removed+#ref+#old else removed=removed+#old-#tostring(next) end
 refChanges[#refChanges+1]={key=ref,next=next}
end
if removed>current then return nil,'CORRUPT' end
local ns,problem=nsCurrent(capacity,KEYS[2],-removed,0,number(ARGV[#ARGV-2]),number(ARGV[#ARGV-1]),number(ARGV[#ARGV-3]));if problem then return nil,problem end
if not redis.acl_check_cmd('HDEL',KEYS[2],unpack(names)) or not redis.acl_check_cmd('HSET',KEYS[2],'bytes',string.format('%.0f',current-removed))  then return nil,'AUTHORITY' end
for _,r in ipairs(refChanges) do if r.next==0 then if not redis.acl_check_cmd('HDEL',KEYS[2],r.key) then return nil,'AUTHORITY' end else if not redis.acl_check_cmd('HSET',KEYS[2],r.key,tostring(r.next)) then return nil,'AUTHORITY' end end end
return {names=names,refChanges=refChanges,bytes=string.format('%.0f',current-removed),ns=ns},nil
end
local function outputReleaseApply(plan)
redis.call('HDEL',KEYS[2],unpack(plan.names))
for _,r in ipairs(plan.refChanges) do if r.next==0 then redis.call('HDEL',KEYS[2],r.key) else redis.call('HSET',KEYS[2],r.key,tostring(r.next)) end end
redis.call('HSET',KEYS[2],'bytes',plan.bytes);nsApply(plan.ns)
end
`

var settleOutputScript = redis.NewScript(outputScript + outputReleaseScript + `
local problem=guard(); if problem then return fail(problem) end
local out; out,problem=output(); if problem then return fail(problem) end
local a=out.values
if a[8]~='W' and a[8]~='N' then return fail('CONFLICT') end
if (ARGV[9]~='0' and ARGV[9]~='1') or (ARGV[10]~='0' and ARGV[10]~='1') then return fail('CORRUPT') end
local ack=(a[9]=='1' or ARGV[9]=='1'); local settled=(a[10]=='1' or ARGV[10]=='1')
if not ack or not settled then
 if not redis.acl_check_cmd('HSET',KEYS[2],out.prefix..'k',ack and '1' or '0',out.prefix..'n',settled and '1' or '0') then return fail('AUTHORITY') end
 redis.call('HSET',KEYS[2],out.prefix..'k',ack and '1' or '0',out.prefix..'n',settled and '1' or '0'); return {'OK'}
end
local release;release,problem=outputReleasePlan(out,KEYS[3]);if problem then return fail(problem) end
outputReleaseApply(release)
return {'OK'}
`)

// OutputState is the retained exact physical state, not permission to write.
type OutputState string

const (
	OutputPending      OutputState = "P"
	OutputStarted      OutputState = "S"
	OutputUnknown      OutputState = "U"
	OutputWritten      OutputState = "W"
	OutputNotWritten   OutputState = "N"
	OutputResumeParked OutputState = "R"
)

type OutputInspection struct {
	Metadata              []byte
	MetadataField         string
	MessageField          string
	WireHash              [32]byte
	State                 OutputState
	Acknowledged, Settled bool
	DeadlineMS            int64
}

// InspectOutput checks the exact immutable attempt and returns metadata only.
// It does not read SIP wire, reserve, or depend on current owner/worker leases.
func (s *Store) InspectOutput(ctx context.Context, o OutputRef) (OutputInspection, error) {
	if !o.valid() {
		return OutputInspection{}, ErrConflict
	}
	args := s.outputArgs(o)
	args = append(args, outputClass(o), s.maxBytes, s.namespaceContexts, s.namespaceBytes, s.primary)
	v, err := inspectOutputScript.Run(ctx, s.client, []string{s.profileKey(), s.contextKey(o.Context)}, args...).Slice()
	if err != nil {
		return OutputInspection{}, err
	}
	if err = status(v); err != nil {
		return OutputInspection{}, err
	}
	if len(v) != 9 {
		return OutputInspection{}, ErrCorrupt
	}
	phase, ok1 := v[1].(string)
	metadata, ok2 := v[2].(string)
	ack, ok3 := v[3].(string)
	settled, ok4 := v[4].(string)
	deadline, err := responseInt(v[5])
	field, ok5 := v[6].(string)
	messageField, ok6 := v[7].(string)
	wireHash, ok7 := v[8].(string)
	if !ok1 || !ok2 || !ok3 || !ok4 || !ok5 || !ok6 || !ok7 || !recordField(field) || !recordField(messageField) || len(wireHash) != 32 || err != nil || len(metadata) == 0 || len(metadata) > storagecodec.MaxNativeOutputBytes || deadline == 0 || (ack != "0" && ack != "1") || (settled != "0" && settled != "1") {
		return OutputInspection{}, ErrCorrupt
	}
	state := OutputState(phase)
	switch state {
	case OutputPending, OutputStarted, OutputUnknown, OutputWritten, OutputNotWritten, OutputResumeParked:
	default:
		return OutputInspection{}, ErrCorrupt
	}
	var hash [32]byte
	copy(hash[:], wireHash)
	return OutputInspection{Metadata: []byte(metadata), MetadataField: field, MessageField: messageField, WireHash: hash, State: state, Acknowledged: ack == "1", Settled: settled == "1", DeadlineMS: deadline}, nil
}

var inspectOutputScript = redis.NewScript(outputScript + `
local problem=guard();if problem then return fail(problem) end
local out;out,problem=output();if problem then return fail(problem) end
local a=out.values;local size=redis.call('HSTRLEN',KEYS[2],a[11]);if size==0 or size>65536 then return fail('CORRUPT') end
local metadata=redis.call('HGET',KEYS[2],a[11]);return {'OK',a[8],metadata,a[9],a[10],a[5],a[11],a[6],a[7]}
`)
