package rediscore

// AdmissionClass identifies the new-work limit charged to an instance. The
// charge stays with the admitting instance, independent of timer execution.
type AdmissionClass string

const (
	AdmissionPending    AdmissionClass = "pending"
	AdmissionInbound    AdmissionClass = "inbound"
	AdmissionOutbound   AdmissionClass = "outbound"
	AdmissionRefusal    AdmissionClass = "refusal"
	AdmissionStandalone AdmissionClass = "standalone"
)

type Admission struct {
	Instance string
	Class    AdmissionClass
	Source   string
}

func (a Admission) valid(create bool) bool {
	if a == (Admission{}) {
		return !create
	}
	if create {
		if !reference(a.Instance) {
			return false
		}
		switch a.Class {
		case AdmissionPending:
			return reference(a.Source)
		case AdmissionOutbound, AdmissionRefusal, AdmissionStandalone:
			return a.Source == ""
		}
		return false
	}
	return a.Instance == "" && a.Source == "" && (a.Class == AdmissionInbound || a.Class == AdmissionRefusal)
}

const admissionScript = `
local admissionLimits={pending=128,inbound=1024,outbound=1024,refusal=1024,standalone=512}
local function admissionPlan(capacity,old,creating,retiring,instance,class,source)
 local changes={}
 local priorInstance,priorClass,priorSource
 if old then
  priorInstance,priorClass,priorSource=string.match(old,'^([%w_-]+):([a-z]+):([%w_-]*)$')
  if not priorInstance or not admissionLimits[priorClass] or ((priorClass=='pending')~=(priorSource~='')) then return nil,'CORRUPT' end
  if not retiring and (class==nil or class=='' or class==priorClass) then return {value=old,changes=changes},nil end
 elseif not creating then
  if class and class~='' then return nil,'CORRUPT' end
  return {changes=changes},nil
 elseif not class or class=='' then
  return {changes=changes},nil
 end
 local pod=priorInstance or instance
 local prefix='admission:'..pod..':'
 local function count(key)
  local value=redis.call('HGET',capacity,prefix..key)
  if not value then return 0 end
  return number(value)
 end
 local function change(key,delta)
  local value=count(key)
  if not value or value+delta<0 then return 'CORRUPT' end
  local field=prefix..key
  local next=string.format('%.0f',value+delta)
  if value+delta==0 then
   if not redis.acl_check_cmd('HDEL',capacity,field) then return 'AUTHORITY' end
   changes[field]=false
  else
   if not redis.acl_check_cmd('HSET',capacity,field,next) then return 'AUTHORITY' end
   changes[field]=next
  end
 end
 local problem
 if priorClass then
  if not retiring and not (priorClass=='pending' and (class=='inbound' or class=='refusal') or priorClass=='inbound' and class=='refusal') then return nil,'CONFLICT' end
  problem=change(priorClass,-1);if problem then return nil,problem end
  if priorClass=='pending' then problem=change('source:'..priorSource,-1);if problem then return nil,problem end end
 end
 if retiring then return {changes=changes},nil end
 if creating then
  local used=count(class);if not used then return nil,'CORRUPT' end
  if used>=admissionLimits[class] then return nil,'CAPACITY' end
  if class=='pending' or class=='refusal' then
   local pending,refusal=count('pending'),count('refusal');if not pending or not refusal then return nil,'CORRUPT' end
   if pending+refusal>=admissionLimits.refusal then return nil,'CAPACITY' end
   if class=='pending' then
    local inbound,sourceUsed=count('inbound'),count('source:'..source);if not inbound or not sourceUsed then return nil,'CORRUPT' end
    if inbound+pending>=admissionLimits.inbound or sourceUsed>=32 then return nil,'CAPACITY' end
    problem=change('source:'..source,1);if problem then return nil,problem end
   end
  end
 end
 problem=change(class,1);if problem then return nil,problem end
 return {value=pod..':'..class..':'..(source or ''),changes=changes},nil
end
`
