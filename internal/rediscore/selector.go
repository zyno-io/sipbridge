package rediscore

import (
	"context"
	"strconv"
	"strings"

	"github.com/redis/go-redis/v9"
)

// SelectorKind distinguishes server/client transactions and dialogs. Digests
// are raw bytes, never textual encodings of private Redis data.
type SelectorKind byte

const (
	SelectorServerTransaction SelectorKind = 1
	SelectorClientTransaction SelectorKind = 2
	SelectorDialog            SelectorKind = 3
)

type Selector struct {
	Kind   SelectorKind
	Digest [32]byte
}

func (s Selector) valid() bool {
	return s.Kind >= SelectorServerTransaction && s.Kind <= SelectorDialog
}
func (s Selector) field() []byte {
	v := make([]byte, 33)
	v[0] = byte(s.Kind)
	copy(v[1:], s.Digest[:])
	return v
}

type SelectorTarget struct{ Context, Incarnation, Record string }

func (t SelectorTarget) valid() bool {
	return reference(t.Context) && reference(t.Incarnation) && recordField(t.Record)
}
func (t SelectorTarget) value() string { return t.Context + "\x00" + t.Incarnation + "\x00" + t.Record }
func parseSelectorTarget(v string) (SelectorTarget, error) {
	p := strings.Split(v, "\x00")
	if len(p) != 3 {
		return SelectorTarget{}, ErrCorrupt
	}
	t := SelectorTarget{p[0], p[1], p[2]}
	if !t.valid() {
		return SelectorTarget{}, ErrCorrupt
	}
	return t, nil
}
func (s *Store) selectorKey() string { return s.root + "core:selectors" }

type SelectorLookup struct {
	Identity Identity
	Record   string
	View     *View
}

// LookupSelector atomically resolves a binary native selector and reads its
// target plus named companion fields with current ownership and revision. This
// backend targets one Redis primary; no preliminary mapping RPC is required.
func (s *Store) LookupSelector(ctx context.Context, selector Selector, fields ...string) (*SelectorLookup, error) {
	if !selector.valid() || len(fields) > maxFields {
		return nil, ErrConflict
	}
	seen := make(map[string]bool, len(fields))
	args := []interface{}{s.profile, selector.field()}
	for _, field := range fields {
		if !recordField(field) || seen[field] {
			return nil, ErrConflict
		}
		seen[field] = true
		args = append(args, field)
	}
	args = append(args, s.primary)
	values, err := selectorLookupScript.Run(ctx, s.client, []string{s.profileKey(), s.selectorKey()}, args...).Slice()
	if err != nil {
		return nil, err
	}
	if err = status(values); err != nil {
		return nil, err
	}
	if len(values) != 13+len(fields) {
		return nil, ErrCorrupt
	}
	var metadata [11]string
	for i := range metadata {
		value, ok := values[i+1].(string)
		if !ok {
			return nil, ErrCorrupt
		}
		metadata[i] = value
	}
	id := Identity{Context: metadata[0], Incarnation: metadata[1], Owner: metadata[2], Term: metadata[3]}
	if !id.valid() || !recordField(metadata[10]) {
		return nil, ErrCorrupt
	}
	revision, err := strconv.ParseUint(metadata[4], 10, 64)
	if err != nil || revision == 0 {
		return nil, ErrCorrupt
	}
	recordBytes, err := responseInt(metadata[5])
	if err != nil {
		return nil, err
	}
	reserved, err := responseInt(metadata[6])
	if err != nil {
		return nil, err
	}
	timer := TimerAssignment{Worker: metadata[7], Epoch: metadata[8], Term: metadata[9]}
	if !timer.empty() && !timer.valid() {
		return nil, ErrCorrupt
	}
	view := &View{Revision: revision, RecordBytes: recordBytes, Reserved: reserved, Timer: timer, Records: make(map[string][]byte, len(fields)+1)}
	var total int64
	put := func(field string, raw interface{}) error {
		total += int64(len(field))
		if raw != nil {
			value, ok := raw.(string)
			if !ok || len(value) > maxRecordBytes {
				return ErrCorrupt
			}
			total += int64(len(value))
			view.Records[field] = []byte(value)
		} else {
			view.Records[field] = nil
		}
		if total > maxOperationSize {
			return ErrCapacity
		}
		return nil
	}
	target := metadata[10]
	if values[12] == nil {
		return nil, ErrCorrupt
	}
	if err = put(target, values[12]); err != nil {
		return nil, err
	}
	for i, field := range fields {
		if field != target {
			if err = put(field, values[13+i]); err != nil {
				return nil, err
			}
		}
	}
	return &SelectorLookup{Identity: id, Record: target, View: view}, nil
}

// SelectorChange is relative to the Publication's context/incarnation. A
// deletion must match that exact record; an insertion cannot replace a peer.
type SelectorChange struct {
	Selector Selector
	Record   string
	Delete   bool
}

var selectorLookupScript = redis.NewScript(commonScript + `
local problem=guard();if problem then return fail(problem) end
if #ARGV<3 or #ARGV>131 or #ARGV[2]~=33 or string.byte(ARGV[2],1)<1 or string.byte(ARGV[2],1)>3 then return fail('CORRUPT') end
local n=redis.call('HSTRLEN',KEYS[2],ARGV[2])
if n==0 then if redis.call('HEXISTS',KEYS[2],ARGV[2])==1 then return fail('CORRUPT') end;return fail('ABSENT') end
if n>580 then return fail('CORRUPT') end
local binding=redis.call('HGET',KEYS[2],ARGV[2])
local first=string.find(binding,'\0',1,true);if not first then return fail('CORRUPT') end
local second=string.find(binding,'\0',first+1,true);if not second or string.find(binding,'\0',second+1,true) then return fail('CORRUPT') end
local contextID=string.sub(binding,1,first-1);local incarnation=string.sub(binding,first+1,second-1);local target=string.sub(binding,second+1)
local function ref(v) return type(v)=='string' and #v>0 and #v<=192 and string.match(v,'^[A-Za-z0-9_-]+$') end
local function record(v) return type(v)=='string' and #v>=3 and #v<=194 and string.match(v,'^r:[A-Za-z0-9_-]+$') end
if not ref(contextID) or not ref(incarnation) or not record(target) then return fail('CORRUPT') end
local root=string.sub(KEYS[1],1,#KEYS[1]-#'core:profile');local context=root..'core:context:'..contextID
if not kind(context,'hash') then return fail('CORRUPT') end
local v=redis.call('HMGET',context,'incarnation','owner','term','revision','bytes','reserved','timer:worker','timer:epoch','timer:term')
if not v[1] or v[1]~=incarnation then return fail('CONFLICT') end
if type(v[2])~='string' or type(v[3])~='string' or (v[2]=='')~=(v[3]=='') or (v[2]~='' and (not ref(v[2]) or not ref(v[3]) or #v[3]>190)) or not decimal(v[4]) or v[4]=='0' or not number(v[5]) or not number(v[6]) then return fail('CORRUPT') end
if (v[7] or v[8] or v[9]) and (not ref(v[7]) or not ref(v[8]) or not ref(v[9])) then return fail('CORRUPT') end
if redis.call('HEXISTS',context,target)==0 then return fail('CORRUPT') end
local total=#target+redis.call('HSTRLEN',context,target);if total>67108864 then return fail('CAPACITY') end
local seen={};local count=1
for i=3,#ARGV-1 do
 local field=ARGV[i];if not record(field) or seen[field] then return fail('CORRUPT') end;seen[field]=true
 if field~=target then count=count+1;total=total+#field+redis.call('HSTRLEN',context,field) end
 if total>67108864 or count>128 then return fail('CAPACITY') end
end
local out={'OK',contextID,v[1],v[2],v[3],v[4],v[5],v[6],v[7] or '',v[8] or '',v[9] or '',target,redis.call('HGET',context,target)}
for i=3,#ARGV-1 do if ARGV[i]==target then out[#out+1]=false else out[#out+1]=redis.call('HGET',context,ARGV[i]) end end
return out
`)

// selectorScript is composed into the selected publication script. The
// Packed reverse references let the driver remove a failed context's exact
// selectors without scanning the shared index.
const selectorScript = `
local function selectorPrepare(index,context,incarnation,pending,changes)
 if not kind(index,'hash') then return nil,'CORRUPT' end
 if #changes>128 then return nil,'CAPACITY' end
 local seen={};local refs={};local writes={};local indexDelta,countDelta=0,0
 local function reverse(field)
  local value=refs[field]
  if value~=nil then return value end
  value=redis.call('HGET',KEYS[2],field)
  if not value then return '' end
  if #value==0 or #value%33~=0 or #value>512*33 then return nil end
  return value
 end
 for _,c in ipairs(changes) do
  local field,record,action=c[1],c[2],c[3]
  if #field~=33 or string.byte(field,1)<1 or string.byte(field,1)>3 or #record<3 or #record>194 or not string.match(record,'^r:[A-Za-z0-9_-]+$') or seen[field] or (action~='set' and action~='delete') then return nil,'CONFLICT' end
  seen[field]=true;local value=context..'\0'..incarnation..'\0'..record
  local old=redis.call('HGET',index,field)
  if old and #old>582 then return nil,'CORRUPT' end
  if action=='delete' then if not old or old~=value then return nil,'CONFLICT' end
  elseif old and old~=value then return nil,'CONFLICT'
  else local recordValue=pending[record];if recordValue==false then return nil,'CONFLICT' elseif recordValue==nil then recordValue=redis.call('HGET',KEYS[2],record) end;if recordValue==false or recordValue==nil then return nil,'CONFLICT' end end
  local change=(action=='delete' and -1 or (old and 0 or 1))
  if change~=0 then
   local ref='sref:'..record;local selectors=reverse(ref);if selectors==nil then return nil,'CORRUPT' end
   local at=nil
   for pos=1,#selectors,33 do if string.sub(selectors,pos,pos+32)==field then at=pos;break end end
   if action=='delete' then
    if not at then return nil,'CORRUPT' end
    selectors=string.sub(selectors,1,at-1)..string.sub(selectors,at+33)
   else
    if at then return nil,'CORRUPT' end
    if #selectors>=512*33 then return nil,'CAPACITY' end
    selectors=selectors..field
   end
   refs[ref]=selectors
   indexDelta=indexDelta+change*(#field+#value)
   countDelta=countDelta+change
   if action=='set' then if not redis.acl_check_cmd('HSET',index,field,value) then return nil,'AUTHORITY' end
   elseif not redis.acl_check_cmd('HDEL',index,field) then return nil,'AUTHORITY' end
   writes[#writes+1]={field=field,value=value,action=action}
  end
 end
 for field,value in pairs(pending) do
  if value==false then local selectors=reverse('sref:'..field);if selectors==nil then return nil,'CORRUPT' end;if #selectors>0 then return nil,'CONFLICT' end end
 end
 local byteDelta,fieldDelta=0,0
 for field,value in pairs(refs) do
  local old=redis.call('HGET',KEYS[2],field);if old then byteDelta=byteDelta-#field-#old;fieldDelta=fieldDelta-1 end
  if #value>0 then byteDelta=byteDelta+#field+#value;fieldDelta=fieldDelta+1;if not redis.acl_check_cmd('HSET',KEYS[2],field,value) then return nil,'AUTHORITY' end
  elseif not redis.acl_check_cmd('HDEL',KEYS[2],field) then return nil,'AUTHORITY' end
 end
 if countDelta~=0 then
  local field='selector_count';local old=redis.call('HGET',KEYS[2],field)
  local count=old and number(old) or 0
  if old and (not number(old) or count==0 or count>512) or count+countDelta<0 then return nil,'CORRUPT' end
  count=count+countDelta;if count>512 then return nil,'CAPACITY' end
  local value=count>0 and tostring(count) or ''
  if old then byteDelta=byteDelta-#field-#old;fieldDelta=fieldDelta-1 end
  if count>0 then byteDelta=byteDelta+#field+#value;fieldDelta=fieldDelta+1;if not redis.acl_check_cmd('HSET',KEYS[2],field,value) then return nil,'AUTHORITY' end
  elseif not redis.acl_check_cmd('HDEL',KEYS[2],field) then return nil,'AUTHORITY' end
  refs[field]=value
 end
 return {index=index,refs=refs,writes=writes,byteDelta=byteDelta,fieldDelta=fieldDelta,indexDelta=indexDelta},nil
end
local function selectorApply(p)
 for field,value in pairs(p.refs) do if #value==0 then redis.call('HDEL',KEYS[2],field) else redis.call('HSET',KEYS[2],field,value) end end
 for _,w in ipairs(p.writes) do if w.action=='set' then redis.call('HSET',p.index,w.field,w.value) else redis.call('HDEL',p.index,w.field) end end
end
`
