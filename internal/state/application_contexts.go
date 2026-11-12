package state

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"sort"
	"strings"
)

const ApplicationContextPageLimit = 64
const ApplicationContextLimit = 4096
const applicationContextGroupLimit = 1024
const applicationContextMemberLimit = 8192

// ApplicationContextIndex is derivative identity metadata, not protocol state.
// LeaseTerm preserves lineage across any later reuse of an owner lease ID.
type ApplicationContextIndex struct {
	LeaseID   string `json:"leaseId"`
	LeaseTerm string `json:"leaseTerm"`
	ContextID string `json:"contextId"`
}
type ApplicationContextEntry struct {
	Ref       Ref    `json:"ref"`
	ContextID string `json:"contextId"`
}
type ApplicationContextPage struct {
	Entries    []ApplicationContextEntry `json:"entries"`
	NextCursor string                    `json:"nextCursor,omitempty"`
}
type applicationContextProjection struct {
	LeaseID string `json:"leaseId"`
	Member  string `json:"member"`
}

func validateApplicationContexts(contexts []ApplicationContextIndex, ids []string) error {
	if len(contexts) > applicationContextGroupLimit {
		return ErrCapacity
	}
	owners := make(map[string]bool, len(ids))
	for _, id := range ids {
		owners[id] = true
	}
	seen := make(map[string]bool, len(contexts))
	for _, entry := range contexts {
		if !validApplicationLeaseRef(ApplicationLeaseRef{ID: entry.LeaseID, Term: entry.LeaseTerm}) || !owners[entry.LeaseID] || !ValidReference(entry.ContextID) || seen[entry.ContextID] {
			return ErrCorrupt
		}
		seen[entry.ContextID] = true
	}
	return nil
}
func applicationContextProjections(ref Ref, contexts []ApplicationContextIndex) ([]applicationContextProjection, error) {
	result := make([]applicationContextProjection, 0, len(contexts))
	for _, entry := range contexts {
		encoded, err := json.Marshal(ApplicationContextEntry{Ref: ref, ContextID: entry.ContextID})
		if err != nil {
			return nil, err
		}
		member := entry.LeaseTerm + "|" + string(encoded)
		if len(member) > applicationContextMemberLimit {
			return nil, ErrCapacity
		}
		result = append(result, applicationContextProjection{LeaseID: entry.LeaseID, Member: member})
	}
	return result, nil
}
func applicationContextCursor(ref ApplicationLeaseRef, cursor string, limit int) (string, error) {
	if !validApplicationLeaseRef(ref) || limit < 1 || limit > ApplicationContextPageLimit {
		return "", ErrConflict
	}
	if cursor == "" {
		return "", nil
	}
	if len(cursor) > base64.RawURLEncoding.EncodedLen(2*applicationContextMemberLimit+1024) {
		return "", ErrConflict
	}
	data, err := base64.RawURLEncoding.DecodeString(cursor)
	var position struct {
		Owner ApplicationLeaseRef `json:"owner"`
		After string              `json:"after"`
	}
	if err != nil || json.Unmarshal(data, &position) != nil || position.Owner != ref || len(position.After) > applicationContextMemberLimit || !strings.HasPrefix(position.After, ref.Term+"|") {
		return "", ErrConflict
	}
	return position.After, nil
}
func encodeApplicationContextCursor(ref ApplicationLeaseRef, member string) string {
	data, _ := json.Marshal(struct {
		Owner ApplicationLeaseRef `json:"owner"`
		After string              `json:"after"`
	}{ref, member})
	return base64.RawURLEncoding.EncodeToString(data)
}
func (s *Store) ListApplicationContexts(ctx context.Context, ref ApplicationLeaseRef, cursor string, limit int) (ApplicationContextPage, error) {
	after, err := applicationContextCursor(ref, cursor, limit)
	if err != nil {
		return ApplicationContextPage{}, err
	}
	key := s.applicationLeaseKeys(ref.ID)[0]
	values, err := s.runKeys(ctx, applicationContextsScript, Ref{Partition: 0}, []string{key, key + ":contexts"}, []interface{}{ref.ID, ref.Term, ref.Generation, after, limit}, false)
	if err != nil {
		return ApplicationContextPage{}, err
	}
	if len(values) != 2 || values[0] != "CONTEXTS" {
		return ApplicationContextPage{}, ErrCorrupt
	}
	var members []string
	encoded, err := textAt(values, 1)
	if err != nil || json.Unmarshal([]byte(encoded), &members) != nil || len(members) > limit+1 {
		return ApplicationContextPage{}, ErrCorrupt
	}
	page := ApplicationContextPage{Entries: make([]ApplicationContextEntry, 0, min(limit, len(members)))}
	previous := after
	for n, member := range members {
		if n == limit {
			page.NextCursor = encodeApplicationContextCursor(ref, members[n-1])
			break
		}
		if len(member) > applicationContextMemberLimit || !strings.HasPrefix(member, ref.Term+"|") || member <= previous {
			return ApplicationContextPage{}, ErrCorrupt
		}
		var entry ApplicationContextEntry
		if json.Unmarshal([]byte(member[len(ref.Term)+1:]), &entry) != nil || s.validateRef(entry.Ref) != nil || entry.Ref.Incarnation == "" || !ValidReference(entry.ContextID) {
			return ApplicationContextPage{}, ErrCorrupt
		}
		page.Entries = append(page.Entries, entry)
		previous = member
	}
	return page, nil
}
func (m *MemoryStore) ListApplicationContexts(ctx context.Context, ref ApplicationLeaseRef, cursor string, limit int) (ApplicationContextPage, error) {
	after, err := applicationContextCursor(ref, cursor, limit)
	if err != nil {
		return ApplicationContextPage{}, err
	}
	now, err := m.lock(ctx)
	if err != nil {
		return ApplicationContextPage{}, err
	}
	defer m.unlock()
	lease := m.liveLease(ref.ID, now)
	if lease == nil || lease.Term != ref.Term || lease.Generation != ref.Generation {
		return ApplicationContextPage{}, ErrStale
	}
	if lease.sealed || lease.ExpiresAtMS <= now {
		return ApplicationContextPage{}, ErrExpired
	}
	entries := m.applicationContexts[ref.ID]
	members := make([]string, 0, len(entries))
	for member := range entries {
		if strings.HasPrefix(member, ref.Term+"|") && member > after {
			members = append(members, member)
		}
	}
	sort.Strings(members)
	page := ApplicationContextPage{Entries: make([]ApplicationContextEntry, 0, min(limit, len(members)))}
	for n, member := range members {
		if n == limit {
			page.NextCursor = encodeApplicationContextCursor(ref, members[n-1])
			break
		}
		page.Entries = append(page.Entries, entries[member])
	}
	return page, nil
}

type memoryContextChange struct {
	owner, member string
	entry         ApplicationContextEntry
	remove        bool
}

func (m *MemoryStore) planApplicationContexts(ref Ref, before, after []ApplicationContextIndex) ([]memoryContextChange, error) {
	old, err := applicationContextProjections(ref, before)
	if err != nil {
		return nil, err
	}
	next, err := applicationContextProjections(ref, after)
	if err != nil {
		return nil, err
	}
	oldSet, newSet := make(map[applicationContextProjection]bool), make(map[applicationContextProjection]bool)
	for _, entry := range old {
		oldSet[entry] = true
	}
	for _, entry := range next {
		newSet[entry] = true
	}
	changes := make([]memoryContextChange, 0, len(old)+len(next))
	delta := make(map[string]int)
	for entry := range oldSet {
		if _, ok := m.applicationContexts[entry.LeaseID][entry.Member]; !ok {
			return nil, ErrCorrupt
		}
		if !newSet[entry] {
			changes = append(changes, memoryContextChange{owner: entry.LeaseID, member: entry.Member, remove: true})
			delta[entry.LeaseID]--
		}
	}
	for entry := range newSet {
		if oldSet[entry] {
			continue
		}
		if _, exists := m.applicationContexts[entry.LeaseID][entry.Member]; exists {
			return nil, ErrCorrupt
		}
		var value ApplicationContextEntry
		if json.Unmarshal([]byte(entry.Member[strings.IndexByte(entry.Member, '|')+1:]), &value) != nil {
			return nil, ErrCorrupt
		}
		changes = append(changes, memoryContextChange{owner: entry.LeaseID, member: entry.Member, entry: value})
		delta[entry.LeaseID]++
	}
	for owner, d := range delta {
		if len(m.applicationContexts[owner])+d > ApplicationContextLimit {
			return nil, ErrCapacity
		}
	}
	return changes, nil
}
func (m *MemoryStore) applyApplicationContexts(changes []memoryContextChange) {
	for _, change := range changes {
		if change.remove {
			delete(m.applicationContexts[change.owner], change.member)
			if len(m.applicationContexts[change.owner]) == 0 {
				delete(m.applicationContexts, change.owner)
			}
			continue
		}
		if m.applicationContexts == nil {
			m.applicationContexts = make(map[string]map[string]ApplicationContextEntry)
		}
		if m.applicationContexts[change.owner] == nil {
			m.applicationContexts[change.owner] = make(map[string]ApplicationContextEntry)
		}
		m.applicationContexts[change.owner][change.member] = change.entry
	}
}

var applicationContextsScript = stateScript(`
if not primary() then return fail('AUTHORITY') end
local id,term,generation,after,limit=ARGV[7],ARGV[8],tonumber(ARGV[9]),ARGV[10],tonumber(ARGV[11])
if not keytype(KEYS[2],'zset') then return fail('CORRUPT') end
local now=nowms(); local lease,err=readApplicationLeaseRecord(KEYS[1],id,120000)
if err then return fail(err) end
if not lease or lease.term~=term or lease.generation~=generation then return fail('STALE') end
if lease.sealed or lease.expires<=now then return fail('EXPIRED') end
if redis.call('ZCARD',KEYS[2])>4096 then return fail('CORRUPT') end
local lower='['..term..'|'
if after~='' then lower='('..after end
local members=redis.call('ZRANGE',KEYS[2],lower,'['..term..'|'..string.char(255),'BYLEX','LIMIT',0,limit+1)
for _,member in ipairs(members) do if redis.call('ZSCORE',KEYS[2],member)~='0' then return fail('CORRUPT') end end
return {'CONTEXTS',#members==0 and '[]' or cjson.encode(members)}
`)

const applicationContextIndexLua = `
local function planApplicationContexts(before,after)
 local prefix=ARGV[#ARGV-2]; local changes={}; local deltas={}; local old,new={},{}
 local function entries(encoded,result)
  local decoded=cjson.decode(encoded)
  if type(decoded)~='table' or #decoded>1024 then return 'CORRUPT' end
  for _,entry in ipairs(decoded) do
   if not validApplicationLeaseID(entry.leaseId) or type(entry.member)~='string' or #entry.member>8192 or #entry.member<38 or
      not validApplicationLeaseTerm(string.sub(entry.member,1,36)) or string.sub(entry.member,37,37)~='|' then return 'CORRUPT' end
   local key=prefix..entry.leaseId..':contexts'
   if not keytype(key,'zset') then return 'CORRUPT' end
   local value=cjson.decode(string.sub(entry.member,38))
   local group=string.match(KEYS[1],':group:([A-Za-z0-9_-]+)$')
   if type(value)~='table' or not validApplicationLeaseID(value.contextId) or type(value.ref)~='table' or value.ref.id~=group or
      value.ref.incarnation~=(redis.call('HGET',KEYS[1],'incarnation') or ARGV[6]) or
      value.ref.callId~=(redis.call('HGET',KEYS[1],'callId') or ARGV[4]) then return 'CORRUPT' end
   local identity=entry.leaseId..'|'..entry.member
   if result[identity] then return 'CORRUPT' end
   result[identity]={key=key,member=entry.member}
  end
  return nil
 end
 local err=entries(before,old); if err then return nil,err end
 err=entries(after,new); if err then return nil,err end
 for identity,entry in pairs(old) do
  if redis.call('ZSCORE',entry.key,entry.member)~='0' then return nil,'CORRUPT' end
  if not new[identity] then
   if not redis.acl_check_cmd('ZREM',entry.key,entry.member) then return nil,'AUTHORITY' end
   table.insert(changes,{key=entry.key,member=entry.member,remove=true}); deltas[entry.key]=(deltas[entry.key] or 0)-1
  end
 end
 for identity,entry in pairs(new) do
  if not old[identity] then
   if redis.call('ZSCORE',entry.key,entry.member) then return nil,'CORRUPT' end
   if not redis.acl_check_cmd('ZADD',entry.key,0,entry.member) then return nil,'AUTHORITY' end
   table.insert(changes,{key=entry.key,member=entry.member}); deltas[entry.key]=(deltas[entry.key] or 0)+1
  end
 end
 for key,delta in pairs(deltas) do if redis.call('ZCARD',key)+delta>4096 then return nil,'CAPACITY' end end
 return changes,nil
end
local function applyApplicationContexts(plan)
 for _,entry in ipairs(plan) do
  if entry.remove then redis.call('ZREM',entry.key,entry.member) else redis.call('ZADD',entry.key,0,entry.member) end
 end
end
`
