// Package rediscore publishes selected binary records. It is the storage
// primitive for the replacement described in docs/redis-core.md, not a serving
// runtime or a compatibility reader for the checkpoint adapter.
package rediscore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

var (
	ErrAbsent    = errors.New("native context absent; historical outcome unknown")
	ErrConflict  = errors.New("native conditional publication conflict")
	ErrAuthority = errors.New("native storage profile or authority changed")
	ErrCorrupt   = errors.New("invalid native storage record")
	ErrCapacity  = errors.New("native storage capacity")
	ErrExpired   = errors.New("native publication deadline elapsed")
	ErrUnknown   = errors.New("native publication outcome unknown")
)

const (
	maxFields        = 128
	maxRecordBytes   = 64 << 20
	maxOperationSize = 64 << 20
	maxExactInteger  = 1<<53 - 1
)

// Config binds a store to an already provisioned private binary profile. New
// never creates a profile, migrates records or changes Redis permissions.
// Profile is the owning private schema's deterministic binary profile tuple.
// The client must have automatic command retry disabled (MaxRetries: -1 when
// constructing go-redis options). A lost mutation reply must remain UNKNOWN.
// RecordBudget bounds the selected record fields plus completion reservations;
// NamespaceMaxContexts and NamespaceMaxBytes bound logical retained context,
// record, reservation and index bytes. Full native admission accounting and
// profile binding are required before a serving adapter can use this store.
type Config struct {
	Root                 string
	Profile              []byte
	RecordBudget         int64
	PrimaryRunID         string
	NamespaceMaxContexts int64
	NamespaceMaxBytes    int64
	InstanceAdmission    bool
}

type Store struct {
	client            *redis.Client
	root              string
	profile           []byte
	maxBytes          int64
	primary           string
	namespaceContexts int64
	namespaceBytes    int64
	instanceAdmission bool
}

func New(client *redis.Client, cfg Config) (*Store, error) {
	if cfg.NamespaceMaxContexts == 0 {
		cfg.NamespaceMaxContexts = DefaultNamespaceMaxContexts
	}
	if cfg.NamespaceMaxBytes == 0 {
		cfg.NamespaceMaxBytes = DefaultNamespaceMaxBytes
	}
	if client == nil || client.Options().MaxRetries != 0 || len(cfg.Root) == 0 || len(cfg.Root) > 512 || !strings.HasSuffix(cfg.Root, ":") ||
		strings.ContainsAny(cfg.Root, "\x00\r\n*?[]") || len(cfg.Profile) == 0 || len(cfg.Profile) > 4096 ||
		cfg.RecordBudget <= 0 || cfg.RecordBudget > maxExactInteger || cfg.NamespaceMaxContexts < 1 || cfg.NamespaceMaxContexts > maxExactInteger || cfg.NamespaceMaxBytes < 1 || cfg.NamespaceMaxBytes > maxExactInteger || !validPrimary(cfg.PrimaryRunID) {
		return nil, ErrAuthority
	}
	return &Store{client: client, root: cfg.Root, profile: bytes.Clone(cfg.Profile), maxBytes: cfg.RecordBudget, primary: cfg.PrimaryRunID, namespaceContexts: cfg.NamespaceMaxContexts, namespaceBytes: cfg.NamespaceMaxBytes, instanceAdmission: cfg.InstanceAdmission}, nil
}

// Identity is a logical context and application authority, never a socket or
// timer-executor identity. Incarnations and context IDs may not be reused.
type Identity struct {
	Context, Incarnation, Owner, Term string
}

func reference(s string) bool {
	if len(s) == 0 || len(s) > 192 {
		return false
	}
	for _, c := range s {
		if c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-' || c == '_' {
			continue
		}
		return false
	}
	return true
}

func (id Identity) valid() bool {
	return reference(id.Context) && reference(id.Incarnation) && ((id.Owner == "" && id.Term == "") || (reference(id.Owner) && validApplicationTerm(id.Term)))
}

func recordField(field string) bool {
	return strings.HasPrefix(field, "r:") && reference(strings.TrimPrefix(field, "r:"))
}

func (s *Store) profileKey() string          { return s.root + "core:profile" }
func (s *Store) contextKey(id string) string { return s.root + "core:context:" + id }

// View owns only the selected raw private-protobuf records. Missing selected
// fields are nil; a present zero-length record is non-nil. No SIP or JSON codec
// runs at this boundary. The engine validates decoded native records.
type View struct {
	Revision              uint64
	RecordBytes, Reserved int64
	Timer                 TimerAssignment
	Records               map[string][]byte
}

func (s *Store) Read(ctx context.Context, id Identity, fields ...string) (*View, error) {
	return s.read(ctx, id, nil, fields...)
}

// ReadApplication captures selected records under an atomically checked live
// application witness. Bound contexts require their exact owner; fresh unbound
// decision records admit a live deciding peer before the engine's policy checks.
func (s *Store) ReadApplication(ctx context.Context, id Identity, lease ApplicationLeaseRef, fields ...string) (*View, error) {
	if !lease.valid() || id.Owner != "" && (id.Owner != lease.Application || id.Term != lease.Term) {
		return nil, ErrAuthority
	}
	return s.read(ctx, id, &lease, fields...)
}

// ReadApplicationWitness fences the exact old identity while validating a
// separately live recipient. The native caller proves the pending operation.
func (s *Store) ReadApplicationWitness(ctx context.Context, id Identity, lease ApplicationLeaseRef, fields ...string) (*View, error) {
	if !lease.valid() {
		return nil, ErrAuthority
	}
	result, err := s.readSelected(ctx, id, &lease, false, true, fields...)
	if err != nil {
		return nil, err
	}
	return result.View, nil
}

type ObservedContext struct {
	Identity Identity
	View     *View
}

// ObserveContext selects bounded fields under the exact incarnation and returns
// its current owner. It grants no application or physical output authority.
func (s *Store) ObserveContext(ctx context.Context, contextID, incarnation string, fields ...string) (*ObservedContext, error) {
	return s.readSelected(ctx, Identity{Context: contextID, Incarnation: incarnation}, nil, true, false, fields...)
}

// ObserveApplicationContext captures the actual owner and selected records in
// one read. The same script checks the live application witness and permits
// only that owner or an unbound pending decision; engine policy distinguishes
// a fresh pending decision from a released, completed application context.
func (s *Store) ObserveApplicationContext(ctx context.Context, contextID, incarnation string, lease ApplicationLeaseRef, fields ...string) (*ObservedContext, error) {
	if !lease.valid() {
		return nil, ErrAuthority
	}
	return s.readSelected(ctx, Identity{Context: contextID, Incarnation: incarnation}, &lease, true, false, fields...)
}

// ObserveApplicationWitness returns the actual old identity and selected fields
// under a separately live recipient. Native policy must prove that recipient is
// authorized for the selected pending operation before disclosing a result.
func (s *Store) ObserveApplicationWitness(ctx context.Context, contextID, incarnation string, lease ApplicationLeaseRef, fields ...string) (*ObservedContext, error) {
	if !lease.valid() {
		return nil, ErrAuthority
	}
	return s.readSelected(ctx, Identity{Context: contextID, Incarnation: incarnation}, &lease, true, true, fields...)
}
func (s *Store) read(ctx context.Context, id Identity, application *ApplicationLeaseRef, fields ...string) (*View, error) {
	result, err := s.readSelected(ctx, id, application, false, false, fields...)
	if err != nil {
		return nil, err
	}
	return result.View, nil
}
func (s *Store) readSelected(ctx context.Context, id Identity, application *ApplicationLeaseRef, observe, witness bool, fields ...string) (*ObservedContext, error) {
	if !id.valid() || len(fields) > maxFields {
		return nil, ErrConflict
	}
	args := []interface{}{s.profile, id.Incarnation, id.Owner, id.Term}
	seen := make(map[string]bool, len(fields))
	for _, field := range fields {
		if !recordField(field) || seen[field] {
			return nil, ErrConflict
		}
		seen[field] = true
		args = append(args, field)
	}
	app, term, attachment := "", "", ""
	if application != nil {
		app, term, attachment = application.Application, application.Term, application.Attachment
	}
	mode := "exact"
	if observe && witness {
		mode = "observe-witness"
	} else if observe {
		mode = "observe"
	} else if witness {
		mode = "witness"
	}
	args = append(args, mode, app, term, attachment, s.primary)
	result, err := readScript.Run(ctx, s.client, []string{s.profileKey(), s.contextKey(id.Context)}, args...).Slice()
	if err != nil {
		return nil, err
	}
	if err = status(result); err != nil {
		return nil, err
	}
	if len(result) != 9+len(fields) {
		return nil, ErrCorrupt
	}
	revision, ok := result[1].(string)
	if !ok {
		return nil, ErrCorrupt
	}
	view := &View{Records: make(map[string][]byte, len(fields))}
	view.Revision, err = strconv.ParseUint(revision, 10, 64)
	if err != nil {
		return nil, ErrCorrupt
	}
	view.RecordBytes, err = responseInt(result[2])
	if err != nil {
		return nil, err
	}
	view.Reserved, err = responseInt(result[3])
	if err != nil {
		return nil, err
	}
	worker, wok := result[4].(string)
	epoch, eok := result[5].(string)
	term, tok := result[6].(string)
	view.Timer = TimerAssignment{Worker: worker, Epoch: epoch, Term: term}
	if !wok || !eok || !tok || !view.Timer.empty() && !view.Timer.valid() {
		return nil, ErrCorrupt
	}
	var selectedBytes int64
	for i, field := range fields {
		selectedBytes += int64(len(field))
		if result[7+i] == nil {
			view.Records[field] = nil
			continue
		}
		value, ok := result[7+i].(string)
		if !ok || len(value) > maxRecordBytes {
			return nil, ErrCorrupt
		}
		selectedBytes += int64(len(value))
		if selectedBytes > maxOperationSize {
			return nil, ErrCapacity
		}
		view.Records[field] = []byte(value)
	}
	owner, ok1 := result[7+len(fields)].(string)
	leaseTerm, ok2 := result[8+len(fields)].(string)
	current := Identity{Context: id.Context, Incarnation: id.Incarnation, Owner: owner, Term: leaseTerm}
	if !ok1 || !ok2 || !current.valid() || (!observe && current != id) {
		return nil, ErrCorrupt
	}
	return &ObservedContext{Identity: current, View: view}, nil
}

// Change touches exactly one private protobuf field. Delete is distinct from
// setting an empty value. One transition cannot touch a field twice.
type Change struct {
	Field  string
	Value  []byte
	Delete bool
}

// Publication is an atomic selected-record publication. Create requires revision
// zero and a fresh non-reused identity. Later revisions are compared exactly as
// decimal strings, including values beyond Lua's integer precision.
// Reserved is the total remaining completion reservation, not encoded JSON size.
// A later CAS refusal never disproves an earlier UNKNOWN operation; inspect its
// exact retained result identity rather than substituting current context state.
type Publication struct {
	Identity Identity
	Revision uint64
	Create   bool
	// Terminal is explicit native proof that all duties end in this publication.
	// Its final records, indexes and remaining credit must be empty.
	Terminal  bool
	Deadline  time.Time
	Reserved  int64
	Admission Admission
	// CreditDelta admits or closes pooled native completion credit. The caller
	// fences each obligation and independently funds new work before any compound
	// publication spends credit for an already admitted completion.
	CreditDelta int64
	// SpendReservation consumes actual positive record/index/envelope growth
	// from previously admitted credit, calculated by Lua. Reserved must be zero.
	SpendReservation   bool
	Changes            []Change
	NewOutputs         []OutputIntent
	Selectors          []SelectorChange
	Timers             *TimerMutation
	Application        *ApplicationLeaseRef
	BindApplication    bool
	ReleaseApplication bool
	// WitnessApplication validates a live recipient without transferring the
	// exact old owner or its delivery authority. Native policy proves its use.
	WitnessApplication bool
	// ResultHint wakes local waiters only after this publication sets that result.
	ResultHint *ResultHint
	// UDPHint is an optional authority-free signal for the exact changed procedure.
	UDPHint *UDPHint
}

func (s *Store) Commit(ctx context.Context, p Publication) (uint64, error) {
	if !p.Identity.valid() || p.Create != (p.Revision == 0) || p.Revision == ^uint64(0) ||
		p.Deadline.IsZero() || p.Deadline.UnixMilli() <= 0 || p.Deadline.UnixMilli() > maxExactInteger ||
		p.Reserved < 0 || p.Reserved > s.maxBytes || p.CreditDelta < -s.maxBytes || p.CreditDelta > s.maxBytes || p.CreditDelta != 0 && p.Reserved != 0 || len(p.Changes) > maxFields || len(p.NewOutputs) > maxOutputs || len(p.Selectors) > maxFields {
		return 0, ErrConflict
	}
	if p.Timers != nil && !p.Timers.valid() {
		return 0, ErrConflict
	}
	admission := p.Admission
	if !s.instanceAdmission {
		admission = Admission{}
	} else if !admission.valid(p.Create) || p.Terminal && admission != (Admission{}) {
		return 0, ErrConflict
	}
	if p.SpendReservation && (p.Create || p.Reserved != 0) {
		return 0, ErrConflict
	}
	if p.Terminal && (p.Create || p.SpendReservation || p.BindApplication || p.ReleaseApplication || len(p.NewOutputs) != 0 || p.Reserved != 0) {
		return 0, ErrConflict
	}
	if p.BindApplication && p.ReleaseApplication || p.Create && (p.BindApplication || p.ReleaseApplication) {
		return 0, ErrConflict
	}
	if p.WitnessApplication && (p.Application == nil || p.Create || p.Terminal || p.BindApplication || p.ReleaseApplication) {
		return 0, ErrConflict
	}
	if p.Application != nil && !p.Application.valid() {
		return 0, ErrConflict
	}
	if p.Create && p.Identity.Owner != "" && p.Application == nil {
		return 0, ErrConflict
	}
	if p.BindApplication && p.Application == nil || p.ReleaseApplication && p.Application != nil {
		return 0, ErrConflict
	}
	if p.Application != nil && !p.BindApplication && !p.WitnessApplication && p.Identity.Owner != "" && (p.Identity.Owner != p.Application.Application || p.Identity.Term != p.Application.Term) {
		return 0, ErrConflict
	}
	if p.BindApplication && p.Identity.Owner == p.Application.Application && p.Identity.Term == p.Application.Term {
		return 0, ErrConflict
	}
	create := "0"
	if p.Create {
		create = "1"
	}
	args := []interface{}{s.profile, p.Identity.Incarnation, p.Identity.Owner, p.Identity.Term,
		strconv.FormatUint(p.Revision, 10), create, p.Deadline.UnixMilli(), p.Reserved, s.maxBytes, len(p.Changes)}
	seen := make(map[string]bool, len(p.Changes))
	var size int64
	for _, change := range p.Changes {
		if p.WitnessApplication && change.Field == "r:delivery-tag" {
			return 0, ErrConflict
		}
		if !recordField(change.Field) || seen[change.Field] || len(change.Value) > maxRecordBytes || change.Delete && len(change.Value) != 0 {
			return 0, ErrConflict
		}
		seen[change.Field] = true
		size += int64(len(change.Field) + len(change.Value))
		if size > maxOperationSize {
			return 0, ErrCapacity
		}
		action := "set"
		if change.Delete {
			action = "delete"
		}
		args = append(args, change.Field, action, change.Value)
	}
	keys := []string{s.profileKey(), s.contextKey(p.Identity.Context)}
	args = append(args, p.Identity.Context, len(p.NewOutputs))
	outputIDs := make(map[string]bool, len(p.NewOutputs))
	for _, out := range p.NewOutputs {
		if !out.valid() || outputIDs[out.ID] {
			return 0, ErrConflict
		}
		outputIDs[out.ID] = true
		keys = append(keys, s.workerReadyKey(out.Worker))
		class, owner, term, attachment, tag := "N", "", "", "", []byte{}
		if out.Application != nil {
			class = "A"
			owner = out.Application.Application
			term = out.Application.Term
			attachment = out.Application.Attachment
			tag = out.DeliveryTag[:]
		}
		args = append(args, out.ID, out.Attempt, out.Worker, out.Epoch, out.Connection, out.Deadline.UnixMilli(), out.MessageField, out.WireHash[:], out.MetadataField, out.ProofHash[:], class, owner, term, tag, strings.Join(out.Dependencies, "\x00"), attachment)
	}
	if p.Timers == nil {
		args = append(args, "0")
	} else {
		timer := p.Timers
		keys = append(keys, s.contextDeadlineKey(p.Identity.Context), s.workerKey(timer.Expected.Worker), s.workerTimerKey(timer.Expected.Worker),
			s.workerKey(timer.Target.Worker), s.workerTimerKey(timer.Target.Worker), s.root+"core:wake:"+timer.Target.Worker)
		requireLease := "0"
		if timer.RequireLease {
			requireLease = "1"
		}
		args = append(args, "1", timer.Expected.Worker, timer.Expected.Epoch, timer.Expected.Term,
			timer.Target.Worker, timer.Target.Epoch, timer.Target.Term, requireLease, len(timer.Changes))
		for _, change := range timer.Changes {
			if change.Delete {
				args = append(args, change.Member, "delete", "")
			} else {
				args = append(args, change.Member, "set", change.Due.UnixMilli())
			}
		}
	}
	args = append(args, len(p.Selectors))
	selectorSeen := make(map[Selector]bool, len(p.Selectors))
	for _, change := range p.Selectors {
		if !change.Selector.valid() || !recordField(change.Record) || selectorSeen[change.Selector] {
			return 0, ErrConflict
		}
		selectorSeen[change.Selector] = true
		action := "set"
		if change.Delete {
			action = "delete"
		}
		args = append(args, change.Selector.field(), change.Record, action)
	}
	keys = append(keys, s.selectorKey())
	mode, targetOwner, targetTerm, targetAttachment := "none", "", "", ""
	if p.Application != nil {
		mode = "guard"
		targetOwner, targetTerm, targetAttachment = p.Application.Application, p.Application.Term, p.Application.Attachment
	}
	if p.BindApplication {
		mode = "bind"
	}
	if p.WitnessApplication {
		mode = "witness"
	}
	if p.ReleaseApplication {
		mode = "release"
	}
	keys = append(keys, s.capacityKey())
	spend := "0"
	if p.SpendReservation {
		spend = "1"
	}
	terminal := "0"
	if p.Terminal {
		terminal = "1"
	}
	var hint []byte
	resultField := ""
	if p.ResultHint != nil {
		if p.ResultHint.Context != p.Identity.Context || p.ResultHint.Incarnation != p.Identity.Incarnation {
			return 0, ErrConflict
		}
		var err error
		hint, err = encodeResultHint(*p.ResultHint)
		if err != nil {
			return 0, err
		}
		resultField = "r:result-" + strconv.FormatUint(p.ResultHint.Sequence, 10)
		if !seen[resultField] {
			return 0, ErrConflict
		}
		for _, change := range p.Changes {
			if change.Field == resultField && change.Delete {
				return 0, ErrConflict
			}
		}
	}
	udpWorker := ""
	var udpHint []byte
	if p.UDPHint != nil {
		if p.UDPHint.Context != p.Identity.Context || p.UDPHint.Incarnation != p.Identity.Incarnation || p.UDPHint.Worker == "results" || !seen[p.UDPHint.Procedure] {
			return 0, ErrConflict
		}
		for _, change := range p.Changes {
			if change.Field == p.UDPHint.Procedure && change.Delete {
				return 0, ErrConflict
			}
		}
		var err error
		udpHint, err = encodeUDPHint(*p.UDPHint)
		if err != nil {
			return 0, err
		}
		udpWorker = p.UDPHint.Worker
	}
	args = append(args, targetAttachment, admission.Instance, string(admission.Class), admission.Source, mode, targetOwner, targetTerm, spend, terminal, p.CreditDelta, s.namespaceContexts, s.namespaceBytes, resultField, hint, udpWorker, udpHint, s.primary)
	result, err := commitScript.Run(ctx, s.client, keys, args...).Slice()
	if err != nil {
		return 0, fmt.Errorf("%w: %w", ErrUnknown, err)
	}
	if err = commitStatus(result); err != nil {
		return 0, err
	}
	if len(result) != 2 {
		return 0, errors.Join(ErrUnknown, ErrCorrupt)
	}
	revision, ok := result[1].(string)
	if !ok {
		return 0, errors.Join(ErrUnknown, ErrCorrupt)
	}
	next, err := strconv.ParseUint(revision, 10, 64)
	if err != nil || next != p.Revision+1 {
		return 0, errors.Join(ErrUnknown, ErrCorrupt)
	}
	return next, nil
}

func responseInt(v interface{}) (int64, error) {
	s, ok := v.(string)
	if !ok {
		return 0, ErrCorrupt
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil || n < 0 || n > maxExactInteger {
		return 0, ErrCorrupt
	}
	return n, nil
}

// Only Commit consumes its optional fixed authority phase. Other operations keep
// their existing status interpretation, and unrecognized data is never echoed.
func commitStatus(result []interface{}) error {
	err := status(result)
	if !errors.Is(err, ErrAuthority) || len(result) != 2 {
		return err
	}
	reason, ok := result[1].(string)
	if !ok || len(reason) > 32 {
		return err
	}
	switch reason {
	case "primary", "profile", "identity", "record-acl", "output-acl", "reference-acl", "dependency-acl",
		"timer", "selector", "application-term", "application-owner", "application-attachment", "output-owner", "binding",
		"terminal-acl", "result-wake-acl", "udp-wake-acl", "namespace", "context-acl":
		return fmt.Errorf("%w: commit %s", err, reason)
	}
	return err
}

func status(result []interface{}) error {
	if len(result) == 0 {
		return errors.Join(ErrUnknown, ErrCorrupt)
	}
	s, ok := result[0].(string)
	if !ok {
		return errors.Join(ErrUnknown, ErrCorrupt)
	}
	switch s {
	case "OK":
		return nil
	case "ABSENT":
		return ErrAbsent
	case "CONFLICT":
		return ErrConflict
	case "AUTHORITY":
		return ErrAuthority
	case "CAPACITY":
		return ErrCapacity
	case "PARKED":
		return ErrParked
	case "EXPIRED":
		return ErrExpired
	case "CORRUPT":
		return ErrCorrupt
	default:
		return errors.Join(ErrUnknown, ErrCorrupt)
	}
}

// Scripts return RESP scalars and raw binary values. Keep every refusal before
// the first write; a Redis Lua error does not roll back an earlier mutation.
const commonScript = primaryScript + `
local function fail(s) return {s} end
local function kind(k,t)
 local v=redis.call('TYPE',k).ok; return v=='none' or v==t
end
local function decimal(s)
 return type(s)=='string' and (#s==1 and s=='0' or #s>0 and #s<=20 and string.match(s,'^[1-9][0-9]*$') and (#s<20 or s<='18446744073709551615'))
end
local function number(s)
 if not decimal(s) then return nil end
 local n=tonumber(s); if not n or n>9007199254740991 then return nil end; return n
end
local function increment(s)
 local out=''; local carry=1
 for i=#s,1,-1 do local n=string.byte(s,i)-48+carry; if n==10 then n=0; carry=1 else carry=0 end; out=string.char(48+n)..out end
 if carry==1 then out='1'..out end; return out
end
local function guard()
 if not primary(ARGV[#ARGV]) then return 'AUTHORITY','primary' end
 if not kind(KEYS[1],'string') or redis.call('GET',KEYS[1])~=ARGV[1] then return 'AUTHORITY','profile' end
 if not kind(KEYS[2],'hash') then return 'CORRUPT' end
 return nil
end
local function identity()
 local v=redis.call('HMGET',KEYS[2],'incarnation','owner','term','revision','bytes','reserved')
 if not v[1] then return nil,'ABSENT' end
 if v[1]~=ARGV[2] or v[2]~=ARGV[3] or v[3]~=ARGV[4] then return nil,'AUTHORITY' end
 if not decimal(v[4]) or v[4]=='0' or not number(v[5]) or not number(v[6]) then return nil,'CORRUPT' end
 return v,nil
end
`

var readScript = redis.NewScript(commonScript + applicationGuardScript + `
local problem=guard(); if problem then return fail(problem) end
if #ARGV<9 or #ARGV>137 then return fail('CORRUPT') end
local mode=ARGV[#ARGV-4];local v
if mode=='exact' or mode=='witness' then v,problem=identity();if problem then return fail(problem) end
elseif mode=='observe' or mode=='observe-witness' then
 v=redis.call('HMGET',KEYS[2],'incarnation','owner','term','revision','bytes','reserved')
 if not v[1] then return fail('ABSENT') end
 if v[1]~=ARGV[2] then return fail('AUTHORITY') end
 if type(v[2])~='string' or type(v[3])~='string' or (v[2]=='')~=(v[3]=='') or not decimal(v[4]) or v[4]=='0' or not number(v[5]) or not number(v[6]) then return fail('CORRUPT') end
else return fail('CORRUPT') end
local app,term,attachment=ARGV[#ARGV-3],ARGV[#ARGV-2],ARGV[#ARGV-1]
if (app=='')~=(term=='') or app=='' and attachment~='' then return fail('CORRUPT') end
if (mode=='witness' or mode=='observe-witness') and app=='' then return fail('CORRUPT') end
if app~='' then
 if mode~='witness' and mode~='observe-witness' and v[2]~='' and (v[2]~=app or v[3]~=term) then return fail('AUTHORITY') end
 local root=string.sub(KEYS[1],1,#KEYS[1]-#'core:profile');local clock=redis.call('TIME');local now=tonumber(clock[1])*1000+math.floor(tonumber(clock[2])/1000)
 problem=guardApplication(root,app,term,now);if problem then return fail(problem) end
 problem=guardApplicationAttachment(root,app,term,attachment,now);if problem then return fail(problem) end
end
local total=0; local seen={}
for i=5,#ARGV-5 do
 local field=ARGV[i]
 if #field<3 or #field>194 or not string.match(field,'^r:[A-Za-z0-9_-]+$') or seen[field] then return fail('CORRUPT') end
 seen[field]=true
 local size=redis.call('HSTRLEN',KEYS[2],field)
 if size>67108864 then return fail('CORRUPT') end
 total=total+#field+size; if total>67108864 then return fail('CAPACITY') end
end
local timer=redis.call('HMGET',KEYS[2],'timer:worker','timer:epoch','timer:term')
if (timer[1] or timer[2] or timer[3]) and (not timer[1] or not timer[2] or not timer[3]) then return fail('CORRUPT') end
local out={'OK',v[4],v[5],v[6],timer[1] or '',timer[2] or '',timer[3] or ''}
for i=5,#ARGV-5 do out[#out+1]=redis.call('HGET',KEYS[2],ARGV[i]) end
out[#out+1]=v[2];out[#out+1]=v[3]
return out
`)

var commitScript = redis.NewScript(commonScript + timerScript + namespaceScript + applicationGuardScript + applicationBindingScript + selectorScript + parkedScript + `
local function refuse(problem,reason)
 if problem=='AUTHORITY' then return {problem,reason} end
 return fail(problem)
end
local problem,reason=guard(); if problem then return refuse(problem,reason) end
local revision=ARGV[5]; local creating=ARGV[6]=='1'
if not decimal(revision) or revision=='18446744073709551615' or (creating and revision~='0') or (not creating and revision=='0') then return fail('CONFLICT') end
local v
if creating then
 if redis.call('EXISTS',KEYS[2])~=0 then return fail('CONFLICT') end
 v={ARGV[2],ARGV[3],ARGV[4],'0','0','0'}
else
 v,problem=identity(); if problem then return refuse(problem,'identity') end
 if v[4]~=revision then return fail('CONFLICT') end
end
local deadline=number(ARGV[7]); local reserved=number(ARGV[8]); local maximum=number(ARGV[9]); local count=number(ARGV[10])
if not deadline or not reserved or not maximum or not count or count>128 then return fail('CORRUPT') end
local offset=11+3*count; local context=ARGV[offset]; local outputCount=number(ARGV[offset+1]); local outputBase=offset+2
if not outputCount or outputCount>32 or not context or #context>192 or not string.match(context,'^[A-Za-z0-9_-]+$') then return fail('CORRUPT') end
local root=string.sub(KEYS[1],1,#KEYS[1]-#'core:profile')
if not creating and parked(root,context,ARGV[2]) then return fail('PARKED') end
local timerBase=outputBase+16*outputCount; local hasTimers=ARGV[timerBase]
if hasTimers~='0' and hasTimers~='1' then return fail('CORRUPT') end
local timerCount=hasTimers=='1' and number(ARGV[timerBase+8]) or 0
if not timerCount or timerCount>128 or (hasTimers=='1' and timerCount==0) or #KEYS~=4+outputCount+(hasTimers=='1' and 6 or 0) then return fail('CORRUPT') end
local selectorBase=timerBase+(hasTimers=='1' and 9+3*timerCount or 1);local selectorCount=number(ARGV[selectorBase]);if not selectorCount or selectorCount>128 or #ARGV~=selectorBase+3*selectorCount+17 then return fail('CORRUPT') end
local clock=redis.call('TIME'); local now=tonumber(clock[1])*1000+math.floor(tonumber(clock[2])/1000)
if deadline<=now then return fail('EXPIRED') end
local total=number(v[5]); local seen={}; local pending={}; local inputBytes=0
local records=creating and 0 or redis.call('HLEN',KEYS[2])-7-redis.call('HEXISTS',KEYS[2],'credit_limit')-redis.call('HEXISTS',KEYS[2],'admission')
local indexes=creating and 0 or number(redis.call('HGET',KEYS[2],'index_bytes'));if not indexes then return fail('CORRUPT') end
if records<0 or records>4096 then return fail('CORRUPT') end
for i=1,count do
 local pos=11+(i-1)*3; local field=ARGV[pos]; local action=ARGV[pos+1]; local value=ARGV[pos+2]
 if #field<3 or #field>194 or not string.match(field,'^r:[A-Za-z0-9_-]+$') or seen[field] or #value>67108864 or (action~='set' and action~='delete') or (action=='delete' and #value~=0) then return fail('CORRUPT') end
 inputBytes=inputBytes+#field+#value; if inputBytes>67108864 then return fail('CAPACITY') end
 seen[field]=true
 local old=redis.call('HGET',KEYS[2],field)
 local refs=redis.call('HGET',KEYS[2],'ref:'..field)
 if refs and (not number(refs) or number(refs)==0) then return fail('CORRUPT') end
 if refs and (action~='set' or value~=old) then return fail('CONFLICT') end
 local dependencies=redis.call('HGET',KEYS[2],'dref:'..field)
 if dependencies and (not number(dependencies) or number(dependencies)==0) then return fail('CORRUPT') end
 if dependencies and action=='delete' then return fail('CONFLICT') end
 if old then total=total-#field-#old; records=records-1 end
 if action=='set' then total=total+#field+#value; records=records+1; pending[field]=value else pending[field]=false end
 if action=='set' and not redis.acl_check_cmd('HSET',KEYS[2],field,value) or action=='delete' and not redis.acl_check_cmd('HDEL',KEYS[2],field) then return refuse('AUTHORITY','record-acl') end
end
local ids={}; local refs={}; local dependencyRefs={}; local writes={}
local function ref(s) return s and #s>0 and #s<=192 and string.match(s,'^[A-Za-z0-9_-]+$') end
for i=1,outputCount do
 local pos=outputBase+(i-1)*16; local id,attempt,worker,epoch,connection,due,field,hash,meta,proof,class,app,term,tag,dependencies,attachment=unpack(ARGV,pos,pos+15); local dueNum=number(due)
 if not ref(id) or not ref(attempt) or not ref(worker) or not ref(epoch) or not ref(connection) or not dueNum or dueNum<=now or not field or #field<3 or #field>194 or not string.match(field,'^r:[A-Za-z0-9_-]+$') or #hash~=32 or #proof~=32 or not meta or meta==field or #meta<3 or #meta>194 or not string.match(meta,'^r:[A-Za-z0-9_-]+$') or (class~='A' and class~='N') or (class=='A' and (not ref(app) or not ref(term) or #tag~=32)) or (class=='N' and (app~='' or term~='' or tag~='')) or ids[id] then return fail('CONFLICT') end
 ids[id]=true; if KEYS[2+i]~=root..'core:ready:'..worker or not kind(KEYS[2+i],'zset') then return fail('CORRUPT') end
 if attachment~='' and (class~='A' or not appRef(attachment)) then return fail('CONFLICT') end
 local value=pending[field]; if value==false then return fail('CONFLICT') elseif value==nil then value=redis.call('HGET',KEYS[2],field) end
 if not value or #value==0 then return fail('CONFLICT') end
 local prefix='o:'..id..':'
 if redis.call('HEXISTS',KEYS[2],prefix..'s')~=0 then return fail('CONFLICT') end
 local metadata=pending[meta];if metadata==false then return fail('CONFLICT') elseif metadata==nil then metadata=redis.call('HGET',KEYS[2],meta) end
 if not metadata or #metadata==0 or #metadata>65536 then return fail('CONFLICT') end
 if class=='A' then local delivery=pending['r:delivery-tag'];if delivery==false then return fail('CONFLICT') elseif delivery==nil then delivery=redis.call('HGET',KEYS[2],'r:delivery-tag') end;if not delivery or delivery~=tag then return fail('CONFLICT') end end
 local dependencyList={};local dependencySeen={}
 for dependent in string.gmatch(dependencies,'[^%z]+') do
  if #dependencyList>=3 or #dependent<3 or #dependent>194 or not string.match(dependent,'^r:[A-Za-z0-9_-]+$') or dependent==field or dependent==meta or dependencySeen[dependent] then return fail('CONFLICT') end
  dependencySeen[dependent]=true;dependencyList[#dependencyList+1]=dependent
  local value=pending[dependent];if value==false then return fail('CONFLICT') elseif value==nil then value=redis.call('HGET',KEYS[2],dependent) end;if value==nil or value==false then return fail('CONFLICT') end
  local key='dref:'..dependent;local prior=dependencyRefs[key]
  if prior==nil then local old=redis.call('HGET',KEYS[2],key);if old and (not number(old) or number(old)==0) then return fail('CORRUPT') end;prior=old and number(old) or 0 end
  if prior>=4096 then return fail('CAPACITY') end;dependencyRefs[key]=prior+1
 end
 if table.concat(dependencyList,'\0')~=dependencies then return fail('CONFLICT') end
 for _,selected in ipairs({field,meta}) do
  local rfield='ref:'..selected;local prior=refs[rfield]
  if prior==nil then local old=redis.call('HGET',KEYS[2],rfield);if old and (not number(old) or number(old)==0) then return fail('CORRUPT') end;prior=old and number(old) or 0 end
  if prior>=4096 then return fail('CAPACITY') end;refs[rfield]=prior+1
 end
 local parts={a=attempt,w=worker,e=epoch,c=connection,d=due,m=field,h=hash,s='P',k='0',n='0',x=meta,p=proof,q=class,u=app,t=term,g=tag,l=dependencies}
 if attachment~='' then parts.z=attachment end
 for suffix,val in pairs(parts) do local k=prefix..suffix; if redis.call('HEXISTS',KEYS[2],k)~=0 then return fail('CORRUPT') end; total=total+#k+#val; records=records+1 end
 local member=context..'/'..id; local wake=root..'core:wake:'..worker
 indexes=indexes+#member+8
 if redis.call('ZSCORE',KEYS[2+i],member) then return fail('CORRUPT') end
 if not redis.acl_check_cmd('ZADD',KEYS[2+i],due,member) or not redis.acl_check_cmd('PUBLISH',wake,'1') then return refuse('AUTHORITY','output-acl') end
 writes[i]={key=KEYS[2+i],member=member,wake=wake,due=due,prefix=prefix,parts=parts,class=class,app=app,term=term,attachment=attachment}
end
for field,n in pairs(refs) do
 local old=redis.call('HGET',KEYS[2],field); local value=string.format('%.0f',n)
 if old then total=total-#field-#old; records=records-1 end
 total=total+#field+#value; records=records+1
 if not redis.acl_check_cmd('HSET',KEYS[2],field,value) then return refuse('AUTHORITY','reference-acl') end
end
for field,n in pairs(dependencyRefs) do
 local old=redis.call('HGET',KEYS[2],field);local value=string.format('%.0f',n)
 if old then total=total-#field-#old;records=records-1 end
 total=total+#field+#value;records=records+1
 if not redis.acl_check_cmd('HSET',KEYS[2],field,value) then return refuse('AUTHORITY','dependency-acl') end
end
local timerPlan=nil
if hasTimers=='1' then
 local expected={ARGV[timerBase+1],ARGV[timerBase+2],ARGV[timerBase+3]}
 local target={ARGV[timerBase+4],ARGV[timerBase+5],ARGV[timerBase+6]}
 local requireLease=ARGV[timerBase+7]
 if requireLease~='0' and requireLease~='1' then return fail('CORRUPT') end
 local changes={}
 for i=1,timerCount do local pos=timerBase+9+(i-1)*3;changes[i]={ARGV[pos],ARGV[pos+1],ARGV[pos+2]} end
 local base=3+outputCount
 timerPlan,problem=timerPrepare(root,KEYS[2],KEYS[base],KEYS[base+1],KEYS[base+2],KEYS[base+3],KEYS[base+4],KEYS[base+5],context,expected,target,changes,requireLease=='1',now)
 if problem then return refuse(problem,'timer') end
 total=total+timerPlan.byteDelta; records=records+timerPlan.fieldDelta; indexes=indexes+timerPlan.indexDelta
end
if KEYS[#KEYS-1]~=root..'core:selectors' then return fail('CORRUPT') end
local selectorChanges={};for i=1,selectorCount do local pos=selectorBase+1+(i-1)*3;selectorChanges[i]={ARGV[pos],ARGV[pos+1],ARGV[pos+2]} end
local selectors;selectors,problem=selectorPrepare(KEYS[#KEYS-1],context,ARGV[2],pending,selectorChanges);if problem then return refuse(problem,'selector') end
total=total+selectors.byteDelta;records=records+selectors.fieldDelta;indexes=indexes+selectors.indexDelta
local targetAttachment=ARGV[#ARGV-16]
local bindingMode=ARGV[#ARGV-12];local targetOwner=ARGV[#ARGV-11];local targetTerm=ARGV[#ARGV-10];local spend=ARGV[#ARGV-9];local terminal=ARGV[#ARGV-8]
local creditText=ARGV[#ARGV-7];local creditDelta=tonumber(creditText)
if not creditDelta or creditDelta~=math.floor(creditDelta) or math.abs(creditDelta)>9007199254740991 or not string.match(creditText,'^-?[0-9]+$') then return fail('CORRUPT') end
if (spend~='0' and spend~='1') or (terminal~='0' and terminal~='1') then return fail('CORRUPT') end
terminal=terminal=='1'
if terminal and (creating or spend=='1' or bindingMode=='bind' or bindingMode=='release' or outputCount~=0 or reserved~=0) then return fail('CONFLICT') end
if bindingMode~='none' and bindingMode~='guard' and bindingMode~='bind' and bindingMode~='release' and bindingMode~='witness' then return fail('CORRUPT') end
local oldOwner,oldTerm=ARGV[3],ARGV[4];local nextOwner,nextTerm=oldOwner,oldTerm
if creating then oldOwner,oldTerm='','' end
if bindingMode=='guard' or bindingMode=='bind' or bindingMode=='witness' then
 if bindingMode=='witness' and (creating or terminal or seen['r:delivery-tag']) then return fail('CONFLICT') end
 local appProblem,appReason=guardApplication(root,targetOwner,targetTerm,now);if appProblem then return refuse(appProblem,appReason) end
 appProblem,appReason=guardApplicationAttachment(root,targetOwner,targetTerm,targetAttachment,now);if appProblem then return refuse(appProblem,appReason) end
 if bindingMode=='guard' and ARGV[3]~='' and (targetOwner~=ARGV[3] or targetTerm~=ARGV[4]) then return fail('CONFLICT') end
 if bindingMode=='bind' then if creating or (oldOwner==targetOwner and oldTerm==targetTerm) then return fail('CONFLICT') end;nextOwner,nextTerm=targetOwner,targetTerm end
elseif bindingMode=='release' then if creating then return fail('CONFLICT') end;nextOwner,nextTerm='',''
elseif creating and (ARGV[3]~='' or ARGV[4]~='') then return fail('CONFLICT') end
if terminal then nextOwner,nextTerm='','' end
for _,w in ipairs(writes) do if w.class=='A' then
 if w.app~=nextOwner or w.term~=nextTerm then return refuse('AUTHORITY','output-owner') end
 if targetAttachment~='' and not (w.app==targetOwner and w.term==targetTerm and w.attachment==targetAttachment) then return refuse('AUTHORITY','application-attachment') end
 local appProblem,appReason=guardApplication(root,w.app,w.term,now);if appProblem then return refuse(appProblem,appReason) end
 appProblem,appReason=guardApplicationAttachment(root,w.app,w.term,w.attachment,now);if appProblem then return refuse(appProblem,appReason) end
end end
local binding;binding,problem=bindingPrepare(root,context,oldOwner,oldTerm,nextOwner,nextTerm,now,bindingMode=='release');if problem then return refuse(problem,'binding') end
indexes=indexes+binding.indexDelta
if total<0 then return fail('CORRUPT') end
if records>4096 or total>maximum or reserved>maximum-total then return fail('CAPACITY') end
local next=increment(revision)
if indexes<0 or indexes>9007199254740991 or KEYS[#KEYS]~=root..'core:capacity' then return fail('CORRUPT') end
if terminal then
 if records~=0 or total~=0 or indexes~=0 then return fail('CONFLICT') end
 local deadlines=root..'core:deadlines:'..context;if not kind(deadlines,'zset') then return fail('CORRUPT') end
 if timerPlan then if timerPlan.next then return fail('CONFLICT') end elseif redis.call('ZCARD',deadlines)~=0 then return fail('CONFLICT') end
 if not redis.acl_check_cmd('DEL',KEYS[2]) then return refuse('AUTHORITY','terminal-acl') end
end
local resultField=ARGV[#ARGV-4];local resultHint=ARGV[#ARGV-3];local resultChannel=root..'core:wake:results'
if (resultField=='')~=(resultHint=='') or (resultField~='' and (not pending[resultField] or #resultHint>395)) then return fail('CORRUPT') end
if resultField~='' and not redis.acl_check_cmd('PUBLISH',resultChannel,resultHint) then return refuse('AUTHORITY','result-wake-acl') end
local udpWorker=ARGV[#ARGV-2];local udpHint=ARGV[#ARGV-1];local udpChannel=root..'core:wake:'..udpWorker
if (udpWorker=='')~=(udpHint=='') or (udpWorker~='' and (not ref(udpWorker) or #udpWorker>190 or udpWorker=='results' or #udpHint>585)) then return fail('CORRUPT') end
if udpWorker~='' and not redis.acl_check_cmd('PUBLISH',udpChannel,udpHint) then return refuse('AUTHORITY','udp-wake-acl') end
local ns;ns,problem=nsPlan(KEYS[#KEYS],KEYS[2],creating,terminal,ARGV[2],nextOwner,nextTerm,next,total,reserved,indexes,number(ARGV[#ARGV-6]),number(ARGV[#ARGV-5]),spend=='1',creditDelta,false,maximum,ARGV[#ARGV-15],ARGV[#ARGV-14],ARGV[#ARGV-13]);if problem then return refuse(problem,'namespace') end
if total+number(ns.reserved)>maximum then return fail('CAPACITY') end
if not redis.acl_check_cmd('HSET',KEYS[2],'incarnation',ARGV[2],'owner',nextOwner,'term',nextTerm,'revision',next,'bytes',string.format('%.0f',total),'reserved',ns.reserved) then return refuse('AUTHORITY','context-acl') end
for i=1,count do local pos=11+(i-1)*3; if ARGV[pos+1]=='set' then redis.call('HSET',KEYS[2],ARGV[pos],ARGV[pos+2]) else redis.call('HDEL',KEYS[2],ARGV[pos]) end end
for field,n in pairs(refs) do redis.call('HSET',KEYS[2],field,string.format('%.0f',n)) end
for field,n in pairs(dependencyRefs) do redis.call('HSET',KEYS[2],field,string.format('%.0f',n)) end
for _,w in ipairs(writes) do for suffix,val in pairs(w.parts) do redis.call('HSET',KEYS[2],w.prefix..suffix,val) end; redis.call('ZADD',w.key,w.due,w.member) end
if not terminal then redis.call('HSET',KEYS[2],'incarnation',ARGV[2],'owner',nextOwner,'term',nextTerm,'revision',next,'bytes',string.format('%.0f',total),'reserved',ns.reserved) end
if timerPlan then timerApply(timerPlan) end
selectorApply(selectors)
bindingApply(binding)
if terminal then redis.call('DEL',KEYS[2]) end
nsApply(ns)
for _,w in ipairs(writes) do redis.call('PUBLISH',w.wake,'1') end
if resultField~='' then redis.call('PUBLISH',resultChannel,resultHint) end
if udpWorker~='' then redis.call('PUBLISH',udpChannel,udpHint) end
return {'OK',next}
`)
