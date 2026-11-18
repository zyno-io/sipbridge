package engine

import (
	"bytes"
	"context"
	"errors"
	"slices"
	"sort"
	"strconv"
	"strings"

	"github.com/emiago/sipgo/sip/portable"
	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/nativepb"
	"sipbridge/internal/rediscore"
	"sipbridge/internal/selectedmessage"
	"sipbridge/internal/state"
)

// NativeFixtureQuery selects synthetic evidence only. No query allocates or
// acquires authority. Call-ID scanning is deliberately an admin fixture path.
type NativeFixtureQuery struct {
	ContextID, Incarnation, CallID, LocalTag, RemoteTag string
	Application                                         *pb.ProcessRef
}

const nativeFixtureMaxBytes = 32 << 20

// InspectNativeFixtureRedis uses only read operations against an existing
// binary profile. In particular it must never use Connection.Refresh, which
// can initialize an empty namespace's profile.
func InspectNativeFixtureRedis(ctx context.Context, client *redis.Client, profile rediscore.ProfileConfig, query NativeFixtureQuery) (NativeFixtureProjection, error) {
	if localPhoneID(query.ContextID) {
		return NativeFixtureProjection{}, errors.New("local phone context requires accepting-owner public Inspect")
	}
	if client == nil || (query.ContextID == "") != (query.Incarnation == "") || (query.LocalTag == "") != (query.RemoteTag == "") || query.ContextID != "" && query.CallID != "" || query.LocalTag != "" && query.CallID == "" || query.ContextID == "" && query.CallID == "" && query.Application == nil {
		return NativeFixtureProjection{}, errors.New("exact native fixture query required")
	}
	if query.ContextID != "" && (!state.ValidReference(query.ContextID) || !state.ValidReference(query.Incarnation)) || len(query.CallID) > 4096 || len(query.LocalTag) > 4096 || len(query.RemoteTag) > 4096 {
		return NativeFixtureProjection{}, errNativeRecord
	}
	binding, err := rediscore.BindProfile(profile)
	if err != nil {
		return NativeFixtureProjection{}, err
	}
	observed, err := nativeFixtureRedis(ctx, client, profile.Root, binding.Value)
	if err != nil {
		return NativeFixtureProjection{}, err
	}
	store, err := rediscore.New(client, rediscore.Config{Root: profile.Root, Profile: binding.Value, PrimaryRunID: observed.PrimaryID, RecordBudget: profile.RecordBudget, NamespaceMaxContexts: profile.NamespaceMaxContexts, NamespaceMaxBytes: profile.NamespaceMaxBytes})
	if err != nil {
		return NativeFixtureProjection{}, err
	}
	provenance, err := nativeFixtureProvenance()
	if err != nil {
		return NativeFixtureProjection{}, err
	}
	provenance.ProfileSHA256 = fixtureHash(binding.Value)
	provenance.Root = profile.Root
	provenance.Authority = nativeFixtureJSON(binding.Authority)
	out := NativeFixtureProjection{Format: "sipbridge.fixture.native.v1", Provenance: provenance, Redis: observed, Contexts: []NativeFixtureContext{}}
	if query.Application != nil {
		app, err := nativeApplicationReference(profile.IdentityKey, query.Application)
		if err != nil {
			return out, err
		}
		projection := &NativeFixtureApplication{Process: nativeFixtureJSON(query.Application), Reference: app, State: "absent", CheckedAtMS: observed.CheckedAtMS}
		lease, e := store.ObserveApplication(ctx, app)
		switch {
		case e == nil:
			projection.State = "live"
			projection.Term = lease.Term
			projection.CheckedAtMS = lease.CheckedAtMS
			projection.ExpiresAtMS = lease.ExpiresAtMS
		case errors.Is(e, rediscore.ErrAbsent):
		case errors.Is(e, rediscore.ErrExpired):
			// Generic ErrExpired can also be a local post-I/O timing refusal. Read
			// actual metadata/TIME before calling this application term expired.
			if e = nativeFixtureExpiredApplication(ctx, client, profile.Root, binding.Value, observed.PrimaryID, projection); e != nil {
				return out, e
			}
		default:
			return out, e
		}
		out.Application = projection
	}
	var selected []*rediscore.ObservedContext
	switch {
	case query.ContextID != "":
		v, e := store.ObserveContext(ctx, query.ContextID, query.Incarnation, "r:context")
		if !errors.Is(e, rediscore.ErrAbsent) && e != nil {
			return out, e
		}
		if e == nil {
			selected = append(selected, v)
		}
	case query.LocalTag != "":
		a, b := query.LocalTag, query.RemoteTag
		if a > b {
			a, b = b, a
		}
		v, e := store.LookupSelector(ctx, nativeSelector(rediscore.SelectorDialog, query.CallID, a, b), "r:context")
		if !errors.Is(e, rediscore.ErrAbsent) && e != nil {
			return out, e
		}
		if e == nil {
			current, decodeErr := decodeNativeContext(v.View.Records["r:context"])
			if decodeErr != nil || !slices.Contains(current.Dialogs, v.Record) {
				return out, errNativeRecord
			}
			dialog, decodeErr := readPrivateDialogRecord(v.View.Records[v.Record])
			if decodeErr != nil {
				return out, decodeErr
			}
			local, remote := dialog.Native.LocalTag, dialog.Native.RemoteTag
			if local > remote {
				local, remote = remote, local
			}
			if dialog.Native.CallId != query.CallID || local != a || remote != b {
				return out, errNativeRecord
			}
			selected = append(selected, &rediscore.ObservedContext{Identity: v.Identity, View: v.View})
		}
	case query.CallID != "":
		var cursor uint64
		seen := make(map[string]bool)
		for {
			keys, next, e := client.Scan(ctx, cursor, profile.Root+"core:context:*", 128).Result()
			if e != nil {
				return out, e
			}
			for _, key := range keys {
				if seen[key] {
					continue
				}
				seen[key] = true
				if len(seen) > 4096 {
					return out, rediscore.ErrCapacity
				}
				// Bound server-side discovery before a raw metadata value crosses RESP.
				raw, e := client.Eval(ctx, `local n=redis.call('HSTRLEN',KEYS[1],'r:context');if n>tonumber(ARGV[1]) then return redis.error_reply('fixture record too large') end;return redis.call('HMGET',KEYS[1],'incarnation','r:context')`, []string{key}, nativeRecordLimit).Slice()
				if e != nil {
					return out, e
				}
				if len(raw) != 2 {
					return out, rediscore.ErrCorrupt
				}
				if raw[0] == nil || raw[1] == nil {
					continue
				}
				incarnation, ok := raw[0].(string)
				record, ok2 := raw[1].(string)
				if !ok || !ok2 {
					return out, rediscore.ErrCorrupt
				}
				c, e := decodeNativeContext([]byte(record))
				if e != nil {
					return out, e
				}
				if c.CallId != query.CallID {
					continue
				}
				id := strings.TrimPrefix(key, profile.Root+"core:context:")
				v, e := store.ObserveContext(ctx, id, incarnation, "r:context")
				if e != nil {
					return out, e
				}
				selected = append(selected, v)
				if len(selected) > 32 {
					return out, rediscore.ErrCapacity
				}
			}
			cursor = next
			if cursor == 0 {
				break
			}
		}
	}
	sort.Slice(selected, func(i, j int) bool { return selected[i].Identity.Context < selected[j].Identity.Context })
	remaining := nativeFixtureMaxBytes
	for _, v := range selected {
		c, e := decodeNativeContext(v.View.Records["r:context"])
		if e != nil {
			return out, e
		}
		if c.Context == nil || c.Context.State == nil || c.Context.Id != v.Identity.Context || c.Context.State.Incarnation != v.Identity.Incarnation || !proto.Equal(c.Context.State.Authority, binding.Authority) {
			return out, rediscore.ErrAuthority
		}
		if query.CallID != "" && c.CallId != query.CallID {
			return out, rediscore.ErrConflict
		}
		item, e := inspectNativeFixtureContext(ctx, store, v, c, &remaining)
		if e != nil {
			return out, e
		}
		out.Contexts = append(out.Contexts, item)
	}
	final, e := nativeFixtureRedis(ctx, client, profile.Root, binding.Value)
	if e != nil {
		return out, e
	}
	if final.PrimaryID != observed.PrimaryID {
		return out, rediscore.ErrAuthority
	}
	out.Redis = final
	return out, nil
}

func nativeFixtureRedis(ctx context.Context, client *redis.Client, root string, profile []byte) (NativeFixtureRedis, error) {
	// One read-only script binds TIME, actual primary identity/role and profile.
	values, err := client.Eval(ctx, `local p=redis.call('GET',KEYS[1]);if not p or p~=ARGV[1] then return redis.error_reply('fixture profile absent or incompatible') end;local server=redis.call('INFO','server');local role=redis.call('INFO','replication');local id=string.match(server,'\r\nrun_id:([a-f0-9]+)\r\n');if not id or not string.find(role,'\r\nrole:master\r\n',1,true) then return redis.error_reply('fixture primary required') end;local t=redis.call('TIME');return {id,t[1],t[2]}`, []string{root + "core:profile"}, profile).Slice()
	if err != nil {
		return NativeFixtureRedis{}, err
	}
	if len(values) != 3 {
		return NativeFixtureRedis{}, rediscore.ErrCorrupt
	}
	id, ok := values[0].(string)
	seconds, ok2 := values[1].(string)
	micros, ok3 := values[2].(string)
	sec, e := strconv.ParseInt(seconds, 10, 64)
	us, e2 := strconv.ParseInt(micros, 10, 64)
	if !ok || !ok2 || !ok3 || len(id) != 40 || e != nil || e2 != nil || sec < 0 || sec > 1<<53/1000 || us < 0 || us >= 1e6 {
		return NativeFixtureRedis{}, rediscore.ErrCorrupt
	}
	return NativeFixtureRedis{sec*1000 + us/1000, id, "master"}, nil
}

func inspectNativeFixtureContext(ctx context.Context, store *rediscore.Store, selected *rediscore.ObservedContext, current *nativepb.Context, remaining *int) (NativeFixtureContext, error) {
	id, revision, assignment := selected.Identity, selected.View.Revision, selected.View.Timer
	out := NativeFixtureContext{Identity: NativeFixtureIdentity{id.Context, id.Incarnation, id.Owner, id.Term}, Revision: revision, Assignment: NativeFixtureAssignment{assignment.Worker, assignment.Epoch, assignment.Term}, Flow: nativeFixtureJSON(current.Flow), Procedures: []NativeFixtureProcedure{}, Dialogs: []NativeFixtureDialog{}, Outputs: []NativeFixtureOutput{}, Events: []NativeFixtureEvent{}, Results: []NativeFixtureResult{}}
	records := map[string][]byte{"r:context": selected.View.Records["r:context"]}
	*remaining -= len(records["r:context"])
	if *remaining < 0 {
		return out, rediscore.ErrCapacity
	}
	read := func(fields []string) error {
		fields = slices.Clone(fields)
		slices.Sort(fields)
		fields = slices.Compact(fields)
		for len(fields) > 0 {
			n := min(128, len(fields))
			v, e := store.Read(ctx, id, fields[:n]...)
			if e != nil {
				return e
			}
			if v.Revision != revision || v.Timer != assignment {
				return rediscore.ErrConflict
			}
			for name, data := range v.Records {
				if _, ok := records[name]; ok {
					continue
				}
				*remaining -= len(name) + len(data)
				if *remaining < 0 {
					return rediscore.ErrCapacity
				}
				records[name] = data
			}
			fields = fields[n:]
		}
		return nil
	}
	procedures := append(slices.Clone(current.InitialProcedures), current.NoninitialProcedures...)
	if len(procedures) > 128 || len(current.Dialogs) > 16 {
		return out, rediscore.ErrCapacity
	}
	fields := append(slices.Clone(procedures), current.Dialogs...)
	if current.Kind == pb.ContextSnapshot_SUBSCRIPTION {
		fields = append(fields, "r:subscription")
	}
	if current.CommandSequence > 0 {
		fields = append(fields, "r:result-"+strconv.FormatUint(current.CommandSequence, 10))
	}
	if err := read(fields); err != nil {
		return out, err
	}
	ps := make(map[string]*nativepb.Procedure)
	ds := make(map[string]*nativepb.Dialog)
	var messages, outputs, events, results []string
	add := func(fields *[]string, s string) {
		if s != "" {
			*fields = append(*fields, s)
		}
	}
	for _, field := range procedures {
		p, e := readPrivateProcedureRecord(records[field])
		if e != nil {
			return out, e
		}
		if p.Native.Version != portable.Version || p.Native.Key == "" {
			return out, errNativeRecord
		}
		ps[field] = p
		for _, s := range []string{p.Origin, p.LastResponse, p.Ack} {
			add(&messages, s)
		}
		if p.SuccessfulAck != nil {
			add(&messages, p.SuccessfulAck.FinalResponse)
			add(&messages, p.SuccessfulAck.Ack)
			add(&results, p.SuccessfulAck.AckResult)
		}
		add(&results, p.CommandResult)
		outputs = append(outputs, p.Outputs...)
		events = append(events, p.Events...)
	}
	for _, field := range current.Dialogs {
		d, e := readPrivateDialogRecord(records[field])
		if e != nil {
			return out, e
		}
		ds[field] = d
		outputs = append(outputs, d.Outputs...)
		add(&results, d.AckResult)
	}
	if len(outputs) > 4096 || len(events) > 384 {
		return out, rediscore.ErrCapacity
	}
	if err := read(append(append(slices.Clone(outputs), events...), results...)); err != nil {
		return out, err
	}
	for _, field := range outputs {
		v, e := decodeNativeOutput(records[field])
		if e != nil {
			return out, e
		}
		add(&results, v.Result)
	}
	if err := read(results); err != nil {
		return out, err
	}
	for _, field := range events {
		event, e := decodeNativeLifecycle(records[field])
		if e != nil {
			return out, e
		}
		if event.Root != "" && (ps[event.Root] == nil || !slices.Contains(ps[event.Root].Events, field)) {
			return out, errNativeRecord
		}
		add(&messages, event.Message)
	}
	if err := read(messages); err != nil {
		return out, err
	}
	memo, e := selectedmessage.NewMemo(min(4096, max(1, len(messages))))
	if e != nil {
		return out, e
	}
	message := func(field string) (*FixtureMessageProof, error) {
		if field == "" {
			return nil, nil
		}
		m, e := memo.Decode(records[field], "native-fixture")
		if e != nil {
			return nil, e
		}
		return fixtureMessage(m)
	}
	result := func(field string) (NativeFixtureResult, error) {
		r := NativeFixtureResult{Field: field}
		if records[field] == nil {
			return r, nil
		}
		v, e := decodeNativeResult(records[field])
		if e != nil {
			return r, e
		}
		if field != "r:result-"+strconv.FormatUint(v.Sequence, 10) || v.Sequence > current.CommandSequence {
			return r, errNativeRecord
		}
		r.Present = true
		r.Sequence = v.Sequence
		r.Certainty = v.Certainty.String()
		r.Pending = v.Pending
		r.CutoffMS = v.CutoffMs
		r.Output = v.Output
		return r, nil
	}
	for _, field := range procedures {
		p := ps[field]
		item := NativeFixtureProcedure{Field: field, Key: p.Native.Key, Family: p.Native.Family, Phase: p.Native.Phase, Cancelled: p.Native.Cancelled, CancelSent: p.Native.CancelSent, Cleanup: p.Cleanup, DecisionPending: p.DecisionPending, OutputConsumed: p.OutputConsumed, FinalResponseWrittenAcknowledged: p.FinalResponseWrittenAcknowledged, Flow: nativeFixtureJSON(p.Flow), Timers: nativeFixtureTimers(p.Native.GetTimers().GetValues()), Outputs: append([]string{}, p.Outputs...), Events: append([]string{}, p.Events...), Root: p.Root}
		var err error
		item.Origin, err = message(p.Origin)
		if err != nil {
			return out, err
		}
		item.Final, err = message(p.LastResponse)
		if err != nil {
			return out, err
		}
		item.ACK, err = message(p.Ack)
		if err != nil {
			return out, err
		}
		if p.CommandResult != "" {
			r, e := result(p.CommandResult)
			if e != nil {
				return out, e
			}
			item.CommandResult = &r
		}
		out.Procedures = append(out.Procedures, item)
	}
	for _, field := range current.Dialogs {
		d := ds[field]
		n := d.Native
		out.Dialogs = append(out.Dialogs, NativeFixtureDialog{Field: field, Role: n.Role, Phase: n.Phase, CallID: n.CallId, LocalTag: n.LocalTag, RemoteTag: n.RemoteTag, LocalCSeq: n.LocalCSeq, RemoteCSeq: n.RemoteCSeq, TargetSHA256: fixtureHash([]byte(n.Target)), RoutesSHA256: fixtureRoutes(n.GetRoutes().GetValues()), AckWritten: d.AckWritten, LocalInvite: nativeFixtureInvite(n.LocalInvite), RemoteInvite: nativeFixtureInvite(n.RemoteInvite), Timers: nativeFixtureTimers(n.GetTimers().GetValues()), Outputs: append([]string{}, d.Outputs...)})
	}
	slices.Sort(outputs)
	outputs = slices.Compact(outputs)
	for _, field := range outputs {
		v, e := decodeNativeOutput(records[field])
		if e != nil {
			return out, e
		}
		ref, e := nativeOutputRef(v.Write)
		if e != nil {
			return out, e
		}
		item := NativeFixtureOutput{Field: field, Procedure: v.Procedure, Dialog: v.Dialog, Result: v.Result, Method: v.Method, Status: v.Status, DeadlineMS: int64(v.Write.Permit.Authorization.DeadlineMs), Flow: nativeFixtureJSON(v.Write.Flow)}
		proof, e := store.InspectOutput(ctx, ref)
		if e == nil {
			if proof.MetadataField != field || !bytes.Equal(proof.Metadata, records[field]) {
				return out, rediscore.ErrConflict
			}
			item.Proof = NativeFixtureProof{true, string(proof.State), &proof.Settled, &proof.Acknowledged}
		} else if !errors.Is(e, rediscore.ErrAbsent) {
			return out, e
		}
		out.Outputs = append(out.Outputs, item)
	}
	slices.Sort(events)
	events = slices.Compact(events)
	for _, field := range events {
		v, e := decodeNativeLifecycle(records[field])
		if e != nil {
			return out, e
		}
		proof, e := message(v.Message)
		if e != nil {
			return out, e
		}
		out.Events = append(out.Events, NativeFixtureEvent{field, v.Kind.String(), v.Root, append([]string{}, v.Owners...), v.OperationId, v.DeadlineMs, proof})
	}
	if current.CommandSequence > 0 {
		results = append(results, "r:result-"+strconv.FormatUint(current.CommandSequence, 10))
	}
	slices.Sort(results)
	results = slices.Compact(results)
	for _, field := range results {
		v, e := result(field)
		if e != nil {
			return out, e
		}
		out.Results = append(out.Results, v)
	}
	if current.Kind == pb.ContextSnapshot_SUBSCRIPTION {
		sub := &NativeFixtureSubscription{}
		if records["r:subscription"] != nil {
			v, e := decodeNativeSubscription(records["r:subscription"])
			if e != nil {
				return out, e
			}
			s := v.Native
			sub = &NativeFixtureSubscription{true, s.LocalSequence, s.RemoteSequence, s.Pending, s.ExpiresMS, s.Terminating, s.Final, fixtureHash([]byte(s.Target))}
		}
		out.Subscription = sub
	}
	snapshot, e := nativeProjectSnapshot(current, nil, nil, nil)
	if e != nil {
		return out, e
	}
	out.Snapshot, e = fixtureSnapshot(snapshot)
	if e != nil {
		return out, e
	}
	// InspectOutput does not advance context revision. Re-read its owner fence
	// after all proof observations; never combine metadata from different views.
	final, e := store.Read(ctx, id, "r:context")
	if e != nil {
		return out, e
	}
	if final.Revision != revision || final.Timer != assignment || !bytes.Equal(final.Records["r:context"], selected.View.Records["r:context"]) {
		return out, rediscore.ErrConflict
	}
	return out, nil
}

// This fixture-only fallback observes the current process term, not a history
// of prior terms. It never interprets an operation error as expiry evidence.
func nativeFixtureExpiredApplication(ctx context.Context, client *redis.Client, root string, profile []byte, primary string, out *NativeFixtureApplication) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	values, err := client.Eval(ctx, `
if redis.call('GET',KEYS[1])~=ARGV[1] then return redis.error_reply('fixture profile changed') end
local info=redis.call('INFO','server');local id=string.match(info,'\r\nrun_id:([a-f0-9]+)\r\n')
if id~=ARGV[2] or not string.find(redis.call('INFO','replication'),'\r\nrole:master\r\n',1,true) then return redis.error_reply('fixture primary changed') end
local t=redis.call('TIME');local now=tonumber(t[1])*1000+math.floor(tonumber(t[2])/1000)
local term=redis.call('GET',KEYS[2]);if not term then return {'absent','',string.format('%.0f',now),'0'} end
if #term==0 or #term>190 or not string.match(term,'^[A-Za-z0-9_-]+$') then return redis.error_reply('fixture invalid application term') end
local v=redis.call('HMGET',ARGV[3]..'core:owner:'..term,'application','expires_ms')
if v[1]~=ARGV[4] or not v[2] or #v[2]>16 or not string.match(v[2],'^%d+$') or (#v[2]>1 and string.sub(v[2],1,1)=='0') then return redis.error_reply('fixture invalid owner metadata') end
local expiry=tonumber(v[2]);if not expiry or expiry<1 or expiry>9007199254740991 then return redis.error_reply('fixture invalid owner expiry') end
local state=expiry<=now and 'expired' or 'live';return {state,term,string.format('%.0f',now),v[2]}
`, []string{root + "core:profile", root + "core:application:" + out.Reference}, profile, primary, root, out.Reference).Slice()
	if err != nil {
		return err
	}
	if len(values) != 4 {
		return rediscore.ErrCorrupt
	}
	state, ok := values[0].(string)
	term, ok2 := values[1].(string)
	clock, ok3 := values[2].(string)
	expiry, ok4 := values[3].(string)
	checked, e := strconv.ParseInt(clock, 10, 64)
	until, e2 := strconv.ParseInt(expiry, 10, 64)
	if !ok || !ok2 || !ok3 || !ok4 || e != nil || e2 != nil || checked <= 0 || (state != "absent" && state != "expired" && state != "live") || (state == "expired" && until > checked) || (state == "live" && until <= checked) {
		return rediscore.ErrCorrupt
	}
	out.State, out.Term, out.CheckedAtMS, out.ExpiresAtMS = state, term, checked, until
	return ctx.Err()
}
