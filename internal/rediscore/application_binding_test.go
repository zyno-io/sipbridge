package rediscore

import (
	"context"
	"errors"
	"maps"
	"strconv"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestApplicationPendingDirectCreateFencesRecipientWithoutBinding(t *testing.T) {
	s, client, app := applicationFixture(t)
	ctx := context.Background()
	lease, err := s.AcquireApplication(ctx, app, "term1")
	if err != nil {
		t.Fatal(err)
	}
	id := Identity{Context: "pending-direct", Incarnation: "pending-incarnation"}
	other := Identity{Context: "expired-pending", Incarnation: "expired-incarnation"}
	t.Cleanup(func() { _ = client.Del(ctx, s.contextKey(id.Context), s.contextKey(other.Context)).Err() })
	p := Publication{Identity: id, Create: true, Deadline: time.Now().Add(time.Minute), Application: &lease.ApplicationLeaseRef,
		Changes: []Change{{Field: "r:procedure", Value: []byte("pending")}}}
	if _, err = s.Commit(ctx, p); err != nil {
		t.Fatal("guarded unbound direct request refused", err)
	}
	if _, err = s.Read(ctx, id, "r:procedure"); err != nil {
		t.Fatal("pending native owner was bound prematurely", err)
	}
	guarded, err := s.ReadApplication(ctx, id, lease.ApplicationLeaseRef, "r:procedure")
	if err != nil || guarded == nil || string(guarded.Records["r:procedure"]) != "pending" {
		t.Fatal("live recipient could not capture unbound selected facts", err)
	}
	observed, err := s.ObserveApplicationContext(ctx, id.Context, id.Incarnation, lease.ApplicationLeaseRef, "r:procedure")
	if err != nil || observed.Identity != id || observed.View.Revision != guarded.Revision || string(observed.View.Records["r:procedure"]) != "pending" {
		t.Fatal("application observation lost unbound identity or selected records", err)
	}
	if count, err := client.SCard(ctx, s.applicationOwnedKey(lease.Term)).Result(); err != nil || count != 0 {
		t.Fatal("pending notification assigned application ownership", count, err)
	}
	checkNamespace(t, s, client, id)
	p.Create, p.Revision = false, 1
	if _, err = s.Commit(ctx, p); err != nil {
		t.Fatal("live witness could not update an admitted unbound request", err)
	}
	if _, err = s.Read(ctx, id); err != nil {
		t.Fatal("unbound semantic refusal bound application ownership", err)
	}
	// A later selected decision establishes ownership under its current live
	// witness, rather than inheriting ownership from notification admission.
	p.Create, p.BindApplication, p.Revision = false, true, 2
	if _, err = s.Commit(ctx, p); err != nil {
		t.Fatal("accepted direct decision could not bind owner", err)
	}
	id.Owner, id.Term = lease.Application, lease.Term
	if _, err = s.Read(ctx, id); err != nil {
		t.Fatal("accepted owner header missing", err)
	}
	observed, err = s.ObserveApplicationContext(ctx, id.Context, id.Incarnation, lease.ApplicationLeaseRef, "r:procedure")
	if err != nil || observed.Identity != id || observed.View.Revision != 3 {
		t.Fatal("application observation did not atomically capture current owner", err)
	}
	if _, err = s.ObserveApplicationContext(ctx, id.Context, id.Incarnation, ApplicationLeaseRef{Application: "other", Term: "other-term"}, "r:procedure"); !errors.Is(err, ErrAuthority) {
		t.Fatal("application observation disclosed another owner's records", err)
	}
	checkNamespace(t, s, client, id)
	expired := time.Now().Add(-time.Second).UnixMilli()
	if err = client.HSet(ctx, s.applicationOwnerKey(lease.Term), "expires_ms", expired).Err(); err != nil {
		t.Fatal(err)
	}
	if err = client.ZAdd(ctx, s.ownerExpiryKey(), redis.Z{Score: float64(expired), Member: "a-" + lease.Term}).Err(); err != nil {
		t.Fatal(err)
	}
	if _, err = s.ReadApplication(ctx, id, lease.ApplicationLeaseRef, "r:procedure"); !errors.Is(err, ErrExpired) {
		t.Fatal("expired application captured business results", err)
	}
	if _, err = s.ObserveApplicationContext(ctx, id.Context, id.Incarnation, lease.ApplicationLeaseRef, "r:procedure"); !errors.Is(err, ErrExpired) {
		t.Fatal("expired application observed current owner or selected records", err)
	}
	if _, err = s.Read(ctx, id, "r:procedure"); err != nil {
		t.Fatal("expired application blocked independent native completion facts", err)
	}
	p.Identity, p.Create, p.BindApplication, p.Revision = other, true, false, 0
	if _, err = s.Commit(ctx, p); !errors.Is(err, ErrExpired) {
		t.Fatal("expired notification recipient admitted fresh request", err)
	}
	if count, err := client.Exists(ctx, s.contextKey(other.Context)).Result(); err != nil || count != 0 {
		t.Fatal("expired recipient refusal partially published state", count, err)
	}
}

func TestApplicationBindingPlanAndApply(t *testing.T) {
	s, c, app := applicationFixture(t)
	ctx := context.Background()
	t.Cleanup(func() { _ = c.Del(context.Background(), s.applicationKey("application2")).Err() })
	first, err := s.AcquireApplication(ctx, app, "term1")
	if err != nil {
		t.Fatal(err)
	}
	if err := c.HSet(ctx, s.coordinatorKey(), "worker", "worker1", "epoch", "epoch1", "expires_ms", time.Now().Add(time.Second).UnixMilli()).Err(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Del(context.Background(), s.coordinatorKey()).Err() })
	wake, err := s.SubscribeWake(ctx, "worker1")
	if err != nil {
		t.Fatal(err)
	}
	defer wake.Close()
	_, wakeCh, _ := wake.Snapshot()
	second, err := s.AcquireApplication(ctx, "application2", "term2")
	if err != nil {
		t.Fatal(err)
	}
	script := redis.NewScript(applicationGuardScript + applicationBindingScript + `
local p,e=bindingPrepare(ARGV[1],ARGV[2],ARGV[3],ARGV[4],ARGV[5],ARGV[6],tonumber(ARGV[7]),ARGV[8]=='1')
if e then return {e} end
bindingApply(p); return {'OK',tostring(p.indexDelta)}
`)
	apply := func(oldApp, oldTerm, newApp, newTerm string, expired bool) (int64, error) {
		t.Helper()
		flag := "0"
		if expired {
			flag = "1"
		}
		v, e := script.Run(ctx, c, nil, s.root, "context1", oldApp, oldTerm, newApp, newTerm, time.Now().UnixMilli(), flag).Slice()
		if e != nil {
			return 0, e
		}
		if e = status(v); e != nil {
			return 0, e
		}
		value, ok := v[1].(string)
		if !ok {
			return 0, ErrCorrupt
		}
		return strconv.ParseInt(value, 10, 64)
	}
	delta, err := apply("", "", app, first.Term, false)
	if err != nil || delta != int64(len("context1")) {
		t.Fatal("attach", delta, err)
	}
	if n, _ := c.SIsMember(ctx, s.applicationOwnedKey(first.Term), "context1").Result(); !n {
		t.Fatal("missing first membership")
	}
	if err = wake.Receive(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case <-wakeCh:
	default:
		t.Fatal("earliest owner expiry did not wake coordinator")
	}
	if _, err = apply(app, first.Term, "application2", "missing", false); err == nil {
		t.Fatal("missing target owner accepted")
	}
	if n, _ := c.SIsMember(ctx, s.applicationOwnedKey(first.Term), "context1").Result(); !n {
		t.Fatal("refusal mutated old membership")
	}
	if _, err = apply(app, first.Term, "", "", true); !errors.Is(err, ErrConflict) {
		t.Fatal("live owner released", err)
	}
	delta, err = apply(app, first.Term, "application2", second.Term, false)
	if err != nil || delta != 0 {
		t.Fatal("transfer", delta, err)
	}
	if n, _ := c.SIsMember(ctx, s.applicationOwnedKey(first.Term), "context1").Result(); n {
		t.Fatal("old membership remained")
	}
	if n, _ := c.SIsMember(ctx, s.applicationOwnedKey(second.Term), "context1").Result(); !n {
		t.Fatal("new membership missing")
	}
	expiredAt := time.Now().Add(-time.Second).UnixMilli()
	if err = c.HSet(ctx, s.applicationOwnerKey(second.Term), "expires_ms", expiredAt).Err(); err != nil {
		t.Fatal(err)
	}
	if err = c.ZAdd(ctx, s.ownerExpiryKey(), redis.Z{Score: float64(expiredAt), Member: "a-" + second.Term}).Err(); err != nil {
		t.Fatal(err)
	}
	delta, err = apply("application2", second.Term, "", "", true)
	if err != nil || delta != -int64(len("context1")) {
		t.Fatal("expired release", delta, err)
	}
	if n, _ := c.SCard(ctx, s.applicationOwnedKey(second.Term)).Result(); n != 0 {
		t.Fatal("expired membership retained")
	}
	if _, err = c.ZScore(ctx, s.ownerExpiryKey(), "a-"+second.Term).Result(); !errors.Is(err, redis.Nil) {
		t.Fatal("expired index retained", err)
	}
}

func TestApplicationBoundPublicationExpiryAndRetirement(t *testing.T) {
	s, c, app := applicationFixture(t)
	ctx := context.Background()
	lease, err := s.AcquireApplication(ctx, app, "term1")
	if err != nil {
		t.Fatal(err)
	}
	id := Identity{Context: "context1", Incarnation: "inc1", Owner: app, Term: lease.Term}
	t.Cleanup(func() { _ = c.Del(context.Background(), s.contextKey(id.Context)).Err() })
	p := Publication{Identity: id, Create: true, Deadline: time.Now().Add(time.Minute), Changes: []Change{{Field: "r:dialog", Value: []byte("dialog")}}, Application: &lease.ApplicationLeaseRef}
	if _, err = s.Commit(ctx, p); err != nil {
		t.Fatal("guarded create", err)
	}
	checkNamespace(t, s, c, id)
	if err = c.HSet(ctx, s.applicationOwnerKey(lease.Term), "expires_ms", time.Now().Add(-time.Second).UnixMilli()).Err(); err != nil {
		t.Fatal(err)
	}
	expired, err := c.HGet(ctx, s.applicationOwnerKey(lease.Term), "expires_ms").Int64()
	if err != nil {
		t.Fatal(err)
	}
	if err = c.ZAdd(ctx, s.ownerExpiryKey(), redis.Z{Score: float64(expired), Member: "a-" + lease.Term}).Err(); err != nil {
		t.Fatal(err)
	}
	p = Publication{Identity: id, Revision: 1, Deadline: time.Now().Add(time.Minute), Changes: []Change{{Field: "r:byeduty", Value: []byte("native")}}, Application: &lease.ApplicationLeaseRef}
	if _, err = s.Commit(ctx, p); !errors.Is(err, ErrExpired) {
		t.Fatal("expired business witness admitted", err)
	}
	p.Application = nil
	if _, err = s.Commit(ctx, p); err != nil {
		t.Fatal("native completion blocked by expired controller", err)
	}
	p = Publication{Identity: id, Revision: 2, Deadline: time.Now().Add(time.Minute), ReleaseApplication: true}
	if _, err = s.Commit(ctx, p); err != nil {
		t.Fatal("expired owner release", err)
	}
	unbound := id
	unbound.Owner = ""
	unbound.Term = ""
	if n, err := c.SCard(ctx, s.applicationOwnedKey(lease.Term)).Result(); err != nil || n != 0 {
		t.Fatal("owner not released", n, err)
	}
	if _, err = s.Read(ctx, unbound, "r:byeduty"); err != nil {
		t.Fatal("native duty not retained", err)
	}
	checkNamespace(t, s, c, unbound)
	if err = s.RetireContext(ctx, unbound, 3, time.Now().Add(time.Minute), true); !errors.Is(err, ErrConflict) {
		t.Fatal("outstanding duty retired", err)
	}
	p = Publication{Identity: unbound, Revision: 3, Deadline: time.Now().Add(time.Minute), Changes: []Change{{Field: "r:dialog", Delete: true}, {Field: "r:byeduty", Delete: true}}}
	if _, err = s.Commit(ctx, p); err != nil {
		t.Fatal(err)
	}
	if err = s.RetireContext(ctx, unbound, 4, time.Now().Add(time.Minute), true); err != nil {
		t.Fatal(err)
	}
	cap, err := c.HGetAll(ctx, s.capacityKey()).Result()
	if err != nil || cap["contexts"] != "0" || cap["bytes"] != "0" {
		t.Fatal("retirement capacity", cap, err)
	}
}

func TestApplicationOwnerTransferIsAtomicAndPhysicalCompletionSurvives(t *testing.T) {
	s, c, id, out, data := outputFixture(t)
	ctx := context.Background()
	app1, app2 := "application1", "application2"
	t.Cleanup(func() {
		_ = c.Del(context.Background(), s.applicationKey(app1), s.applicationKey(app2), s.applicationOwnerKey("term1"), s.applicationOwnerKey("term2"), s.applicationOwnedKey("term1"), s.applicationOwnedKey("term2"), s.ownerExpiryKey()).Err()
	})
	first, err := s.AcquireApplication(ctx, app1, "term1")
	if err != nil {
		t.Fatal(err)
	}
	second, err := s.AcquireApplication(ctx, app2, "term2")
	if err != nil {
		t.Fatal(err)
	}
	id.Owner = app1
	id.Term = first.Term
	_, err = s.Commit(ctx, Publication{Identity: id, Create: true, Deadline: time.Now().Add(time.Minute), Application: &first.ApplicationLeaseRef, Changes: []Change{{Field: out.MessageField, Value: data}, {Field: out.MetadataField, Value: []byte("metadata")}}, NewOutputs: []OutputIntent{out}})
	if err != nil {
		t.Fatal(err)
	}
	ref := outputRef(id, out)
	if _, err = s.ReserveOutput(ctx, ref); err != nil {
		t.Fatal(err)
	}
	checkNamespace(t, s, c, id)
	_, err = s.Commit(ctx, Publication{Identity: id, Revision: 1, Deadline: time.Now().Add(time.Minute), Application: &second.ApplicationLeaseRef, BindApplication: true})
	if err != nil {
		t.Fatal("transfer", err)
	}
	if ok, _ := c.SIsMember(ctx, s.applicationOwnedKey(first.Term), id.Context).Result(); ok {
		t.Fatal("old membership retained")
	}
	if ok, _ := c.SIsMember(ctx, s.applicationOwnedKey(second.Term), id.Context).Result(); !ok {
		t.Fatal("new membership missing")
	}
	newID := id
	newID.Owner = app2
	newID.Term = second.Term
	checkNamespace(t, s, c, newID)
	if _, err = s.Commit(ctx, Publication{Identity: id, Revision: 2, Deadline: time.Now().Add(time.Minute), Application: &first.ApplicationLeaseRef}); !errors.Is(err, ErrAuthority) {
		t.Fatal("old owner not fenced", err)
	}
	if err = s.CompleteOutput(ctx, ref, "WRITTEN"); err != nil {
		t.Fatal("exact physical completion blocked by transfer", err)
	}
}

func TestApplicationBoundRetirementDetachesAndRefunds(t *testing.T) {
	s, c, app := applicationFixture(t)
	ctx := context.Background()
	lease, err := s.AcquireApplication(ctx, app, "term1")
	if err != nil {
		t.Fatal(err)
	}
	id := Identity{Context: "context1", Incarnation: "inc1", Owner: app, Term: lease.Term}
	t.Cleanup(func() { _ = c.Del(context.Background(), s.contextKey(id.Context)).Err() })
	if _, err = s.Commit(ctx, Publication{Identity: id, Create: true, Deadline: time.Now().Add(time.Minute), Application: &lease.ApplicationLeaseRef}); err != nil {
		t.Fatal(err)
	}
	checkNamespace(t, s, c, id)
	if err = s.RetireContext(ctx, id, 1, time.Now().Add(time.Minute), true); err != nil {
		t.Fatal("atomic retirement", err)
	}
	if n, err := c.SCard(ctx, s.applicationOwnedKey(lease.Term)).Result(); err != nil || n != 0 {
		t.Fatal("membership not detached", n, err)
	}
	if _, err = c.ZScore(ctx, s.ownerExpiryKey(), "a-"+lease.Term).Result(); !errors.Is(err, redis.Nil) {
		t.Fatal("expiry not removed", err)
	}
	cap, err := c.HGetAll(ctx, s.capacityKey()).Result()
	if err != nil || cap["contexts"] != "0" || cap["bytes"] != "0" {
		t.Fatal("capacity not refunded", cap, err)
	}
}

func TestApplicationBindingCapacityRefusalAndUnknownTransfer(t *testing.T) {
	s, c, app := applicationFixture(t)
	ctx := context.Background()
	lease, err := s.AcquireApplication(ctx, app, "term1")
	if err != nil {
		t.Fatal(err)
	}
	id := Identity{Context: "context1", Incarnation: "inc1"}
	t.Cleanup(func() { _ = c.Del(context.Background(), s.contextKey(id.Context)).Err() })
	if _, err = s.Commit(ctx, Publication{Identity: id, Create: true, Deadline: time.Now().Add(time.Minute)}); err != nil {
		t.Fatal(err)
	}
	before, err := c.HGetAll(ctx, s.contextKey(id.Context)).Result()
	if err != nil {
		t.Fatal(err)
	}
	limit, err := c.HGet(ctx, s.capacityKey(), "bytes").Int64()
	if err != nil {
		t.Fatal(err)
	}
	oldLimit := s.namespaceBytes
	s.namespaceBytes = limit
	p := Publication{Identity: id, Revision: 1, Deadline: time.Now().Add(time.Minute), Application: &lease.ApplicationLeaseRef, BindApplication: true}
	if _, err = s.Commit(ctx, p); !errors.Is(err, ErrCapacity) {
		t.Fatal("binding capacity admitted", err)
	}
	after, err := c.HGetAll(ctx, s.contextKey(id.Context)).Result()
	if err != nil || !maps.Equal(before, after) {
		t.Fatal("capacity refusal changed context", err)
	}
	if n, _ := c.SCard(ctx, s.applicationOwnedKey(lease.Term)).Result(); n != 0 {
		t.Fatal("capacity refusal bound owner")
	}
	s.namespaceBytes = oldLimit
	client := redis.NewClient(&redis.Options{Addr: c.Options().Addr, MaxRetries: -1})
	defer client.Close()
	client.AddHook(&loseReply{armed: true})
	peer, err := New(client, Config{Root: s.root, Profile: s.profile, RecordBudget: s.maxBytes, PrimaryRunID: s.primary, NamespaceMaxBytes: s.namespaceBytes, NamespaceMaxContexts: s.namespaceContexts})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = peer.Commit(ctx, p); !errors.Is(err, ErrUnknown) {
		t.Fatal("lost transfer not UNKNOWN", err)
	}
	bound := id
	bound.Owner = app
	bound.Term = lease.Term
	if _, err = s.Read(ctx, bound); err != nil {
		t.Fatal("committed transfer missing", err)
	}
	if _, err = s.Commit(ctx, p); !errors.Is(err, ErrAuthority) {
		t.Fatal("retry with old identity not fenced", err)
	}
	checkNamespace(t, s, c, bound)
}
