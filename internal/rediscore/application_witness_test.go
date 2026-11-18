package rediscore

import (
	"context"
	"crypto/sha256"
	"errors"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestApplicationWitnessPreservesOldOwnerAndOutput(t *testing.T) {
	s, c, id, out, wire := outputFixture(t)
	ctx := context.Background()
	deadline := time.Now().Add(time.Minute)
	old, err := s.AcquireApplication(ctx, "old-application", "old-term")
	if err != nil {
		t.Fatal(err)
	}
	next, err := s.AcquireApplication(ctx, "new-application", "new-term")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		keys := []string{s.applicationKey(old.Application), s.applicationOwnerKey(old.Term), s.applicationOwnedKey(old.Term), s.applicationKey(next.Application), s.applicationOwnerKey(next.Term), s.applicationOwnedKey(next.Term), s.applicationAdmissionKey(), s.ownerExpiryKey()}
		if e := c.Del(context.Background(), keys...).Err(); e != nil {
			t.Error(e)
		}
	})
	id.Owner, id.Term = old.Application, old.Term
	out.Application = &old.ApplicationLeaseRef
	out.DeliveryTag = sha256.Sum256([]byte("old-delivery"))
	_, err = s.Commit(ctx, Publication{Identity: id, Create: true, Deadline: deadline, Application: &old.ApplicationLeaseRef, Changes: []Change{{Field: out.MessageField, Value: wire}, {Field: out.MetadataField, Value: []byte("metadata")}, {Field: "r:delivery-tag", Value: out.DeliveryTag[:]}}, NewOutputs: []OutputIntent{out}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.ReadApplication(ctx, id, next.ApplicationLeaseRef, "r:delivery-tag"); !errors.Is(err, ErrAuthority) {
		t.Fatal("ordinary read accepted different owner", err)
	}
	if _, err = s.ObserveApplicationContext(ctx, id.Context, id.Incarnation, next.ApplicationLeaseRef, "r:delivery-tag"); !errors.Is(err, ErrAuthority) {
		t.Fatal("ordinary observation accepted different owner", err)
	}
	observedWitness, err := s.ObserveApplicationWitness(ctx, id.Context, id.Incarnation, next.ApplicationLeaseRef, "r:delivery-tag")
	if err != nil || observedWitness.Identity != id || observedWitness.View.Revision != 1 || string(observedWitness.View.Records["r:delivery-tag"]) != string(out.DeliveryTag[:]) {
		t.Fatal("witness observation did not return exact selected old identity", observedWitness, err)
	}
	if _, err = s.ObserveApplicationWitness(ctx, id.Context, "wrong-incarnation", next.ApplicationLeaseRef); !errors.Is(err, ErrAuthority) {
		t.Fatal("witness observation accepted another incarnation", err)
	}
	view, err := s.ReadApplicationWitness(ctx, id, next.ApplicationLeaseRef, "r:delivery-tag")
	if err != nil || view.Revision != 1 {
		t.Fatal("witness read", view, err)
	}
	wrong := id
	wrong.Term = "wrong-term"
	if _, err = s.ReadApplicationWitness(ctx, wrong, next.ApplicationLeaseRef); !errors.Is(err, ErrAuthority) {
		t.Fatal("witness accepted stale old owner", err)
	}
	_, err = s.Commit(ctx, Publication{Identity: id, Revision: 1, Deadline: deadline, Application: &next.ApplicationLeaseRef, WitnessApplication: true, Changes: []Change{{Field: "r:pending-subscribe", Value: []byte("pending")}}})
	if err != nil {
		t.Fatal("witness publication", err)
	}
	observed, err := s.ObserveContext(ctx, id.Context, id.Incarnation, "r:pending-subscribe", "r:delivery-tag")
	if err != nil || observed.Identity != id || observed.View.Revision != 2 || string(observed.View.Records["r:pending-subscribe"]) != "pending" || string(observed.View.Records["r:delivery-tag"]) != string(out.DeliveryTag[:]) {
		t.Fatal("witness changed identity or delivery", observed, err)
	}
	if member, err := c.SIsMember(ctx, s.applicationOwnedKey(old.Term), id.Context).Result(); err != nil || !member {
		t.Fatal("old owner membership lost", member, err)
	}
	if member, err := c.SIsMember(ctx, s.applicationOwnedKey(next.Term), id.Context).Result(); err != nil || member {
		t.Fatal("new owner prematurely bound", member, err)
	}
	if _, err = s.ReserveOutput(ctx, outputRef(id, out)); err != nil {
		t.Fatal("old authorized output fenced", err)
	}
	if _, err = s.Commit(ctx, Publication{Identity: id, Revision: 2, Deadline: deadline, Application: &next.ApplicationLeaseRef, WitnessApplication: true, Changes: []Change{{Field: "r:delivery-tag", Value: []byte("changed")}}}); !errors.Is(err, ErrConflict) {
		t.Fatal("witness changed delivery", err)
	}
	if err = c.HSet(ctx, s.applicationOwnerKey(next.Term), "expires_ms", time.Now().Add(-time.Second).UnixMilli()).Err(); err != nil {
		t.Fatal(err)
	}
	if _, err = s.ReadApplicationWitness(ctx, id, next.ApplicationLeaseRef); !errors.Is(err, ErrExpired) {
		t.Fatal("expired witness read", err)
	}
	if _, err = s.ObserveApplicationWitness(ctx, id.Context, id.Incarnation, next.ApplicationLeaseRef); !errors.Is(err, ErrExpired) {
		t.Fatal("expired witness observation", err)
	}
	if _, err = s.Commit(ctx, Publication{Identity: id, Revision: 2, Deadline: deadline, Application: &next.ApplicationLeaseRef, WitnessApplication: true, Changes: []Change{{Field: "r:pending-subscribe", Value: []byte("changed")}}}); !errors.Is(err, ErrExpired) {
		t.Fatal("expired witness publication", err)
	}
	after, err := s.Read(ctx, id, "r:pending-subscribe")
	if err != nil || after.Revision != 2 || string(after.Records["r:pending-subscribe"]) != "pending" {
		t.Fatal("refused witness changed context", after, err)
	}
	if err = c.HSet(ctx, s.applicationOwnerKey(next.Term), "expires_ms", time.Now().Add(20*time.Second).UnixMilli()).Err(); err != nil {
		t.Fatal(err)
	}
	if _, err = s.Commit(ctx, Publication{Identity: id, Revision: 2, Deadline: deadline, Application: &next.ApplicationLeaseRef, BindApplication: true}); err != nil {
		t.Fatal("accepted adoption could not bind new owner", err)
	}
	if _, err = s.ReadApplicationWitness(ctx, id, next.ApplicationLeaseRef); !errors.Is(err, ErrAuthority) {
		t.Fatal("old identity survived accepted owner transfer", err)
	}
	id.Owner, id.Term = next.Application, next.Term
	if _, err = s.ReadApplication(ctx, id, next.ApplicationLeaseRef, "r:pending-subscribe"); err != nil {
		t.Fatal("new owner could not read accepted context", err)
	}
}
func TestApplicationWitnessRejectsOwnershipMutations(t *testing.T) {
	s, _, id := fixture(t)
	ctx := context.Background()
	deadline := time.Now().Add(time.Minute)
	ref := ApplicationLeaseRef{Application: "app", Term: "term"}
	for _, p := range []Publication{
		{Identity: id, Create: true, Deadline: deadline, Application: &ref, WitnessApplication: true},
		{Identity: id, Revision: 1, Deadline: deadline, WitnessApplication: true},
		{Identity: id, Revision: 1, Deadline: deadline, Application: &ref, WitnessApplication: true, BindApplication: true},
		{Identity: id, Revision: 1, Deadline: deadline, Application: &ref, WitnessApplication: true, ReleaseApplication: true},
		{Identity: id, Revision: 1, Deadline: deadline, Application: &ref, WitnessApplication: true, Terminal: true},
	} {
		if _, err := s.Commit(ctx, p); !errors.Is(err, ErrConflict) {
			t.Fatal("invalid witness mode accepted", err)
		}
	}
}

func TestApplicationWitnessAllowsExpiredOldOwner(t *testing.T) {
	s, c, id := fixture(t)
	ctx := context.Background()
	deadline := time.Now().Add(time.Minute)
	old, err := s.AcquireApplication(ctx, "old-application", "old-term")
	if err != nil {
		t.Fatal(err)
	}
	next, err := s.AcquireApplication(ctx, "new-application", "new-term")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		keys := []string{s.applicationKey(old.Application), s.applicationOwnerKey(old.Term), s.applicationOwnedKey(old.Term), s.applicationKey(next.Application), s.applicationOwnerKey(next.Term), s.applicationOwnedKey(next.Term), s.applicationAdmissionKey(), s.ownerExpiryKey()}
		if e := c.Del(context.Background(), keys...).Err(); e != nil {
			t.Error(e)
		}
	})
	id.Owner, id.Term = old.Application, old.Term
	_, err = s.Commit(ctx, Publication{Identity: id, Create: true, Deadline: deadline, Application: &old.ApplicationLeaseRef, Changes: []Change{{Field: "r:subscription", Value: []byte("current")}}})
	if err != nil {
		t.Fatal(err)
	}
	expired := time.Now().Add(-time.Second).UnixMilli()
	if err = c.HSet(ctx, s.applicationOwnerKey(old.Term), "expires_ms", expired).Err(); err != nil {
		t.Fatal(err)
	}
	if err = c.ZAdd(ctx, s.ownerExpiryKey(), redis.Z{Score: float64(expired), Member: "a-" + old.Term}).Err(); err != nil {
		t.Fatal(err)
	}
	if _, err = s.ReadApplicationWitness(ctx, id, next.ApplicationLeaseRef, "r:subscription"); err != nil {
		t.Fatal("old owner expiry fenced new recipient", err)
	}
	if _, err = s.Commit(ctx, Publication{Identity: id, Revision: 1, Deadline: deadline, Application: &next.ApplicationLeaseRef, WitnessApplication: true, Changes: []Change{{Field: "r:pending-subscribe", Value: []byte("pending")}}}); err != nil {
		t.Fatal("old owner expiry blocked witnessed pending request", err)
	}
	actual, err := s.ObserveContext(ctx, id.Context, id.Incarnation, "r:subscription")
	if err != nil || actual.Identity != id {
		t.Fatal("expired old owner changed before acceptance", actual, err)
	}
}
