package rediscore

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestApplicationDisconnectExpiresExactOwnedTermAndWakesCoordinator(t *testing.T) {
	s, client, app := applicationFixture(t)
	ctx := context.Background()
	lease, err := s.AcquireApplication(ctx, app, "term1")
	if err != nil {
		t.Fatal(err)
	}
	id := Identity{Context: "disconnect-call", Incarnation: "inc", Owner: app, Term: lease.Term}
	t.Cleanup(func() { _ = client.Del(ctx, s.contextKey(id.Context), s.coordinatorKey()).Err() })
	if _, err = s.Commit(ctx, Publication{Identity: id, Create: true, Deadline: time.Now().Add(time.Minute), Application: &lease.ApplicationLeaseRef,
		Changes: []Change{{Field: "r:context", Value: []byte{1, 2, 3}}}}); err != nil {
		t.Fatal(err)
	}
	if err = client.HSet(ctx, s.coordinatorKey(), "worker", "worker1", "epoch", "epoch1", "expires_ms", time.Now().Add(time.Second).UnixMilli()).Err(); err != nil {
		t.Fatal(err)
	}
	wake, err := s.SubscribeWake(ctx, "worker1")
	if err != nil {
		t.Fatal(err)
	}
	defer wake.Close()
	_, signal, _ := wake.Snapshot()
	if err = s.ExpireApplication(ctx, lease.ApplicationLeaseRef); err != nil {
		t.Fatal(err)
	}
	if err = wake.Receive(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case <-signal:
	default:
		t.Fatal("disconnect did not wake owner-expiry scheduler")
	}
	if _, err = s.RenewApplication(ctx, lease.ApplicationLeaseRef); !errors.Is(err, ErrExpired) {
		t.Fatal("disconnected term renewed", err)
	}
	if _, err = s.ReadApplication(ctx, id, lease.ApplicationLeaseRef, "r:context"); !errors.Is(err, ErrExpired) {
		t.Fatal("disconnected application admitted work", err)
	}
	if _, err = s.Read(ctx, id, "r:context"); err != nil {
		t.Fatal("disconnect destroyed independent native state", err)
	}
	if member, err := client.SIsMember(ctx, s.applicationOwnedKey(lease.Term), id.Context).Result(); err != nil || !member {
		t.Fatal("disconnect dropped owned-context cleanup scope", err)
	}
	if due, err := client.ZScore(ctx, s.ownerExpiryKey(), "a-"+lease.Term).Result(); err != nil || due > float64(time.Now().Add(workerClockTolerance).UnixMilli()) {
		t.Fatal("owner expiry not advanced", due, err)
	}
	replacement, err := s.AcquireApplication(ctx, app, "term2")
	if err != nil || replacement.Term == lease.Term {
		t.Fatal("new hello restored old term", err)
	}
	if err = s.ExpireApplication(ctx, lease.ApplicationLeaseRef); err != nil {
		t.Fatal("late disconnect was not harmless", err)
	}
	if _, err = s.RenewApplication(ctx, replacement.ApplicationLeaseRef); err != nil {
		t.Fatal("late old disconnect fenced new session", err)
	}
}

func TestApplicationDisconnectWithoutContextsCreatesNoCleanupIndex(t *testing.T) {
	s, client, app := applicationFixture(t)
	ctx := context.Background()
	lease, err := s.AcquireApplication(ctx, app, "term1")
	if err != nil {
		t.Fatal(err)
	}
	if err = s.ExpireApplication(ctx, lease.ApplicationLeaseRef); err != nil {
		t.Fatal(err)
	}
	if err = s.ExpireApplication(ctx, lease.ApplicationLeaseRef); err != nil {
		t.Fatal("duplicate disconnect", err)
	}
	if count, err := client.ZCard(ctx, s.ownerExpiryKey()).Result(); err != nil || count != 0 {
		t.Fatal("empty disconnected application created cleanup work", count, err)
	}
	if _, err = s.RenewApplication(ctx, lease.ApplicationLeaseRef); !errors.Is(err, ErrExpired) {
		t.Fatal("disconnected quiet term renewed", err)
	}
}
