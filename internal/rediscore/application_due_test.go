package rediscore

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestPeekExpiredApplicationIsBoundedAndFenced(t *testing.T) {
	s, c, _ := fixture(t)
	ctx := context.Background()
	extra := []string{s.workerKey("coordinator"), s.coordinatorKey(), s.ownerExpiryKey(), s.applicationKey("app"), s.applicationOwnerKey("term"), s.applicationOwnedKey("term"), s.applicationOwnerKey("new-term"), s.applicationOwnedKey("new-term")}
	t.Cleanup(func() {
		if err := c.Del(ctx, extra...).Err(); err != nil {
			t.Error(err)
		}
	})
	lease, err := s.AcquireApplication(ctx, "app", "term")
	if err != nil {
		t.Fatal(err)
	}
	worker, err := s.RenewWorker(ctx, WorkerLeaseRequest{Worker: "coordinator", Epoch: "epoch", PrimaryRunID: s.primary, Acquire: true})
	if err != nil || !worker.Coordinator {
		t.Fatal("coordinator lease", err)
	}
	for i := 0; i < 40; i++ {
		id := Identity{Context: fmt.Sprintf("owned-%02d", i), Incarnation: fmt.Sprintf("inc-%02d", i), Owner: lease.Application, Term: lease.Term}
		extra = append(extra, s.contextKey(id.Context))
		if _, err = s.Commit(ctx, Publication{Identity: id, Create: true, Deadline: time.Now().Add(time.Minute), Application: &lease.ApplicationLeaseRef, Changes: []Change{{Field: "r:context", Value: []byte{1}}}}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err = s.PeekExpiredApplication(ctx, "coordinator", "epoch", "term", 7); !errors.Is(err, ErrConflict) {
		t.Fatal("live owner selected", err)
	}
	past := time.Now().Add(-time.Second).UnixMilli()
	if err = c.HSet(ctx, s.applicationOwnerKey("term"), "expires_ms", past).Err(); err != nil {
		t.Fatal(err)
	}
	if err = c.ZAdd(ctx, s.ownerExpiryKey(), redis.Z{Score: float64(past), Member: "a-term"}).Err(); err != nil {
		t.Fatal(err)
	}
	fresh, err := s.AcquireApplication(ctx, "app", "new-term")
	if err != nil || fresh.Term != "new-term" {
		t.Fatal("fresh term", err)
	}
	batch, err := s.PeekExpiredApplication(ctx, "coordinator", "epoch", "term", 7)
	if err != nil || len(batch.Contexts) != 7 {
		t.Fatal("bounded expired batch", len(batch.Contexts), err)
	}
	seen := map[string]bool{}
	for _, item := range batch.Contexts {
		if seen[item.Identity.Context] || item.Identity.Owner != "app" || item.Identity.Term != "term" || item.Revision != 1 {
			t.Fatal("invalid selected member", item)
		}
		seen[item.Identity.Context] = true
	}
	count, err := c.SCard(ctx, s.applicationOwnedKey("term")).Result()
	if err != nil || count != 40 {
		t.Fatal("peek changed membership", count, err)
	}
	if _, err = s.PeekExpiredApplication(ctx, "coordinator", "wrong-epoch", "term", 7); !errors.Is(err, ErrAuthority) {
		t.Fatal("stale coordinator accepted", err)
	}
	selected := batch.Contexts[0]
	if _, err = s.Commit(ctx, Publication{Identity: selected.Identity, Revision: selected.Revision, Deadline: time.Now().Add(time.Minute), ReleaseApplication: true}); err != nil {
		t.Fatal("release", err)
	}
	count, err = c.SCard(ctx, s.applicationOwnedKey("term")).Result()
	if err != nil || count != 39 {
		t.Fatal("release count", count, err)
	}
	if yes, err := c.SIsMember(ctx, s.applicationOwnedKey("term"), selected.Identity.Context).Result(); err != nil || yes {
		t.Fatal("released member retained", err)
	}
	if err = c.HSet(ctx, s.workerKey("coordinator"), "expires_ms", past).Err(); err != nil {
		t.Fatal(err)
	}
	if _, err = s.PeekExpiredApplication(ctx, "coordinator", "epoch", "term", 7); !errors.Is(err, ErrAuthority) {
		t.Fatal("expired coordinator accepted", err)
	}
}
