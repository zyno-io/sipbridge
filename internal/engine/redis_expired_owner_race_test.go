package engine

import (
	"context"
	"errors"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"sipbridge/internal/rediscore"
)

// Model occupied slots at the persisted membership boundary. The tested
// callbacks must leave these siblings alone and never try to allocate one more.
func fillRedisProcedureSlots(t *testing.T, ctx context.Context, store *rediscore.Store, id rediscore.Identity) []string {
	t.Helper()
	view, err := store.Read(ctx, id, "r:context")
	if err != nil {
		t.Fatal(err)
	}
	current, err := decodeNativeContext(view.Records["r:context"])
	if err != nil {
		t.Fatal(err)
	}
	for len(current.InitialProcedures)+len(current.NoninitialProcedures) < stateNativeProcedures {
		current.NoninitialProcedures = append(current.NoninitialProcedures, "r:occupied-"+strconv.Itoa(len(current.NoninitialProcedures)))
	}
	slices.Sort(current.NoninitialProcedures)
	data, err := encodeNativeContext(current)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = store.Commit(ctx, rediscore.Publication{Identity: id, Revision: view.Revision, Deadline: time.Now().Add(time.Second), SpendReservation: true, Changes: []rediscore.Change{{Field: "r:context", Value: data}}}); err != nil {
		t.Fatal(err)
	}
	return current.NoninitialProcedures
}

func TestStaleRedisExpiredOwner(t *testing.T) {
	store, client, id, _, _, _, root := nativeOutputFixture(t)
	ctx := context.Background()
	t.Cleanup(func() {
		keys := []string{root + "core:application:diagnostic-app", root + "core:owner:diagnostic-term", root + "core:owned:diagnostic-term", root + "core:applications", root + "core:owner-expiry"}
		if err := client.Del(ctx, keys...).Err(); err != nil {
			t.Error(err)
		}
	})
	lease, err := store.AcquireApplication(ctx, "diagnostic-app", "diagnostic-term")
	if err != nil {
		t.Fatal(err)
	}
	id.Owner, id.Term = "diagnostic-app", lease.Term
	deadline := time.Now().Add(time.Minute)
	_, err = store.Commit(ctx, rediscore.Publication{Identity: id, Create: true, Deadline: deadline, Application: &lease.ApplicationLeaseRef, Changes: []rediscore.Change{{Field: "r:dialog", Value: []byte("dialog")}}})
	if err != nil {
		t.Fatal(err)
	}
	if staleRedisExpiredOwner(ctx, store, id) {
		t.Fatal("unchanged owner classified as stale")
	}
	info, err := client.Info(ctx, "server").Result()
	if err != nil {
		t.Fatal(err)
	}
	var runID string
	for _, line := range strings.Split(info, "\r\n") {
		if strings.HasPrefix(line, "run_id:") {
			runID = strings.TrimPrefix(line, "run_id:")
		}
	}
	wrongProfile, err := rediscore.New(client, rediscore.Config{Root: root, Profile: []byte("wrong-profile"), RecordBudget: 1 << 20, PrimaryRunID: runID})
	if err != nil {
		t.Fatal(err)
	}
	if staleRedisExpiredOwner(ctx, wrongProfile, id) {
		t.Fatal("profile mismatch classified as stale")
	}
	expired := time.Now().Add(-time.Second).UnixMilli()
	if err = client.HSet(ctx, root+"core:owner:"+id.Term, "expires_ms", expired).Err(); err != nil {
		t.Fatal(err)
	}
	if err = client.ZAdd(ctx, root+"core:owner-expiry", redis.Z{Score: float64(expired), Member: "a-" + id.Term}).Err(); err != nil {
		t.Fatal(err)
	}
	release := rediscore.Publication{Identity: id, Revision: 1, Deadline: deadline, ReleaseApplication: true}
	if _, err = store.Commit(ctx, release); err != nil {
		t.Fatal("release", err)
	}
	if _, err = store.Commit(ctx, release); !errors.Is(err, rediscore.ErrAuthority) {
		t.Fatal("expected stale identity", err)
	}
	if !staleRedisExpiredOwner(ctx, store, id) {
		t.Fatal("released owner not classified as stale")
	}
	if staleRedisExpiredOwner(ctx, wrongProfile, id) {
		t.Fatal("profile mismatch masked after release")
	}
	other := id
	other.Incarnation = "other-incarnation"
	if staleRedisExpiredOwner(ctx, store, other) {
		t.Fatal("incarnation mismatch classified as stale")
	}
	other = id
	other.Context = "absent-context"
	if !staleRedisExpiredOwner(ctx, store, other) {
		t.Fatal("confirmed absence not classified as stale")
	}
	if staleRedisExpiredOwner(ctx, wrongProfile, other) {
		t.Fatal("absence bypassed profile guard")
	}
}
