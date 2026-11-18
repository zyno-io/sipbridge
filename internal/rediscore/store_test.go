package rediscore

import (
	"bytes"
	"context"
	"errors"
	"maps"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
)

func fixture(t *testing.T) (*Store, *redis.Client, Identity) {
	t.Helper()
	address := os.Getenv("SIPBRIDGE_CORE_TEST_REDIS")
	if address == "" {
		t.Skip("set SIPBRIDGE_CORE_TEST_REDIS to an isolated qualification Redis")
	}
	client := redis.NewClient(&redis.Options{Addr: address, MaxRetries: -1})
	ctx := context.Background()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	info, err := client.Info(ctx, "server").Result()
	if err != nil {
		t.Fatal(err)
	}
	var primary string
	for _, line := range strings.Split(info, "\r\n") {
		if strings.HasPrefix(line, "run_id:") {
			primary = strings.TrimPrefix(line, "run_id:")
		}
	}
	root := "sipstate:test:core-" + uuid.NewString() + ":"
	s, err := New(client, Config{Root: root, Profile: []byte{0x08, 1, 0x12, 3, 0, 0xff, 0x80}, RecordBudget: 1 << 20, PrimaryRunID: primary})
	if err != nil {
		t.Fatal(err)
	}
	id := Identity{Context: "ctx-" + uuid.NewString(), Incarnation: uuid.NewString()}
	if err = client.Set(ctx, s.profileKey(), s.profile, 0).Err(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		// Qualification owns these exact keys. There is no runtime scan, migration
		// or background cleanup of another adapter's native namespace.
		if err := client.Del(ctx, s.contextKey(id.Context), s.profileKey(), s.capacityKey(), s.selectorKey(), s.applicationAdmissionKey()).Err(); err != nil {
			t.Error(err)
		}
		if err := client.Close(); err != nil {
			t.Error(err)
		}
	})
	return s, client, id
}

func publish(t *testing.T, s *Store, id Identity, changes ...Change) uint64 {
	t.Helper()
	revision, err := s.Commit(context.Background(), Publication{Identity: id, Create: true, Deadline: time.Now().Add(time.Minute), Changes: changes})
	if err != nil {
		t.Fatal(err)
	}
	return revision
}

func TestBinarySelectionAndPresence(t *testing.T) {
	s, client, id := fixture(t)
	wire := []byte{0, 0xff, 0xfe, '{', '"', '\n', 0x80}
	encoded, err := proto.Marshal(&pb.SipMessage{Wire: wire})
	if err != nil {
		t.Fatal(err)
	}
	publish(t, s, id, Change{Field: "r:selected", Value: encoded}, Change{Field: "r:empty", Value: []byte{}}, Change{Field: "r:unselected", Value: bytes.Repeat([]byte{0xff}, 1024)})
	view, err := s.Read(context.Background(), id, "r:selected", "r:empty", "r:absent")
	if err != nil {
		t.Fatal(err)
	}
	if view.Revision != 1 || len(view.Records) != 3 || !bytes.Equal(view.Records["r:selected"], encoded) || view.Records["r:empty"] == nil || view.Records["r:absent"] != nil {
		t.Fatalf("selected binary/presence changed: revision=%d", view.Revision)
	}
	raw, err := client.HGet(context.Background(), s.contextKey(id.Context), "r:selected").Bytes()
	if err != nil || !bytes.Equal(raw, encoded) {
		t.Fatal("stored record is not the raw protobuf")
	}
	var decoded pb.SipMessage
	if err = proto.Unmarshal(view.Records["r:selected"], &decoded); err != nil || !bytes.Equal(decoded.Wire, wire) {
		t.Fatal("binary payload did not round-trip")
	}
	next, err := s.Commit(context.Background(), Publication{Identity: id, Revision: 1, Deadline: time.Now().Add(time.Minute), Changes: []Change{{Field: "r:empty", Delete: true}}})
	if err != nil || next != 2 {
		t.Fatalf("delete: %v", err)
	}
	view, err = s.Read(context.Background(), id, "r:empty")
	if err != nil || view.Records["r:empty"] != nil {
		t.Fatal("delete became an empty retained record")
	}
}

func TestTwoWorkersPublishOneRevision(t *testing.T) {
	s, client, id := fixture(t)
	publish(t, s, id, Change{Field: "r:result", Value: []byte{0x08, 1}})
	peer, err := New(client, Config{Root: s.root, Profile: s.profile, RecordBudget: s.maxBytes, PrimaryRunID: s.primary})
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	var mu sync.Mutex
	accepted, conflicts := 0, 0
	start := make(chan struct{})
	for i, worker := range []*Store{s, peer} {
		wg.Add(1)
		go func(worker *Store, selected byte) {
			defer wg.Done()
			<-start
			_, err := worker.Commit(context.Background(), Publication{Identity: id, Revision: 1, Deadline: time.Now().Add(time.Minute), Changes: []Change{{Field: "r:result", Value: []byte{0x08, selected}}}})
			mu.Lock()
			defer mu.Unlock()
			if err == nil {
				accepted++
			} else if errors.Is(err, ErrConflict) {
				conflicts++
			} else {
				t.Error(err)
			}
		}(worker, byte(i+2))
	}
	close(start)
	wg.Wait()
	if accepted != 1 || conflicts != 1 {
		t.Fatalf("accepted=%d conflicts=%d", accepted, conflicts)
	}
	view, err := s.Read(context.Background(), id, "r:result")
	if err != nil || view.Revision != 2 {
		t.Fatalf("publication: %v", err)
	}
}

func TestAggregateSelectionIsBoundedBeforeValuesAreFetched(t *testing.T) {
	s, client, id := fixture(t)
	large, err := New(client, Config{Root: s.root, Profile: s.profile, RecordBudget: 128 << 20, PrimaryRunID: s.primary})
	if err != nil {
		t.Fatal(err)
	}
	// Each independent publication is legal. Their combined selected read must
	// be refused by Redis before constructing a reply containing both values.
	value := bytes.Repeat([]byte{0x08, 1}, 20<<20)
	publish(t, large, id, Change{Field: "r:first", Value: value})
	_, err = large.Commit(context.Background(), Publication{Identity: id, Revision: 1, Deadline: time.Now().Add(time.Minute), Changes: []Change{{Field: "r:second", Value: value}}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = large.Read(context.Background(), id, "r:first", "r:second"); !errors.Is(err, ErrCapacity) {
		t.Fatalf("oversized aggregate read: %v", err)
	}
	view, err := large.Read(context.Background(), id, "r:first")
	if err != nil || !bytes.Equal(view.Records["r:first"], value) {
		t.Fatalf("bounded selected read: %v", err)
	}
}

func TestExactWideRevision(t *testing.T) {
	s, client, id := fixture(t)
	publish(t, s, id)
	const revision uint64 = 9007199254740993
	if err := client.HSet(context.Background(), s.contextKey(id.Context), "revision", strconv.FormatUint(revision, 10)).Err(); err != nil {
		t.Fatal(err)
	}
	// This fixture writes a synthetic revision directly, so account for the
	// changed envelope length just as an atomic publication would.
	if err := client.HIncrBy(context.Background(), s.capacityKey(), "bytes", int64(len(strconv.FormatUint(revision, 10))-1)).Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Commit(context.Background(), Publication{Identity: id, Revision: revision - 1, Deadline: time.Now().Add(time.Minute)}); !errors.Is(err, ErrConflict) {
		t.Fatalf("adjacent wide revision accepted: %v", err)
	}
	next, err := s.Commit(context.Background(), Publication{Identity: id, Revision: revision, Deadline: time.Now().Add(time.Minute)})
	if err != nil || next != revision+1 {
		t.Fatalf("wide revision changed: next=%d err=%v", next, err)
	}
}

func TestRefusalsPublishNothing(t *testing.T) {
	s, client, id := fixture(t)
	publish(t, s, id, Change{Field: "r:tx", Value: []byte{0x08, 1}})
	baseline, err := client.HGetAll(context.Background(), s.contextKey(id.Context)).Result()
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name string
		p    Publication
		want error
	}{
		{"capacity", Publication{Identity: id, Revision: 1, Reserved: s.maxBytes, Deadline: time.Now().Add(time.Minute), Changes: []Change{{Field: "r:tx", Value: []byte{0x08, 2}}}}, ErrCapacity},
		{"deadline", Publication{Identity: id, Revision: 1, Deadline: time.Now().Add(-time.Second), Changes: []Change{{Field: "r:tx", Value: []byte{0x08, 2}}}}, ErrExpired},
		{"duplicate", Publication{Identity: id, Revision: 1, Deadline: time.Now().Add(time.Minute), Changes: []Change{{Field: "r:tx", Value: []byte{0x08, 2}}, {Field: "r:tx", Delete: true}}}, ErrConflict},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			if _, err := s.Commit(context.Background(), test.p); !errors.Is(err, test.want) {
				t.Fatalf("refusal: %v", err)
			}
			after, err := client.HGetAll(context.Background(), s.contextKey(id.Context)).Result()
			if err != nil || !maps.Equal(after, baseline) {
				t.Fatal("refused operation changed records")
			}
		})
	}
	wrong := id
	wrong.Owner = "different-owner"
	wrong.Term = "different-term"
	if _, err = s.Read(context.Background(), wrong, "r:tx"); !errors.Is(err, ErrAuthority) {
		t.Fatalf("owner term: %v", err)
	}
	if err = client.Set(context.Background(), s.profileKey(), []byte{0x08, 2}, 0).Err(); err != nil {
		t.Fatal(err)
	}
	if _, err = s.Read(context.Background(), id, "r:tx"); !errors.Is(err, ErrAuthority) {
		t.Fatalf("profile: %v", err)
	}
}

func TestDeniedLastMutationCannotPartiallyPublish(t *testing.T) {
	s, admin, id := fixture(t)
	publish(t, s, id, Change{Field: "r:tx", Value: []byte{0x08, 1}})
	ctx := context.Background()
	user := "core-" + uuid.NewString()
	const qualificationPassword = "disposable-core-qualification"
	if err := admin.ACLSetUser(ctx, user, "reset", "on", ">"+qualificationPassword, "~"+s.root+"*", "+hello", "+ping", "+client|setinfo", "+client|setname", "+eval", "+evalsha", "+get", "+type", "+hget", "+hmget", "+hlen", "+hexists", "+time", "+hset", "+info").Err(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := admin.ACLDelUser(ctx, user).Err(); err != nil {
			t.Error(err)
		}
	})
	client := redis.NewClient(&redis.Options{Addr: admin.Options().Addr, Username: user, Password: qualificationPassword, MaxRetries: -1})
	defer client.Close()
	restricted, err := New(client, Config{Root: s.root, Profile: s.profile, RecordBudget: s.maxBytes, PrimaryRunID: s.primary})
	if err != nil {
		t.Fatal(err)
	}
	_, err = restricted.Commit(ctx, Publication{Identity: id, Revision: 1, Deadline: time.Now().Add(time.Minute), Changes: []Change{{Field: "r:tx", Value: []byte{0x08, 2}}, {Field: "r:another", Delete: true}}})
	if !errors.Is(err, ErrAuthority) {
		t.Fatalf("last command denial: %v", err)
	}
	view, err := s.Read(ctx, id, "r:tx")
	if err != nil || view.Revision != 1 || !bytes.Equal(view.Records["r:tx"], []byte{0x08, 1}) {
		t.Fatal("last ACL refusal left a partial publication")
	}
}

type loseReply struct{ armed bool }

func (h *loseReply) DialHook(next redis.DialHook) redis.DialHook {
	return func(ctx context.Context, network, address string) (net.Conn, error) {
		return next(ctx, network, address)
	}
}
func (h *loseReply) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}
func (h *loseReply) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		err := next(ctx, cmd)
		if err == nil && h.armed && (cmd.Name() == "evalsha" || cmd.Name() == "eval") {
			h.armed = false
			return errors.New("qualification lost publication reply")
		}
		return err
	}
}

func TestLostCommitReplyStaysUnknown(t *testing.T) {
	s, admin, id := fixture(t)
	publish(t, s, id, Change{Field: "r:result", Value: []byte{0x08, 1}})
	client := redis.NewClient(&redis.Options{Addr: admin.Options().Addr, MaxRetries: -1})
	defer client.Close()
	hook := &loseReply{armed: true}
	client.AddHook(hook)
	worker, err := New(client, Config{Root: s.root, Profile: s.profile, RecordBudget: s.maxBytes, PrimaryRunID: s.primary})
	if err != nil {
		t.Fatal(err)
	}
	_, err = worker.Commit(context.Background(), Publication{Identity: id, Revision: 1, Deadline: time.Now().Add(time.Minute), Changes: []Change{{Field: "r:result", Value: []byte{0x08, 2}}}})
	if !errors.Is(err, ErrUnknown) {
		t.Fatalf("lost reply claimed definite failure: %v", err)
	}
	view, err := s.Read(context.Background(), id, "r:result")
	if err != nil || view.Revision != 2 || !bytes.Equal(view.Records["r:result"], []byte{0x08, 2}) {
		t.Fatal("unknown publication not independently inspectable")
	}
}

func TestAutomaticMutationRetriesAreRejected(t *testing.T) {
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"})
	defer client.Close()
	if _, err := New(client, Config{Root: "test:", Profile: []byte{1}, RecordBudget: 1024}); !errors.Is(err, ErrAuthority) {
		t.Fatal("automatic mutation retry permitted")
	}
}
