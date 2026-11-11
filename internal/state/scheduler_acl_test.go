package state

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
)

func TestSchedulerRequiredPermissionsFailBeforeProtocolMutation(t *testing.T) {
	for _, denied := range []string{"zrange", "xadd", "xread", "xrevrange"} {
		t.Run(denied, func(t *testing.T) {
			s := testStore(t, nil)
			ctx := context.Background()
			user := "scheduler-" + uuid.NewString()
			if err := s.client.ACLSetUser(ctx, user, "reset", "on", ">fixture-only", "~"+s.root+"*", "+@all", "-"+denied).Err(); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { s.client.ACLDelUser(ctx, user) })
			cfg := s.cfg
			cfg.Redis.Username, cfg.Redis.Password = user, "fixture-only"
			restricted, err := Open(ctx, cfg)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { restricted.Close() })
			ref := group(t, restricted, "no-partial-write")
			read, err := restricted.ReadUpdate(ctx, ref, input("create", true))
			if err != nil || read.Update == nil {
				t.Fatal(err)
			}
			if _, err := restricted.CompareAndSwap(ctx, *read.Update, Payload{Timers: []Timer{{ID: "timer", Generation: "1", DueMS: 1, DeadlineMS: 1}}}, nil); !errors.Is(err, ErrAuthority) {
				t.Fatal("missing scheduler permission did not refuse CAS", err)
			}
			keys := append(s.keys(ref), s.schedulerKeys()...)
			if exists := s.client.Exists(ctx, keys...).Val(); exists != 0 {
				t.Fatal("permission error left a partial protocol/index mutation", exists)
			}
		})
	}
}

func TestSchedulerCorruptProjectionFailsBeforeProtocolMutation(t *testing.T) {
	for _, keyIndex := range []int{0, 1} {
		s := testStore(t, nil)
		ctx := context.Background()
		if err := s.client.Set(ctx, s.schedulerKeys()[keyIndex], "wrong-type", 0).Err(); err != nil {
			t.Fatal(err)
		}
		ref := group(t, s, uuid.NewString())
		read, err := s.ReadUpdate(ctx, ref, input("create", true))
		if err != nil || read.Update == nil {
			t.Fatal(err)
		}
		if _, err := s.CompareAndSwap(ctx, *read.Update, Payload{}, nil); !errors.Is(err, ErrCorrupt) {
			t.Fatal("wrong projection type did not refuse CAS", err)
		}
		if exists := s.client.Exists(ctx, s.keys(ref)...).Val(); exists != 0 {
			t.Fatal("projection error partially mutated protocol/index state", exists)
		}
	}
}
