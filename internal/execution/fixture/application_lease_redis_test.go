package fixture

import (
	"bytes"
	"context"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"sipbridge/internal/execution"
	"sipbridge/internal/state"
)

func TestApplicationLeaseCommitExpiryReloadsSameInput(t *testing.T) {
	for _, resume := range []bool{false, true} {
		t.Run(map[bool]string{false: "apply", true: "resume"}[resume], func(t *testing.T) {
			address := os.Getenv("SIPBRIDGE_STATE_TEST_REDIS")
			if address == "" {
				t.Skip("set SIPBRIDGE_STATE_TEST_REDIS for real Redis qualification")
			}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			cfg := state.Config{Redis: state.RedisOptions{Address: address}, Prefix: "sipstate:test", Namespace: "lease-runtime-" + uuid.NewString(),
				NamespaceKey: bytes.Repeat([]byte{7}, 32), Generation: "generation", Versions: state.Versions{Schema: "1", Reducer: "1", Timers: "1"}, Partitions: 2}
			store, err := state.Open(ctx, cfg)
			if err != nil {
				t.Fatal(err)
			}
			client := redis.NewClient(&redis.Options{Addr: address})
			prefix := cfg.Prefix + ":" + cfg.Namespace + ":"
			t.Cleanup(func() {
				defer store.Close()
				defer client.Close()
				var cursor uint64
				for {
					keys, next, err := client.Scan(context.Background(), cursor, prefix+"*", 128).Result()
					if err != nil {
						t.Error(err)
						return
					}
					if len(keys) > 0 {
						if err = client.Del(context.Background(), keys...).Err(); err != nil {
							t.Error(err)
						}
					}
					if next == 0 {
						return
					}
					cursor = next
				}
			})
			if err = store.Ready(ctx); err != nil {
				t.Fatal(err)
			}
			lease, err := store.AcquireApplicationLease(ctx, "owner", uuid.NewString())
			if err != nil {
				t.Fatal(err)
			}
			ref, err := store.GroupForCallID("paused-business")
			if err != nil {
				t.Fatal(err)
			}
			candidate := state.Input{ID: "original-operation", Kind: "business", Fingerprint: "original-fingerprint", Payload: []byte("immutable-request"),
				DeadlineMS: time.Now().Add(time.Second).UnixMilli(), ReceiptUntilMS: time.Now().Add(time.Minute).UnixMilli(), Create: true,
				ApplicationLeaseIDs: []string{"owner"}}
			if resume {
				admitted, err := store.Begin(ctx, ref, "crashed-worker", candidate)
				if err != nil {
					t.Fatal(err)
				}
				ref = admitted.Step.Record.Ref
				groupKey := prefix + "{p0000}:group:" + ref.ID
				if ref.Partition != 0 {
					groupKey = prefix + "{p0001}:group:" + ref.ID
				}
				if err = client.HSet(ctx, groupKey, "claimUntilMs", "0").Err(); err != nil {
					t.Fatal(err)
				}
			}
			paused, release := make(chan struct{}), make(chan struct{})
			var reductions atomic.Int32
			driver, err := New(store, execution.ReducerFunc(func(record state.Record, admitted state.Input, now int64) (state.Payload, []byte, error) {
				if admitted.ID != candidate.ID || admitted.Fingerprint != candidate.Fingerprint || !bytes.Equal(admitted.Payload, candidate.Payload) {
					t.Error("lease retry replaced the admitted operation")
				}
				observed := record.ApplicationLeases["owner"]
				if observed.CheckedAtMS != now || observed.Term != lease.Term {
					t.Error("reducer did not receive an atomic exact-term observation")
				}
				if reductions.Add(1) == 1 {
					close(paused)
					select {
					case <-release:
					case <-ctx.Done():
						return state.Payload{}, nil, ctx.Err()
					}
					return state.Payload{ProtocolState: []byte("forbidden-business"), ApplicationLeaseIDs: []string{"owner"},
						CommitLeaseGuards: []state.ApplicationLeaseRef{{ID: "owner", Term: lease.Term}}}, []byte("forbidden-success"), nil
				}
				if observed.ExpiresAtMS > now {
					t.Error("retry did not reload expired lease authority")
				}
				return state.Payload{ProtocolState: []byte("owner-fenced"), ApplicationLeaseIDs: []string{"owner"}}, []byte("refused"), nil
			}), "live-worker", 1)
			if err != nil {
				t.Fatal(err)
			}
			type result struct {
				receipt state.Receipt
				err     error
			}
			done := make(chan result, 1)
			go func() {
				var receipt state.Receipt
				var err error
				if resume {
					receipt, err = driver.Resume(ctx, ref)
				} else {
					receipt, err = driver.Apply(ctx, ref, candidate)
				}
				done <- result{receipt, err}
			}()
			select {
			case <-paused:
			case <-ctx.Done():
				t.Fatal("reducer did not reach the commit barrier")
			}
			if err = client.Eval(ctx, `
local clock=redis.call('TIME'); local now=tonumber(clock[1])*1000+math.floor(tonumber(clock[2])/1000)
redis.call('HSET',KEYS[1],'expiresAtMs',string.format('%.0f',now)); return now
`, []string{prefix + "{p0000}:application-lease:owner"}).Err(); err != nil {
				t.Fatal(err)
			}
			close(release)
			finished := <-done
			if finished.err != nil || string(finished.receipt.Outcome) != "refused" || reductions.Load() != 2 {
				t.Fatalf("stale computation was not retried safely: %+v reductions=%d", finished, reductions.Load())
			}
			current, err := store.Load(ctx, ref)
			if err != nil || string(current.Payload.ProtocolState) != "owner-fenced" || current.Pending != nil {
				t.Fatalf("expired business mutation escaped: %+v %v", current, err)
			}
		})
	}
}
