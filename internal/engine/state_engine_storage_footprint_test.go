package engine

import (
	"context"
	"crypto/sha256"
	"fmt"
	"testing"

	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/reflect/protoreflect"
	"sipbridge/internal/state"
	"sipbridge/internal/storagecodec"
)

func footprintNamespace(t *testing.T, client *redis.Client, prefix string) (int, int64) {
	t.Helper()
	ctx := context.Background()
	var cursor uint64
	var count int
	var total int64
	for {
		keys, next, err := client.Scan(ctx, cursor, prefix+"*", 128).Result()
		if err != nil {
			t.Fatal(err)
		}
		for _, key := range keys {
			size, err := client.MemoryUsage(ctx, key, 0).Result()
			if err != nil {
				t.Fatal(err)
			}
			total += size
			count++
		}
		if next == 0 {
			return count, total
		}
		cursor = next
	}
}

func footprintByteValues(message protoreflect.Message, unique map[[32]byte]int) (int, int) {
	total, values := 0, 0
	var visit func(protoreflect.Kind, protoreflect.Value)
	visit = func(kind protoreflect.Kind, value protoreflect.Value) {
		switch kind {
		case protoreflect.MessageKind:
			bytes, fields := footprintByteValues(value.Message(), unique)
			total, values = total+bytes, values+fields
		case protoreflect.BytesKind:
			data := value.Bytes()
			if len(data) > 0 {
				total += len(data)
				values++
				unique[sha256.Sum256(data)] = len(data)
			}
		}
	}
	message.Range(func(field protoreflect.FieldDescriptor, value protoreflect.Value) bool {
		switch {
		case field.IsMap():
			value.Map().Range(func(_ protoreflect.MapKey, item protoreflect.Value) bool {
				visit(field.MapValue().Kind(), item)
				return true
			})
		case field.IsList():
			list := value.List()
			for n := 0; n < list.Len(); n++ {
				visit(field.Kind(), list.Get(n))
			}
		default:
			visit(field.Kind(), value)
		}
		return true
	})
	return total, values
}

// Isolated synthetic Redis only. Both representations use the same actual
// native subscription/NOTIFY history and identical current scheduling indexes.
// MEMORY USAGE is per-key allocator accounting, not total Redis process RSS.
func TestNativeSubscriptionStorageFootprint(t *testing.T) {
	for _, transactions := range []int{2, 6, 16} {
		t.Run(fmt.Sprintf("transactions-%d", transactions), func(t *testing.T) {
			fixture := idleSubscriptionOwnerFixture(t, transactions, state.RedisBackend)
			client := redis.NewClient(&redis.Options{Addr: fixture.Config.Redis.Address})
			defer client.Close()
			ctx := context.Background()
			prefix := fixture.Config.Prefix + ":" + fixture.Config.Namespace + ":"
			key := fmt.Sprintf("%s{p%04d}:group:%s", prefix, fixture.Record.Ref.Partition, fixture.Record.Ref.ID)
			actual, err := client.HGet(ctx, key, "data").Bytes()
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := client.HSet(ctx, key, "data", actual).Err(); err != nil {
					t.Error(err)
				}
			}()
			var payload state.Payload
			if err := payload.UnmarshalBinary(actual); err != nil {
				t.Fatal(err)
			}
			var group stateGroup
			if err := group.UnmarshalBinary(payload.ProtocolState); err != nil {
				t.Fatal(err)
			}
			compactProtocol := payload.ProtocolState
			for _, c := range group.Contexts {
				if c.Result != nil {
					c.Result.Snapshot = stateSnapshot(c, fixture.Process)
					if err := stateMaterializeResultMessages(c.Result); err != nil {
						t.Fatal(err)
					}
				}
			}
			legacyProtocol, err := storagecodec.Marshal(storagecodec.Group, storageEncodeEngineStateGroup(group))
			if err != nil {
				t.Fatal(err)
			}
			payload.ProtocolState = legacyProtocol
			legacyPayload, err := payload.MarshalBinary()
			if err != nil {
				t.Fatal(err)
			}
			if err := client.HSet(ctx, key, "data", legacyPayload).Err(); err != nil {
				t.Fatal(err)
			}
			legacyKeys, legacyBytes := footprintNamespace(t, client, prefix)
			if err := client.HSet(ctx, key, "data", actual).Err(); err != nil {
				t.Fatal(err)
			}
			compactKeys, compactBytes := footprintNamespace(t, client, prefix)
			if compactKeys != legacyKeys || compactBytes > legacyBytes || len(compactProtocol) >= len(legacyProtocol) {
				t.Fatal("cached result compaction increased retained footprint or changed indexes")
			}
			var compactGroup stateGroup
			if err := compactGroup.UnmarshalBinary(compactProtocol); err != nil {
				t.Fatal(err)
			}
			unique := make(map[[32]byte]int)
			byteValues, fields := footprintByteValues(storageEncodeEngineStateGroup(compactGroup).ProtoReflect(), unique)
			uniqueBytes := 0
			for _, size := range unique {
				uniqueBytes += size
			}
			t.Logf("protocol bytes %d -> %d; namespace key-memory %d -> %d across %d unchanged keys; retained byte fields=%d total=%d unique=%d",
				len(legacyProtocol), len(compactProtocol), legacyBytes, compactBytes, compactKeys, fields, byteValues, uniqueBytes)
		})
	}
}
