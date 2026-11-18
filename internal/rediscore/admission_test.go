package rediscore

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

func TestInstanceAdmissionCapacity(t *testing.T) {
	store, client, _ := fixture(t)
	store.instanceAdmission = true
	store.namespaceContexts, store.namespaceBytes = maxExactInteger, maxExactInteger
	ctx := context.Background()
	deadline := time.Now().Add(time.Minute)
	var created []Identity
	t.Cleanup(func() {
		keys := make([]string, 0, len(created))
		for _, id := range created {
			keys = append(keys, store.contextKey(id.Context))
		}
		if len(keys) > 0 {
			if err := client.Del(ctx, keys...).Err(); err != nil {
				t.Error(err)
			}
		}
	})
	create := func(name, instance string, class AdmissionClass, source string, want error) Identity {
		t.Helper()
		id := Identity{Context: name, Incarnation: "first"}
		var credit int64
		if class == AdmissionOutbound {
			credit = store.maxBytes
		}
		_, err := store.Commit(ctx, Publication{Identity: id, Create: true, Deadline: deadline, CreditDelta: credit, Admission: Admission{Instance: instance, Class: class, Source: source}})
		if !errors.Is(err, want) {
			t.Fatalf("%s: want %v, got %v", name, want, err)
		}
		if want == nil {
			created = append(created, id)
		}
		return id
	}
	move := func(id Identity, class AdmissionClass) {
		t.Helper()
		_, err := store.Commit(ctx, Publication{Identity: id, Revision: 1, Deadline: deadline, Admission: Admission{Class: class}})
		if err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 32; i++ {
		create(fmt.Sprintf("pending-%d", i), "pod-a", AdmissionPending, "source", nil)
	}
	create("overflow", "pod-a", AdmissionPending, "source", ErrCapacity)
	create("same-source-other-pod", "pod-b", AdmissionPending, "source", nil)
	create("same-pod-other-source", "pod-a", AdmissionPending, "second", nil)
	move(created[0], AdmissionRefusal)
	move(created[1], AdmissionInbound)
	create("source-capacity-reclaimed", "pod-a", AdmissionPending, "source", nil)
	if err := store.RetireContext(ctx, created[0], 2, deadline, true); err != nil {
		t.Fatal(err)
	}
	if count, err := client.HExists(ctx, store.capacityKey(), "admission:pod-a:refusal").Result(); count || err != nil {
		t.Fatalf("empty refusal counter was retained: %v %v", count, err)
	}
	firstNativeRefusal := create("native-refusal-0", "pod-refusal", AdmissionNativeRefusal, "", nil)
	for i := 1; i < 1024; i++ {
		create(fmt.Sprintf("native-refusal-%d", i), "pod-refusal", AdmissionNativeRefusal, "", nil)
	}
	create("native-refusal-overflow", "pod-refusal", AdmissionNativeRefusal, "", ErrCapacity)
	pending := create("pending-after-native-refusals", "pod-refusal", AdmissionPending, "source", nil)
	move(pending, AdmissionInbound)
	if err := store.RetireContext(ctx, firstNativeRefusal, 1, deadline, true); err != nil {
		t.Fatal(err)
	}
	create("native-refusal-capacity-reclaimed", "pod-refusal", AdmissionNativeRefusal, "", nil)
	for _, pod := range []string{"pod-a", "pod-b"} {
		for i := 0; i < 512; i++ {
			in := create(fmt.Sprintf("%s-in-%d", pod, i), pod, AdmissionPending, "normal", nil)
			move(in, AdmissionInbound)
			create(fmt.Sprintf("%s-out-%d", pod, i), pod, AdmissionOutbound, "", nil)
		}
	}
	size, err := client.HGet(ctx, store.capacityKey(), "bytes").Int64()
	if err != nil || size <= DefaultNamespaceMaxBytes {
		t.Fatalf("instance admissions did not exceed the former namespace limit: %d %v", size, err)
	}
	for i := 512; i < 1024; i++ {
		create(fmt.Sprintf("pod-a-out-%d", i), "pod-a", AdmissionOutbound, "", nil)
	}
	create("pod-a-overflow", "pod-a", AdmissionOutbound, "", ErrCapacity)
	create("pod-b-headroom", "pod-b", AdmissionOutbound, "", nil)
}
