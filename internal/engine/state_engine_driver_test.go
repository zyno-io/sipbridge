package engine

import (
	"bytes"
	"context"
	"errors"
	"net"
	"reflect"
	"testing"
	"time"

	"sipbridge/internal/control"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/execution"
	executionfixture "sipbridge/internal/execution/fixture"
	"sipbridge/internal/state"
)

func legacyFixtureRuntime(t *testing.T, engine *StateEngine) *executionfixture.Runtime {
	t.Helper()
	legacy, ok := engine.runtime.(*executionfixture.Runtime)
	if !ok || legacy.UsesCAS() {
		t.Fatal("test requires an explicitly constructed legacy fixture runtime")
	}
	return legacy
}

func assertCASPendingRefused(t *testing.T, engine *StateEngine, ref state.Ref) {
	t.Helper()
	before, err := engine.store.Load(context.Background(), ref)
	if err != nil || before.Pending == nil || !engine.runtime.UsesCAS() {
		t.Fatalf("test requires retained pending input and CAS execution: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	deadline := time.Now().Add(30 * time.Millisecond).UnixMilli()
	_, err = engine.runtime.Apply(ctx, ref, state.Input{ID: "cas-pending-refusal", Kind: "locate", Fingerprint: "cas-pending-refusal",
		DeadlineMS: deadline, ReceiptUntilMS: deadline})
	var timeout net.Error
	deadlineFailure := errors.Is(err, context.DeadlineExceeded) || errors.Is(err, state.ErrExpired) ||
		(errors.As(err, &timeout) && timeout.Timeout())
	if !errors.Is(err, execution.ErrUncommitted) || errors.Is(err, state.ErrUnknown) || !deadlineFailure {
		t.Fatalf("unexpected pending input did not fail closed within the original budget: %v", err)
	}
	after, err := engine.store.Load(context.Background(), ref)
	if err != nil || after.Revision != before.Revision || !reflect.DeepEqual(after.Pending, before.Pending) || !reflect.DeepEqual(after.Payload, before.Payload) {
		t.Fatalf("CAS refusal changed retained legacy work: %v", err)
	}
}

func TestStateProductionUsesCASAndHistoricalConstructorIsFixtureOnly(t *testing.T) {
	for _, profile := range []struct {
		name     string
		versions state.Versions
		native   bool
	}{
		{name: "native", versions: NativeStateVersions(), native: true},
		{name: "historical", versions: state.Versions{Schema: "1", Reducer: "7", Timers: "1"}},
	} {
		for _, mode := range []string{"", "legacy", "cas", "invalid"} {
			t.Run(profile.name+"/"+mode, func(t *testing.T) {
				key := bytes.Repeat([]byte{7}, 32)
				store, err := state.OpenDeferred(state.Config{Redis: state.RedisOptions{Address: "127.0.0.1:1"}, Prefix: "sipstate:driver-test",
					Namespace: "driver-test", NamespaceKey: key, Generation: "generation", Versions: profile.versions, Partitions: 1})
				if err != nil {
					t.Fatal(err)
				}
				defer store.Close()
				engine, err := newStateForFixture(context.Background(), Config{Process: &pb.ProcessRef{InstanceId: "driver-test", Epoch: "epoch"},
					AdvertiseAddress: "sip.example.test:5061", ContactURI: "sip:sipbridge@sip.example.test:5061;transport=tls",
					StateIdentityKey: key}, store, &control.Server{}, mode)
				if !profile.native && mode == "invalid" {
					if err == nil || engine != nil {
						t.Fatal("relay fixture accepted invalid execution selection")
					}
					return
				}
				if err != nil {
					t.Fatal(err)
				}
				defer engine.Close()
				wantCAS := profile.native || mode == "cas"
				if engine.runtime.UsesCAS() != wantCAS {
					t.Fatal("runtime selection does not match the owning profile")
				}
				_, exposesResume := engine.runtime.(interface {
					Resume(context.Context, state.Ref) (state.Receipt, error)
				})
				if exposesResume == wantCAS {
					t.Fatal("CAS execution exposed legacy Resume or relay fixture lost it")
				}
			})
		}
	}
}

func TestStateProductionSchedulerDoesNotResumeUnexpectedPending(t *testing.T) {
	f := newNativeOwnerFixture(t)
	ctx := context.Background()
	ref, err := f.engine.store.GroupForCallID("unexpected-scheduler-pending")
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second).UnixMilli()
	begun, err := f.engine.store.(*state.Store).Begin(ctx, ref, "old-worker", state.Input{ID: "unexpected", Kind: "locate", Fingerprint: "unexpected",
		Payload: []byte(`{}`), Create: true, DeadlineMS: deadline, ReceiptUntilMS: deadline + 120000})
	if err != nil || begun.Step == nil {
		t.Fatalf("could not create retained legacy fixture: %v", err)
	}
	ref = begun.Step.Record.Ref
	key := f.prefix + "{p0000}:group:" + ref.ID
	if err = f.client.HSet(ctx, key, "claimUntilMs", "0").Err(); err != nil {
		t.Fatal(err)
	}
	before, err := f.client.HGetAll(ctx, key).Result()
	if err != nil {
		t.Fatal(err)
	}
	f.engine.work, f.engine.background = make(chan struct{}, 64), make(chan struct{}, 48)
	f.engine.scheduled = make(map[string]bool)
	f.engine.scheduleGroup(ref, false)
	f.engine.wg.Wait()
	after, err := f.client.HGetAll(ctx, key).Result()
	if err != nil || !reflect.DeepEqual(after, before) {
		t.Fatalf("production scheduler resumed or changed unexpected pending input: %v", err)
	}
}
