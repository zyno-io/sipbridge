package engine

import (
	"context"

	pb "sipbridge/internal/controlpb"
	"testing"

	"google.golang.org/protobuf/proto"
	"sipbridge/internal/state"
)

func TestNativeOwnerAdmissionUsesSharedExpiryWithoutContextProbe(t *testing.T) {
	for _, backend := range []string{state.MemoryBackend, state.RedisBackend} {
		t.Run(backend, func(t *testing.T) {
			f := newNativeOwnerFixtureBackend(t, backend)
			ref := f.admit(t)
			record, group := stateTestLoad(t, f.engine, ref)
			c := group.Contexts[ref.Id]
			lease := record.ApplicationLeases[f.engine.applicationLeaseID(f.app)]
			if c.OwnerProbeAtMS != 0 {
				t.Fatal("healthy context retained an owner-probe timestamp")
			}
			r := &stateReduction{engine: f.engine}
			id := r.timerID(ref.Id, "owner", "probe", "")
			for _, timer := range record.Payload.Timers {
				if timer.ID == id {
					t.Fatal("healthy context retained a periodic owner timer")
				}
			}
			head, err := f.engine.store.ApplicationExpiryHead(context.Background())
			if err != nil || head.DueMS != lease.ExpiresAtMS {
				t.Fatal("owner lease has no shared expiry deadline", err)
			}
		})
	}
}

func TestNativeOwnerRenewalAndRepeatedSweepDoNotCommitProtocolState(t *testing.T) {
	for _, backend := range []string{state.MemoryBackend, state.RedisBackend} {
		t.Run(backend, func(t *testing.T) {
			f := newNativeOwnerFixtureBackend(t, backend)
			ref := f.admit(t)
			before, original := stateTestLoad(t, f.engine, ref)
			if _, err := f.engine.ApplicationLease(context.Background(), f.app, f.term); err != nil {
				t.Fatal(err)
			}
			for _, operation := range []string{"first-owner-check", "repeated-owner-check"} {
				result := f.apply(t, ref, operation, stateEngineInput{Kind: "owner-check"})
				if result.Certainty != pb.OperationResult_COMPLETED || result.Error != "" {
					t.Fatal("healthy owner check did not return definite no-op")
				}
			}
			after, current := stateTestLoad(t, f.engine, ref)
			if after.Revision != before.Revision || !proto.Equal(original.Contexts[ref.Id].Snapshot, current.Contexts[ref.Id].Snapshot) || current.Contexts[ref.Id].OwnerLost {
				t.Fatal("owner renewal/no-op sweep rewrote protocol state")
			}
		})
	}
}

func TestNativeOwnerDeadlinePreservesShortLeasesAndFixedGrace(t *testing.T) {
	f := newNativeOwnerFixtureBackend(t, state.MemoryBackend)
	ref := f.admit(t)
	record, group := stateTestLoad(t, f.engine, ref)
	c := group.Contexts[ref.Id]
	id := f.engine.applicationLeaseID(f.app)
	lease := record.ApplicationLeases[id]
	now := lease.CheckedAtMS
	r := &stateReduction{engine: f.engine, record: record, group: group, now: now}
	for _, test := range []struct {
		name    string
		expires int64
		grace   int64
		want    int64
	}{
		{"short live lease", now + 1000, 0, now + 1000},
		{"expired fixed grace", now - 1000, now + 1500, now + 1500},
	} {
		t.Run(test.name, func(t *testing.T) {
			lease.ExpiresAtMS, lease.RecoveryUntilMS = test.expires, test.grace
			record.ApplicationLeases[id] = lease
			deadline, err := r.nativeOwnerProbeDeadline(c)
			if err != nil || deadline != test.want {
				t.Fatalf("deadline %d, want %d: %v", deadline, test.want, err)
			}
		})
	}
	lease.ExpiresAtMS, lease.RecoveryUntilMS = now-1000, now
	record.ApplicationLeases[id] = lease
	if _, err := r.nativeOwnerProbeDeadline(c); err == nil {
		t.Fatal("expired recovery acquired another future probe deadline")
	}
	lease.ExpiresAtMS, lease.Term = now+30000, "successor"
	record.ApplicationLeases[id] = lease
	if _, err := r.nativeOwnerProbeDeadline(c); err == nil {
		t.Fatal("a successor term extended the original context's owner deadline")
	}
}
