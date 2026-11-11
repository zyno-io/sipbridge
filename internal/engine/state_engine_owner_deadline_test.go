package engine

import (
	"strconv"
	"testing"

	"google.golang.org/protobuf/proto"
	"sipbridge/internal/state"
)

func TestNativeOwnerAdmissionSchedulesObservedLeaseExpiry(t *testing.T) {
	for _, backend := range []string{state.MemoryBackend, state.RedisBackend} {
		t.Run(backend, func(t *testing.T) {
			f := newNativeOwnerFixtureBackend(t, backend)
			ref := f.admit(t)
			record, group := stateTestLoad(t, f.engine, ref)
			c := group.Contexts[ref.Id]
			lease := record.ApplicationLeases[f.engine.applicationLeaseID(f.app)]
			if c.OwnerProbeAtMS != lease.ExpiresAtMS {
				t.Fatalf("owner check %d does not follow observed lease expiry %d", c.OwnerProbeAtMS, lease.ExpiresAtMS)
			}
			r := &stateReduction{engine: f.engine}
			id := r.timerID(ref.Id, "owner", "probe", "")
			for _, timer := range record.Payload.Timers {
				if timer.ID == id {
					if timer.DueMS != lease.ExpiresAtMS || timer.Generation != strconv.FormatInt(lease.ExpiresAtMS, 10) {
						t.Fatalf("retained owner timer did not preserve the lease deadline: %+v", timer)
					}
					return
				}
			}
			t.Fatal("native admission did not retain an owner-expiry timer")
		})
	}
}

func TestNativeOwnerDeadlineFollowsRenewalWithoutProtocolWork(t *testing.T) {
	f := newNativeOwnerFixtureBackend(t, state.MemoryBackend)
	ref := f.admit(t)
	record, group := stateTestLoad(t, f.engine, ref)
	c := group.Contexts[ref.Id]
	now := c.OwnerProbeAtMS
	id := f.engine.applicationLeaseID(f.app)
	lease := record.ApplicationLeases[id]
	lease.CheckedAtMS, lease.ExpiresAtMS = now, now+30000
	record.ApplicationLeases[id] = lease
	r := &stateReduction{engine: f.engine, record: record, group: group, payload: record.Payload, now: now}
	r.in.Timer, r.in.Generation = r.timerID(ref.Id, "owner", "probe", ""), strconv.FormatInt(now, 10)
	before := proto.Clone(c.Snapshot)
	effects := len(r.payload.Effects)
	handled, err := r.nativeOwnerTimer()
	if err != nil || !handled || c.OwnerProbeAtMS != lease.ExpiresAtMS {
		t.Fatalf("renewed owner was not rescheduled to its actual deadline: %v", err)
	}
	if !proto.Equal(before, c.Snapshot) || len(r.payload.Effects) != effects || c.OwnerLost {
		t.Fatal("a healthy owner deadline check changed application state or emitted protocol work")
	}
	// A stale five-second check cannot restore periodic work after rescheduling.
	r.now, lease.CheckedAtMS = now+5000, now+5000
	record.ApplicationLeases[id] = lease
	r.in.Generation = strconv.FormatInt(now, 10)
	if _, err = r.nativeOwnerTimer(); err != nil || c.OwnerProbeAtMS != lease.ExpiresAtMS {
		t.Fatal("a stale timer changed the renewed deadline")
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
