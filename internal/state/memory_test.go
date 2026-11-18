package state

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
)

func memoryClockStore(t *testing.T, modify func(*Config)) (*MemoryStore, *time.Time) {
	t.Helper()
	cfg := contractConfig()
	if modify != nil {
		modify(&cfg)
	}
	now := time.Now()
	store, err := newMemory(cfg, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	return store, &now
}
func TestMemoryBoundsRetirementAndStaleIncarnation(t *testing.T) {
	for _, bound := range []string{"groups", "bytes"} {
		t.Run(bound, func(t *testing.T) {
			s, now := memoryClockStore(t, func(c *Config) {
				if bound == "groups" {
					c.Memory.MaxGroups = 1
				} else {
					c.Memory.MaxBytes = int64(c.Limits.GroupBytes)
				}
			})
			ctx := context.Background()
			ref := contractRef(t, s, "capacity")
			first := contractRead(t, s, ref, contractInput("first", true))
			if _, err := s.CompareAndSwap(ctx, first, Payload{}, nil); err != nil {
				t.Fatal(err)
			}
			other := contractRead(t, s, contractRef(t, s, "other"), contractInput("other", true))
			if _, err := s.CompareAndSwap(ctx, other, Payload{}, nil); !errors.Is(err, ErrCapacity) {
				t.Fatal("new group exceeded capacity", err)
			}
			terminal := contractRead(t, s, first.Record.Ref, contractInput("terminal", false))
			if _, err := s.CompareAndSwap(ctx, terminal, Payload{Retention: Retention{Terminal: true}}, nil); err != nil {
				t.Fatal("capacity blocked existing group", err)
			}
			if err := s.Retire(ctx, first.Record.Ref); !errors.Is(err, ErrNotReady) {
				t.Fatal("retired live replay", err)
			}
			*now = now.Add(11 * time.Second)
			if err := s.Retire(ctx, first.Record.Ref); err != nil {
				t.Fatal(err)
			}
			replacementInput := contractInput("replacement", true)
			replacementInput.DeadlineMS = now.UnixMilli() + 5000
			replacementInput.ReceiptUntilMS = now.UnixMilli() + 10000
			replacement := contractRead(t, s, ref, replacementInput)
			if _, err := s.CompareAndSwap(ctx, replacement, Payload{}, nil); err != nil {
				t.Fatal(err)
			}
			if _, err := s.Load(ctx, first.Record.Ref); !errors.Is(err, ErrStale) {
				t.Fatal("old incarnation reached replacement", err)
			}
		})
	}
}
func TestMemoryOwnerExpiryResumeSealAndRetention(t *testing.T) {
	s, now := memoryClockStore(t, nil)
	ctx := context.Background()
	lease, err := s.AcquireApplicationLease(ctx, "owner", uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	in := contractInput("create", true)
	in.ApplicationLeaseIDs = []string{"owner"}
	update := contractRead(t, s, contractRef(t, s, "owner"), in)
	payload := Payload{ApplicationLeaseIDs: []string{"owner"}, CommitLeaseGuards: []ApplicationLeaseRef{{ID: "owner", Term: lease.Term}}}
	if _, err := s.CompareAndSwap(ctx, update, payload, nil); err != nil {
		t.Fatal(err)
	}
	*now = now.Add(31 * time.Second)
	if _, err := s.RenewApplicationLease(ctx, "owner", lease.Term); !errors.Is(err, ErrExpired) {
		t.Fatal("ordinary renewal revived expiry", err)
	}
	observed, err := s.ReadApplicationLease(ctx, "owner")
	if err != nil || observed.RecoveryUntilMS != now.UnixMilli()+5000 {
		t.Fatal("missing recovery grace", err)
	}
	// A resume can win before sealing, retaining the same term and references.
	resumed, err := s.ResumeApplicationLease(ctx, "owner", lease.Term)
	if err != nil || resumed.Term != lease.Term {
		t.Fatal("exact term did not resume", err)
	}
	*now = now.Add(31 * time.Second)
	observed, err = s.ReadApplicationLease(ctx, "owner")
	if err != nil {
		t.Fatal(err)
	}
	*now = now.Add(5 * time.Second)
	fenceInput := contractInput("fence", false)
	fenceInput.Proof = true
	fenceInput.DeadlineMS = now.UnixMilli() + 5000
	fenceInput.ReceiptUntilMS = now.UnixMilli() + 10000
	fence := contractRead(t, s, update.Record.Ref, fenceInput)
	payload.CommitLeaseGuards = nil
	payload.CommitLeaseFences = []ApplicationLeaseRef{{ID: "owner", Term: lease.Term}}
	if _, err := s.CompareAndSwap(ctx, fence, payload, nil); err != nil {
		t.Fatal("seal did not commit", err)
	}
	if _, err := s.ResumeApplicationLease(ctx, "owner", lease.Term); !errors.Is(err, ErrExpired) {
		t.Fatal("sealed owner resumed", err)
	}
	*now = now.Add(3 * time.Minute)
	if retained, err := s.ReadApplicationLease(ctx, "owner"); err != nil || retained.Term != lease.Term {
		t.Fatal("referenced owner expired", err)
	}
	releaseInput := contractInput("release", false)
	releaseInput.DeadlineMS = now.UnixMilli() + 5000
	releaseInput.ReceiptUntilMS = now.UnixMilli() + 10000
	release := contractRead(t, s, update.Record.Ref, releaseInput)
	if _, err := s.CompareAndSwap(ctx, release, Payload{Retention: Retention{Terminal: true}}, nil); err != nil {
		t.Fatal(err)
	}
	*now = now.Add(121 * time.Second)
	if gone, err := s.ReadApplicationLease(ctx, "owner"); err != nil || gone.Term != "" {
		t.Fatal("idle owner retained forever", err)
	}
}
func TestMemoryCommitDeadlineAndOwnedInput(t *testing.T) {
	s, now := memoryClockStore(t, nil)
	ctx := context.Background()
	in := contractInput("owned", true)
	in.Payload = []byte("original")
	in.ApplicationLeaseIDs = []string{"owner"}
	update := contractRead(t, s, contractRef(t, s, "owned"), in)
	in.Payload[0] = 'X'
	in.ApplicationLeaseIDs[0] = "changed"
	if string(update.Input.Payload) != "original" || update.Input.ApplicationLeaseIDs[0] != "owner" {
		t.Fatal("input shares caller buffers")
	}
	*now = now.Add(6 * time.Second)
	if _, err := s.CompareAndSwap(ctx, update, Payload{}, nil); !errors.Is(err, ErrExpired) {
		t.Fatal("late commit accepted", err)
	}
	if len(s.groups) != 0 {
		t.Fatal("expired computation allocated group")
	}
}
func TestMemoryAuthorityLifecycleAndCancellation(t *testing.T) {
	cfg := contractConfig()
	a, err := NewMemory(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	b, err := NewMemory(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	if a.Authority() == b.Authority() {
		t.Fatal("independent stores share authority")
	}
	update := contractRead(t, a, contractRef(t, a, "restart"), contractInput("old", true))
	if _, err := b.CompareAndSwap(context.Background(), update, Payload{}, nil); !errors.Is(err, ErrStale) {
		t.Fatal("foreign token accepted", err)
	}
	a.gate <- struct{}{}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := a.Ready(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("blocked store ignored cancellation", err)
	}
	a.unlock()
	a.Close()
	if err := a.Ready(context.Background()); !errors.Is(err, ErrAuthority) || a.Available() {
		t.Fatal("closed store available", err)
	}
	if _, err := a.CompareAndSwap(context.Background(), update, Payload{}, nil); !errors.Is(err, ErrAuthority) {
		t.Fatal("closed store accepted commit", err)
	}
}
func TestMemorySchedulingHasNoStaleHeapGrowth(t *testing.T) {
	s, now := memoryClockStore(t, nil)
	ctx := context.Background()
	ref := contractRef(t, s, "schedule")
	for n := 0; n < 100; n++ {
		in := contractInput("tick", n == 0)
		in.Proof = true
		in.Kind = "timer"
		update := contractRead(t, s, ref, in)
		payload := Payload{Timers: []Timer{{ID: "timer", Generation: "1", DueMS: now.UnixMilli(), DeadlineMS: now.UnixMilli() + 1000}}}
		if _, err := s.CompareAndSwap(ctx, update, payload, nil); err != nil {
			t.Fatal(err)
		}
		ref = update.Record.Ref
	}
	index := &s.schedules[ref.Partition]
	if index.due.Len() != 1 || len(index.due.byID) != 1 {
		t.Fatal("stale schedule entries accumulated")
	}
	clear := contractRead(t, s, ref, contractInput("clear", false))
	if _, err := s.CompareAndSwap(ctx, clear, Payload{}, nil); err != nil {
		t.Fatal(err)
	}
	if index.due.Len() != 0 || len(index.due.byID) != 0 {
		t.Fatal("obsolete schedule retained")
	}
}

func TestMemoryClockRefusalRecoversWithoutResettingState(t *testing.T) {
	s, now := memoryClockStore(t, nil)
	ctx := context.Background()
	update := contractRead(t, s, contractRef(t, s, "clock"), contractInput("clock", true))
	if _, err := s.CompareAndSwap(ctx, update, Payload{}, nil); err != nil {
		t.Fatal(err)
	}
	*now = now.Add(-time.Second)
	if err := s.Ready(ctx); !errors.Is(err, ErrClock) || s.Available() {
		t.Fatal("backward clock accepted", err)
	}
	if err := s.Ready(ctx); err != nil || !s.Available() {
		t.Fatal("clock recovery latched unavailable", err)
	}
	if _, err := s.Load(ctx, update.Record.Ref); err != nil {
		t.Fatal("clock refusal erased state", err)
	}
}
