package engine

import (
	"context"
	"errors"
	"testing"
	"time"

	"sipbridge/internal/control"
	pb "sipbridge/internal/controlpb"
)

func TestResidentControllerLeaseOneEntryAndFencing(t *testing.T) {
	e := &ResidentMemory{authority: NewResidentMemoryAuthority("test", "1"), leases: make(map[string]*residentLease), deadlineWake: make(chan struct{}, 1)}
	app := &pb.ProcessRef{InstanceId: "app", Epoch: "one"}
	ctx := context.Background()
	first, err := e.ApplicationLease(ctx, app, "")
	if err != nil {
		t.Fatal(err)
	}
	for range 100 {
		renewed, err := e.ApplicationLease(ctx, app, first.Term)
		if err != nil || renewed.Term != first.Term {
			t.Fatal("renewal changed term", err)
		}
	}
	if len(e.leases) != 1 || len(e.deadlines.heap) != 1 {
		t.Fatal("renewal retained extra records")
	}
	entry := e.leases[residentControllerKey(app)]
	stale := residentDeadline{key: residentDeadlineKey{kind: residentControllerDeadline, record: residentControllerKey(app)}, generation: entry.generation - 1}
	entry.expires = time.Now().Add(-time.Second)
	if !e.expireController(stale) || len(e.leases) != 1 {
		t.Fatal("stale deadline removed current lease")
	}
	if _, err = e.ApplicationLease(ctx, app, first.Term); !errors.Is(err, control.ErrApplicationLeaseFenced) {
		t.Fatal("expired term resumed", err)
	}
	current := stale
	current.generation = entry.generation
	if !e.expireController(current) || len(e.leases) != 0 {
		t.Fatal("expired lease retained")
	}
	fresh, err := e.ApplicationLease(ctx, app, "")
	if err != nil || fresh.Term == first.Term {
		t.Fatal("fresh hello revived expired term", err)
	}
	if _, err = e.ApplicationLease(ctx, app, first.Term); !errors.Is(err, control.ErrApplicationLeaseFenced) {
		t.Fatal("old term renewed replacement", err)
	}
}

func TestResidentNewHelloExpiresPreviousOwnerTerm(t *testing.T) {
	e := &ResidentMemory{ctx: context.Background(), authority: NewResidentMemoryAuthority("test", "1"), leases: make(map[string]*residentLease), contexts: make(map[string]*residentContext), deadlineWake: make(chan struct{}, 1)}
	app := &pb.ProcessRef{InstanceId: "app", Epoch: "one"}
	first, err := e.ApplicationLease(context.Background(), app, "")
	if err != nil {
		t.Fatal(err)
	}
	c := &residentContext{snapshot: &pb.ContextSnapshot{Context: &pb.ContextRef{Id: "old"}, Application: app, ApplicationLeaseTerm: first.Term}}
	e.contexts["old"] = c
	c.mu.Lock()
	e.indexResidentOwnerLocked(c)
	c.mu.Unlock()
	e.mu.Lock()
	e.leases[residentControllerKey(app)].expires = time.Now().Add(-time.Second)
	e.mu.Unlock()
	second, err := e.ApplicationLease(context.Background(), app, "")
	if err != nil || second.Term == first.Term {
		t.Fatal("new hello did not replace expired term", err)
	}
	e.wg.Wait()
	c.mu.Lock()
	lost := c.ownerLost
	c.mu.Unlock()
	if !lost {
		t.Fatal("new hello overwrote the only deadline for old-owner cleanup")
	}
}

func TestResidentControlDisconnectFencesOldTermAndCannotExpireNewTerm(t *testing.T) {
	e := &ResidentMemory{ctx: context.Background(), authority: NewResidentMemoryAuthority("test", "1"), leases: make(map[string]*residentLease), contexts: make(map[string]*residentContext), deadlineWake: make(chan struct{}, 1)}
	app := &pb.ProcessRef{InstanceId: "app", Epoch: "one"}
	ctx := context.Background()
	first, err := e.ApplicationLease(ctx, app, "")
	if err != nil {
		t.Fatal(err)
	}
	c := &residentContext{snapshot: &pb.ContextSnapshot{Context: &pb.ContextRef{Id: "call"}, Application: app, ApplicationLeaseTerm: first.Term}}
	e.contexts["call"] = c
	c.mu.Lock()
	e.indexResidentOwnerLocked(c)
	c.mu.Unlock()
	if err = e.DisconnectApplication(ctx, app, first.Term); err != nil {
		t.Fatal(err)
	}
	if _, err = e.ApplicationLease(ctx, app, first.Term); !errors.Is(err, control.ErrApplicationLeaseFenced) {
		t.Fatal("closed control session renewed old authority", err)
	}
	next, err := e.ApplicationLease(ctx, app, "")
	if err != nil || next.Term == first.Term {
		t.Fatal("new session restored the old control term", err)
	}
	e.wg.Wait()
	c.mu.Lock()
	lost := c.ownerLost
	c.mu.Unlock()
	if !lost {
		t.Fatal("closed control session kept its call owner")
	}
	if err = e.DisconnectApplication(ctx, app, first.Term); err != nil {
		t.Fatal(err)
	}
	if _, err = e.ApplicationLease(ctx, app, next.Term); err != nil {
		t.Fatal("late old disconnect expired a new control term", err)
	}
	if len(e.leases) != 1 || len(e.deadlines.heap) != 2 {
		t.Fatal("disconnect added an owner registry or duplicate controller deadline")
	}
}
