package engine

import (
	"context"
	"testing"
	"time"

	pb "sipbridge/internal/controlpb"
)

func TestResidentOwnerExpiryOnlyTouchesMatchingTerm(t *testing.T) {
	app := &pb.ProcessRef{InstanceId: "app", Epoch: "one"}
	e := &ResidentMemory{ctx: context.Background(), contexts: make(map[string]*residentContext), deadlineWake: make(chan struct{}, 1)}
	makeContext := func(id, term string) *residentContext {
		return &residentContext{snapshot: &pb.ContextSnapshot{Context: &pb.ContextRef{Id: id}, Application: app, ApplicationLeaseTerm: term}, transactions: make(map[string]*residentTransaction), dialogs: make(map[string]*residentDialog), outputs: make(map[string]*residentOutput)}
	}
	old := makeContext("old", "term-old")
	live := makeContext("live", "term-new")
	e.contexts["old"], e.contexts["live"] = old, live
	old.mu.Lock()
	e.indexResidentOwnerLocked(old)
	old.mu.Unlock()
	live.mu.Lock()
	e.indexResidentOwnerLocked(live)
	live.mu.Unlock()
	e.expireResidentOwner(app, "term-old")
	old.mu.Lock()
	if !old.ownerLost || old.ownerCleanupAt.IsZero() {
		t.Fatal("old owner was not fenced")
	}
	old.ownerCleanupAt = time.Now().Add(-time.Second)
	e.finishResidentOwnerCleanupLocked(old)
	ended := old.snapshot.Ended
	old.mu.Unlock()
	live.mu.Lock()
	liveLost := live.ownerLost
	live.mu.Unlock()
	if !ended || liveLost {
		t.Fatal("cleanup missed old owner or fenced replacement term")
	}
}
