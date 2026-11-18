package control

import (
	"context"
	"testing"

	pb "sipbridge/internal/controlpb"
)

func TestHasReadyPeerUsesCurrentEstablishedCachedStatusWithoutAllocating(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p := &session{ctx: ctx, established: true}
	s := &Server{claims: map[string]*claim{"controller": {current: p}}}
	assertReady := func(want bool) {
		t.Helper()
		if s.HasReadyPeer() != want || (len(s.ReadyPeers()) > 0) != want {
			t.Fatal("cached readiness predicates disagree")
		}
	}
	assertReady(false)
	p.status = &pb.SessionStatus{RetainedReady: true}
	assertReady(false)
	p.status.BindingsReady = true
	assertReady(true)
	if allocations := testing.AllocsPerRun(100, func() { s.HasReadyPeer() }); allocations != 0 {
		t.Fatalf("cached readiness allocated %v objects", allocations)
	}
	p.established = false
	assertReady(false)
	p.established = true
	cancel()
	assertReady(false)
	s.claims["controller"].current = nil
	assertReady(false)
}
