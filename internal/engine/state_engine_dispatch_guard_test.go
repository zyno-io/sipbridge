package engine

import "testing"

func TestStateDispatchReleaseCannotDeleteSuccessorGuard(t *testing.T) {
	engine := &StateEngine{dispatching: make(map[string]bool)}
	predecessor := engine.acquireDispatch("effect")
	if predecessor == nil || engine.acquireDispatch("effect") != nil {
		t.Fatal("same effect admitted concurrent predecessor RPCs")
	}
	predecessor()
	successor := engine.acquireDispatch("effect")
	if successor == nil {
		t.Fatal("released predecessor blocked its successor")
	}
	predecessor() // The old deferred release runs after the new owner acquired.
	if !engine.dispatching["effect"] || engine.acquireDispatch("effect") != nil {
		t.Fatal("predecessor's repeated release deleted its successor's guard")
	}
	successor()
	if len(engine.dispatching) != 0 {
		t.Fatal("successor did not reclaim its exact transport guard")
	}
}
