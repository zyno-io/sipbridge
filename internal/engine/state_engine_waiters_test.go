package engine

import (
	"context"
	"testing"
	"time"

	"sipbridge/internal/execution"
	"sipbridge/internal/state"
)

func TestStateOperationSignalsCoalesceAndRetireWithoutCrossingIncarnations(t *testing.T) {
	e := &StateEngine{}
	ref := state.Ref{ID: "group", Incarnation: "first"}
	identity := stateOperationIdentity{"context", "operation"}
	first, stopFirst := e.watchOperation(ref, identity)
	second, stopSecond := e.watchOperation(ref, identity)
	other, stopOther := e.watchOperation(state.Ref{ID: ref.ID, Incarnation: "successor"}, identity)
	defer stopFirst()
	defer stopSecond()
	defer stopOther()
	observation := localObservationFixture(t, ref, 200)
	committed := &execution.Committed{Ref: ref, Revision: "1", LocalObservations: []state.LocalObservation{observation}}
	e.publishLocalObservations(committed)
	e.publishLocalObservations(committed)
	if len(first) != 1 || len(second) != 1 || len(other) != 0 {
		t.Fatal("notifications crossed incarnations or accumulated")
	}
	<-first
	stopFirst()
	e.publishLocalObservations(committed)
	if len(first) != 0 || len(second) != 1 {
		t.Fatal("retired command received notifications")
	}
	stopSecond()
	stopOther()
	if len(e.waiters) != 0 {
		t.Fatal("finished commands leaked waiter entries")
	}
}

func TestStateOperationSignalWakesWithoutWaitingForFallback(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	signal := make(chan struct{}, 1)
	signal <- struct{}{}
	// An hour-long fallback makes failure deterministic without tight timing.
	if !waitStateOperationSignal(ctx, time.Hour, signal) {
		t.Fatal("committed progress notification was ignored")
	}
	cancel()
	signal <- struct{}{}
	if waitStateOperationSignal(ctx, time.Hour, signal) {
		t.Fatal("notification revived a cancelled waiter")
	}
}
