package engine

import (
	"testing"

	"google.golang.org/protobuf/proto"

	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/execution"
	"sipbridge/internal/state"
)

func localObservationFixture(t *testing.T, ref state.Ref, status uint32) state.LocalObservation {
	t.Helper()
	result := &pb.OperationResult{Certainty: pb.OperationResult_COMPLETED, SipStatus: status,
		Snapshot: &pb.ContextSnapshot{Context: &pb.ContextRef{Id: "context", State: &pb.StateRef{GroupId: ref.ID, Incarnation: ref.Incarnation}}}}
	outcome, err := proto.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	return state.LocalObservation{ContextID: "context", OperationID: "operation", Outcome: outcome}
}

func TestLocalObservationMatchesIdentityAndMonotonicRevision(t *testing.T) {
	e := &StateEngine{}
	ref := state.Ref{ID: "group", Incarnation: "first"}
	signal, stop := e.watchOperation(ref, stateOperationIdentity{"context", "operation"})
	defer stop()
	other, stopOther := e.watchOperation(ref, stateOperationIdentity{"context", "other-operation"})
	defer stopOther()
	successor := state.Ref{ID: ref.ID, Incarnation: "second"}
	later, stopLater := e.watchOperation(successor, stateOperationIdentity{"context", "operation"})
	defer stopLater()
	publish := func(revision string, status uint32) {
		e.publishLocalObservations(&execution.Committed{Ref: ref, Revision: revision, LocalObservations: []state.LocalObservation{localObservationFixture(t, ref, status)}})
	}
	publish("3", 203)
	publish("2", 202)
	if len(signal) != 1 || len(other) != 0 || len(later) != 0 {
		t.Fatal("observation crossed an operation/incarnation or accumulated notifications")
	}
	result, err := e.observeLocalOperation(ref, "context", "operation", signal, "")
	if err != nil || result.GetSipStatus() != 203 {
		t.Fatal("older completion overwrote a newer committed observation", err)
	}
	publish("2", 202)
	if result, err := e.observeLocalOperation(ref, "context", "operation", signal, ""); err != nil || result != nil {
		t.Fatal("consumption forgot the store-revision fence", err)
	}
	publish("4", 204)
	if result, err := e.observeLocalOperation(ref, "context", "operation", signal, "5"); err != nil || result != nil {
		t.Fatal("a local result superseded a newer validated store read", err)
	}
	publish("4", 204)
	if result, err := e.observeLocalOperation(ref, "context", "operation", signal, ""); err != nil || result != nil {
		t.Fatal("newer read fence was lost", err)
	}
	stop()
	publish("6", 206)
	if result, err := e.observeLocalOperation(ref, "context", "operation", signal, ""); err != nil || result != nil {
		t.Fatal("finished waiter retained an observation", err)
	}
}

func TestLocalObservationBuiltOnlyForMatchingWaiterAndBoundedReply(t *testing.T) {
	e := &StateEngine{cfg: Config{Process: &pb.ProcessRef{InstanceId: "executor", Epoch: "epoch"}}}
	ref := state.Ref{ID: "group", Incarnation: "first"}
	c := &stateContext{Operation: "operation", Snapshot: &pb.ContextSnapshot{Context: &pb.ContextRef{Id: "context", State: &pb.StateRef{GroupId: ref.ID, Incarnation: ref.Incarnation}}}, Result: &pb.OperationResult{Certainty: pb.OperationResult_COMPLETED}}
	r := &stateReduction{engine: e, record: state.Record{Ref: ref}, group: stateGroup{Contexts: map[string]*stateContext{"context": c}}}
	if err := r.projectLocalObservations(); err != nil || len(r.payload.LocalObservations) != 0 {
		t.Fatal("unwatched context retained a local result", err)
	}
	_, stop := e.watchOperation(ref, stateOperationIdentity{"context", "operation"})
	defer stop()
	if err := r.projectLocalObservations(); err != nil || len(r.payload.LocalObservations) != 1 {
		t.Fatal("matching completed result was not projected", err)
	}
	c.Result.Certainty = pb.OperationResult_ACCEPTED
	if err := r.projectLocalObservations(); err != nil || len(r.payload.LocalObservations) != 0 {
		t.Fatal("unfinished operation became a local completion", err)
	}
	c.Result.Certainty = pb.OperationResult_COMPLETED
	c.Snapshot.LocalSdp = make([]byte, stateLocalObservationBytes)
	if err := r.projectLocalObservations(); err != nil || len(r.payload.LocalObservations) != 0 {
		t.Fatal("oversized result bypassed the bounded fallback", err)
	}
	stop()
	if len(e.waiters) != 0 {
		t.Fatal("waiter cleanup retained a local result map")
	}
}

func TestDuplicateWaitersOwnTheirResults(t *testing.T) {
	e := &StateEngine{}
	ref := state.Ref{ID: "group", Incarnation: "incarnation"}
	identity := stateOperationIdentity{"context", "operation"}
	one, stopOne := e.watchOperation(ref, identity)
	two, stopTwo := e.watchOperation(ref, identity)
	defer stopOne()
	defer stopTwo()
	e.publishLocalObservations(&execution.Committed{Ref: ref, Revision: "2", LocalObservations: []state.LocalObservation{localObservationFixture(t, ref, 200)}})
	first, err := e.observeLocalOperation(ref, identity.contextID, identity.operation, one, "")
	if err != nil || first == nil {
		t.Fatal("first result missing", err)
	}
	first.Snapshot.Context.Id = "changed"
	first.SipStatus = 503
	second, err := e.observeLocalOperation(ref, identity.contextID, identity.operation, two, "")
	if err != nil || second.GetSipStatus() != 200 || second.GetSnapshot().GetContext().GetId() != "context" {
		t.Fatal("duplicate consumer shared a mutable result", err)
	}
}
