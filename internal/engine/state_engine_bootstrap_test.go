package engine

import (
	"context"
	"testing"
	"time"

	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/state"
)

func TestStateBootstrapInspectionSurvivesLostCreationReplyWithoutAllocation(t *testing.T) {
	a, b := historicalStateTestEngine(t)
	ctx := context.Background()
	application := &pb.ProcessRef{InstanceId: "controller", Epoch: "application-epoch"}
	placeholder := &pb.ContextRef{Owner: a.cfg.Process, Id: "creation-placeholder"}
	group, err := a.bootstrapGroup(application, placeholder)
	if err != nil {
		t.Fatal(err)
	}
	if absent, err := b.inspectContext(ctx, application, placeholder); err != nil || absent.Certainty != pb.OperationResult_NOT_ACCEPTED || absent.Error != "context not found" {
		t.Fatalf("absence inspection: %v %v", absent, err)
	}
	if _, err = a.store.Load(ctx, group); err != state.ErrNotFound {
		t.Fatalf("inspection allocated state: %v", err)
	}
	if _, err = a.apply(ctx, group, "locate", stateEngineInput{Kind: "locate"}, time.Now().Add(time.Minute), true); err != nil {
		t.Fatal(err)
	}
	record, err := a.store.Load(ctx, group)
	if err != nil {
		t.Fatal(err)
	}
	ref := a.contextRef(record.Ref, placeholder.Id)
	result := stateTestApply(t, a, ref, "initial-create", stateEngineInput{Kind: "command", Application: application, Command: &pb.Command{Context: ref, Sequence: 1, Action: &pb.Command_OpenUac{OpenUac: &pb.OpenUac{Uri: "sip:phone@example.test", BindingId: "binding", PrincipalId: "account", Transport: "tls"}}}})
	if result.Error != "" {
		t.Fatal(result.Error)
	}
	before, err := a.store.Load(ctx, record.Ref)
	if err != nil {
		t.Fatal(err)
	}
	inspected, err := b.inspectContext(ctx, application, placeholder)
	if err != nil || inspected.Snapshot == nil || !stateSameContext(inspected.Snapshot.Context, ref) || !sameProcess(inspected.Snapshot.Executor, b.cfg.Process) {
		t.Fatalf("lost creation result not inspectable on replacement: %v %v", inspected, err)
	}
	after, err := a.store.Load(ctx, record.Ref)
	if err != nil || after.Revision != before.Revision || len(after.Payload.Effects) != len(before.Payload.Effects) {
		t.Fatal("inspection changed native state or resent work")
	}
	foreign := &pb.ProcessRef{InstanceId: application.InstanceId, Epoch: "new-application-epoch"}
	if inspected, err = b.inspectContext(ctx, foreign, placeholder); err != nil || inspected.Snapshot != nil || inspected.Certainty != pb.OperationResult_NOT_ACCEPTED {
		t.Fatal("new Controller epoch discovered old creation")
	}
	input := stateEngineInput{Kind: "command", Context: ref, Application: application, Operation: "pending"}
	data, err := marshalRetainedFixture(input)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = a.store.(*state.Store).Begin(ctx, record.Ref, "paused-worker", state.Input{ID: "pending", Kind: "command", Fingerprint: "pending-fingerprint", Payload: data, DeadlineMS: time.Now().Add(time.Minute).UnixMilli(), ReceiptUntilMS: time.Now().Add(2 * time.Minute).UnixMilli()}); err != nil {
		t.Fatal(err)
	}
	if inspected, err = b.inspectContext(ctx, application, placeholder); err != nil || inspected.Certainty != pb.OperationResult_UNKNOWN {
		t.Fatalf("pending creation/group was reported definite: %v %v", inspected, err)
	}
}
