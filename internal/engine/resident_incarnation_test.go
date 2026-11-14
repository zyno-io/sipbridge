package engine

import (
	"context"
	"testing"
	"time"

	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
)

func TestResidentOutboundBareIDReuseFencesOldIncarnation(t *testing.T) {
	e, peer, _, ctx, _, writes, _ := residentControlFixture(t)
	flow := &pb.FlowRef{Ingress: e.cfg.Process, ConnectionId: "phone", SourceIp: "192.0.2.12", SourcePort: 50300, Listener: "sip.example.test:5061", Transport: "tls"}
	proofs := make(chan *pb.FlowWrite, 2)
	writer := e.cfg.WriteCommitted
	e.cfg.WriteCommitted = func(ctx context.Context, write *pb.FlowWrite) *pb.DeliveryResult {
		proofs <- proto.Clone(write).(*pb.FlowWrite)
		return writer(ctx, write)
	}
	command := &pb.Command{Context: &pb.ContextRef{Owner: peer.Application(), Id: "reused-id"}, Sequence: 1,
		Admission: &pb.CommandAdmission{DirectoryEpoch: "directory", ExpiresAtMs: uint64(time.Now().Add(5 * time.Second).UnixMilli()), SelectedFlow: flow},
		Action:    &pb.Command_OpenUac{OpenUac: &pb.OpenUac{Uri: "sip:phone@example.test", Transport: "tls", BindingId: "binding", Headers: []*pb.Header{{Name: "From", Value: "<sip:service@example.test>"}}}}}
	first, err := e.commandResidentClient(ctx, peer, command, "same-operation", time.Now().Add(5*time.Second))
	if err != nil || first.Certainty != pb.OperationResult_ACCEPTED {
		t.Fatal("first creation failed", err)
	}
	firstWire, firstProof := <-writes, <-proofs
	firstRef := proto.Clone(first.Snapshot.Context).(*pb.ContextRef)
	oldResponse, err := portable.BuildResponseWithHeaders(portable.RestoreMessage(firstWire, "tls", "", ""), 200, "OK", nil, "remote", "sip:phone@example.test", nil)
	if err != nil {
		t.Fatal(err)
	}
	oldObserved, err := Observe(oldResponse.WireBytes(), false)
	if err != nil {
		t.Fatal(err)
	}
	oldContext, err := e.findContext(firstRef)
	if err != nil {
		t.Fatal(err)
	}
	oldContext.mu.Lock()
	oldContext.snapshot.Ended = true
	oldContext.retireReady = true
	for _, tx := range oldContext.transactions {
		next, _, stepErr := portable.StepTransaction(tx.native, portable.Input{Kind: portable.Terminate, Now: time.Now().UnixMilli()})
		if stepErr != nil {
			oldContext.mu.Unlock()
			t.Fatal(stepErr)
		}
		tx.native = next
	}
	for _, output := range oldContext.outputs {
		if err = e.releaseResidentOutputLocked(oldContext, output); err != nil {
			oldContext.mu.Unlock()
			t.Fatal(err)
		}
	}
	e.afterResidentOutputReleaseLocked(oldContext)
	oldContext.mu.Unlock()
	e.mu.Lock()
	oldPresent := e.contexts[command.Context.Id] != nil
	e.mu.Unlock()
	if oldPresent {
		t.Fatal("old context did not retire")
	}
	second, err := e.commandResidentClient(ctx, peer, command, "same-operation", time.Now().Add(5*time.Second))
	if err != nil || second.Certainty != pb.OperationResult_ACCEPTED {
		t.Fatal("second creation failed", err)
	}
	secondWire := <-writes
	<-proofs
	secondRef := second.Snapshot.Context
	if firstRef.State.Incarnation == secondRef.State.Incarnation || firstRef.State.CallId == secondRef.State.CallId {
		t.Fatal("recreated context reused native identity")
	}
	firstMessage, err := sip.ParseMessage((portable.RestoreMessage(firstWire, "tls", "", "")).WireBytes())
	if err != nil {
		t.Fatal(err)
	}
	secondMessage, err := sip.ParseMessage((portable.RestoreMessage(secondWire, "tls", "", "")).WireBytes())
	if err != nil {
		t.Fatal(err)
	}
	firstBranch, _ := firstMessage.Via().Params.Get("branch")
	secondBranch, _ := secondMessage.Via().Params.Get("branch")
	if firstBranch == secondBranch {
		t.Fatal("recreated transaction reused branch")
	}
	if _, err = e.inspectResident(ctx, peer.Application(), firstRef); err == nil {
		t.Fatal("old full reference inspected new context")
	}
	if _, err = e.ingest(ctx, peer, &pb.OwnerIngest{Context: firstRef, Application: peer.Application(), Flow: flow, Wire: firstWire}, time.Now().Add(2*time.Second)); err == nil {
		t.Fatal("old ingest reference addressed new context")
	}
	staleCommand := &pb.Command{Context: firstRef, Sequence: 2, Action: &pb.Command_Cancel{Cancel: &pb.Cancel{}}}
	if _, err = e.commandResidentClient(ctx, peer, staleCommand, "stale-cancel", time.Now().Add(2*time.Second)); err == nil {
		t.Fatal("old command addressed new context")
	}
	if forgotten, reserveErr := e.Reserve(ctx, firstProof); reserveErr != nil || forgotten == nil || !forgotten.Known || forgotten.Outcome != pb.DeliveryResult_UNKNOWN || len(forgotten.Wire) != 0 {
		t.Fatal("old write granted permission against new context", forgotten, reserveErr)
	}
	if err = e.Complete(ctx, firstProof, &pb.DeliveryResult{Effect: firstProof.Effect, Outcome: pb.DeliveryResult_WRITTEN}); err == nil {
		t.Fatal("old write completed against new context")
	}
	if _, err = e.InspectWrite(ctx, firstProof); err == nil {
		t.Fatal("old write inspected against new context")
	}
	if handled, err := e.completeResidentClient(ctx, flow, oldObserved); err != nil || handled {
		t.Fatal("old response reached new transaction", err)
	}
	c, err := e.findContext(secondRef)
	if err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	if c.snapshot.CommandSequence != 1 || c.snapshot.Ended {
		c.mu.Unlock()
		t.Fatal("old evidence mutated recreated context")
	}
	// Even with colliding timestamps/generations, a popped old context-only
	// deadline must not retire or clean up the new incarnation.
	c.snapshot.Ended, c.retireGeneration = true, 7
	c.ownerCleanupAt = time.UnixMilli(8)
	c.ownerLost = true
	c.mu.Unlock()
	e.fireResidentDeadline(residentDeadline{key: residentDeadlineKey{kind: residentContextRetirementDeadline, contextID: command.Context.Id, record: firstRef.State.Incarnation}, generation: 7, at: time.Now().Add(-time.Second)})
	e.fireResidentOwnerCleanup(residentDeadline{key: residentDeadlineKey{kind: residentOwnerCleanupDeadline, contextID: command.Context.Id, record: firstRef.State.Incarnation}, generation: 8})
	c.mu.Lock()
	if c.retireReady || c.ownerCleanupAt.IsZero() {
		c.mu.Unlock()
		t.Fatal("old deadline acted on recreated context")
	}
	c.mu.Unlock()
}

func TestResidentInboundAdmissionIDHasFixedWidth(t *testing.T) {
	e, _, _, ctx, _, _, _ := residentControlFixture(t)
	first, err := e.LocateRoute(ctx, stateTestRequest("REGISTER", 1, "", "width-one"))
	if err != nil {
		t.Fatal(err)
	}
	e.mu.Lock()
	e.admissionSerial = 0xffff
	e.mu.Unlock()
	second, err := e.LocateRoute(ctx, stateTestRequest("REGISTER", 1, "", "width-two"))
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Context.Id) != len(second.Context.Id) {
		t.Fatal("admission ID length grew with history")
	}
}
