package engine

import (
	"testing"
	"time"

	"github.com/emiago/sipgo/sip"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/state"
)

// This uses a real SIP/control subprocess and retained Redis state. The memory
// adapter is qualified separately through the same engine and real control API.
func TestStateNativeReplacementControllerReattachesConfirmedDialog(t *testing.T) {
	h := newNativeProcessHarness(t)
	h.recoverable = true
	h.cfg.PreferredController = "unrelated-controller"
	core := h.start("reattach-core")
	phone := h.phone(core)
	callID := "reattach-" + uuid.NewString()
	ref := processEstablish(h, core, phone, callID)
	_, before := processWaitRecord(h, callID, func(_ state.Record, c *stateContext) bool { return c.Snapshot.InitialAck })
	localTag := before.Snapshot.LocalTag
	saved := proto.Clone(core.peer.lease).(*pb.ApplicationLease)
	if saved.AttachmentGeneration != 1 || saved.LeaseLifetimeMs != 5000 || saved.ReattachTimeoutMs != 10000 || saved.RecoveryToken == "" {
		t.Fatal("recoverable owner did not receive bounded recovery proof")
	}
	h.closeController(core.peer)
	// The process lease is deliberately shorter than the unchanged 30s media
	// timeout. A new process waits for expiry instead of stealing a live owner.
	for {
		lease, err := h.store.ReadApplicationLease(h.ctx, (&StateEngine{cfg: Config{StateIdentityKey: h.cfg.Store.NamespaceKey}}).applicationLeaseID(saved.Application))
		if err != nil {
			t.Fatal(err)
		}
		if lease.ExpiresAtMS <= lease.CheckedAtMS {
			break
		}
		select {
		case <-h.ctx.Done():
			t.Fatal("owner did not expire inside recovery budget")
		case <-time.After(20 * time.Millisecond):
		}
	}
	holder := &pb.ProcessRef{InstanceId: "replacement-controller", Epoch: uuid.NewString()}
	h.connectController(core, 1, holder, &pb.ControllerRecovery{Application: saved.Application, Term: saved.Term, Token: saved.RecoveryToken, AttachmentGeneration: saved.AttachmentGeneration, AttemptId: uuid.NewString()})
	lease := core.peer.lease
	if lease.Term != saved.Term || !proto.Equal(lease.Application, saved.Application) || !proto.Equal(lease.Holder, holder) || lease.AttachmentGeneration != 2 || lease.RecoveryToken == saved.RecoveryToken {
		t.Fatal("replacement forged lineage or failed to fence/rotate attachment")
	}
	listed, err := core.peer.call(&pb.Envelope{Payload: &pb.Envelope_ControllerContexts{ControllerContexts: &pb.ControllerContexts{Limit: 64}}})
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, entry := range listed.GetControllerContextsResult().GetContexts() {
		if stateSameContext(entry, ref) {
			found = true
		}
	}
	if !found {
		t.Fatal("reattached controller could not enumerate its retained dialog")
	}
	inspection, err := core.peer.call(&pb.Envelope{Payload: &pb.Envelope_Inspect{Inspect: &pb.ContextInspect{Context: ref}}})
	if err != nil || inspection.GetResult().GetSnapshot().GetEnded() {
		t.Fatal("reattach lost the confirmed dialog")
	}
	// New-call preference must not divert this established dialog to a different
	// controller merely because its actual holder now differs from its lineage.
	attached := core.peer
	h.connectController(core, 1, &pb.ProcessRef{InstanceId: h.cfg.PreferredController, Epoch: uuid.NewString()}, nil)
	foreign := core.peer
	h.mu.Lock()
	core.peer = attached
	h.routeRequest = func(candidate *processController, envelope *pb.Envelope) bool {
		if candidate != foreign {
			return false
		}
		t.Error("established dialog was routed to an unrelated preferred controller")
		candidate.reply(envelope, &pb.Envelope{Payload: &pb.Envelope_RouteResult{RouteResult: &pb.RouteResult{Outcome: pb.RouteResult_REJECTED, SipStatus: 503}}})
		return true
	}
	h.mu.Unlock()
	phone.send(t, processRequest(t, sip.INVITE, callID, 2, localTag))
	presented := h.request(callID, 2).Envelope.GetRequest()
	if !stateSameContext(presented.Snapshot.Context, ref) || presented.Snapshot.ApplicationLeaseTerm != saved.Term {
		t.Fatal("reattach changed native context identity")
	}
	h.respond(core, presented, 200)
	response := phone.receive(t, sip.INVITE, 200)
	if tag(response.To().Params) != localTag {
		t.Fatal("reattach replaced the dialog tags")
	}
	phone.send(t, processRequest(t, sip.ACK, callID, 2, localTag))
	phone.send(t, processRequest(t, sip.BYE, callID, 3, localTag))
	phone.receive(t, sip.BYE, 200)
	processWaitRecord(h, callID, func(_ state.Record, c *stateContext) bool { return c.Snapshot.Ended })
}
