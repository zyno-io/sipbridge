package engine

import (
	"bufio"
	"net"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
	"github.com/google/uuid"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/state"
)

func nativeProcessWire(t *testing.T, transport string, method sip.RequestMethod, callID string, cseq uint32, localTag string) []byte {
	t.Helper()
	wire := string(processRequest(t, method, callID, cseq, localTag))
	if transport != "tls" {
		wire = strings.ReplaceAll(wire, "SIP/2.0/TLS", "SIP/2.0/"+strings.ToUpper(transport))
		wire = strings.ReplaceAll(wire, ";transport=tls", ";transport="+transport)
	}
	return []byte(wire)
}

func (h *processHarness) nativePhone(worker *processWorker, transport string) *processPhone {
	h.t.Helper()
	if transport == "tls" {
		return h.phone(worker)
	}
	if transport != "udp" {
		h.t.Fatal("unsupported native process transport")
	}
	connection, err := net.Dial("udp", worker.ready.PlainUDP)
	if err != nil {
		h.t.Fatal(err)
	}
	h.t.Cleanup(func() { connection.Close() })
	phone := &processPhone{connection: connection, reader: bufio.NewReader(connection)}
	h.mu.Lock()
	h.phones = append(h.phones, phone)
	h.mu.Unlock()
	return phone
}

func (h *processHarness) closeController(controller *processController) {
	h.t.Helper()
	controller.connection.CloseNow()
	select {
	case <-controller.done:
	case <-h.ctx.Done():
		h.t.Fatal("Controller control reader did not stop")
	}
}

func TestStateNativeCurrentProcessControlReconnectPreservesDialog(t *testing.T) {
	h := newNativeProcessHarness(t)
	a, b := h.start("sip-a"), h.start("sip-b")
	if a.peer.lease.Term != b.peer.lease.Term {
		t.Fatal("native cores acquired different live SIP-owner terms")
	}
	phone := h.phone(a)
	callID := "native-reconnect-" + uuid.NewString()
	ref := processEstablish(h, a, phone, callID)
	_, group := h.record(callID)
	before := processOnlyContext(t, group)
	originalTerm, originalSession := a.peer.lease.Term, a.peer.session
	h.closeController(a.peer)
	h.connect(a, 2)
	if a.peer.lease.Term != originalTerm || a.peer.session == originalSession {
		t.Fatal("living owner reconnect changed its term or reused its control session")
	}
	phone.send(t, processRequest(t, sip.INVITE, callID, 2, before.Snapshot.LocalTag))
	presented := h.request(callID, 2).Envelope.GetRequest()
	if presented.Snapshot.ApplicationLeaseTerm != originalTerm || !stateSameContext(presented.Snapshot.Context, ref) {
		t.Fatal("reconnect changed the retained dialog authority")
	}
	h.respond(a, presented, 200)
	response := phone.receive(t, sip.INVITE, 200)
	if tag(response.To().Params) != before.Snapshot.LocalTag {
		t.Fatal("reconnect changed the receiving dialog tag")
	}
	phone.send(t, processRequest(t, sip.ACK, callID, 2, before.Snapshot.LocalTag))
	phone.send(t, processRequest(t, sip.BYE, callID, 3, before.Snapshot.LocalTag))
	phone.receive(t, sip.BYE, 200)
	processWaitRecord(h, callID, func(_ state.Record, c *stateContext) bool { return c.Snapshot.Ended })
	_, group = h.record(callID)
	if after := processOnlyContext(t, group); after.OwnerLost || after.Snapshot.ApplicationLeaseTerm != originalTerm || !stateSameContext(after.Snapshot.Context, ref) || after.CompletionScope != before.CompletionScope || after.ScopeOrigin == nil || before.ScopeOrigin == nil || string(after.ScopeOrigin.Wire) != string(before.ScopeOrigin.Wire) {
		t.Fatal("living control reconnect fenced or migrated the original call")
	}
}

func processReconcileRefusal(h *processHarness, worker *processWorker, ref *pb.ContextRef) {
	h.t.Helper()
	reply, err := worker.peer.call(&pb.Envelope{Payload: &pb.Envelope_ReconcileContext{ReconcileContext: &pb.ContextReconcile{
		Context: ref, FromDirectoryEpoch: "fixture-directory", DirectoryEpoch: "next-directory",
	}}})
	if err != nil {
		h.t.Fatal(err)
	}
	result := reply.GetResult()
	if result == nil || result.Certainty != pb.OperationResult_NOT_ACCEPTED || result.Error != "directory reconciliation retired; re-establish context" {
		h.t.Fatalf("native reconciliation was not refused by the retired RPC boundary: %s", result)
	}
}

func assertProcessRecordUnchanged(h *processHarness, callID string, before state.Record) {
	h.t.Helper()
	after, _ := h.record(callID)
	if after.Revision != before.Revision || !reflect.DeepEqual(after.Payload, before.Payload) {
		h.t.Fatal("refused reconciliation changed retained native state")
	}
}

func TestStateNativeReconcileRefusalPreservesInboundDialogAndBYE(t *testing.T) {
	h := newNativeProcessHarness(t)
	a := h.start("sip-a")
	phone := h.phone(a)
	callID := "native-anchored-" + uuid.NewString()
	ref := processEstablish(h, a, phone, callID)
	// A received ACK event precedes its retained receiver-completion commit.
	// Establish a quiescent baseline before testing that refusal writes nothing.
	before, context := processWaitRecord(h, callID, func(record state.Record, c *stateContext) bool {
		return c.Snapshot.InitialAck && len(record.Payload.Effects) == 0
	})

	processReconcileRefusal(h, a, ref)
	assertProcessRecordUnchanged(h, callID, before)
	phone.send(t, processRequest(t, sip.BYE, callID, 2, context.Snapshot.LocalTag))
	phone.receive(t, sip.BYE, 200)
	processWaitRecord(h, callID, func(_ state.Record, c *stateContext) bool { return c.Snapshot.Ended })
}

func TestStateNativeReconcileRefusalPreservesDuplicate200ACKAndBYE(t *testing.T) {
	h := newNativeProcessHarness(t)
	a := h.start("sip-a")
	phone := h.phone(a)
	ref, invite := processOutgoingInvite(h, a, phone)
	callID := invite.CallID().Value()
	accepted := processInviteFinal(invite, 200, "phone")
	phone.send(t, []byte(accepted.String()))
	firstACK := phone.receive(t, sip.ACK, 0)
	// Socket receipt precedes ACK completion and application-event commits.
	before, _ := processWaitRecord(h, callID, func(record state.Record, c *stateContext) bool {
		return c.Snapshot.InitialAck && len(record.Payload.Effects) == 0
	})

	processReconcileRefusal(h, a, ref)
	assertProcessRecordUnchanged(h, callID, before)
	phone.send(t, []byte(accepted.String()))
	replayed := phone.receive(t, sip.ACK, 0)
	if replayed.String() != firstACK.String() {
		t.Fatal("refused reconciliation changed duplicate200 ACK wire")
	}
	phone.send(t, processRequest(t, sip.BYE, callID, 1, tag(invite.From().Params)))
	phone.receive(t, sip.BYE, 200)
	processWaitRecord(h, callID, func(_ state.Record, c *stateContext) bool { return c.Snapshot.Ended })
}

type nativeExpiryCall struct {
	transport                      string
	pending, established, outbound *processPhone
	pendingID, establishedID       string
	establishedRef, outboundRef    *pb.ContextRef
	establishedTag                 string
	invite                         *sip.Request
}

func nativeProcessOutgoing(h *processHarness, worker *processWorker, phone *processPhone, transport string) (*pb.ContextRef, *sip.Request) {
	h.t.Helper()
	seed := "native-outgoing-seed-" + uuid.NewString()
	phone.send(h.t, nativeProcessWire(h.t, transport, sip.OPTIONS, seed, 1, ""))
	h.respond(worker, h.request(seed, 1).Envelope.GetRequest(), 200)
	phone.receive(h.t, sip.OPTIONS, 200)
	opened := h.result(worker, &pb.Command{Context: &pb.ContextRef{Owner: worker.process, Id: "native-outgoing-" + uuid.NewString()}, Sequence: 1,
		Action: &pb.Command_OpenUac{OpenUac: &pb.OpenUac{Uri: "sip:phone@example.test", Transport: transport, BindingId: "fixture-binding", PrincipalId: "fixture-phone",
			Headers: []*pb.Header{{Name: "From", Value: "<sip:service@example.test>"}}}}})
	invite := phone.receive(h.t, sip.INVITE, 0).(*sip.Request)
	return opened.Snapshot.Context, invite
}

func nativeProcessResponse(invite *sip.Request, status int, remoteTag, transport string) *sip.Response {
	response := processInviteFinal(invite, status, remoteTag)
	if contact := response.Contact(); contact != nil {
		contact.Address.UriParams.Add("transport", transport)
	}
	return response
}

func TestStateNativeCurrentProcessAllControllerGoneCompletesUDPAndTLS(t *testing.T) {
	// Allow the fixed recovery grace in addition to the existing wire/RPC and
	// real authority-lifetime budgets.
	h := newProcessHarnessForProfile(t, true, 35*time.Second+state.ApplicationLeaseLifetime+state.ApplicationLeaseRecoveryGrace)
	a, b := h.start("sip-a"), h.start("sip-b")
	originalTerm := a.peer.lease.Term
	if b.peer.lease.Term != originalTerm {
		t.Fatal("cores did not share one original owner lease")
	}
	var calls []nativeExpiryCall
	for _, transport := range []string{"udp", "tls"} {
		pending := h.nativePhone(a, transport)
		pendingID := "native-pending-" + transport + "-" + uuid.NewString()
		pending.send(t, nativeProcessWire(t, transport, sip.INVITE, pendingID, 1, ""))
		h.request(pendingID, 1)
		established := h.nativePhone(a, transport)
		establishedID := "native-established-" + transport + "-" + uuid.NewString()
		established.send(t, nativeProcessWire(t, transport, sip.INVITE, establishedID, 1, ""))
		presented := h.request(establishedID, 1).Envelope.GetRequest()
		h.respond(a, presented, 200)
		accepted := established.receive(t, sip.INVITE, 200)
		localTag := tag(accepted.To().Params)
		established.send(t, nativeProcessWire(t, transport, sip.ACK, establishedID, 1, localTag))
		processWaitRecord(h, establishedID, func(_ state.Record, c *stateContext) bool { return c.Snapshot.InitialAck })
		outbound := h.nativePhone(a, transport)
		outboundRef, invite := nativeProcessOutgoing(h, a, outbound, transport)
		outbound.send(t, []byte(nativeProcessResponse(invite, 180, "early-phone", transport).String()))
		h.wait(func(o processObservation) bool {
			event := o.Envelope.GetDialogEvent()
			return event != nil && event.Kind == pb.DialogEvent_PROVISIONAL && event.Snapshot.CallId == invite.CallID().Value()
		})
		calls = append(calls, nativeExpiryCall{transport: transport, pending: pending, established: established, outbound: outbound, pendingID: pendingID, establishedID: establishedID, establishedRef: presented.Snapshot.Context, establishedTag: localTag, outboundRef: outboundRef, invite: invite})
	}
	h.closeController(a.peer)
	h.closeController(b.peer)
	leaseID := (&StateEngine{cfg: Config{StateIdentityKey: h.cfg.Store.NamespaceKey}}).applicationLeaseID(h.application)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	var expired state.ApplicationLease
	for {
		lease, err := h.store.ReadApplicationLease(h.ctx, leaseID)
		if err != nil {
			t.Fatal(err)
		}
		if lease.Term != originalTerm {
			t.Fatal("owner authority changed while every Controller was stopped")
		}
		if lease.ExpiresAtMS <= lease.CheckedAtMS {
			expired = lease
			break
		}
		select {
		case <-ticker.C:
		case <-h.ctx.Done():
			t.Fatal("actual SIP owner lease did not expire")
		}
	}
	for _, call := range calls {
		for _, id := range []string{call.pendingID, call.establishedID, call.invite.CallID().Value()} {
			_, c := processWaitRecord(h, id, func(_ state.Record, c *stateContext) bool { return c.OwnerLost })
			fencedAt := c.OwnerCleanupAtMS - 2*64*portable.DefaultTimers().T1
			if expired.RecoveryUntilMS <= expired.ExpiresAtMS || fencedAt < expired.RecoveryUntilMS || fencedAt > expired.RecoveryUntilMS+stateOwnerProbeInterval+stateOwnerProbeBudget {
				t.Fatalf("owner loss was early or outside bounded detection: expiry=%d recovery=%d fence=%d", expired.ExpiresAtMS, expired.RecoveryUntilMS, fencedAt)
			}
		}
		refusal := call.pending.receive(t, sip.INVITE, 503)
		acknowledgement, err := portable.BuildNon2xxACK(portable.Message{Wire: call.pending.sent[0], Transport: call.transport}, portable.Message{Wire: []byte(refusal.String()), Transport: call.transport})
		if err != nil {
			t.Fatal(err)
		}
		call.pending.send(t, acknowledgement.Wire)
		bye := call.established.receive(t, sip.BYE, 0).(*sip.Request)
		if bye.CallID().Value() != call.establishedID || tag(bye.From().Params) != call.establishedTag {
			t.Fatal("owner cleanup BYE targeted another retained dialog")
		}
		call.established.send(t, []byte(sip.NewResponseFromRequest(bye, 200, "OK", nil).String()))
		cancel := call.outbound.receive(t, sip.CANCEL, 0).(*sip.Request)
		if cancel.CSeq().SeqNo != call.invite.CSeq().SeqNo || cancel.CallID().Value() != call.invite.CallID().Value() {
			t.Fatal("owner cleanup CANCEL changed original transaction identity")
		}
		call.outbound.send(t, []byte(sip.NewResponseFromRequest(cancel, 200, "OK", nil).String()))
		late := nativeProcessResponse(call.invite, 200, "late-phone", call.transport)
		call.outbound.send(t, []byte(late.String()))
		ack := call.outbound.receive(t, sip.ACK, 0).(*sip.Request)
		cleanup := call.outbound.receive(t, sip.BYE, 0).(*sip.Request)
		if tag(ack.To().Params) != "late-phone" || tag(cleanup.To().Params) != "late-phone" || ack.CSeq().SeqNo != call.invite.CSeq().SeqNo || cleanup.CSeq().SeqNo != call.invite.CSeq().SeqNo+1 {
			t.Fatal("late success cleanup changed branch or sequence identity")
		}
		call.outbound.send(t, []byte(sip.NewResponseFromRequest(cleanup, 200, "OK", nil).String()))
		processWaitRecord(h, call.establishedID, func(_ state.Record, c *stateContext) bool { return c.Snapshot.Ended })
		processWaitRecord(h, call.invite.CallID().Value(), func(_ state.Record, c *stateContext) bool {
			late, exists := c.Dialogs[tag(call.invite.From().Params)+"/late-phone"]
			return exists && late.Phase == portable.DialogEnded
		})
		_, retained := h.record(call.invite.CallID().Value())
		c := processOnlyContext(t, retained)
		early, exists := c.Dialogs[tag(call.invite.From().Params)+"/early-phone"]
		if !exists || early.Phase != portable.DialogEarly || c.Snapshot.Answered || c.Dialog != "" || !c.OwnerLost || !c.Cancelled {
			t.Fatal("late-branch cleanup erased the retained early fork or reopened application setup")
		}
		t.Logf("transport=%s owner-term-expired=%d pending503 establishedBYE outboundCANCEL lateACK-BYE completed without Controller", call.transport, expired.ExpiresAtMS)
	}
	// Only after every real cleanup above finishes may a fresh authenticated
	// control session acquire a successor for this same Controller process epoch.
	h.connect(a, 2)
	if a.peer.lease.Term == originalTerm {
		t.Fatal("new hello revived expired SIP-owner term")
	}
	for _, call := range calls {
		reply, err := a.peer.call(&pb.Envelope{Payload: &pb.Envelope_Command{Command: &pb.Command{Context: call.establishedRef, Sequence: 2, Action: &pb.Command_DialogRequest{DialogRequest: &pb.DialogRequest{Method: "INVITE", Body: []byte("v=0\r\n")}}}}})
		if err != nil {
			t.Fatal(err)
		}
		if result := reply.GetResult(); result == nil || result.Certainty != pb.OperationResult_NOT_ACCEPTED {
			t.Fatal("successor session reauthorized an old application context")
		}
		_, g := h.record(call.establishedID)
		c := processOnlyContext(t, g)
		if !c.OwnerLost || c.Snapshot.ApplicationLeaseTerm != originalTerm || !stateSameContext(c.Snapshot.Context, call.establishedRef) {
			t.Fatal("successor session rebound old native authority")
		}
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, observation := range h.observations {
		if egress := observation.Envelope.GetEgress(); egress != nil && egress.Authorization.AuthorityClass != pb.EffectAuthorization_APPLICATION {
			t.Fatal("native protocol completion required a Controller grant")
		}
	}
}
