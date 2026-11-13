package engine

import (
	"bytes"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/state"
)

func TestStateNativeProcessReliableFinalNoStartRecoversOnUnchangedTLS(t *testing.T) {
	h := newRelayProcessHarness(t)
	a, b := h.start("sip-a"), h.start("sip-b")
	phone := h.phone(a)
	callID := "reliable-final-no-start-" + uuid.NewString()
	phone.send(t, processRequest(t, sip.INVITE, callID, 1, ""))
	initial := h.request(callID, 1).Envelope.GetRequest()
	h.respond(a, initial, 200)
	final := phone.receive(t, sip.INVITE, 200)
	phone.send(t, processRequest(t, sip.ACK, callID, 1, tag(final.To().Params)))
	h.wait(func(observed processObservation) bool {
		event := observed.Envelope.GetDialogEvent()
		return event != nil && event.Kind == pb.DialogEvent_ACKNOWLEDGED && event.Snapshot.CallId == callID
	})
	before, group := h.record(callID)
	original := processOnlyContext(t, group)
	flow := proto.Clone(original.Flow).(*pb.FlowRef)
	rule := stateProcessRule{ID: "reliable-final-known-no-start", Stage: "no-start", CallID: callID, Method: "BYE", Class: pb.EffectAuthorization_RESPONSE}
	h.arm(a, rule)
	phone.send(t, processRequest(t, sip.BYE, callID, 2, tag(final.To().Params)))
	barrier := h.workerEvent(a, "barrier", rule.ID)
	h.wait(func(observed processObservation) bool {
		delivery := observed.Envelope.GetDelivery()
		return delivery != nil && delivery.Effect.GetId() == barrier.Token && delivery.Outcome == pb.DeliveryResult_NOT_WRITTEN && delivery.Recorded
	})
	response := phone.receive(t, sip.BYE, 200)
	first := h.wait(func(observed processObservation) bool {
		egress := observed.Envelope.GetEgress()
		return egress != nil && egress.Authorization.Effect.Id == barrier.Token
	}).Envelope.GetEgress()
	retry := h.wait(func(observed processObservation) bool {
		egress := observed.Envelope.GetEgress()
		return egress != nil && egress.Snapshot.CallId == callID && egress.Message.Status == 200 && egress.Message.Method == "BYE" && egress.Authorization.Effect.Id != barrier.Token
	}).Envelope.GetEgress()
	if response.CSeq().SeqNo != 2 || !bytes.Equal(first.Message.Wire, retry.Message.Wire) || !proto.Equal(first.ReturnFlow, retry.ReturnFlow) || !proto.Equal(flow, retry.ReturnFlow) {
		t.Fatal("known no-start recovery changed cached response bytes, CSeq or physical TLS flow")
	}
	after, retained := processWaitRecord(h, callID, func(_ state.Record, c *stateContext) bool {
		for _, tx := range c.Transactions {
			if tx.Native.Family == portable.NonInviteServer && tx.Native.LastResponse != nil && tx.FinalResponse != nil && tx.FinalResponse.Written {
				parsed, _ := tx.Native.Origin.Parse()
				if parsed.CSeq().MethodName == sip.BYE {
					return tx.Native.Phase == portable.Terminated && tx.Emissions == 2
				}
			}
		}
		return false
	})
	if before.Ref.Incarnation != after.Ref.Incarnation || !stateSameContext(initial.Snapshot.Context, retained.Snapshot.Context) ||
		!sameProcess(retained.Snapshot.Application, h.application) || !proto.Equal(retained.Flow, flow) || !retained.Snapshot.Ended {
		t.Fatal("final response recovery replaced native context, application owner or accepted flow")
	}
	// Another replica remains live throughout; recovery does not require moving
	// the original physical writer or opening another handset connection.
	if b.process == nil {
		t.Fatal("second replica missing")
	}
	h.release(a)
}

func processOutgoingInvite(h *processHarness, worker *processWorker, phone *processPhone) (*pb.ContextRef, *sip.Request) {
	h.t.Helper()
	seed := "outgoing-seed-" + uuid.NewString()
	phone.send(h.t, processRequest(h.t, sip.OPTIONS, seed, 1, ""))
	h.respond(worker, h.request(seed, 1).Envelope.GetRequest(), 200)
	phone.receive(h.t, sip.OPTIONS, 200)
	opened := h.result(worker, &pb.Command{Context: &pb.ContextRef{Owner: worker.process, Id: "outgoing-" + uuid.NewString()}, Sequence: 1,
		Action: &pb.Command_OpenUac{OpenUac: &pb.OpenUac{Uri: "sip:phone@example.test", Transport: "tls", BindingId: "fixture-binding", PrincipalId: "fixture-phone",
			Headers: []*pb.Header{{Name: "From", Value: "<sip:service@example.test>"}}}}})
	invite := phone.receive(h.t, sip.INVITE, 0).(*sip.Request)
	return opened.Snapshot.Context, invite
}

func processInviteFinal(invite *sip.Request, status int, remoteTag string) *sip.Response {
	response := sip.NewResponseFromRequest(invite, status, "fixture", nil)
	response.To().Params.Add("tag", remoteTag)
	if status < 300 {
		response.AppendHeader(&sip.ContactHeader{Address: sip.Uri{Scheme: "sip", User: "phone", Host: "phone.example.test", Port: 5061, UriParams: sip.HeaderParams{{K: "transport", V: "tls"}}}})
	}
	return response
}

func processWaitRecord(h *processHarness, callID string, predicate func(state.Record, *stateContext) bool) (state.Record, *stateContext) {
	h.t.Helper()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		record, group := h.record(callID)
		context := processOnlyContext(h.t, group)
		if predicate(record, context) {
			return record, context
		}
		select {
		case <-ticker.C:
		case <-h.ctx.Done():
			h.t.Fatal("committed transaction predicate timed out")
		}
	}
}

func processOriginalTransaction(t *testing.T, c *stateContext, invite *sip.Request) *stateTransaction {
	t.Helper()
	origin := portable.Message{Wire: []byte(invite.String()), Transport: "tls"}
	for _, tx := range c.Transactions {
		if tx.Native.Family == portable.InviteClient && portable.MatchRequest(tx.Native.Origin, origin) {
			return tx
		}
	}
	t.Fatal("original native INVITE transaction missing")
	return nil
}

func TestStateNativeProcessUASReinvitePinsOutboundTransactionScope(t *testing.T) {
	h := newRelayProcessHarness(t)
	const inboundScope = "initial-inbound-dialog"
	h.completionScope = func(egress *pb.EgressRequest) (string, error) {
		parsed, err := sip.ParseMessage(egress.Message.Wire)
		if err != nil {
			return "", err
		}
		auth := egress.Authorization
		switch auth.AuthorityClass {
		case pb.EffectAuthorization_RESPONSE:
			return inboundScope, nil
		case pb.EffectAuthorization_APPLICATION:
			if parsed.CSeq().MethodName == sip.INVITE {
				return fmt.Sprintf("outbound-invite-%d", parsed.CSeq().SeqNo), nil
			}
		case pb.EffectAuthorization_NATIVE_COMPLETION:
			if parsed.CSeq().MethodName == sip.ACK {
				want := fmt.Sprintf("outbound-invite-%d", parsed.CSeq().SeqNo)
				if auth.CompletionScope != want {
					return "", fmt.Errorf("ACK cannot use inbound/other transaction scope")
				}
				return auth.CompletionScope, nil
			}
		case pb.EffectAuthorization_TERMINAL_CLEANUP:
			if parsed.CSeq().MethodName == sip.BYE && auth.CompletionScope == inboundScope {
				return inboundScope, nil
			}
		}
		return "", fmt.Errorf("unexpected fixture completion authority")
	}
	a, b := h.start("sip-a"), h.start("sip-b")
	phone := h.phone(a)
	callID := "uas-reinvite-scope-" + uuid.NewString()
	// The handset's initial CSeq is independent of the UAS's first local CSeq.
	phone.send(t, processRequest(t, sip.INVITE, callID, 2, ""))
	presented := h.request(callID, 2).Envelope.GetRequest()
	ref := presented.Snapshot.Context
	h.respond(a, presented, 200)
	initial := phone.receive(t, sip.INVITE, 200)
	phone.send(t, processRequest(t, sip.ACK, callID, 2, tag(initial.To().Params)))
	h.wait(func(observed processObservation) bool {
		event := observed.Envelope.GetDialogEvent()
		return event != nil && event.Kind == pb.DialogEvent_ACKNOWLEDGED && event.Snapshot.CallId == callID
	})
	// The application RPC waits for the remote final; the handset must be able
	// to read and answer while that real control request remains outstanding.
	type commandReply struct {
		envelope *pb.Envelope
		err      error
	}
	reinviteDone := make(chan commandReply, 1)
	go func() {
		reply, err := b.peer.call(&pb.Envelope{Payload: &pb.Envelope_Command{Command: &pb.Command{Context: ref, Sequence: 2, Action: &pb.Command_DialogRequest{DialogRequest: &pb.DialogRequest{Method: "INVITE"}}}}})
		reinviteDone <- commandReply{reply, err}
	}()
	reinvite := phone.receive(t, sip.INVITE, 0).(*sip.Request)
	if reinvite.CSeq().SeqNo != 1 {
		t.Fatal("local re-INVITE did not use its independent first local CSeq")
	}
	_, before := processWaitRecord(h, callID, func(_ state.Record, c *stateContext) bool {
		return processOriginalTransaction(t, c, reinvite).CompletionScope != ""
	})
	if processOriginalTransaction(t, before, reinvite).CompletionScope != "outbound-invite-1" || before.CompletionScope != inboundScope || before.DialogScopes[before.Dialog] != inboundScope {
		t.Fatal("outbound application transaction inherited or replaced inbound dialog cleanup authority")
	}
	phone.send(t, []byte(processInviteFinal(reinvite, 200, "phone").String()))
	ack := phone.receive(t, sip.ACK, 0)
	select {
	case reply := <-reinviteDone:
		if reply.err != nil {
			t.Fatal(reply.err)
		}
		result := reply.envelope.GetResult()
		if result == nil || result.Certainty != pb.OperationResult_COMPLETED || result.Error != "" {
			t.Fatalf("re-INVITE did not complete from the received final: %s", result)
		}
	case <-h.ctx.Done():
		t.Fatal("re-INVITE command reply did not complete")
	}
	ackGrant := h.wait(func(observed processObservation) bool {
		egress := observed.Envelope.GetEgress()
		return egress != nil && egress.Snapshot.CallId == callID && egress.Message.Method == "ACK"
	}).Envelope.GetEgress()
	if ack.CSeq().SeqNo != 1 || ackGrant.Authorization.CompletionScope != "outbound-invite-1" {
		t.Fatal("successful re-INVITE ACK used the inbound dialog procedure")
	}

	_, written := processWaitRecord(h, callID, func(record state.Record, c *stateContext) bool {
		proof := c.ACKWritten[c.Dialog]
		if proof.Sequence != 1 || proof.WireHash != stateACKHash([]byte(ack.String())) {
			return false
		}
		return true // The exact native ACK proof survives independently of effect compaction.
	})
	if written.CompletionScope != inboundScope || written.DialogScopes[written.Dialog] != inboundScope || !sameProcess(written.Snapshot.Application, h.application) {
		t.Fatal("ACK settlement changed original dialog authority or application owner")
	}
	// Native cancellation now needs a cleanup BYE. Its authority is still the
	// original incoming-dialog scope, not the re-INVITE's outbound grant.
	h.result(b, &pb.Command{Context: ref, Sequence: 3, Action: &pb.Command_Cancel{Cancel: &pb.Cancel{}}})
	bye := phone.receive(t, sip.BYE, 0).(*sip.Request)
	if bye.CSeq().SeqNo != 2 {
		t.Fatal("cleanup BYE changed the native local sequence")
	}
	phone.send(t, []byte(sip.NewResponseFromRequest(bye, 200, "OK", nil).String()))
	processWaitRecord(h, callID, func(_ state.Record, c *stateContext) bool { return c.Snapshot.Ended })
}

func TestStateNativeProcessNon2xxACKRetainedAcrossWriterDeath(t *testing.T) {
	h := newNativeProcessHarness(t)
	a, b := h.start("sip-a"), h.start("sip-b")
	phone := h.phone(a)
	ref, invite := processOutgoingInvite(h, a, phone)
	callID := invite.CallID().Value()
	phone.send(t, []byte(processInviteFinal(invite, 183, "busy-phone").String()))
	h.wait(func(o processObservation) bool {
		event := o.Envelope.GetDialogEvent()
		return event != nil && event.Kind == pb.DialogEvent_PROVISIONAL && event.Snapshot.CallId == callID
	})
	rule := stateProcessRule{ID: "non2xx-ack-before-complete", Stage: "complete", CallID: callID, Method: "ACK", Class: pb.EffectAuthorization_NATIVE_COMPLETION}
	h.arm(a, rule)
	final := processInviteFinal(invite, 486, "busy-phone")
	phone.send(t, []byte(final.String()))
	first := phone.receive(t, sip.ACK, 0)
	barrier := h.workerEvent(a, "barrier", rule.ID)
	before, group := h.record(callID)
	c := processOnlyContext(t, group)
	tx := processOriginalTransaction(t, c, invite)
	if tx.Native.ACK == nil || tx.Native.Phase != portable.Terminated || tx.CompletionScope == "" || !bytes.Equal(tx.Native.ACK.Wire, []byte(first.String())) {
		t.Fatal("reliable non-2xx ACK was not retained with its exact native transaction/scope")
	}
	oldACK := append([]byte(nil), tx.Native.ACK.Wire...)
	h.kill(a)
	replacement := h.phone(b)
	replacement.send(t, []byte(final.String()))
	processWaitRecord(h, callID, func(_ state.Record, c *stateContext) bool {
		tx := processOriginalTransaction(t, c, invite)
		return tx.ResponseFlow != nil && sameProcess(tx.ResponseFlow.Ingress, b.process)
	})
	replayed := replacement.receive(t, sip.ACK, 0)
	if !bytes.Equal(oldACK, []byte(replayed.String())) || replayed.CSeq().SeqNo != invite.CSeq().SeqNo || tag(replayed.To().Params) != "busy-phone" {
		t.Fatal("replacement TLS flow changed non-2xx ACK identity or wire")
	}
	after, group := h.record(callID)
	c = processOnlyContext(t, group)
	if before.Ref.Incarnation != after.Ref.Incarnation || !stateSameContext(ref, c.Snapshot.Context) || !sameProcess(c.Snapshot.Application, h.application) || c.Snapshot.Answered {
		t.Fatal("non-2xx ACK recovery recreated application/dialog ownership")
	}
	retainedUnknown := false
	for _, effect := range after.Payload.Effects {
		if effect.ID == barrier.Token {
			retainedUnknown = effect.Status == state.EffectDispatching || effect.Status == state.EffectUnknown
		}
	}
	if !retainedUnknown {
		t.Fatal("replayed ACK falsely settled the dead writer's original physical attempt")
	}
}

func TestStateNativeProcessNon2xxServerACKCompletesOnReplacementTLS(t *testing.T) {
	h := newNativeProcessHarness(t)
	a, b := h.start("sip-a"), h.start("sip-b")
	phone := h.phone(a)
	callID := "non2xx-server-" + uuid.NewString()
	initial := processRequest(t, sip.INVITE, callID, 1, "")
	phone.send(t, initial)
	presented := h.request(callID, 1).Envelope.GetRequest()
	h.respond(a, presented, 486)
	final := phone.receive(t, sip.INVITE, 486)
	before, group := h.record(callID)
	c := processOnlyContext(t, group)
	var originalKey string
	for key, tx := range c.Transactions {
		if tx.Native.Family == portable.InviteServer {
			originalKey = key
			if tx.Native.Phase != portable.Completed {
				t.Fatal("server rejection did not retain the ACK-waiting transaction")
			}
		}
	}
	ack, err := portable.BuildNon2xxACK(portable.Message{Wire: initial, Transport: "tls"}, portable.Message{Wire: []byte(final.String()), Transport: "tls"})
	if err != nil {
		t.Fatal(err)
	}
	h.kill(a)
	replacement := h.phone(b)
	replacement.send(t, ack.Wire)
	after, ended := processWaitRecord(h, callID, func(_ state.Record, c *stateContext) bool {
		return c.Transactions[originalKey] != nil && c.Transactions[originalKey].Native.Phase == portable.Terminated
	})
	if before.Ref.Incarnation != after.Ref.Incarnation || !stateSameContext(presented.Snapshot.Context, ended.Snapshot.Context) || !sameProcess(ended.Snapshot.Application, h.application) || ended.Dialog != "" {
		t.Fatal("non-2xx server ACK recovery allocated another dialog or owner")
	}
	// The exact INVITE retry still gets its cached486 after the ACK completed.
	replacement.send(t, initial)
	replayed := replacement.receive(t, sip.INVITE, 486)
	if tag(replayed.To().Params) != tag(final.To().Params) || replayed.CSeq().SeqNo != final.CSeq().SeqNo {
		t.Fatal("non-2xx server response retention changed after replacement ACK")
	}
}

func TestStateNativeProcessCancelledLateForksCleanupAfterOwnerDeath(t *testing.T) {
	processCancelledLateForksCleanupAfterOwnerDeath(t, newRelayProcessHarness, true)
}

func TestStateNativeCurrentProcessCancelledLateForksCleanupAfterOwnerDeath(t *testing.T) {
	processCancelledLateForksCleanupAfterOwnerDeath(t, newNativeProcessHarness, false)
}

func processCancelledLateForksCleanupAfterOwnerDeath(t *testing.T, harness func(*testing.T) *processHarness, egressGrantExpiry bool) {
	t.Helper()
	for _, scenario := range []struct {
		name         string
		rejected     bool
		expiredGrant bool
	}{{name: "cancel-pending"}, {name: "cancel-final487", rejected: true}, {name: "cancel-expired-cleanup-grant", expiredGrant: true}} {
		// Native completion does not request the historical controller egress
		// grant whose expiry this relay-only scenario exercises.
		if scenario.expiredGrant && !egressGrantExpiry {
			continue
		}
		t.Run(scenario.name, func(t *testing.T) {
			h := harness(t)
			if scenario.expiredGrant {
				var held sync.Once
				h.beforeEgressReply = func(egress *pb.EgressRequest) {
					if egress.Message.Method != "BYE" {
						return
					}
					held.Do(func() {
						deadline := time.UnixMilli(int64(egress.Authorization.DeadlineMs)).Add(25 * time.Millisecond)
						timer := time.NewTimer(time.Until(deadline))
						defer timer.Stop()
						select {
						case <-timer.C:
						case <-h.ctx.Done():
						}
					})
				}
			}
			a, b := h.start("sip-a"), h.start("sip-b")
			phone := h.phone(a)
			ref, invite := processOutgoingInvite(h, a, phone)
			callID := invite.CallID().Value()
			phone.send(t, []byte(processInviteFinal(invite, 180, "early-phone").String()))
			h.wait(func(o processObservation) bool {
				event := o.Envelope.GetDialogEvent()
				return event != nil && event.Kind == pb.DialogEvent_PROVISIONAL && event.Snapshot.CallId == callID
			})
			h.result(a, &pb.Command{Context: ref, Sequence: 2, Action: &pb.Command_Cancel{Cancel: &pb.Cancel{}}})
			cancel := phone.receive(t, sip.CANCEL, 0).(*sip.Request)
			cancelResponse := sip.NewResponseFromRequest(cancel, 200, "OK", nil)
			cancelResponse.To().Params.Add("tag", "early-phone")
			phone.send(t, []byte(cancelResponse.String()))
			if scenario.rejected {
				phone.send(t, []byte(processInviteFinal(invite, 487, "early-phone").String()))
				phone.receive(t, sip.ACK, 0)
				// Reading the ACK from the socket does not prove its write result
				// is recorded in state. Wait for that proof before killing its
				// worker; uncertain physical writes have separate coverage.
				processWaitRecord(h, callID, func(record state.Record, _ *stateContext) bool {
					for _, effect := range record.Payload.Effects {
						if effect.Kind != "deliver" {
							continue
						}
						var body stateEngineEffect
						if body.unmarshalMetadata(effect.Payload) != nil {
							return false
						}
						if body.WireMethod == "ACK" && body.WireSequence == invite.CSeq().SeqNo &&
							effect.Status != state.EffectWritten {
							return false
						}
					}
					return true // Collection also requires terminal delivery evidence.
				})
			}
			before, group := h.record(callID)
			original := processOnlyContext(t, group)
			if !original.Cancelled || !processOriginalTransaction(t, original, invite).Native.Cancelled {
				t.Fatal("actual CANCEL did not retain its native cancellation intent")
			}
			h.kill(a)
			for _, branch := range []string{"late-one", "late-two"} {
				replacement := h.phone(b)
				response := processInviteFinal(invite, 200, branch)
				replacement.send(t, []byte(response.String()))
				ack := replacement.receive(t, sip.ACK, 0)
				bye := replacement.receive(t, sip.BYE, 0).(*sip.Request)
				if tag(ack.To().Params) != branch || tag(bye.To().Params) != branch || ack.CSeq().SeqNo != invite.CSeq().SeqNo || bye.CSeq().SeqNo != invite.CSeq().SeqNo+1 {
					t.Fatal("late branch ACK/BYE used another fork's identity or CSeq")
				}
				replacement.send(t, []byte(sip.NewResponseFromRequest(bye, 200, "OK", nil).String()))
				key := tag(invite.From().Params) + "/" + branch
				processWaitRecord(h, callID, func(_ state.Record, c *stateContext) bool { return c.Dialogs[key].Phase == portable.DialogEnded })
				// Repeating a successful final after cleanup still receives the
				// same ACK, without allocating another logical BYE procedure.
				replacement.send(t, []byte(response.String()))
				repeated := replacement.receive(t, sip.ACK, 0)
				if repeated.String() != ack.String() {
					t.Fatal("late2xx replay changed the native ACK")
				}
			}
			after, group := h.record(callID)
			c := processOnlyContext(t, group)
			if before.Ref.Incarnation != after.Ref.Incarnation || !stateSameContext(ref, c.Snapshot.Context) || !sameProcess(c.Snapshot.Application, h.application) || c.Snapshot.Answered || c.Dialog != "" {
				t.Fatal("cancelled late successful fork became an application dialog")
			}
			byes := make(map[string]int)
			for _, tx := range c.Transactions {
				message, err := tx.Native.Origin.Parse()
				if err == nil && message.CSeq().MethodName == sip.BYE {
					byes[tag(message.To().Params)]++
				}
			}
			if byes["late-one"] != 1 || byes["late-two"] != 1 {
				t.Fatalf("late successful retransmission duplicated logical cleanup: %v", byes)
			}
			h.mu.Lock()
			defer h.mu.Unlock()
			for _, observed := range h.observations {
				if event := observed.Envelope.GetDialogEvent(); event != nil && event.Snapshot.CallId == callID && event.Kind == pb.DialogEvent_ANSWERED {
					t.Fatal("cancelled branch published ANSWERED after process takeover")
				}
			}
			if scenario.expiredGrant {
				grants := make(map[string]bool)
				for _, observed := range h.observations {
					if egress := observed.Envelope.GetEgress(); egress != nil && egress.Message.Method == "BYE" && egress.Snapshot.RemoteTag == "late-one" {
						grants[egress.Authorization.Effect.Id] = true
					}
				}
				if len(grants) < 2 {
					t.Fatal("expired cleanup grant did not require a fresh finite emission")
				}
			}
			t.Logf("case=%s group=%s incarnation=%s native forks=%d original=%s replacement=%s", scenario.name, after.Ref.ID, after.Ref.Incarnation, len(c.Dialogs), a.process.Epoch, b.process.Epoch)
		})
	}
}
