package engine

import (
	"bufio"
	"bytes"
	"encoding/json"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
)

func processPlainPhone(t *testing.T, h *processHarness, worker *processWorker, transport string) *processPhone {
	t.Helper()
	address := worker.ready.PlainTCP
	if transport == "udp" {
		address = worker.ready.PlainUDP
	}
	conn, err := net.Dial(transport, address)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	phone := &processPhone{connection: conn, reader: bufio.NewReader(conn)}
	h.mu.Lock()
	h.phones = append(h.phones, phone)
	h.mu.Unlock()
	return phone
}

func TestStateNativeProcessRejectedInviteACKRequiresExactFlowAndFinalIdentity(t *testing.T) {
	processRejectedInviteACK(t, false)
}

func TestNativeProcessRefusalACKNeedsNoController(t *testing.T) {
	processRejectedInviteACK(t, true)
}

func processRejectedInviteACK(t *testing.T, autonomous bool) {
	var h *processHarness
	if autonomous {
		h = newNativeProcessHarness(t)
	} else {
		h = newRelayProcessHarness(t)
	}
	h.cfg.PlainSIP = true
	h.routeRequest = func(controller *processController, envelope *pb.Envelope) bool {
		in := envelope.GetIngress()
		h.mu.Lock()
		h.refs[in.Context.State.CallId] = in.Context
		h.mu.Unlock()
		controller.reply(envelope, &pb.Envelope{Payload: &pb.Envelope_RouteResult{RouteResult: &pb.RouteResult{Outcome: pb.RouteResult_REJECTED, SipStatus: 403}}})
		return true
	}
	worker := h.start("sip-a")
	phone, foreign := processPlainPhone(t, h, worker, "udp"), processPlainPhone(t, h, worker, "udp")
	callID := "private-refusal-ack-" + uuid.NewString()
	wire := bytes.ReplaceAll(processRequest(t, sip.INVITE, callID, 1, ""), []byte("SIP/2.0/TLS"), []byte("SIP/2.0/UDP"))
	phone.send(t, wire)
	final := phone.receive(t, sip.INVITE, 403)
	_, group := h.record(callID)
	var id, key string
	for candidate, context := range group.Contexts {
		id = candidate
		for candidate := range context.Transactions {
			key = candidate
		}
	}
	retained := func() *stateTransaction {
		_, group := h.record(callID)
		return group.Contexts[id].Transactions[key]
	}
	assertPending := func() {
		t.Helper()
		tx := retained()
		if tx.Native.Phase != portable.Completed || !tx.Native.Timers[portable.TimerG].Active || !tx.Native.Timers[portable.TimerH].Active {
			t.Fatal("an invalid ACK stopped the retained refusal procedure")
		}
	}
	assertPending()
	ack, err := portable.BuildNon2xxACK(portable.Message{Wire: wire, Transport: "udp"}, portable.Message{Wire: []byte(final.String()), Transport: "udp"})
	if err != nil {
		t.Fatal(err)
	}
	// A subsequent refused INVITE on the same UDP flow/Call-ID is a route-lane
	// barrier: its response follows completion of that flow's preceding ACK.
	foreign.send(t, ack.Wire)
	foreign.send(t, wire)
	foreign.receive(t, sip.INVITE, 403)
	assertPending()
	wrong, err := ack.Parse()
	if err != nil {
		t.Fatal(err)
	}
	wrong.To().Params.Add("tag", "wrong-final")
	phone.send(t, []byte(wrong.String()))
	phone.send(t, wire)
	phone.receive(t, sip.INVITE, 403)
	assertPending()
	if autonomous {
		h.closeController(worker.peer)
	}
	phone.send(t, ack.Wire)
	until := time.NewTimer(3 * time.Second)
	defer until.Stop()
	tick := time.NewTicker(5 * time.Millisecond)
	defer tick.Stop()
	for {
		tx := retained()
		if tx.Native.Phase == portable.Confirmed && !tx.Native.Timers[portable.TimerG].Active && !tx.Native.Timers[portable.TimerH].Active {
			break
		}
		select {
		case <-tick.C:
		case <-until.C:
			t.Fatal("the exact negative-response ACK did not stop G/H")
		}
	}
	_, group = h.record(callID)
	if len(group.Selectors) != 0 || len(group.Contexts) != 2 {
		t.Fatal("private ACK handling published selectors or allocated another context")
	}
}

func TestStateNativeProcessRejectionCannotReplayOrReplaceAdmittedTransaction(t *testing.T) {
	for _, transport := range []string{"udp", "tcp", "tls"} {
		t.Run(transport, func(t *testing.T) {
			h := newRelayProcessHarness(t)
			h.cfg.PlainSIP = true
			worker := h.start("sip-a")
			connect := func() *processPhone {
				if transport == "tls" {
					return h.phone(worker)
				}
				return processPlainPhone(t, h, worker, transport)
			}
			phone := connect()
			callID := "rejected-replay-" + uuid.NewString()
			wire := bytes.ReplaceAll(processRequest(t, sip.INVITE, callID, 1, ""), []byte("SIP/2.0/TLS"), []byte("SIP/2.0/"+strings.ToUpper(transport)))
			phone.send(t, wire)
			presented := h.request(callID, 1).Envelope.GetRequest()
			h.respond(worker, presented, 200)
			accepted := phone.receive(t, sip.INVITE, 200).(*sip.Response)
			ack := bytes.ReplaceAll(processRequest(t, sip.ACK, callID, 1, tag(accepted.To().Params)), []byte("SIP/2.0/TLS"), []byte("SIP/2.0/"+strings.ToUpper(transport)))
			phone.send(t, ack)
			h.wait(func(observed processObservation) bool {
				event := observed.Envelope.GetDialogEvent()
				return event != nil && event.Kind == pb.DialogEvent_ACKNOWLEDGED && event.Snapshot.CallId == callID
			})
			_, before := h.record(callID)
			original := before.Contexts[presented.Snapshot.Context.Id]
			if original == nil {
				t.Fatal("admitted context missing")
			}
			flow := proto.Clone(original.Flow).(*pb.FlowRef)
			var key string
			for candidate := range original.Transactions {
				key = candidate
			}
			retained := original.Transactions[key]
			if retained == nil || retained.Native.LastResponse == nil {
				t.Fatal("successful native transaction missing")
			}
			origin, final := retained.Native.Origin.Clone(), retained.Native.LastResponse.Clone()
			dialogs, _ := json.Marshal(original.Dialogs)
			h.mu.Lock()
			h.routeRequest = func(controller *processController, envelope *pb.Envelope) bool {
				in := envelope.GetIngress()
				if in.Message.Method == "ACK" {
					// Private refusals do not publish a selector. Route their ACK
					// through the native refusal path, without presenting it to
					// the original admitted dialog.
					controller.reply(envelope, &pb.Envelope{Payload: &pb.Envelope_RouteResult{RouteResult: &pb.RouteResult{Outcome: pb.RouteResult_REJECTED, SipStatus: 403}}})
					return true
				}
				status := uint32(403)
				if !bytes.Equal(in.Message.Wire, wire) {
					// Mirror Controller: native refusal must pass through the actual
					// route-result fallback, rather than stopping at the reducer.
					reply, err := controller.call(&pb.Envelope{Payload: &pb.Envelope_Ingest{Ingest: &pb.OwnerIngest{
						Context: presented.Snapshot.Context, Application: h.application, Flow: in.Flow, Wire: in.Message.Wire,
						PrincipalId: "fixture-phone", BindingId: "fixture-binding", Admission: pb.OwnerIngest_AUTHENTICATED_BINDING,
					}}})
					if err != nil || reply.GetResult() == nil || reply.GetResult().Error == "" || reply.GetResult().SipStatus != 481 {
						t.Errorf("changed retained request was not explicitly refused: %v %s", err, reply.GetResult())
						return true
					}
					status = reply.GetResult().SipStatus
				}
				controller.reply(envelope, &pb.Envelope{Payload: &pb.Envelope_RouteResult{RouteResult: &pb.RouteResult{
					Outcome: pb.RouteResult_REJECTED, SipStatus: status,
				}}})
				return true
			}
			h.mu.Unlock()
			for _, attempt := range []struct {
				name   string
				phone  *processPhone
				wire   []byte
				status int
			}{
				{"unauthorized-flow", connect(), wire, 403},
				{"changed-payload", connect(), bytes.Replace(wire, []byte("v=0"), []byte("v=1"), 1), 481},
				{"transaction-key-collision", connect(), bytes.Replace(wire, []byte(";tag=phone"), []byte(";tag=other"), 1), 481},
			} {
				t.Run(attempt.name, func(t *testing.T) {
					attempt.phone.send(t, attempt.wire)
					response := attempt.phone.receive(t, sip.INVITE, -1).(*sip.Response)
					if response.StatusCode != attempt.status || len(response.Body()) != 0 {
						t.Fatalf("refusal leaked a cached response/body: %d %q", response.StatusCode, response.Body())
					}
					// Act as a SIP client: every non-2xx INVITE final requires
					// its exact transaction ACK. Otherwise the three outstanding
					// UDP G/H procedures turn this isolation test into a race
					// against unrelated retransmission/physical-effect deadlines.
					ack, err := portable.BuildNon2xxACK(portable.Message{Wire: attempt.wire, Transport: transport}, portable.Message{Wire: []byte(response.String()), Transport: transport})
					if err != nil {
						t.Fatal(err)
					}
					attempt.phone.send(t, ack.Wire)
					until := time.Now().Add(3 * time.Second)
					for {
						_, current := h.record(callID)
						confirmed := false
						for _, c := range current.Contexts {
							if !c.Rejection || !bytes.Equal(c.Origin.Wire, attempt.wire) {
								continue
							}
							for _, tx := range c.Transactions {
								confirmed = tx.Native.Phase == portable.Confirmed || tx.Native.Phase == portable.Terminated
							}
						}
						if confirmed {
							break
						}
						if time.Now().After(until) {
							t.Fatal("private refusal ACK did not complete its exact native procedure")
						}
						time.Sleep(5 * time.Millisecond)
					}
					_, after := h.record(callID)
					current := after.Contexts[presented.Snapshot.Context.Id]
					if current == nil || !proto.Equal(current.Flow, flow) || len(current.Transactions) != 1 {
						t.Fatal("rejection changed admitted context/flow/transaction count")
					}
					tx := current.Transactions[key]
					currentDialogs, _ := json.Marshal(current.Dialogs)
					if tx == nil || !proto.Equal(tx.Flow, flow) || !bytes.Equal(tx.Native.Origin.Wire, origin.Wire) ||
						tx.Native.LastResponse == nil || !bytes.Equal(tx.Native.LastResponse.Wire, final.Wire) || !bytes.Equal(currentDialogs, dialogs) {
						t.Fatal("rejection overwrote retained response, origin, dialog or return flow")
					}
					for _, id := range after.Selectors {
						if id != presented.Snapshot.Context.Id {
							t.Fatal("refusal displaced an admitted routing selector")
						}
					}
					for id, context := range after.Contexts {
						if id == presented.Snapshot.Context.Id {
							continue
						}
						if !context.Rejection || len(context.Snapshot.Selectors) != 0 {
							t.Fatal("refusal escaped its private native context")
						}
					}
				})
			}
			h.mu.Lock()
			for _, observation := range h.observations {
				egress := observation.Envelope.GetEgress()
				if egress != nil && egress.Snapshot.CallId == callID && egress.Snapshot.Context.Id != presented.Snapshot.Context.Id &&
					(len(egress.Snapshot.Selectors) != 0 || len(egress.Message.Selectors) != 0) {
					t.Error("refusal exposed routing selectors to the trusted controller")
				}
			}
			h.mu.Unlock()
			// The original dialog remains usable, including its normal cached
			// final-response replay, after all three refusals.
			h.mu.Lock()
			h.routeRequest = func(controller *processController, envelope *pb.Envelope) bool {
				in := envelope.GetIngress()
				reply, err := controller.call(&pb.Envelope{Payload: &pb.Envelope_Ingest{Ingest: &pb.OwnerIngest{
					Context: presented.Snapshot.Context, Application: h.application, Flow: in.Flow, Wire: in.Message.Wire,
					PrincipalId: "fixture-phone", BindingId: "fixture-binding", Admission: pb.OwnerIngest_AUTHENTICATED_BINDING,
				}}})
				if err != nil || reply.GetResult() == nil || reply.GetResult().Error != "" {
					t.Errorf("original dialog refused valid terminal request: %v %s", err, reply.GetResult())
				}
				controller.reply(envelope, &pb.Envelope{Payload: &pb.Envelope_RouteResult{RouteResult: &pb.RouteResult{Outcome: pb.RouteResult_ACCEPTED}}})
				return true
			}
			h.mu.Unlock()
			bye := bytes.ReplaceAll(processRequest(t, sip.BYE, callID, 2, tag(accepted.To().Params)), []byte("SIP/2.0/TLS"), []byte("SIP/2.0/"+strings.ToUpper(transport)))
			phone.send(t, bye)
			completed := phone.receive(t, sip.BYE, 200)
			phone.send(t, bye)
			replayed := phone.receive(t, sip.BYE, 200)
			if completed.String() != replayed.String() {
				t.Fatal("valid retained replay changed its accepted response")
			}
		})
	}
}
