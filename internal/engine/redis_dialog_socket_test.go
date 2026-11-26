package engine

import (
	"bytes"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
	"google.golang.org/protobuf/proto"
	"sipbridge/internal/control"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/rediscore"
)

func TestDialogReplacementProvesFirstSIPHop(t *testing.T) {
	for _, test := range []struct {
		name, route, ip string
		port            uint32
		accepted        bool
	}{
		{"direct UA behind NAT", "", "192.0.2.25", 60202, true},
		{"loose IPv4 route", "<sips:192.0.2.25:5061;lr>", "192.0.2.25", 5061, true},
		{"strict IPv6 with default port", "<sips:[2001:db8::20]>", "2001:db8:0:0::20", 5061, true},
		{"different port", "<sips:192.0.2.25:5071;lr>", "192.0.2.25", 5061, false},
		{"unverified hostname", "<sips:proxy.example.test:5061;lr>", "192.0.2.25", 5061, false},
		{"different transport", "<sip:192.0.2.25:5061;Transport=UDP;lr>", "192.0.2.25", 5061, false},
		{"alternate destination", "<sips:192.0.2.25:5061;maddr=192.0.2.26;lr>", "192.0.2.25", 5061, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			dialog := portable.Dialog{LocalTag: "local", RemoteTag: "remote", CallID: "test", SentBy: "local.example.test:5061",
				Target: "sips:callee@10.0.0.8:5061", LocalURI: "sips:local@example.test", RemoteURI: "sips:remote@example.test", Transport: "tls"}
			if test.route != "" {
				dialog.Routes = []string{test.route}
			}
			message, err := portable.BuildDialogRequest(dialog, sip.INFO, 1, nil, "z9hG4bK-replacement-hop")
			if err != nil {
				t.Fatal(err)
			}
			flow := &pb.FlowRef{Transport: "tls", SourceIp: test.ip, SourcePort: test.port}
			err = validateDialogReplacementHop(dialog.Routes, message, flow)
			if test.accepted && err != nil || !test.accepted && !errors.Is(err, errStatePolicyAdmission) {
				t.Fatal("unexpected next-hop decision", err)
			}
		})
	}
}

func TestRedisNativeDialogSocketRefusalDoesNotConsumeCommand(t *testing.T) {
	var live atomic.Bool
	live.Store(true)
	e, peer, conn, ctx, session, flow, writes := peerACKRedisFixtureWithCapabilities(t, false, func(*pb.FlowRef) bool {
		return live.Load()
	}, control.DialogSocketCapability)
	initial := stateTestRequest("INVITE", 1, "", "dialog-socket-owner")
	if err := e.ReceiveRequest(ctx, peer, flow, initial); err != nil {
		t.Fatal(err)
	}
	request := peerACKRead(t, ctx, conn, session).GetRequest()
	if request == nil {
		t.Fatal("initial request missing")
	}
	accepted := residentSendDecision(t, ctx, conn, session, "initial-answer", residentDirectDecision(request, 1, 200))
	if accepted.GetCertainty() != pb.OperationResult_ACCEPTED {
		t.Fatal("initial response", accepted)
	}
	peerACKFinal(t, ctx, writes)

	ref := accepted.GetSnapshot().GetContext()
	store, _, _, err := e.driver.Current()
	if err != nil {
		t.Fatal(err)
	}
	selected, err := store.ObserveContext(ctx, ref.GetId(), ref.GetState().GetIncarnation(), "r:context")
	if err != nil {
		t.Fatal(err)
	}
	current, err := decodeNativeContext(selected.View.Records["r:context"])
	if err != nil || len(current.Dialogs) != 1 || current.CommandSequence != 1 {
		t.Fatal("initial command state", err)
	}
	dialogField := current.Dialogs[0]
	before, err := store.Read(ctx, selected.Identity, "r:context", "r:result-2", dialogField)
	if err != nil {
		t.Fatal(err)
	}
	attempt := func(candidate *pb.FlowRef, operation string) *pb.OperationResult {
		t.Helper()
		until := time.Now().Add(time.Second)
		command := &pb.Command{Context: ref, Sequence: 2, Admission: &pb.CommandAdmission{
			DirectoryEpoch: current.DirectoryEpoch, ExpiresAtMs: uint64(until.UnixMilli()), SelectedFlow: candidate, ExpectedDelivery: current.Delivery,
		}, Action: &pb.Command_DialogRequest{DialogRequest: &pb.DialogRequest{Method: "BYE"}}}
		result, x := e.commandApplicationBye(ctx, peer, command, operation, until)
		if x != nil {
			t.Fatal(x)
		}
		return result
	}
	other := proto.Clone(flow).(*pb.FlowRef)
	other.Ingress.InstanceId = "another-instance"
	old := proto.Clone(flow).(*pb.FlowRef)
	old.Ingress.Epoch = "old-process-epoch"
	for _, candidate := range []*pb.FlowRef{other, old} {
		if result := attempt(candidate, "invalid-socket-owner"); result.GetCertainty() != pb.OperationResult_NOT_ACCEPTED || result.GetDialogRecovery() != nil {
			t.Fatal("nonlocal socket refusal", result)
		}
	}
	live.Store(false)
	unknown := proto.Clone(flow).(*pb.FlowRef)
	unknown.ConnectionId = "unknown-socket"
	if result := attempt(unknown, "unknown-local-socket"); result.GetCertainty() != pb.OperationResult_NOT_ACCEPTED || result.GetDialogRecovery() != nil {
		t.Fatal("unknown socket refusal", result)
	}
	if result := attempt(flow, "closed-dialog-socket"); result.GetCertainty() != pb.OperationResult_NOT_ACCEPTED || result.GetDialogRecovery().GetDisposition() != pb.DialogRecovery_SOCKET_GONE {
		t.Fatal("closed socket refusal", result)
	}
	after, err := store.Read(ctx, selected.Identity, "r:context", "r:result-2", dialogField)
	if err != nil || len(after.Records["r:result-2"]) != 0 || !bytes.Equal(before.Records["r:context"], after.Records["r:context"]) || !bytes.Equal(before.Records[dialogField], after.Records[dialogField]) {
		t.Fatal("refused commands changed native state", err)
	}
	select {
	case wire := <-writes:
		t.Fatalf("refused commands wrote %s", wire)
	default:
	}
	live.Store(true)
	if result := attempt(flow, "same-sequence-live-socket"); result.GetCertainty() != pb.OperationResult_ACCEPTED || result.GetRequest().GetMethod() != "BYE" {
		t.Fatal("same sequence on live socket", result)
	}
	select {
	case wire := <-writes:
		if !bytes.HasPrefix(wire, []byte("BYE ")) {
			t.Fatalf("unexpected live write %s", wire)
		}
	case <-ctx.Done():
		t.Fatal("BYE write missing")
	}
}

func TestRedisNativeDialogRegistrationFallbackAdoption(t *testing.T) {
	for _, scenario := range []struct {
		name       string
		method     string
		status     int
		superseded bool
		firstRoute string
	}{
		{"matching-info-response-adopts", "INFO", 200, false, ""},
		{"matching-reinvite-response-adopts", "INVITE", 200, false, ""},
		{"unknown-dialog-keeps-old-route", "INFO", 481, false, ""},
		{"a-later-accepted-route-wins", "INFO", 200, true, ""},
		{"fallback-cannot-bypass-loose-route", "INFO", 0, false, "<sips:proxy@192.0.2.41:5061;lr>"},
		{"fallback-cannot-bypass-strict-route", "INVITE", 0, false, "<sips:proxy@192.0.2.41:5061>"},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			e, peer, conn, ctx, session, flow, writes := peerACKRedisFixtureWithCapabilities(t, false, func(*pb.FlowRef) bool {
				return true
			}, control.DialogSocketCapability)
			initial := stateTestRequest("INVITE", 1, "", "dialog-registration-fallback")
			if scenario.firstRoute != "" {
				initial.Wire = bytes.Replace(initial.Wire, []byte("\r\n\r\n"), []byte("\r\nRecord-Route: "+scenario.firstRoute+"\r\n\r\n"), 1)
			}
			if err := e.ReceiveRequest(ctx, peer, flow, initial); err != nil {
				t.Fatal(err)
			}
			request := peerACKRead(t, ctx, conn, session).GetRequest()
			if request == nil {
				t.Fatal("initial request missing")
			}
			answered := residentSendDecision(t, ctx, conn, session, "initial-answer", residentDirectDecision(request, 1, 200))
			if answered.GetCertainty() != pb.OperationResult_ACCEPTED {
				t.Fatal("initial response", answered)
			}
			final := peerACKFinal(t, ctx, writes)
			if scenario.method == "INVITE" {
				remote, err := portable.NewDialog(portable.UAC, portable.RestoreMessage(initial.Wire, "tls", "", ""),
					portable.RestoreMessage(final, "tls", "", ""), portable.DefaultTimers())
				if err != nil {
					t.Fatal(err)
				}
				ack, err := portable.BuildDialogRequestWithHeaders(remote, sip.ACK, 1, nil, "z9hG4bK-initial-ack-for-registration-fallback", nil)
				if err != nil {
					t.Fatal(err)
				}
				if handled, x := e.TryNativeCompletion(ctx, flow, &pb.SipMessage{Wire: ack.WireBytes()}); x != nil || !handled {
					t.Fatal("initial ACK", handled, x)
				}
				frame := peerACKRead(t, ctx, conn, session)
				if frame.GetDialogEvent().GetKind() != pb.DialogEvent_ACKNOWLEDGED {
					t.Fatal("initial ACK event missing", frame)
				}
				writeControllerEnvelope(t, ctx, conn, &pb.Envelope{SessionId: session, CorrelationId: frame.CorrelationId,
					Payload: &pb.Envelope_Result{Result: &pb.OperationResult{Certainty: pb.OperationResult_ACCEPTED}}})
			}
			snapshot := answered.GetSnapshot()
			ref := snapshot.GetContext()
			candidateFlow := proto.Clone(flow).(*pb.FlowRef)
			candidateFlow.ConnectionId = "accepted-registration-socket"
			candidateFlow.SourcePort++
			candidate := &pb.ContextDelivery{Flow: candidateFlow, NativeRevision: snapshot.GetDelivery().GetNativeRevision() + 1,
				DirectoryEpoch: snapshot.GetDirectoryEpoch(), RegistrationTicket: snapshot.GetDirectoryEpoch() + ":1"}
			until := time.Now().Add(2 * time.Second)
			command := &pb.Command{Context: ref, Sequence: 2, Admission: &pb.CommandAdmission{
				DirectoryEpoch: snapshot.GetDirectoryEpoch(), ExpiresAtMs: uint64(until.UnixMilli()), SelectedFlow: candidateFlow,
				ExpectedDelivery: snapshot.GetDelivery(), RegistrationFallback: true, FallbackDelivery: candidate,
			}, Action: &pb.Command_DialogRequest{DialogRequest: &pb.DialogRequest{Method: scenario.method}}}
			const operation = "registration-fallback"
			if scenario.firstRoute != "" {
				result, err := e.commandApplicationBye(ctx, peer, command, operation, until)
				if err != nil || result.GetCertainty() != pb.OperationResult_NOT_ACCEPTED || !strings.Contains(result.GetError(), "first SIP next hop") {
					t.Fatalf("routed fallback refusal: %v %v", result, err)
				}
				store, _, _, err := e.driver.Current()
				if err != nil {
					t.Fatal(err)
				}
				records, err := store.ObserveContext(ctx, ref.Id, ref.State.Incarnation, "r:context", "r:result-2")
				if err != nil {
					t.Fatal(err)
				}
				current, err := decodeNativeContext(records.View.Records["r:context"])
				if err != nil || current.CommandSequence != 1 || len(records.View.Records["r:result-2"]) != 0 {
					t.Fatal("refused replacement consumed the command", current, err)
				}
				select {
				case wire := <-writes:
					t.Fatalf("routed fallback wrote through the wrong socket: %s", wire)
				default:
				}
				return
			}
			writeControllerEnvelope(t, ctx, conn, &pb.Envelope{SessionId: session, CorrelationId: operation, RemainingBudgetMs: 2000,
				Payload: &pb.Envelope_Command{Command: command}})
			var wire []byte
			select {
			case wire = <-writes:
				if !bytes.HasPrefix(wire, []byte(scenario.method+" ")) {
					t.Fatalf("unexpected write %s", wire)
				}
			case <-ctx.Done():
				t.Fatal("fallback request missing")
			}
			store, _, _, err := e.driver.Current()
			if err != nil {
				t.Fatal(err)
			}
			expectedDelivery, expectedFlow := snapshot.GetDelivery(), flow
			if scenario.status == 200 {
				expectedDelivery, expectedFlow = candidate, candidateFlow
			}
			if scenario.superseded {
				expectedDelivery = proto.Clone(candidate).(*pb.ContextDelivery)
				expectedDelivery.Flow.ConnectionId = "newer-dialog-socket"
				expectedFlow = expectedDelivery.Flow
				for {
					selected, x := store.ObserveContext(ctx, ref.GetId(), ref.GetState().GetIncarnation(), "r:context")
					if x != nil {
						t.Fatal(x)
					}
					current, x := decodeNativeContext(selected.View.Records["r:context"])
					if x != nil {
						t.Fatal(x)
					}
					current.Flow, current.Delivery = expectedFlow, expectedDelivery
					current.Revision++
					contextData, x := encodeNativeContext(current)
					if x != nil {
						t.Fatal(x)
					}
					tag, x := nativeDeliveryTagChange(expectedDelivery)
					if x != nil {
						t.Fatal(x)
					}
					_, x = store.Commit(ctx, rediscore.Publication{Identity: selected.Identity, Revision: selected.View.Revision,
						Deadline: until, Changes: []rediscore.Change{{Field: "r:context", Value: contextData}, tag}})
					if errors.Is(x, rediscore.ErrConflict) && time.Now().Before(until) {
						continue
					}
					if x != nil {
						t.Fatal(x)
					}
					break
				}
			}
			contact := ""
			if scenario.method == "INVITE" {
				contact = "<sips:phone@192.0.2.12:5061>"
			}
			response, err := portable.BuildResponseWithHeaders(portable.RestoreMessage(wire, "tls", "", ""), scenario.status,
				"fixture final", nil, "", contact, nil)
			if err != nil {
				t.Fatal(err)
			}
			handled, err := e.TryNativeCompletion(ctx, candidateFlow, &pb.SipMessage{Wire: response.WireBytes()})
			if err != nil || !handled {
				t.Fatal("fallback response", handled, err)
			}
			frame := peerACKRead(t, ctx, conn, session)
			result := frame.GetResult()
			if frame.GetCorrelationId() != operation || result.GetCertainty() != pb.OperationResult_COMPLETED || result.GetSipStatus() != uint32(scenario.status) {
				t.Fatal("fallback result", frame)
			}
			selected, err := store.ObserveContext(ctx, ref.GetId(), ref.GetState().GetIncarnation(), "r:context", "r:delivery-tag")
			if err != nil {
				t.Fatal(err)
			}
			current, err := decodeNativeContext(selected.View.Records["r:context"])
			if err != nil {
				t.Fatal(err)
			}
			if !proto.Equal(result.GetSnapshot().GetDelivery(), expectedDelivery) || !proto.Equal(current.Delivery, expectedDelivery) || !proto.Equal(current.Flow, expectedFlow) {
				t.Fatal("unexpected route after final response", current.Delivery, current.Flow)
			}
			tag, err := nativeDeliveryTagChange(expectedDelivery)
			if err != nil || !bytes.Equal(tag.Value, selected.View.Records[tag.Field]) {
				t.Fatal("delivery tag disagrees with the native context", err)
			}
		})
	}
}
