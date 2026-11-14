package engine

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
	"sipbridge/internal/control"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/state"
)

func TestResidentIngressRefusalReusesFrameProof(t *testing.T) {
	for _, route := range []string{"stream", "udp"} {
		for _, reason := range []string{"no-peer", "refused", "unavailable"} {
			t.Run(route+"/"+reason, func(t *testing.T) {
				process := &pb.ProcessRef{InstanceId: "sip", Epoch: "epoch"}
				i := NewIngress(process, &control.Server{}, "", 4, IngressOptions{OnRouteTiming: func(IngressRouteTiming) {}})
				i.stateWriter = &evidenceWriter{}
				i.stateReject = func(context.Context, *pb.FlowRef, *pb.SipMessage, uint32) error {
					return errors.New("unexpected raw refusal")
				}
				wire := methodPolicyWire("REGISTER", "udp", "")
				parsed, err := parseObservedMessage(wire)
				if err != nil {
					t.Fatal(err)
				}
				message := i.projectInboundMessage(wire, parsed)
				flow := &physicalFlow{ref: &pb.FlowRef{Ingress: process, Transport: "udp", SourceIp: "192.0.2.4", SourcePort: 5060}}
				var captured portable.Message
				i.directCompletion = func(_ context.Context, _ *pb.FlowRef, _ *pb.SipMessage, got sip.Message, native portable.Message) (bool, error) {
					if got != parsed {
						return false, errors.New("completion did not receive ingress parse")
					}
					captured = native
					switch reason {
					case "refused":
						return false, rejectIngest(486, "refused")
					case "unavailable":
						return false, &stateReadFailure{state.ErrAuthority}
					}
					return false, nil
				}
				result := make(chan error, 1)
				i.directReject = func(ctx context.Context, gotFlow *pb.FlowRef, gotMessage *pb.SipMessage, status uint32, got sip.Message, native portable.Message) error {
					if got != parsed || !native.Equal(captured) || !bytes.Equal(native.WireBytes(), wire) || gotFlow != flow.ref || gotMessage != message {
						result <- errors.New("refusal did not receive the completion's same-frame evidence")
						return nil
					}
					want := uint32(503)
					if reason == "refused" {
						want = 486
					}
					if status != want || ctx.Err() != nil {
						result <- errors.New("refusal status or deadline changed")
						return nil
					}
					result <- nil
					return nil
				}
				if route == "stream" {
					i.routeParsed(context.Background(), flow, message, parsed)
				} else if !i.routeUDPParsed(context.Background(), flow, message, parsed, parsed.CallID().Value()) {
					t.Fatal("UDP route was not queued")
				}
				select {
				case err := <-result:
					if err != nil {
						t.Fatal(err)
					}
				case <-time.After(time.Second):
					t.Fatal("native refusal was not attempted")
				}
			})
		}
	}
}

func TestResidentPublicRejectValidatesRawWire(t *testing.T) {
	process := &pb.ProcessRef{InstanceId: "sip", Epoch: "epoch"}
	e := &ResidentMemory{cfg: Config{Process: process}}
	flow := &pb.FlowRef{Ingress: process, Transport: "udp"}
	if err := e.Reject(context.Background(), flow, &pb.SipMessage{Wire: []byte("not SIP"), Method: "REGISTER"}, 503); err == nil {
		t.Fatal("raw refusal trusted caller metadata instead of validating wire")
	}
}

func TestResidentParsedRefusalRetainsOriginalFinalOnReplay(t *testing.T) {
	e, _, _, ctx, _, writes, _ := residentControlFixture(t)
	flow := residentDirectFlow(e)
	flow.Transport = "udp"
	wire := methodPolicyWire("REGISTER", "udp", "")
	parsed, err := parseObservedMessage(wire)
	if err != nil {
		t.Fatal(err)
	}
	native, err := portable.FreezeMessage(wire, parsed, flow.Transport, "", "")
	if err != nil {
		t.Fatal(err)
	}
	message := projectObservedMessage(wire, parsed, false)
	if err := e.RejectParsed(ctx, flow, message, 503, parsed, native); err != nil {
		t.Fatal(err)
	}
	first := <-writes
	if err := e.RejectParsed(ctx, flow, message, 486, parsed, native); err != nil {
		t.Fatal(err)
	}
	var second []byte
	select {
	case second = <-writes:
	case <-time.After(time.Second):
		t.Fatal("refusal replay was not emitted")
	}
	if !bytes.Equal(first, second) {
		t.Fatalf("replay changed the selected refusal: %s / %s", first, second)
	}
	response, err := sip.ParseMessage(first)
	if err != nil || response.(*sip.Response).StatusCode != 503 {
		t.Fatalf("unexpected selected refusal: %s (%v)", first, err)
	}
	ackWire := methodPolicyWire("ACK", "udp", "")
	ack, err := parseObservedMessage(ackWire)
	if err != nil {
		t.Fatal(err)
	}
	ackNative, err := portable.FreezeMessage(ackWire, ack, flow.Transport, "", "")
	if err != nil {
		t.Fatal(err)
	}
	if err := e.RejectParsed(ctx, flow, projectObservedMessage(ackWire, ack, false), 503, ack, ackNative); err != nil {
		t.Fatal(err)
	}
	select {
	case wire := <-writes:
		t.Fatalf("refusal emitted a response to ACK: %s", wire)
	default:
	}
}
