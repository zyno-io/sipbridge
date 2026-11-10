package engine

import (
	"bufio"
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/emiago/sipgo/sip"
	"sipbridge/internal/control"
	pb "sipbridge/internal/controlpb"
)

func methodPolicyWire(method, transport, toTag string) []byte {
	return []byte(fmt.Sprintf("%s sip:service@example.test SIP/2.0\r\nVia: SIP/2.0/%s 192.0.2.1:5060;branch=z9hG4bK-policy;rport\r\nFrom: <sip:phone@example.test>;tag=phone\r\nTo: <sip:service@example.test>%s\r\nCall-ID: policy-call\r\nCSeq: 1 %s\r\nExpires: 0\r\nContent-Length: 0\r\n\r\n", method, strings.ToUpper(transport), toTag, method))
}

func assertMethodRefusal(t *testing.T, wire []byte, method, toTag string, policy MethodPolicy) {
	t.Helper()
	parsed, err := sip.ParseMessage(wire)
	if err != nil {
		t.Fatal(err)
	}
	response, ok := parsed.(*sip.Response)
	if !ok || response.StatusCode != 405 || response.Reason != "Method Not Allowed" || response.CSeq().MethodName != sip.RequestMethod(method) {
		t.Fatalf("unexpected method response: %s", wire)
	}
	if header(response, "Allow") != policy.allow() || header(response, "WWW-Authenticate") != "" || header(response, "Retry-After") != "" {
		t.Fatalf("unexpected policy headers: %s", wire)
	}
	if response.Via().Params.GetOr("received", "") != "127.0.0.1" || response.Via().Params.GetOr("rport", "") == "" {
		t.Fatalf("missing physical response evidence: %s", wire)
	}
	tag := response.To().Params.GetOr("tag", "")
	if toTag == "" && !strings.HasPrefix(tag, "policy-") || toTag != "" && tag != "retained" {
		t.Fatalf("unexpected To tag: %q", tag)
	}
}

func TestDisabledMethodsReturnLocal405OverUDPWithoutStateOrControl(t *testing.T) {
	for _, method := range []string{"REGISTER", "SUBSCRIBE"} {
		for _, toTag := range []string{"", ";tag=retained"} {
			t.Run(method+toTag, func(t *testing.T) {
				policy := MethodPolicy{DisableRegister: true, DisableSubscribe: true}
				var probes atomic.Int64
				f := newUDPOrderFixture(t, func(i *Ingress) {
					i.methodPolicy = policy
					i.stateCompletion = func(context.Context, *pb.FlowRef, *pb.SipMessage) (bool, error) {
						probes.Add(1)
						return true, nil
					}
				})
				var previous string
				for attempt := 0; attempt < 2; attempt++ {
					if _, err := f.remote.WriteTo(methodPolicyWire(method, "udp", toTag), f.listener.LocalAddr()); err != nil {
						t.Fatal(err)
					}
					_ = f.remote.SetReadDeadline(time.Now().Add(time.Second))
					wire := make([]byte, 4096)
					n, _, err := f.remote.ReadFrom(wire)
					if err != nil {
						t.Fatal(err)
					}
					assertMethodRefusal(t, wire[:n], method, toTag, policy)
					if attempt > 0 && previous != string(wire[:n]) {
						t.Fatal("retransmission changed local response")
					}
					previous = string(wire[:n])
				}
				if probes.Load() != 0 {
					t.Fatal("disabled request touched state")
				}
				select {
				case message := <-f.received:
					t.Fatalf("disabled request reached control: %v", message.Payload)
				default:
				}
			})
		}
	}
}

func TestDisabledMethodsReturn405OnLiveTCPAndTLSWithoutReadyController(t *testing.T) {
	for _, transport := range []string{"tcp", "tls"} {
		t.Run(transport, func(t *testing.T) {
			local, remote := outboundStreamPair(t, transport)
			policy := MethodPolicy{DisableRegister: true, DisableSubscribe: true}
			ingress := NewIngress(&pb.ProcessRef{InstanceId: "policy", Epoch: "epoch"}, new(control.Server), "sip.example.test:5061", 8, IngressOptions{MethodPolicy: policy})
			flow, err := ingress.addStream(local, "127.0.0.1", 5060, transport)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan struct{})
			go func() { defer close(done); ingress.readStream(ctx, flow) }()
			t.Cleanup(func() { cancel(); local.Close(); <-done })
			phone := &processPhone{connection: remote, reader: bufio.NewReader(remote)}
			for _, method := range []string{"REGISTER", "SUBSCRIBE"} {
				if _, err := remote.Write(methodPolicyWire(method, transport, "")); err != nil {
					t.Fatal(err)
				}
				response := phone.receive(t, sip.RequestMethod(method), 405)
				assertMethodRefusal(t, []byte(response.String()), method, "", policy)
			}
		})
	}
}

func TestMethodPolicyIsIndependentAndNeverBlocksResponsesOrCallMethods(t *testing.T) {
	for _, policy := range []MethodPolicy{{}, {DisableRegister: true}, {DisableSubscribe: true}, {DisableRegister: true, DisableSubscribe: true}} {
		for _, method := range []string{"REGISTER", "SUBSCRIBE", "INVITE", "ACK", "CANCEL", "BYE", "OPTIONS", "INFO", "UPDATE", "PRACK", "REFER", "NOTIFY"} {
			request := &pb.SipMessage{Method: method}
			want := method == "REGISTER" && policy.DisableRegister || method == "SUBSCRIBE" && policy.DisableSubscribe
			if policy.disabled(request) != want {
				t.Fatalf("policy %+v rejected unexpected method %s", policy, method)
			}
			request.Status = 200
			if policy.disabled(request) {
				t.Fatalf("policy blocked response to %s", method)
			}
		}
	}
}
