package engine

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/emiago/sipgo/sip"
	"google.golang.org/protobuf/proto"
	"sipbridge/internal/control"
	pb "sipbridge/internal/controlpb"
)

func testChallengePolicy() *pb.DigestChallengePolicy {
	return &pb.DigestChallengePolicy{Realm: "example.test", AuthenticationEpoch: "redis-one", SigningKey: bytes.Repeat([]byte{7}, 32), ExpiresAtMs: uint64(time.Now().Add(30 * time.Second).UnixMilli())}
}

func untaggedChallengeMessage(t *testing.T, method string) *pb.SipMessage {
	t.Helper()
	message, err := Observe(methodPolicyWire(method, "udp", ""), false)
	if err != nil {
		t.Fatal(err)
	}
	return message
}

func TestRequiredAuthConfigurationRejectsUnsupportedOrConflictingMethods(t *testing.T) {
	for _, raw := range []string{"ACK", "CANCEL", "BYE", "register", "INVITE,INVITE", ",INVITE", "INVITE,"} {
		if _, err := ParseRequiredAuthMethods(raw, MethodPolicy{}); err == nil {
			t.Fatalf("accepted invalid required-auth configuration: %s", raw)
		}
	}
	for _, raw := range []string{"REGISTER", "SUBSCRIBE"} {
		if _, err := ParseRequiredAuthMethods(raw, MethodPolicy{DisableRegister: true, DisableSubscribe: true}); err == nil {
			t.Fatal("accepted disabled required method", raw)
		}
	}
	policy, err := ParseRequiredAuthMethods("REGISTER,INVITE,SUBSCRIBE", MethodPolicy{})
	if err != nil || !policy.RequireRegister || !policy.RequireInvite || !policy.RequireSubscribe {
		t.Fatal("valid required-auth configuration failed", err)
	}
}

func TestLocalChallengeMatchesCanonicalCiscoSizedNonceVector(t *testing.T) {
	data, err := os.ReadFile("testdata/digest-challenge-case.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixture struct {
		Epoch, Realm, Wire, Nonce, SigningKey string
		Timestamp                             int64
	}
	if err := json.Unmarshal(data, &fixture); err != nil {
		t.Fatal(err)
	}
	key, err := base64.RawURLEncoding.DecodeString(fixture.SigningKey)
	if err != nil {
		t.Fatal(err)
	}
	message, err := Observe([]byte(fixture.Wire), false)
	if err != nil {
		t.Fatal(err)
	}
	policy := &pb.DigestChallengePolicy{Realm: fixture.Realm, AuthenticationEpoch: fixture.Epoch, SigningKey: key}
	nonce := localChallengeNonce(policy, message, fixture.Timestamp)
	if nonce != fixture.Nonce || len(nonce) != 120 {
		t.Fatalf("native nonce differs from canonical fixture: %s", nonce)
	}
	for _, from := range []string{"<sip:phone@EXAMPLE.TEST:5061>", "Desk <sips:phone@example.test>"} {
		message.Headers = []*pb.Header{{Name: "from", Value: from}}
		if scope := localChallengeScope(message); scope != "example.test\n" {
			t.Fatalf("noncanonical account scope: %q", scope)
		}
	}
}

func TestLocalChallengesUseNoStateOrControlAndACKStaysLocal(t *testing.T) {
	for _, method := range []string{"REGISTER", "INVITE", "SUBSCRIBE"} {
		t.Run(method, func(t *testing.T) {
			var probes atomic.Int64
			f := newUDPOrderFixture(t, func(i *Ingress) {
				i.authPolicy = &pb.InboundAuthPolicy{RequireRegister: true, RequireInvite: true, RequireSubscribe: true}
				i.stateCompletion = func(context.Context, *pb.FlowRef, *pb.SipMessage) (bool, error) {
					probes.Add(1)
					return true, nil
				}
				if err := i.UpdateChallengePolicy(testChallengePolicy()); err != nil {
					t.Fatal(err)
				}
			})
			message := untaggedChallengeMessage(t, method)
			var response *sip.Response
			for attempt := 0; attempt < 2; attempt++ {
				if _, err := f.remote.WriteTo(message.Wire, f.listener.LocalAddr()); err != nil {
					t.Fatal(err)
				}
				_ = f.remote.SetReadDeadline(time.Now().Add(time.Second))
				buffer := make([]byte, 4096)
				n, _, err := f.remote.ReadFrom(buffer)
				if err != nil {
					t.Fatal(err)
				}
				parsed, err := sip.ParseMessage(buffer[:n])
				if err != nil {
					t.Fatal(err)
				}
				current, ok := parsed.(*sip.Response)
				if !ok || current.StatusCode != 401 || !strings.Contains(header(current, "WWW-Authenticate"), "realm=\"example.test\"") {
					t.Fatalf("unexpected local challenge: %s", buffer[:n])
				}
				if response != nil && response.To().Params.GetOr("tag", "") != current.To().Params.GetOr("tag", "") {
					t.Fatal("challenge retransmission changed transaction tag")
				}
				response = current
			}
			if method == "INVITE" {
				original, _ := sip.ParseMessage(message.Wire)
				ack := original.(*sip.Request).Clone()
				ack.Method, ack.CSeq().MethodName = sip.ACK, sip.ACK
				ack.To().Params.Add("tag", response.To().Params.GetOr("tag", ""))
				if _, err := f.remote.WriteTo([]byte(ack.String()), f.listener.LocalAddr()); err != nil {
					t.Fatal(err)
				}
				// The reader consumes this datagram before the route-lane barrier below.
				waitUDPOrder(t, func() bool { return f.listener.reads.Load() >= 3 })
				// Same-flow/Call-ID retransmission responds only after the ACK lane.
				_, _ = f.remote.WriteTo(message.Wire, f.listener.LocalAddr())
				buffer := make([]byte, 4096)
				_ = f.remote.SetReadDeadline(time.Now().Add(time.Second))
				if _, _, err := f.remote.ReadFrom(buffer); err != nil {
					t.Fatal("challenge ACK did not finish locally", err)
				}
			}
			if probes.Load() != 0 {
				t.Fatal("local challenge/ACK touched native state")
			}
			select {
			case message := <-f.received:
				t.Fatalf("local challenge/ACK reached Controller: %T", message.Payload)
			default:
			}
		})
	}
}

func TestLocalChallengeFlowsOverTCPAndTLSWithoutReadyController(t *testing.T) {
	for _, transport := range []string{"tcp", "tls"} {
		t.Run(transport, func(t *testing.T) {
			local, remote := outboundStreamPair(t, transport)
			ingress := NewIngress(&pb.ProcessRef{InstanceId: "challenge", Epoch: "epoch"}, new(control.Server), "sip.example.test:5061", 8,
				IngressOptions{AuthPolicy: &pb.InboundAuthPolicy{RequireRegister: true, RequireInvite: true}})
			if err := ingress.UpdateChallengePolicy(testChallengePolicy()); err != nil {
				t.Fatal(err)
			}
			flow, err := ingress.addStream(local, "127.0.0.1", 5060, transport)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan struct{})
			go func() { defer close(done); ingress.readStream(ctx, flow) }()
			t.Cleanup(func() { cancel(); local.Close(); <-done })
			phone := &processPhone{connection: remote, reader: bufio.NewReader(remote)}
			for _, method := range []string{"REGISTER", "INVITE"} {
				phone.send(t, methodPolicyWire(method, transport, ""))
				response := phone.receive(t, sip.RequestMethod(method), 401)
				if header(response, "WWW-Authenticate") == "" {
					t.Fatal("local challenge is missing digest parameters")
				}
			}
		})
	}
}

func TestChallengePolicyBoundsExpiryAndRetiredACKKeys(t *testing.T) {
	ingress := NewIngress(&pb.ProcessRef{}, new(control.Server), "", 8)
	for _, invalid := range []*pb.DigestChallengePolicy{nil, {}, {Realm: "bad\r\nrealm"},
		{Realm: "example.test", AuthenticationEpoch: "epoch", SigningKey: make([]byte, 32), ExpiresAtMs: uint64(time.Now().Add(2 * time.Minute).UnixMilli())}} {
		if err := ingress.UpdateChallengePolicy(invalid); err == nil {
			t.Fatal("accepted invalid challenge policy")
		}
	}
	if ingress.ChallengeReady() {
		t.Fatal("cold ingress had issuance authority")
	}
	policy := testChallengePolicy()
	if err := ingress.UpdateChallengePolicy(policy); err != nil {
		t.Fatal(err)
	}
	policy.SigningKey[0]++
	if ingress.challengePolicy().SigningKey[0] != 7 {
		t.Fatal("caller mutation changed accepted key")
	}
	ingress.challenges.until = time.Now().Add(-time.Second)
	if ingress.ChallengeReady() {
		t.Fatal("expired issuance policy stayed ready")
	}
}

func TestRequiredAuthPreservesCredentialResponsesAndDialogPaths(t *testing.T) {
	ingress := &Ingress{authPolicy: &pb.InboundAuthPolicy{RequireRegister: true, RequireInvite: true, RequireSubscribe: true}}
	for _, method := range []string{"REGISTER", "INVITE", "SUBSCRIBE"} {
		message := untaggedChallengeMessage(t, method)
		if !ingress.requiresLocalChallenge(message) {
			t.Fatal("missing credentials bypassed required auth", method)
		}
		message.Authorization = &pb.DigestCredentials{Username: "phone"}
		if ingress.requiresLocalChallenge(message) {
			t.Fatal("credential-bearing request was not sent to trusted verification", method)
		}
		message.Authorization = nil
		message.Status = 401
		if ingress.requiresLocalChallenge(message) {
			t.Fatal("challenged SIP response", method)
		}
		message.Status = 0
		if method != "REGISTER" {
			message.Addresses["to"].Values[0].Params["tag"] = "retained"
			if ingress.requiresLocalChallenge(message) {
				t.Fatal("rechallenged retained dialog", method)
			}
		}
	}
	for _, method := range []string{"ACK", "CANCEL", "BYE", "OPTIONS", "NOTIFY", "REFER", "INFO", "UPDATE", "PRACK"} {
		if ingress.requiresLocalChallenge(&pb.SipMessage{Method: method}) {
			t.Fatal("challenged unconfigured call method", method)
		}
	}
}

func TestExpiredPolicyReturnsLocal503AndMethodDenialWins(t *testing.T) {
	for _, deny := range []bool{false, true} {
		f := newUDPOrderFixture(t, func(i *Ingress) {
			i.authPolicy = &pb.InboundAuthPolicy{RequireRegister: true}
			i.methodPolicy.DisableRegister = deny
		})
		message := untaggedChallengeMessage(t, "REGISTER")
		_, _ = f.remote.WriteTo(message.Wire, f.listener.LocalAddr())
		_ = f.remote.SetReadDeadline(time.Now().Add(time.Second))
		buffer := make([]byte, 4096)
		n, _, err := f.remote.ReadFrom(buffer)
		if err != nil {
			t.Fatal(err)
		}
		parsed, err := sip.ParseMessage(buffer[:n])
		if err != nil {
			t.Fatal(err)
		}
		want := 503
		if deny {
			want = 405
		}
		if parsed.(*sip.Response).StatusCode != want {
			t.Fatal("unexpected local refusal precedence", parsed.String())
		}
	}
}

func TestChallengeACKProofDoesNotAuthorizeAnotherFlowOrTransaction(t *testing.T) {
	ingress := NewIngress(&pb.ProcessRef{}, new(control.Server), "", 8, IngressOptions{AuthPolicy: &pb.InboundAuthPolicy{RequireInvite: true}})
	policy := testChallengePolicy()
	if err := ingress.UpdateChallengePolicy(policy); err != nil {
		t.Fatal(err)
	}
	message := untaggedChallengeMessage(t, "INVITE")
	parsed, _ := sip.ParseMessage(message.Wire)
	request := parsed.(*sip.Request)
	flow := &pb.FlowRef{Ingress: &pb.ProcessRef{InstanceId: "sip", Epoch: "epoch"}, ConnectionId: "flow", SourceIp: "192.0.2.1", SourcePort: 5060, Transport: "tls"}
	request.To().Params.Add("tag", localChallengeTag(policy.SigningKey, request, flow))
	request.Method, request.CSeq().MethodName = sip.ACK, sip.ACK
	ack, err := Observe([]byte(request.String()), false)
	if err != nil || !ingress.localChallengeACK(ack, flow, nil) {
		t.Fatal("valid local challenge ACK was not consumed", err)
	}
	changed := proto.Clone(flow).(*pb.FlowRef)
	changed.ConnectionId = "other"
	if ingress.localChallengeACK(ack, changed, nil) {
		t.Fatal("challenge proof moved to a different physical flow")
	}
	request.CSeq().SeqNo++
	altered, _ := Observe([]byte(request.String()), false)
	if ingress.localChallengeACK(altered, flow, nil) {
		t.Fatal("challenge proof moved to a different transaction")
	}
	policy.SigningKey = bytes.Repeat([]byte{8}, 32)
	if err := ingress.UpdateChallengePolicy(policy); err != nil || !ingress.localChallengeACK(ack, flow, nil) {
		t.Fatal("bounded retired key could not finish its challenge ACK", err)
	}
	for count := 0; count < 12; count++ {
		policy.SigningKey = bytes.Repeat([]byte{byte(7 + count%2)}, 32)
		if err := ingress.UpdateChallengePolicy(policy); err != nil {
			t.Fatal("two rolling controllers exhausted key overlap", err)
		}
	}
	if len(ingress.challenges.retired) > 1 {
		t.Fatal("rolling key overlap retained duplicate keys")
	}
}
