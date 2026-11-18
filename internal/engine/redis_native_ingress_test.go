package engine

import (
	"bytes"
	"net"
	"strconv"
	"testing"

	"github.com/emiago/sipgo/sip/portable"
	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
)

func TestRedisNativeIngressFrozenFrameOwnership(t *testing.T) {
	process := &pb.ProcessRef{InstanceId: "worker", Epoch: "one"}
	flow := &pb.FlowRef{Ingress: process, ConnectionId: "phone", SourceIp: "192.0.2.2", SourcePort: 5061, Listener: "sip.example.test:5061", Transport: "tls"}
	wire := stateTestRequest("INVITE", 1, "", "frozen-native-ingress").Wire
	parsed, err := parseObservedMessage(wire)
	if err != nil {
		t.Fatal(err)
	}
	native, err := portable.FreezeMessage(wire, parsed, flow.Transport, net.JoinHostPort(flow.SourceIp, strconv.Itoa(int(flow.SourcePort))), "")
	if err != nil {
		t.Fatal(err)
	}
	e := &redisNative{cfg: Config{Process: process}}
	// Public projections do not supply protocol facts or selector authority.
	projection := &pb.SipMessage{Wire: bytes.Clone(wire), Method: "BYE", Uri: "sip:forged@example.test", Selectors: []string{"forged"}}
	selected, err := e.redisIngressMessage(flow, projection, parsed, native)
	if err != nil {
		t.Fatal(err)
	}
	facts, err := selected.Facts()
	if err != nil || string(facts.Method) != "INVITE" || facts.CallID == "forged" {
		t.Fatal("projection replaced observed native facts", err)
	}
	for _, changed := range []string{"wire", "source", "transport", "process", "destination"} {
		t.Run(changed, func(t *testing.T) {
			candidate := proto.Clone(flow).(*pb.FlowRef)
			message := proto.Clone(projection).(*pb.SipMessage)
			frozen := native
			switch changed {
			case "wire":
				message.Wire = bytes.Replace(message.Wire, []byte("INVITE sip:"), []byte("OPTIONS sip:"), 1)
			case "source":
				candidate.SourcePort++
			case "transport":
				candidate.Transport = "tcp"
			case "process":
				candidate.Ingress.Epoch = "replacement"
			case "destination":
				frozen = portable.RestoreMessage(wire, flow.Transport, native.Source(), "different")
			}
			if _, receiveErr := e.redisIngressMessage(candidate, message, parsed, frozen); receiveErr == nil {
				t.Fatal("a different physical frame reused frozen admission")
			}
		})
	}
	if _, err = e.redisIngressMessage(flow, projection, parsed, native, native); err == nil {
		t.Fatal("multiple frozen observations were accepted")
	}
}
