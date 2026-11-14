package engine

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
)

func residentSubscriptionFixture(t *testing.T) (*ResidentMemory, *residentContext, *residentTransaction) {
	t.Helper()
	app := &pb.ProcessRef{InstanceId: "controller", Epoch: "epoch"}
	process := &pb.ProcessRef{InstanceId: "sipbridge", Epoch: "epoch"}
	flow := &pb.FlowRef{Ingress: process, ConnectionId: "phone", SourceIp: "192.0.2.12", SourcePort: 50300, Listener: "sip.example.test:5061", Transport: "tls"}
	e := &ResidentMemory{cfg: Config{Process: process, AdvertiseAddress: "sip.example.test:5061", ContactURI: "sips:sip.example.test:5061", StateIdentityKey: bytes.Repeat([]byte{1}, 32)},
		authority: NewResidentMemoryAuthority("test", "1")}
	message := stateTestRequest("SUBSCRIBE", 1, "", "subscribe")
	wire := strings.Replace(string(message.Wire), "Content-Length:", "Event: message-summary\r\nExpires: 90\r\nContent-Length:", 1)
	native, err := portable.NewTransaction(portable.NonInviteServer, portable.RestoreMessage([]byte(wire), "tls", "", ""), portable.DefaultTimers(), "local")
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := sip.ParseMessage(native.Origin.WireBytes())
	if err != nil {
		t.Fatal(err)
	}
	request := parsed.(*sip.Request)
	snapshot := &pb.ContextSnapshot{Context: e.contextRef(request.CallID().Value(), "sub"), Application: app, LocalTag: "local", RemoteTag: tag(request.From().Params),
		CallId: request.CallID().Value(), LocalUri: request.To().Address.String(), RemoteUri: request.From().Address.String(), Transport: "tls"}
	return e, &residentContext{snapshot: snapshot, flow: flow}, &residentTransaction{native: native, flow: flow, application: app, requestID: "request"}
}

func TestResidentSubscriptionCandidateRejectIsAtomic(t *testing.T) {
	e, c, tx := residentSubscriptionFixture(t)
	before := proto.Clone(c.snapshot)
	response := &pb.Respond{Status: 202, Headers: []*pb.Header{{Name: "Expires", Value: "90"}}}
	sub, snapshot, message, err := e.prepareResidentSubscription(c, tx, response, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if c.subscription != nil || !proto.Equal(c.snapshot, before) {
		t.Fatal("candidate mutated live subscription")
	}
	if sub.Event != "message-summary" || snapshot.Kind != pb.ContextSnapshot_SUBSCRIPTION || !bytes.Contains(message.WireBytes(), []byte("202 Accepted")) {
		t.Fatal("missing candidate subscription/response")
	}
	response.Headers[0].Value = "-1"
	if _, _, _, err = e.prepareResidentSubscription(c, tx, response, time.Now()); err == nil {
		t.Fatal("accepted invalid expiry")
	}
	if c.subscription != nil || !proto.Equal(c.snapshot, before) {
		t.Fatal("refused candidate mutated live context")
	}
}

func TestResidentNotifyCandidateHasIndependentSequenceAndFinalFence(t *testing.T) {
	e, c, tx := residentSubscriptionFixture(t)
	now := time.Now()
	sub, snapshot, _, err := e.prepareResidentSubscription(c, tx, &pb.Respond{Status: 202, Headers: []*pb.Header{{Name: "Expires", Value: "90"}}}, now)
	if err != nil {
		t.Fatal(err)
	}
	c.subscription, c.snapshot = sub, snapshot
	command := &pb.DialogRequest{Method: "NOTIFY", Headers: []*pb.Header{{Name: "Subscription-State", Value: "active"}}}
	next, notify, effects, err := e.prepareResidentNotify(c, command, "one", now)
	if err != nil {
		t.Fatal(err)
	}
	if sub.LocalSequence != 0 || next.LocalSequence != 1 || next.RemoteSequence != sub.RemoteSequence || len(effects) == 0 || notify.native.Family != portable.NonInviteClient {
		t.Fatal("NOTIFY candidate changed live sequence or lacks native transaction")
	}
	c.subscription.Terminating = true
	if _, _, _, err = e.prepareResidentNotify(c, command, "two", now); err != errSubscriptionTerminating {
		t.Fatalf("active NOTIFY passed terminal fence: %v", err)
	}
	command.Headers[0].Value = "terminated;reason=timeout"
	next, _, _, err = e.prepareResidentNotify(c, command, "final", now)
	if err != nil || !next.Final || !next.Terminating || sub.Final {
		t.Fatal("final NOTIFY candidate failed or mutated original", err)
	}
}
