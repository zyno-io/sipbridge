package engine

import (
	"bytes"
	"strings"
	"testing"

	"github.com/emiago/sipgo/sip"
	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
)

func TestResidentFramedAdmissionDoesNotRetainParsedRequest(t *testing.T) {
	e, peer, conn, ctx, session, writes, _ := residentControlFixture(t, true)
	message := stateTestRequest("SUBSCRIBE", 1, "", "framed-native-subscription")
	message.Wire = []byte(strings.Replace(string(message.Wire), "Content-Length:", "Event: message-summary\r\nExpires: 90\r\nContent-Length:", 1))
	stream := sip.NewParser().NewSIPStream()
	defer stream.Close()
	if _, err := stream.Write(message.Wire); err != nil {
		t.Fatal(err)
	}
	parsed, size, err := stream.ParseNext()
	if err != nil || size != len(message.Wire) {
		t.Fatal("first stream message did not frame", size, err)
	}
	owned, err := observeFramedMessage(message.Wire, parsed)
	if err != nil {
		t.Fatal(err)
	}
	flow := residentDirectFlow(e)
	if handled, err := e.TryNativeCompletionParsed(ctx, flow, owned, parsed); err != nil || handled {
		t.Fatal("fresh application request entered native completion", handled, err)
	}
	if err := e.ReceiveRequestParsed(ctx, peer, flow, owned, parsed); err != nil {
		t.Fatal(err)
	}
	request := readControllerEnvelope(t, ctx, conn).GetRequest()
	if request == nil || !proto.Equal(request.Message, owned) {
		t.Fatal("framed admission changed the canonical request projection", request)
	}
	// The next framing operation and caller-owned parser objects cannot alter
	// the admitted wire, identity, or the later decision's NOTIFY target.
	next := stateTestRequest("REGISTER", 2, "", "framed-next-request")
	if _, err := stream.Write(next.Wire); err != nil {
		t.Fatal(err)
	}
	if _, _, err := stream.ParseNext(); err != nil {
		t.Fatal(err)
	}
	first := parsed.(*sip.Request)
	first.Method = sip.BYE
	first.From().Address.Host = "mutated.invalid"
	first.Contact().Address.Host = "mutated.invalid"
	first.SetBody([]byte("mutated body"))
	result := residentSendDecision(t, ctx, conn, session, "framed-subscription", residentDirectDecision(request, 1, 202))
	if result.Certainty != pb.OperationResult_ACCEPTED || result.GetSnapshot().GetRemoteUri() != "sip:alice@example.test" {
		t.Fatal("later decision retained the framing parser's mutable request", result)
	}
	<-writes
	if notify := <-writes; !bytes.HasPrefix(notify, []byte("NOTIFY sip:alice@phone.example.test:5061;transport=tls ")) || bytes.Contains(notify, []byte("mutated")) {
		t.Fatal("parser reuse changed retained subscription identity or target", string(notify))
	}
}

func TestResidentUnframedAdmissionDerivesProjectionFromWire(t *testing.T) {
	e, peer, conn, ctx, _, _, _ := residentControlFixture(t, true)
	message := stateTestRequest("REGISTER", 1, "", "unframed-native-registration")
	want := proto.Clone(message).(*pb.SipMessage)
	message.Method, message.Uri = "BYE", "sip:untrusted.invalid"
	message.Selectors = []string{"untrusted-selector"}
	message.Headers, message.Addresses, message.Vias = nil, nil, nil
	if err := e.ReceiveRequest(ctx, peer, residentDirectFlow(e), message); err != nil {
		t.Fatal(err)
	}
	request := readControllerEnvelope(t, ctx, conn).GetRequest()
	if request == nil || !proto.Equal(request.Message, want) {
		t.Fatal("standalone wrapper trusted supplied metadata over actual SIP wire", request)
	}
}
