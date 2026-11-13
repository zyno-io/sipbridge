package engine

import (
	"github.com/emiago/sipgo/sip/portable"
	"testing"

	"github.com/emiago/sipgo/sip"
	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
)

func TestResidentUncertainResponseSuppressesNativeReplay(t *testing.T) {
	tx := &residentTransaction{native: portable.Transaction{Key: "server"}}
	c := &residentContext{outputs: map[string]*residentOutput{"attempt": {transaction: "server", state: residentOutputUnknown}}}
	effects := []portable.Effect{{Kind: portable.Send, Replay: true}, {Kind: portable.Schedule}}
	filtered := residentSuppressUncertainReplayLocked(c, tx, effects)
	if len(filtered) != 1 || filtered[0].Kind != portable.Schedule {
		t.Fatal("uncertain attempt allowed another physical replay")
	}
	c.outputs["attempt"].state = residentOutputWritten
	if got := residentSuppressUncertainReplayLocked(c, tx, effects); len(got) != 2 {
		t.Fatal("known completed attempt suppressed independent native replay")
	}
}

func TestResidentReplacementReplayRequiresTLSAndAdmission(t *testing.T) {
	_, c, _ := residentSubscriptionFixture(t)
	original := c.flow
	changed := proto.Clone(original).(*pb.FlowRef)
	changed.ConnectionId = "replacement"
	tx := &residentTransaction{flow: original}
	inviteMessage := stateTestRequest("INVITE", 1, "", "replay")
	parsed, err := sip.ParseMessage(inviteMessage.Wire)
	if err != nil {
		t.Fatal(err)
	}
	request := parsed.(*sip.Request)
	in := &pb.OwnerIngest{Flow: changed}
	if !residentReplayFlowAllowed(c, tx, in, request) {
		t.Fatal("exact initial INVITE TLS continuity refused")
	}
	changed.Transport = "tcp"
	if residentReplayFlowAllowed(c, tx, in, request) {
		t.Fatal("plaintext replacement admitted without binding")
	}
	changed.Transport = "tls"
	registration := stateTestRequest("REGISTER", 1, "", "replay-register")
	parsed, err = sip.ParseMessage(registration.Wire)
	if err != nil {
		t.Fatal(err)
	}
	if residentReplayFlowAllowed(c, tx, in, parsed.(*sip.Request)) {
		t.Fatal("REGISTER replay accepted replacement flow")
	}
}
