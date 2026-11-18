package engine

import (
	"bytes"
	"testing"

	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/state"
)

func forkFixture(t *testing.T) (*stateReduction, *stateContext, *stateTransaction) {
	t.Helper()
	initial, err := portable.BuildInitialRequest(portable.InitialRequest{Method: sip.INVITE, URI: "sip:phone@example.test", From: "<sip:controller@example.test>", CallID: "fork-call", LocalTag: "local", Branch: "z9hG4bK-initial", CSeq: 12, Advertise: "192.0.2.1:5061", Contact: "<sip:controller@192.0.2.1:5061;transport=tls>", Transport: "tls", Body: []byte("v=0\r\n")})
	if err != nil {
		t.Fatal(err)
	}
	native, err := portable.NewTransaction(portable.InviteClient, initial, portable.DefaultTimers(), "")
	if err != nil {
		t.Fatal(err)
	}
	snapshot, _, _ := sendFingerprintFixture()
	snapshot.CallId = "fork-call"
	snapshot.Context.State.CallId = "fork-call"
	snapshot.Kind = pb.ContextSnapshot_UAC
	c := &stateContext{Snapshot: snapshot, Origin: initial, Flow: sendFingerprintFlow(), Tag: "local", Transactions: make(map[string]*stateTransaction), Dialogs: make(map[string]portable.Dialog), Requests: make(map[string]string), Operation: "open", CompletionScope: "initial-scope"}
	tx := &stateTransaction{Native: native, Flow: c.Flow, Command: "open"}
	c.Transactions[native.Key] = tx
	r := &stateReduction{engine: &StateEngine{cfg: Config{Process: snapshot.Context.Owner, AdvertiseAddress: "192.0.2.1:5061", ContactURI: "sip:controller@192.0.2.1:5061;transport=tls", StateIdentityKey: bytes.Repeat([]byte{1}, 32)}}, record: state.Record{Ref: state.Ref{ID: "group", Incarnation: "incarnation", CallID: "fork-call"}}, group: stateGroup{Contexts: map[string]*stateContext{snapshot.Context.Id: c}, Selectors: make(map[string]string)}, now: 1000}
	return r, c, tx
}

func forkReply(t *testing.T, tx *stateTransaction, status int, remote string) portable.Message {
	t.Helper()
	m, err := portable.BuildResponseWithHeaders(tx.Native.Origin, status, "test", []byte("v=0\r\n"), remote, "sip:phone@"+remote+".example.test:5061;transport=tls", nil)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func forkWireEffects(t *testing.T, effects []state.Effect) ([]string, []*pb.EffectAuthorization) {
	t.Helper()
	var methods []string
	var grants []*pb.EffectAuthorization
	for _, effect := range effects {
		var body stateEngineEffect
		if err := unmarshalRetainedFixture(effect.Payload, &body); err != nil {
			t.Fatal(err)
		}
		if len(body.Wire) == 0 {
			continue
		}
		m, err := sip.ParseMessage(body.Wire)
		if err != nil {
			t.Fatal(err)
		}
		methods = append(methods, string(m.CSeq().MethodName))
		grants = append(grants, body.Authorization)
	}
	return methods, grants
}

func TestStateForkWinnerSelectedBySuccessAndLoserACKByeIsIndependent(t *testing.T) {
	r, c, tx := forkFixture(t)
	if _, err := r.forkResponse(c, tx, forkReply(t, tx, 183, "early")); err != nil {
		t.Fatal(err)
	}
	if c.Dialog != "" || c.Snapshot.Answered {
		t.Fatal("provisional selected winner")
	}
	if _, err := r.forkResponse(c, tx, forkReply(t, tx, 200, "winner")); err != nil {
		t.Fatal(err)
	}
	if c.Dialog != "local/winner" || !c.Snapshot.Answered {
		t.Fatal("success did not select winner")
	}
	winner := c.Dialogs[c.Dialog]
	start := len(r.payload.Effects)
	if _, err := r.forkResponse(c, tx, forkReply(t, tx, 200, "early")); err != nil {
		t.Fatal(err)
	}
	methods, grants := forkWireEffects(t, r.payload.Effects[start:])
	if len(methods) != 2 || methods[0] != "ACK" || methods[1] != "BYE" {
		t.Fatalf("cleanup framing: %v", methods)
	}
	if grants[0].AuthorityClass != pb.EffectAuthorization_NATIVE_COMPLETION || grants[1].AuthorityClass != pb.EffectAuthorization_TERMINAL_CLEANUP {
		t.Fatalf("cleanup authority: %v", grants)
	}
	if c.Dialog != "local/winner" || c.Snapshot.Ended || c.Dialogs[c.Dialog].Target != winner.Target {
		t.Fatal("losing branch changed application dialog")
	}
	loser := c.Dialogs["local/early"]
	if loser.LocalBye == nil || loser.LocalInvite.ACK == nil || loser.LocalCSeq != 13 {
		t.Fatal("branch ACK/BYE not retained")
	}
	start = len(r.payload.Effects)
	if _, err := r.forkResponse(c, tx, forkReply(t, tx, 200, "early")); err != nil {
		t.Fatal(err)
	}
	methods, _ = forkWireEffects(t, r.payload.Effects[start:])
	if len(methods) != 0 {
		t.Fatalf("unstarted identical ACK was not coalesced: %v", methods)
	}
	if c.Dialogs["local/early"].LocalInvite.ACK == nil || c.Dialogs["local/early"].LocalBye == nil {
		t.Fatal("coalescing lost the retained ACK/BYE procedure")
	}

}

func TestStateForkLateCancelledSuccessCannotBecomeApplicationDialog(t *testing.T) {
	r, c, tx := forkFixture(t)
	c.Cancelled = true
	if _, err := r.forkResponse(c, tx, forkReply(t, tx, 200, "late")); err != nil {
		t.Fatal(err)
	}
	methods, _ := forkWireEffects(t, r.payload.Effects)
	if c.Dialog != "" || c.Snapshot.Answered || len(methods) != 2 || methods[0] != "ACK" || methods[1] != "BYE" {
		t.Fatalf("cancelled success escaped native cleanup: dialog=%s methods=%v", c.Dialog, methods)
	}
}

func TestStateReinviteCommitsAcceptedTargetAndBothDescriptions(t *testing.T) {
	r, c, initial := forkFixture(t)
	if _, err := r.forkResponse(c, initial, forkReply(t, initial, 200, "winner")); err != nil {
		t.Fatal(err)
	}
	d, effects, err := portable.StepDialog(c.Dialogs[c.Dialog], portable.DialogInput{Kind: portable.DialogLocalRequest, Now: r.now, Method: sip.INVITE, Body: []byte("v=0\r\na=sendonly\r\n"), Offer: true, Branch: "z9hG4bK-reinvite"})
	if err != nil {
		t.Fatal(err)
	}
	c.Dialogs[c.Dialog] = d
	var offer portable.Message
	for _, effect := range effects {
		if effect.Kind == portable.StartTransaction && effect.Message != nil {
			offer = *effect.Message
		}
	}
	native, err := portable.NewTransaction(portable.InviteClient, offer, portable.DefaultTimers(), "")
	if err != nil {
		t.Fatal(err)
	}
	tx := &stateTransaction{Native: native, Flow: c.Flow, Command: "reinvite"}
	c.Transactions[native.Key] = tx
	answer, err := portable.BuildResponseWithHeaders(offer, 200, "OK", []byte("v=0\r\na=recvonly\r\n"), "winner", "sip:phone@changed.example.test:5061;transport=tls", nil)
	if err != nil {
		t.Fatal(err)
	}
	if handled, err := r.forkResponse(c, tx, answer); !handled || err != nil {
		t.Fatal(err)
	}
	if c.Snapshot.RemoteTarget != "sip:phone@changed.example.test:5061;transport=tls" || string(c.Snapshot.LocalSdp) != "v=0\r\na=sendonly\r\n" || string(c.Snapshot.RemoteSdp) != "v=0\r\na=recvonly\r\n" || c.Result == nil || c.Result.Certainty != pb.OperationResult_COMPLETED {
		t.Fatalf("reinvite snapshot drift: %+v", c.Snapshot)
	}
}
