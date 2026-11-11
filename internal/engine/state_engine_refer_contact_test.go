package engine

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/state"
)

func retainedPeerDialogRequest(t *testing.T, dialog portable.Dialog, method sip.RequestMethod, sequence uint32) portable.Message {
	t.Helper()
	peer := dialog
	peer.LocalTag, peer.RemoteTag = dialog.RemoteTag, dialog.LocalTag
	peer.LocalURI, peer.RemoteURI = dialog.RemoteURI, dialog.LocalURI
	peer.Target, peer.Contact = dialog.Contact, dialog.Target
	peer.SentBy, peer.Routes = "phone.example.test:5061", nil
	message, err := portable.BuildDialogRequestWithHeaders(peer, method, sequence, nil, fmt.Sprintf("z9hG4bK-peer-%s-%d", method, sequence),
		[]portable.Header{{Name: "Refer-To", Value: "<sip:transfer@example.test>"}})
	if err != nil {
		t.Fatal(err)
	}
	return message
}

func TestStateReferResponseUsesExactRetainedDialogContact(t *testing.T) {
	r, c, initial := forkFixture(t)
	for _, remote := range []string{"matched", "preferred"} {
		dialog, err := portable.NewDialog(portable.UAC, initial.Native.Origin, forkReply(t, initial, 200, remote), portable.DefaultTimers())
		if err != nil {
			t.Fatal(err)
		}
		dialog.Contact = "sip:" + remote + "@retained.example.test:5061;transport=tls;ob;profile=" + remote
		c.Dialogs[dialog.LocalTag+"/"+dialog.RemoteTag] = dialog
	}
	c.Dialog = "local/preferred"
	matched := c.Dialogs["local/matched"]
	origin := retainedPeerDialogRequest(t, matched, sip.REFER, 1)
	r.engine.cfg.Process = &pb.ProcessRef{InstanceId: "surviving-worker", Epoch: "replacement-epoch"}
	r.engine.cfg.AdvertiseAddress = "replacement.example.test:6091"
	r.engine.cfg.ContactURI = "sip:replacement@replacement.example.test:6091;transport=tls;profile=replacement"
	before := c.Dialogs["local/matched"]
	for _, status := range []int{200, 202, 299} {
		message, err := r.buildResponse(c, origin, status, "Accepted", nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		parsed, err := message.Parse()
		if err != nil {
			t.Fatal(err)
		}
		response := parsed.(*sip.Response)
		if response.Contact() == nil || response.Contact().Address.String() != matched.Contact || !portable.MatchResponse(origin, message) {
			t.Fatal("successful REFER did not retain its exact native dialog Contact and transaction identity")
		}
		if message.Transport != origin.Transport || !reflect.DeepEqual(c.Dialogs["local/matched"], before) {
			t.Fatal("building REFER response changed transport or retained dialog state")
		}
	}
}

func TestStateReferContactOverrideLeavesOtherResponseAdvertisementsUnchanged(t *testing.T) {
	r, c, initial := forkFixture(t)
	dialog, err := portable.NewDialog(portable.UAC, initial.Native.Origin, forkReply(t, initial, 200, "matched"), portable.DefaultTimers())
	if err != nil {
		t.Fatal(err)
	}
	c.Dialog, c.Dialogs[dialog.LocalTag+"/"+dialog.RemoteTag] = dialog.LocalTag+"/"+dialog.RemoteTag, dialog
	r.engine.cfg.AdvertiseAddress = "replacement.example.test:6091"
	r.engine.cfg.ContactURI = "sip:replacement@replacement.example.test:6091;transport=tls"
	want, _, err := r.engine.advertisement("tls")
	if err != nil {
		t.Fatal(err)
	}
	for _, sample := range []struct {
		method sip.RequestMethod
		status int
	}{{sip.REFER, 183}, {sip.REFER, 488}, {sip.INVITE, 200}, {sip.INFO, 200}, {sip.NOTIFY, 200}} {
		origin := retainedPeerDialogRequest(t, dialog, sample.method, 2)
		message, err := r.buildResponse(c, origin, sample.status, "fixture", nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		parsed, err := message.Parse()
		if err != nil {
			t.Fatal(err)
		}
		if contact := parsed.(*sip.Response).Contact(); contact == nil || contact.Address.String() != want {
			t.Fatalf("%s/%d advertisement changed", sample.method, sample.status)
		}
	}
	parsed, err := initial.Native.Origin.Parse()
	if err != nil {
		t.Fatal(err)
	}
	request := parsed.(*sip.Request)
	request.Method, request.CSeq().MethodName = sip.REFER, sip.REFER
	message, err := r.buildResponse(c, portable.Message{Wire: []byte(request.String()), Transport: "tls"}, 202, "Accepted", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err = message.Parse()
	if err != nil {
		t.Fatal(err)
	}
	if contact := parsed.(*sip.Response).Contact(); contact == nil || contact.Address.String() != want {
		t.Fatal("out-of-dialog REFER advertisement changed")
	}
}

func TestStateReferResponseRejectsMissingOrInconsistentRetainedDialog(t *testing.T) {
	for _, condition := range []string{"missing", "call-id", "transport", "contact"} {
		t.Run(condition, func(t *testing.T) {
			r, c, initial := forkFixture(t)
			dialog, err := portable.NewDialog(portable.UAC, initial.Native.Origin, forkReply(t, initial, 200, "matched"), portable.DefaultTimers())
			if err != nil {
				t.Fatal(err)
			}
			origin := retainedPeerDialogRequest(t, dialog, sip.REFER, 1)
			switch condition {
			case "call-id":
				dialog.CallID = "different-call"
			case "transport":
				dialog.Transport = "udp"
			case "contact":
				dialog.Contact = ""
			}
			if condition != "missing" {
				c.Dialogs[dialog.LocalTag+"/"+dialog.RemoteTag] = dialog
			}
			if _, err := r.buildResponse(c, origin, 202, "Accepted", nil, nil); err == nil {
				t.Fatal("successful in-dialog REFER invented a Contact without its exact retained dialog")
			}
		})
	}
}

func TestStateReferContactSurvivesCrossExecutorUACTakeover(t *testing.T) {
	survivor, original := historicalStateTestEngine(t)
	original.cfg.AdvertiseAddress = "original-b.example.test:5061"
	original.cfg.ContactURI = "sip:bridge-b@original-b.example.test:5061;transport=tls;ob;profile=original-b"
	survivor.cfg.AdvertiseAddress = "survivor-a.example.test:6091"
	survivor.cfg.ContactURI = "sip:bridge-a@survivor-a.example.test:6091;transport=tls;profile=survivor-a"
	r, c, tx := forkFixture(t)
	r.engine = original
	contact, sentBy, err := original.advertisement("tls")
	if err != nil {
		t.Fatal(err)
	}
	initial, err := portable.BuildInitialRequest(portable.InitialRequest{Method: sip.INVITE, URI: "sip:phone@example.test", From: "<sip:controller@example.test>",
		CallID: r.record.Ref.CallID, LocalTag: "local", Branch: "z9hG4bK-retained-b", CSeq: 12, Advertise: sentBy, Contact: contact, Transport: "tls", Body: []byte("v=0\r\n")})
	if err != nil {
		t.Fatal(err)
	}
	tx.Native, err = portable.NewTransaction(portable.InviteClient, initial, portable.DefaultTimers(), "")
	if err != nil {
		t.Fatal(err)
	}
	c.Origin, c.Transactions = initial, map[string]*stateTransaction{tx.Native.Key: tx}
	ctx := context.Background()
	ref, err := original.store.GroupForCallID(r.record.Ref.CallID)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UnixMilli()
	begun, err := original.store.(*state.Store).Begin(ctx, ref, "original-b", state.Input{ID: "seed-uac", Kind: "locate", Fingerprint: "seed-uac", Create: true,
		DeadlineMS: now + 2000, ReceiptUntilMS: now + 120000})
	if err != nil {
		t.Fatal(err)
	}
	r.record, r.now = begun.Step.Record, begun.Step.NowMS
	c.Snapshot.Context = original.contextRef(r.record.Ref, c.Snapshot.Context.Id)
	c.Flow.Ingress = proto.Clone(original.cfg.Process).(*pb.ProcessRef)
	write := initialUACACKFixture(t, r, c, tx)
	writeInitialACKFixture(t, r, write)
	payload, err := r.finish()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := original.store.(*state.Store).Commit(ctx, *begun.Step, payload, nil); err != nil {
		t.Fatal(err)
	}
	// Only survivor A executes after the original B aggregate is committed.
	// It has no resident copy of the native dialog and a different advertisement.
	_, group := stateTestLoad(t, survivor, c.Snapshot.Context)
	current := group.Contexts[c.Snapshot.Context.Id]
	dialog := current.Dialogs[current.Dialog]
	if dialog.Role != portable.UAC || dialog.Contact != contact || dialog.Phase != portable.DialogConfirmed {
		t.Fatal("fixture did not retain the confirmed UAC Contact selected by original B")
	}
	request := retainedPeerDialogRequest(t, dialog, sip.REFER, 1)
	result := stateTestApply(t, survivor, current.Snapshot.Context, "refer-ingest-on-a", stateEngineInput{Kind: "ingest", Application: current.Snapshot.Application,
		Ingest: &pb.OwnerIngest{Context: current.Snapshot.Context, Application: current.Snapshot.Application, Flow: current.Flow, Wire: request.Wire}})
	if result.Error != "" {
		t.Fatal(result.Error)
	}
	_, group = stateTestLoad(t, survivor, current.Snapshot.Context)
	current = group.Contexts[current.Snapshot.Context.Id]
	var requestID string
	for id, key := range current.Requests {
		if portable.MatchRequest(current.Transactions[key].Native.Origin, request) {
			requestID = id
		}
	}
	if requestID == "" {
		t.Fatal("survivor did not retain the exact REFER transaction")
	}
	result = stateTestApply(t, survivor, current.Snapshot.Context, "refer-accepted-on-a", stateEngineInput{Kind: "command", Application: current.Snapshot.Application,
		Command: &pb.Command{Context: current.Snapshot.Context, Sequence: 2, Action: &pb.Command_Respond{Respond: &pb.Respond{RequestId: requestID, Status: 202}}}})
	if result.Error != "" {
		t.Fatal(result.Error)
	}
	record, group := stateTestLoad(t, survivor, current.Snapshot.Context)
	current = group.Contexts[current.Snapshot.Context.Id]
	var selected *sip.Response
	for _, effect := range record.Payload.Effects {
		var body stateEngineEffect
		if body.UnmarshalBinary(effect.Payload) != nil || len(body.Wire) == 0 {
			continue
		}
		parsed, err := sip.ParseMessage(body.Wire)
		if err != nil {
			t.Fatal(err)
		}
		if response, ok := parsed.(*sip.Response); ok && response.CSeq().MethodName == sip.REFER && response.StatusCode == 202 {
			selected = response
			if !proto.Equal(body.Flow, c.Flow) || body.Flow.Transport != "tls" || !sameProcess(body.Application, c.Snapshot.Application) ||
				body.Authorization.GetAuthorityClass() != pb.EffectAuthorization_RESPONSE {
				t.Fatal("REFER changed physical flow, transport or application owner")
			}
		}
	}
	if selected == nil || selected.Contact() == nil || selected.Contact().Address.String() != contact || tag(selected.To().Params) != dialog.LocalTag || tag(selected.From().Params) != dialog.RemoteTag {
		t.Fatal("surviving executor rebuilt REFER Contact or changed its directional dialog tags")
	}
	if current.Dialogs[current.Dialog].Contact != contact {
		t.Fatal("accepting REFER changed the retained native Contact")
	}
	result = stateTestApply(t, survivor, current.Snapshot.Context, "refer-notify-on-a", stateEngineInput{Kind: "command", Application: current.Snapshot.Application,
		Command: &pb.Command{Context: current.Snapshot.Context, Sequence: 3, Action: &pb.Command_DialogRequest{DialogRequest: &pb.DialogRequest{Method: "NOTIFY",
			Headers: []*pb.Header{{Name: "Event", Value: "refer;id=1"}, {Name: "Subscription-State", Value: "terminated;reason=noresource"}}}}}})
	if result.Error != "" {
		t.Fatal(result.Error)
	}
	record, _ = stateTestLoad(t, survivor, current.Snapshot.Context)
	for _, effect := range record.Payload.Effects {
		var body stateEngineEffect
		if body.UnmarshalBinary(effect.Payload) != nil || body.WireMethod != "NOTIFY" {
			continue
		}
		parsed, err := sip.ParseMessage(body.Wire)
		if err != nil {
			t.Fatal(err)
		}
		notify := parsed.(*sip.Request)
		if notify.Contact() == nil || notify.Contact().Address.String() != contact || notify.Via().SentBy() != sentBy || !proto.Equal(body.Flow, c.Flow) ||
			body.Authorization == nil || body.Authorization.AuthorityClass != pb.EffectAuthorization_APPLICATION {
			t.Fatal("REFER NOTIFY changed retained Contact, Via or physical routing after takeover")
		}
		return
	}
	t.Fatal("surviving executor did not generate REFER NOTIFY")
}
