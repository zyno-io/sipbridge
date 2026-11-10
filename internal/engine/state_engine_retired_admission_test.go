package engine

import (
	"errors"
	"reflect"
	"testing"

	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
	pb "sipbridge/internal/controlpb"
)

func retiredAdmissionFixture(t *testing.T, state string) (*stateReduction, *stateContext, portable.Dialog, portable.Message) {
	t.Helper()
	r, c, tx := forkFixture(t)
	response := forkReply(t, tx, 200, "remote-tag")
	dialog, err := portable.NewDialog(portable.UAC, tx.Native.Origin, response, portable.DefaultTimers())
	if err != nil {
		t.Fatal(err)
	}
	dialog, _, err = portable.StepDialog(dialog, portable.DialogInput{Kind: portable.DialogLocalACK, Now: r.now, Branch: "z9hG4bK-retired-local-ack"})
	if err != nil {
		t.Fatal(err)
	}
	if state == "ended" {
		dialog, _, err = portable.StepDialog(dialog, portable.DialogInput{Kind: portable.DialogTerminate, Now: r.now + 1})
	} else if state == "retiring" {
		dialog, _, err = portable.StepDialog(dialog, portable.DialogInput{Kind: portable.DialogLocalRequest, Now: r.now + 1, Method: sip.BYE, Branch: "z9hG4bK-retired-local-bye"})
	}
	if err != nil {
		t.Fatal(err)
	}
	c.Dialog = dialog.LocalTag + "/" + dialog.RemoteTag
	c.Dialogs = map[string]portable.Dialog{c.Dialog: dialog}
	r.group.Contexts = map[string]*stateContext{c.Snapshot.Context.Id: c}
	request := retiredAdmissionPeerRequest(t, dialog, sip.INVITE, dialog.RemoteCSeq+1, "z9hG4bK-retired-reinvite")
	return r, c, dialog, request
}

func retiredAdmissionPeerRequest(t *testing.T, dialog portable.Dialog, method sip.RequestMethod, sequence uint32, branch string) portable.Message {
	t.Helper()
	reverse := portable.Dialog{
		CallID: dialog.CallID, LocalTag: dialog.RemoteTag, RemoteTag: dialog.LocalTag,
		LocalURI: dialog.RemoteURI, RemoteURI: dialog.LocalURI,
		Target: dialog.Contact, Contact: dialog.Target, SentBy: "192.0.2.2:5061", Transport: dialog.Transport,
	}
	message, err := portable.BuildDialogRequest(reverse, method, sequence, []byte("v=0\r\n"), branch)
	if err != nil {
		t.Fatal(err)
	}
	return message
}

func TestStateRetiredDialogRejectsFreshReinviteBeforeAdmission(t *testing.T) {
	for _, ended := range []bool{false, true} {
		name := "retiring"
		if ended {
			name = "ended"
		}
		t.Run(name, func(t *testing.T) {
			dialogState := "retiring"
			if ended {
				dialogState = "ended"
			}
			r, c, beforeDialog, request := retiredAdmissionFixture(t, dialogState)
			observed, err := Observe(request.Wire, false)
			if err != nil {
				t.Fatal(err)
			}
			r.group.Allocations = map[string]stateAdmission{"pending-allocation": {WireHash: "preserve", ExpiresMS: r.now + 1000}}
			r.in.Message = observed
			r.result = &pb.OperationResult{Certainty: pb.OperationResult_COMPLETED}
			for _, selector := range observed.Selectors {
				r.group.Selectors[selector] = c.Snapshot.Context.Id
			}
			revision, nextAllocation := c.Snapshot.Revision, r.group.NextAllocation
			err = r.reserveAdmission()
			var refusal *portable.ProtocolError
			if !errors.As(err, &refusal) || refusal.Status != 481 {
				t.Fatalf("retired tagged re-INVITE did not receive an early native 481: %v", err)
			}
			if r.result.Snapshot != nil || len(r.group.Allocations) != 1 || r.group.Allocations["pending-allocation"].WireHash != "preserve" || r.group.NextAllocation != nextAllocation {
				t.Fatal("retired re-INVITE allocated or changed an admission reservation")
			}
			if c.Snapshot.Revision != revision || !reflect.DeepEqual(c.Dialogs[c.Dialog], beforeDialog) {
				t.Fatal("read-only refusal changed the snapshot or retained dialog")
			}
		})
	}
}

func TestStateRetiredDialogShortcutRequiresExactRetiringDirectionalInvite(t *testing.T) {
	t.Run("live dialog", func(t *testing.T) {
		r, c, _, request := retiredAdmissionFixture(t, "live")
		if err := stateRetiredInvite(c, retiredAdmissionObserve(t, request), r.now); err != nil {
			t.Fatalf("live dialog received retired-dialog refusal: %v", err)
		}
	})

	t.Run("snapshot ended alone", func(t *testing.T) {
		_, c, _, request := retiredAdmissionFixture(t, "live")
		c.Snapshot.Ended = true
		if err := stateRetiredInvite(c, retiredAdmissionObserve(t, request), 1000); err != nil {
			t.Fatalf("application snapshot alone caused retired-dialog refusal: %v", err)
		}
	})

	t.Run("wrong directional tags", func(t *testing.T) {
		r, c, dialog, _ := retiredAdmissionFixture(t, "retiring")
		wrongTo := dialog
		wrongTo.LocalTag = "different-local-tag"
		wrongFrom := dialog
		wrongFrom.RemoteTag = "different-remote-tag"
		for _, candidate := range []portable.Message{
			retiredAdmissionPeerRequest(t, wrongTo, sip.INVITE, 1, "z9hG4bK-wrong-to"),
			retiredAdmissionPeerRequest(t, wrongFrom, sip.INVITE, 1, "z9hG4bK-wrong-from"),
		} {
			if err := stateRetiredInvite(c, retiredAdmissionObserve(t, candidate), r.now); err != nil {
				t.Fatalf("wrong directional tags gained retirement refusal: %v", err)
			}
		}
	})

	t.Run("non re-INVITE methods and responses", func(t *testing.T) {
		r, c, dialog, invite := retiredAdmissionFixture(t, "retiring")
		for _, method := range []sip.RequestMethod{sip.ACK, sip.CANCEL, sip.BYE, sip.INFO} {
			request := retiredAdmissionPeerRequest(t, dialog, method, 40, "z9hG4bK-retired-"+string(method))
			if err := stateRetiredInvite(c, retiredAdmissionObserve(t, request), r.now); err != nil {
				t.Errorf("%s was intercepted by the retired INVITE shortcut: %v", method, err)
			}
		}
		response, err := portable.BuildResponseWithHeaders(invite, 200, "OK", nil, dialog.LocalTag, "", nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := stateRetiredInvite(c, retiredAdmissionObserve(t, response), r.now); err != nil {
			t.Fatalf("response was intercepted by the retired INVITE shortcut: %v", err)
		}
	})
}

func TestStateRetiredDialogDoesNotPreemptOccupiedServerInviteTransactionKey(t *testing.T) {
	r, c, dialog, original := retiredAdmissionFixture(t, "retiring")
	origin := portable.Message{Wire: append([]byte(nil), original.Wire...), Transport: original.Transport}
	native, err := portable.NewTransaction(portable.InviteServer, origin, portable.DefaultTimers(), dialog.LocalTag)
	if err != nil {
		t.Fatal(err)
	}
	native, _, err = portable.StepTransaction(native, portable.Input{Kind: portable.Start, Now: r.now})
	if err != nil {
		t.Fatal(err)
	}
	c.Transactions[native.Key] = &stateTransaction{Native: native}
	for _, variant := range []struct {
		name   string
		mutate func(*sip.Request)
	}{
		{name: "body", mutate: func(request *sip.Request) { request.SetBody([]byte("changed body")) }},
		{name: "cseq", mutate: func(request *sip.Request) { request.CSeq().SeqNo++ }},
		{name: "uri", mutate: func(request *sip.Request) { request.Recipient.User = "changed-uri" }},
	} {
		t.Run(variant.name, func(t *testing.T) {
			parsed, err := sip.ParseMessage(original.Wire)
			if err != nil {
				t.Fatal(err)
			}
			request := parsed.(*sip.Request)
			variant.mutate(request)
			message := portable.Message{Wire: []byte(request.String()), Transport: original.Transport}
			key, err := sip.ServerTxKeyMake(request)
			if err != nil || key != native.Key {
				t.Fatalf("altered request did not retain the occupied native transaction key: key=%q want=%q err=%v", key, native.Key, err)
			}
			if variant.name == "body" && !portable.MatchRequest(origin, message) {
				t.Fatal("body-only retransmission stopped matching its retained native request")
			}
			observed := retiredAdmissionObserve(t, message)
			if err := stateRetiredInvite(c, observed, r.now); err != nil {
				t.Fatalf("occupied transaction was bypassed by retired-dialog shortcut: %v", err)
			}
			for _, selector := range observed.Selectors {
				r.group.Selectors[selector] = c.Snapshot.Context.Id
			}
			r.in.Message = observed
			r.result = &pb.OperationResult{Certainty: pb.OperationResult_COMPLETED}
			if err := r.reserveAdmission(); err != nil || r.result.Snapshot.GetContext().GetId() != c.Snapshot.Context.Id {
				t.Fatalf("occupied transaction did not defer to normal admission validation: snapshot=%v err=%v", r.result.Snapshot, err)
			}
			if len(r.group.Allocations) != 0 {
				t.Fatal("retained transaction path created an unrelated initial admission")
			}
		})
	}
}

func retiredAdmissionObserve(t *testing.T, message portable.Message) *pb.SipMessage {
	t.Helper()
	observed, err := Observe(message.Wire, false)
	if err != nil {
		t.Fatal(err)
	}
	return observed
}
