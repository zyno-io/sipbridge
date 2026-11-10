package engine

import (
	"reflect"
	"testing"

	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
	pb "sipbridge/internal/controlpb"
)

func TestStateOrdinaryDialogResponsesCompleteOnlyTheirTransaction(t *testing.T) {
	for _, method := range []sip.RequestMethod{sip.INFO, sip.REFER, sip.NOTIFY, sip.OPTIONS, sip.MESSAGE} {
		t.Run(string(method), func(t *testing.T) {
			r, c, initial := forkFixture(t)
			if _, err := r.forkResponse(c, initial, forkReply(t, initial, 200, "winner")); err != nil {
				t.Fatal(err)
			}
			dialog, effects, err := portable.StepDialog(c.Dialogs[c.Dialog], portable.DialogInput{Kind: portable.DialogLocalRequest, Now: r.now, Method: method, Branch: "z9hG4bK-dialog-command"})
			if err != nil {
				t.Fatal(err)
			}
			c.Dialogs[c.Dialog] = dialog
			var request portable.Message
			for _, effect := range effects {
				if effect.Kind == portable.StartTransaction && effect.Message != nil {
					request = *effect.Message
				}
			}
			native, err := portable.NewTransaction(portable.NonInviteClient, request, portable.DefaultTimers(), "")
			if err != nil {
				t.Fatal(err)
			}
			tx := &stateTransaction{Native: native, Flow: c.Flow, Command: "dialog-command"}
			c.Transactions[native.Key] = tx
			if err := r.stepTransaction(c, tx, portable.Input{Kind: portable.Start, Now: r.now}); err != nil {
				t.Fatal(err)
			}
			response, err := portable.BuildResponseWithHeaders(request, 200, "OK", nil, "winner", "", nil)
			if err != nil {
				t.Fatal(err)
			}
			if err := r.stepTransaction(c, tx, portable.Input{Kind: portable.ReceiveResponse, Now: r.now + 1, Message: response}); err != nil {
				t.Fatal(err)
			}
			if tx.Native.Phase != portable.Terminated || c.Result == nil || c.Result.Certainty != pb.OperationResult_COMPLETED || c.Result.Response.Status != 200 || c.Result.Request.Method != string(method) {
				t.Fatalf("dialog command did not complete: phase=%s result=%+v", tx.Native.Phase, c.Result)
			}
			if !reflect.DeepEqual(c.Dialogs[c.Dialog], dialog) || c.Snapshot.Ended {
				t.Fatal("ordinary response changed the INVITE dialog")
			}
		})
	}
}
