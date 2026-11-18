package engine

import (
	"testing"

	"github.com/emiago/sipgo/sip/portable"
	pb "sipbridge/internal/controlpb"
)

func TestNativeOwnerByePreservesUASAndUACACKRules(t *testing.T) {
	origin, err := portable.ParseMessage(stateTestRequest("INVITE", 1, "", "owner-bye").Wire, "tls", "192.0.2.12:5061", "")
	if err != nil {
		t.Fatal(err)
	}
	transaction, _, err := nativeServerStart(origin, "local", 1000)
	if err != nil {
		t.Fatal(err)
	}
	response, err := nativePrepareInitialInviteResponse(transaction, &pb.Respond{Status: 200}, "local", "sip:edge@example.test", "edge.example.test:5061", 1001)
	if err != nil || response.Dialog == nil {
		t.Fatal("UAS success dialog missing", err)
	}
	ackDeadline := response.Dialog.Timers[portable.TimerACK]
	timedOut, effects, err := portable.StepDialog(*response.Dialog, portable.DialogInput{Kind: portable.DialogTimer, Timer: portable.TimerACK, Generation: ackDeadline.Generation, Now: ackDeadline.At})
	if err != nil || timedOut.Phase != portable.DialogTerminating {
		t.Fatal("UAS ACK timeout did not enter cleanup", err)
	}
	needBye := false
	for _, effect := range effects {
		needBye = needBye || effect.Kind == portable.NeedBYE
	}
	if !needBye {
		t.Fatal("UAS ACK timeout lost its BYE duty")
	}
	candidate, eligible, err := nativePrepareOwnerBye(timedOut, false, true, "z9hG4bK-timeout-bye", ackDeadline.At)
	if err != nil || !eligible || timedOut.LocalBye != nil || candidate.Dialog.LocalBye == nil || candidate.Transaction.Family != portable.NonInviteClient || candidate.Transaction.Phase != portable.Trying || !candidate.Transaction.Timers[portable.TimerF].Active {
		t.Fatal("UAS timeout cleanup candidate lost its native duties", err)
	}
	uac := nativeEstablishedDialogFixture(t)
	if _, eligible, err = nativePrepareOwnerBye(uac, false, false, "z9hG4bK-uac-bye", 2000); err != nil || eligible {
		t.Fatal("UAC cleanup bypassed its ACK proof", err)
	}
	if _, eligible, err = nativePrepareOwnerBye(uac, true, false, "z9hG4bK-uac-bye", 2000); err != nil || !eligible {
		t.Fatal("UAC cleanup rejected a positively written ACK", err)
	}
}
