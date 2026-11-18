package engine

import (
	"testing"

	"github.com/emiago/sipgo/sip/portable"
	pb "sipbridge/internal/controlpb"
)

func TestNativeInitialInviteResponseCandidates(t *testing.T) {
	for _, status := range []uint32{180, 200, 486} {
		t.Run(reason(status), func(t *testing.T) {
			origin, err := portable.ParseMessage(stateTestRequest("INVITE", 1, "", "candidate").Wire, "tls", "192.0.2.12:5061", "")
			if err != nil {
				t.Fatal(err)
			}
			initial, _, err := nativeServerStart(origin, "local", 1000)
			if err != nil {
				t.Fatal(err)
			}
			candidate, err := nativePrepareInitialInviteResponse(initial, &pb.Respond{Status: status}, "local", "sip:edge@example.test", "edge.example.test:5061", 1001)
			if err != nil {
				t.Fatal(err)
			}
			if initial.LastResponse != nil || candidate.Transaction.LastResponse == nil || !candidate.Transaction.LastResponse.EqualWire(candidate.Message) || !portable.MatchResponse(origin, candidate.Message) {
				t.Fatal("candidate mutated its input or lost the selected response")
			}
			sends := 0
			for _, effect := range candidate.Effects {
				if effect.Kind == portable.Send {
					if effect.Message == nil || !effect.Message.EqualWire(candidate.Message) {
						t.Fatal("transaction staged a different response")
					}
					sends++
				}
			}
			if sends != 1 {
				t.Fatalf("expected one transaction response, got %d", sends)
			}
			switch status {
			case 180:
				if candidate.Dialog != nil || candidate.Transaction.Phase != portable.Proceeding {
					t.Fatal("provisional changed dialog or transaction phase")
				}
			case 200:
				if candidate.Dialog == nil || candidate.Dialog.RemoteInvite == nil || candidate.Transaction.Phase != portable.Accepted || !candidate.Transaction.Timers[portable.TimerL].Active || !candidate.Dialog.Timers[portable.TimerACK].Active || !candidate.Dialog.Timers[portable.Timer2xx].Active {
					t.Fatal("2xx lost native transaction or dialog duties")
				}
			case 486:
				if candidate.Dialog != nil || candidate.Transaction.Phase != portable.Completed || !candidate.Transaction.Timers[portable.TimerH].Active {
					t.Fatal("negative final lost native transaction duty")
				}
			}
		})
	}
}
