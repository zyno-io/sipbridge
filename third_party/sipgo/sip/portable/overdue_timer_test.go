package portable

import (
	"testing"

	"github.com/emiago/sipgo/sip"
)

func TestPortableOverdueTerminalDeadlinePrecedesRetransmission(t *testing.T) {
	for _, family := range []Family{InviteClient, NonInviteClient, InviteServer} {
		t.Run(string(family), func(t *testing.T) {
			method := sip.INVITE
			if family == NonInviteClient {
				method = sip.OPTIONS
			}
			request := origin(t, method, "udp")
			s, _ := step(t, tx(t, family, request), Input{Kind: Start, Now: 1000})
			retransmit, terminal := TimerA, TimerB
			if family == NonInviteClient {
				retransmit, terminal = TimerE, TimerF
			}
			if family == InviteServer {
				s, _ = step(t, s, Input{Kind: SendResponse, Now: 1100, Message: reply(t, request, 486, "remote-tag")})
				retransmit, terminal = TimerG, TimerH
			}
			deadline := s.Timers[terminal]
			if !deadline.Active || !s.Timers[retransmit].Active {
				t.Fatal("fixture timers missing")
			}
			next, effects := step(t, s, Input{Kind: TimerFired, Timer: retransmit, Generation: s.Timers[retransmit].Generation, Now: deadline.At + 1000})
			if next.Phase != Terminated || count(effects, Send) != 0 || count(effects, Ended) != 1 || next.Timers[terminal].At != deadline.At {
				t.Fatalf("overdue retransmission escaped terminal deadline: phase=%s effects=%+v", next.Phase, effects)
			}
			_, effects = step(t, next, Input{Kind: TimerFired, Timer: terminal, Generation: deadline.Generation, Now: deadline.At + 1001})
			if count(effects, Ended) != 0 {
				t.Fatal("later terminal notification repeated termination")
			}
		})
	}
}
