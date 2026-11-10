package portable

import (
	"testing"

	"github.com/emiago/sipgo/sip"
)

func TestPortableCancelledInviteHasFixedFinalResponseDeadline(t *testing.T) {
	for _, transport := range []string{"udp", "tcp", "tls"} {
		t.Run(transport, func(t *testing.T) {
			request := origin(t, sip.INVITE, transport)
			s, _ := step(t, tx(t, InviteClient, request), Input{Kind: Start, Now: 1000})
			s, effects := step(t, s, Input{Kind: RequestCancel, Now: 1100})
			if count(effects, StartTransaction) != 0 || s.Timers[TimerCancel].Active {
				t.Fatal("CANCEL started before a provisional response")
			}
			s, effects = step(t, s, Input{Kind: ReceiveResponse, Now: 1200, Message: reply(t, request, 180, "remote-tag")})
			deadline := s.Timers[TimerCancel]
			if count(effects, StartTransaction) != 1 || !deadline.Active || deadline.At != 1200+64*s.Profile.T1 {
				t.Fatal("original INVITE cancellation deadline missing")
			}
			s, _ = step(t, s, Input{Kind: RequestCancel, Now: 1300})
			s, _ = step(t, s, Input{Kind: ReceiveResponse, Now: 1400, Message: reply(t, request, 183, "remote-tag")})
			if s.Timers[TimerCancel] != deadline {
				t.Fatal("duplicate cancellation or provisional response extended the deadline")
			}
			before, effects := step(t, s, Input{Kind: TimerFired, Timer: TimerCancel, Generation: deadline.Generation, Now: deadline.At - 1})
			if before.Phase != Proceeding || count(effects, Ended) != 0 {
				t.Fatal("cancellation ended before its original deadline")
			}
			ended, effects := step(t, s, Input{Kind: TimerFired, Timer: TimerCancel, Generation: deadline.Generation, Now: deadline.At})
			if ended.Phase != Terminated || ended.Timers[TimerCancel].Active || count(effects, Ended) != 1 {
				t.Fatal("original INVITE did not terminate after cancellation timeout")
			}
			_, effects = step(t, ended, Input{Kind: TimerFired, Timer: TimerCancel, Generation: deadline.Generation, Now: deadline.At + 1})
			if count(effects, Ended) != 0 {
				t.Fatal("cancellation timeout terminated twice")
			}
			for _, status := range []int{200, 487} {
				final, _ := step(t, s, Input{Kind: ReceiveResponse, Now: 1500, Message: reply(t, request, status, "remote-tag")})
				if final.Timers[TimerCancel].Active {
					t.Fatal("INVITE final retained the cancellation watchdog")
				}
				late, effects := step(t, final, Input{Kind: TimerFired, Timer: TimerCancel, Generation: deadline.Generation, Now: deadline.At})
				if late.Phase != final.Phase || count(effects, Ended) != 0 {
					t.Fatal("stale cancellation timer terminated a completed INVITE")
				}
			}
		})
	}
}
