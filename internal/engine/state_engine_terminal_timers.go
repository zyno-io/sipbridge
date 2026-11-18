package engine

import (
	"github.com/emiago/sipgo/sip/portable"
	"sipbridge/internal/state"
)

// Only an actual terminal transition can release these generations. They remain
// necessary whenever a live transaction can rearm a timer, and existing retained
// terminal records are not normalized while reading or inspecting them.
func endTransactionTimers(before portable.Phase, next *portable.Transaction) error {
	if before == portable.Terminated || next.Phase != portable.Terminated {
		return nil
	}
	for _, timer := range next.Timers {
		if timer.Active {
			return state.ErrCorrupt
		}
	}
	next.Timers = nil
	return nil
}
