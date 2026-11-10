package state

import "time"

const clockTolerance = 250 * time.Millisecond

// Clock checks apply only to the current operation and retain no process fence.
func absClock(n int64) int64 {
	if n < 0 {
		return -n
	}
	return n
}

func clockBoundsAt(now time.Time) (int64, int64) {
	return now.UnixMilli() - clockTolerance.Milliseconds(), now.UnixMilli() + clockTolerance.Milliseconds()
}

func verifyClock(start, finish time.Time, redisMS ...int64) error {
	low, _ := clockBoundsAt(start)
	_, high := clockBoundsAt(finish)
	if finish.Sub(start) < 0 || absClock(finish.UnixMilli()-start.UnixMilli()-finish.Sub(start).Milliseconds()) > clockTolerance.Milliseconds() {
		return ErrClock
	}
	for _, observed := range redisMS {
		if observed < low || observed > high {
			return ErrClock
		}
	}
	return nil
}
