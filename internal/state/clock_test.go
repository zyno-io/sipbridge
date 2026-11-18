package state

import (
	"errors"
	"testing"
	"time"
)

func TestClockGuardRecoversAfterSkew(t *testing.T) {
	start := time.Now()
	if err := verifyClock(start, start.Add(20*time.Millisecond), start.Add(time.Second).UnixMilli()); !errors.Is(err, ErrClock) {
		t.Fatal("skew accepted", err)
	}
	if err := verifyClock(start, start.Add(20*time.Millisecond), start.UnixMilli()); err != nil {
		t.Fatal("healthy observation did not recover", err)
	}
}

func TestClockGuardAllowsExecutionWithinSlowStartupInterval(t *testing.T) {
	start := time.Now()
	for _, execution := range []time.Duration{0, 500 * time.Millisecond, time.Second} {
		if err := verifyClock(start, start.Add(time.Second), start.Add(execution).UnixMilli()); err != nil {
			t.Fatalf("healthy slow startup was treated as clock skew: %v", err)
		}
	}
}

func TestClockGuardRejectsProvenStartupSkew(t *testing.T) {
	start := time.Now()
	for _, sample := range []struct{ delay, offset time.Duration }{{20 * time.Millisecond, time.Second}, {20 * time.Millisecond, -time.Second}} {
		if err := verifyClock(start, start.Add(sample.delay), start.Add(sample.offset).UnixMilli()); !errors.Is(err, ErrClock) {
			t.Fatal("unproven startup clock accepted")
		}
	}
}
