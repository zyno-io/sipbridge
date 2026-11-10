package execution

import (
	"context"
	"time"
)

// Timing is per-control-request diagnostic metadata, used by one execution
// goroutine. It retains no input, protocol value, identity or replay authority.
type Timing struct {
	GroupWait, Slot, Read, Reduce, Commit time.Duration
	Reads, Reductions, Commits            int
	Stale, Busy, Unknown                  int
	Preflight                             time.Duration
}

type timingKey struct{}

func WithTiming(ctx context.Context) (context.Context, *Timing) {
	value := &Timing{}
	return context.WithValue(ctx, timingKey{}, value), value
}

func RequestTiming(ctx context.Context) *Timing {
	value, _ := ctx.Value(timingKey{}).(*Timing)
	return value
}
