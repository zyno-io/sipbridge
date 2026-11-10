// Package execution drives pure CAS transitions against the selected authoritative state adapter.
package execution

import (
	"context"
	"time"

	"sipbridge/internal/state"
)

// Reducer must not retain or mutate shared snapshots, perform I/O, or dispatch an
// effect. Application-level rejection is a committed outcome, not a Go error.
// A Go error commits no transition. The historical claimed fixture retains its
// pending input for explicit recovery; CAS never makes computations recoverable.
type Reducer interface {
	Reduce(state.Record, state.Input, int64) (state.Payload, []byte, error)
}

type ReducerFunc func(state.Record, state.Input, int64) (state.Payload, []byte, error)

func (f ReducerFunc) Reduce(r state.Record, in state.Input, now int64) (state.Payload, []byte, error) {
	return f(r, in, now)
}

// Driver exposes execution without retaining the legacy claim-recovery API.
type Driver interface {
	Apply(context.Context, state.Ref, state.Input) (state.Receipt, error)
	ApplyCommitted(context.Context, state.Ref, state.Input) (Result, error)
	UsesCAS() bool
}

type OperationStore interface {
	InspectOperation(context.Context, state.Ref, string, string) (state.Inspection, error)
}

func wait(ctx context.Context) error {
	timer := time.NewTimer(5 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
