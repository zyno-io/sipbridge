// Package fixture preserves the historical claimed driver for compatibility tests.
// Service executables must use execution.CASRuntime and never import this package.
package fixture

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"sipbridge/internal/execution"
	"sipbridge/internal/state"
)

type Store interface {
	execution.OperationStore
	Begin(context.Context, state.Ref, string, state.Input) (state.BeginResult, error)
	Resume(context.Context, state.Ref, string) (state.BeginResult, error)
	Commit(context.Context, state.Step, state.Payload, []byte) (state.Receipt, error)
}

// Prepare performs optional recovery I/O before pure reduction. It may refresh
// observations under the same retained claim, never dispatch external effects.
type Prepare func(context.Context, state.Step) (state.Step, error)

type Runtime struct {
	store    Store
	reducer  execution.Reducer
	process  string
	slots    chan struct{}
	sequence atomic.Uint64
	prepare  Prepare
}

func New(store Store, reducer execution.Reducer, process string, concurrency int, prepare ...Prepare) (*Runtime, error) {
	if len(prepare) > 1 || store == nil || reducer == nil || process == "" || len(process) > 160 || concurrency < 1 || concurrency > 256 {
		return nil, errors.New("invalid Redis execution configuration")
	}
	runtime := &Runtime{store: store, reducer: reducer, process: process, slots: make(chan struct{}, concurrency)}
	if len(prepare) == 1 {
		runtime.prepare = prepare[0]
	}
	return runtime, nil
}

// Apply completes only this exact input. A pending input from a crashed worker
// is reduced first and never overwritten by the caller's candidate input.
func (r *Runtime) Apply(ctx context.Context, ref state.Ref, input state.Input) (state.Receipt, error) {
	result, err := r.ApplyCommitted(ctx, ref, input)
	return result.Receipt, err
}

func (r *Runtime) UsesCAS() bool { return false }

// ApplyCommitted preserves the legacy receipt without CAS dispatch hints.
func (r *Runtime) ApplyCommitted(ctx context.Context, ref state.Ref, input state.Input) (execution.Result, error) {
	receipt, err := r.applyLegacy(ctx, ref, input)
	return execution.Result{Receipt: receipt}, err
}

func (r *Runtime) applyLegacy(ctx context.Context, ref state.Ref, input state.Input) (state.Receipt, error) {
	select {
	case r.slots <- struct{}{}:
		defer func() { <-r.slots }()
	case <-ctx.Done():
		return state.Receipt{}, ctx.Err()
	}
	worker := r.worker()
	for {
		if err := ctx.Err(); err != nil {
			return state.Receipt{}, err
		}
		begun, err := r.store.Begin(ctx, ref, worker, input)
		if errors.Is(err, state.ErrBusy) {
			if err = wait(ctx); err != nil {
				return state.Receipt{}, err
			}
			continue
		}
		if err != nil {
			return state.Receipt{}, err
		}
		if begun.Receipt != nil {
			return *begun.Receipt, nil
		}
		if begun.Step == nil {
			return state.Receipt{}, state.ErrCorrupt
		}
		step := *begun.Step
		ref = step.Record.Ref
		receipt, err := r.step(ctx, step)
		if errors.Is(err, state.ErrStale) {
			// Computation produced no external effects. Reloading may find the
			// winning receipt or a still-pending input under a newer claim.
			continue
		}
		if err != nil {
			return state.Receipt{}, err
		}
		if step.Input.ID == input.ID {
			if step.Input.Fingerprint != input.Fingerprint {
				return state.Receipt{}, state.ErrConflict
			}
			return receipt, nil
		}
		// Continue against the actual created incarnation, rather than letting
		// a later delete/recreate turn the same caller into a fresh creator.
	}
}

// Resume processes one retained input selected by ready-hint/repair work. It
// never manufactures an event or extends the admitted input's original budget.
func (r *Runtime) Resume(ctx context.Context, ref state.Ref) (state.Receipt, error) {
	select {
	case r.slots <- struct{}{}:
		defer func() { <-r.slots }()
	case <-ctx.Done():
		return state.Receipt{}, ctx.Err()
	}
	worker := r.worker()
	for {
		if err := ctx.Err(); err != nil {
			return state.Receipt{}, err
		}
		begun, err := r.store.Resume(ctx, ref, worker)
		if err != nil {
			return state.Receipt{}, err
		}
		if begun.Receipt != nil {
			return *begun.Receipt, nil
		}
		if begun.Step == nil {
			return state.Receipt{}, state.ErrCorrupt
		}
		ref = begun.Step.Record.Ref
		receipt, err := r.step(ctx, *begun.Step)
		if errors.Is(err, state.ErrStale) {
			continue
		}
		return receipt, err
	}
}

func (r *Runtime) worker() string {
	return fmt.Sprintf("%s:%d", r.process, r.sequence.Add(1))
}

func (r *Runtime) step(ctx context.Context, step state.Step) (state.Receipt, error) {
	if r.prepare != nil {
		var err error
		step, err = r.prepare(ctx, step)
		if err != nil {
			return state.Receipt{}, err
		}
	}
	next, outcome, err := r.reducer.Reduce(step.Record, step.Input, step.NowMS)
	if err != nil {
		return state.Receipt{}, err
	}
	receipt, err := r.store.Commit(ctx, step, next, outcome)
	if err == nil {
		return receipt, nil
	}
	if !errors.Is(err, state.ErrUnknown) {
		return state.Receipt{}, err
	}
	// The mutation may have committed. An inspection is read-only and does not
	// introduce a new operation, a different identity, or another external effect.
	inspected, inspectErr := r.store.InspectOperation(ctx, step.Record.Ref, step.Input.ID, step.Input.Fingerprint)
	if inspectErr != nil {
		return state.Receipt{}, errors.Join(state.ErrUnknown, inspectErr)
	}
	if inspected.Receipt != nil {
		return *inspected.Receipt, nil
	}
	return state.Receipt{}, state.ErrUnknown
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
