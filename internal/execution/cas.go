package execution

import (
	"context"
	"errors"
	"time"

	"sipbridge/internal/state"
)

const casGroupGateStripes = 1024

type UpdateStore interface {
	ReadUpdate(context.Context, state.Ref, state.Input) (state.UpdateResult, error)
	CompareAndSwap(context.Context, state.Update, state.Payload, []byte) (state.Receipt, error)
}

type CASStore interface {
	OperationStore
	UpdateStore
}

// CASRuntime executes pure computations without legacy claims or recovery.
type CASRuntime struct {
	store    CASStore
	reducer  Reducer
	slots    chan struct{}
	casGates *[casGroupGateStripes]chan struct{}
}

// ErrUncommitted proves this invocation committed no protocol transition. It
// preserves prompt availability refusals without manufacturing an unknown send.
var ErrUncommitted = errors.New("computation was not committed")

// ErrReadOnly is restricted to a negative internal precondition result. It never
// grants permission for external work and is not a retained operation receipt.
var ErrReadOnly = errors.New("internal computation requires no state change")

// NewCAS uses transient reads and atomic CAS; no pure computation is made recoverable.
func NewCAS(store CASStore, reducer Reducer, concurrency int) (*CASRuntime, error) {
	if store == nil || reducer == nil || concurrency < 1 || concurrency > 256 {
		return nil, errors.New("invalid CAS execution configuration")
	}
	runtime := &CASRuntime{store: store, reducer: reducer, slots: make(chan struct{}, concurrency)}
	runtime.casGates = new([casGroupGateStripes]chan struct{})
	for i := range runtime.casGates {
		gate := make(chan struct{}, 1)
		gate <- struct{}{}
		runtime.casGates[i] = gate
	}
	return runtime, nil
}

func casGroupGateIndex(ref state.Ref) uint32 {
	index := uint32(ref.Partition)
	for i := 0; i < len(ref.ID); i++ {
		index ^= uint32(ref.ID[i])
		index *= 16777619
	}
	return (index ^ (index >> 16)) % casGroupGateStripes
}

func (r *CASRuntime) acquireCASGroup(ctx context.Context, ref state.Ref) (func(), error) {
	index := casGroupGateIndex(ref)
	gate := r.casGates[index]
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	select {
	case <-gate:
		if err := ctx.Err(); err != nil {
			gate <- struct{}{}
			return nil, err
		}
		return func() { gate <- struct{}{} }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (r *CASRuntime) UsesCAS() bool { return r != nil }

func (r *CASRuntime) Apply(ctx context.Context, ref state.Ref, input state.Input) (state.Receipt, error) {
	result, err := r.ApplyCommitted(ctx, ref, input)
	return result.Receipt, err
}

// ApplyCommitted returns dispatch hints only after a definite fresh CAS commit.
func (r *CASRuntime) ApplyCommitted(ctx context.Context, ref state.Ref, input state.Input) (Result, error) {
	parentCtx := ctx
	timing := RequestTiming(parentCtx)
	admissionCtx := parentCtx
	if input.DeadlineMS > 0 {
		var stop context.CancelFunc
		admissionCtx, stop = context.WithDeadline(parentCtx, time.UnixMilli(input.DeadlineMS))
		defer stop()
	}
	readUpdate := func(readCtx context.Context, readRef state.Ref) (state.UpdateResult, error) {
		started := time.Now()
		read, err := r.store.ReadUpdate(readCtx, readRef, input)
		if timing != nil {
			timing.Read += time.Since(started)
			timing.Reads++
		}
		return read, err
	}
	expiredReplay := func() (Result, error) {
		if err := parentCtx.Err(); err != nil {
			return Result{}, errors.Join(ErrUncommitted, err)
		}
		fallbackCtx, cancel := context.WithTimeout(parentCtx, 2*time.Second)
		defer cancel()
		queued := time.Now()
		select {
		case r.slots <- struct{}{}:
			if timing != nil {
				timing.Slot += time.Since(queued)
			}
			defer func() { <-r.slots }()
		case <-fallbackCtx.Done():
			if timing != nil {
				timing.Slot += time.Since(queued)
			}
			return Result{}, errors.Join(ErrUncommitted, fallbackCtx.Err())
		}
		if err := parentCtx.Err(); err != nil {
			return Result{}, errors.Join(ErrUncommitted, err)
		}
		read, err := readUpdate(fallbackCtx, ref)
		if err == nil && read.Receipt != nil {
			return Result{Receipt: *read.Receipt}, nil
		}
		if err != nil {
			return Result{}, errors.Join(ErrUncommitted, context.DeadlineExceeded, err)
		}
		return Result{}, errors.Join(ErrUncommitted, context.DeadlineExceeded)
	}
	semanticExpired := func() bool {
		return input.DeadlineMS > 0 && time.Now().UnixMilli() >= input.DeadlineMS
	}
	waitFailure := func(waitErr error) (Result, error) {
		if err := parentCtx.Err(); err != nil {
			return Result{}, errors.Join(ErrUncommitted, err)
		}
		if semanticExpired() {
			return expiredReplay()
		}
		return Result{}, errors.Join(ErrUncommitted, waitErr)
	}
	groupQueued := time.Now()
	releaseGroup, err := r.acquireCASGroup(admissionCtx, ref)
	if timing != nil {
		timing.GroupWait += time.Since(groupQueued)
	}
	if err != nil {
		return waitFailure(err)
	}
	groupHeld := true
	defer func() {
		if groupHeld {
			releaseGroup()
		}
	}()
	if err := admissionCtx.Err(); err != nil {
		groupHeld = false
		releaseGroup()
		return waitFailure(err)
	}

	queued := time.Now()
	select {
	case r.slots <- struct{}{}:
		if timing != nil {
			timing.Slot += time.Since(queued)
		}
	case <-admissionCtx.Done():
		if timing != nil {
			timing.Slot += time.Since(queued)
		}
		groupHeld = false
		releaseGroup()
		return waitFailure(admissionCtx.Err())
	}
	slotHeld := true
	defer func() {
		if slotHeld {
			<-r.slots
		}
	}()
	if err := admissionCtx.Err(); err != nil {
		slotHeld = false
		<-r.slots
		groupHeld = false
		releaseGroup()
		return waitFailure(err)
	}

	// The admitted first read keeps the caller's existing store-I/O bound. It
	// may return an exact receipt even if the semantic deadline elapsed meanwhile.
	read, err := readUpdate(parentCtx, ref)
	if err == nil && read.Receipt != nil {
		return Result{Receipt: *read.Receipt}, nil
	}
	for {
		if err := parentCtx.Err(); err != nil {
			return Result{}, errors.Join(ErrUncommitted, err)
		}
		if err := admissionCtx.Err(); err != nil {
			return Result{}, errors.Join(ErrUncommitted, err)
		}
		if read.Ref != nil {
			// Keep this incarnation even after a definite conflict. Its deletion
			// cannot turn the request into a fresh creator on the next read.
			ref = *read.Ref
		}
		if err == nil && read.Receipt != nil {
			return Result{Receipt: *read.Receipt}, nil
		}
		if err == nil {
			if read.Update == nil {
				return Result{}, errors.Join(ErrUncommitted, state.ErrCorrupt)
			}
			update := *read.Update
			if update.Record.Pending != nil {
				return Result{}, errors.Join(ErrUncommitted, state.ErrBusy)
			}
			started := time.Now()
			next, outcome, reduceErr := r.reducer.Reduce(update.Record, update.Input, update.NowMS)
			if timing != nil {
				timing.Reduce += time.Since(started)
				timing.Reductions++
			}
			if errors.Is(reduceErr, ErrReadOnly) && update.Record.Pending == nil && input.Proof && (input.Kind == "effect-start" || input.Kind == "effect-result" || input.Kind == "timer") {
				return Result{Receipt: state.Receipt{OperationID: input.ID, Fingerprint: input.Fingerprint, Revision: update.Record.Revision,
					ExpiresMS: input.ReceiptUntilMS, Outcome: outcome}}, nil
			}
			if reduceErr != nil {
				return Result{}, errors.Join(ErrUncommitted, reduceErr)
			}
			started = time.Now()
			receipt, commitErr := r.store.CompareAndSwap(admissionCtx, update, next, outcome)
			if timing != nil {
				timing.Commit += time.Since(started)
				timing.Commits++
				if errors.Is(commitErr, state.ErrUnknown) {
					timing.Unknown++
				}
			}
			if commitErr == nil {
				return Result{Receipt: receipt, Committed: committedEffects(update.Record.Ref, next.Effects)}, nil
			}
			if errors.Is(commitErr, state.ErrUnknown) {
				// Inspect the same external operation against this exact proposed
				// incarnation. Internal uncertainty is never replayed here.
				if !input.Proof || input.Kind == "command" {
					known, inspectErr := r.store.InspectOperation(admissionCtx, update.Record.Ref, input.ID, input.Fingerprint)
					if inspectErr == nil && known.Receipt != nil {
						return Result{Receipt: *known.Receipt}, nil
					}
				}
				return Result{}, commitErr
			}
			err = commitErr
		}
		if !errors.Is(err, state.ErrStale) && !errors.Is(err, state.ErrBusy) {
			return Result{}, errors.Join(ErrUncommitted, err)
		}
		if timing != nil {
			if errors.Is(err, state.ErrStale) {
				timing.Stale++
			} else {
				timing.Busy++
			}
		}
		if err := wait(admissionCtx); err != nil {
			return Result{}, errors.Join(ErrUncommitted, err)
		}
		if err := parentCtx.Err(); err != nil {
			return Result{}, errors.Join(ErrUncommitted, err)
		}
		if err := admissionCtx.Err(); err != nil {
			return Result{}, errors.Join(ErrUncommitted, err)
		}
		read, err = readUpdate(admissionCtx, ref)
	}
}
