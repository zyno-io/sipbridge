package rediscore

import (
	"context"
	"errors"
	"net"
	"strings"
	"sync"
	"time"
)

// DriverConfig connects one physical process to the indexed native duties.
// Callbacks must honor cancellation and perform their own atomic native fences.
type DriverConfig struct {
	Connection        *Connection
	Worker, Epoch     string
	Due               func(context.Context, *Store, *DueContext) error
	ReadyOutput       func(context.Context, *Store, ReadyOutput) error
	OwnerExpired      func(context.Context, *Store, string) error
	ResultHint        func(ResultHint)
	UDPHint           func(UDPHint)
	NativeRecordError error
	OnDiagnostic      func(DriverDiagnostic)
}

// DriverDiagnostic contains only bounded, non-sensitive diagnostic labels.
type DriverDiagnostic struct {
	Stage    string `json:"stage"`
	Category string `json:"category"`
}

func (d *Driver) diagnostic(stage string, err error) {
	if d.cfg.OnDiagnostic == nil {
		return
	}
	category := "other"
	var timeout net.Error
	switch {
	case errors.Is(err, context.DeadlineExceeded), errors.As(err, &timeout) && timeout.Timeout():
		category = "timeout"
	case errors.Is(err, ErrClock):
		category = "clock"
	case errors.Is(err, ErrCorrupt):
		category = "corrupt"
	case errors.Is(err, ErrAuthority):
		category = "authority"
	case errors.Is(err, ErrExpired):
		category = "expired"
	case errors.Is(err, ErrUnknown):
		category = "unknown"
	case d.cfg.NativeRecordError != nil && errors.Is(err, d.cfg.NativeRecordError):
		category = "native-record"
	}
	// An observer cannot undo a fence or crash the driver.
	defer func() { _ = recover() }()
	d.cfg.OnDiagnostic(DriverDiagnostic{Stage: stage, Category: category})
}

type Driver struct {
	cfg             DriverConfig
	mu              sync.Mutex
	running, closed bool
	store           *Store
	wake            *Wake
	leaseCtx        context.Context
	cancelLease     context.CancelFunc
	until           time.Time
	watchdog        *time.Timer
	cancelRun       context.CancelFunc
}

func NewDriver(cfg DriverConfig) (*Driver, error) {
	if cfg.Connection == nil || !reference(cfg.Worker) || len(cfg.Worker) > maxWorkerReferenceBytes || !reference(cfg.Epoch) || cfg.Due == nil || cfg.ReadyOutput == nil || cfg.OwnerExpired == nil || cfg.UDPHint != nil && cfg.ResultHint == nil {
		return nil, ErrConflict
	}
	return &Driver{cfg: cfg}, nil
}

// Current returns a binding that becomes unusable when its lease context ends.
func (d *Driver) Current() (*Store, context.Context, time.Time, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.store == nil || d.leaseCtx == nil || d.leaseCtx.Err() != nil || !time.Now().Before(d.until) {
		return nil, nil, time.Time{}, ErrAuthority
	}
	return d.store, d.leaseCtx, d.until, nil
}
func (d *Driver) Ready() error { _, _, _, err := d.Current(); return err }
func driverChooseDue(turn int, due, owner bool) bool {
	return due && (!owner || turn == 0)
}
func (d *Driver) fence() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.fenceLocked()
}
func (d *Driver) fenceLocked() {
	if d.watchdog != nil {
		d.watchdog.Stop()
		d.watchdog = nil
	}
	if d.cancelLease != nil {
		d.cancelLease()
	}
	d.store = nil
	d.wake = nil
	d.leaseCtx = nil
	d.cancelLease = nil
	d.until = time.Time{}
}
func (d *Driver) fenceDiagnostic(wake *Wake, stage string, err error) {
	d.mu.Lock()
	selected := wake != nil && d.wake == wake
	if selected {
		d.fenceLocked()
	}
	d.mu.Unlock()
	if selected {
		d.diagnostic(stage, err)
	}
}

func (d *Driver) subscriptionFailed(ctx context.Context, wake *Wake, err error) {
	if ctx.Err() != nil {
		d.mu.Lock()
		if d.wake == wake {
			d.fenceLocked()
		}
		d.mu.Unlock()
		return
	}
	d.fenceDiagnostic(wake, "subscription", err)
}
func (d *Driver) publish(store *Store, wake *Wake, until time.Time, fresh bool) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	_, _, connected := wake.Snapshot()
	if d.closed || d.wake != wake || !connected || !time.Now().Before(until) {
		return false
	}
	if d.store == nil {
		if !fresh {
			return false
		}
		d.leaseCtx, d.cancelLease = context.WithCancel(context.Background())
		d.store = store
	}
	if d.store != store {
		return false
	}
	d.until = until
	if d.watchdog != nil {
		d.watchdog.Stop()
	}
	// A stale timer may already be running; compare the exact deadline under lock.
	d.watchdog = time.AfterFunc(time.Until(until), func() {
		d.mu.Lock()
		selected := d.store == store && d.until.Equal(until) && !time.Now().Before(until)
		if selected {
			d.fenceLocked()
		}
		d.mu.Unlock()
		if selected {
			d.diagnostic("watchdog", ErrExpired)
		}
	})
	return true
}
func (d *Driver) Close() error {
	d.mu.Lock()
	d.closed = true
	cancel := d.cancelRun
	d.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	d.fence()
	return nil
}

// Run owns one subscription, one native callback and at most eight physical
// callbacks. A fence cancels all jobs; no new generation starts until they exit.
func (d *Driver) Run(parent context.Context) error {
	d.mu.Lock()
	if d.running || d.closed {
		d.mu.Unlock()
		return ErrConflict
	}
	d.running = true
	ctx, cancel := context.WithCancel(parent)
	d.cancelRun = cancel
	d.mu.Unlock()
	defer func() { cancel(); d.fence(); d.mu.Lock(); d.cancelRun = nil; d.mu.Unlock() }()
	type result struct {
		physical bool
		ref      OutputRef
		err      error
	}
	results := make(chan result, 9)
	inflight := make(map[OutputRef]struct{}, 8)
	nativeBusy := false
	jobs := 0
	defer func() {
		// Disconnect runs first and cancels the lease context. Do not let the
		// owner close shared resources until every callback has returned.
		d.fence()
		for jobs > 0 {
			<-results
			jobs--
		}
	}()
	var store *Store
	var wake *Wake
	var changed <-chan struct{}
	var lease WorkerLease
	var headAt, ownerAt, renewAt, lastRefresh time.Time
	headValid := false
	dirtyProbe := false
	attempts := 0
	acquired := false
	leasePublished := false
	turn := 0
	timer := time.NewTimer(time.Hour)
	if !timer.Stop() {
		<-timer.C
	}
	defer timer.Stop()
	reset := func(delay time.Duration) {
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(max(0, delay))
	}
	disconnect := func(stage string, cause error) {
		if stage == "" || ctx.Err() != nil {
			d.fence()
		} else {
			d.fenceDiagnostic(wake, stage, cause)
		}
		leasePublished = false
		if wake != nil {
			_ = wake.Close()
			wake = nil
		}
		store = nil
		changed = nil
		renewAt = time.Time{}
		lastRefresh = time.Time{}
		headValid = false
		dirtyProbe = false
		attempts = 0
	}
	defer disconnect("", nil)
	finish := func(r result) {
		jobs--
		if r.physical {
			delete(inflight, r.ref)
		} else {
			nativeBusy = false
		}
	}
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if store == nil && jobs == 0 {
			stage := "startup-refresh"
			next, err := d.cfg.Connection.Refresh(ctx)
			if err == nil {
				stage = "startup-subscribe"
				if d.cfg.ResultHint != nil {
					wake, err = next.SubscribeWakeResults(ctx, d.cfg.Worker, d.cfg.ResultHint, d.cfg.UDPHint)
				} else {
					wake, err = next.SubscribeWake(ctx, d.cfg.Worker)
				}
			}
			if err == nil {
				store = next
				d.mu.Lock()
				d.wake = wake
				d.mu.Unlock()
				go func(w *Wake) {
					for {
						if receiveErr := w.Receive(ctx); receiveErr != nil {
							d.subscriptionFailed(ctx, w, receiveErr)
							return
						}
					}
				}(wake)
				_, changed, _ = wake.Snapshot()
				renewAt = time.Time{}
			} else {
				if ctx.Err() == nil {
					d.diagnostic(stage, err)
				}
				reset(WorkerRenewInterval)
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-timer.C:
				}
				continue
			}
		}
		if store == nil {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case r := <-results:
				finish(r)
			}
			continue
		}
		if leasePublished && d.Ready() != nil {
			disconnect("ready", ErrAuthority)
			continue
		}
		now := time.Now()
		if renewAt.IsZero() || !now.Before(renewAt) {
			generation, _, live := wake.Snapshot()
			if !live {
				disconnect("subscription", ErrAuthority)
				continue
			}
			req := WorkerLeaseRequest{Worker: d.cfg.Worker, Epoch: d.cfg.Epoch, PrimaryRunID: store.primary, Acquire: !acquired, Reacquire: acquired && d.Ready() != nil}
			acquired = true
			next, err := store.RenewWorker(ctx, req)
			if err != nil {
				disconnect("renew", err)
				reset(WorkerRenewInterval)
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-timer.C:
				}
				continue
			}
			observed := time.Now()
			lastRefresh = observed
			if !d.publish(store, wake, next.Until, !leasePublished) {
				disconnect("lease-publish", ErrAuthority)
				continue
			}
			leasePublished = true
			after, newChanged, connected := wake.Snapshot()
			if !connected {
				disconnect("subscription", ErrAuthority)
				continue
			}
			renewAt = observed.Add(WorkerRenewInterval)
			lease = next
			headAt = observed.Add(next.Head.After)
			ownerAt = observed.Add(next.OwnerExpiry.After)
			if after != generation {
				headValid = false
				dirtyProbe = true
				attempts++
				if attempts < 2 {
					renewAt = time.Time{}
					changed = newChanged
				} else {
					changed = nil
					attempts = 0
				}
			} else {
				attempts = 0
				headValid = true
				dirtyProbe = false
				changed = newChanged
			}
		}
		_, leaseCtx, _, err := d.Current()
		if err != nil {
			disconnect("ready", err)
			continue
		}
		current := time.Now()
		// Dirty hints never select work. One bounded authoritative probe can still
		// make progress while the next stable subscription generation is pending.
		dueEligible := headValid && lease.Head.Context != "" && !current.Before(headAt) || !headValid && dirtyProbe
		readyEligible := headValid && lease.Ready || !headValid && dirtyProbe
		ownerEligible := lease.Coordinator && lease.OwnerExpiry.Context != "" && !current.Before(ownerAt)
		if !nativeBusy && (dueEligible || ownerEligible) {
			var work func() error
			jobStore := store
			chooseDue := driverChooseDue(turn, dueEligible, ownerEligible)
			if chooseDue {
				due, e := store.PeekDue(leaseCtx, d.cfg.Worker, d.cfg.Epoch)
				if e != nil {
					disconnect("peek-due", e)
					continue
				}
				if due != nil {
					work = func() error { return d.cfg.Due(leaseCtx, jobStore, due) }
					turn = 1
				} else if headValid {
					renewAt = time.Time{}
				}
			} else {
				owner := lease.OwnerExpiry.Context
				if strings.HasPrefix(owner, "w-") {
					source := strings.TrimPrefix(owner, "w-")
					work = func() error {
						_, e := jobStore.TransferExpiredWorker(leaseCtx, WorkerTransfer{Coordinator: d.cfg.Worker, CoordinatorEpoch: d.cfg.Epoch, ExpiredWorker: source, Target: d.cfg.Worker, TargetEpoch: d.cfg.Epoch, Limit: MaxWorkerTransferBatch})
						return e
					}
				} else if strings.HasPrefix(owner, "a-") {
					term := strings.TrimPrefix(owner, "a-")
					work = func() error { return d.cfg.OwnerExpired(leaseCtx, jobStore, term) }
				} else {
					disconnect("owner-head", ErrCorrupt)
					continue
				}
				turn = 0
			}
			if work != nil {
				nativeBusy = true
				jobs++
				go func() { results <- result{err: work()} }()
			}
		}
		if readyEligible && len(inflight) < 8 {
			outputs, e := store.PeekReady(leaseCtx, d.cfg.Worker, d.cfg.Epoch, min(8, len(inflight)+1))
			if e != nil {
				disconnect("peek-ready", e)
				continue
			}
			found := false
			for _, output := range outputs {
				if _, ok := inflight[output.Ref]; ok {
					continue
				}
				item := output
				jobStore := store
				inflight[item.Ref] = struct{}{}
				jobs++
				found = true
				go func() {
					results <- result{physical: true, ref: item.Ref, err: d.cfg.ReadyOutput(leaseCtx, jobStore, item)}
				}()
				break
			}
			if found {
				continue
			}
			// The prefix is already in flight or disappeared; wait for a real change.
			lease.Ready = false
		}
		dirtyProbe = false
		delay := time.Until(renewAt)
		if !nativeBusy {
			if headValid && lease.Head.Context != "" {
				delay = min(delay, time.Until(headAt))
			}
			if lease.Coordinator && lease.OwnerExpiry.Context != "" {
				delay = min(delay, time.Until(ownerAt))
			}
		}
		reset(delay)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-changed:
			// Coalesce a burst of hints without letting a continuously closing
			// notification channel turn into an unbounded renewal loop.
			next := lastRefresh.Add(10 * time.Millisecond)
			if time.Now().Before(next) {
				if renewAt.IsZero() || next.Before(renewAt) {
					renewAt = next
				}
				changed = nil
			} else {
				renewAt = time.Time{}
			}
		case <-timer.C:
		case r := <-results:
			finish(r)
			if r.err != nil && !errors.Is(r.err, ErrConflict) && !errors.Is(r.err, ErrAbsent) {
				stage := "native-callback"
				if r.physical {
					stage = "physical-callback"
				}
				disconnect(stage, r.err)
				reset(WorkerRenewInterval)
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-timer.C:
				}
				continue
			}
			renewAt = time.Time{}
		}
	}
}
