package rediscore

import (
	"context"
	"errors"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"sipbridge/internal/storagecodec"
)

// DriverConfig connects one physical process to the indexed native duties.
// Callbacks must honor cancellation and perform their own atomic native fences.
type DriverConfig struct {
	Connection        *Connection
	Worker, Epoch     string
	Due               func(context.Context, *Store, *DueContext) error
	ReadyOutput       func(context.Context, *Store, ReadyOutput) error
	OwnerExpired      func(context.Context, *Store, ExpiredApplicationContext) error
	ResultHint        func(ResultHint)
	UDPHint           func(UDPHint)
	NativeRecordError error
	OnDiagnostic      func(DriverDiagnostic)
}

// DriverDiagnostic carries bounded operation labels and opaque native references.
type DriverDiagnostic struct {
	Stage       string `json:"stage"`
	Category    string `json:"category"`
	Context     string `json:"context,omitempty"`
	Incarnation string `json:"incarnation,omitempty"`
	Revision    string `json:"revision,omitempty"`
	Work        string `json:"work,omitempty"`
	Member      string `json:"member,omitempty"`
}

func (d *Driver) diagnostic(stage string, err error) {
	d.report(DriverDiagnostic{Stage: stage, Category: d.diagnosticCategory(err)})
}

func (d *Driver) diagnosticCategory(err error) string {
	var timeout net.Error
	switch {
	case errors.Is(err, context.DeadlineExceeded), errors.As(err, &timeout) && timeout.Timeout():
		return "timeout"
	case errors.Is(err, ErrClock):
		return "clock"
	case errors.Is(err, ErrCorrupt):
		return "corrupt"
	case errors.Is(err, ErrAuthority):
		return "authority"
	case errors.Is(err, ErrExpired):
		return "expired"
	case errors.Is(err, ErrUnknown):
		return "unknown"
	case errors.Is(err, errCallbackPanic):
		return "panic"
	case errors.Is(err, storagecodec.ErrRecord), d.cfg.NativeRecordError != nil && errors.Is(err, d.cfg.NativeRecordError):
		return "native-record"
	default:
		return "other"
	}
}

func (d *Driver) report(event DriverDiagnostic) {
	if d.cfg.OnDiagnostic == nil {
		return
	}
	// An observer cannot undo a fence or crash the driver.
	defer func() { _ = recover() }()
	d.cfg.OnDiagnostic(event)
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
func laterTime(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}
	return b
}

func driverChooseDue(turn int, due, owner bool) bool {
	return due && (!owner || turn == 0)
}

var errCallbackPanic = errors.New("native callback panic")

func callDriverWork(work func() error) (err error) {
	defer func() {
		if recover() != nil {
			err = errCallbackPanic
		}
	}()
	return work()
}

func (d *Driver) isolate(ctx context.Context, store *Store, id Identity, revision uint64, work, member string, output OutputRef, err error) {
	if ctx.Err() != nil || !id.valid() || revision == 0 {
		return
	}
	badRecord := errors.Is(err, storagecodec.ErrRecord) || d.cfg.NativeRecordError != nil && errors.Is(err, d.cfg.NativeRecordError)
	if !errors.Is(err, errCallbackPanic) && (!badRecord || errors.Is(err, ErrUnknown) || errors.Is(err, ErrAuthority) ||
		errors.Is(err, ErrExpired) || errors.Is(err, context.DeadlineExceeded)) {
		return
	}
	e := store.park(ctx, d.cfg.Worker, d.cfg.Epoch, id, revision, output)
	if e == nil {
		d.report(DriverDiagnostic{Stage: "context-parked", Category: d.diagnosticCategory(err), Context: id.Context,
			Incarnation: id.Incarnation, Revision: strconv.FormatUint(revision, 10), Work: work, Member: member})
	} else if !errors.Is(e, ErrConflict) && !errors.Is(e, ErrAbsent) {
		d.diagnostic("item-park", e)
	}
}

type driverOwnerPass struct {
	rank, next, workerOffset int
	member                   string
	scan                     applicationScan
	items                    []ExpiredApplicationContext
	scanDone                 bool
	after                    time.Time
}

func (p *driverOwnerPass) advance(drained bool) {
	rank := p.next
	if drained {
		rank--
	}
	*p = driverOwnerPass{rank: max(0, rank)}
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
		stage    string
		ref      OutputRef
		due      *DueContext
		rank     int
		transfer WorkerTransferResult
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
	var dueRank, readyRank int
	var dueAfter, readyAfter time.Time
	var ownerPass driverOwnerPass
	var lastDue *DueContext
	var dueFailures int
	var lastReady OutputRef
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
		dueRank, readyRank, dueAfter, readyAfter = 0, 0, time.Time{}, time.Time{}
		lastDue, lastReady, ownerPass = nil, OutputRef{}, driverOwnerPass{}
		dueFailures = 0
		dirtyProbe = false
		attempts = 0
	}
	defer disconnect("", nil)
	finish := func(r result) {
		jobs--
		if r.stage == "ready" {
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
		dueEligible := !current.Before(dueAfter) && (headValid && lease.Head.Context != "" && !current.Before(headAt) || !headValid && dirtyProbe)
		readyEligible := !current.Before(readyAfter) && (headValid && lease.Ready || !headValid && dirtyProbe)
		ownerEligible := lease.Coordinator && lease.OwnerExpiry.Context != "" && !current.Before(ownerAt) && !current.Before(ownerPass.after)
		if !nativeBusy && (dueEligible || ownerEligible) {
			jobStore := store
			if driverChooseDue(turn, dueEligible, ownerEligible) {
				turn = 1
				due, next, future, e := store.peekDuePage(leaseCtx, d.cfg.Worker, d.cfg.Epoch, dueRank)
				sameTimer := due != nil && lastDue != nil && lastDue.Identity == due.Identity &&
					lastDue.Member == due.Member && lastDue.Assignment == due.Assignment
				switch {
				case e != nil:
					d.diagnostic("item-peek-due", e)
					dueRank, dueAfter = 0, current.Add(WorkerRenewInterval)
				case due == nil:
					dueRank = next
					if next == 0 {
						dueAfter = current.Add(WorkerRenewInterval)
						if !future.IsZero() && future.Before(dueAfter) {
							dueAfter = future
						}
					}
				case sameTimer && (lastDue.Revision == due.Revision || dueFailures >= 2):
					// Recheck a revision race once before yielding to other calls.
					// An unchanged selection has already had its chance.
					dueRank = next + 1
					lastDue = nil
				default:
					if !sameTimer {
						dueFailures = 0
					}
					nativeBusy = true
					jobs++
					go func() {
						e := callDriverWork(func() error { return d.cfg.Due(leaseCtx, jobStore, due) })
						d.isolate(leaseCtx, jobStore, due.Identity, due.Revision, "timer", due.Member, OutputRef{}, e)
						results <- result{stage: "due", due: due, rank: next, err: e}
					}()
				}
			} else {
				turn = 0
				if ownerPass.member == "" {
					member, next, future, e := store.ownerPage(leaseCtx, d.cfg.Worker, d.cfg.Epoch, ownerPass.rank)
					switch {
					case e != nil:
						d.diagnostic("item-peek-owner", e)
						ownerPass = driverOwnerPass{after: current.Add(WorkerRenewInterval)}
					case member == "":
						ownerPass.rank = next
						if next == 0 {
							ownerPass.after = current.Add(WorkerRenewInterval)
							if !future.IsZero() && future.Before(ownerPass.after) {
								ownerPass.after = future
							}
						}
					default:
						ownerPass.member, ownerPass.next = member, next+1
					}
				}
				member := ownerPass.member
				if strings.HasPrefix(member, "w-") {
					source := strings.TrimPrefix(member, "w-")
					offset := ownerPass.workerOffset
					nativeBusy = true
					jobs++
					go func() {
						var transferred WorkerTransferResult
						e := callDriverWork(func() error {
							var err error
							transferred, err = jobStore.TransferExpiredWorker(leaseCtx, WorkerTransfer{Coordinator: d.cfg.Worker, CoordinatorEpoch: d.cfg.Epoch, ExpiredWorker: source, Target: d.cfg.Worker, TargetEpoch: d.cfg.Epoch, Limit: MaxWorkerTransferBatch, offset: offset, isolate: true})
							return err
						})
						results <- result{stage: "transfer", transfer: transferred, err: e}
					}()
				} else if strings.HasPrefix(member, "a-") {
					if len(ownerPass.items) == 0 && !ownerPass.scanDone {
						page, e := store.peekExpiredApplicationPage(leaseCtx, d.cfg.Worker, d.cfg.Epoch, strings.TrimPrefix(member, "a-"), MaxExpiredApplicationBatch, ownerPass.scan)
						if e != nil {
							d.diagnostic("item-peek-application", e)
							ownerPass.advance(errors.Is(e, ErrAbsent))
						} else {
							ownerPass.items, ownerPass.scan = page.Contexts, page.next
							ownerPass.scanDone = page.next == (applicationScan{})
						}
					}
					if len(ownerPass.items) != 0 {
						item := ownerPass.items[0]
						ownerPass.items = ownerPass.items[1:]
						nativeBusy = true
						jobs++
						go func() {
							e := callDriverWork(func() error { return d.cfg.OwnerExpired(leaseCtx, jobStore, item) })
							d.isolate(leaseCtx, jobStore, item.Identity, item.Revision, "application-expiry", "", OutputRef{}, e)
							results <- result{stage: "application", err: e}
						}()
					} else if ownerPass.scanDone {
						ownerPass.advance(false)
					}
				} else if strings.HasPrefix(member, "p-") {
					nativeBusy = true
					jobs++
					go func() {
						event, e := jobStore.abandonParked(leaseCtx, d.cfg.Worker, d.cfg.Epoch, member[2:])
						if e == nil && event.Context != "" {
							d.report(event)
						}
						results <- result{stage: "abandon", err: e}
					}()
				}
			}
		}
		if readyEligible && len(inflight) < 8 {
			pageSize := min(8, len(inflight)+1)
			if readyRank != 0 {
				pageSize = nativeWorkPage
			}
			outputs, next, e := store.peekReadyPage(leaseCtx, d.cfg.Worker, d.cfg.Epoch, pageSize, readyRank)
			if e != nil {
				d.diagnostic("item-peek-ready", e)
				readyRank, readyAfter = 0, current.Add(WorkerRenewInterval)
			} else {
				found := false
				for _, output := range outputs {
					if _, ok := inflight[output.Ref]; ok || output.Ref == lastReady {
						continue
					}
					item := output
					jobStore := store
					inflight[item.Ref] = struct{}{}
					jobs++
					found = true
					go func() {
						e := callDriverWork(func() error { return d.cfg.ReadyOutput(leaseCtx, jobStore, item) })
						d.isolate(leaseCtx, jobStore, item.identity, item.revision, "output", item.Ref.ID, item.Ref, e)
						results <- result{stage: "ready", ref: item.Ref, rank: item.rank, err: e}
					}()
					break
				}
				if found {
					continue
				}
				readyRank = next
				if next == 0 {
					readyAfter = current.Add(WorkerRenewInterval)
				}
			}
		}
		dirtyProbe = false
		delay := time.Until(renewAt)
		if !nativeBusy {
			if headValid && lease.Head.Context != "" {
				delay = min(delay, time.Until(laterTime(headAt, dueAfter)))
			}
			if lease.Coordinator && lease.OwnerExpiry.Context != "" {
				delay = min(delay, time.Until(laterTime(ownerAt, ownerPass.after)))
			}
		}
		if headValid && lease.Ready && len(inflight) < 8 {
			delay = min(delay, time.Until(readyAfter))
		}
		reset(delay)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-changed:
			dueAfter, readyAfter, ownerPass.after = time.Time{}, time.Time{}, time.Time{}
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
			if r.err != nil && !errors.Is(r.err, ErrConflict) && !errors.Is(r.err, ErrAbsent) && !errors.Is(r.err, ErrParked) {
				d.diagnostic("item-"+r.stage, r.err)
			}
			switch r.stage {
			case "due":
				dueRank, lastDue = r.rank, r.due
				if r.err != nil {
					dueFailures++
				} else {
					dueFailures = 0
				}
			case "ready":
				readyRank, readyAfter, lastReady = 0, time.Time{}, r.ref
				if r.err != nil {
					readyRank = r.rank
				}
			case "transfer":
				if r.err == nil && r.transfer.more {
					ownerPass.workerOffset = r.transfer.next
				} else {
					ownerPass.advance(r.transfer.Drained || errors.Is(r.err, ErrAbsent))
				}
			case "abandon":
				ownerPass.advance(r.err == nil || errors.Is(r.err, ErrAbsent))
			}
			renewAt = time.Time{}
		}
	}
}
