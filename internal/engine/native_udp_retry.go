package engine

import (
	"container/heap"
	"context"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/emiago/sipgo/sip/portable"
	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/rediscore"
)

const nativeUDPHintWindow = 2 * time.Second

type nativeUDPRetryKey struct{ Context, Incarnation, Procedure string }
type nativeUDPRetrySeed struct {
	Key     nativeUDPRetryKey
	Attempt rediscore.OutputRef
	// The caller must obtain this exact attempt's inspection after native recording.
	Proof       rediscore.OutputInspection
	Captured    time.Time
	Transaction portable.Transaction
	Flow        *pb.FlowRef
}
type nativeUDPRetryLimits struct {
	Entries, Hints, Writers int
	Bytes                   int64
}

// The callback returns the current lease context and Driver.Current's
// monotonic deadline; wall-only timestamps cannot safely cap a retry.
type nativeUDPRetryLease func() (context.Context, time.Time, error)

// write must treat wire and flow as immutable, honor ctx, and check until again
// immediately before its socket write. It never persists a new physical attempt.
type nativeUDPRetryWrite func(context.Context, *pb.FlowRef, []byte, time.Time) error

type nativeUDPRetryEntry struct {
	key             nativeUDPRetryKey
	tx              portable.Transaction
	dialog          *portable.Dialog
	tick            portable.Timer
	terminal        portable.Timer
	flow            *pb.FlowRef
	wire            []byte
	anchor          time.Time
	anchorMS        int64
	cutoff          time.Time
	ctx             context.Context
	cancel          context.CancelFunc
	stopLease       func() bool
	stopOwner       func() bool
	bytes           int64
	inflight        bool
	accountedParsed bool
	index           int
}
type nativeUDPRetryHeap []*nativeUDPRetryEntry

func (h nativeUDPRetryHeap) Len() int { return len(h) }
func (h nativeUDPRetryHeap) Less(i, j int) bool {
	return nativeUDPRetryAt(h[i]).Before(nativeUDPRetryAt(h[j]))
}
func (h nativeUDPRetryHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i]; h[i].index = i; h[j].index = j }
func (h *nativeUDPRetryHeap) Push(x interface{}) {
	e := x.(*nativeUDPRetryEntry)
	e.index = len(*h)
	*h = append(*h, e)
}
func (h *nativeUDPRetryHeap) Pop() interface{} {
	old := *h
	n := len(old)
	e := old[n-1]
	old[n-1] = nil
	e.index = -1
	*h = old[:n-1]
	return e
}
func nativeUDPRetryAt(e *nativeUDPRetryEntry) time.Time {
	if e.inflight {
		return e.cutoff
	}
	var x, terminal portable.Deadline
	if e.dialog != nil {
		x, terminal = e.dialog.Timers[e.tick], e.dialog.Timers[e.terminal]
	} else {
		x, terminal = e.tx.Timers[e.tick], e.tx.Timers[e.terminal]
	}
	if x.Active && terminal.Active && x.At < terminal.At {
		return e.anchor.Add(time.Duration(x.At-e.anchorMS) * time.Millisecond)
	}
	return e.cutoff
}

func nativeUDPRetryDeadline(leaseUntil time.Time, storedMS int64) time.Time {
	anchor := leaseUntil.Add(-time.Duration(leaseUntil.Nanosecond() % int(time.Millisecond)))
	return anchor.Add(time.Duration(storedMS-leaseUntil.UnixMilli()) * time.Millisecond)
}

type nativeUDPHint struct {
	until time.Time
	stop  bool
}
type nativeUDPRetryResult struct{ entry *nativeUDPRetryEntry }
type nativeUDPRetry struct {
	mu            sync.Mutex
	entries       map[nativeUDPRetryKey]*nativeUDPRetryEntry
	hints         map[nativeUDPRetryKey]nativeUDPHint
	queue         nativeUDPRetryHeap
	bytes         int64
	limits        nativeUDPRetryLimits
	overflowUntil time.Time
	hintCleanupAt time.Time
	lease         nativeUDPRetryLease
	write         nativeUDPRetryWrite
	ctx           context.Context
	cancel        context.CancelFunc
	wake          chan struct{}
	results       chan nativeUDPRetryResult
	done          chan struct{}
}

func newNativeUDPRetry(parent context.Context, limits nativeUDPRetryLimits, lease nativeUDPRetryLease, write nativeUDPRetryWrite) (*nativeUDPRetry, error) {
	if parent == nil || limits.Entries < 1 || limits.Entries > 4096 || limits.Hints < 1 || limits.Hints > 4096 || limits.Writers < 2 || limits.Writers > 64 || limits.Bytes < 1 || limits.Bytes > 1<<30 || lease == nil || write == nil {
		return nil, errors.New("invalid UDP retry configuration")
	}
	ctx, cancel := context.WithCancel(parent)
	m := &nativeUDPRetry{entries: make(map[nativeUDPRetryKey]*nativeUDPRetryEntry), hints: make(map[nativeUDPRetryKey]nativeUDPHint), limits: limits, lease: lease, write: write, ctx: ctx, cancel: cancel, wake: make(chan struct{}, 1), results: make(chan nativeUDPRetryResult, limits.Writers), done: make(chan struct{})}
	go m.run()
	return m, nil
}
func (m *nativeUDPRetry) signal() {
	select {
	case m.wake <- struct{}{}:
	default:
	}
}
func (m *nativeUDPRetry) remove(e *nativeUDPRetryEntry) {
	if m.entries[e.key] != e {
		return
	}
	delete(m.entries, e.key)
	heap.Remove(&m.queue, e.index)
	if !e.inflight {
		m.bytes -= e.bytes
	}
	e.stopLease()
	e.stopOwner()
	e.cancel()
}
func (m *nativeUDPRetry) pruneHints(now time.Time) {
	m.hintCleanupAt = time.Time{}
	for key, h := range m.hints {
		if !now.Before(h.until) {
			delete(m.hints, key)
		} else if m.hintCleanupAt.IsZero() || h.until.Before(m.hintCleanupAt) {
			m.hintCleanupAt = h.until
		}
	}
	if len(m.hints) == 0 {
		m.hints = nil
	}
}
func validNativeUDPRetryKey(k nativeUDPRetryKey) bool {
	return k.Context != "" && len(k.Context) <= 192 && k.Incarnation != "" && len(k.Incarnation) <= 192 && k.Procedure != "" && len(k.Procedure) <= 194
}

// Arm is called only with the exact inspected WRITTEN and recorded output, and
// native selected state read after that proof. No response matching occurs here.
func (m *nativeUDPRetry) Arm(seed nativeUDPRetrySeed) error {
	now := time.Now()
	tx := seed.Transaction
	tick, terminal := portable.TimerE, portable.TimerF
	eligible := tx.Family == portable.NonInviteClient && (tx.Phase == portable.Trying || tx.Phase == portable.Proceeding)
	if tx.Family == portable.InviteClient {
		tick, terminal = portable.TimerA, portable.TimerB
		eligible = tx.Phase == portable.Calling && tx.LastResponse == nil
	}
	e, f := tx.Timers[tick], tx.Timers[terminal]
	if !validNativeUDPRetryKey(seed.Key) || seed.Attempt.Context != seed.Key.Context || seed.Attempt.Incarnation != seed.Key.Incarnation || seed.Attempt.Attempt == "" || seed.Proof.State != rediscore.OutputWritten || !seed.Proof.Settled || seed.Proof.MetadataField == "" || !nativeMessageField(seed.Proof.MetadataField) || seed.Captured.IsZero() || seed.Captured == seed.Captured.Round(0) || now.Before(seed.Captured) || now.Sub(seed.Captured) > nativeUDPHintWindow || !eligible || tx.Reliable || tx.Origin.IsZero() || tx.Origin.Transport() != "udp" || tx.Origin.WireLen() == 0 || tx.Origin.WireLen() > 65536 || len(tx.Key) == 0 || len(tx.Key) > 65536 || !e.Active || !f.Active || e.Generation == 0 || f.Generation == 0 || e.At <= 0 || f.At <= 0 || e.Interval <= 0 || tx.Profile.T1 <= 0 || tx.Profile.T2 < tx.Profile.T1 || !validPolicyFlow(seed.Flow) || int64(proto.Size(seed.Flow)) > m.limits.Bytes || seed.Flow.Transport != "udp" || seed.Flow.ConnectionId != seed.Attempt.Connection {
		return errors.New("invalid UDP retry seed")
	}
	metadata, err := decodeNativeOutput(seed.Proof.Metadata)
	if err != nil || metadata.Procedure != seed.Key.Procedure || metadata.Status != 0 || (metadata.Method == "INVITE") != (tx.Family == portable.InviteClient) || !nativeMessageField(seed.Proof.MessageField) || seed.Proof.MessageField == "" || seed.Proof.WireHash == ([32]byte{}) || !proto.Equal(metadata.Write.Flow, seed.Flow) {
		return errors.New("UDP retry proof metadata mismatch")
	}
	proofRef, err := nativeOutputRef(metadata.Write)
	if err != nil || proofRef != seed.Attempt {
		return errors.New("UDP retry attempt mismatch")
	}
	worker, err := nativeWorkerReference(seed.Flow.Ingress)
	if err != nil || worker != seed.Attempt.Worker || seed.Flow.Ingress.Epoch != seed.Attempt.Epoch {
		return errors.New("UDP retry flow mismatch")
	}
	leaseCtx, until, err := m.lease()
	if err != nil || leaseCtx == nil || leaseCtx.Err() != nil || until == until.Round(0) || !now.Before(until) {
		return errors.New("UDP retry lease unavailable")
	}
	anchorMS := until.UnixMilli()
	const maxOffset = int64((1<<63 - 1) / time.Millisecond)
	if anchorMS <= 0 || e.At > anchorMS && e.At-anchorMS > maxOffset || e.At < anchorMS && anchorMS-e.At > maxOffset || f.At > anchorMS && f.At-anchorMS > maxOffset || f.At < anchorMS && anchorMS-f.At > maxOffset {
		return errors.New("UDP retry deadline out of range")
	}
	// Copy only selected native facts; never retain response, ACK, or other timers.
	slim := portable.Transaction{Version: tx.Version, Family: tx.Family, Phase: tx.Phase, Key: strings.Clone(tx.Key), Origin: tx.Origin, Profile: tx.Profile, Timers: map[portable.Timer]portable.Deadline{tick: e, terminal: f}}
	flow := proto.Clone(seed.Flow).(*pb.FlowRef)
	key := nativeUDPRetryKey{Context: strings.Clone(seed.Key.Context), Incarnation: strings.Clone(seed.Key.Incarnation), Procedure: strings.Clone(seed.Key.Procedure)}
	size := slim.Origin.RetainedBytes() + int64(proto.Size(flow)+len(slim.Key)+len(key.Context)+len(key.Incarnation)+len(key.Procedure))
	entryCtx, cancel := context.WithCancel(leaseCtx)
	anchor := until.Add(-time.Duration(until.Nanosecond() % int(time.Millisecond)))
	cutoff := nativeUDPRetryDeadline(until, f.At)
	entry := &nativeUDPRetryEntry{key: key, tx: slim, tick: tick, terminal: terminal, flow: flow, anchor: anchor, anchorMS: anchorMS, cutoff: cutoff, ctx: entryCtx, cancel: cancel, bytes: size, index: -1}
	return m.install(entry, seed.Captured, leaseCtx, until)
}

func (m *nativeUDPRetry) install(entry *nativeUDPRetryEntry, captured time.Time, leaseCtx context.Context, until time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	admitted := time.Now()
	m.pruneHints(admitted)
	if admitted.Before(captured) || admitted.Sub(captured) > nativeUDPHintWindow || !admitted.Before(entry.cutoff) || m.ctx.Err() != nil || leaseCtx.Err() != nil || !admitted.Before(until) || admitted.Before(m.overflowUntil) || len(m.entries) >= m.limits.Entries || entry.bytes > m.limits.Bytes-m.bytes || m.entries[entry.key] != nil {
		entry.cancel()
		return errors.New("UDP retry capacity or fence")
	}
	if hint, ok := m.hints[entry.key]; ok {
		if hint.stop {
			entry.cancel()
			return errors.New("UDP retry stopped")
		}
		if entry.tick == portable.TimerE {
			entry.tx.Phase = portable.Proceeding
			timer := entry.tx.Timers[portable.TimerE]
			timer.Interval = entry.tx.Profile.T2
			entry.tx.Timers[portable.TimerE] = timer
		}
	}
	entry.stopLease = context.AfterFunc(leaseCtx, func() {
		m.mu.Lock()
		m.remove(entry)
		m.mu.Unlock()
		m.signal()
	})
	entry.stopOwner = context.AfterFunc(m.ctx, entry.cancel)
	m.entries[entry.key] = entry
	m.bytes += entry.bytes
	heap.Push(&m.queue, entry)
	m.signal()
	return nil
}
func (m *nativeUDPRetry) hint(key nativeUDPRetryKey, stop bool) {
	if !validNativeUDPRetryKey(key) {
		return
	}
	key = nativeUDPRetryKey{Context: strings.Clone(key.Context), Incarnation: strings.Clone(key.Incarnation), Procedure: strings.Clone(key.Procedure)}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.ctx.Err() != nil {
		return
	}
	now := time.Now()
	m.pruneHints(now)
	if e := m.entries[key]; e != nil {
		if stop {
			m.remove(e)
		} else if e.tick == portable.TimerE {
			e.tx.Phase = portable.Proceeding
			d := e.tx.Timers[portable.TimerE]
			d.Interval = e.tx.Profile.T2
			e.tx.Timers[portable.TimerE] = d
		}
	}
	previous, exists := m.hints[key]
	if !exists && len(m.hints) >= m.limits.Hints {
		m.overflowUntil = now.Add(nativeUDPHintWindow)
		m.signal()
		return
	}
	if m.hints == nil {
		m.hints = make(map[nativeUDPRetryKey]nativeUDPHint)
	}
	m.hints[key] = nativeUDPHint{until: now.Add(nativeUDPHintWindow), stop: stop || previous.stop}
	until := m.hints[key].until
	if m.hintCleanupAt.IsZero() || until.Before(m.hintCleanupAt) {
		m.hintCleanupAt = until
	}
	m.signal()
}
func (m *nativeUDPRetry) Stop(key nativeUDPRetryKey)       { m.hint(key, true) }
func (m *nativeUDPRetry) Proceeding(key nativeUDPRetryKey) { m.hint(key, false) }
func (m *nativeUDPRetry) Close()                           { m.cancel(); m.signal(); <-m.done }

func (m *nativeUDPRetry) run() {
	defer close(m.done)
	active := 0
	timer := time.NewTimer(time.Hour)
	if !timer.Stop() {
		<-timer.C
	}
	defer timer.Stop()
	reset := func(d time.Duration) {
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(max(0, d))
	}
	for {
		m.mu.Lock()
		if m.ctx.Err() != nil {
			for len(m.queue) > 0 {
				m.remove(m.queue[0])
			}
			m.hints = nil
			m.hintCleanupAt = time.Time{}
			m.overflowUntil = time.Time{}
			m.entries = nil
			m.mu.Unlock()
			for active > 0 {
				<-m.results
				active--
			}
			return
		}
		now := time.Now()
		if !m.hintCleanupAt.IsZero() && !now.Before(m.hintCleanupAt.Add(25*time.Millisecond)) {
			m.pruneHints(now)
		}
		for len(m.queue) > 0 && m.queue[0].ctx.Err() != nil {
			m.remove(m.queue[0])
		}
		if len(m.queue) > 0 && !now.Before(nativeUDPRetryAt(m.queue[0])) {
			entry := m.queue[0]
			if !now.Before(entry.cutoff) {
				m.remove(entry)
				m.mu.Unlock()
				continue
			}
			if !entry.inflight && active < m.limits.Writers {
				elapsed := now.Sub(entry.anchor)
				elapsedMS := elapsed.Milliseconds()
				if elapsed < 0 && elapsed%time.Millisecond != 0 {
					elapsedMS--
				}
				logicalNow := entry.anchorMS + elapsedMS
				var effects []portable.Effect
				var err error
				if entry.dialog != nil {
					d := entry.dialog.Timers[entry.tick]
					var next portable.Dialog
					next, effects, err = portable.StepDialog(*entry.dialog, portable.DialogInput{Kind: portable.DialogTimer, Now: logicalNow, Timer: entry.tick, Generation: d.Generation})
					entry.dialog = &next
				} else {
					d := entry.tx.Timers[entry.tick]
					var next portable.Transaction
					next, effects, err = portable.StepTransaction(entry.tx, portable.Input{Kind: portable.TimerFired, Now: logicalNow, Timer: entry.tick, Generation: d.Generation})
					entry.tx = next
				}
				if err != nil {
					m.remove(entry)
					m.mu.Unlock()
					continue
				}
				send := false
				for _, effect := range effects {
					if effect.Kind == portable.Send || entry.dialog != nil && effect.Kind == portable.Respond {
						send = true
					}
				}
				if entry.tick != portable.TimerE && entry.tick != portable.TimerA && !entry.accountedParsed {
					newBytes := nativeInviteRetryEntryBytes(entry)
					if newBytes > m.limits.Bytes-(m.bytes-entry.bytes) {
						m.remove(entry)
						m.mu.Unlock()
						continue
					}
					m.bytes += newBytes - entry.bytes
					entry.bytes, entry.accountedParsed = newBytes, true
				}
				if !send {
					heap.Fix(&m.queue, entry.index)
					m.mu.Unlock()
					continue
				}
				if entry.wire == nil {
					entry.wire = entry.tx.Origin.WireBytes()
					newBytes := entry.tx.Origin.RetainedBytes() + int64(proto.Size(entry.flow)+len(entry.tx.Key)+len(entry.key.Context)+len(entry.key.Incarnation)+len(entry.key.Procedure)+len(entry.wire))
					if newBytes > m.limits.Bytes-(m.bytes-entry.bytes) {
						m.remove(entry)
						m.mu.Unlock()
						continue
					}
					m.bytes += newBytes - entry.bytes
					entry.bytes = newBytes
				}
				entry.inflight = true
				active++
				cutoff := entry.cutoff
				heap.Fix(&m.queue, entry.index)
				m.mu.Unlock()
				go func(e *nativeUDPRetryEntry, cutoff time.Time) {
					defer func() { m.results <- nativeUDPRetryResult{entry: e} }()
					leaseCtx, until, err := m.lease()
					if err != nil || leaseCtx == nil || leaseCtx.Err() != nil || e.ctx.Err() != nil || until == until.Round(0) || !time.Now().Before(until) || !time.Now().Before(cutoff) {
						return
					}
					if cutoff.Before(until) {
						until = cutoff
					}
					writeCtx, cancel := context.WithDeadline(e.ctx, until)
					stop := context.AfterFunc(leaseCtx, cancel)
					defer func() { stop(); cancel() }()
					if writeCtx.Err() == nil && time.Now().Before(until) {
						_ = m.write(writeCtx, e.flow, e.wire, until)
					}
				}(entry, cutoff)
				continue
			}
		}
		var next time.Time
		if len(m.queue) > 0 {
			at := nativeUDPRetryAt(m.queue[0])
			if active >= m.limits.Writers && !now.Before(at) {
				at = time.Time{}
				for _, entry := range m.queue {
					if at.IsZero() || entry.cutoff.Before(at) {
						at = entry.cutoff
					}
				}
			}
			next = at
		}
		if !m.hintCleanupAt.IsZero() {
			at := m.hintCleanupAt.Add(25 * time.Millisecond)
			if next.IsZero() || at.Before(next) {
				next = at
			}
		}
		m.mu.Unlock()
		var timerC <-chan time.Time
		if !next.IsZero() {
			reset(time.Until(next))
			timerC = timer.C
		} else if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		select {
		case <-m.ctx.Done():
		case <-m.wake:
		case <-timerC:
		case result := <-m.results:
			active--
			m.mu.Lock()
			if m.entries[result.entry.key] == result.entry {
				result.entry.inflight = false
				heap.Fix(&m.queue, result.entry.index)
			} else {
				m.bytes -= result.entry.bytes
			}
			m.mu.Unlock()
		}
	}
}
