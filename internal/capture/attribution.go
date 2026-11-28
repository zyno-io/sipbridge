package capture

import (
	"bytes"
	"context"
	"slices"
	"time"
)

type Attribution struct {
	NativeCallID     string
	BLFDeliveryID    string
	BLFNativeCallIDs []string
}

type observationKey struct{}

type pendingObservation struct {
	exporter *exporter
	item     queued
	hold     string
	timer    *time.Timer
	finished bool
}

// Begin copies a physical observation into the bounded queue budget. Finish
// releases it after native routing, unless the initial INVITE awaits admission.
func (e *Exporter) Begin(ctx context.Context, o Observation) (context.Context, func()) {
	if e == nil || e.exporter == nil {
		return ctx, func() {}
	}
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.closed || len(o.Wire) == 0 || len(o.Wire) > maxWire {
		e.dropped.Add(1)
		return ctx, func() {}
	}
	select {
	case e.slots <- struct{}{}:
	default:
		e.dropped.Add(1)
		return ctx, func() {}
	}
	o.Wire = bytes.Clone(o.Wire)
	o.BLFNativeCallIDs = slices.Clone(o.BLFNativeCallIDs)
	pending := &pendingObservation{exporter: e.exporter, item: queued{Observation: o, Sequence: e.sequence.Add(1)}}
	return context.WithValue(ctx, observationKey{}, pending), pending.finish
}

func (p *pendingObservation) finish() {
	p.exporter.mu.Lock()
	defer p.exporter.mu.Unlock()
	if p.hold == "" {
		p.finishLocked("")
	}
}

func (p *pendingObservation) finishLocked(nativeCallID string) {
	if p.finished {
		return
	}
	p.finished = true
	if p.timer != nil {
		p.timer.Stop()
	}
	if p.hold != "" {
		group := p.exporter.pending[p.hold]
		delete(group, p)
		if len(group) == 0 {
			delete(p.exporter.pending, p.hold)
		}
	}
	if nativeCallID != "" {
		p.item.NativeCallID = nativeCallID
	}
	if p.exporter.closed {
		p.exporter.dropped.Add(1)
		<-p.exporter.slots
		return
	}
	p.exporter.queue <- p.item
}

func Attribute(ctx context.Context, attribution Attribution) {
	if attribution.NativeCallID == "" && attribution.BLFDeliveryID == "" {
		return
	}
	p, ok := ctx.Value(observationKey{}).(*pendingObservation)
	if !ok {
		return
	}
	p.exporter.mu.Lock()
	defer p.exporter.mu.Unlock()
	if !p.finished {
		attribution.BLFNativeCallIDs = slices.Clone(attribution.BLFNativeCallIDs)
		p.item.Attribution = attribution
	}
}

func AttributionFrom(ctx context.Context) Attribution {
	p, ok := ctx.Value(observationKey{}).(*pendingObservation)
	if !ok {
		return Attribution{}
	}
	p.exporter.mu.RLock()
	defer p.exporter.mu.RUnlock()
	return p.item.Attribution
}

func Finish(ctx context.Context) {
	if p, ok := ctx.Value(observationKey{}).(*pendingObservation); ok {
		p.finish()
	}
}

// Hold lets a locally committed decision release the packet immediately.
// An exact, read-only lookup on expiry also handles decisions committed by a
// different worker. Its budget is independent of the SIP decision.
func Hold(ctx context.Context, key string, until time.Time, resolve func(context.Context) string) {
	p, ok := ctx.Value(observationKey{}).(*pendingObservation)
	if !ok {
		return
	}
	p.exporter.mu.Lock()
	defer p.exporter.mu.Unlock()
	if p.finished || p.hold != "" || p.exporter.closed || time.Until(until) <= 0 {
		return
	}
	p.hold = key
	if p.exporter.pending[key] == nil {
		p.exporter.pending[key] = make(map[*pendingObservation]struct{})
	}
	p.exporter.pending[key][p] = struct{}{}
	p.timer = time.AfterFunc(time.Until(until), func() {
		p.exporter.mu.RLock()
		finished := p.finished
		p.exporter.mu.RUnlock()
		if finished {
			return
		}
		var nativeCallID string
		if resolve != nil {
			lookupCtx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
			nativeCallID = resolve(lookupCtx)
			cancel()
		}
		p.exporter.mu.Lock()
		defer p.exporter.mu.Unlock()
		p.finishLocked(nativeCallID)
	})
}

func (e *Exporter) Bind(key, nativeCallID string) {
	if e == nil || e.exporter == nil {
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	for p := range e.pending[key] {
		p.finishLocked(nativeCallID)
	}
}
