package engine

import (
	"context"
	"time"
)

const outboundUnusedGrace = 2 * time.Minute

func (i *Ingress) readOutboundStream(flow *physicalFlow) {
	defer func() { flow.conn.Close(); i.remove(flow); <-i.slots }()
	i.readStream(context.Background(), flow)
}

// Preserve the original open budget and give a wall-only context deadline the
// entry sample's monotonic anchor. Connect completion cannot renew this bound.
func outboundStreamExpiry(ctx context.Context, entry time.Time) time.Time {
	if deadline, ok := ctx.Deadline(); ok {
		return entry.Add(deadline.Sub(entry) + outboundUnusedGrace)
	}
	return entry.Add(outboundUnusedGrace)
}

func (f *physicalFlow) startUnusedLease(until time.Time) {
	f.write.Lock()
	defer f.write.Unlock()
	f.unusedUntil = until
	f.unusedTimer = time.AfterFunc(time.Until(until), func() {
		f.write.Lock()
		defer f.write.Unlock()
		f.expireUnused(time.Now())
	})
}

// All methods below run under write. No expiry can close a syscall admitted by
// the same lock; potential ingress responsibility is recorded before routing.
func (f *physicalFlow) admitSIPUse() bool {
	if f.poisoned || f.closed.Load() || f.expireUnused(time.Now()) {
		return false
	}
	if !f.unusedUntil.IsZero() && !f.sipUsed {
		f.sipUsed = true
		f.stopUnusedTimer()
	}
	return true
}

func (f *physicalFlow) expireUnused(now time.Time) bool {
	if f.unusedUntil.IsZero() || f.sipUsed || now.Before(f.unusedUntil) {
		return false
	}
	f.closed.Store(true)
	_ = f.conn.Close()
	return true
}

func (f *physicalFlow) stopUnusedTimer() {
	if f.unusedTimer != nil {
		f.unusedTimer.Stop()
		f.unusedTimer = nil
	}
}
