package engine

import "sync/atomic"

type operationObservationCounters struct {
	local atomic.Uint64
	store atomic.Uint64
}

// OperationObservationStatistics reports consumed local results and fallback
// store-read attempts. These fixed counters retain no command identity or state;
// repeated observations of one command are separate attempts.
func (e *StateEngine) OperationObservationStatistics() (local, store uint64) {
	return e.operationObservationMetrics.local.Load(), e.operationObservationMetrics.store.Load()
}
