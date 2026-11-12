package execution

import "sync/atomic"

const (
	statRead = iota
	statReduce
	statCommit
	statConflict
	statReadOnly
	statReceipt
	statUnknown
	statEvents
)

var statisticEvents = [statEvents]string{"read", "reduce", "commit", "conflict", "readonly", "receipt", "unknown"}
var statisticKinds = [...]string{"locate", "ingest", "native-ingest", "reject", "command", "reserve", "complete", "owner-control", "owner-result", "owner-check", "digest-result", "effect-result", "effect-start", "timer", "other"}

type runtimeStatistics [len(statisticKinds)][statEvents]atomic.Uint64

type Statistic struct {
	Kind, Event string
	Count       uint64
}

func statisticKind(kind string) int {
	for n := 0; n < len(statisticKinds)-1; n++ {
		if statisticKinds[n] == kind {
			return n
		}
	}
	return len(statisticKinds) - 1
}

// Statistics samples fixed process counters. It never reads the store or
// retains an operation ID, payload or per-dialog label.
func (r *CASRuntime) Statistics() []Statistic {
	var result []Statistic
	for kind := range r.statistics {
		for event := range r.statistics[kind] {
			if count := r.statistics[kind][event].Load(); count != 0 {
				result = append(result, Statistic{Kind: statisticKinds[kind], Event: statisticEvents[event], Count: count})
			}
		}
	}
	return result
}
