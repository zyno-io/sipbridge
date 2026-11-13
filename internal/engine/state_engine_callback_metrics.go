package engine

import "sync/atomic"

type callbackSource uint8

const (
	callbackFresh callbackSource = iota
	callbackIndex
	callbackContinuation
	callbackSourceCount
)

const (
	callbackAttempt = iota
	callbackResponse
	callbackError
	callbackResultCommit
	callbackResultNoProjection
	callbackResultError
)

var callbackSourceNames = [...]string{"fresh", "index", "continuation"}
var callbackKindNames = [...]string{"present", "event"}
var callbackEventNames = [...]string{"attempt", "response", "error", "result_commit", "result_no_projection", "result_error"}
var physicalLocalityNames = [...]string{"local", "remote", "unknown"}

type callbackStatistics struct {
	callbacks [len(callbackKindNames)][callbackSourceCount][len(callbackEventNames)]atomic.Uint64
	physical  [callbackSourceCount][len(physicalLocalityNames)]atomic.Uint64
}

type CallbackStatistic struct {
	Kind, Source, Event string
	Count               uint64
}

type PhysicalLocalityStatistic struct {
	Source, Locality string
	Count            uint64
}

func (s *callbackStatistics) callback(kind string, source callbackSource, event int) {
	if source >= callbackSourceCount || event < 0 || event >= len(callbackEventNames) {
		return
	}
	for i, name := range callbackKindNames {
		if kind == name {
			s.callbacks[i][source][event].Add(1)
			return
		}
	}
}

func (e *StateEngine) CallbackStatistics() ([]CallbackStatistic, []PhysicalLocalityStatistic) {
	return e.callbackMetrics.snapshot()
}

func (s *callbackStatistics) snapshot() ([]CallbackStatistic, []PhysicalLocalityStatistic) {
	var callbacks []CallbackStatistic
	var physical []PhysicalLocalityStatistic
	for kind, name := range callbackKindNames {
		for source, sourceName := range callbackSourceNames {
			for event, eventName := range callbackEventNames {
				if count := s.callbacks[kind][source][event].Load(); count != 0 {
					callbacks = append(callbacks, CallbackStatistic{Kind: name, Source: sourceName, Event: eventName, Count: count})
				}
			}
		}
	}
	for source, sourceName := range callbackSourceNames {
		for locality, name := range physicalLocalityNames {
			if count := s.physical[source][locality].Load(); count != 0 {
				physical = append(physical, PhysicalLocalityStatistic{Source: sourceName, Locality: name, Count: count})
			}
		}
	}
	return callbacks, physical
}
