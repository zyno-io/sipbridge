package engine

import (
	"context"
	"errors"
	"sync/atomic"

	"github.com/emiago/sipgo/sip/portable"
	"sipbridge/internal/state"
)

const (
	schedulerLeaseFailure = iota
	schedulerHeadFailure
	schedulerWatchFailure
	schedulerHintFailure
	schedulerRepairFailure
	schedulerGroupFailure
	schedulerDecodeFailure
	schedulerTimerFailure
	schedulerFailureStages
)

var schedulerStageNames = [schedulerFailureStages]string{"lease", "head", "watch", "hints", "repair", "group", "decode", "timer"}
var schedulerClassNames = [...]string{"timeout", "canceled", "capacity", "corrupt", "conflict", "authority", "stale", "busy", "owner", "sip", "other"}

type schedulerFailureCounters [schedulerFailureStages][len(schedulerClassNames)]atomic.Uint64

type SchedulerFailure struct {
	Stage, Class string
	Count        uint64
}

// Fixed classifications only: errors can contain SIP/account data and must
// never become metric labels or ordinary scheduler log messages.
func (s *schedulerStatistics) fail(stage int, err error) {
	if err == nil {
		return
	}
	kind := len(schedulerClassNames) - 1
	checks := [...]error{context.DeadlineExceeded, context.Canceled, state.ErrCapacity, state.ErrCorrupt,
		state.ErrConflict, state.ErrAuthority, state.ErrStale, state.ErrBusy, errStateApplicationOwner}
	for index, target := range checks {
		if errors.Is(err, target) {
			kind = index
			break
		}
	}
	var protocol *portable.ProtocolError
	if kind == len(schedulerClassNames)-1 && errors.As(err, &protocol) {
		kind = len(schedulerClassNames) - 2
	}
	s.failures[stage][kind].Add(1)
}

func (s *schedulerStatistics) failureSnapshot() []SchedulerFailure {
	var failures []SchedulerFailure
	for stage, name := range schedulerStageNames {
		for class, label := range schedulerClassNames {
			if count := s.failures[stage][class].Load(); count != 0 {
				failures = append(failures, SchedulerFailure{Stage: name, Class: label, Count: count})
			}
		}
	}
	return failures
}
