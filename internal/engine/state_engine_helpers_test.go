package engine

import (
	"context"
	"time"

	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/state"
)

// Test adapters retain synchronous entry points without expanding the service API.
func (e *StateEngine) tickGroup(ctx context.Context, ref state.Ref) {
	plan, err := e.schedulePlan(ctx, ref)
	if err != nil {
		return
	}
	if plan.retire {
		_ = e.store.Retire(ctx, plan.ref)
		return
	}
	plan.effects = nil
	if e.applyScheduledTimers(ctx, plan.ref, plan.timers) {
		e.dispatch(plan.ref)
	}
}

func waitStateOperationPoll(ctx context.Context, delay time.Duration) bool {
	return waitStateOperationSignal(ctx, delay, nil)
}

func (e *StateEngine) awaitOperation(ctx context.Context, ref state.Ref, contextID, operation string, accepted *pb.OperationResult, respond bool) (*pb.OperationResult, error) {
	signal, stop := e.watchOperation(ref, stateOperationIdentity{contextID, operation})
	defer stop()
	return e.awaitOperationSignal(ctx, ref, contextID, operation, accepted, respond, signal, false)
}

func stateSlotBound() (int, int) {
	// 32 regular procedures + one paired CANCEL each + local and remote BYE
	// for sixteen branches =96 native procedures, rounded to128 in the table.
	effects := (2*stateProfileTransactions+2*stateForkDialogs)*stateTransactionEmissions + stateForkDialogs*(2*stateProfileTransmissions+2) + stateProfileContexts*16
	round := func(value int) int {
		n := 1
		for n < value {
			n *= 2
		}
		return n
	}
	effects = round(effects)
	// One group recovery sweep replaces one timer per physical history entry.
	timers := round(max(effects, 4*stateNativeProcedures+2*stateForkDialogs+5*stateProfileContexts+1))
	return effects, timers
}
