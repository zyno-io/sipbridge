package engine

import (
	"strconv"

	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/execution"
	"sipbridge/internal/state"
)

const stateLocalObservationBytes = 1 << 20

type stateOperationIdentity struct{ contextID, operation string }

type stateOperationWaiter struct {
	identity stateOperationIdentity
	signal   chan struct{}
	revision uint64
	outcome  []byte
}

// Only admitted control commands register. The existing work limit bounds these
// transient projections; they never retain the authoritative protocol aggregate.
func (e *StateEngine) watchOperation(ref state.Ref, identity ...stateOperationIdentity) (<-chan struct{}, func()) {
	key := ref.ID + ":" + ref.Incarnation
	signal := make(chan struct{}, 1)
	waiter := &stateOperationWaiter{signal: signal}
	if len(identity) == 1 {
		waiter.identity = identity[0]
	}
	e.lifecycleMu.Lock()
	if e.waiters == nil {
		e.waiters = make(map[string]map[<-chan struct{}]*stateOperationWaiter)
	}
	if e.waiters[key] == nil {
		e.waiters[key] = make(map[<-chan struct{}]*stateOperationWaiter)
	}
	e.waiters[key][signal] = waiter
	e.lifecycleMu.Unlock()
	return signal, func() {
		e.lifecycleMu.Lock()
		delete(e.waiters[key], signal)
		if len(e.waiters[key]) == 0 {
			delete(e.waiters, key)
		}
		e.lifecycleMu.Unlock()
	}
}

func (e *StateEngine) signalOperation(ref state.Ref) {
	key := ref.ID + ":" + ref.Incarnation
	e.lifecycleMu.Lock()
	defer e.lifecycleMu.Unlock()
	for _, waiter := range e.waiters[key] {
		select {
		case waiter.signal <- struct{}{}:
		default:
		}
	}
}

// Build projections only for local commands currently waiting. Registration can
// disappear after this copy; publication checks it again after definite CAS.
func (r *stateReduction) projectLocalObservations() error {
	r.payload.LocalObservations = nil
	key := r.record.Ref.ID + ":" + r.record.Ref.Incarnation
	r.engine.lifecycleMu.Lock()
	var identities []stateOperationIdentity
	for _, waiter := range r.engine.waiters[key] {
		if waiter.identity.contextID != "" && waiter.identity.operation != "" {
			identities = append(identities, waiter.identity)
		}
	}
	r.engine.lifecycleMu.Unlock()
	for n, identity := range identities {
		duplicate := false
		for _, prior := range identities[:n] {
			duplicate = duplicate || prior == identity
		}
		if duplicate {
			continue
		}
		c := r.group.Contexts[identity.contextID]
		if c == nil || c.Operation != identity.operation || c.Result == nil {
			continue
		}
		bye := writtenPublicBye(c)
		if c.Result.Certainty == pb.OperationResult_ACCEPTED && bye == nil {
			continue
		}
		result := proto.Clone(stateResultForStorage(c.Result)).(*pb.OperationResult)
		if bye != nil {
			// Use the same exact written-public-BYE predicate as inspection,
			// leaving message parsing to the waiter return boundary.
			result.Request = &pb.SipMessage{Wire: bye.Native.Origin.Wire}
		}
		result.Snapshot = stateSnapshot(c, r.engine.cfg.Process)
		if proto.Size(result) > stateLocalObservationBytes {
			continue // Large replies retain the ordinary store-read fallback.
		}
		outcome, err := proto.Marshal(result)
		if err != nil {
			return err
		}
		r.payload.LocalObservations = append(r.payload.LocalObservations, state.LocalObservation{
			ContextID: identity.contextID, OperationID: identity.operation, Outcome: outcome,
		})
	}
	return nil
}

func (e *StateEngine) publishLocalObservations(committed *execution.Committed) {
	if committed == nil || len(committed.LocalObservations) == 0 {
		return
	}
	revision, err := strconv.ParseUint(committed.Revision, 10, 64)
	if err != nil || revision == 0 {
		return
	}
	key := committed.Ref.ID + ":" + committed.Ref.Incarnation
	e.lifecycleMu.Lock()
	defer e.lifecycleMu.Unlock()
	for _, observation := range committed.LocalObservations {
		if len(observation.Outcome) == 0 || len(observation.Outcome) > stateLocalObservationBytes {
			continue
		}
		for _, waiter := range e.waiters[key] {
			if waiter.identity.contextID != observation.ContextID || waiter.identity.operation != observation.OperationID || revision < waiter.revision {
				continue
			}
			waiter.revision, waiter.outcome = revision, observation.Outcome
			select {
			case waiter.signal <- struct{}{}:
			default:
			}
		}
	}
}

func (e *StateEngine) observeLocalOperation(ref state.Ref, contextID, operation string, signal <-chan struct{}, after string) (*pb.OperationResult, error) {
	key := ref.ID + ":" + ref.Incarnation
	e.lifecycleMu.Lock()
	waiter := e.waiters[key][signal]
	if waiter == nil || waiter.identity != (stateOperationIdentity{contextID, operation}) {
		e.lifecycleMu.Unlock()
		return nil, nil
	}
	if revision, err := strconv.ParseUint(after, 10, 64); err == nil && revision > waiter.revision {
		waiter.revision, waiter.outcome = revision, nil
	}
	outcome := waiter.outcome
	waiter.outcome = nil
	e.lifecycleMu.Unlock()
	if len(outcome) == 0 {
		return nil, nil
	}
	result := new(pb.OperationResult)
	if proto.Unmarshal(outcome, result) != nil || result.Snapshot.GetContext().GetId() != contextID ||
		result.Snapshot.GetContext().GetState().GetGroupId() != ref.ID || result.Snapshot.GetContext().GetState().GetIncarnation() != ref.Incarnation {
		return nil, state.ErrCorrupt
	}
	return result, nil
}
