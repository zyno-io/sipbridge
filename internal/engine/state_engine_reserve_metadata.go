package engine

import (
	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/state"
)

// reserveMetadata changes only an already-admitted physical obligation. Every
// failed eligibility/check discards this independently decoded reduction; the
// caller then runs normal reduction against the untouched record.
func (e *StateEngine) reserveMetadata(record state.Record, input state.Input, in stateEngineInput, now int64) (state.Payload, []byte, bool) {
	if !e.usesCAS() || !e.autonomousCompletion() || record.Versions != NativeStateVersions() || record.Pending != nil ||
		!input.Proof || input.Kind != "reserve" || in.Kind != "reserve" || input.DeadlineMS <= now ||
		in.Write == nil || in.Write.Effect == nil || in.Effect != in.Write.Effect.Id || !stateSameContext(in.Context, in.Write.Effect.Context) {
		return state.Payload{}, nil, false
	}
	for _, timer := range record.Payload.Timers {
		if timer.DueMS <= now {
			return state.Payload{}, nil, false
		}
	}
	selected := false
	for _, effect := range record.Payload.Effects {
		// A still-unprepared wire obligation needs normal native preparation.
		if effect.Kind == "wire" {
			return state.Payload{}, nil, false
		}
		if effect.ID == in.Effect {
			selected = effect.Kind == "deliver" && effect.Status == state.EffectAuthorized && effect.AttemptID == "" &&
				effect.DeadlineMS > now && effect.Worker != "" && effect.Worker == e.dispatchWorker(in.Executor)
		}
	}
	if !selected {
		return state.Payload{}, nil, false
	}
	group, err := decodeStateGroup(record)
	if err != nil {
		return state.Payload{}, nil, false
	}
	r := &stateReduction{engine: e, record: record, group: group, payload: record.Payload, in: in, now: now,
		deadline: input.DeadlineMS, fingerprint: input.Fingerprint, result: &pb.OperationResult{Certainty: pb.OperationResult_COMPLETED}}
	// Stronger than the owner-check scheduling hint: no expiration, sealed term,
	// generation adoption or recovery/attachment transition may be deferred.
	for _, c := range group.Contexts {
		if c.Rejection || c.OwnerLost || c.OwnerCleanupAtMS != 0 || c.Snapshot == nil || c.Snapshot.Application == nil {
			return state.Payload{}, nil, false
		}
		lease, err := r.nativeLease(c.Snapshot.Application)
		if err != nil || c.Snapshot.ApplicationLeaseTerm == "" || lease.Term != c.Snapshot.ApplicationLeaseTerm ||
			lease.Generation != c.Snapshot.AttachmentGeneration || lease.Sealed || lease.ExpiresAtMS <= now || lease.AttachmentUntilMS != 0 {
			return state.Payload{}, nil, false
		}
	}
	var drain state.LocalDrainObservation
	index, effect, body, err := r.findEffect(in.Effect, &drain)
	if err != nil || !body.Native || body.Settled || len(body.Wire) == 0 || body.Permit == nil || body.Authorization == nil || group.Contexts[body.Context] == nil {
		return state.Payload{}, nil, false
	}
	r.payload.Effects = append([]state.Effect(nil), record.Payload.Effects...)
	r.payload.CommitLeaseGuards, r.payload.CommitLeaseFences = nil, nil
	r.payload.LocalObservations, r.payload.LocalDrain, r.payload.LocalResult = nil, nil, nil
	r.payload.TimerWorker = e.dispatchWorker(e.cfg.Process)
	// Keep the single reservation authority implementation, including selected
	// policy/owner guards, ACK dependency, exact permit/flow/attempt and deadline.
	if err := r.reserveSelected(index, effect, body, drain); err != nil || r.result.Response != nil || len(r.result.GetRequest().GetWire()) == 0 {
		return state.Payload{}, nil, false
	}
	if err := r.projectDispatchWorkers(); err != nil {
		return state.Payload{}, nil, false
	}
	outcome, err := marshalStateReceiptResult(r.result)
	if err != nil {
		return state.Payload{}, nil, false
	}
	if proto.Size(r.result) <= 1<<20 {
		r.payload.LocalResult = r.result
	}
	return r.payload, outcome, true
}
