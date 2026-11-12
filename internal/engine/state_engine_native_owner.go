package engine

import (
	"errors"
	"fmt"
	"sort"
	"strconv"

	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/state"
)

func (r *stateReduction) nativeLease(application *pb.ProcessRef) (state.ApplicationLease, error) {
	id := r.engine.applicationLeaseID(application)
	lease, exists := r.record.ApplicationLeases[id]
	if !exists || lease.ID != id || lease.CheckedAtMS != r.now {
		return state.ApplicationLease{}, errors.New("native application lease observation unavailable")
	}
	return lease, nil
}

func (r *stateReduction) guardNativeLease(application *pb.ProcessRef, term string, generation ...uint64) (state.ApplicationLease, error) {
	lease, err := r.nativeLease(application)
	if err != nil {
		return lease, err
	}
	expected := uint64(0)
	if len(generation) > 0 {
		expected = generation[0]
	}
	if term == "" || lease.Term != term || lease.ExpiresAtMS <= r.now || lease.Generation != expected || lease.Sealed {
		return lease, fmt.Errorf("%w: native application lease expired or superseded", errStateApplicationOwner)
	}
	guard := state.ApplicationLeaseRef{ID: lease.ID, Term: term, Generation: expected}
	for _, existing := range r.payload.CommitLeaseGuards {
		if existing == guard {
			return lease, nil
		}
	}
	r.payload.CommitLeaseGuards = append(r.payload.CommitLeaseGuards, guard)
	return lease, nil
}

func (r *stateReduction) pinNativeOwner(c *stateContext) error {
	if !r.engine.autonomousCompletion() {
		return nil
	}
	if _, err := r.guardNativeLease(c.Snapshot.Application, r.in.ApplicationLeaseTerm, r.in.ApplicationGeneration); err != nil {
		return err
	}
	c.Snapshot.ApplicationLeaseTerm = r.in.ApplicationLeaseTerm
	c.Snapshot.AttachmentGeneration = r.in.ApplicationGeneration
	c.OwnerValidated = true
	return nil
}

func (r *stateReduction) requireNativeOwner(c *stateContext, session bool) (state.ApplicationLease, error) {
	if c.OwnerLost || session && r.in.ApplicationLeaseTerm != c.Snapshot.ApplicationLeaseTerm {
		return state.ApplicationLease{}, fmt.Errorf("%w: original native application term fenced", errStateApplicationOwner)
	}
	generation := c.Snapshot.AttachmentGeneration
	if session {
		generation = r.in.ApplicationGeneration
	}
	return r.guardNativeLease(c.Snapshot.Application, c.Snapshot.ApplicationLeaseTerm, generation)
}

// Called only against a fresh validated mutation read. A no-op acknowledges
// scheduling progress, never application ownership or permission to write.
func (e *StateEngine) nativeOwnerCheckNeeded(group stateGroup, record state.Record, now int64) bool {
	for _, c := range group.Contexts {
		if c.Rejection || c.OwnerLost {
			continue
		}
		lease, exists := record.ApplicationLeases[e.applicationLeaseID(c.Snapshot.Application)]
		if !exists || lease.CheckedAtMS != now || c.Snapshot.ApplicationLeaseTerm == "" || lease.Term != c.Snapshot.ApplicationLeaseTerm {
			return true
		}
		if lease.Recoverable && lease.Generation != c.Snapshot.AttachmentGeneration && lease.ExpiresAtMS > now && lease.AttachmentUntilMS == 0 {
			return true
		}
		if lease.ExpiresAtMS <= now && lease.RecoveryUntilMS <= now {
			return true
		}
	}
	return false
}

// A negative observation for a pinned term remains true after a later handshake
// acquires a successor. It can therefore be committed without a live-term guard.
func (r *stateReduction) expireNativeOwners() error {
	if !r.engine.autonomousCompletion() {
		return nil
	}
	for _, id := range stateOwnerContextIDs(r.group) {
		c := r.group.Contexts[id]
		if c.Rejection || c.OwnerLost {
			continue
		}
		if c.Snapshot.ApplicationLeaseTerm == "" {
			return errors.New("retained native context has no pinned application term")
		}
		lease, err := r.nativeLease(c.Snapshot.Application)
		if err != nil {
			return err
		}
		if lease.Recoverable && lease.Term == c.Snapshot.ApplicationLeaseTerm && lease.Generation != c.Snapshot.AttachmentGeneration && lease.ExpiresAtMS > r.now && lease.AttachmentUntilMS == 0 {
			// Ownership is transferred once in the adapter. Contexts follow lazily,
			// retaining original SIP identity and all physical attempt evidence.
			if c.Snapshot.Answered && c.Snapshot.InitialAck && !c.Snapshot.Ended && c.Subscription == nil {
				if _, err := r.guardNativeLease(c.Snapshot.Application, c.Snapshot.ApplicationLeaseTerm, lease.Generation); err != nil {
					return err
				}
				if err := r.abandonOwnerWork(c, c.Snapshot.Application); err != nil {
					return err
				}
				c.Snapshot.AttachmentGeneration = lease.Generation
				r.touch(c)
			} else {
				if err := r.loseApplicationOwner(c); err != nil {
					return err
				}
				continue
			}
		}
		if lease.Term == c.Snapshot.ApplicationLeaseTerm && lease.ExpiresAtMS <= r.now && lease.RecoveryUntilMS > r.now {
			// Another worker may observe expiry before the worker holding this
			// owner's healthy control session. Give it the shared fixed window.
			continue
		}
		if lease.Term != c.Snapshot.ApplicationLeaseTerm || lease.ExpiresAtMS <= r.now {
			r.payload.CommitLeaseFences = append(r.payload.CommitLeaseFences, state.ApplicationLeaseRef{ID: lease.ID, Term: c.Snapshot.ApplicationLeaseTerm, Generation: lease.Generation})
			if err := r.loseApplicationOwner(c); err != nil {
				return err
			}
		}
	}
	return nil
}

func (r *stateReduction) nativeOwnerTimer() (bool, error) {
	for _, id := range stateOwnerContextIDs(r.group) {
		c := r.group.Contexts[id]
		if r.in.Timer != r.timerID(id, "owner", "probe", "") {
			continue
		}
		due := r.ownerDue(c)
		if due == 0 || r.in.Generation != strconv.FormatInt(due, 10) || r.now < due {
			return true, nil
		}
		if c.OwnerLost && c.Subscription == nil {
			return true, r.finishOwnerCleanup(c)
		}
		next, err := r.nativeOwnerProbeDeadline(c)
		if err != nil {
			return true, err
		}
		c.OwnerProbeAtMS = next
		return true, nil
	}
	return false, nil
}

// A live owner cannot expire before its observed native lease deadline. Rechecking
// every context on a shorter fixed interval only rewrites unchanged protocol state.
func (r *stateReduction) nativeOwnerProbeDeadline(c *stateContext) (int64, error) {
	lease, err := r.nativeLease(c.Snapshot.Application)
	if err != nil {
		return 0, err
	}
	if lease.Term != c.Snapshot.ApplicationLeaseTerm || lease.Sealed {
		return 0, errStateApplicationOwner
	}
	if lease.ExpiresAtMS > r.now {
		return lease.ExpiresAtMS, nil
	}
	if lease.RecoveryUntilMS > r.now {
		return lease.RecoveryUntilMS, nil
	}
	return 0, errStateApplicationOwner
}

func (r *stateReduction) appendNativeOwnerTimers() error {
	if err := r.expireOwnerComputations(); err != nil {
		return err
	}
	pending, err := r.ownerPendingWork()
	if err != nil {
		return err
	}
	for _, id := range stateOwnerContextIDs(r.group) {
		c := r.group.Contexts[id]
		if c.Rejection {
			continue
		}
		if c.OwnerLost {
			if err := r.continueOwnerLoss(c); err != nil {
				return err
			}
		}
		if c.OwnerLost && c.Subscription != nil || !stateOwnerProbeNeeded(c, pending) && c.OwnerCleanupAtMS == 0 {
			continue
		}
		if !c.OwnerLost {
			// Application health has one shared lease deadline. Healthy contexts
			// need no periodic protocol transition to rewrite a probe timestamp.
			c.OwnerProbeAtMS = 0
			continue
		} else if c.OwnerProbeAtMS == 0 {
			c.OwnerProbeAtMS = r.now + stateOwnerProbeInterval
		}
		due := r.ownerDue(c)
		r.payload.Timers = append(r.payload.Timers, state.Timer{ID: r.timerID(id, "owner", "probe", ""), Generation: strconv.FormatInt(due, 10), DueMS: max(due, r.now+1), DeadlineMS: max(due, r.now+1)})
	}
	return nil
}

func (r *stateReduction) projectNativeLeases() {
	ids := make(map[string]bool)
	r.payload.ApplicationContexts = nil
	for _, c := range r.group.Contexts {
		if !c.Rejection {
			id := r.engine.applicationLeaseID(c.Snapshot.Application)
			ids[id] = true
			r.payload.ApplicationContexts = append(r.payload.ApplicationContexts, state.ApplicationContextIndex{LeaseID: id, LeaseTerm: c.Snapshot.ApplicationLeaseTerm, ContextID: c.Snapshot.Context.Id})
		}
	}
	for _, effect := range r.payload.Effects {
		if effect.ApplicationLease != nil {
			ids[effect.ApplicationLease.ID] = true
		}
	}
	r.payload.ApplicationLeaseIDs = make([]string, 0, len(ids))
	for id := range ids {
		r.payload.ApplicationLeaseIDs = append(r.payload.ApplicationLeaseIDs, id)
	}
	sort.Strings(r.payload.ApplicationLeaseIDs)
}

// The directory barrier gates business work, never already-retained native SIP
// completion, native deadlines or a physical write's original outcome evidence.
func (r *stateReduction) autonomousProtocolInput() bool {
	if !r.engine.autonomousCompletion() {
		return false
	}
	switch r.in.Kind {
	case "timer", "complete", "native-ingest", "owner-check":
		return true
	case "ingest":
		in := r.in.Ingest
		if in == nil || in.Flow == nil {
			return false
		}
		message := portable.Message{Wire: in.Wire, Transport: in.Flow.Transport}
		parsed, err := message.Parse()
		if err != nil {
			return false
		}
		if _, response := parsed.(*sip.Response); response {
			return true
		}
		request := parsed.(*sip.Request)
		if request.IsAck() || request.IsCancel() || request.Method == sip.BYE {
			return true
		}
		c := r.group.Contexts[in.GetContext().GetId()]
		return c != nil && c.OwnerLost && r.validateCompletionRelay(c, message, in) == nil
	case "reserve", "effect-start", "effect-result":
		_, effect, body, err := r.findEffect(r.in.Effect)
		if err != nil {
			return false
		}
		if effect.Kind == "deliver" {
			return r.in.Kind == "effect-result" || body.Authorization.GetAuthorityClass() != pb.EffectAuthorization_APPLICATION
		}
		return effect.Kind == "wire" && body.Authorization.GetAuthorityClass() != pb.EffectAuthorization_APPLICATION
	}
	return false
}

func (r *stateReduction) effectNativeOwner(body stateEngineEffect) (*stateContext, error) {
	c := r.group.Contexts[body.Context]
	if c == nil || !sameProcess(c.Snapshot.Application, body.Application) {
		return nil, errStateApplicationOwner
	}
	if _, err := r.requireNativeOwner(c, false); err != nil {
		return nil, err
	}
	return c, nil
}
