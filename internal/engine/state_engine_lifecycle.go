package engine

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/state"
)

func (r *stateReduction) transactionCapacity() error {
	// Future paired/terminal procedures are accounted by capacityFits before
	// fresh work commits; a reserved native completion must not use this gate.
	limits := r.engine.store.Limits()
	count := 0
	for _, c := range r.group.Contexts {
		for _, tx := range c.Transactions {
			if tx.Native.Phase != portable.Terminated {
				count++
			}
		}
	}
	if count >= limits.Transactions {
		return &portable.ProtocolError{Status: 503, Reason: "native transaction capacity"}
	}
	return nil
}

func (r *stateReduction) lifecycleTimer() (bool, error) {
	if r.in.Timer == r.timerID("group", "admission", "expiry", "") {
		return true, nil // finish prunes expired reservations against Redis time.
	}
	for id, c := range r.group.Contexts {
		if r.in.Timer == r.timerID(id, "context", "retire", "") {
			return true, nil
		}
		sub := c.Subscription
		if sub != nil && !c.Snapshot.Ended && r.in.Timer == r.timerID(id, "subscription", "expiry", "") {
			expected := sub.ExpiresMS
			if sub.Terminating {
				expected = sub.TerminalMS
			}
			if r.in.Generation != strconv.FormatInt(expected, 10) || r.now < expected {
				return true, nil
			}
			if sub.Terminating && sub.TerminalMS > 0 && sub.TerminalMS <= r.now {
				c.Snapshot.Ended = true
				c.RetireMS = r.now + 120000
				if err := r.endSubscriptionRequests(c); err != nil {
					return true, err
				}
				r.touch(c)
				r.event(c, pb.DialogEvent_ENDED, nil, c.Operation)
			} else if !sub.Terminating && sub.ExpiresMS <= r.now {
				sub.Terminating = true
				sub.TerminalMS = r.now + 5000
				r.touch(c)
				r.event(c, pb.DialogEvent_SUBSCRIPTION_EXPIRED, nil, "")
			}
			return true, nil
		}
	}
	if r.in.Timer != r.timerID("group", "effects", "recovery", "") {
		return false, nil
	}
	for index, effect := range r.payload.Effects {
		expires := effect.DeadlineMS
		if effect.Kind == "credential" || effect.Kind == "event" {
			expires += 120000
		}
		if effect.Kind != "owner" && (effect.Status == state.EffectPending || effect.Status == state.EffectAuthorized) && expires <= r.now {
			effect.Status = state.EffectCancelled
			if effect.Kind == "deliver" {
				var body stateEngineEffect
				if err := r.unmarshalEffectMetadata(&body, effect); err != nil {
					return true, err
				}
				effect.Status = state.EffectNotWritten
				effect.AttemptID = body.Authorization.GetEffect().GetAttemptId()
			}
			r.payload.Effects[index] = effect
			var body stateEngineEffect
			if err := r.unmarshalEffectMetadata(&body, effect); err != nil {
				return true, err
			}
			c := r.group.Contexts[body.Context]
			if c != nil {
				if effect.Kind == "wire" || effect.Kind == "deliver" {
					r.releaseNoStartServerFinal(c, effect, body)
				}
				if err := r.releaseExpiredACKCleanup(c, body, effect.DeadlineMS); err != nil {
					return true, err
				}
				if c.Operation == body.Command && c.Result != nil && c.Result.Certainty == pb.OperationResult_ACCEPTED {
					if stateServerTransaction(c.Transactions[body.Transaction]) {
						// Publication/policy expiry may select a 503 below. Neither
						// that selection nor an unstarted effect proves delivery.
						r.responseCommandNoStart(c, effect, body)
					} else {
						c.Result.Certainty = pb.OperationResult_COMPLETED
						c.Result.Error = "SIP effect start deadline elapsed"
					}
				}
				if effect.Kind == "open" || effect.Kind == "credential" {
					c.Snapshot.Ended = true
					c.RetireMS = r.now + 120000
					r.touch(c)
					r.event(c, pb.DialogEvent_FAILED, nil, c.Operation)
				}
				if effect.Kind == "adopt" {
					if tx := c.Transactions[body.Transaction]; tx != nil {
						tx.DeferredPresent = false
						tx.PresentDeadlineMS = 0
						tx.PendingResponse = nil
						if c.Subscription != nil {
							c.Subscription.Pending = ""
						}
						if c.Operation == tx.Command && c.Result != nil && c.Result.Certainty == pb.OperationResult_ACCEPTED {
							c.Result.SipStatus = 503
							c.Result.Error = "context publication unavailable"
							tx.RespondStatus = 503
						}
						response, err := r.buildResponse(c, tx.Native.Origin, 503, "Context publication unavailable", nil, nil)
						if err != nil {
							return true, err
						}
						if err := r.stepTransaction(c, tx, portable.Input{Kind: portable.SendResponse, Now: r.now, Message: response}); err != nil {
							return true, err
						}
					}
				}
			}
		}
	}
	return true, r.recoverServerResponses()
}
func (r *stateReduction) appendLifecycleTimers() error {
	r.pruneRetired()
	var admissionDue int64
	for _, reservation := range r.group.Allocations {
		if admissionDue == 0 || reservation.ExpiresMS < admissionDue {
			admissionDue = reservation.ExpiresMS
		}
	}
	if admissionDue > 0 {
		r.payload.Timers = append(r.payload.Timers, state.Timer{ID: r.timerID("group", "admission", "expiry", ""), Generation: strconv.FormatInt(admissionDue, 10), DueMS: admissionDue, DeadlineMS: admissionDue})
	}
	var coldNative map[string]bool
	for id, c := range r.group.Contexts {
		active := false
		for _, tx := range c.Transactions {
			if tx.Native.Phase != portable.Terminated {
				active = true
				break
			}
		}
		setupPending := false
		for _, effect := range r.payload.Effects {
			if effect.Kind != "credential" && effect.Kind != "open" {
				continue
			}
			if effect.Status == state.EffectWritten || effect.Status == state.EffectNotWritten || effect.Status == state.EffectCancelled {
				continue
			}
			var body stateEngineEffect
			if r.unmarshalEffectMetadata(&body, effect) == nil && body.Context == id {
				setupPending = true
				break
			}
		}
		if !setupPending && (c.Snapshot.Kind == pb.ContextSnapshot_TRANSACTION || c.Dialog == "" && c.Subscription == nil) && !active && len(c.Transactions) > 0 && c.RetireMS == 0 {
			c.Snapshot.Ended = true
			c.RetireMS = r.now + 120000
		}
		if r.coldNativeHistory(id, c) {
			if coldNative == nil {
				coldNative = make(map[string]bool)
			}
			coldNative[id] = true
		}
		if c.RetireMS > 0 && !coldNative[id] && !r.coldTrustedHistory(id, c) {
			replayUntil := max(c.ReplayUntilMS, c.RetireMS)
			markerCapacity := replayUntil <= r.now || len(r.group.Retired) < stateRetiredMarkerLimit || r.group.Retired[id] > r.now
			if c.RetireMS <= r.now && !active && markerCapacity && r.contextRetirementProven(id, c) {
				for selector, context := range r.group.Selectors {
					if context == id {
						delete(r.group.Selectors, selector)
					}
				}
				filtered := r.payload.Effects[:0]
				for _, effect := range r.payload.Effects {
					var body stateEngineEffect
					if err := r.unmarshalEffectMetadata(&body, effect); err != nil {
						return err
					}
					if body.Context != id {
						filtered = append(filtered, effect)
					}
				}
				r.payload.Effects = filtered
				delete(r.group.Contexts, id)
				if r.group.Retired == nil {
					r.group.Retired = make(map[string]int64)
				}
				if replayUntil > r.now {
					r.group.Retired[id] = replayUntil
				}
				continue
			}
			retry := c.RetireMS
			// Retained request/transaction proof cannot retire before its fixed
			// cutoff. Native timers run independently; polling cannot shorten it.
			for _, tx := range c.Transactions {
				if tx.RetireMS > 0 {
					retry = max(retry, tx.RetireMS, tx.DispositionUntilMS)
				}
			}
			if retry <= r.now {
				retry = r.now + 1000
			}
			r.payload.Timers = append(r.payload.Timers, state.Timer{ID: r.timerID(id, "context", "retire", ""), Generation: strconv.FormatInt(retry, 10), DueMS: retry, DeadlineMS: retry})
		}
		if sub := c.Subscription; sub != nil && !c.Snapshot.Ended {
			at := sub.ExpiresMS
			if sub.Terminating {
				at = sub.TerminalMS
			}
			if at <= 0 {
				return errors.New("invalid subscription timer")
			}
			r.payload.Timers = append(r.payload.Timers, state.Timer{ID: r.timerID(id, "subscription", "expiry", ""), Generation: strconv.FormatInt(at, 10), DueMS: at, DeadlineMS: at})
		}
	}
	recoveryAt := r.serverResponseDue()
	for _, effect := range r.payload.Effects {
		if !stateSupportedEffectKind(effect.Kind) {
			return state.ErrCorrupt
		}
		var body stateEngineEffect
		decoded := false
		decode := func() error {
			if decoded {
				return nil
			}
			if err := r.unmarshalEffectMetadata(&body, effect); err != nil {
				return err
			}
			decoded = true
			return nil
		}
		if r.engine.autonomousCompletion() {
			if effect.Kind == "wire" {
				continue
			}
			if effect.Kind == "deliver" && (decode() != nil || !body.Native) {
				continue
			}
		}
		if len(coldNative) > 0 && stateHistoricalDelivery(effect) {
			if err := decode(); err != nil {
				return err
			}
			if coldNative[body.Context] {
				continue
			}
		}
		knownUnsettled := false
		if effect.Kind == "deliver" && (effect.Status == state.EffectWritten || effect.Status == state.EffectNotWritten) {
			if err := decode(); err != nil {
				return err
			}
			knownUnsettled = !body.Settled
		}
		if !knownUnsettled && effect.Status != state.EffectPending && effect.Status != state.EffectAuthorized && effect.Status != state.EffectDispatching && effect.Status != state.EffectUnknown {
			continue
		}
		at := effect.DeadlineMS
		if knownUnsettled || effect.Status != state.EffectPending || at <= r.now {
			at = r.now + stateEffectRecoveryMS
		}
		if !knownUnsettled && effect.DeadlineMS+120000 <= r.now && !stateHistoricalDelivery(effect) && !r.historicalAuthorization(effect) {
			continue
		}
		if recoveryAt == 0 || at < recoveryAt {
			recoveryAt = at
		}
	}
	if recoveryAt > 0 {
		r.payload.Timers = append(r.payload.Timers, state.Timer{ID: r.timerID("group", "effects", "recovery", ""), Generation: strconv.FormatInt(recoveryAt, 10), DueMS: recoveryAt, DeadlineMS: recoveryAt})
	}
	return r.appendOwnerTimers()
}

type stateOperationPolling struct {
	load           func(context.Context, state.Ref) (state.Record, error)
	observe        func(string) (*pb.OperationResult, error)
	wait           func(context.Context, time.Duration) bool
	now            func() time.Time
	waitBeforeRead bool
}

func waitStateOperationSignal(ctx context.Context, delay time.Duration, signal <-chan struct{}) bool {
	if ctx.Err() != nil {
		return false
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	case <-signal:
		return true
	}
}

func (e *StateEngine) awaitOperationSignal(ctx context.Context, ref state.Ref, contextID, operation string, accepted *pb.OperationResult, respond bool, signal <-chan struct{}, waitBeforeRead bool) (*pb.OperationResult, error) {
	return e.awaitOperationPolling(ctx, ref, contextID, operation, accepted, respond, stateOperationPolling{
		observe: func(after string) (*pb.OperationResult, error) {
			result, err := e.observeLocalOperation(ref, contextID, operation, signal, after)
			if err == nil && result != nil {
				e.operationObservationMetrics.local.Add(1)
			}
			return result, err
		},
		load: func(ctx context.Context, ref state.Ref) (state.Record, error) {
			e.operationObservationMetrics.store.Add(1)
			view, _, err := e.store.ReadState(ctx, ref)
			if err != nil {
				return state.Record{}, err
			}
			return stateViewProtocolRecord(view), nil
		},
		wait: func(ctx context.Context, delay time.Duration) bool {
			return waitStateOperationSignal(ctx, delay, signal)
		},
		now:            time.Now,
		waitBeforeRead: waitBeforeRead,
	})
}

func (e *StateEngine) awaitOperationPolling(ctx context.Context, ref state.Ref, contextID, operation string, accepted *pb.OperationResult, respond bool, polling stateOperationPolling) (*pb.OperationResult, error) {
	latest := proto.Clone(accepted).(*pb.OperationResult)
	var observedRevision string
	delay := 100 * time.Millisecond
	wait := func(pause time.Duration) bool {
		if deadline, ok := ctx.Deadline(); ok {
			remaining := deadline.Sub(polling.now())
			if remaining <= 100*time.Millisecond+pause {
				pause = min(pause, 10*time.Millisecond)
			}
			pause = min(pause, remaining)
		}
		return pause > 0 && polling.wait(ctx, pause)
	}
	expired := func(reason string) (*pb.OperationResult, error) {
		result := proto.Clone(latest).(*pb.OperationResult)
		if respond {
			result.Certainty = pb.OperationResult_UNKNOWN
			if result.Error == "" {
				result.Error = reason
			}
		}
		if err := stateMaterializeResultMessages(result); err != nil {
			return nil, err
		}
		return result, nil
	}
	if polling.waitBeforeRead && !wait(delay) {
		return expired("SIP response delivery deadline elapsed")
	}
	for {
		if ctx.Err() != nil {
			return expired("SIP response delivery deadline elapsed")
		}
		if polling.observe != nil {
			observed, err := polling.observe(observedRevision)
			if err != nil {
				return nil, err
			}
			if observed != nil && (observed.Certainty != pb.OperationResult_ACCEPTED || !respond && observed.Request != nil) {
				if err := stateMaterializeResultMessages(observed); err != nil {
					return nil, err
				}
				if observed.Certainty == pb.OperationResult_ACCEPTED && observed.Request.GetMethod() != "BYE" {
					return nil, state.ErrCorrupt
				}
				return observed, nil
			}
		}
		record, err := polling.load(ctx, ref)
		if err != nil {
			if respond {
				return expired("SIP response delivery outcome unavailable")
			}
			return nil, err
		}
		group, err := decodeStateGroup(record)
		if err != nil {
			return nil, err
		}
		observedRevision = record.Revision
		c := group.Contexts[contextID]
		if c == nil {
			return &pb.OperationResult{Certainty: pb.OperationResult_UNKNOWN, Error: "native context retired"}, nil
		}
		if c.Operation == operation && c.Result != nil {
			latest = proto.Clone(c.Result).(*pb.OperationResult)
			latest.Snapshot = stateSnapshot(c, e.cfg.Process)
			if latest.Certainty != pb.OperationResult_ACCEPTED || !respond && writtenPublicBye(c) != nil {
				projectWrittenPublicBye(c, latest)
				if err := stateMaterializeResultMessages(latest); err != nil {
					return nil, err
				}
				return latest, nil
			}
		}
		// Keep only bounded command-result metadata, never the authoritative
		// protocol aggregate, across this I/O wait.
		record = state.Record{}
		group = stateGroup{}
		c = nil
		// Pace from the completed read, so slow Redis/decode work cannot
		// accumulate ticker events and immediately start another full read.
		if !wait(delay) {
			return expired("SIP response delivery deadline elapsed")
		}
		delay = min(delay*2, 250*time.Millisecond)
	}
}
func (r *stateReduction) subscriptionResponse(c *stateContext, tx *stateTransaction, message portable.Message) error {
	if c.Subscription == nil {
		return nil
	}
	facts, err := message.Facts()
	if err != nil {
		return err
	}
	if !facts.IsRequest && !c.Snapshot.Ended && facts.CSeqMethod == sip.NOTIFY && facts.Status >= 200 && (c.Subscription.Final || facts.Status == 481) {
		c.Snapshot.Ended = true
		c.RetireMS = r.now + 120000
		if err := r.endSubscriptionRequests(c); err != nil {
			return err
		}
		r.touch(c)
		r.event(c, pb.DialogEvent_ENDED, nil, c.Operation)
	}
	return nil
}

func (e *StateEngine) scheduleGroup(ref state.Ref, _ bool) {
	e.scheduleGroupIn(e.ctx, ref)
}

func (e *StateEngine) scheduleGroupIn(parent context.Context, ref state.Ref) {
	key := ref.ID + ":" + ref.Incarnation
	e.lifecycleMu.Lock()
	if e.closed || e.scheduled[key] || len(e.scheduled) >= 16 {
		e.lifecycleMu.Unlock()
		return
	}
	select {
	case e.background <- struct{}{}:
	default:
		e.lifecycleMu.Unlock()
		return
	}
	select {
	case e.work <- struct{}{}:
	default:
		<-e.background
		e.lifecycleMu.Unlock()
		return
	}
	if e.scheduled == nil {
		e.scheduled = make(map[string]bool)
	}
	e.scheduled[key] = true
	e.wg.Add(1)
	e.lifecycleMu.Unlock()
	go func() {
		defer e.wg.Done()
		defer func() {
			<-e.work
			<-e.background
			e.lifecycleMu.Lock()
			delete(e.scheduled, key)
			e.lifecycleMu.Unlock()
		}()
		ctx, cancel := context.WithTimeout(parent, 2*time.Second)
		defer cancel()
		plan, err := e.schedulePlan(ctx, ref)
		if err != nil {
			e.scheduler.fail(schedulerGroupFailure, err)
			return
		}
		e.runSchedulePlan(ctx, plan)
	}()
}

func (r *stateReduction) cancelUnstartedEffects(c *stateContext) error {
	for index, effect := range r.payload.Effects {
		var body stateEngineEffect
		if err := r.unmarshalEffectMetadata(&body, effect); err != nil {
			return err
		}
		if body.Context != c.Snapshot.Context.Id {
			continue
		}
		if effect.Kind == "open" && (effect.Status == state.EffectPending || effect.Status == state.EffectDispatching || effect.Status == state.EffectUnknown) {
			effect.Status = state.EffectCancelled
			r.setEffect(index, effect, body)
			continue
		}
		if effect.Kind != "wire" && effect.Kind != "deliver" || body.Authorization == nil || body.Authorization.AuthorityClass != pb.EffectAuthorization_APPLICATION {
			continue
		}
		_, facts, err := body.observedMessage()
		initialInvite := err == nil && facts.IsRequest && facts.Method == sip.INVITE && facts.ToTag == ""
		if !initialInvite {
			continue
		}
		switch {
		case effect.Kind == "wire" && effect.Status == state.EffectPending:
			body.Cancelled = true
			effect.Status = state.EffectCancelled
		case effect.Kind == "wire" && (effect.Status == state.EffectDispatching || effect.Status == state.EffectUnknown):
			body.Cancelled = true
			effect.Status = state.EffectUnknown
		case effect.Kind == "deliver" && effect.Status == state.EffectAuthorized:
			body.Cancelled = true
			effect.Status = state.EffectNotWritten
			effect.AttemptID = body.Authorization.Effect.AttemptId
		default:
			continue
		}
		r.setEffect(index, effect, body)
	}
	return nil
}

func stateHistoricalAuthorization(effect state.Effect) bool {
	if effect.Kind != "wire" || effect.Status != state.EffectUnknown {
		return false
	}
	var body stateEngineEffect
	return body.unmarshalMetadata(effect.Payload) == nil && body.Cancelled
}

func (r *stateReduction) historicalAuthorization(effect state.Effect) bool {
	if effect.Kind != "wire" || effect.Status != state.EffectUnknown {
		return false
	}
	var body stateEngineEffect
	return r.unmarshalEffectMetadata(&body, effect) == nil && body.Cancelled
}

const stateRetiredMarkerLimit = 128

func (r *stateReduction) pruneRetired() {
	if r.group.Retired == nil {
		r.group.Retired = make(map[string]int64)
		return
	}
	for id, until := range r.group.Retired {
		if until <= r.now {
			delete(r.group.Retired, id)
		}
	}
}

// runSchedulePlan consumes a transient protocol projection; all timer mutations
// still acquire fresh CAS inputs, and no decoded aggregate crosses external I/O.
func (e *StateEngine) runSchedulePlan(ctx context.Context, plan stateSchedulePlan) {
	if plan.pending {
		legacy, ok := e.runtime.(interface {
			Resume(context.Context, state.Ref) (state.Receipt, error)
		})
		if e.autonomousCompletion() || !ok {
			return // Production and CAS fixtures never recover retained claims.
		}
		// A ready effect is not a pending reducer input. Do not issue an
		// IDLE Begin/recovery request for every ordinary effect/timer hint.
		plan.effects, plan.timers = nil, nil
		_, _ = legacy.Resume(ctx, plan.ref)
		next, err := e.schedulePlan(ctx, plan.ref)
		if err != nil {
			return
		}
		plan = next
	}
	if plan.retire {
		_ = e.store.Retire(ctx, plan.ref)
		return
	}
	if len(plan.timers) > 0 {
		// A timer transition may invalidate an older effect or create its
		// native completion. Never dispatch the pre-transition snapshot.
		plan.effects = nil
		changed := e.applyScheduledTimers(ctx, plan.ref, plan.timers)
		plan.timers = nil
		if ctx.Err() != nil {
			if changed {
				e.dispatch(plan.ref)
			}
			return
		}
		next, err := e.schedulePlan(ctx, plan.ref)
		if err != nil {
			if changed {
				e.dispatch(plan.ref)
			}
			return
		}
		plan = next
	}
	if ctx.Err() == nil && len(plan.effects) > 0 {
		e.dispatchPrepared(plan.ref, plan.effects, callbackIndex)
	}
}
