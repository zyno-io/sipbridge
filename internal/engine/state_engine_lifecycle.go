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
				if err := body.UnmarshalJSON(effect.Payload); err != nil {
					return true, err
				}
				effect.Status = state.EffectNotWritten
				effect.AttemptID = body.Authorization.GetEffect().GetAttemptId()
			}
			r.payload.Effects[index] = effect
			var body stateEngineEffect
			if err := body.UnmarshalJSON(effect.Payload); err != nil {
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
				if effect.Kind == "present" || effect.Kind == "adopt" {
					if tx := c.Transactions[body.Transaction]; tx != nil {
						tx.DeferredPresent = false
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
			if body.UnmarshalJSON(effect.Payload) == nil && body.Context == id {
				setupPending = true
				break
			}
		}
		if !setupPending && (c.Snapshot.Kind == pb.ContextSnapshot_TRANSACTION || c.Dialog == "" && c.Subscription == nil) && !active && len(c.Transactions) > 0 && c.RetireMS == 0 {
			c.Snapshot.Ended = true
			c.RetireMS = r.now + 120000
		}
		if c.RetireMS > 0 && !r.coldTrustedHistory(id, c) {
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
					if err := body.UnmarshalJSON(effect.Payload); err != nil {
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
		if r.engine.autonomousCompletion() && stateTrustedRelayEffect(effect) {
			continue
		}
		knownUnsettled := false
		if effect.Kind == "deliver" && (effect.Status == state.EffectWritten || effect.Status == state.EffectNotWritten) {
			var body stateEngineEffect
			if err := body.UnmarshalJSON(effect.Payload); err != nil {
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
		if stateRetiredMetadataEffect(effect.Kind) || !knownUnsettled && effect.DeadlineMS+120000 <= r.now && !stateHistoricalDelivery(effect) && !stateHistoricalAuthorization(effect) {
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
		load: func(ctx context.Context, ref state.Ref) (state.Record, error) { return e.store.Load(ctx, ref) },
		wait: func(ctx context.Context, delay time.Duration) bool {
			return waitStateOperationSignal(ctx, delay, signal)
		},
		now:            time.Now,
		waitBeforeRead: waitBeforeRead,
	})
}

func (e *StateEngine) awaitOperationPolling(ctx context.Context, ref state.Ref, contextID, operation string, accepted *pb.OperationResult, respond bool, polling stateOperationPolling) (*pb.OperationResult, error) {
	latest := proto.Clone(accepted).(*pb.OperationResult)
	delay := 10 * time.Millisecond
	if polling.waitBeforeRead {
		delay = 40 * time.Millisecond
	}
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
	expired := func(reason string) *pb.OperationResult {
		result := proto.Clone(latest).(*pb.OperationResult)
		if respond {
			result.Certainty = pb.OperationResult_UNKNOWN
			if result.Error == "" {
				result.Error = reason
			}
		}
		return result
	}
	if polling.waitBeforeRead && !wait(delay) {
		return expired("SIP response delivery deadline elapsed"), nil
	}
	for {
		if ctx.Err() != nil {
			return expired("SIP response delivery deadline elapsed"), nil
		}
		record, err := polling.load(ctx, ref)
		if err != nil {
			if respond {
				return expired("SIP response delivery outcome unavailable"), nil
			}
			return nil, err
		}
		group, err := decodeStateGroup(record)
		if err != nil {
			return nil, err
		}
		c := group.Contexts[contextID]
		if c == nil {
			return &pb.OperationResult{Certainty: pb.OperationResult_UNKNOWN, Error: "native context retired"}, nil
		}
		if c.Operation == operation && c.Result != nil {
			latest = proto.Clone(c.Result).(*pb.OperationResult)
			if latest.Certainty != pb.OperationResult_ACCEPTED || !respond && writtenPublicBye(c) != nil {
				projectWrittenPublicBye(c, latest)
				latest.Snapshot = stateSnapshot(c, e.cfg.Process)
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
			return expired("SIP response delivery deadline elapsed"), nil
		}
		delay = min(delay*2, 40*time.Millisecond)
	}
}
func (r *stateReduction) subscriptionResponse(c *stateContext, tx *stateTransaction, message portable.Message) error {
	if c.Subscription == nil {
		return nil
	}
	parsed, err := message.Parse()
	if err != nil {
		return err
	}
	response := parsed.(*sip.Response)
	if response.CSeq().MethodName == sip.NOTIFY && response.StatusCode >= 200 && (c.Subscription.Final || response.StatusCode == 481) {
		c.Snapshot.Ended = true
		c.RetireMS = r.now + 120000
		r.touch(c)
		r.event(c, pb.DialogEvent_ENDED, nil, c.Operation)
	}
	return nil
}

func (e *StateEngine) scheduleGroup(ref state.Ref, _ bool) {
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
		ctx, cancel := context.WithTimeout(e.ctx, 2*time.Second)
		defer cancel()
		plan, err := e.schedulePlan(ctx, ref)
		if err != nil {
			return
		}
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
			plan, err = e.schedulePlan(ctx, plan.ref)
			if err != nil {
				return
			}
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
			plan, err = e.schedulePlan(ctx, plan.ref)
			if err != nil {
				if changed {
					e.dispatch(plan.ref)
				}
				return
			}
		}
		if ctx.Err() == nil && len(plan.effects) > 0 {
			e.dispatchPrepared(plan.ref, plan.effects)
		}
	}()
}

func (r *stateReduction) cancelUnstartedEffects(c *stateContext) error {
	for index, effect := range r.payload.Effects {
		var body stateEngineEffect
		if err := body.UnmarshalJSON(effect.Payload); err != nil {
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
		parsed, err := sip.ParseMessage(body.Wire)
		if err != nil {
			continue
		}
		request, ok := parsed.(*sip.Request)
		if !ok || !request.IsInvite() || tag(request.To().Params) != "" {
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
	return body.UnmarshalJSON(effect.Payload) == nil && body.Cancelled
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
