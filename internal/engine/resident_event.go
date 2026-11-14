package engine

import (
	"context"
	"errors"
	"time"
	"unsafe"

	"github.com/emiago/sipgo/sip/portable"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
)

// A lifecycle event is bounded delivery debt for one already-committed native
// transition. Fresh request presentation never enters this record or scheduler.
type residentEvent struct {
	id        string
	context   *residentContext
	message   *pb.DialogEvent
	native    portable.Message
	selectors []string
	deadline  time.Time
	next      time.Time
	backoff   time.Duration
	bytes     int64
	budget    *residentBudget
	borrowed  int64
	inFlight  bool
}

// Retain wire metadata only; reconstruct the public message at delivery.
func (e *ResidentMemory) stageResidentEventLocked(c *residentContext, kind pb.DialogEvent_Kind, message *pb.SipMessage, operation string, deadline time.Time, budgets ...*residentBudget) (*residentEvent, error) {
	var native portable.Message
	if message != nil && len(message.Wire) != 0 {
		transport := ""
		if c != nil && c.snapshot != nil {
			transport = c.snapshot.Transport
		}
		if transport == "" {
			transport = "tls"
		}
		var err error
		native, err = portable.ParseMessage(message.Wire, transport, "", "")
		if err != nil {
			return nil, err
		}
	}
	return e.stageResidentEventNativeLocked(c, kind, message, native, operation, deadline, budgets...)
}

func (e *ResidentMemory) stageResidentEventNativeLocked(c *residentContext, kind pb.DialogEvent_Kind, message *pb.SipMessage, native portable.Message, operation string, deadline time.Time, budgets ...*residentBudget) (*residentEvent, error) {
	if kind == pb.DialogEvent_UNSPECIFIED || c.snapshot == nil || deadline.IsZero() || !time.Now().Before(deadline) {
		return nil, errors.New("invalid lifecycle event or deadline")
	}
	budget := c.budget
	if c.subscriptionBudget != nil {
		budget = c.subscriptionBudget
	}
	if len(budgets) > 0 {
		budget = budgets[0]
	}
	if kind == pb.DialogEvent_PROVISIONAL && budget != nil && budget.provisional != nil {
		// Transient provisional history is not a recovery guarantee. Keep one
		// outstanding debt per procedure so it cannot crowd out ANSWERED/ENDED.
		// Never replace a record another controller call may still be using.
		return nil, nil
	}
	if budget == nil || !budget.open || budget.events == 0 {
		return nil, errors.New("procedure lifecycle capacity")
	}
	event := &residentEvent{id: "event-" + uuid.NewString(), context: c, deadline: deadline, budget: budget,
		message: &pb.DialogEvent{Kind: kind, Snapshot: proto.Clone(c.snapshot).(*pb.ContextSnapshot), OperationId: operation}}
	if message != nil {
		event.native = native
		event.selectors = append([]string(nil), message.Selectors...)
	}
	event.bytes = int64(unsafe.Sizeof(*event)+unsafe.Sizeof(*event.message)) + residentSnapshotBytes(event.message.Snapshot) + int64(len(operation)) + event.native.RetainedBytes() + 128
	for _, selector := range event.selectors {
		event.bytes += int64(len(selector)) + 16
	}
	var err error
	event.borrowed, err = e.chargeResidentBudgetLocked(c, event.budget, event.bytes)
	if err != nil {
		return nil, err
	}
	budget.events--
	c.reservedEvents--
	if kind == pb.DialogEvent_PROVISIONAL {
		budget.provisional = event
	}
	if c.events == nil {
		c.events = make(map[string]*residentEvent)
	}
	c.events[event.id] = event
	return event, nil
}

func (e *ResidentMemory) dispatchResidentEvents(ctx context.Context, events []*residentEvent) {
	for _, event := range events {
		e.deliverResidentEvent(ctx, event)
	}
}

// A semantic timer publishes its lifecycle debt and returns promptly; bounded
// workers perform controller IO without delaying other native timer deadlines.
func (e *ResidentMemory) scheduleResidentEventsLocked(c *residentContext, events []*residentEvent) {
	now := time.Now()
	e.mu.Lock()
	for _, event := range events {
		if event == nil {
			continue
		}
		if c.events[event.id] != event {
			continue
		}
		event.next = now
		e.deadlines.set(residentDeadlineKey{kind: residentEventDeadline, contextID: c.snapshot.Context.Id, record: event.id}, 1, now)
	}
	e.mu.Unlock()
	e.wakeDeadline()
}

func (e *ResidentMemory) deliverResidentEvent(parent context.Context, event *residentEvent) {
	if event == nil {
		return
	}
	c := event.context
	c.mu.Lock()
	if c.events[event.id] != event || event.inFlight {
		c.mu.Unlock()
		return
	}
	if !time.Now().Before(event.deadline) {
		e.releaseResidentEventLocked(c, event)
		c.mu.Unlock()
		return
	}
	event.inFlight = true
	application := event.message.Snapshot.Application
	deadline := minTime(event.deadline, time.Now().Add(2*time.Second))
	c.mu.Unlock()
	ctx, cancel := context.WithDeadline(parent, deadline)
	accepted := false
	peer, err := e.control.Peer(application)
	public := &pb.DialogEvent{Kind: event.message.Kind, Snapshot: event.message.Snapshot, OperationId: event.message.OperationId, Error: event.message.Error}
	if err == nil && !event.native.IsZero() {
		public.Message, err = projectNativeMessage(event.native, false)
		if err == nil {
			public.Message.Selectors = event.selectors
		}
	}
	if err == nil {
		e.callbackMetrics.callback("event", callbackFresh, callbackAttempt)
		reply, callErr := peer.Call(ctx, &pb.Envelope{CorrelationId: event.id, RemainingBudgetMs: uint32(max(1, time.Until(deadline).Milliseconds())), Payload: &pb.Envelope_DialogEvent{DialogEvent: public}})
		if callErr == nil {
			result := reply.GetResult()
			accepted = result != nil && result.Error == "" && (result.Certainty == pb.OperationResult_ACCEPTED || result.Certainty == pb.OperationResult_COMPLETED)
			e.callbackMetrics.callback("event", callbackFresh, callbackResponse)
		} else {
			e.callbackMetrics.callback("event", callbackFresh, callbackError)
		}
	}
	cancel()
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.events[event.id] != event {
		return
	}
	event.inFlight = false
	if accepted || !time.Now().Before(event.deadline) || e.ctx.Err() != nil {
		e.releaseResidentEventLocked(c, event)
		return
	}
	if event.backoff == 0 {
		event.backoff = time.Second
	} else {
		event.backoff = min(10*time.Second, event.backoff*2)
	}
	event.next = minTime(event.deadline, time.Now().Add(event.backoff))
	e.mu.Lock()
	e.deadlines.set(residentDeadlineKey{kind: residentEventDeadline, contextID: c.snapshot.Context.Id, record: event.id}, 1, event.next)
	e.mu.Unlock()
	e.wakeDeadline()
}

func (e *ResidentMemory) releaseResidentEventLocked(c *residentContext, event *residentEvent) {
	if c.events[event.id] != event {
		return
	}
	delete(c.events, event.id)
	if event.budget != nil {
		if event.budget.provisional == event {
			event.budget.provisional = nil
		}
		if event.budget.open {
			event.budget.events++
			c.reservedEvents++
		}
	}
	_ = e.refundResidentBudgetLocked(c, event.budget, event.bytes, event.borrowed)
	e.mu.Lock()
	e.deadlines.remove(residentDeadlineKey{kind: residentEventDeadline, contextID: c.snapshot.Context.Id, record: event.id})
	e.mu.Unlock()
	e.afterResidentOutputReleaseLocked(c)
}

func (e *ResidentMemory) fireResidentEventDeadline(item residentDeadline) bool {
	if item.key.kind != residentEventDeadline {
		return false
	}
	id, eventID := item.key.contextID, item.key.record
	e.mu.Lock()
	c := e.contexts[id]
	e.mu.Unlock()
	if c == nil {
		return true
	}
	c.mu.Lock()
	event := c.events[eventID]
	if event == nil || time.Now().Before(event.next) {
		c.mu.Unlock()
		return true
	}
	if !time.Now().Before(event.deadline) {
		e.releaseResidentEventLocked(c, event)
		c.mu.Unlock()
		return true
	}
	// Controller IO must not stall unrelated SIP/ACK timers. This is bounded
	// execution of an already-due lifecycle record, not request presentation.
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		c.mu.Unlock()
		return true
	}
	if e.eventWork == nil {
		e.eventWork = make(chan struct{}, 64)
	}
	select {
	case e.eventWork <- struct{}{}:
	default:
		event.next = minTime(event.deadline, time.Now().Add(time.Second))
		e.deadlines.set(item.key, 1, event.next)
		e.mu.Unlock()
		c.mu.Unlock()
		e.wakeDeadline()
		return true
	}
	e.wg.Add(1)
	e.mu.Unlock()
	c.mu.Unlock()
	go func() { defer e.wg.Done(); defer func() { <-e.eventWork }(); e.deliverResidentEvent(e.ctx, event) }()
	return true
}
