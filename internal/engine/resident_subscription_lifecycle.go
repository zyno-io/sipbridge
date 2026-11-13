package engine

import (
	"time"

	pb "sipbridge/internal/controlpb"
)

// scheduleResidentSubscriptionLocked indexes the subscription's actual next
// semantic boundary. A renewal replaces the previous deadline in place.
func (e *ResidentMemory) scheduleResidentSubscriptionLocked(c *residentContext) {
	if c.subscription == nil || c.snapshot.Ended {
		return
	}
	at := c.subscription.ExpiresMS
	if c.subscription.Terminating {
		at = c.subscription.TerminalMS
	}
	if at <= 0 {
		return
	}
	e.mu.Lock()
	e.deadlines.set(residentDeadlineKey{kind: residentSubscriptionDeadline, contextID: c.snapshot.Context.Id}, uint64(at), time.UnixMilli(at))
	e.mu.Unlock()
	e.wakeDeadline()
}

func (e *ResidentMemory) fireResidentSubscriptionDeadline(item residentDeadline) bool {
	id := item.key.contextID
	if item.key.kind != residentSubscriptionDeadline {
		return false
	}
	e.mu.Lock()
	c := e.contexts[id]
	e.mu.Unlock()
	if c == nil {
		return true
	}
	c.mu.Lock()
	sub := c.subscription
	if sub == nil || c.snapshot.Ended {
		c.mu.Unlock()
		return true
	}
	at := sub.ExpiresMS
	kind := pb.DialogEvent_SUBSCRIPTION_EXPIRED
	if sub.Terminating {
		at = sub.TerminalMS
		kind = pb.DialogEvent_ENDED
	}
	now := time.Now()
	if at <= 0 || uint64(at) != item.generation || now.UnixMilli() < at {
		c.mu.Unlock()
		return true
	}
	event, err := e.stageResidentEventLocked(c, kind, nil, "", now.Add(2*time.Minute))
	if err != nil {
		c.mu.Unlock()
		return true
	}
	if sub.Terminating {
		c.snapshot.Ended = true
		if c.requiredNotifyBudget != nil {
			e.closeResidentBudgetLocked(c, c.requiredNotifyBudget)
			c.requiredNotifyBudget = nil
		}
		e.scheduleResidentContextRetirementLocked(c)
	} else {
		sub.Terminating = true
		sub.TerminalMS = now.Add(5 * time.Second).UnixMilli()
		e.scheduleResidentSubscriptionLocked(c)
	}
	c.revision++
	c.snapshot.Revision++
	event.message.Snapshot.Ended = c.snapshot.Ended
	event.message.Snapshot.Revision = c.snapshot.Revision
	e.scheduleResidentEventsLocked(c, []*residentEvent{event})
	c.mu.Unlock()
	return true
}
