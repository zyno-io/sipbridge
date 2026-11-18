package engine

import (
	"time"

	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
)

// Both c.mu and e.mu are held. This index contains references, not protocol state.
func (e *ResidentMemory) indexResidentSubscriptionFlowLocked(c *residentContext) {
	if c.subscription == nil || c.snapshot.Ended || c.flow == nil || !sameProcess(c.flow.Ingress, e.cfg.Process) || c.flow.Transport == "udp" {
		return
	}
	if e.subscriptionFlows == nil {
		e.subscriptionFlows = make(map[string]map[*residentContext]struct{})
	}
	if e.subscriptionFlows[c.flow.ConnectionId] == nil {
		e.subscriptionFlows[c.flow.ConnectionId] = make(map[*residentContext]struct{})
	}
	e.subscriptionFlows[c.flow.ConnectionId][c] = struct{}{}
}

// Both c.mu and e.mu are held.
func (e *ResidentMemory) unindexResidentSubscriptionFlowLocked(c *residentContext) {
	if c.flow == nil {
		return
	}
	bucket := e.subscriptionFlows[c.flow.ConnectionId]
	delete(bucket, c)
	if len(bucket) == 0 {
		delete(e.subscriptionFlows, c.flow.ConnectionId)
	}
}

// FlowClosed is called after ingress has removed the physical connection and
// released its locks. It visits only subscriptions attached to that exact flow.
func (e *ResidentMemory) FlowClosed(flow *pb.FlowRef) {
	if flow == nil || flow.Transport == "udp" || !sameProcess(flow.Ingress, e.cfg.Process) {
		return
	}
	e.mu.Lock()
	bucket := e.subscriptionFlows[flow.ConnectionId]
	contexts := make([]*residentContext, 0, len(bucket))
	for c := range bucket {
		contexts = append(contexts, c)
	}
	delete(e.subscriptionFlows, flow.ConnectionId)
	e.mu.Unlock()
	for _, c := range contexts {
		c.mu.Lock()
		if c.subscription != nil && !c.snapshot.Ended && proto.Equal(c.flow, flow) && e.residentContextCurrentLocked(c, c.snapshot.Context) {
			e.endResidentSubscriptionFlowLocked(c)
		}
		c.mu.Unlock()
	}
}

// c.mu is held. A missing event budget cannot keep a closed subscription live.
func (e *ResidentMemory) endResidentSubscriptionFlowLocked(c *residentContext) {
	c.snapshot.Ended = true
	c.subscription.Terminating = true
	c.subscription.TerminalMS = time.Now().UnixMilli()
	c.retireReady = true
	c.revision++
	c.snapshot.Revision++
	e.mu.Lock()
	e.unindexResidentSubscriptionFlowLocked(c)
	e.deadlines.remove(residentDeadlineKey{kind: residentSubscriptionDeadline, contextID: c.snapshot.Context.Id})
	e.mu.Unlock()
	if c.requiredNotifyBudget != nil {
		e.closeResidentBudgetLocked(c, c.requiredNotifyBudget)
		c.requiredNotifyBudget = nil
	}
	if event, err := e.stageResidentEventLocked(c, pb.DialogEvent_ENDED, nil, "", time.Now().Add(2*time.Minute)); err == nil {
		e.scheduleResidentEventsLocked(c, []*residentEvent{event})
	}
	e.afterResidentOutputReleaseLocked(c)
}
