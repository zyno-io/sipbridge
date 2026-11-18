package engine

import (
	"testing"
	"time"

	"github.com/emiago/sipgo/sip"
	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
)

func TestResidentSubscriptionPhoneCloseEndsOnlyItsExactSubscriptions(t *testing.T) {
	e, c, _ := residentSubscriptionFixture(t)
	e.contexts = map[string]*residentContext{c.snapshot.Context.Id: c}
	e.deadlineWake = make(chan struct{}, 1)
	c.subscription = &stateSubscription{Event: "message-summary", ExpiresMS: time.Now().Add(time.Hour).UnixMilli()}
	other := &residentContext{snapshot: proto.Clone(c.snapshot).(*pb.ContextSnapshot), flow: proto.Clone(c.flow).(*pb.FlowRef), subscription: &stateSubscription{ExpiresMS: time.Now().Add(time.Hour).UnixMilli()}}
	other.snapshot.Context.Id = "other-subscription"
	other.flow.ConnectionId = "other-phone"
	e.contexts[other.snapshot.Context.Id] = other
	call := &residentContext{snapshot: proto.Clone(c.snapshot).(*pb.ContextSnapshot), flow: c.flow}
	call.snapshot.Context.Id = "active-call"
	e.contexts[call.snapshot.Context.Id] = call
	for _, current := range []*residentContext{c, other} {
		current.mu.Lock()
		e.scheduleResidentSubscriptionLocked(current)
		current.mu.Unlock()
	}
	stale := proto.Clone(c.flow).(*pb.FlowRef)
	stale.Ingress.Epoch = "old-epoch"
	e.FlowClosed(stale)
	if c.snapshot.Ended {
		t.Fatal("another process epoch closed this subscription")
	}
	e.FlowClosed(c.flow)
	if !c.snapshot.Ended || !c.subscription.Terminating {
		t.Fatal("socket closure did not end the subscription without event budget")
	}
	if other.snapshot.Ended || call.snapshot.Ended {
		t.Fatal("subscription closure changed another socket or an active call")
	}
	if len(e.subscriptionFlows) != 1 || len(e.deadlines.heap) != 1 || e.contexts[c.snapshot.Context.Id] != nil {
		t.Fatal("closed subscription retained its flow index, expiry or idle context")
	}
	e.FlowClosed(c.flow)
	if len(e.subscriptionFlows) != 1 || other.snapshot.Ended {
		t.Fatal("duplicate flow closure changed another subscription")
	}
}

func TestResidentSubscriptionCloseBeforeIndexCannotStayActive(t *testing.T) {
	e, c, _ := residentSubscriptionFixture(t)
	e.contexts = map[string]*residentContext{c.snapshot.Context.Id: c}
	e.deadlineWake = make(chan struct{}, 1)
	e.cfg.LiveFlow = func(*pb.FlowRef) bool { return false }
	c.subscription = &stateSubscription{ExpiresMS: time.Now().Add(time.Hour).UnixMilli()}
	e.FlowClosed(c.flow) // No published subscription index existed at close time.
	c.mu.Lock()
	e.scheduleResidentSubscriptionLocked(c)
	c.mu.Unlock()
	if !c.snapshot.Ended || len(e.subscriptionFlows) != 0 || len(e.deadlines.heap) != 0 {
		t.Fatal("close-before-index race retained a live subscription or timer")
	}
}

func TestResidentSubscriptionRefreshCannotMovePhoneSocket(t *testing.T) {
	_, c, tx := residentSubscriptionFixture(t)
	c.subscription = &stateSubscription{Event: "message-summary"}
	parsed, err := sip.ParseMessage(tx.native.Origin.WireBytes())
	if err != nil {
		t.Fatal(err)
	}
	request := parsed.(*sip.Request)
	request.To().Params.Add("tag", c.snapshot.LocalTag)
	if err = residentValidateSubscription(c, request, &pb.OwnerIngest{Flow: c.flow}); err != nil {
		t.Fatal("same-socket refresh refused", err)
	}
	other := proto.Clone(c.flow).(*pb.FlowRef)
	other.ConnectionId = "replacement-phone"
	if err = residentValidateSubscription(c, request, &pb.OwnerIngest{Flow: other}); err == nil {
		t.Fatal("another socket moved the subscription")
	}
}
