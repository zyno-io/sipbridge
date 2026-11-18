package engine

import (
	"testing"
	"time"

	pb "sipbridge/internal/controlpb"
)

func TestResidentSubscriptionExpiryAndTerminalEvents(t *testing.T) {
	e, peer, conn, ctx, session, _, _ := residentControlFixture(t)
	_, c, _ := residentSubscriptionFixture(t)
	c.snapshot.Application = peer.Application()
	c.snapshot.ApplicationLeaseTerm = peer.ApplicationLease().Term
	c.subscription = &stateSubscription{ExpiresMS: time.Now().Add(-time.Second).UnixMilli()}
	c.transactions = make(map[string]*residentTransaction)
	c.requests = make(map[string]string)
	c.dialogs = make(map[string]*residentDialog)
	c.outputs = make(map[string]*residentOutput)
	c.snapshot.Context = e.contextRef(c.snapshot.CallId, "subscription:with:colons")
	c.mu.Lock()
	if err := e.accountResidentLocked(c, residentContextBytes(c)+residentSubscriptionBytes(c.subscription), 0); err != nil {
		t.Fatal(err)
	}
	var err error
	c.subscriptionBudget, err = e.openResidentBudgetLocked(c, residentSubscriptionBudget)
	if err != nil {
		t.Fatal(err)
	}
	c.budget = c.subscriptionBudget
	c.mu.Unlock()
	e.mu.Lock()
	e.contexts[c.snapshot.Context.Id] = c
	e.mu.Unlock()
	fire := func(at int64, kind pb.DialogEvent_Kind) {
		done := make(chan bool, 1)
		go func() {
			done <- e.fireResidentSubscriptionDeadline(residentDeadline{key: residentDeadlineKey{kind: residentSubscriptionDeadline, contextID: c.snapshot.Context.Id}, generation: uint64(at)})
		}()
		envelope := readControllerEnvelope(t, ctx, conn)
		event := envelope.GetDialogEvent()
		if event == nil || event.Kind != kind {
			t.Fatalf("got event %v, want %v", event, kind)
		}
		writeControllerEnvelope(t, ctx, conn, &pb.Envelope{SessionId: session, CorrelationId: envelope.CorrelationId, Payload: &pb.Envelope_Result{Result: &pb.OperationResult{Certainty: pb.OperationResult_COMPLETED}}})
		if !<-done {
			t.Fatal("deadline was not handled")
		}
	}
	fire(c.subscription.ExpiresMS, pb.DialogEvent_SUBSCRIPTION_EXPIRED)
	c.mu.Lock()
	if !c.subscription.Terminating || c.snapshot.Ended {
		c.mu.Unlock()
		t.Fatal("expiry transition failed")
	}
	c.subscription.TerminalMS = time.Now().Add(-time.Second).UnixMilli()
	terminal := c.subscription.TerminalMS
	c.mu.Unlock()
	fire(terminal, pb.DialogEvent_ENDED)
	c.mu.Lock()
	ended := c.snapshot.Ended
	c.mu.Unlock()
	if !ended {
		t.Fatal("terminal boundary did not end subscription")
	}
}
