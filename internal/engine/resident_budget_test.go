package engine

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	"github.com/emiago/sipgo/sip/portable"
	pb "sipbridge/internal/controlpb"
)

func TestResidentProvisionalDebtCannotConsumeMandatoryLifecycleSlots(t *testing.T) {
	e, c, _ := residentSubscriptionFixture(t)
	e.maxBytes = 64 << 20
	budget, err := e.openResidentBudgetLocked(c, residentInviteBudget)
	if err != nil {
		t.Fatal(err)
	}
	c.budget = budget
	message := stateTestRequest("INVITE", 1, "", "provisional")
	message.Selectors = []string{"selected-header"}
	first, err := e.stageResidentEventLocked(c, pb.DialogEvent_PROVISIONAL, message, "invite", time.Now().Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	first.inFlight = true
	for range 1000 {
		event, err := e.stageResidentEventLocked(c, pb.DialogEvent_PROVISIONAL, message, "invite", time.Now().Add(time.Minute))
		if err != nil || event != nil {
			t.Fatal("transient history accumulated behind an outstanding provisional", err)
		}
	}
	if len(c.events) != 1 || first.message.Message != nil || !bytes.Equal(first.wire, message.Wire) || first.selectors[0] != "selected-header" {
		t.Fatal("lifecycle debt retained parsed SIP or lost immutable wire metadata")
	}
	// Unrelated admissions must leave the accepted procedure's slots intact.
	unreserved := 256 - c.reservedEvents - len(c.events)
	for n := 0; n < unreserved; n++ {
		c.events[fmt.Sprint(n)] = nil
	}
	if _, err := e.openResidentBudgetLocked(c, residentSmallBudget); err == nil {
		t.Fatal("new work consumed promised lifecycle capacity")
	}
	for _, kind := range []pb.DialogEvent_Kind{pb.DialogEvent_ANSWERED, pb.DialogEvent_ACKNOWLEDGED, pb.DialogEvent_ENDED} {
		if _, err := e.stageResidentEventLocked(c, kind, nil, "invite", time.Now().Add(time.Minute)); err != nil {
			t.Fatal("admitted mandatory lifecycle event was stranded", kind, err)
		}
	}
}

func TestResidentOutputSlotsRemainAvailableForAdmittedProcedure(t *testing.T) {
	e, c, tx := residentSubscriptionFixture(t)
	e.maxBytes = 64 << 20
	c.outputs = make(map[string]*residentOutput)
	budget, err := e.openResidentBudgetLocked(c, residentSmallBudget)
	if err != nil {
		t.Fatal(err)
	}
	c.budget, tx.budget = budget, budget
	limit := stateNativeProcedures * stateTransactionEmissions
	for n := 0; n < limit-budget.outputs; n++ {
		c.outputs[fmt.Sprint(n)] = nil
	}
	if _, err = e.openResidentBudgetLocked(c, residentSmallBudget); err == nil {
		t.Fatal("ordinary admission consumed reserved completion slots")
	}
	message, err := portable.BuildResponseWithHeaders(tx.native.Origin, 200, "OK", nil, c.snapshot.LocalTag, "", nil)
	if err != nil {
		t.Fatal(err)
	}
	writes, err := e.stageResidentOutputsLocked(c, tx, []portable.Effect{{Kind: portable.Send, Message: &message}}, nil, "", time.Now().Add(time.Second))
	if err != nil || len(writes) != 1 || len(c.outputs)+c.reservedOutputs != limit {
		t.Fatal("admitted completion could not use its slot", err)
	}
	output := c.outputs[writes[0].Effect.Id]
	e.closeResidentBudgetLocked(c, budget)
	if c.reservedOutputs != 0 {
		t.Fatal("closed budget retained unused slots")
	}
	if err := e.releaseResidentOutputLocked(c, output); err != nil {
		t.Fatal(err)
	}
	if c.reservedOutputs != 0 {
		t.Fatal("historical proof revived a closed slot reservation")
	}
}

func TestResidentNativeReplayHasFiniteEmissionBound(t *testing.T) {
	e, c, tx := residentSubscriptionFixture(t)
	e.maxBytes = 64 << 20
	c.outputs = make(map[string]*residentOutput)
	var err error
	c.budget, err = e.openResidentBudgetLocked(c, residentSmallBudget)
	if err != nil {
		t.Fatal(err)
	}
	tx.budget = c.budget
	message, err := portable.BuildResponseWithHeaders(tx.native.Origin, 200, "OK", nil, c.snapshot.LocalTag, "", nil)
	if err != nil {
		t.Fatal(err)
	}
	effects := []portable.Effect{{Kind: portable.Send, Message: &message, Replay: true}}
	for n := 0; n < stateProfileTransmissions; n++ {
		writes, err := e.stageResidentOutputsLocked(c, tx, effects, nil, "", time.Now().Add(time.Second))
		if err != nil || len(writes) != 1 {
			t.Fatal("supported retransmission refused", n, err)
		}
	}
	for range 100 {
		writes, err := e.stageResidentOutputsLocked(c, tx, effects, nil, "", time.Now().Add(time.Second))
		if err != nil || len(writes) != 0 {
			t.Fatal("replay exceeded finite emission contract", err)
		}
	}
	if len(c.outputs) != stateProfileTransmissions || tx.emissions != stateProfileTransmissions {
		t.Fatal("replays grew physical history past the bound")
	}
}

func TestResidentExpiredLifecycleDebtDoesNotSpinOnFullWorkers(t *testing.T) {
	e, c, _ := residentSubscriptionFixture(t)
	e.maxBytes = 64 << 20
	e.contexts = map[string]*residentContext{c.snapshot.Context.Id: c}
	e.eventWork = make(chan struct{}, 1)
	e.eventWork <- struct{}{}
	var err error
	c.budget, err = e.openResidentBudgetLocked(c, residentSmallBudget)
	if err != nil {
		t.Fatal(err)
	}
	event, err := e.stageResidentEventLocked(c, pb.DialogEvent_ENDED, nil, "", time.Now().Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	event.deadline = time.Now().Add(-time.Second)
	event.next = event.deadline
	key := residentDeadlineKey{kind: residentEventDeadline, contextID: c.snapshot.Context.Id, record: event.id}
	if !e.fireResidentEventDeadline(residentDeadline{key: key, at: event.deadline}) {
		t.Fatal("event deadline was not handled")
	}
	if len(c.events) != 0 || len(e.deadlines.heap) != 0 {
		t.Fatal("expired lifecycle debt was retained or immediately rescheduled")
	}
}
