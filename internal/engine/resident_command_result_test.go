package engine

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/emiago/sipgo/sip/portable"
	pb "sipbridge/internal/controlpb"
)

func TestResidentWaiterKeepsExactEarlierCommandResult(t *testing.T) {
	c := &residentContext{snapshot: &pb.ContextSnapshot{CommandSequence: 2}}
	first := newResidentCommand("first", 1)
	second := newResidentCommand("second", 2)
	c.command = second
	finishResidentCommandLocked(c, second, &pb.OperationResult{Certainty: pb.OperationResult_COMPLETED, SipStatus: 486})
	finishResidentCommandLocked(c, first, &pb.OperationResult{Certainty: pb.OperationResult_COMPLETED, SipStatus: 200})
	result, err := awaitResidentCommand(context.Background(), c, first)
	if err != nil || result.SipStatus != 200 || second.status != 486 {
		t.Fatal("later command replaced original waiter's result", result, err)
	}
	result.SipStatus = 500
	if first.status != 200 {
		t.Fatal("returned mutable authoritative result")
	}
}

func TestResidentExactLateEvidenceResolvesUnknownReceipt(t *testing.T) {
	c := &residentContext{snapshot: &pb.ContextSnapshot{CommandSequence: 1}}
	command := newResidentCommand("one", 1)
	c.command = command
	finishResidentCommandLocked(c, command, &pb.OperationResult{Certainty: pb.OperationResult_UNKNOWN})
	finishResidentCommandLocked(c, command, &pb.OperationResult{Certainty: pb.OperationResult_COMPLETED, SipStatus: 200})
	result, err := awaitResidentCommand(context.Background(), c, command)
	if err != nil || result.Certainty != pb.OperationResult_COMPLETED || result.SipStatus != 200 {
		t.Fatal("late exact evidence lost", result, err)
	}
}

func TestResidentSupersededReceiptRetainsExactWireUntilOriginalWaiter(t *testing.T) {
	e := &ResidentMemory{maxBytes: 4 << 20}
	c := &residentContext{snapshot: &pb.ContextSnapshot{CommandSequence: 1, Revision: 1}}
	budget, err := e.openResidentBudgetLocked(c, residentSmallBudget)
	if err != nil {
		t.Fatal(err)
	}
	first := newResidentCommand("first", 1)
	first.engine, first.budget = e, budget
	c.command = first
	e.maxBytes = e.bytes + e.reserved
	request := stateTestRequest("OPTIONS", 1, "", "receipt-request")
	native := portable.Message{Wire: request.Wire, Transport: "tls"}
	response, err := portable.BuildResponseWithHeaders(native, 200, "OK", nil, "remote", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	observedResponse, err := Observe(response.Wire, false)
	if err != nil {
		t.Fatal(err)
	}
	finishResidentCommandLocked(c, first, &pb.OperationResult{Certainty: pb.OperationResult_COMPLETED, SipStatus: 200, Request: request, Response: observedResponse})
	if first.borrowed == 0 {
		t.Fatal("wire did not consume procedure capacity")
	}
	second := newResidentCommand("second", 2)
	second.engine, second.budget = e, budget
	c.command = second
	c.snapshot.CommandSequence, c.snapshot.Revision = 2, 2
	releaseResidentCommandWireLocked(c, first)
	if len(first.requestWire) == 0 || len(first.responseWire) == 0 {
		t.Fatal("supersession discarded the original waiter pin")
	}
	finishResidentCommandLocked(c, second, &pb.OperationResult{Certainty: pb.OperationResult_COMPLETED, SipStatus: 486})
	result, err := awaitResidentCommand(context.Background(), c, first)
	if err != nil || result.SipStatus != 200 || result.GetSnapshot().CommandSequence != 2 || result.GetSnapshot().Revision != 2 ||
		!bytes.Equal(result.GetRequest().GetWire(), request.Wire) || !bytes.Equal(result.GetResponse().GetWire(), response.Wire) {
		t.Fatal("superseded waiter lost its exact outcome or current snapshot", result, err)
	}
	if len(first.requestWire) != 0 || len(first.responseWire) != 0 || first.borrowed != 0 {
		t.Fatal("old receipt retained wire after original waiter returned")
	}
	result.Request.Wire[0] = 'X'
	current := residentCommandResultLocked(c, second)
	if current.SipStatus != 486 || current.GetSnapshot().CommandSequence != 2 {
		t.Fatal("older waiter changed latest receipt")
	}
}

func TestResidentReplayWaiterPinsSupersededWire(t *testing.T) {
	e := &ResidentMemory{maxBytes: 4 << 20}
	c := &residentContext{snapshot: &pb.ContextSnapshot{CommandSequence: 1}}
	budget, err := e.openResidentBudgetLocked(c, residentSmallBudget)
	if err != nil {
		t.Fatal(err)
	}
	first := newResidentCommand("first", 1)
	first.engine, first.budget = e, budget
	c.command = first
	request := stateTestRequest("OPTIONS", 1, "", "replay-pin")
	finishResidentCommandLocked(c, first, &pb.OperationResult{Certainty: pb.OperationResult_COMPLETED, Request: request})
	if _, err = awaitResidentCommand(context.Background(), c, first); err != nil {
		t.Fatal(err)
	}
	first.replayWaiters++ // Admission pins before dropping c.mu.
	c.command = newResidentCommand("second", 2)
	c.snapshot.CommandSequence = 2
	releaseResidentCommandWireLocked(c, first)
	if len(first.requestWire) == 0 {
		t.Fatal("supersession discarded active replay evidence")
	}
	result, err := awaitResidentReplay(context.Background(), c, first)
	if err != nil || !bytes.Equal(result.GetRequest().GetWire(), request.Wire) || result.Snapshot.CommandSequence != 2 {
		t.Fatal("replay lost exact request", result, err)
	}
	if first.replayWaiters != 0 || len(first.requestWire) != 0 || first.borrowed != 0 {
		t.Fatal("completed replay retained stale evidence")
	}
}

func TestResidentWrittenByeAcceptanceCanBecomeCompleted(t *testing.T) {
	c := &residentContext{snapshot: &pb.ContextSnapshot{CommandSequence: 1}}
	command := newResidentCommand("bye", 1)
	c.command = command
	finishResidentCommandLocked(c, command, &pb.OperationResult{Certainty: pb.OperationResult_ACCEPTED})
	accepted, err := awaitResidentCommand(context.Background(), c, command)
	if err != nil || accepted.Certainty != pb.OperationResult_ACCEPTED {
		t.Fatal("written BYE not accepted")
	}
	finishResidentCommandLocked(c, command, &pb.OperationResult{Certainty: pb.OperationResult_COMPLETED, SipStatus: 200})
	result := residentCommandResultLocked(c, command)
	if result.Certainty != pb.OperationResult_COMPLETED || result.SipStatus != 200 {
		t.Fatal("terminal BYE result did not replace acceptance")
	}
}

func TestResidentCommandMarkerAccountingFollowsConcreteReferences(t *testing.T) {
	e := &ResidentMemory{maxBytes: 4 << 20}
	c := &residentContext{snapshot: &pb.ContextSnapshot{CommandSequence: 1}}
	budget, err := e.openResidentBudgetLocked(c, residentSmallBudget)
	if err != nil {
		t.Fatal(err)
	}
	initialReserve := c.reserved
	e.maxBytes = e.bytes + e.reserved
	command := newResidentCommand("terminal-operation", 1)
	command.engine, command.budget = e, budget
	if err = e.prepareResidentCommandLocked(c, command, budget); err != nil {
		t.Fatal("reserved marker could not be admitted", err)
	}
	command.registered = true
	c.command = command
	c.receiptPins++
	retainResidentCommandLocked(command) // A committed native transaction owns it.
	text := strings.Repeat("界", 2000)
	finishResidentCommandLocked(c, command, &pb.OperationResult{Certainty: pb.OperationResult_COMPLETED, Error: text})
	if len(command.err) > residentCommandErrorLimit || !utf8.ValidString(command.err) || !strings.HasSuffix(command.err, "...[truncated]") {
		t.Fatal("error detail did not obey the bounded UTF-8 contract")
	}
	if c.bytes != command.baseBytes+int64(len(command.err)) {
		t.Fatalf("marker bytes not accounted: %d", c.bytes)
	}
	result, err := awaitResidentCommand(context.Background(), c, command)
	if err != nil || result.Error != command.err {
		t.Fatal("marker result changed", err)
	}
	c.command = nil
	releaseResidentCommandWireLocked(c, command)
	releaseResidentCommandLocked(c, command) // Latest slot releases; native still owns it.
	if command.references != 1 || command.baseBytes == 0 {
		t.Fatal("live native owner lost marker charge")
	}
	releaseResidentCommandLocked(c, command)
	if command.references != 0 || command.baseBytes != 0 || c.bytes != 0 || c.reserved != initialReserve || budget.remaining != initialReserve {
		t.Fatalf("last native reference did not reclaim marker: bytes=%d reserve=%d", c.bytes, c.reserved)
	}
}

func TestResidentCommandMarkersReclaimAcrossLongLivedContext(t *testing.T) {
	e := &ResidentMemory{maxBytes: 4 << 20}
	c := &residentContext{snapshot: &pb.ContextSnapshot{}}
	budget, err := e.openResidentBudgetLocked(c, residentSmallBudget)
	if err != nil {
		t.Fatal(err)
	}
	baseline := budget.remaining
	for sequence := uint64(1); sequence <= 200; sequence++ {
		command := newResidentCommand("bounded-command", sequence)
		command.engine, command.budget = e, budget
		if err = e.prepareResidentCommandLocked(c, command, budget); err != nil {
			t.Fatal(err)
		}
		command.registered = true
		c.receiptPins++
		previous := c.command
		c.command = command
		releaseResidentCommandWireLocked(c, previous)
		releaseResidentCommandLocked(c, previous)
		retainResidentCommandLocked(command) // Native procedure owns its command.
		finishResidentCommandLocked(c, command, &pb.OperationResult{Certainty: pb.OperationResult_COMPLETED, Error: "bounded detail"})
		if _, err = awaitResidentCommand(context.Background(), c, command); err != nil {
			t.Fatal(err)
		}
		releaseResidentCommandLocked(c, command) // Native procedure retires.
		if command.references != 1 {
			t.Fatal("latest slot must be the sole remaining owner")
		}
		if c.bytes != command.baseBytes+int64(len(command.err)) {
			t.Fatalf("marker accounting grew after %d commands: %d", sequence, c.bytes)
		}
	}
	last := c.command
	c.command = nil
	releaseResidentCommandWireLocked(c, last)
	releaseResidentCommandLocked(c, last)
	if c.bytes != 0 || c.reserved != baseline || budget.remaining != baseline || c.receiptPins != 0 {
		t.Fatalf("repeated commands retained marker history: bytes=%d reserved=%d pins=%d", c.bytes, c.reserved, c.receiptPins)
	}
}
