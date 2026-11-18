package engine

import (
	"fmt"
	"os"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/emiago/sipgo/sip"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/state"
)

func TestStateNativeProcessPausedReservedWriterCannotBeTTLStolen(t *testing.T) {
	h := newRelayProcessHarness(t)
	a, b := h.start("sip-a"), h.start("sip-b")
	phone := h.phone(a)
	seedCall := "reserve-seed"
	phone.send(t, processRequest(t, sip.INFO, seedCall, 1, ""))
	seed := h.request(seedCall, 1).Envelope.GetRequest()
	h.respond(a, seed, 200)
	phone.receive(t, sip.INFO, 200)
	rule := stateProcessRule{ID: "reserved-before-write", Stage: "reserve", Method: "INVITE", Class: pb.EffectAuthorization_APPLICATION}
	h.arm(a, rule)
	opened := h.result(a, &pb.Command{Context: &pb.ContextRef{Owner: a.process, Id: "reserved-outbound"}, Sequence: 1, Action: &pb.Command_OpenUac{OpenUac: &pb.OpenUac{Uri: "sip:phone@example.test", Transport: "tls", BindingId: "fixture-binding", PrincipalId: "fixture-phone", Headers: []*pb.Header{{Name: "From", Value: "<sip:service@example.test>"}}}}})
	barrier := h.workerEvent(a, "barrier", rule.ID)
	if err := a.command.Process.Signal(syscall.SIGSTOP); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = a.command.Process.Signal(syscall.SIGCONT) })
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		status, err := os.ReadFile(fmt.Sprintf("/proc/%d/status", a.command.Process.Pid))
		if err != nil {
			t.Fatal(err)
		}
		stopped := false
		for _, line := range strings.Split(string(status), "\n") {
			if strings.HasPrefix(line, "State:") && strings.Contains(line, "T") {
				stopped = true
			}
		}
		if stopped {
			break
		}
		select {
		case <-ticker.C:
		case <-h.ctx.Done():
			t.Fatal("SIGSTOP was not observed")
		}
	}
	callID := opened.Snapshot.CallId
	var original state.Effect
	for {
		record, _ := h.record(callID)
		found := false
		for _, effect := range record.Payload.Effects {
			if effect.ID == barrier.Token {
				found = true
				original = effect
				if effect.Status != state.EffectDispatching {
					t.Fatal("paused reservation was stolen or falsely completed")
				}
			}
		}
		if !found {
			t.Fatal("paused attempt evidence vanished")
		}
		if time.Now().UnixMilli() > original.DeadlineMS {
			break
		}
		select {
		case <-ticker.C:
		case <-h.ctx.Done():
			t.Fatal("original finite permit never expired")
		}
	}
	// B remains useful while A is stopped; a different group's real TLS request
	// and response must not wait for the paused attempt or its socket.
	other := h.phone(b)
	otherCall := "healthy-while-paused"
	other.send(t, processRequest(t, sip.INFO, otherCall, 1, ""))
	presented := h.request(otherCall, 1).Envelope.GetRequest()
	h.respond(b, presented, 200)
	other.receive(t, sip.INFO, 200)
	if err := a.command.Process.Signal(syscall.SIGCONT); err != nil {
		t.Fatal(err)
	}
	h.release(a)
	h.wait(func(o processObservation) bool {
		delivery := o.Envelope.GetDelivery()
		return delivery != nil && delivery.Effect.GetId() == original.ID && delivery.Outcome == pb.DeliveryResult_NOT_WRITTEN && delivery.Recorded
	})
	// An ordered response on the original stream proves it can resume without
	// slipping the expired INVITE in front of a subsequent valid frame.
	phone.send(t, processRequest(t, sip.INFO, seedCall, 2, ""))
	next := h.request(seedCall, 2).Envelope.GetRequest()
	h.respond(a, next, 200)
	phone.receive(t, sip.INFO, 200)
	for _, message := range phone.received {
		if request, ok := message.(*sip.Request); ok && request.Method == sip.INVITE {
			t.Fatal("expired reserved attempt wrote SIP after process resumed")
		}
	}
	t.Logf("group=%s effect=%s attempt=%s originalDeadline=%d retainedStatus=%s", barrier.Group, original.ID, original.AttemptID, original.DeadlineMS, original.Status)
}
