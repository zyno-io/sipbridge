package engine

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	pb "sipbridge/internal/controlpb"
)

func TestIngressRouteTimingAttributesPreIngressStagesAndOriginalBudget(t *testing.T) {
	diagnostics := make(chan IngressRouteTiming, 1)
	f := newUDPOrderFixture(t, func(i *Ingress) { i.onRouteTiming = func(timing IngressRouteTiming) { diagnostics <- timing } })
	completion, locate := make(chan struct{}), make(chan struct{})
	stages := make(chan string, 2)
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	due, _ := ctx.Deadline()
	f.ingress.stateCompletion = func(stage context.Context, _ *pb.FlowRef, _ *pb.SipMessage) (bool, error) {
		if actual, _ := stage.Deadline(); !actual.Equal(due) {
			t.Error("completion probe received a renewed deadline")
		}
		stages <- "completion"
		<-completion
		return false, nil
	}
	f.ingress.stateLocate = func(stage context.Context, _ *pb.SipMessage) (*pb.ContextSnapshot, error) {
		if actual, _ := stage.Deadline(); !actual.Equal(due) {
			t.Error("location received a renewed deadline")
		}
		stages <- "locate"
		<-locate
		return nil, nil
	}
	done := make(chan struct{})
	rejected := make(chan int, 1)
	go func() {
		defer close(done)
		f.ingress.routeWithin(ctx, &physicalFlow{ref: &pb.FlowRef{}}, udpOrderMessage(t, "REFER", "timing-call", 1),
			func(status int) { rejected <- status }, func() { t.Error("RPC expiry used read-unavailability arbitration") })
	}()
	if <-stages != "completion" {
		t.Fatal("completion stage missing")
	}
	time.Sleep(20 * time.Millisecond)
	close(completion)
	if <-stages != "locate" {
		t.Fatal("location stage missing")
	}
	time.Sleep(20 * time.Millisecond)
	close(locate)
	envelope := f.next(t)
	// Withhold the route reply: every stage must consume the original deadline.
	<-done
	if status := <-rejected; status != 503 {
		t.Fatalf("timing changed route failure response: %d", status)
	}
	observed := <-diagnostics
	if observed.Event != "sipbridge-ingress-route" || observed.Method != "REFER" || observed.Outcome != "route-rpc-error" ||
		observed.CompletionProbeMS < 15 || observed.LocateMS < 15 || observed.RouteRPCMS <= 0 || observed.Hops != 1 ||
		observed.FirstHopBudgetMS < envelope.RemainingBudgetMs || envelope.RemainingBudgetMs == 0 || observed.FirstHopBudgetMS >= 120 || observed.InitialBudgetMS > 150 ||
		observed.RemainingMS != 0 || observed.TotalMS < 100 || observed.SipStatus != 503 {
		t.Fatalf("stages lost consumed budget or failure evidence: %+v envelope budget=%d", observed, envelope.RemainingBudgetMs)
	}
}

func TestIngressRouteTimingBlockedNativeStagesKeepDeadlineAndUncertainty(t *testing.T) {
	for _, stage := range []string{"completion", "locate"} {
		t.Run(stage, func(t *testing.T) {
			diagnostics := make(chan IngressRouteTiming, 1)
			f := newUDPOrderFixture(t, func(i *Ingress) { i.onRouteTiming = func(timing IngressRouteTiming) { diagnostics <- timing } })
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
			defer cancel()
			if stage == "completion" {
				f.ingress.stateCompletion = func(stage context.Context, _ *pb.FlowRef, _ *pb.SipMessage) (bool, error) {
					<-stage.Done()
					return false, stage.Err()
				}
			} else {
				f.ingress.stateLocate = func(stage context.Context, _ *pb.SipMessage) (*pb.ContextSnapshot, error) {
					<-stage.Done()
					return nil, stage.Err()
				}
			}
			rejected := 0
			f.ingress.routeWithin(ctx, &physicalFlow{ref: &pb.FlowRef{}}, &pb.SipMessage{Method: "INVITE"}, func(status int) { rejected = status },
				func() { t.Fatal("stage timeout changed refusal arbitration") })
			observed := <-diagnostics
			if !errors.Is(ctx.Err(), context.DeadlineExceeded) || rejected != 503 || observed.Hops != 0 || observed.RemainingMS != 0 ||
				observed.Outcome != stage+"-error" || observed.CompletionProbeMS+observed.LocateMS < 20 {
				t.Fatalf("blocked stage received a fresh budget or routed after expiry: %+v", observed)
			}
		})
	}
}

func TestIngressRouteTimingFastNativeCompletionEmitsNothing(t *testing.T) {
	ingress := NewIngress(nil, nil, "", 1, IngressOptions{OnRouteTiming: func(IngressRouteTiming) { t.Fatal("fast success emitted timing") }})
	ingress.stateCompletion = func(ctx context.Context, _ *pb.FlowRef, _ *pb.SipMessage) (bool, error) {
		remaining := time.Until(deadline(ctx))
		if remaining > ingressRouteTimeout || remaining < ingressRouteTimeout-50*time.Millisecond {
			t.Fatalf("routing changed its original route budget: %s", remaining)
		}
		return true, nil
	}
	ingress.route(context.Background(), &physicalFlow{ref: &pb.FlowRef{}}, &pb.SipMessage{Method: "ACK"})
}

func TestIngressRedirectConsumesTheOriginalRouteDeadline(t *testing.T) {
	process := &pb.ProcessRef{InstanceId: "sip-a", Epoch: "epoch"}
	firstProcess := &pb.ProcessRef{InstanceId: "controller-a", Epoch: "one"}
	secondProcess := &pb.ProcessRef{InstanceId: "controller-b", Epoch: "one"}
	server, first, second := publicationControllerPair(t, process, firstProcess, secondProcess)
	ready := func(controller publicationTestController) {
		writeControllerEnvelope(t, controller.ctx, controller.connection, &pb.Envelope{SessionId: controller.session, CorrelationId: "ready-" + controller.session,
			Payload: &pb.Envelope_Status{Status: &pb.SessionStatus{RetainedReady: true, BindingsReady: true}}})
		writeControllerEnvelope(t, controller.ctx, controller.connection, &pb.Envelope{SessionId: controller.session, CorrelationId: "barrier-" + controller.session,
			Payload: &pb.Envelope_Ping{Ping: &pb.Heartbeat{Nonce: "barrier-" + controller.session}}})
		if readControllerEnvelope(t, controller.ctx, controller.connection).GetPong() == nil {
			t.Fatal("controller readiness barrier failed")
		}
	}
	ready(first)
	ready(second)
	ingress := NewIngress(process, server, "", 1, IngressOptions{PreferredController: firstProcess.InstanceId})
	flow := &physicalFlow{ref: &pb.FlowRef{Ingress: process, ConnectionId: "redirect-flow", Transport: "udp", SourceIp: "192.0.2.1", SourcePort: 40001}}
	parent, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
	defer cancel()
	done := make(chan struct{})
	go func() {
		ingress.route(parent, flow, &pb.SipMessage{Method: "OPTIONS", Wire: []byte("OPTIONS sip:test@example.test SIP/2.0\r\n\r\n")})
		close(done)
	}()
	firstEnvelope := readControllerEnvelope(t, first.ctx, first.connection)
	firstIngress := firstEnvelope.GetIngress()
	if firstIngress == nil {
		t.Fatalf("first controller received a non-ingress envelope: %s", firstEnvelope)
	}
	firstBudget := firstEnvelope.RemainingBudgetMs
	if firstBudget < 100 || firstBudget > 400 {
		t.Fatalf("first route did not receive the original bounded budget: %s", firstEnvelope)
	}
	time.Sleep(50 * time.Millisecond)
	writeControllerEnvelope(t, first.ctx, first.connection, &pb.Envelope{SessionId: first.session, CorrelationId: firstEnvelope.CorrelationId,
		Payload: &pb.Envelope_RouteResult{RouteResult: &pb.RouteResult{Outcome: pb.RouteResult_REDIRECT, Controller: secondProcess}}})
	secondEnvelope := readControllerEnvelope(t, second.ctx, second.connection)
	secondIngress := secondEnvelope.GetIngress()
	if secondIngress == nil {
		t.Fatalf("redirect target received a non-ingress envelope: %s", secondEnvelope)
	}
	secondBudget := secondEnvelope.RemainingBudgetMs
	if secondEnvelope.CorrelationId != firstEnvelope.CorrelationId || secondBudget == 0 ||
		secondBudget >= firstBudget-20 {
		t.Fatalf("redirect renewed or lost the original route budget: first=%d second=%s", firstBudget, secondEnvelope)
	}
	writeControllerEnvelope(t, second.ctx, second.connection, &pb.Envelope{SessionId: second.session, CorrelationId: secondEnvelope.CorrelationId,
		Payload: &pb.Envelope_RouteResult{RouteResult: &pb.RouteResult{Outcome: pb.RouteResult_ACCEPTED}}})
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("redirected route did not finish")
	}
}

func TestIngressRouteTimingOnlyReportsSanitizedScalars(t *testing.T) {
	for _, callID := range []string{"\ufeff \tSafe-Call@Example\u00a0", "bad\nvalue", strings.Repeat("x", 513)} {
		t.Run(diagnosticCallIDHash(callID), func(t *testing.T) {
			var observed IngressRouteTiming
			ingress := NewIngress(nil, nil, "", 1, IngressOptions{OnRouteTiming: func(summary IngressRouteTiming) { observed = summary }})
			message := &pb.SipMessage{Method: "private-method-secret", Uri: "sip:private-address", Wire: []byte("private-wire"), Reason: "private-error",
				Status: 900, Headers: []*pb.Header{{Name: "Call-ID", Value: callID}, {Name: "Authorization", Value: "private-credential"}}}
			ingress.reportRouteTiming(context.Background(), message, IngressRouteTiming{Outcome: "locate-error"}, time.Now())
			wantHash := ""
			if strings.Contains(callID, "Safe-Call") {
				sum := sha256.Sum256([]byte("Safe-Call@Example"))
				wantHash = hex.EncodeToString(sum[:])
			}
			if observed.CallIDHash != wantHash || observed.Method != "other" || observed.SipStatus != 0 {
				t.Fatalf("unvalidated diagnostic metadata: %+v", observed)
			}
			encoded, err := json.Marshal(observed)
			if err != nil || strings.Contains(string(encoded), "private-") || strings.Contains(string(encoded), "Safe-Call") {
				t.Fatal("sensitive message data entered scalar diagnostics")
			}
		})
	}
}
