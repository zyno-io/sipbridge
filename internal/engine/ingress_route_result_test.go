package engine

import (
	"context"
	"testing"
	"time"

	pb "sipbridge/internal/controlpb"
)

func TestControllerUnavailableIsAValidFailClosedRoute(t *testing.T) {
	for _, method := range []string{"INFO", "INVITE"} {
		for _, test := range []struct {
			name       string
			outcome    pb.RouteResult_Outcome
			supplied   uint32
			status     int
			diagnostic string
		}{
			{"unavailable", pb.RouteResult_UNAVAILABLE, 0, 503, "route-unavailable"},
			{"unavailable-cannot-select-status", pb.RouteResult_UNAVAILABLE, 486, 503, "route-unavailable"},
			{"rejected", pb.RouteResult_REJECTED, 486, 486, "rejected"},
			{"unspecified", pb.RouteResult_UNSPECIFIED, 0, 503, "invalid-route-outcome"},
		} {
			t.Run(method+"/"+test.name, func(t *testing.T) {
				diagnostics := make(chan IngressRouteTiming, 1)
				f := newUDPOrderFixture(t, func(i *Ingress) { i.onRouteTiming = func(timing IngressRouteTiming) { diagnostics <- timing } })
				ctx, cancel := context.WithTimeout(f.ctx, time.Second)
				defer cancel()
				done := make(chan struct{})
				responses := make(chan int, 2)
				go func() {
					defer close(done)
					f.ingress.routeWithin(ctx, &physicalFlow{ref: &pb.FlowRef{}}, udpOrderMessage(t, method, "unavailable-route", 1),
						func(status int) { responses <- status }, func() { t.Error("controller refusal used datastore arbitration") })
				}()
				request := f.next(t)
				writeControllerEnvelope(t, f.ctx, f.controller, &pb.Envelope{SessionId: f.session, CorrelationId: request.CorrelationId,
					Payload: &pb.Envelope_RouteResult{RouteResult: &pb.RouteResult{Outcome: test.outcome, SipStatus: test.supplied}}})
				select {
				case <-done:
				case <-ctx.Done():
					t.Fatal("controller result did not finish within its original deadline")
				}
				if len(responses) != 1 || <-responses != test.status {
					t.Fatal("route did not emit the single expected refusal")
				}
				observed := <-diagnostics
				if observed.Outcome != test.diagnostic || observed.SipStatus != uint32(test.status) || observed.Hops != 1 {
					t.Fatalf("wrong route diagnostic: %+v", observed)
				}
			})
		}
	}
}
