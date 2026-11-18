package engine

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"time"

	pb "sipbridge/internal/controlpb"
)

// IngressRouteTiming is one scalar summary of a routing attempt. It contains no
// SIP payloads, addresses, raw identities or error text.
type IngressRouteTiming struct {
	Event, Method, Outcome                             string
	CallIDHash                                         string `json:",omitempty"`
	SipStatus                                          uint32
	CompletionProbeMS, LocateMS, RouteRPCMS, RefusalMS float64
	InitialBudgetMS, RemainingMS, TotalMS              float64
	FirstHopBudgetMS                                   uint32
	Hops                                               int
}

func ingressElapsedMS(started time.Time) float64 {
	return float64(time.Since(started)) / float64(time.Millisecond)
}

func ingressRemainingMS(ctx context.Context) float64 {
	return max(0, float64(time.Until(deadline(ctx)))/float64(time.Millisecond))
}

func ingressDiagnosticStatus(status int) uint32 {
	if status >= 100 && status <= 699 {
		return uint32(status)
	}
	return 0
}

func (i *Ingress) reportRouteTiming(ctx context.Context, message *pb.SipMessage, summary IngressRouteTiming, started time.Time) {
	summary.TotalMS = ingressElapsedMS(started)
	if summary.TotalMS < 100 && (summary.Outcome == "accepted" || summary.Outcome == "native-completed" || summary.Outcome == "method-disabled" || summary.Outcome == "local-options" || summary.Outcome == "local-challenge" || summary.Outcome == "local-challenge-ack") {
		return
	}
	summary.Event = "sipbridge-ingress-route"
	summary.RemainingMS = ingressRemainingMS(ctx)
	summary.Method = "other"
	switch method := message.GetMethod(); method {
	case "ACK", "BYE", "CANCEL", "INFO", "INVITE", "MESSAGE", "NOTIFY", "OPTIONS", "PRACK", "PUBLISH", "REFER", "REGISTER", "SUBSCRIBE", "UPDATE":
		summary.Method = method
	}
	if message.GetStatus() != 0 {
		if summary.SipStatus == 0 {
			summary.SipStatus = ingressDiagnosticStatus(int(message.GetStatus()))
		}
	}
	for _, header := range message.GetHeaders() {
		if strings.EqualFold(header.GetName(), "Call-ID") || strings.EqualFold(header.GetName(), "i") {
			summary.CallIDHash = diagnosticCallIDHash(header.GetValue())
			break
		}
	}
	if i.onRouteTiming != nil {
		defer func() { _ = recover() }()
		i.onRouteTiming(summary)
		return
	}
	if data, err := json.Marshal(summary); err == nil {
		_, _ = os.Stdout.Write(append(data, '\n'))
	}
}
