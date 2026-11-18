package engine

import (
	"context"
	"mime"
	"strings"

	"github.com/emiago/sipgo/sip"
	pb "sipbridge/internal/controlpb"
)

// OPTIONS reports only this process's cached signaling availability. It neither
// discovers a context nor admits a business operation, even with a To tag.
func (i *Ingress) localOptionsWithin(ctx context.Context, flow *physicalFlow, message *pb.SipMessage, parsed sip.Message) uint32 {
	if parsed == nil {
		var err error
		parsed, err = parseObservedMessage(message.Wire)
		if err != nil {
			i.invalidMessages.Add(1)
			return 0
		}
	}
	request, ok := parsed.(*sip.Request)
	if !ok || request.Method != sip.OPTIONS {
		return 0
	}
	if request.CSeq() == nil || request.CSeq().MethodName != request.Method || request.CallID() == nil || request.CallID().Value() == "" {
		i.invalidMessages.Add(1)
		return 0
	}
	status := 200
	headers := []*pb.Header{{Name: "Allow", Value: i.methodPolicy.allow()}}
	if i.draining.Load() || i.control == nil || !i.control.HasReadyPeer() {
		status = 503
	} else {
		// This local responder implements no SIP option-tag extensions.
		var unsupported strings.Builder
		for _, required := range request.GetHeaders("Require") {
			if value := strings.TrimSpace(required.Value()); value != "" {
				if unsupported.Len() > 0 {
					unsupported.WriteString(", ")
				}
				unsupported.WriteString(value)
			}
		}
		if unsupported.Len() > 0 {
			status = 420
			headers = append(headers, &pb.Header{Name: "Unsupported", Value: unsupported.String()})
		} else if len(request.Body()) > 0 {
			// There is no local request-content handler. RFC 3261 defaults
			// handling to required; only explicitly optional content is ignored.
			dispositions := request.GetHeaders("Content-Disposition")
			_, params, err := mime.ParseMediaType(header(request, "Content-Disposition"))
			if len(dispositions) != 1 || err != nil || !strings.EqualFold(params["handling"], "optional") {
				status = 415
				headers = append(headers, &pb.Header{Name: "Accept", Value: ""})
			}
		}
	}
	i.localResponseParsedWithin(ctx, flow, message, request, status, "options-", headers)
	return uint32(status)
}
