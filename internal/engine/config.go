package engine

import (
	"context"
	"errors"
	"mime"
	"strings"

	"github.com/emiago/sipgo/sip"
	"sipbridge/internal/control"
	pb "sipbridge/internal/controlpb"
)

const Capability = "sip.v1"
const DialogContinuityCapability = control.DialogContinuityCapability
const LocalChallengeCapability = control.LocalChallengeCapability

type Config struct {
	Process                                             *pb.ProcessRef
	AdvertiseAddress, PlainAdvertiseAddress, ContactURI string
	OpenNetwork                                         func(context.Context, string, *pb.NetworkTarget) (*pb.FlowRef, error)
	StateIdentityKey                                    []byte
	LiveFlow                                            func(*pb.FlowRef) bool
	WriteCommitted                                      func(context.Context, *pb.FlowWrite) *pb.DeliveryResult
	InspectCommitted                                    func(context.Context, *pb.FlowWrite) *pb.DeliveryResult
}

func sameProcess(a, b *pb.ProcessRef) bool {
	return a != nil && b != nil && a.InstanceId == b.InstanceId && a.Epoch == b.Epoch
}

type ingestError struct {
	status uint32
	text   string
}

func (e *ingestError) Error() string                { return e.text }
func rejectIngest(status uint32, text string) error { return &ingestError{status: status, text: text} }

func reason(status uint32) string {
	switch status {
	case 100:
		return "Trying"
	case 180:
		return "Ringing"
	case 183:
		return "Session Progress"
	case 200:
		return "OK"
	case 202:
		return "Accepted"
	case 401:
		return "Unauthorized"
	case 403:
		return "Forbidden"
	case 405:
		return "Method Not Allowed"
	case 481:
		return "Call/Transaction Does Not Exist"
	case 487:
		return "Request Terminated"
	case 488:
		return "Not Acceptable Here"
	case 489:
		return "Bad Event"
	case 503:
		return "Service Unavailable"
	default:
		return "SIP response"
	}
}

func uacTransport(cmd *pb.OpenUac) string {
	if cmd.Network == nil {
		if cmd.Transport != "" {
			return cmd.Transport
		}
		return "tls"
	}
	if cmd.Network.Transport != "" {
		return cmd.Network.Transport
	}
	target := cmd.Uri
	if cmd.Network.Proxy != "" {
		target = cmd.Network.Proxy
	}
	var uri sip.Uri
	if sip.ParseUri(target, &uri) == nil {
		if uri.Scheme == "sips" {
			return "tls"
		}
		if transport, ok := uri.UriParams.Get("transport"); ok {
			return transport
		}
	}
	return "udp"
}

var errSubscriptionTerminating = errors.New("subscription terminating")

func eventKey(req *sip.Request) (string, error) {
	raw := header(req, "Event")
	event, _, err := mime.ParseMediaType("application/" + raw)
	if err != nil {
		return "", err
	}
	event = strings.TrimPrefix(event, "application/")
	if event != "dialog" && event != "message-summary" {
		return "", errors.New("unsupported event")
	}
	return raw, nil
}
