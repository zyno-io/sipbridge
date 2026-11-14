package engine

import (
	"github.com/emiago/sipgo/sip/portable"
	pb "sipbridge/internal/controlpb"
)

// The caller has already validated the native message in its transition.
func stateProjectMessage(message portable.Message, outbound bool) *pb.SipMessage {
	projection, _ := projectNativeMessage(message, outbound)
	return projection
}

func stateMessageBody(message portable.Message) []byte {
	body, _ := message.Body()
	return body
}
