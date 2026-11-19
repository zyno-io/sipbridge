package engine

import (
	"errors"

	"github.com/emiago/sipgo/sip/portable"
	pb "sipbridge/internal/controlpb"
)

func commandRefusalStatus(cause error) uint32 {
	var protocol *portable.ProtocolError
	if errors.As(cause, &protocol) {
		return uint32(protocol.Status)
	}
	return 0
}

func commandRefusal(cause error) *pb.OperationResult {
	return &pb.OperationResult{
		Certainty: pb.OperationResult_NOT_ACCEPTED,
		Error:     cause.Error(),
		SipStatus: commandRefusalStatus(cause),
	}
}
