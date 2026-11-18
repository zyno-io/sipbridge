package engine

import (
	"errors"
	"github.com/emiago/sipgo/sip/portable"
	"net"
	pb "sipbridge/internal/controlpb"
	"strconv"
)

const redisNotifyResponseRecordLimit = 65606

func validateRedisNotifyResponseMetadata(message portable.Message, flow *pb.FlowRef) error {
	if !validPolicyFlow(flow) || message.WireLen() == 0 || message.WireLen() > 65536 || message.Transport() != flow.Transport || message.Source() != net.JoinHostPort(flow.SourceIp, strconv.Itoa(int(flow.SourcePort))) || message.Destination() != "" {
		return errors.New("invalid NOTIFY response provenance")
	}
	return nil
}
