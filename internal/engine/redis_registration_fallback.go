package engine

import (
	"math"
	"strconv"
	"strings"

	"github.com/emiago/sipgo/sip/portable"
	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/nativepb"
	"sipbridge/internal/rediscore"
)

func validRegistrationFallbackDelivery(policy *pb.CommandAdmission) bool {
	candidate := policy.GetFallbackDelivery()
	if !policy.GetRegistrationFallback() {
		return candidate == nil
	}
	if candidate == nil || !validPolicyFlow(candidate.Flow) || candidate.Flow.Transport == "udp" ||
		!proto.Equal(candidate.Flow, policy.SelectedFlow) || candidate.DirectoryEpoch != policy.DirectoryEpoch {
		return false
	}
	current := policy.GetExpectedDelivery().GetNativeRevision()
	if current == math.MaxUint64 || candidate.NativeRevision != current+1 {
		return false
	}
	sequence, ok := strings.CutPrefix(candidate.RegistrationTicket, candidate.DirectoryEpoch+":")
	if !ok {
		return false
	}
	ticket, err := strconv.ParseUint(sequence, 10, 64)
	return err == nil && ticket > 0 && strconv.FormatUint(ticket, 10) == sequence
}

// adoptRegistrationFallback consumes the selected procedure's original
// admission after native matching of both its transaction and physical flow.
// The caller commits the delivery revision and tag with the response itself.
func adoptRegistrationFallback(current *nativepb.Context, procedure *nativepb.Procedure, dialog portable.Dialog, flow *pb.FlowRef, response portable.Message, status int, ownerTerm string) ([]rediscore.Change, error) {
	policy := procedure.GetPolicy()
	if !policy.GetRegistrationFallback() || status < 200 || status >= 300 || current.Ended ||
		ownerTerm == "" || current.ApplicationLeaseTerm != ownerTerm || !sameProcess(current.Application, procedure.Application) ||
		!proto.Equal(current.Delivery, policy.ExpectedDelivery) || current.DirectoryEpoch != policy.DirectoryEpoch ||
		!proto.Equal(policy.SelectedFlow, flow) || !portable.MatchDialogResponse(dialog, response) {
		return nil, nil
	}
	if !validRegistrationFallbackDelivery(policy) {
		return nil, errNativeRecord
	}
	candidate := policy.GetFallbackDelivery()
	tag, err := nativeDeliveryTagChange(candidate)
	if err != nil {
		return nil, err
	}
	current.Flow = proto.Clone(flow).(*pb.FlowRef)
	current.Delivery = proto.Clone(candidate).(*pb.ContextDelivery)
	return []rediscore.Change{tag}, nil
}
