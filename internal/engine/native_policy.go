package engine

import (
	"errors"
	"fmt"
	"net"
	"time"

	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/state"
)

const statePolicyAdmissionBytes = 4096

var errStatePolicyAdmission = errors.New("command policy admission refused")

func policyRefusal(reason string) error { return fmt.Errorf("%w: %s", errStatePolicyAdmission, reason) }

func validPolicyFlow(flow *pb.FlowRef) bool {
	return flow != nil && validSendProcess(flow.Ingress) && state.ValidReference(flow.ConnectionId) &&
		net.ParseIP(flow.SourceIp) != nil && flow.SourcePort > 0 && flow.SourcePort <= 65535 &&
		validSendText(flow.Listener, 1024, false) && validSendTransport(flow.Transport)
}

func nativePolicyValid(decision *pb.CommandAdmission, command, term string) bool {
	if decision == nil || !state.ValidReference(decision.DirectoryEpoch) ||
		decision.ExpiresAtMs == 0 || decision.ExpiresAtMs > 1<<53-1 ||
		!validSendText(command, 160, false) || !state.ValidReference(term) {
		return false
	}
	if f := decision.SelectedFlow; f != nil && !validPolicyFlow(f) {
		return false
	}
	if d := decision.ExpectedDelivery; d != nil && (!validPolicyFlow(d.Flow) || d.NativeRevision == 0 ||
		!state.ValidReference(d.DirectoryEpoch) || !validSendText(d.RegistrationTicket, 256, false)) {
		return false
	}
	return true
}

func validateNativeCommandPolicy(snapshot *pb.ContextSnapshot, cmd *pb.Command, operation string, now, deadline time.Time) error {
	if !now.Before(deadline) {
		return policyRefusal("command deadline elapsed")
	}
	decision := cmd.Admission
	if decision == nil {
		if cmd.GetCancel() != nil {
			return nil
		}
		return policyRefusal("command admission required")
	}
	if !nativePolicyValid(decision, operation, snapshot.ApplicationLeaseTerm) || proto.Size(decision) > statePolicyAdmissionBytes {
		return policyRefusal("invalid scope or scope capacity")
	}
	if snapshot.DirectoryEpoch != "" && snapshot.DirectoryEpoch != decision.DirectoryEpoch {
		return policyRefusal("directory epoch changed")
	}
	if !proto.Equal(snapshot.Delivery, decision.ExpectedDelivery) {
		return policyRefusal("context delivery changed")
	}
	transport, binding := snapshot.Transport, snapshot.BindingId
	if open := cmd.GetOpenUac(); open != nil {
		transport, binding = uacTransport(open), open.BindingId
	}
	selected := decision.SelectedFlow
	if selected != nil && transport != "" && selected.Transport != transport {
		return policyRefusal("transport changed")
	}
	request := cmd.GetOpenUac() != nil || cmd.GetDialogRequest() != nil
	if request && binding != "" && selected == nil {
		return policyRefusal("registered route missing")
	}
	if (cmd.GetRespond() != nil || cmd.GetCancel() != nil) && selected != nil {
		return policyRefusal("unexpected route")
	}
	if now.UnixMilli() >= int64(min(decision.ExpiresAtMs, uint64(deadline.UnixMilli()))) {
		return policyRefusal("policy deadline elapsed")
	}
	return nil
}
