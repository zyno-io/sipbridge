package engine

import (
	"crypto/sha256"
	"errors"
	"time"

	"google.golang.org/protobuf/proto"
	"sipbridge/internal/control"
	pb "sipbridge/internal/controlpb"
)

// admitResidentCommandLocked separates admission from semantic execution. Only
// an authenticated live owner with the next sequence and current policy can
// consume a sequence. A subsequent semantic refusal retains that consumed
// sequence's exact result but publishes no partial SIP transition.
func (e *ResidentMemory) admitResidentCommandLocked(c *residentContext, peer *control.Peer, cmd *pb.Command, operation string, deadline time.Time) (*residentCommand, *pb.CommandAdmission, bool, error) {
	if c.snapshot == nil || cmd == nil || cmd.Context == nil || cmd.Context.Id != c.snapshot.GetContext().GetId() || peer == nil {
		return nil, nil, false, errors.New("command owner mismatch")
	}
	application := peer.Application()
	term := c.snapshot.ApplicationLeaseTerm
	owner := sameProcess(c.snapshot.Application, application)
	if !owner && c.subscription != nil && cmd.GetRespond() != nil {
		tx := c.transactions[c.requests[cmd.GetRespond().RequestId]]
		// Only the processor of this exact admitted renewal may respond before
		// its publication receipt transfers the accepted subscription route.
		if tx != nil && c.subscription.Pending == tx.native.Key && sameProcess(tx.application, application) {
			owner, term = true, peer.ApplicationLease().GetTerm()
		}
	}
	if !owner || term == "" || peer.ApplicationLease().GetTerm() != term {
		return nil, nil, false, errors.New("command owner mismatch")
	}
	if cmd.Context.State == nil {
		if cmd.GetOpenUac() == nil {
			return nil, nil, false, errors.New("native context authority required")
		}
	} else if !proto.Equal(cmd.Context.State, c.snapshot.Context.State) {
		return nil, nil, false, errors.New("command context authority changed")
	}
	e.mu.Lock()
	lease := e.leases[residentControllerKey(application)]
	live := !e.closed && lease != nil && lease.term == term && time.Now().Before(lease.expires)
	e.mu.Unlock()
	if !live {
		return nil, nil, false, control.ErrApplicationLeaseFenced
	}
	// Fingerprint only the transient command, never a retained native graph.
	encoded, err := proto.MarshalOptions{Deterministic: true}.Marshal(cmd)
	if err != nil {
		return nil, nil, false, err
	}
	fingerprint := sha256.Sum256(encoded)
	if cmd.Sequence == c.snapshot.CommandSequence && c.commandOperation == operation && c.commandFingerprint == fingerprint && c.command != nil {
		c.command.replayWaiters++
		c.receiptPins++
		retainResidentCommandLocked(c.command)
		return c.command, nil, true, nil
	}
	if cmd.Sequence == 0 || cmd.Sequence > 1<<53-1 || c.snapshot.Revision >= 1<<53-1 || cmd.Sequence != c.snapshot.CommandSequence+1 {
		return nil, nil, false, errors.New("stale command sequence or sequence gap")
	}
	policy, err := residentCommandPolicy(c, cmd, operation, deadline)
	if err != nil {
		return nil, nil, false, err
	}
	receipt := newResidentCommand(operation, cmd.Sequence)
	receipt.engine, receipt.budget = e, c.budget
	var markerBudget *residentBudget
	if cmd.GetOpenUac() != nil || cmd.GetCancel() != nil {
		markerBudget = c.budget
	}
	if response := cmd.GetRespond(); response != nil {
		if tx := c.transactions[c.requests[response.RequestId]]; tx != nil {
			markerBudget = tx.budget
		}
	}
	if request := cmd.GetDialogRequest(); request != nil {
		if request.Method == "ACK" || request.Method == "BYE" {
			if dialog := c.dialogs[c.selectedDialog]; dialog != nil {
				markerBudget = dialog.budget
			}
		} else if request.Method == "NOTIFY" && c.requiredNotifyBudget != nil {
			markerBudget = c.requiredNotifyBudget
		}
	}
	if err := e.prepareResidentCommandLocked(c, receipt, markerBudget); err != nil {
		return nil, nil, false, err
	}
	receipt.registered = true
	c.receiptPins++
	previous := c.command
	c.snapshot.CommandSequence = cmd.Sequence
	c.snapshot.Revision++
	c.revision++
	c.command = receipt
	releaseResidentCommandWireLocked(c, previous)
	releaseResidentCommandLocked(c, previous)
	c.commandOperation, c.commandFingerprint = operation, fingerprint
	if policy != nil {
		c.snapshot.DirectoryEpoch = policy.DirectoryEpoch
	}
	return receipt, policy, false, nil
}

func residentCommandPolicy(c *residentContext, cmd *pb.Command, operation string, deadline time.Time) (*pb.CommandAdmission, error) {
	if !time.Now().Before(deadline) {
		return nil, policyRefusal("command deadline elapsed")
	}
	decision := cmd.Admission
	if decision == nil {
		if cmd.GetCancel() != nil {
			return nil, nil
		}
		return nil, policyRefusal("command admission required")
	}
	scope := &statePolicyAdmission{Decision: decision, Command: operation, Term: c.snapshot.ApplicationLeaseTerm}
	if !scope.valid() || proto.Size(decision) > statePolicyAdmissionBytes {
		return nil, policyRefusal("invalid scope or scope capacity")
	}
	if c.snapshot.DirectoryEpoch != "" && c.snapshot.DirectoryEpoch != decision.DirectoryEpoch {
		return nil, policyRefusal("directory epoch changed")
	}
	if !proto.Equal(c.snapshot.Delivery, decision.ExpectedDelivery) {
		return nil, policyRefusal("context delivery changed")
	}
	transport, binding := c.snapshot.Transport, c.snapshot.BindingId
	if open := cmd.GetOpenUac(); open != nil {
		transport, binding = uacTransport(open), open.BindingId
	}
	selected := decision.SelectedFlow
	if selected != nil && transport != "" && selected.Transport != transport {
		return nil, policyRefusal("transport changed")
	}
	request := cmd.GetOpenUac() != nil || cmd.GetDialogRequest() != nil
	if request && binding != "" && selected == nil {
		return nil, policyRefusal("registered route missing")
	}
	if (cmd.GetRespond() != nil || cmd.GetCancel() != nil) && selected != nil {
		return nil, policyRefusal("unexpected route")
	}
	policy := proto.Clone(decision).(*pb.CommandAdmission)
	policy.ExpiresAtMs = min(policy.ExpiresAtMs, uint64(deadline.UnixMilli()))
	if time.Now().UnixMilli() >= int64(policy.ExpiresAtMs) {
		return nil, policyRefusal("policy deadline elapsed")
	}
	return policy, nil
}
