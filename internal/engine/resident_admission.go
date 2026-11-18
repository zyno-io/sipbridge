package engine

import (
	"crypto/sha256"
	"errors"
	"time"

	"google.golang.org/protobuf/proto"
	"sipbridge/internal/control"
	pb "sipbridge/internal/controlpb"
)

var errResidentSubscriptionOwnerPending = errors.New("subscription processor pending release")

// admitResidentCommandLocked separates admission from semantic execution. Only
// an authenticated live owner with the next sequence and current policy can
// consume a sequence. A subsequent semantic refusal retains that consumed
// sequence's exact result but publishes no partial SIP transition.
func (e *ResidentMemory) admitResidentCommandLocked(c *residentContext, peer *control.Peer, capability *control.ApplicationCapability, cmd *pb.Command, operation string, deadline time.Time, direct ...*residentDecision) (*residentCommand, *pb.CommandAdmission, bool, error) {
	if c.snapshot == nil || cmd == nil || cmd.Context == nil || cmd.Context.Id != c.snapshot.GetContext().GetId() || peer == nil {
		return nil, nil, false, errors.New("command owner mismatch")
	}
	application := peer.Application()
	term := c.snapshot.ApplicationLeaseTerm
	owner := sameProcess(c.snapshot.Application, application)
	if c.directUnbound {
		if len(direct) == 0 {
			return nil, nil, false, errors.New("unbound request requires a direct decision")
		}
		owner, term = true, peer.ApplicationLease().GetTerm()
	}
	if e.phoneDomain && c.subscription != nil && !c.ownerLost && c.applicationCapability != capability && cmd.GetRespond() != nil {
		return nil, nil, false, errResidentSubscriptionOwnerPending
	}
	if (!owner || term != peer.ApplicationLease().GetTerm()) && c.subscription != nil && cmd.GetRespond() != nil {
		tx := c.transactions[c.requests[cmd.GetRespond().RequestId]]
		// Only the processor of this exact admitted renewal may respond before
		// its accepted decision binds the current processor term. The phone
		// flow remains pinned, including for a fresh term of the same process.
		if tx != nil && c.subscription.Pending == tx.native.Key && sameProcess(tx.application, application) {
			owner, term = true, peer.ApplicationLease().GetTerm()
		}
	}
	if (owner && term != peer.ApplicationLease().GetTerm() || e.phoneDomain && (!owner || c.applicationCapability != capability)) && !c.ownerLost && c.subscription != nil &&
		cmd.GetDialogRequest().GetMethod() == "NOTIFY" && cmd.Sequence != 0 && cmd.Sequence == c.snapshot.CommandSequence+1 {
		return nil, nil, false, errResidentSubscriptionOwnerPending
	}
	adoptTerm := (owner || e.phoneDomain) && (!owner || term != peer.ApplicationLease().GetTerm() || e.phoneDomain && c.applicationCapability != capability) && c.ownerLost && c.subscription != nil &&
		cmd.GetDialogRequest().GetMethod() == "NOTIFY"
	if adoptTerm {
		if c.snapshot.Ended || c.subscription.Terminating || time.Now().UnixMilli() >= c.subscription.ExpiresMS ||
			cmd.Admission == nil || !proto.Equal(cmd.Admission.SelectedFlow, c.flow) ||
			c.flow == nil || !sameProcess(c.flow.Ingress, e.cfg.Process) ||
			(c.flow.Transport != "udp" && e.cfg.LiveFlow != nil && !e.cfg.LiveFlow(c.flow)) {
			return nil, nil, false, errors.New("subscription connection unavailable")
		}
		term = peer.ApplicationLease().GetTerm()
		owner = true
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
	live := false
	if e.phoneDomain {
		live = capability.Guard(application, term, nil) == nil
	} else {
		e.mu.Lock()
		lease := e.leases[residentControllerKey(application)]
		live = !e.closed && lease != nil && lease.term == term && time.Now().Before(lease.expires)
		e.mu.Unlock()
	}
	if !live {
		return nil, nil, false, control.ErrApplicationLeaseFenced
	}

	// Fingerprint only the transient command, never a retained native graph.
	var input proto.Message = cmd
	if len(direct) > 0 {
		input = direct[0].value
	}
	encoded, err := proto.MarshalOptions{Deterministic: true}.Marshal(input)
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
	var policy *pb.CommandAdmission
	if len(direct) > 0 {
		policy, err = e.validateResidentDecisionLocked(c, peer, direct[0], operation, deadline)
	} else {
		policy, err = residentCommandPolicy(c, cmd, operation, deadline)
	}
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
	publicationErr := e.guardResidentPhone(capability, application, term, func(until time.Time) error {
		if err := e.prepareResidentCommandLocked(c, receipt, markerBudget); err != nil {
			return err
		}
		if e.phoneDomain && !time.Now().Before(until) {
			receipt.registered = true
			releaseResidentCommandLocked(c, receipt)
			releaseResidentCommandLocked(c, receipt)
			return control.ErrApplicationLeaseFenced
		}
		if adoptTerm {
			c.snapshot.ApplicationLeaseTerm = term
			c.snapshot.Application = cloneResidentApplication(application)
			if e.phoneDomain {
				c.applicationCapability = capability
			}
			c.ownerLost = false
			e.indexResidentOwnerLocked(c)
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
		return nil
	})
	if publicationErr != nil {
		return nil, nil, false, publicationErr
	}
	return receipt, policy, false, nil
}

func residentCommandPolicy(c *residentContext, cmd *pb.Command, operation string, deadline time.Time) (*pb.CommandAdmission, error) {
	if err := validateResidentCommandPolicy(c, cmd, operation, deadline); err != nil {
		return nil, err
	}
	if cmd.Admission == nil {
		return nil, nil
	}
	policy := proto.Clone(cmd.Admission).(*pb.CommandAdmission)
	policy.ExpiresAtMs = min(policy.ExpiresAtMs, uint64(deadline.UnixMilli()))
	return policy, nil
}

func validateResidentCommandPolicy(c *residentContext, cmd *pb.Command, operation string, deadline time.Time) error {
	return validateNativeCommandPolicy(c.snapshot, cmd, operation, time.Now(), deadline)
}
