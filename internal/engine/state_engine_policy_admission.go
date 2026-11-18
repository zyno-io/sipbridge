package engine

import (
	"encoding/json"
	"errors"

	"github.com/emiago/sipgo/sip"
	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
)

// One current procedure selection, not a grant journal. Physical effects retain
// their existing immutable projected authority after this selection changes.
type statePolicyAdmission struct {
	Decision   *pb.CommandAdmission `json:"decision"`
	Command    string               `json:"command"`
	Generation uint64               `json:"generation,omitempty"`
	Term       string               `json:"term"`
}

func (s *statePolicyAdmission) valid() bool {
	return s != nil && nativePolicyValid(s.Decision, s.Command, s.Term)
}

func (s *statePolicyAdmission) UnmarshalJSON(data []byte) error {
	if len(data) > statePolicyAdmissionBytes {
		return errors.New("retained command policy exceeds capacity")
	}
	type plain statePolicyAdmission
	var decoded plain
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	next := statePolicyAdmission(decoded)
	if !next.valid() {
		return errors.New("invalid retained command policy")
	}
	*s = next
	return nil
}

func (r *stateReduction) commandPolicy(c *stateContext) (*statePolicyAdmission, error) {
	decision := r.in.Command.GetAdmission()
	if decision == nil {
		if r.engine.autonomousCompletion() && r.in.Command.GetCancel() == nil {
			return nil, policyRefusal("command admission required")
		}
		return nil, nil
	}
	scope := &statePolicyAdmission{Decision: proto.Clone(decision).(*pb.CommandAdmission), Command: r.in.Operation, Term: r.in.ApplicationLeaseTerm, Generation: r.in.ApplicationGeneration}
	if !r.engine.autonomousCompletion() || !scope.valid() || r.deadline <= 0 {
		return nil, policyRefusal("invalid scope")
	}
	scope.Decision.ExpiresAtMs = min(scope.Decision.ExpiresAtMs, uint64(r.deadline))
	if int64(scope.Decision.ExpiresAtMs) <= r.now {
		return nil, policyRefusal("deadline elapsed")
	}
	encoded, err := json.Marshal(scope)
	if err != nil || len(encoded) > statePolicyAdmissionBytes {
		return nil, policyRefusal("scope capacity")
	}
	var delivery *pb.ContextDelivery
	if c != nil {
		delivery = c.Delivery
		if c.OwnerDirectoryEpoch != "" && c.OwnerDirectoryEpoch != decision.DirectoryEpoch {
			return nil, policyRefusal("directory epoch changed")
		}
		if c.Snapshot.ApplicationLeaseTerm != scope.Term {
			return nil, policyRefusal("owner term changed")
		}
	}
	if !proto.Equal(delivery, decision.ExpectedDelivery) {
		return nil, policyRefusal("context delivery changed")
	}
	selected := decision.SelectedFlow
	open := r.in.Command.GetOpenUac()
	binding, transport := "", ""
	if c != nil {
		binding, transport = c.Snapshot.BindingId, c.Snapshot.Transport
	}
	if open != nil {
		binding, transport = open.BindingId, open.Transport
	}
	if selected != nil && transport != "" && selected.Transport != transport {
		return nil, policyRefusal("transport changed")
	}
	request := open != nil || r.in.Command.GetDialogRequest() != nil
	if request && binding != "" && selected == nil {
		return nil, policyRefusal("registered route missing")
	}
	if (r.in.Command.GetRespond() != nil || r.in.Command.GetCancel() != nil) && selected != nil {
		return nil, policyRefusal("unexpected route")
	}
	return scope, nil
}

func (r *stateReduction) installPolicy(c *stateContext, scope *statePolicyAdmission) {
	if scope == nil {
		return
	}
	c.OwnerValidated, c.OwnerDirectoryEpoch = true, scope.Decision.DirectoryEpoch
}

func (r *stateReduction) policyFlow(c *stateContext, tx *stateTransaction) (*pb.FlowRef, error) {
	scope := tx.Policy
	if scope == nil {
		return nil, nil
	}
	if scope.Command != tx.Command || scope.Term != c.Snapshot.ApplicationLeaseTerm ||
		scope.Decision.DirectoryEpoch != c.OwnerDirectoryEpoch || int64(scope.Decision.ExpiresAtMs) <= r.now ||
		!proto.Equal(scope.Decision.ExpectedDelivery, c.Delivery) {
		return nil, policyRefusal("procedure authority changed")
	}
	flow := scope.Decision.SelectedFlow
	if flow == nil {
		flow = tx.Flow
	}
	if flow == nil {
		flow = c.Flow
	}
	if !validPolicyFlow(flow) || flow.Transport != c.Snapshot.Transport {
		return nil, policyRefusal("peer flow unavailable")
	}
	return flow, nil
}

func (r *stateReduction) anchorPolicyTransaction(c *stateContext, tx *stateTransaction) {
	if tx.Policy == nil {
		return
	}
	facts, err := tx.Native.Origin.Facts()
	if err != nil || !facts.IsRequest || facts.Method != sip.INVITE {
		return
	}
	initial := facts.ToTag == ""
	if tx.CompletionScope == "" {
		if initial && c.CompletionScope != "" {
			tx.CompletionScope = c.CompletionScope
		} else {
			tx.CompletionScope = r.engine.nativeCompletionScope(c.Snapshot.Context)
		}
	}
	if initial && c.CompletionScope == "" {
		c.CompletionScope = tx.CompletionScope
		origin := tx.Native.Origin.Clone()
		c.ScopeOrigin = &origin
	}
}
