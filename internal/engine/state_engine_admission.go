package engine

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strconv"
	"strings"

	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/state"
)

const stateAdmissionMS int64 = 2000
const stateAdmissionLimit = 32

type stateAdmission struct {
	WireHash  string `json:"wireHash"`
	ExpiresMS int64  `json:"expiresMs"`
}

func stateAdmissionHash(wire []byte) string {
	hash := sha256.Sum256(wire)
	return hex.EncodeToString(hash[:])
}

func (e *StateEngine) allocatedContextID(ref state.Ref, ordinal uint64) string {
	sequence := strconv.FormatUint(ordinal, 16)
	return "in-" + sequence + "-" + e.id("inbound-allocation-v1", ref.Incarnation, sequence)[:48]
}

func (e *StateEngine) allocatedContext(ref *pb.ContextRef) bool {
	if ref.GetState() == nil {
		return false
	}
	parts := strings.Split(ref.Id, "-")
	if len(parts) != 3 || parts[0] != "in" {
		return false
	}
	ordinal, err := strconv.ParseUint(parts[1], 16, 53)
	return err == nil && ordinal > 0 && ref.Id == e.allocatedContextID(state.Ref{Incarnation: ref.State.Incarnation}, ordinal)
}

func (r *stateReduction) pruneAdmissions() {
	for id, admission := range r.group.Allocations {
		if admission.ExpiresMS <= r.now {
			delete(r.group.Allocations, id)
		}
	}
}

func (r *stateReduction) reserveAdmission() error {
	parsed, err := parseObservedMessage(r.in.Message.Wire)
	if err != nil {
		return err
	}
	message := projectObservedMessage(r.in.Message.Wire, parsed, false)
	if parsed.CallID().Value() != r.record.Ref.CallID {
		return errors.New("allocation Call-ID mismatch")
	}
	for _, selector := range message.Selectors {
		if id := r.group.Selectors[selector]; id != "" && r.group.Contexts[id] != nil {
			c := r.group.Contexts[id]
			if err := stateRetiredInviteParsed(c, message.Wire, parsed, r.now); err != nil {
				return err
			}
			r.result.Snapshot = stateSnapshot(c, r.engine.cfg.Process)
			return nil
		}
	}
	// This allocator admits only initial requests. Matching retained completion
	// has a read-only ingress path and never allocates here.
	request, ok := parsed.(*sip.Request)
	if !ok || request.To() == nil || tag(request.To().Params) != "" || request.IsAck() || request.IsCancel() {
		return &portable.ProtocolError{Status: 481, Reason: "unknown native context"}
	}
	r.pruneAdmissions()
	hash := stateAdmissionHash(message.Wire)
	for id, reservation := range r.group.Allocations {
		if reservation.WireHash == hash {
			r.result.Snapshot = &pb.ContextSnapshot{Context: r.engine.contextRef(r.record.Ref, id)}
			return nil
		}
	}
	if len(r.group.Contexts)+len(r.group.Allocations) >= stateAdmissionLimit || r.group.NextAllocation >= 1<<53-1 {
		return &portable.ProtocolError{Status: 503, Reason: "native admission capacity"}
	}
	r.group.NextAllocation++
	id := r.engine.allocatedContextID(r.record.Ref, r.group.NextAllocation)
	if r.group.Allocations == nil {
		r.group.Allocations = make(map[string]stateAdmission)
	}
	r.group.Allocations[id] = stateAdmission{WireHash: hash, ExpiresMS: r.now + stateAdmissionMS}
	r.result.Snapshot = &pb.ContextSnapshot{Context: r.engine.contextRef(r.record.Ref, id)}
	return nil
}

// Refusal needs no business routing, but retained transactions must keep their
// normal replay and identity checks before an irrevocable dialog refusal wins.
func stateRetiredInvite(c *stateContext, message *pb.SipMessage, now int64, observed ...sip.Message) error {
	var parsed sip.Message
	if len(observed) > 0 {
		parsed = observed[0]
	} else {
		var err error
		parsed, err = sip.ParseMessage(message.Wire)
		if err != nil {
			return nil
		}
	}
	return stateRetiredInviteParsed(c, message.Wire, parsed, now)
}

func stateRetiredInviteParsed(c *stateContext, wire []byte, parsed sip.Message, now int64) error {
	request, ok := parsed.(*sip.Request)
	if !ok || !request.IsInvite() || request.To() == nil || request.From() == nil {
		return nil
	}
	key, err := sip.ServerTxKeyMake(request)
	if err != nil || c.Transactions[key] != nil {
		return nil
	}
	dialog, found := c.Dialogs[tag(request.To().Params)+"/"+tag(request.From().Params)]
	if !found || !dialog.Retiring && dialog.Phase != portable.DialogEnded {
		return nil
	}
	// Lookup has no physical flow. This retained transport proves only a
	// negative dialog decision; the ordinary rejection path scopes its response.
	m, err := portable.FreezeMessage(wire, request, dialog.Transport, "", "")
	if err != nil {
		return nil
	}
	if !portable.MatchDialogRequest(dialog, m) {
		return nil
	}
	_, _, err = portable.StepDialog(dialog, portable.DialogInput{Kind: portable.DialogReceiveRequest, Now: now, Message: m})
	var refusal *portable.ProtocolError
	if errors.As(err, &refusal) && refusal.Status == 481 {
		return refusal
	}
	return nil
}

func (r *stateReduction) consumeAdmission(ref *pb.ContextRef, wire []byte) error {
	reservation, ok := r.group.Allocations[ref.Id]
	if !ok || reservation.ExpiresMS <= r.now || reservation.WireHash != stateAdmissionHash(wire) || !r.engine.allocatedContext(ref) {
		return &portable.ProtocolError{Status: 481, Reason: "native admission reservation unavailable"}
	}
	delete(r.group.Allocations, ref.Id)
	return nil
}
