package engine

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"time"

	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
)

// reject creates a private native refusal only for an untagged initial request.
// Existing application transactions retain authority over their own responses.
func (e *ResidentMemory) reject(ctx context.Context, flow *pb.FlowRef, message *pb.SipMessage, status uint32) error {
	if flow == nil || message == nil || status < 300 || status > 699 || !sameProcess(flow.Ingress, e.cfg.Process) {
		return errors.New("invalid native rejection")
	}
	parsed, err := sip.ParseMessage(message.Wire)
	if err != nil {
		return err
	}
	return e.rejectParsed(ctx, flow, message, status, parsed, portable.Message{})
}

// rejectParsed accepts only the parse and immutable value from the same
// physical ingress frame. Raw callers use reject's independent wire boundary.
func (e *ResidentMemory) rejectParsed(ctx context.Context, flow *pb.FlowRef, message *pb.SipMessage, status uint32, parsed sip.Message, nativeMessage portable.Message) error {
	if flow == nil || message == nil || status < 300 || status > 699 || !sameProcess(flow.Ingress, e.cfg.Process) {
		return errors.New("invalid native rejection")
	}
	request, ok := parsed.(*sip.Request)
	if !ok || request.To() == nil || request.CallID() == nil {
		return errors.New("invalid rejection request")
	}
	if request.IsAck() {
		return nil
	}
	if tag(request.To().Params) != "" {
		return errors.New("tagged rejection requires retained context")
	}
	var err error
	if nativeMessage.IsZero() {
		nativeMessage, err = portable.FreezeMessage(message.Wire, parsed, flow.Transport, "", "")
	} else {
		_, err = nativeMessage.Facts()
	}
	if err != nil {
		return err
	}
	if err := validateObservedMessage(parsed); err != nil {
		return err
	}
	selectors := observedMessageSelectors(parsed, false)
	hash := sha256.Sum256(message.Wire)
	id := "refusal-" + fmt.Sprintf("%x", hash[:24])
	ref := e.contextRef(request.CallID().Value(), id)
	e.mu.Lock()
	existing := e.contexts[id]
	for _, selector := range selectors {
		if selected := e.transactions[selector]; selected != nil && selected != existing {
			e.mu.Unlock()
			return errors.New("transaction already admitted")
		}
	}
	e.mu.Unlock()
	if existing != nil {
		existing.mu.Lock()
		if !e.residentContextCurrentLocked(existing, ref) {
			existing.mu.Unlock()
			return errors.New("refusal context changed")
		}
		tx := existing.transactions[existing.requests["refusal"]]
		if tx != nil && tx.retired && tx.wireHash == hash && proto.Equal(tx.flow, flow) {
			existing.mu.Unlock()
			return nil
		}
		if tx == nil || tx.retired || !proto.Equal(tx.flow, flow) || !portable.MatchRequest(tx.native.Origin, nativeMessage) {
			existing.mu.Unlock()
			return errors.New("refusal identity mismatch")
		}
		next, effects, stepErr := portable.StepTransaction(tx.native, portable.Input{Kind: portable.ReceiveRequest, Message: nativeMessage, Now: time.Now().UnixMilli()})
		if stepErr != nil {
			existing.mu.Unlock()
			return stepErr
		}
		effects = residentSuppressUncertainReplayLocked(existing, tx, effects)
		delta, borrowed, stageErr := e.stageResidentTransactionBytesLocked(existing, tx, next)
		if stageErr != nil {
			existing.mu.Unlock()
			return stageErr
		}
		writes, stageErr := e.stageResidentOutputsLocked(existing, tx, effects, nil, "", time.Now().Add(2*time.Second))
		if stageErr != nil {
			e.abortResidentTransactionBytesLocked(existing, tx, delta, borrowed)
			existing.mu.Unlock()
			return stageErr
		}
		tx.native = next
		e.commitResidentTransactionBytesLocked(existing, tx, delta, borrowed)
		_ = e.scheduleResidentTransactionLocked(existing, tx, effects)
		existing.mu.Unlock()
		e.dispatchResident(ctx, writes)
		return nil
	}
	now := time.Now()
	in := &pb.OwnerIngest{Context: ref, Flow: flow, Application: e.cfg.Process, Wire: message.Wire}
	c, tx, effects, err := e.prepareResidentInitialParsed(in, now.Add(2*time.Second), now, request, nativeMessage)
	if err != nil {
		return err
	}
	contact, _, err := advertisement(e.cfg, flow.Transport)
	if err != nil {
		return err
	}
	response, err := portable.BuildResponseWithHeaders(tx.native.Origin, int(status), reason(status), nil, c.snapshot.LocalTag, contact, nil)
	if err != nil {
		return err
	}
	next, responseEffects, err := portable.StepTransaction(tx.native, portable.Input{Kind: portable.SendResponse, Message: response, Now: now.UnixMilli()})
	if err != nil {
		return err
	}
	effects = append(effects, responseEffects...)
	c.requests["refusal"] = tx.native.Key
	tx.native = next
	tx.selectors = append([]string(nil), selectors...)
	tx.charged = residentTransactionBytes(tx)
	c.mu.Lock()
	if err = e.accountResidentLocked(c, residentContextBytes(c)+tx.charged, 0); err != nil {
		c.mu.Unlock()
		return err
	}
	tx.budget, err = e.openResidentBudgetLocked(c, residentSmallBudget)
	if err != nil {
		_ = e.accountResidentLocked(c, -c.bytes, 0)
		c.mu.Unlock()
		return err
	}
	c.budget = tx.budget
	writes, err := e.stageResidentOutputsLocked(c, tx, effects, nil, "", now.Add(2*time.Second))
	if err != nil {
		_ = e.accountResidentLocked(c, -c.bytes, -c.reserved)
		c.mu.Unlock()
		return err
	}
	e.mu.Lock()
	valid := !e.closed && len(e.contexts)+len(e.admissions) < e.maxContexts && e.contexts[id] == nil
	for _, selector := range selectors {
		if e.transactions[selector] != nil {
			valid = false
		}
	}
	if valid {
		e.contexts[id] = c
		for _, selector := range selectors {
			e.transactions[selector] = c
		}
	}
	e.mu.Unlock()
	if !valid {
		_ = e.accountResidentLocked(c, -c.bytes, -c.reserved)
		c.mu.Unlock()
		return errors.New("refusal authority changed")
	}
	tx.native = next
	e.scheduleResidentOutputDeadlinesLocked(c, writes)
	_ = e.scheduleResidentTransactionLocked(c, tx, effects)
	e.retireResidentTransactionLocked(c, tx)
	c.mu.Unlock()
	e.dispatchResident(ctx, writes)
	return nil
}
