package engine

import (
	"context"
	"errors"
	"time"

	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/state"
)

var errStateControlPreflight = errors.New("control computation requires current authority preflight")

// A CAS already supplies the fresh record needed to decide whether external
// reconciliation/controller work is necessary. No snapshot crosses that work.
func stateControlPreflightNeeded(in stateEngineInput, c *stateContext, prepared ...portable.Message) bool {
	if in.Kind != "command" && in.Kind != "ingest" || in.Context == nil {
		return false
	}
	if c == nil || c.Subscription == nil || in.Application == nil || sameProcess(c.Snapshot.Application, in.Application) {
		return false
	}
	if in.Kind == "ingest" && in.Ingest != nil {
		message := statePreflightMessage(in, prepared)
		facts, err := message.Facts()
		if err == nil && !facts.IsRequest {
			return false
		}
	}
	return true
}

func stateNonSubscriptionRequest(message portable.Message) bool {
	facts, err := message.Facts()
	return err == nil && facts.IsRequest && facts.Method != sip.SUBSCRIBE && facts.CSeqMethod == facts.Method
}

// This is only a read-cost decision. Begin and the reducer still establish all
// mutation authority from their own fresh state. Never reuse it for receipts.
func stateSkipSubscriptionPreflight(in stateEngineInput, c *stateContext, prepared ...portable.Message) bool {
	if in.Kind != "ingest" || in.Ingest == nil || in.Ingest.Flow == nil ||
		!stateNonSubscriptionRequest(statePreflightMessage(in, prepared)) {
		return false
	}
	// An initial SUBSCRIBE is potential subscription work even before a
	// context exists; classify the input before this missing-context shortcut.
	if c == nil {
		return true
	}
	if c.Subscription != nil || c.Snapshot == nil || c.Snapshot.Kind == pb.ContextSnapshot_SUBSCRIPTION {
		return false
	}
	switch c.Snapshot.Kind {
	case pb.ContextSnapshot_TRANSACTION, pb.ContextSnapshot_UAS, pb.ContextSnapshot_UAC:
	default:
		return false
	}
	// A pending SUBSCRIBE can still have a TRANSACTION snapshot before its
	// accepted response installs the subscription. Uncertain origins stay slow.
	if !stateNonSubscriptionRequest(c.Origin) {
		return false
	}
	for _, tx := range c.Transactions {
		if tx == nil || !stateNonSubscriptionRequest(tx.Native.Origin) {
			return false
		}
	}
	return true
}

func statePreflightMessage(in stateEngineInput, prepared []portable.Message) portable.Message {
	if len(prepared) > 0 && !prepared[0].IsZero() {
		return prepared[0]
	}
	return portable.RestoreMessage(in.Ingest.Wire, in.Ingest.GetFlow().GetTransport(), "", "")
}

func (e *StateEngine) preflightControl(ctx context.Context, ref state.Ref, in stateEngineInput, deadline time.Time) error {
	view, _, err := e.store.ReadState(ctx, ref)
	if err != nil {
		return err
	}
	pending := view.HasPending
	record := stateViewProtocolRecord(view)
	view = state.StateView{}
	group, err := decodeStateGroup(record)
	if err != nil {
		return err
	}
	var message portable.Message
	if in.Kind == "ingest" && in.Ingest != nil {
		message = statePreflightMessage(in, nil)
	}
	if !pending && stateSkipSubscriptionPreflight(in, group.Contexts[in.Context.GetId()], message) {
		return nil
	}
	record, group = state.Record{}, stateGroup{}
	_, err = e.preflightSubscriptionController(ctx, ref, in, deadline, message)
	return err
}
