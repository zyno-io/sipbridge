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
func stateControlPreflightNeeded(in stateEngineInput, c *stateContext) bool {
	if in.Kind != "command" && in.Kind != "ingest" || in.Context == nil {
		return false
	}
	if c == nil || c.Subscription == nil || in.Application == nil || sameProcess(c.Snapshot.Application, in.Application) {
		return false
	}
	if in.Kind == "ingest" && in.Ingest != nil {
		parsed, err := (portable.Message{Wire: in.Ingest.Wire, Transport: in.Ingest.GetFlow().GetTransport()}).Parse()
		if _, response := parsed.(*sip.Response); err == nil && response {
			return false
		}
	}
	return true
}

func stateNonSubscriptionRequest(message portable.Message) bool {
	parsed, err := message.Parse()
	request, ok := parsed.(*sip.Request)
	return err == nil && ok && request.Method != sip.SUBSCRIBE && request.CSeq() != nil && request.CSeq().MethodName == request.Method
}

// This is only a read-cost decision. Begin and the reducer still establish all
// mutation authority from their own fresh state. Never reuse it for receipts.
func stateSkipSubscriptionPreflight(in stateEngineInput, c *stateContext) bool {
	if in.Kind != "ingest" || in.Ingest == nil || in.Ingest.Flow == nil ||
		!stateNonSubscriptionRequest(portable.Message{Wire: in.Ingest.Wire, Transport: in.Ingest.Flow.Transport}) {
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
	if !pending && stateSkipSubscriptionPreflight(in, group.Contexts[in.Context.GetId()]) {
		return nil
	}
	record, group = state.Record{}, stateGroup{}
	_, err = e.preflightSubscriptionController(ctx, ref, in, deadline)
	return err
}
