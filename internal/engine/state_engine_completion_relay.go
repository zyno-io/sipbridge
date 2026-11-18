package engine

import (
	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
	pb "sipbridge/internal/controlpb"
)

// validateCompletionRelay admits only already-retained native protocol work.
// The submitting application remains the authenticated relay; none of this
// proof changes the context's application or grants business send authority.
// Validate before the normal reducer mutates transaction or dialog state.
func (r *stateReduction) validateCompletionRelay(c *stateContext, message portable.Message, in *pb.OwnerIngest) error {
	reject := func() error {
		return &portable.ProtocolError{Status: 481, Reason: "retained completion identity required"}
	}
	if in.Flow.Transport != c.Snapshot.Transport ||
		c.Snapshot.PrincipalId != "" && in.PrincipalId != "" && c.Snapshot.PrincipalId != in.PrincipalId ||
		c.Snapshot.BindingId != "" && in.BindingId != "" && c.Snapshot.BindingId != in.BindingId {
		return reject()
	}
	facts, err := message.Facts()
	if err != nil {
		return err
	}
	if !facts.IsRequest {
		for _, tx := range c.Transactions {
			if (tx.Native.Family == portable.InviteClient || tx.Native.Family == portable.NonInviteClient) && portable.MatchResponse(tx.Native.Origin, message) {
				// The normal response path validates the replacement flow before
				// recording any response or branch completion evidence.
				return nil
			}
		}
		return reject()
	}
	if facts.Method != sip.ACK && facts.Method != sip.CANCEL && facts.Method != sip.BYE {
		for _, tx := range c.Transactions {
			if stateCompletionFinalReplay(c, tx, message) {
				return r.validateReplayFlow(c, tx, message, in)
			}
		}
		return reject()
	}
	for _, tx := range c.Transactions {
		if tx.Native.Family != portable.InviteServer && tx.Native.Family != portable.NonInviteServer || !portable.MatchRequest(tx.Native.Origin, message) {
			continue
		}
		if facts.Method == sip.CANCEL && tx.Native.Family == portable.InviteServer {
			return r.validateReplayFlow(c, tx, message, in)
		}
		if facts.Method == sip.ACK && tx.Native.Family == portable.InviteServer && tx.Native.LastResponse != nil {
			final, err := tx.Native.LastResponse.Facts()
			if err == nil && !final.IsRequest && final.Status >= 300 && final.ToTag == facts.ToTag {
				return r.validateReplayFlow(c, tx, message, in)
			}
		}
		if facts.Method == sip.BYE && tx.Native.Origin.EqualWire(message) {
			return r.validateReplayFlow(c, tx, message, in)
		}
	}
	if facts.Method == sip.CANCEL {
		return reject()
	}
	key := facts.ToTag + "/" + facts.FromTag
	dialog, found := c.Dialogs[key]
	if !found || !portable.MatchDialogRequest(dialog, message) {
		return reject()
	}
	// StepDialog is a pure reducer over a clone. It proves the exact remote
	// INVITE procedure/CSeq for ACK and the native sequence/state for BYE.
	_, _, err = portable.StepDialog(dialog, portable.DialogInput{Kind: portable.DialogReceiveRequest, Now: r.now, Message: message})
	return err
}

// Only a selected final can be replayed without the original business owner.
// This predicate is also used at dispatch so the accepted request cannot enter
// presentation or dialog reduction, even while the INVITE transaction is Accepted.
func stateCompletionFinalReplay(c *stateContext, tx *stateTransaction, message portable.Message) bool {
	facts, err := message.Facts()
	if err != nil || !facts.IsRequest || c.Subscription != nil || c.Snapshot.Kind != pb.ContextSnapshot_UAS && c.Snapshot.Kind != pb.ContextSnapshot_UAC ||
		facts.Method == sip.REGISTER || facts.Method == sip.SUBSCRIBE || facts.Method == sip.NOTIFY || facts.Method == sip.ACK || facts.Method == sip.CANCEL || facts.Method == sip.BYE ||
		facts.ToTag == "" || facts.FromTag == "" || !stateServerTransaction(tx) || tx.Native.Validate() != nil ||
		!portable.MatchRequest(tx.Native.Origin, message) || !tx.Native.Origin.EqualWire(message) || tx.Native.LastResponse == nil {
		return false
	}
	final, err := tx.Native.LastResponse.Facts()
	return err == nil && !final.IsRequest && final.Status >= 200 && portable.MatchResponse(tx.Native.Origin, *tx.Native.LastResponse)
}
