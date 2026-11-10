package engine

import (
	"bytes"

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
	parsed, err := message.Parse()
	if err != nil {
		return err
	}
	if _, response := parsed.(*sip.Response); response {
		for _, tx := range c.Transactions {
			if (tx.Native.Family == portable.InviteClient || tx.Native.Family == portable.NonInviteClient) && portable.MatchResponse(tx.Native.Origin, message) {
				// The normal response path validates the replacement flow before
				// recording any response or branch completion evidence.
				return nil
			}
		}
		return reject()
	}
	request := parsed.(*sip.Request)
	if !request.IsAck() && !request.IsCancel() && request.Method != sip.BYE {
		for _, tx := range c.Transactions {
			if stateCompletionFinalReplay(c, tx, message, request) {
				return r.validateReplayFlow(c, tx, request, in)
			}
		}
		return reject()
	}
	for _, tx := range c.Transactions {
		if tx.Native.Family != portable.InviteServer && tx.Native.Family != portable.NonInviteServer || !portable.MatchRequest(tx.Native.Origin, message) {
			continue
		}
		if request.IsCancel() && tx.Native.Family == portable.InviteServer {
			return r.validateReplayFlow(c, tx, request, in)
		}
		if request.IsAck() && tx.Native.Family == portable.InviteServer && tx.Native.LastResponse != nil {
			final, err := tx.Native.LastResponse.Parse()
			if err == nil && final.(*sip.Response).StatusCode >= 300 && tag(final.To().Params) == tag(request.To().Params) {
				return r.validateReplayFlow(c, tx, request, in)
			}
		}
		if request.Method == sip.BYE && bytes.Equal(tx.Native.Origin.Wire, message.Wire) {
			return r.validateReplayFlow(c, tx, request, in)
		}
	}
	if request.IsCancel() {
		return reject()
	}
	key := tag(request.To().Params) + "/" + tag(request.From().Params)
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
func stateCompletionFinalReplay(c *stateContext, tx *stateTransaction, message portable.Message, request *sip.Request) bool {
	if c.Subscription != nil || c.Snapshot.Kind != pb.ContextSnapshot_UAS && c.Snapshot.Kind != pb.ContextSnapshot_UAC ||
		request.Method == sip.REGISTER || request.Method == sip.SUBSCRIBE || request.Method == sip.NOTIFY || request.IsAck() || request.IsCancel() || request.Method == sip.BYE ||
		tag(request.To().Params) == "" || tag(request.From().Params) == "" || !stateServerTransaction(tx) || tx.Native.Validate() != nil ||
		!portable.MatchRequest(tx.Native.Origin, message) || !bytes.Equal(tx.Native.Origin.Wire, message.Wire) || tx.Native.LastResponse == nil {
		return false
	}
	final, err := tx.Native.LastResponse.Parse()
	response, ok := final.(*sip.Response)
	return err == nil && ok && response.StatusCode >= 200 && portable.MatchResponse(tx.Native.Origin, *tx.Native.LastResponse)
}
