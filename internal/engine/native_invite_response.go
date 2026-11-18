package engine

import (
	"errors"

	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
	pb "sipbridge/internal/controlpb"
)

// nativeInviteResponseCandidate is an unpublished initial UAS response. The
// transaction and dialog effects remain separate: the caller emits the first
// physical response only from the transaction effects.
type nativeInviteResponseCandidate struct {
	Message       portable.Message
	Transaction   portable.Transaction
	Effects       []portable.Effect
	Dialog        *portable.Dialog
	DialogEffects []portable.Effect
}

func nativePrepareInitialInviteResponse(transaction portable.Transaction, response *pb.Respond, localTag, contact, sentBy string, nowMS int64) (nativeInviteResponseCandidate, error) {
	var result nativeInviteResponseCandidate
	if response == nil || transaction.Family != portable.InviteServer || localTag == "" || nowMS <= 0 || response.Status < 100 || response.Status > 699 {
		return result, errors.New("invalid initial INVITE response")
	}
	facts, err := transaction.Origin.Facts()
	if err != nil || !facts.IsRequest || facts.Method != sip.INVITE || facts.ToTag != "" {
		return result, errors.New("initial INVITE origin required")
	}
	contact, err = initialInviteContact(transaction.Origin, int(response.Status), contact)
	if err != nil {
		return result, err
	}
	message, err := portable.BuildResponseWithHeaders(transaction.Origin, int(response.Status), reason(response.Status), response.Body, localTag, contact, stateHeaders(response.Headers))
	if err != nil {
		return result, err
	}
	next, effects, err := nativeServerSelectResponse(transaction, message, nowMS)
	if err != nil {
		return result, err
	}
	result.Message, result.Transaction, result.Effects = message, next, effects
	if response.Status >= 200 && response.Status < 300 {
		dialog, e := portable.NewDialog(portable.UAS, transaction.Origin, message, portable.DefaultTimers())
		if e != nil {
			return nativeInviteResponseCandidate{}, e
		}
		dialog.SentBy = sentBy
		dialog, result.DialogEffects, e = portable.StepDialog(dialog, portable.DialogInput{Kind: portable.DialogSendResponse, Now: nowMS, Message: message, Answer: len(response.Body) > 0})
		if e != nil {
			return nativeInviteResponseCandidate{}, e
		}
		result.Dialog = &dialog
	}
	return result, nil
}
