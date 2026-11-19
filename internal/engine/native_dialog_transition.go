package engine

import (
	"errors"

	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
)

// nativeTimeoutReinviteDialog leaves the dialog untouched if its current
// native state no longer accepts the pending request. The transaction still
// owns the single 503 output.
func nativeTimeoutReinviteDialog(previous portable.Dialog, request, response portable.Message, nowMS int64) (*portable.Dialog, []portable.Effect, error) {
	facts, err := request.Facts()
	if err != nil || !facts.IsRequest || facts.Method != sip.INVITE || facts.ToTag == "" {
		return nil, nil, errors.New("in-dialog INVITE required")
	}
	status, err := response.Facts()
	if err != nil || status.IsRequest || status.Status != 503 {
		return nil, nil, errors.New("503 response required")
	}
	received, effects, err := portable.StepDialog(previous, portable.DialogInput{Kind: portable.DialogReceiveRequest, Message: request, Now: nowMS, Offer: facts.BodyLength > 0})
	if err != nil {
		return nil, nil, nil
	}
	if len(effects) != 1 || effects[0].Kind != portable.PresentRequest {
		return nil, nil, errors.New("unexpected INVITE timeout effect")
	}
	finished, effects, err := portable.StepDialog(received, portable.DialogInput{Kind: portable.DialogSendResponse, Message: response, Now: nowMS})
	if err != nil {
		return nil, nil, err
	}
	return &finished, effects, nil
}

// nativeReinviteResponseCandidate is an unpublished established-dialog
// transition. Physical flow authorization, transaction matching, output
// certainty, accounting and publication remain the caller's responsibility.
type nativeReinviteResponseCandidate struct {
	Dialog        portable.Dialog
	DialogEffects []portable.Effect
	ACKEffects    []portable.Effect
}

// nativeReceiveReinviteResponse applies the dialog reducer and, when its
// protocol state permits, constructs the required ACK using the supplied local
// branch derivation. The supplier is called only for a new ACK obligation.
func nativeReceiveReinviteResponse(dialog portable.Dialog, message portable.Message, nowMS int64, ackBranch func() string) (nativeReinviteResponseCandidate, error) {
	next, effects, err := portable.StepDialog(dialog, portable.DialogInput{Kind: portable.DialogReceiveResponse, Now: nowMS, Message: message})
	if err != nil {
		return nativeReinviteResponseCandidate{}, err
	}
	var ackEffects []portable.Effect
	for _, effect := range effects {
		if effect.Kind == portable.Send {
			ackEffects = append(ackEffects, effect)
		}
		if effect.Kind == portable.NeedACK && next.Offer != portable.RemoteOfferInACK {
			if ackBranch == nil {
				return nativeReinviteResponseCandidate{}, errors.New("ACK branch supplier required")
			}
			next, ackEffects, err = portable.StepDialog(next, portable.DialogInput{Kind: portable.DialogLocalACK, Now: nowMS, Branch: ackBranch(), Answer: true})
			if err != nil {
				return nativeReinviteResponseCandidate{}, err
			}
		}
	}
	return nativeReinviteResponseCandidate{Dialog: next, DialogEffects: effects, ACKEffects: ackEffects}, nil
}

// nativeInitialInviteDialogCandidate is the native fork transition only;
// selection, lifecycle publication, capacity and physical writes stay with
// the caller.
type nativeInitialInviteDialogCandidate struct {
	Dialog        portable.Dialog
	DialogEffects []portable.Effect
	ACKEffects    []portable.Effect
}

// nativeReceiveInitialInviteDialog applies the existing initial-fork reducers.
// A branch is derived only when a new ACK is actually required.
func nativeReceiveInitialInviteDialog(origin, response portable.Message, previous *portable.Dialog, cancel bool, nowMS int64, ackBranch func() string) (nativeInitialInviteDialogCandidate, error) {
	var dialog portable.Dialog
	var err error
	if previous == nil {
		dialog, err = portable.NewDialog(portable.UAC, origin, response, portable.DefaultTimers())
	} else {
		dialog = *previous
	}
	if err != nil {
		return nativeInitialInviteDialogCandidate{}, err
	}
	facts, err := response.Facts()
	if err != nil {
		return nativeInitialInviteDialogCandidate{}, err
	}
	if facts.Status < 200 || facts.Status >= 300 {
		return nativeInitialInviteDialogCandidate{Dialog: dialog}, nil
	}
	var effects []portable.Effect
	if previous == nil {
		dialog, effects, err = portable.StepDialog(dialog, portable.DialogInput{Kind: portable.DialogStart, Now: nowMS})
	} else {
		dialog, effects, err = portable.StepDialog(dialog, portable.DialogInput{Kind: portable.DialogReceiveResponse, Now: nowMS, Message: response})
	}
	if err != nil {
		return nativeInitialInviteDialogCandidate{}, err
	}
	dialogEffects := append([]portable.Effect(nil), effects...)
	if cancel && dialog.LocalInvite != nil && !dialog.LocalInvite.CancelRequested {
		dialog, effects, err = portable.StepDialog(dialog, portable.DialogInput{Kind: portable.DialogCancel, Now: nowMS})
		if err != nil {
			return nativeInitialInviteDialogCandidate{}, err
		}
	}
	dialogEffects = append(dialogEffects, effects...)
	var ackEffects []portable.Effect
	for _, effect := range effects {
		if effect.Kind == portable.Send {
			ackEffects = append(ackEffects, effect)
		}
		if effect.Kind == portable.NeedACK && (dialog.Offer != portable.RemoteOfferInACK || cancel) {
			if ackBranch == nil {
				return nativeInitialInviteDialogCandidate{}, errors.New("ACK branch supplier required")
			}
			branch := ackBranch()
			input := portable.DialogInput{Kind: portable.DialogLocalACK, Now: nowMS, Branch: branch, Answer: true}
			if dialog.Offer == portable.RemoteOfferInACK {
				offer, bodyErr := response.Body()
				if bodyErr != nil {
					return nativeInitialInviteDialogCandidate{}, bodyErr
				}
				input.Body = nativeRejectedSDP(offer, branch)
				input.Headers = []portable.Header{{Name: "Content-Type", Value: "application/sdp"}}
			}
			dialog, ackEffects, err = portable.StepDialog(dialog, input)
			if err != nil {
				return nativeInitialInviteDialogCandidate{}, err
			}
		}
	}
	return nativeInitialInviteDialogCandidate{Dialog: dialog, DialogEffects: dialogEffects, ACKEffects: ackEffects}, nil
}

// nativeRemoteByeCandidate contains only unpublished native records and
// effects. The caller supplies admitted flow authority and publishes both
// records and the physical output together.
type nativeRemoteByeCandidate struct {
	Dialog             portable.Dialog
	Transaction        portable.Transaction
	DialogEffects      []portable.Effect
	TransactionEffects []portable.Effect
}

// nativeReceiveRemoteBye composes the existing dialog and server transaction
// reducers. It does not validate a physical flow, open a socket or publish.
func nativeReceiveRemoteBye(dialog portable.Dialog, message portable.Message, localTag string, nowMS int64) (nativeRemoteByeCandidate, error) {
	facts, err := message.Facts()
	if err != nil {
		return nativeRemoteByeCandidate{}, err
	}
	if !facts.IsRequest || facts.Method != sip.BYE {
		return nativeRemoteByeCandidate{}, errors.New("remote BYE required")
	}
	nextDialog, dialogEffects, err := portable.StepDialog(dialog, portable.DialogInput{Kind: portable.DialogReceiveRequest, Message: message, Now: nowMS})
	if err != nil {
		return nativeRemoteByeCandidate{}, err
	}
	tx, effects, err := nativeServerStart(message, localTag, nowMS)
	if err != nil {
		return nativeRemoteByeCandidate{}, err
	}
	for _, effect := range dialogEffects {
		if effect.Kind == portable.Respond && effect.Message != nil {
			var more []portable.Effect
			tx, more, err = nativeServerSelectResponse(tx, *effect.Message, nowMS)
			if err != nil {
				return nativeRemoteByeCandidate{}, err
			}
			effects = append(effects, more...)
			break
		}
	}
	return nativeRemoteByeCandidate{Dialog: nextDialog, Transaction: tx, DialogEffects: dialogEffects, TransactionEffects: effects}, nil
}
