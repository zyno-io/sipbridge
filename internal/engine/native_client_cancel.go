package engine

import (
	"errors"

	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
)

// nativePairedCancel starts only the actual SDK-emitted paired CANCEL. A
// RequestCancel during Calling deliberately produces no child until a later
// provisional response moves the INVITE to Proceeding.
func nativePairedCancel(invite portable.Transaction, effects []portable.Effect, nowMS int64) (*portable.Transaction, []portable.Effect, error) {
	if invite.Version != portable.Version || invite.Family != portable.InviteClient || nowMS <= 0 {
		return nil, nil, errors.New("INVITE client cancellation required")
	}
	var message *portable.Message
	for _, effect := range effects {
		switch effect.Kind {
		case portable.StartTransaction:
			if effect.Message == nil || message != nil {
				return nil, nil, errors.New("invalid paired CANCEL effect")
			}
			message = effect.Message
		case portable.Send, portable.Respond:
			return nil, nil, errors.New("mixed CANCEL and direct outputs")
		}
	}
	if message == nil {
		return nil, nil, nil
	}
	facts, err := message.Facts()
	if err != nil || !facts.IsRequest || facts.Method != sip.CANCEL || !portable.MatchRequest(invite.Origin, *message) {
		return nil, nil, errors.New("paired CANCEL origin mismatch")
	}
	tx, err := portable.NewTransaction(portable.NonInviteClient, *message, portable.DefaultTimers(), "")
	if err != nil {
		return nil, nil, err
	}
	tx, started, err := portable.StepTransaction(tx, portable.Input{Kind: portable.Start, Now: nowMS})
	if err != nil {
		return nil, nil, err
	}
	return &tx, started, nil
}

func nativeRequestClientCancel(invite portable.Transaction, nowMS int64) (portable.Transaction, []portable.Effect, *portable.Transaction, []portable.Effect, error) {
	if invite.Version != portable.Version || invite.Family != portable.InviteClient || invite.Phase != portable.Calling && invite.Phase != portable.Proceeding && invite.Phase != portable.Completed && invite.Phase != portable.Terminated || nowMS <= 0 {
		return invite, nil, nil, nil, errors.New("cancellable INVITE unavailable")
	}
	next, effects, err := portable.StepTransaction(invite, portable.Input{Kind: portable.RequestCancel, Now: nowMS})
	if err != nil {
		return invite, nil, nil, nil, err
	}
	cancel, started, err := nativePairedCancel(next, effects, nowMS)
	if err != nil {
		return invite, nil, nil, nil, err
	}
	return next, effects, cancel, started, nil
}
