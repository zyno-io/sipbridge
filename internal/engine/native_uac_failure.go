package engine

import (
	"errors"

	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
)

// nativeInitialInviteFailure is an unpublished native response candidate. It
// carries only the SDK transaction and any exact ACK emitted by that step.
type nativeInitialInviteFailure struct {
	Transaction portable.Transaction
	Effects     []portable.Effect
	ACK         *portable.Message
	Replay      bool
}

func nativeReceiveInitialInviteFailure(tx portable.Transaction, response portable.Message, nowMS int64) (nativeInitialInviteFailure, error) {
	facts, err := response.Facts()
	if err != nil {
		return nativeInitialInviteFailure{}, err
	}
	origin, err := tx.Origin.Facts()
	if err != nil {
		return nativeInitialInviteFailure{}, err
	}
	if tx.Family != portable.InviteClient || origin.IsRequest == false || origin.Method != sip.INVITE || origin.ToTag != "" || facts.IsRequest || facts.Status < 300 || !portable.MatchResponse(tx.Origin, response) {
		return nativeInitialInviteFailure{}, errors.New("initial INVITE failure mismatch")
	}
	next, effects, err := portable.StepTransaction(tx, portable.Input{Kind: portable.ReceiveResponse, Now: nowMS, Message: response})
	if err != nil {
		return nativeInitialInviteFailure{}, err
	}
	out := nativeInitialInviteFailure{Transaction: next, Effects: effects}
	for _, effect := range effects {
		if effect.Kind != portable.Send {
			continue
		}
		if effect.Message == nil || out.ACK != nil {
			return nativeInitialInviteFailure{}, errors.New("unexpected INVITE failure outputs")
		}
		ackFacts, e := effect.Message.Facts()
		if e != nil || !ackFacts.IsRequest || ackFacts.Method != sip.ACK || next.ACK == nil || !next.ACK.EqualWire(*effect.Message) {
			return nativeInitialInviteFailure{}, errors.New("invalid native non-2xx ACK")
		}
		ack := *effect.Message
		out.ACK = &ack
		out.Replay = effect.Replay
	}
	return out, nil
}
