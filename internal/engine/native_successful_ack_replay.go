package engine

import (
	"errors"

	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
)

// nativeSuccessfulACKReplay verifies the retained successful final and ACK,
// then asks the existing client transaction reducer whether this exact final
// is still a replay under its original M. The returned effects are the SDK's
// own effects; the caller may stage the already-owned ACK only on success.
func nativeSuccessfulACKReplay(tx portable.Transaction, incoming, final, ack portable.Message, nowMS int64) (portable.Transaction, []portable.Effect, error) {
	if tx.Family != portable.InviteClient || tx.Phase != portable.Accepted || nowMS <= 0 || !tx.Timers[portable.TimerM].Active || nowMS >= tx.Timers[portable.TimerM].At || !incoming.EqualWire(final) || final.Transport() != tx.Origin.Transport() || ack.Transport() != tx.Origin.Transport() || !portable.MatchResponse(tx.Origin, incoming) {
		return portable.Transaction{}, nil, errors.New("successful INVITE replay unavailable")
	}
	origin, err := tx.Origin.Facts()
	if err != nil {
		return portable.Transaction{}, nil, err
	}
	response, err := final.Facts()
	if err != nil {
		return portable.Transaction{}, nil, err
	}
	ackFacts, err := ack.Facts()
	if err != nil {
		return portable.Transaction{}, nil, err
	}
	if !origin.IsRequest || origin.Method != sip.INVITE || response.IsRequest || response.Status < 200 || response.Status >= 300 || response.CSeqMethod != sip.INVITE || response.ToTag == "" || !ackFacts.IsRequest || ackFacts.Method != sip.ACK || ackFacts.CSeqMethod != sip.ACK || ackFacts.CSeq != origin.CSeq || ackFacts.CallID != response.CallID || ackFacts.FromTag != response.FromTag || ackFacts.ToTag != response.ToTag {
		return portable.Transaction{}, nil, errors.New("successful INVITE ACK identity mismatch")
	}
	next, effects, err := portable.StepTransaction(tx, portable.Input{Kind: portable.ReceiveResponse, Now: nowMS, Message: incoming})
	if err != nil {
		return portable.Transaction{}, nil, err
	}
	if next.Phase != portable.Accepted || next.Timers[portable.TimerM] != tx.Timers[portable.TimerM] || len(effects) != 1 || effects[0].Kind != portable.ReportResponse || !effects[0].Replay || effects[0].Message == nil || !effects[0].Message.EqualWire(incoming) {
		return portable.Transaction{}, nil, errors.New("unexpected successful INVITE replay transition")
	}
	return next, effects, nil
}
