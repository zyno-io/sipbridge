package engine

import (
	"errors"
	"time"

	"github.com/emiago/sipgo/sip/portable"
	pb "sipbridge/internal/controlpb"
)

// Immutable holders share their packets with the old dialog. This is one live
// transaction obligation, never a former-dialog snapshot or command history.
type residentSuccessfulACK struct {
	final, ack portable.Message
	flow       *pb.FlowRef
}

func prepareResidentSuccessfulACK(c *residentContext, dialog *residentDialog, nowMS int64) (*residentTransaction, *residentSuccessfulACK, error) {
	if dialog == nil || dialog.localTransaction == "" || dialog.native.LocalInvite == nil {
		return nil, nil, nil
	}
	tx := c.transactions[dialog.localTransaction]
	if tx == nil || tx.retired {
		return nil, nil, errors.New("previous INVITE transaction unavailable")
	}
	local := dialog.native.LocalInvite
	if local.Final != nil && local.ACK != nil && !dialog.ackWritten {
		facts, err := local.Final.Facts()
		if err != nil {
			return nil, nil, err
		}
		if facts.Status >= 200 && facts.Status < 300 {
			return nil, nil, errors.New("previous INVITE ACK not positively written")
		}
	}
	if tx.native.Phase != portable.Accepted || !tx.native.Timers[portable.TimerM].Active || nowMS >= tx.native.Timers[portable.TimerM].At {
		return nil, nil, nil
	}
	if !dialog.ackWritten || local.Final == nil || local.ACK == nil || !validPolicyFlow(dialog.completionFlow) || tx.successfulACK != nil {
		return nil, nil, errors.New("previous INVITE ACK not positively written")
	}
	if _, _, err := nativeSuccessfulACKReplay(tx.native, *local.Final, *local.Final, *local.ACK, nowMS); err != nil {
		return nil, nil, err
	}
	return tx, &residentSuccessfulACK{final: *local.Final, ack: *local.ACK, flow: dialog.completionFlow}, nil
}

func (e *ResidentMemory) replayResidentSuccessfulACKLocked(c *residentContext, tx *residentTransaction, message portable.Message, now time.Time) ([]*pb.FlowWrite, error) {
	saved := tx.successfulACK
	if saved == nil {
		return nil, errors.New("successful INVITE replay unavailable")
	}
	due := tx.native.Timers[portable.TimerM]
	if tx.native.Phase != portable.Accepted || !due.Active || now.UnixMilli() >= due.At {
		return nil, nil
	}
	next, _, err := nativeSuccessfulACKReplay(tx.native, message, saved.final, saved.ack, now.UnixMilli())
	if err != nil {
		return nil, err
	}
	selected := *tx
	selected.native, selected.flow, selected.command = next, saved.flow, nil
	until := minTime(now.Add(2*time.Second), time.UnixMilli(due.At))
	if residentACKAttemptRetained(c, tx, saved.ack) {
		return nil, nil
	}
	effects := []portable.Effect{{Kind: portable.Send, Message: &saved.ack, Replay: true}}
	writes, err := e.stageResidentOutputsLocked(c, &selected, effects, nil, "", until)
	if err != nil {
		return nil, err
	}
	tx.native = next
	return writes, nil
}

// The old transaction remains the exact owner through M and any admitted
// replay proof. Both closers already call the native retirement path.
func (e *ResidentMemory) releaseResidentSuccessfulACKLocked(c *residentContext, tx *residentTransaction) {
	if tx == nil || tx.successfulACK == nil || tx.native.Phase != portable.Terminated {
		return
	}
	if residentACKAttemptRetained(c, tx, tx.successfulACK.ack) {
		return
	}
	before := residentTransactionBytes(tx)
	saved := tx.successfulACK
	tx.successfulACK = nil
	delta := residentTransactionBytes(tx) - before
	released := -delta
	borrowed := min(tx.borrowed, released)
	if err := e.refundResidentBudgetLocked(c, tx.budget, released, borrowed); err != nil {
		tx.successfulACK = saved
		return
	}
	tx.borrowed -= borrowed
	tx.charged += delta
	facts, err := saved.ack.Facts()
	if err == nil && tx.budget != nil {
		delete(tx.budget.acks, residentACKIdentity{local: facts.FromTag, remote: facts.ToTag, sequence: facts.CSeq})
		if len(tx.budget.acks) == 0 {
			tx.budget.acks = nil
		}
	}
}

// A replaced non-2xx client transaction still owns the ACK selected by the
// SDK. It must not feed its response into the newer dialog procedure.
func (e *ResidentMemory) replayResidentSupersededFailureACKLocked(c *residentContext, tx *residentTransaction, next portable.Transaction, effects []portable.Effect, now time.Time) ([]*pb.FlowWrite, error) {
	if tx.native.ACK == nil || tx.native.Phase != portable.Completed {
		return nil, errors.New("previous INVITE ACK unavailable")
	}
	for _, effect := range effects {
		if effect.Kind != portable.Send || !effect.Replay || effect.Message == nil || !effect.Message.EqualWire(*tx.native.ACK) {
			return nil, errors.New("unexpected previous INVITE replay")
		}
	}
	selected := *tx
	selected.native, selected.command = next, nil
	until := now.Add(2 * time.Second)
	if due := tx.native.Timers[portable.TimerD]; due.Active {
		if now.UnixMilli() >= due.At {
			return nil, nil
		}
		until = minTime(until, time.UnixMilli(due.At))
	}
	if residentACKAttemptRetained(c, tx, *tx.native.ACK) {
		return nil, nil
	}
	writes, err := e.stageResidentOutputsLocked(c, &selected, effects, nil, "", until)
	if err != nil {
		return nil, err
	}
	tx.native = next
	return writes, nil
}

// Only this ACK's actual outstanding attempt fences replay. An unrelated
// INVITE or another fork's uncertainty cannot block this native obligation.
func residentACKAttemptRetained(c *residentContext, tx *residentTransaction, ack portable.Message) bool {
	facts, err := ack.Facts()
	if err != nil {
		return true
	}
	identity := drainNativeMessageObservation(ack, facts).ACK
	for _, output := range c.outputs {
		if output.transaction == tx.native.Key && output.method == "ACK" && output.drain.ACK == identity {
			return true
		}
	}
	return false
}
