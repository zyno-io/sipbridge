package engine

import (
	"github.com/emiago/sipgo/sip/portable"
	"sipbridge/internal/state"
)

// Non-INVITE servers have no autonomous timeout while waiting for an
// application response. Once an unaccepted setup has ended and all its original
// replay/admission windows and external work are gone, polling that Trying
// state cannot make progress. Do not apply this to a dialog/subscription or any
// transaction that selected a response or could still have emitted bytes.
func (r *stateReduction) expireAbandonedSetupTransactions() error {
	if !r.engine.autonomousCompletion() {
		return nil
	}
	for id, c := range r.group.Contexts {
		if !c.Snapshot.Ended || c.Snapshot.Answered || c.RetireMS <= 0 || c.RetireMS > r.now || c.ReplayUntilMS > r.now ||
			c.Dialog != "" || len(c.Dialogs) != 0 || c.Subscription != nil || c.CompletionScope != "" ||
			c.OwnerCleanupAtMS != 0 || c.OwnerProbeEffect != "" || r.requestFence != nil && r.requestFence.context == id {
			continue
		}
		candidate := false
		for _, tx := range c.Transactions {
			if abandonedSetupTransaction(tx, r.now) {
				candidate = true
				break
			}
		}
		if !candidate {
			continue
		}
		pending := false
		for _, effect := range r.payload.Effects {
			var body stateEngineEffect
			if err := body.unmarshalMetadata(effect.Payload); err != nil {
				return state.ErrCorrupt
			}
			if body.Context == id {
				pending = true
				break
			}
		}
		if pending {
			continue
		}
		for _, tx := range c.Transactions {
			if !abandonedSetupTransaction(tx, r.now) {
				continue
			}
			next, effects, err := portable.StepTransaction(tx.Native, portable.Input{Kind: portable.Terminate, Now: r.now})
			if err != nil {
				return err
			}
			for _, effect := range effects {
				if effect.Kind != portable.Ended && effect.Kind != portable.CancelTimer {
					return state.ErrCorrupt
				}
			}
			tx.Native = next
			// The context has already ended and its original retention elapsed.
			// No newly emitted SIP message or event needs another replay window.
			tx.RetireMS = max(tx.RetireMS, c.RetireMS)
		}
	}
	return nil
}

func abandonedSetupTransaction(tx *stateTransaction, now int64) bool {
	if tx.Native.Family != portable.NonInviteServer || tx.Native.Phase != portable.Trying || tx.Native.LastResponse != nil ||
		tx.RetireMS > now || tx.DispositionUntilMS <= 0 || tx.DispositionUntilMS > now || tx.Written || tx.CompletionScope != "" ||
		tx.OwnerDeferred || tx.DeferredPresent || tx.PendingResponse != nil || tx.FinalResponse != nil ||
		tx.DigestEffect != "" || tx.DigestAttempts != 0 || tx.RespondStatus != 0 || tx.RespondDeadlineMS > now {
		return false
	}
	for _, timer := range tx.Native.Timers {
		if timer.Active {
			return false
		}
	}
	return true
}
