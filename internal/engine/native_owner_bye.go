package engine

import (
	"errors"

	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
)

type nativeOwnerByeCandidate struct {
	Dialog      portable.Dialog
	Transaction portable.Transaction
	Effects     []portable.Effect
}

func nativeOwnerByeEligible(dialog portable.Dialog, ackWritten, timeout bool) bool {
	if (dialog.Phase != portable.DialogConfirmed && !(timeout && dialog.Phase == portable.DialogTerminating)) || dialog.LocalBye != nil || dialog.LocalInvite != nil && !ackWritten {
		return false
	}
	return timeout || dialog.RemoteInvite == nil || dialog.RemoteInvite.State != portable.InviteAwaitingACK
}

// nativePrepareOwnerBye computes the shared owner-loss/ACK-timeout transition
// without allocating authority, publishing state or writing to a socket.
func nativePrepareOwnerBye(dialog portable.Dialog, ackWritten, timeout bool, branch string, nowMS int64) (nativeOwnerByeCandidate, bool, error) {
	var out nativeOwnerByeCandidate
	if !nativeOwnerByeEligible(dialog, ackWritten, timeout) {
		return out, false, nil
	}
	next, effects, err := portable.StepDialog(dialog, portable.DialogInput{Kind: portable.DialogLocalRequest, Now: nowMS, Method: sip.BYE, Branch: branch})
	if err != nil {
		return out, false, err
	}
	var origin *portable.Message
	for _, effect := range effects {
		if effect.Kind == portable.StartTransaction {
			if origin != nil || effect.Message == nil {
				return out, false, errors.New("owner cleanup BYE is ambiguous")
			}
			origin = effect.Message
		}
	}
	if origin == nil {
		return out, false, errors.New("owner cleanup BYE unavailable")
	}
	transaction, err := portable.NewTransaction(portable.NonInviteClient, *origin, portable.DefaultTimers(), "")
	if err != nil {
		return out, false, err
	}
	transaction, txEffects, err := portable.StepTransaction(transaction, portable.Input{Kind: portable.Start, Now: nowMS})
	if err != nil {
		return out, false, err
	}
	return nativeOwnerByeCandidate{Dialog: next, Transaction: transaction, Effects: txEffects}, true, nil
}
