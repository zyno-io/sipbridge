package engine

import (
	"errors"

	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
)

// nativePrepareDialogRequest computes the same selected-dialog command
// transition for Memory and selected-record storage. It performs no I/O.
func nativePrepareDialogRequest(dialog portable.Dialog, input portable.DialogInput, transactionNow int64) (portable.Dialog, portable.Transaction, []portable.Effect, error) {
	var empty portable.Transaction
	next, dialogEffects, err := portable.StepDialog(dialog, input)
	if err != nil {
		return portable.Dialog{}, empty, nil, err
	}
	var origin *portable.Message
	for _, effect := range dialogEffects {
		if effect.Kind != portable.StartTransaction || effect.Message == nil || origin != nil {
			return portable.Dialog{}, empty, nil, errors.New("unsupported dialog effects")
		}
		origin = effect.Message
	}
	if origin == nil {
		return portable.Dialog{}, empty, nil, errors.New("dialog transaction unavailable")
	}
	family := portable.NonInviteClient
	if input.Method == sip.INVITE {
		family = portable.InviteClient
	}
	tx, err := portable.NewTransaction(family, *origin, portable.DefaultTimers(), "")
	if err != nil {
		return portable.Dialog{}, empty, nil, err
	}
	tx, effects, err := portable.StepTransaction(tx, portable.Input{Kind: portable.Start, Now: transactionNow})
	if err != nil {
		return portable.Dialog{}, empty, nil, err
	}
	return next, tx, effects, nil
}
