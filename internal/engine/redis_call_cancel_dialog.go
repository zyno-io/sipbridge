package engine

import (
	"context"
	"slices"
	"time"

	"github.com/emiago/sipgo/sip/portable"
	"sipbridge/internal/rediscore"
	"sipbridge/internal/storagecodec"
)

// selectRedisReinviteCancelDialog keeps the response and any accepted offer
// tied to the same exact dialog and revision as the paired transactions.
func selectRedisReinviteCancelDialog(ctx context.Context, store *rediscore.Store, selected *selectedRedisServerCancel, final portable.Message, responseField, outputField string, now time.Time) (string, []byte, error) {
	ids, err := nativeSelectorIDs(selected.nextInvite.Origin, false, false)
	if err != nil || len(ids) != 2 || ids[1].Kind != rediscore.SelectorDialog {
		return "", nil, errNativeRecord
	}
	view, err := store.LookupSelector(ctx, ids[1])
	if err != nil {
		return "", nil, err
	}
	if view.Identity != selected.selected.Identity || view.View.Revision != selected.selected.View.Revision || view.View.Timer != selected.selected.View.Timer {
		return "", nil, rediscore.ErrConflict
	}
	dialog, err := readPrivateDialogRecord(view.View.Records[view.Record])
	if err != nil || !slices.Contains(selected.context.Dialogs, view.Record) || slices.Contains(dialog.Outputs, outputField) {
		return "", nil, errNativeRecord
	}
	if !selected.invite.Metadata.DecisionPending {
		oldNative, oldLocal, oldRemote, oldBye := dialog.Native, dialog.LocalInvite, dialog.RemoteInvite, dialog.LocalBye
		record := nativeDialogFromPrivate(dialog)
		dialog.Native, dialog.LocalInvite, dialog.RemoteInvite, dialog.LocalBye = oldNative, oldLocal, oldRemote, oldBye
		if record.Refs.Remote == nil || record.Refs.Remote.Request != selected.invite.Refs.Origin {
			return "", nil, errNativeRecord
		}
		messages := selected.messages
		if messages == nil {
			return "", nil, errNativeRecord
		}
		if record.Refs.Local != nil && record.Refs.Local.Request != record.Refs.Remote.Request {
			wire, e := store.Read(ctx, view.Identity, record.Refs.Local.Request)
			if e != nil {
				return "", nil, e
			}
			if wire.Revision != view.View.Revision || wire.Timer != view.View.Timer {
				return "", nil, rediscore.ErrConflict
			}
			messages.records[record.Refs.Local.Request] = wire.Records[record.Refs.Local.Request]
		}
		previous, e := record.attach(messages, nativeDialogSelection{Local: nativeInviteSelection{Request: true}, Remote: nativeInviteSelection{Request: true}})
		if e != nil {
			return "", nil, e
		}
		next, effects, e := portable.StepDialog(previous, portable.DialogInput{Kind: portable.DialogSendResponse, Message: final, Now: now.UnixMilli()})
		if e != nil {
			return "", nil, e
		}
		for _, effect := range effects {
			if effect.Kind != portable.Respond {
				return "", nil, errNativeRecord
			}
		}
		if e = replaceRedisRemoteInvite(dialog, next, selected.invite.Refs.Origin, responseField); e != nil {
			return "", nil, e
		}
	}
	dialog.Outputs = append(dialog.Outputs, outputField)
	slices.Sort(dialog.Outputs)
	data, err := marshalNativeRecord(storagecodec.NativeDialog, dialog)
	return view.Record, data, err
}
