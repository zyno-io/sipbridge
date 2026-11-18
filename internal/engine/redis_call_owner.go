package engine

import (
	"context"
	"errors"
	"slices"
	"strings"
	"time"

	"github.com/emiago/sipgo/sip/portable"
	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/nativepb"
	"sipbridge/internal/rediscore"
	"sipbridge/internal/selectedmessage"
)

// releaseExpiredRedisCall stages all eligible native BYEs with the exact
// expired-owner release, while existing ACK/timer duties remain indexed.
func releaseExpiredRedisCall(ctx context.Context, store *rediscore.Store, item rediscore.ExpiredApplicationContext, key []byte, executor rediscore.TimerAssignment, deadline time.Time, selected *redisExpiredOwnerSelection) error {
	if store == nil || selected == nil || selected.identity != item.Identity || selected.view == nil || selected.view.Revision != item.Revision || selected.context == nil || item.Identity.Owner == "" || item.Identity.Term == "" || ctx.Err() != nil || !time.Now().Before(deadline) {
		return errNativeRecord
	}
	current := selected.context
	app, err := nativeApplicationReference(key, current.Application)
	if err != nil || app != item.Identity.Owner || current.ApplicationLeaseTerm != item.Identity.Term || current.Context == nil || current.Context.State == nil || current.Context.Id != item.Identity.Context || current.Context.State.Incarnation != item.Identity.Incarnation || current.Kind != pb.ContextSnapshot_UAS && current.Kind != pb.ContextSnapshot_UAC || current.Revision >= 1<<53-1 {
		return errNativeRecord
	}
	next := proto.Clone(current).(*nativepb.Context)
	next.Revision++
	next.Ended = true
	if next.CallRetireAtMs == 0 {
		next.CallRetireAtMs = time.Now().Add(2 * time.Minute).UnixMilli()
	}
	var changes []rediscore.Change
	var outputs []rediscore.OutputIntent
	var selectors []rediscore.SelectorChange
	timers := []rediscore.TimerChange{{Member: "call-retire", Due: time.UnixMilli(next.CallRetireAtMs)}}
	var dialogs *rediscore.View
	if len(current.Dialogs) != 0 {
		dialogs, err = store.Read(ctx, item.Identity, current.Dialogs...)
		if err != nil {
			return err
		}
		if dialogs.Revision != item.Revision || dialogs.Timer != selected.view.Timer {
			return rediscore.ErrConflict
		}
	}
	for _, field := range current.Dialogs {
		dialog, e := decodeNativeDialog(dialogs.Records[field])
		if e != nil {
			return e
		}
		if dialog.Native.Version != portable.Version || dialog.Native.CallID != current.CallId {
			return errNativeRecord
		}
		if dialog.Refs.LocalBye != "" {
			continue
		}
		lateOffer := dialog.Native.LocalInvite != nil && dialog.Native.LocalInvite.Initial && dialog.Native.LocalInvite.State == portable.InviteAwaitingACK && dialog.Native.Offer == portable.RemoteOfferInACK && dialog.Refs.Local != nil && dialog.Refs.Local.ACK == "" && !dialog.Metadata.AckWritten
		if lateOffer {
			for _, output := range dialog.Metadata.Outputs {
				if strings.HasPrefix(output, "r:uac-dialog-ack-output-") {
					return errNativeRecord
				}
			}
		}
		if !lateOffer && !nativeOwnerByeEligible(dialog.Native, dialog.Metadata.AckWritten, false) {
			if dialog.Native.RemoteInvite != nil && dialog.Native.RemoteInvite.State == portable.InviteAwaitingACK {
				ack := dialog.Native.Timers[portable.TimerACK]
				if !ack.Active || ack.Generation == 0 || ack.At <= 0 {
					return errNativeRecord
				}
				// Reassert the original native deadline in the release CAS so
				// a waiting ACK always has an indexed continuation.
				timers = append(timers, rediscore.TimerChange{Member: callDialogTimerMember(field, portable.TimerACK), Due: time.UnixMilli(ack.At)})
			}
			continue
		}
		fields := []string{}
		for _, refs := range []*nativeInviteRefs{dialog.Refs.Local, dialog.Refs.Remote} {
			if refs == nil {
				continue
			}
			for _, ref := range []string{refs.Request, refs.Final, refs.ACK} {
				if ref != "" && !slices.Contains(fields, ref) {
					fields = append(fields, ref)
				}
			}
		}
		if dialog.Refs.LocalBye != "" && !slices.Contains(fields, dialog.Refs.LocalBye) {
			fields = append(fields, dialog.Refs.LocalBye)
		}
		wires, e := store.Read(ctx, item.Identity, fields...)
		if e != nil {
			return e
		}
		if wires.Revision != item.Revision || wires.Timer != selected.view.Timer {
			return rediscore.ErrConflict
		}
		memo, e := selectedmessage.NewMemo(len(fields))
		if e != nil {
			return e
		}
		messages := &nativeSelectedMessages{records: wires.Records, memo: memo, provenance: "admitted"}
		all := nativeInviteSelection{Request: true, Final: true, ACK: true}
		restored, e := dialog.attach(messages, nativeDialogSelection{Local: all, Remote: all, LocalBye: true})
		if e != nil {
			return e
		}
		if lateOffer {
			terminated, effects, e := portable.StepDialog(restored, portable.DialogInput{Kind: portable.DialogTerminate, Now: time.Now().UnixMilli()})
			if e != nil {
				return e
			}
			for _, effect := range effects {
				switch effect.Kind {
				case portable.Ended:
				case portable.CancelTimer:
					timers = append(timers, rediscore.TimerChange{Member: callDialogTimerMember(field, effect.Timer), Delete: true})
				default:
					return errNativeRecord
				}
			}
			data, e := encodeNativeDialog(terminated, dialog.Refs, dialog.Metadata)
			if e != nil {
				return e
			}
			changes = append(changes, rediscore.Change{Field: field, Value: data})
			continue
		}
		flow := dialog.Metadata.CompletionFlow
		if flow == nil {
			flow = current.Flow
		}
		stage, e := prepareRedisOwnerBye(ctx, store, item.Identity, item.Revision, selected.view.Timer, next, field, restored, dialog.Refs, dialog.Metadata, flow, key, time.Now(), deadline)
		if e != nil {
			return e
		}
		if stage == nil {
			return errNativeRecord
		}
		changes = append(changes, stage.changes...)
		changes = append(changes, rediscore.Change{Field: field, Value: stage.dialog})
		outputs = append(outputs, stage.outputs...)
		selectors = append(selectors, stage.selectors...)
		timers = append(timers, stage.timers...)
	}
	data, err := encodeNativeContext(next)
	if err != nil {
		return err
	}
	target := selected.view.Timer
	if target == (rediscore.TimerAssignment{}) {
		target = executor
	}
	changes = append(changes, rediscore.Change{Field: "r:context", Value: data})
	publication := rediscore.Publication{Identity: item.Identity, Revision: item.Revision, Deadline: deadline, SpendReservation: true, ReleaseApplication: true, Changes: changes, NewOutputs: outputs, Selectors: selectors, Timers: &rediscore.TimerMutation{Expected: selected.view.Timer, Target: target, Changes: timers}}
	_, err = store.Commit(ctx, publication)
	if errors.Is(err, rediscore.ErrExpired) && selected.view.Timer != (rediscore.TimerAssignment{}) && selected.view.Timer != executor && ctx.Err() == nil && time.Now().Before(deadline) {
		publication.Timers.Target = executor
		_, err = store.Commit(ctx, publication)
	}
	return err
}
