package engine

import (
	"context"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/rediscore"
	"sipbridge/internal/selectedmessage"
	"sipbridge/internal/storagecodec"
)

// fireRedisDialogInviteTrying keeps a pending 100 private until admission.
// An admitted in-dialog INVITE stages it on the retained transaction flow.
func fireRedisDialogInviteTrying(ctx context.Context, store *rediscore.Store, key []byte, due *rediscore.DueContext, requestID string, deadline time.Time) error {
	procedureField, originField := "r:procedure-"+requestID, "r:origin-"+requestID
	responseField, physicalField, outputField := "r:dialog-response-"+requestID+"-0", "r:dialog-wire-"+requestID+"-0", "r:dialog-output-"+requestID+"-0"
	view, err := store.Read(ctx, due.Identity, "r:context", procedureField, originField, responseField, physicalField, outputField)
	if err != nil {
		return err
	}
	if view.Revision != due.Revision || view.Timer != due.Assignment {
		return rediscore.ErrConflict
	}
	current, err := decodeNativeContext(view.Records["r:context"])
	if err != nil {
		return err
	}
	procedure, err := decodeNativeProcedure(view.Records[procedureField])
	if err != nil {
		return err
	}
	if !procedure.Metadata.DecisionPending {
		text := strings.TrimPrefix(procedure.Metadata.CommandResult, "r:result-")
		sequence, e := strconv.ParseUint(text, 10, 53)
		if e != nil || sequence == 0 || text != strconv.FormatUint(sequence, 10) || procedure.Metadata.CommandResult != "r:result-"+text {
			return errNativeRecord
		}
		responseField, physicalField, outputField = "r:dialog-response-"+requestID+"-"+text, "r:dialog-wire-"+requestID+"-"+text, "r:dialog-output-"+requestID+"-"+text
		extra, e := store.Read(ctx, due.Identity, responseField, physicalField, outputField)
		if e != nil {
			return e
		}
		if extra.Revision != view.Revision || extra.Timer != view.Timer {
			return rediscore.ErrConflict
		}
		for field, data := range extra.Records {
			view.Records[field] = data
		}
	}
	timer := procedure.Native.Timers[portable.TimerTrying]
	if current.Context == nil || current.Context.State == nil || current.Context.Id != due.Identity.Context || current.Context.State.Incarnation != due.Identity.Incarnation || current.Kind != pb.ContextSnapshot_UAS && current.Kind != pb.ContextSnapshot_UAC || !slices.Contains(current.NoninitialProcedures, procedureField) || procedure.Native.Version != portable.Version || procedure.Native.Family != portable.InviteServer || !procedure.Metadata.Direct || procedure.Metadata.RequestId != requestID || procedure.Refs.Origin != originField || procedure.Refs.LastResponse != "" || procedure.Metadata.Emissions != 0 || len(view.Records[responseField])+len(view.Records[physicalField])+len(view.Records[outputField]) != 0 || !timer.Active || timer.Generation == 0 || timer.At != due.DeadlineMS {
		return errNativeRecord
	}
	memo, err := selectedmessage.NewMemo(1)
	if err != nil {
		return err
	}
	tx, err := procedure.attach(&nativeSelectedMessages{records: view.Records, memo: memo, provenance: "admitted"}, true, false, false)
	if err != nil {
		return err
	}
	facts, err := tx.Origin.Facts()
	if err != nil || !facts.IsRequest || facts.Method != sip.INVITE || facts.ToTag == "" {
		return errNativeRecord
	}
	next, effects, err := portable.StepTransaction(tx, portable.Input{Kind: portable.TimerFired, Now: due.CheckedAtMS, Timer: portable.TimerTrying, Generation: timer.Generation})
	if err != nil {
		return err
	}
	if len(effects) != 1 || effects[0].Kind != portable.Send || effects[0].Message == nil || next.LastResponse == nil || !effects[0].Message.EqualWire(*next.LastResponse) {
		return errNativeRecord
	}
	response, err := next.LastResponse.Facts()
	if err != nil || response.IsRequest || response.Status != 100 || response.CSeqMethod != sip.INVITE {
		return errNativeRecord
	}
	refs := procedure.Refs
	refs.LastResponse = responseField
	var changes []rediscore.Change
	var outputs []rediscore.OutputIntent
	timers := []rediscore.TimerChange{{Member: due.Member, Delete: true}}
	shared := false
	if !procedure.Metadata.DecisionPending {
		ids, e := nativeSelectorIDs(tx.Origin, false, false)
		if e != nil || len(ids) != 2 || ids[1].Kind != rediscore.SelectorDialog {
			return errNativeRecord
		}
		dialogView, e := store.LookupSelector(ctx, ids[1])
		if e != nil {
			return e
		}
		if dialogView.Identity != due.Identity || dialogView.View.Revision != due.Revision || dialogView.View.Timer != due.Assignment {
			return rediscore.ErrConflict
		}
		dialog, e := readPrivateDialogRecord(dialogView.View.Records[dialogView.Record])
		if e != nil || !slices.Contains(current.Dialogs, dialogView.Record) || dialog.RemoteInvite == nil || dialog.RemoteInvite.Request != originField || slices.Contains(dialog.Outputs, outputField) {
			return errNativeRecord
		}
		snapshot, e := nativeProjectSnapshot(current, nil, nil, nil)
		if e != nil {
			return e
		}
		until := minTime(deadline, time.Now().Add(2*time.Second))
		intent, more, same, e := prepareRedisCallResponseOutput("INVITE", 100, snapshot, procedure.Metadata.Flow, key, *next.LastResponse, procedureField, dialogView.Record, responseField, physicalField, outputField, until, false)
		if e != nil {
			return e
		}
		shared = same
		changes = append(changes, more...)
		outputs = append(outputs, intent)
		procedure.Metadata.Emissions++
		procedure.Metadata.ProvisionalEmissions++
		procedure.Metadata.Outputs = append(procedure.Metadata.Outputs, outputField)
		slices.Sort(procedure.Metadata.Outputs)
		dialog.Outputs = append(dialog.Outputs, outputField)
		slices.Sort(dialog.Outputs)
		data, e := marshalNativeRecord(storagecodec.NativeDialog, dialog)
		if e != nil {
			return e
		}
		changes = append(changes, rediscore.Change{Field: dialogView.Record, Value: data})
		timers = append(timers, rediscore.TimerChange{Member: "out-" + strings.TrimPrefix(outputField, "r:"), Due: until})
	}
	if !shared {
		data, e := selectedmessage.Marshal(*next.LastResponse)
		if e != nil {
			return e
		}
		changes = append(changes, rediscore.Change{Field: responseField, Value: data})
	}
	data, err := encodeNativeProcedure(next, refs, procedure.Metadata)
	if err != nil {
		return err
	}
	changes = append(changes, rediscore.Change{Field: procedureField, Value: data})
	_, err = store.Commit(ctx, rediscore.Publication{Identity: due.Identity, Revision: due.Revision, Deadline: deadline, SpendReservation: true, Changes: changes, NewOutputs: outputs, Timers: &rediscore.TimerMutation{Expected: due.Assignment, Target: due.Assignment, RequireLease: true, Changes: timers}})
	return err
}
