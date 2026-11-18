package engine

import (
	"context"
	"slices"
	"strings"
	"time"

	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
	"github.com/google/uuid"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/rediscore"
	"sipbridge/internal/selectedmessage"
	"sipbridge/internal/storagecodec"
)

func timeoutRedisDialogServer(ctx context.Context, store *rediscore.Store, key []byte, advertise nativeSIPAdvertisement, due *rediscore.DueContext, deadline time.Time) error {
	const prefix = "call-decision-"
	if store == nil || due == nil || !strings.HasPrefix(due.Member, prefix) || due.CheckedAtMS < due.DeadlineMS || !time.Now().Before(deadline) {
		return errNativeRecord
	}
	id := strings.TrimPrefix(due.Member, prefix)
	parsed, err := uuid.Parse(id)
	if err != nil || parsed.String() != id {
		return errNativeRecord
	}
	procedureField, originField := "r:procedure-"+id, "r:origin-"+id
	outputField, responseField := "r:dialog-output-"+id+"-0", "r:dialog-response-"+id+"-0"
	view, err := store.Read(ctx, due.Identity, "r:context", procedureField, originField, outputField)
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
	if slices.Contains(current.InitialProcedures, procedureField) {
		return timeoutRedisInitialInvite(ctx, store, key, advertise, due, deadline, current, procedure, view, id)
	}
	dialogField := procedure.Metadata.RequestDialog
	if current.Context == nil || current.Context.State == nil || current.Context.Id != due.Identity.Context || current.Context.State.Incarnation != due.Identity.Incarnation || current.Kind != pb.ContextSnapshot_UAS && current.Kind != pb.ContextSnapshot_UAC || !slices.Contains(current.NoninitialProcedures, procedureField) || !slices.Contains(current.Dialogs, dialogField) || procedure.Native.Family != portable.NonInviteServer && procedure.Native.Family != portable.InviteServer || !procedure.Metadata.Direct || !procedure.Metadata.DecisionPending || procedure.Metadata.RequestId != id || procedure.Metadata.PresentDeadlineMs != due.DeadlineMS || procedure.Refs.Origin != originField || len(view.Records[outputField]) != 0 {
		return errNativeRecord
	}
	dialogView, err := store.Read(ctx, due.Identity, dialogField)
	if err != nil {
		return err
	}
	if dialogView.Revision != due.Revision || dialogView.Timer != due.Assignment {
		return rediscore.ErrConflict
	}
	dialog, err := readPrivateDialogRecord(dialogView.Records[dialogField])
	if err != nil {
		return err
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
	if err != nil || facts.Method != sip.INVITE && !redisDialogNonInvite(facts.Method) || facts.Method == sip.INVITE && procedure.Native.Family != portable.InviteServer || facts.Method != sip.INVITE && procedure.Native.Family != portable.NonInviteServer {
		return errNativeRecord
	}
	now := time.Now()
	until := minTime(deadline, now.Add(2*time.Second))
	contact, _, err := advertise.resolve(tx.Origin.Transport())
	if err != nil {
		return err
	}
	response, err := nativeServerBuildResponseForFlow(tx, 503, reason(503), nil, current.LocalTag, contact, nil, procedure.Metadata.Flow, now.UnixMilli())
	if err != nil {
		return err
	}
	snapshot, err := nativeProjectSnapshot(current, nil, nil, nil)
	if err != nil {
		return err
	}
	intent, changes, shared, err := prepareRedisCallResponseOutput(string(facts.Method), 503, snapshot, procedure.Metadata.Flow, key, response.Message, procedureField, dialogField, responseField, "r:dialog-wire-"+id+"-0", outputField, until, false)
	if err != nil {
		return err
	}
	if !shared {
		data, e := selectedmessage.Marshal(response.Message)
		if e != nil {
			return e
		}
		changes = append(changes, rediscore.Change{Field: responseField, Value: data})
	}
	if facts.Method == sip.INVITE {
		native, local, remote, bye := dialog.Native, dialog.LocalInvite, dialog.RemoteInvite, dialog.LocalBye
		record := nativeDialogFromPrivate(dialog)
		dialog.Native, dialog.LocalInvite, dialog.RemoteInvite, dialog.LocalBye = native, local, remote, bye
		fields := []string{}
		for _, refs := range []*nativeInviteRefs{record.Refs.Local, record.Refs.Remote} {
			if refs != nil && refs.Request != originField && !slices.Contains(fields, refs.Request) {
				fields = append(fields, refs.Request)
			}
		}
		if len(fields) != 0 {
			extra, e := store.Read(ctx, due.Identity, fields...)
			if e != nil {
				return e
			}
			if extra.Revision != due.Revision || extra.Timer != due.Assignment {
				return rediscore.ErrConflict
			}
			for field, data := range extra.Records {
				view.Records[field] = data
			}
		}
		dialogMemo, e := selectedmessage.NewMemo(len(fields) + 1)
		if e != nil {
			return e
		}
		messages := &nativeSelectedMessages{records: view.Records, memo: dialogMemo, provenance: "admitted"}
		previous, e := record.attach(messages, nativeDialogSelection{Local: nativeInviteSelection{Request: true}, Remote: nativeInviteSelection{Request: true}})
		if e != nil {
			return e
		}
		changed, dialogEffects, e := nativeTimeoutReinviteDialog(previous, tx.Origin, response.Message, now.UnixMilli())
		if e != nil {
			return e
		}
		if changed != nil {
			for _, effect := range dialogEffects {
				if effect.Kind != portable.Respond {
					return errNativeRecord
				}
			}
			candidate := redisDialogDecision{selected: &rediscore.ObservedContext{Identity: due.Identity, View: view}, current: current, procedure: procedure, dialog: record, privateDialog: dialog, tx: tx, previous: previous}
			replacements, e := planRedisReinviteReplacement(ctx, store, candidate)
			if e != nil {
				return e
			}
			changes = append(changes, replacements...)
			if e = replaceRedisRemoteInvite(dialog, *changed, originField, responseField); e != nil {
				return e
			}
		}
	}
	private := procedure.Metadata
	private.DecisionPending, private.Proposal, private.RequestDialog = false, nil, ""
	private.Emissions++
	private.Outputs = append(private.Outputs, outputField)
	slices.Sort(private.Outputs)
	refs := procedure.Refs
	refs.LastResponse = responseField
	procedureBytes, err := encodeNativeProcedure(response.Transaction, refs, private)
	if err != nil {
		return err
	}
	dialog.Outputs = append(dialog.Outputs, outputField)
	slices.Sort(dialog.Outputs)
	dialogBytes, err := marshalNativeRecord(storagecodec.NativeDialog, dialog)
	if err != nil {
		return err
	}
	timers := []rediscore.TimerChange{{Member: due.Member, Delete: true}, {Member: "out-" + strings.TrimPrefix(outputField, "r:"), Due: until}}
	for _, effect := range response.Effects {
		switch effect.Kind {
		case portable.Send, portable.Ended:
		case portable.Schedule:
			if effect.Timer != portable.TimerG {
				timers = append(timers, rediscore.TimerChange{Member: "call-tx-" + id + "-" + string(effect.Timer), Due: time.UnixMilli(effect.Deadline.At)})
			}
		case portable.CancelTimer:
			if effect.Timer != portable.TimerG {
				timers = append(timers, rediscore.TimerChange{Member: "call-tx-" + id + "-" + string(effect.Timer), Delete: true})
			}
		default:
			return errNativeRecord
		}
	}
	changes = append(changes, rediscore.Change{Field: procedureField, Value: procedureBytes}, rediscore.Change{Field: dialogField, Value: dialogBytes})
	_, err = store.Commit(ctx, rediscore.Publication{Identity: due.Identity, Revision: due.Revision, Deadline: deadline, SpendReservation: true, Changes: changes, NewOutputs: []rediscore.OutputIntent{intent}, Timers: &rediscore.TimerMutation{Expected: due.Assignment, Target: due.Assignment, RequireLease: true, Changes: timers}})
	return err
}

func fireRedisDialogServerJ(ctx context.Context, store *rediscore.Store, due *rediscore.DueContext, deadline time.Time) error {
	if store == nil || due == nil || !strings.HasPrefix(due.Member, "call-tx-") || !strings.HasSuffix(due.Member, "-J") || due.CheckedAtMS < due.DeadlineMS || !time.Now().Before(deadline) {
		return errNativeRecord
	}
	id := strings.TrimSuffix(strings.TrimPrefix(due.Member, "call-tx-"), "-J")
	parsed, err := uuid.Parse(id)
	if err != nil || parsed.String() != id {
		return errNativeRecord
	}
	procedureField := "r:procedure-" + id
	view, err := store.Read(ctx, due.Identity, "r:context", procedureField)
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
	timer := procedure.Native.Timers[portable.TimerJ]
	if current.Context == nil || current.Context.State == nil || current.Context.Id != due.Identity.Context || current.Context.State.Incarnation != due.Identity.Incarnation || current.Kind != pb.ContextSnapshot_UAS && current.Kind != pb.ContextSnapshot_UAC || !slices.Contains(current.NoninitialProcedures, procedureField) || procedure.Native.Family != portable.NonInviteServer || procedure.Native.Reliable || procedure.Native.Phase != portable.Completed || procedure.Metadata.RequestId != id || procedure.Refs.Origin != "r:origin-"+id || !timer.Active || timer.Generation == 0 || timer.At != due.DeadlineMS {
		return errNativeRecord
	}
	fields := []string{procedure.Refs.Origin}
	if procedure.Refs.LastResponse != "" {
		fields = append(fields, procedure.Refs.LastResponse)
	}
	wires, err := store.Read(ctx, due.Identity, fields...)
	if err != nil {
		return err
	}
	if wires.Revision != due.Revision || wires.Timer != due.Assignment {
		return rediscore.ErrConflict
	}
	memo, err := selectedmessage.NewMemo(len(fields))
	if err != nil {
		return err
	}
	tx, err := procedure.attach(&nativeSelectedMessages{records: wires.Records, memo: memo, provenance: "admitted"}, true, true, false)
	if err != nil {
		return err
	}
	facts, err := tx.Origin.Facts()
	if err != nil || !redisDialogNonInvite(facts.Method) {
		return errNativeRecord
	}
	next, effects, err := portable.StepTransaction(tx, portable.Input{Kind: portable.TimerFired, Timer: portable.TimerJ, Generation: timer.Generation, Now: due.CheckedAtMS})
	if err != nil || next.Phase != portable.Terminated {
		return errNativeRecord
	}
	for _, effect := range effects {
		if effect.Kind != portable.Ended && effect.Kind != portable.CancelTimer {
			return errNativeRecord
		}
	}
	data, err := encodeNativeProcedure(next, procedure.Refs, procedure.Metadata)
	if err != nil {
		return err
	}
	timers := []rediscore.TimerChange{{Member: due.Member, Delete: true}}
	if len(procedure.Metadata.Outputs) == 0 {
		timers = append(timers, rediscore.TimerChange{Member: "call-proc-" + id, Due: time.UnixMilli(due.DeadlineMS)})
	}
	if current.Ended && current.CallRetireAtMs > 0 {
		timers = append(timers, rediscore.TimerChange{Member: "call-retire", Due: time.UnixMilli(current.CallRetireAtMs)})
	}
	_, err = store.Commit(ctx, rediscore.Publication{Identity: due.Identity, Revision: due.Revision, Deadline: deadline, SpendReservation: true, Changes: []rediscore.Change{{Field: procedureField, Value: data}}, Timers: &rediscore.TimerMutation{Expected: due.Assignment, Target: due.Assignment, RequireLease: true, Changes: timers}})
	return err
}

func retireRedisDialogServerProcedure(ctx context.Context, store *rediscore.Store, key []byte, due *rediscore.DueContext, deadline time.Time) error {
	const prefix = "call-proc-"
	if store == nil || len(key) < 32 || len(key) > 1024 || due == nil || !strings.HasPrefix(due.Member, prefix) || due.CheckedAtMS < due.DeadlineMS || !time.Now().Before(deadline) {
		return errNativeRecord
	}
	id := strings.TrimPrefix(due.Member, prefix)
	parsed, err := uuid.Parse(id)
	if err != nil || parsed.String() != id {
		return errNativeRecord
	}
	field, origin := "r:procedure-"+id, "r:origin-"+id
	view, err := store.Read(ctx, due.Identity, "r:context", field, origin)
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
	procedure, err := readPrivateProcedureRecord(view.Records[field])
	if err != nil {
		return err
	}
	index := slices.Index(current.NoninitialProcedures, field)
	if current.Context == nil || current.Context.State == nil || current.Context.Id != due.Identity.Context || current.Context.State.Incarnation != due.Identity.Incarnation || current.Kind != pb.ContextSnapshot_UAS && current.Kind != pb.ContextSnapshot_UAC || index < 0 || procedure.Native.Version != portable.Version || procedure.Native.Family != string(portable.NonInviteServer) && procedure.Native.Family != string(portable.InviteServer) || procedure.Native.Phase != string(portable.Terminated) || procedure.RequestId != id || procedure.Origin != origin || procedure.DecisionPending || len(procedure.Outputs) != 0 || procedure.CreditContribution == 0 || procedure.CreditContribution > 1<<53-1 {
		return errNativeRecord
	}
	if len(procedure.Events) != 0 {
		// Each retained event re-drives this exact owner when it closes.
		_, err = store.Commit(ctx, rediscore.Publication{Identity: due.Identity, Revision: due.Revision, Deadline: deadline, Timers: &rediscore.TimerMutation{Expected: due.Assignment, Target: due.Assignment, RequireLease: true, Changes: []rediscore.TimerChange{{Member: due.Member, Delete: true}}}})
		return err
	}
	if procedure.CommandResult != "" {
		resultView, e := store.Read(ctx, due.Identity, procedure.CommandResult)
		if e != nil {
			return e
		}
		if resultView.Revision != due.Revision || resultView.Timer != due.Assignment {
			return rediscore.ErrConflict
		}
		result, e := decodeNativeResult(resultView.Records[procedure.CommandResult])
		if e != nil || result.ServerRequestId != id || result.Pending || result.Output != "" {
			return errNativeRecord
		}
	}
	if procedure.Native.Timers != nil && !nativeDeadlinesInactive(procedure.Native.Timers.Values) {
		return errNativeRecord
	}
	message, err := selectedmessage.DecodeOpaque(view.Records[origin])
	if err != nil {
		return err
	}
	facts, err := message.Facts()
	if err != nil || facts.Method != sip.INVITE && !redisDialogNonInvite(facts.Method) || facts.Method == sip.INVITE && procedure.Native.Family != string(portable.InviteServer) || facts.Method != sip.INVITE && procedure.Native.Family != string(portable.NonInviteServer) {
		return errNativeRecord
	}
	selectors, err := nativeSelectorIDs(message, false, false)
	if err != nil || len(selectors) != 2 || selectors[0].Kind != rediscore.SelectorServerTransaction {
		return errNativeRecord
	}
	if facts.Method == sip.INVITE {
		if current.Context.State.Authority == nil {
			return errNativeRecord
		}
		cancel, e := portable.BuildCancel(message)
		if e != nil {
			return e
		}
		cancelKey, e := cancel.ServerTxKey()
		if e != nil {
			return e
		}
		child := "r:call-cancel-" + redisServerCancelID(key, current.Context.State.Authority.PrimaryId, due.Identity, cancelKey)
		if slices.Contains(current.NoninitialProcedures, child) {
			// Its exact close re-drives this owner; do not spin an overdue head.
			_, e = store.Commit(ctx, rediscore.Publication{Identity: due.Identity, Revision: due.Revision, Deadline: deadline, Timers: &rediscore.TimerMutation{Expected: due.Assignment, Target: due.Assignment, RequireLease: true, Changes: []rediscore.TimerChange{{Member: due.Member, Delete: true}}}})
			return e
		}
	}
	keepOrigin, keepResponse := false, false
	var dialogChange *rediscore.Change
	if facts.Method == sip.INVITE {
		selected, e := store.LookupSelector(ctx, selectors[1])
		if e != nil {
			return e
		}
		if selected.Identity != due.Identity || selected.View.Revision != due.Revision || selected.View.Timer != due.Assignment {
			return rediscore.ErrConflict
		}
		dialog, e := readPrivateDialogRecord(selected.View.Records[selected.Record])
		if e != nil || !slices.Contains(current.Dialogs, selected.Record) {
			return errNativeRecord
		}
		if dialog.Transaction == procedure.Native.Key {
			cleanupID := nativeIdentity(key, "call-ack-timeout-bye", current.Context.State.Authority.PrimaryId, due.Identity.Incarnation, due.Identity.Context, selected.Record)[:32]
			if slices.Contains(current.NoninitialProcedures, "r:call-cleanup-"+cleanupID) {
				// The exact cleanup child re-drives this contribution when closed.
				_, e = store.Commit(ctx, rediscore.Publication{Identity: due.Identity, Revision: due.Revision, Deadline: deadline, Timers: &rediscore.TimerMutation{Expected: due.Assignment, Target: due.Assignment, RequireLease: true, Changes: []rediscore.TimerChange{{Member: due.Member, Delete: true}}}})
				return e
			}
			waiting := dialog.Native.RemoteInvite != nil && dialog.Native.RemoteInvite.State == string(portable.InviteAwaitingACK)
			if !current.Ended || waiting {
				_, e = store.Commit(ctx, rediscore.Publication{Identity: due.Identity, Revision: due.Revision, Deadline: deadline, Timers: &rediscore.TimerMutation{Expected: due.Assignment, Target: due.Assignment, RequireLease: true, Changes: []rediscore.TimerChange{{Member: due.Member, Delete: true}}}})
				return e
			}
			dialog.Transaction = ""
			data, encodeErr := marshalNativeRecord(storagecodec.NativeDialog, dialog)
			if encodeErr != nil {
				return encodeErr
			}
			dialogChange = &rediscore.Change{Field: selected.Record, Value: data}
		}
		keepOrigin = dialog.RemoteInvite != nil && dialog.RemoteInvite.Request == origin
		keepResponse = dialog.RemoteInvite != nil && dialog.RemoteInvite.FinalResponse == procedure.LastResponse
	}
	current.NoninitialProcedures = slices.Delete(current.NoninitialProcedures, index, index+1)
	contextData, err := encodeNativeContext(current)
	if err != nil {
		return err
	}
	changes := []rediscore.Change{{Field: "r:context", Value: contextData}, {Field: field, Delete: true}}
	if !keepOrigin {
		changes = append(changes, rediscore.Change{Field: origin, Delete: true})
	}
	if procedure.LastResponse != "" && !keepResponse {
		changes = append(changes, rediscore.Change{Field: procedure.LastResponse, Delete: true})
	}
	if dialogChange != nil {
		changes = append(changes, *dialogChange)
	}
	timers := []rediscore.TimerChange{{Member: due.Member, Delete: true}}
	if current.Ended && current.CallRetireAtMs > 0 {
		timers = append(timers, rediscore.TimerChange{Member: "call-retire", Due: time.UnixMilli(current.CallRetireAtMs)})
	}
	_, err = store.Commit(ctx, rediscore.Publication{Identity: due.Identity, Revision: due.Revision, Deadline: deadline, CreditDelta: -int64(procedure.CreditContribution), SpendReservation: true, Changes: changes, Selectors: []rediscore.SelectorChange{{Selector: selectors[0], Record: field, Delete: true}}, Timers: &rediscore.TimerMutation{Expected: due.Assignment, Target: due.Assignment, RequireLease: true, Changes: timers}})
	return err
}
