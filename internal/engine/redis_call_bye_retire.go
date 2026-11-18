package engine

import (
	"bytes"
	"context"
	"errors"
	"slices"
	"strings"
	"time"

	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/rediscore"
	"sipbridge/internal/selectedmessage"
	"sipbridge/internal/storagecodec"
)

// retireRedisCallByeKnown consumes or closes one exact definite BYE response.
// UNKNOWN is deliberately outside this path and never implies an outcome.
func retireRedisCallByeKnown(ctx context.Context, store *rediscore.Store, due *rediscore.DueContext, executor rediscore.TimerAssignment, deadline time.Time) error {
	if store == nil || due == nil || due.CheckedAtMS < due.DeadlineMS || !time.Now().Before(deadline) {
		return errNativeRecord
	}
	const outPrefix = "out-call-bye-output-"
	if !strings.HasPrefix(due.Member, outPrefix) {
		return errNativeRecord
	}
	id := strings.TrimPrefix(due.Member, outPrefix)
	if len(id) != 32 {
		return errNativeRecord
	}
	procedureField, originField, responseField := "r:call-bye-"+id, "r:call-bye-origin-"+id, "r:call-bye-final-"+id
	outputField := "r:call-bye-output-" + id
	view, err := store.Read(ctx, due.Identity, "r:context", procedureField, originField, responseField, outputField)
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
	output, err := decodeNativeOutput(view.Records[outputField])
	if err != nil {
		return err
	}
	if current.Context.State == nil || current.Context.Id != due.Identity.Context || current.Context.State.Incarnation != due.Identity.Incarnation || current.Kind != pb.ContextSnapshot_UAS && current.Kind != pb.ContextSnapshot_UAC || output.Method != "BYE" || output.Status != 200 || output.Procedure != procedureField || output.Dialog == "" || !nativeMessageField(output.Dialog) || !proto.Equal(output.Write.Effect.Context, current.Context) || procedure.Native.Family != portable.NonInviteServer || procedure.Refs.Origin != originField || procedure.Refs.LastResponse != responseField || !proto.Equal(procedure.Metadata.Flow, output.Write.Flow) {
		return errNativeRecord
	}
	procedureIndex, found := slices.BinarySearch(current.NoninitialProcedures, procedureField)
	if !found || !slices.Contains(current.Dialogs, output.Dialog) {
		return errNativeRecord
	}
	ref, err := nativeOutputRef(output.Write)
	if err != nil {
		return err
	}
	proof, err := store.InspectOutput(ctx, ref)
	if err == nil {
		if proof.MetadataField != outputField || !bytes.Equal(proof.Metadata, view.Records[outputField]) {
			return rediscore.ErrConflict
		}
		if proof.State != rediscore.OutputWritten && proof.State != rediscore.OutputNotWritten {
			return rediscore.ErrUnknown
		}
		capture := &nativeOutputCapture{ref: ref, proof: proof, metadata: output}
		if err = settleRedisCallByeOutputAndRedrive(ctx, store, output.Write, capture, executor, deadline); err != nil {
			return err
		}
		if !proof.Acknowledged {
			return acknowledgeRedisCallByeOutput(ctx, store, output.Write, deadline)
		}
		return nil
	}
	if !errors.Is(err, rediscore.ErrAbsent) {
		return err
	}
	if !procedure.Metadata.OutputConsumed {
		return errors.New("BYE proof absence lacks consumed marker")
	}
	memo, err := selectedmessage.NewMemo(2)
	if err != nil {
		return err
	}
	tx, err := procedure.attach(&nativeSelectedMessages{records: view.Records, memo: memo, provenance: "admitted"}, true, true, false)
	if err != nil {
		return err
	}
	origin, err := tx.Origin.Facts()
	if err != nil || !origin.IsRequest || origin.Method != sip.BYE || tx.LastResponse == nil || !portable.MatchResponse(tx.Origin, *tx.LastResponse) {
		return errNativeRecord
	}
	if tx.Phase == portable.Completed && !tx.Reliable {
		j := tx.Timers[portable.TimerJ]
		if !j.Active || j.Generation == 0 || j.At <= 0 {
			return errNativeRecord
		}
		if due.CheckedAtMS < j.At {
			_, err = store.Commit(ctx, rediscore.Publication{Identity: due.Identity, Revision: due.Revision, Deadline: deadline, SpendReservation: true, Timers: &rediscore.TimerMutation{Expected: due.Assignment, Target: due.Assignment, RequireLease: true, Changes: []rediscore.TimerChange{{Member: due.Member, Due: time.UnixMilli(j.At)}}}})
			return err
		}
		tx, _, err = portable.StepTransaction(tx, portable.Input{Kind: portable.TimerFired, Timer: portable.TimerJ, Generation: j.Generation, Now: due.CheckedAtMS})
		if err != nil {
			return err
		}
	}
	if tx.Phase != portable.Terminated {
		return errors.New("BYE transaction still active")
	}
	for _, timer := range tx.Timers {
		if timer.Active {
			return errors.New("BYE timer still active")
		}
	}
	dialogView, err := store.Read(ctx, due.Identity, output.Dialog)
	if err != nil {
		return err
	}
	if dialogView.Revision != view.Revision || dialogView.Timer != view.Timer {
		return rediscore.ErrConflict
	}
	dialog, err := readPrivateDialogRecord(dialogView.Records[output.Dialog])
	if err != nil {
		return err
	}
	index, found := slices.BinarySearch(dialog.Outputs, outputField)
	if !found {
		return errNativeRecord
	}
	dialog.Outputs = append(dialog.Outputs[:index], dialog.Outputs[index+1:]...)
	dialogData, err := marshalNativeRecord(storagecodec.NativeDialog, dialog)
	if err != nil {
		return err
	}
	selectors, err := nativeSelectorIDs(tx.Origin, false, false)
	if err != nil || len(selectors) == 0 || selectors[0].Kind != rediscore.SelectorServerTransaction {
		return errNativeRecord
	}
	physical, err := tx.LastResponse.WithObservedResponseFlow(output.Write.Flow.SourceIp, int(output.Write.Flow.SourcePort))
	if err != nil {
		return err
	}
	current.NoninitialProcedures = slices.Delete(current.NoninitialProcedures, procedureIndex, procedureIndex+1)
	contextData, err := encodeNativeContext(current)
	if err != nil {
		return err
	}
	changes := []rediscore.Change{{Field: "r:context", Value: contextData}, {Field: output.Dialog, Value: dialogData}, {Field: procedureField, Delete: true}, {Field: originField, Delete: true}, {Field: responseField, Delete: true}, {Field: outputField, Delete: true}}
	if !physical.EqualWire(*tx.LastResponse) {
		changes = append(changes, rediscore.Change{Field: "r:call-bye-wire-" + id, Delete: true})
	}
	timers := []rediscore.TimerChange{{Member: due.Member, Delete: true}, {Member: "call-tx-" + id + "-J", Delete: true}}
	if current.Ended {
		if current.CallRetireAtMs <= 0 {
			return errNativeRecord
		}
		timers = append(timers, rediscore.TimerChange{Member: "call-retire", Due: time.UnixMilli(current.CallRetireAtMs)})
	}
	_, err = store.Commit(ctx, rediscore.Publication{Identity: due.Identity, Revision: due.Revision, Deadline: deadline, SpendReservation: true, Changes: changes, Selectors: []rediscore.SelectorChange{{Selector: selectors[0], Record: procedureField, Delete: true}}, Timers: &rediscore.TimerMutation{Expected: due.Assignment, Target: due.Assignment, RequireLease: true, Changes: timers}})
	return err
}

// fireRedisCallByeJ terminates one exact UDP BYE server transaction. It never
// resends on a timer; a consumed output receives its existing cleanup re-drive.
func fireRedisCallByeJ(ctx context.Context, store *rediscore.Store, due *rediscore.DueContext, deadline time.Time) error {
	const prefix = "call-tx-"
	if store == nil || due == nil || !strings.HasPrefix(due.Member, prefix) || !strings.HasSuffix(due.Member, "-J") || due.CheckedAtMS < due.DeadlineMS || !time.Now().Before(deadline) {
		return errNativeRecord
	}
	id := strings.TrimSuffix(strings.TrimPrefix(due.Member, prefix), "-J")
	if len(id) != 32 {
		return errNativeRecord
	}
	procedureField, originField, responseField, outputField := "r:call-bye-"+id, "r:call-bye-origin-"+id, "r:call-bye-final-"+id, "r:call-bye-output-"+id
	view, err := store.Read(ctx, due.Identity, "r:context", procedureField, originField, responseField, outputField)
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
	if current.Context.State == nil || current.Context.Id != due.Identity.Context || current.Context.State.Incarnation != due.Identity.Incarnation || current.Kind != pb.ContextSnapshot_UAS && current.Kind != pb.ContextSnapshot_UAC || !slices.Contains(current.NoninitialProcedures, procedureField) {
		return errNativeRecord
	}
	procedure, err := decodeNativeProcedure(view.Records[procedureField])
	if err != nil {
		return err
	}
	output, err := decodeNativeOutput(view.Records[outputField])
	if err != nil {
		return err
	}
	timer := procedure.Native.Timers[portable.TimerJ]
	if procedure.Native.Family != portable.NonInviteServer || procedure.Native.Reliable || procedure.Native.Phase != portable.Completed || procedure.Refs.Origin != originField || procedure.Refs.LastResponse != responseField || !timer.Active || timer.Generation == 0 || timer.At != due.DeadlineMS || output.Method != "BYE" || output.Status != 200 || output.Procedure != procedureField || output.Dialog == "" {
		return errNativeRecord
	}
	dialogView, err := store.Read(ctx, due.Identity, output.Dialog)
	if err != nil {
		return err
	}
	if dialogView.Revision != view.Revision || dialogView.Timer != view.Timer {
		return rediscore.ErrConflict
	}
	dialog, err := readPrivateDialogRecord(dialogView.Records[output.Dialog])
	if err != nil || !slices.Contains(dialog.Outputs, outputField) || !slices.Contains(current.Dialogs, output.Dialog) {
		return errNativeRecord
	}
	memo, err := selectedmessage.NewMemo(2)
	if err != nil {
		return err
	}
	tx, err := procedure.attach(&nativeSelectedMessages{records: view.Records, memo: memo, provenance: "admitted"}, true, true, false)
	if err != nil {
		return err
	}
	facts, err := tx.Origin.Facts()
	if err != nil || !facts.IsRequest || facts.Method != sip.BYE || tx.LastResponse == nil || !portable.MatchResponse(tx.Origin, *tx.LastResponse) {
		return errNativeRecord
	}
	next, effects, err := portable.StepTransaction(tx, portable.Input{Kind: portable.TimerFired, Timer: portable.TimerJ, Generation: timer.Generation, Now: due.CheckedAtMS})
	if err != nil {
		return err
	}
	if next.Phase != portable.Terminated {
		return errNativeRecord
	}
	for _, effect := range effects {
		if effect.Kind != portable.Ended && effect.Kind != portable.CancelTimer {
			return errors.New("unexpected BYE Timer J effect")
		}
	}
	data, err := encodeNativeProcedure(next, procedure.Refs, procedure.Metadata)
	if err != nil {
		return err
	}
	timers := []rediscore.TimerChange{{Member: due.Member, Delete: true}}
	if procedure.Metadata.OutputConsumed {
		original := output.Write.Permit.Authorization.DeadlineMs
		if original == 0 || original > 1<<53-1 {
			return errNativeRecord
		}
		timers = append(timers, rediscore.TimerChange{Member: "out-call-bye-output-" + id, Due: time.UnixMilli(int64(original))})
	}
	_, err = store.Commit(ctx, rediscore.Publication{Identity: due.Identity, Revision: due.Revision, Deadline: deadline, SpendReservation: true, Changes: []rediscore.Change{{Field: procedureField, Value: data}}, Timers: &rediscore.TimerMutation{Expected: due.Assignment, Target: due.Assignment, RequireLease: true, Changes: timers}})
	return err
}
