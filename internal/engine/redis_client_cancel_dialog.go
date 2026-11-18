package engine

import (
	"bytes"
	"context"
	"errors"
	"slices"
	"strconv"
	"time"

	"github.com/emiago/sipgo/sip/portable"
	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/nativepb"
	"sipbridge/internal/rediscore"
	"sipbridge/internal/selectedmessage"
	"sipbridge/internal/storagecodec"
)

// publishRedisCompletedInviteCancel retains the SDK's dialog cancellation
// request. It starts cleanup only if the selected ACK has positive W evidence.
func publishRedisCompletedInviteCancel(ctx context.Context, store *rediscore.Store, key []byte, selected *rediscore.ObservedContext, current *nativepb.Context, dialogField string, dialog nativeDialogRecord, lease rediscore.ApplicationLeaseRef, command *pb.Command, capture redisNotifyWaitCapture, operation string, executor rediscore.TimerAssignment, deadline time.Time) (*pb.OperationResult, error) {
	if store == nil || selected == nil || selected.View == nil || current == nil || dialog.Metadata == nil || dialog.Native.LocalInvite == nil || dialog.Refs.Local == nil || dialog.Refs.Local.Request == "" || dialog.Refs.Local.Final == "" || !slices.Contains(current.Dialogs, dialogField) {
		return nil, errNativeRecord
	}
	fields := []string{}
	for _, refs := range []*nativeInviteRefs{dialog.Refs.Local, dialog.Refs.Remote} {
		if refs != nil {
			for _, field := range []string{refs.Request, refs.Final, refs.ACK} {
				if field != "" && !slices.Contains(fields, field) {
					fields = append(fields, field)
				}
			}
		}
	}
	if dialog.Refs.LocalBye != "" && !slices.Contains(fields, dialog.Refs.LocalBye) {
		fields = append(fields, dialog.Refs.LocalBye)
	}
	wires, err := store.Read(ctx, selected.Identity, fields...)
	if err != nil {
		return nil, err
	}
	if wires.Revision != selected.View.Revision || wires.Timer != selected.View.Timer {
		return nil, rediscore.ErrConflict
	}
	memo, err := selectedmessage.NewMemo(8)
	if err != nil {
		return nil, err
	}
	all := nativeInviteSelection{Request: true, Final: true, ACK: true}
	native, err := dialog.attach(&nativeSelectedMessages{records: wires.Records, memo: memo, provenance: "admitted"}, nativeDialogSelection{Local: all, Remote: all, LocalBye: true})
	if err != nil {
		return nil, err
	}
	if native.LocalInvite == nil {
		return nil, errNativeRecord
	}
	inviteKey, err := native.LocalInvite.Request.ClientTxKey()
	if err != nil {
		return nil, err
	}
	owner, err := store.LookupSelector(ctx, nativeSelector(rediscore.SelectorClientTransaction, inviteKey))
	if err != nil {
		return nil, err
	}
	if owner.Identity != selected.Identity || owner.View.Revision != selected.View.Revision || owner.View.Timer != selected.View.Timer {
		return nil, rediscore.ErrConflict
	}
	if !slices.Contains(current.InitialProcedures, owner.Record) && !slices.Contains(current.NoninitialProcedures, owner.Record) {
		return nil, errNativeRecord
	}
	procedure, err := readPrivateProcedureRecord(owner.View.Records[owner.Record])
	if err != nil {
		return nil, err
	}
	if procedure.Native.Version != portable.Version || procedure.Native.Family != string(portable.InviteClient) || procedure.Native.Key != inviteKey || procedure.Origin != dialog.Refs.Local.Request {
		return nil, errNativeRecord
	}
	if !dialog.Metadata.AckWritten && native.Offer != portable.RemoteOfferInACK {
		evidence := false
		outputView, e := store.Read(ctx, selected.Identity, dialog.Metadata.Outputs...)
		if e != nil {
			return nil, e
		}
		if outputView.Revision != selected.View.Revision || outputView.Timer != selected.View.Timer {
			return nil, rediscore.ErrConflict
		}
		for _, field := range dialog.Metadata.Outputs {
			output, x := decodeNativeOutput(outputView.Records[field])
			if x != nil {
				return nil, x
			}
			if output.Method != "ACK" || output.AckSequence != native.LocalInvite.Sequence || output.Dialog != dialogField || output.Procedure != owner.Record {
				continue
			}
			ref, x := nativeOutputRef(output.Write)
			if x != nil {
				return nil, x
			}
			proof, x := store.InspectOutput(ctx, ref)
			if errors.Is(x, rediscore.ErrAbsent) {
				continue
			}
			if x != nil {
				return nil, x
			}
			if proof.MetadataField != field || proof.MessageField != dialog.Refs.Local.ACK || !bytes.Equal(proof.Metadata, outputView.Records[field]) {
				return nil, rediscore.ErrConflict
			}
			if proof.State == rediscore.OutputPending || proof.State == rediscore.OutputStarted || proof.State == rediscore.OutputUnknown || proof.State == rediscore.OutputWritten {
				evidence = true
			}
		}
		if !evidence {
			return refuseRedisClientCancel(ctx, store, selected, current, lease, command, capture, operation, executor, deadline, errors.New("ACK evidence unavailable"))
		}
	}
	now := time.Now()
	next, effects, err := portable.StepDialog(native, portable.DialogInput{Kind: portable.DialogCancel, Now: now.UnixMilli()})
	if err != nil {
		return refuseRedisClientCancel(ctx, store, selected, current, lease, command, capture, operation, executor, deadline, err)
	}
	for _, effect := range effects {
		if effect.Kind != portable.NeedACK && effect.Kind != portable.NeedBYE {
			return nil, errNativeRecord
		}
	}
	nextContext := proto.Clone(current).(*nativepb.Context)
	nextContext.CommandSequence = command.Sequence
	nextContext.Revision++
	if command.Admission != nil {
		nextContext.DirectoryEpoch = command.Admission.DirectoryEpoch
	}
	candidate, err := buildPrivateDialogRecord(next, dialog.Refs, dialog.Metadata)
	if err != nil {
		return nil, err
	}
	var changes []rediscore.Change
	var outputs []rediscore.OutputIntent
	var selectors []rediscore.SelectorChange
	var timers []rediscore.TimerChange
	if dialog.Metadata.AckWritten && dialog.Refs.LocalBye == "" {
		if !validPolicyFlow(dialog.Metadata.CompletionFlow) {
			return nil, errNativeRecord
		}
		cleanup, e := prepareRedisUACCleanup(ctx, store, selectedUACSuccessACK{capture: nativeOutputCapture{metadata: &nativepb.Output{Dialog: dialogField}}, selected: selected, current: nextContext, procedure: procedure, dialog: candidate}, &pb.FlowWrite{Flow: dialog.Metadata.CompletionFlow}, key, now, deadline)
		if e != nil {
			return nil, e
		}
		changes = append(changes, rediscore.Change{Field: dialogField, Value: cleanup.dialogData})
		changes = append(changes, cleanup.changes...)
		outputs = append(outputs, cleanup.outputs...)
		selectors = append(selectors, cleanup.selectors...)
		timers = append(timers, cleanup.timers...)
	} else {
		data, e := marshalNativeRecord(storagecodec.NativeDialog, candidate)
		if e != nil {
			return nil, e
		}
		changes = append(changes, rediscore.Change{Field: dialogField, Value: data})
	}
	prior, err := decodeNativeResult(selected.View.Records["r:result-"+strconv.FormatUint(command.Sequence-1, 10)])
	if err != nil || prior.Sequence+1 != command.Sequence {
		return nil, errNativeRecord
	}
	previous, err := planRedisDialogPriorReceipt(ctx, store, selected, current, prior)
	if err != nil {
		return nil, err
	}
	timers = append(timers, previous...)
	resultField := "r:result-" + strconv.FormatUint(command.Sequence, 10)
	receipt, err := encodeNativeResult(&nativepb.Result{Certainty: pb.OperationResult_ACCEPTED, CutoffMs: deadline.UnixMilli(), Sequence: command.Sequence, OperationId: operation, Fingerprint: capture.fingerprint[:], DecidingLeaseTerm: lease.Term})
	if err != nil {
		return nil, err
	}
	contextData, err := encodeNativeContext(nextContext)
	if err != nil {
		return nil, err
	}
	changes = append(changes, rediscore.Change{Field: "r:context", Value: contextData}, rediscore.Change{Field: resultField, Value: receipt})
	until := deadline
	if command.Admission != nil {
		until = minTime(until, time.UnixMilli(int64(command.Admission.ExpiresAtMs)))
	}
	target := selected.View.Timer
	if target == (rediscore.TimerAssignment{}) {
		target = executor
	}
	publication := rediscore.Publication{Identity: selected.Identity, Revision: selected.View.Revision, Deadline: until, Application: &lease, SpendReservation: true, Changes: changes, NewOutputs: outputs, Selectors: selectors}
	if len(timers) != 0 {
		publication.Timers = &rediscore.TimerMutation{Expected: selected.View.Timer, Target: target, Changes: timers}
	}
	_, err = store.Commit(ctx, publication)
	if errors.Is(err, rediscore.ErrExpired) && publication.Timers != nil && selected.View.Timer != (rediscore.TimerAssignment{}) && selected.View.Timer != executor && ctx.Err() == nil && time.Now().Before(until) {
		publication.Timers.Target = executor
		_, err = store.Commit(ctx, publication)
	}
	if err != nil {
		return nil, err
	}
	if ctx.Err() != nil || !time.Now().Before(deadline) {
		return nil, rediscore.ErrUnknown
	}
	snapshot, err := nativeProjectSnapshot(nextContext, nil, nil, nil)
	if err != nil {
		return nil, err
	}
	return &pb.OperationResult{Certainty: pb.OperationResult_ACCEPTED, Snapshot: snapshot}, nil
}
