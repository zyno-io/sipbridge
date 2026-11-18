package engine

import (
	"context"
	"errors"
	"slices"
	"strings"
	"time"

	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/nativepb"
	"sipbridge/internal/rediscore"
	"sipbridge/internal/selectedmessage"
)

type uacCleanupCandidate struct {
	dialogData []byte
	changes    []rediscore.Change
	outputs    []rediscore.OutputIntent
	selectors  []rediscore.SelectorChange
	timers     []rediscore.TimerChange
}

// prepareRedisUACCleanup builds the native BYE required after a positively
// written losing ACK. The original INVITE contribution owns this continuation.
func prepareRedisUACCleanup(ctx context.Context, store *rediscore.Store, selected selectedUACSuccessACK, write *pb.FlowWrite, key []byte, now, deadline time.Time) (uacCleanupCandidate, error) {
	var out uacCleanupCandidate
	if len(key) < 32 || len(key) > 1024 || selected.dialog.LocalBye != "" || selected.dialog.LocalInvite == nil {
		return out, errNativeRecord
	}
	record := nativeDialogFromPrivate(selected.dialog)
	fields := []string{record.Refs.Local.Request, record.Refs.Local.Final, record.Refs.Local.ACK}
	if fields[0] == "" || fields[1] == "" || fields[2] == "" {
		return out, errNativeRecord
	}
	view, err := store.Read(ctx, selected.selected.Identity, fields...)
	if errors.Is(err, rediscore.ErrAbsent) {
		return out, rediscore.ErrConflict
	}
	if err != nil {
		return out, err
	}
	if view.Revision != selected.selected.View.Revision || view.Timer != selected.selected.View.Timer {
		return out, rediscore.ErrConflict
	}
	memo, err := selectedmessage.NewMemo(3)
	if err != nil {
		return out, err
	}
	dialog, err := record.attach(&nativeSelectedMessages{records: view.Records, memo: memo, provenance: "admitted"}, nativeDialogSelection{Local: nativeInviteSelection{Request: true, Final: true, ACK: true}})
	if err != nil {
		return out, err
	}
	branch := sip.RFC3261BranchMagicCookie + nativeIdentity(key, "cleanup-bye", selected.current.Context.State.Incarnation, dialog.LocalTag+"/"+dialog.RemoteTag)[:32]
	next, effects, err := portable.StepDialog(dialog, portable.DialogInput{Kind: portable.DialogLocalRequest, Now: now.UnixMilli(), Method: sip.BYE, Branch: branch})
	if err != nil {
		return out, err
	}
	var origin *portable.Message
	for _, effect := range effects {
		if effect.Kind == portable.StartTransaction {
			if effect.Message == nil || origin != nil {
				return out, errNativeRecord
			}
			m := *effect.Message
			origin = &m
		} else if effect.Kind != portable.Ended {
			return out, errNativeRecord
		}
	}
	if origin == nil {
		return out, errors.New("cleanup BYE unavailable")
	}
	tx, err := portable.NewTransaction(portable.NonInviteClient, *origin, portable.DefaultTimers(), "")
	if err != nil {
		return out, err
	}
	tx, txEffects, err := portable.StepTransaction(tx, portable.Input{Kind: portable.Start, Now: now.UnixMilli()})
	if err != nil {
		return out, err
	}
	selectors, err := nativeSelectorIDs(*origin, true, false)
	if err != nil || len(selectors) == 0 || selectors[0].Kind != rediscore.SelectorClientTransaction {
		return out, errNativeRecord
	}
	if len(selected.current.InitialProcedures)+len(selected.current.NoninitialProcedures) >= stateNativeProcedures {
		return out, errNativeRecord
	}
	authority := selected.current.Context.State.Authority
	if authority == nil {
		return out, errNativeRecord
	}
	id := nativeIdentity(key, "uac-cleanup", authority.PrimaryId, selected.current.Context.State.Incarnation, selected.current.Context.Id, selected.capture.metadata.Dialog)[:32]
	procField, originField, outputField := "r:call-cleanup-"+id, "r:call-cleanup-origin-"+id, "r:call-cleanup-output-"+id
	occupied, e := store.Read(ctx, selected.selected.Identity, procField, originField, outputField)
	if e != nil {
		return out, e
	}
	if occupied.Revision != selected.selected.View.Revision || occupied.Timer != selected.selected.View.Timer {
		return out, rediscore.ErrConflict
	}
	if len(occupied.Records[procField])+len(occupied.Records[originField])+len(occupied.Records[outputField]) != 0 {
		return out, rediscore.ErrConflict
	}
	until := minTime(deadline, now.Add(2*time.Second))
	if !now.Before(until) {
		return out, rediscore.ErrExpired
	}
	snapshot, err := nativeProjectSnapshot(selected.current, nil, nil, nil)
	if err != nil {
		return out, err
	}
	facts, err := origin.Facts()
	if err != nil || !facts.IsRequest || facts.Method != sip.BYE {
		return out, errNativeRecord
	}
	if snapshot.Context.State.Authority == nil {
		return out, errNativeRecord
	}
	ref := &pb.StateEffectRef{Context: snapshot.Context, AttemptId: "a-" + uuid.NewString()}
	scope := nativeIdentity(key, "native-completion-v1", authority.Service, authority.Generation, authority.PrimaryId, snapshot.Context.State.GroupId, snapshot.Context.State.Incarnation, snapshot.CallId, snapshot.Context.Id)
	auth := &pb.EffectAuthorization{Effect: ref, AuthorityClass: pb.EffectAuthorization_TERMINAL_CLEANUP, DeadlineMs: uint64(until.UnixMilli()), ApplicationLeaseTerm: snapshot.ApplicationLeaseTerm, CompletionScope: scope}
	hash, err := SendFingerprint(snapshot, &pb.SipMessage{Method: "BYE", Wire: origin.WireBytes()}, auth, write.Flow)
	if err != nil {
		return out, err
	}
	ref.Id, auth.Fingerprint = "e-"+hash, hash
	flowWrite := &pb.FlowWrite{Effect: ref, Flow: write.Flow, Permit: &pb.SendPermit{Authorization: auth, Flow: write.Flow, DirectoryEpoch: authority.PrimaryId, OrderingProof: nativeIdentity(key, "native-write-authority-v1", hash), CompletionScope: scope}}
	drain := drainNativeMessageObservation(*origin, facts)
	meta := &nativepb.Output{Write: flowWrite, Procedure: procField, Dialog: selected.capture.metadata.Dialog, Method: "BYE", Drain: &nativepb.DrainObservation{Transaction: drain.Transaction[:], Ack: drain.ACK[:], Status: uint32(drain.Status), Request: drain.Request, Invite: drain.Invite, IsAck: drain.Ack, Cancel: drain.Cancel, Initial: drain.Initial}}
	intent, outputChanges, err := nativePreparePhysicalOutput(meta, *origin, originField, outputField, nil, nil)
	if err != nil {
		return out, err
	}
	procedure := &nativepb.Procedure{Flow: proto.Clone(write.Flow).(*pb.FlowRef), Cleanup: true, Emissions: 1}
	if selected.current.Application != nil {
		procedure.Application = proto.Clone(selected.current.Application).(*pb.ProcessRef)
	}
	procData, err := encodeNativeProcedure(tx, nativeProcedureRefs{Origin: originField}, procedure)
	if err != nil {
		return out, err
	}
	selected.current.NoninitialProcedures = append(selected.current.NoninitialProcedures, procField)
	slices.Sort(selected.current.NoninitialProcedures)
	metadata := record.Metadata
	metadata.LocalTransaction = tx.Key
	metadata.Outputs = append(metadata.Outputs, outputField)
	slices.Sort(metadata.Outputs)
	record.Refs.LocalBye = originField
	out.dialogData, err = encodeNativeDialog(next, record.Refs, metadata)
	if err != nil {
		return out, err
	}
	out.changes = append(outputChanges, rediscore.Change{Field: procField, Value: procData})
	out.outputs = []rediscore.OutputIntent{intent}
	out.selectors = []rediscore.SelectorChange{{Selector: selectors[0], Record: procField}}
	out.timers = []rediscore.TimerChange{{Member: "out-" + strings.TrimPrefix(outputField, "r:"), Due: until}}
	sends := 0
	for _, effect := range txEffects {
		switch effect.Kind {
		case portable.Send:
			if effect.Message == nil || !effect.Message.Equal(*origin) || effect.Replay {
				return uacCleanupCandidate{}, errNativeRecord
			}
			sends++
		case portable.Schedule:
			if effect.Timer != portable.TimerE {
				out.timers = append(out.timers, rediscore.TimerChange{Member: "call-client-" + id + "-" + string(effect.Timer), Due: time.UnixMilli(effect.Deadline.At)})
			}
		default:
			return uacCleanupCandidate{}, errNativeRecord
		}
	}
	if sends != 1 {
		return uacCleanupCandidate{}, errNativeRecord
	}
	return out, nil
}
