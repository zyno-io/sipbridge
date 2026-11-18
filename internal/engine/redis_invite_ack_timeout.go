package engine

import (
	"context"
	"encoding/hex"
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

// fireRedisInitialInviteACKTimeout stages the SDK-required cleanup BYE with
// the dialog timeout. It remains private until the BYE lifecycle is complete.
func fireRedisInitialInviteACKTimeout(ctx context.Context, store *rediscore.Store, key []byte, due *rediscore.DueContext, deadline time.Time) error {
	if store == nil || due == nil || len(key) < 32 || len(key) > 1024 || !strings.HasPrefix(due.Member, "call-dialog-") || !strings.HasSuffix(due.Member, "-"+string(portable.TimerACK)) || due.CheckedAtMS < due.DeadlineMS || !time.Now().Before(deadline) {
		return errNativeRecord
	}
	view, err := store.Read(ctx, due.Identity, "r:context")
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
	if current.Context == nil || current.Context.State == nil || current.Context.State.Authority == nil || current.Context.Id != due.Identity.Context || current.Context.State.Incarnation != due.Identity.Incarnation || current.Application == nil || current.Kind != pb.ContextSnapshot_UAS && current.Kind != pb.ContextSnapshot_UAC {
		return errNativeRecord
	}
	dialogField := ""
	for _, field := range current.Dialogs {
		if callDialogTimerMember(field, portable.TimerACK) == due.Member {
			if dialogField != "" {
				return errNativeRecord
			}
			dialogField = field
		}
	}
	if dialogField == "" {
		return errNativeRecord
	}
	dialogView, err := store.Read(ctx, due.Identity, dialogField)
	if err != nil {
		return err
	}
	if dialogView.Revision != view.Revision || dialogView.Timer != view.Timer {
		return rediscore.ErrConflict
	}
	dialog, err := decodeNativeDialog(dialogView.Records[dialogField])
	if err != nil || dialog.Refs.Remote == nil || dialog.Refs.Remote.Request == "" || dialog.Refs.Remote.Final == "" || dialog.Metadata.Transaction == "" {
		return errNativeRecord
	}
	timer := dialog.Native.Timers[portable.TimerACK]
	if dialog.Native.Role != portable.UAS && dialog.Native.Role != portable.UAC || !timer.Active || timer.Generation == 0 || timer.At != due.DeadlineMS {
		return errNativeRecord
	}
	fields := []string{dialog.Refs.Remote.Request, dialog.Refs.Remote.Final}
	if dialog.Refs.Local != nil {
		for _, field := range []string{dialog.Refs.Local.Request, dialog.Refs.Local.Final, dialog.Refs.Local.ACK} {
			if field != "" && !slices.Contains(fields, field) {
				fields = append(fields, field)
			}
		}
	}
	if dialog.Refs.LocalBye != "" && !slices.Contains(fields, dialog.Refs.LocalBye) {
		fields = append(fields, dialog.Refs.LocalBye)
	}
	messagesView, err := store.Read(ctx, due.Identity, fields...)
	if err != nil {
		return err
	}
	if messagesView.Revision != view.Revision || messagesView.Timer != view.Timer {
		return rediscore.ErrConflict
	}
	memo, err := selectedmessage.NewMemo(len(fields))
	if err != nil {
		return err
	}
	messages := &nativeSelectedMessages{records: messagesView.Records, memo: memo, provenance: "admitted"}
	restored, err := dialog.attach(messages, nativeDialogSelection{Local: nativeInviteSelection{Request: true, Final: true, ACK: true}, Remote: nativeInviteSelection{Request: true, Final: true}, LocalBye: true})
	if err != nil {
		return err
	}
	if restored.RemoteInvite == nil {
		return errNativeRecord
	}
	selectors, err := nativeSelectorIDs(restored.RemoteInvite.Request, false, false)
	if err != nil || len(selectors) == 0 || selectors[0].Kind != rediscore.SelectorServerTransaction {
		return errNativeRecord
	}
	anchor, err := store.LookupSelector(ctx, selectors[0])
	if err != nil {
		return err
	}
	if anchor.Identity != due.Identity || anchor.View.Revision != view.Revision || anchor.View.Timer != view.Timer || !slices.Contains(current.InitialProcedures, anchor.Record) && !slices.Contains(current.NoninitialProcedures, anchor.Record) {
		return rediscore.ErrConflict
	}
	anchorRecord, err := readPrivateProcedureRecord(anchor.View.Records[anchor.Record])
	if err != nil || anchorRecord.Native.Version != portable.Version || anchorRecord.Native.Family != string(portable.InviteServer) || anchorRecord.Native.Key != dialog.Metadata.Transaction || anchorRecord.Origin != dialog.Refs.Remote.Request || anchorRecord.LastResponse != dialog.Refs.Remote.Final {
		return errNativeRecord
	}
	if restored.LocalBye != nil {
		if err = validateRedisACKTimeoutExistingBYE(ctx, store, due, current, dialog, restored); err != nil {
			return err
		}
	}
	timed, effects, err := portable.StepDialog(restored, portable.DialogInput{Kind: portable.DialogTimer, Timer: portable.TimerACK, Generation: timer.Generation, Now: due.CheckedAtMS})
	if err != nil {
		return err
	}
	needBye := false
	for _, effect := range effects {
		if effect.Kind == portable.NeedBYE {
			needBye = true
		} else if effect.Kind != portable.CancelTimer {
			return errors.New("unexpected ACK timeout effect")
		}
	}
	if !needBye {
		return errNativeRecord
	}
	if restored.LocalBye != nil {
		// Its actual native client already owns termination. Apply the ACK
		// timeout without replacing that BYE, its physical proof or its F/K.
		data, e := encodeNativeDialog(timed, dialog.Refs, dialog.Metadata)
		if e != nil {
			return e
		}
		_, e = store.Commit(ctx, rediscore.Publication{Identity: due.Identity, Revision: due.Revision, Deadline: deadline, SpendReservation: true, Changes: []rediscore.Change{{Field: dialogField, Value: data}}, Timers: &rediscore.TimerMutation{Expected: due.Assignment, Target: due.Assignment, RequireLease: true, Changes: []rediscore.TimerChange{{Member: due.Member, Delete: true}}}})
		return e
	}
	branch := sip.RFC3261BranchMagicCookie + nativeIdentity(key, "owner-loss-bye", due.Identity.Incarnation, dialogField)[:32]
	candidate, eligible, err := nativePrepareOwnerBye(timed, dialog.Metadata.AckWritten, true, branch, due.CheckedAtMS)
	if err != nil || !eligible {
		if err != nil {
			return err
		}
		return errNativeRecord
	}
	flow := dialog.Metadata.CompletionFlow
	if flow == nil {
		flow = current.Flow
	}
	if !validPolicyFlow(flow) || flow.Transport != restored.Transport {
		return errNativeRecord
	}
	authority := current.Context.State.Authority
	id := nativeIdentity(key, "call-ack-timeout-bye", authority.PrimaryId, due.Identity.Incarnation, due.Identity.Context, dialogField)[:32]
	procedureField, wireField, outputField := "r:call-cleanup-"+id, "r:call-cleanup-origin-"+id, "r:call-cleanup-output-"+id
	occupied, err := store.Read(ctx, due.Identity, procedureField, wireField, outputField)
	if err != nil {
		return err
	}
	if occupied.Revision != view.Revision || occupied.Timer != view.Timer {
		return rediscore.ErrConflict
	}
	if len(occupied.Records[procedureField])+len(occupied.Records[wireField])+len(occupied.Records[outputField]) != 0 || slices.Contains(current.NoninitialProcedures, procedureField) || slices.Contains(dialog.Metadata.Outputs, outputField) {
		return rediscore.ErrConflict
	}
	originSelectors, err := nativeSelectorIDs(candidate.Transaction.Origin, true, false)
	if err != nil || len(originSelectors) == 0 || originSelectors[0].Kind != rediscore.SelectorClientTransaction {
		return errNativeRecord
	}
	metadata := &nativepb.Procedure{Flow: proto.Clone(flow).(*pb.FlowRef), Application: proto.Clone(current.Application).(*pb.ProcessRef), Cleanup: true, Emissions: 1}
	procedureBytes, err := encodeNativeProcedure(candidate.Transaction, nativeProcedureRefs{Origin: wireField}, metadata)
	if err != nil {
		return err
	}
	refs := dialog.Refs
	refs.LocalBye = wireField
	dialogMetadata := proto.Clone(dialog.Metadata).(*nativepb.Dialog)
	dialogMetadata.LocalTransaction = candidate.Transaction.Key
	dialogMetadata.Outputs = append(dialogMetadata.Outputs, outputField)
	slices.Sort(dialogMetadata.Outputs)
	dialogBytes, err := encodeNativeDialog(candidate.Dialog, refs, dialogMetadata)
	if err != nil {
		return err
	}
	next := proto.Clone(current).(*nativepb.Context)
	next.NoninitialProcedures = append(next.NoninitialProcedures, procedureField)
	slices.Sort(next.NoninitialProcedures)
	contextBytes, err := encodeNativeContext(next)
	if err != nil {
		return err
	}
	snapshot, err := nativeProjectSnapshot(next, &candidate.Dialog, nil, nil)
	if err != nil {
		return err
	}
	until := minTime(deadline, time.Now().Add(2*time.Second))
	ref := &pb.StateEffectRef{Context: snapshot.Context, AttemptId: "a-" + uuid.NewString()}
	scope := nativeIdentity(key, "native-completion-v1", authority.Service, authority.Generation, authority.PrimaryId, snapshot.Context.State.GroupId, snapshot.Context.State.Incarnation, snapshot.CallId, snapshot.Context.Id)
	auth := &pb.EffectAuthorization{Effect: ref, AuthorityClass: pb.EffectAuthorization_TERMINAL_CLEANUP, DeadlineMs: uint64(until.UnixMilli()), ApplicationLeaseTerm: snapshot.ApplicationLeaseTerm, CompletionScope: scope}
	fingerprint, err := SendFingerprint(snapshot, &pb.SipMessage{Method: "BYE", Wire: candidate.Transaction.Origin.WireBytes()}, auth, flow)
	if err != nil {
		return err
	}
	ref.Id, auth.Fingerprint = "e-"+fingerprint, fingerprint
	write := &pb.FlowWrite{Effect: ref, Flow: flow, Permit: &pb.SendPermit{Authorization: auth, Flow: flow, DirectoryEpoch: authority.PrimaryId, OrderingProof: nativeIdentity(key, "native-write-authority-v1", fingerprint), CompletionScope: scope}}
	facts, err := candidate.Transaction.Origin.Facts()
	if err != nil {
		return err
	}
	drain := drainNativeMessageObservation(candidate.Transaction.Origin, facts)
	output := &nativepb.Output{Write: write, Procedure: procedureField, Dialog: dialogField, Method: "BYE", Drain: &nativepb.DrainObservation{Transaction: drain.Transaction[:], Ack: drain.ACK[:], Status: uint32(drain.Status), Request: drain.Request, Invite: drain.Invite, IsAck: drain.Ack, Cancel: drain.Cancel, Initial: drain.Initial}}
	intent, outputChanges, err := nativePreparePhysicalOutput(output, candidate.Transaction.Origin, wireField, outputField, nil, nil)
	if err != nil {
		return err
	}
	timers := []rediscore.TimerChange{{Member: due.Member, Delete: true}, {Member: "out-" + strings.TrimPrefix(outputField, "r:"), Due: until}}
	sends, finalTimers := 0, 0
	for _, effect := range candidate.Effects {
		switch effect.Kind {
		case portable.Send:
			if effect.Message == nil || !effect.Message.EqualWire(candidate.Transaction.Origin) {
				return errNativeRecord
			}
			sends++
		case portable.Schedule:
			if effect.Timer == portable.TimerF {
				finalTimers++
				timers = append(timers, rediscore.TimerChange{Member: "call-client-" + id + "-" + string(effect.Timer), Due: time.UnixMilli(effect.Deadline.At)})
			} else if effect.Timer != portable.TimerE {
				return errNativeRecord
			}
		default:
			return errors.New("unexpected cleanup BYE effect")
		}
	}
	if sends != 1 || finalTimers != 1 {
		return errNativeRecord
	}
	changes := []rediscore.Change{{Field: "r:context", Value: contextBytes}, {Field: dialogField, Value: dialogBytes}, {Field: procedureField, Value: procedureBytes}}
	changes = append(changes, outputChanges...)
	_, err = store.Commit(ctx, rediscore.Publication{Identity: due.Identity, Revision: view.Revision, Deadline: until, SpendReservation: true, Changes: changes, NewOutputs: []rediscore.OutputIntent{intent}, Selectors: []rediscore.SelectorChange{{Selector: originSelectors[0], Record: procedureField}}, Timers: &rediscore.TimerMutation{Expected: due.Assignment, Target: due.Assignment, RequireLease: true, Changes: timers}})
	return err
}

// The existing wire and actual client owner close NeedBYE without authorizing
// another attempt. No physical outcome or application authority is inferred.
func validateRedisACKTimeoutExistingBYE(ctx context.Context, store *rediscore.Store, due *rediscore.DueContext, current *nativepb.Context, dialog nativeDialogRecord, native portable.Dialog) error {
	if native.LocalBye == nil || dialog.Refs.LocalBye == "" || dialog.Metadata.LocalTransaction == "" {
		return errNativeRecord
	}
	facts, err := native.LocalBye.Facts()
	if err != nil || !facts.IsRequest || facts.Method != sip.BYE || facts.CSeqMethod != sip.BYE || facts.CallID != native.CallID || facts.FromTag != native.LocalTag || facts.ToTag != native.RemoteTag || native.LocalBye.Transport() != native.Transport {
		return errNativeRecord
	}
	txKey, err := native.LocalBye.ClientTxKey()
	if err != nil || txKey != dialog.Metadata.LocalTransaction {
		return errNativeRecord
	}
	id, owner, cleanup := "", "", false
	switch {
	case strings.HasPrefix(dialog.Refs.LocalBye, "r:app-bye-origin-"):
		id = strings.TrimPrefix(dialog.Refs.LocalBye, "r:app-bye-origin-")
		owner = "r:app-bye-" + id
	case strings.HasPrefix(dialog.Refs.LocalBye, "r:call-cleanup-origin-"):
		id = strings.TrimPrefix(dialog.Refs.LocalBye, "r:call-cleanup-origin-")
		owner = "r:call-cleanup-" + id
		cleanup = true
	default:
		return errNativeRecord
	}
	raw, err := hex.DecodeString(id)
	if err != nil || len(raw) != 16 || hex.EncodeToString(raw) != id || !slices.Contains(current.NoninitialProcedures, owner) {
		return errNativeRecord
	}
	view, err := store.Read(ctx, due.Identity, owner)
	if err != nil {
		return err
	}
	if view.Revision != due.Revision || view.Timer != due.Assignment {
		return rediscore.ErrConflict
	}
	p, err := decodeNativeProcedure(view.Records[owner])
	if err != nil {
		return err
	}
	if p.Native.Version != portable.Version || p.Native.Family != portable.NonInviteClient || p.Native.Key != txKey || p.Native.Reliable != (native.Transport != "udp") || p.Refs.Origin != dialog.Refs.LocalBye || p.Metadata.Cleanup != cleanup || cleanup && p.Metadata.CommandResult != "" || !cleanup && p.Metadata.CommandResult == "" || !validPolicyFlow(p.Metadata.Flow) || p.Metadata.Flow.Transport != native.Transport {
		return errNativeRecord
	}
	return nil
}
