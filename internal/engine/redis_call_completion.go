package engine

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net"
	"slices"
	"strconv"
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
	"sipbridge/internal/storagecodec"
)

// selectedCallCompletion is an unpublished native candidate. Its caller owns
// exact selected-record revision fencing, accounting, and atomic publication.
type selectedCallCompletion struct {
	Dialog             portable.Dialog
	DialogEffects      []portable.Effect
	Transaction        portable.Transaction
	TransactionEffects []portable.Effect
	TransactionACK     bool
}

func callDialogTimerMember(field string, timer portable.Timer) string {
	digest := sha256.Sum256([]byte(field))
	return "call-dialog-" + hex.EncodeToString(digest[:16]) + "-" + string(timer)
}

// publishRedisCallTransactionACK persists the exact non-2xx INVITE ACK
// transition. It creates no dialog or controller event.
func publishRedisCallTransactionACK(ctx context.Context, store *rediscore.Store, selected *selectedRedisCall, flow *pb.FlowRef, executor rediscore.TimerAssignment, deadline time.Time) error {
	if store == nil || selected == nil || selected.Context == nil || selected.Context.Kind != pb.ContextSnapshot_UAS && selected.Context.Kind != pb.ContextSnapshot_UAC || selected.Procedure == nil || selected.Procedure.Metadata == nil || !validPolicyFlow(selected.Procedure.Metadata.Flow) || !selected.Candidate.TransactionACK || !proto.Equal(selected.Flow, flow) || !validPolicyFlow(flow) || !time.Now().Before(deadline) {
		return errNativeRecord
	}
	requestID := selected.Procedure.Metadata.RequestId
	if requestID == "" || selected.ProcedureField != "r:procedure-"+requestID || selected.Candidate.Transaction.Family != portable.InviteServer {
		return errNativeRecord
	}
	origin, err := selected.Candidate.Transaction.Origin.Facts()
	if err != nil {
		return err
	}
	if origin.ToTag == "" || nativeInitialRefusalInviteProcedure(selected.Context, selected.Identity, selected.ProcedureField) {
		associated := false
		for _, field := range selected.Context.InitialProcedures {
			if field == selected.ProcedureField {
				associated = true
				break
			}
		}
		if !associated {
			return errNativeRecord
		}
	} else if !slices.Contains(selected.Context.NoninitialProcedures, selected.ProcedureField) {
		return errNativeRecord
	}
	data, err := encodeNativeProcedure(selected.Candidate.Transaction, selected.Procedure.Refs, selected.Procedure.Metadata)
	if err != nil {
		return err
	}
	var timers []rediscore.TimerChange
	acknowledged := false
	for _, effect := range selected.Candidate.TransactionEffects {
		member := "call-tx-" + requestID + "-" + string(effect.Timer)
		switch effect.Kind {
		case portable.Schedule:
			timers = append(timers, rediscore.TimerChange{Member: member, Due: time.UnixMilli(effect.Deadline.At)})
		case portable.CancelTimer:
			timers = append(timers, rediscore.TimerChange{Member: member, Delete: true})
		case portable.Acknowledged:
			acknowledged = true
		case portable.Ended:
		default:
			return errors.New("unexpected transaction ACK effect")
		}
	}
	publication := rediscore.Publication{Identity: selected.Identity, Revision: selected.Revision, Deadline: deadline, SpendReservation: true, Changes: []rediscore.Change{{Field: selected.ProcedureField, Value: data}}}
	if acknowledged && selected.Procedure.Metadata.Flow.Transport == "udp" {
		worker, e := nativeWorkerReference(selected.Procedure.Metadata.Flow.Ingress)
		if e != nil {
			return e
		}
		publication.UDPHint = &rediscore.UDPHint{Worker: worker, Context: selected.Identity.Context, Incarnation: selected.Identity.Incarnation, Procedure: selected.ProcedureField, Stop: true}
	}
	if len(timers) > 0 {
		target := selected.Timer
		if target == (rediscore.TimerAssignment{}) {
			target = executor
		}
		publication.Timers = &rediscore.TimerMutation{Expected: selected.Timer, Target: target, Changes: timers}
	}
	_, err = store.Commit(ctx, publication)
	if errors.Is(err, rediscore.ErrExpired) && publication.Timers != nil && selected.Timer != (rediscore.TimerAssignment{}) && selected.Timer != executor && time.Now().Before(deadline) && ctx.Err() == nil {
		publication.Timers.Target = executor
		_, err = store.Commit(ctx, publication)
	}
	return err
}

// publishRedisCallDialogACK commits the selected successful-final ACK and its
// native timer changes with any newly incurred frozen lifecycle debt.
func publishRedisCallDialogACK(ctx context.Context, store *rediscore.Store, selected *selectedRedisCall, flow *pb.FlowRef, key []byte, executor rediscore.TimerAssignment, deadline time.Time) error {
	if store == nil || selected == nil || selected.Context == nil || selected.Context.Kind != pb.ContextSnapshot_UAS && selected.Context.Kind != pb.ContextSnapshot_UAC || selected.Dialog.Metadata == nil || selected.Procedure == nil || selected.Procedure.Metadata == nil || selected.ProcedureField == "" || len(selected.ProcedureBytes) == 0 || !validPolicyFlow(selected.Procedure.Metadata.Flow) || selected.Candidate.TransactionACK || selected.Dialog.Refs.Remote == nil || !validPolicyFlow(flow) || !proto.Equal(selected.Flow, flow) || !time.Now().Before(deadline) {
		return errNativeRecord
	}
	acknowledged := false
	for _, effect := range selected.Candidate.DialogEffects {
		if effect.Kind == portable.Acknowledged {
			acknowledged = true
		}
	}
	if !acknowledged {
		return nil
	}
	if selected.Candidate.Dialog.RemoteInvite == nil || selected.Candidate.Dialog.RemoteInvite.ACK == nil {
		return errNativeRecord
	}
	refs := selected.Dialog.Refs
	remote := *refs.Remote
	refs.Remote = &remote
	digest := sha256.Sum256([]byte(selected.DialogField + "/" + strconv.FormatUint(uint64(selected.Candidate.Dialog.RemoteInvite.Sequence), 10)))
	ackField := "r:call-ack-" + hex.EncodeToString(digest[:16])
	refs.Remote.ACK = ackField
	metadata := proto.Clone(selected.Dialog.Metadata).(*nativepb.Dialog)
	metadata.CompletionFlow = proto.Clone(flow).(*pb.FlowRef)
	dialogData, err := encodeNativeDialog(selected.Candidate.Dialog, refs, metadata)
	if err != nil {
		return err
	}
	ackData, err := selectedmessage.Marshal(*selected.Candidate.Dialog.RemoteInvite.ACK)
	if err != nil {
		return err
	}
	changes := []rediscore.Change{{Field: selected.DialogField, Value: dialogData}, {Field: ackField, Value: ackData}, {Field: selected.ProcedureField, Value: selected.ProcedureBytes}}
	timers := make([]rediscore.TimerChange, 0, 3)
	for _, effect := range selected.Candidate.DialogEffects {
		switch effect.Kind {
		case portable.CancelTimer:
			timers = append(timers, rediscore.TimerChange{Member: callDialogTimerMember(selected.DialogField, effect.Timer), Delete: true})
		case portable.Schedule:
			timers = append(timers, rediscore.TimerChange{Member: callDialogTimerMember(selected.DialogField, effect.Timer), Due: time.UnixMilli(effect.Deadline.At)})
		case portable.Acknowledged:
		default:
			return errors.New("unexpected dialog ACK effect")
		}
	}
	current := proto.Clone(selected.Context).(*nativepb.Context)
	ownerLost := selected.Identity.Owner == "" && selected.Identity.Term == "" && current.Ended
	contextChanged := false
	if selected.Dialog.Native.Offer == portable.LocalOfferInACK && current.LocalTag == selected.Candidate.Dialog.LocalTag && current.RemoteTag == selected.Candidate.Dialog.RemoteTag {
		if current.Revision >= 1<<53-1 {
			return errNativeRecord
		}
		current.RemoteSdp, err = selected.Candidate.Dialog.RemoteInvite.ACK.Body()
		if err != nil {
			return err
		}
		current.Revision++
		contextChanged = true
	}
	if selected.Candidate.Dialog.RemoteInvite.Initial && !current.InitialAck && current.LocalTag == selected.Candidate.Dialog.LocalTag && current.RemoteTag == selected.Candidate.Dialog.RemoteTag {
		if current.Revision >= 1<<53-1 {
			return errNativeRecord
		}
		current.InitialAck = true
		current.Revision++
		contextChanged = true
		if !ownerLost {
			snapshot, e := nativeProjectSnapshot(current, &selected.Candidate.Dialog, nil, nil)
			if e != nil {
				return e
			}
			now := time.Now()
			eventID := uuid.NewString()
			event, e := encodeNativeLifecycle(&nativepb.Lifecycle{Kind: pb.DialogEvent_ACKNOWLEDGED, Snapshot: snapshot, DeadlineMs: now.Add(2 * time.Minute).UnixMilli(), NextMs: now.UnixMilli()})
			if e != nil {
				return e
			}
			changes = append(changes, rediscore.Change{Field: "r:event-" + eventID, Value: event})
			timers = append(timers, rediscore.TimerChange{Member: "event-" + eventID, Due: now})
		}
	}
	if !selected.Candidate.Dialog.RemoteInvite.Initial && !ownerLost {
		if !slices.Contains(current.NoninitialProcedures, selected.ProcedureField) || current.Revision >= 1<<53-1 {
			return errNativeRecord
		}
		current.Revision++
		contextChanged = true
		snapshot, e := nativeProjectSnapshot(current, &selected.Candidate.Dialog, nil, nil)
		if e != nil {
			return e
		}
		if current.LocalTag != selected.Candidate.Dialog.LocalTag || current.RemoteTag != selected.Candidate.Dialog.RemoteTag {
			// Project media from the accepted fork without changing the selected leg.
			invite := selected.Candidate.Dialog.RemoteInvite
			snapshot.LocalSdp, e = invite.Final.Body()
			if e == nil {
				if selected.Dialog.Native.Offer == portable.LocalOfferInACK {
					snapshot.RemoteSdp, e = invite.ACK.Body()
				} else {
					snapshot.RemoteSdp, e = invite.Request.Body()
				}
			}
			if e != nil {
				return e
			}
		}
		now := time.Now()
		eventID := uuid.NewString()
		eventField := "r:event-" + eventID
		messageField := "r:event-ack-" + eventID
		selectors, e := nativeMessageSelectors(*selected.Candidate.Dialog.RemoteInvite.ACK, false)
		if e != nil {
			return e
		}
		event, e := encodeNativeLifecycle(&nativepb.Lifecycle{Kind: pb.DialogEvent_ACKNOWLEDGED, Snapshot: snapshot,
			Root: selected.ProcedureField, Message: messageField, Selectors: selectors,
			DeadlineMs: now.Add(2 * time.Minute).UnixMilli(), NextMs: now.UnixMilli()})
		if e != nil {
			return e
		}
		procedure, e := readPrivateProcedureRecord(selected.ProcedureBytes)
		if e != nil {
			return e
		}
		procedure.Events = append(procedure.Events, eventField)
		slices.Sort(procedure.Events)
		procedureData, e := marshalNativeRecord(storagecodec.NativeProcedure, procedure)
		if e != nil || !validNativeProcedureMetadata(procedure) {
			return errNativeRecord
		}
		changes[2].Value = procedureData
		changes = append(changes, rediscore.Change{Field: eventField, Value: event}, rediscore.Change{Field: messageField, Value: ackData})
		timers = append(timers, rediscore.TimerChange{Member: "event-" + eventID, Due: now})
	}
	var outputs []rediscore.OutputIntent
	var selectors []rediscore.SelectorChange
	if ownerLost {
		if !contextChanged {
			if current.Revision >= 1<<53-1 {
				return errNativeRecord
			}
			current.Revision++
			contextChanged = true
		}
		stage, e := prepareRedisOwnerBye(ctx, store, selected.Identity, selected.Revision, selected.Timer, current, selected.DialogField, selected.Candidate.Dialog, refs, metadata, flow, key, time.Now(), deadline)
		if e != nil {
			return e
		}
		if stage == nil {
			return errNativeRecord
		}
		changes[0].Value = stage.dialog
		changes = append(changes, stage.changes...)
		outputs, selectors = stage.outputs, stage.selectors
		timers = append(timers, stage.timers...)
	}
	if contextChanged {
		contextData, e := encodeNativeContext(current)
		if e != nil {
			return e
		}
		changes = append(changes, rediscore.Change{Field: "r:context", Value: contextData})
	}
	publication := rediscore.Publication{Identity: selected.Identity, Revision: selected.Revision, Deadline: deadline, SpendReservation: true, Changes: changes, NewOutputs: outputs, Selectors: selectors}
	worker, err := nativeWorkerReference(selected.Procedure.Metadata.Flow.Ingress)
	if err != nil {
		return err
	}
	publication.UDPHint = &rediscore.UDPHint{Worker: worker, Context: selected.Identity.Context, Incarnation: selected.Identity.Incarnation, Procedure: selected.ProcedureField, Stop: true}
	if len(timers) > 0 {
		target := selected.Timer
		if target == (rediscore.TimerAssignment{}) {
			target = executor
		}
		publication.Timers = &rediscore.TimerMutation{Expected: selected.Timer, Target: target, Changes: timers}
	}
	_, err = store.Commit(ctx, publication)
	if errors.Is(err, rediscore.ErrExpired) && publication.Timers != nil && selected.Timer != (rediscore.TimerAssignment{}) && selected.Timer != executor && time.Now().Before(deadline) && ctx.Err() == nil {
		publication.Timers.Target = executor
		_, err = store.Commit(ctx, publication)
	}
	return err
}

type selectedRedisCall struct {
	Identity       rediscore.Identity
	Revision       uint64
	Timer          rediscore.TimerAssignment
	Context        *nativepb.Context
	Flow           *pb.FlowRef
	DialogField    string
	Dialog         nativeDialogRecord
	ProcedureField string
	Procedure      *nativeProcedureRecord
	ProcedureBytes []byte
	Candidate      selectedCallCompletion
}

// selectRedisCallTransactionACK must run before dialog selection. Only a
// matching selected non-2xx INVITE transaction takes this path.
func selectRedisCallTransactionACK(ctx context.Context, store *rediscore.Store, local *pb.ProcessRef, flow *pb.FlowRef, message portable.Message, deadline time.Time) (*selectedRedisCall, bool, error) {
	if store == nil || ctx.Err() != nil || !time.Now().Before(deadline) {
		return nil, false, errNativeRecord
	}
	facts, err := message.Facts()
	if err != nil {
		return nil, false, err
	}
	if !facts.IsRequest || facts.Method != sip.ACK {
		return nil, false, nil
	}
	ids, err := nativeSelectorIDs(message, false, false)
	if err != nil {
		return nil, false, err
	}
	var transaction *rediscore.Selector
	for i := range ids {
		if ids[i].Kind == rediscore.SelectorServerTransaction {
			transaction = &ids[i]
			break
		}
	}
	if transaction == nil {
		return nil, false, nil
	}
	selected, err := store.LookupSelector(ctx, *transaction, "r:context")
	if errors.Is(err, rediscore.ErrAbsent) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	procedure, err := decodeNativeProcedure(selected.View.Records[selected.Record])
	if err != nil {
		return nil, false, err
	}
	if procedure.Native.Family != portable.InviteServer || procedure.Refs.LastResponse == "" {
		return nil, false, nil
	}
	if procedure.Refs.Origin == "" || procedure.Refs.Origin == procedure.Refs.LastResponse {
		return nil, false, errNativeRecord
	}
	view, err := store.Read(ctx, selected.Identity, procedure.Refs.Origin, procedure.Refs.LastResponse)
	if errors.Is(err, rediscore.ErrAbsent) {
		return nil, false, rediscore.ErrConflict
	}
	if err != nil {
		return nil, false, err
	}
	if view.Revision != selected.View.Revision || view.Timer != selected.View.Timer {
		return nil, false, rediscore.ErrConflict
	}
	current, err := decodeNativeContext(selected.View.Records["r:context"])
	if err != nil {
		return nil, false, err
	}
	if current.Context.State == nil || current.Context.Id != selected.Identity.Context || current.Context.State.Incarnation != selected.Identity.Incarnation {
		return nil, false, errNativeRecord
	}
	memo, err := selectedmessage.NewMemo(2)
	if err != nil {
		return nil, false, err
	}
	messages := &nativeSelectedMessages{records: view.Records, memo: memo, provenance: "admitted"}
	last, err := messages.get(procedure.Refs.LastResponse)
	if err != nil {
		return nil, false, err
	}
	final, err := last.Facts()
	if err != nil {
		return nil, false, err
	}
	if final.IsRequest || final.Status < 300 || final.ToTag != facts.ToTag {
		return nil, false, nil
	}
	candidate, err := prepareSelectedCallCompletion(current, nativeDialogRecord{}, &procedure, messages, local, flow, message, time.Now().UnixMilli())
	if err != nil {
		return nil, true, err
	}
	if !candidate.TransactionACK {
		return nil, true, errNativeRecord
	}
	return &selectedRedisCall{Identity: selected.Identity, Revision: selected.View.Revision, Timer: selected.View.Timer, Context: current, Flow: proto.Clone(flow).(*pb.FlowRef), ProcedureField: selected.Record, Procedure: &procedure, Candidate: candidate}, true, nil
}

// selectRedisCallDialog resolves only the selected dialog and its named native
// evidence; a second read must observe the exact selector revision. Incoming
// ACK callers first try selectRedisCallTransactionACK.
func selectRedisCallDialog(ctx context.Context, store *rediscore.Store, local *pb.ProcessRef, flow *pb.FlowRef, message portable.Message, deadline time.Time) (*selectedRedisCall, error) {
	if store == nil || ctx.Err() != nil || !time.Now().Before(deadline) {
		return nil, errNativeRecord
	}
	ids, err := nativeSelectorIDs(message, false, false)
	if err != nil {
		return nil, err
	}
	var dialogID *rediscore.Selector
	for i := range ids {
		if ids[i].Kind == rediscore.SelectorDialog {
			dialogID = &ids[i]
			break
		}
	}
	if dialogID == nil {
		return nil, errNativeRecord
	}
	selected, err := store.LookupSelector(ctx, *dialogID, "r:context")
	if err != nil {
		return nil, err
	}
	current, err := decodeNativeContext(selected.View.Records["r:context"])
	if err != nil {
		return nil, err
	}
	record, err := decodeNativeDialog(selected.View.Records[selected.Record])
	if err != nil {
		return nil, err
	}
	if current.Context.Id != selected.Identity.Context || current.Context.State == nil || current.Context.State.Incarnation != selected.Identity.Incarnation {
		return nil, errNativeRecord
	}
	registered := false
	for _, field := range current.Dialogs {
		if field == selected.Record {
			registered = true
			break
		}
	}
	if !registered {
		return nil, errors.New("selected call dialog is not associated")
	}
	fields := make([]string, 0, 8)
	seen := make(map[string]bool)
	add := func(field string) {
		if field != "" && !seen[field] {
			seen[field] = true
			fields = append(fields, field)
		}
	}
	for _, refs := range []*nativeInviteRefs{record.Refs.Local, record.Refs.Remote} {
		if refs != nil {
			add(refs.Request)
			add(refs.Final)
			add(refs.ACK)
		}
	}
	add(record.Refs.LocalBye)
	var invite *nativeProcedureRecord
	var inviteField string
	var inviteBytes []byte
	facts, err := message.Facts()
	if err != nil {
		return nil, err
	}
	if facts.Method == sip.ACK {
		if record.Metadata.Transaction == "" {
			return nil, errNativeRecord
		}
		assoc, e := store.LookupSelector(ctx, nativeSelector(rediscore.SelectorServerTransaction, record.Metadata.Transaction))
		if errors.Is(e, rediscore.ErrAbsent) {
			return nil, errNativeRecord
		}
		if e != nil {
			return nil, e
		}
		if assoc.Identity != selected.Identity || assoc.View.Revision != selected.View.Revision || assoc.View.Timer != selected.View.Timer {
			return nil, rediscore.ErrConflict
		}
		value, e := decodeNativeProcedure(assoc.View.Records[assoc.Record])
		if e != nil {
			return nil, e
		}
		if value.Native.Key != record.Metadata.Transaction {
			return nil, errNativeRecord
		}
		invite = &value
		inviteField = assoc.Record
		inviteBytes = assoc.View.Records[assoc.Record]
		for _, field := range []string{value.Refs.Origin, value.Refs.LastResponse} {
			add(field)
		}
	}
	view, err := store.Read(ctx, selected.Identity, fields...)
	if errors.Is(err, rediscore.ErrAbsent) {
		return nil, rediscore.ErrConflict
	}
	if err != nil {
		return nil, err
	}
	if view.Revision != selected.View.Revision || view.Timer != selected.View.Timer {
		return nil, rediscore.ErrConflict
	}
	memo, err := selectedmessage.NewMemo(len(fields) + 2)
	if err != nil {
		return nil, err
	}
	messages := &nativeSelectedMessages{records: view.Records, memo: memo, provenance: "admitted"}
	candidate, err := prepareSelectedCallCompletion(current, record, invite, messages, local, flow, message, time.Now().UnixMilli())
	if err != nil {
		return nil, err
	}
	return &selectedRedisCall{Identity: selected.Identity, Revision: selected.View.Revision, Timer: selected.View.Timer, Context: current, Flow: proto.Clone(flow).(*pb.FlowRef), DialogField: selected.Record, Dialog: record, ProcedureField: inviteField, Procedure: invite, ProcedureBytes: inviteBytes, Candidate: candidate}, nil
}

// prepareSelectedCallCompletion composes only existing native transitions over
// operation-local selected records. A matching non-2xx INVITE transaction ACK
// takes precedence over a dialog ACK; no controller lease is needed.
func prepareSelectedCallCompletion(current *nativepb.Context, dialog nativeDialogRecord, invite *nativeProcedureRecord, messages *nativeSelectedMessages, local *pb.ProcessRef, flow *pb.FlowRef, message portable.Message, nowMS int64) (selectedCallCompletion, error) {
	var out selectedCallCompletion
	if current == nil || current.Context == nil || current.Context.State == nil || !validSendProcess(local) || !validPolicyFlow(flow) || !sameProcess(flow.Ingress, local) || message.Transport() != flow.Transport || message.Source() != net.JoinHostPort(flow.SourceIp, strconv.Itoa(int(flow.SourcePort))) || nowMS <= 0 {
		return out, errNativeRecord
	}
	facts, err := message.Facts()
	if err != nil {
		return out, err
	}
	if !facts.IsRequest || facts.Method != sip.ACK && facts.Method != sip.BYE || facts.CallID != current.CallId {
		return out, errors.New("invalid selected call request")
	}
	flowAllowed := func(original *pb.FlowRef) bool {
		return validPolicyFlow(original) && (proto.Equal(original, flow) || original.Transport == "tls" && flow.Transport == "tls" && current.Transport == "tls")
	}
	if facts.Method == sip.ACK && invite != nil && invite.Metadata != nil && invite.Native.Family == portable.InviteServer && invite.Refs.LastResponse != "" {
		tx, e := invite.attach(messages, true, true, false)
		if e != nil {
			return out, e
		}
		final, e := tx.LastResponse.Facts()
		if e != nil {
			return out, e
		}
		if !final.IsRequest && final.Status >= 300 && final.ToTag == facts.ToTag && portable.MatchRequest(tx.Origin, message) {
			if !flowAllowed(invite.Metadata.Flow) {
				return out, errors.New("ACK flow mismatch")
			}
			next, effects, e := portable.StepTransaction(tx, portable.Input{Kind: portable.ReceiveRequest, Message: message, Now: nowMS})
			if e != nil {
				return out, e
			}
			out.Transaction, out.TransactionEffects, out.TransactionACK = next, effects, true
			return out, nil
		}
	}
	restored, err := dialog.complete(messages)
	if err != nil {
		return out, err
	}
	if !portable.MatchDialogRequest(restored, message) {
		return out, errors.New("dialog identity mismatch")
	}
	original := current.Flow
	if facts.Method == sip.ACK {
		if invite == nil || invite.Metadata == nil || invite.Native.Family != portable.InviteServer || restored.RemoteInvite == nil {
			return out, errors.New("ACK transaction required")
		}
		tx, e := invite.attach(messages, true, false, false)
		if e != nil || !tx.Origin.EqualWire(restored.RemoteInvite.Request) {
			return out, errors.New("ACK transaction association changed")
		}
		original = invite.Metadata.Flow
	}
	if !flowAllowed(original) {
		return out, errors.New("dialog flow mismatch")
	}
	if facts.Method == sip.BYE {
		candidate, e := nativeReceiveRemoteBye(restored, message, current.LocalTag, nowMS)
		if e != nil {
			return out, e
		}
		out.Dialog, out.DialogEffects, out.Transaction, out.TransactionEffects = candidate.Dialog, candidate.DialogEffects, candidate.Transaction, candidate.TransactionEffects
		return out, nil
	}
	out.Dialog, out.DialogEffects, err = portable.StepDialog(restored, portable.DialogInput{Kind: portable.DialogReceiveRequest, Message: message, Now: nowMS})
	return out, err
}

// publishRedisCallRemoteBye commits the native BYE, its exact selected response,
// physical attempt and (for the selected dialog) frozen end debt together.
func publishRedisCallRemoteBye(ctx context.Context, store *rediscore.Store, selected *selectedRedisCall, flow *pb.FlowRef, key []byte, executor rediscore.TimerAssignment, deadline time.Time) error {
	if store == nil || selected == nil || selected.Context == nil || selected.Context.Kind != pb.ContextSnapshot_UAS && selected.Context.Kind != pb.ContextSnapshot_UAC || selected.Dialog.Metadata == nil || !validPolicyFlow(flow) || !proto.Equal(selected.Flow, flow) || len(key) < 32 || len(key) > 1024 || !time.Now().Before(deadline) {
		return errNativeRecord
	}
	candidate := selected.Candidate
	if candidate.Transaction.Family != portable.NonInviteServer || candidate.Transaction.LastResponse == nil || candidate.Dialog.Phase != portable.DialogEnded {
		return errNativeRecord
	}
	current := proto.Clone(selected.Context).(*nativepb.Context)
	if current.Context == nil || current.Context.State == nil || current.Context.State.Authority == nil || current.Application == nil || current.Revision >= 1<<53-1 {
		return errNativeRecord
	}
	authority := current.Context.State.Authority
	id := nativeIdentity(key, "call-remote-bye", authority.PrimaryId, selected.Identity.Incarnation, selected.Identity.Context, candidate.Transaction.Key)[:32]
	procedureField, originField, responseField := "r:call-bye-"+id, "r:call-bye-origin-"+id, "r:call-bye-final-"+id
	if slices.Contains(current.NoninitialProcedures, procedureField) {
		return errNativeRecord
	}
	current.NoninitialProcedures = append(current.NoninitialProcedures, procedureField)
	slices.Sort(current.NoninitialProcedures)
	physicalField, outputField := "r:call-bye-wire-"+id, "r:call-bye-output-"+id
	refs := selected.Dialog.Refs
	dialogMetadata := proto.Clone(selected.Dialog.Metadata).(*nativepb.Dialog)
	if slices.Contains(dialogMetadata.Outputs, outputField) {
		return errNativeRecord
	}
	dialogMetadata.Outputs = append(dialogMetadata.Outputs, outputField)
	slices.Sort(dialogMetadata.Outputs)
	dialogData, err := encodeNativeDialog(candidate.Dialog, refs, dialogMetadata)
	if err != nil {
		return err
	}
	originData, err := selectedmessage.Marshal(candidate.Transaction.Origin)
	if err != nil {
		return err
	}
	selectedResponse := *candidate.Transaction.LastResponse
	procedureData, err := encodeNativeProcedure(candidate.Transaction, nativeProcedureRefs{Origin: originField, LastResponse: responseField}, &nativepb.Procedure{Flow: proto.Clone(flow).(*pb.FlowRef), Application: proto.Clone(current.Application).(*pb.ProcessRef), Emissions: 1})
	if err != nil {
		return err
	}
	now := time.Now()
	until := minTime(deadline, now.Add(2*time.Second))
	if !now.Before(until) {
		return errNativeRecord
	}
	snapshot, err := nativeProjectSnapshot(current, &candidate.Dialog, nil, nil)
	if err != nil {
		return err
	}
	intent, outputChanges, shared, err := prepareRedisCallByeOutput(snapshot, flow, key, selectedResponse, procedureField, selected.DialogField, responseField, physicalField, outputField, until, false)
	if err != nil {
		return err
	}
	changes := []rediscore.Change{{Field: selected.DialogField, Value: dialogData}, {Field: procedureField, Value: procedureData}, {Field: originField, Value: originData}}
	if !shared {
		responseData, e := selectedmessage.Marshal(selectedResponse)
		if e != nil {
			return e
		}
		changes = append(changes, rediscore.Change{Field: responseField, Value: responseData})
	}
	changes = append(changes, outputChanges...)
	selectors, err := nativeSelectorIDs(candidate.Transaction.Origin, false, false)
	if err != nil || len(selectors) == 0 || selectors[0].Kind != rediscore.SelectorServerTransaction {
		return errNativeRecord
	}
	timers := []rediscore.TimerChange{{Member: "out-" + strings.TrimPrefix(outputField, "r:"), Due: until}}
	for _, effect := range candidate.TransactionEffects {
		switch effect.Kind {
		case portable.Schedule:
			timers = append(timers, rediscore.TimerChange{Member: "call-tx-" + id + "-" + string(effect.Timer), Due: time.UnixMilli(effect.Deadline.At)})
		case portable.CancelTimer:
			timers = append(timers, rediscore.TimerChange{Member: "call-tx-" + id + "-" + string(effect.Timer), Delete: true})
		case portable.PresentRequest, portable.Send, portable.Ended, portable.Acknowledged:
		default:
			return errors.New("unexpected native BYE transaction effect")
		}
	}
	for _, effect := range candidate.DialogEffects {
		switch effect.Kind {
		case portable.Schedule:
			timers = append(timers, rediscore.TimerChange{Member: callDialogTimerMember(selected.DialogField, effect.Timer), Due: time.UnixMilli(effect.Deadline.At)})
		case portable.CancelTimer:
			timers = append(timers, rediscore.TimerChange{Member: callDialogTimerMember(selected.DialogField, effect.Timer), Delete: true})
		case portable.Respond, portable.Ended:
		default:
			return errors.New("unexpected native BYE dialog effect")
		}
	}
	if !current.Ended && current.LocalTag == candidate.Dialog.LocalTag && current.RemoteTag == candidate.Dialog.RemoteTag {
		current.Ended = true
		current.Revision++
		if current.CallRetireAtMs == 0 {
			current.CallRetireAtMs = now.Add(2 * time.Minute).UnixMilli()
		}
		if current.CallRetireAtMs <= 0 || current.CallRetireAtMs > 1<<53-1 {
			return errNativeRecord
		}
		snapshot, err = nativeProjectSnapshot(current, &candidate.Dialog, nil, nil)
		if err != nil {
			return err
		}
		eventID := uuid.NewString()
		event, err := encodeNativeLifecycle(&nativepb.Lifecycle{Kind: pb.DialogEvent_ENDED, Snapshot: snapshot, DeadlineMs: now.Add(2 * time.Minute).UnixMilli(), NextMs: now.UnixMilli()})
		if err != nil {
			return err
		}
		changes = append(changes, rediscore.Change{Field: "r:event-" + eventID, Value: event})
		timers = append(timers, rediscore.TimerChange{Member: "event-" + eventID, Due: now}, rediscore.TimerChange{Member: "call-retire", Due: time.UnixMilli(current.CallRetireAtMs)})
	}
	contextData, err := encodeNativeContext(current)
	if err != nil {
		return err
	}
	changes = append(changes, rediscore.Change{Field: "r:context", Value: contextData})
	target := selected.Timer
	if target == (rediscore.TimerAssignment{}) {
		target = executor
	}
	publication := rediscore.Publication{Identity: selected.Identity, Revision: selected.Revision, Deadline: deadline, SpendReservation: true, Changes: changes, NewOutputs: []rediscore.OutputIntent{intent}, Selectors: []rediscore.SelectorChange{{Selector: selectors[0], Record: procedureField}}, Timers: &rediscore.TimerMutation{Expected: selected.Timer, Target: target, Changes: timers}}
	_, err = store.Commit(ctx, publication)
	if errors.Is(err, rediscore.ErrExpired) && selected.Timer != (rediscore.TimerAssignment{}) && selected.Timer != executor && time.Now().Before(deadline) && ctx.Err() == nil {
		publication.Timers.Target = executor
		_, err = store.Commit(ctx, publication)
	}
	return err
}
