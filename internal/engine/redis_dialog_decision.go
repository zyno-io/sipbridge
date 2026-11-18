package engine

import (
	"context"
	"crypto/sha256"
	"errors"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/nativepb"
	"sipbridge/internal/rediscore"
	"sipbridge/internal/selectedmessage"
	"sipbridge/internal/storagecodec"
)

// redisDialogDecision is an operation-local selection, never retained authority.
type redisDialogDecision struct {
	selected      *rediscore.ObservedContext
	current       *nativepb.Context
	procedure     nativeProcedureRecord
	dialog        nativeDialogRecord
	privateDialog *nativepb.Dialog
	tx            portable.Transaction
	previous      portable.Dialog
	next          portable.Dialog
	effects       []portable.Effect
	policy        nativeDecisionCandidate
}

func redisDialogNonInvite(method sip.RequestMethod) bool {
	return method == sip.INFO || method == sip.REFER || method == sip.UPDATE || method == sip.NOTIFY
}

func redisDialogResponseContact(advertise nativeSIPAdvertisement, dialog portable.Dialog, origin portable.Message, status uint32) (string, error) {
	contact, _, err := advertise.resolve(origin.Transport())
	if err != nil {
		return "", err
	}
	facts, err := origin.Facts()
	if err != nil || !facts.IsRequest || facts.Method != sip.INVITE && !redisDialogNonInvite(facts.Method) {
		return "", errNativeRecord
	}
	if facts.Method == sip.REFER && status >= 200 && status < 300 {
		if !portable.MatchDialogRequest(dialog, origin) || dialog.Contact == "" {
			return "", errors.New("successful in-dialog REFER requires retained native Contact")
		}
		contact = dialog.Contact
	}
	return contact, nil
}

func publishRedisDialogRefusal(ctx context.Context, store *rediscore.Store, selected *rediscore.ObservedContext, current *nativepb.Context, lease rediscore.ApplicationLeaseRef, sequence uint64, requestID, operation, directory string, fingerprint [32]byte, until, cutoff time.Time, timers []rediscore.TimerChange, executor rediscore.TimerAssignment, cause error) (*pb.OperationResult, error) {
	if selected == nil || current == nil || cause == nil || !time.Now().Before(until) {
		return nil, errNativeRecord
	}
	next := proto.Clone(current).(*nativepb.Context)
	next.CommandSequence, next.Revision = sequence, current.Revision+1
	if directory != "" {
		next.DirectoryEpoch = directory
	}
	contextData, err := encodeNativeContext(next)
	if err != nil {
		return nil, err
	}
	resultData, err := encodeNativeResult(&nativepb.Result{Certainty: pb.OperationResult_NOT_ACCEPTED, Error: cause.Error(), CutoffMs: cutoff.UnixMilli(), Sequence: sequence, OperationId: operation, Fingerprint: fingerprint[:], DecidingLeaseTerm: lease.Term, ServerRequestId: requestID})
	if err != nil {
		return nil, err
	}
	publication := rediscore.Publication{Identity: selected.Identity, Revision: selected.View.Revision, Deadline: until, Application: &lease, SpendReservation: true, Changes: []rediscore.Change{{Field: "r:context", Value: contextData}, {Field: "r:result-" + strconv.FormatUint(sequence, 10), Value: resultData}}, ResultHint: &rediscore.ResultHint{Context: selected.Identity.Context, Incarnation: selected.Identity.Incarnation, Sequence: sequence}}
	if len(timers) != 0 {
		target := selected.View.Timer
		if target == (rediscore.TimerAssignment{}) {
			target = executor
		}
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
	if ctx.Err() != nil || !time.Now().Before(cutoff) {
		return nil, rediscore.ErrUnknown
	}
	snapshot, err := nativeProjectSnapshot(next, nil, nil, nil)
	if err != nil {
		return nil, err
	}
	return &pb.OperationResult{Certainty: pb.OperationResult_NOT_ACCEPTED, Error: cause.Error(), Snapshot: snapshot}, nil
}

func refuseRedisDialogDecision(ctx context.Context, store *rediscore.Store, candidate redisDialogDecision, lease rediscore.ApplicationLeaseRef, decision *pb.RequestDecision, operation string, executor rediscore.TimerAssignment, deadline time.Time, cause error) (*pb.OperationResult, error) {
	if candidate.selected == nil || candidate.current == nil || decision == nil {
		return nil, cause
	}
	prior, err := decodeNativeResult(candidate.selected.View.Records["r:result-"+strconv.FormatUint(candidate.current.CommandSequence, 10)])
	if err != nil {
		return nil, err
	}
	timers, err := planRedisDialogPriorReceipt(ctx, store, candidate.selected, candidate.current, prior)
	if err != nil {
		return nil, err
	}
	wire, err := proto.MarshalOptions{Deterministic: true}.Marshal(decision)
	if err != nil {
		return nil, err
	}
	fingerprint := sha256.Sum256(wire)
	cutoff := minTime(deadline, time.UnixMilli(candidate.procedure.Metadata.PresentDeadlineMs))
	until, directory := cutoff, ""
	if candidate.policy.policy != nil {
		until = minTime(until, time.UnixMilli(int64(candidate.policy.policy.ExpiresAtMs)))
		directory = candidate.policy.policy.DirectoryEpoch
	}
	return publishRedisDialogRefusal(ctx, store, candidate.selected, candidate.current, lease, decision.Sequence, decision.RequestId, operation, directory, fingerprint, until, cutoff, timers, executor, cause)
}

func commitRedisDialogDecision(ctx context.Context, store *rediscore.Store, candidate redisDialogDecision, key []byte, lease rediscore.ApplicationLeaseRef, decision *pb.RequestDecision, operation string, advertise nativeSIPAdvertisement, executor rediscore.TimerAssignment, deadline time.Time) (*pb.OperationResult, error) {
	if decision == nil || decision.Status == 0 && candidate.policy.policy == nil || candidate.selected == nil || candidate.privateDialog == nil || len(candidate.effects) != 1 || candidate.effects[0].Kind != portable.PresentRequest {
		return nil, errNativeRecord
	}
	originalUntil := minTime(deadline, time.UnixMilli(candidate.procedure.Metadata.PresentDeadlineMs))
	until := originalUntil
	if candidate.policy.policy != nil {
		until = minTime(until, time.UnixMilli(int64(candidate.policy.policy.ExpiresAtMs)))
	}
	if !time.Now().Before(until) || ctx.Err() != nil {
		return nil, rediscore.ErrExpired
	}
	resultField := "r:result-" + strconv.FormatUint(decision.Sequence, 10)
	priorField := "r:result-" + strconv.FormatUint(candidate.current.CommandSequence, 10)
	if len(candidate.selected.View.Records[resultField]) != 0 {
		return nil, rediscore.ErrConflict
	}
	prior, err := decodeNativeResult(candidate.selected.View.Records[priorField])
	if err != nil {
		return nil, err
	}
	timers, err := planRedisDialogPriorReceipt(ctx, store, candidate.selected, candidate.current, prior)
	if err != nil {
		return nil, err
	}
	wire, err := proto.MarshalOptions{Deterministic: true}.Marshal(decision)
	if err != nil {
		return nil, err
	}
	fingerprint := sha256.Sum256(wire)
	result, err := encodeNativeResult(&nativepb.Result{Certainty: pb.OperationResult_ACCEPTED, SipStatus: decision.Status, CutoffMs: originalUntil.UnixMilli(), Sequence: decision.Sequence, OperationId: operation, Fingerprint: fingerprint[:], DecidingLeaseTerm: lease.Term, ServerRequestId: decision.RequestId})
	if err != nil {
		return nil, err
	}
	next := proto.Clone(candidate.current).(*nativepb.Context)
	next.CommandSequence, next.Revision = decision.Sequence, next.Revision+1
	if candidate.policy.policy != nil {
		next.DirectoryEpoch = candidate.policy.policy.DirectoryEpoch
		next.PrincipalId, next.BindingId = decision.PrincipalId, decision.BindingId
	}
	if decision.Delivery != nil {
		next.Delivery = proto.Clone(decision.Delivery).(*pb.ContextDelivery)
		next.Flow = proto.Clone(candidate.procedure.Metadata.Flow).(*pb.FlowRef)
	}
	dialogNext := candidate.next
	reinvite := candidate.policy.request.Method == sip.INVITE
	var changes []rediscore.Change
	if reinvite {
		changes, err = planRedisReinviteReplacement(ctx, store, candidate)
		if err != nil {
			return nil, err
		}
	}
	snapshot, err := nativeProjectSnapshot(next, &dialogNext, nil, nil)
	if err != nil {
		return nil, err
	}
	private := candidate.procedure.Metadata
	dialogField := private.RequestDialog
	private.DecisionPending, private.Proposal, private.RequestDialog, private.CommandResult = false, nil, "", resultField
	refs, tx := candidate.procedure.Refs, candidate.tx
	var outputs []rediscore.OutputIntent
	remoteFinal := ""
	heldTrying := reinvite && private.Emissions == 0 && refs.LastResponse != ""
	if heldTrying && (refs.LastResponse != "r:dialog-response-"+decision.RequestId+"-0" || tx.LastResponse == nil) {
		return nil, errNativeRecord
	}
	if decision.Status != 0 {
		if heldTrying {
			changes = append(changes, rediscore.Change{Field: refs.LastResponse, Delete: true})
		}
		now := time.Now()
		contact, e := redisDialogResponseContact(advertise, candidate.previous, tx.Origin, decision.Status)
		if e != nil {
			return refuseRedisDialogDecision(ctx, store, candidate, lease, decision, operation, executor, deadline, e)
		}
		response, e := nativeServerBuildResponseForFlow(tx, int(decision.Status), reason(decision.Status), decision.Body, next.LocalTag, contact, stateHeaders(decision.Headers), private.Flow, now.UnixMilli())
		if e != nil {
			directory := ""
			if candidate.policy.policy != nil {
				directory = candidate.policy.policy.DirectoryEpoch
			}
			return publishRedisDialogRefusal(ctx, store, candidate.selected, candidate.current, lease, decision.Sequence, decision.RequestId, operation, directory, fingerprint, until, originalUntil, timers, executor, e)
		}
		sends := 0
		for _, effect := range response.Effects {
			if effect.Kind == portable.Send {
				sends++
			}
		}
		if sends != 1 {
			return nil, errNativeRecord
		}
		sequence := strconv.FormatUint(decision.Sequence, 10)
		responseField := "r:dialog-response-" + decision.RequestId + "-" + sequence
		outputField := "r:dialog-output-" + decision.RequestId + "-" + sequence
		if reinvite && decision.Status >= 200 {
			var dialogEffects []portable.Effect
			dialogNext, dialogEffects, e = portable.StepDialog(dialogNext, portable.DialogInput{Kind: portable.DialogSendResponse, Now: now.UnixMilli(), Message: response.Message, Answer: len(decision.Body) > 0})
			if e != nil {
				return refuseRedisDialogDecision(ctx, store, candidate, lease, decision, operation, executor, deadline, e)
			}
			for _, effect := range dialogEffects {
				switch effect.Kind {
				case portable.Respond:
				case portable.Schedule:
					if effect.Timer != portable.Timer2xx {
						timers = append(timers, rediscore.TimerChange{Member: callDialogTimerMember(dialogField, effect.Timer), Due: time.UnixMilli(effect.Deadline.At)})
					}
				case portable.CancelTimer:
					if effect.Timer != portable.Timer2xx {
						timers = append(timers, rediscore.TimerChange{Member: callDialogTimerMember(dialogField, effect.Timer), Delete: true})
					}
				default:
					return nil, errNativeRecord
				}
			}
			remoteFinal = responseField
			if decision.Status < 300 {
				retire, retireErr := planRedisSupersededInviteRetirement(ctx, store, candidate.selected, candidate.current, candidate.privateDialog.Transaction, tx.Key, now)
				if retireErr != nil {
					return nil, retireErr
				}
				timers = append(timers, retire...)
				candidate.privateDialog.Transaction = tx.Key
				next.Answered, next.LocalSdp = true, append([]byte(nil), decision.Body...)
				next.RemoteSdp, e = tx.Origin.Body()
				if e != nil {
					return nil, e
				}
				next.LocalTag, next.RemoteTag, next.RemoteTarget = dialogNext.LocalTag, dialogNext.RemoteTag, dialogNext.Target
			}
			snapshot, e = nativeProjectSnapshot(next, &dialogNext, nil, nil)
			if e != nil {
				return nil, e
			}
		}
		intent, outputChanges, shared, e := prepareRedisCallResponseOutput(string(candidate.policy.request.Method), decision.Status, snapshot, private.Flow, key, response.Message, "r:procedure-"+decision.RequestId, dialogField, responseField, "r:dialog-wire-"+decision.RequestId+"-"+sequence, outputField, until, false)
		if e != nil {
			return nil, e
		}
		changes = append(changes, outputChanges...)
		if !shared {
			data, encodeErr := selectedmessage.Marshal(response.Message)
			if encodeErr != nil {
				return nil, encodeErr
			}
			changes = append(changes, rediscore.Change{Field: responseField, Value: data})
		}
		for _, effect := range response.Effects {
			switch effect.Kind {
			case portable.Send:
			case portable.Schedule:
				if effect.Timer != portable.TimerG {
					timers = append(timers, rediscore.TimerChange{Member: "call-tx-" + decision.RequestId + "-" + string(effect.Timer), Due: time.UnixMilli(effect.Deadline.At)})
				}
			case portable.CancelTimer:
				if effect.Timer != portable.TimerG {
					timers = append(timers, rediscore.TimerChange{Member: "call-tx-" + decision.RequestId + "-" + string(effect.Timer), Delete: true})
				}
			case portable.Ended:
			default:
				return nil, errNativeRecord
			}
		}
		private.Emissions++
		private.OutputConsumed = false
		private.Outputs = append(private.Outputs, outputField)
		slices.Sort(private.Outputs)
		candidate.privateDialog.Outputs = append(candidate.privateDialog.Outputs, outputField)
		slices.Sort(candidate.privateDialog.Outputs)
		refs.LastResponse, tx = responseField, response.Transaction
		outputs = append(outputs, intent)
		timers = append(timers, rediscore.TimerChange{Member: "out-" + strings.TrimPrefix(outputField, "r:"), Due: until})
	}
	if decision.Status == 0 && heldTrying {
		sequence := strconv.FormatUint(decision.Sequence, 10)
		responseField := "r:dialog-response-" + decision.RequestId + "-" + sequence
		outputField, physicalField := "r:dialog-output-"+decision.RequestId+"-"+sequence, "r:dialog-wire-"+decision.RequestId+"-"+sequence
		intent, outputChanges, shared, e := prepareRedisCallResponseOutput("INVITE", 100, snapshot, private.Flow, key, *tx.LastResponse, "r:procedure-"+decision.RequestId, dialogField, responseField, physicalField, outputField, until, false)
		if e != nil {
			return nil, e
		}
		changes = append(changes, rediscore.Change{Field: refs.LastResponse, Delete: true})
		refs.LastResponse = responseField
		changes = append(changes, outputChanges...)
		if !shared {
			data, encodeErr := selectedmessage.Marshal(*tx.LastResponse)
			if encodeErr != nil {
				return nil, encodeErr
			}
			changes = append(changes, rediscore.Change{Field: responseField, Value: data})
		}
		outputs = append(outputs, intent)
		private.Emissions++
		private.ProvisionalEmissions++
		private.Outputs = append(private.Outputs, outputField)
		slices.Sort(private.Outputs)
		candidate.privateDialog.Outputs = append(candidate.privateDialog.Outputs, outputField)
		slices.Sort(candidate.privateDialog.Outputs)
		timers = append(timers, rediscore.TimerChange{Member: "out-" + strings.TrimPrefix(outputField, "r:"), Due: until})
	}
	procedureData, err := encodeNativeProcedure(tx, refs, private)
	if err != nil {
		return nil, err
	}
	if reinvite {
		if err = replaceRedisRemoteInvite(candidate.privateDialog, dialogNext, refs.Origin, remoteFinal); err != nil {
			return nil, err
		}
	} else {
		// These non-INVITE requests only advance remote CSeq; retain invite and output refs.
		candidate.privateDialog.Native.RemoteCSeq = dialogNext.RemoteCSeq
		candidate.privateDialog.Native.RemoteSet = dialogNext.RemoteSet
	}
	dialogData, err := marshalNativeRecord(storagecodec.NativeDialog, candidate.privateDialog)
	if err != nil {
		return nil, err
	}
	contextData, err := encodeNativeContext(next)
	if err != nil {
		return nil, err
	}
	changes = append(changes, rediscore.Change{Field: "r:context", Value: contextData}, rediscore.Change{Field: "r:procedure-" + decision.RequestId, Value: procedureData}, rediscore.Change{Field: dialogField, Value: dialogData}, rediscore.Change{Field: resultField, Value: result})
	if decision.Delivery != nil {
		tag, e := nativeDeliveryTagChange(next.Delivery)
		if e != nil {
			return nil, e
		}
		changes = append(changes, tag)
	}
	timers = append(timers, rediscore.TimerChange{Member: "call-decision-" + decision.RequestId, Delete: true})
	target := candidate.selected.View.Timer
	if target == (rediscore.TimerAssignment{}) {
		target = executor
	}
	publication := rediscore.Publication{Identity: candidate.selected.Identity, Revision: candidate.selected.View.Revision, Deadline: until, Application: &lease, SpendReservation: true, Changes: changes, NewOutputs: outputs, Timers: &rediscore.TimerMutation{Expected: candidate.selected.View.Timer, Target: target, Changes: timers}}
	_, err = store.Commit(ctx, publication)
	if errors.Is(err, rediscore.ErrExpired) && candidate.selected.View.Timer != (rediscore.TimerAssignment{}) && candidate.selected.View.Timer != executor && ctx.Err() == nil && time.Now().Before(until) {
		publication.Timers.Target = executor
		_, err = store.Commit(ctx, publication)
	}
	if err != nil {
		return nil, err
	}
	if ctx.Err() != nil || !time.Now().Before(originalUntil) {
		return nil, rediscore.ErrUnknown
	}
	return &pb.OperationResult{Certainty: pb.OperationResult_ACCEPTED, SipStatus: decision.Status, Snapshot: snapshot}, nil
}

// planRedisDialogPriorReceipt follows the already retained owner and never
// assumes the previous receipt belongs to the new dialog request.
func planRedisDialogPriorReceipt(ctx context.Context, store *rediscore.Store, selected *rediscore.ObservedContext, current *nativepb.Context, prior *nativepb.Result) ([]rediscore.TimerChange, error) {
	if selected == nil || selected.View == nil || current == nil || prior == nil || prior.Sequence == 0 || prior.Sequence != current.CommandSequence || prior.CutoffMs <= 0 {
		return nil, errNativeRecord
	}
	field := "r:result-" + strconv.FormatUint(prior.Sequence, 10)
	if current.Kind == pb.ContextSnapshot_UAC && prior.Sequence == 1 {
		view, err := store.Read(ctx, selected.Identity, current.InitialProcedures...)
		if err != nil {
			return nil, err
		}
		if view.Revision != selected.View.Revision || view.Timer != selected.View.Timer {
			return nil, rediscore.ErrConflict
		}
		for _, anchor := range current.InitialProcedures {
			if strings.HasPrefix(anchor, "r:uac-procedure-") {
				procedure, e := readPrivateProcedureRecord(view.Records[anchor])
				if e != nil {
					return nil, e
				}
				if procedure.Root != "" {
					continue
				}
				if procedure.CommandResult != field {
					return nil, errNativeRecord
				}
				change, err := planRedisUACInitialResultExpiry(anchor, prior, prior.Sequence+1)
				if err != nil {
					return nil, err
				}
				return []rediscore.TimerChange{change}, nil
			}
		}
		return nil, errNativeRecord
	}
	if prior.Output != "" {
		view, err := store.Read(ctx, selected.Identity, prior.Output)
		if err != nil {
			return nil, err
		}
		if view.Revision != selected.View.Revision || view.Timer != selected.View.Timer {
			return nil, rediscore.ErrConflict
		}
		output, err := decodeNativeOutput(view.Records[prior.Output])
		if err != nil || output.Result != field || output.Operation != prior.OperationId {
			return nil, errNativeRecord
		}
		return nil, nil
	}
	if prior.ServerRequestId == "" {
		if prior.Pending {
			return nil, errNativeRecord
		}
		if prior.Certainty == pb.OperationResult_NOT_ACCEPTED {
			return []rediscore.TimerChange{{Member: "orig-result-" + strconv.FormatUint(prior.Sequence, 10), Due: time.UnixMilli(prior.CutoffMs)}}, nil
		}
		// A native owner closes its own duty and schedules the original
		// waiter cutoff. Do not leave an immediately-due cleanup head while
		// that actual procedure remains active.
		fields := append(append([]string(nil), current.InitialProcedures...), current.NoninitialProcedures...)
		for len(fields) > 0 {
			n := min(128, len(fields))
			view, err := store.Read(ctx, selected.Identity, fields[:n]...)
			if err != nil {
				return nil, err
			}
			if view.Revision != selected.View.Revision || view.Timer != selected.View.Timer {
				return nil, rediscore.ErrConflict
			}
			for _, field := range fields[:n] {
				procedure, e := readPrivateProcedureRecord(view.Records[field])
				if e != nil {
					return nil, e
				}
				if procedure.CommandResult == "r:result-"+strconv.FormatUint(prior.Sequence, 10) {
					return nil, nil
				}
			}
			fields = fields[n:]
		}
		return []rediscore.TimerChange{{Member: "orig-result-" + strconv.FormatUint(prior.Sequence, 10), Due: time.UnixMilli(prior.CutoffMs)}}, nil
	}
	if prior.Pending || prior.Request != "" || prior.Response != "" {
		return nil, errNativeRecord
	}
	if prior.ServerRequestId != "" {
		owner := "r:procedure-" + prior.ServerRequestId
		if !slices.Contains(current.InitialProcedures, owner) && !slices.Contains(current.NoninitialProcedures, owner) {
			fields := append([]string{owner}, current.InitialProcedures...)
			view, err := store.Read(ctx, selected.Identity, fields...)
			if err != nil {
				return nil, err
			}
			if view.Revision != selected.View.Revision || view.Timer != selected.View.Timer {
				return nil, rediscore.ErrConflict
			}
			if len(view.Records[owner]) != 0 {
				return nil, errNativeRecord
			}
			for _, field := range current.InitialProcedures {
				anchor, e := readPrivateProcedureRecord(view.Records[field])
				if e != nil || anchor.CommandResult == "r:result-"+strconv.FormatUint(prior.Sequence, 10) {
					return nil, errNativeRecord
				}
			}
		}
		if slices.Contains(current.InitialProcedures, owner) {
			view, err := store.Read(ctx, selected.Identity, owner)
			if err != nil {
				return nil, err
			}
			if view.Revision != selected.View.Revision || view.Timer != selected.View.Timer {
				return nil, rediscore.ErrConflict
			}
			procedure, err := readPrivateProcedureRecord(view.Records[owner])
			if err != nil {
				return nil, err
			}
			if procedure.CommandResult == field {
				return []rediscore.TimerChange{{Member: inviteResultTimer(prior.ServerRequestId, prior.Sequence), Due: time.UnixMilli(prior.CutoffMs)}}, nil
			}
		}
		return []rediscore.TimerChange{{Member: inviteResultTimer(prior.ServerRequestId, prior.Sequence), Due: time.UnixMilli(prior.CutoffMs)}}, nil
	}
	// Existing initial-UAS Respond receipts predate ServerRequestId. Their
	// actual initial anchor owns the scalar marker until terminal retirement.
	if current.Kind == pb.ContextSnapshot_UAS && len(current.InitialProcedures) == 1 {
		view, err := store.Read(ctx, selected.Identity, current.InitialProcedures[0])
		if err != nil {
			return nil, err
		}
		if view.Revision != selected.View.Revision || view.Timer != selected.View.Timer {
			return nil, rediscore.ErrConflict
		}
		owner, err := readPrivateProcedureRecord(view.Records[current.InitialProcedures[0]])
		if err != nil || owner.CommandResult != field {
			return nil, errNativeRecord
		}
		id := strings.TrimPrefix(current.InitialProcedures[0], "r:procedure-")
		if id == current.InitialProcedures[0] {
			return nil, errNativeRecord
		}
		return []rediscore.TimerChange{{Member: inviteResultTimer(id, prior.Sequence), Due: time.UnixMilli(prior.CutoffMs)}}, nil
	}
	return nil, errNativeRecord
}

func prepareRedisDialogDecision(ctx context.Context, store *rediscore.Store, selected *rediscore.ObservedContext, current *nativepb.Context, key []byte, application *pb.ProcessRef, lease rediscore.ApplicationLeaseRef, decision *pb.RequestDecision, operation string, deadline time.Time) (redisDialogDecision, error) {
	var out redisDialogDecision
	if store == nil || selected == nil || selected.View == nil || current == nil || decision == nil || decision.Context == nil || current.Context == nil || !proto.Equal(current.Context, decision.Context) || current.Kind != pb.ContextSnapshot_UAS && current.Kind != pb.ContextSnapshot_UAC || current.Ended || current.CommandSequence >= 1<<53-1 || decision.Sequence != current.CommandSequence+1 || current.Revision >= 1<<53-1 || !time.Now().Before(deadline) {
		return out, errNativeRecord
	}
	app, err := nativeApplicationReference(key, application)
	if err != nil || app != lease.Application || selected.Identity.Owner != lease.Application || selected.Identity.Term != lease.Term || !sameProcess(current.Application, application) || current.ApplicationLeaseTerm != lease.Term {
		return out, rediscore.ErrAuthority
	}
	procedureField, originField := "r:procedure-"+decision.RequestId, "r:origin-"+decision.RequestId
	procedure, err := decodeNativeProcedure(selected.View.Records[procedureField])
	if err != nil {
		return out, err
	}
	if procedure.Metadata.RequestId != decision.RequestId || procedure.Metadata.RequestDialog == "" || !slices.Contains(current.NoninitialProcedures, procedureField) || !slices.Contains(current.Dialogs, procedure.Metadata.RequestDialog) || procedure.Refs.Origin != originField || procedure.Native.Family != portable.NonInviteServer && procedure.Native.Family != portable.InviteServer {
		return out, errNativeRecord
	}
	dialogView, err := store.ReadApplication(ctx, selected.Identity, lease, procedure.Metadata.RequestDialog)
	if err != nil {
		return out, err
	}
	if dialogView.Revision != selected.View.Revision || dialogView.Timer != selected.View.Timer {
		return out, rediscore.ErrConflict
	}
	privateDialog, err := readPrivateDialogRecord(dialogView.Records[procedure.Metadata.RequestDialog])
	if err != nil {
		return out, err
	}
	native, local, remote, localBye := privateDialog.Native, privateDialog.LocalInvite, privateDialog.RemoteInvite, privateDialog.LocalBye
	dialog := nativeDialogFromPrivate(privateDialog)
	privateDialog.Native, privateDialog.LocalInvite, privateDialog.RemoteInvite, privateDialog.LocalBye = native, local, remote, localBye
	fields := []string{originField}
	if procedure.Refs.LastResponse != "" {
		fields = append(fields, procedure.Refs.LastResponse)
	}
	for _, refs := range []*nativeInviteRefs{dialog.Refs.Local, dialog.Refs.Remote} {
		if refs != nil && !slices.Contains(fields, refs.Request) {
			fields = append(fields, refs.Request)
		}
	}
	wires, err := store.ReadApplication(ctx, selected.Identity, lease, fields...)
	if errors.Is(err, rediscore.ErrAbsent) {
		return out, rediscore.ErrConflict
	}
	if err != nil {
		return out, err
	}
	if wires.Revision != selected.View.Revision || wires.Timer != selected.View.Timer {
		return out, rediscore.ErrConflict
	}
	memo, err := selectedmessage.NewMemo(len(fields))
	if err != nil {
		return out, err
	}
	messages := &nativeSelectedMessages{records: wires.Records, memo: memo, provenance: "admitted"}
	tx, err := procedure.attach(messages, true, procedure.Refs.LastResponse != "", false)
	if err != nil {
		return out, err
	}
	facts, err := tx.Origin.Facts()
	if err != nil || !facts.IsRequest || facts.Method != sip.INVITE && !redisDialogNonInvite(facts.Method) || facts.Method == sip.INVITE && procedure.Native.Family != portable.InviteServer || facts.Method != sip.INVITE && procedure.Native.Family != portable.NonInviteServer {
		return out, errNativeRecord
	}
	previous, err := dialog.attach(messages, nativeDialogSelection{Local: nativeInviteSelection{Request: true}, Remote: nativeInviteSelection{Request: true}})
	if err != nil {
		return out, err
	}
	snapshot, err := nativeProjectSnapshot(current, &previous, nil, nil)
	if err != nil {
		return out, err
	}
	candidate, err := nativePrepareDecision(nativeDecisionInput{decision: decision, transaction: tx, direct: procedure.Metadata.Direct, decisionPending: procedure.Metadata.DecisionPending, presentDeadline: time.UnixMilli(procedure.Metadata.PresentDeadlineMs), now: time.Now(), deadline: deadline, transactionApplication: procedure.Metadata.Application, application: application, leaseTerm: lease.Term, snapshot: snapshot, currentFlow: current.Flow, transactionFlow: procedure.Metadata.Flow, requestDialog: procedure.Metadata.RequestDialog, proposal: procedure.Metadata.Proposal, operation: operation})
	if err != nil {
		return out, err
	}
	out = redisDialogDecision{selected: selected, current: current, procedure: procedure, dialog: dialog, privateDialog: privateDialog, tx: tx, previous: previous, policy: candidate}
	next, effects, err := portable.StepDialog(previous, portable.DialogInput{Kind: portable.DialogReceiveRequest, Message: tx.Origin, Now: time.Now().UnixMilli(), Offer: facts.Method == sip.INVITE && facts.BodyLength > 0})
	if err != nil {
		return out, err
	}
	out.next, out.effects = next, effects
	return out, nil
}
