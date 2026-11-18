package engine

import (
	"context"
	"crypto/sha256"
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
)

// commitRedisInviteFinal is the initial direct-decision publication. Native
// transaction, optional UAS dialog, response intent and accepted receipt are
// staged together; no physical write is implied by its result.
func commitRedisInviteFinal(ctx context.Context, store *rediscore.Store, key []byte, application *pb.ProcessRef, lease rediscore.ApplicationLeaseRef, decision *pb.RequestDecision, operation, contact, sentBy string, deadline time.Time) (*pb.OperationResult, error) {
	if store == nil || decision == nil || decision.Context == nil || decision.Context.State == nil || decision.Status < 200 || decision.Status > 699 || decision.Sequence != 1 || !validSendText(operation, 160, false) || !time.Now().Before(deadline) {
		return nil, errNativeRecord
	}
	app, err := nativeApplicationReference(key, application)
	if err != nil || app != lease.Application {
		return nil, rediscore.ErrAuthority
	}
	id := decision.RequestId
	procedureField, originField := "r:procedure-"+id, "r:origin-"+id
	heldField := "r:response-" + id + "-100"
	responseField, physicalField, outputField := "r:invite-response-"+id+"-1", "r:invite-wire-"+id+"-1", "r:invite-output-"+id+"-1"
	dialogField := "r:dialog-" + id
	selected, err := store.ObserveApplicationContext(ctx, decision.Context.Id, decision.Context.State.Incarnation, lease, "r:context", procedureField, originField, heldField, responseField, physicalField, outputField, dialogField, "r:result-1")
	if err != nil {
		return nil, err
	}
	current, err := decodeNativeContext(selected.View.Records["r:context"])
	if err != nil {
		return nil, err
	}
	procedure, err := decodeNativeProcedure(selected.View.Records[procedureField])
	if err != nil {
		return nil, err
	}
	if selected.Identity.Owner != "" || current.Context == nil || !proto.Equal(current.Context, decision.Context) || current.Kind != pb.ContextSnapshot_UAS || current.CommandSequence != 0 || current.Revision >= 1<<53-1 || !slices.Contains(current.InitialProcedures, procedureField) || len(current.Dialogs) != 0 || procedure.Metadata.RequestId != id || procedure.Refs.Origin != originField || procedure.Metadata.Emissions != 0 || len(procedure.Metadata.Outputs) != 0 || len(selected.View.Records["r:result-1"]) != 0 || len(selected.View.Records[responseField]) != 0 || len(selected.View.Records[physicalField]) != 0 || len(selected.View.Records[outputField]) != 0 || len(selected.View.Records[dialogField]) != 0 || procedure.Refs.LastResponse != "" && procedure.Refs.LastResponse != heldField {
		return nil, rediscore.ErrConflict
	}
	memo, err := selectedmessage.NewMemo(2)
	if err != nil {
		return nil, err
	}
	tx, err := procedure.attach(&nativeSelectedMessages{records: selected.View.Records, memo: memo, provenance: "admitted"}, true, procedure.Refs.LastResponse != "", false)
	if err != nil {
		return nil, err
	}
	snapshot, err := nativeProjectSnapshot(current, nil, nil, nil)
	if err != nil {
		return nil, err
	}
	now := time.Now()
	candidate, err := nativePrepareDecision(nativeDecisionInput{decision: decision, transaction: tx, direct: procedure.Metadata.Direct, decisionPending: procedure.Metadata.DecisionPending, unbound: true, presentDeadline: time.UnixMilli(procedure.Metadata.PresentDeadlineMs), now: now, deadline: deadline, transactionApplication: procedure.Metadata.Application, application: application, leaseTerm: lease.Term, snapshot: snapshot, currentFlow: current.Flow, transactionFlow: procedure.Metadata.Flow, proposal: procedure.Metadata.Proposal, operation: operation})
	if err != nil || !candidate.bindOwner {
		if err != nil {
			return nil, err
		}
		return nil, errNativeRecord
	}
	response := &pb.Respond{RequestId: id, Status: decision.Status, Headers: decision.Headers, Body: decision.Body}
	native, err := nativePrepareInitialInviteResponse(tx, response, current.LocalTag, contact, sentBy, now.UnixMilli())
	if err != nil {
		return nil, err
	}
	until := minTime(deadline, time.UnixMilli(procedure.Metadata.PresentDeadlineMs))
	decisionWire, err := proto.MarshalOptions{Deterministic: true}.Marshal(decision)
	if err != nil {
		return nil, err
	}
	fingerprint := sha256.Sum256(decisionWire)
	result, err := encodeNativeResult(&nativepb.Result{Certainty: pb.OperationResult_ACCEPTED, SipStatus: decision.Status, CutoffMs: until.UnixMilli(), Sequence: 1, OperationId: operation, Fingerprint: fingerprint[:], DecidingLeaseTerm: lease.Term, ServerRequestId: id})
	if err != nil {
		return nil, err
	}
	next := proto.Clone(current).(*nativepb.Context)
	next.Application, next.ApplicationLeaseTerm = proto.Clone(application).(*pb.ProcessRef), lease.Term
	if decision.Status < 300 {
		if candidate.policy == nil {
			return nil, errNativeRecord
		}
		next.PrincipalId, next.BindingId, next.DirectoryEpoch = decision.PrincipalId, decision.BindingId, candidate.policy.DirectoryEpoch
		if decision.Delivery != nil {
			next.Delivery = proto.Clone(decision.Delivery).(*pb.ContextDelivery)
			next.Flow = proto.Clone(procedure.Metadata.Flow).(*pb.FlowRef)
		}
	}
	next.CommandSequence = 1
	next.Revision++
	var selectors []rediscore.SelectorChange
	var changes []rediscore.Change
	timers := []rediscore.TimerChange{{Member: "call-decision-" + id, Delete: true}}
	for _, effect := range native.Effects {
		switch effect.Kind {
		case portable.Send:
		case portable.CancelTimer:
			if effect.Timer != portable.TimerG {
				timers = append(timers, rediscore.TimerChange{Member: "call-tx-" + id + "-" + string(effect.Timer), Delete: true})
			}
		case portable.Schedule:
			if effect.Timer != portable.TimerG {
				timers = append(timers, rediscore.TimerChange{Member: "call-tx-" + id + "-" + string(effect.Timer), Due: time.UnixMilli(effect.Deadline.At)})
			}
		default:
			return nil, errors.New("unexpected INVITE transaction response effect")
		}
	}
	if native.Dialog != nil {
		dialog := *native.Dialog
		next.Answered, next.LocalSdp = true, append([]byte(nil), decision.Body...)
		next.RemoteSdp, err = tx.Origin.Body()
		if err != nil {
			return nil, err
		}
		next.LocalTag, next.RemoteTag, next.RemoteTarget = dialog.LocalTag, dialog.RemoteTag, dialog.Target
		next.Dialogs = []string{dialogField}
		tags := []string{dialog.LocalTag, dialog.RemoteTag}
		slices.Sort(tags)
		selectors = append(selectors, rediscore.SelectorChange{Selector: nativeSelector(rediscore.SelectorDialog, dialog.CallID, tags[0], tags[1]), Record: dialogField})
		encoded, e := encodeNativeDialog(dialog, nativeDialogRefs{Remote: &nativeInviteRefs{Request: originField, Final: responseField}}, &nativepb.Dialog{Transaction: tx.Key, CompletionFlow: proto.Clone(procedure.Metadata.Flow).(*pb.FlowRef)})
		if e != nil {
			return nil, e
		}
		changes = append(changes, rediscore.Change{Field: dialogField, Value: encoded})
		for _, effect := range native.DialogEffects {
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
				return nil, errors.New("unexpected INVITE dialog response effect")
			}
		}
	}
	finalSnapshot, err := nativeProjectSnapshot(next, native.Dialog, nil, nil)
	if err != nil {
		return nil, err
	}
	outputDialog := ""
	if native.Dialog != nil {
		outputDialog = dialogField
	}
	intent, outputChanges, shared, err := prepareRedisCallResponseOutput("INVITE", decision.Status, finalSnapshot, procedure.Metadata.Flow, key, native.Message, procedureField, outputDialog, responseField, physicalField, outputField, until, false)
	if err != nil {
		return nil, err
	}
	changes = append(changes, outputChanges...)
	if !shared {
		wire, e := selectedmessage.Marshal(native.Message)
		if e != nil {
			return nil, e
		}
		changes = append(changes, rediscore.Change{Field: responseField, Value: wire})
	}
	if procedure.Refs.LastResponse == heldField {
		changes = append(changes, rediscore.Change{Field: heldField, Delete: true})
	}
	metadata := proto.Clone(procedure.Metadata).(*nativepb.Procedure)
	metadata.DecisionPending, metadata.Proposal, metadata.RequestDialog = false, nil, ""
	metadata.CommandResult = "r:result-" + strconv.FormatUint(decision.Sequence, 10)
	metadata.Emissions++
	metadata.Outputs = append(metadata.Outputs, outputField)
	slices.Sort(metadata.Outputs)
	refs := procedure.Refs
	refs.LastResponse = responseField
	procedureBytes, err := encodeNativeProcedure(native.Transaction, refs, metadata)
	if err != nil {
		return nil, err
	}
	contextBytes, err := encodeNativeContext(next)
	if err != nil {
		return nil, err
	}
	changes = append(changes, rediscore.Change{Field: "r:context", Value: contextBytes}, rediscore.Change{Field: procedureField, Value: procedureBytes}, rediscore.Change{Field: metadata.CommandResult, Value: result})
	timers = append(timers, rediscore.TimerChange{Member: "out-" + outputField[2:], Due: until})
	_, err = store.Commit(ctx, rediscore.Publication{Identity: selected.Identity, Revision: selected.View.Revision, Deadline: until, Application: &lease, BindApplication: true, SpendReservation: true, Changes: changes, NewOutputs: []rediscore.OutputIntent{intent}, Selectors: selectors, Timers: &rediscore.TimerMutation{Expected: selected.View.Timer, Target: selected.View.Timer, Changes: timers}})
	if err != nil {
		return nil, err
	}
	if ctx.Err() != nil || !time.Now().Before(until) {
		return nil, rediscore.ErrUnknown
	}
	return &pb.OperationResult{Certainty: pb.OperationResult_ACCEPTED, SipStatus: decision.Status, Snapshot: finalSnapshot}, nil
}
