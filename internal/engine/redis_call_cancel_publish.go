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

// publishRedisInitialServerCancel commits a first CANCEL's exact SDK pair,
// including an established re-INVITE's selected dialog transition. Existing
// CANCELs use their replay path; physical completion remains separate.
func publishRedisInitialServerCancel(ctx context.Context, store *rediscore.Store, selected *selectedRedisServerCancel, key []byte, executor rediscore.TimerAssignment, deadline time.Time) error {
	if store == nil || selected == nil || selected.selected == nil || selected.context == nil || selected.invite.Metadata == nil || selected.prior != nil || selected.context.Kind != pb.ContextSnapshot_UAS && selected.context.Kind != pb.ContextSnapshot_UAC || selected.context.Revision >= 1<<53-1 || len(selected.context.InitialProcedures)+len(selected.context.NoninitialProcedures) >= stateNativeProcedures || !time.Now().Before(deadline) || ctx.Err() != nil {
		return errNativeRecord
	}
	origin, err := selected.nextInvite.Origin.Facts()
	if err != nil || !origin.IsRequest || origin.Method != sip.INVITE || selected.nextInvite.LastResponse == nil && selected.invite.Refs.LastResponse != "" || selected.nextCancel.LastResponse == nil {
		return errNativeRecord
	}
	inDialog := origin.ToTag != "" && !nativeInitialRefusalInviteProcedure(selected.context, selected.selected.Identity, selected.inviteField)
	if inDialog && !slices.Contains(selected.context.NoninitialProcedures, selected.inviteField) || !inDialog && (selected.context.Kind != pb.ContextSnapshot_UAS || !slices.Contains(selected.context.InitialProcedures, selected.inviteField)) {
		return errNativeRecord
	}
	requestID := selected.invite.Metadata.RequestId
	parsed, err := uuid.Parse(requestID)
	if err != nil || parsed.String() != requestID || selected.inviteField != "r:procedure-"+requestID || selected.invite.Refs.Origin != "r:origin-"+requestID || selected.nextInvite.Key != selected.invite.Native.Key || selected.nextCancel.Family != portable.NonInviteServer {
		return errNativeRecord
	}
	id := strings.TrimPrefix(selected.cancelField, "r:call-cancel-")
	if len(id) != 32 || selected.cancelField != "r:call-cancel-"+id || selected.cancelOrigin != "r:call-cancel-origin-"+id || selected.cancelResponse != "r:call-cancel-final-"+id {
		return errNativeRecord
	}
	for _, field := range []string{selected.cancelField, selected.cancelOrigin, selected.cancelResponse, "r:call-cancel-output-" + id, "r:call-cancel-wire-" + id, "r:call-cancel-invite-output-" + id, "r:call-cancel-invite-wire-" + id, "r:call-cancel-invite-final-" + id} {
		if len(selected.selected.View.Records[field]) != 0 {
			return errNativeRecord
		}
	}
	now := time.Now()
	until := minTime(deadline, now.Add(2*time.Second))
	next := proto.Clone(selected.context).(*nativepb.Context)
	next.Revision++
	next.NoninitialProcedures = append(next.NoninitialProcedures, selected.cancelField)
	slices.Sort(next.NoninitialProcedures)
	snapshot, err := nativeProjectSnapshot(next, nil, nil, nil)
	if err != nil {
		return err
	}
	inviteMetadata := proto.Clone(selected.invite.Metadata).(*nativepb.Procedure)
	if !inDialog {
		inviteMetadata.DecisionPending, inviteMetadata.Proposal, inviteMetadata.RequestDialog = false, nil, ""
	}
	cancelMetadata := &nativepb.Procedure{RequestId: id, Flow: proto.Clone(selected.flow).(*pb.FlowRef)}
	if selected.invite.Metadata.Application != nil {
		cancelMetadata.Application = proto.Clone(selected.invite.Metadata.Application).(*pb.ProcessRef)
	}
	inviteRefs := selected.invite.Refs
	var changes []rediscore.Change
	var outputs []rediscore.OutputIntent
	var timers []rediscore.TimerChange
	cancelled, ownResponse, inviteResponse := false, false, false
	for _, effect := range selected.effects {
		switch effect.Kind {
		case portable.Send:
			if effect.Message == nil || effect.Replay {
				return errNativeRecord
			}
			facts, e := effect.Message.Facts()
			if e != nil || facts.IsRequest {
				return errNativeRecord
			}
			procedureField, responseField, physicalField, outputField := selected.cancelField, selected.cancelResponse, "r:call-cancel-wire-"+id, "r:call-cancel-output-"+id
			metadata, flow := cancelMetadata, selected.flow
			switch facts.CSeqMethod {
			case sip.CANCEL:
				if ownResponse || facts.Status != 200 && facts.Status != 481 || !portable.MatchResponse(selected.nextCancel.Origin, *effect.Message) || !effect.Message.EqualWire(*selected.nextCancel.LastResponse) {
					return errNativeRecord
				}
				ownResponse = true
			case sip.INVITE:
				if inviteResponse || facts.Status != 487 || !portable.MatchResponse(selected.nextInvite.Origin, *effect.Message) || selected.nextInvite.LastResponse == nil || !effect.Message.EqualWire(*selected.nextInvite.LastResponse) || !inDialog && (next.Answered || len(next.Dialogs) != 0) {
					return errNativeRecord
				}
				inviteResponse = true
				procedureField, responseField, physicalField, outputField = selected.inviteField, "r:call-cancel-invite-final-"+id, "r:call-cancel-invite-wire-"+id, "r:call-cancel-invite-output-"+id
				metadata, flow = inviteMetadata, selected.invite.Metadata.Flow
				if inDialog {
					responseField, physicalField, outputField = "r:dialog-response-"+requestID+"-0", "r:dialog-wire-"+requestID+"-0", "r:dialog-output-"+requestID+"-0"
					if len(selected.selected.View.Records[physicalField])+len(selected.selected.View.Records[outputField]) != 0 || len(selected.selected.View.Records[responseField]) != 0 && (inviteRefs.LastResponse != responseField || metadata.Emissions != 0) {
						return rediscore.ErrConflict
					}
				}
				if previous := inviteRefs.LastResponse; previous != "" {
					held, e := redisCancelPreviousResponseHeld(requestID, previous, metadata.Outputs, inDialog)
					if e != nil {
						return e
					}
					if !held && previous != responseField {
						changes = append(changes, rediscore.Change{Field: previous, Delete: true})
					}
				}
				inviteRefs.LastResponse = responseField
			default:
				return errNativeRecord
			}
			if metadata.ProvisionalEmissions > metadata.Emissions || metadata.Emissions >= stateTransactionEmissions || metadata.Emissions-metadata.ProvisionalEmissions >= stateProfileTransmissions {
				return rediscore.ErrCapacity
			}
			dialogField := ""
			if inDialog && facts.CSeqMethod == sip.INVITE {
				var dialogData []byte
				dialogField, dialogData, e = selectRedisReinviteCancelDialog(ctx, store, selected, *effect.Message, responseField, outputField, now)
				if e != nil {
					return e
				}
				changes = append(changes, rediscore.Change{Field: dialogField, Value: dialogData})
			}
			intent, wireChanges, shared, e := prepareRedisCallResponseOutput(string(facts.CSeqMethod), uint32(facts.Status), snapshot, flow, key, *effect.Message, procedureField, dialogField, responseField, physicalField, outputField, until, false)
			if e != nil {
				return e
			}
			outputs = append(outputs, intent)
			changes = append(changes, wireChanges...)
			if !shared {
				wire, e := selectedmessage.Marshal(*effect.Message)
				if e != nil {
					return e
				}
				changes = append(changes, rediscore.Change{Field: responseField, Value: wire})
			}
			metadata.Emissions++
			metadata.Outputs = append(metadata.Outputs, outputField)
			slices.Sort(metadata.Outputs)
			timers = append(timers, rediscore.TimerChange{Member: "out-" + outputField[2:], Due: until})
		case portable.Cancelled:
			if cancelled {
				return errNativeRecord
			}
			cancelled = true
		case portable.Schedule, portable.CancelTimer, portable.Ended:
			// The paired SDK effects carry no source procedure. Exact native
			// before/after timer values below supply that association.
		default:
			return errors.New("unexpected first server CANCEL effect")
		}
	}
	if !ownResponse || cancelled != inviteResponse {
		return errNativeRecord
	}
	if cancelled {
		inviteMetadata.DecisionPending, inviteMetadata.Proposal, inviteMetadata.RequestDialog = false, nil, ""
		timers = append(timers, rediscore.TimerChange{Member: "call-decision-" + requestID, Delete: true})
	}
	if cancelled && !inDialog {
		if next.CallRetireAtMs == 0 {
			next.CallRetireAtMs = now.Add(2 * time.Minute).UnixMilli()
		}
		timers = append(timers, rediscore.TimerChange{Member: "call-retire", Due: time.UnixMilli(next.CallRetireAtMs)})
	}
	for _, kind := range []portable.Timer{portable.TimerTrying, portable.TimerH, portable.TimerI, portable.TimerL} {
		before, after := selected.invite.Native.Timers[kind], selected.nextInvite.Timers[kind]
		if after == before {
			continue
		}
		member := "call-tx-" + requestID + "-" + string(kind)
		if after.Active {
			timers = append(timers, rediscore.TimerChange{Member: member, Due: time.UnixMilli(after.At)})
		} else if before.Active {
			timers = append(timers, rediscore.TimerChange{Member: member, Delete: true})
		}
	}
	if j := selected.nextCancel.Timers[portable.TimerJ]; j.Active {
		timers = append(timers, rediscore.TimerChange{Member: "call-cancel-tx-" + id + "-J", Due: time.UnixMilli(j.At)})
	}
	if cancelled && selected.selected.Identity.Owner != "" {
		eventID := uuid.NewString()
		lifecycle := &nativepb.Lifecycle{Kind: pb.DialogEvent_CANCELLED, Snapshot: snapshot, DeadlineMs: now.Add(2 * time.Minute).UnixMilli(), NextMs: now.UnixMilli()}
		if inDialog {
			lifecycle.Root = selected.inviteField
			inviteMetadata.Events = append(inviteMetadata.Events, "r:event-"+eventID)
			slices.Sort(inviteMetadata.Events)
		}
		event, e := encodeNativeLifecycle(lifecycle)
		if e != nil {
			return e
		}
		changes = append(changes, rediscore.Change{Field: "r:event-" + eventID, Value: event})
		timers = append(timers, rediscore.TimerChange{Member: "event-" + eventID, Due: now})
	}
	inviteData, err := encodeNativeProcedure(selected.nextInvite, inviteRefs, inviteMetadata)
	if err != nil {
		return err
	}
	cancelData, err := encodeNativeProcedure(selected.nextCancel, nativeProcedureRefs{Origin: selected.cancelOrigin, LastResponse: selected.cancelResponse}, cancelMetadata)
	if err != nil {
		return err
	}
	cancelOrigin, err := selectedmessage.Marshal(selected.nextCancel.Origin)
	if err != nil {
		return err
	}
	contextData, err := encodeNativeContext(next)
	if err != nil {
		return err
	}
	changes = append(changes, rediscore.Change{Field: "r:context", Value: contextData}, rediscore.Change{Field: selected.inviteField, Value: inviteData}, rediscore.Change{Field: selected.cancelField, Value: cancelData}, rediscore.Change{Field: selected.cancelOrigin, Value: cancelOrigin})
	if inviteResponse && !inDialog {
		redrive, e := planRedisProvisionalOutputRedrive(ctx, store, &rediscore.ObservedContext{Identity: selected.selected.Identity, View: selected.selected.View}, selected.inviteField, selected.invite.Metadata)
		if e != nil {
			return e
		}
		timers = append(timers, redrive...)
	}
	target := selected.selected.View.Timer
	if target == (rediscore.TimerAssignment{}) {
		target = executor
	}
	publication := rediscore.Publication{Identity: selected.selected.Identity, Revision: selected.selected.View.Revision, Deadline: until, SpendReservation: true, Changes: changes, NewOutputs: outputs,
		Selectors: []rediscore.SelectorChange{{Selector: nativeSelector(rediscore.SelectorServerTransaction, selected.nextCancel.Key), Record: selected.cancelField}},
		Timers:    &rediscore.TimerMutation{Expected: selected.selected.View.Timer, Target: target, Changes: timers}}
	_, err = store.Commit(ctx, publication)
	if errors.Is(err, rediscore.ErrExpired) && selected.selected.View.Timer != (rediscore.TimerAssignment{}) && selected.selected.View.Timer != executor && ctx.Err() == nil && time.Now().Before(until) {
		publication.Timers.Target = executor
		_, err = store.Commit(ctx, publication)
	}
	return err
}

// Initial UAS responses have exactly one possible output owner. An un-emitted
// Trying response has none; an emitted response keeps its wire with that output.
func redisInitialResponseHasOutput(requestID, response string, outputs []string) (bool, error) {
	field, err := redisInitialResponseOutputField(requestID, response)
	return slices.Contains(outputs, field), err
}

func redisInitialResponseOutputField(requestID, response string) (string, error) {
	if response == "r:response-"+requestID+"-100" {
		return "r:output-" + requestID + "-100", nil
	}
	prefix := "r:invite-response-" + requestID + "-"
	if !strings.HasPrefix(response, prefix) || len(response) == len(prefix) {
		return "", errNativeRecord
	}
	return "r:invite-output-" + requestID + "-" + strings.TrimPrefix(response, prefix), nil
}

func redisCancelPreviousResponseHeld(requestID, response string, outputs []string, inDialog bool) (bool, error) {
	if !inDialog {
		return redisInitialResponseHasOutput(requestID, response, outputs)
	}
	prefix := "r:dialog-response-" + requestID + "-"
	if !strings.HasPrefix(response, prefix) || len(response) == len(prefix) {
		return false, errNativeRecord
	}
	return slices.Contains(outputs, "r:dialog-output-"+requestID+"-"+strings.TrimPrefix(response, prefix)), nil
}
