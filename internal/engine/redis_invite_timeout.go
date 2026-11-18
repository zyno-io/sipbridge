package engine

import (
	"context"
	"slices"
	"time"

	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/nativepb"
	"sipbridge/internal/rediscore"
	"sipbridge/internal/selectedmessage"
)

// timeoutRedisInitialInvite commits the native 503 for one unbound initial presentation.
func timeoutRedisInitialInvite(ctx context.Context, store *rediscore.Store, key []byte, advertise nativeSIPAdvertisement, due *rediscore.DueContext, deadline time.Time, current *nativepb.Context, procedure nativeProcedureRecord, view *rediscore.View, id string) error {
	procedureField, originField := "r:procedure-"+id, "r:origin-"+id
	heldField := "r:response-" + id + "-100"
	responseField, physicalField, outputField := "r:invite-response-"+id+"-0", "r:invite-wire-"+id+"-0", "r:invite-output-"+id+"-0"
	if due.Identity.Owner != "" || due.Identity.Term != "" || current.Context == nil || current.Context.State == nil || current.Context.Id != due.Identity.Context || current.Context.State.Incarnation != due.Identity.Incarnation ||
		current.Kind != pb.ContextSnapshot_UAS || current.Ended || current.Answered || current.CommandSequence != 0 || current.Revision >= 1<<53-1 || len(current.Dialogs) != 0 ||
		len(current.InitialProcedures) != 1 || current.InitialProcedures[0] != procedureField || len(current.NoninitialProcedures) != 0 ||
		procedure.Native.Family != portable.InviteServer || !procedure.Metadata.Direct || !procedure.Metadata.DecisionPending || procedure.Metadata.RequestId != id ||
		procedure.Metadata.PresentDeadlineMs != due.DeadlineMS || procedure.Metadata.CommandResult != "" || procedure.Metadata.RequestDialog != "" ||
		procedure.Metadata.Emissions != 0 || len(procedure.Metadata.Outputs) != 0 || procedure.Refs.Origin != originField ||
		procedure.Refs.LastResponse != "" && procedure.Refs.LastResponse != heldField {
		return errNativeRecord
	}
	extra, err := store.Read(ctx, due.Identity, heldField, responseField, physicalField, outputField)
	if err != nil {
		return err
	}
	if extra.Revision != due.Revision || extra.Timer != due.Assignment {
		return rediscore.ErrConflict
	}
	if len(extra.Records[responseField]) != 0 || len(extra.Records[physicalField]) != 0 || len(extra.Records[outputField]) != 0 {
		return rediscore.ErrConflict
	}
	for field, data := range extra.Records {
		view.Records[field] = data
	}
	held := procedure.Refs.LastResponse == heldField
	if !held && len(view.Records[heldField]) != 0 {
		return errNativeRecord
	}
	memo, err := selectedmessage.NewMemo(2)
	if err != nil {
		return err
	}
	tx, err := procedure.attach(&nativeSelectedMessages{records: view.Records, memo: memo, provenance: "admitted"}, true, held, false)
	if err != nil {
		return err
	}
	facts, err := tx.Origin.Facts()
	if err != nil || !facts.IsRequest || facts.Method != sip.INVITE || facts.ToTag != "" {
		return errNativeRecord
	}
	now := time.Now()
	contact, sentBy, err := advertise.resolve(tx.Origin.Transport())
	if err != nil {
		return err
	}
	candidate, err := nativePrepareInitialInviteResponse(tx, &pb.Respond{RequestId: id, Status: 503}, current.LocalTag, contact, sentBy, now.UnixMilli())
	if err != nil || candidate.Dialog != nil {
		if err != nil {
			return err
		}
		return errNativeRecord
	}
	next := proto.Clone(current).(*nativepb.Context)
	next.Ended = true
	next.Revision++
	if next.CallRetireAtMs == 0 {
		next.CallRetireAtMs = now.Add(2 * time.Minute).UnixMilli()
	}
	if next.CallRetireAtMs <= 0 || next.CallRetireAtMs > 1<<53-1 {
		return errNativeRecord
	}
	snapshot, err := nativeProjectSnapshot(next, nil, nil, nil)
	if err != nil {
		return err
	}
	until := minTime(deadline, now.Add(2*time.Second))
	intent, changes, shared, err := prepareRedisCallResponseOutput("INVITE", 503, snapshot, procedure.Metadata.Flow, key, candidate.Message, procedureField, "", responseField, physicalField, outputField, until, false)
	if err != nil {
		return err
	}
	if !shared {
		data, e := selectedmessage.Marshal(candidate.Message)
		if e != nil {
			return e
		}
		changes = append(changes, rediscore.Change{Field: responseField, Value: data})
	}
	if held {
		changes = append(changes, rediscore.Change{Field: heldField, Delete: true})
	}
	metadata := proto.Clone(procedure.Metadata).(*nativepb.Procedure)
	metadata.DecisionPending, metadata.Proposal, metadata.RequestDialog = false, nil, ""
	metadata.Emissions++
	metadata.Outputs = append(metadata.Outputs, outputField)
	slices.Sort(metadata.Outputs)
	refs := procedure.Refs
	refs.LastResponse = responseField
	encoded, err := encodeNativeProcedure(candidate.Transaction, refs, metadata)
	if err != nil {
		return err
	}
	contextBytes, err := encodeNativeContext(next)
	if err != nil {
		return err
	}
	changes = append(changes, rediscore.Change{Field: procedureField, Value: encoded}, rediscore.Change{Field: "r:context", Value: contextBytes})
	timers := []rediscore.TimerChange{{Member: due.Member, Delete: true}, {Member: "out-" + outputField[2:], Due: until}, {Member: "call-retire", Due: time.UnixMilli(next.CallRetireAtMs)}}
	for _, effect := range candidate.Effects {
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
	_, err = store.Commit(ctx, rediscore.Publication{Identity: due.Identity, Revision: due.Revision, Deadline: deadline, SpendReservation: true, Changes: changes, NewOutputs: []rediscore.OutputIntent{intent}, Timers: &rediscore.TimerMutation{Expected: due.Assignment, Target: due.Assignment, RequireLease: true, Changes: timers}})
	return err
}

// validRedisInitialInviteTimeoutOutput proves the single no-command 503 producer.
func validRedisInitialInviteTimeoutOutput(identity rediscore.Identity, current *nativepb.Context, procedure *nativepb.Procedure, output *nativepb.Output, id, field string) bool {
	return identity.Owner == "" && identity.Term == "" && current != nil && current.Context != nil && current.Context.State != nil &&
		current.Context.Id == identity.Context && current.Context.State.Incarnation == identity.Incarnation && current.Kind == pb.ContextSnapshot_UAS &&
		current.Ended && current.CallRetireAtMs > 0 && !current.Answered && current.CommandSequence == 0 && len(current.Dialogs) == 0 &&
		slices.Contains(current.InitialProcedures, "r:procedure-"+id) && output != nil && output.Write != nil && output.Write.Flow != nil && procedure != nil && procedure.Native != nil &&
		procedure.Native.Version == portable.Version && procedure.Native.Family == string(portable.InviteServer) &&
		(procedure.Native.Phase == string(portable.Completed) || procedure.Native.Phase == string(portable.Confirmed) || procedure.Native.Phase == string(portable.Terminated)) &&
		procedure.Native.Reliable == (output.Write.Flow.Transport != "udp") && procedure.Direct && !procedure.DecisionPending &&
		procedure.RequestId == id && procedure.PresentDeadlineMs > 0 && procedure.Origin == "r:origin-"+id && procedure.LastResponse == "r:invite-response-"+id+"-0" &&
		procedure.CommandResult == "" && slices.Contains(procedure.Outputs, field) && output.Write.Effect != nil &&
		output.Write.Permit != nil && output.Write.Permit.Authorization != nil && output.Write.Permit.Authorization.AuthorityClass == pb.EffectAuthorization_RESPONSE &&
		proto.Equal(current.Context, output.Write.Effect.Context) && proto.Equal(procedure.Flow, output.Write.Flow) &&
		field == "r:invite-output-"+id+"-0" && output.Procedure == "r:procedure-"+id && output.Method == "INVITE" && output.Status == 503 &&
		output.Result == "" && output.Operation == "" && output.Dialog == "" && !output.Replay && !output.CleanupAfterAck && output.AckSequence == 0
}
