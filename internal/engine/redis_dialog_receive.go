package engine

import (
	"context"
	"errors"
	"math"
	"net"
	"slices"
	"strconv"
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

type redisDialogReceive struct {
	IdentityKey                       []byte
	Context                           *pb.ContextRef
	Application, Executor             *pb.ProcessRef
	Lease                             rediscore.ApplicationLeaseRef
	Flow                              *pb.FlowRef
	Message                           portable.Message
	PrincipalID, BindingID, RequestID string
	Timer                             rediscore.TimerAssignment
	Deadline                          time.Time
}

// receiveRedisDialogRequest admits a pending native request without applying
// its dialog/business transition. The caller owns the one direct presentation.
func receiveRedisDialogRequest(ctx context.Context, store *rediscore.Store, in redisDialogReceive) (*pb.RequestPresented, error) {
	if store == nil || in.Context == nil || in.Context.State == nil || !validSendProcess(in.Application) || !validSendProcess(in.Executor) || !validPolicyFlow(in.Flow) || !sameProcess(in.Flow.Ingress, in.Executor) || !time.Now().Before(in.Deadline) || ctx.Err() != nil {
		return nil, errNativeRecord
	}
	parsed, err := uuid.Parse(in.RequestID)
	if err != nil || parsed.String() != in.RequestID {
		return nil, errNativeRecord
	}
	app, err := nativeApplicationReference(in.IdentityKey, in.Application)
	if err != nil || app != in.Lease.Application || in.Lease.Term == "" {
		return nil, rediscore.ErrAuthority
	}
	facts, err := in.Message.Facts()
	if err != nil || !facts.IsRequest || facts.ToTag == "" || in.Message.Transport() != in.Flow.Transport || in.Message.Source() != net.JoinHostPort(in.Flow.SourceIp, strconv.Itoa(int(in.Flow.SourcePort))) {
		return nil, errNativeRecord
	}
	switch facts.Method {
	case sip.INVITE, sip.REFER, sip.INFO, sip.UPDATE, sip.NOTIFY:
	default:
		return nil, errors.New("application dialog request required")
	}
	ids, err := nativeSelectorIDs(in.Message, false, false)
	if err != nil || len(ids) != 2 || ids[0].Kind != rediscore.SelectorServerTransaction || ids[1].Kind != rediscore.SelectorDialog {
		return nil, errNativeRecord
	}
	selected, err := store.LookupSelector(ctx, ids[1], "r:context")
	if err != nil {
		return nil, err
	}
	if selected.Identity.Context != in.Context.Id || selected.Identity.Incarnation != in.Context.State.Incarnation || selected.Identity.Owner != in.Lease.Application || selected.Identity.Term != in.Lease.Term {
		return nil, rediscore.ErrAuthority
	}
	current, err := decodeNativeContext(selected.View.Records["r:context"])
	if err != nil {
		return nil, err
	}
	dialog, err := decodeNativeDialog(selected.View.Records[selected.Record])
	if err != nil {
		return nil, err
	}
	if current.Kind != pb.ContextSnapshot_UAS && current.Kind != pb.ContextSnapshot_UAC || current.Ended || !proto.Equal(current.Context, in.Context) || !sameProcess(current.Application, in.Application) || current.ApplicationLeaseTerm != in.Lease.Term || current.Transport != in.Flow.Transport || current.CallId != facts.CallID || !slices.Contains(current.Dialogs, selected.Record) || current.Revision >= 1<<53-1 || len(current.InitialProcedures)+len(current.NoninitialProcedures) >= stateNativeProcedures || current.PrincipalId != "" && in.PrincipalID != "" && current.PrincipalId != in.PrincipalID || current.BindingId != "" && in.BindingID != "" && current.BindingId != in.BindingID {
		return nil, errNativeRecord
	}
	if !proto.Equal(current.Flow, in.Flow) && (current.Transport != "tls" || in.Flow.Transport != "tls") {
		return nil, errors.New("dialog replacement requires qualified transport continuity")
	}
	procedureField, originField := "r:procedure-"+in.RequestID, "r:origin-"+in.RequestID
	fields := []string{procedureField, originField}
	for _, refs := range []*nativeInviteRefs{dialog.Refs.Local, dialog.Refs.Remote} {
		if refs != nil && !slices.Contains(fields, refs.Request) {
			fields = append(fields, refs.Request)
		}
	}
	view, err := store.ReadApplicationWitness(ctx, selected.Identity, in.Lease, fields...)
	if err != nil {
		return nil, err
	}
	if view.Revision != selected.View.Revision || view.Timer != selected.View.Timer || len(view.Records[procedureField]) != 0 || len(view.Records[originField]) != 0 || slices.Contains(current.NoninitialProcedures, procedureField) {
		return nil, rediscore.ErrConflict
	}
	memo, err := selectedmessage.NewMemo(3)
	if err != nil {
		return nil, err
	}
	if _, err = memo.Adopt(in.Message, "admitted"); err != nil {
		return nil, err
	}
	previous, err := dialog.attach(&nativeSelectedMessages{records: view.Records, memo: memo, provenance: "admitted"}, nativeDialogSelection{Local: nativeInviteSelection{Request: true}, Remote: nativeInviteSelection{Request: true}})
	if err != nil {
		return nil, err
	}
	now := time.Now()
	if _, _, err = portable.StepDialog(previous, portable.DialogInput{Kind: portable.DialogReceiveRequest, Message: in.Message, Now: now.UnixMilli(), Offer: facts.Method == sip.INVITE && facts.BodyLength > 0}); err != nil {
		return nil, err
	}
	until := minTime(in.Deadline, now.Add(2*time.Second))
	tx, effects, err := nativeServerStart(in.Message, current.LocalTag, now.UnixMilli())
	if err != nil {
		return nil, err
	}
	timers := []rediscore.TimerChange{{Member: "call-decision-" + in.RequestID, Due: until}}
	for _, effect := range effects {
		switch {
		case effect.Kind == portable.PresentRequest:
		case effect.Kind == portable.Schedule && facts.Method == sip.INVITE && effect.Timer == portable.TimerTrying:
			timers = append(timers, rediscore.TimerChange{Member: "call-tx-" + in.RequestID + "-" + string(effect.Timer), Due: time.UnixMilli(effect.Deadline.At)})
		default:
			return nil, errNativeRecord
		}
	}
	var proposal *pb.FlowAdoptionRequest
	if !proto.Equal(current.Flow, in.Flow) {
		if current.Delivery != nil && current.Delivery.NativeRevision == math.MaxUint64 {
			return nil, errNativeRecord
		}
		proposal = &pb.FlowAdoptionRequest{Context: current.Context, Application: proto.Clone(in.Application).(*pb.ProcessRef), PrincipalId: current.PrincipalId, BindingId: current.BindingId, Flow: proto.Clone(in.Flow).(*pb.FlowRef), NativeRevision: current.GetDelivery().GetNativeRevision() + 1}
	}
	credit := residentSmallBudget
	if facts.Method == sip.INVITE {
		credit = residentServerInviteBudget
	}
	procedureData, err := encodeNativeProcedure(tx, nativeProcedureRefs{Origin: originField}, &nativepb.Procedure{Flow: proto.Clone(in.Flow).(*pb.FlowRef), Application: proto.Clone(in.Application).(*pb.ProcessRef), RequestId: in.RequestID, PresentDeadlineMs: until.UnixMilli(), Direct: true, DecisionPending: true, RequestDialog: selected.Record, DeadlineMs: in.Deadline.UnixMilli(), Proposal: proposal, CreditContribution: uint64(credit)})
	if err != nil {
		return nil, err
	}
	originData, err := selectedmessage.Marshal(in.Message)
	if err != nil {
		return nil, err
	}
	next := proto.Clone(current).(*nativepb.Context)
	next.Revision++
	next.NoninitialProcedures = append(next.NoninitialProcedures, procedureField)
	slices.Sort(next.NoninitialProcedures)
	contextData, err := encodeNativeContext(next)
	if err != nil {
		return nil, err
	}
	observed, err := projectObservedNativeMessage(in.Message, false)
	if err != nil {
		return nil, err
	}
	snapshot, err := nativeProjectSnapshot(next, &previous, nil, nil)
	if err != nil {
		return nil, err
	}
	target := view.Timer
	if target == (rediscore.TimerAssignment{}) {
		target = in.Timer
	}
	publication := rediscore.Publication{Identity: selected.Identity, Revision: view.Revision, Deadline: until, Application: &in.Lease, WitnessApplication: true, CreditDelta: credit, SpendReservation: true,
		Changes:   []rediscore.Change{{Field: "r:context", Value: contextData}, {Field: procedureField, Value: procedureData}, {Field: originField, Value: originData}},
		Selectors: []rediscore.SelectorChange{{Selector: ids[0], Record: procedureField}}, Timers: &rediscore.TimerMutation{Expected: view.Timer, Target: target, Changes: timers}}
	_, err = store.Commit(ctx, publication)
	if errors.Is(err, rediscore.ErrExpired) && view.Timer != (rediscore.TimerAssignment{}) && view.Timer != in.Timer && ctx.Err() == nil && time.Now().Before(until) {
		publication.Timers.Target = in.Timer
		_, err = store.Commit(ctx, publication)
	}
	if err != nil {
		return nil, err
	}
	if ctx.Err() != nil || !time.Now().Before(until) {
		return nil, rediscore.ErrUnknown
	}
	attributeNativeCall(ctx, current)
	return &pb.RequestPresented{Snapshot: snapshot, RequestId: in.RequestID, Message: observed, Flow: proto.Clone(in.Flow).(*pb.FlowRef), FlowAdoption: proposal}, nil
}
