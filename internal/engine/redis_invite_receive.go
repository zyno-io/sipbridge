package engine

import (
	"context"
	"errors"
	"net"
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
	"sipbridge/internal/state"
)

// receiveRedisInvite publishes the bounded pending native procedure before its
// one direct presentation. Its application is only the recipient until a
// validated decision binds the context.
func receiveRedisInvite(ctx context.Context, store *rediscore.Store, in redisRegisterReceive) (*pb.RequestPresented, error) {
	now := time.Now()
	if store == nil || ctx.Err() != nil || !now.Before(in.Deadline) || in.Context == nil || in.Context.State == nil || in.Authority == nil || !proto.Equal(in.Context.State.Authority, in.Authority) || in.Context.State.GroupId != in.Context.Id || in.Context.State.Partition != 0 || !state.ValidReference(in.Context.Id) || !state.ValidReference(in.Context.State.Incarnation) || !validSendProcess(in.Application) || !validSendProcess(in.Executor) || !validPolicyFlow(in.Flow) || !sameProcess(in.Flow.Ingress, in.Executor) || !sameProcess(in.Context.Owner, in.Executor) || !state.ValidReference(in.LocalTag) || in.Lease.Application == "" || in.Lease.Term == "" {
		return nil, errors.New("invalid direct INVITE authority")
	}
	requestID, err := uuid.Parse(in.RequestID)
	if err != nil || requestID.String() != in.RequestID {
		return nil, errors.New("invalid INVITE request identity")
	}
	app, err := nativeApplicationReference(in.IdentityKey, in.Application)
	if err != nil || app != in.Lease.Application {
		return nil, errors.New("application lease identity mismatch")
	}
	facts, err := in.Message.Facts()
	if err != nil {
		return nil, err
	}
	if !facts.IsRequest || facts.Method != sip.INVITE || facts.ToTag != "" || facts.CallID != in.Context.State.CallId || in.Message.Transport() != in.Flow.Transport || in.Message.Source() != net.JoinHostPort(in.Flow.SourceIp, strconv.Itoa(int(in.Flow.SourcePort))) {
		return nil, errors.New("invalid initial INVITE identity")
	}
	until := minTime(in.Deadline, now.Add(2*time.Second))
	transaction, effects, err := nativeServerStart(in.Message, in.LocalTag, now.UnixMilli())
	if err != nil {
		return nil, err
	}
	var timers []rediscore.TimerChange
	presented := false
	for _, effect := range effects {
		switch {
		case effect.Kind == portable.PresentRequest:
			if presented {
				return nil, errNativeRecord
			}
			presented = true
		case effect.Kind == portable.Schedule && effect.Timer == portable.TimerTrying:
			timers = append(timers, rediscore.TimerChange{Member: "call-tx-" + in.RequestID + "-" + string(effect.Timer), Due: time.UnixMilli(effect.Deadline.At)})
		default:
			return nil, errors.New("unexpected initial INVITE effect")
		}
	}
	if !presented || len(timers) != 1 {
		return nil, errNativeRecord
	}
	observed, err := projectObservedNativeMessage(in.Message, false)
	if err != nil {
		return nil, err
	}
	selectors, err := nativeSelectorIDs(in.Message, false, false)
	if err != nil || len(selectors) != 1 || selectors[0].Kind != rediscore.SelectorServerTransaction {
		return nil, errNativeRecord
	}
	procedureField, originField := "r:procedure-"+in.RequestID, "r:origin-"+in.RequestID
	current := &nativepb.Context{Context: proto.Clone(in.Context).(*pb.ContextRef), Application: proto.Clone(in.Application).(*pb.ProcessRef), ApplicationLeaseTerm: in.Lease.Term, Executor: proto.Clone(in.Executor).(*pb.ProcessRef), Flow: proto.Clone(in.Flow).(*pb.FlowRef), Kind: pb.ContextSnapshot_UAS, CallId: facts.CallID, LocalTag: in.LocalTag, RemoteTag: facts.FromTag, LocalUri: facts.ToURI, RemoteUri: facts.FromURI, Transport: in.Flow.Transport, Selectors: append([]string(nil), observed.Selectors...), InitialProcedures: []string{procedureField}}
	contextBytes, err := encodeNativeContext(current)
	if err != nil {
		return nil, err
	}
	procedureBytes, err := encodeNativeProcedure(transaction, nativeProcedureRefs{Origin: originField}, &nativepb.Procedure{Flow: proto.Clone(in.Flow).(*pb.FlowRef), Application: proto.Clone(in.Application).(*pb.ProcessRef), RequestId: in.RequestID, PresentDeadlineMs: until.UnixMilli(), Direct: true, DecisionPending: true, DeadlineMs: in.Deadline.UnixMilli(), CreditContribution: uint64(residentServerInviteBudget)})
	if err != nil {
		return nil, err
	}
	origin, err := selectedmessage.Marshal(in.Message)
	if err != nil {
		return nil, err
	}
	snapshot, err := nativeProjectSnapshot(current, nil, nil, observed.Selectors)
	if err != nil {
		return nil, err
	}
	timers = append(timers, rediscore.TimerChange{Member: "call-decision-" + in.RequestID, Due: until})
	_, err = store.Commit(ctx, rediscore.Publication{Identity: rediscore.Identity{Context: in.Context.Id, Incarnation: in.Context.State.Incarnation}, Create: true, Deadline: until, CreditDelta: residentServerInviteBudget, Application: &in.Lease, Changes: []rediscore.Change{{Field: "r:context", Value: contextBytes}, {Field: procedureField, Value: procedureBytes}, {Field: originField, Value: origin}}, Selectors: []rediscore.SelectorChange{{Selector: selectors[0], Record: procedureField}}, Timers: &rediscore.TimerMutation{Target: in.Timer, Changes: timers}})
	if err != nil {
		return nil, err
	}
	if ctx.Err() != nil || !time.Now().Before(until) {
		return nil, rediscore.ErrUnknown
	}
	return &pb.RequestPresented{Snapshot: snapshot, RequestId: in.RequestID, Message: observed, Flow: proto.Clone(in.Flow).(*pb.FlowRef)}, nil
}
