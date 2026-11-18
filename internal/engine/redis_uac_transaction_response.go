package engine

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/emiago/sipgo/sip/portable"
	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/rediscore"
	"sipbridge/internal/selectedmessage"
)

func receiveRedisStandaloneResponse(ctx context.Context, store *rediscore.Store, flow *pb.FlowRef, message portable.Message, executor rediscore.TimerAssignment, deadline time.Time) (bool, error) {
	facts, err := message.Facts()
	if err != nil {
		return false, err
	}
	if facts.IsRequest || !redisStandaloneMethod(string(facts.CSeqMethod)) {
		return false, nil
	}
	if validateRedisNotifyResponseMetadata(message, flow) != nil {
		return true, errNativeRecord
	}
	selectors, err := nativeSelectorIDs(message, false, false)
	if err != nil || len(selectors) == 0 || selectors[0].Kind != rediscore.SelectorClientTransaction {
		return true, errNativeRecord
	}
	match, err := store.LookupSelector(ctx, selectors[0], "r:context")
	if errors.Is(err, rediscore.ErrAbsent) {
		return false, nil
	}
	if err != nil {
		return true, err
	}
	const prefix = "r:transaction-procedure-"
	if !strings.HasPrefix(match.Record, prefix) {
		return false, nil
	}
	s, err := selectRedisTransaction(ctx, store, match.Identity.Context, match.Identity.Incarnation, strings.TrimPrefix(match.Record, prefix))
	if err != nil {
		return true, err
	}
	if s.selected.Identity != match.Identity || s.selected.View.Revision != match.View.Revision || s.selected.View.Timer != match.View.Timer {
		return true, rediscore.ErrConflict
	}
	if !proto.Equal(s.output.Write.Flow, flow) {
		return true, rediscore.ErrConflict
	}
	tx, err := s.attach(ctx, store, &message)
	if err != nil {
		return true, err
	}
	if !portable.MatchResponse(tx.Origin, message) {
		return true, rediscore.ErrConflict
	}
	next, effects, err := portable.StepTransaction(tx, portable.Input{Kind: portable.ReceiveResponse, Now: time.Now().UnixMilli(), Message: message})
	if err != nil {
		return true, err
	}
	// Ignored duplicates do not change packet ownership or the original deadline.
	if tx.Phase == portable.Completed || tx.Phase == portable.Terminated {
		return true, nil
	}
	timers, err := redisTransactionTimers(s.names.id, effects)
	if err != nil {
		return true, err
	}
	refs := s.procedure.Refs
	var changes []rediscore.Change
	if next.LastResponse != nil {
		data, e := selectedmessage.Marshal(*next.LastResponse)
		if e != nil {
			return true, e
		}
		refs.LastResponse = s.names.response
		changes = append(changes, rediscore.Change{Field: s.names.response, Value: data})
	}
	value, err := encodeNativeProcedure(next, refs, s.procedure.Metadata)
	if err != nil {
		return true, err
	}
	changes = append(changes, rediscore.Change{Field: s.names.procedure, Value: value})
	if s.current.Revision >= 1<<53-1 {
		return true, errNativeRecord
	}
	s.current.Revision++
	if facts.Status >= 200 {
		s.current.Ended = true
		s.result.Pending = false
		s.result.Certainty = pb.OperationResult_COMPLETED
		s.result.Error = ""
		s.result.SipStatus = uint32(facts.Status)
		s.result.Request = s.names.origin
		s.result.Response = s.names.response
		data, e := encodeNativeResult(s.result)
		if e != nil {
			return true, e
		}
		changes = append(changes, rediscore.Change{Field: "r:result-1", Value: data})
		timers = append(timers, rediscore.TimerChange{Member: s.names.member, Due: time.UnixMilli(int64(s.output.Write.Permit.Authorization.DeadlineMs))})
	}
	data, err := encodeNativeContext(s.current)
	if err != nil {
		return true, err
	}
	changes = append(changes, rediscore.Change{Field: "r:context", Value: data})
	var hint *rediscore.UDPHint
	if flow.Transport == "udp" {
		worker, e := nativeWorkerReference(flow.Ingress)
		if e != nil {
			return true, e
		}
		hint = &rediscore.UDPHint{Worker: worker, Context: s.selected.Identity.Context, Incarnation: s.selected.Identity.Incarnation, Procedure: s.names.procedure, Stop: facts.Status >= 200}
	}
	return true, redisTransactionCommit(ctx, store, s, changes, timers, executor, deadline, facts.Status >= 200, hint)
}

// F/K remain native SDK duties. E is exclusively the existing local derivative.
func fireRedisStandaloneTimer(ctx context.Context, store *rediscore.Store, due *rediscore.DueContext, deadline time.Time) error {
	if due == nil || due.CheckedAtMS < due.DeadlineMS || !strings.HasPrefix(due.Member, "transaction-") {
		return errNativeRecord
	}
	parts := strings.Split(strings.TrimPrefix(due.Member, "transaction-"), "-")
	if len(parts) != 2 {
		return errNativeRecord
	}
	timer := portable.Timer(parts[1])
	if timer != portable.TimerF && timer != portable.TimerK {
		return errNativeRecord
	}
	s, err := selectRedisTransaction(ctx, store, due.Identity.Context, due.Identity.Incarnation, parts[0])
	if err != nil {
		return err
	}
	if s.selected.Identity != due.Identity || s.selected.View.Revision != due.Revision || s.selected.View.Timer != due.Assignment {
		return rediscore.ErrConflict
	}
	s.requireLease = true
	d := s.procedure.Native.Timers[timer]
	if !d.Active || d.Generation == 0 || d.At != due.DeadlineMS {
		return errNativeRecord
	}
	tx, err := s.attach(ctx, store, nil)
	if err != nil {
		return err
	}
	next, effects, err := portable.StepTransaction(tx, portable.Input{Kind: portable.TimerFired, Timer: timer, Generation: d.Generation, Now: due.CheckedAtMS})
	if err != nil {
		return err
	}
	timers, err := redisTransactionTimers(s.names.id, effects)
	if err != nil {
		return err
	}
	firedIndexed := false
	for _, change := range timers {
		if change.Member == due.Member {
			firedIndexed = true
		}
	}
	if !firedIndexed {
		timers = append(timers, rediscore.TimerChange{Member: due.Member, Delete: true})
	}
	timers = append(timers, rediscore.TimerChange{Member: s.names.member, Due: time.UnixMilli(int64(s.output.Write.Permit.Authorization.DeadlineMs))})
	value, err := encodeNativeProcedure(next, s.procedure.Refs, s.procedure.Metadata)
	if err != nil {
		return err
	}
	changes := []rediscore.Change{{Field: s.names.procedure, Value: value}}
	if timer == portable.TimerF && s.result.Certainty != pb.OperationResult_COMPLETED {
		failure := "SIP transaction timed out"
		for _, effect := range effects {
			if effect.Kind == portable.Ended && effect.Error != "" {
				failure = effect.Error
			}
		}
		s.result.Pending = false
		s.result.Certainty = pb.OperationResult_COMPLETED
		s.result.Error = failure
		s.result.SipStatus = 0
		s.result.Request = s.names.origin
		s.result.Response = ""
		data, e := encodeNativeResult(s.result)
		if e != nil {
			return e
		}
		changes = append(changes, rediscore.Change{Field: "r:result-1", Value: data})
	}
	if !s.current.Ended {
		if s.current.Revision >= 1<<53-1 {
			return errNativeRecord
		}
		s.current.Ended = true
		s.current.Revision++
		data, e := encodeNativeContext(s.current)
		if e != nil {
			return e
		}
		changes = append(changes, rediscore.Change{Field: "r:context", Value: data})
	}
	var hint *rediscore.UDPHint
	if s.output.Write.Flow.Transport == "udp" {
		worker, e := nativeWorkerReference(s.output.Write.Flow.Ingress)
		if e != nil {
			return e
		}
		hint = &rediscore.UDPHint{Worker: worker, Context: due.Identity.Context, Incarnation: due.Identity.Incarnation, Procedure: s.names.procedure, Stop: true}
	}
	return redisTransactionCommit(ctx, store, s, changes, timers, due.Assignment, deadline, timer == portable.TimerF, hint)
}
