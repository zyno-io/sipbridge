package engine

import (
	"context"
	"errors"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/emiago/sipgo/sip/portable"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/nativepb"
	"sipbridge/internal/rediscore"
	"sipbridge/internal/selectedmessage"
	"sipbridge/internal/storagecodec"
)

// completeRedisUACDigest makes one bounded, non-replayed credential lookup.
// Credentials live only in the reply and are cleared by stateBuildDigestNative.
func (e *redisNative) completeRedisUACDigest(ctx context.Context, store *rediscore.Store, ticket *redisUACDigestTicket, executor rediscore.TimerAssignment, deadline time.Time) error {
	if ticket == nil || ticket.candidate == nil || ticket.context == nil || ticket.application == nil {
		return errNativeRecord
	}
	deadline = minTime(deadline, ticket.deadline)
	deadline = minTime(deadline, time.Now().Add(2*time.Second))
	if ctx.Err() != nil || !time.Now().Before(deadline) {
		return rediscore.ErrExpired
	}
	lease, err := store.ObserveApplication(ctx, ticket.identity.Owner)
	if err != nil {
		return err
	}
	if lease.Term != ticket.identity.Term || lease.Until.IsZero() || !time.Now().Before(lease.Until) {
		return rediscore.ErrExpired
	}
	deadline = minTime(deadline, lease.Until)
	peer, err := e.control.Peer(ticket.application)
	if err != nil {
		return e.failRedisUACDigest(ticket, executor)
	}
	remaining := time.Until(deadline).Milliseconds()
	if remaining <= 0 {
		return e.failRedisUACDigest(ticket, executor)
	}
	callCtx, cancel := context.WithDeadline(ctx, deadline)
	reply, err := peer.Call(callCtx, &pb.Envelope{CorrelationId: uuid.NewString(), RemainingBudgetMs: uint32(remaining), Payload: &pb.Envelope_CredentialLookup{CredentialLookup: &pb.CredentialLookup{Context: ticket.context, Credential: ticket.candidate.work.Credential}}})
	cancel()
	if err != nil {
		return e.failRedisUACDigest(ticket, executor)
	}
	request, err := stateBuildDigestNative(ticket.candidate.work, reply.GetCredentialResult())
	if err != nil {
		return e.failRedisUACDigest(ticket, executor)
	}
	for attempts := 0; attempts < 8; attempts++ {
		err = publishRedisUACDigestRetry(ctx, store, ticket, request, e.cfg.StateIdentityKey, executor, deadline)
		if err == nil || errors.Is(err, rediscore.ErrUnknown) {
			return err
		}
		if !errors.Is(err, rediscore.ErrConflict) || ctx.Err() != nil || !time.Now().Before(deadline) {
			break
		}
	}
	return e.failRedisUACDigest(ticket, executor)
}

// publishRedisUACDigestRetry fences the exact challenge and live application
// after lookup. The child owns no new contribution or result backlink.
func publishRedisUACDigestRetry(ctx context.Context, store *rediscore.Store, ticket *redisUACDigestTicket, request portable.Message, key []byte, executor rediscore.TimerAssignment, deadline time.Time) error {
	if store == nil || ticket == nil || ticket.candidate == nil || len(key) < 32 || ctx.Err() != nil || !time.Now().Before(deadline) {
		return errNativeRecord
	}
	fields := []string{"r:context", ticket.procedure, "r:result-1", ticket.response}
	if ticket.root != ticket.procedure {
		fields = append(fields, ticket.root)
	}
	view, err := store.Read(ctx, ticket.identity, fields...)
	if err != nil {
		return err
	}
	current, err := decodeNativeContext(view.Records["r:context"])
	if err != nil {
		return err
	}
	proc, err := readPrivateProcedureRecord(view.Records[ticket.procedure])
	if err != nil {
		return err
	}
	root, err := readPrivateProcedureRecord(view.Records[ticket.root])
	if err != nil {
		return err
	}
	result, err := decodeNativeResult(view.Records["r:result-1"])
	if err != nil {
		return err
	}
	if current.Kind != pb.ContextSnapshot_UAC || !proto.Equal(current.Context, ticket.context) || !proto.Equal(current.Application, ticket.application) || current.ApplicationLeaseTerm != ticket.identity.Term || current.Ended || current.Answered || !slices.Contains(current.InitialProcedures, ticket.procedure) || !slices.Contains(current.InitialProcedures, ticket.root) || proc.Native.Version != portable.Version || proc.Native.Family != string(portable.InviteClient) || proc.Native.Key != ticket.candidate.failure.Transaction.Key || proc.Native.Cancelled || !proc.DigestHandled || proc.DigestAttempts != ticket.candidate.work.Attempt || proc.LastResponse != ticket.response || proc.CommandResult != "r:result-1" || proc.Root != "" && proc.Root != ticket.root || root.Root != "" || root.CreditContribution == 0 || root.CommandResult != "r:result-1" || result.Sequence != 1 || result.OperationId != ticket.operation || !proto.Equal(proc.Policy, ticket.policy) || proc.Policy == nil || proc.Credential == nil || proc.Application == nil || proc.Flow == nil || proc.Policy.DirectoryEpoch != current.DirectoryEpoch || !proto.Equal(proc.Policy.ExpectedDelivery, current.Delivery) || time.Now().UnixMilli() >= int64(proc.Policy.ExpiresAtMs) || current.Revision >= 1<<53-1 || len(current.InitialProcedures) >= 4 {
		return errNativeRecord
	}
	retained, err := selectedmessage.DecodeOpaque(view.Records[ticket.response])
	if err != nil || !retained.Equal(ticket.candidate.work.Challenge) {
		return rediscore.ErrConflict
	}
	if err = stateValidateDigestRetry(ticket.candidate.work, request); err != nil {
		return err
	}
	tx, err := portable.NewTransaction(portable.InviteClient, request, portable.DefaultTimers(), "")
	if err != nil {
		return err
	}
	now := time.Now()
	tx, effects, err := portable.StepTransaction(tx, portable.Input{Kind: portable.Start, Now: now.UnixMilli()})
	if err != nil {
		return err
	}
	ids, err := nativeSelectorIDs(request, true, false)
	if err != nil || len(ids) == 0 || ids[0].Kind != rediscore.SelectorClientTransaction {
		return errNativeRecord
	}
	id := nativeIdentity(key, "uac-digest", ticket.identity.Incarnation, ticket.identity.Context, proc.Native.Key, strconv.FormatUint(uint64(proc.DigestAttempts), 10))[:32]
	procedureField, originField, outputField := "r:uac-procedure-"+id, "r:uac-origin-"+id, "r:uac-output-"+id
	if slices.Contains(current.InitialProcedures, procedureField) {
		return rediscore.ErrConflict
	}
	until := minTime(deadline, time.UnixMilli(int64(proc.Policy.ExpiresAtMs)))
	physicalUntil := minTime(until, now.Add(2*time.Second))
	if !now.Before(physicalUntil) {
		return rediscore.ErrExpired
	}
	metadata := &nativepb.Procedure{Root: ticket.root, Flow: proto.Clone(proc.Flow).(*pb.FlowRef), Application: proto.Clone(proc.Application).(*pb.ProcessRef), Policy: proto.Clone(proc.Policy).(*pb.CommandAdmission), DeadlineMs: proc.DeadlineMs, CommandResult: "r:result-1", Credential: proto.Clone(proc.Credential).(*pb.CredentialRef), DigestAttempts: proc.DigestAttempts, Outputs: []string{outputField}, Emissions: 1}
	procData, err := encodeNativeProcedure(tx, nativeProcedureRefs{Origin: originField}, metadata)
	if err != nil {
		return err
	}
	facts, err := request.Facts()
	if err != nil {
		return err
	}
	current.InitialProcedures = append(current.InitialProcedures, procedureField)
	slices.Sort(current.InitialProcedures)
	current.Revision++
	contextData, err := encodeNativeContext(current)
	if err != nil {
		return err
	}
	snapshot, err := nativeProjectSnapshot(current, nil, nil, nil)
	if err != nil {
		return err
	}
	ref := &pb.StateEffectRef{Context: snapshot.Context, AttemptId: "a-" + uuid.NewString()}
	authority := current.Context.State.Authority
	scope := nativeIdentity(key, "native-completion-v1", authority.Service, authority.Generation, authority.PrimaryId, current.Context.State.GroupId, current.Context.State.Incarnation, current.Context.State.CallId, current.Context.Id)
	auth := &pb.EffectAuthorization{Effect: ref, AuthorityClass: pb.EffectAuthorization_APPLICATION, DeadlineMs: uint64(physicalUntil.UnixMilli()), ApplicationLeaseTerm: ticket.identity.Term, CompletionScope: scope}
	hash, err := SendFingerprint(snapshot, &pb.SipMessage{Method: "INVITE", Wire: request.WireBytes()}, auth, proc.Flow)
	if err != nil {
		return err
	}
	ref.Id, auth.Fingerprint = "e-"+hash, hash
	write := &pb.FlowWrite{Effect: ref, Flow: proc.Flow, Permit: &pb.SendPermit{Authorization: auth, Flow: proc.Flow, DirectoryEpoch: current.DirectoryEpoch, OrderingProof: nativeIdentity(key, "native-write-authority-v1", hash), CompletionScope: scope}}
	drain := drainNativeMessageObservation(request, facts)
	output := &nativepb.Output{Write: write, Procedure: procedureField, Result: "r:result-1", Operation: ticket.operation, Method: "INVITE", Drain: &nativepb.DrainObservation{Transaction: drain.Transaction[:], Ack: drain.ACK[:], Status: uint32(drain.Status), Request: drain.Request, Invite: drain.Invite, IsAck: drain.Ack, Cancel: drain.Cancel, Initial: drain.Initial}}
	lease := rediscore.ApplicationLeaseRef{Application: ticket.identity.Owner, Term: ticket.identity.Term}
	intent, changes, err := nativePreparePhysicalOutput(output, request, originField, outputField, &lease, nil)
	if err != nil {
		return err
	}
	changes = append(changes, rediscore.Change{Field: "r:context", Value: contextData}, rediscore.Change{Field: procedureField, Value: procData})
	timers := []rediscore.TimerChange{{Member: "out-" + strings.TrimPrefix(outputField, "r:"), Due: physicalUntil}}
	sends := 0
	for _, effect := range effects {
		switch effect.Kind {
		case portable.Schedule:
			if effect.Timer != portable.TimerA {
				timers = append(timers, rediscore.TimerChange{Member: "call-tx-" + id + "-" + string(effect.Timer), Due: time.UnixMilli(effect.Deadline.At)})
			}
		case portable.Send:
			if effect.Message == nil || !effect.Message.Equal(request) || effect.Replay {
				return errNativeRecord
			}
			sends++
		default:
			return errors.New("unexpected digest retry effect")
		}
	}
	if sends != 1 {
		return errNativeRecord
	}
	target := view.Timer
	if target == (rediscore.TimerAssignment{}) {
		target = executor
	}
	publication := rediscore.Publication{Identity: ticket.identity, Revision: view.Revision, Deadline: until, SpendReservation: true, Application: &lease, Changes: changes, NewOutputs: []rediscore.OutputIntent{intent}, Selectors: []rediscore.SelectorChange{{Selector: ids[0], Record: procedureField}}, Timers: &rediscore.TimerMutation{Expected: view.Timer, Target: target, Changes: timers}}
	_, err = store.Commit(ctx, publication)
	if errors.Is(err, rediscore.ErrExpired) && view.Timer != (rediscore.TimerAssignment{}) && view.Timer != executor && ctx.Err() == nil && time.Now().Before(until) {
		publication.Timers.Target = executor
		_, err = store.Commit(ctx, publication)
	}
	return err
}

// failRedisUACDigest ends the still-current attempt when credential lookup
// fails. It never changes an already completed result or replays the lookup.
func (e *redisNative) failRedisUACDigest(ticket *redisUACDigestTicket, executor rediscore.TimerAssignment) error {
	deadline := time.Now().Add(2 * time.Second)
	ctx, cancel, store, err := e.operation(e.ctx, deadline)
	if err != nil {
		return err
	}
	defer cancel()
	fields := []string{"r:context", ticket.procedure, "r:result-1"}
	if ticket.root != ticket.procedure {
		fields = append(fields, ticket.root)
	}
	view, err := store.Read(ctx, ticket.identity, fields...)
	if err != nil {
		return err
	}
	current, err := decodeNativeContext(view.Records["r:context"])
	if err != nil {
		return err
	}
	proc, err := readPrivateProcedureRecord(view.Records[ticket.procedure])
	if err != nil {
		return err
	}
	root := proc
	if ticket.root != ticket.procedure {
		root, err = readPrivateProcedureRecord(view.Records[ticket.root])
		if err != nil {
			return err
		}
	}
	result, err := decodeNativeResult(view.Records["r:result-1"])
	if err != nil {
		return err
	}
	if !proto.Equal(current.Context, ticket.context) || !proto.Equal(current.Application, ticket.application) || current.ApplicationLeaseTerm != ticket.identity.Term || current.Ended || current.Answered || !slices.Contains(current.InitialProcedures, ticket.procedure) || !slices.Contains(current.InitialProcedures, ticket.root) || proc.Native.Version != portable.Version || proc.Native.Family != string(portable.InviteClient) || proc.Native.Cancelled || !proc.DigestHandled || proc.DigestAttempts != ticket.candidate.work.Attempt || proc.LastResponse != ticket.response || proc.Native.Key != ticket.candidate.failure.Transaction.Key || proc.CommandResult != "r:result-1" || proc.Root != "" && proc.Root != ticket.root || root.Native.Version != portable.Version || root.Native.Family != string(portable.InviteClient) || root.Root != "" || root.CreditContribution == 0 || root.CommandResult != "r:result-1" || result.Sequence != 1 || result.OperationId != ticket.operation || len(root.Events) >= 3 || current.Revision >= 1<<53-1 {
		return rediscore.ErrConflict
	}
	now := time.Now()
	current.Ended = true
	current.Revision++
	if current.CallRetireAtMs == 0 {
		current.CallRetireAtMs = now.Add(2 * time.Minute).UnixMilli()
	}
	snapshot, err := nativeProjectSnapshot(current, nil, nil, nil)
	if err != nil {
		return err
	}
	eventID := uuid.NewString()
	eventField := "r:event-" + eventID
	facts, err := ticket.candidate.work.Challenge.Facts()
	if err != nil {
		return err
	}
	selectors, err := frozenMessageSelectors(ticket.candidate.work.Challenge, facts, false, false)
	if err != nil {
		return err
	}
	owners := []string{ticket.procedure}
	if (result.Certainty == pb.OperationResult_ACCEPTED || result.Certainty == pb.OperationResult_UNKNOWN) && result.Request != "" {
		if result.Request != root.Origin {
			return errNativeRecord
		}
		result.Response = ticket.response
		owners = append(owners, "r:result-1")
		slices.Sort(owners)
	}
	event, err := encodeNativeLifecycle(&nativepb.Lifecycle{Kind: pb.DialogEvent_FAILED, Snapshot: snapshot, OperationId: ticket.operation, Message: ticket.response, Root: ticket.root, Owners: owners, Selectors: selectors, DeadlineMs: now.Add(2 * time.Minute).UnixMilli(), NextMs: now.UnixMilli()})
	if err != nil {
		return err
	}
	contextData, err := encodeNativeContext(current)
	if err != nil {
		return err
	}
	root.Events = append(root.Events, eventField)
	slices.Sort(root.Events)
	rootData, err := marshalNativeRecord(storagecodec.NativeProcedure, root)
	if err != nil {
		return err
	}
	changes := []rediscore.Change{{Field: "r:context", Value: contextData}, {Field: ticket.root, Value: rootData}, {Field: eventField, Value: event}}
	var hint *rediscore.ResultHint
	if result.Certainty == pb.OperationResult_ACCEPTED || result.Certainty == pb.OperationResult_UNKNOWN {
		result.Certainty = pb.OperationResult_COMPLETED
		if result.Request != "" {
			result.Error = "SIP setup authentication unavailable"
		}
		result.Pending = false
		data, e := encodeNativeResult(result)
		if e != nil {
			return e
		}
		changes = append(changes, rediscore.Change{Field: "r:result-1", Value: data})
		hint = &rediscore.ResultHint{Context: ticket.identity.Context, Incarnation: ticket.identity.Incarnation, Sequence: 1}
	}
	target := view.Timer
	if target == (rediscore.TimerAssignment{}) {
		target = executor
	}
	lease := rediscore.ApplicationLeaseRef{Application: ticket.identity.Owner, Term: ticket.identity.Term}
	publication := rediscore.Publication{Identity: ticket.identity, Revision: view.Revision, Deadline: deadline, SpendReservation: true, Application: &lease, Changes: changes, ResultHint: hint, Timers: &rediscore.TimerMutation{Expected: view.Timer, Target: target, Changes: []rediscore.TimerChange{{Member: "event-" + eventID, Due: now}, {Member: "call-retire", Due: time.UnixMilli(current.CallRetireAtMs)}}}}
	_, err = store.Commit(ctx, publication)
	if errors.Is(err, rediscore.ErrExpired) && view.Timer != (rediscore.TimerAssignment{}) && view.Timer != executor && ctx.Err() == nil && time.Now().Before(deadline) {
		publication.Timers.Target = executor
		_, err = store.Commit(ctx, publication)
	}
	return err
}
