package engine

import (
	"bytes"
	"errors"
	"fmt"
	"strings"

	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/state"
)

var errStateEffectMissing = errors.New("effect not found")

func stateSameContext(a, b *pb.ContextRef) bool {
	return a != nil && b != nil && a.Id == b.Id && a.State != nil && b.State != nil && proto.Equal(a.State, b.State)
}
func (r *stateReduction) findEffect(id string, drain ...*state.LocalDrainObservation) (int, state.Effect, stateEngineEffect, error) {
	for index, effect := range r.payload.Effects {
		if effect.ID == id {
			if !stateSupportedEffectKind(effect.Kind) {
				return 0, effect, stateEngineEffect{}, state.ErrCorrupt
			}
			var body stateEngineEffect
			if body.unmarshalBinary(effect.Payload, false, drain...) != nil {
				return 0, effect, body, state.ErrCorrupt
			}
			return index, effect, body, nil
		}
	}
	return 0, state.Effect{}, stateEngineEffect{}, errStateEffectMissing
}
func (r *stateReduction) setEffect(index int, effect state.Effect, body stateEngineEffect) {
	effect.Payload, _ = body.MarshalBinary()
	r.payload.Effects[index] = effect
}

func (r *stateReduction) reject() error {
	if r.in.Context == nil || r.in.Flow == nil || r.in.Message == nil || r.in.Status < 300 || r.in.Status > 699 {
		return errors.New("invalid rejection input")
	}
	m := portable.Message{Wire: r.in.Message.Wire, Transport: r.in.Flow.Transport}
	parsed, err := m.Parse()
	if err != nil {
		return err
	}
	request, ok := parsed.(*sip.Request)
	if !ok {
		return errors.New("request response not permitted")
	}
	if parsed.CallID().Value() != r.record.Ref.CallID {
		return errors.New("rejection group mismatch")
	}
	if request.IsAck() {
		return r.ackRejection(m)
	}
	if r.requestFence == nil {
		if err := r.protectRequestFence(); err != nil {
			return err
		}
	}
	if r.requestFence != nil {
		if r.requestFence.kind == "refused" {
			return r.handleRefusedRequest()
		}
		return r.handledRequest()
	}
	if r.deadline > 0 && r.now >= r.deadline {
		return r.handledRequest()
	}
	id, err := r.engine.rejectionContextID(r.in.Flow, r.in.Message, r.in.Status)
	if err != nil {
		return err
	}
	if r.in.Context.Id != id {
		return errors.New("rejection context identity mismatch")
	}
	c := r.group.Contexts[r.in.Context.Id]
	if c == nil {
		if len(r.group.Contexts) >= 32 {
			return errors.New("rejection context capacity")
		}
		c = &stateContext{Rejection: true, Snapshot: &pb.ContextSnapshot{Context: r.engine.contextRef(r.record.Ref, r.in.Context.Id), Application: proto.Clone(r.in.Application).(*pb.ProcessRef), CallId: r.record.Ref.CallID, Kind: pb.ContextSnapshot_TRANSACTION, Transport: r.in.Flow.Transport, Ended: true}, Flow: r.in.Flow, Origin: m, Tag: r.engine.id("tag", r.record.Ref.Incarnation, r.in.Context.Id)[:32], Transactions: make(map[string]*stateTransaction), Requests: make(map[string]string), Dialogs: make(map[string]portable.Dialog), RetireMS: r.now + 120000}
		r.group.Contexts[r.in.Context.Id] = c
	}
	if !c.Rejection || c.Snapshot.Kind != pb.ContextSnapshot_TRANSACTION || !c.Snapshot.Ended ||
		!proto.Equal(c.Flow, r.in.Flow) || !bytes.Equal(c.Origin.Wire, m.Wire) || len(c.Snapshot.Selectors) != 0 {
		return errors.New("rejection context conflict")
	}
	for _, tx := range c.Transactions {
		if !portable.MatchRequest(tx.Native.Origin, m) || !bytes.Equal(tx.Native.Origin.Wire, m.Wire) ||
			!proto.Equal(tx.Flow, r.in.Flow) || tx.Native.LastResponse == nil {
			return errors.New("rejection transaction conflict")
		}
		response, err := tx.Native.LastResponse.Parse()
		if err != nil {
			return err
		}
		refusal, ok := response.(*sip.Response)
		if !ok || refusal.StatusCode != int(r.in.Status) {
			return errors.New("rejection response conflict")
		}
		return r.send(c, tx, *tx.Native.LastResponse, true)
	}
	family := portable.NonInviteServer
	if request.IsInvite() {
		family = portable.InviteServer
	}
	native, err := portable.NewTransaction(family, m, portable.DefaultTimers(), c.Tag)
	if err != nil {
		return err
	}
	native, _, err = portable.StepTransaction(native, portable.Input{Kind: portable.Start, Now: r.now})
	if err != nil {
		return err
	}
	tx := &stateTransaction{Native: native, Flow: r.in.Flow}
	tx.DispositionUntilMS, err = r.dispositionFloor()
	if err != nil {
		return err
	}
	if c.Transactions[native.Key] != nil {
		return errors.New("rejection transaction key conflict")
	}
	c.Transactions[native.Key] = tx
	response, err := r.buildResponse(c, m, int(r.in.Status), reason(r.in.Status), nil, nil)
	if err != nil {
		return err
	}
	r.touch(c)
	return r.stepTransaction(c, tx, portable.Input{Kind: portable.SendResponse, Now: r.now, Message: response})
}

func (r *stateReduction) ackRejection(message portable.Message) error {
	// These contexts deliberately publish no routing selectors. Only the exact
	// physical flow and retained native INVITE/final-response identity may stop
	// their negative-response procedure; this cannot admit business/dialog work.
	parsed, err := message.Parse()
	if err != nil {
		return err
	}
	request := parsed.(*sip.Request)
	for _, c := range r.group.Contexts {
		if !c.Rejection || !proto.Equal(c.Flow, r.in.Flow) {
			continue
		}
		for _, tx := range c.Transactions {
			if tx.Native.Family == portable.InviteServer && proto.Equal(tx.Flow, r.in.Flow) &&
				portable.MatchRequest(tx.Native.Origin, message) && tx.Native.LastResponse != nil {
				parsed, err := tx.Native.LastResponse.Parse()
				if err != nil {
					return err
				}
				response, ok := parsed.(*sip.Response)
				if !ok || tag(request.To().Params) != tag(response.To().Params) {
					continue
				}
				return r.stepTransaction(c, tx, portable.Input{Kind: portable.ReceiveRequest, Now: r.now, Message: message})
			}
		}
	}
	return nil
}

func (r *stateReduction) startEffect() error {
	index, effect, body, err := r.findEffect(r.in.Effect)
	if err != nil {
		return err
	}
	if effect.Kind == "deliver" || effect.Status != state.EffectPending {
		return errors.New("RPC effect cannot start")
	}
	business := effect.Kind == "present" || effect.Kind == "event" || effect.Kind == "adopt" || effect.Kind == "open" || effect.Kind == "credential" ||
		effect.Kind == "wire" && body.Authorization.GetAuthorityClass() == pb.EffectAuthorization_APPLICATION
	if r.engine.autonomousCompletion() && business {
		if _, err := r.effectNativeOwner(body); err != nil {
			return err
		}
	}
	startUntil := effect.DeadlineMS
	if effect.Kind == "event" {
		startUntil += 120000
	}
	if effect.Kind != "owner" && startUntil <= r.now {
		return state.ErrExpired
	}
	effect.Status = state.EffectDispatching
	effect.AttemptID = "rpc-" + r.engine.id(effect.ID, r.engine.cfg.Process.InstanceId, r.engine.cfg.Process.Epoch)[:48]
	body.Executor = proto.Clone(r.engine.cfg.Process).(*pb.ProcessRef)
	r.setEffect(index, effect, body)
	r.result.Snapshot = &pb.ContextSnapshot{Executor: body.Executor}
	return nil
}

func (r *stateReduction) reserve() error {
	write := r.in.Write
	if write == nil || write.Effect == nil || write.Permit == nil || write.Permit.Authorization == nil {
		return errors.New("effect permit required")
	}
	var drain state.LocalDrainObservation
	index, effect, body, err := r.findEffect(write.Effect.Id, &drain)
	if errors.Is(err, errStateEffectMissing) && r.engine.usesCAS() {
		r.result.SipStatus = uint32(pb.DeliveryResult_UNKNOWN)
		r.result.Response = &pb.SipMessage{}
		return nil // Collected physical evidence never authorizes another send.
	}
	if err != nil {
		return err
	}
	if effect.Kind != "deliver" || body.Permit == nil || body.Authorization == nil || !proto.Equal(body.Permit, write.Permit) ||
		!stateSameContext(write.Effect.Context, body.Authorization.Effect.Context) || write.Effect.AttemptId != body.Authorization.Effect.AttemptId ||
		!proto.Equal(write.Flow, body.Permit.Flow) || !sameProcess(write.Flow.Ingress, r.in.Executor) {
		return errors.New("effect authority mismatch")
	}
	if !stateACKDependencyReady(r.group.Contexts[body.Context], body) {
		return errors.New("cleanup ACK has no confirmed physical write")
	}
	if effect.AttemptID != "" && effect.AttemptID != write.Effect.AttemptId {
		return errors.New("effect attempt already selected")
	}
	knownUnavailable := r.engine.usesCAS() && (effect.Status == state.EffectCancelled || effect.Status == state.EffectDispatching && len(body.Wire) == 0)
	if effect.Status == state.EffectWritten || effect.Status == state.EffectNotWritten || effect.Status == state.EffectUnknown || knownUnavailable {
		outcome := pb.DeliveryResult_UNKNOWN
		if effect.Status == state.EffectWritten {
			outcome = pb.DeliveryResult_WRITTEN
		} else if effect.Status == state.EffectNotWritten {
			outcome = pb.DeliveryResult_NOT_WRITTEN
		}
		r.result.SipStatus = uint32(outcome)
		r.result.Response = &pb.SipMessage{}
		return nil
	}
	if effect.Status != state.EffectAuthorized && effect.Status != state.EffectDispatching {
		return errors.New("effect is not authorized")
	}
	if effect.Status == state.EffectAuthorized && body.Native && body.Authorization.AuthorityClass == pb.EffectAuthorization_APPLICATION {
		c := r.group.Contexts[body.Context]
		if c == nil {
			return policyRefusal("context unavailable")
		}
		tx := c.Transactions[body.Transaction]
		if tx == nil || tx.Policy == nil || tx.Policy.Command != body.Command {
			return policyRefusal("procedure selection changed")
		}
		flow, err := r.policyFlow(c, tx)
		if err != nil {
			return err
		}
		if !proto.Equal(flow, write.Flow) {
			return policyRefusal("selected flow changed")
		}
		if _, err := r.effectNativeOwner(body); err != nil {
			return err
		}
	}
	if effect.Status != state.EffectDispatching && (effect.DeadlineMS <= r.now || int64(write.Permit.Authorization.DeadlineMs) <= r.now) {
		return state.ErrExpired
	}
	effect.Status = state.EffectDispatching
	effect.AttemptID = write.Effect.AttemptId
	if len(body.Wire) == 0 {
		return errors.New("immutable effect bytes already reserved")
	}
	r.result.Request = &pb.SipMessage{Wire: append([]byte(nil), body.Wire...)}
	r.payload.LocalDrain = &drain
	// The exact wire remains in the bounded native TX/dialog values; after the
	// first physical reservation this effect needs only hash/permit evidence.
	body.Wire = nil
	body.Envelope = nil
	r.setEffect(index, effect, body)
	return nil
}

func (r *stateReduction) complete() error {
	write := r.in.Write
	if write == nil || write.Effect == nil || r.in.Delivery == nil {
		return errors.New("delivery identity required")
	}
	index, effect, body, err := r.findEffect(write.Effect.Id)
	if err != nil {
		return err
	}
	if effect.Kind != "deliver" || body.Permit == nil || body.Authorization == nil || body.Authorization.Effect == nil || write.Flow == nil || !proto.Equal(body.Permit, write.Permit) ||
		!proto.Equal(body.Permit.Flow, write.Flow) || !sameProcess(write.Flow.Ingress, r.in.Executor) ||
		!stateSameContext(body.Authorization.Effect.Context, write.Effect.Context) || body.Authorization.Effect.Id != write.Effect.Id ||
		body.Authorization.Effect.AttemptId != write.Effect.AttemptId {
		return errors.New("delivery proof mismatch")
	}
	if effect.Status == state.EffectAuthorized && r.in.Delivery.Outcome == pb.DeliveryResult_NOT_WRITTEN {
		if effect.AttemptID != "" || !proto.Equal(body.Permit.Authorization, body.Authorization) || r.in.Delivery.Effect == nil ||
			r.in.Delivery.Effect.Id != write.Effect.Id || r.in.Delivery.Effect.AttemptId != write.Effect.AttemptId ||
			!stateSameContext(r.in.Delivery.Effect.Context, write.Effect.Context) {
			return errors.New("unreserved delivery absence proof mismatch")
		}
		// This physical process's exclusive memo proves it never entered the
		// socket syscall. A pending Reserve is serialized before this input;
		// if none committed, the exact immutable attempt can still retire.
		effect.AttemptID = write.Effect.AttemptId
	} else if effect.AttemptID != write.Effect.AttemptId {
		return errors.New("delivery attempt mismatch")
	}
	if effect.Status != state.EffectDispatching && effect.Status != state.EffectUnknown && effect.Status != state.EffectWritten && effect.Status != state.EffectNotWritten {
		if effect.Status != state.EffectAuthorized || r.in.Delivery.Outcome != pb.DeliveryResult_NOT_WRITTEN {
			return errors.New("delivery was never reserved")
		}
	}
	var next state.EffectStatus
	switch r.in.Delivery.Outcome {
	case pb.DeliveryResult_WRITTEN:
		next = state.EffectWritten
	case pb.DeliveryResult_NOT_WRITTEN:
		next = state.EffectNotWritten
	default:
		next = state.EffectUnknown
	}
	if effect.Status == state.EffectWritten || effect.Status == state.EffectNotWritten {
		if effect.Status != next {
			return errors.New("conflicting delivery outcome")
		}
		if next == state.EffectNotWritten {
			r.releaseNoStartServerFinal(r.group.Contexts[body.Context], effect, body)
			return r.releaseNoStartACKCleanup(r.group.Contexts[body.Context], body)
		}
		return nil
	}
	effect.Status = next
	r.setEffect(index, effect, body)
	if next == state.EffectWritten {
		c := r.group.Contexts[body.Context]
		if c != nil {
			stateRecordWrittenFinal(c.Transactions[body.Transaction], body)
		}
		wasAcknowledged := c != nil && c.Snapshot.InitialAck
		if err := stateRecordWrittenACK(c, body); err != nil {
			return err
		}
		if c != nil && !wasAcknowledged && c.Snapshot.InitialAck {
			r.touch(c)
		}
		r.completeInitialUAC(c, body)
		r.recordWrittenPublicBye(c, body)
		if err := r.releaseACKCleanup(r.group.Contexts[body.Context], body); err != nil {
			return err
		}

	}
	if next == state.EffectNotWritten {
		r.releaseNoStartServerFinal(r.group.Contexts[body.Context], effect, body)
	}
	if c := r.group.Contexts[body.Context]; c != nil && body.Command != "" && body.Command == c.Operation && c.Result != nil && c.Result.Certainty != pb.OperationResult_COMPLETED {
		tx := c.Transactions[body.Transaction]
		if writtenPublicBye(c) != nil {
			// A failed later retransmission cannot revoke the first known write.
		} else if stateServerTransaction(tx) && !stateSelectedResponseEffect(tx, body) {
			// Superseded response outcomes retain evidence without settling the current selection.
		} else if next == state.EffectNotWritten && r.responseCommandNoStart(c, effect, body) {
			// The selected response has either a bounded retry or a proven terminal failure.
		} else if stateServerTransaction(tx) && (c.Result.Certainty != pb.OperationResult_ACCEPTED || tx.RespondDeadlineMS > 0 && tx.RespondDeadlineMS <= r.now) {
			if c.Result.Certainty == pb.OperationResult_ACCEPTED {
				c.Result.Certainty = pb.OperationResult_UNKNOWN
				if c.Result.Error == "" {
					c.Result.Error = "SIP response delivery deadline elapsed"
				}
			}
		} else if next != state.EffectWritten {
			c.Result.Certainty = pb.OperationResult_UNKNOWN
			if c.Result.Error == "" {
				c.Result.Error = "SIP delivery unavailable or uncertain"
			}
		} else if stateServerTransaction(tx) {
			c.Result.Certainty = pb.OperationResult_COMPLETED
			if c.Result.SipStatus < 300 {
				c.Result.Error = ""
			}
			c.Result.Snapshot = stateSnapshot(c, r.engine.cfg.Process)
		}
	}
	if next == state.EffectNotWritten {
		return r.releaseNoStartACKCleanup(r.group.Contexts[body.Context], body)
	}
	return nil
}

func (r *stateReduction) effectResult() error {
	index, effect, body, err := r.findEffect(r.in.Effect)
	if err != nil {
		return err
	}
	c := r.group.Contexts[body.Context]
	if c == nil {
		return errors.New("effect context unavailable")
	}
	if effect.Kind == "deliver" {
		result := r.in.Reply.GetResult()
		delivery := r.in.Delivery
		if result != nil && result.Certainty == pb.OperationResult_COMPLETED && delivery != nil && delivery.Recorded && stateSameContext(delivery.GetEffect().GetContext(), body.Authorization.GetEffect().GetContext()) && delivery.GetEffect().GetId() == effect.ID && delivery.GetEffect().GetAttemptId() == body.Authorization.GetEffect().GetAttemptId() {
			if effect.Status == state.EffectWritten && delivery.Outcome == pb.DeliveryResult_WRITTEN || effect.Status == state.EffectNotWritten && delivery.Outcome == pb.DeliveryResult_NOT_WRITTEN {
				body.Settled = true
				r.setEffect(index, effect, body)
			}
		}
		return nil
	}
	if effect.Kind == "wire" && (body.Cancelled || effect.DeadlineMS <= r.now) {
		if absent := r.in.Reply.GetResult(); effect.DeadlineMS <= r.now && absent != nil && absent.Certainty == pb.OperationResult_NOT_ACCEPTED && absent.Error == "send permit not found" {
			effect.Status = state.EffectCancelled
			body.Settled = true
			r.setEffect(index, effect, body)
			r.releaseNoStartServerFinal(c, effect, body)
			r.responseCommandNoStart(c, effect, body)
			return r.releaseExpiredACKCleanup(c, body, effect.DeadlineMS)
		}
		permit := r.in.Reply.GetSendPermit()
		if permit != nil {
			if !proto.Equal(permit.Authorization, body.Authorization) || permit.Flow == nil || permit.DirectoryEpoch == "" || permit.OrderingProof == "" {
				return errors.New("cancelled authorization receipt mismatch")
			}
			if err := r.ownerAdmitPermit(c, body, permit); err != nil {
				return err
			}
			body.Permit = proto.Clone(permit).(*pb.SendPermit)
			body.Executor = nil
			effect.Kind = "deliver"
			effect.Status = state.EffectNotWritten
			effect.AttemptID = body.Authorization.Effect.AttemptId
			r.setEffect(index, effect, body)
			r.releaseNoStartServerFinal(c, effect, body)
			r.responseCommandNoStart(c, effect, body)
			return r.releaseExpiredACKCleanup(c, body, effect.DeadlineMS)
		}
		return nil
	}
	if effect.Status == state.EffectWritten || effect.Status == state.EffectNotWritten || effect.Status == state.EffectCancelled {
		return nil
	}
	if r.engine.receiverEffect(effect.Kind) && body.Executor == nil {
		if err := r.receiverResultOwner(effect, body, c); err != nil {
			return err
		}
	} else if body.Executor == nil || !sameProcess(body.Executor, r.in.Executor) {
		return errors.New("RPC completion executor mismatch")
	}
	if effect.Kind == "adopt" {
		return r.finishFlowAdoption(index, effect, body, c)
	}
	if effect.Kind == "open" {
		if body.Policy != nil {
			if body.Policy.Command != body.Command || body.Policy.Term != c.Snapshot.ApplicationLeaseTerm ||
				body.Policy.Generation != c.Snapshot.AttachmentGeneration ||
				int64(body.Policy.Decision.ExpiresAtMs) <= r.now {
				r.in.Failure = "command policy expired or superseded"
			}
			r.policy = body.Policy
		}
		if r.in.Failure != "" || r.in.Flow == nil {
			c.Snapshot.Ended = true
			c.RetireMS = r.now + 120000
			c.Result = &pb.OperationResult{Certainty: pb.OperationResult_COMPLETED, Error: r.in.Failure, Snapshot: stateSnapshot(c, r.engine.cfg.Process)}
			r.event(c, pb.DialogEvent_FAILED, nil, c.Operation)
		} else if !c.Snapshot.Ended {
			c.Flow = proto.Clone(r.in.Flow).(*pb.FlowRef)
			if c.Flow.Transport != c.Snapshot.Transport {
				return errors.New("opened transport mismatch")
			}
			if err = r.startClient(c); err != nil {
				return err
			}
		}
		effect.Status = state.EffectWritten
		if r.in.Failure != "" {
			effect.Status = state.EffectNotWritten
		}
		r.setEffect(index, effect, body)
		return nil
	}
	if effect.Kind == "wire" {
		permit := r.in.Reply.GetSendPermit()
		if r.in.Failure != "" || permit == nil {
			effect.Status = state.EffectUnknown
			effect.AttemptID = "rpc-" + r.engine.id(effect.ID)[:32]
			r.setEffect(index, effect, body)
			return nil
		}
		if !proto.Equal(permit.Authorization, body.Authorization) || permit.Flow == nil || permit.DirectoryEpoch == "" || permit.OrderingProof == "" || permit.Flow.Transport != c.Snapshot.Transport {
			return errors.New("invalid send permit")
		}
		if err := r.ownerAdmitPermit(c, body, permit); err != nil {
			return err
		}
		body.Permit = proto.Clone(permit).(*pb.SendPermit)
		effect.Kind = "deliver"
		effect.Status = state.EffectAuthorized
		effect.AttemptID = ""
		body.Executor = nil
		if tx := c.Transactions[body.Transaction]; tx != nil && permit.CompletionScope != "" {
			if tx.CompletionScope == "" {
				tx.CompletionScope = permit.CompletionScope
			}
			if (tx.Native.Family == portable.InviteClient || tx.Native.Family == portable.InviteServer) && c.CompletionScope == "" {
				c.CompletionScope = tx.CompletionScope
				origin := tx.Native.Origin.Clone()
				c.ScopeOrigin = &origin
			}
			if tx.Native.Family == portable.InviteServer && c.Dialog != "" {
				if c.DialogScopes == nil {
					c.DialogScopes = make(map[string]string)
				}
				if c.DialogScopes[c.Dialog] == "" {
					c.DialogScopes[c.Dialog] = tx.CompletionScope
				}
			}
		}
		if c.Flow == nil {
			c.Flow = proto.Clone(permit.Flow).(*pb.FlowRef)
		}
		if tx := c.Transactions[body.Transaction]; tx != nil && (tx.Flow == nil || body.Authorization.AuthorityClass == pb.EffectAuthorization_APPLICATION && (tx.Native.Family == portable.InviteClient || tx.Native.Family == portable.NonInviteClient)) {
			tx.Flow = proto.Clone(permit.Flow).(*pb.FlowRef)
		}
		r.setEffect(index, effect, body)
		return nil
	}
	if r.in.Failure == "" {
		acknowledgement := r.in.Reply.GetResult()
		if acknowledgement == nil || (acknowledgement.Certainty != pb.OperationResult_ACCEPTED && acknowledgement.Certainty != pb.OperationResult_COMPLETED) {
			r.in.Failure = "control receiver did not accept retained effect"
		}
	}
	if r.in.Failure != "" {
		effect.Status = state.EffectUnknown
		effect.AttemptID = "rpc-" + r.engine.id(effect.ID)[:32]
	} else {
		effect.Status = state.EffectWritten
		effect.AttemptID = "rpc-" + r.engine.id(effect.ID)[:32]
	}
	r.setEffect(index, effect, body)
	return nil
}

func (r *stateReduction) openUAC() error {
	cmd := r.in.Command
	policy, err := r.commandPolicy(nil)
	if err != nil {
		return err
	}
	r.policy = policy
	if r.engine.autonomousCompletion() && (r.group.OutboundID != "" || strings.HasPrefix(cmd.Context.Id, "in-")) {
		return errors.New("native outbound allocation already consumed or reserved namespace")
	}
	if r.group.Retired[cmd.Context.Id] > r.now {
		return errors.New("native context retired")
	}
	open := cmd.GetOpenUac()
	if cmd.Sequence != 1 || open == nil {
		return errors.New("initial command sequence required")
	}
	transport := uacTransport(open)
	if transport != "udp" && transport != "tcp" && transport != "tls" {
		return errors.New("unsupported transport")
	}
	method := sip.RequestMethod(open.Method)
	if method == "" {
		method = sip.INVITE
	}
	contact, advertise, err := r.engine.advertisement(transport)
	if err != nil {
		return err
	}
	m, err := portable.BuildInitialRequest(portable.InitialRequest{Method: method, URI: open.Uri, CallID: r.record.Ref.CallID, LocalTag: r.engine.id("tag", r.record.Ref.Incarnation, cmd.Context.Id)[:32], Branch: sip.RFC3261BranchMagicCookie + r.engine.id("branch", r.record.Ref.Incarnation, r.in.Operation)[:32], CSeq: 1, Advertise: advertise, Contact: contact, Transport: transport, Headers: stateHeaders(open.Headers), Body: open.Body})
	if err != nil {
		return err
	}
	c := &stateContext{Snapshot: &pb.ContextSnapshot{Context: r.engine.contextRef(r.record.Ref, cmd.Context.Id), Application: proto.Clone(r.in.Application).(*pb.ProcessRef), BindingId: open.BindingId, PrincipalId: open.PrincipalId, Kind: pb.ContextSnapshot_UAC, CallId: r.record.Ref.CallID, Transport: transport, LocalSdp: append([]byte(nil), open.Body...), CommandSequence: 1}, Origin: m, Tag: r.engine.id("tag", r.record.Ref.Incarnation, cmd.Context.Id)[:32], Transactions: make(map[string]*stateTransaction), Requests: make(map[string]string), Dialogs: make(map[string]portable.Dialog), Operation: r.in.Operation, CommandFingerprint: r.fingerprint}
	if err := r.pinNativeOwner(c); err != nil {
		return err
	}
	r.installPolicy(c, policy)
	if policy != nil && policy.Decision.SelectedFlow != nil {
		c.Flow = proto.Clone(policy.Decision.SelectedFlow).(*pb.FlowRef)
	}
	if open.Network != nil && open.Network.Credential != nil {
		c.Credential = proto.Clone(open.Network.Credential).(*pb.CredentialRef)
	}
	parsed, _ := m.Parse()
	c.Snapshot.LocalTag = tag(parsed.From().Params)
	c.Snapshot.LocalUri = parsed.From().Address.String()
	c.Snapshot.RemoteUri = parsed.To().Address.String()
	if method != sip.INVITE {
		c.Snapshot.Kind = pb.ContextSnapshot_TRANSACTION
	}
	r.group.Contexts[cmd.Context.Id] = c
	if r.engine.autonomousCompletion() {
		r.group.OutboundID = cmd.Context.Id
	}
	r.touch(c)
	r.index(c, m, true)
	// The open intent is retained before any DNS/TCP/TLS side effect. Parent
	// ingress owns the connection and returns its exact flow before SIP send.
	request := proto.Clone(open).(*pb.OpenUac)
	encoded, _ := stateEncodeNetworkTarget(request)
	if open.BindingId == "" {
		until := r.now + 5000
		if policy != nil {
			until = min(until, int64(policy.Decision.ExpiresAtMs))
		}
		r.addEffect("open", c, stateEngineEffect{Command: c.Operation, Wire: encoded, Policy: policy}, until)
	} else if err = r.startClient(c); err != nil {
		return err
	}
	c.Result = &pb.OperationResult{Certainty: pb.OperationResult_ACCEPTED, Snapshot: stateSnapshot(c, r.engine.cfg.Process), Request: mustObserve(m.Wire, true)}
	r.result = proto.Clone(c.Result).(*pb.OperationResult)
	return nil
}

func (r *stateReduction) startClient(c *stateContext) error {
	m, err := c.Origin.Parse()
	if err != nil {
		return err
	}
	family := portable.NonInviteClient
	if m.CSeq().MethodName == sip.INVITE {
		family = portable.InviteClient
	}
	native, err := portable.NewTransaction(family, c.Origin, portable.DefaultTimers(), "")
	if err != nil {
		return err
	}
	tx := &stateTransaction{Native: native, Flow: c.Flow, Command: c.Operation, Policy: r.policy}
	r.anchorPolicyTransaction(c, tx)
	c.Transactions[native.Key] = tx
	return r.stepTransaction(c, tx, portable.Input{Kind: portable.Start, Now: r.now})
}

func (r *stateReduction) timerID(context, kind, key string, name portable.Timer) string {
	return "t-" + r.engine.id(context, kind, key, string(name))[:48]
}
func (r *stateReduction) timerByIdentity() error {
	if handled, err := r.ownerTimer(); handled || err != nil {
		return err
	}
	if handled, err := r.lifecycleTimer(); handled || err != nil {
		return err
	}
	generation, err := strconvUint(r.in.Generation)
	if err != nil {
		return err
	}
	for id, c := range r.group.Contexts {
		for key, tx := range c.Transactions {
			for name := range tx.Native.Timers {
				if r.timerID(id, "tx", key, name) == r.in.Timer {
					return r.stepTransaction(c, tx, portable.Input{Kind: portable.TimerFired, Now: r.now, Timer: name, Generation: generation})
				}
			}
		}
		for key, d := range c.Dialogs {
			for name := range d.Timers {
				if r.timerID(id, "dialog", key, name) == r.in.Timer {
					next, effects, err := portable.StepDialog(d, portable.DialogInput{Kind: portable.DialogTimer, Now: r.now, Timer: name, Generation: generation})
					if err != nil {
						return err
					}
					c.Dialogs[key] = next
					return r.dialogEffectsFor(c, key, effects)
				}
			}
		}
	}
	return nil
}
func strconvUint(value string) (uint64, error) {
	var n uint64
	_, err := fmt.Sscan(value, &n)
	return n, err
}
