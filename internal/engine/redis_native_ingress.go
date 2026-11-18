package engine

import (
	"bytes"
	"context"
	"errors"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
	"sipbridge/internal/control"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/rediscore"
)

// redisIngressMessage accepts a frozen value only from this exact physical
// frame. Public wire callers enter through the observed parser first.
func (e *redisNative) redisIngressMessage(flow *pb.FlowRef, message *pb.SipMessage, parsed sip.Message, frozen ...portable.Message) (portable.Message, error) {
	if e == nil || message == nil || !validPolicyFlow(flow) || !sameProcess(flow.Ingress, e.cfg.Process) || len(frozen) > 1 {
		return portable.Message{}, errNativeRecord
	}
	source := net.JoinHostPort(flow.SourceIp, strconv.Itoa(int(flow.SourcePort)))
	if len(frozen) == 1 && !frozen[0].IsZero() {
		selected := frozen[0]
		if !bytes.Equal(selected.WireBytes(), message.Wire) || selected.Transport() != flow.Transport || selected.Source() != source || selected.Destination() != "" {
			return portable.Message{}, errNativeRecord
		}
		if _, err := selected.Facts(); err != nil {
			return portable.Message{}, err
		}
		return selected, nil
	}
	if parsed == nil {
		var err error
		parsed, err = parseObservedMessage(message.Wire)
		if err != nil {
			return portable.Message{}, err
		}
	}
	return portable.FreezeMessage(message.Wire, parsed, flow.Transport, source, "")
}

func (e *redisNative) TryNativeCompletion(ctx context.Context, flow *pb.FlowRef, message *pb.SipMessage) (bool, error) {
	return e.TryNativeCompletionParsed(ctx, flow, message, nil)
}

// TryNativeCompletionParsed runs before controller selection. It never routes a
// completion through an application or reconstructs missing native authority.
func (e *redisNative) TryNativeCompletionParsed(ctx context.Context, flow *pb.FlowRef, message *pb.SipMessage, parsed sip.Message, frozen ...portable.Message) (bool, error) {
	if flow == nil || message == nil {
		return false, nil
	}
	native, err := e.redisIngressMessage(flow, message, parsed, frozen...)
	if err != nil {
		return false, err
	}
	facts, err := native.Facts()
	if err != nil {
		return false, err
	}
	if !facts.IsRequest {

		selectors, selectorErr := frozenMessageSelectors(native, facts, false, false)
		if selectorErr != nil {
			return false, selectorErr
		}
		return e.tryUACResponseCompletion(ctx, flow, native, selectors)
	}
	switch facts.Method {
	case sip.REGISTER, sip.SUBSCRIBE:
		return false, nil
	case sip.ACK:
		return e.tryCallCompletion(ctx, flow, native)
	case sip.BYE:
		if facts.ToTag == "" {
			return e.tryServerRequestDuplicate(ctx, flow, native)
		}
		handled, completionErr := e.tryCallCompletion(ctx, flow, native)
		if errors.Is(completionErr, errNativeRecord) {
			// A rejected tagged BYE owns a native refusal transaction rather
			// than a call BYE. Its exact selector/flow proof still governs replay.
			duplicate, duplicateErr := e.tryServerRequestDuplicate(ctx, flow, native)
			if duplicate && duplicateErr == nil {
				return true, nil
			}
		}
		return handled, completionErr
	case sip.CANCEL:
		handled, completionErr := e.tryCallCompletion(ctx, flow, native)
		if errors.Is(completionErr, rediscore.ErrConflict) {
			// A pre-existing standalone refusal may own CANCEL's exact server
			// selector even if the matching INVITE arrived afterward.
			duplicate, duplicateErr := e.tryServerRequestDuplicate(ctx, flow, native)
			if duplicate && duplicateErr == nil {
				return true, nil
			}
		}
		if handled || completionErr != nil {
			return handled, completionErr
		}
		return e.tryServerRequestDuplicate(ctx, flow, native)
	case sip.INVITE, sip.INFO, sip.REFER, sip.UPDATE, sip.NOTIFY:
		return e.tryServerRequestDuplicate(ctx, flow, native)
	default:
		return e.tryServerRequestDuplicate(ctx, flow, native)
	}
}

func (e *redisNative) ReceiveRequest(ctx context.Context, peer *control.Peer, flow *pb.FlowRef, message *pb.SipMessage) error {
	return e.ReceiveRequestParsed(ctx, peer, flow, message, nil)
}

// ReceiveRequestParsed presents fresh business work exactly once. The selected
// native receive helper owns the publication; no routing snapshot is authority.
func (e *redisNative) ReceiveRequestParsed(parent context.Context, peer *control.Peer, flow *pb.FlowRef, message *pb.SipMessage, parsed sip.Message, frozen ...portable.Message) (receiveErr error) {
	defer func() {
		var protocol *portable.ProtocolError
		if errors.As(receiveErr, &protocol) {
			receiveErr = rejectIngest(uint32(protocol.Status), protocol.Reason)
		}
	}()
	if peer == nil || !peer.HasCapability(control.RequestDecisionCapability) {
		return errors.New("direct request authority required")
	}
	native, err := e.redisIngressMessage(flow, message, parsed, frozen...)
	if err != nil {
		return err
	}
	facts, err := native.Facts()
	if err != nil || !facts.IsRequest {
		return errors.New("direct request requires a SIP request")
	}
	deadline := minTime(deadlineOr(parent, time.Now().Add(2*time.Second)), time.Now().Add(2*time.Second))
	ctx, cancel, store, err := e.operation(parent, deadline)
	if err != nil {
		return err
	}
	defer cancel()
	route, err := lookupRedisIngressRoute(ctx, store, native)
	if err != nil {
		return err
	}
	if route != nil && route.Application != nil {
		selected, peerErr := e.control.Peer(route.Application)
		if peerErr == nil {
			peer = selected
		} else {
			return peerErr
		}
	}
	if _, err = e.controllerWitness(peer); err != nil {
		return err
	}
	if route == nil {
		if facts.ToTag != "" {
			return rejectIngest(481, "unknown native context")
		}
		switch facts.Method {
		case sip.INVITE:
			return e.receiveInitialInvite(ctx, peer, flow, native)
		default:
			return rejectIngest(501, "unsupported initial native request")
		}
	}

	if facts.ToTag == "" || route.Kind != pb.ContextSnapshot_UAS && route.Kind != pb.ContextSnapshot_UAC {
		return rejectIngest(481, "native request already admitted")
	}
	witness, err := e.controllerWitness(peer)
	if err != nil {
		return err
	}
	input := redisDialogReceive{IdentityKey: e.cfg.StateIdentityKey, Context: route.Context, Application: peer.Application(), Executor: e.cfg.Process, Lease: witness, Flow: flow, Message: native, PrincipalID: route.PrincipalId, BindingID: route.BindingId, RequestID: uuid.NewString(), Timer: rediscore.TimerAssignment{Worker: e.worker, Epoch: e.cfg.Process.Epoch, Term: e.cfg.Process.Epoch}, Deadline: deadline}
	var presentation *pb.RequestPresented
	for attempt := 0; attempt < 3; attempt++ {
		presentation, err = receiveRedisDialogRequest(ctx, store, input)
		if !errors.Is(err, rediscore.ErrConflict) || ctx.Err() != nil || !time.Now().Before(deadline) {
			break
		}
	}
	input = redisDialogReceive{}
	if err != nil {
		return err
	}
	remaining := time.Until(deadline).Milliseconds()
	if ctx.Err() != nil || remaining <= 0 {
		return rediscore.ErrUnknown
	}
	return peer.Send(&pb.Envelope{CorrelationId: uuid.NewString(), RemainingBudgetMs: uint32(min(2000, remaining)), Payload: &pb.Envelope_Request{Request: presentation}})
}

func (e *redisNative) Reject(ctx context.Context, flow *pb.FlowRef, message *pb.SipMessage, status uint32) error {
	return e.RejectParsed(ctx, flow, message, status, nil, portable.Message{})
}

func (e *redisNative) RejectParsed(ctx context.Context, flow *pb.FlowRef, message *pb.SipMessage, status uint32, parsed sip.Message, frozen portable.Message) error {
	if status < 300 || status > 699 {
		return errNativeRecord
	}
	native, err := e.redisIngressMessage(flow, message, parsed, frozen)
	if err != nil {
		return err
	}
	facts, err := native.Facts()
	if err != nil || !facts.IsRequest {
		return errNativeRecord
	}
	if facts.Method == sip.REGISTER || facts.Method == sip.SUBSCRIBE {
		return errNativeRecord
	}
	if facts.Method == sip.ACK {
		return nil
	}
	return e.rejectInitialNative(ctx, flow, native, status)
}

// lookupRedisIngressRoute selects routing facts only. The actual native receive
// transition must reselect and fence its identity, term, flow and revision.
func lookupRedisIngressRoute(ctx context.Context, store *rediscore.Store, message portable.Message) (*pb.ContextSnapshot, error) {
	selectors, err := nativeSelectorIDs(message, false, false)
	if err != nil {
		return nil, err
	}
	facts, err := message.Facts()
	if err != nil {
		return nil, err
	}
	for _, selector := range selectors {
		selected, lookupErr := store.LookupSelector(ctx, selector, "r:context")
		if errors.Is(lookupErr, rediscore.ErrAbsent) {
			continue
		}
		if lookupErr != nil {
			return nil, lookupErr
		}
		current, decodeErr := decodeNativeContext(selected.View.Records["r:context"])
		if decodeErr != nil {
			return nil, decodeErr
		}
		if current.Kind == pb.ContextSnapshot_SUBSCRIPTION || current.Context == nil || current.Context.State == nil || current.Context.Id != selected.Identity.Context || current.Context.State.Incarnation != selected.Identity.Incarnation || current.CallId != facts.CallID || current.Ended {
			return nil, rejectIngest(481, "native context unavailable")
		}
		route := &pb.ContextSnapshot{Context: proto.Clone(current.Context).(*pb.ContextRef), Kind: current.Kind, CallId: current.CallId, PrincipalId: current.PrincipalId, BindingId: current.BindingId, ApplicationLeaseTerm: current.ApplicationLeaseTerm}
		if current.Application != nil {
			route.Application = proto.Clone(current.Application).(*pb.ProcessRef)
		}
		return route, nil
	}
	return nil, nil
}

func (e *redisNative) LocateRoute(parent context.Context, message *pb.SipMessage) (*pb.ContextSnapshot, error) {
	if message == nil {
		return nil, errNativeRecord
	}
	parsed, err := parseObservedMessage(message.Wire)
	if err != nil {
		return nil, err
	}
	return e.locateRedisRouteParsed(parent, message, parsed)
}

func (e *redisNative) locateRedisRouteParsed(parent context.Context, message *pb.SipMessage, parsed sip.Message) (*pb.ContextSnapshot, error) {
	// This transport is used only to derive read-only selectors. Physical
	// admission always takes its actual transport from the observed FlowRef.
	native, err := portable.FreezeMessage(message.Wire, parsed, strings.ToLower(parsed.Transport()), "", "")
	if err != nil {
		return nil, err
	}
	ctx, cancel, store, err := e.operation(parent, deadlineOr(parent, time.Now().Add(2*time.Second)))
	if err != nil {
		return nil, err
	}
	defer cancel()
	return lookupRedisIngressRoute(ctx, store, native)
}

// InspectWrite observes only this immutable attempt. Absence is historical
// UNKNOWN, and cannot reserve, retry or recreate a native output.
func (e *redisNative) InspectWrite(parent context.Context, write *pb.FlowWrite) (*pb.DeliveryResult, error) {
	ref, err := nativeOutputRef(write)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithDeadline(parent, deadlineOr(parent, time.Now().Add(2*time.Second)))
	defer cancel()
	store, _, _, bindingErr := e.driver.Current()
	if bindingErr != nil {
		store, err = e.connection.Refresh(ctx)
		if err != nil {
			return nil, err
		}
	}
	result := &pb.DeliveryResult{Effect: proto.Clone(write.Effect).(*pb.StateEffectRef), Outcome: pb.DeliveryResult_UNKNOWN}
	proof, err := store.InspectOutput(ctx, ref)
	if errors.Is(err, rediscore.ErrAbsent) {
		return result, nil
	}
	if err != nil {
		return nil, err
	}
	metadata, err := decodeNativeOutput(proof.Metadata)
	if err != nil || !proto.Equal(metadata.Write, write) {
		return nil, errors.New("historical output authority mismatch")
	}
	if proof.State == rediscore.OutputWritten {
		result.Outcome, result.Recorded = pb.DeliveryResult_WRITTEN, true
	} else if proof.State == rediscore.OutputNotWritten {
		result.Outcome, result.Recorded = pb.DeliveryResult_NOT_WRITTEN, true
	}
	return result, nil
}
