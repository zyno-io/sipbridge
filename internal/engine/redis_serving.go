package engine

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
	"google.golang.org/protobuf/proto"
	"sipbridge/internal/control"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/rediscore"
)

// RedisNative selects one sole owner for each context: the existing resident
// phone FSM or the shared Redis call/standalone-client FSM. No mirror or handoff.
type RedisNative struct {
	*redisNative
	phone *ResidentMemory
}

func NewRedisNative(parent context.Context, cfg Config, connection *rediscore.Connection, server *control.Server) (*RedisNative, error) {
	shared, err := newRedisNative(parent, cfg, connection, server)
	if err != nil {
		return nil, err
	}
	contexts, bytes := connection.AdmissionLimits()
	if int64(int(contexts)) != contexts {
		shared.Close()
		return nil, errors.New("local phone context bound exceeds platform capacity")
	}
	phone, err := NewResidentMemory(parent, cfg, ResidentMemoryOptions{Authority: connection.Authority(),
		MaxContexts: int(contexts), MaxBytes: bytes}, server)
	if err != nil {
		shared.Close()
		return nil, err
	}
	phone.phoneDomain = true
	phone.leases = nil
	if err = server.SetApplicationCapabilityRelease(phone.expireResidentCapability); err != nil {
		phone.Close()
		shared.Close()
		return nil, err
	}
	return &RedisNative{redisNative: shared, phone: phone}, nil
}

func (e *RedisNative) Close()                      { e.phone.Close(); e.redisNative.Close() }
func (e *RedisNative) FlowClosed(flow *pb.FlowRef) { e.phone.FlowClosed(flow) }

func envelopeContext(envelope *pb.Envelope) *pb.ContextRef {
	switch body := envelope.Payload.(type) {
	case *pb.Envelope_Command:
		return body.Command.GetContext()
	case *pb.Envelope_RequestDecision:
		return body.RequestDecision.GetContext()
	case *pb.Envelope_Inspect:
		return body.Inspect.GetContext()
	}
	return nil
}

func (e *RedisNative) Handle(peer *control.Peer, envelope *pb.Envelope) error {
	if envelope != nil && localPhoneID(envelopeContext(envelope).GetId()) {
		return e.phone.Handle(peer, envelope)
	}
	return e.redisNative.Handle(peer, envelope)
}

func (e *RedisNative) Reserve(ctx context.Context, write *pb.FlowWrite) (*CommittedWrite, error) {
	if localPhoneID(write.GetEffect().GetContext().GetId()) {
		return e.phone.Reserve(ctx, write)
	}
	return e.redisNative.Reserve(ctx, write)
}
func (e *RedisNative) Complete(ctx context.Context, write *pb.FlowWrite, result *pb.DeliveryResult) error {
	if localPhoneID(write.GetEffect().GetContext().GetId()) {
		return e.phone.Complete(ctx, write, result)
	}
	return e.redisNative.Complete(ctx, write, result)
}
func (e *RedisNative) InspectWrite(ctx context.Context, write *pb.FlowWrite) (*pb.DeliveryResult, error) {
	if localPhoneID(write.GetEffect().GetContext().GetId()) {
		return e.phone.InspectWrite(ctx, write)
	}
	return e.redisNative.InspectWrite(ctx, write)
}

func phoneRequest(parsed sip.Message) bool {
	request, ok := parsed.(*sip.Request)
	return ok && (request.Method == sip.REGISTER || request.Method == sip.SUBSCRIBE)
}

func (e *RedisNative) LocateRoute(ctx context.Context, message *pb.SipMessage) (*pb.ContextSnapshot, error) {
	if message == nil {
		return nil, errors.New("SIP message required")
	}
	parsed, err := parseObservedMessage(message.Wire)
	if err != nil {
		return nil, err
	}
	if phoneRequest(parsed) {
		return e.phone.locateResidentParsed(ctx, message.Wire, parsed, observedMessageSelectors(parsed, false), true)
	}
	return e.redisNative.locateRedisRouteParsed(ctx, message, parsed)
}
func (e *RedisNative) Reject(ctx context.Context, flow *pb.FlowRef, message *pb.SipMessage, status uint32) error {
	if message == nil {
		return errors.New("SIP message required")
	}
	parsed, err := parseObservedMessage(message.Wire)
	if err != nil {
		return err
	}
	return e.RejectParsed(ctx, flow, message, status, parsed, portable.Message{})
}
func (e *RedisNative) RejectParsed(ctx context.Context, flow *pb.FlowRef, message *pb.SipMessage, status uint32, parsed sip.Message, native portable.Message) error {
	if parsed == nil && message != nil {
		var err error
		parsed, err = parseObservedMessage(message.Wire)
		if err != nil {
			return err
		}
	}
	if phoneRequest(parsed) {
		checked, err := e.redisIngressMessage(flow, message, parsed, native)
		if err != nil {
			return err
		}
		return e.phone.RejectParsed(ctx, flow, message, status, parsed, checked)
	}
	return e.redisNative.RejectParsed(ctx, flow, message, status, parsed, native)
}
func (e *RedisNative) ReceiveRequestParsed(ctx context.Context, peer *control.Peer, flow *pb.FlowRef, message *pb.SipMessage, parsed sip.Message, frozen ...portable.Message) error {
	if parsed == nil && message != nil {
		var err error
		parsed, err = parseObservedMessage(message.Wire)
		if err != nil {
			return err
		}
	}
	if phoneRequest(parsed) {
		native, err := e.redisIngressMessage(flow, message, parsed, frozen...)
		if err != nil {
			return err
		}
		return e.phone.ReceiveRequestParsed(ctx, peer, flow, message, parsed, native)
	}
	return e.redisNative.ReceiveRequestParsed(ctx, peer, flow, message, parsed, frozen...)
}
func (e *RedisNative) TryNativeCompletion(ctx context.Context, flow *pb.FlowRef, message *pb.SipMessage) (bool, error) {
	return e.TryNativeCompletionParsed(ctx, flow, message, nil)
}
func (e *RedisNative) TryNativeCompletionParsed(ctx context.Context, flow *pb.FlowRef, message *pb.SipMessage, parsed sip.Message, frozen ...portable.Message) (bool, error) {
	if message == nil || flow == nil {
		return false, nil
	}
	if parsed == nil {
		var err error
		parsed, err = parseObservedMessage(message.Wire)
		if err != nil {
			return false, err
		}
	}
	selectors := observedMessageSelectors(parsed, false)
	// Select only an actual local transaction. A phone renewal on a dialog is
	// business work, whereas an exact duplicate or client response is native.
	e.phone.mu.Lock()
	var selected *residentContext
	for _, selector := range selectors {
		if !strings.HasPrefix(selector, "stx:") && !strings.HasPrefix(selector, "ctx:") {
			continue
		}
		if selected = e.phone.transactions[selector]; selected != nil {
			break
		}
	}
	e.phone.mu.Unlock()
	if selected != nil {
		native, err := e.redisIngressMessage(flow, message, parsed, frozen...)
		if err != nil {
			return true, err
		}
		if _, response := parsed.(*sip.Response); response {
			return e.phone.TryNativeCompletionParsed(ctx, flow, &pb.SipMessage{Wire: message.Wire, Selectors: selectors}, parsed, native)
		}
		if phoneRequest(parsed) {
			selected.mu.Lock()
			ref := proto.Clone(selected.snapshot.Context).(*pb.ContextRef)
			application := cloneResidentApplication(selected.snapshot.Application)
			selected.mu.Unlock()
			_, err := e.phone.ingestResidentExistingParsed(ctx, selected, &pb.OwnerIngest{Context: ref, Application: application, Flow: flow, Wire: message.Wire}, deadlineOr(ctx, time.Now().Add(2*time.Second)), parsed.(*sip.Request), message, native)
			return true, err
		}
	}
	if phoneRequest(parsed) {
		return false, nil
	}
	return e.redisNative.TryNativeCompletionParsed(ctx, flow, message, parsed, frozen...)
}

func (e *RedisNative) ReceiveRequest(ctx context.Context, peer *control.Peer, flow *pb.FlowRef, message *pb.SipMessage) error {
	return e.ReceiveRequestParsed(ctx, peer, flow, message, nil)
}
