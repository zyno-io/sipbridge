package engine

import (
	"github.com/emiago/sipgo/sip/portable"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/state"
	"sipbridge/internal/storagecodec"
	"sipbridge/internal/storagepb"
)

func (value stateGroup) MarshalBinary() ([]byte, error) {
	encoded := storageEncodeEngineStateGroup(value)
	if encoded.Contexts != nil {
		for _, entry := range encoded.Contexts.Values {
			if entry != nil && entry.Value != nil && entry.Value.Result != nil {
				entry.Value.Result = stateResultForStorage(entry.Value.Result)
			}
		}
	}
	return storagecodec.Marshal(storagecodec.Group, encoded)
}

// The context is the authority for the current snapshot. Cached command results
// retain their outcome and exact SIP wire, with projections built only at public
// boundaries. Copy small DTOs without cloning their immutable wire.
func stateResultForStorage(value *pb.OperationResult) *pb.OperationResult {
	wireOnly := func(message *pb.SipMessage) *pb.SipMessage {
		if message == nil {
			return nil
		}
		if message.Method == "" && message.Uri == "" && message.Status == 0 && message.Reason == "" &&
			len(message.Body) == 0 && len(message.Headers) == 0 && len(message.Addresses) == 0 && len(message.Vias) == 0 &&
			len(message.Selectors) == 0 && message.Authorization == nil && len(message.ProtoReflect().GetUnknown()) == 0 {
			return message
		}
		return &pb.SipMessage{Wire: message.Wire}
	}
	request, response := wireOnly(value.Request), wireOnly(value.Response)
	if value.Snapshot == nil && request == value.Request && response == value.Response && len(value.ProtoReflect().GetUnknown()) == 0 {
		return value
	}
	return &pb.OperationResult{
		Certainty: value.Certainty, Error: value.Error, SipStatus: value.SipStatus,
		Request: request, Response: response, RetirementProof: value.RetirementProof,
	}
}

// The caller owns this public result independently of retained context state.
// Validate both messages before publishing either projection. Exact bytes alone
// are not proof of authority, physical write, or successful SIP completion.
func stateMaterializeResultMessages(result *pb.OperationResult) error {
	if result == nil {
		return nil
	}
	// Parsed operation-local projections were already produced by Observe.
	// Retained cached DTOs always have these discriminator fields cleared by
	// group decode, including records written by the preceding representation.
	request, response := result.Request, result.Response
	var err error
	if request != nil && request.Method == "" {
		request, err = Observe(result.Request.Wire, true)
		if err != nil || request.Method == "" {
			return state.ErrCorrupt
		}
	}
	if response != nil && response.Status == 0 {
		response, err = Observe(result.Response.Wire, false)
		if err != nil || response.Status == 0 {
			return state.ErrCorrupt
		}
	}
	if request != nil && request.Status != 0 {
		return state.ErrCorrupt
	}
	result.Request, result.Response = request, response
	return nil
}

func (value *stateGroup) UnmarshalBinary(data []byte) error {
	next, err := readStateGroupDirect(data)
	if err != nil {
		return state.ErrCorrupt
	}
	for _, context := range next.Contexts {
		if context == nil {
			return state.ErrCorrupt
		}
		// Accept the preceding representation but do not retain its redundant
		// projection or use it as command-result authority.
		if context.Result != nil {
			context.Result = stateResultForStorage(context.Result)
		}
		for _, transaction := range context.Transactions {
			if transaction == nil || transaction.Policy != nil && !transaction.Policy.valid() {
				return state.ErrCorrupt
			}
		}
	}
	*value = next
	return nil
}
func (value stateEngineInput) MarshalBinary() ([]byte, error) {
	if err := stateValidateRetainedInput(value); err != nil {
		return nil, err
	}
	return storagecodec.Marshal(storagecodec.Input, storageEncodeEngineStateEngineInput(value))
}
func (value *stateEngineInput) UnmarshalBinary(data []byte) error {
	decoded := new(storagepb.EngineStateEngineInput)
	if err := storagecodec.Unmarshal(data, storagecodec.Input, decoded); err != nil {
		return state.ErrCorrupt
	}
	next := storageDecodeEngineStateEngineInput(decoded)
	if err := stateValidateRetainedInput(next); err != nil {
		return err
	}
	*value = next
	return nil
}

// Match the established single-wire representation. Parsed public message
// projections are reconstructed from the exact retained bytes on a fresh decode.
func stateEffectForStorage(value stateEngineEffect) stateEngineEffect {
	if value.Envelope == nil {
		return value
	}
	if value.Envelope.GetRequest().GetMessage() == nil && value.Envelope.GetEgress().GetMessage() == nil && value.Envelope.GetDialogEvent().GetMessage() == nil {
		return value
	}
	original := value.Envelope
	envelope := &pb.Envelope{CorrelationId: original.CorrelationId, SessionId: original.SessionId, RemainingBudgetMs: original.RemainingBudgetMs}
	envelope.ProtoReflect().SetUnknown(original.ProtoReflect().GetUnknown())
	var message *pb.SipMessage
	switch payload := original.Payload.(type) {
	case *pb.Envelope_Request:
		request := payload.Request
		retained := &pb.RequestPresented{Snapshot: request.Snapshot, RequestId: request.RequestId, Flow: request.Flow}
		retained.ProtoReflect().SetUnknown(request.ProtoReflect().GetUnknown())
		envelope.Payload = &pb.Envelope_Request{Request: retained}
		message = request.Message
	case *pb.Envelope_Egress:
		egress := payload.Egress
		retained := &pb.EgressRequest{Snapshot: egress.Snapshot, ReturnFlow: egress.ReturnFlow, Authorization: egress.Authorization}
		retained.ProtoReflect().SetUnknown(egress.ProtoReflect().GetUnknown())
		envelope.Payload = &pb.Envelope_Egress{Egress: retained}
		message = egress.Message
	case *pb.Envelope_DialogEvent:
		event := payload.DialogEvent
		retained := &pb.DialogEvent{Kind: event.Kind, Snapshot: event.Snapshot, OperationId: event.OperationId, Error: event.Error}
		retained.ProtoReflect().SetUnknown(event.ProtoReflect().GetUnknown())
		envelope.Payload = &pb.Envelope_DialogEvent{DialogEvent: retained}
		message = event.Message
	}
	if message != nil && len(value.Wire) == 0 {
		value.Wire = message.Wire
	}
	value.Envelope = envelope
	return value
}
func (value stateEngineEffect) MarshalBinary() ([]byte, error) {
	retained := stateEffectForStorage(value)
	return storagecodec.Marshal(storagecodec.Effect, storageEncodeEngineStateEngineEffect(retained))
}
func (value *stateEngineEffect) UnmarshalBinary(data []byte) error {
	return value.unmarshalBinary(data, true)
}

// Metadata consumers validate the same retained SIP identity without building
// controller message projections. Decoded values remain operation-local.
func (value *stateEngineEffect) unmarshalMetadata(data []byte) error {
	return value.unmarshalBinary(data, false)
}

func (value *stateEngineEffect) unmarshalBinary(data []byte, project bool, drain ...*state.LocalDrainObservation) error {
	return value.unmarshalBinaryPrepared(data, project, portable.Message{}, drain...)
}

func (value *stateEngineEffect) unmarshalBinaryPrepared(data []byte, project bool, prepared portable.Message, drain ...*state.LocalDrainObservation) error {
	decoded := new(storagepb.EngineStateEngineEffect)
	if err := storagecodec.Unmarshal(data, storagecodec.Effect, decoded); err != nil {
		return state.ErrCorrupt
	}
	next := storageDecodeEngineStateEngineEffect(decoded)
	if next.Policy != nil && !next.Policy.valid() {
		return state.ErrCorrupt
	}
	if !prepared.IsZero() && prepared.EqualWireBytes(next.Wire) {
		next.prepared = prepared
	}
	if project {
		if err := next.materializeMessage(); err != nil {
			return err
		}
	} else if next.hasMessageWire() || len(drain) > 0 && len(next.Wire) > 0 {
		message, facts, err := next.observedMessage()
		if err != nil {
			return err
		}
		if len(drain) > 0 {
			*drain[0] = drainObservedMessageObservation(message, facts)
		}
	}
	*value = next
	return nil
}

// The cache is scoped to one reduction. Reuse is guarded against the decoded
// effect's exact wire in unmarshalBinaryPrepared; metadata changes can then
// reuse their unchanged immutable SIP value without retaining a second payload.
type stateEffectMessage struct {
	message portable.Message
}

func (r *stateReduction) rememberEffectMessage(id string, message portable.Message, wire []byte) {
	if message.IsZero() || !message.EqualWireBytes(wire) {
		return
	}
	if r.effectMessages == nil {
		r.effectMessages = make(map[string]stateEffectMessage)
	}
	r.effectMessages[id] = stateEffectMessage{message: message}
}

func (r *stateReduction) unmarshalEffectMetadata(value *stateEngineEffect, effect state.Effect, drain ...*state.LocalDrainObservation) error {
	var prepared portable.Message
	if cached, ok := r.effectMessages[effect.ID]; ok {
		prepared = cached.message
	}
	if err := value.unmarshalBinaryPrepared(effect.Payload, false, prepared, drain...); err != nil {
		return err
	}
	if value.prepared.IsZero() && (effect.Kind == "wire" || effect.Kind == "deliver") && len(value.Wire) > 0 {
		// A historical physical effect can retain SIP bytes without an envelope.
		// Allocate its holder only; classification or completion prepares it later.
		value.prepared = portable.RestoreMessage(value.Wire, "", "", "")
	}
	if !value.prepared.IsZero() {
		if r.effectMessages == nil {
			r.effectMessages = make(map[string]stateEffectMessage)
		}
		r.effectMessages[effect.ID] = stateEffectMessage{message: value.prepared}
	}
	return nil
}

func (value *stateEngineEffect) observedMessage() (portable.Message, portable.MessageFacts, error) {
	if value.prepared.IsZero() || !value.prepared.EqualWireBytes(value.Wire) {
		value.prepared = portable.RestoreMessage(value.Wire, "", "", "")
	}
	facts, err := value.prepared.ObservedFacts()
	return value.prepared, facts, err
}

func (value stateEngineEffect) hasMessageWire() bool {
	envelope := value.Envelope
	return envelope != nil && len(value.Wire) > 0 && (envelope.GetRequest() != nil || envelope.GetEgress() != nil || envelope.GetDialogEvent() != nil)
}

// Call only at a boundary requiring the public projection. All retained metadata
// and wire have already been validated; this does not establish send authority.
func (value *stateEngineEffect) materializeMessage() error {
	if !value.hasMessageWire() {
		return nil
	}
	envelope := value.Envelope
	messageValue, _, err := value.observedMessage()
	if err != nil {
		return err
	}
	message, err := projectObservedNativeMessage(messageValue, envelope.GetEgress() != nil)
	if err != nil {
		return err
	}
	if value.Rejection {
		message.Selectors = nil
	}
	if request := envelope.GetRequest(); request != nil && request.Message == nil {
		request.Message = message
	}
	if egress := envelope.GetEgress(); egress != nil && egress.Message == nil {
		egress.Message = message
	}
	if event := envelope.GetDialogEvent(); event != nil && event.Message == nil {
		event.Message = message
	}
	return nil
}

func (value stateDigestChallenge) MarshalBinary() ([]byte, error) {
	return storagecodec.Marshal(storagecodec.Digest, storageEncodeEngineStateDigestChallenge(value))
}
func (value *stateDigestChallenge) UnmarshalBinary(data []byte) error {
	decoded := new(storagepb.EngineStateDigestChallenge)
	if err := storagecodec.Unmarshal(data, storagecodec.Digest, decoded); err != nil {
		return state.ErrCorrupt
	}
	*value = storageDecodeEngineStateDigestChallenge(decoded)
	return nil
}

func stateEncodeNetworkTarget(value *pb.OpenUac) ([]byte, error) {
	return storagecodec.Marshal(storagecodec.NetworkTarget, value)
}
func stateDecodeNetworkTarget(data []byte, value *pb.OpenUac) error {
	return storagecodec.Unmarshal(data, storagecodec.NetworkTarget, value)
}
