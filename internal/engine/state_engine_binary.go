package engine

import (
	"google.golang.org/protobuf/proto"
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
	decoded := new(storagepb.EngineStateGroup)
	if err := storagecodec.Unmarshal(data, storagecodec.Group, decoded); err != nil {
		return state.ErrCorrupt
	}
	next := storageDecodeEngineStateGroup(decoded)
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
	envelope := proto.Clone(value.Envelope).(*pb.Envelope)
	var message *pb.SipMessage
	if request := envelope.GetRequest(); request != nil {
		message, request.Message = request.Message, nil
	}
	if egress := envelope.GetEgress(); egress != nil {
		message, egress.Message = egress.Message, nil
	}
	if event := envelope.GetDialogEvent(); event != nil {
		message, event.Message = event.Message, nil
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
	decoded := new(storagepb.EngineStateEngineEffect)
	if err := storagecodec.Unmarshal(data, storagecodec.Effect, decoded); err != nil {
		return state.ErrCorrupt
	}
	next := storageDecodeEngineStateEngineEffect(decoded)
	if next.Policy != nil && !next.Policy.valid() {
		return state.ErrCorrupt
	}
	if project {
		if err := next.materializeMessage(); err != nil {
			return err
		}
	} else if next.hasMessageWire() || len(drain) > 0 && len(next.Wire) > 0 {
		parsed, err := parseObservedMessage(next.Wire)
		if err != nil {
			return err
		}
		if len(drain) > 0 {
			*drain[0] = drainMessageObservation(parsed)
		}
	}
	*value = next
	return nil
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
	message, err := Observe(value.Wire, envelope.GetEgress() != nil)
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
