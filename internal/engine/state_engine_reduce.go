package engine

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"

	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/execution"
	"sipbridge/internal/state"
)

var errStateApplicationOwner = errors.New("native/application owner unavailable")

type stateGroup struct {
	NextAllocation uint64                    `json:"nextAllocation,omitempty"`
	Allocations    map[string]stateAdmission `json:"allocations,omitempty"`
	OutboundID     string                    `json:"outboundId,omitempty"`
	Retired        map[string]int64          `json:"retired,omitempty"`
	Version        int                       `json:"version"`
	Contexts       map[string]*stateContext  `json:"contexts"`
	Selectors      map[string]string         `json:"selectors"`
}
type stateContext struct {
	Rejection             bool                         `json:"rejection,omitempty"`
	OwnerLost             bool                         `json:"ownerLost,omitempty"`
	OwnerValidated        bool                         `json:"ownerValidated,omitempty"`
	OwnerProbeAtMS        int64                        `json:"ownerProbeAtMs,omitempty"`
	OwnerDirectoryEpoch   string                       `json:"ownerDirectoryEpoch,omitempty"`
	OwnerProbeEffect      string                       `json:"ownerProbeEffect,omitempty"`
	OwnerCleanupAtMS      int64                        `json:"ownerCleanupAtMs,omitempty"`
	ACKEmissions          map[string]stateACKEmission  `json:"ackEmissions,omitempty"`
	ProgressEmissions     map[string]uint32            `json:"progressEmissions,omitempty"`
	ReplayUntilMS         int64                        `json:"replayUntilMs,omitempty"`
	Snapshot              *pb.ContextSnapshot          `json:"snapshot"`
	Flow                  *pb.FlowRef                  `json:"flow,omitempty"`
	Origin                portable.Message             `json:"origin"`
	Tag                   string                       `json:"tag"`
	Transactions          map[string]*stateTransaction `json:"transactions"`
	Requests              map[string]string            `json:"requests"`
	Dialogs               map[string]portable.Dialog   `json:"dialogs"`
	Dialog                string                       `json:"dialog,omitempty"`
	Operation             string                       `json:"operation,omitempty"`
	CommandFingerprint    string                       `json:"commandFingerprint,omitempty"`
	Result                *pb.OperationResult          `json:"result,omitempty"`
	RetireMS              int64                        `json:"retireMs"`
	Sequence              uint64                       `json:"sequence"`
	DialogScopes          map[string]string            `json:"dialogScopes,omitempty"`
	CompletionScope       string                       `json:"completionScope,omitempty"`
	ScopeOrigin           *portable.Message            `json:"scopeOrigin,omitempty"`
	Cancelled             bool                         `json:"cancelled"`
	Credential            *pb.CredentialRef            `json:"credential,omitempty"`
	ACKWritten            map[string]stateACKProof     `json:"ackWritten,omitempty"`
	DialogCompletionFlows map[string]*pb.FlowRef       `json:"dialogCompletionFlows,omitempty"`
	Delivery              *pb.ContextDelivery          `json:"delivery,omitempty"`
	DeliveryRevision      uint64                       `json:"deliveryRevision"`
	Subscription          *stateSubscription           `json:"subscription,omitempty"`
}
type stateTransaction struct {
	stateRequestDisposition
	Policy               *statePolicyAdmission    `json:"policy,omitempty"`
	OwnerValidated       bool                     `json:"ownerValidated,omitempty"`
	OwnerDeferred        bool                     `json:"ownerDeferred,omitempty"`
	Emissions            uint32                   `json:"emissions,omitempty"`
	ProvisionalEmissions uint32                   `json:"provisionalEmissions,omitempty"`
	Native               portable.Transaction     `json:"native"`
	Flow                 *pb.FlowRef              `json:"flow,omitempty"`
	ResponseFlow         *pb.FlowRef              `json:"responseFlow,omitempty"`
	Written              bool                     `json:"written,omitempty"`
	RequestID            string                   `json:"requestId"`
	Command              string                   `json:"command,omitempty"`
	RetireMS             int64                    `json:"retireMs"`
	Application          *pb.ProcessRef           `json:"application,omitempty"`
	Admission            pb.OwnerIngest_Admission `json:"admission"`
	CompletionScope      string                   `json:"completionScope,omitempty"`
	Cleanup              bool                     `json:"cleanup"`
	DigestAttempts       uint32                   `json:"digestAttempts,omitempty"`
	DigestEffect         string                   `json:"digestEffect,omitempty"`
	PendingResponse      *pb.Respond              `json:"pendingResponse,omitempty"`
	DeferredPresent      bool                     `json:"deferredPresent,omitempty"`
	RespondDeadlineMS    int64                    `json:"respondDeadlineMs,omitempty"`
	RespondStatus        uint32                   `json:"respondStatus,omitempty"`
	FinalResponse        *stateServerFinal        `json:"finalResponse,omitempty"`
}

func decodeStateGroup(record state.Record) (stateGroup, error) {
	g := stateGroup{Version: 1, Contexts: make(map[string]*stateContext), Selectors: make(map[string]string)}
	if len(record.Payload.ProtocolState) > 0 {
		if g.UnmarshalBinary(record.Payload.ProtocolState) != nil || g.Version != 1 || g.Contexts == nil || g.Selectors == nil {
			return g, state.ErrCorrupt
		}
	}
	if g.Retired == nil {
		g.Retired = make(map[string]int64)
	}
	if len(g.Allocations) > stateAdmissionLimit || g.NextAllocation > 1<<53-1 {
		return g, state.ErrCorrupt
	}
	for id, reservation := range g.Allocations {
		if id == "" || len(reservation.WireHash) != 64 || reservation.ExpiresMS <= 0 || reservation.ExpiresMS > 1<<53-1 {
			return g, state.ErrCorrupt
		}
	}
	if len(g.Retired) > stateRetiredMarkerLimit {
		return g, state.ErrCorrupt
	}
	for _, until := range g.Retired {
		if until <= 0 || until > 1<<53-1 {
			return g, state.ErrCorrupt
		}
	}
	for _, c := range g.Contexts {
		if c == nil {
			return g, state.ErrCorrupt
		}
		for _, tx := range c.Transactions {
			if tx == nil || (len(tx.AdmittedFlowProofs) > 0 || tx.DispositionUntilMS > 0) && !stateServerTransaction(tx) {
				return g, state.ErrCorrupt
			}
			if record.Versions == NativeStateVersions() && stateServerTransaction(tx) && (tx.DispositionUntilMS == 0 || !c.Rejection && len(tx.AdmittedFlowProofs) == 0) {
				return g, state.ErrCorrupt
			}
			if err := tx.stateRequestDisposition.validate(); err != nil {
				return g, state.ErrCorrupt
			}
		}
	}
	return g, nil
}
func stateMessageCallID(wire []byte) (string, error) {
	m, err := sip.ParseMessage(wire)
	if err != nil || m.CallID() == nil {
		return "", errors.New("valid Call-ID required")
	}
	return m.CallID().Value(), nil
}
func stateSnapshot(c *stateContext, executor *pb.ProcessRef) *pb.ContextSnapshot {
	v := proto.Clone(c.Snapshot).(*pb.ContextSnapshot)
	if c.Delivery != nil {
		v.Delivery = proto.Clone(c.Delivery).(*pb.ContextDelivery)
	}
	v.Executor = proto.Clone(executor).(*pb.ProcessRef)
	v.DirectoryEpoch = c.OwnerDirectoryEpoch
	v.Context.Owner = proto.Clone(executor).(*pb.ProcessRef)
	return v
}
func stateHeaders(headers []*pb.Header) []portable.Header {
	out := make([]portable.Header, 0, len(headers))
	for _, h := range headers {
		out = append(out, portable.Header{Name: h.Name, Value: h.Value})
	}
	return out
}

type stateReduction struct {
	engine            *StateEngine
	record            state.Record
	group             stateGroup
	payload           state.Payload
	in                stateEngineInput
	now               int64
	deadline          int64
	fingerprint       string
	ordinal           int
	result            *pb.OperationResult
	requestFence      *stateRequestFence
	finishedBudget    stateCompletionBudget // Pure result of the latest successful finish, never retained.
	policy            *statePolicyAdmission // Current command selection; never a second authoritative aggregate.
	selectorMemo      map[stateSelectorKey][]string
	selectorMemoBytes int
}

func (e *StateEngine) reduce(record state.Record, input state.Input, now int64) (state.Payload, []byte, error) {
	var in stateEngineInput
	if in.UnmarshalBinary(input.Payload) != nil {
		return record.Payload, nil, state.ErrCorrupt
	}
	if e.usesCAS() && record.Pending == nil && input.Proof && input.Kind == "timer" && in.Kind == "timer" {
		due := false
		for _, timer := range record.Payload.Timers {
			if timer.ID == in.Timer && timer.Generation == in.Generation && timer.DueMS <= now {
				due = true
				break
			}
		}
		if !due {
			outcome, err := json.Marshal(&pb.OperationResult{Certainty: pb.OperationResult_NOT_ACCEPTED, Error: "timer is not due"})
			if err != nil {
				return record.Payload, nil, err
			}
			return record.Payload, outcome, execution.ErrReadOnly
		}
	}
	// A different worker may already have started this RPC. This negative hint
	// grants no executor/write permission and need not contend for a revision.
	// Legacy pending input must still commit its refusal to clear itself.
	if e.usesCAS() && record.Pending == nil && input.Proof && input.Kind == "effect-start" && in.Kind == "effect-start" {
		startable := false
		for _, effect := range record.Payload.Effects {
			if effect.ID == in.Effect {
				startable = effect.Kind != "deliver" && effect.Status == state.EffectPending
				break
			}
		}
		if !startable {
			outcome, err := json.Marshal(&pb.OperationResult{Certainty: pb.OperationResult_NOT_ACCEPTED, Error: "RPC effect cannot start"})
			if err != nil {
				return record.Payload, nil, err
			}
			return record.Payload, outcome, execution.ErrReadOnly
		}
	}
	if e.usesCAS() && record.Pending == nil && input.Proof && input.Kind == "effect-result" && in.Kind == "effect-result" {
		found := false
		for _, effect := range record.Payload.Effects {
			if effect.ID == in.Effect {
				found = true
				break
			}
		}
		if !found {
			outcome, err := json.Marshal(&pb.OperationResult{Certainty: pb.OperationResult_NOT_ACCEPTED, Error: "effect not found"})
			if err != nil {
				return record.Payload, nil, err
			}
			return record.Payload, outcome, execution.ErrReadOnly
		}
	}
	g, err := decodeStateGroup(record)
	if err != nil {
		return record.Payload, nil, err
	}
	if in.Kind == "owner-check" && !e.nativeOwnerCheckNeeded(g, record, now) {
		outcome, err := json.Marshal(&pb.OperationResult{Certainty: pb.OperationResult_COMPLETED})
		if err != nil {
			return record.Payload, nil, err
		}
		return record.Payload, outcome, execution.ErrReadOnly
	}

	var originalRevision uint64
	originalContext := false
	if in.Context != nil {
		if c := g.Contexts[in.Context.Id]; c != nil {
			originalContext = true
			originalRevision = c.Snapshot.Revision
		}
	}
	r := &stateReduction{engine: e, record: record, group: g, payload: record.Payload, in: in, now: now, deadline: input.DeadlineMS, fingerprint: input.Fingerprint, result: &pb.OperationResult{Certainty: pb.OperationResult_COMPLETED}}
	if e.usesCAS() && record.Pending == nil && !r.autonomousProtocolInput() && stateControlPreflightNeeded(in, g.Contexts[in.Context.GetId()]) {
		return record.Payload, nil, errStateControlPreflight
	}
	r.payload.Effects = append([]state.Effect(nil), record.Payload.Effects...)
	r.payload.CommitLeaseGuards = nil
	if err := r.protectRequestFence(); err != nil {
		return record.Payload, nil, err
	}
	r.pruneRetired()
	if err := r.reclaimEffects(); err != nil {
		return record.Payload, nil, err
	}
	r.pruneTransactions()
	if err := r.refreshSelectors(); err != nil {
		return record.Payload, nil, err
	}
	if err := r.expireNativeOwners(); err != nil {
		return record.Payload, nil, err
	}

	err = nil
	if err == nil {
		switch in.Kind {
		case "locate":
			if e.autonomousCompletion() && in.Message != nil {
				err = r.reserveAdmission()
			}
		case "ingest":
			err = r.ingest()
		case "native-ingest":
			err = r.ingestNativeCompletion()
		case "reject":
			err = r.reject()
		case "command":
			err = r.command()
		case "reserve":
			err = r.reserve()
		case "complete":
			err = r.complete()
		case "owner-control":
			err = r.ownerControlResult()
		case "owner-result":
			err = r.ownerProbeResult()
		case "owner-check":
			// expireNativeOwners already applied all required owner transitions.
		case "digest-result":
			err = r.digestResult()
		case "effect-result":
			err = r.effectResult()
		case "effect-start":
			err = r.startEffect()
		case "timer":
			err = r.timerByIdentity()
		default:
			err = errors.New("unsupported native state input")
		}
	}
	ownerRefusal := errors.Is(err, errStateApplicationOwner) || errors.Is(err, errStatePolicyAdmission) || errors.Is(err, errStateBusinessAuthorityChanged)
	if err != nil {
		// Handlers stage values decoded from the immutable record. A failed
		// transition cannot leave a partial dialog mutation or output intent.
		r.clearSelectorMemo()
		r.group, _ = decodeStateGroup(record)
		r.payload = record.Payload
		r.payload.Effects = append([]state.Effect(nil), record.Payload.Effects...)
		r.payload.CommitLeaseGuards = nil
		if expiryErr := r.expireNativeOwners(); expiryErr != nil {
			return record.Payload, nil, expiryErr
		}
		r.result = &pb.OperationResult{Certainty: pb.OperationResult_NOT_ACCEPTED, Error: err.Error()}
		var rejection *portable.ProtocolError
		if errors.As(err, &rejection) {
			r.result.SipStatus = uint32(rejection.Status)
		}
		if !ownerRefusal && in.Kind == "command" && in.Command != nil && in.Context != nil {
			if c := r.group.Contexts[in.Context.Id]; c != nil && in.Command.Sequence == c.Snapshot.CommandSequence+1 {
				if e.autonomousCompletion() && !c.Rejection {
					if _, leaseErr := r.requireNativeOwner(c, true); leaseErr != nil {
						return record.Payload, nil, leaseErr
					}
				}
				c.Snapshot.CommandSequence = in.Command.Sequence
				c.Operation = in.Operation
				c.CommandFingerprint = input.Fingerprint
				c.Result = proto.Clone(r.result).(*pb.OperationResult)
				r.touch(c)
				r.result.Snapshot = stateSnapshot(c, e.cfg.Process)
			}
		}
	}
	if !input.Proof && (in.Kind == "command" || in.Kind == "ingest") && in.Context != nil {
		if c := r.group.Contexts[in.Context.Id]; c != nil && (!originalContext || c.Snapshot.Revision != originalRevision) {
			c.ReplayUntilMS = max(c.ReplayUntilMS, input.ReceiptUntilMS)
		}
	}
	data, err := r.finish()
	if err != nil {
		return record.Payload, nil, err
	}
	if !ownerRefusal && ((in.Kind == "command" || in.Kind == "ingest") && !input.Proof || in.Kind == "reject") && (!e.statePayloadFits(record, data) || !r.capacityFits(r.finishedBudget)) {
		// Fresh work may be refused; it cannot strand a pending slot needed by
		// previously admitted native completion/evidence transitions.
		r.clearSelectorMemo()
		r.group, _ = decodeStateGroup(record)
		r.payload = record.Payload
		r.payload.Effects = append([]state.Effect(nil), record.Payload.Effects...)
		r.payload.CommitLeaseGuards = nil
		if expiryErr := r.expireNativeOwners(); expiryErr != nil {
			return record.Payload, nil, expiryErr
		}
		r.result = &pb.OperationResult{Certainty: pb.OperationResult_NOT_ACCEPTED, SipStatus: 503, Error: "native completion capacity reserved"}
		if in.Kind == "command" && in.Command != nil && in.Context != nil {
			if c := r.group.Contexts[in.Context.Id]; c != nil && in.Command.Sequence == c.Snapshot.CommandSequence+1 {
				if e.autonomousCompletion() && !c.Rejection {
					if _, leaseErr := r.requireNativeOwner(c, true); leaseErr != nil {
						return record.Payload, nil, leaseErr
					}
				}
				c.Snapshot.CommandSequence = in.Command.Sequence
				c.Operation = in.Operation
				c.CommandFingerprint = input.Fingerprint
				c.Result = proto.Clone(r.result).(*pb.OperationResult)
				r.touch(c)
				c.ReplayUntilMS = max(c.ReplayUntilMS, input.ReceiptUntilMS)
				r.result.Snapshot = stateSnapshot(c, e.cfg.Process)
			}
		}
		data, err = r.finish()
		if err != nil {
			return record.Payload, nil, err
		}
	}
	stateRefreshResultSnapshot(r.group, r.result, e.cfg.Process)
	outcome, err := marshalStateReceiptResult(r.result)
	data.LocalResult = nil
	if err == nil && proto.Size(r.result) <= 1<<20 {
		data.LocalResult = r.result
	}
	return data, outcome, err
}

func (r *stateReduction) context() (*stateContext, error) {
	if r.in.Context == nil {
		return nil, errors.New("logical context required")
	}
	c := r.group.Contexts[r.in.Context.Id]
	if c == nil {
		return nil, errors.New("context not found")
	}
	return c, nil
}
func (r *stateReduction) touch(c *stateContext) {
	c.Snapshot.Revision++
	c.Snapshot.Executor = proto.Clone(r.engine.cfg.Process).(*pb.ProcessRef)
	c.Snapshot.Context.Owner = proto.Clone(r.engine.cfg.Process).(*pb.ProcessRef)
}
func (r *stateReduction) index(c *stateContext, m portable.Message, outbound bool) {
	selectors, err := r.selectorsForWire(m.Wire, outbound)
	if err != nil {
		return
	}
	for _, selector := range selectors {
		r.group.Selectors[selector] = c.Snapshot.Context.Id
		if !contains(c.Snapshot.Selectors, selector) {
			c.Snapshot.Selectors = append(c.Snapshot.Selectors, selector)
		}
	}
}
func contains(values []string, value string) bool {
	for _, v := range values {
		if v == value {
			return true
		}
	}
	return false
}
func (r *stateReduction) addEffect(kind string, c *stateContext, body stateEngineEffect, deadline int64) {
	r.ordinal++
	id := "e-" + r.engine.id(r.record.Ref.Incarnation, r.in.Operation, kind, c.Snapshot.Context.Id, strconv.FormatInt(r.now, 10), strconv.Itoa(r.ordinal))[:48]
	if deadline <= r.now {
		deadline = r.now + 2000
	}
	body.Context = c.Snapshot.Context.Id
	if body.Application == nil {
		body.Application = proto.Clone(c.Snapshot.Application).(*pb.ProcessRef)
	}
	if body.Envelope != nil {
		body.Envelope.CorrelationId = id
		body.Envelope.RemainingBudgetMs = uint32(min(2000, deadline-r.now))
	}
	data, _ := body.MarshalBinary()
	r.payload.Effects = append(r.payload.Effects, state.Effect{ID: id, Kind: kind, Status: state.EffectPending, DeadlineMS: deadline, Payload: data})
}
func (r *stateReduction) event(c *stateContext, kind pb.DialogEvent_Kind, message *pb.SipMessage, operation string, failure ...string) {
	if c.OwnerLost {
		return
	}
	if kind == pb.DialogEvent_PROVISIONAL {
		if c.ProgressEmissions == nil {
			c.ProgressEmissions = make(map[string]uint32)
		}
		key := stateProgressKey(message)
		if _, exists := c.ProgressEmissions[key]; !exists && len(c.ProgressEmissions) >= stateForkDialogs+1 {
			return
		}
		if c.ProgressEmissions[key] >= stateProfileTransmissions {
			return
		}
		c.ProgressEmissions[key]++
	}

	event := &pb.DialogEvent{Kind: kind, Snapshot: stateSnapshot(c, r.engine.cfg.Process), Message: message, OperationId: operation}
	if len(failure) > 0 {
		event.Error = failure[0]
	}
	r.addEffect("event", c, stateEngineEffect{Envelope: &pb.Envelope{Payload: &pb.Envelope_DialogEvent{DialogEvent: event}}}, r.now+2000)
}

func (r *stateReduction) ingest() error {
	in := r.in.Ingest
	if in == nil || in.Context == nil || in.Flow == nil || !sameProcess(in.Application, r.in.Application) {
		return errors.New("invalid ingest authority")
	}
	m := portable.Message{Wire: append([]byte(nil), in.Wire...), Transport: in.Flow.Transport, Source: net.JoinHostPort(in.Flow.SourceIp, strconv.Itoa(int(in.Flow.SourcePort)))}
	parsed, err := m.Parse()
	if err != nil {
		return err
	}
	if parsed.CallID().Value() != r.record.Ref.CallID {
		return errors.New("ingest Call-ID mismatch")
	}
	if request, ok := parsed.(*sip.Request); ok && !request.IsAck() {
		if r.requestFence == nil {
			if err := r.protectRequestFence(); err != nil {
				return err
			}
		}
		if r.requestFence != nil && r.requestFence.kind == "refused" {
			return r.handleRefusedRequest()
		}
		if r.requestFence != nil && r.requestFence.kind == "saturated" {
			return r.handledRequest()
		}
	}
	c := r.group.Contexts[in.Context.Id]
	if c != nil && c.Rejection {
		return &portable.ProtocolError{Status: 481, Reason: "rejection context cannot admit SIP work"}
	}
	if c == nil {
		if request, ok := parsed.(*sip.Request); ok && r.deadline > 0 && r.now >= r.deadline && !request.IsAck() && !request.IsCancel() && request.Method != sip.BYE {
			return r.handledRequest()
		}
		if r.engine.autonomousCompletion() {
			if err := r.consumeAdmission(in.Context, in.Wire); err != nil {
				return err
			}
		}
		if r.group.Retired[in.Context.Id] > r.now {
			return &portable.ProtocolError{Status: 481, Reason: "native context retired"}
		}
		request, ok := parsed.(*sip.Request)
		if !ok || tag(request.To().Params) != "" || request.IsAck() || request.IsCancel() {
			return &portable.ProtocolError{Status: 481, Reason: "unknown native context"}
		}
		if len(r.group.Contexts) >= 32 {
			return errors.New("context group capacity")
		}
		ref := r.engine.contextRef(r.record.Ref, in.Context.Id)
		c = &stateContext{Snapshot: &pb.ContextSnapshot{Context: ref, Application: proto.Clone(in.Application).(*pb.ProcessRef), BindingId: in.BindingId, PrincipalId: in.PrincipalId, Kind: pb.ContextSnapshot_TRANSACTION, CallId: parsed.CallID().Value(), RemoteTag: tag(parsed.From().Params), RemoteUri: parsed.From().Address.String(), LocalUri: parsed.To().Address.String(), Transport: in.Flow.Transport}, Flow: proto.Clone(in.Flow).(*pb.FlowRef), Origin: m.Clone(), Tag: r.engine.id("tag", r.record.Ref.Incarnation, in.Context.Id)[:32], Transactions: make(map[string]*stateTransaction), Requests: make(map[string]string), Dialogs: make(map[string]portable.Dialog)}
		c.Snapshot.LocalTag = c.Tag
		if request.IsInvite() {
			c.Snapshot.Kind = pb.ContextSnapshot_UAS
		}
		if err := r.pinNativeOwner(c); err != nil {
			return err
		}
		r.group.Contexts[ref.Id] = c
	} else if !sameProcess(c.Snapshot.Application, in.Application) && c.Snapshot.Kind != pb.ContextSnapshot_SUBSCRIPTION {
		if err := r.validateCompletionRelay(c, m, in); err != nil {
			return err
		}
	}
	if c.Snapshot.Transport != in.Flow.Transport {
		return errors.New("retained transport cannot change")
	}
	if request, ok := parsed.(*sip.Request); ok {
		if !request.IsAck() {
			key, err := sip.ServerTxKeyMake(request)
			if err != nil {
				return err
			}
			if occupied := c.Transactions[key]; occupied != nil && !stateExactServerRequest(occupied, m) {
				return &portable.ProtocolError{Status: 481, Reason: "occupied transaction identity mismatch"}
			}
		}
		var dialogEffects []portable.Effect
		processedDialog := c.Dialog
		for _, tx := range c.Transactions {
			if (tx.Native.Family == portable.InviteServer || tx.Native.Family == portable.NonInviteServer) && portable.MatchRequest(tx.Native.Origin, m) {
				if request.IsCancel() && tx.Native.Family == portable.InviteServer {
					key, err := sip.ServerTxKeyMake(request)
					if err != nil {
						return err
					}
					if retained := c.Transactions[key]; retained != nil && retained.Native.Family == portable.NonInviteServer {
						continue // A replay belongs to CANCEL's own retained transaction.
					}
				}
				if err := r.validateReplayFlow(c, tx, request, in); err != nil {
					return err
				}
				cancelReplay := request.IsCancel() && tx.Native.Family == portable.NonInviteServer
				if !request.IsAck() && (!request.IsCancel() || cancelReplay) && !bytes.Equal(tx.Native.Origin.Wire, m.Wire) {
					return &portable.ProtocolError{Status: 481, Reason: "transaction replay payload mismatch"}
				}
				if !request.IsAck() && !request.IsCancel() || cancelReplay {
					admitted, err := r.admitRequestFlow(tx, in.Flow, m.Wire)
					if err != nil {
						return err
					}
					if !admitted {
						return r.handledRequest()
					}
				}
				tx.Flow = proto.Clone(in.Flow).(*pb.FlowRef)
				if tx.Native.Phase == portable.Terminated && tx.Native.LastResponse != nil && !request.IsAck() && (!request.IsCancel() || cancelReplay) ||
					(!sameProcess(c.Snapshot.Application, in.Application) || r.in.NativeCompletion && c.OwnerLost) && stateCompletionFinalReplay(c, tx, m, request) {
					if err := r.send(c, tx, *tx.Native.LastResponse, true); err != nil {
						return err
					}
					r.result.Certainty = pb.OperationResult_ACCEPTED
					return nil
				}
				if request.IsCancel() && tx.Native.Family == portable.InviteServer {
					return r.cancelIncoming(c, tx, m)
				}
				if request.IsAck() && tx.Native.Phase == portable.Accepted {
					break
				}
				return r.stepTransaction(c, tx, portable.Input{Kind: portable.ReceiveRequest, Now: r.now, Message: m})
			}
		}
		if r.deadline > 0 && r.now >= r.deadline && !request.IsAck() && !request.IsCancel() && request.Method != sip.BYE {
			return r.handledRequest()
		}
		if err := r.ownerGuardRequest(c, request, in); err != nil {
			return err
		}
		if err := r.transactionCapacity(); err != nil && !request.IsAck() && request.Method != sip.BYE {
			return err
		}
		if !proto.Equal(c.Flow, in.Flow) {
			switch in.Admission {
			case pb.OwnerIngest_TLS_DIALOG_CONTINUITY:
				if c.Snapshot.Transport != "tls" {
					return &portable.ProtocolError{Status: 481, Reason: "TLS continuity required"}
				}
			case pb.OwnerIngest_AUTHENTICATED_BINDING:
				if c.Snapshot.PrincipalId == "" || c.Snapshot.BindingId == "" || c.Snapshot.PrincipalId != in.PrincipalId || c.Snapshot.BindingId != in.BindingId {
					return &portable.ProtocolError{Status: 481, Reason: "authenticated binding mismatch"}
				}
			default:
				return &portable.ProtocolError{Status: 481, Reason: "replacement flow admission required"}
			}
		}
		if c.Snapshot.Ended && !request.IsAck() && !(request.Method == sip.BYE && len(c.Dialogs) != 0) {
			return &portable.ProtocolError{Status: 481, Reason: "dialog ended"}
		}
		if c.Subscription != nil {
			if err := r.validateSubscription(c, request, in); err != nil {
				return err
			}
		}
		if c.Dialog == "" && c.Subscription == nil && tag(request.To().Params) == "" {
			origin, _ := c.Origin.Parse()
			if tag(origin.From().Params) != tag(request.From().Params) || origin.From().Address.String() != request.From().Address.String() || origin.To().Address.String() != request.To().Address.String() {
				return &portable.ProtocolError{Status: 481, Reason: "initial context identity mismatch"}
			}
		}
		if c.Dialog != "" || len(c.Dialogs) != 0 {
			dialogKey := tag(request.To().Params) + "/" + tag(request.From().Params)
			processedDialog = dialogKey
			d, exists := c.Dialogs[dialogKey]
			if !exists {
				return &portable.ProtocolError{Status: 481, Reason: "dialog tuple mismatch"}
			}
			next, effects, err := portable.StepDialog(d, portable.DialogInput{Kind: portable.DialogReceiveRequest, Now: r.now, Message: m})
			if err != nil {
				return err
			}
			c.Dialogs[dialogKey] = next
			dialogEffects = effects
			if request.IsAck() {
				r.result.Certainty = pb.OperationResult_ACCEPTED
				acknowledged := false
				for _, effect := range effects {
					if effect.Kind == portable.Acknowledged {
						acknowledged = true
						break
					}
				}
				if !acknowledged {
					return nil
				}
				initial := !c.Snapshot.InitialAck && dialogKey == c.Dialog
				// Completion evidence is independent of the original owner's
				// normal flow publication, including during its lease grace.
				if err := r.ownerAcceptedACK(c, dialogKey, in.Flow); err != nil {
					return err
				}
				if !c.OwnerLost {
					if err := r.proposeFlow(c, nil, in.Flow, c.Snapshot.Application, false); err != nil {
						return err
					}
					if initial {
						r.event(c, pb.DialogEvent_ACKNOWLEDGED, nil, "")
					}
				}
				return r.dialogEffectsFor(c, dialogKey, effects)
			}
		}
		family := portable.NonInviteServer
		if request.IsInvite() {
			family = portable.InviteServer
		}
		native, err := portable.NewTransaction(family, m, portable.DefaultTimers(), c.Tag)
		if err != nil {
			return err
		}
		if len(c.Transactions) >= 64 {
			return errors.New("transaction retention capacity")
		}
		application := c.Snapshot.Application
		if c.Snapshot.Kind == pb.ContextSnapshot_SUBSCRIPTION {
			application = in.Application
		}
		tx := &stateTransaction{Native: native, Flow: proto.Clone(in.Flow).(*pb.FlowRef), RequestID: "r-" + r.engine.id("request", r.record.Ref.Incarnation, native.Key)[:48], Application: application, Admission: in.Admission}
		if _, err := r.admitRequestFlow(tx, in.Flow, m.Wire); err != nil {
			return err
		}
		c.Transactions[native.Key] = tx
		c.Requests[tx.RequestID] = native.Key
		if c.Subscription != nil && request.Method == sip.SUBSCRIBE {
			c.Subscription.RemoteSequence = request.CSeq().SeqNo
			c.Subscription.Pending = native.Key
		}
		r.index(c, m, false)
		r.touch(c)
		if err = r.stepTransaction(c, tx, portable.Input{Kind: portable.Start, Now: r.now}); err != nil {
			return err
		}
		return r.dialogEffectsFor(c, processedDialog, dialogEffects)
	}
	for _, tx := range c.Transactions {
		if (tx.Native.Family == portable.InviteClient || tx.Native.Family == portable.NonInviteClient) && portable.MatchResponse(tx.Native.Origin, m) {
			if err := r.recordResponseFlow(c, tx, m, in); err != nil {
				return err
			}
			if tx.Native.Family == portable.InviteClient && tx.Native.Phase == portable.Terminated {
				response, _ := m.Parse()
				if response.(*sip.Response).IsSuccess() {
					// Native transaction retention is finished, but the exact
					// retained origin still owes late successful branch cleanup.
					_, err := r.forkResponse(c, tx, m)
					return err
				}
				if response.(*sip.Response).StatusCode >= 300 && tx.Native.ACK != nil {
					if err := tx.Native.Validate(); err != nil {
						return err
					}
					ack, err := tx.Native.ACK.Parse()
					if err != nil {
						return err
					}
					if tag(response.To().Params) == tag(ack.To().Params) {
						// Reliable transport ended the native client transaction,
						// but its exact retained ACK still completes a final replay.
						return r.send(c, tx, *tx.Native.ACK, true)
					}
				}
			}
			return r.stepTransaction(c, tx, portable.Input{Kind: portable.ReceiveResponse, Now: r.now, Message: m})
		}
	}
	return &portable.ProtocolError{Status: 481, Reason: "no live client transaction"}
}

func (r *stateReduction) cancelIncoming(c *stateContext, tx *stateTransaction, m portable.Message) error {
	request, _ := m.Parse()
	key, err := sip.ServerTxKeyMake(request)
	if err != nil {
		return err
	}
	cancelTX := c.Transactions[key]
	var prior *portable.Transaction
	if cancelTX != nil {
		if !stateExactServerRequest(cancelTX, m) {
			return &portable.ProtocolError{Status: 481, Reason: "occupied CANCEL identity mismatch"}
		}
		v := cancelTX.Native
		prior = &v
	}
	invite, cancel, effects, err := portable.StepServerCancel(tx.Native, prior, portable.Input{Kind: portable.ReceiveRequest, Now: r.now, Message: m})
	if err != nil {
		return err
	}
	if cancelTX == nil {
		cancelTX = &stateTransaction{Flow: tx.Flow, RequestID: "r-" + r.engine.id("request", key)[:48]}
	}
	candidate := *cancelTX
	candidate.Native = cancel
	admitted, err := r.admitRequestFlow(&candidate, r.in.Ingest.Flow, m.Wire)
	if err != nil {
		return err
	}
	if !admitted {
		return r.handledRequest()
	}
	tx.Native = invite
	*cancelTX = candidate
	c.Transactions[key] = cancelTX
	if cancel.Phase == portable.Terminated && cancelTX.RetireMS == 0 {
		cancelTX.RetireMS = r.now + 120000
	}
	r.touch(c)
	for _, effect := range effects {
		owner := tx
		// StepServerCancel returns CANCEL effects before the INVITE effects.
		// Its only Ended transition belongs to the non-INVITE CANCEL server.
		if effect.Kind == portable.Ended || effect.Message != nil && portable.MatchResponse(cancel.Origin, *effect.Message) {
			owner = cancelTX
		}
		if err := r.transactionEffects(c, owner, []portable.Effect{effect}); err != nil {
			return err
		}
	}
	return nil
}

func (r *stateReduction) stepTransaction(c *stateContext, tx *stateTransaction, input portable.Input) error {
	next, effects, err := portable.StepTransaction(tx.Native, input)
	if err != nil {
		return err
	}
	tx.Native = next
	if next.Phase == portable.Terminated && tx.RetireMS == 0 {
		tx.RetireMS = r.now + 120000
	}
	return r.transactionEffects(c, tx, effects)
}
func (r *stateReduction) transactionEffects(c *stateContext, tx *stateTransaction, effects []portable.Effect) error {
	for _, effect := range effects {
		switch effect.Kind {
		case portable.Send, portable.Respond:
			if effect.Message != nil {
				if err := r.send(c, tx, *effect.Message, effect.Replay); err != nil {
					return err
				}
			}
		case portable.StartTransaction:
			if effect.Message == nil {
				continue
			}
			native, err := portable.NewTransaction(portable.NonInviteClient, *effect.Message, portable.DefaultTimers(), "")
			if err != nil {
				return err
			}
			generated := &stateTransaction{Native: native, Flow: tx.Flow, Command: c.Operation, CompletionScope: tx.CompletionScope, Cleanup: tx.Cleanup}
			c.Transactions[native.Key] = generated
			r.index(c, *effect.Message, true)
			if err = r.stepTransaction(c, generated, portable.Input{Kind: portable.Start, Now: r.now}); err != nil {
				return err
			}
		case portable.PresentRequest:
			if parsed, err := tx.Native.Origin.Parse(); err == nil && parsed.CSeq().MethodName == sip.BYE && len(c.Dialogs) != 0 {
				continue
			}
			if err := r.presentRequest(c, tx); err != nil {
				return err
			}
		case portable.ReportResponse:
			if effect.Message != nil {
				if err := r.receivedResponse(c, tx, *effect.Message); err != nil {
					return err
				}
				if err := r.subscriptionResponse(c, tx, *effect.Message); err != nil {
					return err
				}
			}
		case portable.Cancelled:
			r.event(c, pb.DialogEvent_CANCELLED, nil, c.Operation)
		case portable.Ended:
			if err := r.transactionEnded(c, tx, effect.Error); err != nil {
				return err
			}
		}
	}
	return nil
}

func (r *stateReduction) send(c *stateContext, tx *stateTransaction, m portable.Message, replay bool, until ...int64) error {
	return r.sendOnFlow(c, tx, m, replay, nil, until...)
}

func (r *stateReduction) sendOnFlow(c *stateContext, tx *stateTransaction, m portable.Message, replay bool, responseFlow *pb.FlowRef, until ...int64) error {
	flow := tx.Flow
	if responseFlow != nil {
		flow = responseFlow
	}
	m, err := stateRenderResponse(m, flow)
	if err != nil {
		return err
	}
	message, err := Observe(m.Wire, true)
	if err != nil {
		return err
	}
	if c.Rejection {
		// The trusted controller also indexes message selectors. A refusal
		// must remain outside both native and directory routing indexes.
		message.Selectors = nil
	}
	authority := pb.EffectAuthorization_APPLICATION
	if tx.Cleanup {
		authority = pb.EffectAuthorization_TERMINAL_CLEANUP
	} else if message.Status > 0 {
		authority = pb.EffectAuthorization_RESPONSE
	} else if replay || message.Method == "ACK" || message.Method == "CANCEL" {
		authority = pb.EffectAuthorization_NATIVE_COMPLETION
	}
	r.ordinal++
	id := "e-" + r.engine.id("wire", r.record.Ref.Incarnation, c.Snapshot.Context.Id, tx.Native.Key, fmt.Sprint(c.Snapshot.Revision), fmt.Sprint(r.ordinal), string(m.Wire))[:48]
	ref := &pb.StateEffectRef{Context: proto.Clone(c.Snapshot.Context).(*pb.ContextRef), Id: id, AttemptId: "a-" + r.engine.id("attempt", id)[:32]}
	sendDeadline := r.now + 2000
	if len(until) > 0 {
		sendDeadline = min(sendDeadline, until[0])
	}
	if r.in.Kind == "command" && r.deadline > 0 {
		sendDeadline = min(sendDeadline, r.deadline)
	}
	if authority == pb.EffectAuthorization_APPLICATION && tx.Policy != nil {
		sendDeadline = min(sendDeadline, int64(tx.Policy.Decision.ExpiresAtMs))
	}
	auth := &pb.EffectAuthorization{Effect: ref, AuthorityClass: authority, DeadlineMs: uint64(sendDeadline)}
	var leaseGuard *state.ApplicationLeaseRef
	if r.engine.autonomousCompletion() {
		auth.ApplicationLeaseTerm = c.Snapshot.ApplicationLeaseTerm
		if authority == pb.EffectAuthorization_APPLICATION {
			lease, err := r.requireNativeOwner(c, false)
			if err != nil {
				return err
			}
			sendDeadline = min(sendDeadline, lease.ExpiresAtMS)
			auth.DeadlineMs = uint64(sendDeadline)
			if tx.Policy != nil && tx.Policy.Generation != lease.Generation {
				return fmt.Errorf("%w: command attachment superseded", errStateApplicationOwner)
			}
			leaseGuard = &state.ApplicationLeaseRef{ID: lease.ID, Term: c.Snapshot.ApplicationLeaseTerm, Generation: lease.Generation}
		}
	}
	if authority == pb.EffectAuthorization_NATIVE_COMPLETION || authority == pb.EffectAuthorization_TERMINAL_CLEANUP {
		auth.CompletionScope = tx.CompletionScope
		if auth.CompletionScope == "" {
			auth.CompletionScope = c.CompletionScope
		}
	}
	if r.engine.autonomousCompletion() && (authority != pb.EffectAuthorization_APPLICATION || tx.Policy != nil) {
		auth.CompletionScope = r.engine.nativeCompletionScope(c.Snapshot.Context)
	}
	snapshot := stateSnapshot(c, r.engine.cfg.Process)
	if parsed, err := m.Parse(); err == nil {
		if request, ok := parsed.(*sip.Request); ok && (request.IsAck() || tx.Cleanup) {
			// Completion belongs to this exact native branch, while the retained
			// application snapshot continues to identify the winning dialog.
			snapshot.LocalTag, snapshot.RemoteTag = tag(request.From().Params), tag(request.To().Params)
			if dialog, exists := c.Dialogs[snapshot.LocalTag+"/"+snapshot.RemoteTag]; exists {
				snapshot.DialogId = sip.DialogIDMake(dialog.CallID, dialog.LocalTag, dialog.RemoteTag)
				snapshot.RemoteTarget = dialog.Target
			}
		}
	}
	if message.Status == 0 {
		flow = c.Flow
		if authority == pb.EffectAuthorization_APPLICATION && tx.Policy != nil {
			flow, err = r.policyFlow(c, tx)
			if err != nil {
				return err
			}
			tx.Flow = proto.Clone(flow).(*pb.FlowRef)
		} else if authority == pb.EffectAuthorization_NATIVE_COMPLETION || authority == pb.EffectAuthorization_TERMINAL_CLEANUP {
			if completion := stateCompletionFlow(c, tx, m); completion != nil {
				flow = completion
			}
		}
	}
	auth.Effect.Id = ""
	fingerprint, err := SendFingerprint(snapshot, message, auth, flow)
	if err != nil {
		return err
	}
	id = "e-" + fingerprint
	ref.Id = id
	auth.Fingerprint = fingerprint
	snapshot.LocalSdp, snapshot.RemoteSdp, snapshot.Selectors = nil, nil, nil
	body := stateEngineEffect{Rejection: c.Rejection, Context: c.Snapshot.Context.Id, Application: c.Snapshot.Application, Transaction: tx.Native.Key, Command: tx.Command, Wire: append([]byte(nil), m.Wire...), Flow: flow, Authorization: auth}
	if stateServerTransaction(tx) && message.Status > 0 && message.Status != tx.RespondStatus {
		body.Command = "" // A protocol-generated response is not another Respond's write proof.
	}
	body.WireHash = stateACKHash(m.Wire)
	body.WireMethod = message.Method
	body.WireStatus = message.Status
	if parsed, err := m.Parse(); err == nil && parsed.CSeq() != nil {
		body.WireSequence = parsed.CSeq().SeqNo
	}

	if err := stateAnnotateACKDependency(c, tx, m, &body); err != nil {
		return err
	}
	emit, err := r.reserveEmission(c, tx, m, flow, replay)
	if err != nil {
		return err
	}
	if !emit {
		if r.in.Kind == "command" && r.in.Command.GetRespond() != nil {
			return state.ErrCapacity
		}
		return nil
	}
	if stateServerTransaction(tx) && message.Status > 0 {
		if !stateSelectedResponseEffect(tx, body) {
			priorFinalWritten := tx.FinalResponse != nil && tx.FinalResponse.FinalWritten
			priorAnyWritten := tx.FinalResponse != nil && tx.FinalResponse.Status == message.Status && tx.FinalResponse.Command == body.Command && tx.FinalResponse.AnyWritten
			var commandDeadline int64
			if body.Command != "" {
				commandDeadline = tx.RespondDeadlineMS
			}
			tx.FinalResponse = &stateServerFinal{Status: message.Status, Command: body.Command, DeadlineMS: commandDeadline,
				UntilMS: stateResponseUntil(tx, message.Status, body.Command), WireHash: body.WireHash,
				Flow: proto.Clone(flow).(*pb.FlowRef), FinalWritten: priorFinalWritten, AnyWritten: priorAnyWritten}
		}
		tx.FinalResponse.RetryAtMS = 0
	}
	body.Envelope = &pb.Envelope{CorrelationId: id, RemainingBudgetMs: 2000, Payload: &pb.Envelope_Egress{Egress: &pb.EgressRequest{Snapshot: snapshot, Message: message, ReturnFlow: flow, Authorization: auth}}}
	data, _ := body.MarshalBinary()
	r.payload.Effects = append(r.payload.Effects, state.Effect{ID: id, Kind: "wire", Status: state.EffectPending, DeadlineMS: sendDeadline, Payload: data, ApplicationLease: leaseGuard})
	return nil
}

func (r *stateReduction) command() error {
	cmd := r.in.Command
	if cmd == nil || cmd.Context == nil {
		return errors.New("command context required")
	}
	c := r.group.Contexts[cmd.Context.Id]
	if c != nil && c.Rejection {
		return &portable.ProtocolError{Status: 481, Reason: "rejection context cannot admit commands"}
	}
	if c == nil && cmd.GetOpenUac() != nil {
		if r.deadline > 0 && r.now >= r.deadline {
			return state.ErrExpired
		}
		return r.openUAC()
	}
	if c == nil {
		return errStateApplicationOwner
	}
	owner := sameProcess(c.Snapshot.Application, r.in.Application)
	if !owner && c.Subscription != nil && cmd.GetRespond() != nil {
		tx := c.Transactions[c.Requests[cmd.GetRespond().RequestId]]
		if tx != nil && c.Subscription.Pending == tx.Native.Key && sameProcess(tx.Application, r.in.Application) {
			original, err := tx.Native.Origin.Parse()
			owner = err == nil && original.CSeq().MethodName == sip.SUBSCRIBE
		}
	}
	if !owner {
		return errStateApplicationOwner
	}
	if r.deadline > 0 && r.now >= r.deadline {
		return state.ErrExpired
	}

	if err := r.ownerGuardCommand(c, cmd); err != nil {
		return err
	}
	if cmd.Sequence <= c.Snapshot.CommandSequence {
		if cmd.Sequence == c.Snapshot.CommandSequence && c.Operation == r.in.Operation && c.Result != nil {
			if c.CommandFingerprint == "" || c.CommandFingerprint != r.fingerprint {
				return errors.New("command replay payload conflict")
			}
			r.result = proto.Clone(c.Result).(*pb.OperationResult)
			r.result.Snapshot = stateSnapshot(c, r.engine.cfg.Process)
			return nil
		}
		return errors.New("stale command sequence")
	}
	if cmd.Sequence != c.Snapshot.CommandSequence+1 {
		return errors.New("command sequence gap")
	}
	policy, err := r.commandPolicy(c)
	if err != nil {
		return err
	}
	r.policy = policy
	r.installPolicy(c, policy)
	c.Snapshot.CommandSequence = cmd.Sequence
	c.Operation = r.in.Operation
	c.CommandFingerprint = r.fingerprint
	c.Result = &pb.OperationResult{Certainty: pb.OperationResult_ACCEPTED}
	r.touch(c)
	switch action := cmd.Action.(type) {
	case *pb.Command_Respond:
		key := c.Requests[action.Respond.RequestId]
		tx := c.Transactions[key]
		if tx == nil {
			return errors.New("request handle not found")
		}
		if !stateServerTransaction(tx) || tx.Native.Phase == portable.Terminated || tx.Native.Phase == portable.Completed || tx.Native.Phase == portable.Confirmed {
			return errors.New("server transaction cannot select response")
		}
		if !stateResponseEmissionAvailable(tx, action.Respond.Status) {
			return state.ErrCapacity
		}
		tx.Policy = policy
		if err := r.retainResponseAuthority(c, tx, action.Respond); err != nil {
			return err
		}
		r.anchorPolicyTransaction(c, tx)
		tx.RespondDeadlineMS = r.deadline
		tx.RespondStatus = action.Respond.Status
		if action.Respond.PrincipalId != "" {
			if c.Snapshot.PrincipalId != "" && c.Snapshot.PrincipalId != action.Respond.PrincipalId {
				return errors.New("principal mismatch")
			}
			c.Snapshot.PrincipalId = action.Respond.PrincipalId
			c.Snapshot.BindingId = action.Respond.BindingId
		}
		if action.Respond.Status >= 300 && c.Subscription != nil {
			c.Subscription.Pending = ""
		}
		originalRequest, _ := tx.Native.Origin.Parse()
		if originalRequest.CSeq().MethodName == sip.SUBSCRIBE && action.Respond.Status >= 200 && action.Respond.Status < 300 {
			return r.acceptSubscription(c, tx, action.Respond)
		}
		m, err := r.buildResponse(c, tx.Native.Origin, int(action.Respond.Status), reason(action.Respond.Status), action.Respond.Body, stateHeaders(action.Respond.Headers))
		if err != nil {
			return err
		}
		if action.Respond.Status >= 300 && tx.Native.Family == portable.InviteServer {
			key := tag(originalRequest.To().Params) + "/" + tag(originalRequest.From().Params)
			if d, exists := c.Dialogs[key]; exists {
				next, _, err := portable.StepDialog(d, portable.DialogInput{Kind: portable.DialogSendResponse, Now: r.now, Message: m})
				if err != nil {
					return err
				}
				// The transaction emits the response once. Rejection clears the
				// native pending offer without changing accepted SDP or target.
				c.Dialogs[key] = next
			}
		}
		if action.Respond.Status >= 200 && action.Respond.Status < 300 && tx.Native.Family == portable.InviteServer {
			var d portable.Dialog
			if c.Dialog != "" {
				d = c.Dialogs[c.Dialog]
			} else {
				d, err = portable.NewDialog(portable.UAS, tx.Native.Origin, m, portable.DefaultTimers())
				if err != nil {
					return err
				}
			}
			_, sentBy, err := r.engine.advertisement(c.Snapshot.Transport)
			if err != nil {
				return err
			}
			d.SentBy = sentBy
			key := d.LocalTag + "/" + d.RemoteTag
			c.Dialog = key
			c.Dialogs[key] = d
			d, effects, err := portable.StepDialog(d, portable.DialogInput{Kind: portable.DialogSendResponse, Now: r.now, Message: m, Answer: len(action.Respond.Body) > 0})
			if err != nil {
				return err
			}
			c.Dialogs[key] = d
			// Transaction selection/retention and TU successful-response state are
			// committed together; only the transaction's send effect is emitted here.
			_ = effects
			c.Snapshot.Answered = true
			c.Snapshot.LocalSdp = append([]byte(nil), action.Respond.Body...)
			original, _ := tx.Native.Origin.Parse()
			c.Snapshot.RemoteSdp = append([]byte(nil), original.Body()...)
			c.Snapshot.DialogId = sip.DialogIDMake(d.CallID, d.LocalTag, d.RemoteTag)
			c.Snapshot.LocalTag = d.LocalTag
			c.Snapshot.RemoteTag = d.RemoteTag
			c.Snapshot.RemoteTarget = d.Target
			if r.engine.autonomousCompletion() && tag(originalRequest.To().Params) == "" {
				if c.DialogScopes == nil {
					c.DialogScopes = make(map[string]string)
				}
				c.DialogScopes[key] = c.CompletionScope
			}
			r.index(c, m, true)
		}
		for i, effect := range r.payload.Effects {
			if effect.Kind != "present" {
				continue
			}
			var body stateEngineEffect
			if body.unmarshalMetadata(effect.Payload) == nil && body.Transaction == tx.Native.Key && body.Context == c.Snapshot.Context.Id {
				effect.Status = state.EffectWritten
				if effect.AttemptID == "" {
					effect.AttemptID = "consumed-" + r.engine.id(effect.ID)[:32]
				}
				r.payload.Effects[i] = effect
			}
		}
		tx.Command = r.in.Operation
		emissions := tx.Emissions
		if err := r.stepTransaction(c, tx, portable.Input{Kind: portable.SendResponse, Now: r.now, Message: m}); err != nil {
			return err
		}
		if tx.Emissions == emissions {
			return errors.New("server transaction cannot select response")
		}
		c.Result = &pb.OperationResult{Certainty: pb.OperationResult_ACCEPTED, Snapshot: stateSnapshot(c, r.engine.cfg.Process)}
	case *pb.Command_DialogRequest:
		if c.Subscription != nil {
			if err := r.notifySubscription(c, action.DialogRequest); err != nil {
				if errors.Is(err, errSubscriptionTerminating) {
					c.Result = &pb.OperationResult{Certainty: pb.OperationResult_COMPLETED, SipStatus: 481, Error: err.Error()}
					break
				}
				return err
			}
			break
		}
		if c.Dialog == "" {
			return errors.New("dialog unavailable")
		}
		d := c.Dialogs[c.Dialog]
		next, effects, err := portable.StepDialog(d, portable.DialogInput{Kind: portable.DialogLocalRequest, Now: r.now, Method: sip.RequestMethod(action.DialogRequest.Method), Body: action.DialogRequest.Body, Branch: sip.RFC3261BranchMagicCookie + r.engine.id("branch", r.record.Ref.Incarnation, r.in.Operation)[:32], Headers: stateHeaders(action.DialogRequest.Headers), Offer: action.DialogRequest.Method == "INVITE" && len(action.DialogRequest.Body) > 0})
		if err != nil {
			return err
		}
		c.Dialogs[c.Dialog] = next
		if err = r.dialogEffects(c, effects); err != nil {
			return err
		}
	case *pb.Command_Cancel:
		c.Cancelled = true
		if err := r.cancelUnstartedEffects(c); err != nil {
			return err
		}
		if len(c.Transactions) == 0 && c.Dialog == "" {
			c.Snapshot.Ended = true
			c.RetireMS = r.now + 120000
			r.event(c, pb.DialogEvent_ENDED, nil, c.Operation)
		}
		for _, tx := range c.Transactions {
			if tx.Native.Family == portable.InviteClient && tx.Native.Phase != portable.Terminated {
				if err := r.stepTransaction(c, tx, portable.Input{Kind: portable.RequestCancel, Now: r.now}); err != nil {
					return err
				}
			}
		}
		if c.Dialog != "" {
			d := c.Dialogs[c.Dialog]
			next, effects, err := portable.StepDialog(d, portable.DialogInput{Kind: portable.DialogCancel, Now: r.now})
			if err != nil {
				return err
			}
			c.Dialogs[c.Dialog] = next
			if err = r.dialogEffects(c, effects); err != nil {
				return err
			}
		}
	default:
		return errors.New("unsupported command")
	}
	r.result = proto.Clone(c.Result).(*pb.OperationResult)
	r.result.Snapshot = stateSnapshot(c, r.engine.cfg.Process)
	return nil
}

func (r *stateReduction) receivedResponse(c *stateContext, tx *stateTransaction, m portable.Message) error {
	if handled, err := r.digestChallenge(c, tx, m); handled || err != nil {
		return err
	}
	if handled, err := r.forkResponse(c, tx, m); handled || err != nil {
		return err
	}
	parsed, err := m.Parse()
	if err != nil {
		return err
	}
	response, ok := parsed.(*sip.Response)
	if !ok {
		return errors.New("response required")
	}
	// Only BYE has a dialog response procedure here; INVITE was handled above.
	// Other methods complete their client transaction without entering the
	// INVITE/BYE reducer or changing the dialog's SDP/target/lifecycle.
	for key, d := range c.Dialogs {
		if response.CSeq().MethodName != sip.BYE || !portable.MatchDialogResponse(d, m) {
			continue
		}
		next, effects, err := portable.StepDialog(d, portable.DialogInput{Kind: portable.DialogReceiveResponse, Now: r.now, Message: m})
		if err != nil {
			return err
		}
		c.Dialogs[key] = next
		if err = r.dialogEffectsFor(c, key, effects); err != nil {
			return err
		}
		break
	}
	if tx.Command != "" && !response.IsProvisional() && !tx.Cleanup {
		c.Result = &pb.OperationResult{Certainty: pb.OperationResult_COMPLETED, Request: mustObserve(tx.Native.Origin.Wire, true), Response: mustObserve(m.Wire, false), Snapshot: stateSnapshot(c, r.engine.cfg.Process)}
	}
	return nil
}
func mustObserve(wire []byte, outbound bool) *pb.SipMessage {
	message, _ := Observe(wire, outbound)
	return message
}

func (r *stateReduction) dialogEffects(c *stateContext, effects []portable.Effect) error {
	return r.dialogEffectsFor(c, c.Dialog, effects)
}

func (r *stateReduction) dialogEffectsFor(c *stateContext, dialog string, effects []portable.Effect) error {
	return r.dialogEffectsForClass(c, dialog, effects, false)
}

func (r *stateReduction) dialogEffectsForClass(c *stateContext, dialog string, effects []portable.Effect, cleanup bool) error {
	for _, effect := range effects {
		switch effect.Kind {
		case portable.StartTransaction:
			if effect.Message == nil {
				continue
			}
			parsed, err := effect.Message.Parse()
			if err != nil {
				return err
			}
			family := portable.NonInviteClient
			if parsed.CSeq().MethodName == sip.INVITE {
				family = portable.InviteClient
			}
			native, err := portable.NewTransaction(family, *effect.Message, portable.DefaultTimers(), "")
			if err != nil {
				return err
			}
			tx := &stateTransaction{Native: native, Flow: c.Flow, Command: c.Operation, Cleanup: cleanup || dialog != c.Dialog || c.Cancelled}
			if !tx.Cleanup {
				tx.Policy = r.policy
			}
			if tx.Cleanup && parsed.CSeq().MethodName == sip.BYE {
				tx.CompletionScope = c.DialogScopes[dialog]
			}
			c.Transactions[native.Key] = tx
			r.index(c, *effect.Message, true)
			if err = r.stepTransaction(c, tx, portable.Input{Kind: portable.Start, Now: r.now}); err != nil {
				return err
			}
		case portable.Send, portable.Respond:
			if effect.Message != nil {
				parsed, _ := effect.Message.Parse()
				if request, ok := parsed.(*sip.Request); ok && request.IsAck() {
					d := c.Dialogs[dialog]
					if d.LocalInvite == nil {
						return errors.New("ACK has no retained INVITE")
					}
					found := false
					for _, tx := range c.Transactions {
						if portable.MatchRequest(tx.Native.Origin, d.LocalInvite.Request) {
							if err := r.send(c, tx, *effect.Message, effect.Replay); err != nil {
								return err
							}
							found = true
							break
						}
					}
					if !found {
						return errors.New("ACK native transaction unavailable")
					}
					continue
				}
				for _, tx := range c.Transactions {
					if portable.MatchResponse(tx.Native.Origin, *effect.Message) || portable.MatchRequest(tx.Native.Origin, *effect.Message) {
						if effect.Kind == portable.Respond {
							if err := r.stepTransaction(c, tx, portable.Input{Kind: portable.SendResponse, Now: r.now, Message: *effect.Message}); err != nil {
								return err
							}
						} else {
							if err := r.send(c, tx, *effect.Message, effect.Replay); err != nil {
								return err
							}
						}
						break
					}
				}
			}
		case portable.NeedBYE:
			d := c.Dialogs[dialog]
			next, generated, err := portable.StepDialog(d, portable.DialogInput{Kind: portable.DialogLocalRequest, Now: r.now, Method: sip.BYE, Branch: sip.RFC3261BranchMagicCookie + r.engine.id("cleanup-bye", r.record.Ref.Incarnation, dialog)[:32]})
			if err != nil {
				return err
			}
			c.Dialogs[dialog] = next
			if err = r.dialogEffectsForClass(c, dialog, generated, true); err != nil {
				return err
			}
		case portable.NeedACK:
			d := c.Dialogs[dialog]
			next, generated, err := portable.StepDialog(d, portable.DialogInput{Kind: portable.DialogLocalACK, Now: r.now, Branch: sip.RFC3261BranchMagicCookie + r.engine.id("ack", r.record.Ref.Incarnation, dialog, c.Operation)[:32], Answer: true})
			if err != nil {
				return err
			}
			c.Dialogs[dialog] = next
			if err = r.dialogEffectsFor(c, dialog, generated); err != nil {
				return err
			}
		case portable.Ended:
			if dialog != c.Dialog {
				continue
			}
			c.Snapshot.Ended = true
			c.RetireMS = r.now + 120000
			r.touch(c)
			r.event(c, pb.DialogEvent_ENDED, nil, c.Operation)
		}
	}
	return nil
}

func (r *stateReduction) timer() error {
	parts := strings.SplitN(r.in.Timer, ":", 3)
	if len(parts) != 3 {
		return errors.New("invalid native timer")
	}
	c := r.group.Contexts[parts[0]]
	if c == nil {
		return nil
	}
	generation, err := strconv.ParseUint(r.in.Generation, 10, 64)
	if err != nil {
		return err
	}
	if strings.HasPrefix(parts[1], "d-") {
		key := strings.TrimPrefix(parts[1], "d-")
		d, ok := c.Dialogs[key]
		if !ok {
			return nil
		}
		next, effects, err := portable.StepDialog(d, portable.DialogInput{Kind: portable.DialogTimer, Now: r.now, Timer: portable.Timer(parts[2]), Generation: generation})
		if err != nil {
			return err
		}
		c.Dialogs[key] = next
		return r.dialogEffects(c, effects)
	}
	tx := c.Transactions[parts[1]]
	if tx == nil {
		return nil
	}
	return r.stepTransaction(c, tx, portable.Input{Kind: portable.TimerFired, Now: r.now, Timer: portable.Timer(parts[2]), Generation: generation})
}

func (r *stateReduction) finish() (state.Payload, error) {
	active := 0
	r.payload.Timers = nil
	for id, c := range r.group.Contexts {
		for key, tx := range c.Transactions {
			if r.transactionRetirable(id, key, tx) {
				delete(c.Transactions, key)
				delete(c.Requests, tx.RequestID)
				continue
			}
			if tx.Native.Phase != portable.Terminated {
				active++
			}
			for name, timer := range tx.Native.Timers {
				if timer.Active {
					r.payload.Timers = append(r.payload.Timers, state.Timer{ID: r.timerID(id, "tx", key, name), Generation: strconv.FormatUint(timer.Generation, 10), DueMS: timer.At, DeadlineMS: timer.At})
				}
			}
		}
		for key, d := range c.Dialogs {
			for name, timer := range d.Timers {
				if timer.Active {
					r.payload.Timers = append(r.payload.Timers, state.Timer{ID: r.timerID(id, "dialog", key, name), Generation: strconv.FormatUint(timer.Generation, 10), DueMS: timer.At, DeadlineMS: timer.At})
				}
			}
		}
	}
	if err := r.reclaimEffects(); err != nil {
		return state.Payload{}, err
	}
	filtered := r.payload.Effects[:0]
	for _, effect := range r.payload.Effects {
		if !stateSupportedEffectKind(effect.Kind) {
			return state.Payload{}, state.ErrCorrupt
		}
		if (effect.Status == state.EffectWritten || effect.Status == state.EffectNotWritten || effect.Status == state.EffectCancelled) && effect.DeadlineMS+120000 <= r.now {
			var body stateEngineEffect
			if err := body.unmarshalMetadata(effect.Payload); err != nil {
				return state.Payload{}, err
			}
			if effect.Kind != "deliver" || body.Settled {
				continue
			}
		}
		if (effect.Status == state.EffectWritten || effect.Status == state.EffectNotWritten || effect.Status == state.EffectCancelled) && effect.Kind != "credential" && effect.Kind != "wire" {
			var body stateEngineEffect
			if err := body.unmarshalMetadata(effect.Payload); err != nil {
				return state.Payload{}, err
			}
			if body.Wire != nil || body.Envelope != nil {
				body.Wire = nil
				body.Envelope = nil
				var err error
				effect.Payload, err = body.MarshalBinary()
				if err != nil {
					return state.Payload{}, err
				}
			}
		}
		filtered = append(filtered, effect)
	}
	r.payload.Effects = filtered
	if r.engine.autonomousCompletion() {
		if err := r.prepareAutonomousEffects(); err != nil {
			return state.Payload{}, err
		}
	}
	r.pruneAdmissions()
	if err := r.appendLifecycleTimers(); err != nil {
		return state.Payload{}, err
	}
	if r.engine.autonomousCompletion() {
		// Owner/lifecycle continuation can emit another retained obligation.
		// Every such effect must become native before dispatch sees the commit.
		if err := r.prepareAutonomousEffects(); err != nil {
			return state.Payload{}, err
		}
		r.projectNativeLeases()
		if err := r.projectDispatchWorkers(); err != nil {
			return state.Payload{}, err
		}
	}
	budget, err := r.completionBudget()
	if err != nil {
		return state.Payload{}, err
	}
	r.payload.ReservedCompletionBytes = budget.Bytes

	r.payload.ActiveTransactions = active
	r.payload.Retention = state.Retention{Terminal: len(r.group.Contexts) == 0 && len(r.group.Allocations) == 0, UntilMS: r.now + 120000}
	if err := r.refreshSelectors(); err != nil {
		return state.Payload{}, err
	}
	data, err := r.group.MarshalBinary()
	if err != nil {
		return state.Payload{}, err
	}
	r.payload.ProtocolState = data
	if r.engine.autonomousCompletion() {
		r.payload.TimerWorker = r.engine.dispatchWorker(r.engine.cfg.Process)
	}
	r.finishedBudget = budget
	if err := r.projectLocalObservations(); err != nil {
		return state.Payload{}, err
	}
	return r.payload, nil
}

func (r *stateReduction) projectDispatchWorkers() error {
	workers := make(map[[2]string]string)
	for index := range r.payload.Effects {
		effect := &r.payload.Effects[index]
		effect.Worker = ""
		if !stateDispatchableEffect(*effect, r.now) || effect.Kind == "open" && (effect.Status != state.EffectPending || effect.DeadlineMS <= r.now) {
			continue
		}
		if effect.Kind == "present" || effect.Kind == "event" || effect.Kind == "adopt" || effect.Kind == "credential" || effect.Kind == "open" {
			effect.Worker = state.AnyDispatchWorker
			continue
		}
		if effect.Kind != "deliver" {
			continue
		}
		var body stateEngineEffect
		if err := body.unmarshalMetadata(effect.Payload); err != nil {
			return err
		}
		if body.Native && !body.Settled && body.Permit.GetFlow().GetIngress() != nil {
			process := body.Permit.Flow.Ingress
			key := [2]string{process.InstanceId, process.Epoch}
			worker, found := workers[key]
			if !found {
				worker = r.engine.dispatchWorker(process)
				workers[key] = worker
			}
			effect.Worker = worker
		}
	}
	return nil
}
