package engine

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"

	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/state"
)

// FixtureCompletionProjection is an offline, identifying test view, not retained
// state or a public control contract. Every member is allowlisted; no wire, SDP,
// credential, permit or command-policy object is serialized.
type FixtureCompletionProjection struct {
	Group   fixtureInspectionGroup   `json:"group"`
	Payload fixtureInspectionPayload `json:"payload"`
}
type fixtureInspectionGroup struct {
	Version  int                                 `json:"version"`
	Contexts map[string]fixtureInspectionContext `json:"contexts"`
}
type fixtureInspectionPayload struct {
	ActiveTransactions      int                       `json:"activeTransactions"`
	ReservedCompletionBytes int                       `json:"reservedCompletionBytes"`
	Effects                 []fixtureInspectionEffect `json:"effects"`
	Timers                  []state.Timer             `json:"timers"`
	Retention               state.Retention           `json:"retention"`
}
type fixtureInspectionEffect struct {
	Kind       string                  `json:"kind"`
	Status     state.EffectStatus      `json:"status"`
	DeadlineMS int64                   `json:"deadlineMs"`
	Event      *fixtureInspectionEvent `json:"event,omitempty"`
}
type fixtureInspectionEvent struct {
	Kind        pb.DialogEvent_Kind `json:"kind"`
	OperationID string              `json:"operationId"`
}
type fixtureInspectionContext struct {
	Snapshot              json.RawMessage                         `json:"snapshot"`
	Rejection             bool                                    `json:"rejection"`
	Origin                *FixtureMessageProof                    `json:"origin,omitempty"`
	OwnerLost             bool                                    `json:"ownerLost"`
	OwnerDirectoryEpoch   string                                  `json:"ownerDirectoryEpoch"`
	OwnerProbeAtMS        int64                                   `json:"ownerProbeAtMs"`
	OwnerCleanupAtMS      int64                                   `json:"ownerCleanupAtMs"`
	RetireMS              int64                                   `json:"retireMs"`
	ReplayUntilMS         int64                                   `json:"replayUntilMs"`
	CompletionScope       string                                  `json:"completionScope"`
	Dialog                string                                  `json:"dialog"`
	DialogScopes          map[string]string                       `json:"dialogScopes"`
	DialogCompletionFlows map[string]*pb.FlowRef                  `json:"dialogCompletionFlows"`
	Flow                  *pb.FlowRef                             `json:"flow"`
	Delivery              *fixtureInspectionDelivery              `json:"delivery,omitempty"`
	Subscription          *stateSubscription                      `json:"subscription,omitempty"`
	Transactions          map[string]fixtureInspectionTransaction `json:"transactions"`
	Dialogs               map[string]fixtureInspectionDialog      `json:"dialogs"`
}
type fixtureInspectionDelivery struct {
	Flow                     *pb.FlowRef `json:"flow"`
	NativeRevision           uint64      `json:"nativeRevision,string"`
	DirectoryEpoch           string      `json:"directoryEpoch"`
	RegistrationTicketSHA256 string      `json:"registrationTicketSha256"`
}
type fixtureInspectionTransaction struct {
	Native          fixtureInspectionNative `json:"native"`
	CompletionScope string                  `json:"completionScope"`
	Command         string                  `json:"command"`
	Emissions       uint32                  `json:"emissions"`
	Cleanup         bool                    `json:"cleanup"`
}
type fixtureInspectionNative struct {
	Key          string                               `json:"key"`
	Family       portable.Family                      `json:"family"`
	Phase        portable.Phase                       `json:"phase"`
	Cancelled    bool                                 `json:"cancelled"`
	CancelSent   bool                                 `json:"cancel_sent"`
	Timers       map[portable.Timer]portable.Deadline `json:"timers"`
	Origin       *FixtureMessageProof                 `json:"origin,omitempty"`
	LastResponse *FixtureMessageProof                 `json:"last_response,omitempty"`
}
type fixtureInspectionDialog struct {
	Phase      portable.DialogPhase `json:"phase"`
	LocalCSeq  uint32               `json:"local_cseq"`
	LocalSet   bool                 `json:"local_set"`
	RemoteCSeq uint32               `json:"remote_cseq"`
	RemoteSet  bool                 `json:"remote_set"`
	Target     string               `json:"target"`
	Routes     []string             `json:"routes"`
}

// FixtureMessageProof preserves exact comparisons without exporting message bytes.
type FixtureMessageProof struct {
	Type       string `json:"type"`
	Method     string `json:"method"`
	Status     int    `json:"status,omitempty"`
	CallID     string `json:"callId"`
	CSeq       string `json:"cseq"`
	CSeqNumber uint32 `json:"cseqNumber"`
	CSeqMethod string `json:"cseqMethod"`
	FromTag    string `json:"fromTag"`
	ToTag      string `json:"toTag"`
	WireSHA256 string `json:"wireSha256"`
	FromSHA256 string `json:"fromSha256"`
	ToSHA256   string `json:"toSha256"`
}

func fixtureHash(value []byte) string {
	if len(value) == 0 {
		return ""
	}
	sum := sha256.Sum256(value)
	return hex.EncodeToString(sum[:])
}
func fixtureURI(value string) string {
	if value == "" {
		return ""
	}
	return "sha256:" + fixtureHash([]byte(value))
}
func fixtureRoutes(values []string) []string {
	if values == nil {
		return nil
	}
	out := make([]string, len(values))
	for i, value := range values {
		out[i] = fixtureURI(value)
	}
	return out
}
func fixtureHeader(wire []byte, wanted string) (string, error) {
	head := wire
	if end := bytes.Index(head, []byte("\r\n\r\n")); end >= 0 {
		head = head[:end]
	} else if end := bytes.Index(head, []byte("\n\n")); end >= 0 {
		head = head[:end]
	}
	lines := bytes.Split(head, []byte("\n"))
	var values []string
	active := -1
	for _, line := range lines[1:] {
		line = bytes.TrimSuffix(line, []byte("\r"))
		if len(line) > 0 && (line[0] == ' ' || line[0] == '\t') {
			if active >= 0 {
				values[active] += "\r\n" + string(line)
			}
			continue
		}
		active = -1
		parts := bytes.SplitN(line, []byte(":"), 2)
		if len(parts) != 2 {
			continue
		}
		name := strings.ToLower(strings.TrimSpace(string(parts[0])))
		if name == "f" {
			name = "from"
		}
		if name == "t" {
			name = "to"
		}
		if name == wanted {
			values = append(values, strings.TrimSpace(string(parts[1])))
			active = len(values) - 1
		}
	}
	if len(values) != 1 {
		return "", state.ErrCorrupt
	}
	return values[0], nil
}
func fixtureMessage(message portable.Message) (*FixtureMessageProof, error) {
	if len(message.Wire) == 0 {
		return nil, nil
	}
	parsed, err := message.Parse()
	if err != nil || parsed.CSeq() == nil || parsed.CallID() == nil || parsed.From() == nil || parsed.To() == nil {
		return nil, state.ErrCorrupt
	}
	from, err := fixtureHeader(message.Wire, "from")
	if err != nil {
		return nil, err
	}
	to, err := fixtureHeader(message.Wire, "to")
	if err != nil {
		return nil, err
	}
	cseq, err := fixtureHeader(message.Wire, "cseq")
	if err != nil {
		return nil, err
	}
	out := &FixtureMessageProof{Type: "request", Method: string(parsed.CSeq().MethodName), CallID: parsed.CallID().Value(), CSeq: cseq, CSeqNumber: parsed.CSeq().SeqNo, CSeqMethod: string(parsed.CSeq().MethodName), FromTag: tag(parsed.From().Params), ToTag: tag(parsed.To().Params), WireSHA256: fixtureHash(message.Wire), FromSHA256: fixtureHash([]byte(from)), ToSHA256: fixtureHash([]byte(to))}
	if response, ok := parsed.(*sip.Response); ok {
		out.Type = "response"
		out.Status = response.StatusCode
	}
	return out, nil
}
func fixtureDelivery(value *pb.ContextDelivery) *fixtureInspectionDelivery {
	if value == nil {
		return nil
	}
	return &fixtureInspectionDelivery{Flow: value.Flow, NativeRevision: value.NativeRevision, DirectoryEpoch: value.DirectoryEpoch, RegistrationTicketSHA256: fixtureHash([]byte(value.RegistrationTicket))}
}
func fixtureSnapshot(value *pb.ContextSnapshot) (json.RawMessage, error) {
	if value == nil {
		return nil, state.ErrCorrupt
	}
	copy := proto.Clone(value).(*pb.ContextSnapshot)
	copy.LocalSdp, copy.RemoteSdp = nil, nil
	copy.LocalUri, copy.RemoteUri, copy.RemoteTarget = fixtureURI(copy.LocalUri), fixtureURI(copy.RemoteUri), fixtureURI(copy.RemoteTarget)
	if copy.Delivery != nil {
		copy.Delivery.RegistrationTicket = fixtureURI(copy.Delivery.RegistrationTicket)
	}
	return protojson.MarshalOptions{UseEnumNumbers: true, EmitUnpopulated: true}.Marshal(copy)
}
func InspectFixtureCompletion(payload state.Payload) (FixtureCompletionProjection, error) {
	group, err := decodeStateGroup(state.Record{Payload: payload, Versions: NativeStateVersions()})
	if err != nil {
		return FixtureCompletionProjection{}, state.ErrCorrupt
	}
	out := FixtureCompletionProjection{Group: fixtureInspectionGroup{Version: group.Version, Contexts: make(map[string]fixtureInspectionContext)}, Payload: fixtureInspectionPayload{ActiveTransactions: payload.ActiveTransactions, ReservedCompletionBytes: payload.ReservedCompletionBytes, Timers: payload.Timers, Retention: payload.Retention}}
	for id, c := range group.Contexts {
		snapshot, err := fixtureSnapshot(c.Snapshot)
		if err != nil {
			return FixtureCompletionProjection{}, state.ErrCorrupt
		}
		origin, err := fixtureMessage(c.Origin)
		if err != nil {
			return FixtureCompletionProjection{}, err
		}
		next := fixtureInspectionContext{Snapshot: snapshot, Rejection: c.Rejection, Origin: origin, OwnerLost: c.OwnerLost, OwnerDirectoryEpoch: c.OwnerDirectoryEpoch, OwnerProbeAtMS: c.OwnerProbeAtMS, OwnerCleanupAtMS: c.OwnerCleanupAtMS, RetireMS: c.RetireMS, ReplayUntilMS: c.ReplayUntilMS, CompletionScope: c.CompletionScope, Dialog: c.Dialog, DialogScopes: c.DialogScopes, DialogCompletionFlows: c.DialogCompletionFlows, Flow: c.Flow, Delivery: fixtureDelivery(c.Delivery), Transactions: make(map[string]fixtureInspectionTransaction), Dialogs: make(map[string]fixtureInspectionDialog)}
		if c.Subscription != nil {
			copy := *c.Subscription
			copy.Target = fixtureURI(copy.Target)
			copy.Routes = fixtureRoutes(copy.Routes)
			next.Subscription = &copy
		}
		for key, tx := range c.Transactions {
			origin, err := fixtureMessage(tx.Native.Origin)
			if err != nil {
				return FixtureCompletionProjection{}, err
			}
			var last *FixtureMessageProof
			if tx.Native.LastResponse != nil {
				last, err = fixtureMessage(*tx.Native.LastResponse)
				if err != nil {
					return FixtureCompletionProjection{}, err
				}
			}
			next.Transactions[key] = fixtureInspectionTransaction{CompletionScope: tx.CompletionScope, Command: tx.Command, Emissions: tx.Emissions, Cleanup: tx.Cleanup, Native: fixtureInspectionNative{Key: tx.Native.Key, Family: tx.Native.Family, Phase: tx.Native.Phase, Cancelled: tx.Native.Cancelled, CancelSent: tx.Native.CancelSent, Timers: tx.Native.Timers, Origin: origin, LastResponse: last}}
		}
		for key, dialog := range c.Dialogs {
			next.Dialogs[key] = fixtureInspectionDialog{Phase: dialog.Phase, LocalCSeq: dialog.LocalCSeq, LocalSet: dialog.LocalSet, RemoteCSeq: dialog.RemoteCSeq, RemoteSet: dialog.RemoteSet, Target: fixtureURI(dialog.Target), Routes: fixtureRoutes(dialog.Routes)}
		}
		out.Group.Contexts[id] = next
	}
	for _, effect := range payload.Effects {
		if !stateSupportedEffectKind(effect.Kind) {
			return FixtureCompletionProjection{}, state.ErrCorrupt
		}
		switch effect.Status {
		case state.EffectPending, state.EffectAuthorized, state.EffectDispatching, state.EffectUnknown, state.EffectWritten, state.EffectNotWritten, state.EffectCancelled:
		default:
			return FixtureCompletionProjection{}, state.ErrCorrupt
		}
		var body stateEngineEffect
		if body.unmarshalMetadata(effect.Payload) != nil {
			return FixtureCompletionProjection{}, state.ErrCorrupt
		}
		next := fixtureInspectionEffect{Kind: effect.Kind, Status: effect.Status, DeadlineMS: effect.DeadlineMS}
		if effect.Kind == "event" && body.Envelope.GetDialogEvent() != nil {
			event := body.Envelope.GetDialogEvent()
			next.Event = &fixtureInspectionEvent{Kind: event.Kind, OperationID: event.OperationId}
		}
		out.Payload.Effects = append(out.Payload.Effects, next)
	}
	return out, nil
}
