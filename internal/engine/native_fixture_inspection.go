package engine

import (
	"encoding/hex"
	"encoding/json"
	"errors"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"sipbridge/internal/buildinfo"
	"sipbridge/internal/nativepb"
	"sipbridge/internal/selectedmessage"
	"sipbridge/internal/storagecodec"
	"sipbridge/internal/storagepb"
)

// NativeFixtureProjection is identifying, read-only synthetic qualification
// evidence. It is not retained state, a public control contract or import data.
type NativeFixtureProjection struct {
	Format      string                    `json:"format"`
	Provenance  NativeFixtureProvenance   `json:"provenance"`
	Redis       NativeFixtureRedis        `json:"redis"`
	Application *NativeFixtureApplication `json:"application,omitempty"`
	Contexts    []NativeFixtureContext    `json:"contexts"`
}
type NativeFixtureProvenance struct {
	Encoding               string          `json:"encoding"`
	SchemaIdentity         string          `json:"schemaIdentity"`
	BuildRevision          string          `json:"buildRevision"`
	NativeSchemaSHA256     string          `json:"nativeSchemaSha256"`
	NativeDescriptorSHA256 string          `json:"nativeDescriptorSha256"`
	ProfileSHA256          string          `json:"profileSha256,omitempty"`
	Root                   string          `json:"root,omitempty"`
	Authority              json.RawMessage `json:"authority,omitempty"`
}
type NativeFixtureRedis struct {
	CheckedAtMS int64  `json:"checkedAtMs"`
	PrimaryID   string `json:"primaryId"`
	Role        string `json:"role"`
}
type NativeFixtureApplication struct {
	Process     json.RawMessage `json:"process"`
	Reference   string          `json:"reference"`
	State       string          `json:"state"`
	Term        string          `json:"term,omitempty"`
	CheckedAtMS int64           `json:"checkedAtMs"`
	ExpiresAtMS int64           `json:"expiresAtMs,omitempty"`
}
type NativeFixtureIdentity struct {
	Context     string `json:"context"`
	Incarnation string `json:"incarnation"`
	Owner       string `json:"owner"`
	Term        string `json:"term"`
}
type NativeFixtureAssignment struct {
	Worker string `json:"worker"`
	Epoch  string `json:"epoch"`
	Term   string `json:"term"`
}
type NativeFixtureContext struct {
	Identity     NativeFixtureIdentity      `json:"identity"`
	Revision     uint64                     `json:"revision,string"`
	Assignment   NativeFixtureAssignment    `json:"timerAssignment"`
	Snapshot     json.RawMessage            `json:"snapshot"`
	Flow         json.RawMessage            `json:"flow,omitempty"`
	Procedures   []NativeFixtureProcedure   `json:"procedures"`
	Dialogs      []NativeFixtureDialog      `json:"dialogs"`
	Outputs      []NativeFixtureOutput      `json:"outputs"`
	Events       []NativeFixtureEvent       `json:"events"`
	Results      []NativeFixtureResult      `json:"results"`
	Subscription *NativeFixtureSubscription `json:"subscription,omitempty"`
}
type NativeFixtureTimer struct {
	Generation uint64 `json:"generation,string"`
	At         int64  `json:"at"`
	Interval   int64  `json:"interval"`
	Active     bool   `json:"active"`
}
type NativeFixtureProcedure struct {
	Field                            string                        `json:"field"`
	Key                              string                        `json:"key"`
	Family                           string                        `json:"family"`
	Phase                            string                        `json:"phase"`
	Cancelled                        bool                          `json:"cancelled"`
	CancelSent                       bool                          `json:"cancelSent"`
	Cleanup                          bool                          `json:"cleanup"`
	DecisionPending                  bool                          `json:"decisionPending"`
	OutputConsumed                   bool                          `json:"outputConsumed"`
	FinalResponseWrittenAcknowledged bool                          `json:"finalResponseWrittenAcknowledged"`
	Flow                             json.RawMessage               `json:"flow,omitempty"`
	Timers                           map[string]NativeFixtureTimer `json:"timers"`
	Origin                           *FixtureMessageProof          `json:"origin,omitempty"`
	Final                            *FixtureMessageProof          `json:"final,omitempty"`
	ACK                              *FixtureMessageProof          `json:"ack,omitempty"`
	CommandResult                    *NativeFixtureResult          `json:"commandResult,omitempty"`
	Outputs                          []string                      `json:"outputs"`
	Events                           []string                      `json:"events"`
	Root                             string                        `json:"root,omitempty"`
}
type NativeFixtureInvite struct {
	Sequence        uint32 `json:"sequence"`
	State           string `json:"state"`
	Initial         bool   `json:"initial"`
	CancelRequested bool   `json:"cancelRequested"`
}
type NativeFixtureDialog struct {
	Field        string                        `json:"field"`
	Role         string                        `json:"role"`
	Phase        string                        `json:"phase"`
	CallID       string                        `json:"callId"`
	LocalTag     string                        `json:"localTag"`
	RemoteTag    string                        `json:"remoteTag"`
	LocalCSeq    uint32                        `json:"localCSeq"`
	RemoteCSeq   uint32                        `json:"remoteCSeq"`
	TargetSHA256 string                        `json:"targetSha256"`
	RoutesSHA256 []string                      `json:"routesSha256"`
	AckWritten   bool                          `json:"ackWritten"`
	LocalInvite  *NativeFixtureInvite          `json:"localInvite,omitempty"`
	RemoteInvite *NativeFixtureInvite          `json:"remoteInvite,omitempty"`
	Timers       map[string]NativeFixtureTimer `json:"timers"`
	Outputs      []string                      `json:"outputs"`
}
type NativeFixtureProof struct {
	Present      bool   `json:"present"`
	State        string `json:"state,omitempty"`
	Settled      *bool  `json:"settled,omitempty"`
	Acknowledged *bool  `json:"acknowledged,omitempty"`
}
type NativeFixtureOutput struct {
	Field      string             `json:"field"`
	Procedure  string             `json:"procedure"`
	Dialog     string             `json:"dialog,omitempty"`
	Result     string             `json:"result,omitempty"`
	Method     string             `json:"method"`
	Status     uint32             `json:"status"`
	DeadlineMS int64              `json:"deadlineMs"`
	Flow       json.RawMessage    `json:"flow"`
	Proof      NativeFixtureProof `json:"proof"`
}
type NativeFixtureEvent struct {
	Field       string               `json:"field"`
	Kind        string               `json:"kind"`
	Root        string               `json:"root"`
	Owners      []string             `json:"owners"`
	OperationID string               `json:"operationId"`
	DeadlineMS  int64                `json:"deadlineMs"`
	Message     *FixtureMessageProof `json:"message,omitempty"`
}
type NativeFixtureResult struct {
	Field     string `json:"field"`
	Present   bool   `json:"present"`
	Sequence  uint64 `json:"sequence,string,omitempty"`
	Certainty string `json:"certainty,omitempty"`
	Pending   bool   `json:"pending"`
	CutoffMS  int64  `json:"cutoffMs,omitempty"`
	Output    string `json:"output,omitempty"`
}
type NativeFixtureSubscription struct {
	Present        bool   `json:"present"`
	LocalSequence  uint32 `json:"localSequence"`
	RemoteSequence uint32 `json:"remoteSequence"`
	Pending        string `json:"pending"`
	ExpiresMS      int64  `json:"expiresMs"`
	Terminating    bool   `json:"terminating"`
	Final          bool   `json:"final"`
	TargetSHA256   string `json:"targetSha256"`
}

func nativeFixtureProvenance() (NativeFixtureProvenance, error) {
	var receipt struct {
		SHA256 map[string]string `json:"sha256"`
	}
	if err := json.Unmarshal(storagecodec.Receipt(), &receipt); err != nil {
		return NativeFixtureProvenance{}, err
	}
	native := receipt.SHA256["proto/sipbridge-native-records.proto"]
	decoded, err := hex.DecodeString(native)
	if err != nil || len(decoded) != 32 || hex.EncodeToString(decoded) != native {
		return NativeFixtureProvenance{}, errors.New("native schema receipt missing")
	}
	descriptor := protodesc.ToFileDescriptorProto(nativepb.File_sipbridge_native_records_proto)
	data, err := (proto.MarshalOptions{Deterministic: true}).Marshal(descriptor)
	if err != nil {
		return NativeFixtureProvenance{}, err
	}
	return NativeFixtureProvenance{Encoding: storagecodec.Encoding, SchemaIdentity: storagecodec.SchemaIdentity() + ":" + native, BuildRevision: buildinfo.Revision, NativeSchemaSHA256: native, NativeDescriptorSHA256: fixtureHash(data)}, nil
}
func nativeFixtureJSON(value proto.Message) json.RawMessage {
	if value == nil || !value.ProtoReflect().IsValid() {
		return nil
	}
	data, _ := (protojson.MarshalOptions{UseEnumNumbers: true}).Marshal(value)
	return data
}
func nativeFixtureTimers(values map[string]*storagepb.PortableDeadline) map[string]NativeFixtureTimer {
	out := make(map[string]NativeFixtureTimer)
	for name, d := range values {
		if d != nil && d.Active {
			out[name] = NativeFixtureTimer{d.Generation, d.At, d.Interval, d.Active}
		}
	}
	return out
}
func nativeFixtureInvite(v *storagepb.PortableInviteProcedure) *NativeFixtureInvite {
	if v == nil {
		return nil
	}
	return &NativeFixtureInvite{v.Sequence, v.State, v.Initial, v.CancelRequested}
}

// NativeRecordInspection reports only redacted classes/counts for one native
// selected record. It never falls back to a legacy payload or exports its bytes.
type NativeRecordInspection struct {
	Provenance   NativeFixtureProvenance `json:"provenance"`
	Class        string                  `json:"class"`
	EncodedBytes int                     `json:"encodedBytes"`
	Family       string                  `json:"family,omitempty"`
	Phase        string                  `json:"phase,omitempty"`
	References   int                     `json:"references"`
}

func InspectNativeRecord(data []byte) (NativeRecordInspection, error) {
	provenance, err := nativeFixtureProvenance()
	if err != nil {
		return NativeRecordInspection{}, err
	}
	out := NativeRecordInspection{Provenance: provenance, EncodedBytes: len(data)}
	if len(data) < 6 || len(data) > storagecodec.MaxSelectedMessageBytes && len(data) > nativeRecordLimit {
		return NativeRecordInspection{}, errNativeRecord
	}
	switch data[5] {
	case storagecodec.SelectedMessage:
		if _, e := selectedmessage.DecodeOpaque(data); e != nil {
			return NativeRecordInspection{}, e
		}
		out.Class = "message"
	case storagecodec.NativeContext:
		v, e := decodeNativeContext(data)
		if e != nil {
			return NativeRecordInspection{}, e
		}
		out.Class = "context"
		out.References = len(v.InitialProcedures) + len(v.NoninitialProcedures) + len(v.Dialogs)
	case storagecodec.NativeProcedure:
		v, e := readPrivateProcedureRecord(data)
		if e != nil {
			return NativeRecordInspection{}, e
		}
		out.Class = "procedure"
		out.Family = v.Native.Family
		out.Phase = v.Native.Phase
		out.References = len(v.Outputs) + len(v.Events)
	case storagecodec.NativeDialog:
		v, e := readPrivateDialogRecord(data)
		if e != nil {
			return NativeRecordInspection{}, e
		}
		out.Class = "dialog"
		out.Phase = v.Native.Phase
		out.References = len(v.Outputs)
	case storagecodec.NativeSubscription:
		_, e := decodeNativeSubscription(data)
		if e != nil {
			return NativeRecordInspection{}, e
		}
		out.Class = "subscription"
	case storagecodec.NativeResult:
		_, e := decodeNativeResult(data)
		if e != nil {
			return NativeRecordInspection{}, e
		}
		out.Class = "result"
	case storagecodec.NativeLifecycle:
		v, e := decodeNativeLifecycle(data)
		if e != nil {
			return NativeRecordInspection{}, e
		}
		out.Class = "lifecycle"
		out.References = len(v.Owners)
	case storagecodec.NativeOutput:
		_, e := decodeNativeOutput(data)
		if e != nil {
			return NativeRecordInspection{}, e
		}
		out.Class = "output"
	default:
		return NativeRecordInspection{}, errNativeRecord
	}
	return out, nil
}
