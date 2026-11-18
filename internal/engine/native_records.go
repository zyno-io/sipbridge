package engine

import (
	"bytes"
	"errors"
	"strconv"
	"strings"

	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/nativepb"
	"sipbridge/internal/selectedmessage"
	"sipbridge/internal/state"
	"sipbridge/internal/storagecodec"
)

var errNativeRecord = errors.New("invalid selected native record")

const nativeRecordLimit = storagecodec.MaxNativeRecordBytes

// These record codecs own no storage operations. Decoded references name only
// fields the caller explicitly selected from the same context incarnation.
func marshalNativeRecord(kind byte, value proto.Message) ([]byte, error) {
	if value == nil {
		return nil, errNativeRecord
	}
	data, err := storagecodec.Marshal(kind, value)
	if err != nil {
		return nil, err
	}
	if len(data) > nativeRecordLimit {
		return nil, errNativeRecord
	}
	return data, nil
}
func unmarshalNativeRecord(data []byte, kind byte, value proto.Message) error {
	if len(data) > nativeRecordLimit {
		return errNativeRecord
	}
	return storagecodec.Unmarshal(data, kind, value)
}
func nativeMessageField(s string) bool {
	if s == "" {
		return true
	}
	if !strings.HasPrefix(s, "r:") || len(s) < 3 || len(s) > 194 {
		return false
	}
	for _, c := range s[2:] {
		if c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-' || c == '_' {
			continue
		}
		return false
	}
	return true
}

func nativeOptionalResultField(s string) bool {
	if s == "" {
		return true
	}
	text, ok := strings.CutPrefix(s, "r:result-")
	sequence, err := strconv.ParseUint(text, 10, 53)
	return ok && err == nil && sequence > 0 && strconv.FormatUint(sequence, 10) == text
}
func nativeReferenceMatches(m portable.Message, ref string) bool {
	return nativeMessageField(ref) && (m.WireLen() > 0) == (ref != "")
}
func nativeOptionalReferenceMatches(m *portable.Message, ref string) bool {
	return nativeMessageField(ref) && ((m == nil && ref == "") || (m != nil && m.WireLen() > 0 && ref != ""))
}

type nativeProcedureRefs struct{ Origin, LastResponse, ACK string }
type nativeProcedureRecord struct {
	Native   portable.Transaction
	Refs     nativeProcedureRefs
	Metadata *nativepb.Procedure
}

func validNativeProcedureMetadata(metadata *nativepb.Procedure) bool {
	return metadata != nil && validNativeSuccessfulACK(metadata.SuccessfulAck) && metadata.CreditContribution <= 1<<53-1 && metadata.AckEmissions <= stateProfileTransmissions && (len(metadata.WireHash) == 0 || len(metadata.WireHash) == 32) && nativeMessageField(metadata.CommandResult) && nativeMessageField(metadata.Root) && (metadata.Root == "" || metadata.Root != "r:procedure-"+metadata.RequestId && len(metadata.Events) == 0) && validNativeOrderedReferences(metadata.Outputs, stateTransactionEmissions) && validNativeOrderedReferences(metadata.Events, 3)
}

func validNativeSuccessfulACK(ack *nativepb.SuccessfulAck) bool {
	return ack == nil || ack.FinalResponse != "" && ack.Ack != "" && ack.FinalResponse != ack.Ack && nativeMessageField(ack.FinalResponse) && nativeMessageField(ack.Ack) && nativeOptionalResultField(ack.AckResult) && validPolicyFlow(ack.Flow) && ack.Emissions > 0 && ack.Emissions <= stateProfileTransmissions
}

// Canonical ordering validates uniqueness without a temporary set on every
// selected operation. Empty reference collections encode no field.
func validNativeOrderedReferences(refs []string, limit int) bool {
	if len(refs) > limit {
		return false
	}
	for i, ref := range refs {
		if ref == "" || !nativeMessageField(ref) || i > 0 && refs[i-1] >= ref {
			return false
		}
	}
	return true
}
func encodeNativeProcedure(tx portable.Transaction, refs nativeProcedureRefs, metadata *nativepb.Procedure) ([]byte, error) {
	v, err := buildPrivateProcedureRecord(tx, refs, metadata)
	if err != nil {
		return nil, err
	}
	return marshalNativeRecord(storagecodec.NativeProcedure, v)
}
func buildPrivateProcedureRecord(tx portable.Transaction, refs nativeProcedureRefs, metadata *nativepb.Procedure) (*nativepb.Procedure, error) {
	if !validNativeProcedureMetadata(metadata) || metadata.Native != nil || metadata.Origin != "" || metadata.LastResponse != "" || metadata.Ack != "" || metadata.Root != "" && tx.Family != portable.InviteClient || metadata.SuccessfulAck != nil && (tx.Family != portable.InviteClient || tx.Phase != portable.Accepted && tx.Phase != portable.Terminated) || metadata.AckEmissions != 0 && (tx.Family != portable.InviteClient || tx.ACK == nil || refs.ACK == "") || len(metadata.Events) != 0 && tx.Family != portable.InviteClient && tx.Family != portable.InviteServer && !(tx.Family == portable.NonInviteClient && metadata.Cleanup) || !nativeReferenceMatches(tx.Origin, refs.Origin) || !nativeOptionalReferenceMatches(tx.LastResponse, refs.LastResponse) || !nativeOptionalReferenceMatches(tx.ACK, refs.ACK) {
		return nil, errNativeRecord
	}
	stripped := tx
	stripped.Origin = portable.Message{}
	stripped.LastResponse = nil
	stripped.ACK = nil
	v := proto.Clone(metadata).(*nativepb.Procedure)
	v.Native = storageEncodePortableTransaction(stripped)
	v.Native.Origin = nil
	v.Origin = refs.Origin
	v.LastResponse = refs.LastResponse
	v.Ack = refs.ACK
	return v, nil
}
func readPrivateProcedureRecord(data []byte) (*nativepb.Procedure, error) {
	p := new(nativepb.Procedure)
	if err := unmarshalNativeRecord(data, storagecodec.NativeProcedure, p); err != nil {
		return nil, err
	}
	if !validNativeProcedureMetadata(p) || p.Native == nil || p.Native.Origin != nil || p.Native.LastResponse != nil || p.Native.Ack != nil || p.Root != "" && p.Native.Family != string(portable.InviteClient) || p.SuccessfulAck != nil && (p.Native.Family != string(portable.InviteClient) || p.Native.Phase != string(portable.Accepted) && p.Native.Phase != string(portable.Terminated)) || p.AckEmissions != 0 && (p.Native.Family != string(portable.InviteClient) || p.Ack == "") || len(p.Events) != 0 && p.Native.Family != string(portable.InviteClient) && p.Native.Family != string(portable.InviteServer) && !(p.Native.Family == string(portable.NonInviteClient) && p.Cleanup) || !nativeMessageField(p.Origin) || !nativeMessageField(p.LastResponse) || !nativeMessageField(p.Ack) {
		return nil, errNativeRecord
	}
	return p, nil
}
func decodeNativeProcedure(data []byte) (nativeProcedureRecord, error) {
	p, err := readPrivateProcedureRecord(data)
	if err != nil {
		return nativeProcedureRecord{}, err
	}
	native := storageDecodePortableTransaction(p.Native)
	refs := nativeProcedureRefs{p.Origin, p.LastResponse, p.Ack}
	p.Native = nil
	p.Origin = ""
	p.LastResponse = ""
	p.Ack = ""
	return nativeProcedureRecord{Native: native, Refs: refs, Metadata: p}, nil
}

type nativeInviteRefs struct{ Request, Final, ACK string }
type nativeDialogRefs struct {
	Local, Remote *nativeInviteRefs
	LocalBye      string
}
type nativeDialogRecord struct {
	Native   portable.Dialog
	Refs     nativeDialogRefs
	Metadata *nativepb.Dialog
}

func stripNativeInvite(value *portable.InviteProcedure, refs *nativeInviteRefs) (*portable.InviteProcedure, *nativepb.InviteRefs, error) {
	if value == nil {
		if refs != nil {
			return nil, nil, errNativeRecord
		}
		return nil, nil, nil
	}
	if refs == nil || !nativeReferenceMatches(value.Request, refs.Request) || !nativeOptionalReferenceMatches(value.Final, refs.Final) || !nativeOptionalReferenceMatches(value.ACK, refs.ACK) {
		return nil, nil, errNativeRecord
	}
	v := *value
	v.Request = portable.Message{}
	v.Final = nil
	v.ACK = nil
	return &v, &nativepb.InviteRefs{Request: refs.Request, FinalResponse: refs.Final, Ack: refs.ACK}, nil
}
func encodeNativeDialog(dialog portable.Dialog, refs nativeDialogRefs, metadata *nativepb.Dialog) ([]byte, error) {
	v, err := buildPrivateDialogRecord(dialog, refs, metadata)
	if err != nil {
		return nil, err
	}
	return marshalNativeRecord(storagecodec.NativeDialog, v)
}
func buildPrivateDialogRecord(dialog portable.Dialog, refs nativeDialogRefs, metadata *nativepb.Dialog) (*nativepb.Dialog, error) {
	if metadata == nil || metadata.Native != nil || metadata.LocalInvite != nil || metadata.RemoteInvite != nil || metadata.LocalBye != "" || !nativeOptionalResultField(metadata.AckResult) || metadata.AckResult != "" && (refs.Local == nil || refs.Local.ACK == "") || metadata.AckEmissions > stateProfileTransmissions || metadata.AckEmissions != 0 && (dialog.LocalInvite == nil || dialog.LocalInvite.ACK == nil || refs.Local == nil || refs.Local.ACK == "") || !nativeOptionalReferenceMatches(dialog.LocalBye, refs.LocalBye) || !validNativeOrderedReferences(metadata.Outputs, stateNativeProcedures*stateTransactionEmissions) {
		return nil, errNativeRecord
	}
	local, localRefs, err := stripNativeInvite(dialog.LocalInvite, refs.Local)
	if err != nil {
		return nil, err
	}
	remote, remoteRefs, err := stripNativeInvite(dialog.RemoteInvite, refs.Remote)
	if err != nil {
		return nil, err
	}
	stripped := dialog
	stripped.LocalInvite = local
	stripped.RemoteInvite = remote
	stripped.LocalBye = nil
	v := proto.Clone(metadata).(*nativepb.Dialog)
	v.Native = storageEncodePortableDialog(stripped)
	if v.Native.LocalInvite != nil {
		v.Native.LocalInvite.Request = nil
	}
	if v.Native.RemoteInvite != nil {
		v.Native.RemoteInvite.Request = nil
	}
	v.LocalInvite = localRefs
	v.RemoteInvite = remoteRefs
	v.LocalBye = refs.LocalBye
	return v, nil
}
func validNativeInviteReferences(nativePresent bool, p *nativepb.InviteRefs) bool {
	if !nativePresent {
		return p == nil
	}
	return p != nil && nativeMessageField(p.Request) && nativeMessageField(p.FinalResponse) && nativeMessageField(p.Ack)
}

// readPrivateDialogRecord validates metadata without restoring a native
// dialog or reading its separately retained SIP messages.
func readPrivateDialogRecord(data []byte) (*nativepb.Dialog, error) {
	p := new(nativepb.Dialog)
	if err := unmarshalNativeRecord(data, storagecodec.NativeDialog, p); err != nil {
		return nil, err
	}
	if p.Native == nil || !nativeOptionalResultField(p.AckResult) || p.AckResult != "" && (p.LocalInvite == nil || p.LocalInvite.Ack == "") || p.AckEmissions > stateProfileTransmissions || p.AckEmissions != 0 && (p.Native.LocalInvite == nil || p.LocalInvite == nil || p.LocalInvite.Ack == "") || p.Native.LocalBye != nil || !nativeMessageField(p.LocalBye) || !validNativeOrderedReferences(p.Outputs, stateNativeProcedures*stateTransactionEmissions) {
		return nil, errNativeRecord
	}
	for _, v := range []bool{p.Native.LocalInvite != nil && (p.Native.LocalInvite.Request != nil || p.Native.LocalInvite.Final != nil || p.Native.LocalInvite.Ack != nil), p.Native.RemoteInvite != nil && (p.Native.RemoteInvite.Request != nil || p.Native.RemoteInvite.Final != nil || p.Native.RemoteInvite.Ack != nil)} {
		if v {
			return nil, errNativeRecord
		}
	}
	if !validNativeInviteReferences(p.Native.LocalInvite != nil, p.LocalInvite) || !validNativeInviteReferences(p.Native.RemoteInvite != nil, p.RemoteInvite) {
		return nil, errNativeRecord
	}
	return p, nil
}

func decodeNativeDialog(data []byte) (nativeDialogRecord, error) {
	p, err := readPrivateDialogRecord(data)
	if err != nil {
		return nativeDialogRecord{}, err
	}
	return nativeDialogFromPrivate(p), nil
}

// nativeDialogFromPrivate converts one already-validated owned record without
// decoding its protobuf a second time. The private record becomes metadata.
func nativeDialogFromPrivate(p *nativepb.Dialog) nativeDialogRecord {
	var local, remote *nativeInviteRefs
	if p.LocalInvite != nil {
		local = &nativeInviteRefs{p.LocalInvite.Request, p.LocalInvite.FinalResponse, p.LocalInvite.Ack}
	}
	if p.RemoteInvite != nil {
		remote = &nativeInviteRefs{p.RemoteInvite.Request, p.RemoteInvite.FinalResponse, p.RemoteInvite.Ack}
	}
	native := storageDecodePortableDialog(p.Native)
	refs := nativeDialogRefs{Local: local, Remote: remote, LocalBye: p.LocalBye}
	p.Native = nil
	p.LocalInvite = nil
	p.RemoteInvite = nil
	p.LocalBye = ""
	return nativeDialogRecord{Native: native, Refs: refs, Metadata: p}
}

// nativeSelectedMessages is operation-local. Same field is decoded once; the
// shared immutable memo only coalesces records with identical provenance.
type nativeSelectedMessages struct {
	records    map[string][]byte
	messages   map[string]portable.Message
	memo       *selectedmessage.Memo
	provenance selectedmessage.Provenance
}

func (s *nativeSelectedMessages) get(field string) (portable.Message, error) {
	if field == "" || !nativeMessageField(field) || s == nil || s.memo == nil || s.provenance == "" {
		return portable.Message{}, errNativeRecord
	}
	if m, ok := s.messages[field]; ok {
		return m, nil
	}
	data, ok := s.records[field]
	if !ok || data == nil {
		return portable.Message{}, errNativeRecord
	}
	m, err := s.memo.Decode(data, s.provenance)
	if err != nil {
		return portable.Message{}, err
	}
	if s.messages == nil {
		s.messages = make(map[string]portable.Message)
	}
	s.messages[field] = m
	return m, nil
}
func (r nativeProcedureRecord) attach(s *nativeSelectedMessages, origin, last, ack bool) (portable.Transaction, error) {
	tx := r.Native
	var err error
	if origin && r.Refs.Origin != "" {
		tx.Origin, err = s.get(r.Refs.Origin)
		if err != nil {
			return portable.Transaction{}, err
		}
	}
	if last && r.Refs.LastResponse != "" {
		m, e := s.get(r.Refs.LastResponse)
		if e != nil {
			return portable.Transaction{}, e
		}
		tx.LastResponse = &m
	}
	if ack && r.Refs.ACK != "" {
		m, e := s.get(r.Refs.ACK)
		if e != nil {
			return portable.Transaction{}, e
		}
		tx.ACK = &m
	}
	return tx, nil
}

type nativeInviteSelection struct{ Request, Final, ACK bool }
type nativeDialogSelection struct {
	Local, Remote nativeInviteSelection
	LocalBye      bool
}

func attachNativeInvite(v *portable.InviteProcedure, refs *nativeInviteRefs, selection nativeInviteSelection, s *nativeSelectedMessages) (*portable.InviteProcedure, error) {
	if v == nil {
		return nil, nil
	}
	if refs == nil {
		return nil, errNativeRecord
	}
	next := *v
	if selection.Request && refs.Request != "" {
		m, err := s.get(refs.Request)
		if err != nil {
			return nil, err
		}
		next.Request = m
	}
	if selection.Final && refs.Final != "" {
		m, err := s.get(refs.Final)
		if err != nil {
			return nil, err
		}
		next.Final = &m
	}
	if selection.ACK && refs.ACK != "" {
		m, err := s.get(refs.ACK)
		if err != nil {
			return nil, err
		}
		next.ACK = &m
	}
	return &next, nil
}
func (r nativeDialogRecord) attach(s *nativeSelectedMessages, selection nativeDialogSelection) (portable.Dialog, error) {
	dialog := r.Native
	var err error
	dialog.LocalInvite, err = attachNativeInvite(dialog.LocalInvite, r.Refs.Local, selection.Local, s)
	if err != nil {
		return portable.Dialog{}, err
	}
	dialog.RemoteInvite, err = attachNativeInvite(dialog.RemoteInvite, r.Refs.Remote, selection.Remote, s)
	if err != nil {
		return portable.Dialog{}, err
	}
	if selection.LocalBye && r.Refs.LocalBye != "" {
		m, e := s.get(r.Refs.LocalBye)
		if e != nil {
			return portable.Dialog{}, e
		}
		dialog.LocalBye = &m
	}
	return dialog, nil
}
func (r nativeProcedureRecord) complete(s *nativeSelectedMessages) (portable.Transaction, error) {
	value, err := r.attach(s, true, true, true)
	if err != nil {
		return portable.Transaction{}, err
	}
	if err = value.Validate(); err != nil {
		return portable.Transaction{}, err
	}
	return value, nil
}
func (r nativeDialogRecord) complete(s *nativeSelectedMessages) (portable.Dialog, error) {
	all := nativeInviteSelection{Request: true, Final: true, ACK: true}
	value, err := r.attach(s, nativeDialogSelection{Local: all, Remote: all, LocalBye: true})
	if err != nil {
		return portable.Dialog{}, err
	}
	if err = value.Validate(); err != nil {
		return portable.Dialog{}, err
	}
	return value, nil
}
func validNativePublicSelectors(selectors []string) bool {
	if len(selectors) > storagecodec.MaxNativePublicSelectors {
		return false
	}
	for _, selector := range selectors {
		if !validSendText(selector, storagecodec.MaxNativePublicSelectorBytes, false) {
			return false
		}
	}
	return true
}
func validNativeCallAssociations(v *nativepb.Context) bool {
	if v.CallRetireAtMs < 0 || v.CallRetireAtMs > 1<<53-1 || len(v.InitialProcedures) > 4 || len(v.Dialogs) > 16 || len(v.InitialProcedures)+len(v.NoninitialProcedures) > stateNativeProcedures || !validNativeOrderedReferences(v.NoninitialProcedures, stateNativeProcedures) {
		return false
	}
	for i, ref := range v.InitialProcedures {
		if ref == "" || !nativeMessageField(ref) {
			return false
		}
		for _, prior := range v.InitialProcedures[:i] {
			if prior == ref {
				return false
			}
		}
	}
	for i, ref := range v.Dialogs {
		if ref == "" || !nativeMessageField(ref) {
			return false
		}
		for _, prior := range v.InitialProcedures {
			if prior == ref {
				return false
			}
		}
		for _, prior := range v.Dialogs[:i] {
			if prior == ref {
				return false
			}
		}
	}
	for _, ref := range v.NoninitialProcedures {
		for _, initial := range v.InitialProcedures {
			if ref == initial {
				return false
			}
		}
		for _, dialog := range v.Dialogs {
			if ref == dialog {
				return false
			}
		}
	}
	return true
}
func encodeNativeContext(v *nativepb.Context) ([]byte, error) {
	if v == nil || v.Context == nil || !validNativePublicSelectors(v.Selectors) || !validNativeCallAssociations(v) {
		return nil, errNativeRecord
	}
	return marshalNativeRecord(storagecodec.NativeContext, v)
}
func decodeNativeContext(data []byte) (*nativepb.Context, error) {
	v := new(nativepb.Context)
	if err := unmarshalNativeRecord(data, storagecodec.NativeContext, v); err != nil {
		return nil, err
	}
	if v.Context == nil || !validNativePublicSelectors(v.Selectors) || !validNativeCallAssociations(v) {
		return nil, errNativeRecord
	}
	return v, nil
}

type nativeSubscriptionRecord struct {
	Native     stateSubscription
	BaseCredit uint64
}

func encodeNativeSubscription(v nativeSubscriptionRecord) ([]byte, error) {
	if v.BaseCredit > 1<<53-1 {
		return nil, errNativeRecord
	}
	return marshalNativeRecord(storagecodec.NativeSubscription, &nativepb.Subscription{Native: storageEncodeEngineStateSubscription(v.Native), BaseCredit: v.BaseCredit})
}
func decodeNativeSubscription(data []byte) (nativeSubscriptionRecord, error) {
	v := new(nativepb.Subscription)
	if err := unmarshalNativeRecord(data, storagecodec.NativeSubscription, v); err != nil {
		return nativeSubscriptionRecord{}, err
	}
	if v.Native == nil || v.BaseCredit > 1<<53-1 {
		return nativeSubscriptionRecord{}, errNativeRecord
	}
	return nativeSubscriptionRecord{Native: storageDecodeEngineStateSubscription(v.Native), BaseCredit: v.BaseCredit}, nil
}
func validNativeResult(v *nativepb.Result) bool {
	if v == nil || v.Certainty == pb.OperationResult_UNSPECIFIED || v.Sequence == 0 || v.Sequence > 1<<53-1 || !validSendText(v.OperationId, 160, false) || len(v.Fingerprint) != 32 || !state.ValidReference(v.DecidingLeaseTerm) || v.ServerRequestId != "" && !state.ValidReference(v.ServerRequestId) || !nativeMessageField(v.Output) {
		return false
	}
	if v.Pending && (v.Certainty != pb.OperationResult_UNKNOWN || v.SipStatus != 0 || v.Error != "" || v.Request != "" || v.Response != "" || v.ServerRequestId != "" || v.RetirementProof != nil) {
		return false
	}
	if v.Certainty == pb.OperationResult_NOT_ACCEPTED && (v.Pending || v.SipStatus != 0 || v.Output != "" || v.Request != "" || v.Response != "" || v.RetirementProof != nil) {
		return false
	}
	_, ok := pb.OperationResult_Certainty_name[int32(v.Certainty)]
	return ok
}
func encodeNativeResult(v *nativepb.Result) ([]byte, error) {
	if !validNativeResult(v) || v.CutoffMs <= 0 || !nativeMessageField(v.Request) || !nativeMessageField(v.Response) {
		return nil, errNativeRecord
	}
	return marshalNativeRecord(storagecodec.NativeResult, v)
}
func decodeNativeResult(data []byte) (*nativepb.Result, error) {
	v := new(nativepb.Result)
	if err := unmarshalNativeRecord(data, storagecodec.NativeResult, v); err != nil {
		return nil, err
	}
	if !validNativeResult(v) || v.CutoffMs <= 0 || !nativeMessageField(v.Request) || !nativeMessageField(v.Response) {
		return nil, errNativeRecord
	}
	return v, nil
}
func validNativeLifecycle(v *nativepb.Lifecycle) bool {
	if v == nil || v.Kind == pb.DialogEvent_UNSPECIFIED || !validNativeOrderedReferences(v.Owners, 3) || !nativeMessageField(v.Root) || v.Message != "" && v.Root == "" || v.Message == "" && (len(v.Owners) != 0 || v.Root != "" && v.Kind != pb.DialogEvent_CANCELLED) || v.Root != "" && v.Root == v.Message {
		return false
	}
	for _, owner := range v.Owners {
		if owner == v.Message {
			return false
		}
	}
	_, ok := pb.DialogEvent_Kind_name[int32(v.Kind)]
	return ok
}
func encodeNativeLifecycle(v *nativepb.Lifecycle) ([]byte, error) {
	if !validNativeLifecycle(v) || v.Snapshot == nil || v.DeadlineMs <= 0 || !nativeMessageField(v.Message) {
		return nil, errNativeRecord
	}
	return marshalNativeRecord(storagecodec.NativeLifecycle, v)
}
func decodeNativeLifecycle(data []byte) (*nativepb.Lifecycle, error) {
	v := new(nativepb.Lifecycle)
	if err := unmarshalNativeRecord(data, storagecodec.NativeLifecycle, v); err != nil {
		return nil, err
	}
	if !validNativeLifecycle(v) || v.Snapshot == nil || v.DeadlineMs <= 0 || !nativeMessageField(v.Message) {
		return nil, errNativeRecord
	}
	return v, nil
}

// nativeProjectSnapshot joins only captured current continuity facts for result
// delivery. Lifecycle snapshots are frozen independently when the event is made.
func nativeProjectSnapshot(c *nativepb.Context, dialog *portable.Dialog, subscription *stateSubscription, selectors []string) (*pb.ContextSnapshot, error) {
	if c == nil || c.Context == nil {
		return nil, errNativeRecord
	}
	out := &pb.ContextSnapshot{Context: proto.Clone(c.Context).(*pb.ContextRef), BindingId: c.BindingId, PrincipalId: c.PrincipalId, Kind: c.Kind,
		CallId: c.CallId, LocalTag: c.LocalTag, RemoteTag: c.RemoteTag, LocalUri: c.LocalUri, RemoteUri: c.RemoteUri, RemoteTarget: c.RemoteTarget,
		LocalSdp: bytes.Clone(c.LocalSdp), RemoteSdp: bytes.Clone(c.RemoteSdp), Answered: c.Answered, InitialAck: c.InitialAck, Ended: c.Ended,
		Revision: c.Revision, CommandSequence: c.CommandSequence, Transport: c.Transport,
		DirectoryEpoch: c.DirectoryEpoch, ApplicationLeaseTerm: c.ApplicationLeaseTerm, AttachmentGeneration: c.AttachmentGeneration}
	if selectors == nil {
		selectors = c.Selectors
	}
	if selectors != nil {
		out.Selectors = append(make([]string, 0, len(selectors)), selectors...)
	}
	if c.Application != nil {
		out.Application = proto.Clone(c.Application).(*pb.ProcessRef)
	}
	if c.Executor != nil {
		out.Executor = proto.Clone(c.Executor).(*pb.ProcessRef)
	}
	if c.Delivery != nil {
		out.Delivery = proto.Clone(c.Delivery).(*pb.ContextDelivery)
	}
	if dialog != nil {
		out.CallId = dialog.CallID
		out.LocalTag = dialog.LocalTag
		out.RemoteTag = dialog.RemoteTag
		out.LocalUri = dialog.LocalURI
		out.RemoteUri = dialog.RemoteURI
		out.RemoteTarget = dialog.Target
		out.Transport = dialog.Transport
	}
	if subscription != nil {
		out.RemoteTarget = subscription.Target
	}
	if out.CallId != "" && out.LocalTag != "" && out.RemoteTag != "" {
		out.DialogId = sip.DialogIDMake(out.CallId, out.LocalTag, out.RemoteTag)
	}
	return out, nil
}
