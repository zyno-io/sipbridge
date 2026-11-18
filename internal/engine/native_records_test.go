package engine

import (
	"bytes"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/emiago/sipgo/sip/portable"
	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/nativepb"
	"sipbridge/internal/selectedmessage"
	"sipbridge/internal/storagecodec"
)

func nativeTestMessages(t testing.TB, values map[string]portable.Message) *nativeSelectedMessages {
	t.Helper()
	raw := make(map[string][]byte, len(values))
	for field, m := range values {
		data, err := selectedmessage.Marshal(m)
		if err != nil {
			t.Fatal(err)
		}
		raw[field] = data
	}
	memo, err := selectedmessage.NewMemo(16)
	if err != nil {
		t.Fatal(err)
	}
	return &nativeSelectedMessages{records: raw, memo: memo, provenance: "admitted"}
}
func TestNativeRecordsRegisterAndSubscription(t *testing.T) {
	request := stateTestRequest("REGISTER", 1, "", "records-register")
	m, err := portable.ParseMessage(request.Wire, "tls", "", "")
	if err != nil {
		t.Fatal(err)
	}
	tx, _, err := nativeServerStart(m, "local-tag", 1000)
	if err != nil {
		t.Fatal(err)
	}
	selected, err := nativeServerBuildResponse(tx, 200, "OK", nil, "local-tag", "sip:edge@example.test;transport=tls", nil, 1001)
	if err != nil {
		t.Fatal(err)
	}
	refs := nativeProcedureRefs{Origin: "r:register", LastResponse: "r:response"}
	data, err := encodeNativeProcedure(selected.Transaction, refs, &nativepb.Procedure{RequestId: "request", DeadlineMs: 5000})
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(data, m.WireBytes()) || bytes.Contains(data, selected.Message.WireBytes()) {
		t.Fatal("procedure duplicated exact wire")
	}
	record, err := decodeNativeProcedure(data)
	if err != nil {
		t.Fatal(err)
	}
	source := nativeTestMessages(t, map[string]portable.Message{refs.Origin: m, refs.LastResponse: selected.Message})
	partial, err := record.attach(source, true, false, false)
	if err != nil || partial.LastResponse != nil || len(source.messages) != 1 {
		t.Fatal("unselected response loaded", err)
	}
	if record.Metadata.Native != nil || record.Metadata.Origin != "" || record.Metadata.LastResponse != "" {
		t.Fatal("decoded metadata retained native graph")
	}
	again, err := encodeNativeProcedure(selected.Transaction, record.Refs, record.Metadata)
	if err != nil || !bytes.Equal(data, again) {
		t.Fatal("procedure metadata round trip", err)
	}
	restored, err := record.complete(source)
	if err != nil {
		t.Fatal(err)
	}
	if restored.Key != tx.Key || restored.LastResponse == nil || !restored.LastResponse.EqualWire(selected.Message) || len(source.messages) != 2 {
		t.Fatal("REGISTER transaction changed")
	}
	originalWire := m.WireBytes()
	source.records[refs.Origin][len(source.records[refs.Origin])-1] ^= 1
	if !restored.Origin.EqualWireBytes(originalWire) {
		t.Fatal("restored holder retained record buffer")
	}

	e, c, subTx := residentSubscriptionFixture(t)
	now := time.Unix(1700000000, 0)
	sub, snapshot, response, err := e.prepareResidentSubscription(c, subTx, &pb.Respond{Status: 202}, now)
	if err != nil {
		t.Fatal(err)
	}
	next, notify, effects, err := e.prepareResidentNotify(&residentContext{subscription: sub, snapshot: snapshot, flow: c.flow}, &pb.DialogRequest{Method: "NOTIFY"}, "operation", now)
	if err != nil || len(effects) == 0 {
		t.Fatal("NOTIFY candidate", err)
	}
	subData, err := encodeNativeSubscription(nativeSubscriptionRecord{Native: *next, BaseCredit: 123})
	if err != nil {
		t.Fatal(err)
	}
	round, err := decodeNativeSubscription(subData)
	if err != nil || !reflect.DeepEqual(round.Native, *next) || round.BaseCredit != 123 {
		t.Fatal("subscription changed", err)
	}
	notifyData, err := encodeNativeProcedure(notify.native, nativeProcedureRefs{Origin: "r:notify"}, &nativepb.Procedure{Flow: c.flow})
	if err != nil {
		t.Fatal(err)
	}
	notifyRecord, err := decodeNativeProcedure(notifyData)
	if err != nil {
		t.Fatal(err)
	}
	messages := nativeTestMessages(t, map[string]portable.Message{"r:notify": notify.native.Origin, "r:sub-response": response})
	notifyTx, err := notifyRecord.complete(messages)
	if err != nil || !notifyTx.Origin.EqualWire(notify.native.Origin) || len(messages.messages) != 1 {
		t.Fatal("NOTIFY restore", err)
	}
}
func TestNativeDialogRecordConfirmedAndReferences(t *testing.T) {
	raw := stateTestRequest("INVITE", 1, "", "records-invite")
	invite, err := portable.ParseMessage(raw.Wire, "tls", "", "")
	if err != nil {
		t.Fatal(err)
	}
	final, err := portable.BuildResponseWithHeaders(invite, 200, "OK", nil, "remote", "sip:remote@example.test;transport=tls", nil)
	if err != nil {
		t.Fatal(err)
	}
	d, err := portable.NewDialog(portable.UAC, invite, final, portable.DefaultTimers())
	if err != nil {
		t.Fatal(err)
	}
	d, _, err = portable.StepDialog(d, portable.DialogInput{Kind: portable.DialogLocalACK, Now: 1000, Branch: "z9hG4bK-record-ack"})
	if err != nil || d.Phase != portable.DialogConfirmed {
		t.Fatal("confirmed dialog", err)
	}
	if d.LocalInvite == nil || d.LocalInvite.Final == nil || d.LocalInvite.ACK == nil {
		t.Fatal("missing outstanding INVITE evidence")
	}
	original := *d.LocalInvite
	refs := nativeDialogRefs{Local: &nativeInviteRefs{Request: "r:invite", Final: "r:final", ACK: "r:ack"}}
	data, err := encodeNativeDialog(d, refs, &nativepb.Dialog{Transaction: "tx"})
	if err != nil {
		t.Fatal(err)
	}
	if !d.LocalInvite.Request.EqualWire(original.Request) || d.LocalInvite.Final != original.Final || d.LocalInvite.ACK != original.ACK {
		t.Fatal("encoding mutated source dialog")
	}
	for _, wire := range [][]byte{invite.WireBytes(), final.WireBytes(), d.LocalInvite.ACK.WireBytes()} {
		if bytes.Contains(data, wire) {
			t.Fatal("dialog duplicated exact wire")
		}
	}
	private, err := readPrivateDialogRecord(data)
	if err != nil || private.Native == nil || private.LocalInvite == nil || private.LocalInvite.Request != refs.Local.Request || private.LocalInvite.FinalResponse != refs.Local.Final || private.LocalInvite.Ack != refs.Local.ACK {
		t.Fatal("metadata-only read lost native facts or selected message references", err)
	}
	privateData, err := marshalNativeRecord(storagecodec.NativeDialog, private)
	if err != nil || !bytes.Equal(data, privateData) {
		t.Fatal("metadata-only dialog round trip changed selected facts", err)
	}
	record, err := decodeNativeDialog(data)
	if err != nil {
		t.Fatal(err)
	}
	source := nativeTestMessages(t, map[string]portable.Message{"r:invite": invite, "r:final": final, "r:ack": *d.LocalInvite.ACK})
	partial, err := record.attach(source, nativeDialogSelection{Local: nativeInviteSelection{Request: true}})
	if err != nil || partial.LocalInvite.Final != nil || len(source.messages) != 1 {
		t.Fatal("unselected dialog evidence loaded", err)
	}
	if record.Metadata.Native != nil || record.Metadata.LocalInvite != nil {
		t.Fatal("decoded dialog metadata retained graph")
	}
	again, err := encodeNativeDialog(d, record.Refs, record.Metadata)
	if err != nil || !bytes.Equal(data, again) {
		t.Fatal("dialog metadata round trip", err)
	}
	restored, err := record.complete(source)
	if err != nil || restored.Phase != portable.DialogConfirmed || !restored.LocalInvite.ACK.EqualWire(*d.LocalInvite.ACK) {
		t.Fatal("confirmed dialog or ACK lost", err)
	}
}
func TestNativeRecordsContextResultLifecycleAndStrictKind(t *testing.T) {
	snapshot := &pb.ContextSnapshot{Context: &pb.ContextRef{Id: "context"}, CallId: "call", LocalSdp: []byte("historical")}
	contextData, err := encodeNativeContext(&nativepb.Context{Context: snapshot.Context, CallId: "call", LocalTag: "local", RemoteTag: "remote", LocalSdp: []byte("live"), CommandSequence: 4, Revision: 6})
	if err != nil {
		t.Fatal(err)
	}
	context, err := decodeNativeContext(contextData)
	if err != nil || context.CommandSequence != 4 {
		t.Fatal("context", err)
	}
	joined, err := nativeProjectSnapshot(context, nil, &stateSubscription{Target: "sip:phone@example.test"}, nil)
	if err != nil || joined.CallId != "call" || joined.DialogId == "" || joined.RemoteTarget != "sip:phone@example.test" || string(joined.LocalSdp) != "live" {
		t.Fatal("live snapshot projection", err)
	}
	joined.LocalSdp[0] = 'X'
	if string(context.LocalSdp) != "live" {
		t.Fatal("snapshot aliases context")
	}
	resultData, err := encodeNativeResult(&nativepb.Result{Certainty: pb.OperationResult_ACCEPTED, Request: "r:request", CutoffMs: 10, Sequence: 4, OperationId: "operation", Fingerprint: bytes.Repeat([]byte{7}, 32), DecidingLeaseTerm: "term", Output: "r:output"})
	if err != nil {
		t.Fatal(err)
	}
	snapshot.LocalSdp[0] = 'X'
	result, err := decodeNativeResult(resultData)
	if err != nil || result.Output != "r:output" || result.Sequence != 4 || result.OperationId != "operation" || !bytes.Equal(result.Fingerprint, bytes.Repeat([]byte{7}, 32)) {
		t.Fatal("historical result identity changed", err)
	}
	eventData, err := encodeNativeLifecycle(&nativepb.Lifecycle{Kind: pb.DialogEvent_ANSWERED, Snapshot: snapshot, Message: "r:message", Root: "r:procedure", Owners: []string{"r:procedure"}, DeadlineMs: 10})
	if err != nil {
		t.Fatal(err)
	}
	event, err := decodeNativeLifecycle(eventData)
	if err != nil || event.Message != "r:message" {
		t.Fatal("lifecycle", err)
	}
	if _, err = decodeNativeContext(eventData); err == nil {
		t.Fatal("wrong frame kind accepted")
	}
	p := &nativepb.Procedure{Native: storageEncodePortableTransaction(portable.Transaction{}), Origin: "r:one"}
	bad, err := storagecodec.Marshal(storagecodec.NativeProcedure, p)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = decodeNativeProcedure(bad); err == nil {
		t.Fatal("inline message accepted")
	}
	if _, err = encodeNativeResult(&nativepb.Result{CutoffMs: 0}); err == nil {
		t.Fatal("unbounded result accepted")
	}
	invalidResult := proto.Clone(result).(*nativepb.Result)
	invalidResult.Fingerprint = invalidResult.Fingerprint[:31]
	if _, err = encodeNativeResult(invalidResult); err == nil {
		t.Fatal("truncated command fingerprint accepted")
	}
	invalidResult = proto.Clone(result).(*nativepb.Result)
	invalidResult.Output = "not-a-record"
	if _, err = encodeNativeResult(invalidResult); err == nil {
		t.Fatal("invalid output backlink encoded")
	}
	invalidData, err := storagecodec.Marshal(storagecodec.NativeResult, invalidResult)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = decodeNativeResult(invalidData); err == nil {
		t.Fatal("invalid stored output backlink decoded")
	}
	if !proto.Equal(event.Snapshot.Context, context.Context) {
		t.Fatal("event context identity changed")
	}
}

var nativeRecordBenchBytes []byte
var nativeRecordBenchTx portable.Transaction

func BenchmarkNativeProcedureRecord(b *testing.B) {
	request := stateTestRequest("REGISTER", 1, "", "record-bench")
	message, err := portable.ParseMessage(request.Wire, "tls", "", "")
	if err != nil {
		b.Fatal(err)
	}
	tx, _, err := nativeServerStart(message, "local-tag", 1000)
	if err != nil {
		b.Fatal(err)
	}
	response, err := nativeServerBuildResponse(tx, 200, "OK", nil, "local-tag", "sip:edge@example.test;transport=tls", nil, 1001)
	if err != nil {
		b.Fatal(err)
	}
	refs := nativeProcedureRefs{Origin: "r:register", LastResponse: "r:response"}
	meta := &nativepb.Procedure{RequestId: "request", DeadlineMs: 5000}
	raw, err := encodeNativeProcedure(response.Transaction, refs, meta)
	if err != nil {
		b.Fatal(err)
	}
	source := nativeTestMessages(b, map[string]portable.Message{refs.Origin: message, refs.LastResponse: response.Message})
	b.Logf("procedure-record=%d origin-record=%d response-record=%d", len(raw), len(source.records[refs.Origin]), len(source.records[refs.LastResponse]))
	b.Run("EncodeMetadata", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			v, e := encodeNativeProcedure(response.Transaction, refs, meta)
			if e != nil {
				b.Fatal(e)
			}
			nativeRecordBenchBytes = v
		}
	})
	b.Run("DecodeMetadata", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			v, e := decodeNativeProcedure(raw)
			if e != nil {
				b.Fatal(e)
			}
			nativeRecordBenchTx = v.Native
		}
	})
}

func TestNativeRecordFramedBoundary(t *testing.T) {
	value := &nativepb.Context{Context: &pb.ContextRef{Id: "context"}}
	// Exactly fill the complete framed limit, including protobuf tag/length and
	// storage frame, then prove the next payload byte is rejected on encode.
	base, err := marshalNativeRecord(storagecodec.NativeContext, value)
	if err != nil {
		t.Fatal(err)
	}
	value.LocalSdp = make([]byte, nativeRecordLimit-len(base)-5)
	data, err := marshalNativeRecord(storagecodec.NativeContext, value)
	if err != nil {
		t.Fatal(err)
	}
	if len(data) != nativeRecordLimit {
		t.Fatalf("unexpected boundary length %d", len(data))
	}
	if _, err = decodeNativeContext(data); err != nil {
		t.Fatal(err)
	}
	value.LocalSdp = append(value.LocalSdp, 0)
	if _, err = marshalNativeRecord(storagecodec.NativeContext, value); err == nil {
		t.Fatal("oversize complete frame accepted")
	}
}

func TestNativeContextCallAssociationsBounds(t *testing.T) {
	value := &nativepb.Context{Context: &pb.ContextRef{Id: "context"}, CallRetireAtMs: 123, InitialProcedures: []string{"r:initial", "r:retry"}, Dialogs: []string{"r:dialog"}, NoninitialProcedures: []string{"r:info", "r:reinvite"}}
	data, err := encodeNativeContext(value)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := decodeNativeContext(data)
	if err != nil || !proto.Equal(decoded, value) {
		t.Fatalf("call associations changed: %v", err)
	}
	cases := []*nativepb.Context{
		{Context: value.Context, InitialProcedures: []string{"r:a", "r:b", "r:c", "r:d", "r:e"}},
		{Context: value.Context, Dialogs: make([]string, 17)},
		{Context: value.Context, InitialProcedures: []string{"r:a"}, Dialogs: []string{"r:a"}},
		{Context: value.Context, Dialogs: []string{"not-a-record"}},
		{Context: value.Context, CallRetireAtMs: 1 << 53},
		{Context: value.Context, InitialProcedures: []string{"r:initial"}, NoninitialProcedures: []string{"r:initial"}},
		{Context: value.Context, Dialogs: []string{"r:dialog"}, NoninitialProcedures: []string{"r:dialog"}},
		{Context: value.Context, NoninitialProcedures: []string{"r:z", "r:a"}},
		{Context: value.Context, NoninitialProcedures: []string{"r:a", "r:a"}},
		{Context: value.Context, NoninitialProcedures: []string{""}},
		{Context: value.Context, NoninitialProcedures: []string{"not-a-record"}},
	}
	for i, bad := range cases {
		if _, err := encodeNativeContext(bad); err == nil {
			t.Fatalf("invalid call associations %d accepted", i)
		}
		raw, err := storagecodec.Marshal(storagecodec.NativeContext, bad)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = decodeNativeContext(raw); err == nil {
			t.Fatalf("invalid stored call associations %d accepted", i)
		}
	}
	maximum := proto.Clone(value).(*nativepb.Context)
	maximum.NoninitialProcedures = make([]string, stateNativeProcedures-len(maximum.InitialProcedures))
	for i := range maximum.NoninitialProcedures {
		maximum.NoninitialProcedures[i] = fmt.Sprintf("r:mid-%03d", i)
	}
	data, err = encodeNativeContext(maximum)
	if err != nil {
		t.Fatal("actual native-procedure boundary refused", err)
	}
	decoded, err = decodeNativeContext(data)
	if err != nil || !proto.Equal(decoded, maximum) {
		t.Fatal("actual native-procedure boundary changed", err)
	}
	maximum.NoninitialProcedures = append(maximum.NoninitialProcedures, "r:mid-overflow")
	if _, err = encodeNativeContext(maximum); err == nil {
		t.Fatal("combined initial/noninitial procedure limit bypassed")
	}
	overflow, err := storagecodec.Marshal(storagecodec.NativeContext, maximum)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = decodeNativeContext(overflow); err == nil {
		t.Fatal("stored combined initial/noninitial procedure limit bypassed")
	}
}

func TestNativeOutputOwnershipReferences(t *testing.T) {
	request := stateTestRequest("INVITE", 1, "", "output-ownership")
	message, err := portable.ParseMessage(request.Wire, "tls", "", "")
	if err != nil {
		t.Fatal(err)
	}
	tx, _, err := nativeServerStart(message, "local-tag", 1000)
	if err != nil {
		t.Fatal(err)
	}
	dialog := nativeEstablishedDialogFixture(t)
	for _, test := range []struct {
		name  string
		kind  byte
		limit int
		build func([]string) ([]byte, error)
		read  func([]byte) ([]string, error)
	}{
		{"procedure", storagecodec.NativeProcedure, stateTransactionEmissions,
			func(refs []string) ([]byte, error) {
				return encodeNativeProcedure(tx, nativeProcedureRefs{Origin: "r:origin"}, &nativepb.Procedure{Outputs: refs})
			},
			func(data []byte) ([]string, error) {
				value, e := readPrivateProcedureRecord(data)
				if e != nil {
					return nil, e
				}
				return value.Outputs, nil
			}},
		{"dialog", storagecodec.NativeDialog, stateNativeProcedures * stateTransactionEmissions,
			func(refs []string) ([]byte, error) {
				return encodeNativeDialog(dialog, nativeDialogRefs{Local: &nativeInviteRefs{Request: "r:invite", Final: "r:final", ACK: "r:ack"}}, &nativepb.Dialog{Outputs: refs})
			},
			func(data []byte) ([]string, error) {
				value, e := decodeNativeDialog(data)
				if e != nil {
					return nil, e
				}
				return value.Metadata.Outputs, nil
			}},
	} {
		t.Run(test.name, func(t *testing.T) {
			refs := make([]string, test.limit)
			for i := range refs {
				refs[i] = fmt.Sprintf("r:output-%04d", i)
			}
			data, e := test.build(refs)
			if e != nil {
				t.Fatal(e)
			}
			decoded, e := test.read(data)
			if e != nil || !reflect.DeepEqual(decoded, refs) {
				t.Fatal("exact output ownership changed", e)
			}
			base, e := test.build(nil)
			if e != nil {
				t.Fatal(e)
			}
			if decoded, e := test.read(base); e != nil || len(decoded) != 0 {
				t.Fatal("quiet owner retained output references", e)
			}
			for i, bad := range [][]string{{""}, {"output"}, {"r:z", "r:a"}, {"r:a", "r:a"}, append(refs, "r:output-overflow")} {
				if _, e = test.build(bad); e == nil {
					t.Fatalf("invalid output ownership %d encoded", i)
				}
				var value proto.Message
				if test.kind == storagecodec.NativeProcedure {
					procedure := new(nativepb.Procedure)
					if e = storagecodec.Unmarshal(base, test.kind, procedure); e != nil {
						t.Fatal(e)
					}
					procedure.Outputs = bad
					value = procedure
				} else {
					dialog := new(nativepb.Dialog)
					if e = storagecodec.Unmarshal(base, test.kind, dialog); e != nil {
						t.Fatal(e)
					}
					dialog.Outputs = bad
					value = dialog
				}
				raw, e := storagecodec.Marshal(test.kind, value)
				if e != nil {
					t.Fatal(e)
				}
				if _, e = test.read(raw); e == nil {
					t.Fatalf("invalid stored output ownership %d decoded", i)
				}
			}
		})
	}
}

func TestNativeRefusalHasNoPhysicalObligations(t *testing.T) {
	refusal := &nativepb.Result{Certainty: pb.OperationResult_NOT_ACCEPTED, Error: "declined", CutoffMs: 1000, Sequence: 1, OperationId: "refused", Fingerprint: bytes.Repeat([]byte{7}, 32), DecidingLeaseTerm: "term", ServerRequestId: "request"}
	data, err := encodeNativeResult(refusal)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := decodeNativeResult(data)
	if err != nil || !proto.Equal(decoded, refusal) {
		t.Fatal("bounded refusal identity changed", err)
	}
	for i, mutate := range []func(*nativepb.Result){
		func(v *nativepb.Result) { v.Pending = true },
		func(v *nativepb.Result) { v.SipStatus = 200 },
		func(v *nativepb.Result) { v.Output = "r:output" },
		func(v *nativepb.Result) { v.Request = "r:request" },
		func(v *nativepb.Result) { v.Response = "r:response" },
		func(v *nativepb.Result) { v.RetirementProof = &pb.ContextRetirementProof{} },
	} {
		bad := proto.Clone(refusal).(*nativepb.Result)
		mutate(bad)
		if _, err = encodeNativeResult(bad); err == nil {
			t.Fatalf("refusal with physical obligation %d encoded", i)
		}
		encoded, err := storagecodec.Marshal(storagecodec.NativeResult, bad)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = decodeNativeResult(encoded); err == nil {
			t.Fatalf("stored refusal with physical obligation %d decoded", i)
		}
	}
}

func TestNativePendingResultRequiresOriginalCutoff(t *testing.T) {
	pending := &nativepb.Result{Certainty: pb.OperationResult_UNKNOWN, Pending: true, CutoffMs: 100, Sequence: 1, OperationId: "pending", Fingerprint: bytes.Repeat([]byte{7}, 32), DecidingLeaseTerm: "term", Output: "r:output"}
	encoded, err := encodeNativeResult(pending)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := decodeNativeResult(encoded)
	if err != nil || decoded.CutoffMs != 100 {
		t.Fatal("original cutoff lost", err)
	}
	pending.CutoffMs = 0
	if _, err = encodeNativeResult(pending); err == nil {
		t.Fatal("missing original cutoff accepted")
	}
}

func TestNativeLifecycleOwnershipBounds(t *testing.T) {
	message, err := portable.ParseMessage(stateTestRequest("INVITE", 1, "", "event-root").Wire, "tls", "", "")
	if err != nil {
		t.Fatal(err)
	}
	tx, err := portable.NewTransaction(portable.InviteClient, message, portable.DefaultTimers(), "")
	if err != nil {
		t.Fatal(err)
	}
	refs := nativeProcedureRefs{Origin: "r:origin"}
	metadata := &nativepb.Procedure{RequestId: "original", Events: []string{"r:event-a", "r:event-b", "r:event-c"}}
	data, err := encodeNativeProcedure(tx, refs, metadata)
	if err != nil {
		t.Fatal(err)
	}
	procedure, err := decodeNativeProcedure(data)
	if err != nil || !reflect.DeepEqual(procedure.Metadata.Events, metadata.Events) {
		t.Fatal("canonical anchor event references changed", err)
	}
	for i, bad := range []*nativepb.Procedure{
		{Events: []string{"r:a", "r:b", "r:c", "r:d"}},
		{Events: []string{"r:b", "r:a"}},
		{Events: []string{"r:a", "r:a"}},
		{Root: "not-a-record"},
		{RequestId: "self", Root: "r:procedure-self"},
		{Root: "r:original", Events: []string{"r:event"}},
	} {
		if _, err = encodeNativeProcedure(tx, refs, bad); err == nil {
			t.Fatalf("invalid anchor ownership %d encoded", i)
		}
	}
	event := &nativepb.Lifecycle{Kind: pb.DialogEvent_ANSWERED, Snapshot: &pb.ContextSnapshot{Context: &pb.ContextRef{Id: "context"}, Kind: pb.ContextSnapshot_UAC},
		Message: "r:message", Root: "r:procedure", Owners: []string{"r:dialog", "r:procedure", "r:result"}, DeadlineMs: 100}
	data, err = encodeNativeLifecycle(event)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := decodeNativeLifecycle(data)
	if err != nil || !proto.Equal(decoded, event) {
		t.Fatal("shared response ownership changed", err)
	}
	sole := proto.Clone(event).(*nativepb.Lifecycle)
	sole.Owners = nil
	if _, err = encodeNativeLifecycle(sole); err != nil {
		t.Fatal("sole lifecycle wire owner was refused", err)
	}
	for i, owners := range [][]string{{"r:a", "r:b", "r:c", "r:d"}, {"r:b", "r:a"}, {"r:a", "r:a"}, {"r:message"}, {""}} {
		bad := proto.Clone(event).(*nativepb.Lifecycle)
		bad.Owners = owners
		if _, err = encodeNativeLifecycle(bad); err == nil {
			t.Fatalf("invalid lifecycle owners %d encoded", i)
		}
		raw, err := storagecodec.Marshal(storagecodec.NativeLifecycle, bad)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = decodeNativeLifecycle(raw); err == nil {
			t.Fatalf("invalid stored lifecycle owners %d decoded", i)
		}
	}
	event.Message = ""
	if _, err = encodeNativeLifecycle(event); err == nil {
		t.Fatal("snapshot-only event retained wire-owner metadata")
	}
	sole.Root = ""
	if _, err = encodeNativeLifecycle(sole); err == nil {
		t.Fatal("message event lost its canonical anchor")
	}
}

func TestNativeACKEmissionCounters(t *testing.T) {
	dialog := nativeEstablishedDialogFixture(t)
	origin := dialog.LocalInvite.Request
	tx, err := portable.NewTransaction(portable.InviteClient, origin, portable.DefaultTimers(), "")
	if err != nil {
		t.Fatal(err)
	}
	tx, _, err = portable.StepTransaction(tx, portable.Input{Kind: portable.Start, Now: 1000})
	if err != nil {
		t.Fatal(err)
	}
	rejected, err := portable.BuildResponseWithHeaders(origin, 486, "Busy", nil, "remote-tag", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	tx, _, err = portable.StepTransaction(tx, portable.Input{Kind: portable.ReceiveResponse, Now: 1100, Message: rejected})
	if err != nil {
		t.Fatal(err)
	}
	refs := nativeProcedureRefs{Origin: "r:origin", LastResponse: "r:failure", ACK: "r:ack"}
	data, err := encodeNativeProcedure(tx, refs, &nativepb.Procedure{AckEmissions: stateProfileTransmissions})
	if err != nil {
		t.Fatal(err)
	}
	proc, err := readPrivateProcedureRecord(data)
	if err != nil || proc.AckEmissions != stateProfileTransmissions {
		t.Fatal("procedure count lost", err)
	}
	if _, err = encodeNativeProcedure(tx, refs, &nativepb.Procedure{AckEmissions: stateProfileTransmissions + 1}); err == nil {
		t.Fatal("excess procedure ACKs admitted")
	}
	proc.AckEmissions = stateProfileTransmissions + 1
	invalid, err := marshalNativeRecord(storagecodec.NativeProcedure, proc)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = readPrivateProcedureRecord(invalid); err == nil {
		t.Fatal("decoded excess procedure ACKs")
	}
	missing := tx
	missing.ACK = nil
	if _, err = encodeNativeProcedure(missing, nativeProcedureRefs{Origin: refs.Origin, LastResponse: refs.LastResponse}, &nativepb.Procedure{AckEmissions: 1}); err == nil {
		t.Fatal("procedure count without ACK admitted")
	}
	wrong := tx
	wrong.Family = portable.NonInviteClient
	if _, err = encodeNativeProcedure(wrong, refs, &nativepb.Procedure{AckEmissions: 1}); err == nil {
		t.Fatal("non-INVITE ACK count admitted")
	}
	dialogRefs := nativeDialogRefs{Local: &nativeInviteRefs{Request: "r:origin", Final: "r:final", ACK: "r:dialog-ack"}}
	data, err = encodeNativeDialog(dialog, dialogRefs, &nativepb.Dialog{AckEmissions: stateProfileTransmissions})
	if err != nil {
		t.Fatal(err)
	}
	d, err := readPrivateDialogRecord(data)
	if err != nil || d.AckEmissions != stateProfileTransmissions {
		t.Fatal("dialog count lost", err)
	}
	if _, err = encodeNativeDialog(dialog, dialogRefs, &nativepb.Dialog{AckEmissions: stateProfileTransmissions + 1}); err == nil {
		t.Fatal("excess dialog ACKs admitted")
	}
	d.LocalInvite.Ack = ""
	invalid, err = marshalNativeRecord(storagecodec.NativeDialog, d)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = readPrivateDialogRecord(invalid); err == nil {
		t.Fatal("decoded dialog ACK count without ACK")
	}
	noACK := dialog
	copyInvite := *dialog.LocalInvite
	copyInvite.ACK = nil
	noACK.LocalInvite = &copyInvite
	if _, err = encodeNativeDialog(noACK, nativeDialogRefs{Local: &nativeInviteRefs{Request: "r:origin", Final: "r:final"}}, &nativepb.Dialog{AckEmissions: 1}); err == nil {
		t.Fatal("dialog count without ACK admitted")
	}
}
