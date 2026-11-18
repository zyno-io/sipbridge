package engine

import (
	"bytes"
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/emiago/sipgo/sip/portable"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/state"
)

func TestFixtureCompletionProjectionRetainsProofsWithoutWireOrCredentials(t *testing.T) {
	f := newNativeOwnerFixtureBackend(t, state.MemoryBackend)
	ref := f.admit(t)
	record, group := stateTestLoad(t, f.engine, ref)
	c := group.Contexts[ref.Id]
	const secret = "fixture-private-header-body-sdp-password"
	c.Snapshot.LocalSdp, c.Snapshot.RemoteSdp = []byte(secret), []byte(secret)
	c.Snapshot.LocalUri, c.Snapshot.RemoteUri, c.Snapshot.RemoteTarget = "sip:user:"+secret+"@example.test", "sip:"+secret+"@example.test", "sip:"+secret+"@target.test"
	c.Snapshot.CommandSequence = 1 << 63
	c.Snapshot.Delivery = &pb.ContextDelivery{Flow: c.Flow, RegistrationTicket: secret}
	c.Delivery = &pb.ContextDelivery{Flow: c.Flow, NativeRevision: 9, DirectoryEpoch: "directory", RegistrationTicket: secret}
	c.Subscription = &stateSubscription{Event: "dialog", Target: "sip:" + secret + "@target.test", Routes: []string{"sip:" + secret + "@route.test"}, ExpiresMS: 1234}
	c.CompletionScope = "exact-opaque-completion-scope"
	c.DialogScopes = map[string]string{"dialog": "exact-dialog-scope"}
	c.Credential = &pb.CredentialRef{Id: secret, Version: secret}
	c.Origin = portable.RestoreMessage(bytes.Replace(c.Origin.WireBytes(), []byte("\r\n"), []byte("\r\nX-Fixture-Private: "+secret+"\r\n"), 1), c.Origin.Transport(), c.Origin.Source(), c.Origin.Destination())
	var selected *stateTransaction
	for _, tx := range c.Transactions {
		selected = tx
		break
	}
	if selected == nil {
		t.Fatal("fixture has no real native transaction")
	}
	selected.Native.Origin = c.Origin
	selected.CompletionScope = c.CompletionScope
	selected.Native.Cancelled, selected.Native.CancelSent = true, true
	selected.Native.Timers[portable.TimerCancel] = portable.Deadline{At: 1234, Generation: 7, Active: true}
	response, err := portable.BuildResponseWithHeaders(c.Origin, 503, "Unavailable", nil, "remote-tag", f.engine.cfg.ContactURI, nil)
	if err != nil {
		t.Fatal(err)
	}
	selected.Native.LastResponse = &response
	eventBody, err := (stateEngineEffect{Context: ref.Id, Envelope: &pb.Envelope{Payload: &pb.Envelope_DialogEvent{DialogEvent: &pb.DialogEvent{Kind: pb.DialogEvent_FAILED, OperationId: "event-operation", Snapshot: c.Snapshot, Message: &pb.SipMessage{Wire: c.Origin.WireBytes()}}}}}).MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	payload := record.Payload
	payload.Effects = []state.Effect{{ID: "event", Kind: "event", Status: state.EffectPending, DeadlineMS: 1234, Payload: eventBody}}
	payload.ProtocolState, err = group.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	before := append([]byte(nil), payload.ProtocolState...)
	projected, err := InspectFixtureCompletion(payload)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(projected)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), secret) || strings.Contains(string(encoded), "X-Fixture-Private") || strings.Contains(string(encoded), "credential") || strings.Contains(string(encoded), "permit") {
		t.Fatal("fixture projection exported sensitive bytes or authority material")
	}
	if !bytes.Equal(before, payload.ProtocolState) {
		t.Fatal("inspection mutated its input")
	}
	got := projected.Group.Contexts[ref.Id]
	if got.CompletionScope != c.CompletionScope || got.DialogScopes["dialog"] != "exact-dialog-scope" || got.Subscription.Target == c.Subscription.Target || got.Delivery.RegistrationTicketSHA256 != fixtureHash([]byte(secret)) {
		t.Fatal("fixture lost opaque proof or failed target/ticket redaction")
	}
	if !strings.Contains(string(got.Snapshot), `"commandSequence":"9223372036854775808"`) {
		t.Fatal("fixture lost exact uint64 identity")
	}
	tx := got.Transactions[selected.Native.Key]
	if tx.CompletionScope != c.CompletionScope || !tx.Native.Cancelled || !tx.Native.CancelSent || !reflect.DeepEqual(tx.Native.Timers, selected.Native.Timers) || tx.Native.Origin.WireSHA256 != fixtureHash(c.Origin.WireBytes()) || tx.Native.LastResponse.Status != 503 {
		t.Fatal("fixture lost exact native completion proof")
	}
	if len(projected.Payload.Effects) != 1 || projected.Payload.Effects[0].Event.OperationID != "event-operation" {
		t.Fatal("fixture lost pending event proof")
	}
}

func TestFixtureMessageProofPreservesEntireHeaderEqualityAndDoesNotReadBody(t *testing.T) {
	wire := []byte("NOTIFY sip:200@example.test SIP/2.0\r\nVia: SIP/2.0/TCP example.test;branch=z9hG4bK-fixture\r\nFrom: \"Display  Name\" <sip:100@example.test>;tag=from\r\nTo: <sip:200@example.test>;tag=to\r\nCall-ID: fixture-call\r\nCSeq: 7 NOTIFY\r\nContent-Length: 0\r\n\r\n")
	message := portable.RestoreMessage(wire, "tcp", "", "")
	first, err := fixtureMessage(message)
	if err != nil {
		t.Fatal(err)
	}
	message = portable.RestoreMessage(bytes.Replace(wire, []byte("Display  Name"), []byte("Display Name"), 1), message.Transport(), message.Source(), message.Destination())
	changed, err := fixtureMessage(message)
	if err != nil {
		t.Fatal(err)
	}
	if first.FromTag != changed.FromTag || first.FromSHA256 == changed.FromSHA256 || first.WireSHA256 == changed.WireSHA256 || first.ToSHA256 != changed.ToSHA256 {
		t.Fatal("header hashes weakened exact From/To comparison")
	}
	if first.Method != "NOTIFY" || first.CSeq != "7 NOTIFY" || first.CSeqNumber != 7 || first.CallID != "fixture-call" {
		t.Fatal("message selector metadata changed")
	}
	lf := bytes.ReplaceAll(wire, []byte("\r\n"), []byte("\n"))
	lf = bytes.Replace(lf, []byte("Content-Length: 0\n\n"), []byte("Content-Length: 24\n\nFrom: secret body value!\n"), 1)
	// Header extraction itself must stop before an LF-only message body.
	value, err := fixtureHeader(lf, "from")
	if err != nil || !strings.Contains(value, "Display  Name") {
		t.Fatal("body text entered a header proof")
	}
}

func TestFixtureProjectionRefusesCorruptEffectsBeforeReturningIdentifyingOutput(t *testing.T) {
	f := newNativeOwnerFixtureBackend(t, state.MemoryBackend)
	ref := f.admit(t)
	record, _ := stateTestLoad(t, f.engine, ref)
	record.Payload.Effects = []state.Effect{{Kind: "event", Payload: []byte("private-malformed-record")}}
	if _, err := InspectFixtureCompletion(record.Payload); err == nil {
		t.Fatal("corrupt private effect was accepted")
	}
}
