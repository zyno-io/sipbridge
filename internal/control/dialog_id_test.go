package control

import (
	"bytes"
	"context"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
)

func TestDialogDirectoryDigestMatchesNexusJSON(t *testing.T) {
	authority := &pb.StateAuthority{Service: "redis", Generation: "4", PrimaryId: "primary", SchemaVersion: 1}
	ref := &pb.ContextRef{Id: "dialog", State: &pb.StateRef{Authority: authority, GroupId: "group", Incarnation: "incarnation", Partition: 2}}
	for _, tc := range []struct{ callID, digest string }{
		{"phone@example.test", "75017a2167f17040cd1842484166b402d1b6501a7d9ba49653a996c14c262922"},
		{"<x>&\"quote\"\\u2028\u2028\u2029@phone.test", "0b3b373b4528a516ed495b255f9e7a4808b4181e86bb9130a0eab1081939e87e"},
	} {
		ref.State.CallId = tc.callID
		if digest := dialogDirectoryDigest(ref); digest != tc.digest {
			t.Fatalf("directory key mismatch: %s != %s", digest, tc.digest)
		}
	}
}

func TestRetirementProofUsesOnlyTheSignedDialogID(t *testing.T) {
	key := bytes.Repeat([]byte{3}, 32)
	authority := &pb.StateAuthority{Service: "redis", Generation: "4", PrimaryId: "primary", SchemaVersion: 1}
	ref := &pb.ContextRef{Id: "dialog", State: &pb.StateRef{Authority: authority, GroupId: "call", Incarnation: "one", CallId: "phone@example.test"}}
	proof := &pb.ContextRetirementProof{Context: ref, Reason: pb.ContextRetirementProof_CONTEXT_ABSENT}
	frame := &pb.Envelope{Payload: &pb.Envelope_Result{Result: &pb.OperationResult{RetirementProof: proof}}}

	wire, err := projectDialogIDs(frame, authority, key)
	if err != nil {
		t.Fatal(err)
	}
	sent := wire.GetResult().GetRetirementProof()
	id, err := dialogIDOnlyFor(authority, key, ref)
	if err != nil || sent.Reason != proof.Reason || sent.Authority != nil ||
		!proto.Equal(sent.Context, &pb.ContextRef{OpaqueDialogId: id}) || ref.GetState() == nil {
		t.Fatal("retirement proof did not project just the dialog ID and reason", err)
	}
	if err := normalizeDialogIDs(wire, authority, key, true); err != nil || !proto.Equal(wire, frame) {
		t.Fatal("wire proof changed the native dialog reference", err)
	}
	proof.Authority = authority
	if _, err := projectDialogIDs(frame, authority, key); err == nil {
		t.Fatal("accepted an authority-bearing retirement proof")
	}
	proof.Authority = nil
	other := proto.Clone(authority).(*pb.StateAuthority)
	other.PrimaryId = "another"
	if _, err := projectDialogIDs(frame, other, key); err == nil {
		t.Fatal("accepted a retirement proof from another state scope")
	}
}

func TestServingControlRequiresCurrentDialogContract(t *testing.T) {
	pki := newPKI(t)
	key := bytes.Repeat([]byte{2}, 32)
	authority := &pb.StateAuthority{Service: "redis", Generation: "4", PrimaryId: "run", SchemaVersion: 1}
	ref := &pb.ContextRef{Id: "call", Owner: &pb.ProcessRef{InstanceId: "sip", Epoch: "sip-epoch"},
		State: &pb.StateRef{Authority: authority, GroupId: "call", Incarnation: "one", CallId: "call@example.test"}}
	base := []string{"sip.v1", StateCapability, NativeCompletionCapability, DialogSocketCapability, ControlAttachmentCapability}
	current := append(slices.Clone(base), DialogIDOnlyCapability, DialogRetirementCapability)
	lease := func(app *pb.ProcessRef) *pb.ApplicationLease {
		now := uint64(time.Now().UnixMilli())
		return &pb.ApplicationLease{Application: app, Authority: authority, Term: "term", CheckedAtMs: now, ExpiresAtMs: now + 30000}
	}
	_, address := testServer(t, pki, 8, func(cfg *Config) {
		cfg.Capabilities, cfg.StateAuthority, cfg.DialogIDKey = current, authority, key
		cfg.OnControllerSession = func(_ context.Context, _ *Peer, hello *pb.SessionHello) (*pb.ApplicationLease, error) {
			return lease(hello.Controller), nil
		}
		cfg.OnControllerRenew = func(_ context.Context, peer *Peer) (*pb.ApplicationLease, error) {
			return lease(peer.Application()), nil
		}
		cfg.OnDisconnect = func(*Peer) {}
		cfg.OnMessage = func(peer *Peer, frame *pb.Envelope) error {
			if !proto.Equal(frame.GetInspect().GetContext(), ref) {
				return context.Canceled
			}
			return peer.Send(&pb.Envelope{CorrelationId: frame.CorrelationId, Payload: &pb.Envelope_Result{Result: &pb.OperationResult{
				Certainty:       pb.OperationResult_COMPLETED,
				RetirementProof: &pb.ContextRetirementProof{Context: ref, Reason: pb.ContextRetirementProof_CONTEXT_ABSENT},
			}}})
		}
	})
	for _, tc := range []struct {
		name string
		caps []string
	}{
		{"old format", append(slices.Clone(base), "sip.dialog-id.v1")},
		{"ID without retirement", append(slices.Clone(base), DialogIDOnlyCapability)},
		{"current", current},
	} {
		t.Run(tc.name, func(t *testing.T) {
			conn, err := pki.dial(address)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.CloseNow()
			send(t, conn, &pb.Envelope{CorrelationId: "hello", Payload: &pb.Envelope_Hello{Hello: &pb.SessionHello{
				Controller: &pb.ProcessRef{InstanceId: "controller", Epoch: "epoch"}, ConnectionAttempt: 1,
				Major: 1, Capabilities: tc.caps, Bounds: &pb.Bounds{MaxEnvelopeBytes: 4096},
			}}})
			first, err := receive(conn)
			if !slices.Contains(tc.caps, DialogRetirementCapability) {
				if err == nil {
					t.Fatal("serving session accepted an older dialog contract")
				}
				return
			}
			if err != nil || first.GetHelloAck() == nil {
				t.Fatal("current contract did not connect", err)
			}
			id, err := dialogIDOnlyFor(authority, key, ref)
			if err != nil {
				t.Fatal(err)
			}
			send(t, conn, &pb.Envelope{CorrelationId: "inspect", SessionId: first.GetHelloAck().SessionId, RemainingBudgetMs: 500,
				Payload: &pb.Envelope_Inspect{Inspect: &pb.ContextInspect{Context: &pb.ContextRef{OpaqueDialogId: id}}}})
			reply, err := receive(conn)
			proof := reply.GetResult().GetRetirementProof()
			if err != nil || proof.GetAuthority() != nil || proof.GetReason() != pb.ContextRetirementProof_CONTEXT_ABSENT ||
				!proto.Equal(proof.GetContext(), &pb.ContextRef{OpaqueDialogId: id}) {
				t.Fatal("invalid retirement proof on the current session", err)
			}
		})
	}
}

func TestDialogIDOnlyBindsFullLocatorAndScope(t *testing.T) {
	key := bytes.Repeat([]byte{3}, 32)
	authority := &pb.StateAuthority{Service: "redis", Generation: "4", PrimaryId: "primary", SchemaVersion: 1}
	long := strings.Repeat("a", 96)
	ref := &pb.ContextRef{
		Id: long, Owner: &pb.ProcessRef{InstanceId: long, Epoch: long},
		State: &pb.StateRef{Authority: authority, GroupId: long, Incarnation: long, CallId: strings.Repeat("é", 512), Partition: 4095},
	}
	id, err := dialogIDOnlyFor(authority, key, ref)
	if err != nil || len(id) != maxDialogIDOnlyLength {
		t.Fatalf("maximum sized ID has wrong size: %d: %v", len(id), err)
	}
	decoded, err := decodeDialogIDOnly(authority, key, id)
	if err != nil || !proto.Equal(decoded, ref) {
		t.Fatal("ID-only locator did not preserve its native reference", err)
	}
	decoded.State.Authority.Generation = "5"
	if authority.Generation != "4" {
		t.Fatal("normalized context mutated the negotiated authority")
	}
	if _, err := decodeDialogIDOnly(decoded.State.Authority, key, id); err == nil {
		t.Fatal("ID-only locator accepted in a different state scope")
	}
	parts := strings.Split(id, ".")
	parts[2] = "A" + parts[2][1:]
	if _, err := decodeDialogIDOnly(authority, key, strings.Join(parts, ".")); err == nil {
		t.Fatal("tampered locator was accepted")
	}
	ref.Owner = nil
	ownerless, err := dialogIDOnlyFor(authority, key, ref)
	if err != nil || strings.Split(ownerless, ".")[1] != parts[1] {
		t.Fatal("omitting the owner hint changed the directory digest", err)
	}
}

func TestDialogIDIdentifiesContextReferencesInEffects(t *testing.T) {
	one := &pb.ContextRef{Id: "one"}
	two := &pb.ContextRef{Id: "two"}
	frame := &pb.Envelope{Payload: &pb.Envelope_InspectEffect{InspectEffect: &pb.FlowWrite{
		Effect: &pb.StateEffectRef{Context: one},
		Permit: &pb.SendPermit{Authorization: &pb.EffectAuthorization{Effect: &pb.StateEffectRef{Context: two}}},
	}}}
	if refs := envelopeDialogReferences(frame); !reflect.DeepEqual(refs, []*pb.ContextRef{one, two}) {
		t.Fatal("one of the effect references bypasses the wire boundary")
	}
}
