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

func TestDialogIDProjectionPreservesNativeReference(t *testing.T) {
	key := bytes.Repeat([]byte{1}, 32)
	authority := &pb.StateAuthority{Service: "redis", Generation: "three", PrimaryId: "run", SchemaVersion: 1}
	ref := &pb.ContextRef{Id: "call", Owner: &pb.ProcessRef{InstanceId: "a", Epoch: "a-epoch"},
		State: &pb.StateRef{Authority: authority, GroupId: "call", Incarnation: "one", CallId: "phone-private@example.test"}}
	frame := &pb.Envelope{CorrelationId: "inspect", Payload: &pb.Envelope_Result{Result: &pb.OperationResult{
		Snapshot: &pb.ContextSnapshot{Context: ref}, RetirementProof: &pb.ContextRetirementProof{Context: ref},
	}}}
	projected, err := projectDialogIDs(frame, authority, key, DialogIDCapability, false)
	if err != nil {
		t.Fatal(err)
	}
	id := projected.GetResult().GetSnapshot().GetContext().GetOpaqueDialogId()
	if id == "" || projected.GetResult().GetRetirementProof().GetContext().GetOpaqueDialogId() != id || ref.OpaqueDialogId != "" {
		t.Fatal("wire projection missed a reference or modified native state")
	}
	moved := proto.Clone(ref).(*pb.ContextRef)
	moved.Owner = &pb.ProcessRef{InstanceId: "b", Epoch: "b-epoch"}
	movedID, err := dialogIDFor(authority, key, moved)
	if err != nil || movedID != id {
		t.Fatal("same native incarnation changed ID with a new serving replica", err)
	}
	moved.State.Incarnation = "next"
	movedID, err = dialogIDFor(authority, key, moved)
	if err != nil || movedID == id {
		t.Fatal("a new native incarnation reused its predecessor's dialog ID", err)
	}
	if err := normalizeDialogIDs(projected, authority, key, DialogIDCapability); err != nil || !proto.Equal(frame, projected) {
		t.Fatal("normalization did not restore the exact native envelope", err)
	}
}

func TestCompactRetirementProofRemovesOnlyRedundantAuthority(t *testing.T) {
	authority := &pb.StateAuthority{Service: "redis", Generation: "4", PrimaryId: "primary", SchemaVersion: 1}
	key := bytes.Repeat([]byte{3}, 32)
	ref := &pb.ContextRef{Id: "dialog", State: &pb.StateRef{Authority: authority, GroupId: "call", Incarnation: "one", CallId: "phone@example.test"}}
	proof := &pb.ContextRetirementProof{Context: ref, Authority: authority, Reason: pb.ContextRetirementProof_CONTEXT_ABSENT}
	frame := &pb.Envelope{Payload: &pb.Envelope_Result{Result: &pb.OperationResult{RetirementProof: proof}}}
	legacy, err := projectDialogIDs(frame, authority, key, DialogIDOnlyCapability, false)
	if err != nil || !proto.Equal(legacy.GetResult().GetRetirementProof().GetAuthority(), authority) {
		t.Fatal("an ID-only peer using the original proof format lost its authority", err)
	}
	compact, err := projectDialogIDs(frame, authority, key, DialogIDOnlyCapability, true)
	if err != nil {
		t.Fatal(err)
	}
	sent := compact.GetResult().GetRetirementProof()
	if sent.Authority != nil || sent.Reason != proof.Reason || proof.Authority == nil {
		t.Fatal("projection altered native proof or failed to compact the wire proof")
	}
	if err := normalizeDialogIDs(compact, authority, key, DialogIDOnlyCapability); err != nil || !proto.Equal(sent.Context, ref) {
		t.Fatal("compact proof did not identify the original dialog", err)
	}
	proof.Authority = &pb.StateAuthority{Service: authority.Service, Generation: authority.Generation, PrimaryId: "another", SchemaVersion: 1}
	if _, err := projectDialogIDs(frame, authority, key, DialogIDOnlyCapability, true); err == nil {
		t.Fatal("compact proof masked a mismatched native authority")
	}
}

func TestDialogIDUsesDedicatedWireBoundAndNativeDomain(t *testing.T) {
	key := bytes.Repeat([]byte{3}, 32)
	one := &pb.StateAuthority{Service: "redis", Generation: "three", PrimaryId: "primary-one", SchemaVersion: 1}
	two := &pb.StateAuthority{Service: "redis", Generation: "three", PrimaryId: "primary-two", SchemaVersion: 1}
	long := strings.Repeat("x", 96)
	ref := &pb.ContextRef{Id: long, State: &pb.StateRef{Authority: one, GroupId: long, Incarnation: long, Partition: 4095}}
	id, err := dialogIDFor(one, key, ref)
	if err != nil || len(id) != maxDialogIDLength {
		t.Fatal("maximum-size native reference produced an out-of-bounds handle", len(id), err)
	}
	ref.OpaqueDialogId = id
	ref.State.Authority = two
	frame := &pb.Envelope{Payload: &pb.Envelope_Inspect{Inspect: &pb.ContextInspect{Context: ref}}}
	if err := normalizeDialogIDs(frame, two, key, DialogIDCapability); err == nil {
		t.Fatal("dialog ID escaped its native domain")
	}
	ref.State.Authority = one
	ref.OpaqueDialogId = id + "x"
	if err := normalizeDialogIDs(frame, one, key, DialogIDCapability); err == nil {
		t.Fatal("overlong dialog ID was accepted")
	}
}

func TestDialogIDControlNegotiation(t *testing.T) {
	pki := newPKI(t)
	key := bytes.Repeat([]byte{2}, 32)
	authority := &pb.StateAuthority{Service: "redis", Generation: "three", PrimaryId: "run", SchemaVersion: 1}
	ref := &pb.ContextRef{Id: "call", Owner: &pb.ProcessRef{InstanceId: "sip", Epoch: "sip-epoch"},
		State: &pb.StateRef{Authority: authority, GroupId: "call", Incarnation: "one", CallId: "call@example.test"}}
	shared := []string{"sip.v1", StateCapability, NativeCompletionCapability, DialogSocketCapability, ControlAttachmentCapability}
	dual := append(slices.Clone(shared), DialogIDCapability)
	capabilities := append(slices.Clone(dual), DialogIDOnlyCapability)
	lease := func(app *pb.ProcessRef) *pb.ApplicationLease {
		now := uint64(time.Now().UnixMilli())
		return &pb.ApplicationLease{Application: app, Authority: authority, Term: "term", CheckedAtMs: now, ExpiresAtMs: now + 30000}
	}
	handled := make(chan *pb.ContextRef, 2)
	_, address := testServer(t, pki, 8, func(cfg *Config) {
		cfg.Capabilities, cfg.StateAuthority, cfg.DialogIDKey = capabilities, authority, key
		cfg.OnControllerSession = func(_ context.Context, _ *Peer, hello *pb.SessionHello) (*pb.ApplicationLease, error) {
			return lease(hello.Controller), nil
		}
		cfg.OnControllerRenew = func(_ context.Context, peer *Peer) (*pb.ApplicationLease, error) {
			return lease(peer.Application()), nil
		}
		cfg.OnDisconnect = func(*Peer) {}
		cfg.OnMessage = func(peer *Peer, frame *pb.Envelope) error {
			handled <- frame.GetInspect().GetContext()
			return peer.Send(&pb.Envelope{CorrelationId: frame.CorrelationId, Payload: &pb.Envelope_Result{Result: &pb.OperationResult{
				Certainty: pb.OperationResult_COMPLETED, Snapshot: &pb.ContextSnapshot{Context: ref},
			}}})
		}
	})

	for _, tc := range []struct {
		name string
		caps []string
		mode string
	}{
		{name: "legacy", caps: shared},
		{name: "dual", caps: dual, mode: DialogIDCapability},
		{name: "id-only", caps: capabilities, mode: DialogIDOnlyCapability},
	} {
		t.Run(tc.name, func(t *testing.T) {
			conn, err := pki.dial(address)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.CloseNow()
			send(t, conn, &pb.Envelope{CorrelationId: "hello", Payload: &pb.Envelope_Hello{Hello: &pb.SessionHello{
				Controller: &pb.ProcessRef{InstanceId: tc.name, Epoch: "epoch"}, ConnectionAttempt: 1,
				Major: 1, Capabilities: tc.caps, Bounds: &pb.Bounds{MaxEnvelopeBytes: 4096},
			}}})
			first, err := receive(conn)
			if err != nil || first.GetHelloAck() == nil {
				t.Fatal("hello failed", err)
			}
			ack := first.GetHelloAck()
			if dialogIDMode(ack.Capabilities) != tc.mode {
				t.Fatal("incorrect dialog ID negotiation")
			}
			inspect := func(r *pb.ContextRef) {
				send(t, conn, &pb.Envelope{CorrelationId: "inspect", SessionId: ack.SessionId, RemainingBudgetMs: 500,
					Payload: &pb.Envelope_Inspect{Inspect: &pb.ContextInspect{Context: r}}})
			}
			input := proto.Clone(ref).(*pb.ContextRef)
			id, err := dialogIDFor(authority, key, ref)
			if err != nil {
				t.Fatal(err)
			}
			if tc.mode == DialogIDOnlyCapability {
				id, err = dialogIDOnlyFor(authority, key, ref)
				if err != nil {
					t.Fatal(err)
				}
				// Old persisted references can still request a fresh projection.
				inspect(input)
				resolved, receiveErr := receive(conn)
				if receiveErr != nil || resolved.GetResult().GetSnapshot().GetContext().GetOpaqueDialogId() != id {
					t.Fatal("ID-only session failed to resolve a legacy reference", receiveErr)
				}
				<-handled
				input = &pb.ContextRef{OpaqueDialogId: id}
			} else if tc.mode == DialogIDCapability {
				input.OpaqueDialogId = id
			}
			inspect(input)
			reply, err := receive(conn)
			if err != nil {
				t.Fatal("compatible reference was rejected", err)
			}
			wire := reply.GetResult().GetSnapshot().GetContext()
			if wire == nil || (wire.GetOpaqueDialogId() != "") != (tc.mode != "") || tc.mode != "" && wire.GetOpaqueDialogId() != id {
				t.Fatal("incorrect wire reference")
			}
			if tc.mode == DialogIDOnlyCapability && (wire.GetId() != "" || wire.GetOwner() != nil || wire.GetState() != nil) {
				t.Fatal("ID-only wire reference carried legacy fields")
			}
			select {
			case native := <-handled:
				if !proto.Equal(native, ref) {
					t.Fatal("native inspection boundary changed the retained reference")
				}
			case <-time.After(time.Second):
				t.Fatal("inspection was never handled")
			}

			if tc.mode == DialogIDOnlyCapability {
				input.Id = ref.Id
			} else if tc.mode == DialogIDCapability {
				input.State.Incarnation = "recycled"
			} else {
				input.OpaqueDialogId = id
			}
			inspect(input)
			if _, err := receive(conn); err == nil {
				t.Fatal("unexpected response for invalid reference")
			}
			if len(handled) != 0 {
				t.Fatal("invalid reference reached the native engine")
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
