package engine

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/state"
)

func TestNativeCompletionIngressDoesNotAllocateOnMiss(t *testing.T) {
	for _, backend := range []string{state.MemoryBackend, state.RedisBackend} {
		t.Run(backend, func(t *testing.T) { testNativeCompletionIngressDoesNotAllocateOnMiss(t, backend) })
	}
}

func testNativeCompletionIngressDoesNotAllocateOnMiss(t *testing.T, backend string) {
	f := newNativeOwnerFixtureBackend(t, backend)
	f.engine.work = make(chan struct{}, 64)
	request := stateTestRequest("CANCEL", 1, "", "missing-native-context")
	callID, err := stateMessageCallID(request.Wire)
	if err != nil {
		t.Fatal(err)
	}
	ref, err := f.engine.store.GroupForCallID(callID)
	if err != nil {
		t.Fatal(err)
	}
	handled, err := f.engine.TryNativeCompletion(context.Background(), f.flow, request)
	if err != nil || handled {
		t.Fatalf("unmatched native ingress was admitted: %v / %v", handled, err)
	}
	if _, err = f.engine.store.Load(context.Background(), ref); err != state.ErrNotFound {
		t.Fatalf("completion lookup created a group: %v", err)
	}
}

func TestNativeCompletionReplaysOwnerFencedAcceptedReinviteFinal(t *testing.T) {
	f := newNativeOwnerFixture(t)
	r, c, tx := completionRelayFinalFixture(t, sip.INVITE, 200)
	completeLatestResponse(t, r, pb.DeliveryResult_UNKNOWN)
	if tx.Native.Phase != portable.Accepted {
		t.Fatalf("fixture must retain an Accepted server transaction, got %s", tx.Native.Phase)
	}
	_, original, _ := latestFinalEffect(t, r)
	r.engine = f.engine
	c.OwnerLost = true
	c.Snapshot.Application, c.Snapshot.ApplicationLeaseTerm = f.app, f.term
	c.Snapshot.Context.State.Authority = f.engine.StateAuthority()
	flow := proto.Clone(c.Flow).(*pb.FlowRef)
	flow.ConnectionId = "native-cached-final"
	before := len(r.payload.Effects)
	r.in = stateEngineInput{Kind: "native-ingest", NativeCompletion: true, Context: c.Snapshot.Context, Application: c.Snapshot.Application,
		ApplicationLeaseTerm: f.term, Ingest: &pb.OwnerIngest{Context: c.Snapshot.Context, Application: c.Snapshot.Application,
			Flow: flow, Wire: tx.Native.Origin.Wire, Admission: pb.OwnerIngest_TLS_DIALOG_CONTINUITY}}
	if err := r.ingestNativeCompletion(); err != nil {
		t.Fatal(err)
	}
	if len(r.payload.Effects) != before+1 {
		t.Fatal("exact Accepted re-INVITE replay did not emit its cached final")
	}
	_, replay, body := latestFinalEffect(t, r)
	if replay.ID == original.ID || body.WireStatus != 200 || !proto.Equal(body.Flow, flow) {
		t.Fatal("native replay changed selected final/flow identity")
	}
	_, prior, _, err := r.findEffect(original.ID)
	if err != nil || prior.Status != state.EffectUnknown {
		t.Fatal("native replay erased an earlier physical UNKNOWN")
	}
	if tx.Native.Phase != portable.Accepted {
		t.Fatal("native replay changed the original server transaction phase")
	}
	if err = r.prepareAutonomousEffects(); err != nil {
		t.Fatal(err)
	}
	_, replay, body, err = r.findEffect(replay.ID)
	if err != nil || replay.Kind != "deliver" || !body.Native || body.Envelope != nil {
		t.Fatal("cached final still requires Controller authorization")
	}
}

func TestNativeCompletionIngressRetainsExactFlowAndNativeAuthority(t *testing.T) {
	for _, backend := range []string{state.MemoryBackend, state.RedisBackend} {
		t.Run(backend, func(t *testing.T) { testNativeCompletionIngressRetainsExactFlowAndNativeAuthority(t, backend) })
	}
}

func testNativeCompletionIngressRetainsExactFlowAndNativeAuthority(t *testing.T, backend string) {
	f := newNativeOwnerFixtureBackend(t, backend)
	f.engine.work = make(chan struct{}, 64)
	ref := f.admit(t)
	before, _ := stateTestLoad(t, f.engine, ref)
	wrong := stateTestRequest("CANCEL", 1, "", "wrong-branch")
	if handled, err := f.engine.TryNativeCompletion(context.Background(), f.flow, wrong); err != nil || handled {
		t.Fatalf("wrong branch admitted: %v / %v", handled, err)
	}
	after, _ := stateTestLoad(t, f.engine, ref)
	if after.Revision != before.Revision {
		t.Fatal("rejected completion mutated native state")
	}
	correct := stateTestRequest("CANCEL", 1, "", "native-owner")
	lowered := proto.Clone(f.flow).(*pb.FlowRef)
	lowered.Transport = "tcp"
	if handled, err := f.engine.TryNativeCompletion(context.Background(), lowered, correct); err != nil || handled {
		t.Fatalf("TLS downgrade gained native authority: %v / %v", handled, err)
	}
	if handled, err := f.engine.TryNativeCompletion(context.Background(), f.flow, correct); err != nil || !handled {
		t.Fatalf("exact retained CANCEL required Controller: %v / %v", handled, err)
	}
	record, group := stateTestLoad(t, f.engine, ref)
	if len(group.Contexts) != 1 || group.Contexts[ref.Id].Snapshot.ApplicationLeaseTerm != f.term {
		t.Fatal("native completion changed business ownership")
	}
	native := 0
	for _, effect := range record.Payload.Effects {
		var body stateEngineEffect
		if json.Unmarshal(effect.Payload, &body) != nil {
			t.Fatal("corrupt effect")
		}
		if effect.Kind == "wire" && body.Authorization.GetAuthorityClass() != pb.EffectAuthorization_APPLICATION {
			t.Fatal("native response still delegates authorization to Controller")
		}
		if !body.Native {
			continue
		}
		native++
		if effect.Kind != "deliver" || effect.ApplicationLease != nil || body.Permit == nil || !proto.Equal(body.Permit.Flow, f.flow) || body.Envelope != nil || body.Executor != nil {
			t.Fatal("native completion escaped the retained physical flow/effect")
		}
	}
	if native < 2 {
		t.Fatal("CANCEL did not retain both native 200 and INVITE 487 responses")
	}
	// Call-ID changes, even with the same transaction branch, cannot reach the
	// original group or allocate one through the completion path.
	foreign, err := Observe([]byte(strings.ReplaceAll(string(correct.Wire), "redis-native-call@example.test", "foreign-call@example.test")), false)
	if err != nil {
		t.Fatal(err)
	}
	if handled, err := f.engine.TryNativeCompletion(context.Background(), f.flow, foreign); err != nil || handled {
		t.Fatalf("foreign Call-ID admitted: %v / %v", handled, err)
	}
}
