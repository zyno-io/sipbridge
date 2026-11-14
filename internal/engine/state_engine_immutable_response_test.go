package engine

import (
	"testing"

	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
)

func TestStateResponseFlowVariantPreservesSelectedMessage(t *testing.T) {
	r, c, tx := provisionalResponseFixture(t)
	advertiseCommand(t, r, c, &pb.Command{Sequence: 1, Action: &pb.Command_Respond{Respond: &pb.Respond{RequestId: tx.RequestID, Status: 180}}})
	selected := *tx.Native.LastResponse
	firstIndex, _, first := responseEffectForStatus(t, r, 180)
	flow := proto.Clone(tx.Flow).(*pb.FlowRef)
	flow.ConnectionId = "immutable-response-replacement"
	flow.SourcePort++
	tx.Flow = flow
	r.in = stateEngineInput{Kind: "ingest"}
	if err := r.send(c, tx, selected, true); err != nil {
		t.Fatal(err)
	}
	if !selected.Equal(*tx.Native.LastResponse) {
		t.Fatal("physical flow finalization changed the selected response")
	}
	_, _, replacement := responseEffectForStatus(t, r, 180)
	if firstIndex == len(r.payload.Effects)-1 || first.WireHash == replacement.WireHash || proto.Equal(first.Flow, replacement.Flow) {
		t.Fatal("replacement flow did not retain distinct physical evidence")
	}
}

func TestStateNativeOutputEffectKeepsPreparedMessage(t *testing.T) {
	r, c, tx := forkFixture(t)
	message := tx.Native.Origin
	if err := r.send(c, tx, message, false); err != nil {
		t.Fatal(err)
	}
	for _, effect := range r.payload.Effects {
		if effect.Kind != "wire" {
			continue
		}
		var body stateEngineEffect
		if err := r.unmarshalEffectMetadata(&body, effect); err != nil || body.prepared != message {
			t.Fatal("new native output lost its original immutable holder", err)
		}
		return
	}
	t.Fatal("wire effect missing")
}

func TestStateForkEventsKeepPreparedMessage(t *testing.T) {
	for _, scenario := range []struct {
		name   string
		status int
		tag    string
		kind   pb.DialogEvent_Kind
	}{
		{"untagged provisional", 100, "", pb.DialogEvent_PROVISIONAL},
		{"tagged provisional", 183, "early", pb.DialogEvent_PROVISIONAL},
		{"failed", 486, "failure", pb.DialogEvent_FAILED},
		{"answered", 200, "winner", pb.DialogEvent_ANSWERED},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			r, c, tx := forkFixture(t)
			message := forkReply(t, tx, scenario.status, scenario.tag)
			if _, err := r.forkResponse(c, tx, message); err != nil {
				t.Fatal(err)
			}
			for _, effect := range r.payload.Effects {
				if effect.Kind != "event" {
					continue
				}
				var body stateEngineEffect
				if err := r.unmarshalEffectMetadata(&body, effect); err != nil {
					t.Fatal(err)
				}
				if body.Envelope.GetDialogEvent().GetKind() == scenario.kind {
					if body.prepared != message {
						t.Fatal("fork event lost its original immutable holder")
					}
					return
				}
			}
			t.Fatal("expected fork event missing")
		})
	}
}
