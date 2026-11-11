package engine

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/state"
)

// Controller needs the accepted revision/epoch even when the phone retains its
// socket: otherwise a successful unsubscribe cannot remove its business record
// or send the terminal NOTIFY under the subscription mutation fence.
func TestStateSubscriptionSameFlowRefreshPublishesMutationFence(t *testing.T) {
	for _, expires := range []int{180, 0} {
		t.Run(fmt.Sprintf("expires-%d", expires), func(t *testing.T) {
			a, b := historicalStateTestEngine(t)
			app := &pb.ProcessRef{InstanceId: "controller", Epoch: "epoch"}
			flow := &pb.FlowRef{Ingress: a.cfg.Process, ConnectionId: "original", SourceIp: "192.0.2.12", SourcePort: 50300, Listener: "sip.example.test:5061", Transport: "tls"}
			subscribe := func(sequence int, tag string) *pb.SipMessage {
				m := stateTestRequest("SUBSCRIBE", sequence, tag, fmt.Sprintf("subscribe-%d", sequence))
				wire := strings.Replace(string(m.Wire), "Content-Length:", "Event: message-summary\r\nExpires: 90\r\nContent-Length:", 1)
				observed, err := Observe([]byte(wire), false)
				if err != nil {
					t.Fatal(err)
				}
				return observed
			}
			initial := subscribe(1, "")
			ref, err := a.Locate(context.Background(), initial)
			if err != nil {
				t.Fatal(err)
			}
			ingest := func(id string, request *pb.SipMessage) string {
				result := stateTestApply(t, b, ref, id, stateEngineInput{Kind: "ingest", Application: app, Ingest: &pb.OwnerIngest{Context: ref, Application: app, Flow: flow, Wire: request.Wire, PrincipalId: "principal", BindingId: "binding"}})
				if result.Error != "" {
					t.Fatal(result.Error)
				}
				_, group := stateTestLoad(t, b, ref)
				for handle, key := range group.Contexts[ref.Id].Requests {
					if string(group.Contexts[ref.Id].Transactions[key].Native.Origin.Wire) == string(request.Wire) {
						return handle
					}
				}
				t.Fatal("subscription request was not retained")
				return ""
			}
			accept := func(id, handle string, sequence uint64, expires int) {
				result := stateTestApply(t, b, ref, id, stateEngineInput{Kind: "command", Application: app, Command: &pb.Command{Context: ref, Sequence: sequence, Action: &pb.Command_Respond{Respond: &pb.Respond{RequestId: handle, Status: 202, Headers: []*pb.Header{{Name: "Expires", Value: fmt.Sprint(expires)}}}}}})
				if result.Error != "" {
					t.Fatal(result.Error)
				}
			}
			accept("initial-accept", ingest("initial", initial), 1, 90)
			_, group := stateTestLoad(t, b, ref)
			before := group.Contexts[ref.Id]
			if before.Subscription == nil || before.Delivery != nil {
				t.Fatal("fixture must begin with an accepted, unversioned initial subscription")
			}
			accept("refresh-accept", ingest("refresh", subscribe(2, before.Snapshot.LocalTag)), 2, expires)
			record, group := stateTestLoad(t, a, ref)
			pending := group.Contexts[ref.Id]
			if pending.Subscription.ExpiresMS != before.Subscription.ExpiresMS || pending.Subscription.Terminating || pending.Delivery != nil {
				t.Fatal("same-flow refresh changed its accepted lease before publication")
			}
			var adoption state.Effect
			var proposal *pb.FlowAdoptionRequest
			for _, effect := range record.Payload.Effects {
				if effect.Kind == "adopt" {
					var body stateEngineEffect
					if err := unmarshalRetainedFixture(effect.Payload, &body); err != nil {
						t.Fatal(err)
					}
					adoption, proposal = effect, body.Envelope.GetFlowAdoption()
				}
			}
			if proposal == nil || !proposal.Subscription || proposal.NativeRevision == 0 || !proto.Equal(proposal.Flow, flow) || !sameProcess(proposal.Application, app) {
				t.Fatal("same-flow refresh did not request an exact subscription mutation receipt")
			}
			stateTestApply(t, a, ref, "publication-start", stateEngineInput{Kind: "effect-start", Effect: adoption.ID})
			receipt := &pb.ContextDelivery{Flow: flow, NativeRevision: proposal.NativeRevision, DirectoryEpoch: "directory"}
			stateTestApply(t, a, ref, "publication-complete", stateEngineInput{Kind: "effect-result", Executor: a.cfg.Process, Effect: adoption.ID, Reply: &pb.Envelope{Payload: &pb.Envelope_FlowAdoptionResult{FlowAdoptionResult: &pb.FlowAdoptionResult{Receipt: receipt}}}})
			_, group = stateTestLoad(t, b, ref)
			accepted := group.Contexts[ref.Id]
			if !proto.Equal(accepted.Result.GetSnapshot().GetDelivery(), receipt) || accepted.Subscription.Terminating != (expires == 0) || !proto.Equal(accepted.Flow, flow) {
				t.Fatal("accepted response did not retain the same-flow mutation fence and lease")
			}
			if expires == 0 {
				result := stateTestApply(t, b, ref, "terminal-notify", stateEngineInput{Kind: "command", Application: app, Command: &pb.Command{Context: ref, Sequence: 3, Action: &pb.Command_DialogRequest{DialogRequest: &pb.DialogRequest{Method: "NOTIFY", Headers: []*pb.Header{{Name: "Subscription-State", Value: "terminated;reason=timeout"}}}}}})
				if result.Error != "" {
					t.Fatalf("accepted unsubscribe could not send its terminal NOTIFY: %s", result.Error)
				}
			}
		})
	}
}
