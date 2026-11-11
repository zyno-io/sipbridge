package engine

import (
	"context"
	"testing"

	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/state"
)

func stateOpenOnFlow(t *testing.T, a, b *StateEngine, application *pb.ProcessRef, flow *pb.FlowRef) *pb.ContextRef {
	t.Helper()
	ref, err := a.Locate(context.Background(), stateTestRequest("INVITE", 1, "", "response-flow"))
	if err != nil {
		t.Fatal(err)
	}
	result := stateTestApply(t, a, ref, "open-response-flow", stateEngineInput{Kind: "command", Application: application, Command: &pb.Command{Context: ref, Sequence: 1, Action: &pb.Command_OpenUac{OpenUac: &pb.OpenUac{Uri: "sip:phone@example.test", Body: []byte("v=0\r\n"), Network: &pb.NetworkTarget{Transport: "tls"}}}}})
	if result.Error != "" {
		t.Fatal(result.Error)
	}
	record, _ := stateTestLoad(t, b, ref)
	var selected state.Effect
	for _, effect := range record.Payload.Effects {
		if effect.Kind == "open" {
			selected = effect
		}
	}
	if selected.ID == "" {
		t.Fatal("initial network open intent missing")
	}
	stateTestApply(t, a, ref, "start-open-response-flow", stateEngineInput{Kind: "effect-start", Effect: selected.ID})
	result = stateTestApply(t, b, ref, "opened-response-flow", stateEngineInput{Kind: "effect-result", Effect: selected.ID, Executor: a.cfg.Process, Flow: flow})
	if result.Error != "" {
		t.Fatal(result.Error)
	}
	return ref
}

func stateWireBody(t *testing.T, record state.Record, method, remote string) stateEngineEffect {
	t.Helper()
	for index := len(record.Payload.Effects) - 1; index >= 0; index-- {
		var body stateEngineEffect
		if unmarshalRetainedFixture(record.Payload.Effects[index].Payload, &body) != nil || len(body.Wire) == 0 {
			continue
		}
		parsed, err := sip.ParseMessage(body.Wire)
		request, ok := parsed.(*sip.Request)
		if err == nil && ok && string(request.Method) == method && tag(request.To().Params) == remote {
			return body
		}
	}
	t.Fatalf("%s wire for branch %s missing", method, remote)
	return stateEngineEffect{}
}

func TestStateResponseOnlyTLSReconnectACKUsesBWithoutChangingContextFlow(t *testing.T) {
	a, b := historicalStateTestEngine(t)
	application := &pb.ProcessRef{InstanceId: "controller", Epoch: "owner"}
	old := &pb.FlowRef{Ingress: a.cfg.Process, ConnectionId: "old-a", SourceIp: "192.0.2.10", SourcePort: 5061, Listener: "sip.example.test:5061", Transport: "tls"}
	replacement := proto.Clone(old).(*pb.FlowRef)
	replacement.Ingress, replacement.ConnectionId, replacement.SourcePort = b.cfg.Process, "new-b", 51000
	ref := stateOpenOnFlow(t, a, b, application, old)
	_, group := stateTestLoad(t, b, ref)
	c := group.Contexts[ref.Id]
	var original *stateTransaction
	for _, tx := range c.Transactions {
		if tx.Native.Family == portable.InviteClient {
			original = tx
		}
	}
	if original == nil {
		t.Fatal("initial INVITE transaction missing")
	}
	accepted := forkReply(t, original, 200, "winner")
	result := stateTestApply(t, b, ref, "answer-on-b", stateEngineInput{Kind: "ingest", Application: application, Ingest: &pb.OwnerIngest{Context: ref, Application: application, Flow: replacement, Wire: accepted.Wire, Admission: pb.OwnerIngest_TLS_TRANSACTION_CONTINUITY}})
	if result.Error != "" {
		t.Fatal(result.Error)
	}
	record, group := stateTestLoad(t, a, ref)
	c = group.Contexts[ref.Id]
	ack := stateWireBody(t, record, "ACK", "winner")
	if !proto.Equal(ack.Flow, replacement) || !proto.Equal(c.Flow, old) || c.Delivery != nil || c.DeliveryRevision != 0 {
		t.Fatal("response-only reconnect lost ACK return flow or published ordinary delivery authority")
	}
	if !proto.Equal(c.Transactions[original.Native.Key].ResponseFlow, replacement) || !proto.Equal(c.DialogCompletionFlows[c.Dialog], replacement) {
		t.Fatal("transaction/branch completion flow not retained across replicas")
	}
	// Another fork on a third physical flow cannot move the winner's ACK path.
	losingFlow := proto.Clone(replacement).(*pb.FlowRef)
	losingFlow.ConnectionId, losingFlow.SourcePort = "fork-c", 52000
	loser := forkReply(t, original, 200, "loser")
	result = stateTestApply(t, a, ref, "loser-on-c", stateEngineInput{Kind: "ingest", Application: application, Ingest: &pb.OwnerIngest{Context: ref, Application: application, Flow: losingFlow, Wire: loser.Wire, Admission: pb.OwnerIngest_TLS_TRANSACTION_CONTINUITY}})
	if result.Error != "" {
		t.Fatal(result.Error)
	}
	record, group = stateTestLoad(t, b, ref)
	loserACK, loserBYE := stateWireBody(t, record, "ACK", "loser"), stateWireBody(t, record, "BYE", "loser")
	if !proto.Equal(loserACK.Flow, losingFlow) || !proto.Equal(loserBYE.Flow, losingFlow) || !proto.Equal(group.Contexts[ref.Id].Flow, old) {
		t.Fatal("losing branch completion flow crossed another branch or context route")
	}
	result = stateTestApply(t, b, ref, "winner-repeat-b", stateEngineInput{Kind: "ingest", Application: application, Ingest: &pb.OwnerIngest{Context: ref, Application: application, Flow: replacement, Wire: accepted.Wire, Admission: pb.OwnerIngest_TLS_TRANSACTION_CONTINUITY}})
	if result.Error != "" {
		t.Fatal(result.Error)
	}
	record, group = stateTestLoad(t, a, ref)
	if !proto.Equal(stateWireBody(t, record, "ACK", "winner").Flow, replacement) || !proto.Equal(group.Contexts[ref.Id].DialogCompletionFlows[group.Contexts[ref.Id].Snapshot.LocalTag+"/loser"], losingFlow) {
		t.Fatal("winner retransmission overwrote losing fork completion evidence")
	}
}

func TestStateResponseFlowRequiresNativeReplacementAdmissionAndKeepsCANCELProcedure(t *testing.T) {
	r, c, tx := forkFixture(t)
	replacement := proto.Clone(c.Flow).(*pb.FlowRef)
	replacement.ConnectionId = "replacement"
	provisional := forkReply(t, tx, 183, "early")
	in := &pb.OwnerIngest{Flow: replacement, Admission: pb.OwnerIngest_EXISTING_FLOW}
	if err := r.recordResponseFlow(c, tx, provisional, in); err == nil {
		t.Fatal("unproven replacement response accepted")
	}
	in.Admission = pb.OwnerIngest_TLS_TRANSACTION_CONTINUITY
	if err := r.recordResponseFlow(c, tx, provisional, in); err != nil {
		t.Fatal(err)
	}
	cancel, err := portable.BuildCancel(tx.Native.Origin)
	if err != nil {
		t.Fatal(err)
	}
	cancelNative, err := portable.NewTransaction(portable.NonInviteClient, cancel, portable.DefaultTimers(), "")
	if err != nil {
		t.Fatal(err)
	}
	generated := &stateTransaction{Native: cancelNative, Flow: c.Flow}
	if !proto.Equal(stateCompletionFlow(c, generated, cancel), replacement) || proto.Equal(c.Flow, replacement) {
		t.Fatal("CANCEL ignored parent response flow or changed context delivery")
	}
	plain := proto.Clone(replacement).(*pb.FlowRef)
	plain.Transport = "tcp"
	in.Flow = plain
	if err := r.recordResponseFlow(c, tx, provisional, in); err == nil {
		t.Fatal("replacement response lowered retained TLS transport")
	}
}
