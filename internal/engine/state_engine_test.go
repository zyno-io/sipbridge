package engine

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/emiago/sipgo/sip"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/execution"
	executionfixture "sipbridge/internal/execution/fixture"
	"sipbridge/internal/state"
)

func historicalStateTestEngine(t *testing.T, overrides ...state.Limits) (*StateEngine, *StateEngine) {
	return historicalStateTestEnginePartitioned(t, 1, overrides...)
}

func historicalStateTestEnginePartitioned(t *testing.T, partitions uint16, overrides ...state.Limits) (*StateEngine, *StateEngine) {
	t.Helper()
	address := os.Getenv("SIPBRIDGE_STATE_TEST_REDIS")
	if address == "" {
		t.Skip("set SIPBRIDGE_STATE_TEST_REDIS for Redis-native engine qualification")
	}
	ctx := context.Background()
	prefix := "sipstate:engine-test"
	namespace := "test-" + uuid.NewString()
	key := bytes.Repeat([]byte{17}, 32)
	limits := state.Limits{}
	if len(overrides) > 0 {
		limits = overrides[0]
	}
	s, err := state.Open(ctx, state.Config{Redis: state.RedisOptions{Address: address}, Prefix: prefix, Namespace: namespace, NamespaceKey: key, Partitions: partitions, Generation: "generation", Limits: limits, Versions: state.Versions{Schema: "1", Reducer: "7", Timers: "1"}})
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Ready(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		client := redis.NewClient(&redis.Options{Addr: address})
		defer client.Close()
		var cursor uint64
		for {
			keys, next, err := client.Scan(ctx, cursor, prefix+":"+namespace+":*", 128).Result()
			if err != nil {
				t.Error(err)
				break
			}
			if len(keys) > 0 {
				if err = client.Del(ctx, keys...).Err(); err != nil {
					t.Error(err)
				}
			}
			cursor = next
			if cursor == 0 {
				break
			}
		}
		s.Close()
	})
	makeEngine := func(name string) *StateEngine {
		e := &StateEngine{cfg: Config{Process: &pb.ProcessRef{InstanceId: name, Epoch: "epoch"}, AdvertiseAddress: "sip.example.test:5061", ContactURI: "sip:sipbridge@sip.example.test:5061;transport=tls", StateIdentityKey: key}, store: s, ctx: ctx}
		e.runtime, err = executionfixture.New(s, execution.ReducerFunc(e.reduce), name, 8)
		if err != nil {
			t.Fatal(err)
		}
		return e
	}
	return makeEngine("sip-a"), makeEngine("sip-b")
}
func stateTestRequest(method string, cseq int, toTag, branch string) *pb.SipMessage {
	to := "<sip:service@example.test>"
	if toTag != "" {
		to += ";tag=" + toTag
	}
	wire := fmt.Sprintf("%s sip:service@example.test SIP/2.0\r\nVia: SIP/2.0/TLS phone.example.test:5061;branch=z9hG4bK%s;rport\r\nFrom: <sip:alice@example.test>;tag=remote-tag\r\nTo: %s\r\nCall-ID: redis-native-call@example.test\r\nCSeq: %d %s\r\nContact: <sip:alice@phone.example.test:5061;transport=tls>\r\nContent-Length: 0\r\n\r\n", method, branch, to, cseq, method)
	message, err := Observe([]byte(wire), false)
	if err != nil {
		panic(err)
	}
	return message
}
func stateTestApply(t *testing.T, e *StateEngine, ref *pb.ContextRef, id string, in stateEngineInput) *pb.OperationResult {
	t.Helper()
	group, err := e.group(ref)
	if err != nil {
		t.Fatal(err)
	}
	in.Context = ref
	in.Operation = id
	if respond := in.Command.GetRespond(); respond != nil {
		record, loadErr := e.store.Load(context.Background(), group)
		if loadErr == nil {
			decoded, decodeErr := decodeStateGroup(record)
			if decodeErr != nil {
				t.Fatal(decodeErr)
			}
			if c := decoded.Contexts[ref.Id]; c != nil && !c.OwnerLost {
				tx := c.Transactions[c.Requests[respond.RequestId]]
				if tx != nil && tx.OwnerDeferred && sameProcess(tx.Application, in.Application) && c.OwnerProbeEffect != "" {
					// A business-handler response fixture represents a handler
					// which was presented only after authoritative owner proof.
					epoch := c.OwnerDirectoryEpoch
					if epoch == "" {
						epoch = "directory"
					}
					proof := stateEngineInput{Kind: "owner-result", Context: ref, Effect: c.OwnerProbeEffect, Reply: &pb.Envelope{Payload: &pb.Envelope_ApplicationStatus{ApplicationStatus: &pb.ApplicationStatus{Context: ref, Application: in.Application, DirectoryEpoch: epoch, Live: true, CheckedAtMs: uint64(time.Now().UnixMilli())}}}}
					result, proofErr := e.apply(context.Background(), group, e.id("fixture-owner", c.OwnerProbeEffect), proof, time.Now().Add(2*time.Second), false)
					if proofErr != nil || result.Error != "" {
						t.Fatalf("owner proof fixture: %+v %v", result, proofErr)
					}
				}
			}
		}
	}
	result, err := e.apply(context.Background(), group, id, in, time.Now().Add(10*time.Second), false)
	if err != nil {
		t.Fatal(err)
	}
	return result
}
func stateTestLoad(t testing.TB, e *StateEngine, ref *pb.ContextRef) (state.Record, stateGroup) {
	t.Helper()
	group, err := e.group(ref)
	if err != nil {
		t.Fatal(err)
	}
	record, err := e.store.Load(context.Background(), group)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := decodeStateGroup(record)
	if err != nil {
		t.Fatal(err)
	}
	return record, decoded
}

func TestStateEngineCrossReplicaUASAndRejectedSequenceRollback(t *testing.T) {
	a, b := historicalStateTestEngine(t)
	ctx := context.Background()
	initial := stateTestRequest("INVITE", 1, "", "initial")
	ref, err := a.Locate(ctx, initial)
	if err != nil {
		t.Fatal(err)
	}
	same, err := b.Locate(ctx, initial)
	if err != nil {
		t.Fatal(err)
	}
	if !stateSameContext(ref, same) || same.Owner.InstanceId != "sip-b" {
		t.Fatal("logical identity is pinned to physical executor")
	}
	application := &pb.ProcessRef{InstanceId: "controller", Epoch: "controller-epoch"}
	flow := &pb.FlowRef{Ingress: a.cfg.Process, ConnectionId: "flow-a", SourceIp: "192.0.2.10", SourcePort: 50000, Listener: "sip.example.test:5061", Transport: "tls"}
	result := stateTestApply(t, a, ref, "ingest-initial", stateEngineInput{Kind: "ingest", Application: application, Ingest: &pb.OwnerIngest{Context: ref, Application: application, Flow: flow, Wire: initial.Wire}})
	if result.Error != "" {
		t.Fatal(result.Error)
	}
	_, group := stateTestLoad(t, b, ref)
	c := group.Contexts[ref.Id]
	var requestID string
	for id := range c.Requests {
		requestID = id
	}
	if requestID == "" {
		t.Fatal("native request not retained")
	}
	if c.Snapshot.LocalTag == "" || c.Snapshot.LocalTag != c.Tag {
		t.Fatal("initial presentation omitted native reserved local tag")
	}

	result = stateTestApply(t, b, ref, "answer", stateEngineInput{Kind: "command", Application: application, Command: &pb.Command{Context: ref, Sequence: 1, Action: &pb.Command_Respond{Respond: &pb.Respond{RequestId: requestID, Status: 200}}}})
	if result.Error != "" || result.Snapshot == nil || !result.Snapshot.Answered {
		t.Fatalf("answer after takeover: %+v", result)
	}
	localTag := result.Snapshot.LocalTag
	flowB := &pb.FlowRef{Ingress: b.cfg.Process, ConnectionId: "flow-b", SourceIp: "192.0.2.20", SourcePort: 51000, Listener: "sip.example.test:5061", Transport: "tls"}
	ack := stateTestRequest("ACK", 1, localTag, "ack")
	result = stateTestApply(t, b, ref, "ack", stateEngineInput{Kind: "ingest", Application: application, Ingest: &pb.OwnerIngest{Context: ref, Application: application, Flow: flowB, Wire: ack.Wire, Admission: pb.OwnerIngest_TLS_DIALOG_CONTINUITY}})
	if result.Error != "" {
		t.Fatal(result.Error)
	}
	_, group = stateTestLoad(t, a, ref)
	before := group.Contexts[ref.Id].Dialogs[group.Contexts[ref.Id].Dialog]
	wrong := stateTestRequest("INVITE", 999, "wrong-tag", "wrong")
	result = stateTestApply(t, a, ref, "wrong-tags", stateEngineInput{Kind: "ingest", Application: application, Ingest: &pb.OwnerIngest{Context: ref, Application: application, Flow: flowB, Wire: wrong.Wire, Admission: pb.OwnerIngest_TLS_DIALOG_CONTINUITY}})
	if result.SipStatus != 481 {
		t.Fatalf("wrong tags: %+v", result)
	}
	_, group = stateTestLoad(t, b, ref)
	after := group.Contexts[ref.Id].Dialogs[group.Contexts[ref.Id].Dialog]
	if after.RemoteCSeq != before.RemoteCSeq || after.Phase != before.Phase {
		t.Fatal("rejected request mutated dialog state")
	}
	valid := stateTestRequest("INVITE", 2, localTag, "reinvite")
	result = stateTestApply(t, b, ref, "valid-reinvite", stateEngineInput{Kind: "ingest", Application: application, Ingest: &pb.OwnerIngest{Context: ref, Application: application, Flow: flowB, Wire: valid.Wire, Admission: pb.OwnerIngest_TLS_DIALOG_CONTINUITY}})
	if result.Error != "" {
		t.Fatalf("valid lower peer CSeq after rejection: %s", result.Error)
	}
}

func TestStateEngineCommittedResponseFlowAndTerminalWriteEvidence(t *testing.T) {
	a, other := historicalStateTestEngine(t)
	ctx := context.Background()
	message := stateTestRequest("OPTIONS", 1, "", "options")
	ref, err := a.Locate(ctx, message)
	if err != nil {
		t.Fatal(err)
	}
	application := &pb.ProcessRef{InstanceId: "controller", Epoch: "epoch"}
	flow := &pb.FlowRef{Ingress: a.cfg.Process, ConnectionId: "flow", SourceIp: "192.0.2.30", SourcePort: 50001, Listener: "sip.example.test:5061", Transport: "tls"}
	result := stateTestApply(t, a, ref, "ingest", stateEngineInput{Kind: "ingest", Application: application, Ingest: &pb.OwnerIngest{Context: ref, Application: application, Flow: flow, Wire: message.Wire}})
	if result.Error != "" {
		t.Fatal(result.Error)
	}
	_, group := stateTestLoad(t, a, ref)
	var requestID string
	for id := range group.Contexts[ref.Id].Requests {
		requestID = id
	}
	result = stateTestApply(t, a, ref, "respond", stateEngineInput{Kind: "command", Application: application, Command: &pb.Command{Context: ref, Sequence: 1, Action: &pb.Command_Respond{Respond: &pb.Respond{RequestId: requestID, Status: 200}}}})
	if result.Error != "" {
		t.Fatal(result.Error)
	}
	record, _ := stateTestLoad(t, a, ref)
	var selected state.Effect
	var body stateEngineEffect
	for _, effect := range record.Payload.Effects {
		if effect.Kind == "wire" {
			selected = effect
			if err = unmarshalRetainedFixture(effect.Payload, &body); err != nil {
				t.Fatal(err)
			}
		}
	}
	if selected.ID == "" || body.Authorization.AuthorityClass != pb.EffectAuthorization_RESPONSE || !proto.Equal(body.Flow, flow) {
		t.Fatal("response effect lost request-specific flow")
	}
	permit := &pb.SendPermit{Authorization: body.Authorization, Flow: flow, DirectoryEpoch: "directory", OrderingProof: "proof"}
	stateTestApply(t, a, ref, "start-authorize", stateEngineInput{Kind: "effect-start", Effect: selected.ID})
	result = stateTestApply(t, a, ref, "authorize", stateEngineInput{Kind: "effect-result", Effect: selected.ID, Executor: a.cfg.Process, Reply: &pb.Envelope{Payload: &pb.Envelope_SendPermit{SendPermit: permit}}})
	if result.Error != "" {
		t.Fatal(result.Error)
	}
	write := &pb.FlowWrite{Flow: flow, Effect: body.Authorization.Effect, Permit: permit}
	reserved, err := a.Reserve(ctx, write)
	if err != nil {
		t.Fatal(err)
	}
	if reserved.Known || len(reserved.Wire) == 0 {
		t.Fatal("committed response not reserved")
	}
	parsed, err := sip.ParseMessage(reserved.Wire)
	if err != nil {
		t.Fatal(err)
	}
	if parsed.Via().Params.GetOr("rport", "") != "50001" {
		t.Fatal("observed response rport lost")
	}
	if err = a.Complete(ctx, write, &pb.DeliveryResult{Outcome: pb.DeliveryResult_UNKNOWN, Effect: write.Effect}); err != nil {
		t.Fatal(err)
	}
	groupRef, err := a.group(ref)
	if err != nil {
		t.Fatal(err)
	}
	quotaRecord, err := a.store.Load(ctx, groupRef)
	if err != nil {
		t.Fatal(err)
	}
	for n := len(quotaRecord.Receipts); n < a.store.Limits().Receipts; n++ {
		id := fmt.Sprintf("quota-%d", n)
		candidate := state.Input{ID: id, Kind: "ordinary", Fingerprint: id, Payload: []byte(id), DeadlineMS: time.Now().Add(time.Minute).UnixMilli(), ReceiptUntilMS: time.Now().Add(2 * time.Minute).UnixMilli()}
		begun, beginErr := a.store.(*state.Store).Begin(ctx, groupRef, "quota-worker", candidate)
		if beginErr != nil {
			t.Fatal(beginErr)
		}
		if _, commitErr := a.store.(*state.Store).Commit(ctx, *begun.Step, begun.Step.Record.Payload, nil); commitErr != nil {
			t.Fatal(commitErr)
		}
	}
	denied := state.Input{ID: "denied-fresh", Kind: "ordinary", Fingerprint: "new-work", DeadlineMS: time.Now().Add(time.Minute).UnixMilli(), ReceiptUntilMS: time.Now().Add(2 * time.Minute).UnixMilli()}
	if _, beginErr := a.store.(*state.Store).Begin(ctx, groupRef, "fresh-work", denied); !errors.Is(beginErr, state.ErrCapacity) {
		t.Fatalf("fresh work bypassed full reconciliation history: %v", beginErr)
	}
	if err = a.Complete(ctx, write, &pb.DeliveryResult{Outcome: pb.DeliveryResult_WRITTEN, Effect: write.Effect}); err != nil {
		t.Fatal(err)
	}
	repeated, err := a.Reserve(ctx, write)
	if err != nil {
		t.Fatal(err)
	}
	if !repeated.Known || repeated.Outcome != pb.DeliveryResult_WRITTEN || len(repeated.Wire) != 0 {
		t.Fatal("cached reservation replay restarted a terminal physical attempt")
	}
	// Inspection uses the shared authority, not the old physical sink.
	inspected, err := other.InspectWrite(ctx, write)
	if err != nil || inspected.Outcome != pb.DeliveryResult_WRITTEN || !inspected.Recorded {
		t.Fatalf("cross-replica durable write inspection: %+v %v", inspected, err)
	}
	settled := stateTestApply(t, a, ref, "settled", stateEngineInput{Kind: "effect-result", Effect: write.Effect.Id, Delivery: &pb.DeliveryResult{Outcome: pb.DeliveryResult_WRITTEN, Effect: write.Effect, Recorded: true}, Reply: &pb.Envelope{Payload: &pb.Envelope_Result{Result: &pb.OperationResult{Certainty: pb.OperationResult_COMPLETED}}}})
	if settled.Error != "" {
		t.Fatal(settled.Error)
	}
	finalRecord, finalGroup := stateTestLoad(t, a, ref)
	for _, effect := range finalRecord.Payload.Effects {
		if effect.ID == write.Effect.Id {
			t.Fatal("settled effect did not release its reserved slot")
		}
	}
	written := false
	for _, tx := range finalGroup.Contexts[ref.Id].Transactions {
		written = written || tx.Written
	}
	if !written {
		t.Fatal("settlement reclamation lost transaction written proof")
	}

}

func TestStateEngineReconnectPublicationDefersPresentation(t *testing.T) {
	a, b := historicalStateTestEngine(t)
	ctx := context.Background()
	message := stateTestRequest("INVITE", 1, "", "initial")
	ref, err := a.Locate(ctx, message)
	if err != nil {
		t.Fatal(err)
	}
	app := &pb.ProcessRef{InstanceId: "controller", Epoch: "epoch"}
	flowA := &pb.FlowRef{Ingress: a.cfg.Process, ConnectionId: "original", SourceIp: "192.0.2.11", SourcePort: 50100, Listener: "sip.example.test:5061", Transport: "tls"}
	stateTestApply(t, a, ref, "initial", stateEngineInput{Kind: "ingest", Application: app, Ingest: &pb.OwnerIngest{Context: ref, Application: app, Flow: flowA, Wire: message.Wire}})
	_, g := stateTestLoad(t, a, ref)
	var initialID string
	for id := range g.Contexts[ref.Id].Requests {
		initialID = id
	}
	answer := stateTestApply(t, b, ref, "answer", stateEngineInput{Kind: "command", Application: app, Command: &pb.Command{Context: ref, Sequence: 1, Action: &pb.Command_Respond{Respond: &pb.Respond{RequestId: initialID, Status: 200}}}})
	ack := stateTestRequest("ACK", 1, answer.Snapshot.LocalTag, "ack")
	stateTestApply(t, a, ref, "ack", stateEngineInput{Kind: "ingest", Application: app, Ingest: &pb.OwnerIngest{Context: ref, Application: app, Flow: flowA, Wire: ack.Wire}})
	flowB := proto.Clone(flowA).(*pb.FlowRef)
	flowB.Ingress = b.cfg.Process
	flowB.ConnectionId = "replacement"
	flowB.SourcePort = 50200
	reinvite := stateTestRequest("INVITE", 2, answer.Snapshot.LocalTag, "reinvite")
	stateTestApply(t, b, ref, "reinvite", stateEngineInput{Kind: "ingest", Application: app, Ingest: &pb.OwnerIngest{Context: ref, Application: app, Flow: flowB, Wire: reinvite.Wire, Admission: pb.OwnerIngest_TLS_DIALOG_CONTINUITY}})
	record, g := stateTestLoad(t, b, ref)
	c := g.Contexts[ref.Id]
	if !proto.Equal(c.Flow, flowA) {
		t.Fatal("future route changed before publication receipt")
	}
	var adoption state.Effect
	var body stateEngineEffect
	for _, effect := range record.Payload.Effects {
		var candidate stateEngineEffect
		if err = unmarshalRetainedFixture(effect.Payload, &candidate); err != nil {
			t.Fatal(err)
		}
		if effect.Kind == "adopt" {
			adoption = effect
			body = candidate
		}
		if effect.Kind == "present" && candidate.Envelope.GetRequest() != nil && candidate.Envelope.GetRequest().Message.Method == "INVITE" && candidate.Transaction != c.Requests[initialID] {
			t.Fatal("reconnected request presented before route publication")
		}
	}
	if adoption.ID == "" {
		t.Fatal("reconnect has no retained publication")
	}
	stateTestApply(t, a, ref, "start-adoption", stateEngineInput{Kind: "effect-start", Effect: adoption.ID})
	proposal := body.Envelope.GetFlowAdoption()
	result := stateTestApply(t, a, ref, "adopted", stateEngineInput{Kind: "effect-result", Executor: a.cfg.Process, Effect: adoption.ID, Reply: &pb.Envelope{Payload: &pb.Envelope_FlowAdoptionResult{FlowAdoptionResult: &pb.FlowAdoptionResult{Receipt: &pb.ContextDelivery{Flow: flowB, NativeRevision: proposal.NativeRevision, DirectoryEpoch: "directory", RegistrationTicket: "ticket"}}}}})
	if result.Error != "" {
		t.Fatal(result.Error)
	}
	record, g = stateTestLoad(t, b, ref)
	if !proto.Equal(g.Contexts[ref.Id].Flow, flowB) {
		t.Fatal("accepted publication did not replace future route")
	}
	if g.Contexts[ref.Id].Transactions[body.Transaction].DeferredPresent {
		t.Fatal("accepted publication did not release deferred request")
	}
	for _, effect := range record.Payload.Effects {
		if effect.Kind == "present" {
			t.Fatal("accepted publication persisted presentation")
		}
	}
}

func TestStateEngineSubscriptionPolicyAndPublicationFenceLease(t *testing.T) {
	a, b := historicalStateTestEngine(t)
	ctx := context.Background()
	subscribe := func(seq int, tag, branch string) *pb.SipMessage {
		m := stateTestRequest("SUBSCRIBE", seq, tag, branch)
		wire := strings.Replace(string(m.Wire), "Content-Length:", "Event: message-summary\r\nExpires: 90\r\nContent-Length:", 1)
		observed, err := Observe([]byte(wire), false)
		if err != nil {
			t.Fatal(err)
		}
		return observed
	}
	initial := subscribe(1, "", "subscribe")
	ref, err := a.Locate(ctx, initial)
	if err != nil {
		t.Fatal(err)
	}
	appA := &pb.ProcessRef{InstanceId: "controller-a", Epoch: "epoch"}
	appB := &pb.ProcessRef{InstanceId: "controller-b", Epoch: "epoch"}
	flowA := &pb.FlowRef{Ingress: a.cfg.Process, ConnectionId: "original", SourceIp: "192.0.2.12", SourcePort: 50300, Listener: "sip.example.test:5061", Transport: "tls"}
	stateTestApply(t, a, ref, "initial", stateEngineInput{Kind: "ingest", Application: appA, Ingest: &pb.OwnerIngest{Context: ref, Application: appA, Flow: flowA, Wire: initial.Wire, PrincipalId: "principal", BindingId: "binding"}})
	_, g := stateTestLoad(t, a, ref)
	var requestID string
	for id := range g.Contexts[ref.Id].Requests {
		requestID = id
	}
	result := stateTestApply(t, a, ref, "accept", stateEngineInput{Kind: "command", Application: appA, Command: &pb.Command{Context: ref, Sequence: 1, Action: &pb.Command_Respond{Respond: &pb.Respond{RequestId: requestID, Status: 202, Headers: []*pb.Header{{Name: "Expires", Value: "90"}}}}}})
	if result.Error != "" {
		t.Fatal(result.Error)
	}
	_, g = stateTestLoad(t, a, ref)
	c := g.Contexts[ref.Id]
	if c.Subscription == nil {
		t.Fatal("subscription lease not retained")
	}
	oldExpiry := c.Subscription.ExpiresMS
	flowB := proto.Clone(flowA).(*pb.FlowRef)
	flowB.Ingress = b.cfg.Process
	flowB.ConnectionId = "replacement"
	flowB.SourcePort = 50400
	renewal := subscribe(2, c.Snapshot.LocalTag, "renew")
	result = stateTestApply(t, b, ref, "renewal", stateEngineInput{Kind: "ingest", Application: appB, Ingest: &pb.OwnerIngest{Context: ref, Application: appB, Flow: flowB, Wire: renewal.Wire, PrincipalId: "principal", BindingId: "binding", Admission: pb.OwnerIngest_TLS_DIALOG_CONTINUITY}})
	if result.Error != "" {
		t.Fatal(result.Error)
	}
	record, g := stateTestLoad(t, b, ref)
	c = g.Contexts[ref.Id]
	if !proto.Equal(c.Flow, flowA) || c.Subscription.ExpiresMS != oldExpiry {
		t.Fatal("unaccepted refresh changed accepted lease or route")
	}
	for id, key := range c.Requests {
		if c.Transactions[key].Native.Origin.Wire != nil && string(c.Transactions[key].Native.Origin.Wire) == string(renewal.Wire) {
			requestID = id
		}
	}
	stateTestApply(t, b, ref, "accept-renewal", stateEngineInput{Kind: "command", Application: appB, Command: &pb.Command{Context: ref, Sequence: 2, Action: &pb.Command_Respond{Respond: &pb.Respond{RequestId: requestID, Status: 202, Headers: []*pb.Header{{Name: "Expires", Value: "180"}}}}}})
	record, g = stateTestLoad(t, b, ref)
	if g.Contexts[ref.Id].Subscription.ExpiresMS != oldExpiry {
		t.Fatal("lease changed before publication receipt")
	}
	var adoption state.Effect
	var body stateEngineEffect
	for _, effect := range record.Payload.Effects {
		if effect.Kind == "adopt" {
			adoption = effect
			if err = unmarshalRetainedFixture(effect.Payload, &body); err != nil {
				t.Fatal(err)
			}
		}
	}
	if adoption.ID == "" || !body.Envelope.GetFlowAdoption().Subscription || !sameProcess(body.Application, appB) {
		t.Fatal("refresh has no exact new-processor publication")
	}
	stateTestApply(t, a, ref, "start-renew-publication", stateEngineInput{Kind: "effect-start", Effect: adoption.ID})
	proposal := body.Envelope.GetFlowAdoption()
	stateTestApply(t, a, ref, "renew-published", stateEngineInput{Kind: "effect-result", Executor: a.cfg.Process, Effect: adoption.ID, Reply: &pb.Envelope{Payload: &pb.Envelope_FlowAdoptionResult{FlowAdoptionResult: &pb.FlowAdoptionResult{Receipt: &pb.ContextDelivery{Flow: flowB, NativeRevision: proposal.NativeRevision, DirectoryEpoch: "directory"}}}}})
	_, g = stateTestLoad(t, b, ref)
	c = g.Contexts[ref.Id]
	if c.Subscription.ExpiresMS <= oldExpiry || !proto.Equal(c.Flow, flowB) || !sameProcess(c.Snapshot.Application, appB) {
		t.Fatal("accepted publication did not atomically commit renewal")
	}
	before, oldGroup := stateTestLoad(t, b, ref)
	oldContext := oldGroup.Contexts[ref.Id]
	notify := &pb.Command{Context: ref, Sequence: 3, Action: &pb.Command_DialogRequest{DialogRequest: &pb.DialogRequest{Method: "NOTIFY", Headers: []*pb.Header{{Name: "Content-Type", Value: "application/simple-message-summary"}}, Body: []byte("Messages-Waiting: no\r\n")}}}
	denied := stateTestApply(t, a, ref, "old-processor-notify", stateEngineInput{Kind: "command", Application: appA, Command: notify})
	if denied.Certainty != pb.OperationResult_NOT_ACCEPTED {
		t.Fatal("old processor could allocate a NOTIFY after ownership handoff")
	}
	after, newGroup := stateTestLoad(t, b, ref)
	newContext := newGroup.Contexts[ref.Id]
	if newContext.Subscription.LocalSequence != oldContext.Subscription.LocalSequence || newContext.Snapshot.CommandSequence != oldContext.Snapshot.CommandSequence || newContext.Snapshot.Revision != oldContext.Snapshot.Revision || len(after.Payload.Effects) != len(before.Payload.Effects) {
		t.Fatal("old processor refusal mutated sequence or native effects")
	}
	allowed := stateTestApply(t, b, ref, "current-processor-notify", stateEngineInput{Kind: "command", Application: appB, Command: notify})
	if allowed.Error != "" || allowed.Certainty != pb.OperationResult_ACCEPTED {
		t.Fatalf("new processor could not notify: %+v", allowed)
	}
	_, notified := stateTestLoad(t, b, ref)
	if notified.Contexts[ref.Id].Subscription.LocalSequence != oldContext.Subscription.LocalSequence+1 {
		t.Fatal("authorized NOTIFY did not allocate the next native CSeq")
	}

}

func TestStateEngineRecoveredExpiredFreshCommandCannotEmit(t *testing.T) {
	a, _ := historicalStateTestEngine(t)
	ctx := context.Background()
	ref, err := a.Locate(ctx, stateTestRequest("INVITE", 1, "", "expired"))
	if err != nil {
		t.Fatal(err)
	}
	record, _ := stateTestLoad(t, a, ref)
	input := stateEngineInput{Kind: "command", Context: ref, Application: &pb.ProcessRef{InstanceId: "controller", Epoch: "epoch"}, Operation: "expired-open", Command: &pb.Command{Context: ref, Sequence: 1, Action: &pb.Command_OpenUac{OpenUac: &pb.OpenUac{Uri: "sip:phone@example.test", Transport: "tls"}}}}
	encoded, err := marshalRetainedFixture(input)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UnixMilli()
	next, outcome, err := a.reduce(record, state.Input{ID: "expired-open", Kind: "command", Payload: encoded, DeadlineMS: now - 1}, now)
	if err != nil {
		t.Fatal(err)
	}
	var result pb.OperationResult
	if err = unmarshalRetainedFixture(outcome, &result); err != nil {
		t.Fatal(err)
	}
	if result.Certainty != pb.OperationResult_NOT_ACCEPTED || len(next.Effects) != 0 || next.ActiveTransactions != 0 {
		t.Fatal("recovered expired command admitted fresh native output")
	}
}

func TestStateEngineOldDuplicateACKCannotDisplaceAdoptedFlow(t *testing.T) {
	a, b := historicalStateTestEngine(t)
	ctx := context.Background()
	invite := stateTestRequest("INVITE", 1, "", "initial")
	ref, err := a.Locate(ctx, invite)
	if err != nil {
		t.Fatal(err)
	}
	app := &pb.ProcessRef{InstanceId: "controller", Epoch: "epoch"}
	flowA := &pb.FlowRef{Ingress: a.cfg.Process, ConnectionId: "old-tls", SourceIp: "192.0.2.11", SourcePort: 50100, Listener: "sip.example.test:5061", Transport: "tls"}
	stateTestApply(t, a, ref, "initial", stateEngineInput{Kind: "ingest", Application: app, Ingest: &pb.OwnerIngest{Context: ref, Application: app, Flow: flowA, Wire: invite.Wire}})
	_, group := stateTestLoad(t, a, ref)
	var requestID string
	for id := range group.Contexts[ref.Id].Requests {
		requestID = id
	}
	answered := stateTestApply(t, a, ref, "answer", stateEngineInput{Kind: "command", Application: app, Command: &pb.Command{Context: ref, Sequence: 1, Action: &pb.Command_Respond{Respond: &pb.Respond{RequestId: requestID, Status: 200}}}})
	if answered.Error != "" {
		t.Fatal(answered.Error)
	}
	ack := stateTestRequest("ACK", 1, answered.Snapshot.LocalTag, "ack")
	flowB := proto.Clone(flowA).(*pb.FlowRef)
	flowB.Ingress = b.cfg.Process
	flowB.ConnectionId = "new-tls"
	flowB.SourcePort = 50200
	stateTestApply(t, b, ref, "fresh-ack", stateEngineInput{Kind: "ingest", Application: app, Ingest: &pb.OwnerIngest{Context: ref, Application: app, Flow: flowB, Wire: ack.Wire, Admission: pb.OwnerIngest_TLS_DIALOG_CONTINUITY}})
	record, _ := stateTestLoad(t, b, ref)
	var adoption state.Effect
	var body stateEngineEffect
	for _, effect := range record.Payload.Effects {
		if effect.Kind == "adopt" {
			adoption = effect
			if err = unmarshalRetainedFixture(effect.Payload, &body); err != nil {
				t.Fatal(err)
			}
		}
	}
	if adoption.ID == "" {
		t.Fatal("fresh ACK did not propose replacement flow")
	}
	stateTestApply(t, a, ref, "start-adoption", stateEngineInput{Kind: "effect-start", Effect: adoption.ID})
	proposal := body.Envelope.GetFlowAdoption()
	accepted := stateTestApply(t, a, ref, "adopted", stateEngineInput{Kind: "effect-result", Effect: adoption.ID, Executor: a.cfg.Process, Reply: &pb.Envelope{Payload: &pb.Envelope_FlowAdoptionResult{FlowAdoptionResult: &pb.FlowAdoptionResult{Receipt: &pb.ContextDelivery{Flow: flowB, NativeRevision: proposal.NativeRevision, DirectoryEpoch: "directory"}}}}})
	if accepted.Error != "" {
		t.Fatal(accepted.Error)
	}
	before, beforeGroup := stateTestLoad(t, b, ref)
	beforeContext := beforeGroup.Contexts[ref.Id]
	duplicate := stateTestApply(t, a, ref, "old-socket-duplicate-ack", stateEngineInput{Kind: "ingest", Application: app, Ingest: &pb.OwnerIngest{Context: ref, Application: app, Flow: flowA, Wire: ack.Wire, Admission: pb.OwnerIngest_TLS_DIALOG_CONTINUITY}})
	if duplicate.Error != "" || duplicate.Certainty != pb.OperationResult_ACCEPTED {
		t.Fatalf("duplicate ACK not accepted: %+v", duplicate)
	}
	after, afterGroup := stateTestLoad(t, b, ref)
	afterContext := afterGroup.Contexts[ref.Id]
	if !proto.Equal(afterContext.Flow, flowB) || !proto.Equal(afterContext.Delivery, beforeContext.Delivery) || afterContext.DeliveryRevision != beforeContext.DeliveryRevision || afterContext.Snapshot.Revision != beforeContext.Snapshot.Revision || len(after.Payload.Effects) != len(before.Payload.Effects) {
		t.Fatal("old duplicate ACK displaced the accepted route or emitted another adoption/event")
	}
}
