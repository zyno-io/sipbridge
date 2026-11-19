package engine

import (
	"bytes"
	"context"
	"errors"
	"os"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/emiago/sipgo/sip/portable"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/rediscore"
	"sipbridge/internal/selectedmessage"
)

func TestRedisInviteReceivePublishesOneBoundedPendingProcedure(t *testing.T) {
	address := os.Getenv("SIPBRIDGE_CORE_TEST_REDIS")
	if address == "" {
		t.Skip("isolated Redis fixture required")
	}
	ctx := context.Background()
	client := redis.NewClient(&redis.Options{Addr: address, MaxRetries: -1})
	info, err := client.Info(ctx, "server").Result()
	if err != nil {
		t.Fatal(err)
	}
	primary := ""
	for _, line := range strings.Split(info, "\r\n") {
		if strings.HasPrefix(line, "run_id:") {
			primary = strings.TrimPrefix(line, "run_id:")
		}
	}
	root := "sipstate:test:invite-" + uuid.NewString() + ":"
	profile := []byte{8, 1}
	key := bytes.Repeat([]byte{1}, 32)
	process := &pb.ProcessRef{InstanceId: "worker", Epoch: "epoch"}
	worker, err := nativeWorkerReference(process)
	if err != nil {
		t.Fatal(err)
	}
	application := &pb.ProcessRef{InstanceId: "application", Epoch: "epoch"}
	app, err := nativeApplicationReference(key, application)
	if err != nil {
		t.Fatal(err)
	}
	decider := &pb.ProcessRef{InstanceId: "forwarded-application", Epoch: "epoch"}
	deciderApp, err := nativeApplicationReference(key, decider)
	if err != nil {
		t.Fatal(err)
	}
	keys := []string{"core:profile", "core:capacity", "core:selectors", "core:applications", "core:owner:term", "core:owned:term", "core:owner:decider-term", "core:owned:decider-term", "core:owner-expiry", "core:coordinator", "core:worker:" + worker, "core:timers:" + worker, "core:ready:" + worker, "core:context:context", "core:deadlines:context", "core:context:bound", "core:deadlines:bound", "core:context:final", "core:deadlines:final", "core:context:refused", "core:deadlines:refused"}
	t.Cleanup(func() {
		for _, name := range keys {
			if e := client.Del(ctx, root+name).Err(); e != nil {
				t.Error(e)
			}
		}
		if e := client.Del(ctx, root+"core:application:"+app).Err(); e != nil {
			t.Error(e)
		}
		if e := client.Del(ctx, root+"core:application:"+deciderApp).Err(); e != nil {
			t.Error(e)
		}
		_ = client.Close()
	})
	if err = client.Set(ctx, root+"core:profile", profile, 0).Err(); err != nil {
		t.Fatal(err)
	}
	store, err := rediscore.New(client, rediscore.Config{Root: root, Profile: profile, PrimaryRunID: primary, RecordBudget: 4 << 20})
	if err != nil {
		t.Fatal(err)
	}
	lease, err := store.AcquireApplication(ctx, app, "term")
	if err != nil {
		t.Fatal(err)
	}
	deciderLease, err := store.AcquireApplication(ctx, deciderApp, "decider-term")
	if err != nil {
		t.Fatal(err)
	}
	_, err = store.RenewWorker(ctx, rediscore.WorkerLeaseRequest{Worker: worker, Epoch: process.Epoch, PrimaryRunID: primary, Acquire: true})
	if err != nil {
		t.Fatal(err)
	}
	flow := &pb.FlowRef{Ingress: process, ConnectionId: "phone", SourceIp: "192.0.2.12", SourcePort: 5061, Listener: "sip.example.test:5061", Transport: "tls"}
	message, err := portable.ParseMessage(stateTestRequest("INVITE", 1, "", "invite").Wire, "tls", "192.0.2.12:5061", "")
	if err != nil {
		t.Fatal(err)
	}
	facts, err := message.Facts()
	if err != nil {
		t.Fatal(err)
	}
	authority := &pb.StateAuthority{Service: "svc", Generation: "gen", PrimaryId: "primary", SchemaVersion: 1}
	ref := &pb.ContextRef{Id: "context", Owner: process, State: &pb.StateRef{Authority: authority, GroupId: "context", Incarnation: "incarnation", CallId: facts.CallID}}
	requestID := uuid.NewString()
	in := redisRegisterReceive{IdentityKey: key, Context: ref, Authority: authority, Application: application, Executor: process, Lease: lease.ApplicationLeaseRef, Flow: flow, Message: message, RequestID: requestID, LocalTag: "local", Timer: rediscore.TimerAssignment{Worker: worker, Epoch: process.Epoch, Term: process.Epoch}, Deadline: time.Now().Add(time.Second)}
	rejected := in
	rejected.Lease.Term = "wrong"
	if _, err = receiveRedisInvite(ctx, store, rejected); !errors.Is(err, rediscore.ErrAuthority) {
		t.Fatalf("stale lease: %v", err)
	}
	presented, err := receiveRedisInvite(ctx, store, in)
	if err != nil {
		t.Fatal(err)
	}
	if presented.Snapshot.Kind != pb.ContextSnapshot_UAS || presented.RequestId != requestID || presented.Message.Method != "INVITE" {
		t.Fatal("wrong INVITE presentation")
	}
	identity := rediscore.Identity{Context: ref.Id, Incarnation: ref.State.Incarnation}
	view, err := store.Read(ctx, identity, "r:context", "r:procedure-"+requestID, "r:origin-"+requestID)
	if err != nil {
		t.Fatal(err)
	}
	storedContext, err := decodeNativeContext(view.Records["r:context"])
	if err != nil || len(storedContext.InitialProcedures) != 1 || storedContext.InitialProcedures[0] != "r:procedure-"+requestID || len(storedContext.Dialogs) != 0 {
		t.Fatal("initial INVITE associations changed", err)
	}
	procedure, err := decodeNativeProcedure(view.Records["r:procedure-"+requestID])
	if err != nil || procedure.Native.Family != portable.InviteServer || !procedure.Metadata.DecisionPending || procedure.Metadata.CreditContribution != uint64(redisInviteCompletionCredit(flow.Transport)) {
		t.Fatal("wrong INVITE procedure", err)
	}
	if !procedure.Native.Timers[portable.TimerTrying].Active {
		t.Fatal("Trying not retained")
	}
	for _, member := range []string{"call-tx-" + requestID + "-" + string(portable.TimerTrying), "call-decision-" + requestID} {
		if _, err = client.ZScore(ctx, root+"core:deadlines:context", member).Result(); err != nil {
			t.Fatal("missing original deadline", member, err)
		}
	}
	selectors, err := nativeSelectorIDs(message, false, false)
	if err != nil {
		t.Fatal(err)
	}
	selected, err := store.LookupSelector(ctx, selectors[0])
	if err != nil || selected.Identity != identity || selected.Record != "r:procedure-"+requestID {
		t.Fatal("INVITE selector", err)
	}
	if n, e := client.ZCard(ctx, root+"core:ready:"+worker).Result(); e != nil || n != 0 {
		t.Fatal("pending INVITE emitted early", n, e)
	}
	time.Sleep(225 * time.Millisecond)
	due, err := store.PeekDue(ctx, worker, process.Epoch)
	if err != nil || due == nil || due.Member != "call-tx-"+requestID+"-"+string(portable.TimerTrying) {
		t.Fatal("INVITE Trying not due", due, err)
	}
	if err = fireRedisInviteTrying(ctx, store, key, due, time.Now().Add(time.Second)); err != nil {
		t.Fatal("could not retain unbound Trying", err)
	}
	view, err = store.Read(ctx, identity, "r:context", "r:procedure-"+requestID, "r:origin-"+requestID)
	if err != nil {
		t.Fatal(err)
	}
	if n, e := client.ZCard(ctx, root+"core:ready:"+worker).Result(); e != nil || n != 0 {
		t.Fatal("unbound Trying created a physical output", n, e)
	}
	decision := residentDirectDecision(presented, 1, 0)
	bad := proto.Clone(decision).(*pb.RequestDecision)
	bad.Body = []byte("not allowed")
	if _, err = commitRedisInviteDeferral(ctx, store, key, application, lease.ApplicationLeaseRef, bad, "defer", time.Now().Add(time.Second)); err == nil {
		t.Fatal("deferred response content accepted")
	}
	unchanged, err := store.ObserveContext(ctx, identity.Context, identity.Incarnation, "r:context")
	if err != nil || unchanged.Identity.Owner != "" || unchanged.View.Revision != view.Revision {
		t.Fatal("invalid deferral changed ownership", err)
	}
	accepted, err := commitRedisInviteDeferral(ctx, store, key, decider, deciderLease.ApplicationLeaseRef, decision, "defer", time.Now().Add(time.Second))
	if err != nil || accepted.Certainty != pb.OperationResult_ACCEPTED || accepted.SipStatus != 0 || accepted.Snapshot.ApplicationLeaseTerm != "decider-term" {
		t.Fatalf("deferred INVITE: %+v %v", accepted, err)
	}
	bound, err := store.ObserveContext(ctx, identity.Context, identity.Incarnation, "r:context", "r:procedure-"+requestID, "r:result-1", "r:output-"+requestID+"-100")
	if err != nil || bound.Identity.Owner != deciderApp || bound.Identity.Term != "decider-term" {
		t.Fatal("deferral did not bind deciding owner", err)
	}
	output, err := decodeNativeOutput(bound.View.Records["r:output-"+requestID+"-100"])
	if err != nil || output.Write.Permit.Authorization.ApplicationLeaseTerm != "decider-term" {
		t.Fatal("held Trying used the recipient rather than the deciding lease", err)
	}
	outputRef, err := nativeOutputRef(output.Write)
	if err != nil {
		t.Fatal(err)
	}
	proof, err := store.InspectOutput(ctx, outputRef)
	if err != nil {
		t.Fatal(err)
	}
	selectedWire, err := store.Read(ctx, bound.Identity, proof.MessageField)
	if err != nil || selectedWire.Revision != bound.View.Revision {
		t.Fatal("held Trying wire selection", err)
	}
	wire, err := selectedmessage.DecodeWire(selectedWire.Records[proof.MessageField])
	if err != nil {
		t.Fatal(err)
	}
	fingerprint, err := SendFingerprint(accepted.Snapshot, &pb.SipMessage{Method: "INVITE", Status: 100, Wire: wire}, output.Write.Permit.Authorization, output.Write.Flow)
	if err != nil || fingerprint != output.Write.Permit.Authorization.Fingerprint {
		t.Fatal("held Trying fingerprint did not bind the deciding snapshot", err)
	}
	boundProcedure, err := decodeNativeProcedure(bound.View.Records["r:procedure-"+requestID])
	if err != nil || boundProcedure.Metadata.DecisionPending || boundProcedure.Metadata.CommandResult != "r:result-1" || boundProcedure.Native.Timers[portable.TimerTrying].Active || boundProcedure.Metadata.Emissions != 1 || boundProcedure.Metadata.ProvisionalEmissions != 1 || len(boundProcedure.Metadata.Outputs) != 1 || boundProcedure.Metadata.Outputs[0] != "r:output-"+requestID+"-100" {
		t.Fatal("deferral lost native Trying or receipt", err)
	}
	if _, err = client.ZScore(ctx, root+"core:deadlines:context", "call-decision-"+requestID).Result(); !errors.Is(err, redis.Nil) {
		t.Fatal("decision deadline retained", err)
	}
	if _, err = client.ZScore(ctx, root+"core:deadlines:context", "call-tx-"+requestID+"-"+string(portable.TimerTrying)).Result(); !errors.Is(err, redis.Nil) {
		t.Fatal("completed Trying deadline retained", err)
	}
	if n, e := client.ZCard(ctx, root+"core:ready:"+worker).Result(); e != nil || n != 1 {
		t.Fatal("deferral did not publish one held response", n, e)
	}
	writer := nativeRedisWriter{store: store}
	if _, err = writer.Reserve(ctx, output.Write); err != nil {
		t.Fatal("reserve held Trying", err)
	}
	if err = writer.Complete(ctx, output.Write, &pb.DeliveryResult{Effect: output.Write.Effect, Outcome: pb.DeliveryResult_WRITTEN}); err != nil {
		t.Fatal("record held Trying completion", err)
	}
	proof, err = store.InspectOutput(ctx, outputRef)
	if err != nil {
		t.Fatal(err)
	}
	if err = settleRedisInviteTryingOutput(ctx, store, output.Write, &nativeOutputCapture{ref: outputRef, proof: proof, metadata: output}, in.Timer, time.Now().Add(time.Second)); err != nil {
		t.Fatal("settle held Trying", err)
	}
	proof, err = store.InspectOutput(ctx, outputRef)
	if err != nil || !proof.Settled || proof.Acknowledged {
		t.Fatal("Trying proof released before dispatcher return", err)
	}
	if err = store.SettleOutput(ctx, outputRef, true, false); err != nil {
		t.Fatal("acknowledge returned Trying", err)
	}
	if _, err = store.InspectOutput(ctx, outputRef); !errors.Is(err, rediscore.ErrAbsent) {
		t.Fatal("settled Trying proof retained", err)
	}
	boundWire := bytes.ReplaceAll(stateTestRequest("INVITE", 1, "", "bound").Wire, []byte(facts.CallID), []byte("bound-invite@example.test"))
	boundMessage, err := portable.ParseMessage(boundWire, "tls", "192.0.2.12:5061", "")
	if err != nil {
		t.Fatal(err)
	}
	boundFacts, err := boundMessage.Facts()
	if err != nil {
		t.Fatal(err)
	}
	boundInput := in
	boundInput.Message = boundMessage
	boundInput.RequestID = uuid.NewString()
	boundInput.Context = &pb.ContextRef{Id: "bound", Owner: process, State: &pb.StateRef{Authority: authority, GroupId: "bound", Incarnation: "incarnation", CallId: boundFacts.CallID}}
	boundInput.Deadline = time.Now().Add(time.Second)
	boundPresentation, err := receiveRedisInvite(ctx, store, boundInput)
	if err != nil {
		t.Fatal(err)
	}
	boundDecision := residentDirectDecision(boundPresentation, 1, 0)
	boundDecision.Basis, boundDecision.PrincipalId, boundDecision.BindingId = pb.OwnerIngest_EXISTING_FLOW, "", ""
	if _, err = commitRedisInviteDeferral(ctx, store, key, application, lease.ApplicationLeaseRef, boundDecision, "early-defer", time.Now().Add(time.Second)); err != nil {
		t.Fatal("bind before Trying", err)
	}
	time.Sleep(225 * time.Millisecond)
	boundDue, err := store.PeekDue(ctx, worker, process.Epoch)
	if err != nil || boundDue == nil || boundDue.Member != "call-tx-"+boundInput.RequestID+"-"+string(portable.TimerTrying) {
		t.Fatal("bound Trying not due", boundDue, err)
	}
	if err = fireRedisInviteTrying(ctx, store, key, boundDue, time.Now().Add(time.Second)); err != nil {
		t.Fatal("bound Trying did not publish", err)
	}
	boundSelected, err := store.ObserveContext(ctx, "bound", "incarnation", "r:procedure-"+boundInput.RequestID, "r:output-"+boundInput.RequestID+"-100")
	if err != nil {
		t.Fatal(err)
	}
	boundTrying, err := decodeNativeProcedure(boundSelected.View.Records["r:procedure-"+boundInput.RequestID])
	if err != nil || boundTrying.Metadata.Emissions != 1 || boundTrying.Metadata.ProvisionalEmissions != 1 || len(boundTrying.Metadata.Outputs) != 1 || boundTrying.Metadata.Outputs[0] != "r:output-"+boundInput.RequestID+"-100" || len(boundSelected.View.Records["r:output-"+boundInput.RequestID+"-100"]) == 0 {
		t.Fatal("bound Trying output or counters missing", err)
	}
	boundOutput, err := decodeNativeOutput(boundSelected.View.Records["r:output-"+boundInput.RequestID+"-100"])
	if err != nil {
		t.Fatal(err)
	}
	boundRef, err := nativeOutputRef(boundOutput.Write)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = writer.Reserve(ctx, boundOutput.Write); err != nil {
		t.Fatal("reserve bound Trying", err)
	}
	if err = writer.Complete(ctx, boundOutput.Write, &pb.DeliveryResult{Effect: boundOutput.Write.Effect, Outcome: pb.DeliveryResult_UNKNOWN}); err != nil {
		t.Fatal("record uncertain Trying", err)
	}
	boundProof, err := store.InspectOutput(ctx, boundRef)
	if err != nil || boundProof.State != rediscore.OutputUnknown {
		t.Fatal("uncertain Trying proof missing", err)
	}
	if err = settleRedisInviteTryingOutput(ctx, store, boundOutput.Write, &nativeOutputCapture{ref: boundRef, proof: boundProof, metadata: boundOutput}, in.Timer, time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	boundProof, err = store.InspectOutput(ctx, boundRef)
	if err != nil || boundProof.State != rediscore.OutputUnknown || boundProof.Settled || boundProof.Acknowledged {
		t.Fatal("uncertain Trying was settled or acknowledged", err)
	}
	finalWire := bytes.ReplaceAll(stateTestRequest("INVITE", 1, "", "final").Wire, []byte(facts.CallID), []byte("final-invite@example.test"))
	finalMessage, err := portable.ParseMessage(finalWire, "tls", "192.0.2.12:5061", "")
	if err != nil {
		t.Fatal(err)
	}
	finalFacts, err := finalMessage.Facts()
	if err != nil {
		t.Fatal(err)
	}
	finalInput := in
	finalInput.Message, finalInput.RequestID, finalInput.Deadline = finalMessage, uuid.NewString(), time.Now().Add(time.Second)
	finalInput.Context = &pb.ContextRef{Id: "final", Owner: process, State: &pb.StateRef{Authority: authority, GroupId: "final", Incarnation: "incarnation", CallId: finalFacts.CallID}}
	finalPresentation, err := receiveRedisInvite(ctx, store, finalInput)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(225 * time.Millisecond)
	finalDue, err := store.PeekDue(ctx, worker, process.Epoch)
	if err != nil || finalDue == nil || finalDue.Member != "call-tx-"+finalInput.RequestID+"-"+string(portable.TimerTrying) {
		t.Fatal("direct-final Trying not due", finalDue, err)
	}
	if err = fireRedisInviteTrying(ctx, store, key, finalDue, time.Now().Add(time.Second)); err != nil {
		t.Fatal("retain direct-final Trying", err)
	}
	finalDecision := residentDirectDecision(finalPresentation, 1, 200)
	finalResult, err := commitRedisInviteFinal(ctx, store, key, application, lease.ApplicationLeaseRef, finalDecision, "answer", "sip:edge@example.test", "edge.example.test:5061", time.Now().Add(time.Second))
	if err != nil || finalResult.Certainty != pb.OperationResult_ACCEPTED || finalResult.SipStatus != 200 || !finalResult.Snapshot.Answered {
		t.Fatal("direct INVITE final publication", err)
	}
	finalView, err := store.ObserveContext(ctx, "final", "incarnation", "r:context", "r:procedure-"+finalInput.RequestID, "r:dialog-"+finalInput.RequestID, "r:invite-output-"+finalInput.RequestID+"-1", "r:response-"+finalInput.RequestID+"-100", "r:result-1")
	if err != nil {
		t.Fatal(err)
	}
	finalReceipt, err := decodeNativeResult(finalView.View.Records["r:result-1"])
	if err != nil || finalReceipt.Certainty != pb.OperationResult_ACCEPTED || finalReceipt.SipStatus != 200 {
		t.Fatal("direct INVITE final receipt lost selected status", err)
	}
	finalProcedure, err := decodeNativeProcedure(finalView.View.Records["r:procedure-"+finalInput.RequestID])
	if err != nil || finalProcedure.Native.Phase != portable.Accepted || !finalProcedure.Native.Timers[portable.TimerL].Active || len(finalProcedure.Metadata.Outputs) != 1 || len(finalView.View.Records["r:response-"+finalInput.RequestID+"-100"]) != 0 {
		t.Fatal("accepted INVITE native transaction missing", err)
	}
	finalDialog, err := decodeNativeDialog(finalView.View.Records["r:dialog-"+finalInput.RequestID])
	if err != nil || !finalDialog.Native.Timers[portable.TimerACK].Active || finalDialog.Metadata.Transaction != finalProcedure.Native.Key || len(finalView.View.Records["r:invite-output-"+finalInput.RequestID+"-1"]) == 0 {
		t.Fatal("accepted INVITE dialog or output missing", err)
	}
	finalOutput, err := decodeNativeOutput(finalView.View.Records["r:invite-output-"+finalInput.RequestID+"-1"])
	if err != nil {
		t.Fatal(err)
	}
	finalReserved, err := writer.Reserve(ctx, finalOutput.Write)
	if err != nil || finalReserved == nil || finalReserved.Known || !bytes.HasPrefix(finalReserved.Wire, []byte("SIP/2.0 200")) {
		t.Fatal("reserve exact selected initial 200", err)
	}
	finalRef, err := nativeOutputRef(finalOutput.Write)
	if err != nil {
		t.Fatal(err)
	}
	if err = writer.Complete(ctx, finalOutput.Write, &pb.DeliveryResult{Effect: finalOutput.Write.Effect, Outcome: pb.DeliveryResult_UNKNOWN}); err != nil {
		t.Fatal(err)
	}
	if err = settleRedisInitialInviteOutput(ctx, store, finalOutput.Write, nil, in.Timer, time.Now().Add(time.Second)); err != nil {
		t.Fatal("retain uncertain direct final", err)
	}
	if err = acknowledgeRedisInitialInviteOutput(ctx, store, finalOutput.Write); err == nil {
		t.Fatal("uncertain direct final was acknowledged")
	}
	finalProof, err := store.InspectOutput(ctx, finalRef)
	if err != nil || finalProof.State != rediscore.OutputUnknown || finalProof.Settled || finalProof.Acknowledged {
		t.Fatal("uncertain direct final became physical certainty", err)
	}
	if err = writer.Complete(ctx, finalOutput.Write, &pb.DeliveryResult{Effect: finalOutput.Write.Effect, Outcome: pb.DeliveryResult_WRITTEN}); err != nil {
		t.Fatal(err)
	}
	if err = settleRedisInitialInviteOutput(ctx, store, finalOutput.Write, nil, in.Timer, time.Now().Add(time.Second)); err != nil {
		t.Fatal("settle late exact initial 200", err)
	}
	finalProof, err = store.InspectOutput(ctx, finalRef)
	if err != nil || !finalProof.Settled || finalProof.Acknowledged {
		t.Fatal("initial 200 proof released before dispatcher acknowledgement", err)
	}
	if err = acknowledgeRedisInitialInviteOutput(ctx, store, finalOutput.Write); err != nil {
		t.Fatal(err)
	}
	if _, err = store.InspectOutput(ctx, finalRef); !errors.Is(err, rediscore.ErrAbsent) {
		t.Fatal("initial 200 definite proof not released", err)
	}
	finalAfter, err := store.ObserveContext(ctx, "final", "incarnation", "r:context", "r:result-1")
	if err != nil || !bytes.Equal(finalAfter.View.Records["r:result-1"], finalView.View.Records["r:result-1"]) || !bytes.Equal(finalAfter.View.Records["r:context"], finalView.View.Records["r:context"]) {
		t.Fatal("physical completion replaced direct ACCEPTED certainty or business state", err)
	}
	finalSelected, err := store.ObserveContext(ctx, "final", "incarnation", "r:procedure-"+finalInput.RequestID, "r:origin-"+finalInput.RequestID, "r:invite-response-"+finalInput.RequestID+"-1")
	if err != nil {
		t.Fatal(err)
	}
	finalNative, err := decodeNativeProcedure(finalSelected.View.Records["r:procedure-"+finalInput.RequestID])
	if err != nil {
		t.Fatal(err)
	}
	finalMemo, err := selectedmessage.NewMemo(2)
	if err != nil {
		t.Fatal(err)
	}
	finalTransaction, err := finalNative.attach(&nativeSelectedMessages{records: finalSelected.View.Records, memo: finalMemo, provenance: "admitted"}, true, true, false)
	if err != nil {
		t.Fatal(err)
	}
	semanticL := finalTransaction.Timers[portable.TimerL]
	redisNow, err := client.Time(ctx).Result()
	if err != nil {
		t.Fatal(err)
	}
	semanticL.At = redisNow.Add(-time.Millisecond).UnixMilli()
	finalTransaction.Timers[portable.TimerL] = semanticL
	finalProcedureBytes, err := encodeNativeProcedure(finalTransaction, finalNative.Refs, finalNative.Metadata)
	if err != nil {
		t.Fatal(err)
	}
	lMember := "call-tx-" + finalInput.RequestID + "-" + string(portable.TimerL)
	_, err = store.Commit(ctx, rediscore.Publication{Identity: finalSelected.Identity, Revision: finalSelected.View.Revision, Deadline: time.Now().Add(time.Second), Changes: []rediscore.Change{{Field: "r:procedure-" + finalInput.RequestID, Value: finalProcedureBytes}}, Timers: &rediscore.TimerMutation{Expected: finalSelected.View.Timer, Target: finalSelected.View.Timer, RequireLease: true, Changes: []rediscore.TimerChange{{Member: lMember, Due: time.UnixMilli(semanticL.At)}}}})
	if err != nil {
		t.Fatal(err)
	}
	lDue, err := store.PeekDue(ctx, worker, process.Epoch)
	if err != nil || lDue == nil || lDue.Member != lMember {
		t.Fatal("synthetic Timer L not due", lDue, err)
	}
	if err = fireRedisInviteServerTimer(ctx, store, lDue, time.Now().Add(time.Second)); err != nil {
		t.Fatal("Timer L execution", err)
	}
	if _, err = client.ZScore(ctx, root+"core:deadlines:final", callDialogTimerMember("r:dialog-"+finalInput.RequestID, portable.TimerACK)).Result(); err != nil {
		t.Fatal("Timer L discharged independent dialog ACK duty", err)
	}
	ackView, err := store.ObserveContext(ctx, "final", "incarnation", "r:dialog-"+finalInput.RequestID, "r:origin-"+finalInput.RequestID, "r:invite-response-"+finalInput.RequestID+"-1")
	if err != nil {
		t.Fatal(err)
	}
	ackDialog, err := decodeNativeDialog(ackView.View.Records["r:dialog-"+finalInput.RequestID])
	if err != nil {
		t.Fatal(err)
	}
	ackMemo, err := selectedmessage.NewMemo(2)
	if err != nil {
		t.Fatal(err)
	}
	ackNative, err := ackDialog.attach(&nativeSelectedMessages{records: ackView.View.Records, memo: ackMemo, provenance: "admitted"}, nativeDialogSelection{Remote: nativeInviteSelection{Request: true, Final: true}})
	if err != nil {
		t.Fatal(err)
	}
	redisNow, err = client.Time(ctx).Result()
	if err != nil {
		t.Fatal(err)
	}
	ackTimer := ackNative.Timers[portable.TimerACK]
	ackTimer.At = redisNow.Add(-time.Minute).UnixMilli()
	ackNative.Timers[portable.TimerACK] = ackTimer
	ackBytes, err := encodeNativeDialog(ackNative, ackDialog.Refs, ackDialog.Metadata)
	if err != nil {
		t.Fatal(err)
	}
	ackMember := callDialogTimerMember("r:dialog-"+finalInput.RequestID, portable.TimerACK)
	_, err = store.Commit(ctx, rediscore.Publication{Identity: ackView.Identity, Revision: ackView.View.Revision, Deadline: time.Now().Add(time.Second), Changes: []rediscore.Change{{Field: "r:dialog-" + finalInput.RequestID, Value: ackBytes}}, Timers: &rediscore.TimerMutation{Expected: ackView.View.Timer, Target: ackView.View.Timer, RequireLease: true, Changes: []rediscore.TimerChange{{Member: ackMember, Due: time.UnixMilli(ackTimer.At)}}}})
	if err != nil {
		t.Fatal(err)
	}
	ackDue, err := store.PeekDue(ctx, worker, process.Epoch)
	if err != nil || ackDue == nil || ackDue.Member != ackMember {
		t.Fatal("synthetic ACK timeout not due", ackDue, err)
	}
	staleACK := *ackDue
	staleACK.Revision++
	if err = fireRedisInitialInviteACKTimeout(ctx, store, key, &staleACK, time.Now().Add(time.Second)); !errors.Is(err, rediscore.ErrConflict) {
		t.Fatal("stale ACK timeout was accepted", err)
	}
	limitedACK, err := rediscore.New(client, rediscore.Config{Root: root, Profile: profile, PrimaryRunID: primary, RecordBudget: 4 << 20, NamespaceMaxBytes: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err = fireRedisInitialInviteACKTimeout(ctx, limitedACK, key, ackDue, time.Now().Add(time.Second)); !errors.Is(err, rediscore.ErrCapacity) {
		t.Fatal("ACK timeout capacity refusal was not atomic", err)
	}
	unchangedACK, err := store.ObserveContext(ctx, "final", "incarnation", "r:dialog-"+finalInput.RequestID)
	if err != nil || unchangedACK.View.Revision != ackDue.Revision || !bytes.Equal(unchangedACK.View.Records["r:dialog-"+finalInput.RequestID], ackBytes) {
		t.Fatal("refused ACK timeout changed the dialog", err)
	}
	ackOwnerKey := root + "core:owner:" + lease.Term
	ackOwnerExpiry, err := client.HGet(ctx, ackOwnerKey, "expires_ms").Result()
	if err != nil {
		t.Fatal(err)
	}
	expiredAt := time.Now().Add(-time.Second).UnixMilli()
	if err = client.HSet(ctx, ackOwnerKey, "expires_ms", expiredAt).Err(); err != nil {
		t.Fatal(err)
	}
	if err = client.ZAdd(ctx, root+"core:owner-expiry", redis.Z{Score: float64(expiredAt), Member: "a-" + lease.Term}).Err(); err != nil {
		t.Fatal(err)
	}
	if err = fireRedisInitialInviteACKTimeout(ctx, store, key, ackDue, time.Now().Add(time.Second)); err != nil {
		t.Fatal("ACK timeout cleanup publication", err)
	}
	if err = client.HSet(ctx, ackOwnerKey, "expires_ms", ackOwnerExpiry).Err(); err != nil {
		t.Fatal(err)
	}
	ackOwnerExpiryNumber, err := strconv.ParseInt(ackOwnerExpiry, 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	if err = client.ZAdd(ctx, root+"core:owner-expiry", redis.Z{Score: float64(ackOwnerExpiryNumber), Member: "a-" + lease.Term}).Err(); err != nil {
		t.Fatal(err)
	}
	cleanupView, err := store.ObserveContext(ctx, "final", "incarnation", "r:context", "r:dialog-"+finalInput.RequestID)
	if err != nil {
		t.Fatal(err)
	}
	cleanupContext, err := decodeNativeContext(cleanupView.View.Records["r:context"])
	if err != nil || len(cleanupContext.NoninitialProcedures) != 1 {
		t.Fatal("ACK timeout did not own its cleanup procedure", err)
	}
	cleanupDialog, err := readPrivateDialogRecord(cleanupView.View.Records["r:dialog-"+finalInput.RequestID])
	if err != nil || cleanupDialog.LocalBye == "" || len(cleanupDialog.Outputs) != 1 {
		t.Fatal("ACK timeout did not own its cleanup output", err)
	}
	cleanupRecords, err := store.Read(ctx, cleanupView.Identity, cleanupContext.NoninitialProcedures[0], cleanupDialog.Outputs[0])
	if err != nil {
		t.Fatal(err)
	}
	cleanupProcedure, err := decodeNativeProcedure(cleanupRecords.Records[cleanupContext.NoninitialProcedures[0]])
	if err != nil || !cleanupProcedure.Metadata.Cleanup || cleanupProcedure.Metadata.Root != "" || cleanupProcedure.Metadata.CreditContribution != 0 || cleanupProcedure.Native.Family != portable.NonInviteClient || !cleanupProcedure.Native.Timers[portable.TimerF].Active {
		t.Fatal("ACK timeout cleanup procedure lost its native F duty", err)
	}
	cleanupOutput, err := decodeNativeOutput(cleanupRecords.Records[cleanupDialog.Outputs[0]])
	if err != nil || cleanupOutput.Method != "BYE" || cleanupOutput.Status != 0 || cleanupOutput.Write.Permit.Authorization.AuthorityClass != pb.EffectAuthorization_TERMINAL_CLEANUP {
		t.Fatal("ACK timeout cleanup output lacks native authority", err)
	}
	if err = client.HSet(ctx, ackOwnerKey, "expires_ms", expiredAt).Err(); err != nil {
		t.Fatal(err)
	}
	if err = client.ZAdd(ctx, root+"core:owner-expiry", redis.Z{Score: float64(expiredAt), Member: "a-" + lease.Term}).Err(); err != nil {
		t.Fatal(err)
	}
	if _, err = writer.Reserve(ctx, cleanupOutput.Write); err != nil {
		t.Fatal("cleanup BYE was fenced by expired controller", err)
	}
	cleanupRef, err := nativeOutputRef(cleanupOutput.Write)
	if err != nil {
		t.Fatal(err)
	}
	if err = writer.Complete(ctx, cleanupOutput.Write, &pb.DeliveryResult{Effect: cleanupOutput.Write.Effect, Outcome: pb.DeliveryResult_UNKNOWN}); err != nil {
		t.Fatal(err)
	}
	if err = settleRedisCleanupByeOutput(ctx, store, cleanupOutput.Write, nil, in.Timer, time.Now().Add(time.Second)); err != nil {
		t.Fatal("retain uncertain cleanup BYE", err)
	}
	if err = acknowledgeRedisCleanupByeOutput(ctx, store, cleanupOutput.Write, time.Now().Add(time.Second)); err == nil {
		t.Fatal("uncertain cleanup BYE was acknowledged")
	}

	if err = writer.Complete(ctx, cleanupOutput.Write, &pb.DeliveryResult{Effect: cleanupOutput.Write.Effect, Outcome: pb.DeliveryResult_WRITTEN}); err != nil {
		t.Fatal(err)
	}
	if err = settleRedisCleanupByeOutput(ctx, store, cleanupOutput.Write, nil, in.Timer, time.Now().Add(time.Second)); err != nil {
		t.Fatal("settle late cleanup BYE proof", err)
	}
	cleanupProof, err := store.InspectOutput(ctx, cleanupRef)
	if err != nil || !cleanupProof.Settled || cleanupProof.Acknowledged {
		t.Fatal("cleanup BYE proof released before dispatcher return", err)
	}
	if err = acknowledgeRedisCleanupByeOutput(ctx, store, cleanupOutput.Write, time.Now().Add(time.Second)); err != nil {
		t.Fatal("acknowledge cleanup BYE", err)
	}
	if _, err = store.InspectOutput(ctx, cleanupRef); !errors.Is(err, rediscore.ErrAbsent) {
		t.Fatal("cleanup BYE proof not released", err)
	}
	if err = client.HSet(ctx, ackOwnerKey, "expires_ms", ackOwnerExpiry).Err(); err != nil {
		t.Fatal(err)
	}
	if err = client.ZAdd(ctx, root+"core:owner-expiry", redis.Z{Score: float64(ackOwnerExpiryNumber), Member: "a-" + lease.Term}).Err(); err != nil {
		t.Fatal(err)
	}
	endedView, err := store.ObserveContext(ctx, "final", "incarnation", "r:context", cleanupContext.NoninitialProcedures[0], cleanupDialog.LocalBye)
	if err != nil {
		t.Fatal(err)
	}
	endedContext, err := decodeNativeContext(endedView.View.Records["r:context"])
	if err != nil {
		t.Fatal(err)
	}
	redisNow, err = client.Time(ctx).Result()
	if err != nil {
		t.Fatal(err)
	}
	endedContext.Ended, endedContext.CallRetireAtMs = true, redisNow.Add(2*time.Minute).UnixMilli()
	endedContextBytes, err := encodeNativeContext(endedContext)
	if err != nil {
		t.Fatal(err)
	}
	endedProcedure, err := decodeNativeProcedure(endedView.View.Records[cleanupContext.NoninitialProcedures[0]])
	if err != nil {
		t.Fatal(err)
	}
	endedMemo, err := selectedmessage.NewMemo(1)
	if err != nil {
		t.Fatal(err)
	}
	endedTransaction, err := endedProcedure.attach(&nativeSelectedMessages{records: endedView.View.Records, memo: endedMemo, provenance: "admitted"}, true, false, false)
	if err != nil {
		t.Fatal(err)
	}
	endedF := endedTransaction.Timers[portable.TimerF]
	endedF.At = redisNow.Add(-2 * time.Minute).UnixMilli()
	endedTransaction.Timers[portable.TimerF] = endedF
	endedProcedureBytes, err := encodeNativeProcedure(endedTransaction, endedProcedure.Refs, endedProcedure.Metadata)
	if err != nil {
		t.Fatal(err)
	}
	cleanupID := strings.TrimPrefix(cleanupContext.NoninitialProcedures[0], "r:call-cleanup-")
	endedMember := "call-client-" + cleanupID + "-" + string(portable.TimerF)
	_, err = store.Commit(ctx, rediscore.Publication{Identity: endedView.Identity, Revision: endedView.View.Revision, Deadline: time.Now().Add(time.Second), Changes: []rediscore.Change{{Field: "r:context", Value: endedContextBytes}, {Field: cleanupContext.NoninitialProcedures[0], Value: endedProcedureBytes}}, Timers: &rediscore.TimerMutation{Expected: endedView.View.Timer, Target: endedView.View.Timer, RequireLease: true, Changes: []rediscore.TimerChange{{Member: endedMember, Due: time.UnixMilli(endedF.At)}}}})
	if err != nil {
		t.Fatal(err)
	}
	endedDue, err := store.PeekDue(ctx, worker, process.Epoch)
	if err != nil || endedDue == nil || endedDue.Member != endedMember {
		t.Fatal("ended call cleanup F not due", endedDue, err)
	}
	if err = fireRedisCleanupByeTimer(ctx, store, endedDue, time.Now().Add(time.Second)); err != nil {
		t.Fatal("ended call cleanup F", err)
	}
	retireScore, err := client.ZScore(ctx, root+"core:deadlines:final", "call-retire").Result()
	if err != nil || int64(retireScore) != endedContext.CallRetireAtMs {
		t.Fatal("cleanup F lost or moved the original retirement cutoff", err)
	}
	cleanupCutoff := time.UnixMilli(int64(cleanupOutput.Write.Permit.Authorization.DeadlineMs))
	if wait := time.Until(cleanupCutoff); wait > 0 {
		time.Sleep(wait + 15*time.Millisecond)
	}
	cutoffView, err := store.ObserveContext(ctx, "final", "incarnation", "r:context")
	if err != nil {
		t.Fatal(err)
	}
	checked, err := client.Time(ctx).Result()
	if err != nil {
		t.Fatal(err)
	}
	cleanupDue := &rediscore.DueContext{Identity: cutoffView.Identity, Revision: cutoffView.View.Revision, Assignment: cutoffView.View.Timer, Member: "out-" + strings.TrimPrefix(cleanupDialog.Outputs[0], "r:"), DeadlineMS: cleanupCutoff.UnixMilli(), CheckedAtMS: checked.UnixMilli()}
	if err = dispatchRedisCleanupByeCutoff(ctx, store, cleanupDue, time.Now().Add(time.Second)); err != nil {
		t.Fatal("consumed cleanup output cutoff", err)
	}
	cutoffCleaned, err := store.ObserveContext(ctx, "final", "incarnation", "r:dialog-"+finalInput.RequestID, cleanupDialog.Outputs[0])
	if err != nil {
		t.Fatal(err)
	}
	remainingDialog, err := readPrivateDialogRecord(cutoffCleaned.View.Records["r:dialog-"+finalInput.RequestID])
	if err != nil || slices.Contains(remainingDialog.Outputs, cleanupDialog.Outputs[0]) || len(cutoffCleaned.View.Records[cleanupDialog.Outputs[0]]) != 0 {
		t.Fatal("cleanup cutoff retained released metadata", err)
	}
	firstCutoff := time.UnixMilli(int64(output.Write.Permit.Authorization.DeadlineMs))
	if wait := time.Until(firstCutoff); wait > 0 {
		time.Sleep(wait + 15*time.Millisecond)
	}
	firstDue, err := store.PeekDue(ctx, worker, process.Epoch)
	if err != nil || firstDue == nil || firstDue.Member != "out-output-"+requestID+"-100" {
		t.Fatal("settled Trying cutoff missing", firstDue, err)
	}
	staleDue := *firstDue
	staleDue.Revision++
	if err = dispatchRedisInviteTryingCutoff(ctx, store, &staleDue, time.Now().Add(time.Second)); !errors.Is(err, rediscore.ErrConflict) {
		t.Fatal("stale Trying cutoff was accepted", err)
	}
	if err = dispatchRedisInviteTryingCutoff(ctx, store, firstDue, time.Now().Add(time.Second)); err != nil {
		t.Fatal("settled Trying cleanup", err)
	}
	cleaned, err := store.ObserveContext(ctx, identity.Context, identity.Incarnation, "r:procedure-"+requestID, "r:output-"+requestID+"-100", "r:response-"+requestID+"-100")
	if err != nil {
		t.Fatal(err)
	}
	cleanedProcedure, err := decodeNativeProcedure(cleaned.View.Records["r:procedure-"+requestID])
	if err != nil || len(cleanedProcedure.Metadata.Outputs) != 0 || len(cleaned.View.Records["r:output-"+requestID+"-100"]) != 0 || len(cleaned.View.Records["r:response-"+requestID+"-100"]) == 0 {
		t.Fatal("settled Trying owner cleanup lost native response", err)
	}
	secondCutoff := time.UnixMilli(int64(boundOutput.Write.Permit.Authorization.DeadlineMs))
	if wait := time.Until(secondCutoff); wait > 0 {
		time.Sleep(wait + 15*time.Millisecond)
	}
	unknownDue, err := store.PeekDue(ctx, worker, process.Epoch)
	if err != nil || unknownDue == nil || unknownDue.Member != "out-output-"+boundInput.RequestID+"-100" {
		t.Fatal("uncertain Trying cutoff missing", unknownDue, err)
	}
	rootView, err := store.ObserveContext(ctx, "bound", "incarnation", "r:context")
	if err != nil {
		t.Fatal(err)
	}
	unrooted, err := decodeNativeContext(rootView.View.Records["r:context"])
	if err != nil {
		t.Fatal(err)
	}
	unrooted.InitialProcedures = nil
	unrootedBytes, err := encodeNativeContext(unrooted)
	if err != nil {
		t.Fatal(err)
	}
	_, err = store.Commit(ctx, rediscore.Publication{Identity: rootView.Identity, Revision: rootView.View.Revision, Deadline: time.Now().Add(time.Second), Changes: []rediscore.Change{{Field: "r:context", Value: unrootedBytes}}})
	if err != nil {
		t.Fatal(err)
	}
	unknownDue, err = store.PeekDue(ctx, worker, process.Epoch)
	if err != nil || unknownDue == nil || dispatchRedisInviteTryingCutoff(ctx, store, unknownDue, time.Now().Add(time.Second)) == nil {
		t.Fatal("unreachable Trying owner was accepted", err)
	}
	rootView, err = store.ObserveContext(ctx, "bound", "incarnation", "r:context")
	if err != nil {
		t.Fatal(err)
	}
	unrooted.InitialProcedures = []string{"r:procedure-" + boundInput.RequestID}
	rootedBytes, err := encodeNativeContext(unrooted)
	if err != nil {
		t.Fatal(err)
	}
	_, err = store.Commit(ctx, rediscore.Publication{Identity: rootView.Identity, Revision: rootView.View.Revision, Deadline: time.Now().Add(time.Second), Changes: []rediscore.Change{{Field: "r:context", Value: rootedBytes}}})
	if err != nil {
		t.Fatal(err)
	}
	unknownDue, err = store.PeekDue(ctx, worker, process.Epoch)
	if err != nil || unknownDue == nil {
		t.Fatal("restored Trying cutoff missing", err)
	}
	if err = dispatchRedisInviteTryingCutoff(ctx, store, unknownDue, time.Now().Add(time.Second)); err != nil {
		t.Fatal("uncertain Trying cutoff", err)
	}
	boundProof, err = store.InspectOutput(ctx, boundRef)
	if err != nil || boundProof.State != rediscore.OutputUnknown {
		t.Fatal("uncertain Trying proof did not remain reachable", err)
	}
	if _, err = client.ZScore(ctx, root+"core:deadlines:bound", unknownDue.Member).Result(); !errors.Is(err, redis.Nil) {
		t.Fatal("uncertain Trying retained a polling head", err)
	}
	if err = writer.Complete(ctx, boundOutput.Write, &pb.DeliveryResult{Effect: boundOutput.Write.Effect, Outcome: pb.DeliveryResult_WRITTEN}); err != nil {
		t.Fatal("record late Trying proof", err)
	}
	boundProof, err = store.InspectOutput(ctx, boundRef)
	if err != nil {
		t.Fatal(err)
	}
	if err = settleRedisInviteTryingOutput(ctx, store, boundOutput.Write, &nativeOutputCapture{ref: boundRef, proof: boundProof, metadata: boundOutput}, in.Timer, time.Now().Add(time.Second)); err != nil {
		t.Fatal("re-drive late Trying proof", err)
	}
	if _, err = client.ZScore(ctx, root+"core:deadlines:bound", unknownDue.Member).Result(); err != nil {
		t.Fatal("late Trying proof did not restore its original cutoff", err)
	}
	if err = acknowledgeRedisInviteTryingOutput(ctx, store, boundOutput.Write, in.Timer, time.Now().Add(time.Second)); err != nil {
		t.Fatal("acknowledge late Trying proof", err)
	}
	if _, err = store.RenewApplication(ctx, lease.ApplicationLeaseRef); err != nil {
		t.Fatal("renew application for Respond", err)
	}
	if _, err = store.RenewWorker(ctx, rediscore.WorkerLeaseRequest{Worker: worker, Epoch: process.Epoch, PrimaryRunID: primary}); err != nil {
		t.Fatal("renew worker for Respond", err)
	}
	respond := &pb.Command{Context: boundInput.Context, Sequence: 2, Admission: &pb.CommandAdmission{DirectoryEpoch: "directory", ExpiresAtMs: uint64(time.Now().Add(time.Second).UnixMilli())}, Action: &pb.Command_Respond{Respond: &pb.Respond{RequestId: boundInput.RequestID, Status: 180, PrincipalId: "account", BindingId: "phone"}}}
	respondCapture, err := captureRedisNotifyWait(respond)
	if err != nil {
		t.Fatal(err)
	}
	respondIdentity, err := publishRedisInviteRespond(ctx, store, key, application, lease.ApplicationLeaseRef, respond, respondCapture, "ring", "sip:edge@example.test", "edge.example.test:5061", in.Timer, time.Now().Add(time.Second))
	if err != nil {
		t.Fatal("publish application Respond", err)
	}
	respondView, err := store.Read(ctx, respondIdentity, "r:context", "r:result-2", "r:procedure-"+boundInput.RequestID, "r:invite-output-"+boundInput.RequestID+"-2")
	if err != nil {
		t.Fatal(err)
	}
	respondResult, err := decodeNativeResult(respondView.Records["r:result-2"])
	if err != nil || !respondResult.Pending || respondResult.Certainty != pb.OperationResult_UNKNOWN || respondResult.OperationId != "ring" || respondResult.Output != "r:invite-output-"+boundInput.RequestID+"-2" {
		t.Fatal("Respond pending result missing", err)
	}
	respondOutput, err := decodeNativeOutput(respondView.Records["r:invite-output-"+boundInput.RequestID+"-2"])
	if err != nil || respondOutput.Result != "r:result-2" || respondOutput.Operation != "ring" || respondOutput.Write.Permit.Authorization.AuthorityClass != pb.EffectAuthorization_RESPONSE {
		t.Fatal("Respond output not linked to its application receipt", err)
	}
	respondContext, err := decodeNativeContext(respondView.Records["r:context"])
	if err != nil || respondContext.PrincipalId != "account" || respondContext.BindingId != "phone" {
		t.Fatal("Respond did not install its account binding", err)
	}
	nextRespond := proto.Clone(respond).(*pb.Command)
	nextRespond.Sequence, nextRespond.GetRespond().Status = 3, 183
	nextCapture, err := captureRedisNotifyWait(nextRespond)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = publishRedisInviteRespond(ctx, store, key, application, lease.ApplicationLeaseRef, nextRespond, nextCapture, "progress", "sip:edge@example.test", "edge.example.test:5061", in.Timer, time.Now().Add(time.Second)); err != nil {
		t.Fatal("publish newer response before older completion", err)
	}
	nextView, err := store.Read(ctx, respondIdentity, "r:result-3", "r:invite-output-"+boundInput.RequestID+"-3")
	if err != nil {
		t.Fatal(err)
	}
	nextOutput, err := decodeNativeOutput(nextView.Records["r:invite-output-"+boundInput.RequestID+"-3"])
	if err != nil {
		t.Fatal(err)
	}
	ownerKey := root + "core:owner:" + lease.Term
	ownerExpiry, err := client.HGet(ctx, ownerKey, "expires_ms").Result()
	if err != nil {
		t.Fatal(err)
	}
	ownerExpiredAt := time.Now().Add(-time.Second).UnixMilli()
	if err = client.HSet(ctx, ownerKey, "expires_ms", ownerExpiredAt).Err(); err != nil {
		t.Fatal(err)
	}
	if err = client.ZAdd(ctx, root+"core:owner-expiry", redis.Z{Score: float64(ownerExpiredAt), Member: "a-" + lease.Term}).Err(); err != nil {
		t.Fatal(err)
	}
	// Only this isolated fixture's term is expired. Both responses were already
	// committed; their physical authority cannot depend on its continued lease.
	for _, selected := range []*pb.FlowWrite{respondOutput.Write, nextOutput.Write} {
		reserved, e := writer.Reserve(ctx, selected)
		if e != nil || reserved == nil || reserved.Known || len(reserved.Wire) == 0 {
			t.Fatal("committed response was fenced by later application expiry", e)
		}
	}
	if err = writer.Complete(ctx, respondOutput.Write, &pb.DeliveryResult{Effect: respondOutput.Write.Effect, Outcome: pb.DeliveryResult_UNKNOWN}); err != nil {
		t.Fatal(err)
	}
	if err = settleRedisInitialInviteOutput(ctx, store, respondOutput.Write, nil, in.Timer, time.Now().Add(time.Second)); err != nil {
		t.Fatal("record exact older response uncertainty", err)
	}
	uncertainView, err := store.Read(ctx, respondIdentity, "r:result-2", "r:result-3")
	if err != nil || !bytes.Equal(uncertainView.Records["r:result-3"], nextView.Records["r:result-3"]) {
		t.Fatal("older uncertainty replaced newer response result", err)
	}
	uncertainResult, err := decodeNativeResult(uncertainView.Records["r:result-2"])
	if err != nil || uncertainResult.Pending || uncertainResult.Certainty != pb.OperationResult_UNKNOWN || uncertainResult.SipStatus != 180 || uncertainResult.OperationId != "ring" || uncertainResult.Output != respondResult.Output || uncertainResult.Request != "" || uncertainResult.Response != "" {
		t.Fatal("older response did not retain its own bounded outcome", err)
	}
	if err = acknowledgeRedisInitialInviteOutput(ctx, store, respondOutput.Write); err == nil {
		t.Fatal("ordinary uncertain response was acknowledged")
	}
	if err = writer.Complete(ctx, respondOutput.Write, &pb.DeliveryResult{Effect: respondOutput.Write.Effect, Outcome: pb.DeliveryResult_WRITTEN}); err != nil {
		t.Fatal(err)
	}
	if err = settleRedisInitialInviteOutput(ctx, store, respondOutput.Write, nil, in.Timer, time.Now().Add(time.Second)); err != nil {
		t.Fatal("upgrade exact older response with late positive proof", err)
	}
	completedView, err := store.Read(ctx, respondIdentity, "r:result-2", "r:result-3")
	if err != nil || !bytes.Equal(completedView.Records["r:result-3"], nextView.Records["r:result-3"]) {
		t.Fatal("older completion replaced newer response result", err)
	}
	completedResult, err := decodeNativeResult(completedView.Records["r:result-2"])
	if err != nil || completedResult.Certainty != pb.OperationResult_COMPLETED || completedResult.SipStatus != 180 || completedResult.OperationId != "ring" || completedResult.Output != respondResult.Output || completedResult.Error != "" || completedResult.Request != "" || completedResult.Response != "" || uncertainResult.Certainty != pb.OperationResult_UNKNOWN {
		t.Fatal("older completed response or captured historical uncertainty changed", err)
	}
	if err = acknowledgeRedisInitialInviteOutput(ctx, store, respondOutput.Write); err != nil {
		t.Fatal(err)
	}
	if err = writer.Complete(ctx, nextOutput.Write, &pb.DeliveryResult{Effect: nextOutput.Write.Effect, Outcome: pb.DeliveryResult_NOT_WRITTEN}); err != nil {
		t.Fatal(err)
	}
	if err = settleRedisInitialInviteOutput(ctx, store, nextOutput.Write, nil, in.Timer, time.Now().Add(time.Second)); err != nil {
		t.Fatal("settle exact newer no-write proof", err)
	}
	noWriteView, err := store.Read(ctx, respondIdentity, "r:result-2", "r:result-3")
	if err != nil || !bytes.Equal(noWriteView.Records["r:result-2"], completedView.Records["r:result-2"]) {
		t.Fatal("newer no-write result replaced older completion", err)
	}
	noWriteResult, err := decodeNativeResult(noWriteView.Records["r:result-3"])
	if err != nil || noWriteResult.Pending || noWriteResult.Certainty != pb.OperationResult_UNKNOWN || noWriteResult.SipStatus != 183 || noWriteResult.OperationId != "progress" || noWriteResult.Error != "SIP delivery unavailable or uncertain" {
		t.Fatal("no-write response produced the wrong certainty", err)
	}
	if err = acknowledgeRedisInitialInviteOutput(ctx, store, nextOutput.Write); err != nil {
		t.Fatal(err)
	}
	if err = client.HSet(ctx, ownerKey, "expires_ms", ownerExpiry).Err(); err != nil {
		t.Fatal(err)
	}
	ownerExpiryNumber, err := strconv.ParseInt(ownerExpiry, 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	if err = client.ZAdd(ctx, root+"core:owner-expiry", redis.Z{Score: float64(ownerExpiryNumber), Member: "a-" + lease.Term}).Err(); err != nil {
		t.Fatal(err)
	}
	constrained, err := rediscore.New(client, rediscore.Config{Root: root, Profile: profile, PrimaryRunID: primary, RecordBudget: 4 << 20, NamespaceMaxBytes: 1})
	if err != nil {
		t.Fatal(err)
	}
	secondWire := bytes.ReplaceAll(stateTestRequest("INVITE", 1, "", "other").Wire, []byte(facts.CallID), []byte("other-invite@example.test"))
	secondMessage, err := portable.ParseMessage(secondWire, "tls", "192.0.2.12:5061", "")
	if err != nil {
		t.Fatal(err)
	}
	secondFacts, err := secondMessage.Facts()
	if err != nil {
		t.Fatal(err)
	}
	second := in
	second.Message = secondMessage
	second.Deadline = time.Now().Add(time.Second)
	second.RequestID = uuid.NewString()
	second.Context = &pb.ContextRef{Id: "refused", Owner: process, State: &pb.StateRef{Authority: authority, GroupId: "refused", Incarnation: "incarnation", CallId: secondFacts.CallID}}
	if _, err = receiveRedisInvite(ctx, constrained, second); !errors.Is(err, rediscore.ErrCapacity) {
		t.Fatalf("capacity refusal: %v", err)
	}
	if n, e := client.Exists(ctx, root+"core:context:refused", root+"core:deadlines:refused").Result(); e != nil || n != 0 {
		t.Fatal("capacity refusal partially published INVITE", n, e)
	}
}
