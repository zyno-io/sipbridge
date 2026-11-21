package engine

import (
	"bytes"
	"context"
	"errors"
	"net"
	"os"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/nativepb"
	"sipbridge/internal/rediscore"
	"sipbridge/internal/selectedmessage"
	"sipbridge/internal/storagecodec"
)

func TestRedisDialogRequestAdmissionDefersDialogMutation(t *testing.T) {
	for _, method := range []sip.RequestMethod{sip.INFO, sip.REFER, sip.UPDATE, sip.NOTIFY} {
		t.Run(string(method), func(t *testing.T) {
			testRedisDialogRequestAdmissionDefersDialogMutation(t, method, false, false, false, 0)
		})
	}
	t.Run("INFO-CANCEL", func(t *testing.T) {
		testRedisDialogRequestAdmissionDefersDialogMutation(t, sip.INFO, true, false, false, 0)
	})
	t.Run("INFO-TRYING", func(t *testing.T) {
		testRedisDialogRequestAdmissionDefersDialogMutation(t, sip.INFO, false, true, false, 0)
	})
	t.Run("INFO-ACKTIMEOUT", func(t *testing.T) {
		testRedisDialogRequestAdmissionDefersDialogMutation(t, sip.INFO, false, false, true, 0)
	})
	t.Run("INFO-OWNER", func(t *testing.T) {
		testRedisDialogRequestAdmissionDefersDialogMutation(t, sip.INFO, false, false, false, 1)
	})
	t.Run("INFO-OWNER-ACK", func(t *testing.T) {
		testRedisDialogRequestAdmissionDefersDialogMutation(t, sip.INFO, false, false, false, 2)
	})
	t.Run("INFO-OWNER-LIVE", func(t *testing.T) {
		testRedisDialogRequestAdmissionDefersDialogMutation(t, sip.INFO, false, false, false, 3)
	})
	t.Run("owner-expiry-after-peer-BYE-before-ACK", func(t *testing.T) {
		testRedisDialogRequestAdmissionDefersDialogMutation(t, sip.INFO, false, false, false, 4)
	})
	t.Run("owner-expiry-at-procedure-limit", func(t *testing.T) {
		testRedisDialogRequestAdmissionDefersDialogMutation(t, sip.INFO, false, false, false, 5)
	})
	t.Run("terminal-re-INVITE-output", func(t *testing.T) {
		testRedisDialogRequestAdmissionDefersDialogMutation(t, sip.INFO, false, false, false, 6)
	})
}

func TestRedisPeerACKLifecycleRetention(t *testing.T) {
	for _, outcome := range []string{"accepted", "expired", "without-local-session", "no-capability", "changed-term", "fork-late", "fork-early"} {
		t.Run(outcome, func(t *testing.T) {
			testRedisDialogRequestAdmissionDefersDialogMutation(t, sip.INFO, false, false, false, 0, outcome)
		})
	}
}

func testRedisDialogRequestAdmissionDefersDialogMutation(t *testing.T, method sip.RequestMethod, cancelCase, tryingCase, ackTimeoutCase bool, ownerCase int, ackEvent ...string) {
	address := os.Getenv("SIPBRIDGE_CORE_TEST_REDIS")
	if address == "" {
		t.Skip("isolated Redis fixture required")
	}
	ctx := context.Background()
	client := redis.NewClient(&redis.Options{Addr: address, MaxRetries: -1})
	root := "sipstate:test:dialog-receive-" + uuid.NewString() + ":"
	key := bytes.Repeat([]byte{7}, 32)
	advertise := nativeSIPAdvertisement{contactURI: "sip:sip.example.test:5061", address: "sip.example.test:5061"}
	app := &pb.ProcessRef{InstanceId: "controller", Epoch: "epoch"}
	executor := &pb.ProcessRef{InstanceId: "replacement", Epoch: "new"}
	old := &pb.ProcessRef{InstanceId: "original", Epoch: "old"}
	appID, err := nativeApplicationReference(key, app)
	if err != nil {
		t.Fatal(err)
	}
	worker, err := nativeWorkerReference(executor)
	if err != nil {
		t.Fatal(err)
	}
	oldWorker, err := nativeWorkerReference(old)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		defer client.Close()
		for _, name := range []string{"profile", "capacity", "selectors", "context:call", "deadlines:call", "context:udp", "deadlines:udp", "applications", "application:" + appID, "owner:term", "owned:term", "owner-expiry", "coordinator", "worker:" + worker, "timers:" + worker, "ready:" + worker, "worker:" + oldWorker, "timers:" + oldWorker, "ready:" + oldWorker} {
			if e := client.Del(context.Background(), root+"core:"+name).Err(); e != nil {
				t.Error(e)
			}
		}
	})
	info, err := client.Info(ctx, "server").Result()
	if err != nil {
		t.Fatal(err)
	}
	primary := ""
	for _, line := range strings.Split(info, "\r\n") {
		if value, found := strings.CutPrefix(line, "run_id:"); found {
			primary = value
		}
	}
	profile := []byte{3, 2, 1}
	if err = client.Set(ctx, root+"core:profile", profile, 0).Err(); err != nil {
		t.Fatal(err)
	}
	store, err := rediscore.New(client, rediscore.Config{Root: root, Profile: profile, PrimaryRunID: primary, RecordBudget: 4 << 20})
	if err != nil {
		t.Fatal(err)
	}
	lease, err := store.AcquireApplication(ctx, appID, "term")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = store.RenewWorker(ctx, rediscore.WorkerLeaseRequest{Worker: worker, Epoch: executor.Epoch, PrimaryRunID: primary, Acquire: true}); err != nil {
		t.Fatal(err)
	}
	if _, err = store.RenewWorker(ctx, rediscore.WorkerLeaseRequest{Worker: oldWorker, Epoch: old.Epoch, PrimaryRunID: primary, Acquire: true}); err != nil {
		t.Fatal(err)
	}
	dialog := nativeEstablishedDialogFixture(t)
	dialog.RemoteCSeq, dialog.RemoteSet = 11, true
	oldFlow := &pb.FlowRef{Ingress: old, ConnectionId: "old", SourceIp: "192.0.2.2", SourcePort: 5061, Listener: "sip.example.test:5061", Transport: "tls"}
	flow := &pb.FlowRef{Ingress: executor, ConnectionId: "new", SourceIp: "192.0.2.3", SourcePort: 6789, Listener: "sip.example.test:5061", Transport: "tls"}
	ref := &pb.ContextRef{Id: "call", Owner: old, State: &pb.StateRef{Authority: &pb.StateAuthority{Service: "test", Generation: "one", PrimaryId: "primary", SchemaVersion: 1}, GroupId: "call", Incarnation: "inc", CallId: dialog.CallID}}
	current := &nativepb.Context{Context: ref, Application: app, ApplicationLeaseTerm: lease.Term, Kind: pb.ContextSnapshot_UAC, CallId: dialog.CallID, LocalTag: dialog.LocalTag, RemoteTag: dialog.RemoteTag,
		LocalUri: dialog.LocalURI, RemoteUri: dialog.RemoteURI, Transport: "tls", Flow: oldFlow, Answered: true, InitialAck: true, PrincipalId: "principal", BindingId: "binding", Dialogs: []string{"r:dialog"},
		Delivery: &pb.ContextDelivery{Flow: oldFlow, NativeRevision: 1, DirectoryEpoch: "directory", RegistrationTicket: "ticket"}}
	anchorField := "r:uac-procedure-0123456789abcdef0123456789abcdef"
	anchor, err := portable.NewTransaction(portable.InviteClient, dialog.LocalInvite.Request, portable.DefaultTimers(), "")
	if err != nil {
		t.Fatal(err)
	}
	anchor, _, err = portable.StepTransaction(anchor, portable.Input{Kind: portable.Start, Now: time.Now().UnixMilli()})
	if err != nil {
		t.Fatal(err)
	}
	anchorData, err := encodeNativeProcedure(anchor, nativeProcedureRefs{Origin: "r:invite"}, &nativepb.Procedure{Flow: oldFlow, Application: app, CommandResult: "r:result-1"})
	if err != nil {
		t.Fatal(err)
	}
	resultData, err := encodeNativeResult(&nativepb.Result{Certainty: pb.OperationResult_ACCEPTED, Request: "r:invite", Sequence: 1, OperationId: "initial", Fingerprint: bytes.Repeat([]byte{1}, 32), DecidingLeaseTerm: lease.Term, CutoffMs: time.Now().Add(time.Minute).UnixMilli()})
	if err != nil {
		t.Fatal(err)
	}
	current.CommandSequence = 1
	current.InitialProcedures = []string{anchorField}
	contextData, err := encodeNativeContext(current)
	if err != nil {
		t.Fatal(err)
	}
	dialogData, err := encodeNativeDialog(dialog, nativeDialogRefs{Local: &nativeInviteRefs{Request: "r:invite", Final: "r:final", ACK: "r:ack"}}, &nativepb.Dialog{Transaction: anchor.Key, LocalTransaction: anchor.Key, AckWritten: true})
	if err != nil {
		t.Fatal(err)
	}
	originData, err := selectedmessage.Marshal(dialog.LocalInvite.Request)
	if err != nil {
		t.Fatal(err)
	}
	finalData, err := selectedmessage.Marshal(*dialog.LocalInvite.Final)
	if err != nil {
		t.Fatal(err)
	}
	ackData, err := selectedmessage.Marshal(*dialog.LocalInvite.ACK)
	if err != nil {
		t.Fatal(err)
	}
	tags := []string{dialog.LocalTag, dialog.RemoteTag}
	slices.Sort(tags)
	id := rediscore.Identity{Context: "call", Incarnation: "inc", Owner: appID, Term: lease.Term}
	_, err = store.Commit(ctx, rediscore.Publication{Identity: id, Create: true, Deadline: time.Now().Add(time.Second), CreditDelta: 128 << 10, Application: &lease.ApplicationLeaseRef,
		Changes:   []rediscore.Change{{Field: "r:context", Value: contextData}, {Field: "r:dialog", Value: dialogData}, {Field: "r:invite", Value: originData}, {Field: "r:final", Value: finalData}, {Field: "r:ack", Value: ackData}, {Field: anchorField, Value: anchorData}, {Field: "r:result-1", Value: resultData}},
		Selectors: []rediscore.SelectorChange{{Selector: nativeSelector(rediscore.SelectorDialog, dialog.CallID, tags[0], tags[1]), Record: "r:dialog"}, {Selector: nativeSelector(rediscore.SelectorClientTransaction, anchor.Key), Record: anchorField}}})
	if err != nil {
		t.Fatal(err)
	}
	// The selected operation needs only the retained INVITE request. Reject
	// explicit reads of unrelated final/ACK fields rather than parsing them.
	client.AddHook(rejectUnusedDialogRead{})
	peer := portable.Dialog{CallID: dialog.CallID, LocalTag: dialog.RemoteTag, RemoteTag: dialog.LocalTag, LocalURI: dialog.RemoteURI, RemoteURI: dialog.LocalURI, Target: dialog.Contact, Contact: dialog.Target, SentBy: "192.0.2.3:6789", Transport: "tls"}
	var infoPresentation *pb.RequestPresented
	for _, selectedMethod := range []sip.RequestMethod{method} {
		message, e := portable.BuildDialogRequest(peer, selectedMethod, 12, nil, "z9hG4bK-pending-"+string(selectedMethod))
		if e != nil {
			t.Fatal(e)
		}
		message = portable.RestoreMessage(message.WireBytes(), "tls", "192.0.2.3:6789", "")
		requestID := uuid.NewString()
		presented, e := receiveRedisDialogRequest(ctx, store, redisDialogReceive{IdentityKey: key, Context: ref, Application: app, Executor: executor, Lease: lease.ApplicationLeaseRef, Flow: flow, Message: message, PrincipalID: "principal", BindingID: "binding", RequestID: requestID,
			Timer: rediscore.TimerAssignment{Worker: oldWorker, Epoch: old.Epoch, Term: old.Epoch}, Deadline: time.Now().Add(time.Second)})
		if e != nil {
			t.Fatal("pending dialog admission", selectedMethod, e)
		}
		if presented == nil || presented.FlowAdoption == nil || presented.FlowAdoption.NativeRevision != 2 || !proto.Equal(presented.Flow, flow) || !proto.Equal(presented.FlowAdoption.Flow, flow) || presented.Snapshot.CommandSequence != 1 || !proto.Equal(presented.Snapshot.Delivery, current.Delivery) {
			t.Fatal("pending admission published a replacement flow or lost its proposal")
		}
		view, e := store.Read(ctx, id, "r:context", "r:dialog", "r:procedure-"+requestID)
		if e != nil || !bytes.Equal(view.Records["r:dialog"], dialogData) {
			t.Fatal("pending request changed native CSeq or offer before decision", e)
		}
		retained, e := decodeNativeContext(view.Records["r:context"])
		if e != nil || !proto.Equal(retained.Flow, oldFlow) || !proto.Equal(retained.Delivery, current.Delivery) || !slices.Contains(retained.NoninitialProcedures, "r:procedure-"+requestID) {
			t.Fatal("pending request moved context delivery", e)
		}
		procedure, e := readPrivateProcedureRecord(view.Records["r:procedure-"+requestID])
		if e != nil || !procedure.DecisionPending || procedure.RequestDialog != "r:dialog" || procedure.LastResponse != "" || procedure.Emissions != 0 {
			t.Fatal("pending request selected a response before controller policy", e)
		}
		if selectedMethod == method {
			infoPresentation = presented
		}
	}
	before, err := store.Read(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	stale, err := portable.BuildDialogRequest(peer, method, 11, nil, "z9hG4bK-stale-info")
	if err != nil {
		t.Fatal(err)
	}
	stale = portable.RestoreMessage(stale.WireBytes(), "tls", "192.0.2.3:6789", "")
	if _, err = receiveRedisDialogRequest(ctx, store, redisDialogReceive{IdentityKey: key, Context: ref, Application: app, Executor: executor, Lease: lease.ApplicationLeaseRef, Flow: flow, Message: stale, RequestID: uuid.NewString(), Timer: rediscore.TimerAssignment{Worker: worker, Epoch: executor.Epoch, Term: executor.Epoch}, Deadline: time.Now().Add(time.Second)}); err == nil {
		t.Fatal("stale dialog sequence admitted")
	}
	after, err := store.Read(ctx, id)
	if err != nil || after.Revision != before.Revision {
		t.Fatal("rejected dialog request published pending work", err)
	}
	redisNow, err := client.Time(ctx).Result()
	if err != nil {
		t.Fatal(err)
	}
	if err = client.HSet(ctx, root+"core:worker:"+oldWorker, "expires_ms", redisNow.Add(-time.Second).UnixMilli()).Err(); err != nil {
		t.Fatal(err)
	}
	decision := residentDirectDecision(infoPresentation, 2, 0)
	decision.PrincipalId, decision.BindingId, decision.Basis = "principal", "binding", pb.OwnerIngest_TLS_DIALOG_CONTINUITY
	selected, err := store.ObserveApplicationWitness(ctx, "call", "inc", lease.ApplicationLeaseRef, "r:context", "r:procedure-"+decision.RequestId, "r:origin-"+decision.RequestId, "r:result-1", "r:result-2")
	if err != nil {
		t.Fatal(err)
	}
	selectedContext, err := decodeNativeContext(selected.View.Records["r:context"])
	if err != nil {
		t.Fatal(err)
	}
	candidate, err := prepareRedisDialogDecision(ctx, store, selected, selectedContext, key, app, lease.ApplicationLeaseRef, decision, "info-defer", time.Now().Add(time.Second))
	if err != nil {
		t.Fatal("prepare INFO", err)
	}
	expiredCandidate := candidate
	expiredCandidate.policy.policy = proto.Clone(candidate.policy.policy).(*pb.CommandAdmission)
	expiredCandidate.policy.policy.ExpiresAtMs = uint64(time.Now().Add(-time.Second).UnixMilli())
	if _, e := commitRedisDialogDecision(ctx, store, expiredCandidate, key, lease.ApplicationLeaseRef, decision, "info-defer", advertise, rediscore.TimerAssignment{Worker: worker, Epoch: executor.Epoch, Term: executor.Epoch}, time.Now().Add(time.Second)); !errors.Is(e, rediscore.ErrExpired) {
		t.Fatal("expired decision policy accepted", e)
	}
	accepted, err := commitRedisDialogDecision(ctx, store, candidate, key, lease.ApplicationLeaseRef, decision, "info-defer", advertise, rediscore.TimerAssignment{Worker: worker, Epoch: executor.Epoch, Term: executor.Epoch}, time.Now().Add(time.Second))
	if err != nil || accepted.Certainty != pb.OperationResult_ACCEPTED || accepted.Snapshot.CommandSequence != 2 || !proto.Equal(accepted.Snapshot.Delivery, decision.Delivery) {
		t.Fatal("defer INFO", accepted, err)
	}
	if rebound, e := store.Read(ctx, id); e != nil || rebound.Timer.Worker != worker {
		t.Fatal("expired executor not rebound", e)
	}
	committed, err := store.Read(ctx, id, "r:context", "r:dialog", "r:procedure-"+decision.RequestId, "r:result-2")
	if err != nil {
		t.Fatal(err)
	}
	selectedDialog, err := readPrivateDialogRecord(committed.Records["r:dialog"])
	if err != nil || selectedDialog.Native.RemoteCSeq != 12 {
		t.Fatal("INFO remote sequence", err)
	}
	selectedProcedure, err := readPrivateProcedureRecord(committed.Records["r:procedure-"+decision.RequestId])
	if err != nil || selectedProcedure.DecisionPending || selectedProcedure.RequestDialog != "" {
		t.Fatal("INFO pending state retained", err)
	}
	command := &pb.Command{Context: ref, Sequence: 3, Admission: &pb.CommandAdmission{DirectoryEpoch: "directory", ExpectedDelivery: decision.Delivery, ExpiresAtMs: uint64(time.Now().Add(time.Second).UnixMilli())}, Action: &pb.Command_Respond{Respond: &pb.Respond{RequestId: decision.RequestId, Status: 200}}}
	capture, err := captureRedisNotifyWait(command)
	if err != nil {
		t.Fatal(err)
	}
	identity, err := publishRedisDialogRespond(ctx, store, key, app, lease.ApplicationLeaseRef, command, capture, "info-response", advertise, rediscore.TimerAssignment{Worker: worker, Epoch: executor.Epoch, Term: executor.Epoch}, time.Now().Add(time.Second))
	if err != nil {
		t.Fatal("publish INFO response", err)
	}
	entries, err := store.PeekReady(ctx, worker, executor.Epoch, 2)
	if err != nil || len(entries) != 1 {
		t.Fatal("INFO output not ready", err)
	}
	runtime := &redisNative{worker: worker, cfg: Config{Process: executor, StateIdentityKey: key}}
	responseContact, _, err := advertise.resolve("tls")
	if err != nil {
		t.Fatal(err)
	}
	if method == sip.REFER {
		responseContact = dialog.Contact
	}
	runtime.cfg.WriteCommitted = func(ctx context.Context, write *pb.FlowWrite) *pb.DeliveryResult {
		reserved, reserveErr := (nativeRedisWriter{store: store}).Reserve(ctx, write)
		if reserveErr != nil || reserved == nil || len(reserved.Wire) == 0 {
			t.Errorf("reserve INFO: %v", reserveErr)
			return nil
		}
		assertDialogResponseContact(t, reserved.Wire, responseContact)
		return &pb.DeliveryResult{Effect: write.Effect, Outcome: pb.DeliveryResult_WRITTEN}
	}
	if err = runtime.readyOutput(ctx, store, entries[0]); err != nil {
		t.Fatal("INFO physical dispatch", err)
	}
	if _, err = store.InspectOutput(ctx, entries[0].Ref); !errors.Is(err, rediscore.ErrAbsent) {
		t.Fatal("INFO proof not consumed", err)
	}
	completed, err := store.ObserveContext(ctx, "call", "inc", "r:context", "r:result-1", "r:result-3")
	if err != nil {
		t.Fatal(err)
	}
	infoResult, err := decodeNativeResult(completed.View.Records["r:result-3"])
	if err != nil || infoResult.Certainty != pb.OperationResult_COMPLETED || infoResult.SipStatus != 200 || infoResult.ServerRequestId != decision.RequestId || infoResult.Output == "" {
		t.Fatal("INFO response receipt", err)
	}
	if _, err = decodeNativeResult(completed.View.Records["r:result-1"]); err != nil {
		t.Fatal("original UAC marker lost", err)
	}
	due := &rediscore.DueContext{Identity: identity, Revision: completed.View.Revision, Assignment: completed.View.Timer, Member: "out-" + strings.TrimPrefix(infoResult.Output, "r:"), DeadlineMS: entries[0].DeadlineMS, CheckedAtMS: entries[0].DeadlineMS}
	if err = dispatchRedisDialogOutputCutoff(ctx, store, due, time.Now().Add(time.Second)); err != nil {
		t.Fatal("INFO output cutoff", err)
	}
	clean, err := store.ObserveContext(ctx, "call", "inc", "r:context", "r:dialog", "r:procedure-"+decision.RequestId, "r:result-3")
	if err != nil {
		t.Fatal(err)
	}
	cleanResult, err := decodeNativeResult(clean.View.Records["r:result-3"])
	if err != nil || cleanResult.Output != "" {
		t.Fatal("INFO output backlink retained", err)
	}
	procDue := &rediscore.DueContext{Identity: identity, Revision: clean.View.Revision, Assignment: clean.View.Timer, Member: "call-proc-" + decision.RequestId, DeadlineMS: entries[0].DeadlineMS, CheckedAtMS: entries[0].DeadlineMS}
	if err = retireRedisDialogServerProcedure(ctx, store, key, procDue, time.Now().Add(time.Second)); err != nil {
		t.Fatal("INFO procedure retirement", err)
	}
	retired, err := store.ObserveContext(ctx, "call", "inc", "r:context", "r:procedure-"+decision.RequestId, "r:result-3")
	if err != nil || len(retired.View.Records["r:procedure-"+decision.RequestId]) != 0 || len(retired.View.Records["r:result-3"]) == 0 {
		t.Fatal("INFO receipt/procedure ownership", err)
	}
	waiter := &nativeResultWaiter{changed: make(chan struct{})}
	answer, err := runtime.awaitRedisInviteRespondResult(ctx, store, identity, lease.ApplicationLeaseRef, capture, decision.RequestId, "info-response", time.Now().Add(time.Second), time.Now().Add(2*time.Second), waiter, true)
	if err != nil || answer.Certainty != pb.OperationResult_COMPLETED || answer.SipStatus != 200 {
		t.Fatal("retired INFO result replay", err)
	}
	second, err := portable.BuildDialogRequest(peer, method, 13, nil, "z9hG4bK-refused-info")
	if err != nil {
		t.Fatal(err)
	}
	second = portable.RestoreMessage(second.WireBytes(), "tls", "192.0.2.3:6789", "")
	secondPresentation, err := receiveRedisDialogRequest(ctx, store, redisDialogReceive{IdentityKey: key, Context: ref, Application: app, Executor: executor, Lease: lease.ApplicationLeaseRef, Flow: flow, Message: second, PrincipalID: "principal", BindingID: "binding", RequestID: uuid.NewString(), Timer: rediscore.TimerAssignment{Worker: worker, Epoch: executor.Epoch, Term: executor.Epoch}, Deadline: time.Now().Add(time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	badDecision := residentDirectDecision(secondPresentation, 4, 200)
	badDecision.PrincipalId, badDecision.BindingId, badDecision.Basis = "principal", "binding", pb.OwnerIngest_EXISTING_FLOW
	badDecision.Headers = []*pb.Header{{Name: "invalid\nheader", Value: "x"}}
	badSelection, err := store.ObserveApplicationWitness(ctx, "call", "inc", lease.ApplicationLeaseRef, "r:context", "r:procedure-"+badDecision.RequestId, "r:origin-"+badDecision.RequestId, "r:result-3", "r:result-4")
	if err != nil {
		t.Fatal(err)
	}
	badContext, err := decodeNativeContext(badSelection.View.Records["r:context"])
	if err != nil {
		t.Fatal(err)
	}
	badCandidate, err := prepareRedisDialogDecision(ctx, store, badSelection, badContext, key, app, lease.ApplicationLeaseRef, badDecision, "bad-info", time.Now().Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	refused, err := commitRedisDialogDecision(ctx, store, badCandidate, key, lease.ApplicationLeaseRef, badDecision, "bad-info", advertise, rediscore.TimerAssignment{Worker: worker, Epoch: executor.Epoch, Term: executor.Epoch}, time.Now().Add(time.Second))
	if err != nil || refused.Certainty != pb.OperationResult_NOT_ACCEPTED || refused.Snapshot.CommandSequence != 4 {
		t.Fatal("semantic refusal not recorded", refused, err)
	}
	refusedView, err := store.ObserveApplicationContext(ctx, "call", "inc", lease.ApplicationLeaseRef, "r:context", "r:dialog", "r:procedure-"+badDecision.RequestId, "r:result-4")
	if err != nil {
		t.Fatal(err)
	}
	refusedDialog, err := readPrivateDialogRecord(refusedView.View.Records["r:dialog"])
	if err != nil || refusedDialog.Native.RemoteCSeq != 12 {
		t.Fatal("refusal changed dialog sequence", err)
	}
	refusedProcedure, err := readPrivateProcedureRecord(refusedView.View.Records["r:procedure-"+badDecision.RequestId])
	if err != nil || !refusedProcedure.DecisionPending {
		t.Fatal("refusal consumed pending request", err)
	}
	redisNow, err = client.Time(ctx).Result()
	if err != nil {
		t.Fatal(err)
	}
	refusedProcedure.PresentDeadlineMs = redisNow.Add(-time.Minute).UnixMilli()
	procedureBytes, err := marshalNativeRecord(storagecodec.NativeProcedure, refusedProcedure)
	if err != nil {
		t.Fatal(err)
	}
	decisionMember := "call-decision-" + badDecision.RequestId
	_, err = store.Commit(ctx, rediscore.Publication{Identity: refusedView.Identity, Revision: refusedView.View.Revision, Deadline: time.Now().Add(time.Second), SpendReservation: true, Changes: []rediscore.Change{{Field: "r:procedure-" + badDecision.RequestId, Value: procedureBytes}}, Timers: &rediscore.TimerMutation{Expected: refusedView.View.Timer, Target: refusedView.View.Timer, RequireLease: true, Changes: []rediscore.TimerChange{{Member: decisionMember, Due: time.UnixMilli(refusedProcedure.PresentDeadlineMs)}}}})
	if err != nil {
		t.Fatal(err)
	}
	timeoutDue, err := store.PeekDue(ctx, worker, executor.Epoch)
	if err != nil || timeoutDue == nil || timeoutDue.Member != decisionMember {
		t.Fatal("INFO decision not due", timeoutDue, err)
	}
	timeoutDeadline := time.Now().Add(400 * time.Millisecond)
	if err = timeoutRedisDialogServer(ctx, store, key, advertise, timeoutDue, timeoutDeadline); err != nil {
		t.Fatal("INFO timeout", err)
	}
	timedOut, err := store.ObserveContext(ctx, "call", "inc", "r:context", "r:dialog", "r:procedure-"+badDecision.RequestId, "r:dialog-output-"+badDecision.RequestId+"-0")
	if err != nil {
		t.Fatal(err)
	}
	timedOutProcedure, err := readPrivateProcedureRecord(timedOut.View.Records["r:procedure-"+badDecision.RequestId])
	if err != nil || timedOutProcedure.DecisionPending {
		t.Fatal("INFO timeout left pending", err)
	}
	timedOutDialog, err := readPrivateDialogRecord(timedOut.View.Records["r:dialog"])
	if err != nil || timedOutDialog.Native.RemoteCSeq != 12 {
		t.Fatal("INFO timeout changed dialog sequence", err)
	}
	timedOutOutput, err := decodeNativeOutput(timedOut.View.Records["r:dialog-output-"+badDecision.RequestId+"-0"])
	if err != nil || timedOutOutput.Status != 503 || timedOutOutput.Result != "" {
		t.Fatal("INFO timeout response", err)
	}
	unknownEntries, err := store.PeekReady(ctx, worker, executor.Epoch, 2)
	if err != nil || len(unknownEntries) != 1 {
		t.Fatal("timeout output not ready", err)
	}
	unknownWrite := timedOutOutput.Write
	writer := nativeRedisWriter{store: store}
	unknownReservation, err := writer.Reserve(ctx, unknownWrite)
	if err != nil {
		t.Fatal(err)
	}
	timeoutContact, _, err := advertise.resolve("tls")
	if err != nil {
		t.Fatal(err)
	}
	assertDialogResponseContact(t, unknownReservation.Wire, timeoutContact)
	if err = writer.Complete(ctx, unknownWrite, &pb.DeliveryResult{Effect: unknownWrite.Effect, Outcome: pb.DeliveryResult_UNKNOWN}); err != nil {
		t.Fatal(err)
	}
	if wait := time.Until(time.UnixMilli(unknownEntries[0].DeadlineMS)); wait > 0 {
		time.Sleep(wait + 5*time.Millisecond)
	}
	if _, err = store.RenewWorker(ctx, rediscore.WorkerLeaseRequest{Worker: worker, Epoch: executor.Epoch, PrimaryRunID: primary}); err != nil {
		t.Fatal(err)
	}
	unknownDue, err := waitCallTestDue(ctx, store, worker, executor.Epoch, time.Second)
	if err != nil || unknownDue == nil || unknownDue.Member != "out-dialog-output-"+badDecision.RequestId+"-0" {
		t.Fatal("INFO uncertainty not due", unknownDue, err)
	}
	if err = dispatchRedisDialogOutputCutoff(ctx, store, unknownDue, time.Now().Add(time.Second)); err != nil {
		t.Fatal("INFO unknown cutoff", err)
	}
	unknownProof, err := store.InspectOutput(ctx, unknownEntries[0].Ref)
	if err != nil || unknownProof.State != rediscore.OutputUnknown {
		t.Fatal("INFO uncertainty discarded or recast", err)
	}
	unknownView, err := store.ObserveContext(ctx, "call", "inc", "r:dialog", "r:procedure-"+badDecision.RequestId)
	if err != nil {
		t.Fatal(err)
	}
	unknownDialog, err := readPrivateDialogRecord(unknownView.View.Records["r:dialog"])
	if err != nil || !slices.Contains(unknownDialog.Outputs, unknownProof.MetadataField) {
		t.Fatal("INFO unknown dialog owner lost", err)
	}
	unknownProcedure, err := readPrivateProcedureRecord(unknownView.View.Records["r:procedure-"+badDecision.RequestId])
	if err != nil || !slices.Contains(unknownProcedure.Outputs, unknownProof.MetadataField) {
		t.Fatal("INFO unknown procedure owner lost", err)
	}
	udpFlow := proto.Clone(flow).(*pb.FlowRef)
	udpFlow.Transport = "udp"
	udpPeer := portable.Dialog{CallID: "udp-info@example.test", LocalTag: dialog.RemoteTag, RemoteTag: dialog.LocalTag, LocalURI: dialog.RemoteURI, RemoteURI: dialog.LocalURI, Target: dialog.Contact, Contact: dialog.Target, SentBy: "192.0.2.3:6789", Transport: "udp"}
	udpRequest, err := portable.BuildDialogRequest(udpPeer, sip.INFO, 14, nil, "z9hG4bK-udp-info")
	if err != nil {
		t.Fatal(err)
	}
	udpRequest = portable.RestoreMessage(udpRequest.WireBytes(), "udp", "192.0.2.3:6789", "")
	udpTx, _, err := nativeServerStart(udpRequest, dialog.LocalTag, time.Now().UnixMilli())
	if err != nil {
		t.Fatal(err)
	}
	udpResponse, err := nativeServerBuildResponseForFlow(udpTx, 200, reason(200), nil, dialog.LocalTag, "", nil, udpFlow, time.Now().UnixMilli())
	if err != nil {
		t.Fatal(err)
	}
	udpTx = udpResponse.Transaction
	redisNow, err = client.Time(ctx).Result()
	if err != nil {
		t.Fatal(err)
	}
	j := udpTx.Timers[portable.TimerJ]
	if !j.Active {
		t.Fatal("UDP INFO Timer J missing")
	}
	j.At = redisNow.Add(-time.Minute).UnixMilli()
	udpTx.Timers[portable.TimerJ] = j
	udpID := uuid.NewString()
	udpProcedure, udpOrigin, udpFinal := "r:procedure-"+udpID, "r:origin-"+udpID, "r:dialog-response-"+udpID+"-1"
	udpProcedureData, err := encodeNativeProcedure(udpTx, nativeProcedureRefs{Origin: udpOrigin, LastResponse: udpFinal}, &nativepb.Procedure{RequestId: udpID, Flow: udpFlow, Application: app, CreditContribution: uint64(residentSmallBudget)})
	if err != nil {
		t.Fatal(err)
	}
	udpOriginData, err := selectedmessage.Marshal(udpRequest)
	if err != nil {
		t.Fatal(err)
	}
	udpFinalData, err := selectedmessage.Marshal(udpResponse.Message)
	if err != nil {
		t.Fatal(err)
	}
	udpContext := proto.Clone(current).(*nativepb.Context)
	udpContext.Context = proto.Clone(ref).(*pb.ContextRef)
	udpContext.Context.Id, udpContext.Context.State.GroupId, udpContext.Context.State.Incarnation = "udp", "udp", "udp-inc"
	udpContext.Context.State.CallId, udpContext.CallId = udpPeer.CallID, udpPeer.CallID
	udpContext.Flow, udpContext.Transport, udpContext.Delivery = udpFlow, "udp", nil
	udpContext.InitialProcedures, udpContext.Dialogs, udpContext.NoninitialProcedures = nil, nil, []string{udpProcedure}
	udpContextData, err := encodeNativeContext(udpContext)
	if err != nil {
		t.Fatal(err)
	}
	udpSelectors, err := nativeSelectorIDs(udpRequest, false, false)
	if err != nil || len(udpSelectors) != 2 {
		t.Fatal("UDP selector", err)
	}
	if _, e := store.LookupSelector(ctx, udpSelectors[0]); !errors.Is(e, rediscore.ErrAbsent) {
		t.Fatal("UDP transaction selector already exists", e)
	}
	udpIdentity := rediscore.Identity{Context: "udp", Incarnation: "udp-inc", Owner: appID, Term: lease.Term}
	jMember := "call-tx-" + udpID + "-J"
	_, err = store.Commit(ctx, rediscore.Publication{Identity: udpIdentity, Create: true, Deadline: time.Now().Add(time.Second), Application: &lease.ApplicationLeaseRef, CreditDelta: residentSmallBudget, Changes: []rediscore.Change{{Field: "r:context", Value: udpContextData}, {Field: udpProcedure, Value: udpProcedureData}, {Field: udpOrigin, Value: udpOriginData}, {Field: udpFinal, Value: udpFinalData}}, Selectors: []rediscore.SelectorChange{{Selector: udpSelectors[0], Record: udpProcedure}}, Timers: &rediscore.TimerMutation{Target: rediscore.TimerAssignment{Worker: worker, Epoch: executor.Epoch, Term: executor.Epoch}, Changes: []rediscore.TimerChange{{Member: jMember, Due: time.UnixMilli(j.At)}}}})
	if err != nil {
		t.Fatal("create UDP INFO", err)
	}
	jDue, err := store.PeekDue(ctx, worker, executor.Epoch)
	if err != nil || jDue == nil || jDue.Member != jMember {
		t.Fatal("UDP J not due", jDue, err)
	}
	if err = fireRedisDialogServerJ(ctx, store, jDue, time.Now().Add(time.Second)); err != nil {
		t.Fatal("UDP J", err)
	}
	closed, err := store.ObserveContext(ctx, "udp", "udp-inc", udpProcedure)
	if err != nil {
		t.Fatal(err)
	}
	closedProcedure, err := readPrivateProcedureRecord(closed.View.Records[udpProcedure])
	if err != nil || closedProcedure.Native.Phase != string(portable.Terminated) {
		t.Fatal("UDP INFO not terminated", err)
	}
	retireDue, err := store.PeekDue(ctx, worker, executor.Epoch)
	if err != nil || retireDue == nil || retireDue.Member != "call-proc-"+udpID {
		t.Fatal("UDP INFO retirement not due", retireDue, err)
	}
	if err = retireRedisDialogServerProcedure(ctx, store, key, retireDue, time.Now().Add(time.Second)); err != nil {
		t.Fatal("UDP INFO retirement", err)
	}
	closed, err = store.ObserveContext(ctx, "udp", "udp-inc", "r:context", udpProcedure)
	if err != nil || len(closed.View.Records[udpProcedure]) != 0 {
		t.Fatal("UDP INFO procedure retained", err)
	}
	if _, err = store.RenewWorker(ctx, rediscore.WorkerLeaseRequest{Worker: worker, Epoch: executor.Epoch, PrimaryRunID: primary}); err != nil {
		t.Fatal(err)
	}
	finalRequest, err := portable.BuildDialogRequest(peer, method, 13, nil, "z9hG4bK-final-info")
	if err != nil {
		t.Fatal(err)
	}
	finalRequest = portable.RestoreMessage(finalRequest.WireBytes(), "tls", "192.0.2.3:6789", "")
	finalPresentation, err := receiveRedisDialogRequest(ctx, store, redisDialogReceive{IdentityKey: key, Context: ref, Application: app, Executor: executor, Lease: lease.ApplicationLeaseRef, Flow: flow, Message: finalRequest, PrincipalID: "principal", BindingID: "binding", RequestID: uuid.NewString(), Timer: rediscore.TimerAssignment{Worker: worker, Epoch: executor.Epoch, Term: executor.Epoch}, Deadline: time.Now().Add(time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	finalDecision := residentDirectDecision(finalPresentation, 5, 200)
	finalDecision.PrincipalId, finalDecision.BindingId, finalDecision.Basis = "principal", "binding", pb.OwnerIngest_EXISTING_FLOW
	finalSelection, err := store.ObserveApplicationWitness(ctx, "call", "inc", lease.ApplicationLeaseRef, "r:context", "r:procedure-"+finalDecision.RequestId, "r:origin-"+finalDecision.RequestId, "r:result-4", "r:result-5")
	if err != nil {
		t.Fatal(err)
	}
	finalContext, err := decodeNativeContext(finalSelection.View.Records["r:context"])
	if err != nil {
		t.Fatal(err)
	}
	finalCandidate, err := prepareRedisDialogDecision(ctx, store, finalSelection, finalContext, key, app, lease.ApplicationLeaseRef, finalDecision, "final-info", time.Now().Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	finalResult, err := commitRedisDialogDecision(ctx, store, finalCandidate, key, lease.ApplicationLeaseRef, finalDecision, "final-info", advertise, rediscore.TimerAssignment{Worker: worker, Epoch: executor.Epoch, Term: executor.Epoch}, time.Now().Add(time.Second))
	if err != nil || finalResult.Certainty != pb.OperationResult_ACCEPTED || finalResult.SipStatus != 200 {
		t.Fatal("final INFO decision", err)
	}
	finalEntries, err := store.PeekReady(ctx, worker, executor.Epoch, 4)
	if err != nil || len(finalEntries) != 1 {
		t.Fatal("final INFO output", err)
	}
	runtime.cfg.WriteCommitted = func(ctx context.Context, write *pb.FlowWrite) *pb.DeliveryResult {
		reserved, reserveErr := (nativeRedisWriter{store: store}).Reserve(ctx, write)
		if reserveErr != nil || reserved == nil || len(reserved.Wire) == 0 {
			t.Errorf("reserve final INFO: %v", reserveErr)
			return nil
		}
		assertDialogResponseContact(t, reserved.Wire, responseContact)
		return &pb.DeliveryResult{Effect: write.Effect, Outcome: pb.DeliveryResult_NOT_WRITTEN}
	}
	if err = runtime.readyOutput(ctx, store, finalEntries[0]); err != nil {
		t.Fatal("final INFO not-written", err)
	}
	finalReceiptView, err := store.ObserveContext(ctx, "call", "inc", "r:result-5")
	if err != nil {
		t.Fatal(err)
	}
	finalReceipt, err := decodeNativeResult(finalReceiptView.View.Records["r:result-5"])
	if err != nil || finalReceipt.Certainty != pb.OperationResult_ACCEPTED || finalReceipt.SipStatus != 200 {
		t.Fatal("INFO write changed decision certainty", err)
	}
	var offer []byte
	if method == sip.INFO && (len(ackEvent) == 0 || ackEvent[0] == "fork-early") {
		offer = []byte("v=0\r\ns=hold\r\n")
	}
	invite, err := portable.BuildDialogRequest(peer, sip.INVITE, 14, offer, "z9hG4bK-pending-invite")
	if err != nil {
		t.Fatal(err)
	}
	invite = portable.RestoreMessage(invite.WireBytes(), "tls", "192.0.2.3:6789", "")
	inviteID := uuid.NewString()
	invitePresentation, err := receiveRedisDialogRequest(ctx, store, redisDialogReceive{IdentityKey: key, Context: ref, Application: app, Executor: executor, Lease: lease.ApplicationLeaseRef, Flow: flow, Message: invite, PrincipalID: "principal", BindingID: "binding", RequestID: inviteID, Timer: rediscore.TimerAssignment{Worker: worker, Epoch: executor.Epoch, Term: executor.Epoch}, Deadline: time.Now().Add(time.Second)})
	if err != nil || invitePresentation == nil {
		t.Fatal("pending in-dialog INVITE", err)
	}
	inviteView, err := store.Read(ctx, id, "r:dialog", "r:procedure-"+inviteID)
	if err != nil {
		t.Fatal(err)
	}
	inviteDialog, err := readPrivateDialogRecord(inviteView.Records["r:dialog"])
	if err != nil || inviteDialog.Native.RemoteCSeq != 13 {
		t.Fatal("pending INVITE changed dialog", err)
	}
	inviteProcedure, err := readPrivateProcedureRecord(inviteView.Records["r:procedure-"+inviteID])
	if err != nil || !inviteProcedure.DecisionPending || inviteProcedure.RequestDialog != "r:dialog" || inviteProcedure.LastResponse != "" {
		t.Fatal("pending INVITE selected response", err)
	}
	if method != sip.INFO {
		return
	}
	if tryingCase {
		redisNow, err = client.Time(ctx).Result()
		if err != nil {
			t.Fatal(err)
		}
		trying := inviteProcedure.Native.Timers.Values[string(portable.TimerTrying)]
		if trying == nil || !trying.Active {
			t.Fatal("missing native Trying")
		}
		trying.At = redisNow.Add(-time.Minute).UnixMilli()
		data, e := marshalNativeRecord(storagecodec.NativeProcedure, inviteProcedure)
		if e != nil {
			t.Fatal(e)
		}
		member := "call-tx-" + inviteID + "-" + string(portable.TimerTrying)
		_, e = store.Commit(ctx, rediscore.Publication{Identity: id, Revision: inviteView.Revision, Deadline: time.Now().Add(time.Second), SpendReservation: true, Changes: []rediscore.Change{{Field: "r:procedure-" + inviteID, Value: data}}, Timers: &rediscore.TimerMutation{Expected: inviteView.Timer, Target: inviteView.Timer, RequireLease: true, Changes: []rediscore.TimerChange{{Member: member, Due: time.UnixMilli(trying.At)}}}})
		if e != nil {
			t.Fatal(e)
		}
		tryingDue, e := store.PeekDue(ctx, worker, executor.Epoch)
		if e != nil || tryingDue == nil || tryingDue.Member != member {
			t.Fatal("Trying not due", tryingDue, e)
		}
		if e = fireRedisInviteTrying(ctx, store, key, tryingDue, time.Now().Add(time.Second)); e != nil {
			t.Fatal("retain re-INVITE Trying", e)
		}
		held, e := store.Read(ctx, id, "r:procedure-"+inviteID, "r:dialog-output-"+inviteID+"-6")
		if e != nil {
			t.Fatal(e)
		}
		heldProcedure, e := readPrivateProcedureRecord(held.Records["r:procedure-"+inviteID])
		if e != nil || heldProcedure.LastResponse != "r:dialog-response-"+inviteID+"-0" || heldProcedure.Emissions != 0 || len(held.Records["r:dialog-output-"+inviteID+"-6"]) != 0 {
			t.Fatal("Trying emitted before admission", e)
		}
	}
	inviteDecision := residentDirectDecision(invitePresentation, 6, 0)
	inviteDecision.PrincipalId, inviteDecision.BindingId, inviteDecision.Basis = "principal", "binding", pb.OwnerIngest_EXISTING_FLOW
	inviteSelection, err := store.ObserveApplicationWitness(ctx, "call", "inc", lease.ApplicationLeaseRef, "r:context", "r:procedure-"+inviteID, "r:origin-"+inviteID, "r:result-5", "r:result-6")
	if err != nil {
		t.Fatal(err)
	}
	inviteContext, err := decodeNativeContext(inviteSelection.View.Records["r:context"])
	if err != nil {
		t.Fatal(err)
	}
	inviteCandidate, err := prepareRedisDialogDecision(ctx, store, inviteSelection, inviteContext, key, app, lease.ApplicationLeaseRef, inviteDecision, "reinvite-defer", time.Now().Add(time.Second))
	if err != nil {
		t.Fatal("prepare re-INVITE", err)
	}
	deferred, err := commitRedisDialogDecision(ctx, store, inviteCandidate, key, lease.ApplicationLeaseRef, inviteDecision, "reinvite-defer", advertise, rediscore.TimerAssignment{Worker: worker, Epoch: executor.Epoch, Term: executor.Epoch}, time.Now().Add(time.Second))
	if err != nil || deferred.Certainty != pb.OperationResult_ACCEPTED {
		t.Fatal("defer re-INVITE", err)
	}
	if tryingCase {
		staged, e := store.Read(ctx, id, "r:procedure-"+inviteID, "r:dialog", "r:dialog-output-"+inviteID+"-6", "r:dialog-response-"+inviteID+"-0")
		if e != nil {
			t.Fatal(e)
		}
		owner, e := readPrivateProcedureRecord(staged.Records["r:procedure-"+inviteID])
		if e != nil || owner.LastResponse != "r:dialog-response-"+inviteID+"-6" || owner.Emissions != 1 || len(staged.Records["r:dialog-response-"+inviteID+"-0"]) != 0 {
			t.Fatal("held Trying not moved on admission", e)
		}
		dialog, e := readPrivateDialogRecord(staged.Records["r:dialog"])
		if e != nil || !slices.Contains(dialog.Outputs, "r:dialog-output-"+inviteID+"-6") || len(staged.Records["r:dialog-output-"+inviteID+"-6"]) == 0 {
			t.Fatal("held Trying output lacks owners", e)
		}
		cancel, e := portable.BuildCancel(invite)
		if e != nil {
			t.Fatal(e)
		}
		cancel = portable.RestoreMessage(cancel.WireBytes(), "tls", "192.0.2.3:6789", "")
		selected, handled, e := selectRedisServerCancel(ctx, store, executor, flow, cancel, key, time.Now().Add(time.Second))
		if e != nil || !handled {
			t.Fatal("select CANCEL with retained Trying", handled, e)
		}
		if e = publishRedisInitialServerCancel(ctx, store, selected, key, rediscore.TimerAssignment{Worker: worker, Epoch: executor.Epoch, Term: executor.Epoch}, time.Now().Add(time.Second)); e != nil {
			t.Fatal("CANCEL with retained Trying", e)
		}
		retained, e := store.Read(ctx, id, "r:dialog-output-"+inviteID+"-6", "r:dialog-output-"+inviteID+"-0", "r:dialog", "r:procedure-"+inviteID)
		if e != nil {
			t.Fatal(e)
		}
		owner, e = readPrivateProcedureRecord(retained.Records["r:procedure-"+inviteID])
		if e != nil || !slices.Contains(owner.Outputs, "r:dialog-output-"+inviteID+"-6") || !slices.Contains(owner.Outputs, "r:dialog-output-"+inviteID+"-0") || len(retained.Records["r:dialog-output-"+inviteID+"-6"]) == 0 || len(retained.Records["r:dialog-output-"+inviteID+"-0"]) == 0 {
			t.Fatal("CANCEL lost pinned Trying", e)
		}
		return
	}
	pendingView, err := store.Read(ctx, id, "r:dialog")
	if err != nil {
		t.Fatal(err)
	}
	pendingDialog, err := readPrivateDialogRecord(pendingView.Records["r:dialog"])
	if err != nil || pendingDialog.Native.RemoteInvite == nil || pendingDialog.Native.RemoteInvite.State != string(portable.InvitePending) || pendingDialog.RemoteInvite.Request != "r:origin-"+inviteID || pendingDialog.Transaction != anchor.Key {
		t.Fatal("re-INVITE pending association", err)
	}
	reinviteResponse := &pb.Command{Context: ref, Sequence: 7, Admission: &pb.CommandAdmission{DirectoryEpoch: "directory", ExpectedDelivery: decision.Delivery, ExpiresAtMs: uint64(time.Now().Add(time.Second).UnixMilli())}, Action: &pb.Command_Respond{Respond: &pb.Respond{RequestId: inviteID, Status: 200, Body: []byte("v=0\r\ns=answer\r\n")}}}
	reinviteCapture, err := captureRedisNotifyWait(reinviteResponse)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = publishRedisDialogRespond(ctx, store, key, app, lease.ApplicationLeaseRef, reinviteResponse, reinviteCapture, "reinvite-response", advertise, rediscore.TimerAssignment{Worker: worker, Epoch: executor.Epoch, Term: executor.Epoch}, time.Now().Add(time.Second)); err != nil {
		t.Fatal("respond to re-INVITE", err)
	}
	reinviteEntries, err := store.PeekReady(ctx, worker, executor.Epoch, 4)
	if err != nil {
		t.Fatal(err)
	}
	var reinviteEntry *rediscore.ReadyOutput
	for i := range reinviteEntries {
		metadata, e := decodeNativeOutput(reinviteEntries[i].Metadata)
		if e != nil {
			t.Fatal(e)
		}
		if metadata.Method == "INVITE" {
			reinviteEntry = &reinviteEntries[i]
		}
	}
	if reinviteEntry == nil {
		t.Fatal("re-INVITE response not ready")
	}
	if ownerCase == 6 {
		output, e := decodeNativeOutput(reinviteEntry.Metadata)
		if e != nil {
			t.Fatal(e)
		}
		view, e := store.Read(ctx, id, "r:context", output.Procedure, output.Dialog)
		if e != nil {
			t.Fatal(e)
		}
		current, e := decodeNativeContext(view.Records["r:context"])
		if e != nil {
			t.Fatal(e)
		}
		procedure, e := readPrivateProcedureRecord(view.Records[output.Procedure])
		if e != nil {
			t.Fatal(e)
		}
		dialog, e := readPrivateDialogRecord(view.Records[output.Dialog])
		if e != nil {
			t.Fatal(e)
		}
		owners := &terminalCallOwners{current: current, procedures: map[string]*nativepb.Procedure{output.Procedure: procedure}, dialogs: map[string]*nativepb.Dialog{output.Dialog: dialog}}
		field := "r:dialog-output-" + inviteID + "-7"
		if e = validateRedisTerminalOutput(field, output, owners); e != nil {
			t.Fatal("terminal cleanup rejected an admitted in-dialog INVITE output", e)
		}
		return
	}
	runtime.cfg.WriteCommitted = func(ctx context.Context, write *pb.FlowWrite) *pb.DeliveryResult {
		reserved, e := (nativeRedisWriter{store: store}).Reserve(ctx, write)
		if e != nil || reserved == nil || len(reserved.Wire) == 0 {
			t.Errorf("reserve re-INVITE: %v", e)
			return nil
		}
		return &pb.DeliveryResult{Effect: write.Effect, Outcome: pb.DeliveryResult_WRITTEN}
	}
	if err = runtime.readyOutput(ctx, store, *reinviteEntry); err != nil {
		t.Fatal("re-INVITE physical response", err)
	}
	completeView, err := store.Read(ctx, id, "r:context", "r:dialog", "r:procedure-"+inviteID)
	if err != nil {
		t.Fatal(err)
	}
	completeContext, err := decodeNativeContext(completeView.Records["r:context"])
	if err != nil || !bytes.Equal(completeContext.RemoteSdp, offer) || !bytes.Equal(completeContext.LocalSdp, reinviteResponse.GetRespond().Body) {
		t.Fatal("re-INVITE offer/answer snapshot", err)
	}
	completeDialog, err := readPrivateDialogRecord(completeView.Records["r:dialog"])
	if err != nil || completeDialog.Native.RemoteInvite.State != string(portable.InviteAwaitingACK) || completeDialog.Transaction == anchor.Key {
		t.Fatal("re-INVITE final association", err)
	}
	if ackTimeoutCase {
		redisNow, err = client.Time(ctx).Result()
		if err != nil {
			t.Fatal(err)
		}
		ackTimer := completeDialog.Native.Timers.Values[string(portable.TimerACK)]
		if ackTimer == nil || !ackTimer.Active {
			t.Fatal("missing re-INVITE ACK deadline")
		}
		ackTimer.At = redisNow.Add(-time.Minute).UnixMilli()
		data, e := marshalNativeRecord(storagecodec.NativeDialog, completeDialog)
		if e != nil {
			t.Fatal(e)
		}
		member := callDialogTimerMember("r:dialog", portable.TimerACK)
		_, e = store.Commit(ctx, rediscore.Publication{Identity: id, Revision: completeView.Revision, Deadline: time.Now().Add(time.Second), SpendReservation: true, Changes: []rediscore.Change{{Field: "r:dialog", Value: data}}, Timers: &rediscore.TimerMutation{Expected: completeView.Timer, Target: completeView.Timer, RequireLease: true, Changes: []rediscore.TimerChange{{Member: member, Due: time.UnixMilli(ackTimer.At)}}}})
		if e != nil {
			t.Fatal(e)
		}
		due, e := store.PeekDue(ctx, worker, executor.Epoch)
		if e != nil || due == nil || due.Member != member {
			t.Fatal("ACK timeout not due", due, e)
		}
		timeoutClient := redis.NewClient(&redis.Options{Addr: address, MaxRetries: -1})
		defer timeoutClient.Close()
		timeoutStore, e := rediscore.New(timeoutClient, rediscore.Config{Root: root, Profile: profile, PrimaryRunID: primary, RecordBudget: 4 << 20})
		if e != nil {
			t.Fatal(e)
		}
		if e = runtime.due(ctx, timeoutStore, due); e != nil {
			t.Fatal("re-INVITE ACK timeout", e)
		}
		cleanupID := nativeIdentity(key, "call-ack-timeout-bye", completeContext.Context.State.Authority.PrimaryId, id.Incarnation, id.Context, "r:dialog")[:32]
		cleanupField := "r:call-cleanup-" + cleanupID
		outputField := "r:call-cleanup-output-" + cleanupID
		result, e := store.Read(ctx, id, "r:context", "r:dialog", cleanupField, outputField)
		if e != nil {
			t.Fatal(e)
		}
		call, e := decodeNativeContext(result.Records["r:context"])
		if e != nil || !slices.Contains(call.NoninitialProcedures, cleanupField) {
			t.Fatal("ACK timeout missing cleanup owner", e)
		}
		cleanup, e := readPrivateProcedureRecord(result.Records[cleanupField])
		if e != nil || !cleanup.Cleanup || cleanup.Native.Family != string(portable.NonInviteClient) {
			t.Fatal("ACK timeout cleanup transaction", e)
		}
		dialog, e := readPrivateDialogRecord(result.Records["r:dialog"])
		if e != nil || dialog.Transaction != completeDialog.Transaction || dialog.LocalBye == "" || !slices.Contains(dialog.Outputs, outputField) || len(result.Records[outputField]) == 0 {
			t.Fatal("ACK timeout lost dialog association", e)
		}
		cleanupOutput, e := decodeNativeOutput(result.Records[outputField])
		if e != nil {
			t.Fatal(e)
		}
		writer := nativeRedisWriter{store: timeoutStore}
		physical, e := writer.Reserve(ctx, cleanupOutput.Write)
		if e != nil || physical == nil {
			t.Fatal("reserve timeout BYE", e)
		}
		if e = writer.Complete(ctx, cleanupOutput.Write, &pb.DeliveryResult{Effect: cleanupOutput.Write.Effect, Outcome: pb.DeliveryResult_WRITTEN}); e != nil {
			t.Fatal(e)
		}
		if e = settleRedisCleanupByeOutput(ctx, timeoutStore, cleanupOutput.Write, nil, due.Assignment, time.Now().Add(time.Second)); e != nil {
			t.Fatal(e)
		}
		if e = acknowledgeRedisCleanupByeOutput(ctx, timeoutStore, cleanupOutput.Write, time.Now().Add(time.Second)); e != nil {
			t.Fatal(e)
		}
		parsed, e := parseObservedMessage(physical.Wire)
		if e != nil {
			t.Fatal(e)
		}
		request, ok := parsed.(*sip.Request)
		if !ok {
			t.Fatal("timeout cleanup was not BYE")
		}
		response := sip.NewResponseFromRequest(request, 200, "OK", nil)
		responseMessage, e := portable.ParseMessage([]byte(response.String()), cleanupOutput.Write.Flow.Transport, net.JoinHostPort(cleanupOutput.Write.Flow.SourceIp, strconv.Itoa(int(cleanupOutput.Write.Flow.SourcePort))), "")
		if e != nil {
			t.Fatal(e)
		}
		handled, e := completeRedisCleanupByeResponse(ctx, timeoutStore, cleanupOutput.Write.Flow, responseMessage, nil, time.Now().Add(time.Second))
		if e != nil || !handled {
			t.Fatal("timeout cleanup response", handled, e)
		}
		after, e := timeoutStore.Read(ctx, id, "r:context", cleanupField, anchorField)
		if e != nil {
			t.Fatal(e)
		}
		ended, e := decodeNativeContext(after.Records["r:context"])
		if e != nil || !ended.Ended {
			t.Fatal("timeout cleanup did not end call", e)
		}
		child, e := readPrivateProcedureRecord(after.Records[cleanupField])
		if e != nil || len(child.Events) != 1 {
			t.Fatal("cleanup did not retain ENDED event", e)
		}
		root, e := readPrivateProcedureRecord(after.Records[anchorField])
		if e != nil || !slices.Contains(root.Events, child.Events[0]) {
			t.Fatal("ENDED lost original lifecycle root", e)
		}
		retainedEvent, e := timeoutStore.Read(ctx, id, child.Events[0])
		if e != nil {
			t.Fatal(e)
		}
		event, e := decodeNativeLifecycle(retainedEvent.Records[child.Events[0]])
		if e != nil || event.Root != anchorField || !slices.Contains(event.Owners, cleanupField) {
			t.Fatal("ENDED lost actual cleanup owner", e)
		}
		redisNow, e = client.Time(ctx).Result()
		if e != nil {
			t.Fatal(e)
		}
		clockWait := time.Now().Add(time.Second)
		for redisNow.UnixMilli() < event.NextMs {
			if time.Now().After(clockWait) {
				t.Fatal("lifecycle deadline did not become due")
			}
			time.Sleep(time.Millisecond)
			redisNow, e = client.Time(ctx).Result()
			if e != nil {
				t.Fatal(e)
			}
		}
		observed := time.Now()
		eventDue := &rediscore.DueContext{Identity: id, Revision: retainedEvent.Revision, Assignment: retainedEvent.Timer, Member: strings.TrimPrefix(child.Events[0], "r:"), DeadlineMS: event.NextMs, CheckedAtMS: redisNow.UnixMilli(), Observed: observed}
		selectedEvent, delivered, e := selectRedisNativeLifecycle(ctx, timeoutStore, eventDue)
		if e != nil || delivered == nil || delivered.Kind != pb.DialogEvent_ENDED {
			t.Fatal("select cleanup ENDED", e)
		}
		stagedEvent, e := prepareRedisNativeLifecycleAttempt(ctx, timeoutStore, selectedEvent, observed.Add(time.Second), time.Now().Add(time.Second))
		if e != nil {
			t.Fatal("stage cleanup ENDED", e)
		}
		if e = completeRedisNativeLifecycle(ctx, timeoutStore, stagedEvent, true, time.Now().Add(time.Second)); e != nil {
			t.Fatal("complete cleanup ENDED", e)
		}
		closed, e := timeoutStore.Read(ctx, id, cleanupField, outputField)
		if e != nil {
			t.Fatal(e)
		}
		child, e = readPrivateProcedureRecord(closed.Records[cleanupField])
		if e != nil || len(child.Events) != 0 {
			t.Fatal("cleanup event owner not released", e)
		}
		physicalDue := time.UnixMilli(int64(cleanupOutput.Write.Permit.Authorization.DeadlineMs))
		for time.Now().Before(physicalDue) {
			if _, e = timeoutStore.RenewWorker(ctx, rediscore.WorkerLeaseRequest{Worker: worker, Epoch: executor.Epoch, PrimaryRunID: primary}); e != nil {
				t.Fatal(e)
			}
			time.Sleep(min(200*time.Millisecond, time.Until(physicalDue)))
		}
		redisNow, e = client.Time(ctx).Result()
		if e != nil {
			t.Fatal(e)
		}
		clockWait = time.Now().Add(time.Second)
		for redisNow.UnixMilli() < physicalDue.UnixMilli() {
			if time.Now().After(clockWait) {
				t.Fatal("cleanup output deadline did not become due")
			}
			time.Sleep(time.Millisecond)
			redisNow, e = client.Time(ctx).Result()
			if e != nil {
				t.Fatal(e)
			}
		}
		outDue := &rediscore.DueContext{Identity: id, Revision: closed.Revision, Assignment: closed.Timer, Member: "out-" + strings.TrimPrefix(outputField, "r:"), DeadlineMS: physicalDue.UnixMilli(), CheckedAtMS: redisNow.UnixMilli(), Observed: time.Now()}
		if e = dispatchRedisCleanupByeCutoff(ctx, timeoutStore, outDue, time.Now().Add(time.Second)); e != nil {
			t.Fatal("retire timeout cleanup", e)
		}
		retired, e := timeoutStore.Read(ctx, id, "r:context", cleanupField, "r:procedure-"+inviteID)
		if e != nil {
			t.Fatal(e)
		}
		owner, e := readPrivateProcedureRecord(retired.Records["r:procedure-"+inviteID])
		if e != nil || owner.CreditContribution == 0 || len(retired.Records[cleanupField]) != 0 {
			t.Fatal("cleanup closed wrong owner or retained child", e)
		}
		return
	}
	remoteClient := redis.NewClient(&redis.Options{Addr: address, MaxRetries: -1})
	defer remoteClient.Close()
	remoteStore, err := rediscore.New(remoteClient, rediscore.Config{Root: root, Profile: profile, PrimaryRunID: primary, RecordBudget: 4 << 20})
	if err != nil {
		t.Fatal(err)
	}
	ackBody := []byte(nil)
	if len(ackEvent) > 0 {
		ackBody = peerACKAnswer
		if ackEvent[0] == "fork-early" {
			ackBody = []byte("opaque early-offer ACK body")
		}
		if strings.HasPrefix(ackEvent[0], "fork-") {
			view, e := remoteStore.Read(ctx, id, "r:context")
			if e != nil {
				t.Fatal(e)
			}
			selectedLeg, e := decodeNativeContext(view.Records["r:context"])
			if e != nil {
				t.Fatal(e)
			}
			// Another fork is selected while this admitted dialog is awaiting ACK.
			selectedLeg.RemoteTag = "selected-other-fork"
			selectedLeg.LocalSdp, selectedLeg.RemoteSdp = []byte("selected local SDP"), []byte("selected remote SDP")
			data, e := encodeNativeContext(selectedLeg)
			if e != nil {
				t.Fatal(e)
			}
			_, e = remoteStore.Commit(ctx, rediscore.Publication{Identity: id, Revision: view.Revision, Deadline: time.Now().Add(time.Second), SpendReservation: true, Changes: []rediscore.Change{{Field: "r:context", Value: data}}})
			if e != nil {
				t.Fatal(e)
			}
		}
	}
	ack, err := portable.BuildDialogRequest(peer, sip.ACK, 14, ackBody, "z9hG4bK-reinvite-ack")
	if err != nil {
		t.Fatal(err)
	}
	ack = portable.RestoreMessage(ack.WireBytes(), "tls", "192.0.2.3:6789", "")
	releaseOwner := func() {
		t.Helper()
		view, e := remoteStore.Read(ctx, id, "r:context", "r:subscription")
		if e != nil {
			t.Fatal(e)
		}
		current, e := decodeNativeContext(view.Records["r:context"])
		if e != nil {
			t.Fatal(e)
		}
		item := rediscore.ExpiredApplicationContext{Identity: id, Revision: view.Revision}
		capture := &redisExpiredOwnerSelection{identity: id, view: view, context: current}
		if e = releaseExpiredRedisCall(ctx, remoteStore, item, key, rediscore.TimerAssignment{Worker: worker, Epoch: executor.Epoch, Term: executor.Epoch}, time.Now().Add(time.Second), capture); e != nil {
			t.Fatal("expire call owner", e)
		}
	}
	if ownerCase == 4 {
		bye, e := portable.BuildDialogRequest(peer, sip.BYE, 15, nil, "z9hG4bK-bye-before-ack")
		if e != nil {
			t.Fatal(e)
		}
		bye = portable.RestoreMessage(bye.WireBytes(), "tls", "192.0.2.3:6789", "")
		timer := rediscore.TimerAssignment{Worker: worker, Epoch: executor.Epoch, Term: executor.Epoch}
		if handled, e := matchRedisCallCompletion(ctx, remoteStore, executor, flow, bye, key, timer, time.Now().Add(time.Second)); e != nil || !handled {
			t.Fatal("peer BYE before ACK", handled, e)
		}
		if e = remoteStore.ExpireApplication(ctx, lease.ApplicationLeaseRef); e != nil {
			t.Fatal(e)
		}
		releaseOwner()
		ownerless, e := remoteStore.ObserveContext(ctx, id.Context, id.Incarnation, "r:dialog")
		if e != nil || ownerless.Identity.Owner != "" {
			t.Fatal("ended call retained controller ownership", e)
		}
		dialog, e := decodeNativeDialog(ownerless.View.Records["r:dialog"])
		if e != nil || dialog.Native.Phase != portable.DialogEnded || dialog.Native.RemoteInvite == nil || dialog.Native.RemoteInvite.State != portable.InviteAwaitingACK || dialog.Native.Timers[portable.TimerACK].Active {
			t.Fatal("owner release altered the ended dialog or revived its ACK timer", e)
		}
		return
	}
	if ownerCase != 0 && ownerCase != 3 && ownerCase != 5 {
		if err = remoteStore.ExpireApplication(ctx, lease.ApplicationLeaseRef); err != nil {
			t.Fatal("expire controller term", err)
		}
		if ownerCase == 2 {
			releaseOwner()
		}
	}
	selectedACK, err := selectRedisCallDialog(ctx, remoteStore, executor, flow, ack, time.Now().Add(time.Second))
	if err != nil {
		t.Fatal("select re-INVITE ACK", err)
	}
	if selectedACK.ProcedureField != "r:procedure-"+inviteID {
		t.Fatal("ACK associated with wrong INVITE")
	}
	// This receiving worker has no local control session. Shared debt must still
	// be created by the production matching path for delivery on its executor.
	if handled, e := matchRedisCallCompletion(ctx, remoteStore, executor, flow, ack, key, rediscore.TimerAssignment{Worker: worker, Epoch: executor.Epoch, Term: executor.Epoch}, time.Now().Add(time.Second)); e != nil || !handled {
		t.Fatal("commit re-INVITE ACK without local controller", handled, e)
	}
	acked, err := remoteStore.Read(ctx, selectedACK.Identity, "r:dialog")
	if err != nil {
		t.Fatal(err)
	}
	ackedDialog, err := readPrivateDialogRecord(acked.Records["r:dialog"])
	if err != nil || ackedDialog.Native.RemoteInvite.State != string(portable.InviteAcked) {
		t.Fatal("re-INVITE ACK not applied", err)
	}
	if ownerCase == 5 {
		refs := fillRedisProcedureSlots(t, ctx, remoteStore, id)
		if e := remoteStore.ExpireApplication(ctx, lease.ApplicationLeaseRef); e != nil {
			t.Fatal(e)
		}
		releaseOwner()
		ownerless, e := remoteStore.ObserveContext(ctx, id.Context, id.Incarnation, "r:context", "r:dialog")
		if e != nil || ownerless.Identity.Owner != "" {
			t.Fatal("full call retained controller ownership", e)
		}
		current, e := decodeNativeContext(ownerless.View.Records["r:context"])
		if e != nil || !current.Ended || !slices.Equal(current.NoninitialProcedures, refs) {
			t.Fatal("owner expiry changed occupied procedures or left the call active", e)
		}
		dialog, e := decodeNativeDialog(ownerless.View.Records["r:dialog"])
		if e != nil || dialog.Native.Phase != portable.DialogEnded || dialog.Refs.LocalBye != "" {
			t.Fatal("full call was not ended locally", e)
		}
		return
	}
	if len(ackEvent) > 0 {
		assertRedisPeerACKRetention(t, ctx, remoteStore, client, root, selectedACK.Identity, "r:procedure-"+inviteID, "r:dialog", ack, ackEvent[0])
		return
	}
	if ownerCase == 3 {
		view, e := remoteStore.Read(ctx, id, "r:context", "r:subscription")
		if e != nil {
			t.Fatal(e)
		}
		current, e := decodeNativeContext(view.Records["r:context"])
		if e != nil {
			t.Fatal(e)
		}
		item := rediscore.ExpiredApplicationContext{Identity: id, Revision: view.Revision}
		capture := &redisExpiredOwnerSelection{identity: id, view: view, context: current}
		if e = releaseExpiredRedisCall(ctx, remoteStore, item, key, rediscore.TimerAssignment{Worker: worker, Epoch: executor.Epoch, Term: executor.Epoch}, time.Now().Add(time.Second), capture); e == nil {
			t.Fatal("live owner lost its call")
		}
		after, e := remoteStore.Read(ctx, id, "r:context", "r:dialog")
		if e != nil || after.Revision != view.Revision || !bytes.Equal(after.Records["r:context"], view.Records["r:context"]) {
			t.Fatal("live owner refusal partially published", e)
		}
		return
	}
	if ownerCase != 0 {
		if ownerCase == 1 {
			releaseOwner()
		}
		ownerless, e := remoteStore.ObserveContext(ctx, "call", "inc", "r:context", "r:dialog")
		if e != nil || ownerless.Identity.Owner != "" || ownerless.Identity.Term != "" {
			t.Fatal("expired owner remains", e)
		}
		call, e := decodeNativeContext(ownerless.View.Records["r:context"])
		if e != nil || !call.Ended {
			t.Fatal("disconnected call not ended", e)
		}
		dialog, e := readPrivateDialogRecord(ownerless.View.Records["r:dialog"])
		if e != nil || dialog.LocalBye == "" {
			t.Fatal("owner loss did not stage BYE", e)
		}
		cleanupID := nativeIdentity(key, "call-ack-timeout-bye", call.Context.State.Authority.PrimaryId, id.Incarnation, id.Context, "r:dialog")[:32]
		childField, outputField := "r:call-cleanup-"+cleanupID, "r:call-cleanup-output-"+cleanupID
		childView, e := remoteStore.Read(ctx, ownerless.Identity, childField, outputField)
		if e != nil || !slices.Contains(call.NoninitialProcedures, childField) || !slices.Contains(dialog.Outputs, outputField) || len(childView.Records[outputField]) == 0 {
			t.Fatal("owner loss cleanup not retained", e)
		}
		output, e := decodeNativeOutput(childView.Records[outputField])
		if e != nil || !proto.Equal(output.Write.Flow, flow) || output.Write.Permit.Authorization.AuthorityClass != pb.EffectAuthorization_TERMINAL_CLEANUP {
			t.Fatal("owner loss used wrong physical flow or authority", e)
		}
		writer := nativeRedisWriter{store: remoteStore}
		reserved, e := writer.Reserve(ctx, output.Write)
		if e != nil || reserved == nil || !bytes.HasPrefix(reserved.Wire, []byte("BYE ")) {
			t.Fatal("reserve owner-loss BYE", e)
		}
		if e = writer.Complete(ctx, output.Write, &pb.DeliveryResult{Effect: output.Write.Effect, Outcome: pb.DeliveryResult_WRITTEN}); e != nil {
			t.Fatal(e)
		}
		if e = settleRedisCleanupByeOutput(ctx, remoteStore, output.Write, nil, rediscore.TimerAssignment{Worker: worker, Epoch: executor.Epoch, Term: executor.Epoch}, time.Now().Add(time.Second)); e != nil {
			t.Fatal(e)
		}
		if e = acknowledgeRedisCleanupByeOutput(ctx, remoteStore, output.Write, time.Now().Add(time.Second)); e != nil {
			t.Fatal(e)
		}
		return
	}

	if !cancelCase {
		// An application timeout rejects only this new offer and keeps the call's
		// previously accepted association and controller-facing state intact.
		timeoutInvite, err := portable.BuildDialogRequest(peer, sip.INVITE, 15, []byte("v=0\r\ns=second-offer\r\n"), "z9hG4bK-timeout-reinvite")
		if err != nil {
			t.Fatal(err)
		}
		timeoutInvite = portable.RestoreMessage(timeoutInvite.WireBytes(), "tls", "192.0.2.3:6789", "")
		timeoutID := uuid.NewString()
		_, err = receiveRedisDialogRequest(ctx, store, redisDialogReceive{IdentityKey: key, Context: ref, Application: app, Executor: executor, Lease: lease.ApplicationLeaseRef, Flow: flow, Message: timeoutInvite, PrincipalID: "principal", BindingID: "binding", RequestID: timeoutID, Timer: rediscore.TimerAssignment{Worker: worker, Epoch: executor.Epoch, Term: executor.Epoch}, Deadline: time.Now().Add(time.Second)})
		if err != nil {
			t.Fatal("receive timeout re-INVITE", err)
		}
		timeoutSelection, err := store.ObserveContext(ctx, "call", "inc", "r:context", "r:procedure-"+timeoutID)
		if err != nil {
			t.Fatal(err)
		}
		beforeTimeout, err := decodeNativeContext(timeoutSelection.View.Records["r:context"])
		if err != nil {
			t.Fatal(err)
		}
		timeoutProcedure, err := readPrivateProcedureRecord(timeoutSelection.View.Records["r:procedure-"+timeoutID])
		if err != nil {
			t.Fatal(err)
		}
		redisNow, err = client.Time(ctx).Result()
		if err != nil {
			t.Fatal(err)
		}
		timeoutProcedure.PresentDeadlineMs = redisNow.Add(-time.Minute).UnixMilli()
		timeoutBytes, err := marshalNativeRecord(storagecodec.NativeProcedure, timeoutProcedure)
		if err != nil {
			t.Fatal(err)
		}
		timeoutMember := "call-decision-" + timeoutID
		_, err = store.Commit(ctx, rediscore.Publication{Identity: timeoutSelection.Identity, Revision: timeoutSelection.View.Revision, Deadline: time.Now().Add(time.Second), SpendReservation: true, Changes: []rediscore.Change{{Field: "r:procedure-" + timeoutID, Value: timeoutBytes}}, Timers: &rediscore.TimerMutation{Expected: timeoutSelection.View.Timer, Target: timeoutSelection.View.Timer, RequireLease: true, Changes: []rediscore.TimerChange{{Member: timeoutMember, Due: time.UnixMilli(timeoutProcedure.PresentDeadlineMs)}}}})
		if err != nil {
			t.Fatal(err)
		}
		reinviteDue, err := store.PeekDue(ctx, worker, executor.Epoch)
		if err != nil || reinviteDue == nil || reinviteDue.Member != timeoutMember {
			t.Fatal("re-INVITE decision not due", reinviteDue, err)
		}
		if err = timeoutRedisDialogServer(ctx, store, key, advertise, reinviteDue, time.Now().Add(time.Second)); err != nil {
			t.Fatal("timeout re-INVITE", err)
		}
		timeoutResult, err := store.Read(ctx, id, "r:context", "r:dialog", "r:procedure-"+timeoutID)
		if err != nil {
			t.Fatal(err)
		}
		afterTimeout, err := decodeNativeContext(timeoutResult.Records["r:context"])
		if err != nil || afterTimeout.Ended || !bytes.Equal(afterTimeout.LocalSdp, beforeTimeout.LocalSdp) || !bytes.Equal(afterTimeout.RemoteSdp, beforeTimeout.RemoteSdp) || !proto.Equal(afterTimeout.Context, beforeTimeout.Context) {
			t.Fatal("timeout changed established call state", err)
		}
		rejectedDialog, err := readPrivateDialogRecord(timeoutResult.Records["r:dialog"])
		if err != nil || rejectedDialog.Transaction != ackedDialog.Transaction || rejectedDialog.Native.RemoteCSeq != 15 || rejectedDialog.Native.RemoteInvite == nil || rejectedDialog.Native.RemoteInvite.State != string(portable.InviteRejected) {
			t.Fatal("timeout did not reject exact offer while retaining association", err)
		}

		return
	}
	cancelInvite, err := portable.BuildDialogRequest(peer, sip.INVITE, 15, nil, "z9hG4bK-cancel-reinvite")
	if err != nil {
		t.Fatal(err)
	}
	cancelInvite = portable.RestoreMessage(cancelInvite.WireBytes(), "tls", "192.0.2.3:6789", "")
	cancelID := uuid.NewString()
	cancelPresentation, err := receiveRedisDialogRequest(ctx, store, redisDialogReceive{IdentityKey: key, Context: ref, Application: app, Executor: executor, Lease: lease.ApplicationLeaseRef, Flow: flow, Message: cancelInvite, PrincipalID: "principal", BindingID: "binding", RequestID: cancelID, Timer: rediscore.TimerAssignment{Worker: worker, Epoch: executor.Epoch, Term: executor.Epoch}, Deadline: time.Now().Add(time.Second)})
	if err != nil {
		t.Fatal("receive cancellable re-INVITE", err)
	}
	cancelDecision := residentDirectDecision(cancelPresentation, 8, 0)
	cancelDecision.PrincipalId, cancelDecision.BindingId, cancelDecision.Basis = "principal", "binding", pb.OwnerIngest_EXISTING_FLOW
	cancelSelection, err := store.ObserveApplicationWitness(ctx, "call", "inc", lease.ApplicationLeaseRef, "r:context", "r:procedure-"+cancelID, "r:origin-"+cancelID, "r:result-7", "r:result-8")
	if err != nil {
		t.Fatal(err)
	}
	cancelContext, err := decodeNativeContext(cancelSelection.View.Records["r:context"])
	if err != nil {
		t.Fatal(err)
	}
	cancelCandidate, err := prepareRedisDialogDecision(ctx, store, cancelSelection, cancelContext, key, app, lease.ApplicationLeaseRef, cancelDecision, "reinvite-cancel", time.Now().Add(time.Second))
	if err != nil {
		t.Fatal("prepare cancellable re-INVITE", err)
	}
	_, err = commitRedisDialogDecision(ctx, store, cancelCandidate, key, lease.ApplicationLeaseRef, cancelDecision, "reinvite-cancel", advertise, rediscore.TimerAssignment{Worker: worker, Epoch: executor.Epoch, Term: executor.Epoch}, time.Now().Add(time.Second))
	if err != nil {
		t.Fatal("accept cancellable re-INVITE", err)
	}
	cancel, err := portable.BuildCancel(cancelInvite)
	if err != nil {
		t.Fatal(err)
	}
	cancel = portable.RestoreMessage(cancel.WireBytes(), "tls", "192.0.2.3:6789", "")
	selectedCancel, handled, err := selectRedisServerCancel(ctx, store, executor, flow, cancel, key, time.Now().Add(time.Second))
	if err != nil || !handled {
		t.Fatal("select re-INVITE CANCEL", handled, err)
	}
	handled, err = matchRedisCallCompletion(ctx, store, executor, flow, cancel, key, rediscore.TimerAssignment{Worker: worker, Epoch: executor.Epoch, Term: executor.Epoch}, time.Now().Add(time.Second))
	if err != nil || !handled {
		t.Fatal("publish re-INVITE CANCEL", handled, err)
	}
	cancelResult, err := store.Read(ctx, id, "r:context", "r:dialog", "r:procedure-"+cancelID, "r:dialog-output-"+cancelID+"-0", selectedCancel.cancelField)
	if err != nil {
		t.Fatal(err)
	}
	cancelledContext, err := decodeNativeContext(cancelResult.Records["r:context"])
	if err != nil || cancelledContext.Ended || !cancelledContext.Answered || !proto.Equal(cancelledContext.Delivery, completeContext.Delivery) || !bytes.Equal(cancelledContext.RemoteSdp, completeContext.RemoteSdp) || !bytes.Equal(cancelledContext.LocalSdp, completeContext.LocalSdp) {
		t.Fatal("CANCEL changed established call", err)
	}
	cancelledDialog, err := readPrivateDialogRecord(cancelResult.Records["r:dialog"])
	if err != nil || cancelledDialog.Native.RemoteInvite.State != string(portable.InviteRejected) || cancelledDialog.Transaction != ackedDialog.Transaction || !slices.Contains(cancelledDialog.Outputs, "r:dialog-output-"+cancelID+"-0") {
		t.Fatal("CANCEL did not reject exact dialog offer", err)
	}
	cancelledInvite, err := readPrivateProcedureRecord(cancelResult.Records["r:procedure-"+cancelID])
	if err != nil || len(cancelledInvite.Events) != 1 || !slices.Contains(cancelledInvite.Outputs, "r:dialog-output-"+cancelID+"-0") || len(cancelResult.Records[selectedCancel.cancelField]) == 0 {
		t.Fatal("CANCEL lost native debt ownership", err)
	}
	cancelEvent, err := store.Read(ctx, id, cancelledInvite.Events[0])
	if err != nil {
		t.Fatal(err)
	}
	event, err := decodeNativeLifecycle(cancelEvent.Records[cancelledInvite.Events[0]])
	if err != nil || event.Kind != pb.DialogEvent_CANCELLED || event.Root != "r:procedure-"+cancelID || !event.Snapshot.Answered || event.Snapshot.Ended {
		t.Fatal("CANCELLED event did not retain its established owner", err)
	}
	redisNow, err = client.Time(ctx).Result()
	if err != nil {
		t.Fatal(err)
	}
	observedAt := time.Now()
	eventDue := &rediscore.DueContext{Identity: id, Revision: cancelEvent.Revision, Assignment: cancelEvent.Timer, Member: strings.TrimPrefix(cancelledInvite.Events[0], "r:"), DeadlineMS: event.NextMs, CheckedAtMS: redisNow.UnixMilli(), Observed: observedAt}
	selectedEvent, delivered, err := selectRedisNativeLifecycle(ctx, store, eventDue)
	if err != nil || delivered == nil || delivered.Kind != pb.DialogEvent_CANCELLED {
		t.Fatal("select rooted CANCELLED", err)
	}
	stagedEvent, err := prepareRedisNativeLifecycleAttempt(ctx, store, selectedEvent, observedAt.Add(time.Second), time.Now().Add(time.Second))
	if err != nil {
		t.Fatal("stage rooted CANCELLED", err)
	}
	if err = completeRedisNativeLifecycle(ctx, store, stagedEvent, true, time.Now().Add(time.Second)); err != nil {
		t.Fatal("complete rooted CANCELLED", err)
	}
	closedEvent, err := store.Read(ctx, id, "r:procedure-"+cancelID, cancelledInvite.Events[0])
	if err != nil {
		t.Fatal(err)
	}
	ownerAfterEvent, err := readPrivateProcedureRecord(closedEvent.Records["r:procedure-"+cancelID])
	if err != nil || len(ownerAfterEvent.Events) != 0 || len(closedEvent.Records[cancelledInvite.Events[0]]) != 0 {
		t.Fatal("CANCELLED event owner not released", err)
	}
}

func assertDialogResponseContact(t *testing.T, wire []byte, expected string) {
	t.Helper()
	message, err := portable.ParseMessage(wire, "tls", "192.0.2.3:6789", "")
	if err != nil {
		t.Fatal(err)
	}
	values, err := message.HeaderValues("Contact")
	if err != nil || len(values) != 1 || !strings.Contains(values[0], expected) {
		t.Fatalf("response Contact = %v, want %q: %v", values, expected, err)
	}
}

type rejectUnusedDialogRead struct{}

func (rejectUnusedDialogRead) DialHook(next redis.DialHook) redis.DialHook { return next }
func (rejectUnusedDialogRead) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}
func (rejectUnusedDialogRead) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		for _, argument := range cmd.Args() {
			if field, ok := argument.(string); ok && (field == "r:final" || field == "r:ack") {
				return errors.New("unselected SIP field was read")
			}
		}
		return next(ctx, cmd)
	}
}
