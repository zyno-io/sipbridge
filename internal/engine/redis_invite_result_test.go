package engine

import (
	"bytes"
	"context"
	"errors"
	"os"
	"slices"
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

func TestRedisInviteResponseCutoffReleasesOnlySupersededReceipt(t *testing.T) {
	testRedisInviteResponseCutoff(t, "")
}

func TestRedisInviteResponseReplacementAfterProvisionalCutoff(t *testing.T) {
	for _, replacement := range []string{"final", "provisional"} {
		t.Run(replacement, func(t *testing.T) { testRedisInviteResponseCutoff(t, "displaced-"+replacement) })
	}
}

func TestRedisSupersededProvisionalUnknownLifetime(t *testing.T) {
	for _, mode := range []string{"forget", "released-proof", "late-written", "trying-forget", "trying-released-proof", "trying-late-written", "trying-no-final"} {
		t.Run(mode, func(t *testing.T) { testRedisInviteResponseCutoff(t, mode) })
	}
}

func testRedisInviteResponseCutoff(t *testing.T, obsolete string) {
	trying := strings.HasPrefix(obsolete, "trying-")
	obsolete = strings.TrimPrefix(obsolete, "trying-")
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
	root := "sipstate:test:invite-result-" + uuid.NewString() + ":"
	profile, key := []byte{8, 1}, bytes.Repeat([]byte{1}, 32)
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
	keys := []string{"core:profile", "core:capacity", "core:selectors", "core:applications", "core:owner:term", "core:owned:term", "core:owner-expiry", "core:coordinator", "core:worker:" + worker, "core:timers:" + worker, "core:ready:" + worker, "core:context:context", "core:deadlines:context", "core:context:udp-context", "core:deadlines:udp-context", "core:context:udp-cleanup", "core:deadlines:udp-cleanup", "core:context:udp-reply", "core:deadlines:udp-reply", "core:context:udp-forget", "core:deadlines:udp-forget", "core:application:" + app}
	t.Cleanup(func() {
		for _, name := range keys {
			if e := client.Del(ctx, root+name).Err(); e != nil {
				t.Error(e)
			}
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
	if _, err = store.RenewWorker(ctx, rediscore.WorkerLeaseRequest{Worker: worker, Epoch: process.Epoch, PrimaryRunID: primary, Acquire: true}); err != nil {
		t.Fatal(err)
	}
	flow := &pb.FlowRef{Ingress: process, ConnectionId: "phone", SourceIp: "192.0.2.12", SourcePort: 5061, Listener: "sip.example.test:5061", Transport: "tls"}
	message, err := portable.ParseMessage(stateTestRequest("INVITE", 1, "", "result").Wire, "tls", "192.0.2.12:5061", "")
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
	executor := rediscore.TimerAssignment{Worker: worker, Epoch: process.Epoch, Term: process.Epoch}
	input := redisRegisterReceive{IdentityKey: key, Context: ref, Authority: authority, Application: application, Executor: process, Lease: lease.ApplicationLeaseRef, Flow: flow, Message: message, RequestID: requestID, LocalTag: "local", Timer: executor, Deadline: time.Now().Add(1500 * time.Millisecond)}
	presented, err := receiveRedisInvite(ctx, store, input)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = commitRedisInviteDeferral(ctx, store, key, application, lease.ApplicationLeaseRef, residentDirectDecision(presented, 1, 0), "defer", input.Deadline); err != nil {
		t.Fatal(err)
	}
	var tryingOutput *nativepb.Output
	if trying {
		selected, e := store.ObserveContext(ctx, "context", "incarnation", "r:procedure-"+requestID)
		if e != nil {
			t.Fatal(e)
		}
		procedure, e := decodeNativeProcedure(selected.View.Records["r:procedure-"+requestID])
		if e != nil {
			t.Fatal(e)
		}
		waitRedisFixtureTime(t, ctx, client, time.UnixMilli(procedure.Native.Timers[portable.TimerTrying].At))
		due, e := store.PeekDue(ctx, worker, process.Epoch)
		if e != nil || due == nil || due.Member != "call-tx-"+requestID+"-trying" {
			t.Fatal("automatic Trying not due", due, e)
		}
		if e = fireRedisInviteTrying(ctx, store, key, due, time.Now().Add(500*time.Millisecond)); e != nil {
			t.Fatal(e)
		}
		view, e := store.Read(ctx, selected.Identity, "r:output-"+requestID+"-100")
		if e != nil {
			t.Fatal(e)
		}
		tryingOutput, e = decodeNativeOutput(view.Records["r:output-"+requestID+"-100"])
		if e != nil {
			t.Fatal(e)
		}
		writer := nativeRedisWriter{store: store}
		if _, e = writer.Reserve(ctx, tryingOutput.Write); e != nil {
			t.Fatal(e)
		}
		if e = writer.Complete(ctx, tryingOutput.Write, &pb.DeliveryResult{Effect: tryingOutput.Write.Effect, Outcome: pb.DeliveryResult_UNKNOWN}); e != nil {
			t.Fatal(e)
		}
	}
	cutoff := time.Now().Add(300 * time.Millisecond)
	command := &pb.Command{Context: ref, Sequence: 2, Admission: &pb.CommandAdmission{DirectoryEpoch: "directory", ExpiresAtMs: uint64(cutoff.UnixMilli())}, Action: &pb.Command_Respond{Respond: &pb.Respond{RequestId: requestID, Status: 180}}}
	capture, err := captureRedisNotifyWait(command)
	if err != nil {
		t.Fatal(err)
	}
	identity, err := publishRedisInviteRespond(ctx, store, key, application, lease.ApplicationLeaseRef, command, capture, "ring", "sip:edge@example.test", "edge.example.test:5061", executor, cutoff)
	if err != nil {
		t.Fatal(err)
	}
	view, err := store.Read(ctx, identity, "r:invite-output-"+requestID+"-2")
	if err != nil {
		t.Fatal(err)
	}
	output, err := decodeNativeOutput(view.Records["r:invite-output-"+requestID+"-2"])
	if err != nil {
		t.Fatal(err)
	}
	writer := nativeRedisWriter{store: store}
	if _, err = writer.Reserve(ctx, output.Write); err != nil {
		t.Fatal(err)
	}
	if err = writer.Complete(ctx, output.Write, &pb.DeliveryResult{Effect: output.Write.Effect, Outcome: pb.DeliveryResult_WRITTEN}); err != nil {
		t.Fatal(err)
	}
	if err = settleRedisInitialInviteOutput(ctx, store, output.Write, nil, executor, time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if err = acknowledgeRedisInitialInviteOutput(ctx, store, output.Write); err != nil {
		t.Fatal(err)
	}
	if strings.HasPrefix(obsolete, "displaced-") {
		waitRedisFixtureTime(t, ctx, client, cutoff)
		due, e := store.PeekDue(ctx, worker, process.Epoch)
		if e != nil || due == nil || due.Member != "out-invite-output-"+requestID+"-2" {
			t.Fatal("original provisional output cutoff", due, e)
		}
		if e = dispatchRedisInitialInviteOutputCutoff(ctx, store, due, time.Now().Add(time.Second)); e != nil {
			t.Fatal(e)
		}
		oldResponse := "r:invite-response-" + requestID + "-2"
		closed, e := store.Read(ctx, identity, "r:procedure-"+requestID, oldResponse, "r:invite-output-"+requestID+"-2")
		if e != nil {
			t.Fatal(e)
		}
		owner, e := readPrivateProcedureRecord(closed.Records["r:procedure-"+requestID])
		if e != nil || owner.LastResponse != oldResponse || len(owner.Outputs) != 0 || len(closed.Records[oldResponse]) == 0 || len(closed.Records["r:invite-output-"+requestID+"-2"]) != 0 {
			t.Fatal("provisional cutoff must retain only its actual transaction response owner", e)
		}
		next := proto.Clone(command).(*pb.Command)
		next.Sequence, next.GetRespond().Status = 3, 200
		if obsolete == "displaced-provisional" {
			next.GetRespond().Status = 183
		}
		until := time.Now().Add(time.Second)
		next.Admission.ExpiresAtMs = uint64(until.UnixMilli())
		capture, e := captureRedisNotifyWait(next)
		if e != nil {
			t.Fatal(e)
		}
		if _, e = publishRedisInviteRespond(ctx, store, key, application, lease.ApplicationLeaseRef, next, capture, "replace-after-cutoff", "sip:edge@example.test", "edge.example.test:5061", executor, until); e != nil {
			t.Fatal(e)
		}
		if present, e := client.HExists(ctx, root+"core:context:context", oldResponse).Result(); e != nil || present {
			t.Fatal("displaced provisional wire survived its last actual owner", e)
		}
		return
	}
	next := proto.Clone(command).(*pb.Command)
	next.Sequence, next.GetRespond().Status = 3, 183
	nextCapture, err := captureRedisNotifyWait(next)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = publishRedisInviteRespond(ctx, store, key, application, lease.ApplicationLeaseRef, next, nextCapture, "progress", "sip:edge@example.test", "edge.example.test:5061", executor, cutoff); err != nil {
		t.Fatal(err)
	}
	view, err = store.Read(ctx, identity, "r:invite-output-"+requestID+"-3")
	if err != nil {
		t.Fatal(err)
	}
	predecessor, err := store.ObserveContext(ctx, identity.Context, identity.Incarnation, "r:procedure-"+requestID, "r:result-3")
	if err != nil {
		t.Fatal(err)
	}
	owner, err := readPrivateProcedureRecord(predecessor.View.Records["r:procedure-"+requestID])
	if err != nil {
		t.Fatal(err)
	}
	prior, err := decodeNativeResult(predecessor.View.Records["r:result-3"])
	if err != nil {
		t.Fatal(err)
	}
	if timers, e := planPreviousInviteResult(ctx, store, predecessor, owner, prior, uuid.NewString()); e != nil || len(timers) != 0 {
		t.Fatal("exact predecessor output backlink was not followed", e)
	}
	unsupported := proto.Clone(prior).(*nativepb.Result)
	unsupported.Output, unsupported.Pending = "", false
	owner.CommandResult = "r:result-2"
	if _, e := planPreviousInviteResult(ctx, store, predecessor, owner, unsupported, uuid.NewString()); e == nil {
		t.Fatal("unproved empty predecessor backlink was accepted")
	}
	uncertainOutput, err := decodeNativeOutput(view.Records["r:invite-output-"+requestID+"-3"])
	if err != nil {
		t.Fatal(err)
	}
	uncertainRef, err := nativeOutputRef(uncertainOutput.Write)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = writer.Reserve(ctx, uncertainOutput.Write); err != nil {
		t.Fatal(err)
	}
	if err = writer.Complete(ctx, uncertainOutput.Write, &pb.DeliveryResult{Effect: uncertainOutput.Write.Effect, Outcome: pb.DeliveryResult_UNKNOWN}); err != nil {
		t.Fatal(err)
	}
	if err = settleRedisInitialInviteOutput(ctx, store, uncertainOutput.Write, nil, executor, time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if wait := time.Until(cutoff); wait > 0 {
		time.Sleep(wait + 20*time.Millisecond)
	}
	due, err := store.PeekDue(ctx, worker, process.Epoch)
	if err == nil && due == nil {
		// Host wall time can lead Redis TIME by more than the old 20ms
		// margin. Wait for actual indexed readiness in this fixture only.
		limit := time.Now().Add(time.Second)
		for due == nil && err == nil && time.Now().Before(limit) {
			time.Sleep(5 * time.Millisecond)
			due, err = store.PeekDue(ctx, worker, process.Epoch)
		}
	}
	if err != nil || due == nil || due.Member != "out-invite-output-"+requestID+"-2" {
		t.Fatal("older exact output cutoff missing", due, err)
	}
	stale := *due
	stale.Revision++
	if err = dispatchRedisInitialInviteOutputCutoff(ctx, store, &stale, time.Now().Add(time.Second)); !errors.Is(err, rediscore.ErrConflict) {
		t.Fatal("stale output cutoff accepted", err)
	}
	if err = dispatchRedisInitialInviteOutputCutoff(ctx, store, due, time.Now().Add(time.Second)); err != nil {
		t.Fatal("older output cleanup", err)
	}
	selected, err := store.Read(ctx, identity, "r:result-2", "r:result-3", "r:invite-output-"+requestID+"-2")
	if err != nil {
		t.Fatal(err)
	}
	result, err := decodeNativeResult(selected.Records["r:result-2"])
	if err != nil || result.Output != "" || result.Certainty != pb.OperationResult_COMPLETED || len(selected.Records["r:invite-output-"+requestID+"-2"]) != 0 || len(selected.Records["r:result-3"]) == 0 {
		t.Fatal("exact output release changed the wrong result", err)
	}
	due, err = store.PeekDue(ctx, worker, process.Epoch)
	if err != nil || due == nil || due.Member != inviteResultTimer(requestID, 2) {
		t.Fatal("superseded result expiry was not re-driven", due, err)
	}
	if err = expireRedisInviteResult(ctx, store, due, time.Now().Add(time.Second)); err != nil {
		t.Fatal("expire superseded result", err)
	}
	selected, err = store.Read(ctx, identity, "r:result-2", "r:result-3")
	if err != nil || len(selected.Records["r:result-2"]) != 0 || len(selected.Records["r:result-3"]) == 0 {
		t.Fatal("receipt expiration lost the current result", err)
	}
	due, err = store.PeekDue(ctx, worker, process.Epoch)
	if err != nil || due == nil || due.Member != "out-invite-output-"+requestID+"-3" {
		t.Fatal("uncertain output cutoff missing", due, err)
	}
	if err = dispatchRedisInitialInviteOutputCutoff(ctx, store, due, time.Now().Add(time.Second)); err != nil {
		t.Fatal("uncertain output cutoff", err)
	}
	proof, err := store.InspectOutput(ctx, uncertainRef)
	if err != nil || proof.State != rediscore.OutputUnknown {
		t.Fatal("uncertain proof was released", err)
	}
	selected, err = store.Read(ctx, identity, "r:result-3")
	if err != nil {
		t.Fatal(err)
	}
	currentResult, err := decodeNativeResult(selected.Records["r:result-3"])
	if err != nil || currentResult.Output != "r:invite-output-"+requestID+"-3" || currentResult.Certainty != pb.OperationResult_UNKNOWN {
		t.Fatal("uncertain result lost its exact output", err)
	}
	if obsolete != "" {
		// The UNKNOWN head has already been removed. Selecting the final
		// must re-drive that actual provisional owner without a cleanup poll.
		member := "out-invite-output-" + requestID + "-3"
		if obsolete == "no-final" {
			// Use the native transport-failure transition: termination can
			// retain 183 without ever selecting a final response.
			selected, e := store.ObserveContext(ctx, identity.Context, identity.Incarnation, "r:context", "r:procedure-"+requestID)
			if e != nil {
				t.Fatal(e)
			}
			procedure, e := decodeNativeProcedure(selected.View.Records["r:procedure-"+requestID])
			if e != nil {
				t.Fatal(e)
			}
			wires, e := store.Read(ctx, identity, procedure.Refs.Origin, procedure.Refs.LastResponse)
			if e != nil {
				t.Fatal(e)
			}
			memo, e := selectedmessage.NewMemo(2)
			if e != nil {
				t.Fatal(e)
			}
			tx, e := procedure.attach(&nativeSelectedMessages{records: wires.Records, memo: memo, provenance: "admitted"}, true, true, false)
			if e != nil {
				t.Fatal(e)
			}
			tx, _, e = portable.StepTransaction(tx, portable.Input{Kind: portable.TransportFailure, Now: time.Now().UnixMilli(), Error: "fixture transport failure"})
			if e != nil || tx.Phase != portable.Terminated {
				t.Fatal("fixture did not terminate", e)
			}
			procedureData, e := encodeNativeProcedure(tx, procedure.Refs, procedure.Metadata)
			if e != nil {
				t.Fatal(e)
			}
			current, e := decodeNativeContext(selected.View.Records["r:context"])
			if e != nil {
				t.Fatal(e)
			}
			current.Ended, current.CallRetireAtMs = true, time.Now().Add(2*time.Minute).UnixMilli()
			current.Revision++
			contextData, e := encodeNativeContext(current)
			if e != nil {
				t.Fatal(e)
			}
			_, e = store.Commit(ctx, rediscore.Publication{Identity: identity, Revision: selected.View.Revision, Deadline: time.Now().Add(time.Second), SpendReservation: true,
				Changes: []rediscore.Change{{Field: "r:context", Value: contextData}, {Field: "r:procedure-" + requestID, Value: procedureData}},
				Timers:  &rediscore.TimerMutation{Expected: selected.View.Timer, Target: selected.View.Timer, Changes: []rediscore.TimerChange{{Member: "call-retire", Due: time.UnixMilli(current.CallRetireAtMs)}}}})
			if e != nil {
				t.Fatal(e)
			}
		} else {
			finalCommand := proto.Clone(command).(*pb.Command)
			finalCommand.Sequence, finalCommand.GetRespond().Status = 4, 200
			finalDeadline := time.Now().Add(time.Second)
			finalCommand.Admission.ExpiresAtMs = uint64(finalDeadline.UnixMilli())
			finalCapture, e := captureRedisNotifyWait(finalCommand)
			if e != nil {
				t.Fatal(e)
			}
			if _, e = publishRedisInviteRespond(ctx, store, key, application, lease.ApplicationLeaseRef, finalCommand, finalCapture, "answer", "sip:edge@example.test", "edge.example.test:5061", executor, finalDeadline); e != nil {
				t.Fatal("final did not re-drive obsolete provisional", e)
			}
			due, e = store.PeekDue(ctx, worker, process.Epoch)
			if e != nil || due == nil || due.Member != "out-invite-output-"+requestID+"-3" || due.DeadlineMS != cutoff.UnixMilli() {
				t.Fatal("final lost original provisional physical duty", due, e)
			}
			if e = dispatchRedisInitialInviteOutputCutoff(ctx, store, due, time.Now().Add(time.Second)); e != nil {
				t.Fatal("provisional grace staging", e)
			}
			grace, e := client.ZScore(ctx, root+"core:deadlines:context", member).Result()
			if e != nil || int64(grace) != cutoff.Add(rediscore.OutputObsoleteGrace).UnixMilli() {
				t.Fatal("provisional grace slid or disappeared", grace, e)
			}
		}
		// Mature only this fixture's physical timestamp and deterministic
		// write hash, preserving encoded widths/accounting. Redis time and
		// every final/dialog deadline remain unchanged.
		oldDeadline := time.Now().Add(-rediscore.OutputObsoleteGrace - time.Second).UnixMilli()
		outputField := "r:invite-output-" + requestID + "-3"
		responseField := "r:invite-response-" + requestID + "-3"
		dispatch := dispatchRedisInitialInviteOutputCutoff
		if trying {
			uncertainOutput = tryingOutput
			outputField, responseField = "r:output-"+requestID+"-100", "r:response-"+requestID+"-100"
			member, dispatch = "out-output-"+requestID+"-100", dispatchRedisInviteTryingCutoff
		}
		before, e := store.Read(ctx, identity, outputField)
		if e != nil {
			t.Fatal(e)
		}
		uncertainOutput.Write.Permit.Authorization.DeadlineMs = uint64(oldDeadline)
		encoded, e := encodeNativeOutput(uncertainOutput)
		if e != nil || len(encoded) != len(before.Records[outputField]) {
			t.Fatal("mature provisional changed metadata width", e)
		}
		hash, e := nativeFlowWriteHash(uncertainOutput.Write)
		if e != nil {
			t.Fatal(e)
		}
		prefix := "o:" + uncertainOutput.Write.Effect.Id + ":"
		if e = client.HSet(ctx, root+"core:context:context", outputField, encoded, prefix+"p", hash[:], prefix+"d", oldDeadline).Err(); e != nil {
			t.Fatal(e)
		}
		matureHead := time.UnixMilli(oldDeadline).Add(rediscore.OutputObsoleteGrace)
		if trying {
			matureHead = time.UnixMilli(oldDeadline)
		}
		_, e = store.Commit(ctx, rediscore.Publication{Identity: identity, Revision: before.Revision, Deadline: time.Now().Add(time.Second),
			Timers: &rediscore.TimerMutation{Expected: before.Timer, Target: before.Timer, Changes: []rediscore.TimerChange{{Member: member, Due: matureHead}}}})
		if e != nil {
			t.Fatal(e)
		}
		uncertainRef, e = nativeOutputRef(uncertainOutput.Write)
		if e != nil {
			t.Fatal(e)
		}
		due, e = store.PeekDue(ctx, worker, process.Epoch)
		if e != nil || due == nil || due.Member != member {
			t.Fatal("mature provisional head unavailable", due, e)
		}
		if trying {
			if e = dispatch(ctx, store, due, time.Now().Add(time.Second)); e != nil {
				t.Fatal("Trying original cutoff did not stage grace", e)
			}
			due, e = store.PeekDue(ctx, worker, process.Epoch)
			if e != nil || due == nil || due.Member != member || due.DeadlineMS != oldDeadline+rediscore.OutputObsoleteGrace.Milliseconds() {
				t.Fatal("Trying lost fixed grace head", due, e)
			}
		}
		if obsolete == "released-proof" {
			// Simulate process loss after exact proof release and before the
			// metadata publication. A later dispatch must reconcile absence.
			selected, e := store.ObserveContext(ctx, identity.Context, identity.Incarnation, "r:context", "r:procedure-"+requestID, outputField)
			if e != nil {
				t.Fatal(e)
			}
			current, e := decodeNativeContext(selected.View.Records["r:context"])
			if e != nil {
				t.Fatal(e)
			}
			procedure, e := readPrivateProcedureRecord(selected.View.Records["r:procedure-"+requestID])
			if e != nil {
				t.Fatal(e)
			}
			released, e := forgetRedisProvisionalOutput(ctx, store, due, selected, current, procedure, uncertainOutput, outputField, responseField, time.Now().Add(time.Second))
			if e != nil || !released {
				t.Fatal("exact obsolete proof release", e)
			}
			retained, e := store.Read(ctx, identity, outputField)
			if e != nil || !bytes.Equal(retained.Records[outputField], encoded) {
				t.Fatal("proof release lost reconciliation metadata", e)
			}
		}
		if obsolete == "late-written" {
			if e = writer.Complete(ctx, uncertainOutput.Write, &pb.DeliveryResult{Effect: uncertainOutput.Write.Effect, Outcome: pb.DeliveryResult_WRITTEN}); e != nil {
				t.Fatal(e)
			}
		}
		if e = dispatch(ctx, store, due, time.Now().Add(time.Second)); e != nil {
			t.Fatal("obsolete provisional closure", e)
		}
		if obsolete == "no-final" {
			proof, e := store.InspectOutput(ctx, uncertainRef)
			if e != nil || proof.State != rediscore.OutputUnknown {
				t.Fatal("termination invented a final or released its proof", e)
			}
			retained, e := store.Read(ctx, identity, outputField)
			if e != nil || !bytes.Equal(retained.Records[outputField], encoded) {
				t.Fatal("termination lost native output ownership", e)
			}
			if _, e = client.ZScore(ctx, root+"core:deadlines:context", member).Result(); !errors.Is(e, redis.Nil) {
				t.Fatal("ineligible provisional left a spinning grace head", e)
			}
			return
		}
		if obsolete == "late-written" {
			due, e = store.PeekDue(ctx, worker, process.Epoch)
			if e != nil || due == nil || due.Member != member || due.DeadlineMS != oldDeadline {
				t.Fatal("late definite proof bypassed original consumer", due, e)
			}
			if e = dispatch(ctx, store, due, time.Now().Add(time.Second)); e != nil {
				t.Fatal("late definite provisional completion", e)
			}
		}
		if _, e = store.InspectOutput(ctx, uncertainRef); !errors.Is(e, rediscore.ErrAbsent) {
			t.Fatal("obsolete provisional proof retained", e)
		}
		retained, e := store.Read(ctx, identity, "r:context", outputField, responseField, "r:result-3", "r:result-4", "r:dialog-"+requestID)
		if e != nil || len(retained.Records[outputField]) != 0 || len(retained.Records[responseField]) != 0 || len(retained.Records["r:result-4"]) == 0 || len(retained.Records["r:dialog-"+requestID]) == 0 {
			t.Fatal("provisional closure lost final/dialog or retained obsolete wire", e)
		}
		result, e := decodeNativeResult(retained.Records["r:result-3"])
		certainty := pb.OperationResult_UNKNOWN
		if obsolete == "late-written" {
			certainty = pb.OperationResult_COMPLETED
		}
		if trying {
			certainty = pb.OperationResult_UNKNOWN
		}
		if e != nil || result.Output != "" && !trying || result.Certainty != certainty || result.Pending {
			t.Fatal("provisional closure changed historical certainty", e)
		}
		return
	}
	if err = writer.Complete(ctx, uncertainOutput.Write, &pb.DeliveryResult{Effect: uncertainOutput.Write.Effect, Outcome: pb.DeliveryResult_WRITTEN}); err != nil {
		t.Fatal(err)
	}
	if err = settleRedisInitialInviteOutput(ctx, store, uncertainOutput.Write, nil, executor, time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if err = acknowledgeRedisInitialInviteOutput(ctx, store, uncertainOutput.Write); err != nil {
		t.Fatal(err)
	}
	due, err = store.PeekDue(ctx, worker, process.Epoch)
	if err != nil || due == nil || due.Member != "out-invite-output-"+requestID+"-3" {
		t.Fatal("late proof did not re-drive its original output cutoff", due, err)
	}
	if err = dispatchRedisInitialInviteOutputCutoff(ctx, store, due, time.Now().Add(time.Second)); err != nil {
		t.Fatal("late proof cleanup", err)
	}
	selected, err = store.Read(ctx, identity, "r:result-3")
	if err != nil {
		t.Fatal(err)
	}
	currentResult, err = decodeNativeResult(selected.Records["r:result-3"])
	if err != nil || currentResult.Output != "" || currentResult.Certainty != pb.OperationResult_COMPLETED {
		t.Fatal("late proof did not release only the output backlink", err)
	}
	before, err := store.Read(ctx, identity, "r:procedure-"+requestID)
	if err != nil {
		t.Fatal(err)
	}
	refused := proto.Clone(command).(*pb.Command)
	refused.Sequence, refused.GetRespond().PrincipalId = 4, "other-account"
	refusalDeadline := time.Now().Add(time.Second)
	refused.Admission.ExpiresAtMs = uint64(refusalDeadline.UnixMilli())
	refusedCapture, err := captureRedisNotifyWait(refused)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = publishRedisInviteRespond(ctx, store, key, application, lease.ApplicationLeaseRef, refused, refusedCapture, "refuse", "sip:edge@example.test", "edge.example.test:5061", executor, refusalDeadline); err != nil {
		t.Fatal("publish semantic refusal", err)
	}
	after, err := store.Read(ctx, identity, "r:procedure-"+requestID, "r:result-4", "r:invite-output-"+requestID+"-4")
	if err != nil {
		t.Fatal(err)
	}
	refusedResult, err := decodeNativeResult(after.Records["r:result-4"])
	if err != nil || refusedResult.Certainty != pb.OperationResult_NOT_ACCEPTED || refusedResult.Output != "" || len(after.Records["r:invite-output-"+requestID+"-4"]) != 0 || !bytes.Equal(before.Records["r:procedure-"+requestID], after.Records["r:procedure-"+requestID]) {
		t.Fatal("semantic refusal changed the native transaction or output", err)
	}
	accepted := proto.Clone(command).(*pb.Command)
	accepted.Sequence = 5
	acceptedDeadline := time.Now().Add(750 * time.Millisecond)
	accepted.Admission.ExpiresAtMs = uint64(acceptedDeadline.UnixMilli())
	acceptedCapture, err := captureRedisNotifyWait(accepted)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = publishRedisInviteRespond(ctx, store, key, application, lease.ApplicationLeaseRef, accepted, acceptedCapture, "after-refusal", "sip:edge@example.test", "edge.example.test:5061", executor, acceptedDeadline); err != nil {
		t.Fatal("valid successor after refusal", err)
	}
	waitView, err := store.Read(ctx, identity, "r:invite-output-"+requestID+"-5")
	if err != nil {
		t.Fatal(err)
	}
	waitOutput, err := decodeNativeOutput(waitView.Records["r:invite-output-"+requestID+"-5"])
	if err != nil {
		t.Fatal(err)
	}
	if _, err = writer.Reserve(ctx, waitOutput.Write); err != nil {
		t.Fatal(err)
	}
	workerLease, err := store.RenewWorker(ctx, rediscore.WorkerLeaseRequest{Worker: worker, Epoch: process.Epoch, PrimaryRunID: primary})
	if err != nil {
		t.Fatal(err)
	}
	engine := &redisNative{cfg: Config{Process: process}, worker: worker, resultWaiters: make(map[rediscore.ResultHint]*nativeResultWaiter)}
	hint := rediscore.ResultHint{Context: identity.Context, Incarnation: identity.Incarnation, Sequence: 5}
	waiter, err := engine.registerResultWaiter(hint)
	if err != nil {
		t.Fatal(err)
	}
	defer engine.releaseResultWaiter(hint, waiter)
	shortDeadline := time.Now().Add(20 * time.Millisecond)
	if _, err = engine.awaitRedisInviteRespondResult(ctx, store, identity, lease.ApplicationLeaseRef, acceptedCapture, requestID, "after-refusal", shortDeadline, workerLease.Until, waiter); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("waiter ignored its own shorter deadline", err)
	}
	changed := make(chan error, 1)
	go func() {
		time.Sleep(40 * time.Millisecond)
		view, e := store.Read(ctx, identity, "r:context")
		if e != nil {
			changed <- e
			return
		}
		current, e := decodeNativeContext(view.Records["r:context"])
		if e != nil {
			changed <- e
			return
		}
		current.BindingId = "updated-binding"
		value, e := encodeNativeContext(current)
		if e == nil {
			_, e = store.Commit(ctx, rediscore.Publication{Identity: identity, Revision: view.Revision, Deadline: time.Now().Add(time.Second), Changes: []rediscore.Change{{Field: "r:context", Value: value}}})
		}
		if e == nil && !time.Now().Before(acceptedDeadline) {
			e = errors.New("snapshot fixture update missed original deadline")
		}
		changed <- e
	}()
	answer, err := engine.awaitRedisInviteRespondResult(ctx, store, identity, lease.ApplicationLeaseRef, acceptedCapture, requestID, "after-refusal", time.Now().Add(time.Second), workerLease.Until, waiter)
	if changeErr := <-changed; changeErr != nil {
		t.Fatal("update current snapshot during wait", changeErr)
	}
	if err != nil || answer.Certainty != pb.OperationResult_UNKNOWN || answer.Snapshot.BindingId != "updated-binding" {
		t.Fatal("original deadline did not produce UNKNOWN", err)
	}
	if err = writer.Complete(ctx, waitOutput.Write, &pb.DeliveryResult{Effect: waitOutput.Write.Effect, Outcome: pb.DeliveryResult_WRITTEN}); err != nil {
		t.Fatal(err)
	}
	if err = settleRedisInitialInviteOutput(ctx, store, waitOutput.Write, nil, executor, time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	late, err := store.Read(ctx, identity, "r:result-5")
	if err != nil {
		t.Fatal(err)
	}
	lateResult, err := decodeNativeResult(late.Records["r:result-5"])
	if err != nil || lateResult.Certainty != pb.OperationResult_COMPLETED || answer.Certainty != pb.OperationResult_UNKNOWN {
		t.Fatal("late physical completion changed captured result or retained pending deadline", err)
	}
	rejected := proto.Clone(command).(*pb.Command)
	rejected.Sequence, rejected.GetRespond().Status = 6, 486
	rejectedDeadline := time.Now().Add(time.Second)
	rejected.Admission.ExpiresAtMs = uint64(rejectedDeadline.UnixMilli())
	rejectedCapture, err := captureRedisNotifyWait(rejected)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = publishRedisInviteRespond(ctx, store, key, application, lease.ApplicationLeaseRef, rejected, rejectedCapture, "reject", "sip:edge@example.test", "edge.example.test:5061", executor, rejectedDeadline); err != nil {
		t.Fatal("select negative final", err)
	}
	hView, err := store.ObserveContext(ctx, identity.Context, identity.Incarnation, "r:procedure-"+requestID, "r:origin-"+requestID, "r:invite-response-"+requestID+"-6")
	if err != nil {
		t.Fatal(err)
	}
	hProcedure, err := decodeNativeProcedure(hView.View.Records["r:procedure-"+requestID])
	if err != nil {
		t.Fatal(err)
	}
	hMemo, err := selectedmessage.NewMemo(2)
	if err != nil {
		t.Fatal(err)
	}
	hTransaction, err := hProcedure.attach(&nativeSelectedMessages{records: hView.View.Records, memo: hMemo, provenance: "admitted"}, true, true, false)
	if err != nil {
		t.Fatal(err)
	}
	redisNow, err := client.Time(ctx).Result()
	if err != nil {
		t.Fatal(err)
	}
	hTimer := hTransaction.Timers[portable.TimerH]
	hTimer.At = redisNow.Add(-time.Minute).UnixMilli()
	hTransaction.Timers[portable.TimerH] = hTimer
	hBytes, err := encodeNativeProcedure(hTransaction, hProcedure.Refs, hProcedure.Metadata)
	if err != nil {
		t.Fatal(err)
	}
	hMember := "call-tx-" + requestID + "-" + string(portable.TimerH)
	_, err = store.Commit(ctx, rediscore.Publication{Identity: hView.Identity, Revision: hView.View.Revision, Deadline: time.Now().Add(time.Second), Changes: []rediscore.Change{{Field: "r:procedure-" + requestID, Value: hBytes}}, Timers: &rediscore.TimerMutation{Expected: hView.View.Timer, Target: hView.View.Timer, RequireLease: true, Changes: []rediscore.TimerChange{{Member: hMember, Due: time.UnixMilli(hTimer.At)}}}})
	if err != nil {
		t.Fatal(err)
	}
	hDue, err := store.PeekDue(ctx, worker, process.Epoch)
	if err != nil || hDue == nil || hDue.Member != hMember {
		t.Fatal("synthetic Timer H not due", hDue, err)
	}
	staleH := *hDue
	staleH.Revision++
	if err = fireRedisInviteServerTimer(ctx, store, &staleH, time.Now().Add(time.Second)); !errors.Is(err, rediscore.ErrConflict) {
		t.Fatal("stale Timer H was accepted", err)
	}
	if err = fireRedisInviteServerTimer(ctx, store, hDue, time.Now().Add(time.Second)); err != nil {
		t.Fatal("Timer H execution", err)
	}
	afterH, err := store.Read(ctx, identity, "r:procedure-"+requestID, "r:invite-output-"+requestID+"-6")
	if err != nil {
		t.Fatal(err)
	}
	endedTx, err := decodeNativeProcedure(afterH.Records["r:procedure-"+requestID])
	if err != nil || endedTx.Native.Phase != portable.Terminated || len(afterH.Records["r:invite-output-"+requestID+"-6"]) == 0 {
		t.Fatal("Timer H lost transaction termination or physical evidence", err)
	}
	udpWire := bytes.ReplaceAll(stateTestRequest("INVITE", 1, "", "timer-i").Wire, []byte("SIP/2.0/TLS"), []byte("SIP/2.0/UDP"))
	udpWire = bytes.ReplaceAll(udpWire, []byte(facts.CallID), []byte("timer-i@example.test"))
	udpMessage, err := portable.ParseMessage(udpWire, "udp", "192.0.2.12:5061", "")
	if err != nil {
		t.Fatal(err)
	}
	udpFacts, err := udpMessage.Facts()
	if err != nil {
		t.Fatal(err)
	}
	udpFlow := proto.Clone(flow).(*pb.FlowRef)
	udpFlow.Transport, udpFlow.ConnectionId = "udp", "udp-phone"
	udpInput := input
	udpInput.Context = &pb.ContextRef{Id: "udp-context", Owner: process, State: &pb.StateRef{Authority: authority, GroupId: "udp-context", Incarnation: "incarnation", CallId: udpFacts.CallID}}
	udpInput.RequestID, udpInput.Flow, udpInput.Message, udpInput.Deadline = uuid.NewString(), udpFlow, udpMessage, time.Now().Add(time.Second)
	udpPresentation, err := receiveRedisInvite(ctx, store, udpInput)
	if err != nil {
		t.Fatal(err)
	}
	udpDecision := residentDirectDecision(udpPresentation, 1, 486)
	if _, err = commitRedisInviteFinal(ctx, store, key, application, lease.ApplicationLeaseRef, udpDecision, "udp-reject", "sip:edge@example.test", "edge.example.test:5060", time.Now().Add(time.Second)); err != nil {
		t.Fatal("select UDP negative final", err)
	}
	iView, err := store.ObserveContext(ctx, "udp-context", "incarnation", "r:procedure-"+udpInput.RequestID, "r:origin-"+udpInput.RequestID, "r:invite-response-"+udpInput.RequestID+"-1")
	if err != nil {
		t.Fatal(err)
	}
	iProcedure, err := decodeNativeProcedure(iView.View.Records["r:procedure-"+udpInput.RequestID])
	if err != nil {
		t.Fatal(err)
	}
	iMemo, err := selectedmessage.NewMemo(2)
	if err != nil {
		t.Fatal(err)
	}
	iTransaction, err := iProcedure.attach(&nativeSelectedMessages{records: iView.View.Records, memo: iMemo, provenance: "admitted"}, true, true, false)
	if err != nil {
		t.Fatal(err)
	}
	ackMessage, err := portable.BuildNon2xxACK(iTransaction.Origin, *iTransaction.LastResponse)
	if err != nil {
		t.Fatal(err)
	}
	iTransaction, _, err = portable.StepTransaction(iTransaction, portable.Input{Kind: portable.ReceiveRequest, Message: ackMessage, Now: time.Now().UnixMilli()})
	if err != nil || iTransaction.Phase != portable.Confirmed || !iTransaction.Timers[portable.TimerI].Active {
		t.Fatal("UDP transaction ACK did not arm I", err)
	}
	redisNow, err = client.Time(ctx).Result()
	if err != nil {
		t.Fatal(err)
	}
	iTimer := iTransaction.Timers[portable.TimerI]
	iTimer.At = redisNow.Add(-2 * time.Minute).UnixMilli()
	iTransaction.Timers[portable.TimerI] = iTimer
	iBytes, err := encodeNativeProcedure(iTransaction, iProcedure.Refs, iProcedure.Metadata)
	if err != nil {
		t.Fatal(err)
	}
	iMember := "call-tx-" + udpInput.RequestID + "-" + string(portable.TimerI)
	_, err = store.Commit(ctx, rediscore.Publication{Identity: iView.Identity, Revision: iView.View.Revision, Deadline: time.Now().Add(time.Second), Changes: []rediscore.Change{{Field: "r:procedure-" + udpInput.RequestID, Value: iBytes}}, Timers: &rediscore.TimerMutation{Expected: iView.View.Timer, Target: iView.View.Timer, RequireLease: true, Changes: []rediscore.TimerChange{{Member: "call-tx-" + udpInput.RequestID + "-" + string(portable.TimerH), Delete: true}, {Member: iMember, Due: time.UnixMilli(iTimer.At)}}}})
	if err != nil {
		t.Fatal(err)
	}
	iDue, err := store.PeekDue(ctx, worker, process.Epoch)
	if err != nil || iDue == nil || iDue.Member != iMember {
		t.Fatal("synthetic Timer I not due", iDue, err)
	}
	if err = fireRedisInviteServerTimer(ctx, store, iDue, time.Now().Add(time.Second)); err != nil {
		t.Fatal("Timer I execution", err)
	}
	iAfter, err := store.Read(ctx, iView.Identity, "r:procedure-"+udpInput.RequestID)
	if err != nil {
		t.Fatal(err)
	}
	iEnded, err := decodeNativeProcedure(iAfter.Records["r:procedure-"+udpInput.RequestID])
	if err != nil || iEnded.Native.Phase != portable.Terminated {
		t.Fatal("Timer I did not terminate UDP transaction", err)
	}
	if _, err = store.RenewApplication(ctx, lease.ApplicationLeaseRef); err != nil {
		t.Fatal(err)
	}
	if _, err = store.RenewWorker(ctx, rediscore.WorkerLeaseRequest{Worker: worker, Epoch: process.Epoch, PrimaryRunID: primary}); err != nil {
		t.Fatal(err)
	}
	cleanupWire := bytes.ReplaceAll(stateTestRequest("INVITE", 1, "", "udp-cleanup").Wire, []byte("SIP/2.0/TLS"), []byte("SIP/2.0/UDP"))
	cleanupWire = bytes.ReplaceAll(cleanupWire, []byte(facts.CallID), []byte("udp-cleanup@example.test"))
	cleanupMessage, err := portable.ParseMessage(cleanupWire, "udp", "192.0.2.12:5061", "")
	if err != nil {
		t.Fatal(err)
	}
	cleanupFacts, err := cleanupMessage.Facts()
	if err != nil {
		t.Fatal(err)
	}
	cleanupInput := udpInput
	cleanupInput.Context = &pb.ContextRef{Id: "udp-cleanup", Owner: process, State: &pb.StateRef{Authority: authority, GroupId: "udp-cleanup", Incarnation: "incarnation", CallId: cleanupFacts.CallID}}
	cleanupInput.RequestID, cleanupInput.Message, cleanupInput.Deadline = uuid.NewString(), cleanupMessage, time.Now().Add(time.Second)
	cleanupPresentation, err := receiveRedisInvite(ctx, store, cleanupInput)
	if err != nil {
		t.Fatal(err)
	}
	cleanupDecision := residentDirectDecision(cleanupPresentation, 1, 200)
	if _, err = commitRedisInviteFinal(ctx, store, key, application, lease.ApplicationLeaseRef, cleanupDecision, "udp-answer", "sip:edge@example.test", "edge.example.test:5060", cleanupInput.Deadline); err != nil {
		t.Fatal(err)
	}
	cleanupSelected, err := store.ObserveContext(ctx, "udp-cleanup", "incarnation", "r:invite-output-"+cleanupInput.RequestID+"-1", "r:dialog-"+cleanupInput.RequestID, "r:origin-"+cleanupInput.RequestID, "r:invite-response-"+cleanupInput.RequestID+"-1")
	if err != nil {
		t.Fatal(err)
	}
	initialOutput, err := decodeNativeOutput(cleanupSelected.View.Records["r:invite-output-"+cleanupInput.RequestID+"-1"])
	if err != nil {
		t.Fatal(err)
	}
	if _, err = writer.Reserve(ctx, initialOutput.Write); err != nil {
		t.Fatal(err)
	}
	if err = writer.Complete(ctx, initialOutput.Write, &pb.DeliveryResult{Effect: initialOutput.Write.Effect, Outcome: pb.DeliveryResult_WRITTEN}); err != nil {
		t.Fatal(err)
	}
	if err = settleRedisInitialInviteOutput(ctx, store, initialOutput.Write, nil, executor, time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if err = acknowledgeRedisInitialInviteOutput(ctx, store, initialOutput.Write); err != nil {
		t.Fatal(err)
	}
	cleanupSelected, err = store.ObserveContext(ctx, "udp-cleanup", "incarnation", "r:dialog-"+cleanupInput.RequestID, "r:origin-"+cleanupInput.RequestID, "r:invite-response-"+cleanupInput.RequestID+"-1")
	if err != nil {
		t.Fatal(err)
	}
	cleanupDialog, err := decodeNativeDialog(cleanupSelected.View.Records["r:dialog-"+cleanupInput.RequestID])
	if err != nil {
		t.Fatal(err)
	}
	cleanupMemo, err := selectedmessage.NewMemo(2)
	if err != nil {
		t.Fatal(err)
	}
	cleanupNative, err := cleanupDialog.attach(&nativeSelectedMessages{records: cleanupSelected.View.Records, memo: cleanupMemo, provenance: "admitted"}, nativeDialogSelection{Remote: nativeInviteSelection{Request: true, Final: true}})
	if err != nil {
		t.Fatal(err)
	}
	redisNow, err = client.Time(ctx).Result()
	if err != nil {
		t.Fatal(err)
	}
	cleanupTimer := cleanupNative.Timers[portable.TimerACK]
	cleanupTimer.At = redisNow.Add(-3 * time.Minute).UnixMilli()
	cleanupNative.Timers[portable.TimerACK] = cleanupTimer
	cleanupDialogBytes, err := encodeNativeDialog(cleanupNative, cleanupDialog.Refs, cleanupDialog.Metadata)
	if err != nil {
		t.Fatal(err)
	}
	cleanupMember := callDialogTimerMember("r:dialog-"+cleanupInput.RequestID, portable.TimerACK)
	_, err = store.Commit(ctx, rediscore.Publication{Identity: cleanupSelected.Identity, Revision: cleanupSelected.View.Revision, Deadline: time.Now().Add(time.Second), Changes: []rediscore.Change{{Field: "r:dialog-" + cleanupInput.RequestID, Value: cleanupDialogBytes}}, Timers: &rediscore.TimerMutation{Expected: cleanupSelected.View.Timer, Target: cleanupSelected.View.Timer, RequireLease: true, Changes: []rediscore.TimerChange{{Member: cleanupMember, Due: time.UnixMilli(cleanupTimer.At)}}}})
	if err != nil {
		t.Fatal(err)
	}
	cleanupDue, err := store.PeekDue(ctx, worker, process.Epoch)
	if err != nil || cleanupDue == nil || cleanupDue.Member != cleanupMember {
		t.Fatal("synthetic UDP ACK timeout missing", cleanupDue, err)
	}
	if err = fireRedisInitialInviteACKTimeout(ctx, store, key, cleanupDue, time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	cleanupSelected, err = store.ObserveContext(ctx, "udp-cleanup", "incarnation", "r:context", "r:dialog-"+cleanupInput.RequestID)
	if err != nil {
		t.Fatal(err)
	}
	cleanupContext, err := decodeNativeContext(cleanupSelected.View.Records["r:context"])
	if err != nil || len(cleanupContext.NoninitialProcedures) != 1 {
		t.Fatal("UDP cleanup procedure missing", err)
	}
	cleanupDialogRecord, err := readPrivateDialogRecord(cleanupSelected.View.Records["r:dialog-"+cleanupInput.RequestID])
	if err != nil || len(cleanupDialogRecord.Outputs) != 1 {
		t.Fatal("UDP cleanup output missing", err)
	}
	cleanupOutputView, err := store.Read(ctx, cleanupSelected.Identity, cleanupDialogRecord.Outputs[0])
	if err != nil {
		t.Fatal(err)
	}
	cleanupOutput, err := decodeNativeOutput(cleanupOutputView.Records[cleanupDialogRecord.Outputs[0]])
	if err != nil {
		t.Fatal(err)
	}
	committed, err := writer.Reserve(ctx, cleanupOutput.Write)
	if err != nil {
		t.Fatal(err)
	}
	if err = writer.Complete(ctx, cleanupOutput.Write, &pb.DeliveryResult{Effect: cleanupOutput.Write.Effect, Outcome: pb.DeliveryResult_WRITTEN}); err != nil {
		t.Fatal(err)
	}
	if err = settleRedisCleanupByeOutput(ctx, store, cleanupOutput.Write, nil, executor, time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	writes := make(chan []byte, 2)
	manager, _ := udpRetryManager(t, nativeUDPRetryLimits{Entries: 2, Hints: 2, Writers: 2, Bytes: 1 << 20}, func(_ context.Context, _ *pb.FlowRef, wire []byte, _ time.Time) error {
		writes <- bytes.Clone(wire)
		return nil
	})
	engine.udp = manager
	cleanupRef, err := nativeOutputRef(cleanupOutput.Write)
	if err != nil {
		t.Fatal(err)
	}
	if err = engine.armRedisCleanupByeUDP(ctx, store, cleanupRef, cleanupOutput); err != nil {
		t.Fatal("arm exact cleanup BYE E derivative", err)
	}
	if err = acknowledgeRedisCleanupByeOutput(ctx, store, cleanupOutput.Write, time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	select {
	case retry := <-writes:
		if !bytes.Equal(retry, committed.Wire) {
			t.Fatal("cleanup BYE retry changed physical wire")
		}
	case <-time.After(time.Second):
		t.Fatal("cleanup BYE E derivative missing")
	}
	manager.Stop(nativeUDPRetryKey{Context: "udp-cleanup", Incarnation: "incarnation", Procedure: cleanupContext.NoninitialProcedures[0]})
	fView, err := store.ObserveContext(ctx, "udp-cleanup", "incarnation", cleanupContext.NoninitialProcedures[0], cleanupDialogRecord.LocalBye)
	if err != nil {
		t.Fatal(err)
	}
	fProcedure, err := decodeNativeProcedure(fView.View.Records[cleanupContext.NoninitialProcedures[0]])
	if err != nil {
		t.Fatal(err)
	}
	fMemo, err := selectedmessage.NewMemo(1)
	if err != nil {
		t.Fatal(err)
	}
	fTransaction, err := fProcedure.attach(&nativeSelectedMessages{records: fView.View.Records, memo: fMemo, provenance: "admitted"}, true, false, false)
	if err != nil {
		t.Fatal(err)
	}
	redisNow, err = client.Time(ctx).Result()
	if err != nil {
		t.Fatal(err)
	}
	fTimer := fTransaction.Timers[portable.TimerF]
	fTimer.At = redisNow.Add(-10 * time.Minute).UnixMilli()
	fTransaction.Timers[portable.TimerF] = fTimer
	fBytes, err := encodeNativeProcedure(fTransaction, fProcedure.Refs, fProcedure.Metadata)
	if err != nil {
		t.Fatal(err)
	}
	fID := strings.TrimPrefix(cleanupContext.NoninitialProcedures[0], "r:call-cleanup-")
	fMember := "call-client-" + fID + "-" + string(portable.TimerF)
	_, err = store.Commit(ctx, rediscore.Publication{Identity: fView.Identity, Revision: fView.View.Revision, Deadline: time.Now().Add(time.Second), Changes: []rediscore.Change{{Field: cleanupContext.NoninitialProcedures[0], Value: fBytes}}, Timers: &rediscore.TimerMutation{Expected: fView.View.Timer, Target: fView.View.Timer, RequireLease: true, Changes: []rediscore.TimerChange{{Member: fMember, Due: time.UnixMilli(fTimer.At)}}}})
	if err != nil {
		t.Fatal(err)
	}
	fDue, err := store.PeekDue(ctx, worker, process.Epoch)
	if err != nil || fDue == nil || fDue.Member != fMember {
		t.Fatal("cleanup BYE F not due", fDue, err)
	}
	staleF := *fDue
	staleF.Revision++
	if err = fireRedisCleanupByeTimer(ctx, store, &staleF, time.Now().Add(time.Second)); !errors.Is(err, rediscore.ErrConflict) {
		t.Fatal("stale cleanup BYE F was accepted", err)
	}
	if err = fireRedisCleanupByeTimer(ctx, store, fDue, time.Now().Add(time.Second)); err != nil {
		t.Fatal("cleanup BYE F execution", err)
	}
	fAfter, err := store.ObserveContext(ctx, "udp-cleanup", "incarnation", "r:context", "r:dialog-"+cleanupInput.RequestID, cleanupContext.NoninitialProcedures[0], cleanupDialogRecord.Outputs[0])
	if err != nil {
		t.Fatal(err)
	}
	fContext, err := decodeNativeContext(fAfter.View.Records["r:context"])
	if err != nil || !fContext.Ended || fContext.CallRetireAtMs == 0 {
		t.Fatal("cleanup BYE F did not end its selected call", err)
	}
	fDialog, err := decodeNativeDialog(fAfter.View.Records["r:dialog-"+cleanupInput.RequestID])
	if err != nil || fDialog.Native.Phase != portable.DialogEnded || len(fAfter.View.Records[cleanupDialogRecord.Outputs[0]]) == 0 {
		t.Fatal("cleanup BYE F lost dialog end or output evidence", err)
	}
	deadlineMembers, err := client.ZRange(ctx, root+"core:deadlines:udp-cleanup", 0, -1).Result()
	if err != nil {
		t.Fatal(err)
	}
	events := 0
	var terminalEvent string
	for _, member := range deadlineMembers {
		if strings.HasPrefix(member, "event-") {
			events++
			terminalEvent = member
		}
	}
	if events != 1 {
		t.Fatalf("cleanup BYE F staged %d terminal events", events)
	}
	eventView, err := store.Read(ctx, fAfter.Identity, "r:"+terminalEvent)
	if err != nil {
		t.Fatal(err)
	}
	event, err := decodeNativeLifecycle(eventView.Records["r:"+terminalEvent])
	if err != nil || event.Kind != pb.DialogEvent_ENDED || event.Message != "" || event.Snapshot == nil || !event.Snapshot.Ended {
		t.Fatal("cleanup BYE F did not retain a snapshot-only ENDED event", err)
	}
	{
		if _, err = store.RenewWorker(ctx, rediscore.WorkerLeaseRequest{Worker: worker, Epoch: process.Epoch, PrimaryRunID: primary}); err != nil {
			t.Fatal(err)
		}
		cleanupWire := bytes.ReplaceAll(stateTestRequest("INVITE", 1, "", "udp-reply").Wire, []byte("SIP/2.0/TLS"), []byte("SIP/2.0/UDP"))
		cleanupWire = bytes.ReplaceAll(cleanupWire, []byte(facts.CallID), []byte("udp-reply@example.test"))
		cleanupMessage, err := portable.ParseMessage(cleanupWire, "udp", "192.0.2.12:5061", "")
		if err != nil {
			t.Fatal(err)
		}
		cleanupFacts, err := cleanupMessage.Facts()
		if err != nil {
			t.Fatal(err)
		}
		cleanupInput := udpInput
		cleanupInput.Context = &pb.ContextRef{Id: "udp-reply", Owner: process, State: &pb.StateRef{Authority: authority, GroupId: "udp-reply", Incarnation: "incarnation", CallId: cleanupFacts.CallID}}
		cleanupInput.RequestID, cleanupInput.Message, cleanupInput.Deadline = uuid.NewString(), cleanupMessage, time.Now().Add(time.Second)
		cleanupPresentation, err := receiveRedisInvite(ctx, store, cleanupInput)
		if err != nil {
			t.Fatal(err)
		}
		cleanupDecision := residentDirectDecision(cleanupPresentation, 1, 200)
		if _, err = commitRedisInviteFinal(ctx, store, key, application, lease.ApplicationLeaseRef, cleanupDecision, "udp-answer", "sip:edge@example.test", "edge.example.test:5060", cleanupInput.Deadline); err != nil {
			t.Fatal(err)
		}
		cleanupSelected, err := store.ObserveContext(ctx, "udp-reply", "incarnation", "r:invite-output-"+cleanupInput.RequestID+"-1", "r:dialog-"+cleanupInput.RequestID, "r:origin-"+cleanupInput.RequestID, "r:invite-response-"+cleanupInput.RequestID+"-1")
		if err != nil {
			t.Fatal(err)
		}
		initialOutput, err := decodeNativeOutput(cleanupSelected.View.Records["r:invite-output-"+cleanupInput.RequestID+"-1"])
		if err != nil {
			t.Fatal(err)
		}
		if _, err = writer.Reserve(ctx, initialOutput.Write); err != nil {
			t.Fatal(err)
		}
		if err = writer.Complete(ctx, initialOutput.Write, &pb.DeliveryResult{Effect: initialOutput.Write.Effect, Outcome: pb.DeliveryResult_WRITTEN}); err != nil {
			t.Fatal(err)
		}
		if err = settleRedisInitialInviteOutput(ctx, store, initialOutput.Write, nil, executor, time.Now().Add(time.Second)); err != nil {
			t.Fatal(err)
		}
		if err = acknowledgeRedisInitialInviteOutput(ctx, store, initialOutput.Write); err != nil {
			t.Fatal(err)
		}
		cleanupSelected, err = store.ObserveContext(ctx, "udp-reply", "incarnation", "r:dialog-"+cleanupInput.RequestID, "r:origin-"+cleanupInput.RequestID, "r:invite-response-"+cleanupInput.RequestID+"-1")
		if err != nil {
			t.Fatal(err)
		}
		cleanupDialog, err := decodeNativeDialog(cleanupSelected.View.Records["r:dialog-"+cleanupInput.RequestID])
		if err != nil {
			t.Fatal(err)
		}
		cleanupMemo, err := selectedmessage.NewMemo(2)
		if err != nil {
			t.Fatal(err)
		}
		cleanupNative, err := cleanupDialog.attach(&nativeSelectedMessages{records: cleanupSelected.View.Records, memo: cleanupMemo, provenance: "admitted"}, nativeDialogSelection{Remote: nativeInviteSelection{Request: true, Final: true}})
		if err != nil {
			t.Fatal(err)
		}
		redisNow, err = client.Time(ctx).Result()
		if err != nil {
			t.Fatal(err)
		}
		cleanupTimer := cleanupNative.Timers[portable.TimerACK]
		cleanupTimer.At = redisNow.Add(-3 * time.Minute).UnixMilli()
		cleanupNative.Timers[portable.TimerACK] = cleanupTimer
		cleanupDialogBytes, err := encodeNativeDialog(cleanupNative, cleanupDialog.Refs, cleanupDialog.Metadata)
		if err != nil {
			t.Fatal(err)
		}
		cleanupMember := callDialogTimerMember("r:dialog-"+cleanupInput.RequestID, portable.TimerACK)
		_, err = store.Commit(ctx, rediscore.Publication{Identity: cleanupSelected.Identity, Revision: cleanupSelected.View.Revision, Deadline: time.Now().Add(time.Second), Changes: []rediscore.Change{{Field: "r:dialog-" + cleanupInput.RequestID, Value: cleanupDialogBytes}}, Timers: &rediscore.TimerMutation{Expected: cleanupSelected.View.Timer, Target: cleanupSelected.View.Timer, RequireLease: true, Changes: []rediscore.TimerChange{{Member: cleanupMember, Due: time.UnixMilli(cleanupTimer.At)}}}})
		if err != nil {
			t.Fatal(err)
		}
		cleanupDue, err := store.PeekDue(ctx, worker, process.Epoch)
		if err != nil || cleanupDue == nil || cleanupDue.Member != cleanupMember {
			t.Fatal("synthetic UDP ACK timeout missing", cleanupDue, err)
		}
		if err = fireRedisInitialInviteACKTimeout(ctx, store, key, cleanupDue, time.Now().Add(time.Second)); err != nil {
			t.Fatal(err)
		}
		cleanupSelected, err = store.ObserveContext(ctx, "udp-reply", "incarnation", "r:context", "r:dialog-"+cleanupInput.RequestID)
		if err != nil {
			t.Fatal(err)
		}
		cleanupContext, err := decodeNativeContext(cleanupSelected.View.Records["r:context"])
		if err != nil || len(cleanupContext.NoninitialProcedures) != 1 {
			t.Fatal("UDP cleanup procedure missing", err)
		}
		cleanupDialogRecord, err := readPrivateDialogRecord(cleanupSelected.View.Records["r:dialog-"+cleanupInput.RequestID])
		if err != nil || len(cleanupDialogRecord.Outputs) != 1 {
			t.Fatal("UDP cleanup output missing", err)
		}
		cleanupOutputView, err := store.Read(ctx, cleanupSelected.Identity, cleanupDialogRecord.Outputs[0])
		if err != nil {
			t.Fatal(err)
		}
		cleanupOutput, err := decodeNativeOutput(cleanupOutputView.Records[cleanupDialogRecord.Outputs[0]])
		if err != nil {
			t.Fatal(err)
		}
		committed, err := writer.Reserve(ctx, cleanupOutput.Write)
		if err != nil {
			t.Fatal(err)
		}
		if err = writer.Complete(ctx, cleanupOutput.Write, &pb.DeliveryResult{Effect: cleanupOutput.Write.Effect, Outcome: pb.DeliveryResult_UNKNOWN}); err != nil {
			t.Fatal(err)
		}
		cleanupDeadline := time.UnixMilli(int64(cleanupOutput.Write.Permit.Authorization.DeadlineMs))
		if wait := time.Until(cleanupDeadline); wait > 0 {
			time.Sleep(wait + 15*time.Millisecond)
		}
		if _, err = store.RenewWorker(ctx, rediscore.WorkerLeaseRequest{Worker: worker, Epoch: process.Epoch, PrimaryRunID: primary}); err != nil {
			t.Fatal(err)
		}
		firstView, err := store.ObserveContext(ctx, "udp-reply", "incarnation", "r:context")
		if err != nil {
			t.Fatal(err)
		}
		firstDeadline := int64(initialOutput.Write.Permit.Authorization.DeadlineMs)
		checkedNow := waitRedisFixtureTime(t, ctx, client, time.UnixMilli(firstDeadline))
		firstDue := &rediscore.DueContext{Identity: firstView.Identity, Revision: firstView.View.Revision, Assignment: firstView.View.Timer, Member: "out-invite-output-" + cleanupInput.RequestID + "-1", DeadlineMS: firstDeadline, CheckedAtMS: checkedNow.UnixMilli()}
		if err = dispatchRedisInitialInviteOutputCutoff(ctx, store, firstDue, time.Now().Add(time.Second)); err != nil {
			t.Fatal("initial proof cutoff before cleanup", err)
		}
		unknownView, err := store.ObserveContext(ctx, "udp-reply", "incarnation", "r:context")
		if err != nil {
			t.Fatal(err)
		}
		checkedNow = waitRedisFixtureTime(t, ctx, client, cleanupDeadline)
		unknownDue := &rediscore.DueContext{Identity: unknownView.Identity, Revision: unknownView.View.Revision, Assignment: unknownView.View.Timer, Member: "out-" + strings.TrimPrefix(cleanupDialogRecord.Outputs[0], "r:"), DeadlineMS: cleanupDeadline.UnixMilli(), CheckedAtMS: checkedNow.UnixMilli()}
		if err = dispatchRedisCleanupByeCutoff(ctx, store, unknownDue, time.Now().Add(time.Second)); err != nil {
			t.Fatal("uncertain cleanup cutoff", err)
		}
		unknownRef, err := nativeOutputRef(cleanupOutput.Write)
		if err != nil {
			t.Fatal(err)
		}
		unknownProof, err := store.InspectOutput(ctx, unknownRef)
		if err != nil || unknownProof.State != rediscore.OutputUnknown || unknownProof.Settled || unknownProof.Acknowledged {
			t.Fatal("unknown proof was consumed", err)
		}
		if _, err = client.ZScore(ctx, root+"core:deadlines:udp-reply", unknownDue.Member).Result(); !errors.Is(err, redis.Nil) {
			t.Fatal("unknown output still indexed", err)
		}
		unknownOwner, err := store.ObserveContext(ctx, "udp-reply", "incarnation", "r:context", "r:dialog-"+cleanupInput.RequestID, cleanupDialogRecord.Outputs[0])
		if err != nil {
			t.Fatal(err)
		}
		unknownContext, err := decodeNativeContext(unknownOwner.View.Records["r:context"])
		if err != nil || !slices.Contains(unknownContext.NoninitialProcedures, cleanupContext.NoninitialProcedures[0]) {
			t.Fatal("unknown procedure is unreachable", err)
		}
		unknownDialog, err := readPrivateDialogRecord(unknownOwner.View.Records["r:dialog-"+cleanupInput.RequestID])
		if err != nil || !slices.Contains(unknownDialog.Outputs, cleanupDialogRecord.Outputs[0]) || len(unknownOwner.View.Records[cleanupDialogRecord.Outputs[0]]) == 0 {
			t.Fatal("unknown output is unreachable", err)
		}
		activeRetire := *unknownDue
		activeRetire.Revision = unknownOwner.View.Revision
		activeRetire.Member = "call-retire"
		if forgotten, e := forgetRedisEndedCleanupBye(ctx, store, &activeRetire, time.Now().Add(time.Second)); e == nil || forgotten {
			t.Fatal("active call allowed uncertain proof reclamation", e)
		}
		if _, e := store.InspectOutput(ctx, unknownRef); e != nil {
			t.Fatal("active refusal changed proof", e)
		}
		if err = writer.Complete(ctx, cleanupOutput.Write, &pb.DeliveryResult{Effect: cleanupOutput.Write.Effect, Outcome: pb.DeliveryResult_WRITTEN}); err != nil {
			t.Fatal(err)
		}
		if err = settleRedisCleanupByeOutput(ctx, store, cleanupOutput.Write, nil, executor, time.Now().Add(time.Second)); err != nil {
			t.Fatal(err)
		}

		parsed, e := parseObservedMessage(committed.Wire)
		if e != nil {
			t.Fatal(e)
		}
		request, ok := parsed.(*sip.Request)
		if !ok {
			t.Fatal("cleanup BYE was not a request")
		}
		response := sip.NewResponseFromRequest(request, 200, "OK", nil)
		responseWire := []byte(response.String())
		responseMessage, e := portable.ParseMessage(responseWire, "udp", "192.0.2.12:5061", "")
		if e != nil {
			t.Fatal(e)
		}
		handled, e := completeRedisCleanupByeResponse(ctx, store, cleanupOutput.Write.Flow, responseMessage, []string{"fixture-selector"}, time.Now().Add(time.Second))
		if e != nil || !handled {
			t.Fatal("cleanup BYE response publication", e)
		}
		after, e := store.ObserveContext(ctx, "udp-reply", "incarnation", "r:context", cleanupContext.NoninitialProcedures[0], "r:procedure-"+cleanupInput.RequestID)
		if e != nil {
			t.Fatal(e)
		}
		afterContext, e := decodeNativeContext(after.View.Records["r:context"])
		if e != nil || !afterContext.Ended {
			t.Fatal("cleanup response did not end call", e)
		}
		anchor, e := readPrivateProcedureRecord(after.View.Records["r:procedure-"+cleanupInput.RequestID])
		if e != nil || len(anchor.Events) != 1 {
			t.Fatal("cleanup response event not rooted", e)
		}
		eventView, e := store.Read(ctx, after.Identity, anchor.Events[0])
		if e != nil {
			t.Fatal(e)
		}
		event, e := decodeNativeLifecycle(eventView.Records[anchor.Events[0]])
		if e != nil || event.Kind != pb.DialogEvent_ENDED || event.Root != "r:procedure-"+cleanupInput.RequestID || event.Message == "" || len(event.Owners) != 1 || event.Owners[0] != cleanupContext.NoninitialProcedures[0] || len(event.Selectors) != 1 || event.Selectors[0] != "fixture-selector" {
			t.Fatal("cleanup response event lost exact ownership", e)
		}
		messageView, e := store.Read(ctx, after.Identity, event.Message)
		if e != nil {
			t.Fatal(e)
		}
		stored, e := selectedmessage.DecodeWire(messageView.Records[event.Message])
		if e != nil || !bytes.Equal(stored, responseWire) {
			t.Fatal("cleanup response wire not retained exactly", e)
		}
		if e = acknowledgeRedisCleanupByeOutput(ctx, store, cleanupOutput.Write, time.Now().Add(time.Second)); e != nil {
			t.Fatal(e)
		}
		if _, e = store.RenewWorker(ctx, rediscore.WorkerLeaseRequest{Worker: worker, Epoch: process.Epoch, PrimaryRunID: primary}); e != nil {
			t.Fatal(e)
		}
		procField := cleanupContext.NoninitialProcedures[0]
		kView, e := store.ObserveContext(ctx, "udp-reply", "incarnation", procField, cleanupDialogRecord.LocalBye, event.Message)
		if e != nil {
			t.Fatal(e)
		}
		kProcedure, e := decodeNativeProcedure(kView.View.Records[procField])
		if e != nil {
			t.Fatal(e)
		}
		kMemo, e := selectedmessage.NewMemo(2)
		if e != nil {
			t.Fatal(e)
		}
		kTransaction, e := kProcedure.attach(&nativeSelectedMessages{records: kView.View.Records, memo: kMemo, provenance: "admitted"}, true, true, false)
		if e != nil {
			t.Fatal(e)
		}
		kNow, e := client.Time(ctx).Result()
		if e != nil {
			t.Fatal(e)
		}
		kTimer := kTransaction.Timers[portable.TimerK]
		if !kTimer.Active {
			t.Fatal("cleanup response missing K")
		}
		kTimer.At = kNow.Add(-time.Minute).UnixMilli()
		kTransaction.Timers[portable.TimerK] = kTimer
		kBytes, e := encodeNativeProcedure(kTransaction, kProcedure.Refs, kProcedure.Metadata)
		if e != nil {
			t.Fatal(e)
		}
		kID := strings.TrimPrefix(procField, "r:call-cleanup-")
		kMember := "call-client-" + kID + "-" + string(portable.TimerK)
		_, e = store.Commit(ctx, rediscore.Publication{Identity: kView.Identity, Revision: kView.View.Revision, Deadline: time.Now().Add(time.Second), Changes: []rediscore.Change{{Field: procField, Value: kBytes}}, Timers: &rediscore.TimerMutation{Expected: kView.View.Timer, Target: kView.View.Timer, RequireLease: true, Changes: []rediscore.TimerChange{{Member: kMember, Due: time.UnixMilli(kTimer.At)}}}})
		if e != nil {
			t.Fatal(e)
		}
		kDue, e := store.PeekDue(ctx, worker, process.Epoch)
		if e != nil || kDue == nil || kDue.Member != kMember {
			t.Fatal("cleanup K not due", kDue, e)
		}
		if e = fireRedisCleanupByeTimer(ctx, store, kDue, time.Now().Add(time.Second)); e != nil {
			t.Fatal(e)
		}
		cutoff := time.UnixMilli(int64(cleanupOutput.Write.Permit.Authorization.DeadlineMs))
		if wait := time.Until(cutoff); wait > 0 {
			time.Sleep(wait + 15*time.Millisecond)
		}
		if _, e = store.RenewWorker(ctx, rediscore.WorkerLeaseRequest{Worker: worker, Epoch: process.Epoch, PrimaryRunID: primary}); e != nil {
			t.Fatal(e)
		}
		retireView, e := store.ObserveContext(ctx, "udp-reply", "incarnation", "r:context")
		if e != nil {
			t.Fatal(e)
		}
		checked, e := client.Time(ctx).Result()
		if e != nil {
			t.Fatal(e)
		}
		outDue := &rediscore.DueContext{Identity: retireView.Identity, Revision: retireView.View.Revision, Assignment: retireView.View.Timer, Member: "out-" + strings.TrimPrefix(cleanupDialogRecord.Outputs[0], "r:"), DeadlineMS: cutoff.UnixMilli(), CheckedAtMS: checked.UnixMilli()}
		if e = dispatchRedisCleanupByeCutoff(ctx, store, outDue, time.Now().Add(time.Second)); e != nil {
			t.Fatal("cleanup procedure retirement", e)
		}
		retired, e := store.ObserveContext(ctx, "udp-reply", "incarnation", "r:context", procField, anchor.Events[0], event.Message, cleanupDialogRecord.LocalBye)
		if e != nil {
			t.Fatal(e)
		}
		retiredContext, e := decodeNativeContext(retired.View.Records["r:context"])
		if e != nil || len(retiredContext.NoninitialProcedures) != 0 || len(retired.View.Records[procField]) != 0 {
			t.Fatal("cleanup procedure remains", e)
		}
		retiredEvent, e := decodeNativeLifecycle(retired.View.Records[anchor.Events[0]])
		if e != nil || len(retiredEvent.Owners) != 0 || len(retired.View.Records[event.Message]) == 0 || len(retired.View.Records[cleanupDialogRecord.LocalBye]) == 0 {
			t.Fatal("retirement lost event or dialog wire ownership", e)
		}
	}

	{
		if _, err = store.RenewWorker(ctx, rediscore.WorkerLeaseRequest{Worker: worker, Epoch: process.Epoch, PrimaryRunID: primary}); err != nil {
			t.Fatal(err)
		}
		cleanupWire := bytes.ReplaceAll(stateTestRequest("INVITE", 1, "", "udp-forget").Wire, []byte("SIP/2.0/TLS"), []byte("SIP/2.0/UDP"))
		cleanupWire = bytes.ReplaceAll(cleanupWire, []byte(facts.CallID), []byte("udp-forget@example.test"))
		cleanupMessage, err := portable.ParseMessage(cleanupWire, "udp", "192.0.2.12:5061", "")
		if err != nil {
			t.Fatal(err)
		}
		cleanupFacts, err := cleanupMessage.Facts()
		if err != nil {
			t.Fatal(err)
		}
		cleanupInput := udpInput
		cleanupInput.Context = &pb.ContextRef{Id: "udp-forget", Owner: process, State: &pb.StateRef{Authority: authority, GroupId: "udp-forget", Incarnation: "incarnation", CallId: cleanupFacts.CallID}}
		cleanupInput.RequestID, cleanupInput.Message, cleanupInput.Deadline = uuid.NewString(), cleanupMessage, time.Now().Add(time.Second)
		cleanupPresentation, err := receiveRedisInvite(ctx, store, cleanupInput)
		if err != nil {
			t.Fatal(err)
		}
		cleanupDecision := residentDirectDecision(cleanupPresentation, 1, 200)
		if _, err = commitRedisInviteFinal(ctx, store, key, application, lease.ApplicationLeaseRef, cleanupDecision, "udp-answer", "sip:edge@example.test", "edge.example.test:5060", cleanupInput.Deadline); err != nil {
			t.Fatal(err)
		}
		cleanupSelected, err := store.ObserveContext(ctx, "udp-forget", "incarnation", "r:invite-output-"+cleanupInput.RequestID+"-1", "r:dialog-"+cleanupInput.RequestID, "r:origin-"+cleanupInput.RequestID, "r:invite-response-"+cleanupInput.RequestID+"-1")
		if err != nil {
			t.Fatal(err)
		}
		initialOutput, err := decodeNativeOutput(cleanupSelected.View.Records["r:invite-output-"+cleanupInput.RequestID+"-1"])
		if err != nil {
			t.Fatal(err)
		}
		if _, err = writer.Reserve(ctx, initialOutput.Write); err != nil {
			t.Fatal(err)
		}
		if err = writer.Complete(ctx, initialOutput.Write, &pb.DeliveryResult{Effect: initialOutput.Write.Effect, Outcome: pb.DeliveryResult_WRITTEN}); err != nil {
			t.Fatal(err)
		}
		if err = settleRedisInitialInviteOutput(ctx, store, initialOutput.Write, nil, executor, time.Now().Add(time.Second)); err != nil {
			t.Fatal(err)
		}
		if err = acknowledgeRedisInitialInviteOutput(ctx, store, initialOutput.Write); err != nil {
			t.Fatal(err)
		}
		cleanupSelected, err = store.ObserveContext(ctx, "udp-forget", "incarnation", "r:dialog-"+cleanupInput.RequestID, "r:origin-"+cleanupInput.RequestID, "r:invite-response-"+cleanupInput.RequestID+"-1")
		if err != nil {
			t.Fatal(err)
		}
		cleanupDialog, err := decodeNativeDialog(cleanupSelected.View.Records["r:dialog-"+cleanupInput.RequestID])
		if err != nil {
			t.Fatal(err)
		}
		cleanupMemo, err := selectedmessage.NewMemo(2)
		if err != nil {
			t.Fatal(err)
		}
		cleanupNative, err := cleanupDialog.attach(&nativeSelectedMessages{records: cleanupSelected.View.Records, memo: cleanupMemo, provenance: "admitted"}, nativeDialogSelection{Remote: nativeInviteSelection{Request: true, Final: true}})
		if err != nil {
			t.Fatal(err)
		}
		redisNow, err = client.Time(ctx).Result()
		if err != nil {
			t.Fatal(err)
		}
		cleanupTimer := cleanupNative.Timers[portable.TimerACK]
		cleanupTimer.At = redisNow.Add(-3 * time.Minute).UnixMilli()
		cleanupNative.Timers[portable.TimerACK] = cleanupTimer
		cleanupDialogBytes, err := encodeNativeDialog(cleanupNative, cleanupDialog.Refs, cleanupDialog.Metadata)
		if err != nil {
			t.Fatal(err)
		}
		cleanupMember := callDialogTimerMember("r:dialog-"+cleanupInput.RequestID, portable.TimerACK)
		_, err = store.Commit(ctx, rediscore.Publication{Identity: cleanupSelected.Identity, Revision: cleanupSelected.View.Revision, Deadline: time.Now().Add(time.Second), Changes: []rediscore.Change{{Field: "r:dialog-" + cleanupInput.RequestID, Value: cleanupDialogBytes}}, Timers: &rediscore.TimerMutation{Expected: cleanupSelected.View.Timer, Target: cleanupSelected.View.Timer, RequireLease: true, Changes: []rediscore.TimerChange{{Member: cleanupMember, Due: time.UnixMilli(cleanupTimer.At)}}}})
		if err != nil {
			t.Fatal(err)
		}
		cleanupDue, err := store.PeekDue(ctx, worker, process.Epoch)
		if err != nil || cleanupDue == nil || cleanupDue.Member != cleanupMember {
			t.Fatal("synthetic UDP ACK timeout missing", cleanupDue, err)
		}
		if err = fireRedisInitialInviteACKTimeout(ctx, store, key, cleanupDue, time.Now().Add(time.Second)); err != nil {
			t.Fatal(err)
		}
		cleanupSelected, err = store.ObserveContext(ctx, "udp-forget", "incarnation", "r:context", "r:dialog-"+cleanupInput.RequestID)
		if err != nil {
			t.Fatal(err)
		}
		cleanupContext, err := decodeNativeContext(cleanupSelected.View.Records["r:context"])
		if err != nil || len(cleanupContext.NoninitialProcedures) != 1 {
			t.Fatal("UDP cleanup procedure missing", err)
		}
		cleanupDialogRecord, err := readPrivateDialogRecord(cleanupSelected.View.Records["r:dialog-"+cleanupInput.RequestID])
		if err != nil || len(cleanupDialogRecord.Outputs) != 1 {
			t.Fatal("UDP cleanup output missing", err)
		}
		cleanupOutputView, err := store.Read(ctx, cleanupSelected.Identity, cleanupDialogRecord.Outputs[0])
		if err != nil {
			t.Fatal(err)
		}
		cleanupOutput, err := decodeNativeOutput(cleanupOutputView.Records[cleanupDialogRecord.Outputs[0]])
		if err != nil {
			t.Fatal(err)
		}
		_, err = writer.Reserve(ctx, cleanupOutput.Write)
		if err != nil {
			t.Fatal(err)
		}
		if err = writer.Complete(ctx, cleanupOutput.Write, &pb.DeliveryResult{Effect: cleanupOutput.Write.Effect, Outcome: pb.DeliveryResult_UNKNOWN}); err != nil {
			t.Fatal(err)
		}
		cleanupDeadline := time.UnixMilli(int64(cleanupOutput.Write.Permit.Authorization.DeadlineMs))
		if wait := time.Until(cleanupDeadline); wait > 0 {
			time.Sleep(wait + 15*time.Millisecond)
		}
		if _, err = store.RenewWorker(ctx, rediscore.WorkerLeaseRequest{Worker: worker, Epoch: process.Epoch, PrimaryRunID: primary}); err != nil {
			t.Fatal(err)
		}
		firstView, err := store.ObserveContext(ctx, "udp-forget", "incarnation", "r:context")
		if err != nil {
			t.Fatal(err)
		}
		firstDeadline := int64(initialOutput.Write.Permit.Authorization.DeadlineMs)
		checkedNow := waitRedisFixtureTime(t, ctx, client, time.UnixMilli(firstDeadline))
		firstDue := &rediscore.DueContext{Identity: firstView.Identity, Revision: firstView.View.Revision, Assignment: firstView.View.Timer, Member: "out-invite-output-" + cleanupInput.RequestID + "-1", DeadlineMS: firstDeadline, CheckedAtMS: checkedNow.UnixMilli()}
		if err = dispatchRedisInitialInviteOutputCutoff(ctx, store, firstDue, time.Now().Add(time.Second)); err != nil {
			t.Fatal("initial proof cutoff before cleanup", err)
		}
		unknownView, err := store.ObserveContext(ctx, "udp-forget", "incarnation", "r:context")
		if err != nil {
			t.Fatal(err)
		}
		checkedNow = waitRedisFixtureTime(t, ctx, client, cleanupDeadline)
		unknownDue := &rediscore.DueContext{Identity: unknownView.Identity, Revision: unknownView.View.Revision, Assignment: unknownView.View.Timer, Member: "out-" + strings.TrimPrefix(cleanupDialogRecord.Outputs[0], "r:"), DeadlineMS: cleanupDeadline.UnixMilli(), CheckedAtMS: checkedNow.UnixMilli()}
		if err = dispatchRedisCleanupByeCutoff(ctx, store, unknownDue, time.Now().Add(time.Second)); err != nil {
			t.Fatal("uncertain cleanup cutoff", err)
		}
		unknownRef, err := nativeOutputRef(cleanupOutput.Write)
		if err != nil {
			t.Fatal(err)
		}
		unknownProof, err := store.InspectOutput(ctx, unknownRef)
		if err != nil || unknownProof.State != rediscore.OutputUnknown || unknownProof.Settled || unknownProof.Acknowledged {
			t.Fatal("unknown proof was consumed", err)
		}
		if _, err = client.ZScore(ctx, root+"core:deadlines:udp-forget", unknownDue.Member).Result(); !errors.Is(err, redis.Nil) {
			t.Fatal("unknown output still indexed", err)
		}
		unknownOwner, err := store.ObserveContext(ctx, "udp-forget", "incarnation", "r:context", "r:dialog-"+cleanupInput.RequestID, cleanupDialogRecord.Outputs[0])
		if err != nil {
			t.Fatal(err)
		}
		unknownContext, err := decodeNativeContext(unknownOwner.View.Records["r:context"])
		if err != nil || !slices.Contains(unknownContext.NoninitialProcedures, cleanupContext.NoninitialProcedures[0]) {
			t.Fatal("unknown procedure is unreachable", err)
		}
		unknownDialog, err := readPrivateDialogRecord(unknownOwner.View.Records["r:dialog-"+cleanupInput.RequestID])
		if err != nil || !slices.Contains(unknownDialog.Outputs, cleanupDialogRecord.Outputs[0]) || len(unknownOwner.View.Records[cleanupDialogRecord.Outputs[0]]) == 0 {
			t.Fatal("unknown output is unreachable", err)
		}
		activeRetire := *unknownDue
		activeRetire.Revision = unknownOwner.View.Revision
		activeRetire.Member = "call-retire"
		if forgotten, e := forgetRedisEndedCleanupBye(ctx, store, &activeRetire, time.Now().Add(time.Second)); e == nil || forgotten {
			t.Fatal("active call allowed uncertain proof reclamation", e)
		}
		if _, e := store.InspectOutput(ctx, unknownRef); e != nil {
			t.Fatal("active refusal changed proof", e)
		}
		// Advance the already-qualified native graph to its fully ended cutoff;
		// the actual U proof and its ownership are kept unchanged until Forget.
		graph, e := store.ObserveContext(ctx, "udp-forget", "incarnation", "r:context", "r:procedure-"+cleanupInput.RequestID, "r:dialog-"+cleanupInput.RequestID, cleanupContext.NoninitialProcedures[0], cleanupDialogRecord.Outputs[0])
		if e != nil {
			t.Fatal(e)
		}
		graphContext, e := decodeNativeContext(graph.View.Records["r:context"])
		if e != nil {
			t.Fatal(e)
		}
		anchor, e := readPrivateProcedureRecord(graph.View.Records["r:procedure-"+cleanupInput.RequestID])
		if e != nil {
			t.Fatal(e)
		}
		dialog, e := readPrivateDialogRecord(graph.View.Records["r:dialog-"+cleanupInput.RequestID])
		if e != nil {
			t.Fatal(e)
		}
		proc, e := readPrivateProcedureRecord(graph.View.Records[cleanupContext.NoninitialProcedures[0]])
		if e != nil {
			t.Fatal(e)
		}
		graphNow, e := client.Time(ctx).Result()
		if e != nil {
			t.Fatal(e)
		}
		graphContext.Ended = true
		graphContext.CallRetireAtMs = graphNow.Add(-time.Second).UnixMilli()
		anchor.Native.Phase = string(portable.Terminated)
		if anchor.Native.Timers != nil {
			for _, timer := range anchor.Native.Timers.Values {
				timer.Active = false
			}
		}
		dialog.Native.Phase = string(portable.DialogEnded)
		if dialog.Native.Timers != nil {
			for _, timer := range dialog.Native.Timers.Values {
				timer.Active = false
			}
		}
		proc.Native.Phase = string(portable.Terminated)
		if proc.Native.Timers != nil {
			for _, timer := range proc.Native.Timers.Values {
				timer.Active = false
			}
		}
		contextBytes, e := encodeNativeContext(graphContext)
		if e != nil {
			t.Fatal(e)
		}
		anchorBytes, e := marshalNativeRecord(storagecodec.NativeProcedure, anchor)
		if e != nil {
			t.Fatal(e)
		}
		dialogBytes, e := marshalNativeRecord(storagecodec.NativeDialog, dialog)
		if e != nil {
			t.Fatal(e)
		}
		procBytes, e := marshalNativeRecord(storagecodec.NativeProcedure, proc)
		if e != nil {
			t.Fatal(e)
		}
		physicalPast := graphNow.Add(-rediscore.OutputObsoleteGrace - time.Second).UnixMilli()
		cleanupOutput.Write.Permit.Authorization.DeadlineMs = uint64(physicalPast)
		outputBytes, e := encodeNativeOutput(cleanupOutput)
		if e != nil {
			t.Fatal(e)
		}
		proofHash, e := nativeFlowWriteHash(cleanupOutput.Write)
		if e != nil {
			t.Fatal(e)
		}
		prefix := "o:" + cleanupOutput.Write.Effect.Id + ":"
		if e = client.HSet(ctx, root+"core:context:udp-forget", cleanupDialogRecord.Outputs[0], outputBytes, prefix+"p", proofHash[:], prefix+"d", physicalPast).Err(); e != nil {
			t.Fatal(e)
		}
		procID := strings.TrimPrefix(cleanupContext.NoninitialProcedures[0], "r:call-cleanup-")
		cutoff := max(graphContext.CallRetireAtMs, physicalPast+rediscore.OutputObsoleteGrace.Milliseconds())
		_, e = store.Commit(ctx, rediscore.Publication{Identity: graph.Identity, Revision: graph.View.Revision, Deadline: time.Now().Add(time.Second), Changes: []rediscore.Change{{Field: "r:context", Value: contextBytes}, {Field: "r:procedure-" + cleanupInput.RequestID, Value: anchorBytes}, {Field: "r:dialog-" + cleanupInput.RequestID, Value: dialogBytes}, {Field: cleanupContext.NoninitialProcedures[0], Value: procBytes}}, Timers: &rediscore.TimerMutation{Expected: graph.View.Timer, Target: graph.View.Timer, RequireLease: true, Changes: []rediscore.TimerChange{{Member: "call-tx-" + cleanupInput.RequestID + "-L", Delete: true}, {Member: "call-client-" + procID + "-F", Delete: true}, {Member: "call-retire", Due: time.UnixMilli(cutoff)}}}})
		if e != nil {
			t.Fatal(e)
		}
		dueView, e := store.ObserveContext(ctx, "udp-forget", "incarnation", "r:context")
		if e != nil {
			t.Fatal(e)
		}
		checked, e := client.Time(ctx).Result()
		if e != nil {
			t.Fatal(e)
		}
		terminal := &rediscore.DueContext{Identity: dueView.Identity, Revision: dueView.View.Revision, Assignment: dueView.View.Timer, Member: "call-retire", DeadlineMS: cutoff, CheckedAtMS: checked.UnixMilli()}
		_, e = store.Commit(ctx, rediscore.Publication{Identity: terminal.Identity, Revision: terminal.Revision, Deadline: time.Now().Add(time.Second), Timers: &rediscore.TimerMutation{Expected: terminal.Assignment, Target: terminal.Assignment, RequireLease: true, Changes: []rediscore.TimerChange{{Member: "fixture-duty", Due: time.UnixMilli(cutoff)}}}})
		if e != nil {
			t.Fatal(e)
		}
		terminal.Revision++
		yielded, e := yieldRedisCallRetirement(ctx, store, terminal, time.Now().Add(time.Second))
		if e != nil || !yielded {
			t.Fatal("retirement did not yield", e)
		}
		yieldedScore, e := client.ZScore(ctx, root+"core:deadlines:udp-forget", "call-retire").Result()
		if e != nil || int64(yieldedScore) != max(cutoff, terminal.CheckedAtMS)+1 {
			t.Fatal("retirement did not yield strictly after actual duty", yieldedScore, e)
		}
		afterYield, e := store.ObserveContext(ctx, "udp-forget", "incarnation", "r:context")
		if e != nil {
			t.Fatal(e)
		}
		afterYieldContext, e := decodeNativeContext(afterYield.View.Records["r:context"])
		if e != nil || afterYieldContext.CallRetireAtMs != graphContext.CallRetireAtMs {
			t.Fatal("yield moved immutable cutoff", e)
		}
		_, e = store.Commit(ctx, rediscore.Publication{Identity: afterYield.Identity, Revision: afterYield.View.Revision, Deadline: time.Now().Add(time.Second), Timers: &rediscore.TimerMutation{Expected: afterYield.View.Timer, Target: afterYield.View.Timer, RequireLease: true, Changes: []rediscore.TimerChange{{Member: "fixture-duty", Delete: true}, {Member: "call-retire", Due: time.UnixMilli(cutoff)}}}})
		if e != nil {
			t.Fatal(e)
		}
		dueView, e = store.ObserveContext(ctx, "udp-forget", "incarnation", "r:context")
		if e != nil {
			t.Fatal(e)
		}
		terminal.Identity, terminal.Revision, terminal.Assignment = dueView.Identity, dueView.View.Revision, dueView.View.Timer
		reconcileMulti := func(duty *rediscore.DueContext) (bool, error) {
			limit := time.Now().Add(time.Second)
			yielded, e := yieldRedisCallRetirement(ctx, store, duty, limit)
			if e != nil || yielded {
				return false, e
			}
			selected, e := store.Read(ctx, duty.Identity, "r:context")
			if e != nil {
				return false, e
			}
			if selected.Revision != duty.Revision || selected.Timer != duty.Assignment {
				return false, rediscore.ErrConflict
			}
			current, e := decodeNativeContext(selected.Records["r:context"])
			if e != nil {
				return false, e
			}
			owners, e := selectRedisTerminalOwners(ctx, store, duty, current, limit)
			if e != nil {
				return false, e
			}
			if owners == nil {
				return false, errNativeRecord
			}
			_, e = reconcileRedisTerminalOutput(ctx, store, duty, owners, limit)
			return false, e
		}
		resultField := "r:result-1"
		resultView, e := store.Read(ctx, terminal.Identity, resultField)
		if e != nil {
			t.Fatal(e)
		}
		retainedResult, e := decodeNativeResult(resultView.Records[resultField])
		if e != nil {
			t.Fatal(e)
		}
		retentionUntil := time.Now().Add(200 * time.Millisecond).UnixMilli()
		retainedResult.CutoffMs = retentionUntil
		resultBytes, e := encodeNativeResult(retainedResult)
		if e != nil {
			t.Fatal(e)
		}
		_, e = store.Commit(ctx, rediscore.Publication{Identity: terminal.Identity, Revision: resultView.Revision, Deadline: time.Now().Add(time.Second), Changes: []rediscore.Change{{Field: resultField, Value: resultBytes}}})
		if e != nil {
			t.Fatal(e)
		}
		terminal.Revision++
		if finished, e := reconcileMulti(terminal); e != nil || finished {
			t.Fatal("waiter retention did not yield", e)
		}
		retentionScore, e := client.ZScore(ctx, root+"core:deadlines:udp-forget", "call-retire").Result()
		if e != nil || int64(retentionScore) != retentionUntil {
			t.Fatal("waiter retention cutoff not indexed", retentionScore, e)
		}
		if wait := time.Until(time.UnixMilli(retentionUntil)); wait > 0 {
			time.Sleep(wait + 15*time.Millisecond)
		}
		dueView, e = store.ObserveContext(ctx, "udp-forget", "incarnation", "r:context")
		if e != nil {
			t.Fatal(e)
		}
		checked = waitRedisFixtureTime(t, ctx, client, time.UnixMilli(retentionUntil))
		terminal.Identity, terminal.Revision, terminal.Assignment, terminal.DeadlineMS, terminal.CheckedAtMS = dueView.Identity, dueView.View.Revision, dueView.View.Timer, retentionUntil, checked.UnixMilli()
		if finished, e := reconcileMulti(terminal); e != nil || finished {
			t.Fatal("obsolete cutoff normalization failed", e)
		}
		normalized, e := client.ZScore(ctx, root+"core:deadlines:udp-forget", "call-retire").Result()
		if e != nil || int64(normalized) != cutoff {
			t.Fatal("immutable obsolete cutoff not restored", normalized, e)
		}
		dueView, e = store.ObserveContext(ctx, "udp-forget", "incarnation", "r:context")
		if e != nil {
			t.Fatal(e)
		}
		terminal.Identity, terminal.Revision, terminal.Assignment, terminal.DeadlineMS = dueView.Identity, dueView.View.Revision, dueView.View.Timer, cutoff
		ref, e := nativeOutputRef(cleanupOutput.Write)
		if e != nil {
			t.Fatal(e)
		}
		finished, e := reconcileMulti(terminal)
		if e != nil || finished {
			t.Fatal("forget exact ended U", e)
		}
		if _, e = store.InspectOutput(ctx, ref); !errors.Is(e, rediscore.ErrAbsent) {
			t.Fatal("ended U proof retained", e)
		}
		finished, e = reconcileMulti(terminal)
		if e != nil || finished {
			t.Fatal("reconcile forgotten proof", e)
		}
		retired, e := store.ObserveContext(ctx, "udp-forget", "incarnation", "r:context", cleanupContext.NoninitialProcedures[0], cleanupDialogRecord.LocalBye)
		if e != nil {
			t.Fatal(e)
		}
		retiredContext, e := decodeNativeContext(retired.View.Records["r:context"])
		if e != nil || len(retiredContext.NoninitialProcedures) != 0 || len(retired.View.Records[cleanupContext.NoninitialProcedures[0]]) != 0 || len(retired.View.Records[cleanupDialogRecord.LocalBye]) == 0 {
			t.Fatal("ended cleanup retirement lost ownership", e)
		}
	}

}

// Test-only bounded observation; native due guards must use Redis TIME even
// when the host clock leads it. Production scheduling never enters this loop.
func waitRedisFixtureTime(t *testing.T, ctx context.Context, client *redis.Client, deadline time.Time) time.Time {
	t.Helper()
	limit := time.Now().Add(time.Second)
	for {
		observed, err := client.Time(ctx).Result()
		if err != nil {
			t.Fatal(err)
		}
		if !observed.Before(deadline) {
			return observed
		}
		if !time.Now().Before(limit) {
			t.Fatalf("Redis fixture deadline not due: observed=%s deadline=%s", observed, deadline)
		}
		time.Sleep(5 * time.Millisecond)
	}
}
