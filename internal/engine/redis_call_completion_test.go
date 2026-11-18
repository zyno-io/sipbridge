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

func TestSelectedCallCompletionRemoteByeReplacementFlow(t *testing.T) {
	dialog := nativeEstablishedDialogFixture(t)
	refs := nativeDialogRefs{Local: &nativeInviteRefs{Request: "r:invite", Final: "r:final", ACK: "r:ack"}}
	data, err := encodeNativeDialog(dialog, refs, &nativepb.Dialog{Transaction: "initial"})
	if err != nil {
		t.Fatal(err)
	}
	record, err := decodeNativeDialog(data)
	if err != nil {
		t.Fatal(err)
	}
	messages := nativeTestMessages(t, map[string]portable.Message{"r:invite": dialog.LocalInvite.Request, "r:final": *dialog.LocalInvite.Final, "r:ack": *dialog.LocalInvite.ACK})
	worker := &pb.ProcessRef{InstanceId: "new-worker", Epoch: "new-epoch"}
	oldFlow := &pb.FlowRef{Ingress: &pb.ProcessRef{InstanceId: "old-worker", Epoch: "old-epoch"}, ConnectionId: "old", SourceIp: "192.0.2.2", SourcePort: 5061, Listener: "sip.example.test:5061", Transport: "tls"}
	flow := &pb.FlowRef{Ingress: worker, ConnectionId: "new", SourceIp: "192.0.2.3", SourcePort: 5061, Listener: "sip.example.test:5061", Transport: "tls"}
	current := &nativepb.Context{Context: &pb.ContextRef{Id: "context", State: &pb.StateRef{Incarnation: "incarnation"}}, CallId: dialog.CallID, LocalTag: dialog.LocalTag, Transport: "tls", Flow: oldFlow}
	original := nativePeerBye(t, dialog, 12)
	incoming := portable.RestoreMessage(original.WireBytes(), "tls", "192.0.2.3:5061", "")
	candidate, err := prepareSelectedCallCompletion(current, record, nil, messages, worker, flow, incoming, 2000)
	if err != nil {
		t.Fatal(err)
	}
	if candidate.Dialog.Phase != portable.DialogEnded || candidate.Transaction.Family != portable.NonInviteServer || candidate.Transaction.LastResponse == nil {
		t.Fatal("replacement-flow BYE did not produce native end and response")
	}
	if current.Ended || dialog.Phase == portable.DialogEnded {
		t.Fatal("candidate mutated selected authority")
	}
	foreign := proto.Clone(flow).(*pb.FlowRef)
	foreign.Transport = "tcp"
	if _, err = prepareSelectedCallCompletion(current, record, nil, messages, worker, foreign, incoming, 2000); err == nil {
		t.Fatal("foreign transport accepted")
	}
	if _, err = prepareSelectedCallCompletion(current, record, nil, messages, &pb.ProcessRef{InstanceId: "other", Epoch: "other"}, flow, incoming, 2000); err == nil {
		t.Fatal("foreign ingress accepted")
	}
}

func TestSelectedCallCompletionDialogACK(t *testing.T) {
	invite, err := portable.ParseMessage(stateTestRequest("INVITE", 1, "", "selected-ack").Wire, "tls", "192.0.2.2:5061", "")
	if err != nil {
		t.Fatal(err)
	}
	final, err := portable.BuildResponseWithHeaders(invite, 200, "OK", nil, "local-tag", "sip:server@example.test;transport=tls", nil)
	if err != nil {
		t.Fatal(err)
	}
	dialog, err := portable.NewDialog(portable.UAS, invite, final, portable.DefaultTimers())
	if err != nil {
		t.Fatal(err)
	}
	dialog, _, err = portable.StepDialog(dialog, portable.DialogInput{Kind: portable.DialogSendResponse, Message: final, Now: 1000})
	if err != nil {
		t.Fatal(err)
	}
	peer, err := portable.NewDialog(portable.UAC, invite, final, portable.DefaultTimers())
	if err != nil {
		t.Fatal(err)
	}
	ack, err := portable.BuildDialogRequest(peer, sip.ACK, 1, nil, "z9hG4bK-selected-ack")
	if err != nil {
		t.Fatal(err)
	}
	ack = portable.RestoreMessage(ack.WireBytes(), "tls", "192.0.2.3:5061", "")
	worker := &pb.ProcessRef{InstanceId: "worker", Epoch: "epoch"}
	oldFlow := &pb.FlowRef{Ingress: worker, ConnectionId: "old", SourceIp: "192.0.2.2", SourcePort: 5061, Listener: "sip.example.test:5061", Transport: "tls"}
	flow := &pb.FlowRef{Ingress: worker, ConnectionId: "new", SourceIp: "192.0.2.3", SourcePort: 5061, Listener: "sip.example.test:5061", Transport: "tls"}
	tx, _, err := nativeServerStart(invite, "local-tag", 1000)
	if err != nil {
		t.Fatal(err)
	}
	tx, _, err = nativeServerSelectResponse(tx, final, 1001)
	if err != nil {
		t.Fatal(err)
	}
	refs := nativeDialogRefs{Remote: &nativeInviteRefs{Request: "r:invite", Final: "r:final"}}
	data, err := encodeNativeDialog(dialog, refs, &nativepb.Dialog{Transaction: tx.Key})
	if err != nil {
		t.Fatal(err)
	}
	record, err := decodeNativeDialog(data)
	if err != nil {
		t.Fatal(err)
	}
	txData, err := encodeNativeProcedure(tx, nativeProcedureRefs{Origin: "r:invite", LastResponse: "r:final"}, &nativepb.Procedure{Flow: oldFlow})
	if err != nil {
		t.Fatal(err)
	}
	selectedTx, err := decodeNativeProcedure(txData)
	if err != nil {
		t.Fatal(err)
	}
	messages := nativeTestMessages(t, map[string]portable.Message{"r:invite": invite, "r:final": final})
	current := &nativepb.Context{Context: &pb.ContextRef{Id: "context", State: &pb.StateRef{Incarnation: "incarnation"}}, CallId: dialog.CallID, LocalTag: dialog.LocalTag, Transport: "tls", Flow: oldFlow}
	candidate, err := prepareSelectedCallCompletion(current, record, &selectedTx, messages, worker, flow, ack, 2000)
	if err != nil {
		t.Fatal(err)
	}
	if candidate.TransactionACK || candidate.Dialog.RemoteInvite.State != portable.InviteAcked {
		t.Fatal("2xx ACK did not acknowledge dialog")
	}
	seen := false
	for _, effect := range candidate.DialogEffects {
		seen = seen || effect.Kind == portable.Acknowledged
	}
	if !seen {
		t.Fatal("ACKNOWLEDGED effect missing")
	}
	selectedTx.Native.Family = portable.NonInviteServer
	if _, err = prepareSelectedCallCompletion(current, record, &selectedTx, messages, worker, flow, ack, 2000); err == nil {
		t.Fatal("foreign transaction accepted")
	}
}

func TestSelectedCallCompletionNon2xxACKUsesTransaction(t *testing.T) {
	raw := stateTestRequest("INVITE", 1, "", "selected-reject")
	invite, err := portable.ParseMessage(raw.Wire, "tls", "192.0.2.2:5061", "")
	if err != nil {
		t.Fatal(err)
	}
	final, err := portable.BuildResponseWithHeaders(invite, 486, "Busy Here", nil, "local-tag", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	tx, _, err := nativeServerStart(invite, "local-tag", 1000)
	if err != nil {
		t.Fatal(err)
	}
	tx, _, err = nativeServerSelectResponse(tx, final, 1001)
	if err != nil {
		t.Fatal(err)
	}
	ackWire := strings.Replace(string(raw.Wire), "INVITE sip:", "ACK sip:", 1)
	ackWire = strings.Replace(ackWire, "CSeq: 1 INVITE", "CSeq: 1 ACK", 1)
	ackWire = strings.Replace(ackWire, "To: <sip:service@example.test>", "To: <sip:service@example.test>;tag=local-tag", 1)
	ack, err := portable.ParseMessage([]byte(ackWire), "tls", "192.0.2.2:5061", "")
	if err != nil {
		t.Fatal(err)
	}
	worker := &pb.ProcessRef{InstanceId: "worker", Epoch: "epoch"}
	flow := &pb.FlowRef{Ingress: worker, ConnectionId: "phone", SourceIp: "192.0.2.2", SourcePort: 5061, Listener: "sip.example.test:5061", Transport: "tls"}
	txData, err := encodeNativeProcedure(tx, nativeProcedureRefs{Origin: "r:invite", LastResponse: "r:final"}, &nativepb.Procedure{Flow: flow})
	if err != nil {
		t.Fatal(err)
	}
	selected, err := decodeNativeProcedure(txData)
	if err != nil {
		t.Fatal(err)
	}
	messages := nativeTestMessages(t, map[string]portable.Message{"r:invite": invite, "r:final": final})
	facts, err := invite.Facts()
	if err != nil {
		t.Fatal(err)
	}
	current := &nativepb.Context{Context: &pb.ContextRef{Id: "context", State: &pb.StateRef{Incarnation: "inc"}}, CallId: facts.CallID, Transport: "tls", Flow: flow}
	candidate, err := prepareSelectedCallCompletion(current, nativeDialogRecord{}, &selected, messages, worker, flow, ack, 2000)
	if err != nil {
		t.Fatal(err)
	}
	if !candidate.TransactionACK || candidate.Transaction.Phase != portable.Terminated {
		t.Fatal("non-2xx ACK did not complete its transaction")
	}
}

func TestSelectedRedisCallDialogReplacementFlow(t *testing.T) {
	t.Run("written-unknown-and-j", func(t *testing.T) { testSelectedRedisCallDialogReplacementFlow(t, false) })
	t.Run("pending-expires-not-written", func(t *testing.T) { testSelectedRedisCallDialogReplacementFlow(t, true) })
}

func testSelectedRedisCallDialogReplacementFlow(t *testing.T, pendingOnly bool) {
	address := os.Getenv("SIPBRIDGE_CORE_TEST_REDIS")
	if address == "" {
		t.Skip("isolated Redis fixture not configured")
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
	root := "sipstate:test:core-" + uuid.NewString() + ":"
	profile := []byte{2, 3, 4}
	physicalWorker, err := nativeWorkerReference(&pb.ProcessRef{InstanceId: "worker", Epoch: "epoch"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		for _, key := range []string{"core:profile", "core:capacity", "core:selectors", "core:context:call", "core:deadlines:call", "core:context:udp-call", "core:deadlines:udp-call", "core:context:udp-uas", "core:deadlines:udp-uas", "core:context:udp-rejected", "core:deadlines:udp-rejected", "core:context:terminal-call", "core:deadlines:terminal-call", "core:worker:terminal-worker", "core:timers:terminal-worker", "core:ready:worker", "core:context:rejected", "core:context:uas", "core:deadlines:uas", "core:worker:worker", "core:timers:worker", "core:worker:uas-worker", "core:timers:uas-worker"} {
			if e := client.Del(ctx, root+key).Err(); e != nil {
				t.Error(e)
			}
		}
		if e := client.Del(ctx, root+"core:ready:"+physicalWorker).Err(); e != nil {
			t.Error(e)
		}
		if e := client.Del(ctx, root+"core:worker:"+physicalWorker).Err(); e != nil {
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
	dialog := nativeEstablishedDialogFixture(t)
	refs := nativeDialogRefs{Local: &nativeInviteRefs{Request: "r:invite", Final: "r:final", ACK: "r:ack"}}
	dialogData, err := encodeNativeDialog(dialog, refs, &nativepb.Dialog{Transaction: "initial"})
	if err != nil {
		t.Fatal(err)
	}
	worker := &pb.ProcessRef{InstanceId: "worker", Epoch: "epoch"}
	old := &pb.FlowRef{Ingress: &pb.ProcessRef{InstanceId: "old", Epoch: "old"}, ConnectionId: "old", SourceIp: "192.0.2.2", SourcePort: 5061, Listener: "sip.example.test:5061", Transport: "tls"}
	flow := &pb.FlowRef{Ingress: worker, ConnectionId: "new", SourceIp: "192.0.2.3", SourcePort: 5061, Listener: "sip.example.test:5061", Transport: "tls"}
	authority := &pb.StateAuthority{Service: "service", Generation: "generation", PrimaryId: "primary", SchemaVersion: 1}
	current := &nativepb.Context{Context: &pb.ContextRef{Id: "call", Owner: worker, State: &pb.StateRef{Authority: authority, GroupId: "call", Incarnation: "inc", CallId: dialog.CallID}}, Application: &pb.ProcessRef{InstanceId: "application", Epoch: "epoch"}, Kind: pb.ContextSnapshot_UAC, CallId: dialog.CallID, LocalTag: dialog.LocalTag, RemoteTag: dialog.RemoteTag, Transport: "tls", Flow: old, Dialogs: []string{"r:dialog"}}
	contextData, err := encodeNativeContext(current)
	if err != nil {
		t.Fatal(err)
	}
	request := nativePeerBye(t, dialog, 12)
	message := portable.RestoreMessage(request.WireBytes(), "tls", "192.0.2.3:5061", "")
	selectors, err := nativeSelectorIDs(message, false, false)
	if err != nil {
		t.Fatal(err)
	}
	var dialogSelector rediscore.Selector
	for _, selector := range selectors {
		if selector.Kind == rediscore.SelectorDialog {
			dialogSelector = selector
		}
	}
	changes := []rediscore.Change{{Field: "r:context", Value: contextData}, {Field: "r:dialog", Value: dialogData}}
	for field, msg := range map[string]portable.Message{"r:invite": dialog.LocalInvite.Request, "r:final": *dialog.LocalInvite.Final, "r:ack": *dialog.LocalInvite.ACK} {
		wire, e := selectedmessage.Marshal(msg)
		if e != nil {
			t.Fatal(e)
		}
		changes = append(changes, rediscore.Change{Field: field, Value: wire})
	}
	id := rediscore.Identity{Context: "call", Incarnation: "inc"}
	_, err = store.Commit(ctx, rediscore.Publication{Identity: id, Create: true, Deadline: time.Now().Add(time.Second), CreditDelta: 1 << 20, Changes: changes, Selectors: []rediscore.SelectorChange{{Selector: dialogSelector, Record: "r:dialog"}}})
	if err != nil {
		t.Fatal(err)
	}
	selected, err := selectRedisCallDialog(ctx, store, worker, flow, message, time.Now().Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if selected.Identity != id || selected.Revision != 1 || selected.Candidate.Dialog.Phase != portable.DialogEnded || selected.Candidate.Transaction.LastResponse == nil {
		t.Fatal("selected replacement BYE candidate incomplete")
	}
	if _, err = store.RenewWorker(ctx, rediscore.WorkerLeaseRequest{Worker: "worker", Epoch: "epoch", PrimaryRunID: primary, Acquire: true}); err != nil {
		t.Fatal(err)
	}
	if _, err = store.RenewWorker(ctx, rediscore.WorkerLeaseRequest{Worker: physicalWorker, Epoch: "epoch", PrimaryRunID: primary, Acquire: true}); err != nil {
		t.Fatal(err)
	}
	assignment := rediscore.TimerAssignment{Worker: "worker", Epoch: "epoch", Term: "epoch"}
	wrongSocket := proto.Clone(flow).(*pb.FlowRef)
	wrongSocket.ConnectionId = "wrong-socket"
	if err = publishRedisCallRemoteBye(ctx, store, selected, wrongSocket, bytes.Repeat([]byte{1}, 32), assignment, time.Now().Add(time.Second)); err == nil {
		t.Fatal("BYE publication accepted a different selected flow")
	}
	badSelected := *selected
	badSelected.Context = proto.Clone(selected.Context).(*nativepb.Context)
	badSelected.Context.Kind = pb.ContextSnapshot_UNKNOWN
	if err = publishRedisCallRemoteBye(ctx, store, &badSelected, flow, bytes.Repeat([]byte{1}, 32), assignment, time.Now().Add(time.Second)); err == nil {
		t.Fatal("BYE publication accepted an undeliverable context kind")
	}
	constrained, err := rediscore.New(client, rediscore.Config{Root: root, Profile: profile, PrimaryRunID: primary, RecordBudget: 4 << 20, NamespaceMaxBytes: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err = publishRedisCallRemoteBye(ctx, constrained, selected, flow, bytes.Repeat([]byte{1}, 32), assignment, time.Now().Add(time.Second)); err == nil {
		t.Fatal("capacity refusal published BYE")
	}
	unchanged, err := store.ObserveContext(ctx, "call", "inc", "r:context", "r:dialog")
	if err != nil || unchanged.View.Revision != selected.Revision {
		t.Fatalf("capacity refusal changed BYE state: %v", err)
	}
	if err = publishRedisCallRemoteBye(ctx, store, selected, flow, bytes.Repeat([]byte{1}, 32), assignment, time.Now().Add(time.Second)); err != nil {
		t.Fatalf("publish replacement BYE: %v", err)
	}
	if err = publishRedisCallRemoteBye(ctx, store, selected, flow, bytes.Repeat([]byte{1}, 32), assignment, time.Now().Add(time.Second)); !errors.Is(err, rediscore.ErrConflict) {
		t.Fatalf("stale BYE publication: %v", err)
	}
	byeAfter, err := store.ObserveContext(ctx, "call", "inc", "r:context", "r:dialog")
	if err != nil {
		t.Fatal(err)
	}
	ended, err := decodeNativeContext(byeAfter.View.Records["r:context"])
	if err != nil || !ended.Ended || ended.CallRetireAtMs <= 0 {
		t.Fatalf("missing atomic end and cutoff: %v", err)
	}
	endedDialog, err := decodeNativeDialog(byeAfter.View.Records["r:dialog"])
	if err != nil || len(endedDialog.Metadata.Outputs) != 1 || !strings.HasPrefix(endedDialog.Metadata.Outputs[0], "r:call-bye-output-") {
		t.Fatalf("BYE output not atomically linked to dialog: %v", err)
	}
	byeSelector := selectors[0]
	byeProcedure, err := store.LookupSelector(ctx, byeSelector)
	if err != nil || byeProcedure.Identity != id {
		t.Fatalf("missing BYE procedure: %v", err)
	}
	if len(ended.NoninitialProcedures) != 1 || ended.NoninitialProcedures[0] != byeProcedure.Record {
		t.Fatal("BYE procedure not atomically linked to context")
	}
	// Exercise the ownership fence independently of waiting two minutes; the
	// real timer owner supplies an authoritative due observation in production.
	ownershipDue := &rediscore.DueContext{Identity: id, Revision: byeAfter.View.Revision, Assignment: byeAfter.View.Timer, Member: "call-retire", DeadlineMS: ended.CallRetireAtMs, CheckedAtMS: ended.CallRetireAtMs, Observed: time.Now()}
	if finished, e := retireRedisQuiescentCall(ctx, store, ownershipDue, time.Now().Add(time.Second)); e != nil || finished {
		t.Fatalf("live noninitial BYE did not fence call retirement: %v", e)
	}
	ownershipAfter, e := store.ObserveContext(ctx, "call", "inc", "r:context")
	if e != nil || ownershipAfter.View.Revision != byeAfter.View.Revision {
		t.Fatalf("ownership fence mutated state: %v", e)
	}
	byeDue, err := waitCallTestDue(ctx, store, "worker", "epoch", time.Second)
	if err != nil || byeDue == nil || !strings.HasPrefix(byeDue.Member, "event-") {
		t.Fatalf("missing BYE lifecycle duty: %v", err)
	}
	ready, err := nativeReadyWrites(ctx, store, physicalWorker, "epoch", 1)
	if err != nil || len(ready) != 1 {
		t.Fatalf("missing BYE output: %v", err)
	}
	if pendingOnly {
		// Move only the unrelated lifecycle head so the actual original output
		// deadline is the next worker duty in this isolated fixture.
		currentView, e := store.ObserveContext(ctx, id.Context, id.Incarnation, "r:context")
		if e != nil {
			t.Fatal(e)
		}
		_, e = store.Commit(ctx, rediscore.Publication{Identity: id, Revision: currentView.View.Revision, Deadline: time.Now().Add(time.Second), SpendReservation: true, Timers: &rediscore.TimerMutation{Expected: currentView.View.Timer, Target: currentView.View.Timer, Changes: []rediscore.TimerChange{{Member: byeDue.Member, Due: time.Now().Add(time.Minute)}}}})
		if e != nil {
			t.Fatal(e)
		}
		original := time.UnixMilli(int64(ready[0].Permit.Authorization.DeadlineMs))
		for time.Now().Before(original.Add(20 * time.Millisecond)) {
			for _, workerID := range []string{"worker", physicalWorker} {
				if _, e = store.RenewWorker(ctx, rediscore.WorkerLeaseRequest{Worker: workerID, Epoch: "epoch", PrimaryRunID: primary}); e != nil {
					t.Fatal(e)
				}
			}
			time.Sleep(50 * time.Millisecond)
		}
		outDue, e := waitCallTestDue(ctx, store, "worker", "epoch", time.Second)
		if e != nil || outDue == nil || !strings.HasPrefix(outDue.Member, "out-call-bye-output-") {
			t.Fatal("pending BYE cutoff not due", e)
		}
		if e = (&redisNative{}).due(ctx, store, outDue); e != nil {
			t.Fatal("pending BYE cutoff", e)
		}
		after, e := store.Read(ctx, id, byeProcedure.Record)
		if e != nil {
			t.Fatal(e)
		}
		proc, e := readPrivateProcedureRecord(after.Records[byeProcedure.Record])
		if e != nil || !proc.OutputConsumed || proc.FinalResponseWrittenAcknowledged {
			t.Fatal("pending expiry fabricated write or lost consumption", e)
		}
		ref, e := nativeOutputRef(ready[0])
		if e != nil {
			t.Fatal(e)
		}
		if _, e = store.InspectOutput(ctx, ref); !errors.Is(e, rediscore.ErrAbsent) {
			t.Fatal("definite not-written proof not consumed", e)
		}
		return
	}
	reserved, err := (nativeRedisWriter{store: store}).Reserve(ctx, ready[0])
	if err != nil || reserved == nil || reserved.Known || len(reserved.Wire) == 0 {
		t.Fatalf("reserve BYE response: %v", err)
	}
	physical, err := portable.ParseMessage(reserved.Wire, "tls", "", "192.0.2.3:5061")
	if err != nil {
		t.Fatal(err)
	}
	physicalFacts, err := physical.Facts()
	if err != nil || physicalFacts.IsRequest || physicalFacts.Status != 200 || physicalFacts.CSeqMethod != sip.BYE {
		t.Fatalf("reserved BYE response identity: %v", err)
	}
	if err = completeRedisCallByeOutput(ctx, store, ready[0], &pb.DeliveryResult{Effect: ready[0].Effect, Outcome: pb.DeliveryResult_WRITTEN}); err != nil {
		t.Fatal(err)
	}
	if err = acknowledgeRedisCallByeOutput(ctx, store, ready[0], time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	byeID := strings.TrimPrefix(byeProcedure.Record, "r:call-bye-")
	byeOutMember := "out-call-bye-output-" + byeID
	byeCurrent, err := store.ObserveContext(ctx, "call", "inc", "r:context")
	if err != nil {
		t.Fatal(err)
	}
	serverNow, err := client.Time(ctx).Result()
	if err != nil {
		t.Fatal(err)
	}
	cutoff := int64(ready[0].Permit.Authorization.DeadlineMs)
	_, err = store.Commit(ctx, rediscore.Publication{Identity: byeCurrent.Identity, Revision: byeCurrent.View.Revision, Deadline: time.Now().Add(time.Second), SpendReservation: true, Timers: &rediscore.TimerMutation{Expected: byeCurrent.View.Timer, Target: byeCurrent.View.Timer, Changes: []rediscore.TimerChange{{Member: byeOutMember, Due: time.UnixMilli(cutoff)}}}})
	if err != nil {
		t.Fatal(err)
	}
	byeCurrent, err = store.ObserveContext(ctx, "call", "inc", "r:context")
	if err != nil {
		t.Fatal(err)
	}
	byeRetire := &rediscore.DueContext{Identity: byeCurrent.Identity, Revision: byeCurrent.View.Revision, Assignment: byeCurrent.View.Timer, Member: byeOutMember, DeadlineMS: cutoff, CheckedAtMS: max(serverNow.UnixMilli(), cutoff), Observed: time.Now()}
	if err = (&redisNative{}).due(ctx, store, byeRetire); err != nil {
		t.Fatalf("known BYE cleanup: %v", err)
	}
	byeClean, err := store.Read(ctx, id, "r:context", "r:dialog", byeProcedure.Record, "r:call-bye-output-"+byeID)
	if err != nil {
		t.Fatal(err)
	}
	cleanDialog, err := decodeNativeDialog(byeClean.Records["r:dialog"])
	if err != nil || len(cleanDialog.Metadata.Outputs) != 0 || len(byeClean.Records[byeProcedure.Record]) != 0 || len(byeClean.Records["r:call-bye-output-"+byeID]) != 0 {
		t.Fatalf("known BYE cleanup left records: %v", err)
	}
	cleanContext, err := decodeNativeContext(byeClean.Records["r:context"])
	if err != nil || len(cleanContext.NoninitialProcedures) != 0 {
		t.Fatalf("known BYE procedure link remained: %v", err)
	}

	udpInvite, err := portable.BuildInitialRequest(portable.InitialRequest{Method: sip.INVITE, URI: "sip:phone@example.test", From: "<sip:controller@example.test>", CallID: "udp-call", LocalTag: "local", Branch: "z9hG4bK-udp-invite", CSeq: 1, Advertise: "192.0.2.1:5060", Contact: "<sip:controller@192.0.2.1:5060;transport=udp>", Transport: "udp"})
	if err != nil {
		t.Fatal(err)
	}
	udpFinal, err := portable.BuildResponseWithHeaders(udpInvite, 200, "OK", nil, "remote", "sip:phone@192.0.2.3:5060;transport=udp", nil)
	if err != nil {
		t.Fatal(err)
	}
	udpDialog, err := portable.NewDialog(portable.UAC, udpInvite, udpFinal, portable.DefaultTimers())
	if err != nil {
		t.Fatal(err)
	}
	udpDialog, _, err = portable.StepDialog(udpDialog, portable.DialogInput{Kind: portable.DialogLocalACK, Now: 1000, Branch: "z9hG4bK-udp-ack"})
	if err != nil {
		t.Fatal(err)
	}
	udpRefs := nativeDialogRefs{Local: &nativeInviteRefs{Request: "r:udp-invite", Final: "r:udp-final", ACK: "r:udp-ack"}}
	udpDialogData, err := encodeNativeDialog(udpDialog, udpRefs, &nativepb.Dialog{Transaction: "udp-initial"})
	if err != nil {
		t.Fatal(err)
	}
	udpFlow := &pb.FlowRef{Ingress: worker, ConnectionId: "udp-socket", SourceIp: "192.0.2.3", SourcePort: 5060, Listener: "sip.example.test:5060", Transport: "udp"}
	udpContext := &nativepb.Context{Context: &pb.ContextRef{Id: "udp-call", Owner: worker, State: &pb.StateRef{Authority: authority, GroupId: "udp-call", Incarnation: "inc", CallId: udpDialog.CallID}}, Application: current.Application, Kind: pb.ContextSnapshot_UAC, CallId: udpDialog.CallID, LocalTag: udpDialog.LocalTag, RemoteTag: udpDialog.RemoteTag, Transport: "udp", Flow: udpFlow, Dialogs: []string{"r:udp-dialog"}}
	udpContextData, err := encodeNativeContext(udpContext)
	if err != nil {
		t.Fatal(err)
	}
	udpPeer := portable.Dialog{CallID: udpDialog.CallID, LocalTag: udpDialog.RemoteTag, RemoteTag: udpDialog.LocalTag, LocalURI: udpDialog.RemoteURI, RemoteURI: udpDialog.LocalURI, Target: udpDialog.Contact, Contact: udpDialog.Target, SentBy: "192.0.2.2:5060", Transport: "udp"}
	udpBye, err := portable.BuildDialogRequest(udpPeer, sip.BYE, 12, nil, "z9hG4bK-udp-remote-bye")
	if err != nil {
		t.Fatal(err)
	}
	udpIncoming := portable.RestoreMessage(udpBye.WireBytes(), "udp", "192.0.2.3:5060", "")
	udpSelectors, err := nativeSelectorIDs(udpIncoming, false, false)
	if err != nil {
		t.Fatal(err)
	}
	var udpDialogSelector rediscore.Selector
	for _, s := range udpSelectors {
		if s.Kind == rediscore.SelectorDialog {
			udpDialogSelector = s
		}
	}
	udpChanges := []rediscore.Change{{Field: "r:context", Value: udpContextData}, {Field: "r:udp-dialog", Value: udpDialogData}}
	for field, msg := range map[string]portable.Message{"r:udp-invite": udpInvite, "r:udp-final": udpFinal, "r:udp-ack": *udpDialog.LocalInvite.ACK} {
		data, e := selectedmessage.Marshal(msg)
		if e != nil {
			t.Fatal(e)
		}
		udpChanges = append(udpChanges, rediscore.Change{Field: field, Value: data})
	}
	udpID := rediscore.Identity{Context: "udp-call", Incarnation: "inc"}
	_, err = store.Commit(ctx, rediscore.Publication{Identity: udpID, Create: true, Deadline: time.Now().Add(time.Second), CreditDelta: 1 << 20, Changes: udpChanges, Selectors: []rediscore.SelectorChange{{Selector: udpDialogSelector, Record: "r:udp-dialog"}}})
	if err != nil {
		t.Fatal(err)
	}
	udpSelected, err := selectRedisCallDialog(ctx, store, worker, udpFlow, udpIncoming, time.Now().Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if err = publishRedisCallRemoteBye(ctx, store, udpSelected, udpFlow, bytes.Repeat([]byte{1}, 32), assignment, time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if handled, err := matchRedisCallByeDuplicate(ctx, store, worker, udpFlow, udpIncoming, bytes.Repeat([]byte{1}, 32), time.Now().Add(time.Second)); err != nil || !handled {
		t.Fatalf("pending BYE duplicate: %v", err)
	}
	udpReady, err := nativeReadyWrites(ctx, store, physicalWorker, "epoch", 4)
	if err != nil || len(udpReady) != 1 {
		t.Fatalf("UDP BYE ready: %v", err)
	}
	udpFirst := udpReady[0]
	udpReserved, err := (nativeRedisWriter{store: store}).Reserve(ctx, udpFirst)
	if err != nil || udpReserved == nil || udpReserved.Known {
		t.Fatalf("UDP BYE reserve: %v", err)
	}
	if handled, err := matchRedisCallByeDuplicate(ctx, store, worker, udpFlow, udpIncoming, bytes.Repeat([]byte{1}, 32), time.Now().Add(time.Second)); err != nil || !handled {
		t.Fatalf("started BYE duplicate: %v", err)
	}
	if pending, err := nativeReadyWrites(ctx, store, physicalWorker, "epoch", 4); err != nil || len(pending) != 0 {
		t.Fatalf("started BYE created a second attempt: %v", err)
	}
	if err = completeRedisCallByeOutput(ctx, store, udpFirst, &pb.DeliveryResult{Effect: udpFirst.Effect, Outcome: pb.DeliveryResult_UNKNOWN}); err != nil {
		t.Fatal(err)
	}
	if handled, err := matchRedisCallByeDuplicate(ctx, store, worker, udpFlow, udpIncoming, bytes.Repeat([]byte{1}, 32), time.Now().Add(time.Second)); err != nil || !handled {
		t.Fatalf("unknown BYE duplicate: %v", err)
	}
	if pending, err := nativeReadyWrites(ctx, store, physicalWorker, "epoch", 4); err != nil || len(pending) != 0 {
		t.Fatalf("unknown BYE created a second attempt: %v", err)
	}
	udpProcedure, err := store.LookupSelector(ctx, udpSelectors[0])
	if err != nil {
		t.Fatal(err)
	}
	udpOutputMember := "out-call-bye-output-" + strings.TrimPrefix(udpProcedure.Record, "r:call-bye-")
	// Keep the unrelated lifecycle head later so the real output deadline is
	// the worker's authoritative due head for this physical-cutoff fixture.
	members, err := client.ZRange(ctx, root+"core:deadlines:udp-call", 0, -1).Result()
	if err != nil {
		t.Fatal(err)
	}
	var parkedEvents []rediscore.TimerChange
	for _, member := range members {
		if strings.HasPrefix(member, "event-") {
			parkedEvents = append(parkedEvents, rediscore.TimerChange{Member: member, Due: time.Now().Add(time.Minute)})
		}
	}
	if len(parkedEvents) != 0 {
		_, err = store.Commit(ctx, rediscore.Publication{Identity: udpProcedure.Identity, Revision: udpProcedure.View.Revision, Deadline: time.Now().Add(time.Second), SpendReservation: true, Timers: &rediscore.TimerMutation{Expected: udpProcedure.View.Timer, Target: udpProcedure.View.Timer, Changes: parkedEvents}})
		if err != nil {
			t.Fatal(err)
		}
	}
	unknownAt := int64(udpFirst.Permit.Authorization.DeadlineMs)
	for time.Now().Before(time.UnixMilli(unknownAt).Add(20 * time.Millisecond)) {
		for _, workerID := range []string{"worker", physicalWorker} {
			if _, e := store.RenewWorker(ctx, rediscore.WorkerLeaseRequest{Worker: workerID, Epoch: "epoch", PrimaryRunID: primary}); e != nil {
				t.Fatal("renew worker while waiting for original BYE cutoff", e)
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	udpProcedure, err = store.LookupSelector(ctx, udpSelectors[0])
	if err != nil {
		t.Fatal(err)
	}
	unknownNow, err := client.Time(ctx).Result()
	if err != nil {
		t.Fatal(err)
	}
	unknownDue := &rediscore.DueContext{Identity: udpProcedure.Identity, Revision: udpProcedure.View.Revision, Assignment: udpProcedure.View.Timer, Member: udpOutputMember, DeadlineMS: unknownAt, CheckedAtMS: unknownNow.UnixMilli(), Observed: time.Now()}
	if err = (&redisNative{}).due(ctx, store, unknownDue); err != nil {
		t.Fatalf("routed unknown BYE cutoff: %v", err)
	}
	unknownRef, err := nativeOutputRef(udpFirst)
	if err != nil {
		t.Fatal(err)
	}
	unknownProof, err := store.InspectOutput(ctx, unknownRef)
	if err != nil || unknownProof.State != rediscore.OutputUnknown || unknownProof.Settled || unknownProof.Acknowledged {
		t.Fatalf("unknown BYE proof was not retained: %v", err)
	}
	if _, err = client.ZScore(ctx, root+"core:deadlines:udp-call", udpOutputMember).Result(); !errors.Is(err, redis.Nil) {
		t.Fatalf("unknown BYE cutoff head retained: %v", err)
	}
	if err = (nativeRedisWriter{store: store}).Complete(ctx, udpFirst, &pb.DeliveryResult{Effect: udpFirst.Effect, Outcome: pb.DeliveryResult_WRITTEN}); err != nil {
		t.Fatal(err)
	}
	udpRef, err := nativeOutputRef(udpFirst)
	if err != nil {
		t.Fatal(err)
	}
	udpProof, err := store.InspectOutput(ctx, udpRef)
	if err != nil {
		t.Fatal(err)
	}
	udpMetadata, err := decodeNativeOutput(udpProof.Metadata)
	if err != nil {
		t.Fatal(err)
	}
	udpCapture := &nativeOutputCapture{ref: udpRef, proof: udpProof, metadata: udpMetadata}
	changedMetadata := proto.Clone(udpMetadata).(*nativepb.Output)
	changedMetadata.Operation = "replacement"
	changedBytes, err := encodeNativeOutput(changedMetadata)
	if err != nil {
		t.Fatal(err)
	}
	if err = client.HSet(ctx, root+"core:context:udp-call", udpProof.MetadataField, changedBytes).Err(); err != nil {
		t.Fatal(err)
	}
	if err = settleRedisCallByeOutputAndRedrive(ctx, store, udpFirst, udpCapture, assignment, time.Now().Add(time.Second)); !errors.Is(err, rediscore.ErrConflict) {
		t.Fatalf("stale output proof must conflict: %v", err)
	}
	conflictedProof, err := store.InspectOutput(ctx, udpRef)
	if err != nil || conflictedProof.Settled {
		t.Fatalf("conflict settled proof without re-drive: %v", err)
	}
	if _, err = client.ZScore(ctx, root+"core:deadlines:udp-call", udpOutputMember).Result(); !errors.Is(err, redis.Nil) {
		t.Fatalf("conflict unexpectedly installed due: %v", err)
	}
	if err = client.HSet(ctx, root+"core:context:udp-call", udpProof.MetadataField, udpProof.Metadata).Err(); err != nil {
		t.Fatal(err)
	}
	if err = settleRedisCallByeOutputAndRedrive(ctx, store, udpFirst, udpCapture, assignment, time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	rearmed, err := client.ZScore(ctx, root+"core:deadlines:udp-call", udpOutputMember).Result()
	if err != nil || int64(rearmed) != int64(udpFirst.Permit.Authorization.DeadlineMs) {
		t.Fatalf("late proof did not re-arm original cutoff: %v", err)
	}
	if err = acknowledgeRedisCallByeOutput(ctx, store, udpFirst, time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if handled, err := matchRedisCallByeDuplicate(ctx, store, worker, udpFlow, udpIncoming, bytes.Repeat([]byte{1}, 32), time.Now().Add(time.Second)); err != nil || !handled {
		t.Fatalf("completed BYE duplicate: %v", err)
	}
	udpReplay, err := nativeReadyWrites(ctx, store, physicalWorker, "epoch", 4)
	if err != nil || len(udpReplay) != 1 || proto.Equal(udpReplay[0].Effect, udpFirst.Effect) {
		t.Fatalf("missing exact UDP BYE replay: %v", err)
	}
	replayWrite := udpReplay[0]
	if replayReserve, e := (nativeRedisWriter{store: store}).Reserve(ctx, replayWrite); e != nil || replayReserve == nil || replayReserve.Known {
		t.Fatalf("reserve replay: %v", e)
	}
	if err = completeRedisCallByeOutput(ctx, store, replayWrite, &pb.DeliveryResult{Effect: replayWrite.Effect, Outcome: pb.DeliveryResult_WRITTEN}); err != nil {
		t.Fatal(err)
	}
	if err = acknowledgeRedisCallByeOutput(ctx, store, replayWrite, time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	udpProcView, err := store.Read(ctx, udpID, udpProcedure.Record, "r:call-bye-origin-"+strings.TrimPrefix(udpProcedure.Record, "r:call-bye-"), "r:call-bye-final-"+strings.TrimPrefix(udpProcedure.Record, "r:call-bye-"))
	if err != nil {
		t.Fatal(err)
	}
	udpProc, err := decodeNativeProcedure(udpProcView.Records[udpProcedure.Record])
	if err != nil {
		t.Fatal(err)
	}
	udpMemo, err := selectedmessage.NewMemo(2)
	if err != nil {
		t.Fatal(err)
	}
	udpTx, err := udpProc.attach(&nativeSelectedMessages{records: udpProcView.Records, memo: udpMemo, provenance: "admitted"}, true, true, false)
	if err != nil {
		t.Fatal(err)
	}
	udpJ := udpTx.Timers[portable.TimerJ]
	udpJ.At = time.Now().Add(-time.Second).UnixMilli()
	udpTx.Timers[portable.TimerJ] = udpJ
	udpProcData, err := encodeNativeProcedure(udpTx, udpProc.Refs, udpProc.Metadata)
	if err != nil {
		t.Fatal(err)
	}
	udpJMember := "call-tx-" + strings.TrimPrefix(udpProcedure.Record, "r:call-bye-") + "-J"
	_, err = store.Commit(ctx, rediscore.Publication{Identity: udpID, Revision: udpProcView.Revision, Deadline: time.Now().Add(time.Second), SpendReservation: true, Changes: []rediscore.Change{{Field: udpProcedure.Record, Value: udpProcData}}, Timers: &rediscore.TimerMutation{Expected: udpProcView.Timer, Target: udpProcView.Timer, Changes: []rediscore.TimerChange{{Member: udpJMember, Due: time.UnixMilli(udpJ.At)}}}})
	if err != nil {
		t.Fatal(err)
	}
	udpJView, err := store.ObserveContext(ctx, "udp-call", "inc", udpProcedure.Record)
	if err != nil {
		t.Fatal(err)
	}
	udpNow, err := client.Time(ctx).Result()
	if err != nil {
		t.Fatal(err)
	}
	udpJDue := &rediscore.DueContext{Identity: udpJView.Identity, Revision: udpJView.View.Revision, Assignment: udpJView.View.Timer, Member: udpJMember, DeadlineMS: udpJ.At, CheckedAtMS: udpNow.UnixMilli(), Observed: time.Now()}
	if err = (&redisNative{}).due(ctx, store, udpJDue); err != nil {
		t.Fatalf("UDP BYE Timer J: %v", err)
	}
	udpJAfter, err := store.Read(ctx, udpID, udpProcedure.Record)
	if err != nil {
		t.Fatal(err)
	}
	udpTerminated, err := decodeNativeProcedure(udpJAfter.Records[udpProcedure.Record])
	if err != nil || udpTerminated.Native.Phase != portable.Terminated {
		t.Fatalf("UDP BYE did not terminate: %v", err)
	}
	if score, e := client.ZScore(ctx, root+"core:deadlines:udp-call", udpOutputMember).Result(); e != nil || int64(score) != int64(replayWrite.Permit.Authorization.DeadlineMs) {
		t.Fatalf("UDP Timer J did not re-drive output: %v", e)
	}

	raw := stateTestRequest("INVITE", 1, "", "selected-reject")
	invite, err := portable.ParseMessage(raw.Wire, "tls", "192.0.2.3:5061", "")
	if err != nil {
		t.Fatal(err)
	}
	final, err := portable.BuildResponseWithHeaders(invite, 486, "Busy Here", nil, "local-tag", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	tx, _, err := nativeServerStart(invite, "local-tag", 1000)
	if err != nil {
		t.Fatal(err)
	}
	tx, _, err = nativeServerSelectResponse(tx, final, 1001)
	if err != nil {
		t.Fatal(err)
	}
	procedureData, err := encodeNativeProcedure(tx, nativeProcedureRefs{Origin: "r:origin", LastResponse: "r:response"}, &nativepb.Procedure{Flow: flow, RequestId: "rejected"})
	if err != nil {
		t.Fatal(err)
	}
	originData, err := selectedmessage.Marshal(invite)
	if err != nil {
		t.Fatal(err)
	}
	responseData, err := selectedmessage.Marshal(final)
	if err != nil {
		t.Fatal(err)
	}
	facts, err := invite.Facts()
	if err != nil {
		t.Fatal(err)
	}
	rejected := &nativepb.Context{Context: &pb.ContextRef{Id: "rejected", State: &pb.StateRef{Incarnation: "inc"}}, Kind: pb.ContextSnapshot_UAS, InitialProcedures: []string{"r:procedure-rejected"}, CallId: facts.CallID, Transport: "tls", Flow: flow}
	rejectedData, err := encodeNativeContext(rejected)
	if err != nil {
		t.Fatal(err)
	}
	inviteSelectors, err := nativeSelectorIDs(invite, false, false)
	if err != nil || len(inviteSelectors) == 0 {
		t.Fatal("missing transaction selector", err)
	}
	_, err = store.Commit(ctx, rediscore.Publication{Identity: rediscore.Identity{Context: "rejected", Incarnation: "inc"}, Create: true, Deadline: time.Now().Add(time.Second), CreditDelta: 1 << 16, Changes: []rediscore.Change{{Field: "r:context", Value: rejectedData}, {Field: "r:procedure-rejected", Value: procedureData}, {Field: "r:origin", Value: originData}, {Field: "r:response", Value: responseData}}, Selectors: []rediscore.SelectorChange{{Selector: inviteSelectors[0], Record: "r:procedure-rejected"}}})
	if err != nil {
		t.Fatal(err)
	}
	ackWire := strings.Replace(string(raw.Wire), "INVITE sip:", "ACK sip:", 1)
	ackWire = strings.Replace(ackWire, "CSeq: 1 INVITE", "CSeq: 1 ACK", 1)
	ackWire = strings.Replace(ackWire, "To: <sip:service@example.test>", "To: <sip:service@example.test>;tag=local-tag", 1)
	ack, err := portable.ParseMessage([]byte(ackWire), "tls", "192.0.2.3:5061", "")
	if err != nil {
		t.Fatal(err)
	}
	selectedACK, handled, err := selectRedisCallTransactionACK(ctx, store, worker, flow, ack, time.Now().Add(time.Second))
	if err != nil || !handled || selectedACK == nil || !selectedACK.Candidate.TransactionACK || selectedACK.Candidate.Transaction.Phase != portable.Terminated {
		t.Fatalf("selected non-2xx ACK: handled=%v err=%v", handled, err)
	}
	if err = publishRedisCallTransactionACK(ctx, store, selectedACK, flow, assignment, time.Now().Add(time.Second)); err != nil {
		t.Fatalf("publish non-2xx ACK: %v", err)
	}
	rejectedAfter, err := store.Read(ctx, rediscore.Identity{Context: "rejected", Incarnation: "inc"}, "r:procedure-rejected")
	if err != nil {
		t.Fatal(err)
	}
	rejectedTx, err := decodeNativeProcedure(rejectedAfter.Records["r:procedure-rejected"])
	if err != nil || rejectedTx.Native.Phase != portable.Terminated {
		t.Fatalf("non-2xx ACK not committed: %v", err)
	}

	uasInvite, err := portable.ParseMessage(stateTestRequest("INVITE", 1, "", "uas-selected").Wire, "tls", "192.0.2.2:5061", "")
	if err != nil {
		t.Fatal(err)
	}
	uasFinal, err := portable.BuildResponseWithHeaders(uasInvite, 200, "OK", nil, "local-tag", "sip:server@example.test;transport=tls", nil)
	if err != nil {
		t.Fatal(err)
	}
	uasDialog, err := portable.NewDialog(portable.UAS, uasInvite, uasFinal, portable.DefaultTimers())
	if err != nil {
		t.Fatal(err)
	}
	uasDialog, uasEffects, err := portable.StepDialog(uasDialog, portable.DialogInput{Kind: portable.DialogSendResponse, Message: uasFinal, Now: time.Now().UnixMilli()})
	if err != nil {
		t.Fatal(err)
	}
	uasPeer, err := portable.NewDialog(portable.UAC, uasInvite, uasFinal, portable.DefaultTimers())
	if err != nil {
		t.Fatal(err)
	}
	uasACK, err := portable.BuildDialogRequest(uasPeer, sip.ACK, 1, nil, "z9hG4bK-uas-ack")
	if err != nil {
		t.Fatal(err)
	}
	uasACK = portable.RestoreMessage(uasACK.WireBytes(), "tls", "192.0.2.3:5061", "")
	uasTx, _, err := nativeServerStart(uasInvite, "local-tag", time.Now().UnixMilli())
	if err != nil {
		t.Fatal(err)
	}
	uasTx, _, err = nativeServerSelectResponse(uasTx, uasFinal, time.Now().UnixMilli())
	if err != nil {
		t.Fatal(err)
	}
	uasDialogData, err := encodeNativeDialog(uasDialog, nativeDialogRefs{Remote: &nativeInviteRefs{Request: "r:uas-origin", Final: "r:uas-final"}}, &nativepb.Dialog{Transaction: uasTx.Key})
	if err != nil {
		t.Fatal(err)
	}
	uasTxData, err := encodeNativeProcedure(uasTx, nativeProcedureRefs{Origin: "r:uas-origin", LastResponse: "r:uas-final"}, &nativepb.Procedure{Flow: old})
	if err != nil {
		t.Fatal(err)
	}
	uasOrigin, err := selectedmessage.Marshal(uasInvite)
	if err != nil {
		t.Fatal(err)
	}
	uasResponse, err := selectedmessage.Marshal(uasFinal)
	if err != nil {
		t.Fatal(err)
	}
	uasContext := &nativepb.Context{Context: &pb.ContextRef{Id: "uas", Owner: worker, State: &pb.StateRef{Authority: authority, GroupId: "uas", Incarnation: "inc", CallId: uasDialog.CallID}}, Application: &pb.ProcessRef{InstanceId: "application", Epoch: "epoch"}, Kind: pb.ContextSnapshot_UAS, CallId: uasDialog.CallID, LocalTag: uasDialog.LocalTag, RemoteTag: uasDialog.RemoteTag, Transport: "tls", Flow: old, InitialProcedures: []string{"r:uas-tx"}, Dialogs: []string{"r:uas-dialog"}}
	uasContextData, err := encodeNativeContext(uasContext)
	if err != nil {
		t.Fatal(err)
	}
	uasInviteIDs, err := nativeSelectorIDs(uasInvite, false, false)
	if err != nil {
		t.Fatal(err)
	}
	uasACKIDs, err := nativeSelectorIDs(uasACK, false, false)
	if err != nil {
		t.Fatal(err)
	}
	var uasDialogID rediscore.Selector
	for _, s := range uasACKIDs {
		if s.Kind == rediscore.SelectorDialog {
			uasDialogID = s
		}
	}
	if _, err = store.RenewWorker(ctx, rediscore.WorkerLeaseRequest{Worker: "uas-worker", Epoch: "epoch", PrimaryRunID: primary, Acquire: true}); err != nil {
		t.Fatal(err)
	}
	uasAssignment := rediscore.TimerAssignment{Worker: "uas-worker", Epoch: "epoch", Term: "epoch"}
	var uasTimers []rediscore.TimerChange
	for _, effect := range uasEffects {
		if effect.Kind == portable.Schedule {
			uasTimers = append(uasTimers, rediscore.TimerChange{Member: callDialogTimerMember("r:uas-dialog", effect.Timer), Due: time.UnixMilli(effect.Deadline.At)})
		}
	}
	_, err = store.Commit(ctx, rediscore.Publication{Identity: rediscore.Identity{Context: "uas", Incarnation: "inc"}, Create: true, Deadline: time.Now().Add(time.Second), CreditDelta: 1 << 20, Changes: []rediscore.Change{{Field: "r:context", Value: uasContextData}, {Field: "r:uas-dialog", Value: uasDialogData}, {Field: "r:uas-tx", Value: uasTxData}, {Field: "r:uas-origin", Value: uasOrigin}, {Field: "r:uas-final", Value: uasResponse}}, Selectors: []rediscore.SelectorChange{{Selector: uasInviteIDs[0], Record: "r:uas-tx"}, {Selector: uasDialogID, Record: "r:uas-dialog"}}, Timers: &rediscore.TimerMutation{Target: uasAssignment, Changes: uasTimers}})
	if err != nil {
		t.Fatal(err)
	}
	originalWorker, err := nativeWorkerReference(old.Ingress)
	if err != nil {
		t.Fatal(err)
	}
	tlsHints := make(chan rediscore.UDPHint, 1)
	tlsWake, err := store.SubscribeWakeResults(ctx, originalWorker, func(rediscore.ResultHint) {}, func(h rediscore.UDPHint) { tlsHints <- h })
	if err != nil {
		t.Fatal(err)
	}
	defer tlsWake.Close()
	remoteClient := redis.NewClient(&redis.Options{Addr: address, MaxRetries: -1})
	defer remoteClient.Close()
	remoteStore, err := rediscore.New(remoteClient, rediscore.Config{Root: root, Profile: profile, PrimaryRunID: primary, RecordBudget: 4 << 20})
	if err != nil {
		t.Fatal(err)
	}
	uasSelected, err := selectRedisCallDialog(ctx, remoteStore, worker, flow, uasACK, time.Now().Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if err = publishRedisCallDialogACK(ctx, remoteStore, uasSelected, wrongSocket, bytes.Repeat([]byte{7}, 32), uasAssignment, time.Now().Add(time.Second)); err == nil {
		t.Fatal("ACK publication accepted a different selected flow")
	}
	if err = publishRedisCallDialogACK(ctx, remoteStore, uasSelected, flow, bytes.Repeat([]byte{7}, 32), uasAssignment, time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	tlsHintCtx, cancelTLSHint := context.WithTimeout(ctx, time.Second)
	defer cancelTLSHint()
	for len(tlsHints) == 0 {
		if err = tlsWake.Receive(tlsHintCtx); err != nil {
			t.Fatal(err)
		}
	}
	if hint := <-tlsHints; !hint.Stop || hint.Context != "uas" || hint.Incarnation != "inc" || hint.Procedure != "r:uas-tx" || hint.Worker != originalWorker {
		t.Fatalf("wrong replacement-flow TLS ACK stop hint: %+v", hint)
	}
	uasAfter, err := store.ObserveContext(ctx, "uas", "inc", "r:context", "r:uas-dialog")
	if err != nil {
		t.Fatal(err)
	}
	updatedContext, err := decodeNativeContext(uasAfter.View.Records["r:context"])
	if err != nil || !updatedContext.InitialAck {
		t.Fatal("ACK context not published", err)
	}
	updatedDialog, err := decodeNativeDialog(uasAfter.View.Records["r:uas-dialog"])
	if err != nil || updatedDialog.Refs.Remote.ACK == "" {
		t.Fatal("ACK evidence not published", err)
	}
	ackMembers, err := client.ZRange(ctx, root+"core:deadlines:uas", 0, 0).Result()
	if err != nil || len(ackMembers) != 1 || !strings.HasPrefix(ackMembers[0], "event-") {
		t.Fatalf("ACK lifecycle duty missing: %v", err)
	}
	eventView, err := store.Read(ctx, uasAfter.Identity, "r:"+ackMembers[0])
	if err != nil {
		t.Fatal(err)
	}
	event, err := decodeNativeLifecycle(eventView.Records["r:"+ackMembers[0]])
	if err != nil || event.Kind != pb.DialogEvent_ACKNOWLEDGED || event.Message != "" || !event.Snapshot.InitialAck {
		t.Fatalf("ACK lifecycle evidence: %v", err)
	}
	_, err = store.Commit(ctx, rediscore.Publication{Identity: uasAfter.Identity, Revision: uasAfter.View.Revision, Deadline: time.Now().Add(time.Second), SpendReservation: true, Selectors: []rediscore.SelectorChange{{Selector: uasInviteIDs[0], Record: "r:uas-tx", Delete: true}}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = selectRedisCallDialog(ctx, store, worker, flow, uasACK, time.Now().Add(time.Second)); !errors.Is(err, errNativeRecord) {
		t.Fatalf("missing associated INVITE was treated as an unmatched dialog: %v", err)
	}

	// Both ACK families publish a stop hint on the original physical worker's
	// wake stream as part of the same native transition.
	hints := make(chan rediscore.UDPHint, 2)
	wake, err := store.SubscribeWakeResults(ctx, physicalWorker, func(rediscore.ResultHint) {}, func(h rediscore.UDPHint) { hints <- h })
	if err != nil {
		t.Fatal(err)
	}
	defer wake.Close()
	udpRaw := strings.Replace(string(stateTestRequest("INVITE", 1, "", "udp-uas-ack").Wire), "SIP/2.0/TLS", "SIP/2.0/UDP", 1)
	udpRaw = strings.Replace(udpRaw, "redis-native-call@example.test", "udp-uas-call@example.test", 1)
	uInvite, err := portable.ParseMessage([]byte(udpRaw), "udp", "192.0.2.3:5060", "")
	if err != nil {
		t.Fatal(err)
	}
	uFinal, err := portable.BuildResponseWithHeaders(uInvite, 200, "OK", nil, "local-tag", "sip:server@example.test;transport=udp", nil)
	if err != nil {
		t.Fatal(err)
	}
	uDialog, err := portable.NewDialog(portable.UAS, uInvite, uFinal, portable.DefaultTimers())
	if err != nil {
		t.Fatal(err)
	}
	uDialog, uEffects, err := portable.StepDialog(uDialog, portable.DialogInput{Kind: portable.DialogSendResponse, Message: uFinal, Now: time.Now().UnixMilli()})
	if err != nil {
		t.Fatal(err)
	}
	uPeer, err := portable.NewDialog(portable.UAC, uInvite, uFinal, portable.DefaultTimers())
	if err != nil {
		t.Fatal(err)
	}
	uAck, err := portable.BuildDialogRequest(uPeer, sip.ACK, 1, nil, "z9hG4bK-udp-uas-ack")
	if err != nil {
		t.Fatal(err)
	}
	uAck = portable.RestoreMessage(uAck.WireBytes(), "udp", "192.0.2.3:5060", "")
	uTx, _, err := nativeServerStart(uInvite, "local-tag", time.Now().UnixMilli())
	if err != nil {
		t.Fatal(err)
	}
	uTx, _, err = nativeServerSelectResponse(uTx, uFinal, time.Now().UnixMilli())
	if err != nil {
		t.Fatal(err)
	}
	uDialogData, err := encodeNativeDialog(uDialog, nativeDialogRefs{Remote: &nativeInviteRefs{Request: "r:u-origin", Final: "r:u-final"}}, &nativepb.Dialog{Transaction: uTx.Key})
	if err != nil {
		t.Fatal(err)
	}
	uTxData, err := encodeNativeProcedure(uTx, nativeProcedureRefs{Origin: "r:u-origin", LastResponse: "r:u-final"}, &nativepb.Procedure{Flow: udpFlow})
	if err != nil {
		t.Fatal(err)
	}
	uOrigin, err := selectedmessage.Marshal(uInvite)
	if err != nil {
		t.Fatal(err)
	}
	uResponse, err := selectedmessage.Marshal(uFinal)
	if err != nil {
		t.Fatal(err)
	}
	uContext := &nativepb.Context{Context: &pb.ContextRef{Id: "udp-uas", Owner: worker, State: &pb.StateRef{Authority: authority, GroupId: "udp-uas", Incarnation: "inc", CallId: uDialog.CallID}}, Application: current.Application, Kind: pb.ContextSnapshot_UAS, CallId: uDialog.CallID, LocalTag: uDialog.LocalTag, RemoteTag: uDialog.RemoteTag, Transport: "udp", Flow: udpFlow, InitialProcedures: []string{"r:u-tx"}, Dialogs: []string{"r:u-dialog"}}
	uContextData, err := encodeNativeContext(uContext)
	if err != nil {
		t.Fatal(err)
	}
	uInviteIDs, err := nativeSelectorIDs(uInvite, false, false)
	if err != nil {
		t.Fatal(err)
	}
	uAckIDs, err := nativeSelectorIDs(uAck, false, false)
	if err != nil {
		t.Fatal(err)
	}
	var uDialogID rediscore.Selector
	for _, s := range uAckIDs {
		if s.Kind == rediscore.SelectorDialog {
			uDialogID = s
		}
	}
	var uTimers []rediscore.TimerChange
	for _, effect := range uEffects {
		if effect.Kind == portable.Schedule {
			uTimers = append(uTimers, rediscore.TimerChange{Member: callDialogTimerMember("r:u-dialog", effect.Timer), Due: time.UnixMilli(effect.Deadline.At)})
		}
	}
	_, err = store.Commit(ctx, rediscore.Publication{Identity: rediscore.Identity{Context: "udp-uas", Incarnation: "inc"}, Create: true, Deadline: time.Now().Add(time.Second), CreditDelta: 1 << 20, Changes: []rediscore.Change{{Field: "r:context", Value: uContextData}, {Field: "r:u-dialog", Value: uDialogData}, {Field: "r:u-tx", Value: uTxData}, {Field: "r:u-origin", Value: uOrigin}, {Field: "r:u-final", Value: uResponse}}, Selectors: []rediscore.SelectorChange{{Selector: uInviteIDs[0], Record: "r:u-tx"}, {Selector: uDialogID, Record: "r:u-dialog"}}, Timers: &rediscore.TimerMutation{Target: assignment, Changes: uTimers}})
	if err != nil {
		t.Fatal(err)
	}
	uSelected, err := selectRedisCallDialog(ctx, store, worker, udpFlow, uAck, time.Now().Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if err = publishRedisCallDialogACK(ctx, store, uSelected, udpFlow, bytes.Repeat([]byte{7}, 32), assignment, time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	hintCtx, cancelHint := context.WithTimeout(ctx, time.Second)
	defer cancelHint()
	for len(hints) == 0 {
		if err = wake.Receive(hintCtx); err != nil {
			t.Fatal(err)
		}
	}
	if hint := <-hints; !hint.Stop || hint.Context != "udp-uas" || hint.Procedure != "r:u-tx" || hint.Worker != physicalWorker {
		t.Fatalf("wrong 2xx ACK stop hint: %+v", hint)
	}
	rejectRaw := strings.Replace(string(stateTestRequest("INVITE", 1, "", "udp-reject").Wire), "SIP/2.0/TLS", "SIP/2.0/UDP", 1)
	rejectRaw = strings.Replace(rejectRaw, "redis-native-call@example.test", "udp-rejected@example.test", 1)
	rInvite, err := portable.ParseMessage([]byte(rejectRaw), "udp", "192.0.2.3:5060", "")
	if err != nil {
		t.Fatal(err)
	}
	rFinal, err := portable.BuildResponseWithHeaders(rInvite, 486, "Busy Here", nil, "local-tag", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	rTx, _, err := nativeServerStart(rInvite, "local-tag", time.Now().UnixMilli())
	if err != nil {
		t.Fatal(err)
	}
	rTx, _, err = nativeServerSelectResponse(rTx, rFinal, time.Now().UnixMilli())
	if err != nil {
		t.Fatal(err)
	}
	rTxData, err := encodeNativeProcedure(rTx, nativeProcedureRefs{Origin: "r:r-origin", LastResponse: "r:r-final"}, &nativepb.Procedure{Flow: udpFlow, RequestId: "udp-rejected"})
	if err != nil {
		t.Fatal(err)
	}
	rOrigin, err := selectedmessage.Marshal(rInvite)
	if err != nil {
		t.Fatal(err)
	}
	rResponse, err := selectedmessage.Marshal(rFinal)
	if err != nil {
		t.Fatal(err)
	}
	rFacts, err := rInvite.Facts()
	if err != nil {
		t.Fatal(err)
	}
	rContextData, err := encodeNativeContext(&nativepb.Context{Context: &pb.ContextRef{Id: "udp-rejected", State: &pb.StateRef{Incarnation: "inc"}}, Kind: pb.ContextSnapshot_UAS, InitialProcedures: []string{"r:procedure-udp-rejected"}, CallId: rFacts.CallID, Transport: "udp", Flow: udpFlow})
	if err != nil {
		t.Fatal(err)
	}
	rIDs, err := nativeSelectorIDs(rInvite, false, false)
	if err != nil {
		t.Fatal(err)
	}
	_, err = store.Commit(ctx, rediscore.Publication{Identity: rediscore.Identity{Context: "udp-rejected", Incarnation: "inc"}, Create: true, Deadline: time.Now().Add(time.Second), CreditDelta: 1 << 16, Changes: []rediscore.Change{{Field: "r:context", Value: rContextData}, {Field: "r:procedure-udp-rejected", Value: rTxData}, {Field: "r:r-origin", Value: rOrigin}, {Field: "r:r-final", Value: rResponse}}, Selectors: []rediscore.SelectorChange{{Selector: rIDs[0], Record: "r:procedure-udp-rejected"}}})
	if err != nil {
		t.Fatal(err)
	}
	rAckWire := strings.Replace(rejectRaw, "INVITE sip:", "ACK sip:", 1)
	rAckWire = strings.Replace(rAckWire, "CSeq: 1 INVITE", "CSeq: 1 ACK", 1)
	rAckWire = strings.Replace(rAckWire, "To: <sip:service@example.test>", "To: <sip:service@example.test>;tag=local-tag", 1)
	rAck, err := portable.ParseMessage([]byte(rAckWire), "udp", "192.0.2.3:5060", "")
	if err != nil {
		t.Fatal(err)
	}
	rSelected, handled, err := selectRedisCallTransactionACK(ctx, store, worker, udpFlow, rAck, time.Now().Add(time.Second))
	if err != nil || !handled {
		t.Fatalf("select UDP non-2xx ACK: %v", err)
	}
	if err = publishRedisCallTransactionACK(ctx, store, rSelected, udpFlow, assignment, time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	for len(hints) == 0 {
		if err = wake.Receive(hintCtx); err != nil {
			t.Fatal(err)
		}
	}
	if hint := <-hints; !hint.Stop || hint.Context != "udp-rejected" || hint.Procedure != "r:procedure-udp-rejected" || hint.Worker != physicalWorker {
		t.Fatalf("wrong non-2xx ACK stop hint: %+v", hint)
	}

	terminalInvite, err := portable.BuildInitialRequest(portable.InitialRequest{Method: sip.INVITE, URI: "sip:phone@example.test", From: "<sip:controller@example.test>", CallID: "terminal-native-call", LocalTag: "local", Branch: "z9hG4bK-terminal", CSeq: 1, Advertise: "192.0.2.1:5061", Contact: "<sip:controller@192.0.2.1:5061;transport=tls>", Transport: "tls"})
	if err != nil {
		t.Fatal(err)
	}
	terminalFinal, err := portable.BuildResponseWithHeaders(terminalInvite, 200, "OK", nil, "remote", "sip:phone@192.0.2.2:5061;transport=tls", nil)
	if err != nil {
		t.Fatal(err)
	}
	terminalDialog, err := portable.NewDialog(portable.UAC, terminalInvite, terminalFinal, portable.DefaultTimers())
	if err != nil {
		t.Fatal(err)
	}
	terminalDialog, _, err = portable.StepDialog(terminalDialog, portable.DialogInput{Kind: portable.DialogLocalACK, Now: 1000, Branch: "z9hG4bK-terminal-ack"})
	if err != nil {
		t.Fatal(err)
	}
	terminalDialog, _, err = portable.StepDialog(terminalDialog, portable.DialogInput{Kind: portable.DialogTerminate, Now: time.Now().UnixMilli()})
	if err != nil {
		t.Fatal(err)
	}
	terminalProgress, err := portable.BuildResponseWithHeaders(terminalInvite, 180, "Ringing", nil, "early", "sip:phone@192.0.2.4:5061;transport=tls", nil)
	if err != nil {
		t.Fatal(err)
	}
	terminalEarly, err := portable.NewDialog(portable.UAC, terminalInvite, terminalProgress, portable.DefaultTimers())
	if err != nil {
		t.Fatal(err)
	}
	terminalTxKey, err := terminalInvite.ClientTxKey()
	if err != nil {
		t.Fatal(err)
	}
	terminalEarlyData, err := encodeNativeDialog(terminalEarly, nativeDialogRefs{Local: &nativeInviteRefs{Request: "r:terminal-origin"}}, &nativepb.Dialog{Transaction: terminalTxKey})
	if err != nil {
		t.Fatal(err)
	}
	terminalTx, err := portable.NewTransaction(portable.InviteClient, terminalInvite, portable.DefaultTimers(), "")
	if err != nil {
		t.Fatal(err)
	}
	terminalTx, _, err = portable.StepTransaction(terminalTx, portable.Input{Kind: portable.Start, Now: time.Now().UnixMilli()})
	if err != nil {
		t.Fatal(err)
	}
	terminalTx, _, err = portable.StepTransaction(terminalTx, portable.Input{Kind: portable.Terminate, Now: time.Now().UnixMilli()})
	if err != nil {
		t.Fatal(err)
	}
	terminalRefs := nativeDialogRefs{Local: &nativeInviteRefs{Request: "r:terminal-origin", Final: "r:terminal-final", ACK: "r:terminal-ack"}}
	terminalDialogData, err := encodeNativeDialog(terminalDialog, terminalRefs, &nativepb.Dialog{Transaction: terminalTx.Key})
	if err != nil {
		t.Fatal(err)
	}
	terminalTxData, err := encodeNativeProcedure(terminalTx, nativeProcedureRefs{Origin: "r:terminal-origin"}, &nativepb.Procedure{CreditContribution: 1 << 16, CommandResult: "r:result-1"})
	if err != nil {
		t.Fatal(err)
	}
	terminalNow, err := client.Time(ctx).Result()
	if err != nil {
		t.Fatal(err)
	}
	terminalAt := terminalNow.Add(-time.Second).UnixMilli()
	terminalContext := &nativepb.Context{Context: &pb.ContextRef{Id: "terminal-call", State: &pb.StateRef{Incarnation: "inc"}}, Kind: pb.ContextSnapshot_UAC, CallId: terminalDialog.CallID, Ended: true, CallRetireAtMs: terminalAt, CommandSequence: 1, InitialProcedures: []string{"r:terminal-tx"}, Dialogs: []string{"r:terminal-early", "r:terminal-dialog"}}
	terminalContextData, err := encodeNativeContext(terminalContext)
	if err != nil {
		t.Fatal(err)
	}
	terminalChanges := []rediscore.Change{{Field: "r:context", Value: terminalContextData}, {Field: "r:terminal-early", Value: terminalEarlyData}, {Field: "r:terminal-dialog", Value: terminalDialogData}, {Field: "r:terminal-tx", Value: terminalTxData}}
	terminalResult := &nativepb.Result{Certainty: pb.OperationResult_COMPLETED, Sequence: 1, OperationId: "terminal-operation", Fingerprint: bytes.Repeat([]byte{1}, 32), DecidingLeaseTerm: "term", CutoffMs: terminalNow.Add(time.Minute).UnixMilli()}
	terminalResultData, err := encodeNativeResult(terminalResult)
	if err != nil {
		t.Fatal(err)
	}
	terminalChanges = append(terminalChanges, rediscore.Change{Field: "r:result-1", Value: terminalResultData})
	for field, msg := range map[string]portable.Message{"r:terminal-origin": terminalInvite, "r:terminal-final": *terminalDialog.LocalInvite.Final, "r:terminal-ack": *terminalDialog.LocalInvite.ACK} {
		data, e := selectedmessage.Marshal(msg)
		if e != nil {
			t.Fatal(e)
		}
		terminalChanges = append(terminalChanges, rediscore.Change{Field: field, Value: data})
	}
	termTags := []string{terminalDialog.LocalTag, terminalDialog.RemoteTag}
	slices.Sort(termTags)
	terminalSelectors := []rediscore.SelectorChange{{Selector: nativeSelector(rediscore.SelectorDialog, terminalDialog.CallID, termTags[0], termTags[1]), Record: "r:terminal-dialog"}, {Selector: nativeSelector(rediscore.SelectorClientTransaction, terminalTx.Key), Record: "r:terminal-tx"}}
	earlyTags := []string{terminalEarly.LocalTag, terminalEarly.RemoteTag}
	slices.Sort(earlyTags)
	terminalSelectors = append(terminalSelectors, rediscore.SelectorChange{Selector: nativeSelector(rediscore.SelectorDialog, terminalEarly.CallID, earlyTags[0], earlyTags[1]), Record: "r:terminal-early"})
	if _, err = store.RenewWorker(ctx, rediscore.WorkerLeaseRequest{Worker: "terminal-worker", Epoch: "epoch", PrimaryRunID: primary, Acquire: true}); err != nil {
		t.Fatal(err)
	}
	terminalAssignment := rediscore.TimerAssignment{Worker: "terminal-worker", Epoch: "epoch", Term: "epoch"}
	terminalID := rediscore.Identity{Context: "terminal-call", Incarnation: "inc"}
	_, err = store.Commit(ctx, rediscore.Publication{Identity: terminalID, Create: true, Deadline: time.Now().Add(time.Second), CreditDelta: 1 << 16, Changes: terminalChanges, Selectors: terminalSelectors, Timers: &rediscore.TimerMutation{Target: terminalAssignment, Changes: []rediscore.TimerChange{{Member: "call-retire", Due: time.UnixMilli(terminalAt)}}}})
	if err != nil {
		t.Fatal(err)
	}
	firstRetire, err := store.PeekDue(ctx, "terminal-worker", "epoch")
	if err != nil || firstRetire == nil {
		t.Fatalf("initial terminal due: %v", err)
	}
	if finished, e := retireRedisQuiescentCall(ctx, store, firstRetire, time.Now().Add(time.Second)); e != nil || finished {
		t.Fatalf("waiter cutoff not retained: finished=%v err=%v", finished, e)
	}
	if score, e := client.ZScore(ctx, root+"core:deadlines:terminal-call", "call-retire").Result(); e != nil || int64(score) != terminalResult.CutoffMs {
		t.Fatalf("wrong waiter re-drive cutoff: %v", e)
	}
	terminalView, err := store.ObserveContext(ctx, "terminal-call", "inc", "r:result-1")
	if err != nil {
		t.Fatal(err)
	}
	terminalHead, err := store.Read(ctx, terminalID, "r:context")
	if err != nil {
		t.Fatal(err)
	}
	terminalCurrent, err := decodeNativeContext(terminalHead.Records["r:context"])
	if err != nil || terminalCurrent.CallRetireAtMs != terminalAt {
		t.Fatalf("first-end cutoff was changed: %v", err)
	}
	terminalResult.CutoffMs = terminalAt
	terminalResultData, err = encodeNativeResult(terminalResult)
	if err != nil {
		t.Fatal(err)
	}
	_, err = store.Commit(ctx, rediscore.Publication{Identity: terminalID, Revision: terminalView.View.Revision, Deadline: time.Now().Add(time.Second), SpendReservation: true, Changes: []rediscore.Change{{Field: "r:result-1", Value: terminalResultData}}, Timers: &rediscore.TimerMutation{Expected: terminalView.View.Timer, Target: terminalView.View.Timer, Changes: []rediscore.TimerChange{{Member: "call-retire", Due: time.UnixMilli(terminalAt)}}}})
	if err != nil {
		t.Fatal(err)
	}
	// The actual event reference blocks anchor retirement even if its timer
	// index is unexpectedly absent; only event completion releases the root.
	terminalGuard, err := store.ObserveContext(ctx, "terminal-call", "inc", "r:terminal-tx")
	if err != nil {
		t.Fatal(err)
	}
	guardProc, err := readPrivateProcedureRecord(terminalGuard.View.Records["r:terminal-tx"])
	if err != nil {
		t.Fatal(err)
	}
	guardProc.Events = []string{"r:event-held"}
	guardData, err := marshalNativeRecord(storagecodec.NativeProcedure, guardProc)
	if err != nil {
		t.Fatal(err)
	}
	guardEvent, err := encodeNativeLifecycle(&nativepb.Lifecycle{Kind: pb.DialogEvent_ANSWERED, Snapshot: &pb.ContextSnapshot{Context: terminalContext.Context, Kind: pb.ContextSnapshot_UAC}, Message: "r:terminal-final", Root: "r:terminal-tx", Owners: []string{"r:terminal-dialog"}, DeadlineMs: terminalNow.Add(time.Minute).UnixMilli(), NextMs: terminalNow.UnixMilli()})
	if err != nil {
		t.Fatal(err)
	}
	_, err = store.Commit(ctx, rediscore.Publication{Identity: terminalID, Revision: terminalGuard.View.Revision, Deadline: time.Now().Add(time.Second), SpendReservation: true, Changes: []rediscore.Change{{Field: "r:terminal-tx", Value: guardData}, {Field: "r:event-held", Value: guardEvent}}})
	if err != nil {
		t.Fatal(err)
	}
	guardDue, err := store.PeekDue(ctx, "terminal-worker", "epoch")
	if err != nil || guardDue == nil {
		t.Fatal("missing guard due", err)
	}
	if finished, e := retireRedisQuiescentCall(ctx, store, guardDue, time.Now().Add(time.Second)); e != nil || finished {
		t.Fatalf("event-owned root retired: %v", e)
	}
	guardView, err := store.ObserveContext(ctx, "terminal-call", "inc", "r:terminal-tx")
	if err != nil {
		t.Fatal(err)
	}
	guardProc.Events = nil
	guardData, err = marshalNativeRecord(storagecodec.NativeProcedure, guardProc)
	if err != nil {
		t.Fatal(err)
	}
	_, err = store.Commit(ctx, rediscore.Publication{Identity: terminalID, Revision: guardView.View.Revision, Deadline: time.Now().Add(time.Second), SpendReservation: true, Changes: []rediscore.Change{{Field: "r:terminal-tx", Value: guardData}, {Field: "r:event-held", Delete: true}}})
	if err != nil {
		t.Fatal(err)
	}
	for stage := 0; stage < 4; stage++ {
		due, e := store.PeekDue(ctx, "terminal-worker", "epoch")
		if e != nil || due == nil {
			t.Fatalf("terminal stage %d due: %v", stage, e)
		}
		finished, e := retireRedisQuiescentCall(ctx, store, due, time.Now().Add(time.Second))
		if e != nil || finished != (stage == 3) {
			t.Fatalf("terminal stage %d: finished=%v err=%v", stage, finished, e)
		}
		if stage == 0 {
			retained, e := store.Read(ctx, terminalID, "r:terminal-origin")
			if e != nil || len(retained.Records["r:terminal-origin"]) == 0 {
				t.Fatalf("shared initial wire retired with early fork: %v", e)
			}
		}
	}
	if _, err = store.ObserveContext(ctx, "terminal-call", "inc", "r:context"); !errors.Is(err, rediscore.ErrAbsent) {
		t.Fatalf("quiescent call retained: %v", err)
	}
}

// Fixture timing follows the Redis clock rather than assuming it matches the
// host clock at the instant a real semantic duty is published.
func waitCallTestDue(ctx context.Context, store *rediscore.Store, worker, epoch string, timeout time.Duration) (*rediscore.DueContext, error) {
	deadline := time.Now().Add(timeout)
	for {
		due, err := store.PeekDue(ctx, worker, epoch)
		if err != nil || due != nil {
			return due, err
		}
		if !time.Now().Before(deadline) {
			return nil, context.DeadlineExceeded
		}
		time.Sleep(5 * time.Millisecond)
	}
}
