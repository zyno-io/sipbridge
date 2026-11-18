package engine

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/nativepb"
	"sipbridge/internal/rediscore"
	"sipbridge/internal/selectedmessage"
)

// The call and dialog are constructed at the terminal boundary with SDK
// transitions. The INFO response is an actual reserved UNKNOWN attempt; only
// its original deadline/projection are advanced by the isolated fixture.
func TestRedisTerminalDialogUnknownOutput(t *testing.T) {
	address := os.Getenv("SIPBRIDGE_CORE_TEST_REDIS")
	if address == "" {
		t.Skip("isolated Redis fixture required")
	}
	ctx := context.Background()
	client := redis.NewClient(&redis.Options{Addr: address, MaxRetries: -1})
	root := "sipstate:test:terminal-dialog-" + uuid.NewString() + ":"
	process := &pb.ProcessRef{InstanceId: "worker", Epoch: "epoch"}
	worker, err := nativeWorkerReference(process)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		for _, name := range []string{"profile", "capacity", "selectors", "context:call", "deadlines:call", "worker:" + worker, "timers:" + worker, "ready:" + worker, "owner-expiry", "coordinator"} {
			if e := client.Del(ctx, root+"core:"+name).Err(); e != nil {
				t.Error(e)
			}
		}
		_ = client.Close()
	})
	info, err := client.Info(ctx, "server").Result()
	if err != nil {
		t.Fatal(err)
	}
	primary := ""
	for _, line := range strings.Split(info, "\r\n") {
		if value, ok := strings.CutPrefix(line, "run_id:"); ok {
			primary = value
		}
	}
	profile := []byte{6, 7}
	if err = client.Set(ctx, root+"core:profile", profile, 0).Err(); err != nil {
		t.Fatal(err)
	}
	store, err := rediscore.New(client, rediscore.Config{Root: root, Profile: profile, PrimaryRunID: primary, RecordBudget: 4 << 20})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = store.RenewWorker(ctx, rediscore.WorkerLeaseRequest{Worker: worker, Epoch: process.Epoch, PrimaryRunID: primary, Acquire: true}); err != nil {
		t.Fatal(err)
	}
	now, err := client.Time(ctx).Result()
	if err != nil {
		t.Fatal(err)
	}
	dialog := nativeEstablishedDialogFixture(t)
	peer := portable.Dialog{CallID: dialog.CallID, LocalTag: dialog.RemoteTag, RemoteTag: dialog.LocalTag, LocalURI: dialog.RemoteURI, RemoteURI: dialog.LocalURI, Target: dialog.Contact, Contact: dialog.Target, SentBy: "192.0.2.10:5061", Transport: "tls"}
	request, err := portable.BuildDialogRequest(peer, sip.INFO, 12, nil, "z9hG4bK-terminal-info")
	if err != nil {
		t.Fatal(err)
	}
	request = portable.RestoreMessage(request.WireBytes(), "tls", "192.0.2.10:5061", "")
	tx, _, err := nativeServerStart(request, dialog.LocalTag, now.UnixMilli())
	if err != nil {
		t.Fatal(err)
	}
	flow := &pb.FlowRef{Ingress: process, ConnectionId: "socket", SourceIp: "192.0.2.10", SourcePort: 5061, Listener: "edge.test:5061", Transport: "tls"}
	response, err := nativeServerBuildResponseForFlow(tx, 503, "Service Unavailable", nil, dialog.LocalTag, "", nil, flow, now.UnixMilli())
	if err != nil {
		t.Fatal(err)
	}
	tx = response.Transaction
	if tx.Phase != portable.Terminated {
		t.Fatal("TLS server transaction not terminated")
	}
	dialog, _, err = portable.StepDialog(dialog, portable.DialogInput{Kind: portable.DialogTerminate, Now: now.UnixMilli()})
	if err != nil {
		t.Fatal(err)
	}
	id := uuid.NewString()
	procField, originField := "r:procedure-"+id, "r:origin-"+id
	outField, responseField := "r:dialog-output-"+id+"-0", "r:dialog-response-"+id+"-0"
	contextRef := &pb.ContextRef{Id: "call", State: &pb.StateRef{Incarnation: "inc"}}
	current := &nativepb.Context{Context: contextRef, Kind: pb.ContextSnapshot_UAC, CallId: dialog.CallID, LocalTag: dialog.LocalTag, RemoteTag: dialog.RemoteTag, Ended: true, CallRetireAtMs: now.Add(-500 * time.Millisecond).UnixMilli(), Dialogs: []string{"r:dialog"}, NoninitialProcedures: []string{procField}}
	contextData, err := encodeNativeContext(current)
	if err != nil {
		t.Fatal(err)
	}
	procData, err := encodeNativeProcedure(tx, nativeProcedureRefs{Origin: originField, LastResponse: responseField}, &nativepb.Procedure{RequestId: id, Flow: flow, Outputs: []string{outField}, CreditContribution: 1 << 16})
	if err != nil {
		t.Fatal(err)
	}
	refs := nativeDialogRefs{Local: &nativeInviteRefs{Request: "r:invite", Final: "r:final", ACK: "r:ack"}}
	dialogData, err := encodeNativeDialog(dialog, refs, &nativepb.Dialog{Transaction: "initial", Outputs: []string{outField}})
	if err != nil {
		t.Fatal(err)
	}
	effect := &pb.StateEffectRef{Context: contextRef, Id: "effect-info", AttemptId: "attempt-info"}
	auth := &pb.EffectAuthorization{Effect: effect, AuthorityClass: pb.EffectAuthorization_RESPONSE, DeadlineMs: uint64(now.Add(time.Second).UnixMilli())}
	write := &pb.FlowWrite{Effect: effect, Flow: flow, Permit: &pb.SendPermit{Authorization: auth, Flow: flow}}
	output := &nativepb.Output{Write: write, Procedure: procField, Dialog: "r:dialog", Method: "INFO", Status: 503}
	intent, changes, err := nativePreparePhysicalOutput(output, response.Message, responseField, outField, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	originData, err := selectedmessage.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	changes = append(changes, rediscore.Change{Field: "r:context", Value: contextData}, rediscore.Change{Field: "r:dialog", Value: dialogData}, rediscore.Change{Field: procField, Value: procData}, rediscore.Change{Field: originField, Value: originData})
	for field, msg := range map[string]portable.Message{"r:invite": dialog.LocalInvite.Request, "r:final": *dialog.LocalInvite.Final, "r:ack": *dialog.LocalInvite.ACK} {
		data, e := selectedmessage.Marshal(msg)
		if e != nil {
			t.Fatal(e)
		}
		changes = append(changes, rediscore.Change{Field: field, Value: data})
	}
	assignment := rediscore.TimerAssignment{Worker: worker, Epoch: process.Epoch, Term: process.Epoch}
	identity := rediscore.Identity{Context: "call", Incarnation: "inc"}
	outMember := "out-" + strings.TrimPrefix(outField, "r:")
	_, err = store.Commit(ctx, rediscore.Publication{Identity: identity, Create: true, Deadline: time.Now().Add(time.Second), CreditDelta: 1 << 16, Changes: changes, NewOutputs: []rediscore.OutputIntent{intent}, Timers: &rediscore.TimerMutation{Target: assignment, Changes: []rediscore.TimerChange{{Member: outMember, Due: intent.Deadline}, {Member: "call-retire", Due: time.UnixMilli(current.CallRetireAtMs)}}}})
	if err != nil {
		t.Fatal(err)
	}
	writer := nativeRedisWriter{store: store}
	if _, err = writer.Reserve(ctx, write); err != nil {
		t.Fatal(err)
	}
	if err = writer.Complete(ctx, write, &pb.DeliveryResult{Effect: effect, Outcome: pb.DeliveryResult_UNKNOWN}); err != nil {
		t.Fatal(err)
	}
	past := now.Add(-rediscore.OutputObsoleteGrace - time.Second).UnixMilli()
	write.Permit.Authorization.DeadlineMs = uint64(past)
	data, err := encodeNativeOutput(output)
	if err != nil {
		t.Fatal(err)
	}
	hash, err := nativeFlowWriteHash(write)
	if err != nil {
		t.Fatal(err)
	}
	if err = client.HSet(ctx, root+"core:context:call", outField, data, "o:"+effect.Id+":p", hash[:], "o:"+effect.Id+":d", past).Err(); err != nil {
		t.Fatal(err)
	}
	view, err := store.Read(ctx, identity, "r:context")
	if err != nil {
		t.Fatal(err)
	}
	_, err = store.Commit(ctx, rediscore.Publication{Identity: identity, Revision: view.Revision, Deadline: time.Now().Add(time.Second), Timers: &rediscore.TimerMutation{Expected: view.Timer, Target: view.Timer, RequireLease: true, Changes: []rediscore.TimerChange{{Member: outMember, Due: time.UnixMilli(past)}}}})
	if err != nil {
		t.Fatal(err)
	}
	due, err := store.PeekDue(ctx, worker, process.Epoch)
	if err != nil || due == nil || due.Member != outMember {
		t.Fatal("INFO output due", err)
	}
	if err = dispatchRedisDialogOutputCutoff(ctx, store, due, time.Now().Add(time.Second)); err != nil {
		t.Fatal("INFO U cutoff", err)
	}
	ref, err := nativeOutputRef(write)
	if err != nil {
		t.Fatal(err)
	}
	due, err = store.PeekDue(ctx, worker, process.Epoch)
	if err != nil || due == nil || due.Member != "call-retire" {
		t.Fatal("terminal due", err)
	}
	if _, err = dispatchRedisCallRetirement(ctx, store, due, time.Now().Add(time.Second)); err != nil {
		t.Fatal("forget INFO U", err)
	}
	if _, err = store.InspectOutput(ctx, ref); !errors.Is(err, rediscore.ErrAbsent) {
		t.Fatal("INFO U proof not forgotten", err)
	}
	retained, err := store.Read(ctx, identity, procField, "r:dialog", outField)
	if err != nil || len(retained.Records[outField]) == 0 {
		t.Fatal("native ownership lost before reconcile", err)
	}
	due, err = store.PeekDue(ctx, worker, process.Epoch)
	if err != nil || due == nil || due.Member != "call-retire" {
		t.Fatal("terminal re-drive", err)
	}
	if _, err = dispatchRedisCallRetirement(ctx, store, due, time.Now().Add(time.Second)); err != nil {
		t.Fatal("rearm INFO cutoff", err)
	}
	due, err = store.PeekDue(ctx, worker, process.Epoch)
	if err != nil || due == nil || due.Member != outMember {
		t.Fatal("INFO rearmed cutoff", err)
	}
	if err = dispatchRedisDialogOutputCutoff(ctx, store, due, time.Now().Add(time.Second)); err != nil {
		t.Fatal("INFO metadata reconcile", err)
	}
	after, err := store.Read(ctx, identity, procField, "r:dialog", outField)
	if err != nil || len(after.Records[outField]) != 0 {
		t.Fatal("INFO metadata retained", err)
	}
	p, err := readPrivateProcedureRecord(after.Records[procField])
	if err != nil || len(p.Outputs) != 0 {
		t.Fatal("INFO procedure output retained", err)
	}
	d, err := readPrivateDialogRecord(after.Records["r:dialog"])
	if err != nil || len(d.Outputs) != 0 {
		t.Fatal("INFO dialog output retained", err)
	}
}
