package engine

import (
	"bytes"
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/emiago/sipgo/sip/portable"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/nativepb"
	"sipbridge/internal/redisclient"
	"sipbridge/internal/rediscore"
	"sipbridge/internal/selectedmessage"
)

func callByeOutputFixture(t *testing.T, ingress ...bool) (*rediscore.Store, *pb.FlowWrite, rediscore.OutputRef, *nativepb.Output, *rediscore.Connection) {
	t.Helper()
	address := os.Getenv("SIPBRIDGE_CORE_TEST_REDIS")
	if address == "" {
		t.Skip("isolated Redis fixture required")
	}
	ctx := context.Background()
	client := redis.NewClient(&redis.Options{Addr: address, MaxRetries: -1})
	root := "sipstate:test:call-output-" + uuid.NewString() + ":"
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
	process := &pb.ProcessRef{InstanceId: "new-worker", Epoch: "epoch"}
	worker, err := nativeWorkerReference(process)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		defer client.Close()
		for _, name := range []string{"profile", "capacity", "selectors", "owner-expiry", "coordinator", "worker:" + worker, "timers:" + worker,
			"ready:" + worker, "context:call", "deadlines:call"} {
			if e := client.Del(ctx, root+"core:"+name).Err(); e != nil {
				t.Error(e)
			}
		}
	})
	connection, err := rediscore.NewConnection(rediscore.ConnectionConfig{Redis: redisclient.Options{Address: address},
		Profile: rediscore.ProfileConfig{Root: root, Service: "test", Generation: "one", IdentityKey: bytes.Repeat([]byte{7}, 32), RecordBudget: 4 << 20}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	store, err := connection.Refresh(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_, err = store.RenewWorker(ctx, rediscore.WorkerLeaseRequest{Worker: worker, Epoch: process.Epoch, PrimaryRunID: primary, Acquire: true})
	if err != nil {
		t.Fatal(err)
	}
	dialog := nativeEstablishedDialogFixture(t)
	refs := nativeDialogRefs{Local: &nativeInviteRefs{Request: "r:invite", Final: "r:final", ACK: "r:ack"}}
	dialogData, err := encodeNativeDialog(dialog, refs, &nativepb.Dialog{Transaction: "original"})
	if err != nil {
		t.Fatal(err)
	}
	old := &pb.ProcessRef{InstanceId: "old-worker", Epoch: "old"}
	oldFlow := &pb.FlowRef{Ingress: old, ConnectionId: "old", SourceIp: "192.0.2.2", SourcePort: 5061, Listener: "sip.example.test:5061", Transport: "tls"}
	flow := &pb.FlowRef{Ingress: process, ConnectionId: "new", SourceIp: "192.0.2.3", SourcePort: 5061, Listener: "sip.example.test:5061", Transport: "tls"}
	authority := connection.Authority()
	current := &nativepb.Context{Context: &pb.ContextRef{Id: "call", Owner: old, State: &pb.StateRef{Authority: authority, GroupId: "call", Incarnation: "inc", CallId: dialog.CallID}},
		Kind: pb.ContextSnapshot_UAC, Application: &pb.ProcessRef{InstanceId: "absent-controller", Epoch: "old"}, Flow: oldFlow,
		CallId: dialog.CallID, LocalTag: dialog.LocalTag, RemoteTag: dialog.RemoteTag, Transport: "tls", Dialogs: []string{"r:dialog"}}
	contextData, err := encodeNativeContext(current)
	if err != nil {
		t.Fatal(err)
	}
	changes := []rediscore.Change{{Field: "r:context", Value: contextData}, {Field: "r:dialog", Value: dialogData}}
	for field, message := range map[string]portable.Message{"r:invite": dialog.LocalInvite.Request, "r:final": *dialog.LocalInvite.Final, "r:ack": *dialog.LocalInvite.ACK} {
		data, e := selectedmessage.Marshal(message)
		if e != nil {
			t.Fatal(e)
		}
		changes = append(changes, rediscore.Change{Field: field, Value: data})
	}
	bye := nativePeerBye(t, dialog, 12)
	message := portable.RestoreMessage(bye.WireBytes(), "tls", "192.0.2.3:5061", "")
	selectors, err := nativeSelectorIDs(message, false, false)
	if err != nil || len(selectors) != 2 {
		t.Fatal("missing dialog selector", err)
	}
	_, err = store.Commit(ctx, rediscore.Publication{Identity: rediscore.Identity{Context: "call", Incarnation: "inc"}, Create: true,
		Deadline: time.Now().Add(time.Second), CreditDelta: 1 << 20, Changes: changes,
		Selectors: []rediscore.SelectorChange{{Selector: selectors[1], Record: "r:dialog"}}})
	if err != nil {
		t.Fatal(err)
	}
	assignment := rediscore.TimerAssignment{Worker: worker, Epoch: process.Epoch, Term: process.Epoch}
	if len(ingress) != 0 && ingress[0] {
		var handled bool
		handled, err = matchRedisCallCompletion(ctx, store, process, flow, message, bytes.Repeat([]byte{7}, 32), assignment, time.Now().Add(time.Second))
		if !handled {
			t.Fatal("native BYE ingress was not handled")
		}
	} else {
		var selected *selectedRedisCall
		selected, err = selectRedisCallDialog(ctx, store, process, flow, message, time.Now().Add(time.Second))
		if err != nil {
			t.Fatal(err)
		}
		err = publishRedisCallRemoteBye(ctx, store, selected, flow, bytes.Repeat([]byte{7}, 32), assignment, time.Now().Add(time.Second))
	}
	if err != nil {
		t.Fatal(err)
	}
	ready, err := nativeReadyWrites(ctx, store, worker, process.Epoch, 1)
	if err != nil || len(ready) != 1 {
		t.Fatal("BYE response not ready", err)
	}
	write := ready[0]
	reserved, err := (nativeRedisWriter{store: store}).Reserve(ctx, write)
	if err != nil || reserved.Known || len(reserved.Wire) == 0 {
		t.Fatal("BYE response not reserved", err)
	}
	ref, err := nativeOutputRef(write)
	if err != nil {
		t.Fatal(err)
	}
	proof, err := store.InspectOutput(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	metadata, err := decodeNativeOutput(proof.Metadata)
	if err != nil {
		t.Fatal(err)
	}
	return store, write, ref, metadata, connection
}

func TestRedisNativeCallIngressAndByeCompletionWithoutController(t *testing.T) {
	store, write, ref, metadata, _ := callByeOutputFixture(t, true)
	ctx := context.Background()
	assignment := rediscore.TimerAssignment{Worker: ref.Worker, Epoch: ref.Epoch, Term: ref.Epoch}
	before, err := store.ObserveContext(ctx, ref.Context, ref.Incarnation, "r:context")
	if err != nil {
		t.Fatal(err)
	}
	event, err := store.PeekContextDeadline(ctx, before.Identity, before.View.Revision, "")
	if err != nil || event == nil || !strings.HasPrefix(event.Member, "event-") {
		t.Fatal("missing native end-event duty", event, err)
	}
	member := "out-call-bye-output-" + strings.TrimPrefix(metadata.Procedure, "r:call-bye-")
	_, err = store.Commit(ctx, rediscore.Publication{Identity: before.Identity, Revision: before.View.Revision, Deadline: time.Now().Add(time.Second), SpendReservation: true,
		Timers: &rediscore.TimerMutation{Expected: before.View.Timer, Target: before.View.Timer, Changes: []rediscore.TimerChange{{Member: member, Delete: true}}}})
	if err != nil {
		t.Fatal(err)
	}
	if err = completeNativeOutput(ctx, store, write, &pb.DeliveryResult{Effect: write.Effect, Outcome: pb.DeliveryResult_WRITTEN}, assignment); err != nil {
		t.Fatal(err)
	}
	selected, err := store.ObserveContext(ctx, ref.Context, ref.Incarnation, "r:context", metadata.Procedure)
	if err != nil {
		t.Fatal(err)
	}
	head, err := store.PeekContextDeadline(ctx, selected.Identity, selected.View.Revision, event.Member)
	if err != nil || head == nil || head.Member != member || head.DeadlineMS != int64(write.Permit.Authorization.DeadlineMs) {
		t.Fatal("definite BYE completion did not restore original output cutoff", head, err)
	}
	current, err := decodeNativeContext(selected.View.Records["r:context"])
	if err != nil || !current.Ended || !bytes.Equal(before.View.Records["r:context"], selected.View.Records["r:context"]) {
		t.Fatal("native ingress/settlement changed business authority or delivery", err)
	}
	if err = acknowledgeRedisCallByeOutput(ctx, store, write, time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err = store.InspectOutput(ctx, ref); !errors.Is(err, rediscore.ErrAbsent) {
		t.Fatal("dispatcher did not consume and release definite proof", err)
	}
}

func TestRedisCallByeDefiniteConsumptionPrecedesProofRelease(t *testing.T) {
	for _, outcome := range []pb.DeliveryResult_Outcome{pb.DeliveryResult_WRITTEN, pb.DeliveryResult_NOT_WRITTEN} {
		t.Run(outcome.String(), func(t *testing.T) {
			store, write, ref, metadata, _ := callByeOutputFixture(t)
			ctx := context.Background()
			if err := acknowledgeRedisCallByeOutput(ctx, store, write, time.Now().Add(time.Second)); err == nil {
				t.Fatal("started attempt was acknowledged")
			}
			err := completeRedisCallByeOutput(ctx, store, write, &pb.DeliveryResult{Effect: write.Effect, Outcome: outcome})
			if err != nil {
				t.Fatal(err)
			}
			proof, err := store.InspectOutput(ctx, ref)
			if err != nil || !proof.Settled || proof.Acknowledged {
				t.Fatal("definite proof released before consumer return", err)
			}
			if err = acknowledgeRedisCallByeOutput(ctx, store, write, time.Now().Add(time.Second)); err != nil {
				t.Fatal(err)
			}
			if _, err = store.InspectOutput(ctx, ref); !errors.Is(err, rediscore.ErrAbsent) {
				t.Fatal("acknowledged proof was not released", err)
			}
			selected, err := store.ObserveContext(ctx, ref.Context, ref.Incarnation, metadata.Procedure)
			if err != nil {
				t.Fatal(err)
			}
			procedure, err := readPrivateProcedureRecord(selected.View.Records[metadata.Procedure])
			if err != nil || !procedure.OutputConsumed || procedure.FinalResponseWrittenAcknowledged != (outcome == pb.DeliveryResult_WRITTEN) {
				t.Fatal("native marker lost exact physical outcome", err)
			}
		})
	}
}

func TestRedisCallByeUnknownRetainsProofAndNoConsumption(t *testing.T) {
	store, write, ref, metadata, _ := callByeOutputFixture(t)
	ctx := context.Background()
	if err := completeRedisCallByeOutput(ctx, store, write, &pb.DeliveryResult{Effect: write.Effect, Outcome: pb.DeliveryResult_UNKNOWN}); err != nil {
		t.Fatal(err)
	}
	if err := acknowledgeRedisCallByeOutput(ctx, store, write, time.Now().Add(time.Second)); err == nil {
		t.Fatal("uncertain attempt was acknowledged")
	}
	proof, err := store.InspectOutput(ctx, ref)
	if err != nil || proof.State != rediscore.OutputUnknown || proof.Settled || proof.Acknowledged {
		t.Fatal("uncertain physical proof changed", err)
	}
	selected, err := store.ObserveContext(ctx, ref.Context, ref.Incarnation, metadata.Procedure)
	if err != nil {
		t.Fatal(err)
	}
	procedure, err := readPrivateProcedureRecord(selected.View.Records[metadata.Procedure])
	if err != nil || procedure.OutputConsumed || procedure.FinalResponseWrittenAcknowledged {
		t.Fatal("uncertainty became native consumption", err)
	}
	reserved, err := (nativeRedisWriter{store: store}).Reserve(ctx, write)
	if err != nil || !reserved.Known || reserved.Outcome != pb.DeliveryResult_UNKNOWN {
		t.Fatal("uncertain attempt authorized another write", err)
	}
}

func TestRedisNativeLatePhysicalProofDoesNotResumeFencedWork(t *testing.T) {
	store, write, ref, metadata, connection := callByeOutputFixture(t)
	ctx := context.Background()
	if err := (nativeRedisWriter{store: store}).Complete(ctx, write, &pb.DeliveryResult{Effect: write.Effect, Outcome: pb.DeliveryResult_UNKNOWN}); err != nil {
		t.Fatal(err)
	}
	driver, err := rediscore.NewDriver(rediscore.DriverConfig{Connection: connection, Worker: ref.Worker, Epoch: ref.Epoch,
		Due:          func(context.Context, *rediscore.Store, *rediscore.DueContext) error { return nil },
		ReadyOutput:  func(context.Context, *rediscore.Store, rediscore.ReadyOutput) error { return nil },
		OwnerExpired: func(context.Context, *rediscore.Store, rediscore.ExpiredApplicationContext) error { return nil }})
	if err != nil {
		t.Fatal(err)
	}
	defer driver.Close()
	process, stop := context.WithCancel(ctx)
	defer stop()
	e := &redisNative{ctx: process, driver: driver, connection: connection}
	request, cancel := context.WithCancel(ctx)
	cancel()
	if err = e.Complete(request, write, &pb.DeliveryResult{Effect: write.Effect, Outcome: pb.DeliveryResult_WRITTEN}); err != nil {
		t.Fatal("cancelled request lost its exact late physical fact", err)
	}
	proof, err := store.InspectOutput(ctx, ref)
	if err != nil || proof.State != rediscore.OutputWritten || proof.Settled || proof.Acknowledged {
		t.Fatal("late fact started a native continuation or failed to resolve uncertainty", err)
	}
	if e.Ready() == nil {
		t.Fatal("physical proof recording reacquired the worker")
	}
	selected, err := store.ObserveContext(ctx, ref.Context, ref.Incarnation, metadata.Procedure)
	if err != nil {
		t.Fatal(err)
	}
	procedure, err := readPrivateProcedureRecord(selected.View.Records[metadata.Procedure])
	if err != nil || procedure.OutputConsumed || procedure.FinalResponseWrittenAcknowledged {
		t.Fatal("proof-only path marked native consumption", err)
	}
}
