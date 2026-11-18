package engine

import (
	"bytes"
	"context"
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
	"sipbridge/internal/storagecodec"
)

func TestRedisLifecycleMessagePayloadAndExactOwners(t *testing.T) {
	for _, owner := range []string{"transaction", "result", "sole", "transferred", "expired"} {
		t.Run(owner, func(t *testing.T) { testRedisLifecycleMessageOwners(t, owner) })
	}
}

func testRedisLifecycleMessageOwners(t *testing.T, owner string) {
	address := os.Getenv("SIPBRIDGE_CORE_TEST_REDIS")
	if address == "" {
		t.Skip("isolated Redis fixture required")
	}
	transport, peer, websocket, ctx, session, _, _ := residentControlFixture(t)
	root := "sipstate:test:lifecycle-message-" + uuid.NewString() + ":"
	connection, err := rediscore.NewConnection(rediscore.ConnectionConfig{Redis: redisclient.Options{Address: address},
		Profile: rediscore.ProfileConfig{Root: root, Service: "test", Generation: "one", IdentityKey: bytes.Repeat([]byte{7}, 32), RecordBudget: 1 << 20}})
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	store, err := connection.Refresh(ctx)
	if err != nil {
		t.Fatal(err)
	}
	client := redis.NewClient(&redis.Options{Addr: address, MaxRetries: -1})
	t.Cleanup(func() {
		defer client.Close()
		for _, name := range []string{"profile", "capacity", "worker:worker", "timers:worker", "context:context", "deadlines:context"} {
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
	_, err = store.RenewWorker(ctx, rediscore.WorkerLeaseRequest{Worker: "worker", Epoch: "epoch", PrimaryRunID: primary, Acquire: true})
	if err != nil {
		t.Fatal(err)
	}
	id := rediscore.Identity{Context: "context", Incarnation: "incarnation"}
	assignment := rediscore.TimerAssignment{Worker: "worker", Epoch: "epoch", Term: "epoch"}
	request, err := portable.ParseMessage(stateTestRequest("INVITE", 1, "", "lifecycle-message").Wire, "tls", "", "")
	if err != nil {
		t.Fatal(err)
	}
	response, err := portable.ResponseFor(request, 180, "Ringing", nil, "remote")
	if err != nil {
		t.Fatal(err)
	}
	laterResponse, err := portable.ResponseFor(request, 183, "Session Progress", nil, "remote")
	if err != nil {
		t.Fatal(err)
	}
	facts, err := response.Facts()
	if err != nil {
		t.Fatal(err)
	}
	tx, err := portable.NewTransaction(portable.InviteClient, request, portable.DefaultTimers(), "")
	if err != nil {
		t.Fatal(err)
	}
	tx, _, err = portable.StepTransaction(tx, portable.Input{Kind: portable.Start, Now: 1000})
	if err != nil {
		t.Fatal(err)
	}
	tx, _, err = portable.StepTransaction(tx, portable.Input{Kind: portable.ReceiveResponse, Message: response, Now: 1001})
	if err != nil {
		t.Fatal(err)
	}
	ref := &pb.ContextRef{Id: id.Context, State: &pb.StateRef{Authority: connection.Authority(), GroupId: id.Context, Incarnation: id.Incarnation, CallId: facts.CallID}}
	current := &nativepb.Context{Context: ref, Application: peer.Application(), Kind: pb.ContextSnapshot_UAC, CallId: facts.CallID, CommandSequence: 1, InitialProcedures: []string{"r:procedure-anchor"}}
	contextData, err := encodeNativeContext(current)
	if err != nil {
		t.Fatal(err)
	}
	last := "r:message"
	owners := []string{"r:procedure-anchor"}
	if owner == "sole" || owner == "result" || owner == "expired" {
		last = "r:latest"
		owners = nil
	}
	if owner == "result" {
		owners = []string{"r:result-1"}
	}
	procedure, err := encodeNativeProcedure(tx, nativeProcedureRefs{Origin: "r:origin", LastResponse: last}, &nativepb.Procedure{RequestId: "anchor", Events: []string{"r:event-one"}})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	event := &nativepb.Lifecycle{Kind: pb.DialogEvent_PROVISIONAL, Snapshot: &pb.ContextSnapshot{Context: ref, Application: peer.Application(), Kind: current.Kind, CallId: facts.CallID},
		OperationId: "original", Message: "r:message", Root: "r:procedure-anchor", Owners: owners, DeadlineMs: now.Add(time.Minute).UnixMilli(), NextMs: now.Add(-time.Second).UnixMilli()}
	if owner == "expired" {
		event.DeadlineMs = now.Add(-time.Millisecond).UnixMilli()
	}
	projected, err := projectNativeMessage(response, false)
	if err != nil {
		t.Fatal(err)
	}
	event.Selectors = projected.Selectors
	eventData, err := encodeNativeLifecycle(event)
	if err != nil {
		t.Fatal(err)
	}
	originData, err := selectedmessage.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	messageData, err := selectedmessage.Marshal(response)
	if err != nil {
		t.Fatal(err)
	}
	if owner == "expired" {
		// Expiry needs ownership metadata only, even when the old packet
		// cannot be decoded. No callback may be constructed on this path.
		messageData = []byte("expired undecodable packet")
	}
	latestData, err := selectedmessage.Marshal(laterResponse)
	if err != nil {
		t.Fatal(err)
	}
	result, err := encodeNativeResult(&nativepb.Result{Certainty: pb.OperationResult_COMPLETED, SipStatus: 180, Response: "r:message", CutoffMs: now.Add(time.Minute).UnixMilli(),
		Sequence: 1, OperationId: "original", Fingerprint: bytes.Repeat([]byte{7}, 32), DecidingLeaseTerm: "term"})
	if err != nil {
		t.Fatal(err)
	}
	changes := []rediscore.Change{{Field: "r:context", Value: contextData}, {Field: "r:procedure-anchor", Value: procedure}, {Field: "r:origin", Value: originData}, {Field: "r:message", Value: messageData}, {Field: "r:event-one", Value: eventData}}
	if owner == "result" {
		changes = append(changes, rediscore.Change{Field: "r:result-1", Value: result})
	}
	if last == "r:latest" {
		changes = append(changes, rediscore.Change{Field: "r:latest", Value: latestData})
	}
	_, err = store.Commit(ctx, rediscore.Publication{Identity: id, Create: true, Deadline: now.Add(time.Second), CreditDelta: 32768, Changes: changes,
		Timers: &rediscore.TimerMutation{Target: assignment, Changes: []rediscore.TimerChange{{Member: "event-one", Due: time.UnixMilli(event.NextMs)}}}})
	if err != nil {
		t.Fatal(err)
	}
	due, err := store.PeekDue(ctx, "worker", "epoch")
	if err != nil || due == nil {
		t.Fatal("message event missing", err)
	}
	if owner == "sole" {
		view, x := store.Read(ctx, id, "r:context")
		if x != nil {
			t.Fatal(x)
		}
		unrooted, x := decodeNativeContext(view.Records["r:context"])
		if x != nil {
			t.Fatal(x)
		}
		unrooted.InitialProcedures = nil
		encoded, x := encodeNativeContext(unrooted)
		if x != nil {
			t.Fatal(x)
		}
		_, x = store.Commit(ctx, rediscore.Publication{Identity: id, Revision: view.Revision, Deadline: time.Now().Add(time.Second), Changes: []rediscore.Change{{Field: "r:context", Value: encoded}}})
		if x != nil {
			t.Fatal(x)
		}
		due, x = store.PeekDue(ctx, "worker", "epoch")
		if x != nil || due == nil {
			t.Fatal(x)
		}
		if _, _, x = selectRedisNativeLifecycle(ctx, store, due); x == nil {
			t.Fatal("unrooted message was allowed before controller I/O")
		}
		_, x = store.Commit(ctx, rediscore.Publication{Identity: id, Revision: due.Revision, Deadline: time.Now().Add(time.Second), Changes: []rediscore.Change{{Field: "r:context", Value: contextData}}})
		if x != nil {
			t.Fatal(x)
		}
		due, x = store.PeekDue(ctx, "worker", "epoch")
		if x != nil || due == nil {
			t.Fatal(x)
		}
	}
	e := &redisNative{control: transport.control, eventWork: make(chan struct{}, 8), events: make(map[redisNativeEventKey]struct{}, 8)}
	if owner == "expired" {
		_, public, x := selectRedisNativeLifecycle(ctx, store, due)
		if x != nil || public != nil {
			t.Fatal("expired event parsed or projected its packet", x)
		}
	}
	if err = e.dispatchLifecycle(ctx, store, due); err != nil {
		t.Fatal(err)
	}
	if owner != "expired" {
		notification := readControllerEnvelope(t, ctx, websocket)
		public := notification.GetDialogEvent()
		if public == nil || public.Message == nil || !bytes.Equal(public.Message.Wire, response.WireBytes()) || public.Kind != event.Kind || public.OperationId != event.OperationId {
			t.Fatal("message lifecycle did not preserve its frozen payload")
		}
		if owner == "transferred" {
			view, x := store.Read(ctx, id, "r:procedure-anchor", "r:event-one")
			if x != nil {
				t.Fatal(x)
			}
			anchor, x := readPrivateProcedureRecord(view.Records["r:procedure-anchor"])
			if x != nil {
				t.Fatal(x)
			}
			anchor.LastResponse = "r:latest"
			encoded, x := marshalNativeRecord(storagecodec.NativeProcedure, anchor)
			if x != nil {
				t.Fatal(x)
			}
			moved, x := decodeNativeLifecycle(view.Records["r:event-one"])
			if x != nil {
				t.Fatal(x)
			}
			moved.Owners = []string{"r:result-1"}
			encodedEvent, x := encodeNativeLifecycle(moved)
			if x != nil {
				t.Fatal(x)
			}
			_, x = store.Commit(ctx, rediscore.Publication{Identity: id, Revision: view.Revision, Deadline: time.Now().Add(time.Second), SpendReservation: true,
				Changes: []rediscore.Change{{Field: "r:procedure-anchor", Value: encoded}, {Field: "r:result-1", Value: result}, {Field: "r:event-one", Value: encodedEvent}, {Field: "r:latest", Value: latestData}}})
			if x != nil {
				t.Fatal(x)
			}
		}
		writeControllerEnvelope(t, ctx, websocket, &pb.Envelope{SessionId: session, CorrelationId: notification.CorrelationId,
			Payload: &pb.Envelope_Result{Result: &pb.OperationResult{Certainty: pb.OperationResult_ACCEPTED}}})
	}
	done := make(chan struct{})
	go func() { e.wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("message event acknowledgement did not finish")
	}
	finished, err := store.Read(ctx, id, "r:event-one", "r:message", "r:procedure-anchor")
	if err != nil {
		t.Fatal(err)
	}
	anchor, err := readPrivateProcedureRecord(finished.Records["r:procedure-anchor"])
	if err != nil || len(anchor.Events) != 0 || len(finished.Records["r:event-one"]) != 0 {
		t.Fatal("message event left its canonical ownership debt", err)
	}
	if (len(finished.Records["r:message"]) == 0) != (owner == "sole" || owner == "expired") {
		t.Fatal("message wire did not follow its current exact owners")
	}
}
