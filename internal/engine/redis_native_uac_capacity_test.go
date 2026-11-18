package engine

import (
	"bytes"
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/emiago/sipgo/sip/portable"
	"github.com/redis/go-redis/v9"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/rediscore"
)

func TestRedisNativeUACCapacityAfterFailedProposals(t *testing.T) {
	testRedisNativeOpenUACCommand(t, 4)
}

// Keep the failed calls' real native timers and physical duties. A new proposal
// starts after FAILED without waiting for the preceding native ACK write.
func testRedisNativeUACCapacityAfterFailures(t *testing.T, ctx context.Context, e *redisNative, ws *websocket.Conn, hello *pb.SessionHelloAck, flow *pb.FlowRef, send func(string, string, *pb.FlowRef) *pb.OperationResult, written <-chan []byte, client *redis.Client, root string, ids []string) {
	t.Helper()
	acks := make(map[string]bool)
	snapshots := make([]*pb.ContextSnapshot, 0, len(ids))
	observeACK := func(wire []byte) bool {
		t.Helper()
		if !bytes.HasPrefix(wire, []byte("ACK ")) {
			return false
		}
		message := portable.RestoreMessage(wire, flow.Transport, "", "")
		facts, err := message.Facts()
		if err != nil || acks[facts.CallID] {
			t.Fatal("invalid or duplicate physical ACK", err)
		}
		acks[facts.CallID] = true
		return true
	}
	var last *pb.OperationResult
	for index, id := range ids {
		last = send(id, id, flow)
		if last == nil || last.Certainty != pb.OperationResult_ACCEPTED || last.Error != "" || last.Request == nil || last.Snapshot == nil {
			t.Fatalf("fresh proposal %d was not admitted: %v", index+1, last)
		}
		snapshots = append(snapshots, last.Snapshot)
		var origin portable.Message
		for {
			select {
			case wire := <-written:
				if observeACK(wire) {
					continue
				}
				origin = portable.RestoreMessage(wire, flow.Transport, "", "")
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			break
		}
		facts, err := origin.Facts()
		if err != nil || facts.Method != "INVITE" || facts.CallID != last.Snapshot.CallId {
			t.Fatal("fresh native INVITE missing", err)
		}
		status, kind := 488, pb.DialogEvent_FAILED
		if index == len(ids)-1 {
			status, kind = 200, pb.DialogEvent_ANSWERED
		}
		response, err := portable.BuildResponseWithHeaders(origin, status, reason(uint32(status)), nil, "phone-tag", "<sip:phone@192.0.2.12:5061>", nil)
		if err != nil {
			t.Fatal(err)
		}
		response = portable.RestoreMessage(response.WireBytes(), flow.Transport, "192.0.2.12:5061", "")
		if handled, err := e.tryUACResponseCompletion(ctx, flow, response, nil); err != nil || !handled {
			t.Fatal("matched proposal final", index+1, handled, err)
		}
		for {
			value := readControllerEnvelope(t, ctx, ws)
			if ping := value.GetPing(); ping != nil {
				writeControllerEnvelope(t, ctx, ws, &pb.Envelope{SessionId: hello.SessionId, CorrelationId: value.CorrelationId, Payload: &pb.Envelope_Pong{Pong: ping}})
				continue
			}
			if event := value.GetDialogEvent(); event != nil {
				writeControllerEnvelope(t, ctx, ws, &pb.Envelope{SessionId: hello.SessionId, CorrelationId: value.CorrelationId, Payload: &pb.Envelope_Result{Result: &pb.OperationResult{Certainty: pb.OperationResult_ACCEPTED}}})
				if event.Snapshot.Context.Id == id && event.Kind == kind {
					break
				}
			}
		}
	}
	for len(acks) < len(ids) {
		select {
		case wire := <-written:
			if !observeACK(wire) {
				t.Fatal("unexpected SIP output after proposal finals")
			}
		case <-ctx.Done():
			t.Fatal("admitted native ACK missing", ctx.Err())
		}
	}
	if !acks[last.Snapshot.CallId] {
		t.Fatal("successful final has no actual ACK")
	}
	appID, err := nativeApplicationReference(e.cfg.StateIdentityKey, last.Snapshot.Application)
	if err != nil {
		t.Fatal(err)
	}
	identity := rediscore.Identity{Context: last.Snapshot.Context.Id, Incarnation: last.Snapshot.Context.State.Incarnation, Owner: appID, Term: hello.ApplicationLease.Term}
	for {
		if err := e.Ready(); err != nil {
			t.Fatal("failed proposals fenced worker", err)
		}
		store, _, _, err := e.driver.Current()
		if err != nil {
			t.Fatal(err)
		}
		view, err := store.Read(ctx, identity, "r:context")
		if err != nil {
			t.Fatal(err)
		}
		current, err := decodeNativeContext(view.Records["r:context"])
		if err != nil {
			t.Fatal(err)
		}
		if current.Answered && current.InitialAck && !current.Ended {
			break
		}
		select {
		case <-time.After(time.Millisecond):
		case <-ctx.Done():
			t.Fatal("successful proposal has no positive ACK witness", ctx.Err())
		}
	}
	store, _, _, err := e.driver.Current()
	if err != nil {
		t.Fatal(err)
	}
	for _, snapshot := range snapshots[:len(snapshots)-1] {
		failed := identity
		failed.Context, failed.Incarnation = snapshot.Context.Id, snapshot.Context.State.Incarnation
		view, err := store.Read(ctx, failed, "r:context")
		if err != nil {
			t.Fatal(err)
		}
		current, err := decodeNativeContext(view.Records["r:context"])
		if err != nil || !current.Ended || len(current.InitialProcedures) != 1 {
			t.Fatal("failed proposal lost its retained native owner", err)
		}
		selected, err := store.Read(ctx, failed, current.InitialProcedures[0])
		if err != nil {
			t.Fatal(err)
		}
		procedure, err := readPrivateProcedureRecord(selected.Records[current.InitialProcedures[0]])
		if err != nil || procedure.Native.Phase != string(portable.Completed) || procedure.Native.Timers == nil ||
			procedure.Native.Timers.Values[string(portable.TimerD)] == nil || !procedure.Native.Timers.Values[string(portable.TimerD)].Active {
			t.Fatal("failed proposal lost its original native Timer D", err)
		}
	}
	capacity, err := client.HGetAll(ctx, root+"core:capacity").Result()
	if err != nil {
		t.Fatal(err)
	}
	contexts, countErr := strconv.Atoi(capacity["contexts"])
	charged, bytesErr := strconv.ParseInt(capacity["bytes"], 10, 64)
	if countErr != nil || bytesErr != nil || contexts != len(ids) || charged >= 1<<28 {
		t.Fatal("failed proposals lost native state or retained speculative fork headroom", capacity, countErr, bytesErr)
	}
}
