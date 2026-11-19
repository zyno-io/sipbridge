package engine

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/emiago/sipgo/sip/portable"
	"github.com/redis/go-redis/v9"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/rediscore"
)

func TestRedisNativeDialogCommandPrepublicationConflict(t *testing.T) {
	t.Run("definite-rejection-can-retry", func(t *testing.T) { testRedisNativeOpenUACCommand(t, 23) })
	t.Run("uncertain-read-stays-unknown", func(t *testing.T) { testRedisNativeOpenUACCommand(t, 24) })
	t.Run("accepted-command-read-failure-stays-unknown", func(t *testing.T) { testRedisNativeOpenUACCommand(t, 25) })
}

func testRedisDialogCommandConflict(t *testing.T, ctx context.Context, e *redisNative, ws *websocket.Conn, hello *pb.SessionHelloAck, flow *pb.FlowRef, ref *pb.ContextRef, identity rediscore.Identity, root string, proxy *redisReservationFaultProxy, mode int, written <-chan []byte) {
	t.Helper()
	read := func(correlation string) *pb.OperationResult {
		t.Helper()
		for {
			frame := readControllerEnvelope(t, ctx, ws)
			if frame.GetPing() != nil {
				writeControllerEnvelope(t, ctx, ws, &pb.Envelope{SessionId: hello.SessionId, CorrelationId: frame.CorrelationId, Payload: &pb.Envelope_Pong{Pong: frame.GetPing()}})
				continue
			}
			if frame.GetDialogEvent() != nil {
				writeControllerEnvelope(t, ctx, ws, &pb.Envelope{SessionId: hello.SessionId, CorrelationId: frame.CorrelationId, Payload: &pb.Envelope_Result{Result: &pb.OperationResult{Certainty: pb.OperationResult_ACCEPTED}}})
				continue
			}
			if frame.CorrelationId != correlation || frame.GetResult() == nil {
				t.Fatalf("unexpected dialog command reply %T", frame.Payload)
			}
			return frame.GetResult()
		}
	}
	admin := redis.NewClient(&redis.Options{Addr: proxy.upstream, MaxRetries: -1})
	defer admin.Close()
	profile, err := admin.Get(ctx, root+"core:profile").Bytes()
	if err != nil {
		t.Fatal(err)
	}
	info, err := admin.Info(ctx, "server").Result()
	if err != nil {
		t.Fatal(err)
	}
	primary := ""
	for _, line := range strings.Split(info, "\r\n") {
		if value, found := strings.CutPrefix(line, "run_id:"); found {
			primary = value
		}
	}
	independent, err := rediscore.New(admin, rediscore.Config{Root: root, Profile: profile, PrimaryRunID: primary, RecordBudget: 32 << 20})
	if err != nil {
		t.Fatal(err)
	}
	var hits atomic.Int32
	proxy.mu.Lock()
	proxy.beforeCommand = func(frame redisFaultFrame) error {
		args := frame.children
		if frame.kind != '*' || len(args) < 8 || string(args[0].scalar) != "evalsha" && string(args[0].scalar) != "eval" || string(args[3].scalar) != root+"core:profile" {
			return nil
		}
		if mode == 25 {
			if string(args[4].scalar) != root+"core:context:"+identity.Context {
				return nil
			}
			found := false
			for _, arg := range args {
				found = found || string(arg.scalar) == "observe-witness"
			}
			if !found {
				return nil
			}
		} else if string(args[4].scalar) != root+"core:selectors" || len(args[6].scalar) != 33 || args[6].scalar[0] != byte(rediscore.SelectorDialog) {
			return nil
		}
		hits.Add(1)
		if mode != 23 {
			return errors.New("fixture interrupted Redis read")
		}
		for attempt := 0; attempt < 8; attempt++ {
			view, x := independent.Read(ctx, identity, "r:context")
			if x != nil {
				return x
			}
			current, x := decodeNativeContext(view.Records["r:context"])
			if x != nil {
				return x
			}
			current.Revision++
			data, x := encodeNativeContext(current)
			if x != nil {
				return x
			}
			_, x = independent.Commit(ctx, rediscore.Publication{Identity: identity, Revision: view.Revision, Deadline: time.Now().Add(time.Second), SpendReservation: true, Changes: []rediscore.Change{{Field: "r:context", Value: data}}})
			if errors.Is(x, rediscore.ErrConflict) && !errors.Is(x, rediscore.ErrUnknown) {
				continue
			}
			if x != nil {
				t.Errorf("inject definite publication conflict: %v", x)
			}
			return x
		}
		t.Error("fixture could not inject definite publication conflict")
		return errors.New("fixture contention")
	}
	proxy.mu.Unlock()
	until := time.Now().Add(2 * time.Second)
	command := &pb.Command{Context: ref, Sequence: 2, Admission: &pb.CommandAdmission{DirectoryEpoch: "directory", ExpiresAtMs: uint64(until.UnixMilli()), SelectedFlow: flow}, Action: &pb.Command_DialogRequest{DialogRequest: &pb.DialogRequest{Method: "NOTIFY", Headers: []*pb.Header{{Name: "Event", Value: "refer;id=2"}, {Name: "Subscription-State", Value: "terminated;reason=noresource"}, {Name: "Content-Type", Value: "message/sipfrag;version=2.0"}}, Body: []byte("SIP/2.0 200 OK\r\n")}}}
	writeControllerEnvelope(t, ctx, ws, &pb.Envelope{SessionId: hello.SessionId, CorrelationId: "conflicted-notify", RemainingBudgetMs: 2000, Payload: &pb.Envelope_Command{Command: command}})
	result := read("conflicted-notify")
	proxy.mu.Lock()
	proxy.beforeCommand = nil
	proxy.mu.Unlock()
	after, err := independent.Read(ctx, identity, "r:context", "r:result-2")
	if err != nil {
		t.Fatal(err)
	}
	current, err := decodeNativeContext(after.Records["r:context"])
	if err != nil {
		t.Fatal(err)
	}
	if mode != 23 {
		if result.Certainty != pb.OperationResult_UNKNOWN || hits.Load() != 1 {
			t.Fatal("uncertain read was retried or reported as a rejection", result.Certainty, hits.Load())
		}
		if mode == 25 && (current.CommandSequence != 2 || len(after.Records["r:result-2"]) == 0 || len(current.NoninitialProcedures) != 1) {
			t.Fatal("accepted command lost its native ownership")
		}
		return
	}
	if result.Certainty != pb.OperationResult_NOT_ACCEPTED || hits.Load() != 3 || current.CommandSequence != 1 || len(current.NoninitialProcedures) != 0 || len(after.Records["r:result-2"]) != 0 {
		t.Fatal("definite prepublication conflicts did not preserve an unaccepted command", result.Certainty, hits.Load(), current.CommandSequence)
	}
	select {
	case wire := <-written:
		t.Fatalf("rejected NOTIFY emitted bytes %q", wire)
	default:
	}
	remainingMS := time.Until(until) / time.Millisecond
	if remainingMS <= 0 {
		t.Fatal("original command deadline elapsed before retry")
	}
	remaining := uint32(remainingMS)
	writeControllerEnvelope(t, ctx, ws, &pb.Envelope{SessionId: hello.SessionId, CorrelationId: "retry-notify", RemainingBudgetMs: remaining, Payload: &pb.Envelope_Command{Command: command}})
	select {
	case wire := <-written:
		if !bytes.HasPrefix(wire, []byte("NOTIFY ")) {
			t.Fatal("retried command did not emit NOTIFY")
		}
		message := portable.RestoreMessage(wire, flow.Transport, "", "")
		response, x := portable.BuildResponseWithHeaders(message, 200, "OK", nil, "", "", nil)
		if x != nil {
			t.Fatal(x)
		}
		response = portable.RestoreMessage(response.WireBytes(), flow.Transport, "192.0.2.12:5061", "")
		if handled, x := e.tryUACResponseCompletion(ctx, flow, response, nil); x != nil || !handled {
			t.Fatal("retried NOTIFY response", handled, x)
		}
	case <-ctx.Done():
		t.Fatal("retried NOTIFY was not written", ctx.Err())
	}
	result = read("retry-notify")
	if result.Certainty != pb.OperationResult_COMPLETED || result.SipStatus != 200 || result.Snapshot.GetCommandSequence() != 2 || result.Snapshot.Ended {
		t.Fatal("same-sequence retry did not complete on the live dialog", result)
	}
}
