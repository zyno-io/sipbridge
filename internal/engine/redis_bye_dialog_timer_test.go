package engine

import (
	"bytes"
	"context"
	"errors"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
	"github.com/redis/go-redis/v9"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/rediscore"
)

func TestRedisNativeBYEFinalCancelsUnacknowledgedInviteTimer(t *testing.T) {
	for _, status := range []uint32{200, 481} {
		t.Run(strconv.Itoa(int(status)), func(t *testing.T) {
			testRedisNativeInitialInviteJSONDecisionAndRespond(t, false, true, status)
		})
	}
}

// Keep the original ACK horizon: the immediate shared-index assertion catches
// the producer omission before waiting to prove that it cannot fence the worker.
func testRedisBYEFinalCancelsACK(t *testing.T, ctx context.Context, e *redisNative, ws *websocket.Conn, hello *pb.SessionHelloAck, presentation *pb.RequestPresented, store *rediscore.Store, client *redis.Client, root string, written, retried <-chan []byte, status uint32) {
	t.Helper()
	ref := presentation.Snapshot.Context
	dialogField := "r:dialog-" + presentation.RequestId
	selected, err := store.ObserveContext(ctx, ref.Id, ref.State.Incarnation, dialogField)
	if err != nil {
		t.Fatal(err)
	}
	dialog, err := readPrivateDialogRecord(selected.View.Records[dialogField])
	if err != nil {
		t.Fatal(err)
	}
	ack := dialog.Native.Timers.Values[string(portable.TimerACK)]
	member := callDialogTimerMember(dialogField, portable.TimerACK)
	score, err := client.ZScore(ctx, root+"core:deadlines:"+ref.Id, member).Result()
	if err != nil || ack == nil || !ack.Active || int64(score) != ack.At {
		t.Fatal("initial ACK timer was not actually indexed", err)
	}
	command := &pb.Command{Context: ref, Sequence: 4, Admission: &pb.CommandAdmission{DirectoryEpoch: "directory", ExpiresAtMs: uint64(time.Now().Add(2 * time.Second).UnixMilli()), SelectedFlow: presentation.Flow}, Action: &pb.Command_DialogRequest{DialogRequest: &pb.DialogRequest{Method: "BYE"}}}
	writeControllerEnvelope(t, ctx, ws, &pb.Envelope{SessionId: hello.SessionId, CorrelationId: "bye-before-ack", RemainingBudgetMs: 2000, Payload: &pb.Envelope_Command{Command: command}})
	var accepted *pb.OperationResult
	for accepted == nil {
		frame := readControllerEnvelope(t, ctx, ws)
		if ping := frame.GetPing(); ping != nil {
			writeControllerEnvelope(t, ctx, ws, &pb.Envelope{SessionId: hello.SessionId, CorrelationId: frame.CorrelationId, Payload: &pb.Envelope_Pong{Pong: ping}})
			continue
		}
		if frame.CorrelationId != "bye-before-ack" {
			t.Fatal("unexpected BYE reply")
		}
		accepted = frame.GetResult()
	}
	if accepted.Certainty != pb.OperationResult_ACCEPTED || accepted.Error != "" || accepted.Request == nil {
		t.Fatal("application BYE was not written", accepted.GetCertainty())
	}
	var byeWire []byte
	select {
	case byeWire = <-written:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	parsed, err := sip.ParseMessage(byeWire)
	if err != nil {
		t.Fatal(err)
	}
	bye, ok := parsed.(*sip.Request)
	if !ok || bye.Method != sip.BYE {
		t.Fatal("actual BYE wire missing")
	}
	before, err := store.ObserveContext(ctx, ref.Id, ref.State.Incarnation, "r:result-4")
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := decodeNativeResult(before.View.Records["r:result-4"])
	if err != nil || receipt.Output == "" || receipt.Certainty != pb.OperationResult_ACCEPTED {
		t.Fatal("original written BYE receipt missing", err)
	}
	outputView, err := store.Read(ctx, before.Identity, receipt.Output)
	if err != nil {
		t.Fatal(err)
	}
	originalOutput := bytes.Clone(outputView.Records[receipt.Output])
	output, err := decodeNativeOutput(originalOutput)
	if err != nil {
		t.Fatal(err)
	}
	procView, err := store.Read(ctx, before.Identity, output.Procedure)
	if err != nil {
		t.Fatal(err)
	}
	procedure, err := readPrivateProcedureRecord(procView.Records[output.Procedure])
	if err != nil || procedure.Native.Timers.Values[string(portable.TimerF)] == nil {
		t.Fatal("original BYE F missing", err)
	}
	originalF := procedure.Native.Timers.Values[string(portable.TimerF)].At
	ended, failures := make(chan struct{}, 1), make(chan error, 1)
	go func() {
		for {
			frame, readErr := readACKTimeoutEnvelope(ctx, ws)
			if readErr != nil {
				failures <- readErr
				return
			}
			if ping := frame.GetPing(); ping != nil {
				if writeErr := writeACKTimeoutEnvelope(ctx, ws, &pb.Envelope{SessionId: hello.SessionId, CorrelationId: frame.CorrelationId, Payload: &pb.Envelope_Pong{Pong: ping}}); writeErr != nil {
					failures <- writeErr
					return
				}
			}
			if event := frame.GetDialogEvent(); event != nil {
				if writeErr := writeACKTimeoutEnvelope(ctx, ws, &pb.Envelope{SessionId: hello.SessionId, CorrelationId: frame.CorrelationId, Payload: &pb.Envelope_Result{Result: &pb.OperationResult{Certainty: pb.OperationResult_ACCEPTED}}}); writeErr != nil {
					failures <- writeErr
					return
				}
				if event.Kind == pb.DialogEvent_ENDED {
					select {
					case ended <- struct{}{}:
					default:
					}
				}
			}
		}
	}()
	response := sip.NewResponseFromRequest(bye, int(status), reason(status), nil)
	message, err := portable.ParseMessage([]byte(response.String()), presentation.Flow.Transport, net.JoinHostPort(presentation.Flow.SourceIp, strconv.Itoa(int(presentation.Flow.SourcePort))), "")
	if err != nil {
		t.Fatal(err)
	}
	if handled, completeErr := e.TryNativeCompletionParsed(ctx, presentation.Flow, &pb.SipMessage{Wire: message.WireBytes()}, response, message); completeErr != nil || !handled {
		t.Fatal("actual BYE final did not complete", completeErr)
	}
	if _, err = client.ZScore(ctx, root+"core:deadlines:"+ref.Id, member).Result(); !errors.Is(err, redis.Nil) {
		t.Fatal("BYE final retained canceled shared ACK timer", err)
	}
	after, err := store.ObserveContext(ctx, ref.Id, ref.State.Incarnation, "r:context", dialogField, "r:result-4", receipt.Output, output.Procedure)
	if err != nil {
		t.Fatal(err)
	}
	finalDialog, err := readPrivateDialogRecord(after.View.Records[dialogField])
	if err != nil || finalDialog.Native.Phase != string(portable.DialogEnded) {
		t.Fatal("BYE final did not end the selected dialog", err)
	}
	finalACK := finalDialog.Native.Timers.Values[string(portable.TimerACK)]
	if finalACK == nil || finalACK.Active || finalACK.At != ack.At {
		t.Fatal("SDK ACK timer was not canceled under its original bound")
	}
	current, err := decodeNativeContext(after.View.Records["r:context"])
	if err != nil || !current.Ended || current.InitialAck {
		t.Fatal("BYE final changed missing-ACK facts", err)
	}
	finalResult, err := decodeNativeResult(after.View.Records["r:result-4"])
	if err != nil || finalResult.Certainty != pb.OperationResult_COMPLETED || finalResult.Pending || finalResult.Error != "" || finalResult.SipStatus != status || finalResult.OperationId != receipt.OperationId || finalResult.CutoffMs != receipt.CutoffMs || finalResult.Request == "" || finalResult.Response == "" {
		t.Fatal("BYE final changed original receipt identity or certainty", err)
	}
	if retained := after.View.Records[receipt.Output]; len(retained) != 0 && !bytes.Equal(retained, originalOutput) {
		t.Fatal("BYE final changed original physical output")
	}
	if retained := after.View.Records[output.Procedure]; len(retained) != 0 {
		p, decodeErr := readPrivateProcedureRecord(retained)
		if decodeErr != nil {
			t.Fatal(decodeErr)
		}
		f := p.Native.Timers.Values[string(portable.TimerF)]
		if f == nil || f.Active || f.At != originalF {
			t.Fatal("BYE final changed its original F cutoff")
		}
	}
	select {
	case <-ended:
	case readErr := <-failures:
		t.Fatal(readErr)
	case <-ctx.Done():
		t.Fatal("BYE ENDED event missing", ctx.Err())
	}
	for time.Now().Before(time.UnixMilli(ack.At).Add(200 * time.Millisecond)) {
		if readyErr := e.Ready(); readyErr != nil {
			t.Fatal("canceled ACK timer fenced worker", readyErr)
		}
		select {
		case wire := <-written:
			if bytes.HasPrefix(wire, []byte("BYE ")) {
				t.Fatal("ended dialog generated a second BYE")
			}
		case <-retried:
		case readErr := <-failures:
			t.Fatal(readErr)
		case <-time.After(100 * time.Millisecond):
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	if err = e.Ready(); err != nil {
		t.Fatal("worker unavailable past original ACK horizon", err)
	}
}
