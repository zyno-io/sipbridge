package engine

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/emiago/sipgo/sip/portable"
	"google.golang.org/protobuf/encoding/protojson"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/rediscore"
)

func TestRedisNativeACKTimeoutPreservesExistingApplicationBYE(t *testing.T) {
	testRedisNativeInitialInviteJSONDecisionAndRespond(t, false, true)
}

// No ACK is sent. The actual original 32s ACK and BYE F deadlines run naturally;
// only the actual reserved writes establish physical evidence.
func testRedisACKTimeoutKeepsApplicationBYE(t *testing.T, ctx context.Context, e *redisNative, ws *websocket.Conn, hello *pb.SessionHelloAck, presentation *pb.RequestPresented, store *rediscore.Store, written, retried <-chan []byte) {
	t.Helper()
	ref := presentation.Snapshot.Context
	command := &pb.Command{Context: ref, Sequence: 4, Admission: &pb.CommandAdmission{DirectoryEpoch: "directory", ExpiresAtMs: uint64(time.Now().Add(2 * time.Second).UnixMilli()), SelectedFlow: presentation.Flow}, Action: &pb.Command_DialogRequest{DialogRequest: &pb.DialogRequest{Method: "BYE"}}}
	writeControllerEnvelope(t, ctx, ws, &pb.Envelope{SessionId: hello.SessionId, CorrelationId: "before-ack-timeout", RemainingBudgetMs: 2000, Payload: &pb.Envelope_Command{Command: command}})
	var result *pb.OperationResult
	for result == nil {
		frame := readControllerEnvelope(t, ctx, ws)
		if ping := frame.GetPing(); ping != nil {
			writeControllerEnvelope(t, ctx, ws, &pb.Envelope{SessionId: hello.SessionId, CorrelationId: frame.CorrelationId, Payload: &pb.Envelope_Pong{Pong: ping}})
			continue
		}
		if frame.CorrelationId != "before-ack-timeout" {
			t.Fatalf("unexpected BYE control frame %T", frame.Payload)
		}
		result = frame.GetResult()
	}
	if result.Certainty != pb.OperationResult_ACCEPTED || result.Error != "" || result.Request == nil || result.Request.Method != "BYE" {
		t.Fatal("actual application BYE not written", result.GetCertainty(), result.GetError())
	}
	select {
	case wire := <-written:
		if !bytes.HasPrefix(wire, []byte("BYE ")) {
			t.Fatal("application BYE write missing")
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	dialogField := "r:dialog-" + presentation.RequestId
	selected, err := store.ObserveContext(ctx, ref.Id, ref.State.Incarnation, "r:context", dialogField)
	if err != nil {
		t.Fatal(err)
	}
	dialog, err := readPrivateDialogRecord(selected.View.Records[dialogField])
	if err != nil {
		t.Fatal(err)
	}
	oldOrigin, oldKey := dialog.LocalBye, dialog.LocalTransaction
	if !strings.HasPrefix(oldOrigin, "r:app-bye-origin-") {
		t.Fatal("actual BYE association missing")
	}
	id := strings.TrimPrefix(oldOrigin, "r:app-bye-origin-")
	procField, outputField := "r:app-bye-"+id, "r:app-bye-output-"+id
	oldACK := dialog.Native.Timers.Values[string(portable.TimerACK)]
	if oldACK == nil || !oldACK.Active {
		t.Fatal("actual original ACK deadline missing")
	}
	owned, err := store.Read(ctx, selected.Identity, procField, outputField, "r:result-4")
	if err != nil {
		t.Fatal(err)
	}
	proc, err := readPrivateProcedureRecord(owned.Records[procField])
	if err != nil {
		t.Fatal(err)
	}
	oldF := proc.Native.Timers.Values[string(portable.TimerF)]
	if oldF == nil || !oldF.Active || oldF.At <= oldACK.At {
		t.Fatal("actual F must follow original ACK deadline")
	}
	oldOutput := bytes.Clone(owned.Records[outputField])
	oldReceipt, err := decodeNativeResult(owned.Records["r:result-4"])
	if err != nil || oldReceipt.Certainty != pb.OperationResult_ACCEPTED || oldReceipt.Pending {
		t.Fatal("actual written receipt missing", err)
	}
	failures := make(chan error, 1)
	ended := make(chan struct{}, 1)
	go func() {
		for {
			frame, readErr := readACKTimeoutEnvelope(ctx, ws)
			if readErr != nil {
				select {
				case failures <- readErr:
				default:
				}
				return
			}
			if ping := frame.GetPing(); ping != nil {
				if writeErr := writeACKTimeoutEnvelope(ctx, ws, &pb.Envelope{SessionId: hello.SessionId, CorrelationId: frame.CorrelationId, Payload: &pb.Envelope_Pong{Pong: ping}}); writeErr != nil {
					select {
					case failures <- writeErr:
					default:
					}
					return
				}
			}
			if event := frame.GetDialogEvent(); event != nil {
				if writeErr := writeACKTimeoutEnvelope(ctx, ws, &pb.Envelope{SessionId: hello.SessionId, CorrelationId: frame.CorrelationId, Payload: &pb.Envelope_Result{Result: &pb.OperationResult{Certainty: pb.OperationResult_ACCEPTED}}}); writeErr != nil {
					select {
					case failures <- writeErr:
					default:
					}
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
	for {
		if err = e.Ready(); err != nil {
			t.Fatal("original BYE deadline fenced worker", err)
		}
		view, readErr := store.ObserveContext(ctx, ref.Id, ref.State.Incarnation, "r:context", dialogField, procField, outputField, "r:result-4")
		if readErr != nil {
			t.Fatal(readErr)
		}
		d, decodeErr := readPrivateDialogRecord(view.View.Records[dialogField])
		if decodeErr != nil {
			t.Fatal(decodeErr)
		}
		if d.LocalBye != oldOrigin || d.LocalTransaction != oldKey {
			t.Fatal("ACK timeout replaced actual application BYE association")
		}

		r, decodeErr := decodeNativeResult(view.View.Records["r:result-4"])
		if decodeErr != nil || r.CutoffMs != oldReceipt.CutoffMs || r.OperationId != oldReceipt.OperationId {
			t.Fatal("ACK timeout changed original receipt identity", decodeErr)
		}
		ack := d.Native.Timers.Values[string(portable.TimerACK)]
		if r.Certainty == pb.OperationResult_COMPLETED {
			if ack == nil || ack.Active || r.Pending || r.Error == "" || r.Response != "" || r.Output != "" && r.Output != oldReceipt.Output {
				t.Fatal("original F did not settle its own timeout receipt")
			}
			current, x := decodeNativeContext(view.View.Records["r:context"])
			if x != nil || !current.Ended {
				t.Fatal("original F did not end its dialog", x)
			}
			break
		}
		p, decodeErr := readPrivateProcedureRecord(view.View.Records[procField])
		if decodeErr != nil {
			t.Fatal(decodeErr)
		}
		if !bytes.Equal(view.View.Records[outputField], oldOutput) || r.Output != oldReceipt.Output {
			t.Fatal("ACK timeout changed the pending BYE output owner")
		}
		f := p.Native.Timers.Values[string(portable.TimerF)]
		if f == nil || !f.Active || f.At != oldF.At || f.Generation != oldF.Generation {
			t.Fatal("ACK timeout moved original BYE F")
		}

		select {
		case wire := <-written:
			if bytes.HasPrefix(wire, []byte("BYE ")) {
				t.Fatal("ACK timeout created a second BYE")
			}
		case <-retried:
		case readErr := <-failures:
			t.Fatal(readErr)
		case <-time.After(100 * time.Millisecond):
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	select {
	case <-ended:
	case readErr := <-failures:
		t.Fatal(readErr)
	case <-ctx.Done():
		t.Fatal("F completion event missing", ctx.Err())
	}
	select {
	case wire := <-written:
		if bytes.HasPrefix(wire, []byte("BYE ")) {
			t.Fatal("ACK timeout created a second BYE")
		}
	default:
	}
	if err = e.Ready(); err != nil {
		t.Fatal("worker unavailable after original F", err)
	}
}

func readACKTimeoutEnvelope(ctx context.Context, ws *websocket.Conn) (*pb.Envelope, error) {
	kind, wire, err := ws.Read(ctx)
	if err != nil {
		return nil, err
	}
	if kind != websocket.MessageText {
		return nil, errors.New("non-JSON control frame")
	}
	value := new(pb.Envelope)
	if err = protojson.Unmarshal(wire, value); err != nil {
		return nil, err
	}
	return value, nil
}
func writeACKTimeoutEnvelope(ctx context.Context, ws *websocket.Conn, value *pb.Envelope) error {
	wire, err := protojson.Marshal(value)
	if err != nil {
		return err
	}
	return ws.Write(ctx, websocket.MessageText, wire)
}
