package engine

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/emiago/sipgo/sip/portable"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/rediscore"
)

func TestRedisNativeCurrentACKReplayBeforeCutoff(t *testing.T) {
	testRedisNativeOpenUACCommand(t, 11)
}

func TestRedisNativeResumeCompletesPreviousWrittenACK(t *testing.T) {
	for index, name := range []string{"written", "not-written", "unknown"} {
		t.Run(name, func(t *testing.T) { testRedisNativeOpenUACCommand(t, 5+index) })
	}
}

// Pause the actual consumer after physical evidence has been recorded, before
// semantic settlement. The driver may independently finish this exact output.
type redisPreviousACKPause struct {
	outcome  pb.DeliveryResult_Outcome
	used     atomic.Bool
	once     sync.Once
	release  chan struct{}
	recorded chan *pb.FlowWrite
	done     chan error
}

func newRedisPreviousACKPause(outcome pb.DeliveryResult_Outcome) *redisPreviousACKPause {
	return &redisPreviousACKPause{outcome: outcome, release: make(chan struct{}), recorded: make(chan *pb.FlowWrite, 1), done: make(chan error, 1)}
}
func (p *redisPreviousACKPause) unblock() { p.once.Do(func() { close(p.release) }) }
func (p *redisPreviousACKPause) selectWrite(wire []byte, transport string) bool {
	facts, err := portable.RestoreMessage(wire, transport, "", "").Facts()
	return err == nil && facts.Method == "ACK" && facts.CSeq == 2 && p.used.CompareAndSwap(false, true)
}
func (p *redisPreviousACKPause) write(ctx context.Context, e *redisNative, write *pb.FlowWrite, wire []byte, written chan<- []byte) *pb.DeliveryResult {
	result := &pb.DeliveryResult{Effect: write.Effect, Outcome: p.outcome}
	if p.outcome != pb.DeliveryResult_NOT_WRITTEN {
		select {
		case written <- bytes.Clone(wire):
		case <-ctx.Done():
			return result
		}
	}
	store, _, _, err := e.driver.Current()
	if err == nil {
		err = (nativeRedisWriter{store: store}).Complete(ctx, write, result)
	}
	if err != nil {
		result.Error = err.Error()
		p.done <- err
		return result
	}
	p.recorded <- write
	select {
	case <-p.release:
	case <-ctx.Done():
		p.done <- ctx.Err()
		return result
	}
	err = e.Complete(ctx, write, result)
	// The admitted successor may already have consumed this exact proof and
	// retired its obsolete metadata. The old callback cannot recreate it.
	if err == nil || errors.Is(err, rediscore.ErrAbsent) || errors.Is(err, rediscore.ErrConflict) {
		result.Recorded = true
	} else {
		result.Error = err.Error()
	}
	p.done <- err
	return result
}

func testRedisPreviousACKCompletionBeforeResume(t *testing.T, ctx context.Context, e *redisNative, ws *websocket.Conn, hello *pb.SessionHelloAck, flow *pb.FlowRef, opened *pb.OperationResult, store *rediscore.Store, identity rediscore.Identity, origin portable.Message, written <-chan []byte, pause *redisPreviousACKPause, earlyReplay bool) {
	t.Helper()
	readCtx, stopRead := context.WithCancel(ctx)
	defer stopRead()
	replies := make(chan *pb.Envelope, 4)
	go func() {
		for {
			_, wire, err := ws.Read(readCtx)
			if err != nil {
				return
			}
			frame := new(pb.Envelope)
			if protojson.Unmarshal(wire, frame) != nil {
				return
			}
			if ping := frame.GetPing(); ping != nil {
				writeControllerEnvelope(t, ctx, ws, &pb.Envelope{SessionId: hello.SessionId, CorrelationId: frame.CorrelationId, Payload: &pb.Envelope_Pong{Pong: ping}})
				continue
			}
			if frame.GetDialogEvent() != nil {
				writeControllerEnvelope(t, ctx, ws, &pb.Envelope{SessionId: hello.SessionId, CorrelationId: frame.CorrelationId, Payload: &pb.Envelope_Result{Result: &pb.OperationResult{Certainty: pb.OperationResult_ACCEPTED}}})
				continue
			}
			select {
			case replies <- frame:
			case <-readCtx.Done():
				return
			}
		}
	}()
	nextWire := func(prefix string) []byte {
		t.Helper()
		select {
		case wire := <-written:
			if !bytes.HasPrefix(wire, []byte(prefix)) {
				t.Fatal("unexpected physical method")
			}
			return wire
		case <-ctx.Done():
			t.Fatal("physical output missing")
			return nil
		}
	}
	final := func(request portable.Message, tag string) portable.Message {
		t.Helper()
		message, err := portable.BuildResponseWithHeaders(request, 200, "OK", []byte("v=0\r\ns=answer\r\n"), tag, "<sip:phone@192.0.2.12:5061>", []portable.Header{{Name: "Content-Type", Value: "application/sdp"}})
		if err != nil {
			t.Fatal(err)
		}
		message = portable.RestoreMessage(message.WireBytes(), "udp", "192.0.2.12:5061", "")
		if handled, err := e.tryUACResponseCompletion(ctx, flow, message, nil); err != nil || !handled {
			t.Fatal("matched final", err)
		}
		return message
	}
	result := func(operation string) *pb.OperationResult {
		t.Helper()
		select {
		case frame := <-replies:
			if frame.CorrelationId != operation || frame.GetResult() == nil {
				t.Fatal("unexpected RPC reply")
			}
			return frame.GetResult()
		case <-ctx.Done():
			t.Fatal("RPC reply missing")
			return nil
		}
	}
	send := func(sequence uint64, operation, method string) {
		t.Helper()
		command := &pb.Command{Context: opened.Snapshot.Context, Sequence: sequence, Admission: &pb.CommandAdmission{DirectoryEpoch: "directory", ExpiresAtMs: uint64(time.Now().Add(2 * time.Second).UnixMilli()), SelectedFlow: flow}, Action: &pb.Command_DialogRequest{DialogRequest: &pb.DialogRequest{Method: method, Body: []byte("v=0\r\ns=hold-resume\r\n"), Headers: []*pb.Header{{Name: "Content-Type", Value: "application/sdp"}}}}}
		writeControllerEnvelope(t, ctx, ws, &pb.Envelope{SessionId: hello.SessionId, CorrelationId: operation, RemainingBudgetMs: 2000, Payload: &pb.Envelope_Command{Command: command}})
	}
	final(origin, "phone-tag")
	nextWire("ACK ")
	// Start the first re-INVITE only after the actual initial ACK consumer has
	// released its proof. The hold ACK's consumer is deliberately paused below.
	for {
		view, err := store.Read(ctx, identity, "r:context")
		if err != nil {
			t.Fatal(err)
		}
		current, err := decodeNativeContext(view.Records["r:context"])
		if err != nil {
			t.Fatal(err)
		}
		ready := current.InitialAck
		if ready {
			dv, err := store.Read(ctx, identity, current.Dialogs[0])
			if err != nil {
				t.Fatal(err)
			}
			d, err := readPrivateDialogRecord(dv.Records[current.Dialogs[0]])
			if err != nil {
				t.Fatal(err)
			}
			for _, field := range d.Outputs {
				if !strings.HasPrefix(field, "r:uac-dialog-ack-output-") {
					continue
				}
				ov, err := store.Read(ctx, identity, field)
				if err != nil {
					t.Fatal(err)
				}
				o, err := decodeNativeOutput(ov.Records[field])
				if err != nil {
					t.Fatal(err)
				}
				ref, err := nativeOutputRef(o.Write)
				if err != nil {
					t.Fatal(err)
				}
				_, err = store.InspectOutput(ctx, ref)
				if err == nil {
					ready = false
				} else if !errors.Is(err, rediscore.ErrAbsent) {
					t.Fatal(err)
				}
			}
		}
		if ready {
			break
		}
		select {
		case <-time.After(time.Millisecond):
		case <-ctx.Done():
			t.Fatal("initial ACK consumer did not finish")
		}
	}
	send(2, "hold", "INVITE")
	holdWire := nextWire("INVITE ")
	holdFinal := final(portable.RestoreMessage(holdWire, "udp", "", ""), "")
	var holdACK []byte
	if pause.outcome != pb.DeliveryResult_NOT_WRITTEN {
		holdACK = nextWire("ACK ")
	}
	holdResult := result("hold")
	if holdResult.Certainty != pb.OperationResult_COMPLETED || holdResult.SipStatus != 200 {
		t.Fatal("hold did not complete")
	}
	var blocked *pb.FlowWrite
	select {
	case blocked = <-pause.recorded:
	case <-ctx.Done():
		t.Fatal("paused physical evidence missing")
	}
	ref, err := nativeOutputRef(blocked)
	if err != nil {
		t.Fatal(err)
	}
	proof, err := store.InspectOutput(ctx, ref)
	expected := map[pb.DeliveryResult_Outcome]rediscore.OutputState{pb.DeliveryResult_WRITTEN: rediscore.OutputWritten, pb.DeliveryResult_NOT_WRITTEN: rediscore.OutputNotWritten, pb.DeliveryResult_UNKNOWN: rediscore.OutputUnknown}[pause.outcome]
	if err != nil || proof.State != expected || proof.Settled || proof.DeadlineMS <= time.Now().UnixMilli() {
		t.Fatal("original physical attempt not retained", err)
	}
	metadata, err := decodeNativeOutput(proof.Metadata)
	if err != nil {
		t.Fatal(err)
	}
	before, err := store.Read(ctx, identity, metadata.Dialog, metadata.Procedure)
	if err != nil {
		t.Fatal(err)
	}
	oldDialog, err := readPrivateDialogRecord(before.Records[metadata.Dialog])
	if err != nil || oldDialog.AckWritten {
		t.Fatal("ACK native consumer was not paused", err)
	}
	oldProcedure, err := readPrivateProcedureRecord(before.Records[metadata.Procedure])
	if err != nil {
		t.Fatal(err)
	}
	oldM := oldProcedure.Native.GetTimers().GetValues()[string(portable.TimerM)]
	if oldM == nil || !oldM.Active {
		t.Fatal("previous INVITE lost original M")
	}
	originalM := oldM.At
	if earlyReplay {
		pause.unblock()
		select {
		case callbackErr := <-pause.done:
			if callbackErr != nil {
				t.Fatal("current ACK callback", callbackErr)
			}
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
		for {
			_, err = store.InspectOutput(ctx, ref)
			if errors.Is(err, rediscore.ErrAbsent) {
				break
			}
			if err != nil || time.Now().UnixMilli() >= proof.DeadlineMS {
				t.Fatal("current ACK proof did not close before original cutoff", err)
			}
			time.Sleep(time.Millisecond)
		}
		retained, x := store.Read(ctx, identity, metadata.Dialog, proof.MetadataField)
		if x != nil {
			t.Fatal(x)
		}
		d, x := readPrivateDialogRecord(retained.Records[metadata.Dialog])
		if x != nil || !d.AckWritten || d.Native.LocalInvite.Sequence != 2 || len(retained.Records[proof.MetadataField]) == 0 {
			t.Fatal("actual current ACK positive witness missing", x)
		}
		if handled, x := e.tryUACResponseCompletion(ctx, flow, holdFinal, nil); x != nil || !handled {
			t.Fatal("current successful final replay", handled, x)
		}
		after, x := store.Read(ctx, identity, metadata.Dialog, metadata.Procedure, proof.MetadataField)
		if x != nil {
			t.Fatal(x)
		}
		d, x = readPrivateDialogRecord(after.Records[metadata.Dialog])
		if x != nil {
			t.Fatal(x)
		}
		p, x := readPrivateProcedureRecord(after.Records[metadata.Procedure])
		if x != nil || d.AckEmissions != 2 || len(after.Records[proof.MetadataField]) != 0 || p.Native.Timers.Values[string(portable.TimerM)].At != originalM {
			t.Fatal("consumed current ACK metadata suppressed replay before cutoff", x)
		}
		if replay := nextWire("ACK "); !bytes.Equal(replay, holdACK) {
			t.Fatal("current cached ACK replay changed bytes")
		}
		return
	}
	send(3, "resume", "INVITE")
	if pause.outcome == pb.DeliveryResult_WRITTEN {
		var resumeWire []byte
		select {
		case resumeWire = <-written:
			if !bytes.HasPrefix(resumeWire, []byte("INVITE ")) {
				t.Fatal("resume emitted another method")
			}
		case refused := <-replies:
			t.Fatalf("written ACK blocked resume: certainty=%s reason=%s", refused.GetResult().GetCertainty(), refused.GetResult().GetError())
		case <-ctx.Done():
			t.Fatal("resume physical output missing")
		}
		final(portable.RestoreMessage(resumeWire, "udp", "", ""), "")
		nextWire("ACK ")
		resumed := result("resume")
		if resumed.Certainty != pb.OperationResult_COMPLETED || resumed.SipStatus != 200 || resumed.Snapshot.Ended {
			t.Fatal("written hold ACK blocked resume")
		}
		after, err := store.Read(ctx, identity, metadata.Procedure)
		if err != nil {
			t.Fatal(err)
		}
		previous, err := readPrivateProcedureRecord(after.Records[metadata.Procedure])
		if err != nil || previous.SuccessfulAck == nil || previous.SuccessfulAck.Ack != oldDialog.LocalInvite.Ack || previous.SuccessfulAck.FinalResponse != oldDialog.LocalInvite.FinalResponse || !proto.Equal(previous.SuccessfulAck.Flow, flow) || previous.Native.Timers.Values[string(portable.TimerM)].At != originalM {
			t.Fatal("previous ACK replay ownership or original M changed", err)
		}
	} else {
		refused := result("resume")
		if refused.Certainty != pb.OperationResult_NOT_ACCEPTED || refused.SipStatus != 491 || !strings.Contains(refused.Error, errPreviousInviteACKPending.Error()) || refused.Snapshot.Ended {
			t.Fatal("unwritten ACK granted replacement")
		}
		retained, err := store.InspectOutput(ctx, ref)
		if err != nil || retained.State != expected || retained.Settled {
			t.Fatal("nonpositive evidence changed", err)
		}
	}
	pause.unblock()
	select {
	case callbackErr := <-pause.done:
		if callbackErr != nil && !errors.Is(callbackErr, rediscore.ErrAbsent) && !errors.Is(callbackErr, rediscore.ErrConflict) {
			t.Fatal("old callback failed", callbackErr)
		}
	case <-ctx.Done():
		t.Fatal("old callback did not finish")
	}
	if e.Ready() != nil {
		t.Fatal("old callback fenced the worker")
	}
	select {
	case <-written:
		t.Fatal("old callback wrote another SIP message")
	default:
	}
	if pause.outcome == pb.DeliveryResult_WRITTEN {
		// Its independent old INVITE still owns the exact successful ACK under M.
		if handled, err := e.tryUACResponseCompletion(ctx, flow, holdFinal, nil); err != nil || !handled {
			t.Fatal("previous final replay", err)
		}
		if replay := nextWire("ACK "); !bytes.Equal(replay, holdACK) {
			t.Fatal("previous ACK replay changed bytes")
		}
		send(4, "after-old-callback", "INFO")
		info := portable.RestoreMessage(nextWire("INFO "), "udp", "", "")
		response, err := portable.ResponseFor(info, 200, "OK", nil, "")
		if err != nil {
			t.Fatal(err)
		}
		response = portable.RestoreMessage(response.WireBytes(), "udp", "192.0.2.12:5061", "")
		if handled, err := e.tryUACResponseCompletion(ctx, flow, response, nil); err != nil || !handled {
			t.Fatal("fresh INFO final", err)
		}
		if fresh := result("after-old-callback"); fresh.Certainty != pb.OperationResult_COMPLETED || fresh.Snapshot.Ended || e.Ready() != nil {
			t.Fatal("old callback changed successor or readiness")
		}
	}
}
