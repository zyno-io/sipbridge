package engine

import (
	"bytes"
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/rediscore"
)

func TestRedisNativeInitialACKCutoffAfterApplicationBYE(t *testing.T) {
	testRedisNativeOpenUACCommand(t, 3)
}

func TestRedisNativeInitialACKCutoffAfterRemoteReinvite(t *testing.T) {
	testRedisNativeOpenUACCommand(t, 8)
}

func TestRedisNativeApplicationBYEAfterRemoteReinvites(t *testing.T) {
	testRedisNativeOpenUACCommand(t, 9)
}

func TestRedisNativeWinningACKReplayBeforeCutoff(t *testing.T) {
	testRedisNativeOpenUACCommand(t, 10)
}

// Exercise the original, undigested initial INVITE through its natural ACK
// cutoff, after either application BYE or independent remote re-INVITEs.
func testRedisInitialACKAfterApplicationBYE(t *testing.T, ctx context.Context, e *redisNative, ws *websocket.Conn, hello *pb.SessionHelloAck, flow *pb.FlowRef, opened *pb.OperationResult, store *rediscore.Store, identity rediscore.Identity, origin portable.Message, written <-chan []byte, remoteReinvite, applicationByeAfterReinvite, earlyReplay bool, decisionRoot string, decisionProxy *redisReservationFaultProxy, decisionCase int) {
	final, err := portable.BuildResponseWithHeaders(origin, 200, "OK", nil, "phone-tag", "<sips:phone@192.0.2.12:5061>", nil)
	if err != nil {
		t.Fatal(err)
	}
	final = portable.RestoreMessage(final.WireBytes(), "tls", "192.0.2.12:5061", "")
	if handled, err := e.tryUACResponseCompletion(ctx, flow, final, nil); err != nil || !handled {
		t.Fatal("initial 200", handled, err)
	}
	var initialACK []byte
	select {
	case wire := <-written:
		if !bytes.HasPrefix(wire, []byte("ACK ")) {
			t.Fatal("initial ACK missing")
		}
		initialACK = wire
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	var outputField, procedureField, dialogField string
	var cutoff, originalM int64
	for {
		view, err := store.Read(ctx, identity, "r:context")
		if err != nil {
			t.Fatal(err)
		}
		current, err := decodeNativeContext(view.Records["r:context"])
		if err != nil || len(current.InitialProcedures) != 1 || len(current.Dialogs) != 1 {
			t.Fatal("initial owners", err)
		}
		procedureField, dialogField = current.InitialProcedures[0], current.Dialogs[0]
		selected, err := store.Read(ctx, identity, dialogField)
		if err != nil {
			t.Fatal(err)
		}
		if selected.Revision != view.Revision || selected.Timer != view.Timer {
			continue
		}
		dialog, err := readPrivateDialogRecord(selected.Records[dialogField])
		if err != nil {
			t.Fatal(err)
		}
		if current.InitialAck && dialog.AckWritten {
			for _, field := range dialog.Outputs {
				if strings.HasPrefix(field, "r:uac-dialog-ack-output-") {
					if outputField != "" && outputField != field {
						t.Fatal("multiple initial ACK attempts")
					}
					outputField = field
				}
			}
			if outputField == "" {
				t.Fatal("initial ACK owner missing")
			}
			actual, err := store.Read(ctx, identity, outputField)
			if err != nil {
				t.Fatal(err)
			}
			if actual.Revision != selected.Revision || actual.Timer != selected.Timer {
				continue
			}
			output, err := decodeNativeOutput(actual.Records[outputField])
			if err != nil {
				t.Fatal(err)
			}
			cutoff = int64(output.Write.Permit.Authorization.DeadlineMs)
			if cutoff <= time.Now().UnixMilli() {
				t.Fatal("fixture missed original ACK cutoff")
			}
			break
		}
		select {
		case <-time.After(time.Millisecond):
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	if decisionProxy != nil {
		if decisionCase >= 23 {
			testRedisDialogCommandConflict(t, ctx, e, ws, hello, flow, opened.Snapshot.Context, identity, decisionRoot, decisionProxy, decisionCase, written)
			return
		}
		testRedisDirectDecisionReadConflict(t, ctx, e, ws, hello, flow, opened.Snapshot.Context, store, identity, origin, final, initialACK, dialogField, decisionRoot, decisionProxy, decisionCase, written)
		return
	}
	var peerDialog portable.Dialog
	if remoteReinvite {
		peerDialog = testRedisInitialACKRemoteReinvites(t, ctx, e, ws, hello, flow, opened.Snapshot.Context, origin, final, initialACK, written)
		selected, x := store.Read(ctx, identity, outputField, procedureField, dialogField)
		if x != nil {
			t.Fatal(x)
		}
		procedure, x := readPrivateProcedureRecord(selected.Records[procedureField])
		if x != nil {
			t.Fatal(x)
		}
		dialog, x := readPrivateDialogRecord(selected.Records[dialogField])
		if x != nil || dialog.Transaction == procedure.Native.Key || dialog.LocalTransaction != procedure.Native.Key {
			t.Fatal("remote INVITE did not preserve its separate local ACK owner", x)
		}
		m := procedure.Native.Timers.Values[string(portable.TimerM)]
		if m == nil || !m.Active {
			t.Fatal("original initial INVITE M missing")
		}
		originalM = m.At
		output, x := decodeNativeOutput(selected.Records[outputField])
		if x != nil {
			t.Fatal(x)
		}
		ref, x := nativeOutputRef(output.Write)
		if x != nil {
			t.Fatal(x)
		}
		for {
			_, x = store.InspectOutput(ctx, ref)
			if errors.Is(x, rediscore.ErrAbsent) {
				break
			}
			if x != nil || time.Now().UnixMilli() >= cutoff {
				t.Fatal("original ACK proof was not consumed before its cutoff", x)
			}
			time.Sleep(time.Millisecond)
		}
		if earlyReplay {
			if time.Now().UnixMilli() >= cutoff {
				t.Fatal("fixture missed retained ACK metadata window")
			}
			if handled, x := e.tryUACResponseCompletion(ctx, flow, final, nil); x != nil || !handled {
				t.Fatal("winning duplicate final before original ACK cutoff", handled, x)
			}
			after, x := store.Read(ctx, identity, outputField, procedureField, dialogField)
			if x != nil {
				t.Fatal(x)
			}
			owner, x := readPrivateProcedureRecord(after.Records[procedureField])
			if x != nil {
				t.Fatal(x)
			}
			replayed, x := readPrivateDialogRecord(after.Records[dialogField])
			if x != nil || replayed.AckEmissions != 2 || !replayed.AckWritten || len(after.Records[outputField]) != 0 || slices.Contains(owner.Outputs, outputField) || slices.Contains(replayed.Outputs, outputField) || owner.Native.Timers.Values[string(portable.TimerM)].At != originalM {
				t.Fatal("consumed ACK metadata suppressed actual replay before original cutoff", x)
			}
			select {
			case wire := <-written:
				if !bytes.Equal(wire, initialACK) {
					t.Fatal("early duplicate final changed cached ACK bytes")
				}
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
		}
	} else {
		bye := &pb.Command{Context: opened.Snapshot.Context, Sequence: 2, Admission: &pb.CommandAdmission{DirectoryEpoch: "directory", ExpiresAtMs: uint64(time.Now().Add(2 * time.Second).UnixMilli()), SelectedFlow: flow}, Action: &pb.Command_DialogRequest{DialogRequest: &pb.DialogRequest{Method: "BYE"}}}
		writeControllerEnvelope(t, ctx, ws, &pb.Envelope{SessionId: hello.SessionId, CorrelationId: "initial-application-bye", RemainingBudgetMs: 2000, Payload: &pb.Envelope_Command{Command: bye}})
		var byeWire []byte
		select {
		case byeWire = <-written:
			if !bytes.HasPrefix(byeWire, []byte("BYE ")) {
				t.Fatal("application BYE missing")
			}
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
		for {
			reply := readControllerEnvelope(t, ctx, ws)
			if ping := reply.GetPing(); ping != nil {
				writeControllerEnvelope(t, ctx, ws, &pb.Envelope{SessionId: hello.SessionId, CorrelationId: reply.CorrelationId, Payload: &pb.Envelope_Pong{Pong: ping}})
				continue
			}
			if reply.GetDialogEvent() != nil {
				writeControllerEnvelope(t, ctx, ws, &pb.Envelope{SessionId: hello.SessionId, CorrelationId: reply.CorrelationId, Payload: &pb.Envelope_Result{Result: &pb.OperationResult{Certainty: pb.OperationResult_ACCEPTED}}})
				continue
			}
			if reply.CorrelationId != "initial-application-bye" || reply.GetResult().GetCertainty() != pb.OperationResult_ACCEPTED {
				t.Fatal("application BYE receipt", reply)
			}
			break
		}
		response, err := portable.ResponseFor(portable.RestoreMessage(byeWire, "tls", "", ""), 200, "OK", nil, "")
		if err != nil {
			t.Fatal(err)
		}
		response = portable.RestoreMessage(response.WireBytes(), "tls", "192.0.2.12:5061", "")
		if handled, err := e.tryUACResponseCompletion(ctx, flow, response, nil); err != nil || !handled {
			t.Fatal("BYE final", handled, err)
		}
	}
	results, failures := make(chan *pb.Envelope, 2), make(chan error, 1)
	events := make(chan *pb.DialogEvent, 8)
	go func() {
		for {
			_, wire, err := ws.Read(ctx)
			if err != nil {
				failures <- err
				return
			}
			value := new(pb.Envelope)
			if err := protojson.Unmarshal(wire, value); err != nil {
				failures <- err
				return
			}
			var reply *pb.Envelope
			if ping := value.GetPing(); ping != nil {
				reply = &pb.Envelope{SessionId: hello.SessionId, CorrelationId: value.CorrelationId, Payload: &pb.Envelope_Pong{Pong: ping}}
			} else if value.GetDialogEvent() != nil {
				events <- value.GetDialogEvent()
				reply = &pb.Envelope{SessionId: hello.SessionId, CorrelationId: value.CorrelationId, Payload: &pb.Envelope_Result{Result: &pb.OperationResult{Certainty: pb.OperationResult_ACCEPTED}}}
			} else {
				results <- value
				continue
			}
			wire, err = protojson.Marshal(reply)
			if err == nil {
				err = ws.Write(ctx, websocket.MessageText, wire)
			}
			if err != nil {
				failures <- err
				return
			}
		}
	}()
	until := time.UnixMilli(cutoff).Add(500 * time.Millisecond)
	for time.Now().Before(until) {
		if err := e.Ready(); err != nil {
			t.Fatal("original ACK cutoff fenced healthy worker", err)
		}
		select {
		case err := <-failures:
			t.Fatal(err)
		case <-time.After(10 * time.Millisecond):
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	if err := e.Ready(); err != nil {
		t.Fatal("post-cutoff worker binding", err)
	}
	view, err := store.Read(ctx, identity, outputField, procedureField, dialogField)
	if err != nil {
		t.Fatal(err)
	}
	procedure, err := readPrivateProcedureRecord(view.Records[procedureField])
	if err != nil {
		t.Fatal(err)
	}
	dialog, err := readPrivateDialogRecord(view.Records[dialogField])
	if err != nil {
		t.Fatal(err)
	}
	phase := portable.DialogEnded
	if remoteReinvite {
		phase = portable.DialogConfirmed
	}
	if len(view.Records[outputField]) != 0 || slices.Contains(procedure.Outputs, outputField) || slices.Contains(dialog.Outputs, outputField) || !dialog.AckWritten || dialog.Native.Phase != string(phase) {
		t.Fatal("original ACK cutoff did not release only actual metadata/owners")
	}
	select {
	case wire := <-written:
		t.Fatalf("cutoff emitted unexpected SIP method: %.12s", wire)
	default:
	}
	if remoteReinvite {
		// Only an actual duplicate final grants this byte-identical replay.
		// Remote offers must not replace or extend the original local M.
		if handled, x := e.tryUACResponseCompletion(ctx, flow, final, nil); x != nil || !handled {
			t.Fatal("duplicate initial 200 after remote re-INVITEs", handled, x)
		}
		select {
		case wire := <-written:
			if !bytes.Equal(wire, initialACK) {
				t.Fatal("duplicate initial 200 changed its original ACK")
			}
		case <-ctx.Done():
			t.Fatal("duplicate initial 200 lost ACK replay", ctx.Err())
		}
		selected, x := store.Read(ctx, identity, procedureField, dialogField)
		if x != nil {
			t.Fatal(x)
		}
		owner, x := readPrivateProcedureRecord(selected.Records[procedureField])
		if x != nil {
			t.Fatal(x)
		}
		replayed, x := readPrivateDialogRecord(selected.Records[dialogField])
		m := owner.Native.Timers.Values[string(portable.TimerM)]
		expectedEmissions := uint32(2)
		if earlyReplay {
			expectedEmissions++
		}
		if x != nil || m == nil || !m.Active || m.At != originalM || replayed.AckEmissions != expectedEmissions || replayed.Transaction != dialog.Transaction || replayed.LocalTransaction != owner.Native.Key {
			t.Fatal("duplicate initial final changed local/remote ownership or original M", x)
		}
		if applicationByeAfterReinvite {
			bye := &pb.Command{Context: opened.Snapshot.Context, Sequence: 6, Admission: &pb.CommandAdmission{DirectoryEpoch: "directory", ExpiresAtMs: uint64(time.Now().Add(2 * time.Second).UnixMilli()), SelectedFlow: flow}, Action: &pb.Command_DialogRequest{DialogRequest: &pb.DialogRequest{Method: "BYE"}}}
			writeControllerEnvelope(t, ctx, ws, &pb.Envelope{SessionId: hello.SessionId, CorrelationId: "application-bye-after-remote-offers", RemainingBudgetMs: 2000, Payload: &pb.Envelope_Command{Command: bye}})
			var wire []byte
			select {
			case wire = <-written:
				if !bytes.HasPrefix(wire, []byte("BYE ")) {
					t.Fatal("application BYE after remote offers missing")
				}
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			select {
			case reply := <-results:
				if reply.CorrelationId != "application-bye-after-remote-offers" || reply.GetResult().GetCertainty() != pb.OperationResult_ACCEPTED {
					t.Fatal("application BYE after remote offers receipt", reply.GetResult().GetCertainty(), reply.GetResult().GetError())
				}
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			response, x := portable.ResponseFor(portable.RestoreMessage(wire, "tls", "", ""), 200, "OK", nil, "")
			if x != nil {
				t.Fatal(x)
			}
			response = portable.RestoreMessage(response.WireBytes(), "tls", "192.0.2.12:5061", "")
			if handled, x := e.tryUACResponseCompletion(ctx, flow, response, nil); x != nil || !handled {
				t.Fatal("application BYE matched 200 after remote re-INVITEs", handled, x)
			}
		} else {
			bye, x := portable.BuildDialogRequest(peerDialog, sip.BYE, 4, nil, "z9hG4bK-peer-bye-after-ack-cutoff")
			if x != nil {
				t.Fatal(x)
			}
			bye = portable.RestoreMessage(bye.WireBytes(), "tls", "192.0.2.12:5061", "")
			if handled, x := e.tryCallCompletion(ctx, flow, bye); x != nil || !handled {
				t.Fatal("remote BYE after original ACK cutoff", handled, x)
			}
			select {
			case wire := <-written:
				if !bytes.HasPrefix(wire, []byte("SIP/2.0 200")) {
					t.Fatal("remote BYE response missing")
				}
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
		}
		select {
		case event := <-events:
			if event.Kind != pb.DialogEvent_ENDED || !event.Snapshot.InitialAck || !event.Snapshot.Ended {
				t.Fatal("post-cutoff native lifecycle lost completed ACK/BYE")
			}
		case <-ctx.Done():
			t.Fatal("post-cutoff ENDED event missing", ctx.Err())
		}
	}
	if applicationByeAfterReinvite {
		// The ordinary application receipt/procedure own the exact final;
		// cleanup-only event backlinks must not strand their retirement.
		for {
			view, x := store.Read(ctx, identity, "r:context", procedureField, "r:result-6")
			if x != nil {
				t.Fatal(x)
			}
			current, x := decodeNativeContext(view.Records["r:context"])
			if x != nil || !current.Ended {
				t.Fatal("application BYE did not end actual native call", x)
			}
			root, x := readPrivateProcedureRecord(view.Records[procedureField])
			if x != nil {
				t.Fatal(x)
			}
			if len(root.Events) != 0 {
				time.Sleep(time.Millisecond)
				continue
			}
			receipt, x := decodeNativeResult(view.Records["r:result-6"])
			if x != nil || receipt.Pending || receipt.Certainty != pb.OperationResult_COMPLETED || receipt.Request == "" || receipt.Response == "" || receipt.Output == "" {
				t.Fatal("application BYE final receipt lost native completion", x)
			}
			selected, x := store.Read(ctx, identity, receipt.Output)
			if x != nil {
				t.Fatal(x)
			}
			if selected.Revision != view.Revision || selected.Timer != view.Timer {
				continue
			}
			output, x := decodeNativeOutput(selected.Records[receipt.Output])
			if x != nil {
				t.Fatal(x)
			}
			owned, x := store.Read(ctx, identity, output.Procedure, receipt.Request, receipt.Response)
			if x != nil {
				t.Fatal(x)
			}
			if owned.Revision != view.Revision || owned.Timer != view.Timer {
				continue
			}
			bye, x := readPrivateProcedureRecord(owned.Records[output.Procedure])
			if x != nil || len(bye.Events) != 0 || bye.Origin != receipt.Request || bye.LastResponse != receipt.Response || !slices.Contains(bye.Outputs, receipt.Output) || len(owned.Records[receipt.Request]) == 0 || len(owned.Records[receipt.Response]) == 0 {
				t.Fatal("application BYE exact response owners changed", x)
			}
			testRedisBYECutoffAfterRemoteInviteRetirement(t, ctx, e, store, identity, dialogField, output.Procedure, receipt.Output, receipt.Response)
			break
		}
	}
	open := &pb.Command{Context: &pb.ContextRef{Id: "after-ack-cutoff"}, Sequence: 1, Admission: &pb.CommandAdmission{DirectoryEpoch: "directory", ExpiresAtMs: uint64(time.Now().Add(2 * time.Second).UnixMilli()), SelectedFlow: flow}, Action: &pb.Command_OpenUac{OpenUac: &pb.OpenUac{Uri: "sip:phone@example.test", Transport: "tls", Headers: []*pb.Header{{Name: "From", Value: "<sip:service@example.test>"}}}}}
	writeControllerEnvelope(t, ctx, ws, &pb.Envelope{SessionId: hello.SessionId, CorrelationId: "after-original-cutoff", RemainingBudgetMs: 2000, Payload: &pb.Envelope_Command{Command: open}})
	var fresh *pb.ContextRef
	select {
	case reply := <-results:
		if reply.CorrelationId != "after-original-cutoff" || reply.GetResult().GetCertainty() != pb.OperationResult_ACCEPTED {
			t.Fatal("fresh call lost healthy worker", reply)
		}
		fresh = reply.GetResult().GetSnapshot().GetContext()
	case err := <-failures:
		t.Fatal(err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	select {
	case wire := <-written:
		if !bytes.HasPrefix(wire, []byte("INVITE ")) {
			t.Fatal("fresh call wire missing")
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}

	// Bytes become visible before the synthetic writer's Complete returns.
	// Wait for this fresh intent's actual returned-consumer acknowledgment.
	if fresh == nil || fresh.State == nil {
		t.Fatal("fresh accepted call has no native context")
	}
	selected, err := store.ObserveContext(ctx, fresh.Id, fresh.State.Incarnation, "r:context")
	if err != nil {
		t.Fatal(err)
	}
	current, err := decodeNativeContext(selected.View.Records["r:context"])
	if err != nil || len(current.InitialProcedures) != 1 {
		t.Fatal("fresh initial owner", err)
	}
	owners, err := store.Read(ctx, selected.Identity, current.InitialProcedures[0])
	if err != nil {
		t.Fatal(err)
	}
	owner, err := readPrivateProcedureRecord(owners.Records[current.InitialProcedures[0]])
	if err != nil || len(owner.Outputs) != 1 {
		t.Fatal("fresh initial output owner", err)
	}
	metadata, err := store.Read(ctx, selected.Identity, owner.Outputs[0])
	if err != nil {
		t.Fatal(err)
	}
	output, err := decodeNativeOutput(metadata.Records[owner.Outputs[0]])
	if err != nil || output.Method != "INVITE" || output.Status != 0 || !proto.Equal(output.Write.Effect.Context, fresh) {
		t.Fatal("fresh initial output identity", err)
	}
	ref, err := nativeOutputRef(output.Write)
	if err != nil {
		t.Fatal(err)
	}
	for {
		_, err = store.InspectOutput(ctx, ref)
		if errors.Is(err, rediscore.ErrAbsent) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		select {
		case <-time.After(time.Millisecond):
		case <-ctx.Done():
			t.Fatal("fresh INVITE consumer did not acknowledge", ctx.Err())
		}
	}
}

func testRedisBYECutoffAfterRemoteInviteRetirement(t *testing.T, ctx context.Context, e *redisNative, store *rediscore.Store, identity rediscore.Identity, dialogField, byeField, outputField, responseField string) {
	t.Helper()
	view, err := store.Read(ctx, identity, dialogField)
	if err != nil {
		t.Fatal(err)
	}
	dialog, err := readPrivateDialogRecord(view.Records[dialogField])
	if err != nil || dialog.Transaction == "" {
		t.Fatal("expected a selected remote INVITE transaction", err)
	}
	parent, err := store.LookupSelector(ctx, nativeSelector(rediscore.SelectorServerTransaction, dialog.Transaction))
	if err != nil {
		t.Fatal(err)
	}
	procedure, err := readPrivateProcedureRecord(parent.View.Records[parent.Record])
	if err != nil || procedure.Native.Timers == nil {
		t.Fatal("expected a retained remote INVITE procedure", err)
	}
	timer := procedure.Native.Timers.Values[string(portable.TimerL)]
	if timer == nil || !timer.Active {
		t.Fatal("expected active remote INVITE Timer L")
	}
	// Advance the real server timer to its selected logical deadline. Its
	// resulting retirement must precede the local BYE output cutoff.
	due := &rediscore.DueContext{Identity: parent.Identity, Revision: parent.View.Revision, Assignment: parent.View.Timer,
		Member: "call-tx-" + procedure.RequestId + "-L", DeadlineMS: timer.At, CheckedAtMS: timer.At}
	if err = fireRedisInviteServerTimer(ctx, store, due, time.Now().Add(time.Second)); err != nil {
		t.Fatal("remote INVITE Timer L", err)
	}
	view, err = store.Read(ctx, identity, parent.Record)
	if err != nil {
		t.Fatal(err)
	}
	due.Revision, due.Assignment, due.Member = view.Revision, view.Timer, "call-proc-"+procedure.RequestId
	if err = retireRedisDialogServerProcedure(ctx, store, e.cfg.StateIdentityKey, due, time.Now().Add(time.Second)); err != nil {
		t.Fatal("remote INVITE retirement", err)
	}
	view, err = store.Read(ctx, identity, dialogField, parent.Record)
	if err != nil {
		t.Fatal(err)
	}
	dialog, err = readPrivateDialogRecord(view.Records[dialogField])
	if err != nil || dialog.Transaction != "" || dialog.RemoteInvite == nil || len(view.Records[parent.Record]) != 0 {
		t.Fatal("remote INVITE retirement did not preserve only the dialog references", err)
	}
	for {
		if err = e.Ready(); err != nil {
			t.Fatal("local BYE cutoff fenced the worker after remote INVITE retirement", err)
		}
		view, err = store.Read(ctx, identity, byeField, outputField, "r:result-6", responseField)
		if err != nil {
			t.Fatal(err)
		}
		if len(view.Records[outputField]) == 0 {
			receipt, err := decodeNativeResult(view.Records["r:result-6"])
			if err != nil || receipt.GetOutput() != "" || receipt.GetResponse() != responseField || len(view.Records[responseField]) == 0 || len(view.Records[byeField]) != 0 {
				t.Fatal("BYE did not retire while preserving its final response", err)
			}
			break
		}
		select {
		case <-time.After(5 * time.Millisecond):
		case <-ctx.Done():
			t.Fatal("BYE output did not retire", ctx.Err())
		}
	}
}

// Drive real remote offers through pending publication, JSON decision/Respond,
// physical 200 and matching ACK. Only the remote transaction may be replaced.
func testRedisInitialACKRemoteReinvites(t *testing.T, ctx context.Context, e *redisNative, ws *websocket.Conn, hello *pb.SessionHelloAck, flow *pb.FlowRef, ref *pb.ContextRef, origin, final portable.Message, ackWire []byte, written <-chan []byte) portable.Dialog {
	t.Helper()
	peerDialog, err := portable.NewDialog(portable.UAS, origin, final, portable.DefaultTimers())
	if err != nil {
		t.Fatal(err)
	}
	peerDialog, _, err = portable.StepDialog(peerDialog, portable.DialogInput{Kind: portable.DialogSendResponse, Message: final, Now: time.Now().UnixMilli()})
	if err != nil {
		t.Fatal(err)
	}
	peerDialog, _, err = portable.StepDialog(peerDialog, portable.DialogInput{Kind: portable.DialogReceiveRequest, Message: portable.RestoreMessage(ackWire, "tls", "", ""), Now: time.Now().UnixMilli()})
	if err != nil {
		t.Fatal(err)
	}
	read := func(match func(*pb.Envelope) bool) *pb.Envelope {
		for {
			value := readControllerEnvelope(t, ctx, ws)
			if ping := value.GetPing(); ping != nil {
				writeControllerEnvelope(t, ctx, ws, &pb.Envelope{SessionId: hello.SessionId, CorrelationId: value.CorrelationId, Payload: &pb.Envelope_Pong{Pong: ping}})
				continue
			}
			if value.GetDialogEvent() != nil {
				writeControllerEnvelope(t, ctx, ws, &pb.Envelope{SessionId: hello.SessionId, CorrelationId: value.CorrelationId, Payload: &pb.Envelope_Result{Result: &pb.OperationResult{Certainty: pb.OperationResult_ACCEPTED}}})
				if match(value) {
					return value
				}
				continue
			}
			if !match(value) {
				t.Fatalf("unexpected remote-reINVITE control frame %T", value.Payload)
			}
			return value
		}
	}
	read(func(value *pb.Envelope) bool { return value.GetDialogEvent().GetKind() == pb.DialogEvent_ANSWERED })
	store, _, _, err := e.driver.Current()
	if err != nil {
		t.Fatal(err)
	}
	// Wait for the actual initial event acknowledgement CAS before issuing an
	// independent decision, rather than race that unrelated context revision.
	for {
		view, x := store.ObserveContext(ctx, ref.Id, ref.State.Incarnation, "r:context")
		if x != nil {
			t.Fatal(x)
		}
		current, x := decodeNativeContext(view.View.Records["r:context"])
		if x != nil || len(current.InitialProcedures) != 1 {
			t.Fatal("initial lifecycle owner", x)
		}
		selected, x := store.Read(ctx, view.Identity, current.InitialProcedures[0])
		if x != nil {
			t.Fatal(x)
		}
		if selected.Revision != view.View.Revision || selected.Timer != view.View.Timer {
			continue
		}
		root, x := readPrivateProcedureRecord(selected.Records[current.InitialProcedures[0]])
		if x != nil {
			t.Fatal(x)
		}
		if len(root.Events) == 0 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	peer, err := e.control.Peer(hello.ApplicationLease.Application)
	if err != nil {
		t.Fatal(err)
	}
	for index, operation := range []string{"remote-first", "remote-second"} {
		sequence := uint32(index + 2)
		request, x := portable.BuildDialogRequest(peerDialog, sip.INVITE, sequence, nil, "z9hG4bK-"+operation)
		if x != nil {
			t.Fatal(x)
		}
		request = portable.RestoreMessage(request.WireBytes(), "tls", "192.0.2.12:5061", "")
		if x = e.receiveDialog(ctx, peer, flow, request, ref, "", ""); x != nil {
			t.Fatal("remote INVITE publication", x)
		}
		presentation := read(func(value *pb.Envelope) bool { return value.GetRequest() != nil }).GetRequest()
		policy := &pb.CommandAdmission{DirectoryEpoch: "directory", ExpiresAtMs: uint64(time.Now().Add(2 * time.Second).UnixMilli()), ExpectedDelivery: cloneResidentDelivery(presentation.Snapshot.Delivery)}
		decision := &pb.RequestDecision{Context: ref, RequestId: presentation.RequestId, Sequence: uint64(index*2 + 2), Basis: pb.OwnerIngest_EXISTING_FLOW, Admission: policy}
		writeControllerEnvelope(t, ctx, ws, &pb.Envelope{SessionId: hello.SessionId, CorrelationId: operation + "-admit", RemainingBudgetMs: 2000, Payload: &pb.Envelope_RequestDecision{RequestDecision: decision}})
		admitted := read(func(value *pb.Envelope) bool { return value.CorrelationId == operation+"-admit" }).GetResult()
		if admitted == nil || admitted.Certainty != pb.OperationResult_ACCEPTED {
			t.Fatal("remote INVITE decision", admitted.GetCertainty(), admitted.GetError())
		}
		command := &pb.Command{Context: ref, Sequence: decision.Sequence + 1, Admission: policy, Action: &pb.Command_Respond{Respond: &pb.Respond{RequestId: presentation.RequestId, Status: 200}}}
		writeControllerEnvelope(t, ctx, ws, &pb.Envelope{SessionId: hello.SessionId, CorrelationId: operation + "-answer", RemainingBudgetMs: 2000, Payload: &pb.Envelope_Command{Command: command}})
		answered := read(func(value *pb.Envelope) bool { return value.CorrelationId == operation+"-answer" }).GetResult()
		if answered == nil || answered.Certainty != pb.OperationResult_COMPLETED {
			t.Fatal("remote INVITE response", answered.GetCertainty(), answered.GetError())
		}
		for {
			select {
			case wire := <-written:
				if bytes.HasPrefix(wire, []byte("SIP/2.0 100")) {
					continue
				}
				if !bytes.HasPrefix(wire, []byte("SIP/2.0 200")) {
					t.Fatal("remote INVITE physical final missing")
				}
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			break
		}
		ack, x := portable.BuildDialogRequest(peerDialog, sip.ACK, sequence, nil, "z9hG4bK-ack-"+operation)
		if x != nil {
			t.Fatal(x)
		}
		ack = portable.RestoreMessage(ack.WireBytes(), "tls", "192.0.2.12:5061", "")
		if handled, x := e.tryCallCompletion(ctx, flow, ack); x != nil || !handled {
			t.Fatal("remote INVITE ACK", handled, x)
		}
	}
	return peerDialog
}
