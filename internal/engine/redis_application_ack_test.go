package engine

import (
	"bytes"
	"context"
	"errors"
	"net"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
	"github.com/redis/go-redis/v9"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/nativepb"
	"sipbridge/internal/rediscore"
	"sipbridge/internal/selectedmessage"
	"sipbridge/internal/storagecodec"
)

func testRedisApplicationAnswerACK(t *testing.T, ctx context.Context, store *rediscore.Store, input redisUACQualification, dialogField, eventField string, kind int, client *redis.Client, root string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	sequence := uint64(2)
	if kind == 52 {
		sequence = 3
	}
	resultField := "r:result-" + strconv.FormatUint(sequence, 10)
	command := &pb.Command{Context: input.Context, Sequence: sequence, Admission: &pb.CommandAdmission{DirectoryEpoch: "directory", ExpiresAtMs: uint64(deadline.UnixMilli()), SelectedFlow: input.Flow}, Action: &pb.Command_DialogRequest{DialogRequest: &pb.DialogRequest{Method: "ACK", Body: []byte("v=0\r\ns=answer\r\n"), Headers: []*pb.Header{{Name: "Content-Type", Value: "application/sdp"}}}}}
	capture, err := captureRedisNotifyWait(command)
	if err != nil {
		t.Fatal(err)
	}
	identity, err := publishRedisApplicationAnswerACK(ctx, store, input.Key, input.Application, input.Lease, command, capture, "answer", input.ExecutorTimer, deadline)
	if err != nil {
		t.Fatal("publish answer ACK", err)
	}
	selected, err := store.Read(ctx, identity, "r:context", resultField, dialogField)
	if err != nil {
		t.Fatal(err)
	}
	current, err := decodeNativeContext(selected.Records["r:context"])
	if err != nil {
		t.Fatal(err)
	}
	result, err := decodeNativeResult(selected.Records[resultField])
	if err != nil || !result.Pending || result.Output == "" {
		t.Fatal("pending ACK receipt", err)
	}
	dialog, err := readPrivateDialogRecord(selected.Records[dialogField])
	if err != nil || dialog.LocalInvite == nil || dialog.LocalInvite.Ack == "" || dialog.AckResult != resultField || dialog.AckWritten {
		t.Fatal("ACK native selection", err)
	}
	outputField := result.Output
	outputView, err := store.Read(ctx, identity, outputField)
	if err != nil {
		t.Fatal(err)
	}
	output, err := decodeNativeOutput(outputView.Records[outputField])
	if err != nil || output.Method != "ACK" || output.Dialog != dialogField || output.Result != resultField || output.AckSequence != dialog.Native.LocalInvite.Sequence {
		t.Fatal("ACK output identity", err)
	}
	writer := nativeRedisWriter{store: store}
	reserved, err := writer.Reserve(ctx, output.Write)
	if err != nil || reserved == nil {
		t.Fatal("reserve ACK", err)
	}
	message, err := portable.ParseMessage(reserved.Wire, input.Flow.Transport, "", "")
	if err != nil {
		t.Fatal(err)
	}
	facts, err := message.Facts()
	if err != nil || facts.Method != sip.ACK {
		t.Fatal("wrong ACK method", err)
	}
	body, err := message.Body()
	if err != nil || !bytes.Equal(body, command.GetDialogRequest().Body) {
		t.Fatal("wrong ACK answer", err)
	}
	outcome := pb.DeliveryResult_WRITTEN
	if kind == 45 {
		outcome = pb.DeliveryResult_NOT_WRITTEN
	}
	if kind == 46 || kind == 53 || kind == 54 {
		outcome = pb.DeliveryResult_UNKNOWN
	}
	if err = writer.Complete(ctx, output.Write, &pb.DeliveryResult{Effect: output.Write.Effect, Outcome: outcome}); err != nil {
		t.Fatal("record ACK", err)
	}
	ref, err := nativeOutputRef(output.Write)
	if err != nil {
		t.Fatal(err)
	}
	proof, err := store.InspectOutput(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	physical := &nativeOutputCapture{ref: ref, proof: proof, metadata: output}
	if err = settleRedisApplicationACK(ctx, store, output.Write, physical, input.Key, input.ExecutorTimer, time.Now().Add(time.Second)); err != nil {
		t.Fatal("settle ACK", err)
	}
	after, err := store.Read(ctx, identity, "r:context", "r:result-1", resultField, dialogField, eventField)
	if err != nil {
		t.Fatal(err)
	}
	current, err = decodeNativeContext(after.Records["r:context"])
	if err != nil {
		t.Fatal(err)
	}
	original, err := decodeNativeResult(after.Records["r:result-1"])
	if err != nil {
		t.Fatal(err)
	}
	result, err = decodeNativeResult(after.Records[resultField])
	if err != nil {
		t.Fatal(err)
	}
	dialog, err = readPrivateDialogRecord(after.Records[dialogField])
	if err != nil {
		t.Fatal(err)
	}
	event, err := decodeNativeLifecycle(after.Records[eventField])
	if err != nil {
		t.Fatal(err)
	}
	if result.Pending || result.Output != outputField {
		t.Fatal("ACK receipt not settled")
	}
	if outcome == pb.DeliveryResult_WRITTEN {
		if !current.InitialAck || !dialog.AckWritten || original.Certainty != pb.OperationResult_COMPLETED || result.Certainty != pb.OperationResult_COMPLETED || result.Request != dialog.LocalInvite.Ack || !slices.Contains(event.Owners, "r:result-1") {
			t.Fatal("written ACK did not publish native and receipt witnesses")
		}
	} else if current.InitialAck || dialog.AckWritten || original.Certainty != pb.OperationResult_ACCEPTED || result.Certainty != pb.OperationResult_UNKNOWN || result.Request != "" {
		t.Fatal("non-written ACK claimed positive evidence")
	}
	if kind == 45 || kind == 46 {
		selected, x := store.Read(ctx, identity, dialog.LocalInvite.FinalResponse)
		if x != nil {
			t.Fatal(x)
		}
		final, x := selectedmessage.DecodeOpaque(selected.Records[dialog.LocalInvite.FinalResponse])
		if x != nil {
			t.Fatal(x)
		}
		if handled, x := receiveRedisUACProvisional(ctx, store, input.Flow, final, input.Key, input.ExecutorTimer, time.Now().Add(time.Second)); x != nil || !handled {
			t.Fatal("nonpositive application ACK duplicate final", handled, x)
		}
		selected, x = store.Read(ctx, identity, dialogField)
		if x != nil {
			t.Fatal(x)
		}
		retained, x := readPrivateDialogRecord(selected.Records[dialogField])
		if x != nil || retained.AckWritten || retained.AckEmissions != 1 || !slices.Equal(retained.Outputs, dialog.Outputs) {
			t.Fatal("N/U application ACK duplicate final gained replay authority", x)
		}
	}
	if outcome == pb.DeliveryResult_UNKNOWN {
		proof, err = store.InspectOutput(ctx, ref)
		if err != nil || proof.State != rediscore.OutputUnknown {
			t.Fatal("UNKNOWN proof lost", err)
		}
		latest, readErr := store.Read(ctx, identity, "r:context")
		if readErr != nil {
			t.Fatal(readErr)
		}
		due := &rediscore.DueContext{Identity: identity, Revision: latest.Revision, Assignment: latest.Timer, Member: "out-" + outputField[2:], DeadlineMS: proof.DeadlineMS, CheckedAtMS: proof.DeadlineMS}
		if e := retireRedisApplicationACK(ctx, store, due, input.Key, time.Now().Add(time.Second)); e != nil {
			t.Fatal("retain UNKNOWN ACK", e)
		}
		proof, readErr = store.InspectOutput(ctx, ref)
		if readErr != nil || proof.State != rediscore.OutputUnknown {
			t.Fatal("UNKNOWN cutoff inferred outcome", readErr)
		}
		if kind == 53 || kind == 54 {
			testAbandonedApplicationACK(t, ctx, client, store, root, input, output, outputField, eventField, kind == 54)
			return
		}
		if e := writer.Complete(ctx, output.Write, &pb.DeliveryResult{Effect: output.Write.Effect, Outcome: pb.DeliveryResult_WRITTEN}); e != nil {
			t.Fatal("late ACK write", e)
		}
		proof, readErr = store.InspectOutput(ctx, ref)
		if readErr != nil {
			t.Fatal(readErr)
		}
		physical.proof = proof
		if e := settleRedisApplicationACK(ctx, store, output.Write, physical, input.Key, input.ExecutorTimer, time.Now().Add(time.Second)); e != nil {
			t.Fatal("settle late ACK", e)
		}
		late, readErr := store.Read(ctx, identity, "r:context", "r:result-1", resultField, dialogField)
		if readErr != nil {
			t.Fatal(readErr)
		}
		lateContext, e := decodeNativeContext(late.Records["r:context"])
		if e != nil {
			t.Fatal(e)
		}
		lateOriginal, e := decodeNativeResult(late.Records["r:result-1"])
		if e != nil {
			t.Fatal(e)
		}
		lateReceipt, e := decodeNativeResult(late.Records[resultField])
		if e != nil {
			t.Fatal(e)
		}
		lateDialog, e := readPrivateDialogRecord(late.Records[dialogField])
		if e != nil {
			t.Fatal(e)
		}
		if !lateContext.InitialAck || !lateDialog.AckWritten || lateOriginal.Certainty != pb.OperationResult_COMPLETED || lateReceipt.Certainty != pb.OperationResult_COMPLETED || lateReceipt.Request != "" {
			t.Fatal("late proof did not preserve receipt boundary")
		}
	}
	if err = acknowledgeRedisApplicationACK(ctx, store, output.Write, time.Now().Add(time.Second)); err != nil {
		t.Fatal("acknowledge ACK", err)
	}
	if _, err = store.InspectOutput(ctx, ref); !errors.Is(err, rediscore.ErrAbsent) {
		t.Fatal("ACK proof not consumed", err)
	}
	if kind == 71 {
		testRedisApplicationACKReplayBeforeCutoff(t, ctx, store, input, client, root, output, outputField, resultField, reserved.Wire)
		return
	}
	if kind >= 64 && kind <= 66 {
		testRedisSuccessfulACKTransferExpiry(t, ctx, store, client, input, identity, output, outputField, dialogField, kind == 65, kind == 66)
		return
	}
	latest, err := store.Read(ctx, identity, "r:context")
	if err != nil {
		t.Fatal(err)
	}
	due := &rediscore.DueContext{Identity: identity, Revision: latest.Revision, Assignment: latest.Timer, Member: "out-" + outputField[2:], DeadlineMS: proof.DeadlineMS, CheckedAtMS: proof.DeadlineMS}
	if err = retireRedisApplicationACK(ctx, store, due, input.Key, time.Now().Add(time.Second)); err != nil {
		t.Fatal("retire ACK", err)
	}
	closed, err := store.Read(ctx, identity, dialogField, resultField, outputField)
	if err != nil {
		t.Fatal(err)
	}
	dialog, err = readPrivateDialogRecord(closed.Records[dialogField])
	if err != nil || dialog.LocalInvite.Ack == "" || dialog.AckResult != resultField || slices.Contains(dialog.Outputs, outputField) {
		t.Fatal("native ACK wire not retained", err)
	}
	result, err = decodeNativeResult(closed.Records[resultField])
	if err != nil || result.Output != "" || closed.Records[outputField] != nil {
		t.Fatal("physical ACK not retired", err)
	}
	if kind == 44 || kind == 47 || kind >= 55 && kind <= 58 || kind == 61 || kind == 62 || kind == 67 || kind == 68 || kind == 69 || kind == 72 || kind == 73 {
		again := &pb.Command{Context: input.Context, Sequence: 3, Admission: &pb.CommandAdmission{DirectoryEpoch: "directory", ExpiresAtMs: uint64(time.Now().Add(2 * time.Second).UnixMilli()), SelectedFlow: input.Flow}, Action: &pb.Command_DialogRequest{DialogRequest: &pb.DialogRequest{Method: "ACK", Body: command.GetDialogRequest().Body}}}
		second, e := captureRedisNotifyWait(again)
		if e != nil {
			t.Fatal(e)
		}
		if _, e = publishRedisApplicationAnswerACK(ctx, store, input.Key, input.Application, input.Lease, again, second, "second-answer", input.ExecutorTimer, time.Now().Add(time.Second)); e != nil {
			t.Fatal("refuse duplicate ACK", e)
		}
		refusal, e := store.Read(ctx, identity, "r:context", "r:result-3")
		if e != nil {
			t.Fatal(e)
		}
		denied, e := decodeNativeResult(refusal.Records["r:result-3"])
		if e != nil || denied.Certainty != pb.OperationResult_NOT_ACCEPTED {
			t.Fatal("missing semantic refusal", e)
		}
		after, e := decodeNativeContext(refusal.Records["r:context"])
		if e != nil || after.CommandSequence != 3 {
			t.Fatal("refusal did not consume sequence", e)
		}
		// Age only the original waiter cutoff and its matching index; the ACK
		// and all of its real owners above remain unchanged.
		beforeExpiry, e := store.Read(ctx, identity, "r:result-2", dialogField)
		if e != nil {
			t.Fatal(e)
		}
		oldResult, e := decodeNativeResult(beforeExpiry.Records["r:result-2"])
		if e != nil {
			t.Fatal(e)
		}
		oldResult.CutoffMs = time.Now().Add(-time.Second).UnixMilli()
		aged, e := encodeNativeResult(oldResult)
		if e != nil {
			t.Fatal(e)
		}
		_, e = store.Commit(ctx, rediscore.Publication{Identity: identity, Revision: beforeExpiry.Revision, Deadline: time.Now().Add(time.Second), SpendReservation: true, Changes: []rediscore.Change{{Field: "r:result-2", Value: aged}}, Timers: &rediscore.TimerMutation{Expected: beforeExpiry.Timer, Target: beforeExpiry.Timer, RequireLease: true, Changes: []rediscore.TimerChange{{Member: "orig-result-2", Due: time.UnixMilli(oldResult.CutoffMs)}}}})
		if e != nil {
			t.Fatal("age ACK result cutoff", e)
		}
		selected, e := store.Read(ctx, identity, dialogField)
		if e != nil {
			t.Fatal(e)
		}
		duty := &rediscore.DueContext{Identity: identity, Revision: selected.Revision, Assignment: selected.Timer, Member: "orig-result-2", DeadlineMS: oldResult.CutoffMs, CheckedAtMS: time.Now().UnixMilli()}
		if e = expireRedisSupersededResult(ctx, store, duty, time.Now().Add(time.Second)); e != nil {
			t.Fatal("expire ACK result", e)
		}
		expired, e := store.Read(ctx, identity, dialogField, "r:result-2", dialog.LocalInvite.Ack)
		if e != nil {
			t.Fatal(e)
		}
		retained, e := readPrivateDialogRecord(expired.Records[dialogField])
		if e != nil || retained.AckResult != "" || retained.LocalInvite.Ack != dialog.LocalInvite.Ack || len(expired.Records["r:result-2"]) != 0 || len(expired.Records[dialog.LocalInvite.Ack]) == 0 {
			t.Fatal("ACK expiry did not preserve dialog-owned wire", e)
		}
		observation, e := store.ObserveContext(ctx, identity.Context, identity.Incarnation, "r:context", dialogField)
		if e != nil {
			t.Fatal(e)
		}
		current, e := decodeNativeContext(observation.View.Records["r:context"])
		if e != nil {
			t.Fatal(e)
		}
		leaseUntil := testRedisInviteWorkerAnchor(t, ctx, store, client, input)
		planned, _, previous, _, e := planRedisPreviousInviteRelease(ctx, store, observation, current, dialogField, retained, leaseUntil)
		if e != nil || previous != output.Procedure {
			t.Fatal("select previous local INVITE", e)
		}
		removedACK := false
		var transferred *nativepb.Procedure
		for _, change := range planned {
			removedACK = removedACK || change.Field == retained.LocalInvite.Ack && change.Delete
			if change.Field == previous && !change.Delete {
				transferred, e = readPrivateProcedureRecord(change.Value)
				if e != nil {
					t.Fatal(e)
				}
			}
		}
		if removedACK || transferred == nil || transferred.SuccessfulAck == nil || transferred.SuccessfulAck.Ack != retained.LocalInvite.Ack || transferred.SuccessfulAck.FinalResponse != retained.LocalInvite.FinalResponse {
			t.Fatal("live previous ACK was not transferred to its M owner")
		}
		inviteDeadline := time.Now().Add(2 * time.Second)
		invite := &pb.Command{Context: input.Context, Sequence: 4, Admission: &pb.CommandAdmission{DirectoryEpoch: "directory", ExpiresAtMs: uint64(inviteDeadline.UnixMilli()), SelectedFlow: input.Flow}, Action: &pb.Command_DialogRequest{DialogRequest: &pb.DialogRequest{Method: "INVITE", Body: []byte("v=0\r\ns=new-offer\r\n"), Headers: []*pb.Header{{Name: "Content-Type", Value: "application/sdp"}}}}}
		if kind == 58 {
			invite.GetDialogRequest().Body = nil
			invite.GetDialogRequest().Headers = nil
		}
		inviteCapture, e := captureRedisNotifyWait(invite)
		if e != nil {
			t.Fatal(e)
		}
		if _, e = publishRedisApplicationInvite(ctx, store, input.Key, input.Application, input.Lease, invite, inviteCapture, "reinvite", input.ExecutorTimer, inviteDeadline, leaseUntil); e != nil {
			t.Fatal("publish local re-INVITE", e)
		}
		id, e := redisApplicationByeID(inviteCapture.fingerprint[:])
		if e != nil {
			t.Fatal(e)
		}
		procedureField, inviteOutputField := "r:app-invite-"+id, "r:app-invite-output-"+id
		published, e := store.Read(ctx, identity, "r:context", "r:result-4", dialogField, procedureField, inviteOutputField, retained.LocalInvite.Ack, eventField, previous)
		if e != nil {
			t.Fatal(e)
		}
		publishedDialog, e := readPrivateDialogRecord(published.Records[dialogField])
		priorOwner, priorErr := readPrivateProcedureRecord(published.Records[previous])
		if e != nil || priorErr != nil || publishedDialog.AckResult != "" || publishedDialog.LocalInvite == nil || publishedDialog.LocalInvite.Request != "r:app-invite-origin-"+id || publishedDialog.LocalInvite.Ack != "" || len(published.Records[retained.LocalInvite.Ack]) == 0 || priorOwner.SuccessfulAck == nil || priorOwner.SuccessfulAck.Ack != retained.LocalInvite.Ack {
			t.Fatal("new local INVITE did not replace its prior dialog wires", e)
		}
		publishedEvent, e := decodeNativeLifecycle(published.Records[eventField])
		if e != nil || slices.Contains(publishedEvent.Owners, dialogField) || publishedEvent.Message == "" {
			t.Fatal("re-INVITE left a stale lifecycle owner", e)
		}
		procedure, e := readPrivateProcedureRecord(published.Records[procedureField])
		if e != nil || procedure.Native.Family != string(portable.InviteClient) || procedure.Native.Key != publishedDialog.LocalTransaction || procedure.CommandResult != "r:result-4" || !slices.Contains(procedure.Outputs, inviteOutputField) || !slices.Contains(publishedDialog.Outputs, inviteOutputField) {
			t.Fatal("new INVITE native ownership", e)
		}
		inviteResult, e := decodeNativeResult(published.Records["r:result-4"])
		if e != nil || !inviteResult.Pending || inviteResult.Output != inviteOutputField {
			t.Fatal("new INVITE receipt", e)
		}
		inviteOutput, e := decodeNativeOutput(published.Records[inviteOutputField])
		if e != nil || inviteOutput.Method != "INVITE" || inviteOutput.Procedure != procedureField || inviteOutput.Dialog != dialogField {
			t.Fatal("new INVITE output", e)
		}
		inviteWire, e := writer.Reserve(ctx, inviteOutput.Write)
		if e != nil || inviteWire == nil {
			t.Fatal("reserve new INVITE", e)
		}
		message, e := portable.ParseMessage(inviteWire.Wire, input.Flow.Transport, "", "")
		if e != nil {
			t.Fatal(e)
		}
		facts, e := message.Facts()
		if e != nil || facts.Method != sip.INVITE {
			t.Fatal("wrong new INVITE wire", e)
		}
		if kind == 73 {
			testRedisLocalInviteTimerB(t, ctx, store, client, input, identity, inviteOutput, inviteWire.Wire, false)
			return
		}
		if e = writer.Complete(ctx, inviteOutput.Write, &pb.DeliveryResult{Effect: inviteOutput.Write.Effect, Outcome: pb.DeliveryResult_WRITTEN}); e != nil {
			t.Fatal("record new INVITE write", e)
		}
		if e = settleRedisApplicationByeOutput(ctx, store, inviteOutput.Write, nil, input.ExecutorTimer, time.Now().Add(time.Second)); e != nil {
			t.Fatal("settle new INVITE write", e)
		}
		if kind == 72 {
			testRedisLocalInviteTimerB(t, ctx, store, client, input, identity, inviteOutput, inviteWire.Wire, true)
			return
		}
		var retryStopped <-chan struct{}
		if kind == 47 && input.Flow.Transport == "udp" {
			retryStopped = testRedisApplicationInviteUDPArm(t, ctx, store, client, input, inviteOutput, inviteWire.Wire)
		}
		if e = acknowledgeRedisApplicationByeOutput(ctx, store, inviteOutput.Write, time.Now().Add(time.Second)); e != nil {
			t.Fatal("acknowledge new INVITE write", e)
		}
		witness, e := store.Read(ctx, identity, "r:result-4", procedureField)
		if e != nil {
			t.Fatal(e)
		}
		writtenResult, e := decodeNativeResult(witness.Records["r:result-4"])
		writtenProcedure, procedureErr := readPrivateProcedureRecord(witness.Records[procedureField])
		if e != nil || procedureErr != nil || !writtenResult.Pending || !writtenProcedure.OutputConsumed {
			t.Fatal("written INVITE did not retain its pending response receipt", e, procedureErr)
		}
		if kind == 61 || kind == 62 {
			cancel := &pb.Command{Context: input.Context, Sequence: 5, Action: &pb.Command_Cancel{Cancel: &pb.Cancel{}}}
			cancelCapture, x := captureRedisNotifyWait(cancel)
			if x != nil {
				t.Fatal(x)
			}
			cancelResult, x := publishRedisClientCancel(ctx, store, input.Key, input.Application, input.Lease, cancel, cancelCapture, "reinvite-cancel", input.ExecutorTimer, time.Now().Add(2*time.Second))
			if x != nil || cancelResult == nil || cancelResult.Certainty != pb.OperationResult_ACCEPTED {
				t.Fatal("cancel local re-INVITE", x)
			}
			cancelled, x := store.Read(ctx, identity, "r:context", procedureField)
			if x != nil {
				t.Fatal(x)
			}
			cancelledContext, x := decodeNativeContext(cancelled.Records["r:context"])
			cancelledProcedure, pe := readPrivateProcedureRecord(cancelled.Records[procedureField])
			if x != nil || pe != nil || !cancelledProcedure.Native.Cancelled || cancelledProcedure.Native.CancelSent {
				t.Fatal("early re-INVITE CANCEL was not deferred", x, pe)
			}
			if kind == 61 {
				ringing, re := portable.BuildResponseWithHeaders(message, 180, "Ringing", nil, "", "", nil)
				if re != nil {
					t.Fatal(re)
				}
				ringing = portable.RestoreMessage(ringing.WireBytes(), input.Flow.Transport, net.JoinHostPort(input.Flow.SourceIp, strconv.Itoa(int(input.Flow.SourcePort))), "")
				handled, re := completeRedisApplicationInviteResponse(ctx, store, input.Flow, ringing, input.Key, time.Now().Add(time.Second))
				if re != nil || !handled {
					t.Fatal("deferred paired CANCEL", re)
				}
				pairedView, re := store.Read(ctx, identity, "r:context", procedureField)
				if re != nil {
					t.Fatal(re)
				}
				pairedContext, re := decodeNativeContext(pairedView.Records["r:context"])
				pairedParent, pe := readPrivateProcedureRecord(pairedView.Records[procedureField])
				if re != nil || pe != nil {
					t.Fatal("read paired re-INVITE", re, pe)
				}
				childID, ce := redisClientCancelID(input.Key, pairedContext, pairedParent.Native.Key)
				if re != nil || pe != nil || ce != nil || !pairedParent.Native.CancelSent || !slices.Contains(pairedContext.NoninitialProcedures, "r:cancel-client-"+childID) || len(pairedContext.NoninitialProcedures) != len(cancelledContext.NoninitialProcedures)+1 {
					t.Fatal("paired re-INVITE CANCEL child missing", re, pe, ce)
				}
				return
			}
		}
		if kind == 47 || kind >= 55 && kind <= 58 || kind == 62 || kind == 67 || kind == 68 || kind == 69 {
			status, reason := 200, "OK"
			if kind == 57 || kind == 67 {
				status, reason = 486, "Busy Here"
			}
			response, x := portable.BuildResponseWithHeaders(message, status, reason, []byte("v=0\r\ns=answer\r\n"), "", "sip:phone@192.0.2.2:5060;transport="+input.Flow.Transport, []portable.Header{{Name: "Content-Type", Value: "application/sdp"}})
			if x != nil {
				t.Fatal(x)
			}
			response = portable.RestoreMessage(response.WireBytes(), input.Flow.Transport, net.JoinHostPort(input.Flow.SourceIp, strconv.Itoa(int(input.Flow.SourcePort))), "")
			handled, x := completeRedisApplicationInviteResponse(ctx, store, input.Flow, response, input.Key, time.Now().Add(time.Second))
			if x != nil || !handled {
				t.Fatal("matched re-INVITE final", x)
			}
			if retryStopped != nil {
				select {
				case <-retryStopped:
				case <-time.After(time.Second):
					t.Fatal("re-INVITE final did not stop original UDP retry")
				}
			}
			finished, x := store.Read(ctx, identity, "r:result-4", dialogField, procedureField)
			if x != nil {
				t.Fatal(x)
			}
			receipt, x := decodeNativeResult(finished.Records["r:result-4"])
			finishedDialog, dialogErr := readPrivateDialogRecord(finished.Records[dialogField])
			finishedProcedure, procErr := readPrivateProcedureRecord(finished.Records[procedureField])
			expectedPhase := string(portable.Accepted)
			if kind == 57 || kind == 67 {
				expectedPhase = string(portable.Completed)
			}
			if x != nil || dialogErr != nil || procErr != nil || receipt.Pending || receipt.Certainty != pb.OperationResult_COMPLETED || receipt.SipStatus != uint32(status) || receipt.Request != procedure.Origin || receipt.Response == "" || finishedDialog.LocalInvite == nil || finishedDialog.AckWritten || finishedProcedure.Native.Phase != expectedPhase {
				t.Fatal("re-INVITE final did not retain exact result and ACK duty", x, dialogErr, procErr)
			}
			if kind == 58 {
				if finishedDialog.LocalInvite.Ack != "" {
					t.Fatal("remote offer fabricated an ACK")
				}
				answerDeadline := time.Now().Add(2 * time.Second)
				answer := &pb.Command{Context: input.Context, Sequence: 5, Admission: &pb.CommandAdmission{DirectoryEpoch: "directory", ExpiresAtMs: uint64(answerDeadline.UnixMilli()), SelectedFlow: input.Flow}, Action: &pb.Command_DialogRequest{DialogRequest: &pb.DialogRequest{Method: "ACK", Body: []byte("v=0\r\ns=answer\r\n"), Headers: []*pb.Header{{Name: "Content-Type", Value: "application/sdp"}}}}}
				answerCapture, e := captureRedisNotifyWait(answer)
				if e != nil {
					t.Fatal(e)
				}
				if _, e = publishRedisApplicationAnswerACK(ctx, store, input.Key, input.Application, input.Lease, answer, answerCapture, "reinvite-answer", input.ExecutorTimer, answerDeadline); e != nil {
					t.Fatal("publish re-INVITE answer ACK", e)
				}
				answerView, e := store.Read(ctx, identity, "r:result-5", dialogField)
				if e != nil {
					t.Fatal(e)
				}
				answerResult, e := decodeNativeResult(answerView.Records["r:result-5"])
				answerDialog, de := readPrivateDialogRecord(answerView.Records[dialogField])
				if e != nil || de != nil || !answerResult.Pending || answerResult.Output == "" || answerDialog.AckResult != "r:result-5" || answerDialog.LocalInvite.Ack == "" {
					t.Fatal("re-INVITE answer ACK not retained", e, de)
				}
				return
			}
			ackField := ""
			for _, field := range finishedProcedure.Outputs {
				if strings.HasPrefix(field, "r:app-invite-ack-output-") {
					ackField = field
				}
			}
			if ackField == "" || kind != 57 && kind != 67 && !slices.Contains(finishedDialog.Outputs, ackField) || (kind == 57 || kind == 67) && finishedProcedure.Ack == "" {
				t.Fatal("generated ACK output missing")
			}
			if kind == 62 {
				beforeACK, e := store.Read(ctx, identity, "r:context")
				if e != nil {
					t.Fatal(e)
				}
				contextRecord, e := decodeNativeContext(beforeACK.Records["r:context"])
				if e != nil {
					t.Fatal(e)
				}
				for _, field := range contextRecord.NoninitialProcedures {
					if strings.HasPrefix(field, "r:call-cleanup-") {
						t.Fatal("cleanup BYE preceded positive ACK")
					}
				}
			}
			ackView, x := store.Read(ctx, identity, ackField)
			if x != nil {
				t.Fatal(x)
			}
			ackOutput, x := decodeNativeOutput(ackView.Records[ackField])
			if x != nil {
				t.Fatal(x)
			}
			reservedACK, x := writer.Reserve(ctx, ackOutput.Write)
			if x != nil || reservedACK == nil {
				t.Fatal("reserve re-INVITE ACK", x)
			}
			ackOutcome := pb.DeliveryResult_WRITTEN
			if kind == 55 {
				ackOutcome = pb.DeliveryResult_NOT_WRITTEN
			}
			if kind == 56 || kind == 68 || kind == 69 {
				ackOutcome = pb.DeliveryResult_UNKNOWN
			}
			if x = writer.Complete(ctx, ackOutput.Write, &pb.DeliveryResult{Effect: ackOutput.Write.Effect, Outcome: ackOutcome}); x != nil {
				t.Fatal(x)
			}
			for attempt := 0; attempt < 4; attempt++ {
				x = settleRedisApplicationInviteACK(ctx, store, ackOutput.Write, nil, input.Key, input.ExecutorTimer, time.Now().Add(time.Second))
				if !errors.Is(x, rediscore.ErrConflict) {
					break
				}
			}
			if x != nil {
				t.Fatal("settle re-INVITE ACK", x)
			}
			if kind == 56 || kind == 68 || kind == 69 {
				ref, e := nativeOutputRef(ackOutput.Write)
				if e != nil {
					t.Fatal(e)
				}
				proof, e := store.InspectOutput(ctx, ref)
				if e != nil || proof.State != rediscore.OutputUnknown {
					t.Fatal("uncertain re-INVITE ACK not retained", e)
				}
				if kind == 56 {
					next := &pb.Command{Context: input.Context, Sequence: 5, Admission: invite.Admission, Action: &pb.Command_DialogRequest{DialogRequest: &pb.DialogRequest{Method: "INVITE"}}}
					capture, e := captureRedisNotifyWait(next)
					if e != nil {
						t.Fatal(e)
					}
					_, e = publishRedisApplicationInvite(ctx, store, input.Key, input.Application, input.Lease, next, capture, "invite-before-previous-ack", input.ExecutorTimer, time.Now().Add(time.Second), leaseUntil)
					if e != nil {
						t.Fatal(e)
					}
					view, e := store.Read(ctx, identity, "r:result-5")
					if e != nil {
						t.Fatal(e)
					}
					refusal, e := decodeNativeResult(view.Records["r:result-5"])
					if e != nil || refusal.GetCertainty() != pb.OperationResult_NOT_ACCEPTED || refusal.GetSipStatus() != 491 || refusal.GetError() != "SIP 491: previous INVITE ACK output still pending" {
						t.Fatal("a prior ACK that may still be in flight did not cause a 491 refusal", refusal, e)
					}
				}
				if kind == 68 {
					testRedisInviteACKDuringBye(t, ctx, store, client, input, identity, dialogField, ackOutput, response)
				}
				if kind == 69 {
					testRedisInviteACKUnknownTerminal(t, ctx, store, client, root, input, identity, dialogField, ackOutput, ackField)
				}
				return
			}
			if x = acknowledgeRedisApplicationInviteACK(ctx, store, ackOutput.Write, time.Now().Add(time.Second)); x != nil {
				t.Fatal("acknowledge re-INVITE ACK", x)
			}
			acknowledged, x := store.Read(ctx, identity, dialogField)
			if x != nil {
				t.Fatal(x)
			}
			confirmed, x := readPrivateDialogRecord(acknowledged.Records[dialogField])
			if x != nil || confirmed.AckWritten != (kind != 55 && kind != 57 && kind != 67) {
				t.Fatal("written ACK did not update its dialog", x)
			}
			if kind == 62 {
				cleaned, e := store.Read(ctx, identity, "r:context", dialogField, procedureField)
				if e != nil {
					t.Fatal(e)
				}
				contextRecord, e := decodeNativeContext(cleaned.Records["r:context"])
				cleanupDialog, de := readPrivateDialogRecord(cleaned.Records[dialogField])
				parent, pe := readPrivateProcedureRecord(cleaned.Records[procedureField])
				found := false
				for _, field := range contextRecord.NoninitialProcedures {
					found = found || strings.HasPrefix(field, "r:call-cleanup-")
				}
				if e != nil || de != nil || pe != nil || !found || cleanupDialog.LocalBye == "" || parent.CreditContribution == 0 {
					t.Fatal("positive ACK did not create owned cleanup BYE", e, de, pe)
				}
			}
			if kind == 47 || kind == 57 || kind == 67 {
				if kind == 67 {
					testRedisPreviousInviteFailureReplay(t, ctx, store, client, input, identity, procedureField, dialogField, ackField, ackOutput, response, reservedACK.Wire)
					return
				}
				timer := portable.TimerM
				if kind == 57 || kind == 67 {
					timer = portable.TimerD
				}
				testRedisApplicationInviteRetirement(t, ctx, store, client, root, input, identity, dialogField, inviteOutput, ackOutput, ackField, timer)
			}
			return
		}
		// Advance only the original B deadline and its exact index to exercise
		// the real timer reducer without waiting for the SDK's full interval.
		if writtenProcedure.Native.Timers == nil || writtenProcedure.Native.Timers.Values[string(portable.TimerB)] == nil {
			t.Fatal("re-INVITE B timer missing")
		}
		b := writtenProcedure.Native.Timers.Values[string(portable.TimerB)]
		b.At = time.Now().Add(-time.Second).UnixMilli()
		agedProcedure, e := marshalNativeRecord(storagecodec.NativeProcedure, writtenProcedure)
		if e != nil {
			t.Fatal(e)
		}
		member := "app-invite-" + id + "-" + string(portable.TimerB)
		_, e = store.Commit(ctx, rediscore.Publication{Identity: identity, Revision: witness.Revision, Deadline: time.Now().Add(time.Second), SpendReservation: true, Changes: []rediscore.Change{{Field: procedureField, Value: agedProcedure}}, Timers: &rediscore.TimerMutation{Expected: witness.Timer, Target: witness.Timer, RequireLease: true, Changes: []rediscore.TimerChange{{Member: member, Due: time.UnixMilli(b.At)}}}})
		if e != nil {
			t.Fatal("age re-INVITE B", e)
		}
		timerView, e := store.Read(ctx, identity, "r:context")
		if e != nil {
			t.Fatal(e)
		}
		timerDue := &rediscore.DueContext{Identity: identity, Revision: timerView.Revision, Assignment: timerView.Timer, Member: member, DeadlineMS: b.At, CheckedAtMS: time.Now().UnixMilli()}
		if e = fireRedisApplicationInviteDeadline(ctx, store, timerDue, time.Now().Add(time.Second)); e != nil {
			t.Fatal("fire re-INVITE B", e)
		}
		timed, e := store.Read(ctx, identity, "r:context", "r:result-4", procedureField)
		if e != nil {
			t.Fatal(e)
		}
		timedContext, e := decodeNativeContext(timed.Records["r:context"])
		timedResult, resultErr := decodeNativeResult(timed.Records["r:result-4"])
		timedProcedure, timerErr := readPrivateProcedureRecord(timed.Records[procedureField])
		if e != nil || resultErr != nil || timerErr != nil || timedContext.Ended || !timedResult.Pending || timedProcedure.Native.Phase != string(portable.Terminated) {
			t.Fatal("B timeout did not preserve the established call and original waiter", e, resultErr, timerErr)
		}
		// Age only the original result cutoff and its matching retirement index.
		timedResult.CutoffMs = time.Now().Add(-time.Second).UnixMilli()
		agedResult, e := encodeNativeResult(timedResult)
		if e != nil {
			t.Fatal(e)
		}
		retireMember := "app-invite-retire-" + id
		_, e = store.Commit(ctx, rediscore.Publication{Identity: identity, Revision: timed.Revision, Deadline: time.Now().Add(time.Second), SpendReservation: true, Changes: []rediscore.Change{{Field: "r:result-4", Value: agedResult}}, Timers: &rediscore.TimerMutation{Expected: timed.Timer, Target: timed.Timer, RequireLease: true, Changes: []rediscore.TimerChange{{Member: retireMember, Due: time.UnixMilli(timedResult.CutoffMs)}}}})
		if e != nil {
			t.Fatal("age re-INVITE waiter", e)
		}
		retireView, e := store.Read(ctx, identity, "r:context")
		if e != nil {
			t.Fatal(e)
		}
		retireDue := &rediscore.DueContext{Identity: identity, Revision: retireView.Revision, Assignment: retireView.Timer, Member: retireMember, DeadlineMS: timedResult.CutoffMs, CheckedAtMS: time.Now().UnixMilli()}
		if e = retireRedisApplicationInvite(ctx, store, retireDue, input.Key, time.Now().Add(time.Second)); e != nil {
			t.Fatal("retire expired re-INVITE waiter", e)
		}
		retainedState, e := store.Read(ctx, identity, "r:result-4", procedureField, inviteOutputField, dialogField)
		if e != nil {
			t.Fatal(e)
		}
		expiredReceipt, e := decodeNativeResult(retainedState.Records["r:result-4"])
		retainedProcedure, procErr := readPrivateProcedureRecord(retainedState.Records[procedureField])
		retainedDialog, dialogErr := readPrivateDialogRecord(retainedState.Records[dialogField])
		if e != nil || procErr != nil || dialogErr != nil || expiredReceipt.Pending || expiredReceipt.Certainty != pb.OperationResult_UNKNOWN || expiredReceipt.Output != inviteOutputField || !slices.Contains(retainedProcedure.Outputs, inviteOutputField) || !slices.Contains(retainedDialog.Outputs, inviteOutputField) {
			t.Fatal("expired waiter lost native or output ownership", e, procErr, dialogErr)
		}
	}
}
