package engine

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/emiago/sipgo/sip/portable"
	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/nativepb"
	"sipbridge/internal/rediscore"
	"sipbridge/internal/selectedmessage"
	"sipbridge/internal/storagecodec"
)

func testRedisSuccessfulACKTransferExpiry(t *testing.T, ctx context.Context, store *rediscore.Store, client *redis.Client, input redisUACQualification, identity rediscore.Identity, ackOutput *nativepb.Output, ackOutputField, dialogField string, overdue, mFirst bool) {
	t.Helper()
	before, err := store.Read(ctx, identity, ackOutput.Procedure, dialogField, ackOutput.Result, ackOutputField)
	if err != nil {
		t.Fatal(err)
	}
	old, err := readPrivateProcedureRecord(before.Records[ackOutput.Procedure])
	if err != nil || old.Native.Timers == nil || old.Native.Timers.Values[string(portable.TimerM)] == nil {
		t.Fatal("old M", err)
	}
	if overdue {
		m := old.Native.Timers.Values[string(portable.TimerM)]
		m.At = time.Now().Add(-time.Second).UnixMilli()
		data, e := marshalNativeRecord(storagecodec.NativeProcedure, old)
		if e != nil {
			t.Fatal(e)
		}
		member := "call-tx-" + strings.TrimPrefix(ackOutput.Procedure, "r:uac-procedure-") + "-" + string(portable.TimerM)
		_, e = store.Commit(ctx, rediscore.Publication{Identity: identity, Revision: before.Revision, Deadline: time.Now().Add(time.Second), SpendReservation: true, Changes: []rediscore.Change{{Field: ackOutput.Procedure, Value: data}}, Timers: &rediscore.TimerMutation{Expected: before.Timer, Target: before.Timer, RequireLease: true, Changes: []rediscore.TimerChange{{Member: member, Due: time.UnixMilli(m.At)}}}})
		if e != nil {
			t.Fatal("age old M", e)
		}
		before, e = store.Read(ctx, identity, ackOutput.Procedure, dialogField, ackOutput.Result, ackOutputField)
		if e != nil {
			t.Fatal(e)
		}
		old, e = readPrivateProcedureRecord(before.Records[ackOutput.Procedure])
		if e != nil {
			t.Fatal(e)
		}
	}
	originalM := proto.Clone(old.Native.Timers.Values[string(portable.TimerM)])
	dialog, err := readPrivateDialogRecord(before.Records[dialogField])
	if err != nil || !dialog.AckWritten || dialog.LocalInvite == nil || dialog.LocalInvite.Ack == "" {
		t.Fatal("old written ACK", err)
	}
	ackWire, finalWire := dialog.LocalInvite.Ack, dialog.LocalInvite.FinalResponse
	deadline := time.Now().Add(2 * time.Second)
	command := &pb.Command{Context: input.Context, Sequence: 3, Admission: &pb.CommandAdmission{DirectoryEpoch: "directory", ExpiresAtMs: uint64(deadline.UnixMilli()), SelectedFlow: input.Flow}, Action: &pb.Command_DialogRequest{DialogRequest: &pb.DialogRequest{Method: "INVITE", Body: []byte("v=0\r\ns=offer\r\n"), Headers: []*pb.Header{{Name: "Content-Type", Value: "application/sdp"}}}}}
	capture, err := captureRedisNotifyWait(command)
	if err != nil {
		t.Fatal(err)
	}
	leaseUntil := testRedisInviteWorkerAnchor(t, ctx, store, client, input)
	if _, err = publishRedisApplicationInvite(ctx, store, input.Key, input.Application, input.Lease, command, capture, "transfer-reinvite", input.ExecutorTimer, deadline, leaseUntil); err != nil {
		t.Fatal("replace after consumed ACK", err)
	}
	after, err := store.Read(ctx, identity, ackOutput.Procedure, ackOutput.Result, ackOutputField, dialogField, ackWire, finalWire)
	if err != nil {
		t.Fatal(err)
	}
	owner, err := readPrivateProcedureRecord(after.Records[ackOutput.Procedure])
	result, resultErr := decodeNativeResult(after.Records[ackOutput.Result])
	updatedDialog, dialogErr := readPrivateDialogRecord(after.Records[dialogField])
	if err != nil || resultErr != nil || dialogErr != nil || owner.Native.Timers == nil || owner.Native.Timers.Values[string(portable.TimerM)] == nil || !proto.Equal(owner.Native.Timers.Values[string(portable.TimerM)], originalM) || result.Output != "" || result.Request != ackWire || len(after.Records[ackOutputField]) != 0 || len(after.Records[ackWire]) == 0 || updatedDialog.LocalInvite == nil || updatedDialog.LocalInvite.Request == old.Origin {
		t.Fatal("consumed ACK transfer lost its exact live owners", err, resultErr, dialogErr)
	}
	if overdue {
		if owner.SuccessfulAck != nil {
			t.Fatal("overdue M created a retained replay owner")
		}
	} else if owner.SuccessfulAck == nil || owner.SuccessfulAck.Ack != ackWire || owner.SuccessfulAck.FinalResponse != finalWire || owner.SuccessfulAck.AckResult != ackOutput.Result || !proto.Equal(owner.SuccessfulAck.Flow, input.Flow) {
		t.Fatal("live M did not receive its exact ACK owner")
	}
	if mFirst {
		checked, e := client.Time(ctx).Result()
		if e != nil {
			t.Fatal(e)
		}
		m := owner.Native.Timers.Values[string(portable.TimerM)]
		m.At = checked.Add(-time.Millisecond).UnixMilli()
		value, e := marshalNativeRecord(storagecodec.NativeProcedure, owner)
		if e != nil {
			t.Fatal(e)
		}
		member := "call-tx-" + strings.TrimPrefix(ackOutput.Procedure, "r:uac-procedure-") + "-" + string(portable.TimerM)
		_, e = store.Commit(ctx, rediscore.Publication{Identity: identity, Revision: after.Revision, Deadline: time.Now().Add(time.Second), SpendReservation: true, Changes: []rediscore.Change{{Field: ackOutput.Procedure, Value: value}}, Timers: &rediscore.TimerMutation{Expected: after.Timer, Target: after.Timer, RequireLease: true, Changes: []rediscore.TimerChange{{Member: member, Due: time.UnixMilli(m.At)}}}})
		if e != nil {
			t.Fatal("age original M", e)
		}
		after, e = store.Read(ctx, identity, ackOutput.Procedure, ackOutput.Result, ackWire)
		if e != nil {
			t.Fatal(e)
		}
		checked = waitRedisFixtureTime(t, ctx, client, time.UnixMilli(m.At))
		due := &rediscore.DueContext{Identity: identity, Revision: after.Revision, Assignment: after.Timer, Member: member, DeadlineMS: m.At, CheckedAtMS: checked.UnixMilli()}
		if e = fireRedisUACDeadline(ctx, store, due, time.Now().Add(time.Second)); e != nil {
			t.Fatal("fire M before result expiry", e)
		}
		after, e = store.Read(ctx, identity, ackOutput.Procedure, ackOutput.Result, ackWire)
		if e != nil {
			t.Fatal(e)
		}
		owner, e = readPrivateProcedureRecord(after.Records[ackOutput.Procedure])
		if e != nil || owner.SuccessfulAck != nil || len(after.Records[ackWire]) == 0 {
			t.Fatal("M did not preserve result-owned ACK", e)
		}
	}
	// Advance only the original ACK result cutoff and its matching duty.
	result.CutoffMs = time.Now().Add(-time.Second).UnixMilli()
	data, err := encodeNativeResult(result)
	if err != nil {
		t.Fatal(err)
	}
	member := "orig-result-2"
	_, err = store.Commit(ctx, rediscore.Publication{Identity: identity, Revision: after.Revision, Deadline: time.Now().Add(time.Second), SpendReservation: true, Changes: []rediscore.Change{{Field: ackOutput.Result, Value: data}}, Timers: &rediscore.TimerMutation{Expected: after.Timer, Target: after.Timer, RequireLease: true, Changes: []rediscore.TimerChange{{Member: member, Due: time.UnixMilli(result.CutoffMs)}}}})
	if err != nil {
		t.Fatal("age original ACK result", err)
	}
	selected, err := store.Read(ctx, identity, "r:context")
	if err != nil {
		t.Fatal(err)
	}
	due := &rediscore.DueContext{Identity: identity, Revision: selected.Revision, Assignment: selected.Timer, Member: member, DeadlineMS: result.CutoffMs, CheckedAtMS: time.Now().UnixMilli()}
	if err = expireRedisSupersededResult(ctx, store, due, time.Now().Add(time.Second)); err != nil {
		t.Fatal("expire transferred ACK result", err)
	}
	closed, err := store.Read(ctx, identity, ackOutput.Procedure, ackOutput.Result, ackWire, finalWire)
	if err != nil {
		t.Fatal(err)
	}
	owner, err = readPrivateProcedureRecord(closed.Records[ackOutput.Procedure])
	if err != nil || len(closed.Records[ackOutput.Result]) != 0 || len(closed.Records[finalWire]) == 0 {
		t.Fatal("result expiry lost native ACK ownership", err)
	}
	if overdue || mFirst {
		if owner.SuccessfulAck != nil || len(closed.Records[ackWire]) != 0 {
			t.Fatal("expired M retained unowned ACK")
		}
	} else if owner.SuccessfulAck == nil || owner.SuccessfulAck.AckResult != "" || owner.SuccessfulAck.Ack != ackWire || len(closed.Records[ackWire]) == 0 {
		t.Fatal("live M lost its ACK after result expiry")
	}
	if overdue || mFirst {
		return
	}
	// The old exact 200 is handled from the procedure after the new INVITE
	// replaced its dialog slot, without changing that new slot or M.
	selected, err = store.Read(ctx, identity, "r:context", ackOutput.Procedure, finalWire, dialogField)
	if err != nil {
		t.Fatal(err)
	}
	current, err := decodeNativeContext(selected.Records["r:context"])
	if err != nil {
		t.Fatal(err)
	}
	procedure, err := decodeNativeProcedure(selected.Records[ackOutput.Procedure])
	if err != nil {
		t.Fatal(err)
	}
	final, err := selectedmessage.DecodeOpaque(selected.Records[finalWire])
	if err != nil {
		t.Fatal(err)
	}
	leaseUntil = testRedisInviteWorkerAnchor(t, ctx, store, client, input)
	// Renewal can update the assignment; capture the actual selected revision.
	selected, err = store.Read(ctx, identity, "r:context", ackOutput.Procedure, dialogField)
	if err != nil {
		t.Fatal(err)
	}
	current, err = decodeNativeContext(selected.Records["r:context"])
	if err != nil {
		t.Fatal(err)
	}
	procedure, err = decodeNativeProcedure(selected.Records[ackOutput.Procedure])
	if err != nil {
		t.Fatal(err)
	}
	dialogBefore := append([]byte(nil), selected.Records[dialogField]...)
	handled, err := receiveRedisRetainedSuccessfulACK(ctx, store, identity, selected.Revision, selected.Timer, current, ackOutput.Procedure, procedure, input.Flow, final, input.Key, input.ExecutorTimer, leaseUntil, time.Now().Add(2*time.Second))
	if err != nil || !handled {
		t.Fatal("retained ACK replay", handled, err)
	}
	replayed, err := store.Read(ctx, identity, ackOutput.Procedure, dialogField)
	if err != nil {
		t.Fatal(err)
	}
	owner, err = readPrivateProcedureRecord(replayed.Records[ackOutput.Procedure])
	if err != nil || !proto.Equal(owner.Native.Timers.Values[string(portable.TimerM)], originalM) || string(replayed.Records[dialogField]) != string(dialogBefore) {
		t.Fatal("replay changed M or current dialog", err)
	}
	var physical string
	for _, field := range owner.Outputs {
		if strings.HasPrefix(field, "r:retained-ack-output-") {
			physical = field
		}
	}
	if physical == "" {
		t.Fatal("retained ACK physical output missing")
	}
	outputView, err := store.Read(ctx, identity, physical)
	if err != nil {
		t.Fatal(err)
	}
	output, err := decodeNativeOutput(outputView.Records[physical])
	if err != nil || !validReadyRetainedACK(output, physical) {
		t.Fatal("invalid retained ACK metadata", err)
	}
	writer := nativeRedisWriter{store: store}
	ack, err := selectedmessage.DecodeOpaque(closed.Records[ackWire])
	if err != nil {
		t.Fatal(err)
	}
	reserved, err := writer.Reserve(ctx, output.Write)
	if err != nil || string(reserved.Wire) != string(ack.WireBytes()) {
		t.Fatal("reserve retained ACK", err)
	}
	// A consumed retained replay may still have physical metadata before its
	// original cutoff; another actual final still requires its exact ACK.
	if err = writer.Complete(ctx, output.Write, &pb.DeliveryResult{Effect: output.Write.Effect, Outcome: pb.DeliveryResult_WRITTEN}); err != nil {
		t.Fatal(err)
	}
	if err = settleRedisRetainedACK(ctx, store, output.Write, nil, input.ExecutorTimer, time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if err = acknowledgeRedisRetainedACK(ctx, store, output.Write, nil); err != nil {
		t.Fatal(err)
	}
	oldPhysical := physical
	selected, err = store.Read(ctx, identity, "r:context", ackOutput.Procedure, dialogField, oldPhysical)
	if err != nil {
		t.Fatal(err)
	}
	if len(selected.Records[oldPhysical]) == 0 || time.Now().UnixMilli() >= int64(output.Write.Permit.Authorization.DeadlineMs) {
		t.Fatal("fixture missed retained positive ACK metadata window")
	}
	current, err = decodeNativeContext(selected.Records["r:context"])
	if err != nil {
		t.Fatal(err)
	}
	procedure, err = decodeNativeProcedure(selected.Records[ackOutput.Procedure])
	if err != nil {
		t.Fatal(err)
	}
	priorEmissions := procedure.Metadata.SuccessfulAck.Emissions
	if handled, x := receiveRedisRetainedSuccessfulACK(ctx, store, identity, selected.Revision, selected.Timer, current, ackOutput.Procedure, procedure, input.Flow, final, input.Key, input.ExecutorTimer, leaseUntil, time.Now().Add(time.Second)); x != nil || !handled {
		t.Fatal("retained final replay before old cutoff", handled, x)
	}
	selected, err = store.Read(ctx, identity, ackOutput.Procedure, dialogField, oldPhysical)
	if err != nil {
		t.Fatal(err)
	}
	owner, err = readPrivateProcedureRecord(selected.Records[ackOutput.Procedure])
	if err != nil || owner.SuccessfulAck.Emissions != priorEmissions+1 || len(selected.Records[oldPhysical]) != 0 || !proto.Equal(owner.Native.Timers.Values[string(portable.TimerM)], originalM) || string(selected.Records[dialogField]) != string(dialogBefore) {
		t.Fatal("consumed retained ACK metadata suppressed replay before cutoff", err)
	}
	physical = ""
	for _, field := range owner.Outputs {
		if strings.HasPrefix(field, "r:retained-ack-output-") {
			physical = field
		}
	}
	selected, err = store.Read(ctx, identity, physical)
	if err != nil {
		t.Fatal(err)
	}
	output, err = decodeNativeOutput(selected.Records[physical])
	if err != nil {
		t.Fatal(err)
	}
	reserved, err = writer.Reserve(ctx, output.Write)
	if err != nil || string(reserved.Wire) != string(ack.WireBytes()) {
		t.Fatal("repeated retained ACK changed bytes", err)
	}
	if err = writer.Complete(ctx, output.Write, &pb.DeliveryResult{Effect: output.Write.Effect, Outcome: pb.DeliveryResult_UNKNOWN}); err != nil {
		t.Fatal(err)
	}
	if err = settleRedisRetainedACK(ctx, store, output.Write, nil, input.ExecutorTimer, time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	// Advance only the original native M and its matching index. An unresolved
	// replay proof must retain the saved pair even after that real handler runs.
	selected, err = store.Read(ctx, identity, ackOutput.Procedure)
	if err != nil {
		t.Fatal(err)
	}
	owner, err = readPrivateProcedureRecord(selected.Records[ackOutput.Procedure])
	if err != nil {
		t.Fatal(err)
	}
	checked, err := client.Time(ctx).Result()
	if err != nil {
		t.Fatal(err)
	}
	m := owner.Native.Timers.Values[string(portable.TimerM)]
	m.At = checked.Add(-time.Millisecond).UnixMilli()
	data, err = marshalNativeRecord(storagecodec.NativeProcedure, owner)
	if err != nil {
		t.Fatal(err)
	}
	mMember := "call-tx-" + strings.TrimPrefix(ackOutput.Procedure, "r:uac-procedure-") + "-" + string(portable.TimerM)
	_, err = store.Commit(ctx, rediscore.Publication{Identity: identity, Revision: selected.Revision, Deadline: time.Now().Add(time.Second), SpendReservation: true, Changes: []rediscore.Change{{Field: ackOutput.Procedure, Value: data}}, Timers: &rediscore.TimerMutation{Expected: selected.Timer, Target: selected.Timer, RequireLease: true, Changes: []rediscore.TimerChange{{Member: mMember, Due: time.UnixMilli(m.At)}}}})
	if err != nil {
		t.Fatal("age original M", err)
	}
	selected, err = store.Read(ctx, identity, ackOutput.Procedure)
	if err != nil {
		t.Fatal(err)
	}
	checked = waitRedisFixtureTime(t, ctx, client, time.UnixMilli(m.At))
	mDue := &rediscore.DueContext{Identity: identity, Revision: selected.Revision, Assignment: selected.Timer, Member: mMember, DeadlineMS: m.At, CheckedAtMS: checked.UnixMilli()}
	if err = fireRedisUACDeadline(ctx, store, mDue, time.Now().Add(time.Second)); err != nil {
		t.Fatal("fire original M", err)
	}
	selected, err = store.Read(ctx, identity, ackOutput.Procedure)
	if err != nil {
		t.Fatal(err)
	}
	owner, err = readPrivateProcedureRecord(selected.Records[ackOutput.Procedure])
	if err != nil || owner.SuccessfulAck == nil || owner.Native.Phase != string(portable.Terminated) {
		t.Fatal("M lost unresolved replay", err)
	}
	if err = writer.Complete(ctx, output.Write, &pb.DeliveryResult{Effect: output.Write.Effect, Outcome: pb.DeliveryResult_WRITTEN}); err != nil {
		t.Fatal(err)
	}
	if err = settleRedisRetainedACK(ctx, store, output.Write, nil, input.ExecutorTimer, time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if err = acknowledgeRedisRetainedACK(ctx, store, output.Write, nil); err != nil {
		t.Fatal(err)
	}
	physicalDeadline := time.UnixMilli(int64(output.Write.Permit.Authorization.DeadlineMs))
	limit := time.Now().Add(3 * time.Second)
	nextRenew := time.Now()
	for {
		if !time.Now().Before(nextRenew) {
			testRedisInviteWorkerAnchor(t, ctx, store, client, input)
			nextRenew = time.Now().Add(250 * time.Millisecond)
		}
		checked, err = client.Time(ctx).Result()
		if err != nil {
			t.Fatal(err)
		}
		if !checked.Before(physicalDeadline) {
			break
		}
		if !time.Now().Before(limit) {
			t.Fatal("retained ACK physical deadline did not become due")
		}
		time.Sleep(5 * time.Millisecond)
	}
	testRedisInviteWorkerAnchor(t, ctx, store, client, input)
	selected, err = store.Read(ctx, identity, ackOutput.Procedure)
	if err != nil {
		t.Fatal(err)
	}
	ackDue := &rediscore.DueContext{Identity: identity, Revision: selected.Revision, Assignment: selected.Timer, Member: "out-" + strings.TrimPrefix(physical, "r:"), DeadlineMS: int64(output.Write.Permit.Authorization.DeadlineMs), CheckedAtMS: checked.UnixMilli()}
	if err = retireRedisRetainedACK(ctx, store, ackDue, time.Now().Add(time.Second)); err != nil {
		t.Fatal("retire retained ACK", err)
	}
	closed, err = store.Read(ctx, identity, ackOutput.Procedure, ackWire, physical)
	if err != nil {
		t.Fatal(err)
	}
	owner, err = readPrivateProcedureRecord(closed.Records[ackOutput.Procedure])
	if err != nil || owner.SuccessfulAck != nil || len(closed.Records[physical]) != 0 || len(closed.Records[ackWire]) != 0 {
		t.Fatal("M and last proof did not release saved ACK", err)
	}
}
