package engine

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/emiago/sipgo/sip/portable"
	"github.com/redis/go-redis/v9"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/nativepb"
	"sipbridge/internal/rediscore"
)

func testRedisPreviousInviteFailureReplay(t *testing.T, ctx context.Context, store *rediscore.Store, client *redis.Client, input redisUACQualification, identity rediscore.Identity, procedureField, dialogField, ackField string, ackOutput *nativepb.Output, response portable.Message, ackWire []byte) {
	t.Helper()
	// Close the original ACK's real physical duty while keeping original D live.
	physicalDeadline := time.UnixMilli(int64(ackOutput.Write.Permit.Authorization.DeadlineMs))
	limit, renew := time.Now().Add(3*time.Second), time.Now()
	var checked time.Time
	for {
		if !time.Now().Before(renew) {
			testRedisInviteWorkerAnchor(t, ctx, store, client, input)
			renew = time.Now().Add(250 * time.Millisecond)
		}
		var err error
		checked, err = client.Time(ctx).Result()
		if err != nil {
			t.Fatal(err)
		}
		if !checked.Before(physicalDeadline) {
			break
		}
		if !time.Now().Before(limit) {
			t.Fatal("original ACK cutoff did not become due")
		}
		time.Sleep(5 * time.Millisecond)
	}
	selected, err := store.Read(ctx, identity, procedureField)
	if err != nil {
		t.Fatal(err)
	}
	due := &rediscore.DueContext{Identity: identity, Revision: selected.Revision, Assignment: selected.Timer, Member: "out-" + strings.TrimPrefix(ackField, "r:"), DeadlineMS: physicalDeadline.UnixMilli(), CheckedAtMS: checked.UnixMilli()}
	if err = retireRedisApplicationInviteACK(ctx, store, due, input.Key, time.Now().Add(time.Second)); err != nil {
		t.Fatal("retire original transaction ACK", err)
	}
	// A new INVITE replaces the dialog's local slot while the old transaction
	// still owns its non-2xx ACK and original D.
	deadline := time.Now().Add(2 * time.Second)
	command := &pb.Command{Context: input.Context, Sequence: 5, Admission: &pb.CommandAdmission{DirectoryEpoch: "directory", ExpiresAtMs: uint64(deadline.UnixMilli()), SelectedFlow: input.Flow}, Action: &pb.Command_DialogRequest{DialogRequest: &pb.DialogRequest{Method: "INVITE", Body: []byte("v=0\r\ns=next\r\n"), Headers: []*pb.Header{{Name: "Content-Type", Value: "application/sdp"}}}}}
	capture, err := captureRedisNotifyWait(command)
	if err != nil {
		t.Fatal(err)
	}
	leaseUntil := testRedisInviteWorkerAnchor(t, ctx, store, client, input)
	if _, err = publishRedisApplicationInvite(ctx, store, input.Key, input.Application, input.Lease, command, capture, "after-failure", input.ExecutorTimer, deadline, leaseUntil); err != nil {
		t.Fatal("replace failed INVITE", err)
	}
	selected, err = store.Read(ctx, identity, "r:context", procedureField, dialogField, "r:result-4")
	if err != nil {
		t.Fatal(err)
	}
	current, err := decodeNativeContext(selected.Records["r:context"])
	if err != nil {
		t.Fatal(err)
	}
	procedure, err := decodeNativeProcedure(selected.Records[procedureField])
	if err != nil {
		t.Fatal(err)
	}
	if procedure.Native.Phase != portable.Completed || !procedure.Native.Timers[portable.TimerD].Active {
		t.Fatal("old original D not retained")
	}
	originalD := procedure.Native.Timers[portable.TimerD]
	dialogBefore := append([]byte(nil), selected.Records[dialogField]...)
	handled, err := replayRedisPreviousInviteFailure(ctx, store, identity, selected.Revision, selected.Timer, current, procedureField, procedure, "reinvite", response, input.Flow, input.Key, input.ExecutorTimer, leaseUntil, time.Now().Add(time.Second))
	if err != nil || !handled {
		t.Fatal("replay old non-2xx", handled, err)
	}
	selected, err = store.Read(ctx, identity, procedureField, dialogField)
	if err != nil {
		t.Fatal(err)
	}
	owner, err := readPrivateProcedureRecord(selected.Records[procedureField])
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(dialogBefore, selected.Records[dialogField]) || owner.Native.Timers.Values[string(portable.TimerD)].At != originalD.At {
		t.Fatal("old failure replay changed new dialog or D")
	}
	var field string
	for _, f := range owner.Outputs {
		if strings.HasPrefix(f, "r:app-invite-ack-output-") {
			field = f
		}
	}
	if field == "" {
		t.Fatal("replayed transaction ACK missing")
	}
	selected, err = store.Read(ctx, identity, field)
	if err != nil {
		t.Fatal(err)
	}
	output, err := decodeNativeOutput(selected.Records[field])
	if err != nil {
		t.Fatal(err)
	}
	writer := nativeRedisWriter{store: store}
	reserved, err := writer.Reserve(ctx, output.Write)
	if err != nil || !bytes.Equal(reserved.Wire, ackWire) {
		t.Fatal("wrong replay ACK wire", err)
	}
	if err = writer.Complete(ctx, output.Write, &pb.DeliveryResult{Effect: output.Write.Effect, Outcome: pb.DeliveryResult_WRITTEN}); err != nil {
		t.Fatal(err)
	}
	if err = settleRedisApplicationInviteACK(ctx, store, output.Write, nil, input.Key, input.ExecutorTimer, time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if err = acknowledgeRedisApplicationInviteACK(ctx, store, output.Write, time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
}
