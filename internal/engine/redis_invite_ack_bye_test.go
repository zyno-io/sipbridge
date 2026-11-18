package engine

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/emiago/sipgo/sip/portable"
	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/nativepb"
	"sipbridge/internal/rediscore"
)

func testRedisInviteACKDuringBye(t *testing.T, ctx context.Context, store *rediscore.Store, client *redis.Client, input redisUACQualification, identity rediscore.Identity, dialogField string, ack *nativepb.Output, response portable.Message) {
	t.Helper()
	checkSuppressed := func() {
		ids, err := nativeSelectorIDs(response, false, false)
		if err != nil || len(ids) == 0 {
			t.Fatal("response selector", err)
		}
		selected, err := store.LookupSelector(ctx, ids[0], "r:context")
		if err != nil {
			t.Fatal(err)
		}
		current, err := decodeNativeContext(selected.View.Records["r:context"])
		if err != nil {
			t.Fatal(err)
		}
		procedure, err := decodeNativeProcedure(selected.View.Records[selected.Record])
		if err != nil {
			t.Fatal(err)
		}
		leaseUntil := testRedisInviteWorkerAnchor(t, ctx, store, client, input)
		handled, err := replayRedisCurrentInviteACK(ctx, store, selected, current, procedure, input.Flow, response, input.Key, input.ExecutorTimer, leaseUntil, time.Now().Add(time.Second))
		if err != nil || !handled {
			t.Fatal("suppress duplicate while ACK is UNKNOWN", handled, err)
		}
		after, err := store.Read(ctx, selected.Identity, selected.Record)
		if err != nil || after.Revision != selected.View.Revision {
			t.Fatal("UNKNOWN replay published another attempt", err)
		}
	}
	checkSuppressed()
	deadline := time.Now().Add(2 * time.Second)
	command := &pb.Command{Context: input.Context, Sequence: 5, Admission: &pb.CommandAdmission{DirectoryEpoch: "directory", ExpiresAtMs: uint64(deadline.UnixMilli()), SelectedFlow: input.Flow}, Action: &pb.Command_DialogRequest{DialogRequest: &pb.DialogRequest{Method: "BYE"}}}
	capture, err := captureRedisNotifyWait(command)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = publishRedisApplicationBye(ctx, store, input.Key, input.Application, input.Lease, command, capture, "bye-after-invite", input.ExecutorTimer, deadline); err != nil {
		t.Fatal("admit BYE during uncertain ACK", err)
	}
	selected, err := store.Read(ctx, identity, "r:context", dialogField)
	if err != nil {
		t.Fatal(err)
	}
	current, err := decodeNativeContext(selected.Records["r:context"])
	if err != nil {
		t.Fatal(err)
	}
	dialog, err := readPrivateDialogRecord(selected.Records[dialogField])
	if err != nil || dialog.Native.Phase != string(portable.DialogTerminating) || dialog.LocalBye == "" || dialog.AckWritten {
		t.Fatal("BYE did not retain uncertain ACK", err)
	}
	var byeField string
	for _, field := range current.NoninitialProcedures {
		if strings.HasPrefix(field, "r:app-bye-") {
			byeField = field
		}
	}
	if byeField == "" {
		t.Fatal("actual application BYE owner missing")
	}
	checkSuppressed()
	before, err := store.Read(ctx, identity, byeField)
	if err != nil {
		t.Fatal(err)
	}
	bye, err := readPrivateProcedureRecord(before.Records[byeField])
	if err != nil || bye.Native.Timers == nil || bye.Native.Timers.Values[string(portable.TimerF)] == nil {
		t.Fatal("BYE F missing", err)
	}
	timerF := proto.Clone(bye.Native.Timers.Values[string(portable.TimerF)])
	writer := nativeRedisWriter{store: store}
	if err = writer.Complete(ctx, ack.Write, &pb.DeliveryResult{Effect: ack.Write.Effect, Outcome: pb.DeliveryResult_WRITTEN}); err != nil {
		t.Fatal("late ACK proof", err)
	}
	if err = settleRedisApplicationInviteACK(ctx, store, ack.Write, nil, input.Key, input.ExecutorTimer, time.Now().Add(time.Second)); err != nil {
		t.Fatal("consume ACK while BYE pending", err)
	}
	if err = acknowledgeRedisApplicationInviteACK(ctx, store, ack.Write, time.Now().Add(time.Second)); err != nil {
		t.Fatal("acknowledge late ACK", err)
	}
	after, err := store.Read(ctx, identity, "r:context", dialogField, byeField)
	if err != nil {
		t.Fatal(err)
	}
	next, err := decodeNativeContext(after.Records["r:context"])
	if err != nil {
		t.Fatal(err)
	}
	newDialog, err := readPrivateDialogRecord(after.Records[dialogField])
	if err != nil {
		t.Fatal(err)
	}
	newBye, err := readPrivateProcedureRecord(after.Records[byeField])
	if err != nil {
		t.Fatal(err)
	}
	if !newDialog.AckWritten || newDialog.LocalBye != dialog.LocalBye || newDialog.LocalTransaction != dialog.LocalTransaction || !slices.Equal(current.NoninitialProcedures, next.NoninitialProcedures) || newBye.Native.Timers == nil || newBye.Native.Timers.Values[string(portable.TimerF)] == nil || !proto.Equal(newBye.Native.Timers.Values[string(portable.TimerF)], timerF) {
		t.Fatal("late ACK changed BYE or its original F")
	}
}
