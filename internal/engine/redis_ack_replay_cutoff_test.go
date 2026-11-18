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

	"github.com/emiago/sipgo/sip/portable"
	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/nativepb"
	"sipbridge/internal/rediscore"
	"sipbridge/internal/selectedmessage"
)

// Finish a real ownerless fork's positive ACK and cleanup BYE, then retransmit
// its exact successful final while consumed ACK metadata is still retained.
func testRedisCleanupACKReplayBeforeCutoff(t *testing.T, ctx context.Context, store *rediscore.Store, input redisUACQualification, worker string, final portable.Message, ack *pb.FlowWrite, wire []byte, metadata *nativepb.Output, ref rediscore.OutputRef) {
	t.Helper()
	deadline := func() time.Time { return time.Now().Add(time.Second) }
	proof, err := store.InspectOutput(ctx, ref)
	if err != nil || proof.State != rediscore.OutputWritten || !proof.Settled {
		t.Fatal("initial cleanup ACK positive proof", err)
	}
	oldField := proof.MetadataField
	if err = acknowledgeRedisUACSuccessACK(ctx, store, ack, &nativeOutputCapture{ref: ref, proof: proof, metadata: metadata}); err != nil {
		t.Fatal(err)
	}
	writes, err := nativeReadyWrites(ctx, store, worker, input.Executor.Epoch, 8)
	if err != nil {
		t.Fatal(err)
	}
	writer := nativeRedisWriter{store: store}
	var bye *pb.FlowWrite
	for _, write := range writes {
		r, x := nativeOutputRef(write)
		if x != nil {
			t.Fatal(x)
		}
		p, x := store.InspectOutput(ctx, r)
		if x != nil {
			t.Fatal(x)
		}
		m, x := decodeNativeOutput(p.Metadata)
		if x != nil {
			t.Fatal(x)
		}
		if m.Method == "BYE" && m.Dialog == metadata.Dialog {
			bye = write
		}
	}
	if bye == nil {
		t.Fatal("cleanup BYE missing")
	}
	reserved, err := writer.Reserve(ctx, bye)
	if err != nil {
		t.Fatal(err)
	}
	if err = writer.Complete(ctx, bye, &pb.DeliveryResult{Effect: bye.Effect, Outcome: pb.DeliveryResult_WRITTEN}); err != nil {
		t.Fatal(err)
	}
	if err = settleRedisCleanupByeOutput(ctx, store, bye, nil, input.ExecutorTimer, deadline()); err != nil {
		t.Fatal(err)
	}
	if err = acknowledgeRedisCleanupByeOutput(ctx, store, bye, deadline()); err != nil {
		t.Fatal(err)
	}
	request := portable.RestoreMessage(reserved.Wire, input.Flow.Transport, "", "")
	response, err := portable.ResponseFor(request, 200, "OK", nil, "")
	if err != nil {
		t.Fatal(err)
	}
	response = portable.RestoreMessage(response.WireBytes(), input.Flow.Transport, net.JoinHostPort(input.Flow.SourceIp, strconv.Itoa(int(input.Flow.SourcePort))), "")
	if handled, x := completeRedisCleanupByeResponse(ctx, store, input.Flow, response, nil, deadline()); x != nil || !handled {
		t.Fatal("cleanup BYE matched final", handled, x)
	}
	before, err := store.ObserveContext(ctx, "uac", "incarnation", "r:context", metadata.Dialog, metadata.Procedure, oldField)
	if err != nil {
		t.Fatal(err)
	}
	dialog, err := readPrivateDialogRecord(before.View.Records[metadata.Dialog])
	if err != nil || dialog.Native.Phase != string(portable.DialogEnded) || !dialog.AckWritten || len(before.View.Records[oldField]) == 0 || time.Now().UnixMilli() >= proof.DeadlineMS {
		t.Fatal("fixture missed ended fork's consumed metadata window", err)
	}
	if _, err = store.InspectOutput(ctx, ref); !errors.Is(err, rediscore.ErrAbsent) {
		t.Fatal("cleanup ACK proof was not consumed", err)
	}
	owner, err := readPrivateProcedureRecord(before.View.Records[metadata.Procedure])
	if err != nil {
		t.Fatal(err)
	}
	originalM := owner.Native.Timers.Values[string(portable.TimerM)]
	if originalM == nil || !originalM.Active {
		t.Fatal("original fork M missing")
	}
	replay := func(count uint32, removed bool) {
		t.Helper()
		if handled, x := receiveRedisUACProvisional(ctx, store, input.Flow, final, input.Key, input.ExecutorTimer, deadline()); x != nil || !handled {
			t.Fatal("ownerless successful final replay", handled, x)
		}
		after, x := store.ObserveContext(ctx, "uac", "incarnation", "r:context", metadata.Dialog, metadata.Procedure, oldField)
		if x != nil {
			t.Fatal(x)
		}
		d, x := readPrivateDialogRecord(after.View.Records[metadata.Dialog])
		if x != nil {
			t.Fatal(x)
		}
		p, x := readPrivateProcedureRecord(after.View.Records[metadata.Procedure])
		if x != nil || d.AckEmissions != count || !d.AckWritten || p.Native.Timers.Values[string(portable.TimerM)].At != originalM.At {
			t.Fatal("consumed cleanup ACK metadata suppressed replay or changed M", x)
		}
		if removed && (len(after.View.Records[oldField]) != 0 || slices.Contains(p.Outputs, oldField) || slices.Contains(d.Outputs, oldField)) {
			t.Fatal("consumed old ACK metadata still owns replay")
		}
		c, x := decodeNativeContext(after.View.Records["r:context"])
		if x != nil || after.Identity.Owner != "" || after.Identity.Term != "" || c.Answered || c.CommandSequence != 1 || len(c.NoninitialProcedures) != 1 {
			t.Fatal("replay revived owner/call or duplicated cleanup", x)
		}
	}
	reserveReplay := func() (*pb.FlowWrite, *nativeOutputCapture) {
		t.Helper()
		entries, x := store.PeekReady(ctx, worker, input.Executor.Epoch, 8)
		if x != nil {
			t.Fatal(x)
		}
		for _, entry := range entries {
			m, x := decodeNativeOutput(entry.Metadata)
			if x != nil {
				t.Fatal(x)
			}
			if m.Method != "ACK" || m.Dialog != metadata.Dialog {
				continue
			}
			actual, x := writer.Reserve(ctx, m.Write)
			if x != nil || !bytes.Equal(actual.Wire, wire) {
				t.Fatal("fork replay changed exact ACK bytes", x)
			}
			return m.Write, &nativeOutputCapture{ref: entry.Ref, metadata: m}
		}
		t.Fatal("actual fork ACK replay missing")
		return nil, nil
	}
	replay(2, true)
	attempt, captured := reserveReplay()
	if err = writer.Complete(ctx, attempt, &pb.DeliveryResult{Effect: attempt.Effect, Outcome: pb.DeliveryResult_UNKNOWN}); err != nil {
		t.Fatal(err)
	}
	captured.proof, err = store.InspectOutput(ctx, captured.ref)
	if err != nil {
		t.Fatal(err)
	}
	if err = settleRedisUACSuccessACK(ctx, store, attempt, captured, input.Key, input.ExecutorTimer, deadline()); err != nil {
		t.Fatal(err)
	}
	replay(2, true) // A prior W cannot lend authority to the newer U attempt.
	if err = writer.Complete(ctx, attempt, &pb.DeliveryResult{Effect: attempt.Effect, Outcome: pb.DeliveryResult_NOT_WRITTEN}); err != nil {
		t.Fatal(err)
	}
	replay(2, true) // Present N proof must also remain independently owned.
	captured.proof, err = store.InspectOutput(ctx, captured.ref)
	if err != nil {
		t.Fatal(err)
	}
	if err = settleRedisUACSuccessACK(ctx, store, attempt, captured, input.Key, input.ExecutorTimer, deadline()); err != nil {
		t.Fatal(err)
	}
	captured.proof, err = store.InspectOutput(ctx, captured.ref)
	if err != nil {
		t.Fatal(err)
	}
	if err = acknowledgeRedisUACSuccessACK(ctx, store, attempt, captured); err != nil {
		t.Fatal(err)
	}
	replay(3, true) // The exact cached wire still has its independent initial W.
	attempt, captured = reserveReplay()
	if err = writer.Complete(ctx, attempt, &pb.DeliveryResult{Effect: attempt.Effect, Outcome: pb.DeliveryResult_WRITTEN}); err != nil {
		t.Fatal(err)
	}
	captured.proof, err = store.InspectOutput(ctx, captured.ref)
	if err != nil {
		t.Fatal(err)
	}
	if err = settleRedisUACSuccessACK(ctx, store, attempt, captured, input.Key, input.ExecutorTimer, deadline()); err != nil {
		t.Fatal(err)
	}
	captured.proof, err = store.InspectOutput(ctx, captured.ref)
	if err != nil {
		t.Fatal(err)
	}
	if err = acknowledgeRedisUACSuccessACK(ctx, store, attempt, captured); err != nil {
		t.Fatal(err)
	}
}

// The application answer owns its ACK wire/result even when a successful-final
// replay retires its consumed physical output before that original cutoff.
func testRedisApplicationACKReplayBeforeCutoff(t *testing.T, ctx context.Context, store *rediscore.Store, input redisUACQualification, client *redis.Client, root string, old *nativepb.Output, oldField, resultField string, wire []byte) {
	t.Helper()
	view, err := store.ObserveContext(ctx, "uac", "incarnation", "r:context", old.Dialog, old.Procedure, oldField, resultField)
	if err != nil {
		t.Fatal(err)
	}
	dialog, err := readPrivateDialogRecord(view.View.Records[old.Dialog])
	if err != nil || !dialog.AckWritten || dialog.AckResult != resultField || time.Now().UnixMilli() >= int64(old.Write.Permit.Authorization.DeadlineMs) {
		t.Fatal("application ACK consumed metadata window", err)
	}
	owner, err := readPrivateProcedureRecord(view.View.Records[old.Procedure])
	if err != nil {
		t.Fatal(err)
	}
	originalM := owner.Native.Timers.Values[string(portable.TimerM)].At
	original, err := decodeNativeResult(view.View.Records[resultField])
	if err != nil {
		t.Fatal(err)
	}
	selected, err := store.Read(ctx, view.Identity, dialog.LocalInvite.FinalResponse)
	if err != nil {
		t.Fatal(err)
	}
	final, err := selectedmessage.DecodeOpaque(selected.Records[dialog.LocalInvite.FinalResponse])
	if err != nil {
		t.Fatal(err)
	}
	if handled, x := receiveRedisUACProvisional(ctx, store, input.Flow, final, input.Key, input.ExecutorTimer, time.Now().Add(time.Second)); x != nil || !handled {
		t.Fatal("application-answer cached ACK replay", handled, x)
	}
	after, err := store.Read(ctx, view.Identity, "r:context", old.Dialog, old.Procedure, oldField, resultField, dialog.LocalInvite.Ack)
	if err != nil {
		t.Fatal(err)
	}
	d, err := readPrivateDialogRecord(after.Records[old.Dialog])
	if err != nil {
		t.Fatal(err)
	}
	p, err := readPrivateProcedureRecord(after.Records[old.Procedure])
	if err != nil || d.AckEmissions != 2 || d.AckResult != resultField || len(after.Records[oldField]) != 0 || slices.Contains(p.Outputs, oldField) || slices.Contains(d.Outputs, oldField) || p.Native.Timers.Values[string(portable.TimerM)].At != originalM || len(after.Records[dialog.LocalInvite.Ack]) == 0 {
		t.Fatal("application ACK replay changed actual owners", err)
	}
	completed, err := decodeNativeResult(after.Records[resultField])
	if err != nil {
		t.Fatal(err)
	}
	original.Output = ""
	if !proto.Equal(original, completed) {
		t.Fatal("application ACK replay changed original receipt beyond output backlink")
	}
	if _, err = client.ZScore(ctx, root+"core:deadlines:uac", "orig-result-2").Result(); !errors.Is(err, redis.Nil) {
		t.Fatal("current ACK receipt acquired superseded-result lifetime", err)
	}
	entries, err := store.PeekReady(ctx, input.ExecutorTimer.Worker, input.Executor.Epoch, 8)
	if err != nil {
		t.Fatal(err)
	}
	writer := nativeRedisWriter{store: store}
	for _, entry := range entries {
		output, x := decodeNativeOutput(entry.Metadata)
		if x != nil {
			t.Fatal(x)
		}
		if output.Method != "ACK" || output.Dialog != old.Dialog {
			continue
		}
		reserved, x := writer.Reserve(ctx, output.Write)
		if x != nil || !bytes.Equal(reserved.Wire, wire) {
			t.Fatal("application-answer ACK replay changed exact bytes", x)
		}
		if x = writer.Complete(ctx, output.Write, &pb.DeliveryResult{Effect: output.Write.Effect, Outcome: pb.DeliveryResult_WRITTEN}); x != nil {
			t.Fatal(x)
		}
		if x = settleRedisUACSuccessACK(ctx, store, output.Write, nil, input.Key, input.ExecutorTimer, time.Now().Add(time.Second)); x != nil {
			t.Fatal("consume replay of actual application-owned ACK wire", x)
		}
		if x = acknowledgeRedisUACSuccessACK(ctx, store, output.Write, nil); x != nil {
			t.Fatal(x)
		}
		// Mature the actual replay deadline, then exercise its absent-proof
		// consumer against the same application-owned wire namespace.
		testRedisInviteWorkerAnchor(t, ctx, store, client, input)
		checked := waitRedisFixtureTime(t, ctx, client, time.UnixMilli(int64(output.Write.Permit.Authorization.DeadlineMs)))
		testRedisInviteWorkerAnchor(t, ctx, store, client, input)
		selected, x := store.ObserveContext(ctx, "uac", "incarnation", "r:context")
		if x != nil {
			t.Fatal(x)
		}
		due := &rediscore.DueContext{Identity: selected.Identity, Revision: selected.View.Revision, Assignment: selected.View.Timer, Member: "out-" + strings.TrimPrefix(entry.MetadataField, "r:"), DeadlineMS: int64(output.Write.Permit.Authorization.DeadlineMs), CheckedAtMS: checked.UnixMilli()}
		if x = retireRedisUACSuccessACK(ctx, store, due, input.Key, time.Now().Add(time.Second)); x != nil {
			t.Fatal("retire actual application-owned ACK replay metadata", x)
		}
		closed, x := store.Read(ctx, selected.Identity, old.Dialog, entry.MetadataField, dialog.LocalInvite.Ack)
		if x != nil {
			t.Fatal(x)
		}
		owner, x := readPrivateDialogRecord(closed.Records[old.Dialog])
		if x != nil || len(closed.Records[entry.MetadataField]) != 0 || owner.LocalInvite.Ack != dialog.LocalInvite.Ack || owner.AckResult != resultField || len(closed.Records[dialog.LocalInvite.Ack]) == 0 {
			t.Fatal("replay retirement lost application-owned ACK wire/result", x)
		}
		return
	}
	t.Fatal("application answer ACK replay output missing")
}
