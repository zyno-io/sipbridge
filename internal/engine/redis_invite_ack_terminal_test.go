package engine

import (
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
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/nativepb"
	"sipbridge/internal/rediscore"
	"sipbridge/internal/storagecodec"
)

// This fixture ends the call through an actual application BYE and response.
// Only elapsed timer, receipt and terminal retention horizons are advanced.
func testRedisInviteACKUnknownTerminal(t *testing.T, ctx context.Context, store *rediscore.Store, client *redis.Client, root string, input redisUACQualification, identity rediscore.Identity, dialogField string, ack *nativepb.Output, ackField string) {
	t.Helper()
	deadline := func() time.Time { return time.Now().Add(time.Second) }
	until := time.Now().Add(2 * time.Second)
	command := &pb.Command{Context: input.Context, Sequence: 5, Admission: &pb.CommandAdmission{DirectoryEpoch: "directory", ExpiresAtMs: uint64(until.UnixMilli()), SelectedFlow: input.Flow}, Action: &pb.Command_DialogRequest{DialogRequest: &pb.DialogRequest{Method: "BYE"}}}
	capture, err := captureRedisNotifyWait(command)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = publishRedisApplicationBye(ctx, store, input.Key, input.Application, input.Lease, command, capture, "terminal-bye", input.ExecutorTimer, until); err != nil {
		t.Fatal("admit actual BYE", err)
	}
	selected, err := store.Read(ctx, identity, "r:result-5")
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := decodeNativeResult(selected.Records["r:result-5"])
	if err != nil || receipt.Output == "" {
		t.Fatal("BYE output missing", err)
	}
	selected, err = store.Read(ctx, identity, receipt.Output)
	if err != nil {
		t.Fatal(err)
	}
	bye, err := decodeNativeOutput(selected.Records[receipt.Output])
	if err != nil {
		t.Fatal(err)
	}
	writer := nativeRedisWriter{store: store}
	reserved, err := writer.Reserve(ctx, bye.Write)
	if err != nil {
		t.Fatal("reserve BYE", err)
	}
	request, err := portable.ParseMessage(reserved.Wire, input.Flow.Transport, "", "")
	if err != nil {
		t.Fatal(err)
	}
	if err = writer.Complete(ctx, bye.Write, &pb.DeliveryResult{Effect: bye.Write.Effect, Outcome: pb.DeliveryResult_WRITTEN}); err != nil {
		t.Fatal(err)
	}
	if err = settleRedisApplicationByeOutput(ctx, store, bye.Write, nil, input.ExecutorTimer, deadline()); err != nil {
		t.Fatal("settle BYE", err)
	}
	if err = acknowledgeRedisApplicationByeOutput(ctx, store, bye.Write, deadline()); err != nil {
		t.Fatal("consume BYE", err)
	}
	response, err := portable.BuildResponseWithHeaders(request, 200, "OK", nil, "", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	response = portable.RestoreMessage(response.WireBytes(), input.Flow.Transport, net.JoinHostPort(input.Flow.SourceIp, strconv.Itoa(int(input.Flow.SourcePort))), "")
	handled, err := completeRedisCleanupByeResponse(ctx, store, input.Flow, response, nil, deadline())
	if err != nil || !handled {
		t.Fatal("actual BYE response", handled, err)
	}
	selected, err = store.Read(ctx, identity, "r:context", dialogField)
	if err != nil {
		t.Fatal(err)
	}
	current, err := decodeNativeContext(selected.Records["r:context"])
	if err != nil || !current.Ended {
		t.Fatal("BYE did not end call", err)
	}
	dialog, err := readPrivateDialogRecord(selected.Records[dialogField])
	if err != nil || dialog.Native.Phase != string(portable.DialogEnded) || dialog.AckWritten {
		t.Fatal("BYE changed unknown ACK evidence", err)
	}
	// Fire both actual INVITE M reducers after moving only each timer and index.
	for _, field := range []string{current.InitialProcedures[0], ack.Procedure} {
		selected, err = store.Read(ctx, identity, field)
		if err != nil {
			t.Fatal(err)
		}
		p, e := readPrivateProcedureRecord(selected.Records[field])
		if e != nil {
			t.Fatal(e)
		}
		m := p.Native.Timers.Values[string(portable.TimerM)]
		if m == nil || !m.Active {
			t.Fatal("INVITE M missing")
		}
		now, e := client.Time(ctx).Result()
		if e != nil {
			t.Fatal(e)
		}
		m.At = now.Add(-time.Millisecond).UnixMilli()
		data, e := marshalNativeRecord(storagecodec.NativeProcedure, p)
		if e != nil {
			t.Fatal(e)
		}
		member := "call-tx-" + strings.TrimPrefix(field, "r:uac-procedure-") + "-M"
		if strings.HasPrefix(field, "r:app-invite-") {
			member = "app-invite-" + strings.TrimPrefix(field, "r:app-invite-") + "-M"
		}
		_, e = store.Commit(ctx, rediscore.Publication{Identity: identity, Revision: selected.Revision, Deadline: deadline(), SpendReservation: true, Changes: []rediscore.Change{{Field: field, Value: data}}, Timers: &rediscore.TimerMutation{Expected: selected.Timer, Target: selected.Timer, RequireLease: true, Changes: []rediscore.TimerChange{{Member: member, Due: time.UnixMilli(m.At)}}}})
		if e != nil {
			t.Fatal("age real M", e)
		}
		selected, e = store.Read(ctx, identity, "r:context")
		if e != nil {
			t.Fatal(e)
		}
		due := &rediscore.DueContext{Identity: identity, Revision: selected.Revision, Assignment: selected.Timer, Member: member, DeadlineMS: m.At, CheckedAtMS: now.UnixMilli()}
		if strings.HasPrefix(field, "r:app-invite-") {
			e = fireRedisApplicationInviteDeadline(ctx, store, due, deadline())
		} else {
			e = fireRedisUACDeadline(ctx, store, due, deadline())
		}
		if e != nil {
			t.Fatal("fire real M", member, e)
		}
	}
	// Stage the original ended and physical fixed horizons in this fixture.
	fields := []string{"r:context", ackField}
	for _, n := range []string{"r:result-1", "r:result-3", "r:result-4", "r:result-5"} {
		fields = append(fields, n)
	}
	selected, err = store.Read(ctx, identity, fields...)
	if err != nil {
		t.Fatal(err)
	}
	current, err = decodeNativeContext(selected.Records["r:context"])
	if err != nil || !current.Ended {
		t.Fatal(err)
	}
	ack, err = decodeNativeOutput(selected.Records[ackField])
	if err != nil {
		t.Fatal(err)
	}
	now, err := client.Time(ctx).Result()
	if err != nil {
		t.Fatal(err)
	}
	firstEnd := now.Add(-time.Second).UnixMilli()
	physical := now.Add(-rediscore.OutputObsoleteGrace - time.Second).UnixMilli()
	current.CallRetireAtMs = firstEnd
	cdata, err := encodeNativeContext(current)
	if err != nil {
		t.Fatal(err)
	}
	ack.Write.Permit.Authorization.DeadlineMs = uint64(physical)
	adata, err := encodeNativeOutput(ack)
	if err != nil {
		t.Fatal(err)
	}
	hash, err := nativeFlowWriteHash(ack.Write)
	if err != nil {
		t.Fatal(err)
	}
	proofKey := "o:" + ack.Write.Effect.Id + ":"
	if err = client.HSet(ctx, root+"core:context:"+identity.Context, ackField, adata, proofKey+"p", hash[:], proofKey+"d", physical).Err(); err != nil {
		t.Fatal(err)
	}
	changes := []rediscore.Change{{Field: "r:context", Value: cdata}}
	originalCutoffs := make(map[string]int64)
	for _, field := range fields[2:] {
		if len(selected.Records[field]) == 0 {
			continue
		}
		result, e := decodeNativeResult(selected.Records[field])
		if e != nil {
			t.Fatal(e)
		}
		if result.Pending {
			t.Fatal("actual command remains pending", field)
		}
		originalCutoffs[field] = result.CutoffMs
		result.CutoffMs = firstEnd
		data, e := encodeNativeResult(result)
		if e != nil {
			t.Fatal(e)
		}
		changes = append(changes, rediscore.Change{Field: field, Value: data})
	}
	cutoff := max(firstEnd, physical+rediscore.OutputObsoleteGrace.Milliseconds())
	timers := []rediscore.TimerChange{{Member: "call-retire", Due: time.UnixMilli(cutoff)}, {Member: "out-" + strings.TrimPrefix(ackField, "r:"), Due: time.UnixMilli(physical)}}
	// Age only existing receipt duties with their exact original cutoff. The
	// expiry consumers require the selected receipt and index to agree; moving
	// a receipt alone would fabricate an invalid due input during this fixture.
	indexed, err := client.ZRangeWithScores(ctx, root+"core:deadlines:"+identity.Context, 0, -1).Result()
	if err != nil {
		t.Fatal(err)
	}
	for _, item := range indexed {
		member, ok := item.Member.(string)
		if !ok {
			t.Fatal("invalid fixture deadline member")
		}
		field := ""
		if strings.HasPrefix(member, "orig-result-") {
			field = "r:result-" + strings.TrimPrefix(member, "orig-result-")
		} else if strings.HasPrefix(member, "uac-result-") {
			if !slices.Contains(current.InitialProcedures, "r:uac-procedure-"+strings.TrimPrefix(member, "uac-result-")) {
				t.Fatal("initial receipt duty lost its actual owner")
			}
			field = "r:result-1"
		}
		if original, changed := originalCutoffs[field]; changed {
			if item.Score != float64(original) {
				t.Fatal("receipt deadline differs from its original cutoff", member)
			}
			timers = append(timers, rediscore.TimerChange{Member: member, Due: time.UnixMilli(firstEnd)})
		}
	}
	_, err = store.Commit(ctx, rediscore.Publication{Identity: identity, Revision: selected.Revision, Deadline: deadline(), SpendReservation: true, Changes: changes, Timers: &rediscore.TimerMutation{Expected: selected.Timer, Target: selected.Timer, RequireLease: true, Changes: timers}})
	if err != nil {
		t.Fatal("age terminal witness", err)
	}
	ref, err := nativeOutputRef(ack.Write)
	if err != nil {
		t.Fatal(err)
	}
	proof, err := store.InspectOutput(ctx, ref)
	if err != nil || proof.State != rediscore.OutputUnknown {
		t.Fatal("ACK U not retained", err)
	}
	e := &redisNative{cfg: Config{Process: input.Executor, StateIdentityKey: input.Key}, worker: input.ExecutorTimer.Worker}
	// Use actual indexed duties. Lifecycle events are completed through their
	// normal acknowledgement and every other duty through the private handler.
	limit, renew := time.Now().Add(8*time.Second), time.Now()
	forgot := false
	for time.Now().Before(limit) {
		if !time.Now().Before(renew) {
			testRedisInviteWorkerAnchor(t, ctx, store, client, input)
			renew = time.Now().Add(250 * time.Millisecond)
		}
		due, x := store.PeekDue(ctx, input.ExecutorTimer.Worker, input.Executor.Epoch)
		if x != nil {
			t.Fatal("peek terminal", x)
		}
		if due == nil {
			time.Sleep(5 * time.Millisecond)
			continue
		}
		if strings.HasPrefix(due.Member, "event-") {
			c, _, y := selectRedisNativeLifecycle(ctx, store, due)
			if y != nil {
				t.Fatal("select event", y)
			}
			x = completeRedisNativeLifecycle(ctx, store, c, true, deadline())
		} else {
			x = e.due(ctx, store, due)
		}
		if x != nil {
			t.Fatal("terminal duty", due.Member, x)
		}
		state, y := store.Read(ctx, identity, ackField, dialogField, ack.Procedure)
		if y != nil {
			t.Fatal(y)
		}
		_, y = store.InspectOutput(ctx, ref)
		if errors.Is(y, rediscore.ErrAbsent) && len(state.Records[ackField]) != 0 {
			forgot = true
		}
		if len(state.Records[ackField]) == 0 {
			if !forgot || !errors.Is(y, rediscore.ErrAbsent) {
				t.Fatal("ACK removed without observed U-only release", y)
			}
			d, z := readPrivateDialogRecord(state.Records[dialogField])
			if z != nil || d.AckWritten {
				t.Fatal("terminal inferred ACK write", z)
			}
			return
		}
	}
	t.Fatal("terminal ACK did not close within bounded fixture")
}
