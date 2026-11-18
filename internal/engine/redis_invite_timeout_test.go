package engine

import (
	"bytes"
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/emiago/sipgo/sip/portable"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/rediscore"
	"sipbridge/internal/selectedmessage"
	"sipbridge/internal/storagecodec"
)

func TestRedisInitialInviteTimeoutOwnsNoReceiptAndRetires(t *testing.T) {
	t.Run("written", func(t *testing.T) { testRedisInitialInviteTimeoutOwnsNoReceiptAndRetires(t, false) })
	t.Run("unknown-terminal", func(t *testing.T) { testRedisInitialInviteTimeoutOwnsNoReceiptAndRetires(t, true) })
}

func testRedisInitialInviteTimeoutOwnsNoReceiptAndRetires(t *testing.T, unknown bool) {
	address := os.Getenv("SIPBRIDGE_CORE_TEST_REDIS")
	if address == "" {
		t.Skip("isolated Redis fixture required")
	}
	ctx := context.Background()
	client := redis.NewClient(&redis.Options{Addr: address, MaxRetries: -1})
	root := "sipstate:test:initial-timeout-" + uuid.NewString() + ":"
	t.Cleanup(func() {
		defer client.Close()
		var cursor uint64
		for {
			keys, next, err := client.Scan(ctx, cursor, root+"*", 128).Result()
			if err != nil {
				t.Error(err)
				return
			}
			if len(keys) > 0 {
				if err = client.Del(ctx, keys...).Err(); err != nil {
					t.Error(err)
					return
				}
			}
			if next == 0 {
				return
			}
			cursor = next
		}
	})
	info, err := client.Info(ctx, "server").Result()
	if err != nil {
		t.Fatal(err)
	}
	primary := ""
	for _, line := range strings.Split(info, "\r\n") {
		if v, ok := strings.CutPrefix(line, "run_id:"); ok {
			primary = v
		}
	}
	profile, key := []byte{8, 1}, bytes.Repeat([]byte{1}, 32)
	if err = client.Set(ctx, root+"core:profile", profile, 0).Err(); err != nil {
		t.Fatal(err)
	}
	store, err := rediscore.New(client, rediscore.Config{Root: root, Profile: profile, PrimaryRunID: primary, RecordBudget: 4 << 20})
	if err != nil {
		t.Fatal(err)
	}
	process, app := &pb.ProcessRef{InstanceId: "worker", Epoch: "epoch"}, &pb.ProcessRef{InstanceId: "application", Epoch: "epoch"}
	worker, err := nativeWorkerReference(process)
	if err != nil {
		t.Fatal(err)
	}
	appID, err := nativeApplicationReference(key, app)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := store.AcquireApplication(ctx, appID, "term")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = store.RenewWorker(ctx, rediscore.WorkerLeaseRequest{Worker: worker, Epoch: process.Epoch, PrimaryRunID: primary, Acquire: true}); err != nil {
		t.Fatal(err)
	}
	flow := &pb.FlowRef{Ingress: process, ConnectionId: "phone", SourceIp: "192.0.2.12", SourcePort: 5061, Listener: "sip.example.test:5061", Transport: "tls"}
	message, err := portable.ParseMessage(stateTestRequest("INVITE", 1, "", "initial-timeout").Wire, "tls", "192.0.2.12:5061", "")
	if err != nil {
		t.Fatal(err)
	}
	facts, err := message.Facts()
	if err != nil {
		t.Fatal(err)
	}
	authority := &pb.StateAuthority{Service: "svc", Generation: "gen", PrimaryId: "primary", SchemaVersion: 1}
	ref := &pb.ContextRef{Id: "context", Owner: process, State: &pb.StateRef{Authority: authority, GroupId: "context", Incarnation: "incarnation", CallId: facts.CallID}}
	requestID := uuid.NewString()
	in := redisRegisterReceive{IdentityKey: key, Context: ref, Authority: authority, Application: app, Executor: process, Lease: lease.ApplicationLeaseRef, Flow: flow, Message: message, RequestID: requestID, LocalTag: "local", Timer: rediscore.TimerAssignment{Worker: worker, Epoch: process.Epoch, Term: process.Epoch}, Deadline: time.Now().Add(time.Second)}
	if _, err = receiveRedisInvite(ctx, store, in); err != nil {
		t.Fatal(err)
	}
	identity := rediscore.Identity{Context: ref.Id, Incarnation: ref.State.Incarnation}
	procField := "r:procedure-" + requestID
	view, err := store.Read(ctx, identity, procField)
	if err != nil {
		t.Fatal(err)
	}
	procedure, err := readPrivateProcedureRecord(view.Records[procField])
	if err != nil {
		t.Fatal(err)
	}
	now, err := client.Time(ctx).Result()
	if err != nil {
		t.Fatal(err)
	}
	procedure.PresentDeadlineMs = now.Add(-time.Second).UnixMilli()
	encoded, err := marshalNativeRecord(storagecodec.NativeProcedure, procedure)
	if err != nil {
		t.Fatal(err)
	}
	member := "call-decision-" + requestID
	_, err = store.Commit(ctx, rediscore.Publication{Identity: identity, Revision: view.Revision, Deadline: time.Now().Add(time.Second), SpendReservation: true, Changes: []rediscore.Change{{Field: procField, Value: encoded}}, Timers: &rediscore.TimerMutation{Expected: view.Timer, Target: view.Timer, RequireLease: true, Changes: []rediscore.TimerChange{{Member: member, Due: time.UnixMilli(procedure.PresentDeadlineMs)}}}})
	if err != nil {
		t.Fatal(err)
	}
	due, err := store.PeekDue(ctx, worker, process.Epoch)
	if err != nil || due == nil || due.Member != member {
		t.Fatal("initial decision not due", err)
	}
	runtime := &redisNative{worker: worker, cfg: Config{Process: process, StateIdentityKey: key, ContactURI: "sip:sip.example.test:5061", AdvertiseAddress: "sip.example.test:5061"}}
	if err = runtime.due(ctx, store, due); err != nil {
		t.Fatal("timeout dispatch", err)
	}
	field := "r:invite-output-" + requestID + "-0"
	selected, err := store.Read(ctx, identity, "r:context", procField, field, "r:result-1")
	if err != nil {
		t.Fatal(err)
	}
	current, err := decodeNativeContext(selected.Records["r:context"])
	if err != nil || !current.Ended || current.CommandSequence != 0 || current.CallRetireAtMs <= 0 || len(selected.Records["r:result-1"]) != 0 {
		t.Fatal("timeout retained a receipt or missed retirement", err)
	}
	output, err := decodeNativeOutput(selected.Records[field])
	if err != nil || output.Status != 503 || output.Result != "" {
		t.Fatal("missing no-result 503 output", err)
	}
	ready, err := store.PeekReady(ctx, worker, process.Epoch, 4)
	if err != nil || len(ready) != 1 {
		t.Fatal("503 not ready", err)
	}
	// A malformed initial owner must be rejected before any physical write.
	private, err := readPrivateProcedureRecord(selected.Records[procField])
	if err != nil {
		t.Fatal(err)
	}
	private.Direct = false
	badOwner, err := marshalNativeRecord(storagecodec.NativeProcedure, private)
	if err != nil {
		t.Fatal(err)
	}
	if err = client.HSet(ctx, root+"core:context:context", procField, badOwner).Err(); err != nil {
		t.Fatal(err)
	}
	writes := 0
	runtime.cfg.WriteCommitted = func(context.Context, *pb.FlowWrite) *pb.DeliveryResult { writes++; return nil }
	if err = runtime.readyOutput(ctx, store, ready[0]); err == nil || writes != 0 {
		t.Fatal("invalid timeout owner reached physical writer", err)
	}
	if err = client.HSet(ctx, root+"core:context:context", procField, selected.Records[procField]).Err(); err != nil {
		t.Fatal(err)
	}
	runtime.cfg.WriteCommitted = func(ctx context.Context, write *pb.FlowWrite) *pb.DeliveryResult {
		reservation, e := (nativeRedisWriter{store: store}).Reserve(ctx, write)
		if e != nil || reservation == nil || len(reservation.Wire) == 0 {
			t.Error("503 reserve failed", e)
			return nil
		}
		outcome := pb.DeliveryResult_WRITTEN
		if unknown {
			outcome = pb.DeliveryResult_UNKNOWN
		}
		return &pb.DeliveryResult{Effect: write.Effect, Outcome: outcome}
	}
	if err = runtime.readyOutput(ctx, store, ready[0]); err != nil {
		t.Fatal("503 completion", err)
	}
	selected, err = store.Read(ctx, identity, "r:context", procField, field, "r:origin-"+requestID, "r:invite-response-"+requestID+"-0")
	if err != nil {
		t.Fatal(err)
	}
	original := int64(output.Write.Permit.Authorization.DeadlineMs)
	if unknown {
		// Age the exact existing proof and permit together, as in the terminal
		// fixture, to exercise both fixed horizons without a two-minute sleep.
		now, e := client.Time(ctx).Result()
		if e != nil {
			t.Fatal(e)
		}
		original = now.Add(-rediscore.OutputObsoleteGrace - 2*time.Second).UnixMilli()
		output.Write.Permit.Authorization.DeadlineMs = uint64(original)
		encodedOutput, e := encodeNativeOutput(output)
		if e != nil {
			t.Fatal(e)
		}
		hash, e := nativeFlowWriteHash(output.Write)
		if e != nil {
			t.Fatal(e)
		}
		prefix := "o:" + output.Write.Effect.Id + ":"
		if e = client.HSet(ctx, root+"core:context:context", field, encodedOutput, prefix+"p", hash[:], prefix+"d", original).Err(); e != nil {
			t.Fatal(e)
		}
		_, e = store.Commit(ctx, rediscore.Publication{Identity: identity, Revision: selected.Revision, Deadline: time.Now().Add(time.Second), SpendReservation: true,
			Timers: &rediscore.TimerMutation{Expected: selected.Timer, Target: selected.Timer, RequireLease: true, Changes: []rediscore.TimerChange{{Member: "out-" + field[2:], Due: time.UnixMilli(original)}}}})
		if e != nil {
			t.Fatal(e)
		}
		selected, e = store.Read(ctx, identity, "r:context", procField, field, "r:origin-"+requestID, "r:invite-response-"+requestID+"-0")
		if e != nil {
			t.Fatal(e)
		}
	}
	outputDue := &rediscore.DueContext{Identity: identity, Revision: selected.Revision, Assignment: selected.Timer, Member: "out-" + field[2:], DeadlineMS: original, CheckedAtMS: max(time.Now().UnixMilli(), original)}
	if err = runtime.due(ctx, store, outputDue); err != nil {
		t.Fatal("503 output cutoff", err)
	}
	if unknown {
		ref, e := nativeOutputRef(output.Write)
		if e != nil {
			t.Fatal(e)
		}
		proof, e := store.InspectOutput(ctx, ref)
		if e != nil || proof.State != rediscore.OutputUnknown {
			t.Fatal("unknown proof lost at cutoff", e)
		}
	}
	selected, err = store.Read(ctx, identity, "r:context", procField, "r:origin-"+requestID, "r:invite-response-"+requestID+"-0")
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := decodeNativeProcedure(selected.Records[procField])
	if err != nil {
		t.Fatal(err)
	}
	memo, err := selectedmessage.NewMemo(2)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := decoded.attach(&nativeSelectedMessages{records: selected.Records, memo: memo, provenance: "admitted"}, true, true, false)
	if err != nil {
		t.Fatal(err)
	}
	now, err = client.Time(ctx).Result()
	if err != nil {
		t.Fatal(err)
	}
	h := tx.Timers[portable.TimerH]
	if !h.Active {
		t.Fatal("503 missing H")
	}
	h.At = now.Add(-2 * time.Second).UnixMilli()
	tx.Timers[portable.TimerH] = h
	procBytes, err := encodeNativeProcedure(tx, decoded.Refs, decoded.Metadata)
	if err != nil {
		t.Fatal(err)
	}
	current, err = decodeNativeContext(selected.Records["r:context"])
	if err != nil {
		t.Fatal(err)
	}
	current.CallRetireAtMs = now.Add(-time.Second).UnixMilli()
	contextBytes, err := encodeNativeContext(current)
	if err != nil {
		t.Fatal(err)
	}
	hMember := "call-tx-" + requestID + "-" + string(portable.TimerH)
	_, err = store.Commit(ctx, rediscore.Publication{Identity: identity, Revision: selected.Revision, Deadline: time.Now().Add(time.Second), SpendReservation: true, Changes: []rediscore.Change{{Field: procField, Value: procBytes}, {Field: "r:context", Value: contextBytes}}, Timers: &rediscore.TimerMutation{Expected: selected.Timer, Target: selected.Timer, RequireLease: true, Changes: []rediscore.TimerChange{{Member: hMember, Due: time.UnixMilli(h.At)}, {Member: "call-retire", Due: time.UnixMilli(current.CallRetireAtMs)}}}})
	if err != nil {
		t.Fatal(err)
	}
	due, err = store.PeekDue(ctx, worker, process.Epoch)
	if err != nil || due == nil || due.Member != hMember {
		t.Fatal("H not due", err)
	}
	if err = runtime.due(ctx, store, due); err != nil {
		t.Fatal("H dispatch", err)
	}
	for attempt := 0; attempt < 8; attempt++ {
		due, err = store.PeekDue(ctx, worker, process.Epoch)
		if err != nil || due == nil || due.Member != "call-retire" && due.Member != "out-"+field[2:] {
			t.Fatal("retirement not due", err)
		}
		if err = runtime.due(ctx, store, due); err != nil {
			t.Fatal("terminal retirement", err)
		}
		if _, err = store.Read(ctx, identity, "r:context"); errors.Is(err, rediscore.ErrAbsent) {
			return
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	t.Fatal("unbound timed-out call retained after bounded retirement")
}
