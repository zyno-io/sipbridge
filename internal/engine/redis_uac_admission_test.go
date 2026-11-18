package engine

import (
	"bytes"
	"context"
	"errors"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/emiago/sipgo/sip/portable"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/nativepb"
	"sipbridge/internal/rediscore"
	"sipbridge/internal/selectedmessage"
	"sipbridge/internal/storagecodec"
)

func TestRedisUACQualificationInitialAdmission(t *testing.T) {
	t.Run("not-written-and-unknown", func(t *testing.T) { testRedisUACQualificationInitialAdmission(t, false, false, false, false, false, 0) })
	t.Run("written", func(t *testing.T) { testRedisUACQualificationInitialAdmission(t, true, false, false, false, false, 0) })
	t.Run("abandoned-digest-owner-expiry", func(t *testing.T) {
		testRedisUACQualificationInitialAdmission(t, false, false, false, false, false, 39)
	})
	t.Run("cancel-digest-udp", func(t *testing.T) {
		testRedisUACQualificationInitialAdmission(t, false, false, false, false, false, 63)
	})
	t.Run("cancel-digest-terminal", func(t *testing.T) {
		testRedisUACQualificationInitialAdmission(t, false, false, false, false, false, 60)
	})
	t.Run("provisional", func(t *testing.T) { testRedisUACQualificationInitialAdmission(t, false, false, false, true, false, 0) })
	t.Run("terminal-winning-unknown", func(t *testing.T) { testRedisUACQualificationInitialAdmission(t, false, false, false, true, false, 11) })
	t.Run("outgoing-bye-refusal", func(t *testing.T) { testRedisUACQualificationInitialAdmission(t, false, false, false, true, false, 14) })
	t.Run("outgoing-bye-timeout", func(t *testing.T) { testRedisUACQualificationInitialAdmission(t, false, false, false, true, false, 13) })
	t.Run("outgoing-bye-admission", func(t *testing.T) { testRedisUACQualificationInitialAdmission(t, false, false, false, true, false, 12) })
	t.Run("outgoing-bye-runtime", func(t *testing.T) { testRedisUACQualificationInitialAdmission(t, false, false, false, true, false, 32) })
	t.Run("outgoing-bye-runtime-not-written", func(t *testing.T) { testRedisUACQualificationInitialAdmission(t, false, false, false, true, false, 33) })
	t.Run("outgoing-bye-runtime-unknown", func(t *testing.T) { testRedisUACQualificationInitialAdmission(t, false, false, false, true, false, 34) })
	for i, method := range []string{"INFO", "REFER", "UPDATE", "NOTIFY"} {
		t.Run("outgoing-dialog-"+method, func(t *testing.T) {
			testRedisUACQualificationInitialAdmission(t, false, false, false, true, false, 35+i)
		})
	}
	t.Run("outgoing-dialog-timeout", func(t *testing.T) { testRedisUACQualificationInitialAdmission(t, false, false, false, true, false, 40) })
	t.Run("outgoing-dialog-not-written", func(t *testing.T) { testRedisUACQualificationInitialAdmission(t, false, false, false, true, false, 41) })
	t.Run("outgoing-dialog-unknown", func(t *testing.T) { testRedisUACQualificationInitialAdmission(t, false, false, false, true, false, 42) })
	t.Run("outgoing-dialog-unknown-terminal", func(t *testing.T) { testRedisUACQualificationInitialAdmission(t, false, false, false, true, false, 43) })
	t.Run("first-winner", func(t *testing.T) { testRedisUACQualificationInitialAdmission(t, false, false, false, true, false, 1) })
	t.Run("first-winner-late-offer", func(t *testing.T) { testRedisUACQualificationInitialAdmission(t, false, false, false, true, false, 2) })
	t.Run("cancel-unknown-terminal", func(t *testing.T) {
		testRedisUACQualificationInitialAdmission(t, false, false, false, false, false, 59)
	})
	t.Run("cancel-before-provisional", func(t *testing.T) {
		testRedisUACQualificationInitialAdmission(t, false, false, false, false, false, 48)
	})
	t.Run("cancel-after-provisional", func(t *testing.T) {
		testRedisUACQualificationInitialAdmission(t, false, false, false, false, false, 49)
	})
	t.Run("cancel-after-ack-written", func(t *testing.T) { testRedisUACQualificationInitialAdmission(t, false, false, false, true, false, 50) })
	t.Run("cancel-before-ack-written", func(t *testing.T) { testRedisUACQualificationInitialAdmission(t, false, false, false, true, false, 51) })
	t.Run("cancel-late-offer-answer", func(t *testing.T) { testRedisUACQualificationInitialAdmission(t, false, false, false, true, false, 52) })
	t.Run("late-offer-app-ack-written", func(t *testing.T) { testRedisUACQualificationInitialAdmission(t, false, false, false, true, false, 44) })
	t.Run("late-offer-app-ack-early-replay", func(t *testing.T) { testRedisUACQualificationInitialAdmission(t, false, false, false, true, false, 71) })
	t.Run("late-offer-app-ack-not-written", func(t *testing.T) { testRedisUACQualificationInitialAdmission(t, false, false, false, true, false, 45) })
	t.Run("late-offer-app-ack-owner-lost-unknown", func(t *testing.T) { testRedisUACQualificationInitialAdmission(t, false, false, false, true, false, 53) })
	t.Run("late-offer-app-ack-owner-lost-late-write", func(t *testing.T) { testRedisUACQualificationInitialAdmission(t, false, false, false, true, false, 54) })
	t.Run("late-offer-app-ack-unknown", func(t *testing.T) { testRedisUACQualificationInitialAdmission(t, false, false, false, true, false, 46) })
	t.Run("local-reinvite-response", func(t *testing.T) { testRedisUACQualificationInitialAdmission(t, false, false, false, true, false, 47) })
	t.Run("late-offer-owner-disconnect", func(t *testing.T) { testRedisUACQualificationInitialAdmission(t, false, false, false, true, false, 25) })
	t.Run("first-winner-ack-not-written", func(t *testing.T) { testRedisUACQualificationInitialAdmission(t, false, false, false, true, false, 4) })
	t.Run("losing-fork-ack-not-written", func(t *testing.T) { testRedisUACQualificationInitialAdmission(t, false, false, false, true, false, 5) })
	t.Run("late-fork-after-compaction-and-M", func(t *testing.T) { testRedisUACQualificationInitialAdmission(t, false, false, false, true, false, 7) })
	t.Run("compacted-original-late-ack", func(t *testing.T) { testRedisUACQualificationInitialAdmission(t, false, false, false, true, false, 6) })
	t.Run("cancelled-first-success", func(t *testing.T) { testRedisUACQualificationInitialAdmission(t, false, false, false, true, false, 8) })
	t.Run("owner-lost-first-success", func(t *testing.T) { testRedisUACQualificationInitialAdmission(t, false, false, false, true, false, 9) })
	t.Run("owner-lost-early-ack-replay", func(t *testing.T) { testRedisUACQualificationInitialAdmission(t, false, false, false, true, false, 70) })
	t.Run("terminated-first-success", func(t *testing.T) { testRedisUACQualificationInitialAdmission(t, false, false, false, true, false, 10) })
	t.Run("first-winner-no-early", func(t *testing.T) { testRedisUACQualificationInitialAdmission(t, false, false, false, true, false, 3) })
	t.Run("provisional-ack-not-written", func(t *testing.T) { testRedisUACQualificationInitialAdmission(t, false, false, false, true, true, 0) })
	t.Run("preserve-completed", func(t *testing.T) { testRedisUACQualificationInitialAdmission(t, false, true, false, false, false, 0) })
	t.Run("late-not-written-after-timeout", func(t *testing.T) { testRedisUACQualificationInitialAdmission(t, false, false, true, false, false, 0) })
	t.Run("timer-B", func(t *testing.T) {
		testRedisUACQualificationInitialAdmission(t, false, false, false, false, false, 20)
	})
	t.Run("timer-D", func(t *testing.T) {
		testRedisUACQualificationInitialAdmission(t, false, false, false, false, false, 21)
	})
	t.Run("timer-M", func(t *testing.T) {
		testRedisUACQualificationInitialAdmission(t, false, false, false, false, false, 22)
	})
	t.Run("runtime-ready", func(t *testing.T) {
		testRedisUACQualificationInitialAdmission(t, false, false, false, false, false, 23)
	})
	t.Run("runtime-cleanup", func(t *testing.T) {
		testRedisUACQualificationInitialAdmission(t, false, false, false, true, false, 24)
	})
}

func testRedisUACQualificationInitialAdmission(t *testing.T, written, preserveCompleted, timedOut, provisional, ackNotWritten bool, successCase int) {
	address := os.Getenv("SIPBRIDGE_CORE_TEST_REDIS")
	if address == "" {
		t.Skip("isolated Redis fixture not configured")
	}
	ctx := context.Background()
	client := redis.NewClient(&redis.Options{Addr: address, MaxRetries: -1})
	info, err := client.Info(ctx, "server").Result()
	if err != nil {
		t.Fatal(err)
	}
	primary := ""
	for _, line := range strings.Split(info, "\r\n") {
		if strings.HasPrefix(line, "run_id:") {
			primary = strings.TrimPrefix(line, "run_id:")
		}
	}
	root := "sipstate:test:core-" + uuid.NewString() + ":"
	profile := []byte{3, 2, 1}
	key := bytes.Repeat([]byte{9}, 32)
	app := &pb.ProcessRef{InstanceId: "application", Epoch: "epoch"}
	worker := &pb.ProcessRef{InstanceId: "worker", Epoch: "epoch"}
	workerID, err := nativeWorkerReference(worker)
	if err != nil {
		t.Fatal(err)
	}
	appID, err := nativeApplicationReference(key, app)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		for _, name := range []string{"core:profile", "core:capacity", "core:selectors", "core:applications", "core:application:" + appID, "core:owner:term", "core:owned:term", "core:owner-expiry", "core:worker:" + workerID, "core:timers:" + workerID, "core:ready:" + workerID, "core:context:uac", "core:deadlines:uac"} {
			if e := client.Del(ctx, root+name).Err(); e != nil {
				t.Error(e)
			}
		}
		_ = client.Close()
	})
	if err = client.Set(ctx, root+"core:profile", profile, 0).Err(); err != nil {
		t.Fatal(err)
	}
	store, err := rediscore.New(client, rediscore.Config{Root: root, Profile: profile, PrimaryRunID: primary, RecordBudget: 4 << 20})
	if err != nil {
		t.Fatal(err)
	}
	lease, err := store.AcquireApplication(ctx, appID, "term")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = store.RenewWorker(ctx, rediscore.WorkerLeaseRequest{Worker: workerID, Epoch: worker.Epoch, PrimaryRunID: primary, Acquire: true}); err != nil {
		t.Fatal(err)
	}
	authority := &pb.StateAuthority{Service: "service", Generation: "generation", PrimaryId: "primary", SchemaVersion: 1}
	ref := &pb.ContextRef{Id: "uac", Owner: worker, State: &pb.StateRef{Authority: authority, GroupId: "uac", Incarnation: "incarnation", CallId: "outbound@example.test"}}
	flow := &pb.FlowRef{Ingress: worker, ConnectionId: "socket", SourceIp: "192.0.2.1", SourcePort: 5060, Listener: "sip.example.test:5060", Transport: "udp"}
	if successCase == 11 || successCase == 39 || successCase == 43 || successCase == 60 || successCase == 69 {
		flow.Transport = "tls"
	}
	deadline := time.Now().Add(5 * time.Second)
	cmd := &pb.Command{Context: &pb.ContextRef{Id: "uac"}, Sequence: 1, Admission: &pb.CommandAdmission{DirectoryEpoch: "directory", ExpiresAtMs: uint64(deadline.Add(-2 * time.Second).UnixMilli()), SelectedFlow: flow}, Action: &pb.Command_OpenUac{OpenUac: &pb.OpenUac{Uri: "sip:phone@example.test", Transport: "udp", Headers: []*pb.Header{{Name: "From", Value: "<sip:service@example.test>"}}}}}
	if successCase == 11 || successCase == 39 || successCase == 43 || successCase == 60 || successCase == 69 {
		cmd.GetOpenUac().Transport = "tls"
	}
	if successCase == 39 || successCase == 60 || successCase == 63 {
		cmd.GetOpenUac().Network = &pb.NetworkTarget{Transport: flow.Transport, Credential: &pb.CredentialRef{Id: "carrier", Version: "one"}}
	}
	input := redisUACQualification{Key: key, Context: ref, Authority: authority, Application: app, Executor: worker, Lease: lease.ApplicationLeaseRef, Flow: flow, Command: cmd, Operation: "operation", Advertisement: nativeSIPAdvertisement{contactURI: "sip:sip.example.test:5060", address: "sip.example.test:5060"}, ExecutorTimer: rediscore.TimerAssignment{Worker: workerID, Epoch: worker.Epoch, Term: worker.Epoch}, Credit: 1 << 20, Deadline: deadline}
	bad := input
	bad.Authority = proto.Clone(authority).(*pb.StateAuthority)
	bad.Authority.PrimaryId = "different"
	if _, err = admitRedisUACQualification(ctx, store, bad); err == nil {
		t.Fatal("foreign state authority admitted")
	}
	bad = input
	bad.Lease.Term = "wrong"
	if _, err = admitRedisUACQualification(ctx, store, bad); err == nil {
		t.Fatal("stale lease admitted")
	}
	if n, e := client.Exists(ctx, root+"core:context:uac").Result(); e != nil || n != 0 {
		t.Fatal("stale lease changed context", n, e)
	}
	tooLarge := input
	tooLarge.Context = proto.Clone(input.Context).(*pb.ContextRef)
	tooLarge.Context.Id, tooLarge.Context.State.GroupId = "over-capacity", "over-capacity"
	tooLarge.Command = proto.Clone(cmd).(*pb.Command)
	tooLarge.Command.Context.Id = "over-capacity"
	tooLarge.Credit = 8 << 20
	if _, err = admitRedisUACQualification(ctx, store, tooLarge); err == nil {
		t.Fatal("over-budget qualification was admitted")
	}
	if n, e := client.Exists(ctx, root+"core:context:over-capacity").Result(); e != nil || n != 0 {
		t.Fatal("capacity refusal changed context", n, e)
	}
	result, err := admitRedisUACQualification(ctx, store, input)
	if err != nil {
		t.Fatal(err)
	}
	if result.Certainty != pb.OperationResult_ACCEPTED || result.Request == nil || result.Snapshot == nil || result.Snapshot.CommandSequence != 1 {
		t.Fatal("initial acceptance lost exact evidence")
	}
	if _, err = admitRedisUACQualification(ctx, store, input); err == nil {
		t.Fatal("duplicate create admitted another output")
	}
	selected, err := store.ObserveContext(ctx, "uac", "incarnation", "r:context", "r:result-1")
	if err != nil {
		t.Fatal(err)
	}
	if selected.Identity.Owner != appID || selected.Identity.Term != lease.Term {
		t.Fatal("initial UAC context was not bound to the witnessed lease")
	}
	record, err := decodeNativeContext(selected.View.Records["r:context"])
	if err != nil || len(record.InitialProcedures) != 1 || record.Kind != pb.ContextSnapshot_UAC {
		t.Fatalf("missing UAC anchor: %v", err)
	}
	receipt, err := decodeNativeResult(selected.View.Records["r:result-1"])
	if err != nil || receipt.Certainty != pb.OperationResult_ACCEPTED || receipt.Request == "" || receipt.DecidingLeaseTerm != lease.Term {
		t.Fatalf("invalid exact receipt: %v", err)
	}
	procView, err := store.Read(ctx, selected.Identity, record.InitialProcedures[0], receipt.Request)
	if err != nil {
		t.Fatal(err)
	}
	proc, err := decodeNativeProcedure(procView.Records[record.InitialProcedures[0]])
	if err != nil || proc.Refs.Origin != receipt.Request || len(proc.Metadata.Outputs) != 1 || receipt.Output != proc.Metadata.Outputs[0] || proc.Metadata.CreditContribution != input.Credit {
		t.Fatalf("invalid selected anchor: %v", err)
	}
	exact, err := selectedmessage.DecodeWire(procView.Records[receipt.Request])
	if err != nil || !bytes.Equal(exact, result.Request.Wire) {
		t.Fatal("result request differs from selected origin", err)
	}
	ready, err := nativeReadyWrites(ctx, store, workerID, worker.Epoch, 2)
	if err != nil || len(ready) != 1 || ready[0].Permit.Authorization.AuthorityClass != pb.EffectAuthorization_APPLICATION {
		t.Fatalf("initial output missing: %v", err)
	}
	if int64(ready[0].Permit.Authorization.DeadlineMs) > time.Now().Add(2*time.Second).UnixMilli() || receipt.CutoffMs != deadline.UnixMilli() {
		t.Fatal("physical deadline or original waiter cutoff changed")
	}
	if selected.View.Reserved != int64(input.Credit) {
		t.Fatalf("qualification headroom = %d", selected.View.Reserved)
	}
	members, err := client.ZRange(ctx, root+"core:deadlines:uac", 0, -1).Result()
	if err != nil {
		t.Fatal(err)
	}
	sawB := false
	for _, member := range members {
		if strings.HasSuffix(member, "-A") {
			t.Fatal("UDP retransmit tick persisted")
		}
		if strings.HasSuffix(member, "-B") {
			sawB = true
		}
	}
	if !sawB {
		t.Fatal("native Timer B missing")
	}
	if successCase == 48 || successCase == 49 || successCase == 59 {
		testRedisInitialClientCancel(t, ctx, store, client, root, input, record.InitialProcedures[0], procView.Records[receipt.Request], workerID, primary, successCase == 49 || successCase == 59, successCase == 59)
		return
	}
	if successCase == 39 || successCase == 60 || successCase == 63 {
		runtime := &redisNative{worker: workerID, cfg: Config{Process: worker, StateIdentityKey: key}}
		runtime.cfg.WriteCommitted = func(ctx context.Context, write *pb.FlowWrite) *pb.DeliveryResult {
			writer := nativeRedisWriter{store: store}
			if _, e := writer.Reserve(ctx, write); e != nil {
				t.Error(e)
				return nil
			}
			return &pb.DeliveryResult{Effect: write.Effect, Outcome: pb.DeliveryResult_WRITTEN}
		}
		entries, e := store.PeekReady(ctx, workerID, worker.Epoch, 1)
		if e != nil || len(entries) != 1 {
			t.Fatal("initial ready", e)
		}
		if e = runtime.readyOutput(ctx, store, entries[0]); e != nil {
			t.Fatal("initial output", e)
		}
		origin, e := selectedmessage.DecodeOpaque(procView.Records[receipt.Request])
		if e != nil {
			t.Fatal(e)
		}
		challenge := digestReply(t, origin, 401)
		challenge = portable.RestoreMessage(challenge.WireBytes(), flow.Transport, "192.0.2.1:5060", "")
		ticket, handled, e := publishRedisUACDigestChallenge(ctx, store, flow, challenge, key, input.ExecutorTimer, time.Now().Add(time.Second))
		if e != nil || !handled || ticket == nil {
			t.Fatal("publish challenge", e)
		}
		entries, e = store.PeekReady(ctx, workerID, worker.Epoch, 2)
		if e != nil || len(entries) != 1 {
			t.Fatal("challenge ACK ready", e)
		}
		if e = runtime.readyOutput(ctx, store, entries[0]); e != nil {
			t.Fatal("challenge ACK", e)
		}
		if successCase == 60 || successCase == 63 {
			command := &pb.Command{Context: ref, Sequence: 2, Action: &pb.Command_Cancel{Cancel: &pb.Cancel{}}}
			wait, x := captureRedisNotifyWait(command)
			if x != nil {
				t.Fatal(x)
			}
			result, x := publishRedisClientCancel(ctx, store, key, app, lease.ApplicationLeaseRef, command, wait, "cancel-digest", input.ExecutorTimer, time.Now().Add(time.Second))
			if x != nil || result.Certainty != pb.OperationResult_ACCEPTED || !result.Snapshot.Ended {
				t.Fatal("cancel challenge", x)
			}
			members, x := client.ZRange(ctx, root+"core:deadlines:uac", 0, -1).Result()
			if x != nil {
				t.Fatal(x)
			}
			for _, member := range members {
				if !strings.HasPrefix(member, "event-") {
					continue
				}
				v, x := store.ObserveContext(ctx, "uac", "incarnation", "r:"+member)
				if x != nil {
					t.Fatal(x)
				}
				event, x := decodeNativeLifecycle(v.View.Records["r:"+member])
				if x != nil {
					t.Fatal(x)
				}
				checked := waitRedisFixtureTime(t, ctx, client, time.UnixMilli(event.NextMs))
				d := &rediscore.DueContext{Identity: v.Identity, Revision: v.View.Revision, Assignment: v.View.Timer, Member: member, DeadlineMS: event.NextMs, CheckedAtMS: checked.UnixMilli(), Observed: time.Now()}
				c, _, x := selectRedisNativeLifecycle(ctx, store, d)
				if x != nil {
					t.Fatal(x)
				}
				if x = completeRedisNativeLifecycle(ctx, store, c, true, time.Now().Add(time.Second)); x != nil {
					t.Fatal(x)
				}
			}
			if successCase == 63 {
				state, x := store.ObserveContext(ctx, "uac", "incarnation", "r:context", record.InitialProcedures[0])
				if x != nil {
					t.Fatal(x)
				}
				current, x := decodeNativeContext(state.View.Records["r:context"])
				parent, pErr := readPrivateProcedureRecord(state.View.Records[record.InitialProcedures[0]])
				if x != nil || pErr != nil || !current.Ended || parent.Native.Phase != string(portable.Completed) || !parent.Native.Cancelled || parent.Ack == "" || len(current.InitialProcedures) != 1 {
					t.Fatal("UDP challenge cancellation", x, pErr)
				}
				return
			}
		} else if e = store.ExpireApplication(ctx, lease.ApplicationLeaseRef); e != nil {
			t.Fatal(e)
		}
		before, e := store.ObserveContext(ctx, "uac", "incarnation", "r:context", record.InitialProcedures[0])
		if e != nil {
			t.Fatal(e)
		}
		current, e := decodeNativeContext(before.View.Records["r:context"])
		if e != nil {
			t.Fatal(e)
		}
		item := rediscore.ExpiredApplicationContext{Identity: before.Identity, Revision: before.View.Revision}
		capture := &redisExpiredOwnerSelection{identity: before.Identity, view: before.View, context: current}
		if successCase == 39 {
			if e = releaseExpiredRedisCall(ctx, store, item, key, input.ExecutorTimer, time.Now().Add(time.Second), capture); e != nil {
				t.Fatal("release abandoned digest", e)
			}
		}
		after, e := store.ObserveContext(ctx, "uac", "incarnation", "r:context", record.InitialProcedures[0])
		if e != nil || successCase == 39 && after.Identity.Owner != "" {
			t.Fatal("owner not released", e)
		}
		ended, e := decodeNativeContext(after.View.Records["r:context"])
		anchor, pErr := readPrivateProcedureRecord(after.View.Records[record.InitialProcedures[0]])
		if e != nil || pErr != nil || !ended.Ended || ended.CallRetireAtMs == 0 || anchor.Native.Phase != string(portable.Terminated) || !anchor.DigestHandled {
			t.Fatal("abandoned challenge state", e, pErr)
		}
		members, e := client.ZRange(ctx, root+"core:deadlines:uac", 0, -1).Result()
		if e != nil || !slices.Contains(members, "call-retire") {
			t.Fatal("missing existing retirement duty", e)
		}
		// Consume each real physical cutoff before advancing the immutable
		// retirement and original receipt cutoffs in this bounded fixture.
		limit := time.Now().Add(4 * time.Second)
		for {
			observed, readErr := store.ObserveContext(ctx, "uac", "incarnation", record.InitialProcedures[0])
			if readErr != nil {
				t.Fatal(readErr)
			}
			p, readErr := readPrivateProcedureRecord(observed.View.Records[record.InitialProcedures[0]])
			if readErr != nil {
				t.Fatal(readErr)
			}
			if len(p.Outputs) == 0 {
				break
			}
			if !time.Now().Before(limit) {
				t.Fatal("physical outputs did not retire")
			}
			if _, readErr = store.RenewWorker(ctx, rediscore.WorkerLeaseRequest{Worker: workerID, Epoch: worker.Epoch, PrimaryRunID: primary}); readErr != nil {
				t.Fatal(readErr)
			}
			due, readErr := store.PeekDue(ctx, workerID, worker.Epoch)
			if readErr != nil {
				t.Fatal(readErr)
			}
			if due == nil {
				time.Sleep(5 * time.Millisecond)
				continue
			}
			switch {
			case strings.HasPrefix(due.Member, "out-uac-output-"):
				readErr = retireRedisUACInitialOutput(ctx, store, due, time.Now().Add(time.Second))
			case strings.HasPrefix(due.Member, "out-uac-ack-output-"):
				readErr = retireRedisUACFailureACK(ctx, store, due, time.Now().Add(time.Second))
			default:
				t.Fatal("unexpected due", due.Member)
			}
			if readErr != nil {
				t.Fatal("retire physical output", readErr)
			}
		}
		terminal, e := store.ObserveContext(ctx, "uac", "incarnation", "r:context", "r:result-1", "r:result-2")
		if e != nil {
			t.Fatal(e)
		}
		finished, e := decodeNativeContext(terminal.View.Records["r:context"])
		if e != nil {
			t.Fatal(e)
		}
		receipt, e := decodeNativeResult(terminal.View.Records["r:result-1"])
		if e != nil {
			t.Fatal(e)
		}
		past := time.Now().Add(-time.Second)
		finished.CallRetireAtMs = past.UnixMilli()
		finished.Revision++
		receipt.CutoffMs = past.UnixMilli()
		contextData, e := encodeNativeContext(finished)
		if e != nil {
			t.Fatal(e)
		}
		resultData, e := encodeNativeResult(receipt)
		if e != nil {
			t.Fatal(e)
		}
		changes := []rediscore.Change{{Field: "r:context", Value: contextData}, {Field: "r:result-1", Value: resultData}}
		timers := []rediscore.TimerChange{{Member: "call-retire", Due: past}}
		if successCase == 60 {
			cancelResult, x := decodeNativeResult(terminal.View.Records["r:result-2"])
			if x != nil {
				t.Fatal(x)
			}
			cancelResult.CutoffMs = past.UnixMilli()
			data, x := encodeNativeResult(cancelResult)
			if x != nil {
				t.Fatal(x)
			}
			changes = append(changes, rediscore.Change{Field: "r:result-2", Value: data})
			member, x := uacInitialResultTimer(record.InitialProcedures[0])
			if x != nil {
				t.Fatal(x)
			}
			timers = append(timers, rediscore.TimerChange{Member: member, Due: past})
		}
		_, e = store.Commit(ctx, rediscore.Publication{Identity: terminal.Identity, Revision: terminal.View.Revision, Deadline: time.Now().Add(time.Second), SpendReservation: true, Changes: changes, Timers: &rediscore.TimerMutation{Expected: terminal.View.Timer, Target: terminal.View.Timer, RequireLease: true, Changes: timers}})
		if e != nil {
			t.Fatal(e)
		}
		closed := false
		for i := 0; i < 4; i++ {
			due, readErr := store.PeekDue(ctx, workerID, worker.Epoch)
			if readErr != nil || due == nil {
				t.Fatal("retirement due", due, readErr)
			}
			if successCase == 60 && strings.HasPrefix(due.Member, "uac-result-") {
				if readErr = expireRedisUACInitialResult(ctx, store, due, key, time.Now().Add(time.Second)); readErr != nil {
					t.Fatal("expire original result", readErr)
				}
				continue
			}
			if due.Member != "call-retire" {
				t.Fatal("unexpected retirement due", due.Member)
			}
			closed, readErr = dispatchRedisCallRetirement(ctx, store, due, time.Now().Add(time.Second))
			if readErr != nil {
				t.Fatal("terminal cleanup", readErr)
			}
			if closed {
				break
			}
		}
		if !closed {
			t.Fatal("abandoned digest context did not retire")
		}
		return
	}
	if successCase >= 20 && successCase <= 22 {
		timer := portable.TimerB
		if successCase != 20 {
			origin, e := selectedmessage.DecodeOpaque(procView.Records[receipt.Request])
			if e != nil {
				t.Fatal(e)
			}
			status := 486
			if successCase == 22 {
				status = 200
				timer = portable.TimerM
			} else {
				timer = portable.TimerD
			}
			built, e := portable.BuildResponseWithHeaders(origin, status, "Result", nil, "remote-tag", "sip:phone@192.0.2.1:5060;transport="+flow.Transport, nil)
			if e != nil {
				t.Fatal(e)
			}
			response, e := portable.ParseMessage(built.WireBytes(), flow.Transport, "192.0.2.1:5060", "")
			if e != nil {
				t.Fatal(e)
			}
			if handled, e := receiveRedisUACProvisional(ctx, store, flow, response, key, input.ExecutorTimer, time.Now().Add(time.Second)); e != nil || !handled {
				t.Fatal("select final", e)
			}
		}
		before, e := store.ObserveContext(ctx, "uac", "incarnation", "r:context", record.InitialProcedures[0], "r:result-1")
		if e != nil {
			t.Fatal(e)
		}
		p, e := decodeNativeProcedure(before.View.Records[record.InitialProcedures[0]])
		if e != nil {
			t.Fatal(e)
		}
		fields := []string{p.Refs.Origin}
		if p.Refs.LastResponse != "" {
			fields = append(fields, p.Refs.LastResponse)
		}
		if p.Refs.ACK != "" {
			fields = append(fields, p.Refs.ACK)
		}
		wireView, e := store.Read(ctx, before.Identity, fields...)
		if e != nil {
			t.Fatal(e)
		}
		memo, e := selectedmessage.NewMemo(len(fields))
		if e != nil {
			t.Fatal(e)
		}
		tx, e := p.attach(&nativeSelectedMessages{records: wireView.Records, memo: memo, provenance: "admitted"}, true, true, true)
		if e != nil {
			t.Fatal(e)
		}
		redisNow, e := client.Time(ctx).Result()
		if e != nil {
			t.Fatal(e)
		}
		nativeTimer := tx.Timers[timer]
		if !nativeTimer.Active {
			t.Fatalf("%s was not active", timer)
		}
		nativeTimer.At = redisNow.Add(-time.Minute).UnixMilli()
		tx.Timers[timer] = nativeTimer
		encoded, e := encodeNativeProcedure(tx, p.Refs, p.Metadata)
		if e != nil {
			t.Fatal(e)
		}
		id := strings.TrimPrefix(record.InitialProcedures[0], "r:uac-procedure-")
		member := "call-tx-" + id + "-" + string(timer)
		_, e = store.Commit(ctx, rediscore.Publication{Identity: before.Identity, Revision: before.View.Revision, Deadline: time.Now().Add(time.Second), SpendReservation: true, Changes: []rediscore.Change{{Field: record.InitialProcedures[0], Value: encoded}}, Timers: &rediscore.TimerMutation{Expected: before.View.Timer, Target: before.View.Timer, RequireLease: true, Changes: []rediscore.TimerChange{{Member: member, Due: time.UnixMilli(nativeTimer.At)}}}})
		if e != nil {
			t.Fatal(e)
		}
		due, e := store.PeekDue(ctx, workerID, worker.Epoch)
		if e != nil || due == nil || due.Member != member {
			t.Fatal("timer not due", due, e)
		}
		stale := *due
		stale.Revision++
		if e = fireRedisUACDeadline(ctx, store, &stale, time.Now().Add(time.Second)); !errors.Is(e, rediscore.ErrConflict) {
			t.Fatal("stale timer accepted", e)
		}
		if e = fireRedisUACDeadline(ctx, store, due, time.Now().Add(time.Second)); e != nil {
			t.Fatal("timer failed", e)
		}
		after, e := store.ObserveContext(ctx, "uac", "incarnation", "r:context", record.InitialProcedures[0], "r:result-1")
		if e != nil {
			t.Fatal(e)
		}
		finished, e := decodeNativeProcedure(after.View.Records[record.InitialProcedures[0]])
		if e != nil || finished.Native.Phase != portable.Terminated {
			t.Fatal("anchor not terminated", e)
		}
		contextRecord, e := decodeNativeContext(after.View.Records["r:context"])
		if e != nil || !slices.Contains(contextRecord.InitialProcedures, record.InitialProcedures[0]) || timer == portable.TimerB && !contextRecord.Ended {
			t.Fatal("anchor or timeout state lost", e)
		}
		if timer == portable.TimerB {
			updated, decodeErr := decodeNativeResult(after.View.Records["r:result-1"])
			if decodeErr != nil || updated.Certainty != pb.OperationResult_COMPLETED || updated.Error != "timeout" {
				t.Fatal("timeout receipt not completed", decodeErr)
			}
			members, readErr := client.ZRange(ctx, root+"core:deadlines:uac", 0, -1).Result()
			if readErr != nil {
				t.Fatal(readErr)
			}
			eventField := ""
			for _, item := range members {
				if strings.HasPrefix(item, "event-") {
					eventField = "r:" + item
				}
			}
			if eventField == "" {
				t.Fatal("FAILED lifecycle deadline missing")
			}
			eventView, readErr := store.Read(ctx, after.Identity, eventField)
			if readErr != nil {
				t.Fatal(readErr)
			}
			event, decodeErr := decodeNativeLifecycle(eventView.Records[eventField])
			if decodeErr != nil || event.Kind != pb.DialogEvent_FAILED || event.Snapshot == nil || !event.Snapshot.Ended {
				t.Fatal("invalid FAILED lifecycle", decodeErr)
			}
		} else if !bytes.Equal(before.View.Records["r:context"], after.View.Records["r:context"]) || !bytes.Equal(before.View.Records["r:result-1"], after.View.Records["r:result-1"]) {
			t.Fatal("transaction timer changed context or receipt")
		}
		return
	}
	if successCase == 23 {
		entries, e := store.PeekReady(ctx, workerID, worker.Epoch, 1)
		if e != nil || len(entries) != 1 {
			t.Fatal("missing ready entry", e)
		}
		runtime := &redisNative{worker: workerID, cfg: Config{Process: worker, StateIdentityKey: key}}
		runtime.cfg.WriteCommitted = func(ctx context.Context, write *pb.FlowWrite) *pb.DeliveryResult {
			writer := nativeRedisWriter{store: store}
			reserved, reserveErr := writer.Reserve(ctx, write)
			if reserveErr != nil || reserved == nil || len(reserved.Wire) == 0 {
				t.Errorf("reserve: %v", reserveErr)
				return nil
			}
			return &pb.DeliveryResult{Effect: write.Effect, Outcome: pb.DeliveryResult_WRITTEN}
		}
		if e = runtime.readyOutput(ctx, store, entries[0]); e != nil {
			t.Fatal("ready dispatch", e)
		}
		if _, e = store.InspectOutput(ctx, entries[0].Ref); !errors.Is(e, rediscore.ErrAbsent) {
			t.Fatal("definite proof not settled and acknowledged", e)
		}
		after, e := store.ObserveContext(ctx, "uac", "incarnation", "r:result-1")
		if e != nil {
			t.Fatal(e)
		}
		updated, e := decodeNativeResult(after.View.Records["r:result-1"])
		if e != nil || updated.Certainty != pb.OperationResult_ACCEPTED {
			t.Fatal("ready dispatch changed acceptance", e)
		}
		return
	}
	if !proto.Equal(ready[0].Flow, flow) {
		t.Fatal("selected output flow changed")
	}
	if provisional {
		origin, e := selectedmessage.DecodeOpaque(procView.Records[receipt.Request])
		if e != nil {
			t.Fatal(e)
		}
		makeResponse := func(status int) portable.Message {
			built, e := portable.BuildResponseWithHeaders(origin, status, "Ringing", nil, "remote-tag", "sip:phone@192.0.2.1:5060;transport=udp", nil)
			if e != nil {
				t.Fatal(e)
			}
			m, e := portable.ParseMessage(built.WireBytes(), flow.Transport, "192.0.2.1:5060", "")
			if e != nil {
				t.Fatal(e)
			}
			return m
		}
		if successCase == 8 || successCase == 9 || successCase == 10 || successCase == 24 || successCase == 70 {
			if handled, e := receiveRedisUACProvisional(ctx, store, flow, makeResponse(180), key, input.ExecutorTimer, time.Now().Add(time.Second)); e != nil || !handled {
				t.Fatal("pre-cancellation provisional", e)
			}
			base, e := store.ObserveContext(ctx, "uac", "incarnation", record.InitialProcedures[0])
			if e != nil {
				t.Fatal(e)
			}
			before, e := readPrivateProcedureRecord(base.View.Records[record.InitialProcedures[0]])
			if e != nil || len(before.Events) != 1 {
				t.Fatal("pre-cancellation provisional debt", e)
			}
			provisionalField, provisionalWire := before.Events[0], before.LastResponse
			if successCase == 8 || successCase == 10 || successCase == 24 {
				p, e := decodeNativeProcedure(base.View.Records[record.InitialProcedures[0]])
				if e != nil {
					t.Fatal(e)
				}
				wires, e := store.Read(ctx, base.Identity, p.Refs.Origin, p.Refs.LastResponse)
				if e != nil {
					t.Fatal(e)
				}
				memo, e := selectedmessage.NewMemo(2)
				if e != nil {
					t.Fatal(e)
				}
				tx, e := p.attach(&nativeSelectedMessages{records: wires.Records, memo: memo, provenance: "admitted"}, true, true, false)
				if e != nil {
					t.Fatal(e)
				}
				input := portable.Input{Kind: portable.RequestCancel, Now: time.Now().UnixMilli()}
				if successCase == 10 {
					input.Kind, input.Error = portable.TransportFailure, "fixture transport failure"
				}
				tx, _, e = portable.StepTransaction(tx, input)
				if e != nil {
					t.Fatal(e)
				}
				data, e := encodeNativeProcedure(tx, p.Refs, p.Metadata)
				if e != nil {
					t.Fatal(e)
				}
				_, e = store.Commit(ctx, rediscore.Publication{Identity: base.Identity, Revision: base.View.Revision, Deadline: time.Now().Add(time.Second), SpendReservation: true, Changes: []rediscore.Change{{Field: record.InitialProcedures[0], Value: data}}})
				if e != nil {
					t.Fatal(e)
				}
			} else {
				if e = client.HSet(ctx, root+"core:owner:"+lease.Term, "expires_ms", "1").Err(); e != nil {
					t.Fatal(e)
				}
				if e = client.ZAdd(ctx, root+"core:owner-expiry", redis.Z{Score: 1, Member: "a-" + lease.Term}).Err(); e != nil {
					t.Fatal(e)
				}
				_, e = store.Commit(ctx, rediscore.Publication{Identity: base.Identity, Revision: base.View.Revision, Deadline: time.Now().Add(time.Second), ReleaseApplication: true})
				if e != nil {
					t.Fatal(e)
				}
			}
			final := makeResponse(200)
			handled, e := receiveRedisUACProvisional(ctx, store, flow, final, key, input.ExecutorTimer, time.Now().Add(time.Second))
			if e != nil || !handled {
				t.Fatalf("late first success: %v", e)
			}
			after, e := store.ObserveContext(ctx, "uac", "incarnation", "r:context", record.InitialProcedures[0])
			if e != nil {
				t.Fatal(e)
			}
			c, e := decodeNativeContext(after.View.Records["r:context"])
			if e != nil || c.Answered || len(c.Dialogs) != 1 {
				t.Fatal("cancelled/owner-lost fork selected winner", e)
			}
			p, e := readPrivateProcedureRecord(after.View.Records[record.InitialProcedures[0]])
			if e != nil || len(p.Events) != 0 {
				t.Fatal("cancelled/owner-lost fork emitted ANSWERED", e)
			}
			obsolete, e := store.Read(ctx, after.Identity, provisionalField, provisionalWire)
			if e != nil || len(obsolete.Records[provisionalField]) != 0 || (len(obsolete.Records[provisionalWire]) != 0) != (p.LastResponse == provisionalWire) {
				t.Fatal("cancelled/owner-lost final retained provisional history", e)
			}
			if _, e = client.ZScore(ctx, root+"core:deadlines:uac", strings.TrimPrefix(provisionalField, "r:")).Result(); !errors.Is(e, redis.Nil) {
				t.Fatal("cancelled/owner-lost final retained provisional timer", e)
			}
			writes, e := nativeReadyWrites(ctx, store, workerID, worker.Epoch, 4)
			if e != nil {
				t.Fatal(e)
			}
			var ack *pb.FlowWrite
			var meta *nativepb.Output
			var ref rediscore.OutputRef
			for _, write := range writes {
				r, e := nativeOutputRef(write)
				if e != nil {
					t.Fatal(e)
				}
				proof, e := store.InspectOutput(ctx, r)
				if e != nil {
					t.Fatal(e)
				}
				m, e := decodeNativeOutput(proof.Metadata)
				if e != nil {
					t.Fatal(e)
				}
				if m.Method == "ACK" {
					ack, meta, ref = write, m, r
				}
			}
			if ack == nil || !meta.CleanupAfterAck || meta.Result != "" {
				t.Fatal("missing native cleanup ACK")
			}
			if successCase == 24 {
				runtime := &redisNative{worker: workerID, cfg: Config{Process: worker, StateIdentityKey: key}}
				runtime.cfg.WriteCommitted = func(ctx context.Context, write *pb.FlowWrite) *pb.DeliveryResult {
					reserved, reserveErr := (nativeRedisWriter{store: store}).Reserve(ctx, write)
					if reserveErr != nil || reserved == nil || len(reserved.Wire) == 0 {
						t.Errorf("reserve: %v", reserveErr)
						return nil
					}
					return &pb.DeliveryResult{Effect: write.Effect, Outcome: pb.DeliveryResult_WRITTEN}
				}
				for _, method := range []string{"ACK", "BYE"} {
					entries, peekErr := store.PeekReady(ctx, workerID, worker.Epoch, 8)
					if peekErr != nil {
						t.Fatal(peekErr)
					}
					found := false
					for _, entry := range entries {
						output, decodeErr := decodeNativeOutput(entry.Metadata)
						if decodeErr != nil {
							t.Fatal(decodeErr)
						}
						if output.Method == method {
							if dispatchErr := runtime.readyOutput(ctx, store, entry); dispatchErr != nil {
								t.Fatal(method, dispatchErr)
							}
							if _, inspectErr := store.InspectOutput(ctx, entry.Ref); !errors.Is(inspectErr, rediscore.ErrAbsent) {
								t.Fatal(method, "proof not consumed", inspectErr)
							}
							found = true
							break
						}
					}
					if !found {
						t.Fatal("missing ready", method)
					}
				}
				final, readErr := store.ObserveContext(ctx, "uac", "incarnation", "r:result-1")
				if readErr != nil {
					t.Fatal(readErr)
				}
				result, decodeErr := decodeNativeResult(final.View.Records["r:result-1"])
				if decodeErr != nil || result.Certainty != pb.OperationResult_ACCEPTED {
					t.Fatal("cleanup changed original acceptance", decodeErr)
				}
				return
			}
			w := nativeRedisWriter{store: store}
			reservedACK, e := w.Reserve(ctx, ack)
			if e != nil {
				t.Fatal(e)
			}
			if e = w.Complete(ctx, ack, &pb.DeliveryResult{Effect: ack.Effect, Outcome: pb.DeliveryResult_WRITTEN}); e != nil {
				t.Fatal(e)
			}
			proof, e := store.InspectOutput(ctx, ref)
			if e != nil {
				t.Fatal(e)
			}
			capture := &nativeOutputCapture{ref: ref, proof: proof, metadata: meta}
			if e = settleRedisUACSuccessACK(ctx, store, ack, capture, key, input.ExecutorTimer, time.Now().Add(time.Second)); e != nil {
				t.Fatal(e)
			}
			finalState, e := store.ObserveContext(ctx, "uac", "incarnation", "r:context", meta.Dialog)
			if e != nil {
				t.Fatal(e)
			}
			c, e = decodeNativeContext(finalState.View.Records["r:context"])
			if e != nil || c.Answered || len(c.NoninitialProcedures) != 1 {
				t.Fatal("late first success lacked cleanup", e)
			}
			d, e := readPrivateDialogRecord(finalState.View.Records[meta.Dialog])
			if e != nil || !d.AckWritten || d.LocalBye == "" {
				t.Fatal("late first ACK did not stage BYE", e)
			}
			if successCase == 70 {
				testRedisCleanupACKReplayBeforeCutoff(t, ctx, store, input, workerID, final, ack, reservedACK.Wire, meta, ref)
			}
			return
		}
		if successCase == 3 {
			final := makeResponse(200)
			handled, e := receiveRedisUACProvisional(ctx, store, flow, final, key, input.ExecutorTimer, time.Now().Add(time.Second))
			if e != nil || !handled {
				t.Fatalf("winner without early dialog: %v", e)
			}
			selected, e := store.ObserveContext(ctx, "uac", "incarnation", "r:context", record.InitialProcedures[0])
			if e != nil {
				t.Fatal(e)
			}
			current, e := decodeNativeContext(selected.View.Records["r:context"])
			if e != nil || !current.Answered || len(current.Dialogs) != 1 {
				t.Fatal("direct winner missing dialog", e)
			}
			p, e := readPrivateProcedureRecord(selected.View.Records[record.InitialProcedures[0]])
			if e != nil || len(p.Events) != 1 || len(p.Outputs) != 2 {
				t.Fatal("direct winner obligations missing", e)
			}
			return
		}
		first := makeResponse(180)
		foreign := proto.Clone(flow).(*pb.FlowRef)
		foreign.ConnectionId = "other-socket"
		if handled, e := receiveRedisUACProvisional(ctx, store, foreign, first, key, input.ExecutorTimer, time.Now().Add(time.Second)); !handled || e == nil {
			t.Fatal("foreign response flow admitted")
		}
		final := makeResponse(401)
		if ticket, handled, e := publishRedisUACDigestChallenge(ctx, store, flow, final, key, input.ExecutorTimer, time.Now().Add(time.Second)); e != nil || handled || ticket != nil {
			t.Fatal("credential-free response started digest", e)
		}
		unchanged, e := store.ObserveContext(ctx, "uac", "incarnation", "r:context")
		if e != nil || unchanged.View.Revision != selected.View.Revision {
			t.Fatal("refusal mutated context", e)
		}
		if successCase == 69 {
			// Close the original INVITE's actual write proof before constructing
			// the later ACK-U terminal case; only that ACK is meant to stay unknown.
			runtime := &redisNative{worker: workerID, cfg: Config{Process: worker, StateIdentityKey: key}}
			runtime.cfg.WriteCommitted = func(ctx context.Context, write *pb.FlowWrite) *pb.DeliveryResult {
				reserved, e := (nativeRedisWriter{store: store}).Reserve(ctx, write)
				if e != nil || reserved == nil || reserved.Known || !bytes.Equal(reserved.Wire, result.Request.Wire) {
					t.Errorf("reserve original INVITE: %v", e)
					return nil
				}
				return &pb.DeliveryResult{Effect: write.Effect, Outcome: pb.DeliveryResult_WRITTEN}
			}
			entries, e := store.PeekReady(ctx, workerID, worker.Epoch, 1)
			if e != nil || len(entries) != 1 {
				t.Fatal("original INVITE ready output", e)
			}
			if e = runtime.readyOutput(ctx, store, entries[0]); e != nil {
				t.Fatal("write original INVITE", e)
			}
		}
		handled, e := receiveRedisUACProvisional(ctx, store, flow, first, key, input.ExecutorTimer, time.Now().Add(time.Second))
		if e != nil || !handled {
			t.Fatalf("first provisional: %v", e)
		}
		firstView, e := store.ObserveContext(ctx, "uac", "incarnation", "r:context", record.InitialProcedures[0])
		if e != nil {
			t.Fatal(e)
		}
		firstProc, e := readPrivateProcedureRecord(firstView.View.Records[record.InitialProcedures[0]])
		if e != nil || len(firstProc.Events) != 1 {
			t.Fatal("missing provisional event", e)
		}
		firstContext, e := decodeNativeContext(firstView.View.Records["r:context"])
		if e != nil || len(firstContext.Dialogs) != 1 {
			t.Fatal("missing early dialog", e)
		}
		eventField := firstProc.Events[0]
		eventView, e := store.Read(ctx, firstView.Identity, eventField, firstProc.LastResponse)
		if e != nil {
			t.Fatal(e)
		}
		event, e := decodeNativeLifecycle(eventView.Records[eventField])
		if e != nil || event.Kind != pb.DialogEvent_PROVISIONAL || event.Root != record.InitialProcedures[0] || event.Message != firstProc.LastResponse || !slices.Equal(event.Owners, []string{record.InitialProcedures[0]}) || event.OperationId != input.Operation {
			t.Fatal("bad provisional event", e)
		}
		oldField := event.Message
		// Capture the immutable call before the final supersedes its debt.
		// No network call is needed to exercise a late completion publication.
		captured, public, e := selectRedisNativeLifecycle(ctx, store, &rediscore.DueContext{Identity: firstView.Identity, Revision: firstView.View.Revision, Assignment: firstView.View.Timer,
			Member: strings.TrimPrefix(eventField, "r:"), DeadlineMS: event.NextMs, CheckedAtMS: time.Now().UnixMilli(), Observed: time.Now()})
		if e != nil || public == nil || public.Kind != pb.DialogEvent_PROVISIONAL {
			t.Fatal("provisional call capture", e)
		}
		handled, e = receiveRedisUACProvisional(ctx, store, flow, makeResponse(180), key, input.ExecutorTimer, time.Now().Add(time.Second))
		if e != nil || !handled {
			t.Fatalf("repeated provisional: %v", e)
		}
		repeatView, e := store.ObserveContext(ctx, "uac", "incarnation", record.InitialProcedures[0], eventField)
		if e != nil {
			t.Fatal(e)
		}
		repeatProc, e := readPrivateProcedureRecord(repeatView.View.Records[record.InitialProcedures[0]])
		if e != nil || repeatProc.LastResponse != oldField || !slices.Equal(repeatProc.Events, []string{eventField}) {
			t.Fatal("repeat allocated another response", e)
		}
		repeatEvent, e := decodeNativeLifecycle(repeatView.View.Records[eventField])
		if e != nil || !slices.Equal(repeatEvent.Owners, []string{record.InitialProcedures[0]}) {
			t.Fatal("repeat lost existing owner", e)
		}
		second := makeResponse(183)
		handled, e = receiveRedisUACProvisional(ctx, store, flow, second, key, input.ExecutorTimer, time.Now().Add(time.Second))
		if e != nil || !handled {
			t.Fatalf("second provisional: %v", e)
		}
		secondView, e := store.ObserveContext(ctx, "uac", "incarnation", record.InitialProcedures[0], eventField, oldField)
		if e != nil {
			t.Fatal(e)
		}
		secondProc, e := readPrivateProcedureRecord(secondView.View.Records[record.InitialProcedures[0]])
		if e != nil || !slices.Equal(secondProc.Events, []string{eventField}) || secondProc.LastResponse == oldField {
			t.Fatal("provisional did not coalesce", e)
		}
		retained, e := decodeNativeLifecycle(secondView.View.Records[eventField])
		if e != nil || retained.Message != oldField || len(retained.Owners) != 0 || secondView.View.Records[oldField] == nil {
			t.Fatal("prior event lost sole wire ownership", e)
		}
		assertFinalSupersedesProvisional := func() {
			t.Helper()
			view, e := store.ObserveContext(ctx, "uac", "incarnation", eventField, oldField, secondProc.LastResponse)
			if e != nil || len(view.View.Records[eventField]) != 0 || len(view.View.Records[oldField]) != 0 || len(view.View.Records[secondProc.LastResponse]) != 0 {
				t.Fatal("final retained provisional event or unowned wire", e)
			}
			_, e = client.ZScore(ctx, root+"core:deadlines:uac", strings.TrimPrefix(eventField, "r:")).Result()
			if !errors.Is(e, redis.Nil) {
				t.Fatal("final retained provisional deadline", e)
			}
			if e = completeRedisNativeLifecycle(ctx, store, captured, true, time.Now().Add(time.Second)); e != nil {
				t.Fatal("late provisional completion", e)
			}
			after, e := store.Read(ctx, view.Identity, eventField)
			if e != nil || after.Revision != view.View.Revision || len(after.Records[eventField]) != 0 {
				t.Fatal("late provisional completion recreated debt", e)
			}
		}
		if successCase != 0 {
			var body []byte
			var headers []portable.Header
			if successCase == 2 || successCase == 25 || successCase >= 44 && successCase <= 47 || successCase >= 52 && successCase <= 58 || successCase == 61 || successCase == 62 || successCase >= 64 && successCase <= 71 {
				body = []byte("v=0\r\n")
				headers = []portable.Header{{Name: "Content-Type", Value: "application/sdp"}}
			}
			built, e := portable.BuildResponseWithHeaders(origin, 200, "OK", body, "remote-tag", "sip:phone@192.0.2.1:5060;transport="+flow.Transport, headers)
			if e != nil {
				t.Fatal(e)
			}
			final, e := portable.ParseMessage(built.WireBytes(), flow.Transport, "192.0.2.1:5060", "")
			if e != nil {
				t.Fatal(e)
			}
			handled, e = receiveRedisUACProvisional(ctx, store, flow, final, key, input.ExecutorTimer, time.Now().Add(time.Second))
			if e != nil || !handled {
				t.Fatalf("first winner: %v", e)
			}
			assertFinalSupersedesProvisional()
			winner, e := store.ObserveContext(ctx, "uac", "incarnation", "r:context", record.InitialProcedures[0], "r:result-1", firstContext.Dialogs[0])
			if e != nil {
				t.Fatal(e)
			}
			winnerContext, e := decodeNativeContext(winner.View.Records["r:context"])
			if e != nil || !winnerContext.Answered || winnerContext.Ended || winnerContext.RemoteTag != "remote-tag" || !bytes.Equal(winnerContext.RemoteSdp, body) {
				t.Fatal("winner snapshot", e)
			}
			winnerProc, e := readPrivateProcedureRecord(winner.View.Records[record.InitialProcedures[0]])
			if e != nil || len(winnerProc.Events) != 1 {
				t.Fatal("winner lifecycle missing", e)
			}
			winnerDialog, e := readPrivateDialogRecord(winner.View.Records[firstContext.Dialogs[0]])
			if e != nil || winnerDialog.LocalInvite == nil || winnerDialog.LocalInvite.FinalResponse != winnerProc.LastResponse {
				t.Fatal("winner response not shared", e)
			}
			winnerResult, e := decodeNativeResult(winner.View.Records["r:result-1"])
			if e != nil || winnerResult.Certainty != pb.OperationResult_ACCEPTED || winnerResult.Output != receipt.Output {
				t.Fatal("winner altered original result", e)
			}
			wantedACK := uint32(1)
			if successCase == 2 || successCase == 25 || successCase >= 44 && successCase <= 47 || successCase >= 52 && successCase <= 58 || successCase == 61 || successCase == 62 || successCase >= 64 && successCase <= 71 {
				wantedACK = 0
			}
			if winnerDialog.AckEmissions != wantedACK || len(winnerDialog.Outputs) != int(wantedACK) || len(winnerProc.Outputs) != 1+int(wantedACK) {
				t.Fatal("winner ACK/late-offer state")
			}
			if (successCase == 2 || successCase == 25 || successCase >= 44 && successCase <= 47 || successCase >= 52 && successCase <= 58 || successCase == 61 || successCase == 62 || successCase >= 64 && successCase <= 71) && winnerDialog.LocalInvite.Ack != "" {
				t.Fatal("late offer sent ACK before answer")
			}
			var answered *nativepb.Lifecycle
			answeredField := ""
			for _, field := range winnerProc.Events {
				v, e := store.Read(ctx, winner.Identity, field)
				if e != nil {
					t.Fatal(e)
				}
				event, e := decodeNativeLifecycle(v.Records[field])
				if e != nil {
					t.Fatal(e)
				}
				if event.Kind == pb.DialogEvent_ANSWERED {
					answered, answeredField = event, field
				}
			}
			if answered == nil || answered.Message != winnerProc.LastResponse || !answered.Snapshot.Answered || len(answered.Owners) != 2 || !slices.Contains(answered.Owners, record.InitialProcedures[0]) || !slices.Contains(answered.Owners, firstContext.Dialogs[0]) {
				t.Fatal("ANSWERED message ownership")
			}
			if successCase == 51 || successCase == 52 {
				testRedisCompletedInviteCancel(t, ctx, store, input, firstContext.Dialogs[0], answeredField, successCase)
				return
			}
			if successCase >= 44 && successCase <= 47 || successCase >= 53 && successCase <= 58 || successCase == 61 || successCase == 62 || successCase >= 64 && successCase <= 71 {
				testRedisApplicationAnswerACK(t, ctx, store, input, firstContext.Dialogs[0], answeredField, successCase, client, root)
				return
			}
			if successCase == 25 {
				if e = store.ExpireApplication(ctx, lease.ApplicationLeaseRef); e != nil {
					t.Fatal(e)
				}
				item := rediscore.ExpiredApplicationContext{Identity: winner.Identity, Revision: winner.View.Revision}
				capture := &redisExpiredOwnerSelection{identity: winner.Identity, view: winner.View, context: winnerContext}
				if e = releaseExpiredRedisCall(ctx, store, item, key, input.ExecutorTimer, time.Now().Add(time.Second), capture); e != nil {
					t.Fatal("expire late-offer owner", e)
				}
				after, e := store.ObserveContext(ctx, "uac", "incarnation", "r:context", firstContext.Dialogs[0], record.InitialProcedures[0])
				if e != nil || after.Identity.Owner != "" || after.Identity.Term != "" {
					t.Fatal("late-offer owner retained", e)
				}
				context, e := decodeNativeContext(after.View.Records["r:context"])
				if e != nil || !context.Ended {
					t.Fatal("late-offer call not ended", e)
				}
				dialog, e := readPrivateDialogRecord(after.View.Records[firstContext.Dialogs[0]])
				if e != nil || dialog.Native.Phase != string(portable.DialogEnded) || dialog.LocalInvite.Ack != "" || dialog.LocalBye != "" || len(dialog.Outputs) != 0 {
					t.Fatal("late-offer cleanup fabricated output", e)
				}
				anchor, e := readPrivateProcedureRecord(after.View.Records[record.InitialProcedures[0]])
				if e != nil || anchor.Native.Timers == nil || anchor.Native.Timers.Values[string(portable.TimerM)] == nil || !anchor.Native.Timers.Values[string(portable.TimerM)].Active {
					t.Fatal("late-offer cleanup lost fork timer", e)
				}
				return
			}
			if successCase == 50 || successCase == 1 || successCase == 4 || successCase == 5 || successCase == 6 || successCase == 7 || successCase == 11 || successCase == 12 || successCase == 13 || successCase == 14 || successCase >= 32 && successCase <= 38 || successCase >= 40 && successCase <= 43 {
				var outField string
				for _, f := range winnerProc.Outputs {
					if strings.HasPrefix(f, "r:uac-dialog-ack-output-") {
						outField = f
					}
				}
				outView, e := store.Read(ctx, winner.Identity, outField)
				if e != nil {
					t.Fatal(e)
				}
				out, e := decodeNativeOutput(outView.Records[outField])
				if e != nil || out.Result != "r:result-1" || out.Dialog != firstContext.Dialogs[0] || out.Procedure != record.InitialProcedures[0] {
					t.Fatal("winning ACK dependencies", e)
				}
				if successCase == 6 {
					// Simulate a later admitted command after the original waiter
					// cutoff, then compact only the original replay payload.
					base, e := store.ObserveContext(ctx, "uac", "incarnation", "r:context", "r:result-1")
					if e != nil {
						t.Fatal(e)
					}
					c, e := decodeNativeContext(base.View.Records["r:context"])
					if e != nil {
						t.Fatal(e)
					}
					c.CommandSequence = 2
					c.Revision++
					r, e := decodeNativeResult(base.View.Records["r:result-1"])
					if e != nil {
						t.Fatal(e)
					}
					r.CutoffMs = time.Now().Add(-time.Second).UnixMilli()
					plan, e := planRedisUACInitialResultExpiry(record.InitialProcedures[0], r, 2)
					if e != nil {
						t.Fatal(e)
					}
					contextBytes, e := encodeNativeContext(c)
					if e != nil {
						t.Fatal(e)
					}
					originalBytes, e := encodeNativeResult(r)
					if e != nil {
						t.Fatal(e)
					}
					currentBytes, e := encodeNativeResult(&nativepb.Result{Certainty: pb.OperationResult_ACCEPTED, Sequence: 2, OperationId: "later", Fingerprint: bytes.Repeat([]byte{2}, 32), DecidingLeaseTerm: lease.Term, CutoffMs: time.Now().Add(time.Minute).UnixMilli()})
					if e != nil {
						t.Fatal(e)
					}
					_, e = store.Commit(ctx, rediscore.Publication{Identity: base.Identity, Revision: base.View.Revision, Deadline: time.Now().Add(time.Second), SpendReservation: true, Changes: []rediscore.Change{{Field: "r:context", Value: contextBytes}, {Field: "r:result-1", Value: originalBytes}, {Field: "r:result-2", Value: currentBytes}}, Timers: &rediscore.TimerMutation{Expected: base.View.Timer, Target: base.View.Timer, Changes: []rediscore.TimerChange{plan}}})
					if e != nil {
						t.Fatal(e)
					}
					before, e := store.ObserveContext(ctx, "uac", "incarnation", "r:result-1")
					if e != nil {
						t.Fatal(e)
					}
					due := &rediscore.DueContext{Identity: before.Identity, Revision: before.View.Revision, Assignment: before.View.Timer, Member: plan.Member, DeadlineMS: r.CutoffMs, CheckedAtMS: r.CutoffMs}
					if e = expireRedisUACInitialResult(ctx, store, due, key, time.Now().Add(time.Second)); e != nil {
						t.Fatal(e)
					}
					compacted, e := store.Read(ctx, before.Identity, "r:result-1", receipt.Request)
					if e != nil {
						t.Fatal(e)
					}
					marker, e := decodeNativeResult(compacted.Records["r:result-1"])
					if e != nil || marker.Request != "" || marker.Response != "" || marker.OperationId != input.Operation || compacted.Records[receipt.Request] == nil {
						t.Fatal("original marker or native origin lost", e)
					}
					w := nativeRedisWriter{store: store}
					if _, e = w.Reserve(ctx, out.Write); e != nil {
						t.Fatal(e)
					}
					if e = w.Complete(ctx, out.Write, &pb.DeliveryResult{Effect: out.Write.Effect, Outcome: pb.DeliveryResult_WRITTEN}); e != nil {
						t.Fatal(e)
					}
					ackRef, e := nativeOutputRef(out.Write)
					if e != nil {
						t.Fatal(e)
					}
					proof, e := store.InspectOutput(ctx, ackRef)
					if e != nil {
						t.Fatal(e)
					}
					cap := &nativeOutputCapture{ref: ackRef, proof: proof, metadata: out}
					if e = settleRedisUACSuccessACK(ctx, store, out.Write, cap, key, input.ExecutorTimer, time.Now().Add(time.Second)); e != nil {
						t.Fatal(e)
					}
					after, e := store.Read(ctx, before.Identity, "r:result-1", answeredField)
					if e != nil {
						t.Fatal(e)
					}
					marker, e = decodeNativeResult(after.Records["r:result-1"])
					if e != nil || marker.Certainty != pb.OperationResult_COMPLETED || marker.Request != "" || marker.Response != "" {
						t.Fatal("late ACK restored compacted payload", e)
					}
					afterEvent, e := decodeNativeLifecycle(after.Records[answeredField])
					if e != nil || slices.Contains(afterEvent.Owners, "r:result-1") {
						t.Fatal("compacted result reacquired event wire", e)
					}
					return
				}
				// Release the original INVITE proof first. The distinct ACK dref
				// must still prevent the original result from being deleted.
				w := nativeRedisWriter{store: store}
				if _, e = w.Reserve(ctx, ready[0]); e != nil {
					t.Fatal(e)
				}
				if e = w.Complete(ctx, ready[0], &pb.DeliveryResult{Effect: ready[0].Effect, Outcome: pb.DeliveryResult_WRITTEN}); e != nil {
					t.Fatal(e)
				}
				initialRef, e := nativeOutputRef(ready[0])
				if e != nil {
					t.Fatal(e)
				}
				initialProof, e := store.InspectOutput(ctx, initialRef)
				if e != nil {
					t.Fatal(e)
				}
				initialOutput, e := decodeNativeOutput(initialProof.Metadata)
				if e != nil {
					t.Fatal(e)
				}
				cap := &nativeOutputCapture{ref: initialRef, proof: initialProof, metadata: initialOutput}
				if e = settleRedisUACInitialOutput(ctx, store, ready[0], cap, input.ExecutorTimer, time.Now().Add(time.Second)); e != nil {
					t.Fatal(e)
				}
				initialProof, e = store.InspectOutput(ctx, initialRef)
				if e != nil {
					t.Fatal(e)
				}
				cap.proof = initialProof
				if e = acknowledgeRedisUACInitialOutput(ctx, store, cap, time.Now().Add(time.Second)); e != nil {
					t.Fatal(e)
				}
				before, e := store.ObserveContext(ctx, "uac", "incarnation", "r:result-1")
				if e != nil {
					t.Fatal(e)
				}
				due := &rediscore.DueContext{Identity: before.Identity, Revision: before.View.Revision, Assignment: before.View.Timer, Member: "out-" + strings.TrimPrefix(receipt.Output, "r:"), DeadlineMS: initialProof.DeadlineMS, CheckedAtMS: initialProof.DeadlineMS}
				if e = retireRedisUACInitialOutput(ctx, store, due, time.Now().Add(time.Second)); e != nil {
					t.Fatal(e)
				}
				after, e := store.ObserveContext(ctx, "uac", "incarnation", "r:result-1")
				if e != nil {
					t.Fatal(e)
				}
				still, e := decodeNativeResult(after.View.Records["r:result-1"])
				if e != nil || still.Output != "" {
					t.Fatal("original result backlink not released", e)
				}
				_, e = store.Commit(ctx, rediscore.Publication{Identity: after.Identity, Revision: after.View.Revision, Deadline: time.Now().Add(time.Second), SpendReservation: true, Changes: []rediscore.Change{{Field: "r:result-1", Delete: true}}})
				if !errors.Is(e, rediscore.ErrConflict) {
					t.Fatalf("winning ACK failed to pin result: %v", e)
				}
				if successCase == 1 || successCase == 5 {
					loserBuilt, e := portable.BuildResponseWithHeaders(origin, 200, "OK", nil, "loser-tag", "sip:phone@192.0.2.1:5060;transport=udp", nil)
					if e != nil {
						t.Fatal(e)
					}
					loser, e := portable.ParseMessage(loserBuilt.WireBytes(), "udp", "192.0.2.1:5060", "")
					if e != nil {
						t.Fatal(e)
					}
					handled, e = receiveRedisUACProvisional(ctx, store, flow, loser, key, input.ExecutorTimer, time.Now().Add(time.Second))
					if e != nil || !handled {
						t.Fatalf("losing fork: %v", e)
					}
					loserState, e := store.ObserveContext(ctx, "uac", "incarnation", "r:context", record.InitialProcedures[0])
					if e != nil {
						t.Fatal(e)
					}
					loserContext, e := decodeNativeContext(loserState.View.Records["r:context"])
					if e != nil || loserContext.RemoteTag != "remote-tag" || len(loserContext.Dialogs) != 2 {
						t.Fatal("losing fork displaced winner", e)
					}
					writes, e := nativeReadyWrites(ctx, store, workerID, worker.Epoch, 4)
					if e != nil {
						t.Fatal(e)
					}
					var loserWrite *pb.FlowWrite
					for _, candidate := range writes {
						r, e := nativeOutputRef(candidate)
						if e != nil {
							t.Fatal(e)
						}
						p, e := store.InspectOutput(ctx, r)
						if e != nil {
							t.Fatal(e)
						}
						m, e := decodeNativeOutput(p.Metadata)
						if e != nil {
							t.Fatal(e)
						}
						if m.CleanupAfterAck {
							loserWrite = candidate
						}
					}
					if loserWrite == nil {
						t.Fatal("losing fork ACK missing")
					}
					handled, e = receiveRedisUACProvisional(ctx, store, flow, loser, key, input.ExecutorTimer, time.Now().Add(time.Second))
					if e != nil || !handled {
						t.Fatalf("pending losing ACK duplicate: %v", e)
					}
					writes, e = nativeReadyWrites(ctx, store, workerID, worker.Epoch, 4)
					if e != nil || len(writes) != 2 {
						t.Fatal("pending losing ACK was duplicated", e)
					}
					if _, e = w.Reserve(ctx, loserWrite); e != nil {
						t.Fatal(e)
					}
					if e = w.Complete(ctx, loserWrite, &pb.DeliveryResult{Effect: loserWrite.Effect, Outcome: pb.DeliveryResult_UNKNOWN}); e != nil {
						t.Fatal(e)
					}
					loserRef, e := nativeOutputRef(loserWrite)
					if e != nil {
						t.Fatal(e)
					}
					loserProof, e := store.InspectOutput(ctx, loserRef)
					if e != nil {
						t.Fatal(e)
					}
					loserMeta, e := decodeNativeOutput(loserProof.Metadata)
					if e != nil {
						t.Fatal(e)
					}
					loserCapture := &nativeOutputCapture{ref: loserRef, proof: loserProof, metadata: loserMeta}
					if e = settleRedisUACSuccessACK(ctx, store, loserWrite, loserCapture, key, input.ExecutorTimer, time.Now().Add(time.Second)); e != nil {
						t.Fatal(e)
					}
					u, e := store.ObserveContext(ctx, "uac", "incarnation", "r:context")
					if e != nil {
						t.Fatal(e)
					}
					uc, e := decodeNativeContext(u.View.Records["r:context"])
					if e != nil || len(uc.NoninitialProcedures) != 0 {
						t.Fatal("uncertain loser authorized BYE", e)
					}
					loserOutcome := pb.DeliveryResult_WRITTEN
					if successCase == 5 {
						loserOutcome = pb.DeliveryResult_NOT_WRITTEN
					}
					if e = w.Complete(ctx, loserWrite, &pb.DeliveryResult{Effect: loserWrite.Effect, Outcome: loserOutcome}); e != nil {
						t.Fatal(e)
					}
					loserProof, e = store.InspectOutput(ctx, loserRef)
					if e != nil {
						t.Fatal(e)
					}
					loserCapture.proof = loserProof
					if e = settleRedisUACSuccessACK(ctx, store, loserWrite, loserCapture, key, input.ExecutorTimer, time.Now().Add(time.Second)); e != nil {
						t.Fatal(e)
					}
					l, e := store.ObserveContext(ctx, "uac", "incarnation", "r:context", loserMeta.Dialog)
					if e != nil {
						t.Fatal(e)
					}
					lc, e := decodeNativeContext(l.View.Records["r:context"])
					if e != nil {
						t.Fatal(e)
					}
					ld, e := readPrivateDialogRecord(l.View.Records[loserMeta.Dialog])
					if e != nil {
						t.Fatal(e)
					}
					if successCase == 1 {
						if len(lc.NoninitialProcedures) != 1 || ld.LocalBye == "" || !ld.AckWritten {
							t.Fatal("written losing ACK did not stage BYE")
						}
						byeField, byeTransaction := ld.LocalBye, ld.LocalTransaction
						handled, e = receiveRedisUACProvisional(ctx, store, flow, loser, key, input.ExecutorTimer, time.Now().Add(time.Second))
						if e != nil || !handled {
							t.Fatalf("duplicate after cleanup BYE: %v", e)
						}
						preserved, e := store.ObserveContext(ctx, "uac", "incarnation", loserMeta.Dialog)
						if e != nil {
							t.Fatal(e)
						}
						pd, e := readPrivateDialogRecord(preserved.View.Records[loserMeta.Dialog])
						if e != nil || pd.LocalBye != byeField || pd.LocalTransaction != byeTransaction {
							t.Fatal("duplicate lost cleanup BYE", e)
						}
						loserProof, e = store.InspectOutput(ctx, loserRef)
						if e != nil || !loserProof.Settled {
							t.Fatal("losing ACK not settled", e)
						}
						loserCapture.proof = loserProof
						if e = acknowledgeRedisUACSuccessACK(ctx, store, loserWrite, loserCapture); e != nil {
							t.Fatal(e)
						}
						after, e := store.ObserveContext(ctx, "uac", "incarnation", "r:context")
						if e != nil {
							t.Fatal(e)
						}
						loserDue := &rediscore.DueContext{Identity: after.Identity, Revision: after.View.Revision, Assignment: after.View.Timer, Member: "out-" + strings.TrimPrefix(loserProof.MetadataField, "r:"), DeadlineMS: loserProof.DeadlineMS, CheckedAtMS: loserProof.DeadlineMS}
						if e = retireRedisUACSuccessACK(ctx, store, loserDue, key, time.Now().Add(time.Second)); e != nil {
							t.Fatal(e)
						}
						handled, e = receiveRedisUACProvisional(ctx, store, flow, loser, key, input.ExecutorTimer, time.Now().Add(time.Second))
						if e != nil || !handled {
							t.Fatalf("loser ACK replay after proof cleanup: %v", e)
						}
						preserved, e = store.ObserveContext(ctx, "uac", "incarnation", "r:context", loserMeta.Dialog)
						if e != nil {
							t.Fatal(e)
						}
						pd, e = readPrivateDialogRecord(preserved.View.Records[loserMeta.Dialog])
						if e != nil || pd.LocalBye != byeField || pd.LocalTransaction != byeTransaction || pd.AckEmissions != 2 {
							t.Fatal("loser ACK replay lost cleanup or count", e)
						}
						pc, e := decodeNativeContext(preserved.View.Records["r:context"])
						if e != nil || len(pc.NoninitialProcedures) != 1 {
							t.Fatal("loser replay duplicated cleanup", e)
						}
						cleanup, e := store.Read(ctx, preserved.Identity, pc.NoninitialProcedures[0])
						if e != nil {
							t.Fatal(e)
						}
						cleanupProc, e := readPrivateProcedureRecord(cleanup.Records[pc.NoninitialProcedures[0]])
						if e != nil || cleanupProc.Origin != byeField || cleanupProc.Native.Key != byeTransaction || !strings.HasPrefix(pc.NoninitialProcedures[0], "r:call-cleanup-") {
							t.Fatal("cleanup BYE shape", e)
						}
						replayed, e := nativeReadyWrites(ctx, store, workerID, worker.Epoch, 8)
						if e != nil {
							t.Fatal(e)
						}
						var replayWrite *pb.FlowWrite
						for _, candidate := range replayed {
							r, e := nativeOutputRef(candidate)
							if e != nil {
								t.Fatal(e)
							}
							p, e := store.InspectOutput(ctx, r)
							if e != nil {
								t.Fatal(e)
							}
							m, e := decodeNativeOutput(p.Metadata)
							if e != nil {
								t.Fatal(e)
							}
							if m.Method == "ACK" && m.Dialog == loserMeta.Dialog && m.Replay {
								replayWrite = candidate
							}
						}
						if replayWrite == nil {
							t.Fatal("post-cleanup ACK replay missing")
						}
						if _, e = w.Reserve(ctx, replayWrite); e != nil {
							t.Fatal(e)
						}
						if e = w.Complete(ctx, replayWrite, &pb.DeliveryResult{Effect: replayWrite.Effect, Outcome: pb.DeliveryResult_WRITTEN}); e != nil {
							t.Fatal(e)
						}
						r, e := nativeOutputRef(replayWrite)
						if e != nil {
							t.Fatal(e)
						}
						p, e := store.InspectOutput(ctx, r)
						if e != nil {
							t.Fatal(e)
						}
						m, e := decodeNativeOutput(p.Metadata)
						if e != nil {
							t.Fatal(e)
						}
						replayCapture := &nativeOutputCapture{ref: r, proof: p, metadata: m}
						if e = settleRedisUACSuccessACK(ctx, store, replayWrite, replayCapture, key, input.ExecutorTimer, time.Now().Add(time.Second)); e != nil {
							t.Fatal(e)
						}
						p, e = store.InspectOutput(ctx, r)
						if e != nil || !p.Settled {
							t.Fatal("replayed ACK not settled", e)
						}
						replayCapture.proof = p
						if e = acknowledgeRedisUACSuccessACK(ctx, store, replayWrite, replayCapture); e != nil {
							t.Fatal(e)
						}
						afterReplay, e := store.ObserveContext(ctx, "uac", "incarnation", "r:context", loserMeta.Dialog)
						if e != nil {
							t.Fatal(e)
						}
						afterContext, e := decodeNativeContext(afterReplay.View.Records["r:context"])
						if e != nil || len(afterContext.NoninitialProcedures) != 1 {
							t.Fatal("replayed ACK duplicated BYE", e)
						}
					} else if len(lc.NoninitialProcedures) != 0 || ld.LocalBye != "" || ld.AckWritten {
						t.Fatal("not-written losing ACK staged BYE")
					}
				}
				// U never supplies ACK evidence. A later exact W/N is settled
				// against the current dialog sequence and original result.
				ackRef, e := nativeOutputRef(out.Write)
				if e != nil {
					t.Fatal(e)
				}
				if _, e = w.Reserve(ctx, out.Write); e != nil {
					t.Fatal(e)
				}
				if e = w.Complete(ctx, out.Write, &pb.DeliveryResult{Effect: out.Write.Effect, Outcome: pb.DeliveryResult_UNKNOWN}); e != nil {
					t.Fatal(e)
				}
				ackProof, e := store.InspectOutput(ctx, ackRef)
				if e != nil {
					t.Fatal(e)
				}
				ackCapture := &nativeOutputCapture{ref: ackRef, proof: ackProof, metadata: out}
				if e = settleRedisUACSuccessACK(ctx, store, out.Write, ackCapture, key, input.ExecutorTimer, time.Now().Add(time.Second)); e != nil {
					t.Fatal(e)
				}
				unknown, e := store.ObserveContext(ctx, "uac", "incarnation", "r:context", firstContext.Dialogs[0], "r:result-1")
				if e != nil {
					t.Fatal(e)
				}
				unknownContext, e := decodeNativeContext(unknown.View.Records["r:context"])
				if e != nil || unknownContext.InitialAck {
					t.Fatal("uncertain ACK marked written", e)
				}
				ackDue := &rediscore.DueContext{Identity: unknown.Identity, Revision: unknown.View.Revision, Assignment: unknown.View.Timer, Member: "out-" + strings.TrimPrefix(outField, "r:"), DeadlineMS: ackProof.DeadlineMS, CheckedAtMS: ackProof.DeadlineMS}
				if e = retireRedisUACSuccessACK(ctx, store, ackDue, key, time.Now().Add(time.Second)); e != nil {
					t.Fatal(e)
				}
				if _, e = client.ZScore(ctx, root+"core:deadlines:uac", ackDue.Member).Result(); !errors.Is(e, redis.Nil) {
					t.Fatal("uncertain winner ACK retained expired head", e)
				}
				proofStill, e := store.InspectOutput(ctx, ackRef)
				if e != nil || proofStill.State != rediscore.OutputUnknown {
					t.Fatal("uncertain winner ACK lost proof", e)
				}
				if successCase == 11 {
					testWinningAckUnknownTerminal(t, ctx, client, store, root, input, workerID, origin, final, out, outField)
					return
				}
				outcome := pb.DeliveryResult_WRITTEN
				if successCase == 4 {
					outcome = pb.DeliveryResult_NOT_WRITTEN
				}
				if e = w.Complete(ctx, out.Write, &pb.DeliveryResult{Effect: out.Write.Effect, Outcome: outcome}); e != nil {
					t.Fatal(e)
				}
				ackProof, e = store.InspectOutput(ctx, ackRef)
				if e != nil {
					t.Fatal(e)
				}
				ackCapture.proof = ackProof
				if e = settleRedisUACSuccessACK(ctx, store, out.Write, ackCapture, key, input.ExecutorTimer, time.Now().Add(time.Second)); e != nil {
					t.Fatal(e)
				}
				settled, e := store.ObserveContext(ctx, "uac", "incarnation", "r:context", firstContext.Dialogs[0], "r:result-1", answeredField)
				if e != nil {
					t.Fatal(e)
				}
				settledContext, e := decodeNativeContext(settled.View.Records["r:context"])
				if e != nil {
					t.Fatal(e)
				}
				settledDialog, e := readPrivateDialogRecord(settled.View.Records[firstContext.Dialogs[0]])
				if e != nil {
					t.Fatal(e)
				}
				settledResult, e := decodeNativeResult(settled.View.Records["r:result-1"])
				if e != nil {
					t.Fatal(e)
				}
				settledEvent, e := decodeNativeLifecycle(settled.View.Records[answeredField])
				if e != nil {
					t.Fatal(e)
				}
				if successCase == 50 || successCase == 1 || successCase == 5 || successCase == 7 || successCase == 12 || successCase == 13 || successCase == 14 || successCase >= 32 && successCase <= 38 || successCase >= 40 && successCase <= 43 {
					if !settledContext.InitialAck || !settledDialog.AckWritten || !proto.Equal(settledDialog.CompletionFlow, flow) || settledResult.Certainty != pb.OperationResult_COMPLETED || settledResult.Request != receipt.Request || settledResult.Response != winnerProc.LastResponse || !slices.Contains(settledEvent.Owners, "r:result-1") {
						t.Fatal("written ACK did not complete exact winner")
					}
				} else if settledContext.InitialAck || settledDialog.AckWritten || settledResult.Certainty != pb.OperationResult_ACCEPTED || slices.Contains(settledEvent.Owners, "r:result-1") {
					t.Fatal("not-written ACK claimed positive evidence")
				}
				ackProof, e = store.InspectOutput(ctx, ackRef)
				if e != nil || !ackProof.Settled {
					t.Fatal("ACK proof not settled", e)
				}
				ackCapture.proof = ackProof
				if e = acknowledgeRedisUACSuccessACK(ctx, store, out.Write, ackCapture); e != nil {
					t.Fatal(e)
				}
				afterAck, e := store.ObserveContext(ctx, "uac", "incarnation", "r:context")
				if e != nil {
					t.Fatal(e)
				}
				ackDue.Identity, ackDue.Revision, ackDue.Assignment = afterAck.Identity, afterAck.View.Revision, afterAck.View.Timer
				if e = retireRedisUACSuccessACK(ctx, store, ackDue, key, time.Now().Add(time.Second)); e != nil {
					t.Fatal(e)
				}
				closed, e := store.Read(ctx, afterAck.Identity, record.InitialProcedures[0], firstContext.Dialogs[0], outField)
				if e != nil {
					t.Fatal(e)
				}
				closedProc, e := readPrivateProcedureRecord(closed.Records[record.InitialProcedures[0]])
				if e != nil || slices.Contains(closedProc.Outputs, outField) {
					t.Fatal("ACK remained on procedure", e)
				}
				closedDialog, e := readPrivateDialogRecord(closed.Records[firstContext.Dialogs[0]])
				if e != nil || slices.Contains(closedDialog.Outputs, outField) || closedDialog.LocalInvite.Ack == "" || closed.Records[outField] != nil {
					t.Fatal("ACK cleanup lost native ref or retained metadata", e)
				}
				if successCase == 50 {
					testRedisCompletedInviteCancel(t, ctx, store, input, firstContext.Dialogs[0], answeredField, successCase)
					return
				}
				if successCase == 12 || successCase == 13 || successCase == 14 || successCase >= 32 && successCase <= 34 {
					testRedisApplicationByeAdmission(t, ctx, store, client, root, input, workerID, primary, successCase == 13, successCase == 14, successCase)
					return
				}
				if successCase >= 35 && successCase <= 38 {
					method := []string{"INFO", "REFER", "UPDATE", "NOTIFY"}[successCase-35]
					testRedisApplicationDialogRequest(t, ctx, store, client, root, input, workerID, primary, method, false, pb.DeliveryResult_WRITTEN)
					return
				}
				if successCase == 40 {
					testRedisApplicationDialogRequest(t, ctx, store, client, root, input, workerID, primary, "INFO", true, pb.DeliveryResult_WRITTEN)
					return
				}
				if successCase == 41 || successCase == 42 {
					outcome := pb.DeliveryResult_NOT_WRITTEN
					if successCase == 42 {
						outcome = pb.DeliveryResult_UNKNOWN
					}
					testRedisApplicationDialogRequest(t, ctx, store, client, root, input, workerID, primary, "INFO", false, outcome)
					return
				}
				if successCase == 43 {
					testRedisApplicationDialogRequest(t, ctx, store, client, root, input, workerID, primary, "INFO", false, pb.DeliveryResult_UNKNOWN, origin, final)
					return
				}
				if successCase == 7 {
					base, e := store.ObserveContext(ctx, "uac", "incarnation", "r:context", "r:result-1")
					if e != nil {
						t.Fatal(e)
					}
					c, e := decodeNativeContext(base.View.Records["r:context"])
					if e != nil {
						t.Fatal(e)
					}
					c.CommandSequence = 2
					c.Revision++
					r, e := decodeNativeResult(base.View.Records["r:result-1"])
					if e != nil {
						t.Fatal(e)
					}
					oldFinal := r.Response
					r.CutoffMs = time.Now().Add(-time.Second).UnixMilli()
					plan, e := planRedisUACInitialResultExpiry(record.InitialProcedures[0], r, 2)
					if e != nil {
						t.Fatal(e)
					}
					contextBytes, e := encodeNativeContext(c)
					if e != nil {
						t.Fatal(e)
					}
					originalBytes, e := encodeNativeResult(r)
					if e != nil {
						t.Fatal(e)
					}
					currentBytes, e := encodeNativeResult(&nativepb.Result{Certainty: pb.OperationResult_ACCEPTED, Sequence: 2, OperationId: "later", Fingerprint: bytes.Repeat([]byte{2}, 32), DecidingLeaseTerm: lease.Term, CutoffMs: time.Now().Add(time.Minute).UnixMilli()})
					if e != nil {
						t.Fatal(e)
					}
					_, e = store.Commit(ctx, rediscore.Publication{Identity: base.Identity, Revision: base.View.Revision, Deadline: time.Now().Add(time.Second), SpendReservation: true, Changes: []rediscore.Change{{Field: "r:context", Value: contextBytes}, {Field: "r:result-1", Value: originalBytes}, {Field: "r:result-2", Value: currentBytes}}, Timers: &rediscore.TimerMutation{Expected: base.View.Timer, Target: base.View.Timer, Changes: []rediscore.TimerChange{plan}}})
					if e != nil {
						t.Fatal(e)
					}
					before, e := store.ObserveContext(ctx, "uac", "incarnation", "r:result-1")
					if e != nil {
						t.Fatal(e)
					}
					due := &rediscore.DueContext{Identity: before.Identity, Revision: before.View.Revision, Assignment: before.View.Timer, Member: plan.Member, DeadlineMS: r.CutoffMs, CheckedAtMS: r.CutoffMs}
					if e = expireRedisUACInitialResult(ctx, store, due, key, time.Now().Add(time.Second)); e != nil {
						t.Fatal(e)
					}
					after, e := store.ObserveContext(ctx, "uac", "incarnation", "r:context", "r:result-1", answeredField)
					if e != nil {
						t.Fatal(e)
					}
					marker, e := decodeNativeResult(after.View.Records["r:result-1"])
					if e != nil || marker.Request != "" || marker.Response != "" || marker.OperationId != input.Operation {
						t.Fatal("completed result did not compact", e)
					}
					event, e := decodeNativeLifecycle(after.View.Records[answeredField])
					if e != nil || slices.Contains(event.Owners, "r:result-1") {
						t.Fatal("event retained expired receipt owner", e)
					}
					ctxRecord, e := decodeNativeContext(after.View.Records["r:context"])
					if e != nil {
						t.Fatal(e)
					}
					release, e := planRedisLifecycleMessageRelease(ctx, store, after.Identity, after.View.Revision, after.View.Timer, ctxRecord, answeredField, event)
					if e != nil {
						t.Fatal(e)
					}
					release = append(release, rediscore.Change{Field: answeredField, Delete: true})
					_, e = store.Commit(ctx, rediscore.Publication{Identity: after.Identity, Revision: after.View.Revision, Deadline: time.Now().Add(time.Second), SpendReservation: true, Changes: release, Timers: &rediscore.TimerMutation{Expected: after.View.Timer, Target: after.View.Timer, RequireLease: true, Changes: []rediscore.TimerChange{{Member: strings.TrimPrefix(answeredField, "r:"), Delete: true}}}})
					if e != nil {
						t.Fatal(e)
					}
					anchor, e := store.ObserveContext(ctx, "uac", "incarnation", record.InitialProcedures[0])
					if e != nil {
						t.Fatal(e)
					}
					p, e := decodeNativeProcedure(anchor.View.Records[record.InitialProcedures[0]])
					if e != nil {
						t.Fatal(e)
					}
					wires, e := store.Read(ctx, anchor.Identity, p.Refs.Origin, p.Refs.LastResponse)
					if e != nil || wires.Revision != anchor.View.Revision {
						t.Fatal("anchor wire selection", e)
					}
					memo, e := selectedmessage.NewMemo(2)
					if e != nil {
						t.Fatal(e)
					}
					tx, e := p.attach(&nativeSelectedMessages{records: wires.Records, memo: memo, provenance: "admitted"}, true, true, false)
					if e != nil {
						t.Fatal(e)
					}
					m := tx.Timers[portable.TimerM]
					if !m.Active {
						t.Fatal("Timer M not active")
					}
					tx, _, e = portable.StepTransaction(tx, portable.Input{Kind: portable.TimerFired, Timer: portable.TimerM, Generation: m.Generation, Now: m.At})
					if e != nil || tx.Phase != portable.Terminated {
						t.Fatal("Timer M did not terminate", e)
					}
					data, e := encodeNativeProcedure(tx, p.Refs, p.Metadata)
					if e != nil {
						t.Fatal(e)
					}
					_, e = store.Commit(ctx, rediscore.Publication{Identity: anchor.Identity, Revision: anchor.View.Revision, Deadline: time.Now().Add(time.Second), SpendReservation: true, Changes: []rediscore.Change{{Field: record.InitialProcedures[0], Value: data}}, Timers: &rediscore.TimerMutation{Expected: anchor.View.Timer, Target: anchor.View.Timer, Changes: []rediscore.TimerChange{{Member: "call-tx-" + strings.TrimPrefix(record.InitialProcedures[0], "r:uac-procedure-") + "-M", Delete: true}}}})
					if e != nil {
						t.Fatal(e)
					}
					lateBuilt, e := portable.BuildResponseWithHeaders(origin, 200, "OK", nil, "late-tag", "sip:phone@192.0.2.1:5060;transport=udp", nil)
					if e != nil {
						t.Fatal(e)
					}
					late, e := portable.ParseMessage(lateBuilt.WireBytes(), "udp", "192.0.2.1:5060", "")
					if e != nil {
						t.Fatal(e)
					}
					handled, e = receiveRedisUACProvisional(ctx, store, flow, late, key, input.ExecutorTimer, time.Now().Add(time.Second))
					if e != nil || !handled {
						t.Fatalf("late fork after M: %v", e)
					}
					retained, e := store.Read(ctx, anchor.Identity, oldFinal, firstContext.Dialogs[0], "r:result-1")
					if e != nil {
						t.Fatal(e)
					}
					oldDialog, e := readPrivateDialogRecord(retained.Records[firstContext.Dialogs[0]])
					if e != nil || oldDialog.LocalInvite.FinalResponse != oldFinal || retained.Records[oldFinal] == nil {
						t.Fatal("late fork lost winner final", e)
					}
					marker, e = decodeNativeResult(retained.Records["r:result-1"])
					if e != nil || marker.OperationId != input.Operation || marker.Request != "" || marker.Response != "" {
						t.Fatal("late fork lost scalar marker", e)
					}
				}
			}
			return
		}
		failed := makeResponse(486)
		handled, e = receiveRedisUACProvisional(ctx, store, flow, failed, key, input.ExecutorTimer, time.Now().Add(time.Second))
		if e != nil || !handled {
			t.Fatalf("failure response: %v", e)
		}
		assertFinalSupersedesProvisional()
		failureView, e := store.ObserveContext(ctx, "uac", "incarnation", "r:context", record.InitialProcedures[0], "r:result-1")
		if e != nil {
			t.Fatal(e)
		}
		failureContext, e := decodeNativeContext(failureView.View.Records["r:context"])
		if e != nil || !failureContext.Ended || failureContext.CallRetireAtMs == 0 {
			t.Fatal("failure did not end call", e)
		}
		failureProc, e := readPrivateProcedureRecord(failureView.View.Records[record.InitialProcedures[0]])
		if e != nil || failureProc.AckEmissions != 1 || failureProc.Ack == "" || len(failureProc.Outputs) != 2 || len(failureProc.Events) != 1 {
			t.Fatal("failure ACK/event not staged", e)
		}
		failureResult, e := decodeNativeResult(failureView.View.Records["r:result-1"])
		if e != nil || failureResult.Certainty != pb.OperationResult_ACCEPTED || failureResult.Output != receipt.Output {
			t.Fatal("failure changed original receipt", e)
		}
		writes, e := nativeReadyWrites(ctx, store, workerID, worker.Epoch, 4)
		if e != nil || len(writes) != 2 {
			t.Fatal("initial and ACK outputs not independent", e)
		}
		ackCount := 0
		var ackWrite *pb.FlowWrite
		var ackProof rediscore.OutputInspection
		for _, write := range writes {
			r, e := nativeOutputRef(write)
			if e != nil {
				t.Fatal(e)
			}
			proof, e := store.InspectOutput(ctx, r)
			if e != nil {
				t.Fatal(e)
			}
			metadata, e := decodeNativeOutput(proof.Metadata)
			if e != nil {
				t.Fatal(e)
			}
			if metadata.Method == "ACK" {
				ackCount++
				ackWrite, ackProof = write, proof
				if metadata.Procedure != record.InitialProcedures[0] || proof.MessageField != failureProc.Ack || metadata.Write.Permit.Authorization.AuthorityClass != pb.EffectAuthorization_NATIVE_COMPLETION {
					t.Fatal("incorrect ACK authority")
				}
				oversized := portable.RestoreMessage(bytes.Repeat([]byte{'x'}, storagecodec.MaxSelectedMessageFieldBytes+1), flow.Transport, "", "")
				if _, _, e = nativePrepareExistingPhysicalOutput(metadata, oversized, failureProc.Ack, "r:oversized-output"); e == nil {
					t.Fatal("existing-wire output accepted oversized packet")
				}
			}
		}
		if ackCount != 1 {
			t.Fatal("missing non-2xx ACK")
		}
		handled, e = receiveRedisUACProvisional(ctx, store, flow, failed, key, input.ExecutorTimer, time.Now().Add(time.Second))
		if e != nil || !handled {
			t.Fatalf("pending ACK replay suppression: %v", e)
		}
		pendingView, e := store.ObserveContext(ctx, "uac", "incarnation", record.InitialProcedures[0])
		if e != nil {
			t.Fatal(e)
		}
		pendingProc, e := readPrivateProcedureRecord(pendingView.View.Records[record.InitialProcedures[0]])
		if e != nil || pendingProc.AckEmissions != 1 || len(pendingProc.Outputs) != 2 {
			t.Fatal("pending ACK admitted duplicate", e)
		}
		ackWriter := nativeRedisWriter{store: store}
		if _, e = ackWriter.Reserve(ctx, ackWrite); e != nil {
			t.Fatal(e)
		}
		if e = ackWriter.Complete(ctx, ackWrite, &pb.DeliveryResult{Effect: ackWrite.Effect, Outcome: pb.DeliveryResult_UNKNOWN}); e != nil {
			t.Fatal(e)
		}
		ackRef, e := nativeOutputRef(ackWrite)
		if e != nil {
			t.Fatal(e)
		}
		ackDue := &rediscore.DueContext{Identity: failureView.Identity, Revision: failureView.View.Revision, Assignment: failureView.View.Timer, Member: "out-" + strings.TrimPrefix(ackProof.MetadataField, "r:"), DeadlineMS: ackProof.DeadlineMS, CheckedAtMS: ackProof.DeadlineMS}
		if e = retireRedisUACFailureACK(ctx, store, ackDue, time.Now().Add(time.Second)); e != nil {
			t.Fatalf("uncertain ACK cutoff: %v", e)
		}
		if _, e = client.ZScore(ctx, root+"core:deadlines:uac", ackDue.Member).Result(); !errors.Is(e, redis.Nil) {
			t.Fatalf("uncertain ACK left expired head: %v", e)
		}
		unknownACK, e := store.InspectOutput(ctx, ackRef)
		if e != nil || unknownACK.State != rediscore.OutputUnknown {
			t.Fatal("uncertain proof lost", e)
		}
		ackOutcome := pb.DeliveryResult_WRITTEN
		if ackNotWritten {
			ackOutcome = pb.DeliveryResult_NOT_WRITTEN
		}
		if e = ackWriter.Complete(ctx, ackWrite, &pb.DeliveryResult{Effect: ackWrite.Effect, Outcome: ackOutcome}); e != nil {
			t.Fatal(e)
		}
		lateProof, e := store.InspectOutput(ctx, ackRef)
		if e != nil {
			t.Fatal(e)
		}
		lateMeta, e := decodeNativeOutput(lateProof.Metadata)
		if e != nil {
			t.Fatal(e)
		}
		if e = settleRedisUACFailureACK(ctx, store, ackWrite, &nativeOutputCapture{ref: ackRef, proof: lateProof, metadata: lateMeta}, input.ExecutorTimer, time.Now().Add(time.Second)); e != nil {
			t.Fatal(e)
		}
		lateView, e := store.ObserveContext(ctx, "uac", "incarnation", "r:context")
		if e != nil {
			t.Fatal(e)
		}
		ackDue.Identity, ackDue.Revision, ackDue.Assignment = lateView.Identity, lateView.View.Revision, lateView.View.Timer
		if e = retireRedisUACFailureACK(ctx, store, ackDue, time.Now().Add(time.Second)); e != nil {
			t.Fatal(e)
		}
		if _, e = store.InspectOutput(ctx, ackRef); !errors.Is(e, rediscore.ErrAbsent) {
			t.Fatalf("recovered ACK proof retained: %v", e)
		}
		afterACK, e := store.ObserveContext(ctx, "uac", "incarnation", record.InitialProcedures[0])
		if e != nil {
			t.Fatal(e)
		}
		ackDue.Identity, ackDue.Revision, ackDue.Assignment = afterACK.Identity, afterACK.View.Revision, afterACK.View.Timer
		if e = retireRedisUACFailureACK(ctx, store, ackDue, time.Now().Add(time.Second)); e != nil {
			t.Fatal(e)
		}
		closed, e := store.Read(ctx, afterACK.Identity, record.InitialProcedures[0], ackProof.MetadataField)
		if e != nil {
			t.Fatal(e)
		}
		closedProc, e := readPrivateProcedureRecord(closed.Records[record.InitialProcedures[0]])
		if e != nil || closedProc.Ack != failureProc.Ack || closedProc.AckEmissions != 1 || len(closedProc.Outputs) != 1 || closed.Records[ackProof.MetadataField] != nil {
			t.Fatal("ACK cleanup lost transaction or retained output", e)
		}
		capView, e := store.ObserveContext(ctx, "uac", "incarnation", record.InitialProcedures[0])
		if e != nil {
			t.Fatal(e)
		}
		capProc, e := readPrivateProcedureRecord(capView.View.Records[record.InitialProcedures[0]])
		if e != nil {
			t.Fatal(e)
		}
		capProc.AckEmissions = stateProfileTransmissions
		capData, e := marshalNativeRecord(storagecodec.NativeProcedure, capProc)
		if e != nil {
			t.Fatal(e)
		}
		_, e = store.Commit(ctx, rediscore.Publication{Identity: capView.Identity, Revision: capView.View.Revision, Deadline: time.Now().Add(time.Second), SpendReservation: true, Changes: []rediscore.Change{{Field: record.InitialProcedures[0], Value: capData}}})
		if e != nil {
			t.Fatal(e)
		}
		handled, e = receiveRedisUACProvisional(ctx, store, flow, failed, key, input.ExecutorTimer, time.Now().Add(time.Second))
		if e != nil || !handled {
			t.Fatalf("capped ACK suppression: %v", e)
		}
		capView, e = store.ObserveContext(ctx, "uac", "incarnation", record.InitialProcedures[0])
		if e != nil {
			t.Fatal(e)
		}
		capProc, e = readPrivateProcedureRecord(capView.View.Records[record.InitialProcedures[0]])
		if e != nil || capProc.AckEmissions != stateProfileTransmissions || len(capProc.Outputs) != 1 {
			t.Fatal("ACK cap admitted output", e)
		}
		capProc.AckEmissions = 1
		capData, e = marshalNativeRecord(storagecodec.NativeProcedure, capProc)
		if e != nil {
			t.Fatal(e)
		}
		_, e = store.Commit(ctx, rediscore.Publication{Identity: capView.Identity, Revision: capView.View.Revision, Deadline: time.Now().Add(time.Second), SpendReservation: true, Changes: []rediscore.Change{{Field: record.InitialProcedures[0], Value: capData}}})
		if e != nil {
			t.Fatal(e)
		}
		beforeReplay, e := store.Read(ctx, afterACK.Identity, record.InitialProcedures[0], failureProc.Ack)
		if e != nil {
			t.Fatal(e)
		}
		beforeNative, e := decodeNativeProcedure(beforeReplay.Records[record.InitialProcedures[0]])
		if e != nil {
			t.Fatal(e)
		}
		originalD := beforeNative.Native.Timers[portable.TimerD]
		ackBytes := bytes.Clone(beforeReplay.Records[failureProc.Ack])
		handled, e = receiveRedisUACProvisional(ctx, store, flow, failed, key, input.ExecutorTimer, time.Now().Add(time.Second))
		if e != nil || !handled {
			t.Fatalf("duplicate failure replay: %v", e)
		}
		afterReplay, e := store.Read(ctx, afterACK.Identity, record.InitialProcedures[0], failureProc.Ack)
		if e != nil {
			t.Fatal(e)
		}
		replayedProc, e := decodeNativeProcedure(afterReplay.Records[record.InitialProcedures[0]])
		if e != nil || replayedProc.Metadata.AckEmissions != 2 || len(replayedProc.Metadata.Outputs) != 2 || replayedProc.Native.Timers[portable.TimerD] != originalD || !bytes.Equal(afterReplay.Records[failureProc.Ack], ackBytes) {
			t.Fatal("replay changed ACK or timer", e)
		}
		replayedWrites, e := nativeReadyWrites(ctx, store, workerID, worker.Epoch, 4)
		if e != nil || len(replayedWrites) != 2 {
			t.Fatal("replay output missing", e)
		}
		return
	}
	// A definite failure consumes the original receipt and stages a frozen
	// FAILED event before the physical proof can be released.
	writer := nativeRedisWriter{store: store}
	committed, err := writer.Reserve(ctx, ready[0])
	if err != nil || committed == nil || len(committed.Wire) == 0 {
		t.Fatalf("reserve initial output: %v", err)
	}
	if written {
		if err = writer.Complete(ctx, ready[0], &pb.DeliveryResult{Effect: ready[0].Effect, Outcome: pb.DeliveryResult_WRITTEN}); err != nil {
			t.Fatal(err)
		}
		ref, err := nativeOutputRef(ready[0])
		if err != nil {
			t.Fatal(err)
		}
		proof, err := store.InspectOutput(ctx, ref)
		if err != nil {
			t.Fatal(err)
		}
		output, err := decodeNativeOutput(proof.Metadata)
		if err != nil {
			t.Fatal(err)
		}
		capture := &nativeOutputCapture{ref: ref, proof: proof, metadata: output}
		if err = settleRedisUACInitialOutput(ctx, store, ready[0], capture, input.ExecutorTimer, time.Now().Add(time.Second)); err != nil {
			t.Fatal(err)
		}
		view, err := store.ObserveContext(ctx, "uac", "incarnation", "r:context", "r:result-1")
		if err != nil {
			t.Fatal(err)
		}
		current, err := decodeNativeContext(view.View.Records["r:context"])
		if err != nil || current.Ended {
			t.Fatal("WRITTEN ended call", err)
		}
		result, err := decodeNativeResult(view.View.Records["r:result-1"])
		if err != nil || result.Certainty != pb.OperationResult_ACCEPTED || result.Output != receipt.Output {
			t.Fatal("WRITTEN changed receipt", err)
		}
		proof, err = store.InspectOutput(ctx, ref)
		if err != nil || !proof.Settled || proof.Acknowledged {
			t.Fatal("WRITTEN proof", err)
		}
		capture.proof = proof
		if successCase == 30 || successCase == 31 {
			testRedisInitialUACUDP(t, ctx, store, client, input, capture, successCase == 31)
			return
		}
		recovery, err := store.ObserveContext(ctx, "uac", "incarnation", "r:context")
		if err != nil {
			t.Fatal(err)
		}
		due := &rediscore.DueContext{Identity: recovery.Identity, Revision: recovery.View.Revision, Assignment: recovery.View.Timer, Member: "out-" + strings.TrimPrefix(receipt.Output, "r:"), DeadlineMS: proof.DeadlineMS, CheckedAtMS: proof.DeadlineMS}
		if err = retireRedisUACInitialOutput(ctx, store, due, time.Now().Add(time.Second)); err != nil {
			t.Fatal(err)
		}
		if _, err = store.InspectOutput(ctx, ref); !errors.Is(err, rediscore.ErrAbsent) {
			t.Fatalf("recovered consumer did not acknowledge: %v", err)
		}
		recovery, err = store.ObserveContext(ctx, "uac", "incarnation", "r:context")
		if err != nil {
			t.Fatal(err)
		}
		due.Identity, due.Revision, due.Assignment = recovery.Identity, recovery.View.Revision, recovery.View.Timer
		if err = retireRedisUACInitialOutput(ctx, store, due, time.Now().Add(time.Second)); err != nil {
			t.Fatal(err)
		}
		return
	}
	if err = writer.Complete(ctx, ready[0], &pb.DeliveryResult{Effect: ready[0].Effect, Outcome: pb.DeliveryResult_UNKNOWN}); err != nil {
		t.Fatal(err)
	}
	unknownRef, err := nativeOutputRef(ready[0])
	if err != nil {
		t.Fatal(err)
	}
	unknownProof, err := store.InspectOutput(ctx, unknownRef)
	if err != nil {
		t.Fatal(err)
	}
	unknownOutput, err := decodeNativeOutput(unknownProof.Metadata)
	if err != nil {
		t.Fatal(err)
	}
	if err = settleRedisUACInitialOutput(ctx, store, ready[0], &nativeOutputCapture{ref: unknownRef, proof: unknownProof, metadata: unknownOutput}, input.ExecutorTimer, time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	unknownView, err := store.ObserveContext(ctx, "uac", "incarnation", "r:context", "r:result-1")
	if err != nil {
		t.Fatal(err)
	}
	unknownContext, err := decodeNativeContext(unknownView.View.Records["r:context"])
	if err != nil || unknownContext.Ended {
		t.Fatal("UNKNOWN ended call", err)
	}
	unknownResult, err := decodeNativeResult(unknownView.View.Records["r:result-1"])
	if err != nil || unknownResult.Certainty != pb.OperationResult_ACCEPTED {
		t.Fatal("UNKNOWN changed acceptance", err)
	}
	if timedOut {
		memo, e := selectedmessage.NewMemo(1)
		if e != nil {
			t.Fatal(e)
		}
		tx, e := proc.attach(&nativeSelectedMessages{records: procView.Records, memo: memo, provenance: "admitted"}, true, false, false)
		if e != nil {
			t.Fatal(e)
		}
		b := tx.Timers[portable.TimerB]
		if !b.Active {
			t.Fatal("initial Timer B missing")
		}
		tx, _, e = portable.StepTransaction(tx, portable.Input{Kind: portable.TimerFired, Timer: portable.TimerB, Generation: b.Generation, Now: b.At})
		if e != nil {
			t.Fatal(e)
		}
		if tx.Phase != portable.Terminated {
			t.Fatal("Timer B did not terminate")
		}
		unknownContext.Ended = true
		unknownContext.Revision++
		unknownContext.CallRetireAtMs = b.At + int64((2*time.Minute)/time.Millisecond)
		unknownResult.Certainty, unknownResult.Error = pb.OperationResult_COMPLETED, "timeout"
		contextData, e := encodeNativeContext(unknownContext)
		if e != nil {
			t.Fatal(e)
		}
		procData, e := encodeNativeProcedure(tx, proc.Refs, proc.Metadata)
		if e != nil {
			t.Fatal(e)
		}
		resultData, e := encodeNativeResult(unknownResult)
		if e != nil {
			t.Fatal(e)
		}
		_, e = store.Commit(ctx, rediscore.Publication{Identity: unknownView.Identity, Revision: unknownView.View.Revision, Deadline: time.Now().Add(time.Second), SpendReservation: true, Changes: []rediscore.Change{{Field: "r:context", Value: contextData}, {Field: record.InitialProcedures[0], Value: procData}, {Field: "r:result-1", Value: resultData}}})
		if e != nil {
			t.Fatal(e)
		}
	}
	if preserveCompleted {
		unknownResult.Certainty, unknownResult.Error = pb.OperationResult_COMPLETED, "earlier completion"
		data, e := encodeNativeResult(unknownResult)
		if e != nil {
			t.Fatal(e)
		}
		_, e = store.Commit(ctx, rediscore.Publication{Identity: unknownView.Identity, Revision: unknownView.View.Revision, Deadline: time.Now().Add(time.Second), SpendReservation: true, Changes: []rediscore.Change{{Field: "r:result-1", Value: data}}})
		if e != nil {
			t.Fatal(e)
		}
	}
	if err = writer.Complete(ctx, ready[0], &pb.DeliveryResult{Effect: ready[0].Effect, Outcome: pb.DeliveryResult_NOT_WRITTEN}); err != nil {
		t.Fatal(err)
	}
	outRef, err := nativeOutputRef(ready[0])
	if err != nil {
		t.Fatal(err)
	}
	proof, err := store.InspectOutput(ctx, outRef)
	if err != nil {
		t.Fatal(err)
	}
	output, err := decodeNativeOutput(proof.Metadata)
	if err != nil {
		t.Fatal(err)
	}
	capture := &nativeOutputCapture{ref: outRef, proof: proof, metadata: output}
	if err = settleRedisUACInitialOutput(ctx, store, ready[0], capture, input.ExecutorTimer, time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	updated, err := store.ObserveContext(ctx, "uac", "incarnation", "r:context", "r:result-1", record.InitialProcedures[0])
	if err != nil {
		t.Fatal(err)
	}
	ended, err := decodeNativeContext(updated.View.Records["r:context"])
	if err != nil || !ended.Ended || ended.CallRetireAtMs == 0 {
		t.Fatalf("missing failed call: %v", err)
	}
	finished, err := decodeNativeResult(updated.View.Records["r:result-1"])
	expectedError := "SIP write did not start"
	if preserveCompleted {
		expectedError = "earlier completion"
	}
	if timedOut {
		expectedError = "timeout"
	}
	if err != nil || finished.Certainty != pb.OperationResult_COMPLETED || finished.Error != expectedError || finished.Output != receipt.Output || !preserveCompleted && !timedOut && finished.Request != "" {
		t.Fatalf("failure receipt: %+v %v", finished, err)
	}
	if timedOut {
		if ended.Revision != unknownContext.Revision {
			t.Fatal("late failure changed timeout snapshot")
		}
		members, e := client.ZRange(ctx, root+"core:deadlines:uac", 0, -1).Result()
		if e != nil {
			t.Fatal(e)
		}
		for _, member := range members {
			if strings.HasPrefix(member, "event-") {
				t.Fatal("late failure duplicated lifecycle event")
			}
		}
		proof, e := store.InspectOutput(ctx, outRef)
		if e != nil || !proof.Settled {
			t.Fatal("late timeout proof not settled", e)
		}
		return
	}
	updatedProc, err := decodeNativeProcedure(updated.View.Records[record.InitialProcedures[0]])
	if err != nil || updatedProc.Native.Phase != portable.Terminated {
		t.Fatalf("failure transaction: %v", err)
	}
	members, err = client.ZRange(ctx, root+"core:deadlines:uac", 0, -1).Result()
	if err != nil {
		t.Fatal(err)
	}
	eventField := ""
	for _, member := range members {
		if strings.HasPrefix(member, "event-") {
			eventField = "r:" + member
		}
	}
	if eventField == "" {
		t.Fatal("FAILED event not indexed")
	}
	eventView, err := store.Read(ctx, updated.Identity, eventField)
	if err != nil {
		t.Fatal(err)
	}
	event, err := decodeNativeLifecycle(eventView.Records[eventField])
	if err != nil || event.Kind != pb.DialogEvent_FAILED || event.OperationId != input.Operation || event.Message != "" || !event.Snapshot.Ended {
		t.Fatalf("FAILED event: %+v %v", event, err)
	}
	proof, err = store.InspectOutput(ctx, outRef)
	if err != nil || !proof.Settled || proof.Acknowledged {
		t.Fatalf("failure proof: %+v %v", proof, err)
	}
	capture.proof = proof
	if err = settleRedisUACInitialOutput(ctx, store, ready[0], capture, input.ExecutorTimer, time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	replayed, err := store.ObserveContext(ctx, "uac", "incarnation", "r:context")
	if err != nil {
		t.Fatal(err)
	}
	replayedContext, err := decodeNativeContext(replayed.View.Records["r:context"])
	if err != nil || replayedContext.Revision != ended.Revision {
		t.Fatal("duplicate failure advanced snapshot", err)
	}
	if err = acknowledgeRedisUACInitialOutput(ctx, store, capture, time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err = store.InspectOutput(ctx, outRef); !errors.Is(err, rediscore.ErrAbsent) {
		t.Fatalf("proof not released: %v", err)
	}
	beforeRetire, err := store.ObserveContext(ctx, "uac", "incarnation", "r:context")
	if err != nil {
		t.Fatal(err)
	}
	due := &rediscore.DueContext{Identity: beforeRetire.Identity, Revision: beforeRetire.View.Revision, Assignment: beforeRetire.View.Timer, Member: "out-" + strings.TrimPrefix(receipt.Output, "r:"), DeadlineMS: proof.DeadlineMS, CheckedAtMS: proof.DeadlineMS}
	if err = retireRedisUACInitialOutput(ctx, store, due, time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	afterRetire, err := store.Read(ctx, beforeRetire.Identity, record.InitialProcedures[0], "r:result-1", receipt.Output)
	if err != nil {
		t.Fatal(err)
	}
	retained, err := decodeNativeProcedure(afterRetire.Records[record.InitialProcedures[0]])
	if err != nil || len(retained.Metadata.Outputs) != 0 || retained.Refs.Origin != receipt.Request {
		t.Fatal("initial anchor changed", err)
	}
	closedResult, err := decodeNativeResult(afterRetire.Records["r:result-1"])
	if err != nil || closedResult.Output != "" || closedResult.Certainty != pb.OperationResult_COMPLETED {
		t.Fatal("result backlink not closed", err)
	}
	if afterRetire.Records[receipt.Output] != nil {
		t.Fatal("output metadata retained after consumption")
	}

}
