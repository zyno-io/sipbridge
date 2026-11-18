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
	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/nativepb"
	"sipbridge/internal/rediscore"
)

type redisStandaloneFixture struct {
	ctx    context.Context
	store  *rediscore.Store
	client *redis.Client
	root   string
	in     redisUACQualification
	id     string
}

func newRedisStandaloneFixture(t *testing.T, method, transport string) *redisStandaloneFixture {
	t.Helper()
	address := os.Getenv("SIPBRIDGE_CORE_TEST_REDIS")
	if address == "" {
		t.Skip("isolated Redis fixture required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	t.Cleanup(cancel)
	client := redis.NewClient(&redis.Options{Addr: address, MaxRetries: -1})
	t.Cleanup(func() { _ = client.Close() })
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
	root := "sipstate:test:standalone-" + uuid.NewString() + ":"
	profile := []byte{1, 5, 9}
	if err = client.Set(ctx, root+"core:profile", profile, 0).Err(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		keys, e := client.Keys(context.Background(), root+"*").Result()
		if e != nil {
			t.Error(e)
			return
		}
		if len(keys) > 0 {
			if e = client.Del(context.Background(), keys...).Err(); e != nil {
				t.Error(e)
			}
		}
	})
	store, err := rediscore.New(client, rediscore.Config{Root: root, Profile: profile, PrimaryRunID: primary, RecordBudget: 4 << 20})
	if err != nil {
		t.Fatal(err)
	}
	key := bytes.Repeat([]byte{8}, 32)
	process := &pb.ProcessRef{InstanceId: "worker", Epoch: "epoch"}
	app := &pb.ProcessRef{InstanceId: "controller", Epoch: "epoch"}
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
	authority := &pb.StateAuthority{Service: "test", Generation: "one", PrimaryId: "primary", SchemaVersion: 1}
	ref := &pb.ContextRef{Id: "outbound", Owner: process, State: &pb.StateRef{Authority: authority, GroupId: "outbound", Incarnation: "incarnation", CallId: "standalone@example.test"}}
	flow := &pb.FlowRef{Ingress: process, ConnectionId: "phone", Transport: transport, SourceIp: "192.0.2.12", SourcePort: 5060, Listener: "sip.example.test:5060"}
	deadline := time.Now().Add(3 * time.Second)
	command := &pb.Command{Context: &pb.ContextRef{Id: ref.Id}, Sequence: 1, Admission: &pb.CommandAdmission{DirectoryEpoch: "directory", ExpiresAtMs: uint64(deadline.UnixMilli()), SelectedFlow: flow}, Action: &pb.Command_OpenUac{OpenUac: &pb.OpenUac{Method: method, Uri: "sip:phone@example.test", Transport: transport, Headers: []*pb.Header{{Name: "Event", Value: "check-sync"}}, Body: []byte("fixture")}}}
	in := redisUACQualification{Key: key, Context: ref, Authority: authority, Application: app, Executor: process, Lease: lease.ApplicationLeaseRef, Flow: flow, Command: command, Operation: "open", Advertisement: nativeSIPAdvertisement{contactURI: "sip:sip.example.test:5060", address: "sip.example.test:5060"}, ExecutorTimer: rediscore.TimerAssignment{Worker: worker, Epoch: process.Epoch, Term: process.Epoch}, Credit: uint64(residentSmallBudget), Deadline: deadline}
	return &redisStandaloneFixture{ctx, store, client, root, in, nativeIdentity(key, "uac-initial", authority.PrimaryId, ref.State.Incarnation, ref.Id, "open")[:32]}
}
func (f *redisStandaloneFixture) selected(t *testing.T) selectedRedisTransaction {
	t.Helper()
	s, err := selectRedisTransaction(f.ctx, f.store, f.in.Context.Id, f.in.Context.State.Incarnation, f.id)
	if err != nil {
		t.Fatal(err)
	}
	return s
}
func (f *redisStandaloneFixture) due(t *testing.T) *rediscore.DueContext {
	t.Helper()
	due, err := f.store.PeekDue(f.ctx, f.in.ExecutorTimer.Worker, f.in.Executor.Epoch)
	if err != nil || due == nil {
		t.Fatal("expected actual due head", err)
	}
	return due
}
func (f *redisStandaloneFixture) matureNativeTimer(t *testing.T, timer portable.Timer) {
	t.Helper()
	s := f.selected(t)
	d := s.procedure.Native.Timers[timer]
	if !d.Active {
		t.Fatal("native timer missing", timer)
	}
	now, err := f.client.Time(f.ctx).Result()
	if err != nil {
		t.Fatal(err)
	}
	d.At = now.Add(-time.Millisecond).UnixMilli()
	tx, err := s.attach(f.ctx, f.store, nil)
	if err != nil {
		t.Fatal(err)
	}
	tx.Timers[timer] = d
	value, err := encodeNativeProcedure(tx, s.procedure.Refs, s.procedure.Metadata)
	if err != nil {
		t.Fatal(err)
	}
	member := "transaction-" + f.id + "-" + string(timer)
	if err = redisTransactionCommit(f.ctx, f.store, s, []rediscore.Change{{Field: s.names.procedure, Value: value}}, []rediscore.TimerChange{{Member: member, Due: time.UnixMilli(d.At)}}, f.in.ExecutorTimer, time.Now().Add(time.Second), false, nil); err != nil {
		t.Fatal(err)
	}
	s = f.selected(t)
	due := &rediscore.DueContext{Identity: s.selected.Identity, Revision: s.selected.View.Revision, Assignment: s.selected.View.Timer, Member: member, DeadlineMS: d.At, CheckedAtMS: now.UnixMilli(), Observed: time.Now()}
	if err = fireRedisStandaloneTimer(f.ctx, f.store, due, time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
}
func (f *redisStandaloneFixture) maturePhysical(t *testing.T, grace bool) {
	t.Helper()
	s := f.selected(t)
	now, err := f.client.Time(f.ctx).Result()
	if err != nil {
		t.Fatal(err)
	}
	physical := now.Add(-time.Millisecond).UnixMilli()
	if grace {
		physical = now.Add(-rediscore.OutputObsoleteGrace - time.Second).UnixMilli()
	}
	s.output.Write.Permit.Authorization.DeadlineMs = uint64(physical)
	data, err := encodeNativeOutput(s.output)
	if err != nil {
		t.Fatal(err)
	}
	hash, err := nativeFlowWriteHash(s.output.Write)
	if err != nil {
		t.Fatal(err)
	}
	// Fixture-only clock advancement preserves the immutable attempt fingerprint
	// and physical deadline together; no production clock or reducer is bypassed.
	p := "o:" + s.output.Write.Effect.Id + ":"
	if err = f.client.HSet(f.ctx, f.root+"core:context:outbound", s.names.output, data, p+"p", hash[:], p+"d", physical).Err(); err != nil {
		t.Fatal(err)
	}
	if grace {
		s.result.CutoffMs = now.Add(-time.Second).UnixMilli()
		s.result.Pending = false
	}
	rdata, err := encodeNativeResult(s.result)
	if err != nil {
		t.Fatal(err)
	}
	at := physical
	if grace {
		at += rediscore.OutputObsoleteGrace.Milliseconds()
	}
	if err = redisTransactionCommit(f.ctx, f.store, s, []rediscore.Change{{Field: "r:result-1", Value: rdata}}, []rediscore.TimerChange{{Member: s.names.member, Due: time.UnixMilli(at)}}, f.in.ExecutorTimer, time.Now().Add(time.Second), false, nil); err != nil {
		t.Fatal(err)
	}
}

func TestRedisStandaloneTransactions(t *testing.T) {
	for _, method := range []string{"OPTIONS", "MESSAGE", "NOTIFY", "REGISTER", "PUBLISH"} {
		t.Run(method, func(t *testing.T) {
			f := newRedisStandaloneFixture(t, method, "tls")
			bad := f.in
			bad.Lease.Term = "stale"
			if _, err := admitRedisUACStart(f.ctx, f.store, bad, true); err == nil {
				t.Fatal("stale app admitted")
			}
			bad = f.in
			bad.Command = proto.Clone(f.in.Command).(*pb.Command)
			bad.Command.GetOpenUac().Method = "ACK"
			if _, err := admitRedisUACStart(f.ctx, f.store, bad, true); err == nil {
				t.Fatal("ACK admitted standalone")
			}
			if _, err := admitRedisUACStart(f.ctx, f.store, f.in, true); err != nil {
				t.Fatal(err)
			}
			s := f.selected(t)
			if s.current.Kind != pb.ContextSnapshot_TRANSACTION || !s.result.Pending || len(s.current.Dialogs) != 0 {
				t.Fatal("not a standalone pending transaction")
			}
			if _, err := admitRedisUACStart(f.ctx, f.store, f.in, true); !errors.Is(err, rediscore.ErrConflict) {
				t.Fatal("duplicate create", err)
			}
			ready, err := f.store.PeekReady(f.ctx, f.in.ExecutorTimer.Worker, f.in.Executor.Epoch, 1)
			if err != nil || len(ready) != 1 {
				t.Fatal("ready metadata", err)
			}
			corrupt := proto.Clone(s.output).(*nativepb.Output)
			corrupt.Result = "r:result-2"
			ready[0].Metadata, err = encodeNativeOutput(corrupt)
			if err != nil {
				t.Fatal(err)
			}
			writes := 0
			engine := &redisNative{cfg: Config{Process: f.in.Executor, WriteCommitted: func(context.Context, *pb.FlowWrite) *pb.DeliveryResult { writes++; return nil }}}
			if err = engine.readyOutput(f.ctx, f.store, ready[0]); err == nil || writes != 0 {
				t.Fatal("malformed metadata reached socket", err, writes)
			}
			writer := nativeRedisWriter{store: f.store}
			reserved, err := writer.Reserve(f.ctx, s.output.Write)
			if err != nil || reserved.Known || !bytes.HasPrefix(reserved.Wire, []byte(method+" ")) {
				t.Fatal("exact native request missing", err)
			}
			if err = writer.Complete(f.ctx, s.output.Write, &pb.DeliveryResult{Effect: s.output.Write.Effect, Outcome: pb.DeliveryResult_WRITTEN}); err != nil {
				t.Fatal(err)
			}
			if err = settleNativeOutput(f.ctx, f.store, s.output.Write, f.in.ExecutorTimer); err != nil {
				t.Fatal(err)
			}
			if err = acknowledgeRedisStandaloneOutput(f.ctx, f.store, s.output.Write, time.Now().Add(time.Second)); err != nil {
				t.Fatal(err)
			}
			if !f.selected(t).result.Pending {
				t.Fatal("W prematurely completed command")
			}
			tx, err := f.selected(t).attach(f.ctx, f.store, nil)
			if err != nil {
				t.Fatal(err)
			}
			status := 200
			if method == "REGISTER" {
				status = 401
			}
			if method == "PUBLISH" {
				status = 407
			}
			response, err := portable.ResponseFor(tx.Origin, status, "Fixture", []byte("reply"), "remote")
			if err != nil {
				t.Fatal(err)
			}
			response = portable.RestoreMessage(response.WireBytes(), "tls", "192.0.2.12:5060", "")
			wrong := proto.Clone(f.in.Flow).(*pb.FlowRef)
			wrong.ConnectionId = "replacement"
			if _, err = receiveRedisStandaloneResponse(f.ctx, f.store, wrong, response, f.in.ExecutorTimer, time.Now().Add(time.Second)); !errors.Is(err, rediscore.ErrConflict) {
				t.Fatal("wrong flow accepted", err)
			}
			if handled, e := receiveRedisStandaloneResponse(f.ctx, f.store, f.in.Flow, response, f.in.ExecutorTimer, time.Now().Add(time.Second)); e != nil || !handled {
				t.Fatal("final", e)
			}
			s = f.selected(t)
			if s.result.Pending || s.result.Certainty != pb.OperationResult_COMPLETED || s.result.SipStatus != uint32(status) || s.result.Request != s.names.origin || s.result.Response != s.names.response || !redisStandaloneTerminated(s) {
				t.Fatal("final did not complete native transaction")
			}
			revision := s.selected.View.Revision
			if _, err = receiveRedisStandaloneResponse(f.ctx, f.store, f.in.Flow, response, f.in.ExecutorTimer, time.Now().Add(time.Second)); err != nil {
				t.Fatal(err)
			}
			if f.selected(t).selected.View.Revision != revision {
				t.Fatal("duplicate changed state")
			}
			// Mature only the original waiter cutoff; all protocol/proof work above is real.
			s = f.selected(t)
			checked, e := f.client.Time(f.ctx).Result()
			if e != nil {
				t.Fatal(e)
			}
			s.result.CutoffMs = checked.Add(-time.Second).UnixMilli()
			rdata, e := encodeNativeResult(s.result)
			if e != nil {
				t.Fatal(e)
			}
			if e = redisTransactionCommit(f.ctx, f.store, s, []rediscore.Change{{Field: "r:result-1", Value: rdata}}, []rediscore.TimerChange{{Member: s.names.member, Due: checked.Add(-time.Second)}}, f.in.ExecutorTimer, time.Now().Add(time.Second), false, nil); e != nil {
				t.Fatal(e)
			}
			if e = retireRedisStandaloneOutput(f.ctx, f.store, f.due(t), time.Now().Add(time.Second)); e != nil {
				t.Fatal("terminal", e)
			}
			if _, e = f.store.ObserveContext(f.ctx, "outbound", "incarnation", "r:context"); !errors.Is(e, rediscore.ErrAbsent) {
				t.Fatal("context not retired", e)
			}
		})
	}
	for _, outcome := range []pb.DeliveryResult_Outcome{pb.DeliveryResult_NOT_WRITTEN, pb.DeliveryResult_UNKNOWN} {
		t.Run(outcome.String(), func(t *testing.T) {
			f := newRedisStandaloneFixture(t, "MESSAGE", "udp")
			if _, err := admitRedisUACStart(f.ctx, f.store, f.in, true); err != nil {
				t.Fatal(err)
			}
			s := f.selected(t)
			writer := nativeRedisWriter{store: f.store}
			if _, err := writer.Reserve(f.ctx, s.output.Write); err != nil {
				t.Fatal(err)
			}
			if err := writer.Complete(f.ctx, s.output.Write, &pb.DeliveryResult{Effect: s.output.Write.Effect, Outcome: outcome}); err != nil {
				t.Fatal(err)
			}
			if err := settleNativeOutput(f.ctx, f.store, s.output.Write, f.in.ExecutorTimer); err != nil {
				t.Fatal(err)
			}
			if r := f.selected(t).result; r.Pending || r.Certainty != pb.OperationResult_UNKNOWN {
				t.Fatal("uncertain physical result overstated")
			}
			if outcome == pb.DeliveryResult_NOT_WRITTEN {
				if err := acknowledgeRedisStandaloneOutput(f.ctx, f.store, s.output.Write, time.Now().Add(time.Second)); err != nil {
					t.Fatal(err)
				}
			}
			f.matureNativeTimer(t, portable.TimerF)
			s = f.selected(t)
			if s.result.Certainty != pb.OperationResult_COMPLETED || s.result.Error == "" || !redisStandaloneTerminated(s) {
				t.Fatal("F not truthful completion")
			}
			if outcome == pb.DeliveryResult_UNKNOWN {
				ref, err := nativeOutputRef(s.output.Write)
				if err != nil {
					t.Fatal(err)
				}
				proof, err := f.store.InspectOutput(f.ctx, ref)
				if err != nil || proof.State != rediscore.OutputUnknown || proof.Settled {
					t.Fatal("U lost before horizon", err)
				}
				f.maturePhysical(t, true)
				s = f.selected(t)
				ref, err = nativeOutputRef(s.output.Write)
				if err != nil {
					t.Fatal(err)
				}
				if err = retireRedisStandaloneOutput(f.ctx, f.store, f.due(t), time.Now().Add(time.Second)); err != nil {
					t.Fatal("forget", err)
				}
				if _, err = f.store.InspectOutput(f.ctx, ref); !errors.Is(err, rediscore.ErrAbsent) {
					t.Fatal("proof not released", err)
				}
				if f.selected(t).procedure.Metadata.OutputConsumed {
					t.Fatal("forget invented consumption")
				}
				if err = retireRedisStandaloneOutput(f.ctx, f.store, f.due(t), time.Now().Add(time.Second)); err != nil {
					t.Fatal("absence reconciliation", err)
				}
			}
		})
	}
}

func TestRedisStandaloneLateWriteAndTimerK(t *testing.T) {
	f := newRedisStandaloneFixture(t, "NOTIFY", "udp")
	if _, err := admitRedisUACStart(f.ctx, f.store, f.in, true); err != nil {
		t.Fatal(err)
	}
	s := f.selected(t)
	writer := nativeRedisWriter{store: f.store}
	if _, err := writer.Reserve(f.ctx, s.output.Write); err != nil {
		t.Fatal(err)
	}
	members, err := f.client.ZRange(f.ctx, f.root+"core:deadlines:outbound", 0, -1).Result()
	if err != nil {
		t.Fatal(err)
	}
	for _, member := range members {
		if strings.HasSuffix(member, "-E") {
			t.Fatal("local E persisted")
		}
	}
	// Only the isolated physical deadline/fingerprint are advanced. The production
	// exact ExpireOutput path must convert the actually STARTED attempt into U.
	f.maturePhysical(t, false)
	if err = retireRedisStandaloneOutput(f.ctx, f.store, f.due(t), time.Now().Add(time.Second)); err != nil {
		t.Fatal("physical cutoff", err)
	}
	s = f.selected(t)
	ref, err := nativeOutputRef(s.output.Write)
	if err != nil {
		t.Fatal(err)
	}
	proof, err := f.store.InspectOutput(f.ctx, ref)
	if err != nil || proof.State != rediscore.OutputUnknown || proof.Settled || s.result.Pending {
		t.Fatal("STARTED cutoff overstated evidence", err)
	}
	if err = retireRedisStandaloneOutput(f.ctx, f.store, f.due(t), time.Now().Add(time.Second)); err != nil {
		t.Fatal("grace indexing", err)
	}
	oldCutoff := s.result.CutoffMs
	// A raw late W has not applied native/result work. Normal settlement restores
	// the original physical member before the exact proof can be acknowledged.
	if err = writer.Complete(f.ctx, s.output.Write, &pb.DeliveryResult{Effect: s.output.Write.Effect, Outcome: pb.DeliveryResult_WRITTEN}); err != nil {
		t.Fatal(err)
	}
	if err = settleNativeOutput(f.ctx, f.store, s.output.Write, f.in.ExecutorTimer); err != nil {
		t.Fatal(err)
	}
	due := f.due(t)
	if due.Member != s.names.member || due.DeadlineMS != int64(s.output.Write.Permit.Authorization.DeadlineMs) {
		t.Fatal("late W did not rearm original duty")
	}
	if err = acknowledgeRedisStandaloneOutput(f.ctx, f.store, s.output.Write, time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	s = f.selected(t)
	if s.result.Certainty != pb.OperationResult_UNKNOWN || s.result.CutoffMs != oldCutoff {
		t.Fatal("W invented final or extended waiter")
	}
	tx, err := s.attach(f.ctx, f.store, nil)
	if err != nil {
		t.Fatal(err)
	}
	response, err := portable.ResponseFor(tx.Origin, 200, "OK", nil, "remote")
	if err != nil {
		t.Fatal(err)
	}
	response = portable.RestoreMessage(response.WireBytes(), "udp", "192.0.2.12:5060", "")
	if handled, e := receiveRedisStandaloneResponse(f.ctx, f.store, f.in.Flow, response, f.in.ExecutorTimer, time.Now().Add(time.Second)); e != nil || !handled {
		t.Fatal("late final", e)
	}
	s = f.selected(t)
	if s.result.Certainty != pb.OperationResult_COMPLETED || s.procedure.Native.Phase != portable.Completed || !s.procedure.Native.Timers[portable.TimerK].Active {
		t.Fatal("UDP final skipped K")
	}
	indexedK, err := f.client.ZScore(f.ctx, f.root+"core:deadlines:outbound", "transaction-"+f.id+"-K").Result()
	if err != nil || int64(indexedK) != s.procedure.Native.Timers[portable.TimerK].At {
		t.Fatal("native K index missing", err)
	}
	// The physical cleanup yields to actual K without an expired spinning head.
	if err = retireRedisStandaloneOutput(f.ctx, f.store, f.due(t), time.Now().Add(time.Second)); err != nil {
		t.Fatal("yield physical member", err)
	}
	next, err := f.store.PeekDue(f.ctx, f.in.ExecutorTimer.Worker, f.in.Executor.Epoch)
	if err != nil && !errors.Is(err, rediscore.ErrAbsent) || next != nil && (next.Member != "transaction-"+f.id+"-K" || next.DeadlineMS <= next.CheckedAtMS) {
		t.Fatal("past due physical head remained", next, err)
	}
	f.matureNativeTimer(t, portable.TimerK)
	s = f.selected(t)
	if !redisStandaloneTerminated(s) {
		t.Fatal("K did not terminate")
	}
	s.result.CutoffMs = time.Now().Add(-time.Second).UnixMilli()
	data, err := encodeNativeResult(s.result)
	if err != nil {
		t.Fatal(err)
	}
	if err = redisTransactionCommit(f.ctx, f.store, s, []rediscore.Change{{Field: "r:result-1", Value: data}}, nil, f.in.ExecutorTimer, time.Now().Add(time.Second), false, nil); err != nil {
		t.Fatal(err)
	}
	if err = retireRedisStandaloneOutput(f.ctx, f.store, f.due(t), time.Now().Add(time.Second)); err != nil {
		t.Fatal("K closure", err)
	}
}
