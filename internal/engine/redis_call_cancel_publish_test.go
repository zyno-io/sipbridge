package engine

import (
	"bytes"
	"context"
	"errors"
	"net"
	"os"
	"slices"
	"strconv"
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

func TestRedisInitialServerCancelAtomicPair(t *testing.T) {
	address := os.Getenv("SIPBRIDGE_CORE_TEST_REDIS")
	if address == "" {
		t.Skip("isolated Redis fixture required")
	}
	for _, label := range []string{"tls", "udp", "udp-unknown", "udp-obsolete", "udp-raw-late", "udp-both-unknown"} {
		t.Run(label, func(t *testing.T) {
			bothUnknown := label == "udp-both-unknown"
			transport, unresolved := label, label == "udp-unknown" || label == "udp-obsolete" || label == "udp-raw-late" || bothUnknown
			if unresolved {
				transport = "udp"
			}
			ctx := context.Background()
			client := redis.NewClient(&redis.Options{Addr: address, MaxRetries: -1})
			root := "sipstate:test:cancel-publish-" + uuid.NewString() + ":"
			old := &pb.ProcessRef{InstanceId: "original", Epoch: "old"}
			local := &pb.ProcessRef{InstanceId: "replacement", Epoch: "new"}
			oldWorker, err := nativeWorkerReference(old)
			if err != nil {
				t.Fatal(err)
			}
			newWorker, err := nativeWorkerReference(local)
			if err != nil {
				t.Fatal(err)
			}
			cleanupWorkers := []string{oldWorker, newWorker}
			t.Cleanup(func() {
				defer client.Close()
				names := []string{"profile", "capacity", "selectors", "context:call", "deadlines:call", "owner-expiry", "coordinator"}
				for _, worker := range cleanupWorkers {
					names = append(names, "worker:"+worker, "timers:"+worker, "ready:"+worker)
				}
				for _, name := range names {
					if err := client.Del(context.Background(), root+"core:"+name).Err(); err != nil {
						t.Error(err)
					}
				}
			})
			info, err := client.Info(ctx, "server").Result()
			if err != nil {
				t.Fatal(err)
			}
			primary := ""
			for _, line := range strings.Split(info, "\r\n") {
				if value, found := strings.CutPrefix(line, "run_id:"); found {
					primary = value
				}
			}
			profile := []byte{2, 3, 4}
			if err = client.Set(ctx, root+"core:profile", profile, 0).Err(); err != nil {
				t.Fatal(err)
			}
			store, err := rediscore.New(client, rediscore.Config{Root: root, Profile: profile, PrimaryRunID: primary, RecordBudget: 1 << 20})
			if err != nil {
				t.Fatal(err)
			}
			for _, process := range []*pb.ProcessRef{old, local} {
				worker, e := nativeWorkerReference(process)
				if e != nil {
					t.Fatal(e)
				}
				if _, e = store.RenewWorker(ctx, rediscore.WorkerLeaseRequest{Worker: worker, Epoch: process.Epoch, PrimaryRunID: primary, Acquire: true}); e != nil {
					t.Fatal(e)
				}
			}
			oldFlow := &pb.FlowRef{Ingress: old, ConnectionId: "old", SourceIp: "192.0.2.3", SourcePort: 5061, Listener: "sip.example.test:5061", Transport: transport}
			flow := proto.Clone(oldFlow).(*pb.FlowRef)
			if transport == "tls" {
				flow.Ingress, flow.ConnectionId, flow.SourcePort = local, "replacement", 6789
			} else {
				local, newWorker = old, oldWorker
			}
			invite, err := portable.ParseMessage(stateTestRequest("INVITE", 1, "", "cancel-publish").Wire, transport, "192.0.2.3:5061", "")
			if err != nil {
				t.Fatal(err)
			}
			facts, err := invite.Facts()
			if err != nil {
				t.Fatal(err)
			}
			tx, _, err := nativeServerStart(invite, "local", time.Now().UnixMilli())
			if err != nil {
				t.Fatal(err)
			}
			// Exercise the real indexed H/I/J transitions without a 32-second
			// fixture. These are supported SDK timer values, local to this test.
			tx.Profile.T1, tx.Profile.T4 = 125, 25
			requestID := uuid.NewString()
			procedureField, originField := "r:procedure-"+requestID, "r:origin-"+requestID
			ref := &pb.ContextRef{Id: "call", Owner: old, State: &pb.StateRef{Authority: &pb.StateAuthority{Service: "test", Generation: "one", PrimaryId: "primary", SchemaVersion: 1}, GroupId: "call", Incarnation: "inc", CallId: facts.CallID}}
			current := &nativepb.Context{Context: ref, Kind: pb.ContextSnapshot_UAS, CallId: facts.CallID, Transport: transport, Flow: oldFlow, InitialProcedures: []string{procedureField}, Application: &pb.ProcessRef{InstanceId: "absent-controller", Epoch: "old"}}
			contextData, err := encodeNativeContext(current)
			if err != nil {
				t.Fatal(err)
			}
			procedureData, err := encodeNativeProcedure(tx, nativeProcedureRefs{Origin: originField}, &nativepb.Procedure{RequestId: requestID, Direct: true, DecisionPending: true, Flow: oldFlow, CreditContribution: 128 << 10})
			if err != nil {
				t.Fatal(err)
			}
			originData, err := selectedmessage.Marshal(invite)
			if err != nil {
				t.Fatal(err)
			}
			identity := rediscore.Identity{Context: "call", Incarnation: "inc"}
			assignment := rediscore.TimerAssignment{Worker: newWorker, Epoch: local.Epoch, Term: local.Epoch}
			_, err = store.Commit(ctx, rediscore.Publication{Identity: identity, Create: true, Deadline: time.Now().Add(time.Second), CreditDelta: 128 << 10,
				Changes:   []rediscore.Change{{Field: "r:context", Value: contextData}, {Field: procedureField, Value: procedureData}, {Field: originField, Value: originData}},
				Selectors: []rediscore.SelectorChange{{Selector: nativeSelector(rediscore.SelectorServerTransaction, tx.Key), Record: procedureField}},
				Timers:    &rediscore.TimerMutation{Target: assignment, Changes: []rediscore.TimerChange{{Member: "call-tx-" + requestID + "-" + string(portable.TimerTrying), Due: time.UnixMilli(tx.Timers[portable.TimerTrying].At)}, {Member: "call-decision-" + requestID, Due: time.Now().Add(time.Second)}}}})
			if err != nil {
				t.Fatal(err)
			}
			cancel, err := portable.BuildCancel(invite)
			if err != nil {
				t.Fatal(err)
			}
			cancel = portable.RestoreMessage(cancel.WireBytes(), transport, net.JoinHostPort(flow.SourceIp, strconv.Itoa(int(flow.SourcePort))), "")
			key := bytes.Repeat([]byte{7}, 32)
			selected, handled, err := selectRedisServerCancel(ctx, store, local, flow, cancel, key, time.Now().Add(time.Second))
			if err != nil || !handled || selected == nil {
				t.Fatal("select first CANCEL", handled, err)
			}
			// A lost race cannot expose only one side of the pair.
			_, err = store.Commit(ctx, rediscore.Publication{Identity: identity, Revision: selected.selected.View.Revision, Deadline: time.Now().Add(time.Second), Changes: []rediscore.Change{{Field: "r:context", Value: contextData}}})
			if err != nil {
				t.Fatal(err)
			}
			if err = publishRedisInitialServerCancel(ctx, store, selected, key, assignment, time.Now().Add(time.Second)); !errors.Is(err, rediscore.ErrConflict) {
				t.Fatal("stale paired publication", err)
			}
			before, err := store.Read(ctx, identity, procedureField, selected.cancelField)
			if err != nil || !bytes.Equal(before.Records[procedureField], procedureData) || len(before.Records[selected.cancelField]) != 0 {
				t.Fatal("stale publication changed a native transaction", err)
			}
			selected, handled, err = selectRedisServerCancel(ctx, store, local, flow, cancel, key, time.Now().Add(time.Second))
			if err != nil || !handled {
				t.Fatal(err)
			}
			if err = publishRedisInitialServerCancel(ctx, store, selected, key, assignment, time.Now().Add(time.Second)); err != nil {
				t.Fatal("publish paired CANCEL", err)
			}
			view, err := store.Read(ctx, identity, "r:context", procedureField, selected.cancelField)
			if err != nil || view.Revision != before.Revision+1 {
				t.Fatal("paired native revision", err)
			}
			after, err := decodeNativeContext(view.Records["r:context"])
			if err != nil || after.CommandSequence != 0 || after.ApplicationLeaseTerm != "" || after.Ended || after.CallRetireAtMs <= 0 || !slices.Contains(after.NoninitialProcedures, selected.cancelField) {
				t.Fatal("unbound CANCEL changed business authority", err)
			}
			nextInvite, err := decodeNativeProcedure(view.Records[procedureField])
			if err != nil || nextInvite.Native.Phase != portable.Completed || nextInvite.Metadata.DecisionPending || len(nextInvite.Metadata.Outputs) != 1 {
				t.Fatal("INVITE side not published", err)
			}
			nextCancel, err := decodeNativeProcedure(view.Records[selected.cancelField])
			if err != nil || len(nextCancel.Metadata.Outputs) != 1 || nextCancel.Native.Key != selected.nextCancel.Key {
				t.Fatal("CANCEL side not published", err)
			}
			assertSuppressed := func(label string) {
				t.Helper()
				before, e := store.Read(ctx, identity)
				if e != nil {
					t.Fatal(e)
				}
				duplicate, handled, e := selectRedisServerCancel(ctx, store, local, flow, cancel, key, time.Now().Add(time.Second))
				if e != nil || !handled || duplicate == nil || duplicate.prior == nil {
					t.Fatal(label, "duplicate selection", e)
				}
				if e = replayRedisServerCancel(ctx, store, duplicate, key, time.Now().Add(time.Second)); e != nil {
					t.Fatal(label, e)
				}
				after, e := store.Read(ctx, identity)
				if e != nil || after.Revision != before.Revision {
					t.Fatal(label, "duplicate response was reissued", e)
				}
			}
			assertSuppressed("PENDING")
			statuses := make(map[uint32]bool)
			writesByStatus := make(map[uint32]*pb.FlowWrite)
			workers := []string{oldWorker}
			if newWorker != oldWorker {
				workers = append(workers, newWorker)
			}
			for _, worker := range workers {
				epoch := old.Epoch
				if worker == newWorker {
					epoch = local.Epoch
				}
				writes, e := nativeReadyWrites(ctx, store, worker, epoch, 2)
				if e != nil {
					t.Fatal(e)
				}
				for _, write := range writes {
					reserved, e := (nativeRedisWriter{store: store}).Reserve(ctx, write)
					if e != nil || reserved.Known {
						t.Fatal("paired response reserve", e)
					}
					response, e := portable.ParseMessage(reserved.Wire, transport, "", "")
					if e != nil {
						t.Fatal(e)
					}
					facts, e := response.Facts()
					if e != nil {
						t.Fatal(e)
					}
					statuses[uint32(facts.Status)] = true
					writesByStatus[uint32(facts.Status)] = write
					if facts.Status == 200 && !proto.Equal(write.Flow, flow) || facts.Status == 487 && !proto.Equal(write.Flow, oldFlow) {
						t.Fatal("paired response lost its observed transaction socket")
					}
				}
			}
			if !statuses[200] || !statuses[487] || len(statuses) != 2 {
				t.Fatal("both native responses were not reservable", statuses)
			}
			reportRedisContextFootprint(t, ctx, client, root+"core:context:call", oldFlow.Transport, "CANCEL and INVITE finals in flight")
			assertSuppressed("STARTED")
			ownWrite, inviteWrite := writesByStatus[200], writesByStatus[487]
			var unknownReplay *pb.FlowWrite
			ownRef, err := nativeOutputRef(ownWrite)
			if err != nil {
				t.Fatal(err)
			}
			inviteRef, err := nativeOutputRef(inviteWrite)
			if err != nil {
				t.Fatal(err)
			}
			if err = acknowledgeRedisCancelOutput(ctx, store, ownWrite, time.Now().Add(time.Second)); err == nil {
				t.Fatal("STARTED response was acknowledged")
			}
			writer := nativeRedisWriter{store: store}
			if err = writer.Complete(ctx, ownWrite, &pb.DeliveryResult{Effect: ownWrite.Effect, Outcome: pb.DeliveryResult_UNKNOWN}); err != nil {
				t.Fatal(err)
			}
			if err = settleRedisCancelOutput(ctx, store, ownWrite, nil, assignment, time.Now().Add(time.Second)); err != nil {
				t.Fatal(err)
			}
			unknown, err := store.InspectOutput(ctx, ownRef)
			if err != nil || unknown.State != rediscore.OutputUnknown || unknown.Acknowledged || unknown.Settled {
				t.Fatal("UNKNOWN CANCEL response was consumed", err)
			}
			assertSuppressed("UNKNOWN")
			selectedOwn, err := store.ObserveContext(ctx, ownRef.Context, ownRef.Incarnation)
			if err != nil {
				t.Fatal(err)
			}
			// Model a prior unknown cutoff removing its head. A late definite
			// completion must put back the original cutoff before settlement.
			_, err = store.Commit(ctx, rediscore.Publication{Identity: selectedOwn.Identity, Revision: selectedOwn.View.Revision, Deadline: time.Now().Add(time.Second),
				Timers: &rediscore.TimerMutation{Expected: selectedOwn.View.Timer, Target: selectedOwn.View.Timer, Changes: []rediscore.TimerChange{{Member: "out-" + unknown.MetadataField[2:], Delete: true}}}})
			if err != nil {
				t.Fatal(err)
			}
			if err = writer.Complete(ctx, ownWrite, &pb.DeliveryResult{Effect: ownWrite.Effect, Outcome: pb.DeliveryResult_WRITTEN}); err != nil {
				t.Fatal(err)
			}
			if err = settleRedisCancelOutput(ctx, store, ownWrite, nil, assignment, time.Now().Add(time.Second)); err != nil {
				t.Fatal(err)
			}
			ownProof, err := store.InspectOutput(ctx, ownRef)
			if err != nil || !ownProof.Settled || ownProof.Acknowledged {
				t.Fatal("definite CANCEL response not settled independently", err)
			}
			assertSuppressed("WRITTEN before consumer ACK")
			score, err := client.ZScore(ctx, root+"core:deadlines:call", "out-"+ownProof.MetadataField[2:]).Result()
			if err != nil || int64(score) != ownProof.DeadlineMS {
				t.Fatal("late completion moved original physical cutoff", err)
			}
			if err = acknowledgeRedisCancelOutput(ctx, store, ownWrite, time.Now().Add(time.Second)); err != nil {
				t.Fatal(err)
			}
			if _, err = store.InspectOutput(ctx, ownRef); !errors.Is(err, rediscore.ErrAbsent) {
				t.Fatal("CANCEL proof not released", err)
			}
			otherProof, err := store.InspectOutput(ctx, inviteRef)
			if err != nil || otherProof.State != rediscore.OutputStarted || otherProof.Settled || otherProof.Acknowledged {
				t.Fatal("CANCEL completion consumed INVITE 487", err)
			}
			if transport == "udp" {
				duplicate, handled, e := selectRedisServerCancel(ctx, store, local, flow, cancel, key, time.Now().Add(time.Second))
				if e != nil || !handled {
					t.Fatal(e)
				}
				if e = replayRedisServerCancel(ctx, store, duplicate, key, time.Now().Add(time.Second)); e != nil {
					t.Fatal("consumed UDP response replay", e)
				}
				replayed, e := store.Read(ctx, identity, selected.cancelField, procedureField)
				if e != nil || !bytes.Equal(replayed.Records[procedureField], view.Records[procedureField]) {
					t.Fatal("CANCEL replay changed the INVITE transaction", e)
				}
				proc, e := decodeNativeProcedure(replayed.Records[selected.cancelField])
				if e != nil || proc.Metadata.OutputConsumed || proc.Metadata.Emissions != 2 || proc.Native.Timers[portable.TimerJ] != nextCancel.Native.Timers[portable.TimerJ] {
					t.Fatal("CANCEL replay reset native lifetime or predecessor proof", e)
				}
				assertSuppressed("replayed PENDING")
				if unresolved {
					ready, e := nativeReadyWrites(ctx, store, newWorker, local.Epoch, 1)
					if e != nil || len(ready) != 1 {
						t.Fatal("missing exact UDP replay", e)
					}
					unknownReplay = ready[0]
					if _, e = writer.Reserve(ctx, unknownReplay); e != nil {
						t.Fatal(e)
					}
					if e = writer.Complete(ctx, unknownReplay, &pb.DeliveryResult{Effect: unknownReplay.Effect, Outcome: pb.DeliveryResult_UNKNOWN}); e != nil {
						t.Fatal(e)
					}
				}
			} else {
				assertSuppressed("retired reliable response")
			}
			// The original INVITE's real non-2xx ACK can arrive before its
			// physical 487 callback. Late proof settlement must remain valid.
			ack, err := portable.BuildNon2xxACK(invite, *selected.nextInvite.LastResponse)
			if err != nil {
				t.Fatal(err)
			}
			ack = portable.RestoreMessage(ack.WireBytes(), transport, net.JoinHostPort(flow.SourceIp, strconv.Itoa(int(flow.SourcePort))), "")
			if handled, e := matchRedisCallCompletion(ctx, store, local, flow, ack, key, assignment, time.Now().Add(time.Second)); e != nil || !handled {
				t.Fatal("ACK before physical INVITE completion", e)
			}
			acknowledged, err := store.Read(ctx, identity, procedureField)
			if err != nil {
				t.Fatal(err)
			}
			inviteOutcome := pb.DeliveryResult_NOT_WRITTEN
			if bothUnknown {
				inviteOutcome = pb.DeliveryResult_UNKNOWN
			}
			if err = writer.Complete(ctx, inviteWrite, &pb.DeliveryResult{Effect: inviteWrite.Effect, Outcome: inviteOutcome}); err != nil {
				t.Fatal(err)
			}
			if err = settleRedisCancelOutput(ctx, store, inviteWrite, nil, assignment, time.Now().Add(time.Second)); err != nil {
				t.Fatal(err)
			}
			if !bothUnknown {
				if err = acknowledgeRedisCancelOutput(ctx, store, inviteWrite, time.Now().Add(time.Second)); err != nil {
					t.Fatal(err)
				}
			}
			completed, err := store.Read(ctx, identity, "r:context", procedureField, selected.cancelField)
			if err != nil || !bytes.Equal(completed.Records["r:context"], view.Records["r:context"]) || !bytes.Equal(completed.Records[procedureField], acknowledged.Records[procedureField]) {
				t.Fatal("physical completion changed prior INVITE or business authority", err)
			}
			consumed, err := readPrivateProcedureRecord(completed.Records[selected.cancelField])
			if err != nil || consumed.OutputConsumed != (transport == "tls") {
				t.Fatal("CANCEL consumption marker missing before proof release", err)
			}
			if transport == "udp" {
				// Replay was checked against its original J above. Model the
				// later elapsed J while preserving its generation and projection.
				jView, e := store.ObserveContext(ctx, identity.Context, identity.Incarnation, selected.cancelField)
				if e != nil {
					t.Fatal(e)
				}
				jProcedure, e := readPrivateProcedureRecord(jView.View.Records[selected.cancelField])
				if e != nil || jProcedure.Native.Timers == nil {
					t.Fatal("CANCEL timer J missing", e)
				}
				j := jProcedure.Native.Timers.Values[string(portable.TimerJ)]
				if j == nil || !j.Active {
					t.Fatal("CANCEL timer J inactive")
				}
				redisNow, e := client.Time(ctx).Result()
				if e != nil {
					t.Fatal(e)
				}
				j.At = redisNow.Add(-time.Millisecond).UnixMilli()
				jBytes, e := marshalNativeRecord(storagecodec.NativeProcedure, jProcedure)
				if e != nil {
					t.Fatal(e)
				}
				jID := strings.TrimPrefix(selected.cancelField, "r:call-cancel-")
				_, e = store.Commit(ctx, rediscore.Publication{Identity: jView.Identity, Revision: jView.View.Revision, Deadline: time.Now().Add(time.Second), Changes: []rediscore.Change{{Field: selected.cancelField, Value: jBytes}}, Timers: &rediscore.TimerMutation{Expected: jView.View.Timer, Target: jView.View.Timer, RequireLease: true, Changes: []rediscore.TimerChange{{Member: "call-cancel-tx-" + jID + "-J", Due: time.UnixMilli(j.At)}}}})
				if e != nil {
					t.Fatal(e)
				}
			}
			for step := 0; step < 8; step++ {
				observed, e := store.ObserveContext(ctx, identity.Context, identity.Incarnation)
				if e != nil {
					t.Fatal(e)
				}
				head, e := store.PeekContextDeadline(ctx, observed.Identity, observed.View.Revision, "")
				if e != nil {
					t.Fatal(e)
				}
				if head == nil || head.Member == "call-retire" {
					break
				}
				if _, e = store.RenewWorker(ctx, rediscore.WorkerLeaseRequest{Worker: newWorker, Epoch: local.Epoch, PrimaryRunID: primary}); e != nil {
					t.Fatal(e)
				}
				if remaining := time.Until(time.UnixMilli(head.DeadlineMS).Add(5 * time.Millisecond)); remaining > 0 {
					time.Sleep(remaining)
				}
				due, e := store.PeekDue(ctx, newWorker, local.Epoch)
				if e != nil || due == nil {
					t.Fatal("indexed native obligation missing", e)
				}
				switch {
				case strings.HasPrefix(due.Member, "call-cancel-tx-"):
					e = fireRedisServerCancelJ(ctx, store, due, time.Now().Add(time.Second))
				case strings.HasPrefix(due.Member, "call-tx-"):
					e = fireRedisInviteServerTimer(ctx, store, due, time.Now().Add(time.Second))
				case strings.HasPrefix(due.Member, "out-call-cancel-"):
					e = dispatchRedisCancelOutputCutoff(ctx, store, due, time.Now().Add(time.Second))
				default:
					t.Fatal("unexpected original duty", due.Member)
				}
				if e != nil {
					t.Fatal("native CANCEL closure", due.Member, e)
				}
			}
			closed, err := store.Read(ctx, identity, "r:context", procedureField, selected.cancelField)
			if err != nil || (len(closed.Records[selected.cancelField]) != 0) != unresolved {
				t.Fatal("CANCEL procedure retained after exact closure", err)
			}
			closedContext, err := decodeNativeContext(closed.Records["r:context"])
			if err != nil || (len(closedContext.NoninitialProcedures) != 0) != unresolved || closedContext.Ended || closedContext.Answered || closedContext.CallRetireAtMs != after.CallRetireAtMs {
				t.Fatal("CANCEL procedure membership retained", err)
			}
			closedInvite, err := readPrivateProcedureRecord(closed.Records[procedureField])
			if err != nil || (len(closedInvite.Outputs) != 0) != bothUnknown || closedInvite.LastResponse == "" {
				t.Fatal("INVITE output closure lost its selected native final", err)
			}
			if _, err = store.LookupSelector(ctx, nativeSelector(rediscore.SelectorServerTransaction, selected.nextCancel.Key)); !unresolved && !errors.Is(err, rediscore.ErrAbsent) || unresolved && err != nil {
				t.Fatal("CANCEL selector retained after exact closure", err)
			}
			// Only the elapsed setup-retention branch is synthetic. All actual
			// preceding ACK/native/physical closures ran through indexed duties.
			closedContext.CallRetireAtMs = time.Now().Add(-time.Millisecond).UnixMilli()
			if label == "udp-obsolete" || label == "udp-raw-late" || bothUnknown {
				closedContext.CallRetireAtMs = time.Now().Add(-10 * time.Second).UnixMilli()
			}
			elapsed, err := encodeNativeContext(closedContext)
			if err != nil {
				t.Fatal(err)
			}
			_, err = store.Commit(ctx, rediscore.Publication{Identity: identity, Revision: closed.Revision, Deadline: time.Now().Add(time.Second),
				Changes: []rediscore.Change{{Field: "r:context", Value: elapsed}}, Timers: &rediscore.TimerMutation{Expected: closed.Timer, Target: closed.Timer, Changes: []rediscore.TimerChange{{Member: "call-retire", Due: time.UnixMilli(closedContext.CallRetireAtMs)}}}})
			if err != nil {
				t.Fatal(err)
			}
			if unresolved {
				due, e := store.PeekDue(ctx, newWorker, local.Epoch)
				if e != nil || due == nil || due.Member != "call-retire" {
					t.Fatal("uncertain setup lost terminal duty", e)
				}
				finished, e := dispatchRedisCallRetirement(ctx, store, due, time.Now().Add(time.Second))
				if e != nil || finished {
					t.Fatal("UNKNOWN setup was retired", e)
				}
				parked, e := store.ObserveContext(ctx, identity.Context, identity.Incarnation, "r:context", selected.cancelField)
				if e != nil {
					t.Fatal(e)
				}
				if head, x := store.PeekContextDeadline(ctx, parked.Identity, parked.View.Revision, ""); x != nil || head == nil || head.Member != "call-retire" || len(parked.View.Records[selected.cancelField]) == 0 {
					t.Fatal("UNKNOWN setup lost its terminal head or owner", x)
				}
				ref, e := nativeOutputRef(unknownReplay)
				if e != nil {
					t.Fatal(e)
				}
				proof, e := store.InspectOutput(ctx, ref)
				if e != nil || proof.State != rediscore.OutputUnknown || proof.Acknowledged || proof.Settled {
					t.Fatal("parked setup lost its exact uncertainty", e)
				}
				if label == "udp-obsolete" || bothUnknown {
					for _, mutation := range []func(*nativepb.Context){
						func(c *nativepb.Context) { c.Answered = true },
						func(c *nativepb.Context) { c.Ended = true },
						func(c *nativepb.Context) { c.CommandSequence = 1 },
						func(c *nativepb.Context) { c.Dialogs = []string{"r:dialog-other"} },
					} {
						ineligible := proto.Clone(closedContext).(*nativepb.Context)
						mutation(ineligible)
						if handled, x := forgetRedisUnboundCancelledSetup(ctx, store, due, ineligible, time.Now().Add(time.Second)); x != nil || handled {
							t.Fatal("ineligible setup accepted", x)
						}
					}
					if _, x := store.InspectOutput(ctx, ref); x != nil {
						t.Fatal("ineligible setup changed proof", x)
					}
					// Move the original physical timestamp only inside this isolated
					// fixture; retain matching metadata and exact proof fingerprint.
					past := time.Now().Add(-rediscore.OutputObsoleteGrace - time.Second).UnixMilli()
					metadataView, x := store.Read(ctx, parked.Identity, proof.MetadataField)
					if x != nil {
						t.Fatal(x)
					}
					metadata, x := decodeNativeOutput(metadataView.Records[proof.MetadataField])
					if x != nil {
						t.Fatal(x)
					}
					metadata.Write.Permit.Authorization.DeadlineMs = uint64(past)
					metadataBytes, x := encodeNativeOutput(metadata)
					if x != nil {
						t.Fatal(x)
					}
					hash, x := nativeFlowWriteHash(metadata.Write)
					if x != nil {
						t.Fatal(x)
					}
					prefix := "o:" + metadata.Write.Effect.Id + ":"
					if x = client.HSet(ctx, root+"core:context:call", proof.MetadataField, metadataBytes, prefix+"p", hash[:], prefix+"d", past).Err(); x != nil {
						t.Fatal(x)
					}
					var inviteObsoleteRef rediscore.OutputRef
					if bothUnknown {
						inviteProof, y := store.InspectOutput(ctx, inviteRef)
						if y != nil || inviteProof.State != rediscore.OutputUnknown || inviteProof.Acknowledged || inviteProof.Settled {
							t.Fatal("initial 487 is not independently UNKNOWN", y)
						}
						inviteView, y := store.Read(ctx, parked.Identity, inviteProof.MetadataField)
						if y != nil {
							t.Fatal(y)
						}
						inviteMetadata, y := decodeNativeOutput(inviteView.Records[inviteProof.MetadataField])
						if y != nil {
							t.Fatal(y)
						}
						invitePast := past - 1000
						inviteMetadata.Write.Permit.Authorization.DeadlineMs = uint64(invitePast)
						inviteBytes, y := encodeNativeOutput(inviteMetadata)
						if y != nil {
							t.Fatal(y)
						}
						inviteHash, y := nativeFlowWriteHash(inviteMetadata.Write)
						if y != nil {
							t.Fatal(y)
						}
						invitePrefix := "o:" + inviteMetadata.Write.Effect.Id + ":"
						if y = client.HSet(ctx, root+"core:context:call", inviteProof.MetadataField, inviteBytes, invitePrefix+"p", inviteHash[:], invitePrefix+"d", invitePast).Err(); y != nil {
							t.Fatal(y)
						}
						inviteObsoleteRef, y = nativeOutputRef(inviteMetadata.Write)
						if y != nil {
							t.Fatal(y)
						}
					}
					_, x = store.Commit(ctx, rediscore.Publication{Identity: parked.Identity, Revision: metadataView.Revision, Deadline: time.Now().Add(time.Second), Timers: &rediscore.TimerMutation{Expected: metadataView.Timer, Target: metadataView.Timer, RequireLease: true, Changes: []rediscore.TimerChange{{Member: "call-retire", Due: time.UnixMilli(closedContext.CallRetireAtMs)}}}})
					if x != nil {
						t.Fatal(x)
					}
					// First normalize the retained head to the later fixed horizon,
					// then forget proof, then reconcile exact absence and retire owner.
					steps := 3
					if bothUnknown {
						steps = 6
					}
					var lostStore *rediscore.Store
					if bothUnknown {
						lostClient := redis.NewClient(&redis.Options{Addr: address, MaxRetries: -1})
						defer lostClient.Close()
						lostClient.AddHook(&loseSetupObsoleteReply{armed: true})
						lostStore, x = rediscore.New(lostClient, rediscore.Config{Root: root, Profile: profile, PrimaryRunID: primary, RecordBudget: 1 << 20})
						if x != nil {
							t.Fatal(x)
						}
					}
					for step := 0; step < steps; step++ {
						duty, x := store.PeekDue(ctx, newWorker, local.Epoch)
						if x != nil || duty == nil || duty.Member != "call-retire" {
							t.Fatal("obsolete setup duty missing", duty, x)
						}
						activeStore := store
						if bothUnknown && step == 1 {
							activeStore = lostStore
						}
						finished, x := dispatchRedisCallRetirement(ctx, activeStore, duty, time.Now().Add(time.Second))
						if bothUnknown && step == 1 {
							if !errors.Is(x, rediscore.ErrUnknown) || finished {
								t.Fatal("lost release reply invented certainty", x)
							}
							if _, y := store.InspectOutput(ctx, inviteObsoleteRef); !errors.Is(y, rediscore.ErrAbsent) {
								t.Fatal("lost reply did not release first proof", y)
							}
							retained, y := store.ObserveContext(ctx, identity.Context, identity.Incarnation, "r:context", procedureField, selected.cancelField)
							if y != nil || len(retained.View.Records[procedureField]) == 0 || len(retained.View.Records[selected.cancelField]) == 0 {
								t.Fatal("lost reply removed native owners", y)
							}
							continue
						}
						if x != nil || finished {
							t.Fatal("obsolete setup reconciliation", step, x)
						}
					}
					newRef, x := nativeOutputRef(metadata.Write)
					if x != nil {
						t.Fatal(x)
					}
					if _, x = store.InspectOutput(ctx, newRef); !errors.Is(x, rediscore.ErrAbsent) {
						t.Fatal("obsolete proof retained", x)
					}
					if bothUnknown {
						if _, x = store.InspectOutput(ctx, inviteObsoleteRef); !errors.Is(x, rediscore.ErrAbsent) {
							t.Fatal("initial obsolete proof retained", x)
						}
					}
				} else {
					if e = writer.Complete(ctx, unknownReplay, &pb.DeliveryResult{Effect: unknownReplay.Effect, Outcome: pb.DeliveryResult_WRITTEN}); e != nil {
						t.Fatal(e)
					}
					if label == "udp-raw-late" {
						rawProof, x := store.InspectOutput(ctx, ref)
						if x != nil || rawProof.State != rediscore.OutputWritten || rawProof.Settled || rawProof.Acknowledged {
							t.Fatal("raw completion did not retain unconsumed W", x)
						}
						// Advance only this fixture's original physical timestamp,
						// retaining the raw W proof without a native continuation.
						past := time.Now().Add(-rediscore.OutputObsoleteGrace - time.Second).UnixMilli()
						metadataView, x := store.Read(ctx, parked.Identity, proof.MetadataField)
						if x != nil {
							t.Fatal(x)
						}
						metadata, x := decodeNativeOutput(metadataView.Records[proof.MetadataField])
						if x != nil {
							t.Fatal(x)
						}
						metadata.Write.Permit.Authorization.DeadlineMs = uint64(past)
						metadataBytes, x := encodeNativeOutput(metadata)
						if x != nil {
							t.Fatal(x)
						}
						hash, x := nativeFlowWriteHash(metadata.Write)
						if x != nil {
							t.Fatal(x)
						}
						prefix := "o:" + metadata.Write.Effect.Id + ":"
						if x = client.HSet(ctx, root+"core:context:call", proof.MetadataField, metadataBytes, prefix+"p", hash[:], prefix+"d", past).Err(); x != nil {
							t.Fatal(x)
						}
						_, x = store.Commit(ctx, rediscore.Publication{Identity: parked.Identity, Revision: metadataView.Revision, Deadline: time.Now().Add(time.Second), Timers: &rediscore.TimerMutation{Expected: metadataView.Timer, Target: metadataView.Timer, RequireLease: true, Changes: []rediscore.TimerChange{{Member: "call-retire", Due: time.UnixMilli(closedContext.CallRetireAtMs)}}}})
						if x != nil {
							t.Fatal(x)
						}
						duty, x := store.PeekDue(ctx, newWorker, local.Epoch)
						if x != nil || duty == nil || duty.Member != "call-retire" {
							t.Fatal("raw W lost retained terminal head", duty, x)
						}
						if finished, x := dispatchRedisCallRetirement(ctx, store, duty, time.Now().Add(time.Second)); x != nil || finished {
							t.Fatal("raw W was not recovered", x)
						}
						newRef, x := nativeOutputRef(metadata.Write)
						if x != nil {
							t.Fatal(x)
						}
						if _, x = store.InspectOutput(ctx, newRef); !errors.Is(x, rediscore.ErrAbsent) {
							t.Fatal("terminal consumer did not release W proof", x)
						}
						next, x := store.PeekDue(ctx, newWorker, local.Epoch)
						if x != nil || next == nil || next.Member != "out-"+proof.MetadataField[2:] || next.DeadlineMS != past {
							t.Fatal("terminal consumer did not re-arm exact physical cutoff", next, x)
						}
					} else {
						if e = settleRedisCancelOutput(ctx, store, unknownReplay, nil, assignment, time.Now().Add(time.Second)); e != nil {
							t.Fatal(e)
						}
						if e = acknowledgeRedisCancelOutput(ctx, store, unknownReplay, time.Now().Add(time.Second)); e != nil {
							t.Fatal(e)
						}
					}
					due, e = store.PeekDue(ctx, newWorker, local.Epoch)
					if e != nil || due == nil || !strings.HasPrefix(due.Member, "out-call-cancel-output-") {
						t.Fatal("late definite setup result failed to re-arm physical cutoff", e)
					}
					if e = dispatchRedisCancelOutputCutoff(ctx, store, due, time.Now().Add(time.Second)); e != nil {
						t.Fatal(e)
					}
				}
			}
			for step := 0; step < 2; step++ {
				due, e := store.PeekDue(ctx, newWorker, local.Epoch)
				if e != nil || due == nil || due.Member != "call-retire" {
					t.Fatal("cancelled setup lost its terminal duty", e)
				}
				finished, e := dispatchRedisCallRetirement(ctx, store, due, time.Now().Add(time.Second))
				if e != nil || finished != (step == 1) {
					t.Fatal("cancelled setup terminal publication", finished, e)
				}
			}
			if _, err = store.ObserveContext(ctx, identity.Context, identity.Incarnation); !errors.Is(err, rediscore.ErrAbsent) {
				t.Fatal("cancelled setup retained its context and pool", err)
			}
			if bothUnknown {
				capacity, e := client.HMGet(ctx, root+"core:capacity", "contexts", "bytes").Result()
				if e != nil || len(capacity) != 2 || capacity[0] != "0" || capacity[1] != "0" {
					t.Fatal("two-U retirement retained namespace credit", capacity, e)
				}
			}
		})
	}
}

// loseSetupObsoleteReply drops only the successful fixed-horizon release reply.
type loseSetupObsoleteReply struct{ armed bool }

func (h *loseSetupObsoleteReply) DialHook(next redis.DialHook) redis.DialHook { return next }
func (h *loseSetupObsoleteReply) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}
func (h *loseSetupObsoleteReply) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		matchHead, matchGrace := false, false
		if h.armed && (cmd.Name() == "eval" || cmd.Name() == "evalsha") {
			for _, arg := range cmd.Args() {
				switch value := arg.(type) {
				case string:
					matchHead = matchHead || value == "call-retire"
					matchGrace = matchGrace || value == "120000"
				case int64:
					matchGrace = matchGrace || value == 120000
				case int:
					matchGrace = matchGrace || value == 120000
				}
			}
		}
		err := next(ctx, cmd)
		if err == nil && matchHead && matchGrace {
			h.armed = false
			return errors.New("fixture dropped obsolete reply")
		}
		return err
	}
}
