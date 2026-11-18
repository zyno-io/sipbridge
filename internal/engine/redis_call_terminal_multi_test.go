package engine

import (
	"bytes"
	"context"
	"errors"
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

// Synthetic elapsed horizons isolate bounded multiowner retirement; the two
// UNKNOWN output proofs themselves are created by the real physical writer.
func TestRedisTerminalReconcilesTwoActualUnknownOutputs(t *testing.T) {
	address := os.Getenv("SIPBRIDGE_CORE_TEST_REDIS")
	if address == "" {
		t.Skip("isolated Redis fixture required")
	}
	ctx := context.Background()
	client := redis.NewClient(&redis.Options{Addr: address, MaxRetries: -1})
	root := "sipstate:test:terminal-multi-" + uuid.NewString() + ":"
	process := &pb.ProcessRef{InstanceId: "worker", Epoch: "epoch"}
	worker, err := nativeWorkerReference(process)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		for _, name := range []string{"profile", "capacity", "selectors", "context:call", "deadlines:call", "worker:" + worker, "timers:" + worker, "ready:" + worker, "owner-expiry", "coordinator"} {
			if e := client.Del(ctx, root+"core:"+name).Err(); e != nil {
				t.Error(e)
			}
		}
		_ = client.Close()
	})
	info, err := client.Info(ctx, "server").Result()
	if err != nil {
		t.Fatal(err)
	}
	primary := ""
	for _, line := range strings.Split(info, "\r\n") {
		if value, ok := strings.CutPrefix(line, "run_id:"); ok {
			primary = value
		}
	}
	profile := []byte{7, 4}
	if err = client.Set(ctx, root+"core:profile", profile, 0).Err(); err != nil {
		t.Fatal(err)
	}
	store, err := rediscore.New(client, rediscore.Config{Root: root, Profile: profile, PrimaryRunID: primary, RecordBudget: 4 << 20})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = store.RenewWorker(ctx, rediscore.WorkerLeaseRequest{Worker: worker, Epoch: process.Epoch, PrimaryRunID: primary, Acquire: true}); err != nil {
		t.Fatal(err)
	}
	now, err := client.Time(ctx).Result()
	if err != nil {
		t.Fatal(err)
	}
	identity := rediscore.Identity{Context: "call", Incarnation: "inc"}
	assignment := rediscore.TimerAssignment{Worker: worker, Epoch: process.Epoch, Term: process.Epoch}
	flow := &pb.FlowRef{Ingress: process, ConnectionId: "socket", SourceIp: "192.0.2.10", SourcePort: 5061, Listener: "edge.test:5061", Transport: "tls"}
	request, err := portable.ParseMessage(stateTestRequest("INVITE", 1, "", "terminal-multi").Wire, "tls", "192.0.2.10:5061", "")
	if err != nil {
		t.Fatal(err)
	}
	facts, err := request.Facts()
	if err != nil {
		t.Fatal(err)
	}
	trying, err := portable.BuildResponseWithHeaders(request, 100, "Trying", nil, "", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	final, err := portable.BuildResponseWithHeaders(request, 486, "Busy Here", nil, "local", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := portable.NewTransaction(portable.InviteServer, request, portable.DefaultTimers(), "local")
	if err != nil {
		t.Fatal(err)
	}
	tx, _, err = portable.StepTransaction(tx, portable.Input{Kind: portable.Start, Now: now.UnixMilli()})
	if err != nil {
		t.Fatal(err)
	}
	tx, _, err = portable.StepTransaction(tx, portable.Input{Kind: portable.SendResponse, Now: now.UnixMilli(), Message: final})
	if err != nil {
		t.Fatal(err)
	}
	tx, _, err = portable.StepTransaction(tx, portable.Input{Kind: portable.Terminate, Now: now.UnixMilli()})
	if err != nil {
		t.Fatal(err)
	}
	requestID := uuid.NewString()
	procField, originField := "r:procedure-"+requestID, "r:origin-"+requestID
	tryingField, tryingOutput := "r:response-"+requestID+"-100", "r:output-"+requestID+"-100"
	finalField, finalOutput := "r:invite-response-"+requestID+"-1", "r:invite-output-"+requestID+"-1"
	contextRef := &pb.ContextRef{Id: "call", State: &pb.StateRef{Incarnation: "inc"}}
	current := &nativepb.Context{Context: contextRef, Kind: pb.ContextSnapshot_UAS, CallId: facts.CallID, Ended: true, CallRetireAtMs: now.Add(-10 * time.Second).UnixMilli(), CommandSequence: 1, InitialProcedures: []string{procField}, Flow: flow}
	contextBytes, err := encodeNativeContext(current)
	if err != nil {
		t.Fatal(err)
	}
	outputs := []string{tryingOutput, finalOutput}
	slices.Sort(outputs)
	procBytes, err := encodeNativeProcedure(tx, nativeProcedureRefs{Origin: originField, LastResponse: finalField}, &nativepb.Procedure{RequestId: requestID, Direct: true, Flow: flow, Outputs: outputs, CreditContribution: 1 << 16})
	if err != nil {
		t.Fatal(err)
	}
	origin, err := selectedmessage.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	result, err := encodeNativeResult(&nativepb.Result{Certainty: pb.OperationResult_ACCEPTED, Sequence: 1, OperationId: "direct", Fingerprint: make([]byte, 32), DecidingLeaseTerm: "term", CutoffMs: current.CallRetireAtMs})
	if err != nil {
		t.Fatal(err)
	}
	changes := []rediscore.Change{{Field: "r:context", Value: contextBytes}, {Field: procField, Value: procBytes}, {Field: originField, Value: origin}, {Field: "r:result-1", Value: result}}
	var intents []rediscore.OutputIntent
	var writes []*pb.FlowWrite
	for i, item := range []struct {
		message     portable.Message
		wire, field string
		status      uint32
	}{{trying, tryingField, tryingOutput, 100}, {final, finalField, finalOutput, 486}} {
		effect := &pb.StateEffectRef{Context: contextRef, Id: "effect-" + strconv.Itoa(i), AttemptId: "attempt-" + strconv.Itoa(i)}
		auth := &pb.EffectAuthorization{Effect: effect, AuthorityClass: pb.EffectAuthorization_RESPONSE, DeadlineMs: uint64(now.Add(time.Minute).UnixMilli())}
		write := &pb.FlowWrite{Effect: effect, Flow: flow, Permit: &pb.SendPermit{Authorization: auth, Flow: flow}}
		output := &nativepb.Output{Write: write, Procedure: procField, Method: "INVITE", Status: item.status}
		intent, records, e := nativePreparePhysicalOutput(output, item.message, item.wire, item.field, nil, nil)
		if e != nil {
			t.Fatal(e)
		}
		intents = append(intents, intent)
		changes = append(changes, records...)
		writes = append(writes, write)
	}
	_, err = store.Commit(ctx, rediscore.Publication{Identity: identity, Create: true, Deadline: time.Now().Add(time.Second), CreditDelta: 1 << 16, Changes: changes, NewOutputs: intents, Selectors: []rediscore.SelectorChange{{Selector: nativeSelector(rediscore.SelectorServerTransaction, tx.Key), Record: procField}}, Timers: &rediscore.TimerMutation{Target: assignment, Changes: []rediscore.TimerChange{{Member: "call-retire", Due: time.UnixMilli(current.CallRetireAtMs)}}}})
	if err != nil {
		t.Fatal(err)
	}
	// An unsupported actual output must block before an EARLY dialog is
	// terminated, even when that output sorts after supported outputs.
	earlyResponse, err := portable.BuildResponseWithHeaders(request, 180, "Ringing", nil, "local", "sip:server@192.0.2.1:5061;transport=tls", nil)
	if err != nil {
		t.Fatal(err)
	}
	early, err := portable.NewDialog(portable.UAS, request, earlyResponse, portable.DefaultTimers())
	if err != nil {
		t.Fatal(err)
	}
	earlyBytes, err := encodeNativeDialog(early, nativeDialogRefs{Remote: &nativeInviteRefs{Request: originField}}, &nativepb.Dialog{Transaction: tx.Key})
	if err != nil {
		t.Fatal(err)
	}
	earlyContext := proto.Clone(current).(*nativepb.Context)
	earlyContext.Dialogs = []string{"r:early-dialog"}
	earlyContextBytes, err := encodeNativeContext(earlyContext)
	if err != nil {
		t.Fatal(err)
	}
	p, err := readPrivateProcedureRecord(procBytes)
	if err != nil {
		t.Fatal(err)
	}
	p.Outputs = append(p.Outputs, "r:unsupported-output")
	slices.Sort(p.Outputs)
	unsupportedProc, err := marshalNativeRecord(storagecodec.NativeProcedure, p)
	if err != nil {
		t.Fatal(err)
	}
	unsupported, err := encodeNativeOutput(&nativepb.Output{Write: writes[0], Procedure: procField, Method: "INVITE", Status: 100})
	if err != nil {
		t.Fatal(err)
	}
	view, err := store.Read(ctx, identity, "r:context")
	if err != nil {
		t.Fatal(err)
	}
	_, err = store.Commit(ctx, rediscore.Publication{Identity: identity, Revision: view.Revision, Deadline: time.Now().Add(time.Second), Changes: []rediscore.Change{{Field: "r:context", Value: earlyContextBytes}, {Field: procField, Value: unsupportedProc}, {Field: "r:early-dialog", Value: earlyBytes}, {Field: "r:unsupported-output", Value: unsupported}}})
	if err != nil {
		t.Fatal(err)
	}
	due, err := store.PeekDue(ctx, worker, process.Epoch)
	if err != nil || due == nil {
		t.Fatal("preflight due missing", err)
	}
	if _, err = dispatchRedisCallRetirement(ctx, store, due, time.Now().Add(time.Second)); err == nil {
		t.Fatal("unsupported output accepted")
	}
	untouched, err := store.Read(ctx, identity, "r:context", procField, "r:early-dialog", "r:unsupported-output")
	if err != nil || untouched.Revision != due.Revision || !bytes.Equal(untouched.Records["r:early-dialog"], earlyBytes) || !bytes.Equal(untouched.Records[procField], unsupportedProc) {
		t.Fatal("preflight mutated early dialog or owner", err)
	}
	_, err = store.Commit(ctx, rediscore.Publication{Identity: identity, Revision: untouched.Revision, Deadline: time.Now().Add(time.Second), Changes: []rediscore.Change{{Field: "r:context", Value: contextBytes}, {Field: procField, Value: procBytes}, {Field: "r:early-dialog", Delete: true}, {Field: "r:unsupported-output", Delete: true}}})
	if err != nil {
		t.Fatal(err)
	}
	writer := nativeRedisWriter{store: store}
	for i, write := range writes {
		if _, err = writer.Reserve(ctx, write); err != nil {
			t.Fatal(err)
		}
		if err = writer.Complete(ctx, write, &pb.DeliveryResult{Effect: write.Effect, Outcome: pb.DeliveryResult_UNKNOWN}); err != nil {
			t.Fatal(err)
		}
		past := now.Add(-rediscore.OutputObsoleteGrace - time.Duration(i+1)*time.Second).UnixMilli()
		write.Permit.Authorization.DeadlineMs = uint64(past)
		selected, e := store.Read(ctx, identity, outputsFieldForStatus(i, tryingOutput, finalOutput))
		if e != nil {
			t.Fatal(e)
		}
		field := outputsFieldForStatus(i, tryingOutput, finalOutput)
		metadata, e := decodeNativeOutput(selected.Records[field])
		if e != nil {
			t.Fatal(e)
		}
		metadata.Write = write
		data, e := encodeNativeOutput(metadata)
		if e != nil {
			t.Fatal(e)
		}
		hash, e := nativeFlowWriteHash(write)
		if e != nil {
			t.Fatal(e)
		}
		prefix := "o:" + write.Effect.Id + ":"
		if e = client.HSet(ctx, root+"core:context:call", field, data, prefix+"p", hash[:], prefix+"d", past).Err(); e != nil {
			t.Fatal(e)
		}
	}
	for stage := 0; stage < 12; stage++ {
		due, e := store.PeekDue(ctx, worker, process.Epoch)
		if e != nil || due == nil {
			t.Fatal("terminal multiowner due missing", stage, e)
		}
		switch {
		case due.Member == "call-retire":
			e = (&redisNative{}).due(ctx, store, due)
		case strings.HasPrefix(due.Member, "out-invite-output-"):
			e = dispatchRedisInitialInviteOutputCutoff(ctx, store, due, time.Now().Add(time.Second))
		case strings.HasPrefix(due.Member, "out-output-"):
			e = dispatchRedisInviteTryingCutoff(ctx, store, due, time.Now().Add(time.Second))
		default:
			t.Fatal("unexpected terminal duty", due.Member)
		}
		if errors.Is(e, rediscore.ErrUnknown) && due.Member == "call-retire" {
			// The exact proof-only release may have committed before its reply
			// was lost. Inspect both attempts and require their native owners
			// and the terminal head to remain before reselecting the next duty.
			witness, witnessErr := store.ObserveContext(ctx, "call", "inc", procField, tryingOutput, finalOutput)
			if witnessErr != nil || len(witness.View.Records[procField]) == 0 {
				t.Fatal("UNKNOWN lost terminal owner", witnessErr)
			}
			owner, ownerErr := readPrivateProcedureRecord(witness.View.Records[procField])
			if ownerErr != nil {
				t.Fatal("UNKNOWN owner decode", ownerErr)
			}
			for i, write := range writes {
				field := outputsFieldForStatus(i, tryingOutput, finalOutput)
				ref, refErr := nativeOutputRef(write)
				if refErr != nil {
					t.Fatal(refErr)
				}
				proof, proofErr := store.InspectOutput(ctx, ref)
				if proofErr == nil && proof.State != rediscore.OutputUnknown || proofErr != nil && !errors.Is(proofErr, rediscore.ErrAbsent) {
					t.Fatal("UNKNOWN produced unexpected proof", proofErr)
				}
				retained := len(witness.View.Records[field]) != 0
				if proofErr == nil && !retained || retained != slices.Contains(owner.Outputs, field) {
					t.Fatal("UNKNOWN lost exact proof ownership")
				}
			}
			if _, headErr := client.ZScore(ctx, root+"core:deadlines:call", "call-retire").Result(); headErr != nil {
				t.Fatal("UNKNOWN lost terminal head", headErr)
			}
			continue
		}
		if e != nil {
			t.Fatal("terminal multiowner stage", stage, due.Member, e)
		}
		if _, e = store.ObserveContext(ctx, "call", "inc"); errors.Is(e, rediscore.ErrAbsent) {
			return
		} else if e != nil {
			t.Fatal(e)
		}
	}
	t.Fatal("terminal multiowner did not close within bounded stages")
}

func outputsFieldForStatus(index int, trying, final string) string {
	if index == 0 {
		return trying
	}
	return final
}
