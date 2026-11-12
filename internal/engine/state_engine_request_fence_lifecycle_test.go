package engine

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
	"google.golang.org/protobuf/proto"
	"sipbridge/internal/control"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/execution"
	executionfixture "sipbridge/internal/execution/fixture"
	"sipbridge/internal/state"
)

func TestRequestFenceRetentionDoesNotSlideOnRepeatedInput(t *testing.T) {
	r, c, tx := advertiseFixture(t, "tls", sip.OPTIONS, "")
	r.deadline = r.now + 2000
	if admitted, err := r.admitRequestFlow(tx, c.Flow, tx.Native.Origin.Wire); err != nil || !admitted {
		t.Fatalf("initial proof: %v / %v", admitted, err)
	}
	first := tx.DispositionUntilMS
	r.deadline += 100000
	if admitted, err := r.admitRequestFlow(tx, c.Flow, tx.Native.Origin.Wire); err != nil || !admitted || tx.DispositionUntilMS != first || len(tx.AdmittedFlowProofs) != 1 {
		t.Fatalf("new RPC deadline slid the same admission proof: %v / %v", admitted, err)
	}
	for index := 1; index < stateRequestFlowProofLimit; index++ {
		flow := proto.Clone(c.Flow).(*pb.FlowRef)
		flow.ConnectionId = fmt.Sprintf("admitted-%d", index)
		r.deadline++
		if admitted, err := r.admitRequestFlow(tx, flow, tx.Native.Origin.Wire); err != nil || !admitted || tx.DispositionUntilMS != r.deadline+120000 {
			t.Fatalf("new admitted flow did not receive its fixed floor: %v / %v", admitted, err)
		}
	}
	fullFloor := tx.DispositionUntilMS
	flow := proto.Clone(c.Flow).(*pb.FlowRef)
	flow.ConnectionId = "over-cap"
	r.deadline += 100000
	if admitted, err := r.admitRequestFlow(tx, flow, tx.Native.Origin.Wire); err != nil || admitted || tx.DispositionUntilMS != fullFloor {
		t.Fatalf("saturated new flow changed admitted proof: %v / %v", admitted, err)
	}
	if admitted, err := r.admitRequestFlow(tx, c.Flow, tx.Native.Origin.Wire); err != nil || !admitted || tx.DispositionUntilMS != fullFloor {
		t.Fatal("saturated proof reuse slid retention or stopped its admitted source")
	}
}

type cancelAfterRecoveredCommit struct {
	executionfixture.Store
	operation string
	cancel    context.CancelFunc
	committed bool
}

func (s *cancelAfterRecoveredCommit) Commit(ctx context.Context, step state.Step, next state.Payload, outcome []byte) (state.Receipt, error) {
	receipt, err := s.Store.Commit(ctx, step, next, outcome)
	if err == nil && step.Input.ID == s.operation {
		s.committed = true
		s.cancel()
	}
	return receipt, err
}

func TestRequestFenceRecoveredCommitCancellationIsNotReadFailure(t *testing.T) {
	for _, path := range []string{"locate", "native-completion"} {
		t.Run(path, func(t *testing.T) {
			f := newNativeOwnerFixture(t)
			f.engine.work = make(chan struct{}, 64)
			message := stateTestRequest("INVITE", 1, "", "recovered-"+path)
			ref, err := f.engine.Locate(context.Background(), message)
			if err != nil {
				t.Fatal(err)
			}
			if path == "native-completion" {
				result := f.apply(t, ref, "admit-original", stateEngineInput{Kind: "ingest", Application: f.app, ApplicationLeaseTerm: f.term,
					Ingest: &pb.OwnerIngest{Context: ref, Application: f.app, Flow: f.flow, Wire: message.Wire}})
				if result.Error != "" {
					t.Fatal(result.Error)
				}
			}
			operation := "recover-before-cancellation"
			old := stateEngineInput{Kind: "ingest", Context: ref, Application: f.app, ApplicationLeaseTerm: f.term, Operation: operation,
				Ingest: &pb.OwnerIngest{Context: ref, Application: f.app, Flow: f.flow, Wire: message.Wire}}
			if path == "native-completion" {
				foreign := proto.Clone(f.flow).(*pb.FlowRef)
				foreign.ConnectionId = "separate-refused-source"
				id, idErr := f.engine.rejectionContextID(foreign, message, 503)
				if idErr != nil {
					t.Fatal(idErr)
				}
				private := proto.Clone(ref).(*pb.ContextRef)
				private.Id = id
				old = stateEngineInput{Kind: "reject", Context: private, Application: f.engine.cfg.Process, Operation: operation, Flow: foreign, Message: message, Status: 503}
			}
			payload, err := marshalRetainedFixture(old)
			if err != nil {
				t.Fatal(err)
			}
			group, err := f.engine.group(ref)
			if err != nil {
				t.Fatal(err)
			}
			now := time.Now().UnixMilli()
			input := state.Input{ID: operation, Kind: old.Kind, Fingerprint: operation, Payload: payload, DeadlineMS: now + 2000, ReceiptUntilMS: now + 122000}
			if old.ApplicationLeaseTerm != "" {
				input.ApplicationLeaseIDs = []string{f.engine.applicationLeaseID(f.app)}
			}
			begun, err := f.engine.store.(*state.Store).Begin(context.Background(), group, "crashed-worker", input)
			if err != nil || begun.Step == nil {
				t.Fatalf("pending native input: %v", err)
			}
			key := f.prefix + "{p0000}:group:" + group.ID
			if err = f.client.HSet(context.Background(), key, "claimUntilMs", "1").Err(); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			store := &cancelAfterRecoveredCommit{Store: f.engine.store.(*state.Store), operation: operation, cancel: cancel}
			f.engine.runtime, err = executionfixture.New(store, execution.ReducerFunc(f.engine.reduce), "recover-and-cancel", 8, f.engine.resumeConnectedOwners)
			if err != nil {
				t.Fatal(err)
			}
			if path == "locate" {
				_, err = f.engine.Locate(ctx, message)
			} else {
				cancellation := stateTestRequest("CANCEL", 1, "", "recovered-"+path)
				_, err = f.engine.TryNativeCompletion(ctx, f.flow, cancellation)
			}
			if !store.committed || !errors.Is(err, state.ErrUnknown) || !errors.Is(err, context.Canceled) || stateReadUnavailable(err) {
				t.Fatalf("committed prior native input became a read-only failure: committed=%v err=%v", store.committed, err)
			}
			record, retained := stateTestLoad(t, f.engine, ref)
			if path == "locate" && (retained.Contexts[ref.Id] == nil || len(retained.Contexts[ref.Id].Transactions) == 0) || path != "locate" && len(record.Payload.Effects) == 0 {
				t.Fatal("fixture did not retain the prior committed SIP work")
			}
		})
	}
}

func TestRequestFenceProtectsBothGCPathsForLateIngestAndReject(t *testing.T) {
	for _, kind := range []string{"ingest", "reject", "admitted-ingest", "admitted-ingest-floor"} {
		t.Run(kind, func(t *testing.T) {
			r, c, tx := advertiseFixture(t, "tls", sip.OPTIONS, "")
			r.deadline = r.now + 2000
			if kind != "ingest" {
				if _, err := r.admitRequestFlow(tx, c.Flow, tx.Native.Origin.Wire); err != nil {
					t.Fatal(err)
				}
			} else {
				c.Rejection, c.Snapshot.Ended = true, true
				tx.DispositionUntilMS = r.deadline + 120000
			}
			if kind != "reject" {
				response, err := portable.BuildResponseWithHeaders(tx.Native.Origin, 403, "Forbidden", nil, c.Tag, "", nil)
				if err != nil {
					t.Fatal(err)
				}
				tx.Native, _, err = portable.StepTransaction(tx.Native, portable.Input{Kind: portable.SendResponse, Now: r.now, Message: response})
				if err != nil {
					t.Fatal(err)
				}
			}
			tx.Native.Phase, tx.Native.Timers = portable.Terminated, nil
			tx.RetireMS = r.now + 120000
			r.now = tx.DispositionUntilMS + 1000 // Outage outlasted both immutable bounds.
			if kind == "admitted-ingest-floor" {
				r.now = tx.RetireMS + 1 // Proof remains retained; native replay permission expired.
				if r.now >= tx.DispositionUntilMS {
					t.Fatal("fixture must separate protocol retirement from proof retention")
				}
			}
			message, err := Observe(tx.Native.Origin.Wire, false)
			if err != nil {
				t.Fatal(err)
			}
			r.in = stateEngineInput{Kind: kind, Context: c.Snapshot.Context, Application: c.Snapshot.Application, Flow: c.Flow, Message: message, Status: 503,
				Ingest: &pb.OwnerIngest{Context: c.Snapshot.Context, Application: c.Snapshot.Application, Flow: c.Flow, Wire: message.Wire}}
			if kind == "admitted-ingest" || kind == "admitted-ingest-floor" {
				r.in.Kind = "ingest"
			}
			if err = r.protectRequestFence(); err != nil || r.requestFence == nil {
				t.Fatalf("lost the input's original opposite disposition: %v", err)
			}
			r.pruneTransactions()
			if c.Transactions[tx.Native.Key] != tx {
				t.Fatal("reduce-start GC erased the current immutable input's proof")
			}
			if kind == "reject" {
				err = r.reject()
			} else {
				err = r.ingest()
			}
			if err != nil || r.result.Certainty != pb.OperationResult_COMPLETED || r.result.Error != "" || r.result.SipStatus != 0 || len(r.payload.Effects) != 0 {
				t.Fatalf("late recovered input created a competing response: %s / %v", r.result, err)
			}
			if _, err = r.finish(); err != nil || c.Transactions[tx.Native.Key] != tx || r.contextRetirementProven(c.Snapshot.Context.Id, c) {
				t.Fatalf("finish GC erased a protected transaction or context: %v", err)
			}
			r.requestFence = nil
			r.now = max(r.now, tx.DispositionUntilMS+1)
			r.pruneTransactions()
			if c.Transactions[tx.Native.Key] != nil {
				t.Fatal("consumed input retained expired proof forever")
			}
		})
	}
}

func TestRequestFenceExpiredInputsCannotStartNewWork(t *testing.T) {
	for _, kind := range []string{"ingest", "reject"} {
		t.Run(kind, func(t *testing.T) {
			r, c, tx := advertiseFixture(t, "tls", sip.SUBSCRIBE, "")
			c.Transactions, c.Requests = make(map[string]*stateTransaction), make(map[string]string)
			c.Subscription = &stateSubscription{Event: "dialog", RemoteSequence: 1, Target: "sip:original@example.test", ExpiresMS: r.now + 60000}
			r.deadline = r.now - 1
			message, err := Observe(tx.Native.Origin.Wire, false)
			if err != nil {
				t.Fatal(err)
			}
			r.in = stateEngineInput{Kind: kind, Context: c.Snapshot.Context, Application: c.Snapshot.Application, Flow: c.Flow, Message: message, Status: 503,
				Ingest: &pb.OwnerIngest{Context: c.Snapshot.Context, Application: c.Snapshot.Application, Flow: c.Flow, Wire: message.Wire}}
			before, _ := marshalRetainedFixture(r.group)
			if kind == "ingest" {
				err = r.ingest()
			} else {
				err = r.reject()
			}
			after, _ := marshalRetainedFixture(r.group)
			if err != nil || string(before) != string(after) || len(r.payload.Effects) != 0 || r.result.Certainty != pb.OperationResult_COMPLETED || r.result.Error != "" || r.result.SipStatus != 0 {
				t.Fatalf("expired %s started new work or selected a fallback: %s / %v", kind, r.result, err)
			}
		})
	}
}

func TestRequestFenceSaturationCannotRecreateFinalThroughReject(t *testing.T) {
	r, c, tx := advertiseFixture(t, "tls", sip.OPTIONS, "")
	r.deadline = r.now + 2000
	for index := 0; index < stateRequestFlowProofLimit; index++ {
		flow := proto.Clone(c.Flow).(*pb.FlowRef)
		flow.ConnectionId = fmt.Sprintf("flow-%d", index)
		if _, err := r.admitRequestFlow(tx, flow, tx.Native.Origin.Wire); err != nil {
			t.Fatal(err)
		}
	}
	unproved := proto.Clone(c.Flow).(*pb.FlowRef)
	unproved.ConnectionId = "unproved-extra-flow"
	message, err := Observe(tx.Native.Origin.Wire, false)
	if err != nil {
		t.Fatal(err)
	}
	r.in = stateEngineInput{Kind: "reject", Context: c.Snapshot.Context, Application: c.Snapshot.Application, Flow: unproved, Message: message, Status: 503}
	before, _ := marshalRetainedFixture(c)
	if err = r.reject(); err != nil || r.result.Certainty != pb.OperationResult_COMPLETED || r.result.Error != "" || r.result.SipStatus != 0 || len(r.payload.Effects) != 0 {
		t.Fatalf("lost saturated handoff result selected another final: %s / %v", r.result, err)
	}
	after, _ := marshalRetainedFixture(c)
	if string(before) != string(after) {
		t.Fatal("saturation admitted the extra flow or altered original SIP state")
	}
}

func TestNativeIngressErrorProvenanceControlsFallback(t *testing.T) {
	for _, source := range []string{"read-only", "possible-recovery", "generic-cancellation"} {
		t.Run(source, func(t *testing.T) {
			err := error(context.Canceled)
			if source == "read-only" {
				err = &stateReadFailure{err}
			} else if source == "possible-recovery" {
				err = errors.Join(state.ErrUnknown, err)
			}
			ingress := &Ingress{stateCompletion: func(context.Context, *pb.FlowRef, *pb.SipMessage) (bool, error) { return true, err }}
			rejected, unavailable := 0, 0
			ingress.routeWithin(context.Background(), &physicalFlow{}, nil, func(int) { rejected++ }, func() { unavailable++ })
			if source == "read-only" && (unavailable != 1 || rejected != 0) || source == "possible-recovery" && (unavailable != 0 || rejected != 0) || source == "generic-cancellation" && (unavailable != 0 || rejected != 1) {
				t.Fatalf("wrong native error provenance: rejected=%d stateless=%d", rejected, unavailable)
			}
		})
	}
}

func TestRequestFenceNoPeersArbitratesBeforeBestEffortOutage(t *testing.T) {
	for _, result := range []error{nil, &stateReadFailure{state.ErrAuthority}, state.ErrUnknown, context.Canceled} {
		t.Run(fmt.Sprint(result), func(t *testing.T) {
			attempts, rejected, unavailable := 0, 0, 0
			ingress := &Ingress{control: &control.Server{}, stateWriter: &StateEngine{}, stateReject: func(context.Context, *pb.FlowRef, *pb.SipMessage, uint32) error {
				attempts++
				return result
			}}
			ingress.routeWithin(context.Background(), &physicalFlow{}, &pb.SipMessage{Method: "SUBSCRIBE"}, func(int) { rejected++ }, func() { unavailable++ })
			wantUnavailable := 0
			if stateReadUnavailable(result) {
				wantUnavailable = 1
			}
			if attempts != 1 || rejected != 0 || unavailable != wantUnavailable {
				t.Fatalf("health hint bypassed arbitration: attempts=%d rejected=%d unavailable=%d", attempts, rejected, unavailable)
			}
		})
	}
}

func TestRequestFenceReadFailureClassification(t *testing.T) {
	for _, failure := range []error{state.ErrAuthority, state.ErrVersion, state.ErrClock, state.ErrCorrupt} {
		if !stateReadUnavailable(classifyStateReadFailure(failure)) {
			t.Fatalf("detected unusable state lost actionable failure: %v", failure)
		}
	}
	for _, failure := range []error{context.Canceled, state.ErrCapacity, state.ErrConflict, state.ErrStale, state.ErrNotFound, state.ErrNotReady, errors.New("programming failure"), errors.Join(state.ErrUnknown, state.ErrCorrupt)} {
		if stateReadUnavailable(classifyStateReadFailure(failure)) {
			t.Fatalf("unrelated error became an outage claim: %v", failure)
		}
	}
}

func TestRequestFenceNativeMetadataIsMandatory(t *testing.T) {
	for _, invalid := range []string{"missing-proof", "missing-floor", "private-missing-floor", "client-floor"} {
		t.Run(invalid, func(t *testing.T) {
			r, c, tx := advertiseFixture(t, "tls", sip.OPTIONS, "")
			r.group.Version = 1
			r.deadline = r.now + 2000
			if _, err := r.admitRequestFlow(tx, c.Flow, tx.Native.Origin.Wire); err != nil {
				t.Fatal(err)
			}
			switch invalid {
			case "missing-proof":
				tx.AdmittedFlowProofs = nil
			case "missing-floor":
				tx.DispositionUntilMS = 0
			case "private-missing-floor":
				c.Rejection = true
				tx.AdmittedFlowProofs, tx.DispositionUntilMS = nil, 0
			case "client-floor":
				tx.Native.Family, tx.AdmittedFlowProofs = portable.NonInviteClient, nil
			}
			encoded, err := marshalRetainedFixture(r.group)
			if err != nil {
				t.Fatal(err)
			}
			record := state.Record{Versions: NativeStateVersions(), Payload: state.Payload{ProtocolState: encoded}}
			if _, err = decodeStateGroup(record); !errors.Is(err, state.ErrCorrupt) {
				t.Fatalf("native disposition corruption was accepted: %v", err)
			}
		})
	}
}

func TestRequestFenceRejectsUnsupportedNativeEngineProfiles(t *testing.T) {
	for _, version := range []state.Versions{{Schema: "1", Reducer: "7", Timers: "1"}, {Schema: "2", Reducer: "8", Timers: "1"}, {Schema: "2", Reducer: "9", Timers: "2"}, {Schema: "2", Reducer: "10", Timers: "1"}, {Schema: "3", Reducer: "9", Timers: "1"}, {Schema: "2", Reducer: "9", Timers: "1"}} {
		t.Run(version.Schema+"/"+version.Reducer+"/"+version.Timers, func(t *testing.T) {
			key := bytes.Repeat([]byte{7}, 32)
			store, err := state.OpenDeferred(state.Config{Redis: state.RedisOptions{Address: "127.0.0.1:1"}, Prefix: "sipstate:profile-test", Namespace: "profile-test", NamespaceKey: key, Generation: "generation", Versions: version, Partitions: 1})
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			engine, err := NewState(context.Background(), Config{Process: &pb.ProcessRef{InstanceId: "profile-test", Epoch: "epoch"}, AdvertiseAddress: "sip.example.test:5061", ContactURI: "sip:sipbridge@sip.example.test:5061;transport=tls", StateIdentityKey: key}, store, &control.Server{})
			if engine != nil || err == nil {
				t.Fatalf("unsupported profile started the native engine: %+v / %v", version, err)
			}
		})
	}
}
