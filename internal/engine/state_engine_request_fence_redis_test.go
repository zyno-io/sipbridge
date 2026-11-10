package engine

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/emiago/sipgo/sip"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/execution"
	"sipbridge/internal/state"
)

// Pause one real CAS after its immutable input and reduced payload are ready,
// then let a competing disposition commit against the same observed revision.
type requestFencePausedCASStore struct {
	execution.CASStore
	kind    string
	started chan state.Update
	release chan struct{}
	paused  atomic.Bool
	stale   atomic.Int64
	once    sync.Once
}

func (s *requestFencePausedCASStore) CompareAndSwap(ctx context.Context, update state.Update, payload state.Payload, outcome []byte) (state.Receipt, error) {
	if update.Input.Kind == s.kind && s.paused.CompareAndSwap(false, true) {
		s.started <- update
		select {
		case <-s.release:
		case <-ctx.Done():
			return state.Receipt{}, ctx.Err()
		}
	}
	receipt, err := s.CASStore.CompareAndSwap(ctx, update, payload, outcome)
	if errors.Is(err, state.ErrStale) {
		s.stale.Add(1)
	}
	return receipt, err
}

func (s *requestFencePausedCASStore) unpause() { s.once.Do(func() { close(s.release) }) }

func pauseRequestFenceCASWorker(t *testing.T, f nativeOwnerFixture, kind string) *requestFencePausedCASStore {
	t.Helper()
	store := &requestFencePausedCASStore{CASStore: f.engine.store.(*state.Store), kind: kind,
		started: make(chan state.Update, 1), release: make(chan struct{})}
	runtime, err := execution.NewCAS(store, execution.ReducerFunc(f.engine.reduce), 8)
	if err != nil {
		t.Fatal(err)
	}
	f.engine.runtime = runtime
	t.Cleanup(store.unpause)
	return store
}

func requestFenceSubscribe(t *testing.T, sequence int, toTag, branch string) *pb.SipMessage {
	t.Helper()
	message := stateTestRequest("SUBSCRIBE", sequence, toTag, branch)
	wire := strings.Replace(string(message.Wire), "Content-Length:", "Event: message-summary\r\nExpires: 120\r\nContent-Length:", 1)
	observed, err := Observe([]byte(wire), false)
	if err != nil {
		t.Fatal(err)
	}
	return observed
}

func requestFenceSubscriptionFixture(t *testing.T) (nativeOwnerFixture, *pb.ContextRef, *pb.SipMessage) {
	t.Helper()
	f := newNativeOwnerFixture(t)
	message := requestFenceSubscribe(t, 1, "", "fence-subscription")
	ref, err := f.engine.Locate(context.Background(), message)
	if err != nil {
		t.Fatal(err)
	}
	result := f.apply(t, ref, "subscription-initial", stateEngineInput{Kind: "ingest", Application: f.app, ApplicationLeaseTerm: f.term,
		Ingest: &pb.OwnerIngest{Context: ref, Application: f.app, Flow: f.flow, Wire: message.Wire, PrincipalId: "phone", BindingId: "binding"}})
	if result.Error != "" {
		t.Fatal(result.Error)
	}
	_, group := stateTestLoad(t, f.engine, ref)
	var requestID string
	for id := range group.Contexts[ref.Id].Requests {
		requestID = id
	}
	result = f.apply(t, ref, "subscription-accepted", stateEngineInput{Kind: "command", Application: f.app, ApplicationLeaseTerm: f.term,
		Command: &pb.Command{Context: ref, Sequence: 1, Action: &pb.Command_Respond{Respond: &pb.Respond{RequestId: requestID, Status: 202,
			Headers: []*pb.Header{{Name: "Expires", Value: "120"}}, Authority: f.responseAuthority(t, ref, requestID)}}}})
	if result.Error != "" {
		t.Fatal(result.Error)
	}
	_, group = stateTestLoad(t, f.engine, ref)
	c := group.Contexts[ref.Id]
	if c.Subscription == nil || c.Subscription.RemoteSequence != 1 || c.Subscription.Pending != "" {
		t.Fatal("fixture did not retain an accepted initial subscription")
	}
	return f, ref, requestFenceSubscribe(t, 2, c.Snapshot.LocalTag, "fence-subscription-renewal")
}

type requestFenceCallResult struct {
	result *pb.OperationResult
	err    error
}

func TestNativeRequestFenceRedisPendingCASArbitratesBothOrders(t *testing.T) {
	for _, first := range []string{"ingest", "reject"} {
		t.Run(first+"-first", func(t *testing.T) {
			a, ref, renewal := requestFenceSubscriptionFixture(t)
			b := a.secondWorker(t)
			flow := proto.Clone(a.flow).(*pb.FlowRef)
			flow.Ingress, flow.ConnectionId, flow.SourcePort = b.engine.cfg.Process, "renewal-on-b", 51001
			_, initial := stateTestLoad(t, a.engine, ref)
			before, err := json.Marshal(initial.Contexts[ref.Id].Subscription)
			if err != nil {
				t.Fatal(err)
			}
			beforeRequests, err := json.Marshal(initial.Contexts[ref.Id].Requests)
			if err != nil {
				t.Fatal(err)
			}
			paused := pauseRequestFenceCASWorker(t, a, first)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			defer paused.unpause()
			completed := make(chan requestFenceCallResult, 1)
			ingest := func(engine *StateEngine, id string) (*pb.OperationResult, error) {
				group, err := engine.group(ref)
				if err != nil {
					return nil, err
				}
				input := stateEngineInput{Kind: "ingest", Context: ref, Application: a.app, ApplicationLeaseTerm: a.term, Operation: id,
					Ingest: &pb.OwnerIngest{Context: ref, Application: a.app, Flow: flow, Wire: renewal.Wire,
						PrincipalId: "phone", BindingId: "binding", Admission: pb.OwnerIngest_TLS_DIALOG_CONTINUITY}}
				return engine.apply(ctx, group, id, input, time.Now().Add(4*time.Second), false)
			}
			go func() {
				if first == "ingest" {
					result, err := ingest(a.engine, "paused-ingest-"+uuid.NewString())
					completed <- requestFenceCallResult{result: result, err: err}
					return
				}
				completed <- requestFenceCallResult{err: a.engine.Reject(ctx, flow, renewal, 503)}
			}()
			var update state.Update
			select {
			case update = <-paused.started:
			case result := <-completed:
				t.Fatalf("first worker never held an authoritative claim: %v %s", result.err, result.result)
			case <-ctx.Done():
				t.Fatal("first worker did not reach its claim barrier")
			}
			// Keep this CAS's original validated snapshot while the competing
			// mutation commits. Its compare-and-set must then retry against the
			// winning request disposition.
			if update.Record.Ref.Incarnation == "" {
				t.Fatal("paused CAS lost its observed group incarnation")
			}
			if first == "ingest" {
				if err = b.engine.Reject(ctx, flow, renewal, 503); err != nil {
					t.Fatal(err)
				}
			} else {
				result, err := ingest(b.engine, "late-ingest-"+uuid.NewString())
				if err != nil || result.Certainty != pb.OperationResult_COMPLETED || result.Error != "" || result.SipStatus != 0 {
					t.Fatalf("refused late ingest did not return clean native handling: %v %s", err, result)
				}
			}
			paused.unpause()
			select {
			case result := <-completed:
				if result.err != nil || result.result != nil && result.result.Error != "" {
					t.Fatalf("stale worker did not reconcile the winning disposition: %v %s", result.err, result.result)
				}
			case <-ctx.Done():
				t.Fatal("stale worker did not reconcile after release")
			}
			if paused.stale.Load() == 0 {
				t.Fatal("barrier did not exercise a stale real-Redis Commit")
			}
			_, after := stateTestLoad(t, b.engine, ref)
			c := after.Contexts[ref.Id]
			private := 0
			for _, candidate := range after.Contexts {
				if !candidate.Rejection {
					continue
				}
				private++
				for _, tx := range candidate.Transactions {
					if tx.Native.LastResponse == nil {
						t.Fatal("refusal lost its selected native final")
					}
					response, err := tx.Native.LastResponse.Parse()
					final, ok := response.(*sip.Response)
					if err != nil || !ok || final.StatusCode != 503 {
						t.Fatalf("refusal selected a different final: %v %v", response, err)
					}
				}
			}
			if first == "reject" {
				if private != 0 || c.Subscription.RemoteSequence != 2 || c.Subscription.Pending == "" {
					t.Fatal("stale rejection replaced the CAS-committed renewal")
				}
				var renewalTX *stateTransaction
				for _, tx := range c.Transactions {
					if bytes.Equal(tx.Native.Origin.Wire, renewal.Wire) {
						renewalTX = tx
					}
				}
				if renewalTX == nil || !proto.Equal(renewalTX.Flow, flow) || len(renewalTX.AdmittedFlowProofs) != 1 {
					t.Fatal("recovered admission lost its exact replacement-flow proof")
				}
			} else {
				requests, err := json.Marshal(c.Requests)
				if err != nil {
					t.Fatal(err)
				}
				subscription, err := json.Marshal(c.Subscription)
				if err != nil || private != 1 || !bytes.Equal(subscription, before) || !bytes.Equal(requests, beforeRequests) || !proto.Equal(c.Flow, a.flow) {
					t.Fatalf("stale renewal crossed the CAS-committed refusal: private=%d subscription=%+v err=%v", private, c.Subscription, err)
				}
			}
		})
	}
}

func TestNativeRequestFenceRedisRefusedSourceDoesNotCancelAnotherReservation(t *testing.T) {
	a := newNativeOwnerFixture(t)
	b := a.secondWorker(t)
	ctx := context.Background()
	message := stateTestRequest("OPTIONS", 1, "", "allocation-source-isolation")
	ref, err := a.engine.Locate(ctx, message)
	if err != nil {
		t.Fatal(err)
	}
	_, initial := stateTestLoad(t, a.engine, ref)
	reservation := initial.Allocations[ref.Id]
	foreign := proto.Clone(a.flow).(*pb.FlowRef)
	foreign.Ingress, foreign.ConnectionId, foreign.SourcePort = b.engine.cfg.Process, "refused-source-b", 51002
	if err = b.engine.Reject(ctx, foreign, message, 403); err != nil {
		t.Fatal(err)
	}
	_, afterRefusal := stateTestLoad(t, a.engine, ref)
	if afterRefusal.Allocations[ref.Id] != reservation || reservation.WireHash == "" {
		t.Fatal("refused source deleted or extended another source's reservation")
	}
	admitted := a.apply(t, ref, "source-a-admit", stateEngineInput{Kind: "ingest", Application: a.app, ApplicationLeaseTerm: a.term,
		Ingest: &pb.OwnerIngest{Context: ref, Application: a.app, Flow: a.flow, Wire: message.Wire, PrincipalId: "phone", BindingId: "binding"}})
	if admitted.Error != "" {
		t.Fatalf("valid source could not consume its own surviving reservation: %s", admitted)
	}
	_, beforeLate := stateTestLoad(t, a.engine, ref)
	before, err := json.Marshal(beforeLate.Contexts[ref.Id])
	if err != nil {
		t.Fatal(err)
	}
	late := b.apply(t, ref, "source-b-late", stateEngineInput{Kind: "ingest", Application: a.app, ApplicationLeaseTerm: a.term,
		Ingest: &pb.OwnerIngest{Context: ref, Application: a.app, Flow: foreign, Wire: message.Wire, PrincipalId: "phone", BindingId: "binding"}})
	if late.Certainty != pb.OperationResult_COMPLETED || late.Error != "" || late.SipStatus != 0 {
		t.Fatalf("refused source reentered routing instead of stopping cleanly: %s", late)
	}
	_, afterLate := stateTestLoad(t, a.engine, ref)
	after, err := json.Marshal(afterLate.Contexts[ref.Id])
	if err != nil || !bytes.Equal(after, before) || afterLate.Allocations[ref.Id].WireHash != "" {
		t.Fatal("late refused source changed the valid admitted context or recreated its reservation")
	}
}

func TestNativeRequestFenceRedisAdmittedHistorySurvivesMutableCurrentFlow(t *testing.T) {
	a := newNativeOwnerFixture(t)
	b := a.secondWorker(t)
	ctx := context.Background()
	message := stateTestRequest("OPTIONS", 1, "", "historical-admitted-flow")
	ref, err := a.engine.Locate(ctx, message)
	if err != nil {
		t.Fatal(err)
	}
	ingest := func(worker nativeOwnerFixture, id string, flow *pb.FlowRef) {
		result := worker.apply(t, ref, id, stateEngineInput{Kind: "ingest", Application: a.app, ApplicationLeaseTerm: a.term,
			Ingest: &pb.OwnerIngest{Context: ref, Application: a.app, Flow: flow, Wire: message.Wire,
				PrincipalId: "phone", BindingId: "binding", Admission: pb.OwnerIngest_AUTHENTICATED_BINDING}})
		if result.Error != "" {
			t.Fatal(result.Error)
		}
	}
	ingest(a, "history-original", a.flow)
	replacement := proto.Clone(a.flow).(*pb.FlowRef)
	replacement.Ingress, replacement.ConnectionId, replacement.SourcePort = b.engine.cfg.Process, "history-replacement", 51003
	ingest(b, "history-replacement", replacement)
	_, before := stateTestLoad(t, b.engine, ref)
	var key string
	for txKey := range before.Contexts[ref.Id].Transactions {
		key = txKey
	}
	tx := before.Contexts[ref.Id].Transactions[key]
	if !proto.Equal(tx.Flow, replacement) || len(tx.AdmittedFlowProofs) != 2 {
		t.Fatal("authorized replay did not record historical admission before replacing current flow")
	}
	if err = b.engine.Reject(ctx, a.flow, message, 503); err != nil {
		t.Fatal(err)
	}
	_, after := stateTestLoad(t, a.engine, ref)
	if len(after.Contexts) != 1 || !proto.Equal(after.Contexts[ref.Id].Transactions[key].Flow, replacement) {
		t.Fatal("delayed refusal from the original admitted flow created a competing final")
	}
	// An unproved third source still receives its own explicit policy refusal;
	// historical admission cannot become authority for any same-Call-ID source.
	foreign := proto.Clone(replacement).(*pb.FlowRef)
	foreign.ConnectionId, foreign.SourcePort = "history-unapproved", 51004
	if err = a.engine.Reject(ctx, foreign, message, 403); err != nil {
		t.Fatal(err)
	}
	_, refused := stateTestLoad(t, a.engine, ref)
	if len(refused.Contexts) != 2 || !proto.Equal(refused.Contexts[ref.Id].Transactions[key].Flow, replacement) {
		t.Fatal("unapproved source inherited another flow's proof or changed the admitted transaction")
	}
	selected403 := false
	for _, c := range refused.Contexts {
		if !c.Rejection || !proto.Equal(c.Flow, foreign) {
			continue
		}
		for _, tx := range c.Transactions {
			if tx.Native.LastResponse == nil {
				continue
			}
			message, err := tx.Native.LastResponse.Parse()
			if err != nil {
				t.Fatal(err)
			}
			response, ok := message.(*sip.Response)
			selected403 = selected403 || ok && response.StatusCode == 403
		}
	}
	if !selected403 {
		t.Fatal("unapproved source did not retain its explicit 403 final")
	}
}

func TestNativeRequestFenceRedisOccupiedKeyCollisionPreservesOriginal(t *testing.T) {
	f := newNativeOwnerFixture(t)
	ref := f.admit(t)
	_, initial := stateTestLoad(t, f.engine, ref)
	c := initial.Contexts[ref.Id]
	original, err := json.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	for _, changed := range []struct{ name, old, next string }{
		{"sequence", "CSeq: 1 INVITE", "CSeq: 2 INVITE"},
		{"request-uri", "INVITE sip:service@example.test SIP/2.0", "INVITE sip:different@example.test SIP/2.0"},
		{"from-tag", ";tag=remote-tag", ";tag=other-tag"},
		{"payload", "Contact: <sip:alice@phone.example.test:5061;transport=tls>", "Contact: <sip:alice@other.example.test:5061;transport=tls>"},
	} {
		t.Run(changed.name, func(t *testing.T) {
			wire := bytes.Replace(c.Origin.Wire, []byte(changed.old), []byte(changed.next), 1)
			if bytes.Equal(wire, c.Origin.Wire) {
				t.Fatal("collision fixture did not change the intended identity")
			}
			result := f.apply(t, ref, "collision-"+changed.name, stateEngineInput{Kind: "ingest", Application: f.app, ApplicationLeaseTerm: f.term,
				Ingest: &pb.OwnerIngest{Context: ref, Application: f.app, Flow: f.flow, Wire: wire,
					PrincipalId: "phone", BindingId: "binding", Admission: pb.OwnerIngest_AUTHENTICATED_BINDING}})
			if result.Certainty != pb.OperationResult_NOT_ACCEPTED || result.SipStatus != 481 {
				t.Fatalf("occupied server key was not explicitly refused: %s", result)
			}
			_, after := stateTestLoad(t, f.engine, ref)
			encoded, err := json.Marshal(after.Contexts[ref.Id])
			if err != nil || !bytes.Equal(encoded, original) {
				t.Fatalf("collision changed the original transaction, flow, sequence or request index: %v", err)
			}
		})
	}
}
