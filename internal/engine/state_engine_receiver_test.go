package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/coder/websocket"
	"github.com/emiago/sipgo/sip/portable"
	"google.golang.org/protobuf/encoding/protojson"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/execution"
	executionfixture "sipbridge/internal/execution/fixture"
	"sipbridge/internal/state"
)

func receiverMode(t *testing.T, f nativeOwnerFixture, cas bool) {
	t.Helper()
	var err error
	if cas {
		f.engine.runtime, err = execution.NewCAS(f.engine.store, execution.ReducerFunc(f.engine.reduce), 8)
	} else {
		f.engine.runtime, err = executionfixture.New(f.engine.store.(*state.Store), execution.ReducerFunc(f.engine.reduce), f.engine.cfg.Process.InstanceId, 8, f.engine.resumeConnectedOwners)
	}
	if err != nil {
		t.Fatal(err)
	}
}

func receiverIntent(t *testing.T, f nativeOwnerFixture, ref *pb.ContextRef, kind string) (state.Effect, stateEngineEffect) {
	t.Helper()
	record, _ := stateTestLoad(t, f.engine, ref)
	for _, effect := range record.Payload.Effects {
		if effect.Kind != kind {
			continue
		}
		var body stateEngineEffect
		if err := json.Unmarshal(effect.Payload, &body); err != nil {
			t.Fatal(err)
		}
		return effect, body
	}
	t.Fatal("receiver intent missing", kind)
	return state.Effect{}, stateEngineEffect{}
}

func receiverAccepted(effect string) stateEngineInput {
	return stateEngineInput{Kind: "effect-result", Effect: effect, Reply: &pb.Envelope{Payload: &pb.Envelope_Result{Result: &pb.OperationResult{Certainty: pb.OperationResult_ACCEPTED}}}}
}

func TestNativeReceiverPendingAndUnknownCrossExecutionModes(t *testing.T) {
	for _, kind := range []string{"present", "event"} {
		for _, casReader := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/CASReader=%t", kind, casReader), func(t *testing.T) {
				writer := newNativeOwnerFixture(t)
				receiverMode(t, writer, !casReader)
				ref := writer.admit(t)
				if kind == "event" {
					_, group := stateTestLoad(t, writer.engine, ref)
					cancel, err := portable.BuildCancel(group.Contexts[ref.Id].Origin)
					if err != nil {
						t.Fatal(err)
					}
					result := writer.apply(t, ref, "cancel", stateEngineInput{Kind: "ingest", Application: writer.app, ApplicationLeaseTerm: writer.term,
						Ingest: &pb.OwnerIngest{Context: ref, Application: writer.app, Flow: writer.flow, Wire: cancel.Wire, Admission: pb.OwnerIngest_EXISTING_FLOW}})
					if result.Error != "" {
						t.Fatal(result.Error)
					}
				}
				effect, body := receiverIntent(t, writer, ref, kind)
				if effect.Status != state.EffectPending || body.Executor != nil {
					t.Fatal("lookup fixture started a receiver")
				}
				failed := stateEngineInput{Kind: "effect-result", Effect: effect.ID, Failure: "lost receiver acknowledgement"}
				if result := writer.apply(t, ref, "lost-ack", failed); result.Error != "" {
					t.Fatal(result.Error)
				}
				unknown, saved := receiverIntent(t, writer, ref, kind)
				if unknown.Status != state.EffectUnknown || saved.Executor != nil {
					t.Fatal("lost ACK acquired executor authority")
				}
				reader := writer.secondWorker(t)
				receiverMode(t, reader, casReader)
				if result := reader.apply(t, ref, "same-receiver-result", receiverAccepted(effect.ID)); result.Error != "" {
					t.Fatal(result.Error)
				}
				record, _ := stateTestLoad(t, reader.engine, ref)
				for _, remaining := range record.Payload.Effects {
					if remaining.ID == effect.ID {
						t.Fatal("accepted receiver intent was not collected")
					}
				}
			})
		}
	}
}

func TestNativeReceiverLegacyStartKeepsOriginalExecutor(t *testing.T) {
	writer := newNativeOwnerFixture(t)
	ref := writer.admit(t)
	effect, _ := receiverIntent(t, writer, ref, "present")
	if result := writer.apply(t, ref, "old-start", stateEngineInput{Kind: "effect-start", Effect: effect.ID}); result.Error != "" {
		t.Fatal(result.Error)
	}
	reader := writer.secondWorker(t)
	receiverMode(t, reader, true)
	input := receiverAccepted(effect.ID)
	input.Executor = reader.engine.cfg.Process
	if result := reader.apply(t, ref, "wrong-worker", input); result.Error == "" {
		t.Fatal("legacy executor proof was discarded")
	}
	input.Executor = writer.engine.cfg.Process
	if result := reader.apply(t, ref, "recorded-worker", input); result.Error != "" {
		t.Fatal(result.Error)
	}
}

func TestNativeReceiverResultRequiresOriginalScopeAndLiveTerm(t *testing.T) {
	for _, change := range []string{"term", "context", "application", "correlation", "lease", "ended-event"} {
		t.Run(change, func(t *testing.T) {
			f := newNativeOwnerFixture(t)
			ref := f.admit(t)
			effect, body := receiverIntent(t, f, ref, "present")
			if change == "lease" {
				f.expire(t, f.app, -1)
				successor, err := f.engine.ApplicationLease(context.Background(), f.app, "")
				if err != nil || successor.Term == f.term {
					t.Fatal("could not fence the old native term", err)
				}
			}
			record, group := stateTestLoad(t, f.engine, ref)
			c := group.Contexts[ref.Id]
			snapshot := body.Envelope.GetRequest().Snapshot
			switch change {
			case "term":
				snapshot.ApplicationLeaseTerm = "different-term"
			case "context":
				snapshot.Context.State.Incarnation = "another-incarnation"
			case "application":
				snapshot.Application.Epoch = "another-owner"
			case "correlation":
				body.Envelope.CorrelationId = "another-effect"
			case "ended-event":
				c.Snapshot.Ended = true
				effect.Kind = "event"
				snapshot.Ended = true
				body.Envelope.Payload = &pb.Envelope_DialogEvent{DialogEvent: &pb.DialogEvent{Snapshot: snapshot, Kind: pb.DialogEvent_ENDED}}
			}
			lease := record.ApplicationLeases[f.engine.applicationLeaseID(f.app)]
			r := stateReduction{engine: f.engine, record: record, group: group, payload: record.Payload, now: lease.CheckedAtMS}
			err := r.receiverResultOwner(effect, body, c)
			if change == "ended-event" {
				if err != nil {
					t.Fatal("terminal cleanup callback was refused", err)
				}
			} else if err == nil {
				t.Fatal("changed receiver authority was accepted")
			}
		})
	}
}

// Exercise the actual start decision, retained representation at RPC time, and
// result construction. A reducer-only test cannot catch losing a just-started
// legacy executor while the dispatcher's earlier local body still contains nil.
func TestNativeReceiverDispatcherStartsOnlyLegacyAndRecordsAcceptance(t *testing.T) {
	for _, cas := range []bool{false, true} {
		t.Run(fmt.Sprintf("CAS=%t", cas), func(t *testing.T) {
			f := newNativeOwnerFixture(t)
			receiverMode(t, f, cas)
			ref := f.admit(t)
			effect, _ := receiverIntent(t, f, ref, "present")
			conn, _, ctx, _ := stateControllerPair(t, f.engine, f.app, &pb.ProcessRef{InstanceId: "draining", Epoch: "other"})
			f.engine.ctx, f.engine.cancel = context.WithCancel(ctx)
			f.engine.work, f.engine.background = make(chan struct{}, 64), make(chan struct{}, 48)
			t.Cleanup(f.engine.Close)
			replyErrors := make(chan error, 1)
			go func() {
				_, wire, err := conn.Read(ctx)
				if err != nil {
					replyErrors <- err
					return
				}
				var request pb.Envelope
				if err = protojson.Unmarshal(wire, &request); err != nil {
					replyErrors <- err
					return
				}
				if request.GetRequest() == nil {
					replyErrors <- errors.New("expected retained presentation")
					return
				}
				group, _ := f.engine.group(ref)
				record, err := f.engine.store.Load(ctx, group)
				if err != nil {
					replyErrors <- err
					return
				}
				found := false
				for _, current := range record.Payload.Effects {
					if current.ID != effect.ID {
						continue
					}
					found = true
					var body stateEngineEffect
					if err = json.Unmarshal(current.Payload, &body); err != nil {
						replyErrors <- err
						return
					}
					if cas && (current.Status != state.EffectPending || body.Executor != nil) {
						replyErrors <- errors.New("CAS persisted a receiver start")
						return
					}
					if !cas && (current.Status != state.EffectDispatching || !sameProcess(body.Executor, f.engine.cfg.Process)) {
						replyErrors <- errors.New("legacy did not retain its executor")
						return
					}
				}
				if !found {
					replyErrors <- errors.New("receiver intent disappeared before its RPC")
					return
				}
				reply := &pb.Envelope{SessionId: request.SessionId, CorrelationId: request.CorrelationId,
					Payload: &pb.Envelope_Result{Result: &pb.OperationResult{Certainty: pb.OperationResult_ACCEPTED}}}
				encoded, err := protojson.Marshal(reply)
				if err == nil {
					err = conn.Write(ctx, websocket.MessageText, encoded)
				}
				replyErrors <- err
			}()
			group, err := f.engine.group(ref)
			if err != nil {
				t.Fatal(err)
			}
			f.engine.dispatchEffectWithin(ctx, group, effect, false)
			if err := <-replyErrors; err != nil {
				t.Fatal(err)
			}
			waitRPCDispatchDrain(t, f.engine)
			record, _ := stateTestLoad(t, f.engine, ref)
			for _, remaining := range record.Payload.Effects {
				if remaining.ID == effect.ID {
					t.Fatal("receiver acknowledgement was rejected after actual dispatch")
				}
			}
		})
	}
}
