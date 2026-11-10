package engine

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/emiago/sipgo/sip"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/state"
)

func TestStateRPCProgressRequiresForwardPersistedEffectState(t *testing.T) {
	for _, sample := range []struct {
		name, before string
		after        state.Effect
		want         bool
	}{
		{"wire-unknown", "wire", state.Effect{Kind: "wire", Status: state.EffectUnknown}, false},
		{"credential-unchanged", "credential", state.Effect{Kind: "credential", Status: state.EffectDispatching}, false},
		{"owner-unchanged", "owner", state.Effect{Kind: "owner", Status: state.EffectDispatching}, false},
		{"permit-to-deliver", "wire", state.Effect{Kind: "deliver", Status: state.EffectAuthorized}, true},
		{"already-reserved", "wire", state.Effect{Kind: "deliver", Status: state.EffectDispatching}, false},
		{"physical-unknown", "wire", state.Effect{Kind: "deliver", Status: state.EffectUnknown}, false},
		{"expired-permit-to-settle", "wire", state.Effect{Kind: "deliver", Status: state.EffectNotWritten}, true},
		{"unacknowledged-settlement", "deliver", state.Effect{Kind: "deliver", Status: state.EffectWritten, Payload: []byte(`{}`)}, false},
		{"acknowledged-settlement", "deliver", state.Effect{Kind: "deliver", Status: state.EffectWritten, Payload: []byte(`{"settled":true}`)}, true},
		{"terminal-policy", "adopt", state.Effect{Kind: "adopt", Status: state.EffectNotWritten}, true},
	} {
		t.Run(sample.name, func(t *testing.T) {
			sample.after.ID = "effect"
			if got := stateRPCProgressed("effect", sample.before, []state.Effect{sample.after}); got != sample.want {
				t.Fatalf("forward-progress classification: got %v, want %v", got, sample.want)
			}
		})
	}
	if !stateRPCProgressed("removed", "event", nil) {
		t.Fatal("collected effect did not release its next-effect chain")
	}
}

// The actual Redis driver and mTLS control socket exercise result replay and
// recursive dispatch; the controller has no product-side retry hooks.
func TestStateRPCRefusalWaitsForDurableRecoveryThenProgresses(t *testing.T) {
	for _, kind := range []string{"wire"} {
		t.Run(kind, func(t *testing.T) {
			engine, _ := historicalStateTestEngine(t)
			r, c, tx := advertiseFixture(t, "tls", sip.OPTIONS, "")
			r.engine, r.now = engine, time.Now().UnixMilli()
			r.group.Version = 1
			r.group.Retired = make(map[string]int64)
			r.payload.Effects = nil
			r.deadline = r.now + 20000
			groupRef, err := engine.store.GroupForCallID(r.record.Ref.CallID)
			if err != nil {
				t.Fatal(err)
			}
			begun, err := engine.store.(*state.Store).Begin(context.Background(), groupRef, "seed", state.Input{ID: "seed", Kind: "locate", Fingerprint: "seed", Create: true, DeadlineMS: r.now + 2000, ReceiptUntilMS: r.now + 120000})
			if err != nil {
				t.Fatal(err)
			}
			r.record = begun.Step.Record
			c.Snapshot.Context = engine.contextRef(r.record.Ref, c.Snapshot.Context.Id)
			c.Flow.Ingress = proto.Clone(engine.cfg.Process).(*pb.ProcessRef)
			c.OwnerProbeAtMS = r.now + 60000
			advertiseCommand(t, r, c, &pb.Command{Sequence: 1, Action: &pb.Command_Respond{Respond: &pb.Respond{RequestId: tx.RequestID, Status: 200}}})
			payload, err := r.finish()
			if err != nil {
				t.Fatal(err)
			}
			if _, err = engine.store.(*state.Store).Commit(context.Background(), *begun.Step, payload, nil); err != nil {
				t.Fatal(err)
			}
			initial := payload.Effects[0]
			var initialBody stateEngineEffect
			if json.Unmarshal(initial.Payload, &initialBody) != nil || initial.Kind != "wire" {
				t.Fatal("fixture effect mismatch")
			}
			var allow atomic.Bool
			var calls, deliveries atomic.Int64
			controllerErrors := make(chan error, 1)
			connection, _, controllerContext, _ := stateControllerPair(t, engine, c.Snapshot.Application, &pb.ProcessRef{InstanceId: "controller-draining", Epoch: "draining"})
			engine.ctx, engine.cancel = context.WithCancel(controllerContext)
			engine.work, engine.background = make(chan struct{}, 64), make(chan struct{}, 48)
			engine.scheduled = make(map[string]bool)
			t.Cleanup(engine.Close)
			go func() {
				for {
					_, wire, err := connection.Read(engine.ctx)
					if err != nil {
						return
					}
					request := new(pb.Envelope)
					if err := protojson.Unmarshal(wire, request); err != nil {
						controllerErrors <- err
						return
					}
					reply := &pb.Envelope{SessionId: request.SessionId, CorrelationId: request.CorrelationId}
					if ping := request.GetPing(); ping != nil {
						reply.Payload = &pb.Envelope_Pong{Pong: ping}
					} else {
						calls.Add(1)
						if !allow.Load() {
							reply.Payload = &pb.Envelope_Result{Result: &pb.OperationResult{Certainty: pb.OperationResult_NOT_ACCEPTED, Error: "fixture authority fenced"}}
						} else if egress := request.GetEgress(); egress != nil {
							reply.Payload = &pb.Envelope_SendPermit{SendPermit: &pb.SendPermit{Authorization: egress.Authorization, Flow: initialBody.Flow,
								DirectoryEpoch: "directory", OrderingProof: "fixture-proof", CompletionScope: "fixture-scope"}}
						} else if request.GetDeliverEffect() != nil {
							deliveries.Add(1)
							reply.Payload = &pb.Envelope_Result{Result: &pb.OperationResult{Certainty: pb.OperationResult_ACCEPTED}}
						} else {
							controllerErrors <- errors.New("unexpected recovery RPC")
							return
						}
					}
					encoded, err := protojson.Marshal(reply)
					if err == nil {
						err = connection.Write(engine.ctx, websocket.MessageText, encoded)
					}
					if err != nil {
						controllerErrors <- err
						return
					}
				}
			}()
			engine.dispatch(r.record.Ref)
			waitRPCDispatchDrain(t, engine)
			if count := calls.Load(); count != 1 {
				t.Fatalf("fast refusal recursively repeated RPC: %d calls", count)
			}
			record, err := engine.store.Load(engine.ctx, r.record.Ref)
			if err != nil {
				t.Fatal(err)
			}
			var retained state.Effect
			var recovery state.Timer
			for _, effect := range record.Payload.Effects {
				if effect.ID == initial.ID {
					retained = effect
				}
			}
			for _, timer := range record.Payload.Timers {
				if timer.ID == r.timerID("group", "effects", "recovery", "") {
					recovery = timer
				}
			}
			if retained.ID != initial.ID || retained.Status != state.EffectUnknown || retained.DeadlineMS != initial.DeadlineMS || retained.AttemptID == "" || recovery.DueMS <= time.Now().UnixMilli() {
				t.Fatal("refusal changed identity/deadline or lost its future durable recovery timer")
			}
			var retainedBody stateEngineEffect
			if json.Unmarshal(retained.Payload, &retainedBody) != nil || !proto.Equal(retainedBody.Authorization, initialBody.Authorization) {
				t.Fatal("refusal changed effect authorization")
			}
			// Execute the original durable timer, without changing its generation,
			// effect/attempt or native deadline, after the peer becomes available.
			allow.Store(true)
			timer := time.NewTimer(time.Until(time.UnixMilli(recovery.DueMS)))
			select {
			case <-timer.C:
			case <-engine.ctx.Done():
				timer.Stop()
				t.Fatal("fixture expired before durable recovery")
			}
			engine.tickGroup(engine.ctx, r.record.Ref)
			waitRPCDispatchDrain(t, engine)
			if deliveries.Load() != 1 {
				t.Fatal("successful permit did not promptly chain its exact delivery")
			}
			if calls.Load() != 3 {
				t.Fatalf("timer recovery spun or failed to progress: got %d RPCs, want 3", calls.Load())
			}
			record, err = engine.store.Load(engine.ctx, r.record.Ref)
			if err != nil {
				t.Fatal(err)
			}
			if len(record.Payload.Effects) != 1 || record.Payload.Effects[0].ID != initial.ID || record.Payload.Effects[0].Kind != "deliver" || record.Payload.Effects[0].Status != state.EffectAuthorized || record.Payload.Effects[0].DeadlineMS != initial.DeadlineMS {
				t.Fatal("successful recovery allocated another effect/attempt or changed its deadline")
			}
			select {
			case err := <-controllerErrors:
				t.Fatal(err)
			default:
			}
		})
	}
}

func waitRPCDispatchDrain(t *testing.T, engine *StateEngine) {
	t.Helper()
	done := make(chan struct{})
	go func() { engine.wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("no-progress RPC kept the immediate dispatch chain alive")
	}
}
