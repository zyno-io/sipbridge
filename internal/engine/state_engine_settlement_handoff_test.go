package engine

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/coder/websocket"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/state"
)

type settlementControl struct {
	connection *websocket.Conn
	requests   chan *pb.Envelope
	errors     chan error
}

// Use the real Redis driver, native successful INVITE/ACK procedure and mTLS
// control channel. No scheduler runs: every immediate RPC is causally visible.
func settlementFixture(t *testing.T, outcome *pb.DeliveryResult_Outcome, mutate func(*stateReduction, *stateContext, *pb.FlowWrite)) (*StateEngine, *stateContext, *pb.FlowWrite, state.Ref, *settlementControl) {
	t.Helper()
	engine, _ := historicalStateTestEngine(t)
	r, c, tx := forkFixture(t)
	r.engine, r.now = engine, time.Now().UnixMilli()
	ref, err := engine.store.GroupForCallID(r.record.Ref.CallID)
	if err != nil {
		t.Fatal(err)
	}
	begun, err := engine.store.(*state.Store).Begin(context.Background(), ref, "seed", state.Input{ID: "seed", Kind: "locate", Fingerprint: "seed", Create: true, DeadlineMS: r.now + 2000, ReceiptUntilMS: r.now + 120000})
	if err != nil {
		t.Fatal(err)
	}
	r.record = begun.Step.Record
	c.Snapshot.Context = engine.contextRef(r.record.Ref, c.Snapshot.Context.Id)
	c.Flow.Ingress = proto.Clone(engine.cfg.Process).(*pb.ProcessRef)
	write := initialUACACKFixture(t, r, c, tx)
	if outcome != nil {
		writeInitialACKFixture(t, r, write)
		if *outcome == pb.DeliveryResult_NOT_WRITTEN {
			// The initial handshake remains positively written; this exact later
			// native replay is known not to have started and still needs settlement.
			index, effect, body, err := r.findEffect(write.Effect.Id)
			if err != nil {
				t.Fatal(err)
			}
			body.Settled = true
			r.setEffect(index, effect, body)
			if err := r.reclaimEffects(); err != nil {
				t.Fatal(err)
			}
			r.in = stateEngineInput{Kind: "ingest"}
			if _, err := r.forkResponse(c, tx, forkReply(t, tx, 200, "winner")); err != nil {
				t.Fatal(err)
			}
			index, effect, body = causalEffect(t, r, "ACK", "winner")
			write = causalPermit(r, index, effect, body)
			r.in = stateEngineInput{Write: write, Executor: write.Flow.Ingress}
			if err := r.reserve(); err != nil {
				t.Fatal(err)
			}
			recordResponseWrite(t, r, write, *outcome)
		}
	}
	if mutate != nil {
		mutate(r, c, write)
	}
	payload, err := r.finish()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := engine.store.(*state.Store).Commit(context.Background(), *begun.Step, payload, nil); err != nil {
		t.Fatal(err)
	}
	connection, _, controllerContext, _ := stateControllerPair(t, engine, c.Snapshot.Application, &pb.ProcessRef{InstanceId: "controller-draining", Epoch: "draining"})
	engine.ctx, engine.cancel = context.WithCancel(controllerContext)
	engine.work, engine.background = make(chan struct{}, 64), make(chan struct{}, 48)
	engine.scheduled = make(map[string]bool)
	engine.cfg.LiveFlow = func(flow *pb.FlowRef) bool { return proto.Equal(flow, c.Flow) }
	t.Cleanup(engine.Close)
	controller := &settlementControl{connection: connection, requests: make(chan *pb.Envelope, 16), errors: make(chan error, 1)}
	go func() {
		for {
			_, wire, err := connection.Read(engine.ctx)
			if err != nil {
				return
			}
			request := new(pb.Envelope)
			if err := protojson.Unmarshal(wire, request); err != nil {
				controller.errors <- errors.New("invalid fixture control envelope")
				return
			}
			if ping := request.GetPing(); ping != nil {
				reply := &pb.Envelope{SessionId: request.SessionId, CorrelationId: request.CorrelationId, Payload: &pb.Envelope_Pong{Pong: ping}}
				encoded, err := protojson.Marshal(reply)
				if err == nil {
					err = connection.Write(engine.ctx, websocket.MessageText, encoded)
				}
				if err != nil {
					return
				}
				continue
			}
			select {
			case controller.requests <- request:
			case <-engine.ctx.Done():
				return
			}
		}
	}()
	return engine, c, write, r.record.Ref, controller
}

func (c *settlementControl) receive(t *testing.T) *pb.Envelope {
	t.Helper()
	select {
	case request := <-c.requests:
		return request
	case err := <-c.errors:
		t.Fatal(err)
	case <-time.After(time.Second):
		t.Fatal("expected physical settlement RPC did not arrive")
	}
	return nil
}

func (c *settlementControl) reply(t *testing.T, request, reply *pb.Envelope) {
	t.Helper()
	reply.SessionId, reply.CorrelationId = request.SessionId, request.CorrelationId
	writeControllerEnvelope(t, context.Background(), c.connection, reply)
}

func (c *settlementControl) noRPC(t *testing.T, duration time.Duration) {
	t.Helper()
	select {
	case <-c.requests:
		t.Fatal("unexpected extra delivery, inspection, settlement or application RPC")
	case err := <-c.errors:
		t.Fatal(err)
	case <-time.After(duration):
	}
}

func TestStatePhysicalCompletionImmediatelyChainsOnlyRecordedSettlement(t *testing.T) {
	for _, mode := range []string{"written", "unknown", "unrecorded", "settlement-refused"} {
		t.Run(mode, func(t *testing.T) {
			engine, original, write, ref, controller := settlementFixture(t, nil, nil)
			engine.background = make(chan struct{}, 2)
			engine.background <- struct{}{} // Force Complete's broad dispatch to lose its slot.
			engine.dispatch(ref)
			physical := controller.receive(t)
			if physical.GetDeliverEffect() == nil || !proto.Equal(physical.GetDeliverEffect().Effect, write.Effect) {
				t.Fatal("initial physical effect did not retain exact identity")
			}
			if mode != "unrecorded" {
				reserved, err := engine.Reserve(engine.ctx, write)
				if err != nil || reserved.Known || len(reserved.Wire) == 0 {
					t.Fatalf("exact physical reservation failed: %v", err)
				}
				outcome := pb.DeliveryResult_WRITTEN
				if mode == "unknown" {
					outcome = pb.DeliveryResult_UNKNOWN
				}
				if err := engine.Complete(engine.ctx, write, &pb.DeliveryResult{Effect: write.Effect, Outcome: outcome}); err != nil {
					t.Fatal(err)
				}
			}
			<-engine.background
			returned := time.Now()
			controller.reply(t, physical, &pb.Envelope{Payload: &pb.Envelope_Result{Result: &pb.OperationResult{Certainty: pb.OperationResult_COMPLETED}}})
			if mode == "unrecorded" || mode == "unknown" {
				waitRPCDispatchDrain(t, engine)
				controller.noRPC(t, 30*time.Millisecond)
				record, _ := stateTestLoad(t, engine, original.Snapshot.Context)
				want := state.EffectAuthorized
				if mode == "unknown" {
					want = state.EffectUnknown
				}
				if len(record.Payload.Effects) != 1 || record.Payload.Effects[0].ID != write.Effect.Id || record.Payload.Effects[0].Status != want || record.Payload.Effects[0].DeadlineMS != int64(write.Permit.Authorization.DeadlineMs) {
					t.Fatal("control reply replaced physical uncertainty or created a new attempt/deadline")
				}
				return
			}
			settlement := controller.receive(t)
			if settlement.GetSettleEffect() == nil || settlement.GetSettleEffect().Delivery.Outcome != pb.DeliveryResult_WRITTEN ||
				!settlement.GetSettleEffect().Delivery.Recorded || time.Since(returned) >= 500*time.Millisecond {
				t.Fatal("physical return did not promptly chain its exact recorded settlement")
			}
			// Let the predecessor return while its successor's RPC stays held.
			until := time.Now().Add(500 * time.Millisecond)
			for len(engine.background) != 1 && time.Now().Before(until) {
				time.Sleep(time.Millisecond)
			}
			engine.dispatchMu.Lock()
			guarded := engine.dispatching[write.Effect.Id]
			engine.dispatchMu.Unlock()
			if len(engine.background) != 1 || !guarded {
				t.Fatal("physical predecessor released the settlement successor's guard")
			}
			controller.noRPC(t, 20*time.Millisecond)
			certainty := pb.OperationResult_COMPLETED
			if mode == "settlement-refused" {
				certainty = pb.OperationResult_NOT_ACCEPTED
			}
			controller.reply(t, settlement, &pb.Envelope{Payload: &pb.Envelope_Result{Result: &pb.OperationResult{Certainty: certainty}}})
			waitRPCDispatchDrain(t, engine)
			controller.noRPC(t, 30*time.Millisecond)
			record, group := stateTestLoad(t, engine, original.Snapshot.Context)
			if mode == "written" && len(record.Payload.Effects) != 0 || mode == "settlement-refused" && (len(record.Payload.Effects) != 1 || record.Payload.Effects[0].Status != state.EffectWritten) {
				t.Fatal("settlement collection/refusal lost exact recorded evidence")
			}
			if group.Contexts[original.Snapshot.Context.Id].Snapshot.CommandSequence != 1 {
				t.Fatal("physical settlement consumed another application command")
			}
		})
	}
}

func TestStateDispatchReleaseCannotDeleteSuccessorGuard(t *testing.T) {
	engine := &StateEngine{dispatching: make(map[string]bool)}
	predecessor := engine.acquireDispatch("effect")
	if predecessor == nil || engine.acquireDispatch("effect") != nil {
		t.Fatal("same effect admitted concurrent predecessor RPCs")
	}
	predecessor()
	successor := engine.acquireDispatch("effect")
	if successor == nil {
		t.Fatal("released predecessor blocked its successor")
	}
	predecessor() // The old deferred release runs after the new owner acquired.
	if !engine.dispatching["effect"] || engine.acquireDispatch("effect") != nil {
		t.Fatal("predecessor's repeated release deleted its successor's guard")
	}
	successor()
	if len(engine.dispatching) != 0 {
		t.Fatal("successor did not reclaim its exact transport guard")
	}
}
