package engine

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/state"
)

// A native negative final creates ACK work before its FAILED notification. The
// fixture retains the notification after that predecessor has consumed its
// original two-second budget, without changing the notification's identity.
func delayedFailureEventFixture(t *testing.T, engine *StateEngine, record state.Record, age time.Duration) (*stateReduction, *stateContext, state.Effect) {
	t.Helper()
	r, c, tx := forkFixture(t)
	r.engine, r.now = engine, time.Now().Add(-age).UnixMilli()
	r.record = record
	r.group.Version = 1
	r.group.Retired = make(map[string]int64)
	r.result = &pb.OperationResult{Certainty: pb.OperationResult_COMPLETED}
	r.in.Operation = "negative-final"
	c.Snapshot.Context = engine.contextRef(r.record.Ref, c.Snapshot.Context.Id)
	c.Flow.Ingress = proto.Clone(engine.cfg.Process).(*pb.ProcessRef)
	c.OwnerValidated, c.OwnerDirectoryEpoch = true, "directory"
	c.OwnerProbeAtMS = time.Now().Add(time.Minute).UnixMilli()
	tx.Application, tx.CompletionScope = c.Snapshot.Application, c.CompletionScope
	if err := r.stepTransaction(c, tx, portable.Input{Kind: portable.Start, Now: r.now}); err != nil {
		t.Fatal(err)
	}
	if err := r.stepTransaction(c, tx, portable.Input{Kind: portable.ReceiveResponse, Now: r.now, Message: forkReply(t, tx, 488, "codec-refused")}); err != nil {
		t.Fatal(err)
	}
	var notification state.Effect
	ack := false
	for _, effect := range r.payload.Effects {
		var body stateEngineEffect
		if unmarshalRetainedFixture(effect.Payload, &body) != nil {
			t.Fatal("invalid native effect")
		}
		if effect.Kind == "wire" {
			message, err := sip.ParseMessage(body.Wire)
			if err == nil && message.CSeq().MethodName == sip.ACK {
				ack = true
			}
		}
		if effect.Kind == "event" && body.Envelope.GetDialogEvent().GetKind() == pb.DialogEvent_FAILED {
			if !ack {
				t.Fatal("native FAILED notification was not queued behind its ACK work")
			}
			notification = effect
		}
	}
	if !ack || notification.ID == "" || notification.DeadlineMS >= time.Now().UnixMilli() {
		t.Fatal("fixture requires native ACK before an expired pending FAILED notification")
	}
	// The preceding physical work has settled; this is the durable queue state
	// seen when the serial dispatcher finally reaches the untouched event.
	r.payload.Effects = []state.Effect{notification}
	return r, c, notification
}

func TestStatePendingFailedEventRecoversAfterPredecessorBudgetAndPreservesOrder(t *testing.T) {
	engine, _ := historicalStateTestEngine(t)
	ref, err := engine.store.GroupForCallID("fork-call")
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	begun, err := engine.store.(*state.Store).Begin(ctx, ref, "seed", state.Input{ID: "seed", Kind: "locate", Fingerprint: "seed", Create: true, DeadlineMS: time.Now().Add(time.Second).UnixMilli(), ReceiptUntilMS: time.Now().Add(time.Minute).UnixMilli()})
	if err != nil {
		t.Fatal(err)
	}
	r, c, first := delayedFailureEventFixture(t, engine, begun.Step.Record, 3*time.Second)
	r.event(c, pb.DialogEvent_ENDED, nil, "later-ended")
	second := r.payload.Effects[1]
	payload, err := r.finish()
	if err != nil {
		t.Fatal(err)
	}
	if _, err = engine.store.(*state.Store).Commit(ctx, *begun.Step, payload, nil); err != nil {
		t.Fatal(err)
	}
	connection, _, controllerContext, _ := stateControllerPair(t, engine, c.Snapshot.Application, &pb.ProcessRef{InstanceId: "other-controller", Epoch: "other"})
	engine.ctx, engine.cancel = context.WithCancel(controllerContext)
	engine.work, engine.background = make(chan struct{}, 64), make(chan struct{}, 48)
	engine.scheduled = make(map[string]bool)
	t.Cleanup(engine.Close)
	var recovery state.Timer
	for _, timer := range payload.Timers {
		if timer.ID == r.timerID("group", "effects", "recovery", "") {
			recovery = timer
		}
	}
	if recovery.ID == "" || recovery.DueMS >= time.Now().UnixMilli() || !engine.applyScheduledTimers(engine.ctx, r.record.Ref, []state.Timer{recovery}) {
		t.Fatal("fixture did not execute the overdue durable event recovery timer")
	}
	var allow atomic.Bool
	var calls atomic.Int64
	observed := make(chan string, 4)
	controllerErrors := make(chan error, 1)
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
				if request.GetDialogEvent() == nil || !stateSameContext(request.GetDialogEvent().GetSnapshot().GetContext(), c.Snapshot.Context) {
					controllerErrors <- errors.New("unexpected non-event RPC")
					return
				}
				calls.Add(1)
				observed <- request.CorrelationId
				certainty := pb.OperationResult_NOT_ACCEPTED
				if allow.Load() {
					certainty = pb.OperationResult_COMPLETED
				}
				reply.Payload = &pb.Envelope_Result{Result: &pb.OperationResult{Certainty: certainty}}
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
	if got := calls.Load(); got != 1 {
		t.Fatalf("FAILED receiver calls after predecessor exhausted original 2s budget: got %d, want 1", got)
	}
	if got := <-observed; got != first.ID {
		t.Fatal("later event passed unresolved FAILED prefix")
	}
	record, err := engine.store.Load(engine.ctx, r.record.Ref)
	if err != nil {
		t.Fatal(err)
	}
	if len(record.Payload.Effects) != 2 || record.Payload.Effects[0].ID != first.ID || record.Payload.Effects[0].Status != state.EffectUnknown || record.Payload.Effects[0].DeadlineMS != first.DeadlineMS || record.Payload.Effects[1].Status != state.EffectPending {
		t.Fatal("recovery changed original deadline/identity, or dispatched the later event")
	}
	allow.Store(true)
	engine.dispatch(r.record.Ref)
	waitRPCDispatchDrain(t, engine)
	if calls.Load() != 3 || <-observed != first.ID || <-observed != second.ID {
		t.Fatal("recovery did not reuse FAILED identity before advancing the event prefix")
	}
	record, err = engine.store.Load(engine.ctx, r.record.Ref)
	if err != nil || len(record.Payload.Effects) != 0 {
		t.Fatalf("acknowledged event prefix did not settle: %v", err)
	}
	select {
	case err := <-controllerErrors:
		t.Fatal(err)
	default:
	}
}

func TestStateEventRPCCannotOutliveOriginalRecoveryWindow(t *testing.T) {
	for _, status := range []state.EffectStatus{state.EffectPending, state.EffectUnknown} {
		t.Run(string(status), func(t *testing.T) {
			engine, _ := historicalStateTestEngine(t)
			ref, err := engine.store.GroupForCallID("fork-call")
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			begun, err := engine.store.(*state.Store).Begin(ctx, ref, "seed", state.Input{ID: "seed", Kind: "locate", Fingerprint: "seed", Create: true, DeadlineMS: time.Now().Add(time.Second).UnixMilli(), ReceiptUntilMS: time.Now().Add(time.Minute).UnixMilli()})
			if err != nil {
				t.Fatal(err)
			}
			r, c, event := delayedFailureEventFixture(t, engine, begun.Step.Record, 121250*time.Millisecond)
			if status == state.EffectUnknown {
				var body stateEngineEffect
				if err := unmarshalRetainedFixture(event.Payload, &body); err != nil {
					t.Fatal(err)
				}
				body.Executor = proto.Clone(engine.cfg.Process).(*pb.ProcessRef)
				event.Status, event.AttemptID = status, "previous-rpc"
				event.Payload, _ = marshalRetainedFixture(body)
				r.payload.Effects[0] = event
			}
			payload, err := r.finish()
			if err != nil {
				t.Fatal(err)
			}
			if _, err = engine.store.(*state.Store).Commit(ctx, *begun.Step, payload, nil); err != nil {
				t.Fatal(err)
			}
			connection, _, controllerContext, _ := stateControllerPair(t, engine, c.Snapshot.Application, &pb.ProcessRef{InstanceId: "other-controller", Epoch: "other"})
			engine.ctx, engine.cancel = context.WithCancel(controllerContext)
			t.Cleanup(engine.Close)
			observed := make(chan *pb.Envelope, 1)
			go func() {
				_, wire, err := connection.Read(engine.ctx)
				if err != nil {
					return
				}
				request := new(pb.Envelope)
				if protojson.Unmarshal(wire, request) == nil {
					observed <- request
				}
				// Withhold the receipt to exercise the actual RPC's context bound.
			}()
			boundary := time.UnixMilli(event.DeadlineMS + 120000)
			engine.dispatchEffectWithin(engine.ctx, r.record.Ref, event, false)
			if time.Now().After(boundary.Add(500 * time.Millisecond)) {
				t.Fatal("event RPC used a fresh two seconds beyond its original recovery boundary")
			}
			select {
			case request := <-observed:
				if request.CorrelationId != event.ID || request.GetDialogEvent() == nil || request.RemainingBudgetMs == 0 || request.RemainingBudgetMs > 750 {
					t.Fatal("event framing exceeded the remaining recovery budget or changed its identity")
				}
			default:
				t.Fatal("pending/recovered event did not reach its receiver before the recovery boundary")
			}
			// An executor retaining the same effect after the boundary cannot emit
			// another frame, even when called directly outside scheduler filtering.
			after := make(chan struct{}, 1)
			go func() {
				if _, _, err := connection.Read(engine.ctx); err == nil {
					after <- struct{}{}
				}
			}()
			engine.dispatchEffectWithin(engine.ctx, r.record.Ref, event, false)
			select {
			case <-after:
				t.Fatal("event RPC started after its original recovery boundary")
			case <-time.After(20 * time.Millisecond):
			}
		})
	}
}

func TestStatePendingEventStartUsesOriginalRecoveryBoundaryOnly(t *testing.T) {
	for _, offset := range []int64{-1, 0, 1} {
		t.Run((time.Duration(offset) * time.Millisecond).String(), func(t *testing.T) {
			r, c, _ := forkFixture(t)
			r.result = &pb.OperationResult{}
			r.event(c, pb.DialogEvent_FAILED, nil, "failure")
			original := r.payload.Effects[0]
			r.now = original.DeadlineMS + 120000 + offset
			if offset < 0 {
				r.in.Timer = r.timerID("group", "effects", "recovery", "")
				if handled, err := r.lifecycleTimer(); !handled || err != nil || r.payload.Effects[0].Status != state.EffectPending {
					t.Fatalf("recovery timer abandoned pending event before its recovery boundary: %v", err)
				}
			}
			r.in.Effect = original.ID
			err := r.startEffect()
			if offset >= 0 {
				if !errors.Is(err, state.ErrExpired) || r.payload.Effects[0].Status != state.EffectPending {
					t.Fatal("event started at or beyond its original recovery boundary")
				}
				r.in.Timer = r.timerID("group", "effects", "recovery", "")
				if handled, err := r.lifecycleTimer(); !handled || err != nil || r.payload.Effects[0].Status != state.EffectCancelled {
					t.Fatalf("recovery timer retained pending event at/beyond its recovery boundary: %v", err)
				}
				return
			}
			if err != nil || r.payload.Effects[0].Status != state.EffectDispatching || r.payload.Effects[0].DeadlineMS != original.DeadlineMS {
				t.Fatalf("pending event could not start before its unchanged recovery boundary: %v", err)
			}
			r.in = stateEngineInput{Effect: original.ID, Executor: &pb.ProcessRef{InstanceId: "stale-executor", Epoch: "stale"}, Reply: &pb.Envelope{Payload: &pb.Envelope_Result{Result: &pb.OperationResult{Certainty: pb.OperationResult_COMPLETED}}}}
			if err := r.effectResult(); err == nil || r.payload.Effects[0].Status != state.EffectDispatching {
				t.Fatal("stale executor settled recovered event")
			}
		})
	}
	for _, kind := range []string{"wire", "open", "credential", "present", "adopt"} {
		t.Run(kind, func(t *testing.T) {
			r, c, _ := forkFixture(t)
			r.result = &pb.OperationResult{}
			r.addEffect(kind, c, stateEngineEffect{}, r.now+2000)
			r.in.Effect = r.payload.Effects[0].ID
			r.now += 2001
			if err := r.startEffect(); !errors.Is(err, state.ErrExpired) {
				t.Fatal("lifecycle recovery extended another effect's start authority")
			}
		})
	}
}

func TestStateOwnerLossCancelsPendingRecoveredEvent(t *testing.T) {
	r, c, _ := ownerFixture(t, false)
	r.event(c, pb.DialogEvent_FAILED, nil, "failure")
	first := r.payload.Effects[0]
	r.now = first.DeadlineMS + 1
	if err := r.loseApplicationOwner(c); err != nil {
		t.Fatal(err)
	}
	r.in.Effect = first.ID
	if err := r.startEffect(); err == nil {
		t.Fatal("owner-lost pending event was resurrected by its recovery window")
	}
	before := len(r.payload.Effects)
	r.event(c, pb.DialogEvent_ENDED, nil, "later")
	if len(r.payload.Effects) != before {
		t.Fatal("owner-lost context emitted another business event")
	}
}
