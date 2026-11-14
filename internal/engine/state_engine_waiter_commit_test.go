package engine

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/emiago/sipgo/sip"

	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/execution"
	"sipbridge/internal/state"
)

type waiterReadGuard struct {
	state.Backend
	blocked atomic.Bool
	reads   atomic.Int32
}

func (g *waiterReadGuard) ReadState(ctx context.Context, ref state.Ref) (state.StateView, int64, error) {
	if g.blocked.Load() {
		g.reads.Add(1)
		return state.StateView{}, 0, errors.New("unexpected result fallback read")
	}
	return g.Backend.ReadState(ctx, ref)
}

type waiterCommitDriver struct {
	execution.Driver
	command string
	after   func(execution.Result) error
	failure error
}

func (d *waiterCommitDriver) ApplyCommitted(ctx context.Context, ref state.Ref, in state.Input) (execution.Result, error) {
	if in.ID == d.command && d.failure != nil {
		return execution.Result{}, d.failure
	}
	result, err := d.Driver.ApplyCommitted(ctx, ref, in)
	if err == nil && in.ID == d.command && d.after != nil {
		err = d.after(result)
	}
	return result, err
}

func TestCommandWaiterRegistersBeforeCommitAndCleansUp(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(map[bool]string{false: "complete-before-return", true: "producer-error"}[fail], func(t *testing.T) {
			f := newNativeOwnerFixtureBackend(t, state.MemoryBackend)
			ref, _, _ := freshPresentation(t, f)
			_, group := stateTestLoad(t, f.engine, ref)
			c := group.Contexts[ref.Id]
			var requestID string
			for id := range c.Requests {
				requestID = id
			}
			_, _, ctx, _ := stateControllerPair(t, f.engine, f.app, &pb.ProcessRef{InstanceId: "draining", Epoch: "other"})
			peer, err := f.engine.control.Peer(f.app)
			if err != nil {
				t.Fatal(err)
			}
			command := &pb.Command{Context: ref, Sequence: 1, Action: &pb.Command_Respond{Respond: &pb.Respond{RequestId: requestID, Status: 200, Authority: f.responseAuthority(t, ref, requestID)}}}
			f.admitCommand(t, ref, "wait-before-commit", command)
			readGuard := &waiterReadGuard{Backend: f.engine.store}
			f.engine.store = readGuard
			driver := &waiterCommitDriver{Driver: f.engine.runtime, command: "wait-before-commit"}
			if fail {
				driver.failure = errors.New("injected failure")
			} else {
				driver.after = func(result execution.Result) error {
					logical, _ := f.engine.group(ref)
					key := logical.ID + ":" + logical.Incarnation
					f.engine.lifecycleMu.Lock()
					count := len(f.engine.waiters[key])
					f.engine.lifecycleMu.Unlock()
					if count != 1 {
						t.Fatalf("waiter absent before producer return: %d", count)
					}
					var write *pb.FlowWrite
					for _, effect := range result.Committed.Effects {
						if effect.Kind != "deliver" {
							continue
						}
						var body stateEngineEffect
						if err := body.unmarshalBinary(effect.Payload, false); err != nil {
							return err
						}
						if body.Native && body.Permit != nil && body.Authorization != nil {
							write = &pb.FlowWrite{Flow: body.Permit.Flow, Effect: body.Authorization.Effect, Permit: body.Permit}
							break
						}
					}
					if write == nil {
						return errors.New("committed delivery missing")
					}
					if _, err := f.engine.Reserve(ctx, write); err != nil {
						return err
					}
					if err := f.engine.Complete(ctx, write, &pb.DeliveryResult{Effect: write.Effect, Outcome: pb.DeliveryResult_WRITTEN}); err != nil {
						return err
					}
					readGuard.blocked.Store(true)
					f.engine.lifecycleMu.Lock()
					defer f.engine.lifecycleMu.Unlock()
					for _, waiter := range f.engine.waiters[key] {
						if len(waiter.outcome) == 0 || len(waiter.signal) != 1 {
							t.Fatal("completion before producer return did not publish exact result")
						}
					}
					return nil
				}
			}
			f.engine.runtime = driver
			result, err := f.engine.handle(ctx, peer, &pb.Envelope{CorrelationId: "wait-before-commit", Payload: &pb.Envelope_Command{Command: command}}, time.Now().Add(2*time.Second))
			if fail {
				if err == nil {
					t.Fatal("injected producer error was lost")
				}
			} else if err != nil || result.GetCertainty() != pb.OperationResult_COMPLETED {
				t.Fatalf("result: %+v %v", result, err)
			}
			if !fail && (f.engine.operationObservationMetrics.local.Load() != 1 || f.engine.operationObservationMetrics.store.Load() != 0) {
				t.Fatal("metrics did not record local completion exclusively")
			}
			if fail && (f.engine.operationObservationMetrics.local.Load() != 0 || f.engine.operationObservationMetrics.store.Load() != 0) {
				t.Fatal("failed admission recorded an observation")
			}
			if readGuard.reads.Load() != 0 {
				t.Fatal("local completion reread retained state")
			}
			f.engine.lifecycleMu.Lock()
			defer f.engine.lifecycleMu.Unlock()
			if len(f.engine.waiters) != 0 {
				t.Fatal("command return leaked waiter")
			}
		})
	}
}

func TestNotifyWaiterIgnoresIntermediateWriteAndUnrelatedIngest(t *testing.T) {
	f := newNativeOwnerFixtureBackend(t, state.MemoryBackend)
	ref := migrationSubscribe(t, f, 3600)
	logical, err := f.engine.group(ref)
	if err != nil {
		t.Fatal(err)
	}
	signal, stop := f.engine.watchOperation(logical, stateOperationIdentity{ref.Id, "notify-wait"})
	defer stop()
	result := f.apply(t, ref, "notify-wait", stateEngineInput{Kind: "command", Application: f.app, ApplicationLeaseTerm: f.term,
		Command: &pb.Command{Context: ref, Sequence: 2, Action: &pb.Command_DialogRequest{DialogRequest: &pb.DialogRequest{Method: "NOTIFY", Headers: []*pb.Header{{Name: "Event", Value: "message-summary"}, {Name: "Subscription-State", Value: "active;expires=3600"}}}}}})
	if result.Error != "" || result.Certainty != pb.OperationResult_ACCEPTED {
		t.Fatalf("notify setup: %+v", result)
	}
	record, group := stateTestLoad(t, f.engine, ref)
	c := group.Contexts[ref.Id]
	var write *pb.FlowWrite
	for _, effect := range record.Payload.Effects {
		if effect.Kind != "deliver" {
			continue
		}
		var body stateEngineEffect
		if err := body.UnmarshalBinary(effect.Payload); err != nil {
			t.Fatal(err)
		}
		if body.Command == "notify-wait" && body.WireMethod == "NOTIFY" {
			write = &pb.FlowWrite{Flow: body.Permit.Flow, Effect: body.Authorization.Effect, Permit: body.Permit}
			break
		}
	}
	if write == nil {
		t.Fatal("NOTIFY delivery absent")
	}
	reserved, err := f.engine.Reserve(context.Background(), write)
	if err != nil || reserved.Known {
		t.Fatal("NOTIFY reserve", err)
	}
	if err = f.engine.Complete(context.Background(), write, &pb.DeliveryResult{Effect: write.Effect, Outcome: pb.DeliveryResult_WRITTEN}); err != nil {
		t.Fatal(err)
	}
	if len(signal) != 0 {
		t.Fatal("intermediate NOTIFY write woke command waiter")
	}
	result = f.apply(t, ref, "unrelated-subscribe-replay", stateEngineInput{Kind: "ingest", Application: f.app, ApplicationLeaseTerm: f.term,
		Ingest: &pb.OwnerIngest{Context: ref, Application: f.app, Flow: f.flow, Wire: c.Origin.WireBytes()}})
	if result.Error != "" {
		t.Fatal(result.Error)
	}
	if len(signal) != 0 {
		t.Fatal("unrelated ingress woke command waiter")
	}
	parsed, err := sip.ParseMessage(reserved.Wire)
	if err != nil {
		t.Fatal(err)
	}
	request, ok := parsed.(*sip.Request)
	if !ok {
		t.Fatal("NOTIFY wire was not a request")
	}
	response := sip.NewResponseFromRequest(request, 200, "OK", nil)
	result = f.apply(t, ref, "notify-final", stateEngineInput{Kind: "ingest", Application: f.app, ApplicationLeaseTerm: f.term,
		Ingest: &pb.OwnerIngest{Context: ref, Application: f.app, Flow: f.flow, Wire: []byte(response.String())}})
	if result.Error != "" {
		t.Fatal(result.Error)
	}
	if len(signal) != 1 {
		t.Fatal("matching phone final did not wake waiter")
	}
	observed, err := f.engine.observeLocalOperation(logical, ref.Id, "notify-wait", signal, "")
	if err != nil || observed.GetCertainty() != pb.OperationResult_COMPLETED {
		t.Fatalf("matching result missing: %+v %v", observed, err)
	}
}

type cancelAfterWaitRead struct {
	state.Backend
	cancel context.CancelFunc
}

func (s *cancelAfterWaitRead) ReadState(ctx context.Context, ref state.Ref) (state.StateView, int64, error) {
	view, now, err := s.Backend.ReadState(ctx, ref)
	s.cancel()
	return view, now, err
}

func TestCommandFallbackObservationCountsStoreRead(t *testing.T) {
	f := newNativeOwnerFixtureBackend(t, state.MemoryBackend)
	ref, _, _ := freshPresentation(t, f)
	logical, err := f.engine.group(ref)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f.engine.store = &cancelAfterWaitRead{Backend: f.engine.store, cancel: cancel}
	signal, stop := f.engine.watchOperation(logical, stateOperationIdentity{ref.Id, "remote-operation"})
	defer stop()
	_, err = f.engine.awaitOperationSignal(ctx, logical, ref.Id, "remote-operation", &pb.OperationResult{Certainty: pb.OperationResult_ACCEPTED}, true, signal, false)
	if err != nil {
		t.Fatal(err)
	}
	if f.engine.operationObservationMetrics.store.Load() != 1 || f.engine.operationObservationMetrics.local.Load() != 0 {
		t.Fatal("fallback read source was not counted")
	}
}

func TestCommandRefusalUnregistersWaiter(t *testing.T) {
	f := newNativeOwnerFixtureBackend(t, state.MemoryBackend)
	ref, _, _ := freshPresentation(t, f)
	_, group := stateTestLoad(t, f.engine, ref)
	var requestID string
	for id := range group.Contexts[ref.Id].Requests {
		requestID = id
	}
	_, _, ctx, _ := stateControllerPair(t, f.engine, f.app, &pb.ProcessRef{InstanceId: "draining", Epoch: "other"})
	peer, err := f.engine.control.Peer(f.app)
	if err != nil {
		t.Fatal(err)
	}
	command := &pb.Command{Context: ref, Sequence: 4, Action: &pb.Command_Respond{Respond: &pb.Respond{RequestId: requestID, Status: 200, Authority: f.responseAuthority(t, ref, requestID)}}}
	f.admitCommand(t, ref, "refused-command", command)
	result, err := f.engine.handle(ctx, peer, &pb.Envelope{CorrelationId: "refused-command", Payload: &pb.Envelope_Command{Command: command}}, time.Now().Add(2*time.Second))
	if err != nil || result.GetCertainty() != pb.OperationResult_NOT_ACCEPTED {
		t.Fatalf("expected definite refusal: %+v %v", result, err)
	}
	f.engine.lifecycleMu.Lock()
	defer f.engine.lifecycleMu.Unlock()
	if len(f.engine.waiters) != 0 {
		t.Fatal("definite refusal leaked waiter")
	}
}
