package engine

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/state"
)

func operationWaitRecord(t *testing.T, operation string, result *pb.OperationResult) state.Record {
	t.Helper()
	group := stateGroup{Version: 1, Contexts: map[string]*stateContext{"context": {
		Snapshot: &pb.ContextSnapshot{Context: &pb.ContextRef{Id: "context"}, Revision: 17}, Operation: operation, Result: result,
	}}, Selectors: make(map[string]string)}
	encoded, err := json.Marshal(group)
	if err != nil {
		t.Fatal(err)
	}
	return state.Record{Payload: state.Payload{ProtocolState: encoded}}
}

func TestStateOperationWaitFreshCommitUsesHintOrInitialIntervalBeforeReading(t *testing.T) {
	for _, mode := range []string{"signal-during-dispatch", "remote-fallback", "deadline-tail", "replay", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			now := time.Now().Add(time.Hour)
			if mode == "deadline-tail" {
				var stop context.CancelFunc
				ctx, stop = context.WithDeadline(ctx, now.Add(25*time.Millisecond))
				defer stop()
			}
			engine := &StateEngine{cfg: Config{Process: &pb.ProcessRef{InstanceId: "poller", Epoch: "epoch"}}}
			ref := state.Ref{ID: "group", Incarnation: "incarnation"}
			signal, stop := engine.watchOperation(ref)
			defer stop()
			if mode == "signal-during-dispatch" {
				engine.signalOperation(ref)
			}
			start := now
			var events []string
			accepted := &pb.OperationResult{Certainty: pb.OperationResult_ACCEPTED}
			result, err := engine.awaitOperationPolling(ctx, ref, "context", "operation", accepted, true, stateOperationPolling{
				waitBeforeRead: mode != "replay",
				wait: func(ctx context.Context, delay time.Duration) bool {
					events = append(events, "wait")
					expected := 40 * time.Millisecond
					if mode == "deadline-tail" {
						expected = 10 * time.Millisecond
					}
					if delay != expected {
						t.Fatalf("initial fallback changed: %s", delay)
					}
					if mode == "cancel" {
						cancel()
						return false
					}
					if mode == "signal-during-dispatch" {
						return waitStateOperationSignal(ctx, time.Hour, signal)
					}
					now = now.Add(delay)
					return true
				},
				load: func(context.Context, state.Ref) (state.Record, error) {
					events = append(events, "read")
					return operationWaitRecord(t, "operation", &pb.OperationResult{Certainty: pb.OperationResult_COMPLETED}), nil
				},
				now: func() time.Time { return now },
			})
			if err != nil || accepted.Certainty != pb.OperationResult_ACCEPTED {
				t.Fatalf("observation mutated acceptance or failed: %v", err)
			}
			if mode == "cancel" {
				if result.Certainty != pb.OperationResult_UNKNOWN || result.Error == "" || !reflect.DeepEqual(events, []string{"wait"}) {
					t.Fatal("cancelled Respond read state or returned unproven acceptance")
				}
			} else {
				want := []string{"wait", "read"}
				if mode == "replay" {
					want = []string{"read"}
				}
				if result.Certainty != pb.OperationResult_COMPLETED || result.Snapshot.Revision != 17 || !reflect.DeepEqual(events, want) {
					t.Fatalf("hint became authority or observation order changed: %v %v", result, events)
				}
			}
			expectedElapsed := time.Duration(0)
			if mode == "remote-fallback" {
				expectedElapsed = 40 * time.Millisecond
			} else if mode == "deadline-tail" {
				expectedElapsed = 10 * time.Millisecond
			}
			if now.Sub(start) != expectedElapsed {
				t.Fatal("local hint, replay or cancellation gained a fallback delay")
			}
			stop()
			if len(engine.waiters) != 0 {
				t.Fatal("observation leaked its local hint")
			}
		})
	}
}

func TestStateOperationWaitBackoffReadsFreshStateWithoutCatchUp(t *testing.T) {
	engine := &StateEngine{cfg: Config{Process: &pb.ProcessRef{InstanceId: "poller", Epoch: "epoch"}}}
	ref := state.Ref{ID: "retained-group", Incarnation: "incarnation"}
	accepted := &pb.OperationResult{Certainty: pb.OperationResult_ACCEPTED}
	now := time.Now()
	start := now
	var reads []time.Duration
	var pauses []time.Duration
	result, err := engine.awaitOperationPolling(context.Background(), ref, "context", "operation", accepted, true, stateOperationPolling{
		load: func(ctx context.Context, actual state.Ref) (state.Record, error) {
			if actual != ref || ctx.Err() != nil {
				t.Fatal("poll changed the authoritative reference or context")
			}
			reads = append(reads, now.Sub(start))
			// A read slower than the old ticker must still be followed by a wait.
			now = now.Add(85 * time.Millisecond)
			certainty := pb.OperationResult_ACCEPTED
			if len(reads) == 5 {
				certainty = pb.OperationResult_COMPLETED
			}
			return operationWaitRecord(t, "operation", &pb.OperationResult{Certainty: certainty, SipStatus: 200}), nil
		},
		wait: func(_ context.Context, pause time.Duration) bool {
			pauses = append(pauses, pause)
			now = now.Add(pause)
			return true
		},
		now: func() time.Time { return now },
	})
	if err != nil || result.Certainty != pb.OperationResult_COMPLETED || result.Snapshot.Revision != 17 || !proto.Equal(result.Snapshot.Executor, engine.cfg.Process) {
		t.Fatalf("did not return the newly loaded completed snapshot: %s %v", result, err)
	}
	if !reflect.DeepEqual(pauses, []time.Duration{10 * time.Millisecond, 20 * time.Millisecond, 40 * time.Millisecond, 40 * time.Millisecond}) {
		t.Fatalf("unexpected full-read pacing: %s", pauses)
	}
	if reads[0] != 0 || accepted.Certainty != pb.OperationResult_ACCEPTED {
		t.Fatal("initial poll was delayed or the caller's accepted result mutated")
	}
	for index := 1; index < len(reads); index++ {
		if reads[index]-reads[index-1] != 85*time.Millisecond+pauses[index-1] {
			t.Fatal("slow reads accumulated catch-up polls")
		}
	}
}

func TestStateOperationWaitDeadlineTailPreservesLatestRefusal(t *testing.T) {
	// Keep the real context comfortably live while the injected clock advances
	// deterministically to its original deadline without wall-clock sleeps.
	start := time.Now().Add(time.Hour)
	deadline := start.Add(235 * time.Millisecond)
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()
	now := start
	reads, waits := 0, 0
	accepted := &pb.OperationResult{Certainty: pb.OperationResult_ACCEPTED}
	result, err := (&StateEngine{}).awaitOperationPolling(ctx, state.Ref{}, "context", "operation", accepted, true, stateOperationPolling{
		load: func(actual context.Context, _ state.Ref) (state.Record, error) {
			actualDeadline, ok := actual.Deadline()
			if !ok || actualDeadline != deadline || !now.Before(deadline) {
				t.Fatal("poll lost or extended the original deadline")
			}
			reads++
			status, reason := uint32(200), ""
			if now.Sub(start) >= 100*time.Millisecond {
				status, reason = 503, "retained publication refused"
			}
			return operationWaitRecord(t, "operation", &pb.OperationResult{Certainty: pb.OperationResult_ACCEPTED, SipStatus: status, Error: reason}), nil
		},
		wait: func(_ context.Context, pause time.Duration) bool {
			waits++
			if pause <= 0 || now.Add(pause).After(deadline) || now.Add(pause).After(deadline.Add(-100*time.Millisecond)) && pause > 10*time.Millisecond {
				t.Fatalf("wait escaped the deadline tail or remaining budget: %s at %s", pause, now.Sub(start))
			}
			now = now.Add(pause)
			if now == deadline {
				if pause != 5*time.Millisecond {
					t.Fatalf("last wait was not clamped to remaining budget: %s", pause)
				}
				cancel()
				return false
			}
			return true
		},
		now: func() time.Time { return now },
	})
	if err != nil || result.Certainty != pb.OperationResult_UNKNOWN || result.SipStatus != 503 || result.Error != "retained publication refused" || now != deadline || reads != waits || reads != 17 {
		t.Fatalf("deadline wait changed refusal/uncertainty or repeated reads: %s %v reads=%d waits=%d elapsed=%s", result, err, reads, waits, now.Sub(start))
	}
}

func TestStateOperationWaitNearDeadlineCompletion(t *testing.T) {
	start := time.Now().Add(time.Hour)
	deadline := start.Add(235 * time.Millisecond)
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()
	now := start
	engine := &StateEngine{cfg: Config{Process: &pb.ProcessRef{InstanceId: "poller", Epoch: "epoch"}}}
	result, err := engine.awaitOperationPolling(ctx, state.Ref{}, "context", "operation", &pb.OperationResult{Certainty: pb.OperationResult_ACCEPTED}, true, stateOperationPolling{
		load: func(context.Context, state.Ref) (state.Record, error) {
			certainty := pb.OperationResult_ACCEPTED
			if !now.Before(deadline.Add(-15 * time.Millisecond)) {
				certainty = pb.OperationResult_COMPLETED
			}
			return operationWaitRecord(t, "operation", &pb.OperationResult{Certainty: certainty}), nil
		},
		wait: func(_ context.Context, pause time.Duration) bool {
			now = now.Add(pause)
			if !now.Before(deadline) {
				cancel()
				return false
			}
			return true
		},
		now: func() time.Time { return now },
	})
	if err != nil || result.Certainty != pb.OperationResult_COMPLETED || now.After(deadline.Add(-5*time.Millisecond)) {
		t.Fatalf("tail backoff missed a completion available before deadline: %s %v at %s", result, err, now.Sub(start))
	}
}

func TestStateOperationWaitRequiresExactOperationAndFreshResult(t *testing.T) {
	engine := &StateEngine{cfg: Config{Process: &pb.ProcessRef{InstanceId: "poller", Epoch: "epoch"}}}
	reads, waits := 0, 0
	result, err := engine.awaitOperationPolling(context.Background(), state.Ref{}, "context", "wanted", &pb.OperationResult{Certainty: pb.OperationResult_ACCEPTED}, false, stateOperationPolling{
		load: func(context.Context, state.Ref) (state.Record, error) {
			reads++
			if reads == 1 {
				return operationWaitRecord(t, "different-operation", &pb.OperationResult{Certainty: pb.OperationResult_COMPLETED, SipStatus: 488}), nil
			}
			return operationWaitRecord(t, "wanted", &pb.OperationResult{Certainty: pb.OperationResult_COMPLETED, SipStatus: 200}), nil
		},
		wait: func(context.Context, time.Duration) bool { waits++; return true },
		now:  time.Now,
	})
	if err != nil || reads != 2 || waits != 1 || result.Certainty != pb.OperationResult_COMPLETED || result.SipStatus != 200 {
		t.Fatalf("another operation satisfied the retained wait: %s %v reads=%d", result, err, reads)
	}
}

func TestStateOperationWaitErrorsAndCorruptionStillStopPolling(t *testing.T) {
	for _, respond := range []bool{false, true} {
		for _, problem := range []error{state.ErrNotFound, state.ErrAuthority, state.ErrCorrupt, context.DeadlineExceeded} {
			t.Run(problem.Error()+"/respond="+map[bool]string{false: "false", true: "true"}[respond], func(t *testing.T) {
				reads := 0
				result, err := (&StateEngine{}).awaitOperationPolling(context.Background(), state.Ref{}, "context", "operation",
					&pb.OperationResult{Certainty: pb.OperationResult_ACCEPTED, SipStatus: 503, Error: "selected refusal"}, respond, stateOperationPolling{
						load: func(context.Context, state.Ref) (state.Record, error) { reads++; return state.Record{}, problem },
						wait: func(context.Context, time.Duration) bool { t.Fatal("read error was retried"); return false },
						now:  time.Now,
					})
				if reads != 1 || respond && (err != nil || result.Certainty != pb.OperationResult_UNKNOWN || result.SipStatus != 503 || result.Error != "selected refusal") || !respond && (!errors.Is(err, problem) || result != nil) {
					t.Fatalf("read error changed outcome semantics: %s %v reads=%d", result, err, reads)
				}
			})
		}
	}
	result, err := (&StateEngine{}).awaitOperationPolling(context.Background(), state.Ref{}, "context", "operation", &pb.OperationResult{Certainty: pb.OperationResult_ACCEPTED}, true, stateOperationPolling{
		load: func(context.Context, state.Ref) (state.Record, error) {
			return state.Record{Payload: state.Payload{ProtocolState: []byte("{")}}, nil
		},
		wait: func(context.Context, time.Duration) bool { t.Fatal("corrupt group was ignored"); return false },
		now:  time.Now,
	})
	if result != nil || !errors.Is(err, state.ErrCorrupt) {
		t.Fatalf("corrupt native aggregate was accepted: %s %v", result, err)
	}
}

func TestWaitStateOperationPollTimerAndCancellation(t *testing.T) {
	if !waitStateOperationPoll(context.Background(), 0) {
		t.Fatal("elapsed timer did not admit the next poll")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if waitStateOperationPoll(ctx, 0) {
		t.Fatal("already-cancelled context admitted another poll")
	}
	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()
	done := make(chan bool, 1)
	go func() { done <- waitStateOperationPoll(ctx, time.Hour) }()
	cancel()
	select {
	case due := <-done:
		if due {
			t.Fatal("cancelled wait admitted another full read")
		}
	case <-time.After(time.Second):
		t.Fatal("cancellation waited for the polling timer")
	}
}
