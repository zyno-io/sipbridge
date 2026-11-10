package state

import (
	"context"
	"errors"
	"net"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

type delayedClockClient struct {
	command string
	before  time.Duration
	after   time.Duration
	once    atomic.Bool
	change  func(redis.Cmder)
}

func (h *delayedClockClient) DialHook(next redis.DialHook) redis.DialHook {
	return func(ctx context.Context, network, address string) (net.Conn, error) {
		return next(ctx, network, address)
	}
}

func (h *delayedClockClient) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

func (h *delayedClockClient) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, command redis.Cmder) error {
		matches := command.Name() == h.command || h.command == "script" && (command.Name() == "evalsha" || command.Name() == "eval")
		if !matches || !h.once.CompareAndSwap(false, true) {
			return next(ctx, command)
		}
		pause := func(duration time.Duration) error {
			if duration == 0 {
				return nil
			}
			timer := time.NewTimer(duration)
			defer timer.Stop()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-timer.C:
				return nil
			}
		}
		if err := pause(h.before); err != nil {
			return err
		}
		err := next(ctx, command)
		if err == nil && h.change != nil {
			h.change(command)
		}
		if delayErr := pause(h.after); delayErr != nil {
			return delayErr
		}
		return err
	}
}

func TestClockExecutionIntervalAllowsDelayedReadyBeginAndCommit(t *testing.T) {
	for _, direction := range []string{"before", "after"} {
		for _, operation := range []string{"ready", "begin", "commit"} {
			t.Run(direction+"_"+operation, func(t *testing.T) {
				s := testStore(t, func(c *Config) { c.ClaimTTL = 3 * time.Second })
				readyStore(t, s)
				ref := group(t, s, direction+"-"+operation)
				var step *Step
				if operation == "commit" {
					step = begin(t, s, ref, "commit-delay", true)
				}
				hook := &delayedClockClient{command: "script"}
				if operation == "ready" {
					hook.command = "time"
				}
				if direction == "before" {
					hook.before = 400 * time.Millisecond
				} else {
					hook.after = 400 * time.Millisecond
				}
				s.client.AddHook(hook)
				switch operation {
				case "ready":
					if err := s.Ready(context.Background()); err != nil {
						t.Fatal(err)
					}
				case "begin":
					if result, err := s.Begin(context.Background(), ref, "slow-begin", input("begin-delay", true)); err != nil || result.Step == nil {
						t.Fatalf("delayed Begin: %+v %v", result, err)
					}
				case "commit":
					if _, err := s.Commit(context.Background(), *step, Payload{}, []byte("committed")); err != nil {
						t.Fatalf("delayed Commit: %v", err)
					}
				}
				if !hook.once.Load() {
					t.Fatal("ordinary client scheduling/network delay latched the process clock fence")
				}
				if err := s.Ready(context.Background()); err != nil {
					t.Fatalf("process did not remain ready after healthy delayed execution: %v", err)
				}
			})
		}
	}
}

func TestClockAsymmetricStartupDoesNotBiasSubsequentFastReady(t *testing.T) {
	for _, direction := range []string{"request", "response"} {
		t.Run(direction, func(t *testing.T) {
			s := testStore(t, nil)
			readyStore(t, s)
			hook := &delayedClockClient{command: "time"}
			if direction == "request" {
				hook.before = 650 * time.Millisecond
			} else {
				hook.after = 650 * time.Millisecond
			}
			s.client.AddHook(hook)
			start := time.Now()
			clock, err := s.client.Time(context.Background()).Result()
			finish := time.Now()
			if err != nil {
				t.Fatal(err)
			}
			// Reproduce the startup observation with a correctly synchronized
			// server whose execution lies at one asymmetric end of a slow RTT.
			if err := verifyClock(start, finish, clock.UnixMilli()); err != nil {
				t.Fatal(err)
			}
			if err := s.Ready(context.Background()); err != nil {
				t.Fatalf("slow startup biased a subsequent fast healthy observation: %v", err)
			}
		})
	}
}

func TestClockExecutionPastIODeadlineIsTransient(t *testing.T) {
	s := testStore(t, func(c *Config) { c.Redis.IOTimeout = 100 * time.Millisecond })
	readyStore(t, s)
	s.client.AddHook(&delayedClockClient{command: "script", before: 400 * time.Millisecond})
	ref := group(t, s, "slow-deadline")
	if _, err := s.Begin(context.Background(), ref, "worker", input("slow", true)); !errors.Is(err, ErrUnknown) || errors.Is(err, ErrClock) {
		t.Fatalf("slow I/O was classified as a proven clock jump: %v", err)
	}
	if err := s.Ready(context.Background()); err != nil {
		t.Fatalf("ordinary deadline permanently fenced healthy Redis: %v", err)
	}
}

func TestClockReadyTimeoutAndLateCommitReplyDoNotLatchClockFence(t *testing.T) {
	for _, operation := range []string{"ready", "begin-reply"} {
		t.Run(operation, func(t *testing.T) {
			s := testStore(t, func(c *Config) { c.Redis.IOTimeout = 100 * time.Millisecond })
			readyStore(t, s)
			ref := group(t, s, "reply-timeout-"+operation)
			if operation == "ready" {
				s.client.AddHook(&delayedClockClient{command: "time", before: 400 * time.Millisecond})
				if err := s.Ready(context.Background()); !errors.Is(err, context.DeadlineExceeded) || errors.Is(err, ErrClock) {
					t.Fatalf("readiness timeout became permanent clock skew: %v", err)
				}
			} else {
				// Warm the script so the hook delays the reply of an executed
				// mutation instead of an initial NOSCRIPT cache miss.
				warmRef := group(t, s, "warm-reply-timeout")
				begin(t, s, warmRef, "warm", true)
				s.client.AddHook(&delayedClockClient{command: "script", after: 400 * time.Millisecond})
				if result, err := s.Begin(context.Background(), ref, "worker", input("unanswered", true)); !errors.Is(err, ErrUnknown) || errors.Is(err, ErrClock) || result.Step != nil {
					t.Fatalf("late mutation reply exposed a claim or clock fence: %+v %v", result, err)
				}
				if exists, err := s.client.Exists(context.Background(), s.keys(ref)[0]).Result(); err != nil || exists != 1 {
					t.Fatal("late-reply fixture did not execute its mutation")
				}
			}
			if err := s.Ready(context.Background()); err != nil {
				t.Fatalf("healthy clocks remained unavailable after timeout: %v", err)
			}
		})
	}
}

func TestClockLuaWindowFailureNeedsExecutionProofBeforeLatching(t *testing.T) {
	s := testStore(t, nil)
	readyStore(t, s)
	ref := group(t, s, "expired-clock-envelope")
	// Reproduce a script's pre-mutation clock-window refusal with execution
	// timestamps that prove synchronized clocks at the actual receive interval.
	_, err := s.run(context.Background(), stateScript("return fail('CLOCK')"), ref, nil, true)
	if !errors.Is(err, ErrUnknown) || errors.Is(err, ErrClock) {
		t.Fatalf("a bounded execution-window refusal proved nonexistent clock skew: %v", err)
	}
	if err := s.Ready(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestClockSkewAfterMutationIsUnknownAndLaterWorkRecovers(t *testing.T) {
	s := testStore(t, nil)
	ref := group(t, s, "forward-clock-step")
	begin(t, s, group(t, s, "warm-clock-step"), "warm-clock", true)
	s.client.AddHook(&delayedClockClient{command: "script", change: func(command redis.Cmder) {
		response := command.(*redis.Cmd)
		values := response.Val().([]interface{})
		values[2] = strconv.FormatInt(time.Now().Add(time.Second).UnixMilli(), 10)
		response.SetVal(values)
	}})
	result, err := s.Begin(context.Background(), ref, "worker", input("forward-step", true))
	if !errors.Is(err, ErrClock) || !errors.Is(err, ErrUnknown) || result.Step != nil {
		t.Fatal("ambiguous mutation exposed a claim", err)
	}
	if err := s.Ready(context.Background()); err != nil {
		t.Fatal("clock recovery required restart", err)
	}
}

func TestClockReadyRejectsProvenRemoteStep(t *testing.T) {
	s := testStore(t, nil)
	readyStore(t, s)
	s.client.AddHook(&delayedClockClient{command: "time", change: func(command redis.Cmder) {
		clock := command.(*redis.TimeCmd)
		clock.SetVal(clock.Val().Add(time.Second))
	}})
	if err := s.Ready(context.Background()); !errors.Is(err, ErrClock) {
		t.Fatalf("proven Redis TIME jump did not fence: %v", err)
	}
}

func TestClockProvenCommitSkewKeepsMutationUnknown(t *testing.T) {
	s := testStore(t, nil)
	readyStore(t, s)
	ref := group(t, s, "commit-clock-step")
	warm := begin(t, s, ref, "warm-commit", true)
	if _, err := s.Commit(context.Background(), *warm, Payload{}, []byte("warm")); err != nil {
		t.Fatal(err)
	}
	step := begin(t, s, ref, "uncertain-commit", false)
	s.client.AddHook(&delayedClockClient{command: "script", change: func(command redis.Cmder) {
		response := command.(*redis.Cmd)
		values := response.Val().([]interface{})
		observed, err := strconv.ParseInt(values[2].(string), 10, 64)
		if err != nil {
			panic(err)
		}
		values[2] = strconv.FormatInt(observed+1000, 10)
		response.SetVal(values)
	}})
	if _, err := s.Commit(context.Background(), *step, Payload{}, []byte("committed-but-not-exposed")); !errors.Is(err, ErrClock) || !errors.Is(err, ErrUnknown) {
		t.Fatalf("proven post-commit discontinuity exposed a safe result: %v", err)
	}
	receipts, err := s.client.HGet(context.Background(), s.keys(ref)[0], "receipts").Result()
	if err != nil || !strings.Contains(receipts, "uncertain-commit") {
		t.Fatalf("fixture did not exercise an already-applied commit: %v", err)
	}
}

func TestClockScriptEnvelopePreservesErrorsAndResultShapes(t *testing.T) {
	s := testStore(t, nil)
	readyStore(t, s)
	ref := group(t, s, "envelope-errors")
	if _, err := s.Load(context.Background(), ref); !errors.Is(err, ErrNotFound) {
		t.Fatalf("wrapped read changed top-level error mapping: %v", err)
	}
	step := begin(t, s, ref, "original", true)
	if _, err := s.Begin(context.Background(), ref, "other-worker", input("other", false)); !errors.Is(err, ErrBusy) {
		t.Fatalf("wrapped Begin changed busy mapping: %v", err)
	}
	if _, err := s.Commit(context.Background(), *step, Payload{}, []byte("original-result")); err != nil {
		t.Fatal(err)
	}
	loaded, err := s.Load(context.Background(), ref)
	if err != nil || len(loaded.Receipts) != 1 || !strings.Contains(string(loaded.Receipts[0].Outcome), "original-result") {
		t.Fatalf("wrapped result changed record/receipt shape: %+v %v", loaded, err)
	}
}
