package engine

import (
	"bytes"
	"context"
	"net"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/emiago/sipgo/sip/portable"
	"github.com/redis/go-redis/v9"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/rediscore"
	"sipbridge/internal/selectedmessage"
)

func TestRedisInitialUACUDPDerivative(t *testing.T) {
	t.Run("written-arm-and-stop", func(t *testing.T) { testRedisUACQualificationInitialAdmission(t, true, false, false, false, false, 30) })
	t.Run("response-before-arm", func(t *testing.T) { testRedisUACQualificationInitialAdmission(t, true, false, false, false, false, 31) })
}

type retryCommandCounter struct{ calls atomic.Int64 }

func (h *retryCommandCounter) DialHook(next redis.DialHook) redis.DialHook { return next }
func (h *retryCommandCounter) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		h.calls.Add(1)
		return next(ctx, cmd)
	}
}
func (h *retryCommandCounter) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		h.calls.Add(int64(len(cmds)))
		return next(ctx, cmds)
	}
}

// The shared fixture admitted and physically recorded the real initial INVITE.
// Only the derivative's socket callback/live local lease are fixture boundaries.
func testRedisInitialUACUDP(t *testing.T, ctx context.Context, store *rediscore.Store, client *redis.Client, input redisUACQualification, capture *nativeOutputCapture, beforeArmResponse bool) {
	t.Helper()
	type retry struct {
		at    time.Time
		wire  []byte
		calls int64
	}
	counter := &retryCommandCounter{}
	client.AddHook(counter)
	writes := make(chan retry, 4)
	manager, _ := udpRetryManager(t, nativeUDPRetryLimits{Entries: 2, Hints: 2, Writers: 2, Bytes: 1 << 20}, func(ctx context.Context, _ *pb.FlowRef, wire []byte, _ time.Time) error {
		select {
		case writes <- retry{time.Now(), bytes.Clone(wire), counter.calls.Load()}:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	e := &redisNative{cfg: Config{Process: input.Executor}, worker: input.ExecutorTimer.Worker, udp: manager}
	stopped := make(chan struct{}, 1)
	wake, err := store.SubscribeWakeResults(ctx, e.worker, func(rediscore.ResultHint) {}, func(h rediscore.UDPHint) {
		e.udpHint(h)
		if h.Stop && h.Context == capture.ref.Context && h.Incarnation == capture.ref.Incarnation && h.Procedure == capture.metadata.Procedure {
			select {
			case stopped <- struct{}{}:
			default:
			}
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for wake.Receive(ctx) == nil {
		}
	}()
	t.Cleanup(func() { _ = wake.Close(); <-done })
	originView, err := store.Read(ctx, rediscore.Identity{Context: capture.ref.Context, Incarnation: capture.ref.Incarnation, Owner: input.Lease.Application, Term: input.Lease.Term}, capture.proof.MessageField)
	if err != nil {
		t.Fatal(err)
	}
	origin, err := selectedmessage.DecodeOpaque(originView.Records[capture.proof.MessageField])
	if err != nil {
		t.Fatal(err)
	}
	receive := func() {
		t.Helper()
		built, err := portable.BuildResponseWithHeaders(origin, 180, "Ringing", nil, "remote-stop", "", nil)
		if err != nil {
			t.Fatal(err)
		}
		response, err := portable.ParseMessage(built.WireBytes(), input.Flow.Transport, net.JoinHostPort(input.Flow.SourceIp, strconv.Itoa(int(input.Flow.SourcePort))), "")
		if err != nil {
			t.Fatal(err)
		}
		handled, err := receiveRedisUACProvisional(ctx, store, input.Flow, response, input.Key, input.ExecutorTimer, time.Now().Add(time.Second))
		if err != nil || !handled {
			t.Fatal("matching response", err)
		}
		select {
		case <-stopped:
		case <-time.After(time.Second):
			t.Fatal("committed response did not stop exact original retry")
		}
	}
	if beforeArmResponse {
		receive()
		if err = e.armRedisInitialUACUDP(ctx, store, capture.ref, capture.metadata); err == nil {
			t.Fatal("already proceeding INVITE armed A")
		}
	} else {
		if err = e.armRedisInitialUACUDP(ctx, store, capture.ref, capture.metadata); err != nil {
			t.Fatal("exact settled-W INVITE did not arm A", err)
		}
	}
	if err = acknowledgeRedisUACInitialOutput(ctx, store, capture, time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if beforeArmResponse {
		manager.mu.Lock()
		count := len(manager.entries)
		manager.mu.Unlock()
		if count != 0 {
			t.Fatal("response-before-arm retained a derivative")
		}
		return
	}
	baseline := counter.calls.Load()
	stable := time.Now()
	for {
		select {
		case write := <-writes:
			if write.at.Before(stable) {
				continue
			}
			if !bytes.Equal(write.wire, origin.WireBytes()) || write.calls != baseline || counter.calls.Load() != baseline {
				t.Fatal("Timer A changed wire or performed a Redis operation")
			}
		case <-time.After(2 * time.Second):
			t.Fatal("Timer A retry missing")
		}
		break
	}
	receive()
	manager.mu.Lock()
	count := len(manager.entries)
	manager.mu.Unlock()
	if count != 0 {
		t.Fatal("matched response left local A live")
	}
}
