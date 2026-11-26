package engine

import (
	"crypto/sha256"
	"errors"

	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"

	"sipbridge/internal/rediscore"
)

// Result waiters are transient hints only. Their authoritative result is always
// read under the caller's live application and worker leases.
const nativeResultWaiterLimit = 1024

type nativeResultWaiter struct {
	changed                 chan struct{}
	users                   int
	allowDialogResume       bool
	dialogResumeFingerprint [32]byte
	dialogResumeID          string
	dialogResumeRef         rediscore.OutputRef
}

type redisNotifyWaitCapture struct {
	state       *pb.StateRef
	sequence    uint64
	fingerprint [32]byte
	allowResume bool
}

func captureRedisNotifyWait(command *pb.Command) (redisNotifyWaitCapture, error) {
	var capture redisNotifyWaitCapture
	if command == nil || command.Context == nil || command.Context.State == nil || command.Sequence == 0 {
		return capture, errNativeRecord
	}
	encoded, err := proto.MarshalOptions{Deterministic: true}.Marshal(command)
	if err != nil {
		return capture, err
	}
	capture.state = proto.Clone(command.Context.State).(*pb.StateRef)
	capture.sequence = command.Sequence
	capture.fingerprint = sha256.Sum256(encoded)
	return capture, nil
}

func (e *redisNative) resultHint(h rediscore.ResultHint) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if waiter := e.resultWaiters[h]; waiter != nil {
		close(waiter.changed)
		waiter.changed = make(chan struct{})
	}
}

// registerResultWaiter must run before the first authoritative result read.
// The returned channel is captured before each read so a concurrent hint is
// never lost between that read and the caller's wait.
func (e *redisNative) registerResultWaiter(h rediscore.ResultHint) (*nativeResultWaiter, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed || e.resultWaiterCount >= nativeResultWaiterLimit {
		return nil, errors.New("result waiters unavailable")
	}
	waiter := e.resultWaiters[h]
	if waiter == nil {
		waiter = &nativeResultWaiter{changed: make(chan struct{})}
		e.resultWaiters[h] = waiter
	}
	waiter.users++
	e.resultWaiterCount++
	return waiter, nil
}

func (e *redisNative) resultWaiterSignal(waiter *nativeResultWaiter) <-chan struct{} {
	e.mu.Lock()
	defer e.mu.Unlock()
	return waiter.changed
}

func (e *redisNative) releaseResultWaiter(h rediscore.ResultHint, waiter *nativeResultWaiter) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.resultWaiters[h] != waiter {
		return
	}
	waiter.users--
	e.resultWaiterCount--
	if waiter.users == 0 {
		delete(e.resultWaiters, h)
	}
}
