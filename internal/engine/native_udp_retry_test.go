package engine

import (
	"bytes"
	"context"
	"sync"
	"testing"
	"time"

	"github.com/emiago/sipgo/sip/portable"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/nativepb"
	"sipbridge/internal/rediscore"
)

func udpRetrySeed(t *testing.T, key nativeUDPRetryKey) nativeUDPRetrySeed {
	return udpClientRetrySeed(t, key, "NOTIFY", portable.NonInviteClient)
}

func udpClientRetrySeed(t *testing.T, key nativeUDPRetryKey, method string, family portable.Family) nativeUDPRetrySeed {
	t.Helper()
	request := stateTestRequest(method, 1, "", "udp-retry")
	parsed, err := portable.ParseMessage(request.Wire, "udp", "", "")
	if err != nil {
		t.Fatal(err)
	}
	profile := portable.DefaultTimers()
	profile.T1 = 25
	profile.T2 = 100
	tx, err := portable.NewTransaction(family, parsed, profile, "")
	if err != nil {
		t.Fatal(err)
	}
	tx, _, err = portable.StepTransaction(tx, portable.Input{Kind: portable.Start, Now: time.Now().UnixMilli()})
	if err != nil {
		t.Fatal(err)
	}
	tx.Origin = portable.RestoreMessage(request.Wire, "udp", "", "")
	process := &pb.ProcessRef{InstanceId: "worker", Epoch: "epoch"}
	flow := &pb.FlowRef{Ingress: process, ConnectionId: "connection", SourceIp: "192.0.2.1", SourcePort: 5060, Listener: "sip.example.test:5060", Transport: "udp"}
	effect := &pb.StateEffectRef{Context: &pb.ContextRef{Id: key.Context, State: &pb.StateRef{Incarnation: key.Incarnation}}, Id: "effect", AttemptId: "attempt"}
	auth := &pb.EffectAuthorization{Effect: effect, AuthorityClass: pb.EffectAuthorization_NATIVE_COMPLETION, DeadlineMs: uint64(time.Now().Add(time.Second).UnixMilli())}
	write := &pb.FlowWrite{Flow: flow, Effect: effect, Permit: &pb.SendPermit{Flow: flow, Authorization: auth}}
	metadata, err := encodeNativeOutput(&nativepb.Output{Write: write, Procedure: key.Procedure, Method: method})
	if err != nil {
		t.Fatal(err)
	}
	ref, err := nativeOutputRef(write)
	if err != nil {
		t.Fatal(err)
	}
	return nativeUDPRetrySeed{Key: key, Attempt: ref, Proof: rediscore.OutputInspection{State: rediscore.OutputWritten, Settled: true, Metadata: metadata, MetadataField: "r:output", MessageField: "r:origin", WireHash: tx.Origin.WireHash()}, Captured: time.Now(), Transaction: tx, Flow: flow}
}
func udpRetryManager(t *testing.T, limits nativeUDPRetryLimits, write nativeUDPRetryWrite) (*nativeUDPRetry, context.CancelFunc) {
	t.Helper()
	leaseCtx, cancel := context.WithCancel(context.Background())
	manager, err := newNativeUDPRetry(context.Background(), limits, func() (context.Context, time.Time, error) { return leaseCtx, time.Now().Add(time.Second), nil }, write)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cancel(); manager.Close() })
	return manager, cancel
}
func udpKey(id string) nativeUDPRetryKey {
	return nativeUDPRetryKey{Context: "context-" + id, Incarnation: "incarnation", Procedure: "r:procedure-" + id}
}
func TestUDPRetryStopBeforeArmAndLazyParse(t *testing.T) {
	m, _ := udpRetryManager(t, nativeUDPRetryLimits{Entries: 2, Hints: 2, Writers: 2, Bytes: 1 << 20}, func(context.Context, *pb.FlowRef, []byte, time.Time) error {
		t.Error("stopped transaction wrote")
		return nil
	})
	seed := udpRetrySeed(t, udpKey("one"))
	before := seed.Transaction.Origin.RetainedBytes()
	m.Stop(seed.Key)
	if err := m.Arm(seed); err == nil {
		t.Fatal("stop-before-arm lost")
	}
	if got := seed.Transaction.Origin.RetainedBytes(); got != before {
		t.Fatal("early response forced packet parse", before, got)
	}
	stale := udpRetrySeed(t, udpKey("two"))
	stale.Captured = time.Now().Add(-3 * time.Second)
	if err := m.Arm(stale); err == nil {
		t.Fatal("expired post-proof capture armed")
	}
	unknown := udpRetrySeed(t, udpKey("three"))
	unknown.Proof.State = rediscore.OutputUnknown
	if err := m.Arm(unknown); err == nil {
		t.Fatal("uncertain output armed")
	}
}
func TestUDPRetryTimerEReusesExactWireAndProceedingInterval(t *testing.T) {
	type event struct {
		at      time.Time
		wire    []byte
		address *byte
	}
	writes := make(chan event, 4)
	m, _ := udpRetryManager(t, nativeUDPRetryLimits{Entries: 2, Hints: 2, Writers: 2, Bytes: 1 << 20}, func(_ context.Context, _ *pb.FlowRef, wire []byte, _ time.Time) error {
		writes <- event{time.Now(), bytes.Clone(wire), &wire[0]}
		return nil
	})
	seed := udpRetrySeed(t, udpKey("one"))
	original := seed.Transaction.Origin.WireBytes()
	before := seed.Transaction.Origin.RetainedBytes()
	m.Proceeding(seed.Key)
	if err := m.Arm(seed); err != nil {
		t.Fatal(err)
	}
	var first, second event
	select {
	case first = <-writes:
	case <-time.After(time.Second):
		t.Fatal("first retry missing")
	}
	select {
	case second = <-writes:
	case <-time.After(time.Second):
		t.Fatal("second retry missing")
	}
	if !bytes.Equal(first.wire, original) || !bytes.Equal(second.wire, original) || first.address != second.address {
		t.Fatal("wire changed or copied per tick")
	}
	if second.at.Sub(first.at) < 75*time.Millisecond {
		t.Fatal("provisional response did not use T2", second.at.Sub(first.at))
	}
	if seed.Transaction.Origin.RetainedBytes() <= before {
		t.Fatal("first actual retry did not parse selected message")
	}
	m.Stop(seed.Key)
}
func TestUDPRetryProofFenceAndHintOverflow(t *testing.T) {
	m, _ := udpRetryManager(t, nativeUDPRetryLimits{Entries: 2, Hints: 1, Writers: 2, Bytes: 1 << 20}, func(context.Context, *pb.FlowRef, []byte, time.Time) error { return nil })
	a, b := udpKey("a"), udpKey("b")
	m.Stop(a)
	m.Stop(b)
	seed := udpRetrySeed(t, udpKey("c"))
	if err := m.Arm(seed); err == nil {
		t.Fatal("hint overflow armed an unchecked seed")
	}
	m.mu.Lock()
	m.overflowUntil = time.Now().Add(-time.Second)
	m.hints = make(map[nativeUDPRetryKey]nativeUDPHint)
	m.mu.Unlock()
	seed = udpRetrySeed(t, udpKey("d"))
	seed.Proof.Settled = false
	if err := m.Arm(seed); err == nil {
		t.Fatal("unrecorded written proof armed")
	}
	seed = udpRetrySeed(t, udpKey("e"))
	seed.Attempt.Attempt = "different"
	if err := m.Arm(seed); err == nil {
		t.Fatal("different attempt armed")
	}
}
func TestUDPRetryBlockedFlowDoesNotBlockOtherAndLeaseFences(t *testing.T) {
	blocked := make(chan struct{}, 1)
	other := make(chan struct{}, 1)
	var once sync.Once
	m, cancelLease := udpRetryManager(t, nativeUDPRetryLimits{Entries: 3, Hints: 3, Writers: 2, Bytes: 1 << 20}, func(ctx context.Context, flow *pb.FlowRef, _ []byte, _ time.Time) error {
		if flow.ConnectionId == "connection" {
			once.Do(func() { blocked <- struct{}{} })
			<-ctx.Done()
			return ctx.Err()
		}
		other <- struct{}{}
		return nil
	})
	first := udpRetrySeed(t, udpKey("one"))
	second := udpRetrySeed(t, udpKey("two"))
	// Distinct exact flow must be part of its independently verified proof.
	metadata, err := decodeNativeOutput(second.Proof.Metadata)
	if err != nil {
		t.Fatal(err)
	}
	second.Flow.ConnectionId = "other"
	metadata.Write.Flow.ConnectionId = "other"
	metadata.Write.Permit.Flow.ConnectionId = "other"
	second.Proof.Metadata, err = encodeNativeOutput(metadata)
	if err != nil {
		t.Fatal(err)
	}
	second.Attempt, err = nativeOutputRef(metadata.Write)
	if err != nil {
		t.Fatal(err)
	}
	if err = m.Arm(first); err != nil {
		t.Fatal(err)
	}
	if err = m.Arm(second); err != nil {
		t.Fatal(err)
	}
	select {
	case <-blocked:
	case <-time.After(time.Second):
		t.Fatal("blocked flow did not start")
	}
	select {
	case <-other:
	case <-time.After(time.Second):
		t.Fatal("blocked flow stalled other retries")
	}
	cancelLease()
	limit := time.Now().Add(time.Second)
	for {
		m.mu.Lock()
		n := len(m.entries)
		m.mu.Unlock()
		if n == 0 {
			break
		}
		if time.Now().After(limit) {
			t.Fatal("lease loss retained retries")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestUDPRetryStopsBeforeFirstTimerWithoutParsing(t *testing.T) {
	writes := make(chan struct{}, 1)
	m, _ := udpRetryManager(t, nativeUDPRetryLimits{Entries: 2, Hints: 2, Writers: 2, Bytes: 1 << 20}, func(context.Context, *pb.FlowRef, []byte, time.Time) error { writes <- struct{}{}; return nil })
	seed := udpRetrySeed(t, udpKey("stop"))
	timer := seed.Transaction.Timers[portable.TimerE]
	timer.At = time.Now().Add(150 * time.Millisecond).UnixMilli()
	seed.Transaction.Timers[portable.TimerE] = timer
	before := seed.Transaction.Origin.RetainedBytes()
	if err := m.Arm(seed); err != nil {
		t.Fatal(err)
	}
	m.Stop(seed.Key)
	select {
	case <-writes:
		t.Fatal("stopped cache wrote")
	case <-time.After(200 * time.Millisecond):
	}
	if seed.Transaction.Origin.RetainedBytes() != before {
		t.Fatal("stopped origin was parsed")
	}
}
func TestUDPRetryPreservesOriginalTimerF(t *testing.T) {
	writes := make(chan time.Time, 16)
	m, _ := udpRetryManager(t, nativeUDPRetryLimits{Entries: 2, Hints: 2, Writers: 2, Bytes: 1 << 20}, func(_ context.Context, _ *pb.FlowRef, _ []byte, _ time.Time) error { writes <- time.Now(); return nil })
	seed := udpRetrySeed(t, udpKey("cutoff"))
	f := seed.Transaction.Timers[portable.TimerF]
	f.At = time.Now().Add(170 * time.Millisecond).UnixMilli()
	seed.Transaction.Timers[portable.TimerF] = f
	if err := m.Arm(seed); err != nil {
		t.Fatal(err)
	}
	select {
	case <-writes:
	case <-time.After(time.Second):
		t.Fatal("retry absent before F")
	}
	time.Sleep(220 * time.Millisecond)
	for {
		select {
		case at := <-writes:
			if at.UnixMilli() >= f.At {
				t.Fatal("retry escaped original F", at, f.At)
			}
		default:
			return
		}
	}
}

func TestUDPRetryRequiresMonotonicCapture(t *testing.T) {
	m, _ := udpRetryManager(t, nativeUDPRetryLimits{Entries: 2, Hints: 2, Writers: 2, Bytes: 1 << 20}, func(context.Context, *pb.FlowRef, []byte, time.Time) error { return nil })
	seed := udpRetrySeed(t, udpKey("wall-only"))
	seed.Captured = seed.Captured.Round(0)
	if err := m.Arm(seed); err == nil {
		t.Fatal("wall-only selection timestamp armed a retry")
	}
}
func TestUDPRetryWriterReceivesAnchoredMonotonicCutoff(t *testing.T) {
	got := make(chan time.Time, 1)
	m, _ := udpRetryManager(t, nativeUDPRetryLimits{Entries: 2, Hints: 2, Writers: 2, Bytes: 1 << 20}, func(_ context.Context, _ *pb.FlowRef, _ []byte, until time.Time) error {
		select {
		case got <- until:
		default:
		}
		return nil
	})
	seed := udpRetrySeed(t, udpKey("anchor"))
	f := seed.Transaction.Timers[portable.TimerF]
	f.At = seed.Captured.UnixMilli() + 200
	seed.Transaction.Timers[portable.TimerF] = f
	anchor := seed.Captured.Add(-time.Duration(seed.Captured.Nanosecond() % int(time.Millisecond)))
	expected := anchor.Add(200 * time.Millisecond)
	if err := m.Arm(seed); err != nil {
		t.Fatal(err)
	}
	select {
	case until := <-got:
		if until == until.Round(0) || until.Sub(expected) < -time.Millisecond || until.Sub(expected) > time.Millisecond {
			t.Fatal("writer cutoff lost monotonic anchor", until, expected)
		}
	case <-time.After(time.Second):
		t.Fatal("retry missing")
	}
}

func TestUDPRetryOldLeaseCancelsBeforeRemovalLock(t *testing.T) {
	oldLease, cancelOld := context.WithCancel(context.Background())
	defer cancelOld()
	replacement, cancelReplacement := context.WithCancel(context.Background())
	defer cancelReplacement()
	var leaseMu sync.Mutex
	current := oldLease
	m, err := newNativeUDPRetry(context.Background(), nativeUDPRetryLimits{Entries: 2, Hints: 2, Writers: 2, Bytes: 1 << 20}, func() (context.Context, time.Time, error) {
		leaseMu.Lock()
		defer leaseMu.Unlock()
		return current, time.Now().Add(time.Second), nil
	}, func(context.Context, *pb.FlowRef, []byte, time.Time) error {
		t.Error("old generation wrote")
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	seed := udpRetrySeed(t, udpKey("generation"))
	e := seed.Transaction.Timers[portable.TimerE]
	e.At = time.Now().Add(150 * time.Millisecond).UnixMilli()
	seed.Transaction.Timers[portable.TimerE] = e
	if err = m.Arm(seed); err != nil {
		t.Fatal(err)
	}
	m.mu.Lock()
	entry := m.entries[seed.Key]
	cancelOld()
	leaseMu.Lock()
	current = replacement
	leaseMu.Unlock()
	if entry.ctx.Err() == nil {
		m.mu.Unlock()
		t.Fatal("old lease cancellation waited for removal lock")
	}
	m.mu.Unlock()
	time.Sleep(200 * time.Millisecond)
}

func TestUDPRetryRejectsWallOnlyLeaseDeadline(t *testing.T) {
	leaseCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var mu sync.Mutex
	calls := 0
	writes := make(chan struct{}, 1)
	manager, err := newNativeUDPRetry(context.Background(), nativeUDPRetryLimits{Entries: 2, Hints: 2, Writers: 2, Bytes: 1 << 20}, func() (context.Context, time.Time, error) {
		mu.Lock()
		defer mu.Unlock()
		calls++
		until := time.Now().Add(time.Second)
		if calls > 1 {
			until = until.Round(0)
		}
		return leaseCtx, until, nil
	}, func(context.Context, *pb.FlowRef, []byte, time.Time) error { writes <- struct{}{}; return nil })
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()
	seed := udpRetrySeed(t, udpKey("lease-clock"))
	if err = manager.Arm(seed); err != nil {
		t.Fatal(err)
	}
	select {
	case <-writes:
		t.Fatal("writer accepted wall-only lease deadline")
	case <-time.After(150 * time.Millisecond):
	}
	wallManager, err := newNativeUDPRetry(context.Background(), nativeUDPRetryLimits{Entries: 2, Hints: 2, Writers: 2, Bytes: 1 << 20}, func() (context.Context, time.Time, error) { return leaseCtx, time.Now().Add(time.Second).Round(0), nil }, func(context.Context, *pb.FlowRef, []byte, time.Time) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	defer wallManager.Close()
	if err = wallManager.Arm(udpRetrySeed(t, udpKey("lease-wall"))); err == nil {
		t.Fatal("Arm accepted wall-only lease deadline")
	}
}

func TestUDPRetryDeadlineUsesLeaseAnchorBeforeCaptureRollback(t *testing.T) {
	leaseUntil := time.Now().Add(2 * time.Second)
	storedF := leaseUntil.UnixMilli() - 1800
	// Simulate the wall timestamp at a later capture being ten seconds behind;
	// that untrusted wall value must not enter the persisted-deadline mapping.
	rolledBackCaptureMS := time.Now().UnixMilli() - 10000
	naiveDelay := time.Duration(storedF-rolledBackCaptureMS) * time.Millisecond
	if naiveDelay < 10*time.Second {
		t.Fatal("invalid rollback fixture", naiveDelay)
	}
	cutoff := nativeUDPRetryDeadline(leaseUntil, storedF)
	if cutoff == cutoff.Round(0) {
		t.Fatal("deadline lost monotonic component")
	}
	remaining := time.Until(cutoff)
	if remaining < 100*time.Millisecond || remaining > 300*time.Millisecond {
		t.Fatal("pre-capture rollback prolonged deadline", remaining)
	}
}

func TestUDPRetryLastHintExpiresWithoutIdleWork(t *testing.T) {
	m, _ := udpRetryManager(t, nativeUDPRetryLimits{Entries: 2, Hints: 2, Writers: 2, Bytes: 1 << 20}, func(context.Context, *pb.FlowRef, []byte, time.Time) error {
		t.Error("unexpected idle write")
		return nil
	})
	m.Stop(udpKey("idle"))
	deadline := time.Now().Add(nativeUDPHintWindow + time.Second)
	for {
		m.mu.Lock()
		count := len(m.hints)
		at := m.hintCleanupAt
		entries := len(m.entries)
		m.mu.Unlock()
		if count == 0 {
			if !at.IsZero() || entries != 0 {
				t.Fatal("idle retry manager retained work")
			}
			break
		}
		if !time.Now().Before(deadline) {
			t.Fatal("last hint retained after expiry")
		}
		time.Sleep(10 * time.Millisecond)
	}
}
