package rediscore

import (
	"context"
	"crypto/sha256"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
	"sipbridge/internal/selectedmessage"
)

func TestDriverRenewsWhileJobBlocksAndFencesBeforeReacquire(t *testing.T) {
	cfg, client := connectionFixture(t)
	conn, err := NewConnection(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	started := make(chan context.Context, 1)
	release := make(chan struct{})
	exited := make(chan struct{})
	var once sync.Once
	diagnostics := make(chan DriverDiagnostic, 4)
	var d *Driver
	d, err = NewDriver(DriverConfig{Connection: conn, Worker: "worker", Epoch: "epoch", Due: func(ctx context.Context, _ *Store, _ *DueContext) error {
		select {
		case started <- ctx:
		default:
		}
		<-release
		once.Do(func() { close(exited) })
		return nil
	}, ReadyOutput: func(context.Context, *Store, ReadyOutput) error { return nil }, OwnerExpired: func(context.Context, *Store, ExpiredApplicationContext) error { return nil }, OnDiagnostic: func(event DriverDiagnostic) {
		if d.Ready() == nil {
			t.Error("diagnostic observed unfenced driver")
		}
		diagnostics <- event
	}})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- d.Run(ctx) }()
	defer func() {
		cancel()
		_ = d.Close()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Error("driver failed to exit")
		}
	}()
	waitReady := func() {
		t.Helper()
		deadline := time.Now().Add(5 * time.Second)
		for d.Ready() != nil {
			if time.Now().After(deadline) {
				t.Fatal("driver did not become ready")
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	waitReady()
	store, _, _, err := d.Current()
	if err != nil {
		t.Fatal(err)
	}
	id := Identity{Context: "context", Incarnation: "incarnation"}
	assignment := TimerAssignment{Worker: "worker", Epoch: "epoch", Term: "epoch"}
	t.Cleanup(func() {
		keys := []string{store.contextKey(id.Context), store.contextDeadlineKey(id.Context), store.workerKey("worker"), store.workerTimerKey("worker"), store.workerReadyKey("worker"), store.ownerExpiryKey(), store.coordinatorKey()}
		if e := client.Del(context.Background(), keys...).Err(); e != nil {
			t.Error(e)
		}
	})
	_, err = store.Commit(ctx, Publication{Identity: id, Create: true, Deadline: time.Now().Add(time.Minute), Timers: &TimerMutation{Target: assignment, Changes: []TimerChange{{Member: "duty", Due: time.Now().Add(100 * time.Millisecond)}}}})
	if err != nil {
		t.Fatal(err)
	}
	var jobCtx context.Context
	select {
	case jobCtx = <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("due callback not started")
	}
	time.Sleep(2500 * time.Millisecond)
	if jobCtx.Err() != nil || d.Ready() != nil {
		t.Fatal("healthy renewal stopped while callback blocked")
	}
	if err = client.Del(ctx, store.profileKey()).Err(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-jobCtx.Done():
	case <-time.After(3 * time.Second):
		t.Fatal("lease failure did not cancel callback")
	}
	if d.Ready() == nil {
		t.Fatal("failed lease remained ready")
	}
	select {
	case event := <-diagnostics:
		if event.Stage != "renew" || event.Category != "authority" {
			t.Fatalf("unexpected diagnostic: %+v", event)
		}
	case <-time.After(time.Second):
		t.Fatal("missing renewal diagnostic")
	}
	if err = client.Set(ctx, store.profileKey(), store.profile, 0).Err(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(1200 * time.Millisecond)
	if d.Ready() == nil {
		t.Fatal("reacquired while old callback remained active")
	}
	close(release)
	select {
	case <-exited:
	case <-time.After(time.Second):
		t.Fatal("callback did not finish")
	}
	waitReady()
	if err := d.Run(ctx); !errors.Is(err, ErrConflict) {
		t.Fatal("second Run accepted", err)
	}
}

func TestDriverDispatchesExpiredApplicationHead(t *testing.T) {
	cfg, client := connectionFixture(t)
	conn, err := NewConnection(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	store, err := conn.Refresh(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	setup := context.Background()
	lease, err := store.AcquireApplication(setup, "app1", "term1")
	if err != nil {
		t.Fatal(err)
	}
	id := Identity{Context: "expired-call", Incarnation: "incarnation", Owner: "app1", Term: lease.Term}
	if _, err = store.Commit(setup, Publication{Identity: id, Create: true, Deadline: time.Now().Add(time.Second), Application: &lease.ApplicationLeaseRef}); err != nil {
		t.Fatal(err)
	}
	if err = store.ExpireApplication(setup, lease.ApplicationLeaseRef); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if e := client.Del(setup, store.workerKey("worker"), store.workerTimerKey("worker"), store.workerReadyKey("worker"), store.ownerExpiryKey(), store.coordinatorKey(), store.contextKey(id.Context), store.applicationKey(id.Owner), store.applicationOwnerKey(id.Term), store.applicationOwnedKey(id.Term)).Err(); e != nil {
			t.Error(e)
		}
	})
	observed := make(chan string, 1)
	d, err := NewDriver(DriverConfig{Connection: conn, Worker: "worker", Epoch: "epoch", Due: func(context.Context, *Store, *DueContext) error { return nil }, ReadyOutput: func(context.Context, *Store, ReadyOutput) error { return nil }, OwnerExpired: func(ctx context.Context, _ *Store, item ExpiredApplicationContext) error {
		term := item.Identity.Term
		observed <- term
		_, err := store.Commit(ctx, Publication{Identity: item.Identity, Revision: item.Revision, Deadline: time.Now().Add(time.Second), ReleaseApplication: true})
		return err
	}})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- d.Run(ctx) }()
	defer func() { cancel(); _ = d.Close(); <-done }()
	select {
	case term := <-observed:
		if term != "term1" {
			t.Fatal("wrong expired application", term)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("owner callback not invoked")
	}
}

func TestDriverExpiredLocalLeaseCannotReviveOldJob(t *testing.T) {
	d := &Driver{}
	store := &Store{}
	wake := &Wake{connected: true, generation: 1, changed: make(chan struct{})}
	d.wake = wake
	if !d.publish(store, wake, time.Now().Add(30*time.Millisecond), true) {
		t.Fatal("initial lease refused")
	}
	_, ctx, _, err := d.Current()
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("watchdog did not fence expired lease")
	}
	if d.publish(store, wake, time.Now().Add(time.Second), false) {
		t.Fatal("late renewal revived a fenced job")
	}
	if d.Ready() == nil {
		t.Fatal("expired lease remains ready")
	}
}

func TestDriverRotatesEligibleDuties(t *testing.T) {
	if !driverChooseDue(0, true, true) || driverChooseDue(1, true, true) || !driverChooseDue(1, true, false) || driverChooseDue(0, false, true) {
		t.Fatal("native due and owner work were not fairly selected")
	}
}

func TestDriverSubscriptionFailureFencesOnlyItsBinding(t *testing.T) {
	d := &Driver{}
	current := &Wake{connected: true, generation: 1, changed: make(chan struct{})}
	stale := &Wake{connected: true, generation: 1, changed: make(chan struct{})}
	store := &Store{}
	d.wake = current
	if !d.publish(store, current, time.Now().Add(time.Second), true) {
		t.Fatal("initial binding refused")
	}
	_, ctx, _, err := d.Current()
	if err != nil {
		t.Fatal(err)
	}
	d.subscriptionFailed(context.Background(), stale, ErrAuthority)
	if ctx.Err() != nil || d.Ready() != nil {
		t.Fatal("old receiver fenced replacement")
	}
	d.subscriptionFailed(context.Background(), current, ErrAuthority)
	if ctx.Err() == nil || d.Ready() == nil {
		t.Fatal("failed active receiver did not immediately fence binding")
	}
	if d.publish(store, current, time.Now().Add(time.Second), true) {
		t.Fatal("failed subscription published binding")
	}
}

func TestDriverPhysicalCallbacksDoNotBlockDueWork(t *testing.T) {
	cfg, client := connectionFixture(t)
	conn, err := NewConnection(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	physical := make(chan string, 2)
	dueCalled := make(chan struct{}, 1)
	allowReturn := make(chan struct{})
	var unblock sync.Once
	d, err := NewDriver(DriverConfig{Connection: conn, Worker: "worker", Epoch: "epoch", Due: func(ctx context.Context, _ *Store, _ *DueContext) error {
		select {
		case dueCalled <- struct{}{}:
		default:
		}
		<-ctx.Done()
		return ctx.Err()
	}, ReadyOutput: func(ctx context.Context, _ *Store, out ReadyOutput) error {
		physical <- out.Ref.ID
		<-ctx.Done()
		<-allowReturn
		return ctx.Err()
	}, OwnerExpired: func(context.Context, *Store, ExpiredApplicationContext) error { return nil }})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	ended := make(chan struct{})
	go func() { done <- d.Run(ctx); close(ended) }()
	defer func() { cancel(); _ = d.Close(); <-done }()
	defer unblock.Do(func() { close(allowReturn) })
	var store *Store
	deadline := time.Now().Add(5 * time.Second)
	for store == nil {
		store, _, _, err = d.Current()
		if err != nil {
			if time.Now().After(deadline) {
				t.Fatal("driver not ready")
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	stopWake := make(chan struct{})
	wakeDone := make(chan struct{})
	go func() {
		defer close(wakeDone)
		ticker := time.NewTicker(time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stopWake:
				return
			case <-ticker.C:
				_ = client.Publish(context.Background(), store.root+"core:wake:worker", "1").Err()
			}
		}
	}()
	defer func() { close(stopWake); <-wakeDone }()
	id := Identity{Context: "context", Incarnation: "incarnation"}
	t.Cleanup(func() {
		if e := client.Del(context.Background(), store.contextKey(id.Context), store.contextDeadlineKey(id.Context), store.workerKey("worker"), store.workerTimerKey("worker"), store.workerReadyKey("worker"), store.ownerExpiryKey(), store.coordinatorKey()).Err(); e != nil {
			t.Error(e)
		}
	})
	message, err := portable.BuildInitialRequest(portable.InitialRequest{Method: sip.NOTIFY, URI: "sip:phone@example.test", From: "<sip:alice@example.test>", CallID: "test-call", LocalTag: "tag", Branch: "z9hG4bK-test", CSeq: 1, Advertise: "192.0.2.1:5061", Transport: "tls"})
	if err != nil {
		t.Fatal(err)
	}
	data, err := selectedmessage.Marshal(message)
	if err != nil {
		t.Fatal(err)
	}
	output := OutputIntent{ID: "output1", Attempt: "attempt1", Worker: "worker", Epoch: "epoch", Connection: "connection", MessageField: "r:message", MetadataField: "r:metadata", WireHash: message.WireHash(), ProofHash: sha256.Sum256([]byte("proof1")), Deadline: time.Now().Add(time.Minute)}
	second := output
	second.ID = "output2"
	second.Attempt = "attempt2"
	second.ProofHash = sha256.Sum256([]byte("proof2"))
	assignment := TimerAssignment{Worker: "worker", Epoch: "epoch", Term: "epoch"}
	_, err = store.Commit(ctx, Publication{Identity: id, Create: true, Deadline: time.Now().Add(time.Minute), Changes: []Change{{Field: "r:message", Value: data}, {Field: "r:metadata", Value: []byte("metadata")}}, NewOutputs: []OutputIntent{output, second}, Timers: &TimerMutation{Target: assignment, Changes: []TimerChange{{Member: "duty", Due: time.Now().Add(100 * time.Millisecond)}}}})
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for len(seen) < 2 {
		select {
		case id := <-physical:
			seen[id] = true
		case <-time.After(5 * time.Second):
			t.Fatal("physical callbacks serialized", seen)
		}
	}
	select {
	case <-dueCalled:
	case <-time.After(3 * time.Second):
		t.Fatal("blocked sockets starved due callback")
	}
	time.Sleep(2200 * time.Millisecond)
	if d.Ready() != nil {
		t.Fatal("wake traffic starved healthy lease renewal")
	}
	cancel()
	select {
	case <-ended:
		t.Fatal("driver returned before physical callbacks finished")
	case <-time.After(100 * time.Millisecond):
	}
	unblock.Do(func() { close(allowReturn) })
}
