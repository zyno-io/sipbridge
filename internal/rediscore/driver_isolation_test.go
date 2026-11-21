package rediscore

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
	"github.com/redis/go-redis/v9"
	"sipbridge/internal/selectedmessage"
)

type isolationDriverFixture struct {
	driver       *Driver
	store        *Store
	client       *redis.Client
	lease        context.Context
	initialBound time.Time
}

func startIsolationDriver(t *testing.T, callbacks DriverConfig) isolationDriverFixture {
	t.Helper()
	config, client := connectionFixture(t)
	connection, err := NewConnection(config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	callbacks.Connection, callbacks.Worker, callbacks.Epoch = connection, "worker", "epoch"
	driver, err := NewDriver(callbacks)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- driver.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		_ = driver.Close()
		<-done
		// The fixture owns this UUID namespace, including observations left by a failed assertion.
		keys, err := client.Keys(context.Background(), config.Profile.Root+"*").Result()
		if err != nil {
			t.Error(err)
		} else if len(keys) != 0 {
			if err = client.Del(context.Background(), keys...).Err(); err != nil {
				t.Error(err)
			}
		}
	})
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(5 * time.Millisecond) {
		store, lease, until, err := driver.Current()
		if err == nil {
			return isolationDriverFixture{driver, store, client, lease, until}
		}
	}
	t.Fatal("worker failed to acquire initial lease")
	return isolationDriverFixture{}
}

func (f isolationDriverFixture) waitForPark(t *testing.T, id Identity) {
	t.Helper()
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
		count, err := f.client.Exists(context.Background(), f.store.root+"core:parked:"+id.Context).Result()
		if err != nil {
			t.Fatal(err)
		}
		if count == 1 {
			return
		}
	}
	t.Fatal("missing parked context", id.Context)
}

func TestDriverIsolatesRejectedTimerAcrossRenewal(t *testing.T) {
	ctx := context.Background()
	bad := Identity{Context: "bad", Incarnation: "bad-inc"}
	good := Identity{Context: "good", Incarnation: "good-inc"}
	var rejected atomic.Int32
	nativeFault := errors.New("invalid selected native record")
	finished := make(chan error, 1)
	f := startIsolationDriver(t, DriverConfig{
		NativeRecordError: nativeFault,
		Due: func(ctx context.Context, store *Store, due *DueContext) error {
			if due.Identity == bad {
				rejected.Add(1)
				return nativeFault
			}
			_, err := store.Commit(ctx, Publication{Identity: due.Identity, Revision: due.Revision, Deadline: time.Now().Add(time.Second),
				Timers: &TimerMutation{Expected: due.Assignment, Target: due.Assignment, RequireLease: true, Changes: []TimerChange{{Member: due.Member, Delete: true}}}})
			select {
			case finished <- err:
			default:
			}
			return err
		},
		ReadyOutput:  func(context.Context, *Store, ReadyOutput) error { return nil },
		OwnerExpired: func(context.Context, *Store, ExpiredApplicationContext) error { return nil },
	})
	assignment := TimerAssignment{Worker: "worker", Epoch: "epoch", Term: "epoch"}
	publish := func(id Identity, when time.Time) {
		t.Helper()
		_, err := f.store.Commit(ctx, Publication{Identity: id, Create: true, Deadline: time.Now().Add(time.Second),
			Timers: &TimerMutation{Target: assignment, Changes: []TimerChange{{Member: "duty", Due: when}}}})
		if err != nil {
			t.Fatal(err)
		}
	}
	head := time.Now().Add(-time.Second)
	publish(bad, head)
	f.waitForPark(t, bad)
	publish(good, head.Add(time.Millisecond))
	select {
	case err := <-finished:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("bad earliest timer blocked an unrelated timer")
	}
	original, err := f.client.ZScore(ctx, f.store.contextDeadlineKey(bad.Context), "duty").Result()
	if err != nil || int64(original) != head.UnixMilli() {
		t.Fatal("parking changed semantic deadline", original, err)
	}
	// Wait through the original conservative bound so an immediate recheck cannot hide a lost renewal.
	select {
	case <-f.lease.Done():
		t.Fatal("item fault canceled worker lease")
	case <-time.After(time.Until(f.initialBound) + 50*time.Millisecond):
	}
	_, active, until, err := f.driver.Current()
	if err != nil || active != f.lease || !until.After(f.initialBound) || rejected.Load() != 1 {
		t.Fatal("worker did not renew continuously or redispatched parked work", err, rejected.Load())
	}
	if err = f.client.HSet(ctx, f.store.workerKey("worker"), "epoch", "replacement").Err(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-f.lease.Done():
	case <-time.After(3 * time.Second):
		t.Fatal("actual worker epoch replacement failed to fence binding")
	}
}

func TestDriverReleasesHealthyExpiredOwnerBesideParkedContext(t *testing.T) {
	ctx := context.Background()
	nativeFault := errors.New("invalid selected native record")
	bad := Identity{Context: "bad-owner", Incarnation: "bad-inc", Owner: "app", Term: "old"}
	finished := make(chan error, 2)
	f := startIsolationDriver(t, DriverConfig{
		NativeRecordError: nativeFault,
		Due:               func(context.Context, *Store, *DueContext) error { return nil },
		ReadyOutput:       func(context.Context, *Store, ReadyOutput) error { return nil },
		OwnerExpired: func(ctx context.Context, store *Store, item ExpiredApplicationContext) error {
			if item.Identity == bad {
				return nativeFault
			}
			_, err := store.Commit(ctx, Publication{Identity: item.Identity, Revision: item.Revision, Deadline: time.Now().Add(time.Second), ReleaseApplication: true})
			select {
			case finished <- err:
			default:
			}
			return err
		},
	})
	publish := func(app, term string, contexts ...string) {
		t.Helper()
		appLease, err := f.store.AcquireApplication(ctx, app, term)
		if err != nil {
			t.Fatal(err)
		}
		for _, name := range contexts {
			id := Identity{Context: name, Incarnation: name + "-inc", Owner: app, Term: term}
			if name == bad.Context {
				id = bad
			}
			_, err = f.store.Commit(ctx, Publication{Identity: id, Create: true, Deadline: time.Now().Add(time.Second), Application: &appLease.ApplicationLeaseRef, Changes: []Change{{Field: "r:context", Value: []byte{1}}}})
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	expire := func(term string, when time.Time) {
		t.Helper()
		pipe := f.client.TxPipeline()
		pipe.HSet(ctx, f.store.applicationOwnerKey(term), "expires_ms", when.UnixMilli())
		pipe.ZAdd(ctx, f.store.ownerExpiryKey(), redis.Z{Score: float64(when.UnixMilli()), Member: "a-" + term})
		pipe.Publish(ctx, f.store.root+"core:wake:worker", "1")
		if _, err := pipe.Exec(ctx); err != nil {
			t.Fatal(err)
		}
	}
	publish("app", "old", bad.Context, "same-term")
	publish("second", "later", "other-term")
	head := time.Now().Add(-time.Second)
	expire("old", head)
	f.waitForPark(t, bad)
	expire("later", head.Add(time.Millisecond))
	for i := 0; i < 2; i++ {
		select {
		case err := <-finished:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("parked owner blocked healthy release", i)
		}
	}
	for term, name := range map[string]string{"old": "same-term", "later": "other-term"} {
		member, err := f.client.SIsMember(ctx, f.store.applicationOwnedKey(term), name).Result()
		if err != nil || member {
			t.Fatal("healthy context retained ownership", term, name, err)
		}
	}
	_, lease, _, err := f.driver.Current()
	if err != nil || lease != f.lease || f.lease.Err() != nil {
		t.Fatal("owner failure fenced worker", err)
	}
}

func TestDriverAbandonsParkedContextAndReleasesItsIndexes(t *testing.T) {
	ctx := context.Background()
	bad := Identity{Context: "parked", Incarnation: "bad-inc", Owner: "app", Term: "term"}
	good := Identity{Context: "healthy", Incarnation: "good-inc"}
	nativeFault := errors.New("invalid selected native record")
	events := make(chan DriverDiagnostic, 8)
	f := startIsolationDriver(t, DriverConfig{
		NativeRecordError: nativeFault,
		Due:               func(context.Context, *Store, *DueContext) error { return nativeFault },
		ReadyOutput:       func(context.Context, *Store, ReadyOutput) error { return nil },
		OwnerExpired:      func(context.Context, *Store, ExpiredApplicationContext) error { return nil },
		OnDiagnostic: func(event DriverDiagnostic) {
			if event.Context == bad.Context {
				events <- event
			}
		},
	})
	badSelector := Selector{Kind: SelectorDialog, Digest: [32]byte{0xff, 1}}
	goodSelector := Selector{Kind: SelectorDialog, Digest: [32]byte{0xff, 2}}
	now := time.Now()
	publish := func(id Identity, selector Selector) {
		t.Helper()
		_, err := f.store.Commit(ctx, Publication{Identity: id, Create: true, Deadline: now.Add(time.Second), CreditDelta: 8192,
			Changes:   []Change{{Field: "r:context", Value: []byte("opaque")}},
			Selectors: []SelectorChange{{Selector: selector, Record: "r:context"}}})
		if err != nil {
			t.Fatal(err)
		}
	}
	publish(good, goodSelector)
	unbound := Identity{Context: bad.Context, Incarnation: bad.Incarnation}
	publish(unbound, badSelector)
	lease, err := f.store.AcquireApplication(ctx, bad.Owner, bad.Term)
	if err != nil {
		t.Fatal(err)
	}
	_, err = f.store.Commit(ctx, Publication{Identity: unbound, Revision: 1, Deadline: now.Add(time.Second),
		BindApplication: true, Application: &ApplicationLeaseRef{Application: lease.Application, Term: lease.Term},
		Timers: &TimerMutation{Target: TimerAssignment{Worker: "worker", Epoch: "epoch", Term: "epoch"},
			Changes: []TimerChange{{Member: "duty", Due: now}}}})
	if err != nil {
		t.Fatal(err)
	}
	wait := func(stage string) {
		t.Helper()
		select {
		case event := <-events:
			if event.Stage != stage || event.Incarnation != bad.Incarnation || event.Revision != "2" {
				t.Fatal("unexpected context diagnostic", event)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("missing context diagnostic", stage)
		}
	}
	wait("context-parked")
	index := f.store.ownerExpiryKey()
	member := "p-" + bad.Context
	due, err := f.client.ZScore(ctx, index, member).Result()
	if err != nil || due < float64(now.Add(parkedCallGrace).UnixMilli()) {
		t.Fatal("park did not persist its grace deadline", due, err)
	}
	// Move the fixture's clock forward to the stored expiry.
	if err = f.client.ZAdd(ctx, index, redis.Z{Member: member, Score: float64(time.Now().UnixMilli() - 1)}).Err(); err != nil {
		t.Fatal(err)
	}
	wait("context-abandoned")
	if _, err = f.store.LookupSelector(ctx, badSelector); !errors.Is(err, ErrAbsent) {
		t.Fatal("abandoned selector still present", err)
	}
	if _, err = f.store.LookupSelector(ctx, goodSelector); err != nil {
		t.Fatal("healthy selector was changed", err)
	}
	count, err := f.client.Exists(ctx, f.store.contextKey(bad.Context), f.store.contextDeadlineKey(bad.Context),
		f.store.applicationOwnedKey(bad.Term), f.store.root+"core:parked:"+bad.Context).Result()
	if err != nil || count != 0 {
		t.Fatal("abandoned state remained", count, err)
	}
	if _, err = f.client.ZScore(ctx, f.store.workerTimerKey("worker"), bad.Context).Result(); !errors.Is(err, redis.Nil) {
		t.Fatal("abandoned worker deadline remained", err)
	}
	checkNamespace(t, f.store, f.client, good)
	if f.lease.Err() != nil {
		t.Fatal("automatic abandonment fenced worker")
	}
}

func TestDriverIsolatesOutputFaultsAndPreservesStartedAttempt(t *testing.T) {
	ctx := context.Background()
	nativeFault := errors.New("invalid selected native record")
	bad := Identity{Context: "bad-output", Incarnation: "bad-inc"}
	unknown := Identity{Context: "ambiguous-output", Incarnation: "unknown-inc"}
	good := Identity{Context: "good-output", Incarnation: "good-inc"}
	started := make(chan error, 1)
	finished := make(chan error, 1)
	var ambiguousCalls atomic.Int32
	f := startIsolationDriver(t, DriverConfig{
		NativeRecordError: nativeFault, Due: func(context.Context, *Store, *DueContext) error { return nil },
		OwnerExpired: func(context.Context, *Store, ExpiredApplicationContext) error { return nil },
		ReadyOutput: func(ctx context.Context, store *Store, item ReadyOutput) error {
			switch item.Ref.Context {
			case bad.Context:
				return nativeFault
			case unknown.Context:
				ambiguousCalls.Add(1)
				_, err := store.ReserveOutput(ctx, item.Ref)
				select {
				case started <- err:
				default:
				}
				if err != nil {
					return err
				}
				return ErrUnknown
			default:
				_, err := store.ReserveOutput(ctx, item.Ref)
				if err == nil {
					err = store.CompleteOutput(ctx, item.Ref, "WRITTEN")
				}
				select {
				case finished <- err:
				default:
				}
				return err
			}
		},
	})
	message, err := portable.BuildInitialRequest(portable.InitialRequest{Method: sip.NOTIFY, URI: "sip:phone@example.test", From: "<sip:alice@example.test>", CallID: "isolation", LocalTag: "tag", Branch: "z9hG4bK-isolation", CSeq: 1, Advertise: "192.0.2.1:5061", Transport: "tls"})
	if err != nil {
		t.Fatal(err)
	}
	data, err := selectedmessage.Marshal(message)
	if err != nil {
		t.Fatal(err)
	}
	head := time.Now().Add(time.Minute)
	publish := func(id Identity, deadline time.Time) OutputRef {
		t.Helper()
		intent := OutputIntent{ID: "output", Attempt: "attempt", Worker: "worker", Epoch: "epoch", Connection: "flow", MessageField: "r:message", MetadataField: "r:metadata", WireHash: message.WireHash(), ProofHash: sha256.Sum256([]byte(id.Context)), Deadline: deadline}
		publishOutput(t, f.store, id, intent, data)
		return outputRef(id, intent)
	}
	badRef := publish(bad, head)
	f.waitForPark(t, bad)
	unknownRef := publish(unknown, head.Add(time.Second))
	select {
	case err = <-started:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("bad ready head blocked independent reservation")
	}
	goodRef := publish(good, head.Add(2*time.Second))
	select {
	case err = <-finished:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("ambiguous physical outcome blocked unrelated output")
	}
	for _, test := range []struct {
		ref  OutputRef
		want OutputState
	}{{badRef, OutputPending}, {unknownRef, OutputStarted}, {goodRef, OutputWritten}} {
		proof, err := f.store.InspectOutput(ctx, test.ref)
		if err != nil || proof.State != test.want {
			t.Fatal("physical evidence changed", test.ref.Context, proof.State, err)
		}
	}
	result, err := f.store.ReserveOutput(ctx, unknownRef)
	if !errors.Is(err, ErrUnknown) || len(result.Wire) != 0 || ambiguousCalls.Load() != 1 {
		t.Fatal("ambiguous write became eligible again", err, ambiguousCalls.Load())
	}
	_, lease, _, err := f.driver.Current()
	if err != nil || lease != f.lease || f.lease.Err() != nil {
		t.Fatal("physical item error fenced binding", err)
	}
}

func TestParkedAbandonmentWaitsForPhysicalCutoffAndSurvivesCoordinatorChange(t *testing.T) {
	ctx := context.Background()
	s, client, id := fixture(t)
	old := WorkerLeaseRequest{Worker: "old", Epoch: "old-epoch", PrimaryRunID: s.primary, Acquire: true}
	if _, err := s.RenewWorker(ctx, old); err != nil {
		t.Fatal(err)
	}
	message, err := portable.BuildInitialRequest(portable.InitialRequest{Method: sip.NOTIFY, URI: "sip:phone@example.test", From: "<sip:alice@example.test>", CallID: "abandon", LocalTag: "tag", Branch: "z9hG4bK-abandon", CSeq: 1, Advertise: "192.0.2.1:5061", Transport: "tls"})
	if err != nil {
		t.Fatal(err)
	}
	data, err := selectedmessage.Marshal(message)
	if err != nil {
		t.Fatal(err)
	}
	cutoff := time.Now().Add(time.Minute).Truncate(time.Millisecond)
	intent := OutputIntent{ID: "output", Attempt: "attempt", Worker: old.Worker, Epoch: old.Epoch, Connection: "flow", MessageField: "r:message", MetadataField: "r:metadata", WireHash: message.WireHash(), ProofHash: sha256.Sum256([]byte(id.Context)), Deadline: cutoff}
	pending := intent
	pending.ID = "pending"
	_, err = s.Commit(ctx, Publication{Identity: id, Create: true, Deadline: cutoff,
		Changes:    []Change{{Field: intent.MessageField, Value: data}, {Field: intent.MetadataField, Value: []byte("metadata")}},
		NewOutputs: []OutputIntent{intent, pending}})
	if err != nil {
		t.Fatal(err)
	}
	ref := outputRef(id, intent)
	if _, err = s.ReserveOutput(ctx, ref); err != nil {
		t.Fatal(err)
	}
	if err = s.park(ctx, old.Worker, old.Epoch, id, 1, OutputRef{}); err != nil {
		t.Fatal(err)
	}
	if _, err = s.ReserveOutput(ctx, outputRef(id, pending)); !errors.Is(err, ErrParked) {
		t.Fatal("parked context admitted a new write", err)
	}
	t.Cleanup(func() {
		if err := client.Del(ctx, s.ownerExpiryKey(), s.root+"core:parked:"+id.Context, s.coordinatorKey(), s.workerKey(old.Worker),
			s.workerKey("new"), s.workerReadyKey(old.Worker)).Err(); err != nil {
			t.Error(err)
		}
	})
	makeDue := func() {
		t.Helper()
		if err := client.ZAdd(ctx, s.ownerExpiryKey(), redis.Z{Member: "p-" + id.Context, Score: float64(time.Now().UnixMilli() - 1)}).Err(); err != nil {
			t.Fatal(err)
		}
	}
	makeDue()
	if event, err := s.abandonParked(ctx, old.Worker, old.Epoch, id.Context); err != nil || event.Context != "" {
		t.Fatal("attempt still inside its physical cutoff was abandoned", event, err)
	}
	score, err := client.ZScore(ctx, s.ownerExpiryKey(), "p-"+id.Context).Result()
	if err != nil || int64(score) != cutoff.Add(parkedCallGrace).UnixMilli() {
		t.Fatal("unexpected rescheduled deadline", score, err)
	}
	if err = s.CompleteOutput(ctx, ref, "WRITTEN"); err != nil {
		t.Fatal("already started write could not report success after parking", err)
	}
	output, err := s.InspectOutput(ctx, ref)
	if err != nil || output.State != OutputWritten {
		t.Fatal("definite physical outcome not retained during grace", output.State, err)
	}
	makeDue()
	if err = client.HSet(ctx, s.contextKey(id.Context), "o:"+intent.ID+":d", "0").Err(); err != nil {
		t.Fatal(err)
	}
	if _, err = s.abandonParked(ctx, old.Worker, old.Epoch, id.Context); !errors.Is(err, ErrCorrupt) {
		t.Fatal("invalid physical deadline allowed abandonment", err)
	}
	// Advance the fixture through both the coordinator and output deadlines.
	expired := time.Now().Add(-parkedCallGrace - time.Second).UnixMilli()
	if err = client.HSet(ctx, s.coordinatorKey(), "expires_ms", expired).Err(); err != nil {
		t.Fatal(err)
	}
	if err = client.HSet(ctx, s.contextKey(id.Context), "o:"+intent.ID+":d", expired, "o:"+pending.ID+":d", expired).Err(); err != nil {
		t.Fatal(err)
	}
	makeDue()
	if _, err = s.abandonParked(ctx, old.Worker, old.Epoch, id.Context); !errors.Is(err, ErrAuthority) {
		t.Fatal("expired coordinator could abandon context", err)
	}
	next := WorkerLeaseRequest{Worker: "new", Epoch: "new-epoch", PrimaryRunID: s.primary, Acquire: true}
	if lease, err := s.RenewWorker(ctx, next); err != nil || !lease.Coordinator {
		t.Fatal("replacement coordinator did not acquire authority", lease, err)
	}
	if event, err := s.abandonParked(ctx, next.Worker, next.Epoch, id.Context); err != nil || event.Incarnation != id.Incarnation {
		t.Fatal("replacement coordinator did not abandon the parked context", event, err)
	}
	if _, err = s.InspectOutput(ctx, ref); !errors.Is(err, ErrAuthority) {
		t.Fatal("expired physical proof still had context authority", err)
	}
	if _, err = client.ZScore(ctx, s.workerReadyKey(old.Worker), id.Context+"/"+pending.ID).Result(); !errors.Is(err, redis.Nil) {
		t.Fatal("abandoned ready output remained", err)
	}
	checkNamespace(t, s, client)
}

func TestDriverSelectsDuePastMalformedEqualScorePage(t *testing.T) {
	ctx := context.Background()
	finished := make(chan error, 1)
	f := startIsolationDriver(t, DriverConfig{
		Due: func(ctx context.Context, store *Store, item *DueContext) error {
			_, err := store.Commit(ctx, Publication{Identity: item.Identity, Revision: item.Revision, Deadline: time.Now().Add(time.Second), Timers: &TimerMutation{Expected: item.Assignment, Target: item.Assignment, RequireLease: true, Changes: []TimerChange{{Member: item.Member, Delete: true}}}})
			select {
			case finished <- err:
			default:
			}
			return err
		},
		ReadyOutput:  func(context.Context, *Store, ReadyOutput) error { return nil },
		OwnerExpired: func(context.Context, *Store, ExpiredApplicationContext) error { return nil },
	})
	// A lexicographically later valid member at the same score exercises page continuation within a tie.
	when := time.Now().Add(time.Second)
	orphans := make([]redis.Z, 129)
	for i := range orphans {
		orphans[i] = redis.Z{Score: float64(when.UnixMilli()), Member: fmt.Sprintf("a-orphan-%03d", i)}
	}
	good := Identity{Context: "z-good", Incarnation: "good-inc"}
	assignment := TimerAssignment{Worker: "worker", Epoch: "epoch", Term: "epoch"}
	_, err := f.store.Commit(ctx, Publication{Identity: good, Create: true, Deadline: time.Now().Add(time.Second), Timers: &TimerMutation{Target: assignment, Changes: []TimerChange{{Member: "duty", Due: when}}}})
	if err != nil {
		t.Fatal(err)
	}
	if err = f.client.ZAdd(ctx, f.store.workerTimerKey("worker"), orphans...).Err(); err != nil {
		t.Fatal(err)
	}
	if !time.Now().Before(when) {
		t.Fatal("fixture did not install corrupt prefix before healthy timer became due")
	}
	select {
	case err = <-finished:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(8 * time.Second):
		t.Fatal("malformed timer prefix hid valid equal-score context")
	}
	_, lease, _, err := f.driver.Current()
	if err != nil || lease != f.lease || f.lease.Err() != nil {
		t.Fatal("per-item index corruption fenced worker", err)
	}
}

func TestDriverTransfersHealthyAndParkedTimersPastCorruptExpiredWorkerEntry(t *testing.T) {
	ctx := context.Background()
	bad := Identity{Context: "b-parked", Incarnation: "bad-inc"}
	good := Identity{Context: "c-healthy", Incarnation: "good-inc"}
	var badCallbacks atomic.Int32
	finished := make(chan error, 1)
	f := startIsolationDriver(t, DriverConfig{
		Due: func(ctx context.Context, store *Store, item *DueContext) error {
			if item.Identity != good {
				badCallbacks.Add(1)
				return errors.New("unexpected timer callback")
			}
			_, err := store.Commit(ctx, Publication{Identity: item.Identity, Revision: item.Revision, Deadline: time.Now().Add(time.Second),
				Timers: &TimerMutation{Expected: item.Assignment, Target: item.Assignment, RequireLease: true, Changes: []TimerChange{{Member: item.Member, Delete: true}}}})
			select {
			case finished <- err:
			default:
			}
			return err
		},
		ReadyOutput:  func(context.Context, *Store, ReadyOutput) error { return nil },
		OwnerExpired: func(context.Context, *Store, ExpiredApplicationContext) error { return nil },
	})
	source := TimerAssignment{Worker: "old-worker", Epoch: "old-epoch", Term: "old-term"}
	target := TimerAssignment{Worker: "worker", Epoch: "epoch", Term: "epoch"}
	_, err := f.store.RenewWorker(ctx, WorkerLeaseRequest{Worker: source.Worker, Epoch: source.Epoch, Acquire: true, PrimaryRunID: f.store.primary})
	if err != nil {
		t.Fatal(err)
	}
	due := time.Now().Add(-time.Second)
	for _, id := range []Identity{bad, good} {
		_, err = f.store.Commit(ctx, Publication{Identity: id, Create: true, Deadline: time.Now().Add(time.Second), Timers: &TimerMutation{Target: source, Changes: []TimerChange{{Member: "duty", Due: due}}}})
		if err != nil {
			t.Fatal(err)
		}
	}
	if err = f.store.park(ctx, source.Worker, source.Epoch, bad, 1, OutputRef{}); err != nil {
		t.Fatal(err)
	}
	// Keep the malformed member ahead of both valid contexts in the source projection.
	pipe := f.client.TxPipeline()
	pipe.ZAdd(ctx, f.store.workerTimerKey(source.Worker), redis.Z{Score: float64(due.UnixMilli() - 1), Member: "a-orphan"})
	pipe.HSet(ctx, f.store.workerKey(source.Worker), "expires_ms", due.UnixMilli())
	pipe.ZAdd(ctx, f.store.ownerExpiryKey(), redis.Z{Score: float64(due.UnixMilli()), Member: "w-" + source.Worker})
	pipe.Publish(ctx, f.store.root+"core:wake:worker", "1")
	if _, err = pipe.Exec(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case err = <-finished:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("corrupt source timer blocked expired-worker transfer")
	}
	view, err := f.store.Read(ctx, bad)
	if err != nil || view.Revision != 2 || view.Timer != target {
		t.Fatal("parked context reassignment", view, err)
	}
	marker, err := f.client.Get(ctx, f.store.root+"core:parked:"+bad.Context).Result()
	if err != nil || marker != bad.Incarnation {
		t.Fatal("parking marker changed on reassignment", marker, err)
	}
	score, err := f.client.ZScore(ctx, f.store.workerTimerKey(target.Worker), bad.Context).Result()
	if err != nil || int64(score) != due.UnixMilli() {
		t.Fatal("parked timer lost its canonical score", score, err)
	}
	if count, err := f.client.ZCard(ctx, f.store.workerTimerKey(source.Worker)).Result(); err != nil || count != 1 {
		t.Fatal("unexpected remaining source members", count, err)
	}
	if selected, err := f.store.PeekDue(ctx, target.Worker, target.Epoch); err != nil || selected != nil {
		t.Fatal("parked timer became selectable on replacement worker", selected, err)
	}
	_, lease, _, err := f.driver.Current()
	if err != nil || lease != f.lease || f.lease.Err() != nil || badCallbacks.Load() != 0 {
		t.Fatal("worker rebound or dispatched parked context", err, badCallbacks.Load())
	}
}
