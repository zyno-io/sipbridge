package rediscore

import (
	"context"
	"crypto/sha256"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func pendingApplicationOutputFixture(t *testing.T, lifetime ...time.Duration) (*Store, *redis.Client, Identity, OutputRef) {
	t.Helper()
	s, client, id, out, wire := outputFixture(t)
	ctx := context.Background()
	lease, err := s.AcquireApplication(ctx, "controller", "term")
	if err != nil {
		t.Fatal(err)
	}
	id.Owner, id.Term = lease.Application, lease.Term
	out.Application = &lease.ApplicationLeaseRef
	out.DeliveryTag = sha256.Sum256([]byte("selected route"))
	out.Dependencies = []string{"r:procedure"}
	var timers *TimerMutation
	if len(lifetime) != 0 {
		out.Deadline = time.Now().Add(lifetime[0])
		timers = &TimerMutation{Target: TimerAssignment{Worker: out.Worker, Epoch: out.Epoch, Term: out.Epoch}, Changes: []TimerChange{{Member: "out-" + out.MetadataField[2:], Due: out.Deadline}}}
	}
	_, err = s.Commit(ctx, Publication{
		Identity: id, Create: true, Deadline: out.Deadline, Application: out.Application, Reserved: 4096,
		Changes:    []Change{{Field: out.MessageField, Value: wire}, {Field: out.MetadataField, Value: []byte("metadata")}, {Field: "r:delivery-tag", Value: out.DeliveryTag[:]}, {Field: "r:procedure", Value: []byte("before")}},
		NewOutputs: []OutputIntent{out}, Timers: timers,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := client.Del(ctx, s.applicationKey(id.Owner), s.applicationOwnerKey(id.Term), s.applicationOwnedKey(id.Term), s.ownerExpiryKey(), s.applicationAdmissionKey(), s.workerTimerKey(out.Worker), s.contextDeadlineKey(id.Context)).Err(); err != nil {
			t.Error(err)
		}
	})
	return s, client, id, outputRef(id, out)
}

func TestParkPendingApplicationOutput(t *testing.T) {
	s, client, id, ref := pendingApplicationOutputFixture(t)
	ctx := context.Background()
	if err := s.ParkPendingApplicationOutput(ctx, id, 2, ref); !errors.Is(err, ErrConflict) {
		t.Fatalf("stale context revision: %v", err)
	}
	wrong := ref
	wrong.Connection = "another-socket"
	if err := s.ParkPendingApplicationOutput(ctx, id, 1, wrong); !errors.Is(err, ErrConflict) {
		t.Fatalf("wrong socket: %v", err)
	}
	if err := s.ParkPendingApplicationOutput(ctx, id, 1, ref); err != nil {
		t.Fatal(err)
	}
	if err := s.ParkPendingApplicationOutput(ctx, id, 1, ref); !errors.Is(err, ErrConflict) {
		t.Fatalf("terminal output supplied new revocation proof: %v", err)
	}
	if got, err := s.ReserveOutput(ctx, ref); !errors.Is(err, ErrUnknown) || got.Outcome != "" || len(got.Wire) > 0 {
		t.Fatalf("parked output entered the writer: %+v %v", got, err)
	}
	for _, outcome := range []string{"WRITTEN", "NOT_WRITTEN", "UNKNOWN"} {
		if err := s.CompleteOutput(ctx, ref, outcome); !errors.Is(err, ErrConflict) {
			t.Fatalf("stale %s completion changed parked output: %v", outcome, err)
		}
	}
	if err := s.SettleOutput(ctx, ref, true, true); !errors.Is(err, ErrConflict) {
		t.Fatalf("parked output released before its semantic cutoff: %v", err)
	}
	checkNamespace(t, s, client, id)
}

func TestParkPendingApplicationOutputRacesReservation(t *testing.T) {
	s, client, id, ref := pendingApplicationOutputFixture(t)
	ctx := context.Background()
	begin := make(chan struct{})
	var reserved OutputResult
	var reserveError, parkError error
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		<-begin
		reserved, reserveError = s.ReserveOutput(ctx, ref)
	}()
	go func() {
		defer wg.Done()
		<-begin
		parkError = s.ParkPendingApplicationOutput(ctx, id, 1, ref)
	}()
	close(begin)
	wg.Wait()
	if parkError == nil {
		if !errors.Is(reserveError, ErrUnknown) || len(reserved.Wire) > 0 || reserved.Outcome != "" {
			t.Fatalf("park let reservation escape: %+v %v", reserved, reserveError)
		}
	} else if !errors.Is(parkError, ErrConflict) || reserveError != nil || len(reserved.Wire) == 0 || reserved.Outcome != "" {
		t.Fatalf("reservation did not exclusively win: %+v %v", reserved, parkError)
	}
	checkNamespace(t, s, client, id)
}

func TestParkPendingApplicationOutputRejectsPriorReservation(t *testing.T) {
	for _, outcome := range []string{"STARTED", "UNKNOWN", "WRITTEN", "NOT_WRITTEN"} {
		t.Run(outcome, func(t *testing.T) {
			s, _, id, ref := pendingApplicationOutputFixture(t)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if got, err := s.ReserveOutput(ctx, ref); err != nil || len(got.Wire) == 0 {
				t.Fatalf("initial reservation: %+v %v", got, err)
			}
			if outcome != "STARTED" {
				if err := s.CompleteOutput(ctx, ref, outcome); err != nil {
					t.Fatal(err)
				}
			}
			if err := s.ParkPendingApplicationOutput(ctx, id, 1, ref); !errors.Is(err, ErrConflict) {
				t.Fatalf("prior reservation produced fresh revoke proof: %v", err)
			}
		})
	}
}

func TestParkPendingApplicationOutputLostReplyRemainsUnknown(t *testing.T) {
	s, client, id, ref := pendingApplicationOutputFixture(t)
	ctx := context.Background()
	peerClient := redis.NewClient(&redis.Options{Addr: client.Options().Addr, MaxRetries: -1})
	defer peerClient.Close()
	peerClient.AddHook(&loseReply{armed: true})
	peer, err := New(peerClient, Config{Root: s.root, Profile: s.profile, RecordBudget: s.maxBytes, PrimaryRunID: s.primary})
	if err != nil {
		t.Fatal(err)
	}
	if err = peer.ParkPendingApplicationOutput(ctx, id, 1, ref); !errors.Is(err, ErrUnknown) {
		t.Fatalf("lost reply produced fresh proof: %v", err)
	}
	proof, err := s.InspectOutput(ctx, ref)
	if err != nil || proof.State != OutputResumeParked {
		t.Fatalf("server park did not commit: %+v %v", proof, err)
	}
	if err = s.ParkPendingApplicationOutput(ctx, id, 1, ref); !errors.Is(err, ErrConflict) {
		t.Fatalf("retry invented fresh proof: %v", err)
	}
}

func TestParkPendingApplicationOutputPreservesOriginalCutoff(t *testing.T) {
	s, client, id, ref := pendingApplicationOutputFixture(t, 200*time.Millisecond)
	ctx := context.Background()
	if err := s.ParkPendingApplicationOutput(ctx, id, 1, ref); err != nil {
		t.Fatal(err)
	}
	proof, err := s.InspectOutput(ctx, ref)
	if err != nil || proof.State != OutputResumeParked {
		t.Fatalf("park preserved the wrong proof: %+v %v", proof, err)
	}
	if due, err := s.PeekDue(ctx, ref.Worker, ref.Epoch); err != nil || due != nil {
		t.Fatalf("park made the original cutoff due early: %+v %v", due, err)
	}
	if wait := time.Until(time.UnixMilli(proof.DeadlineMS).Add(15 * time.Millisecond)); wait > 0 {
		time.Sleep(wait)
	}
	due, err := s.PeekDue(ctx, ref.Worker, ref.Epoch)
	if err != nil || due == nil || due.DeadlineMS != proof.DeadlineMS {
		t.Fatalf("original cutoff disappeared: %+v %v", due, err)
	}
	if phase, err := s.ExpireOutput(ctx, due, ref); err != nil || phase != OutputNotWritten {
		t.Fatalf("original cutoff did not classify parked attempt: %v %v", phase, err)
	}
	if err := s.SettleOutput(ctx, ref, true, true); err != nil {
		t.Fatal(err)
	}
	checkNamespace(t, s, client, id)
}
