package rediscore

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"
)

func retainedWithoutCredit(t *testing.T, s *Store, id Identity) (int64, int64) {
	t.Helper()
	h, err := s.client.HGetAll(context.Background(), s.contextKey(id.Context)).Result()
	if err != nil {
		t.Fatal(err)
	}
	credit, err := strconv.ParseInt(h["reserved"], 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	indexes, err := strconv.ParseInt(h["index_bytes"], 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	actual := int64(len(s.contextKey(id.Context))) + indexes
	for field, value := range h {
		actual += int64(len(field))
		if field != "reserved" {
			actual += int64(len(value))
		}
	}
	return actual, credit
}

func TestReservationSpendUsesActualCombinedPublicationGrowth(t *testing.T) {
	s, client, id, output, wire := outputFixture(t)
	ctx := context.Background()
	lease, err := s.AcquireApplication(ctx, "application", "term1")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = client.Del(ctx, s.applicationKey(lease.Application), s.applicationOwnerKey(lease.Term), s.applicationOwnedKey(lease.Term), s.ownerExpiryKey(), s.contextDeadlineKey(id.Context), s.workerTimerKey(output.Worker)).Err()
	})
	_, err = s.Commit(ctx, Publication{Identity: id, Create: true, Deadline: time.Now().Add(time.Minute), Reserved: 20000})
	if err != nil {
		t.Fatal(err)
	}
	// Cross a revision decimal-width boundary in the same ordinary publication.
	if err = client.HSet(ctx, s.contextKey(id.Context), "revision", "9").Err(); err != nil {
		t.Fatal(err)
	}
	before, credit := retainedWithoutCredit(t, s, id)
	p := Publication{Identity: id, Revision: 9, Deadline: time.Now().Add(time.Minute), SpendReservation: true,
		Application: &lease.ApplicationLeaseRef, BindApplication: true,
		Changes:    []Change{{Field: output.MessageField, Value: wire}, {Field: output.MetadataField, Value: []byte("metadata")}},
		NewOutputs: []OutputIntent{output},
		Timers:     &TimerMutation{Target: TimerAssignment{Worker: output.Worker, Epoch: output.Epoch, Term: output.Epoch}, Changes: []TimerChange{{Member: "procedure", Due: time.Now().Add(time.Minute)}}}}
	if revision, err := s.Commit(ctx, p); err != nil || revision != 10 {
		t.Fatal("admitted credit could not fund exact native publication", revision, err)
	}
	id.Owner, id.Term = lease.Application, lease.Term
	after, remaining := retainedWithoutCredit(t, s, id)
	if remaining != credit-(after-before) || remaining >= credit {
		t.Fatalf("remaining credit %d, expected %d from actual growth %d", remaining, credit-(after-before), after-before)
	}
	checkNamespace(t, s, client, id)
}

func TestReservationSpendRefusesInsufficientCreditBeforeWrites(t *testing.T) {
	s, client, id := fixture(t)
	ctx := context.Background()
	_, err := s.Commit(ctx, Publication{Identity: id, Create: true, Deadline: time.Now().Add(time.Minute), Reserved: 1})
	if err != nil {
		t.Fatal(err)
	}
	p := Publication{Identity: id, Revision: 1, Deadline: time.Now().Add(time.Minute), SpendReservation: true, Changes: []Change{{Field: "r:new", Value: []byte("native record")}}}
	if _, err = s.Commit(ctx, p); !errors.Is(err, ErrCapacity) {
		t.Fatal("insufficient admitted credit accepted", err)
	}
	view, err := s.Read(ctx, id, "r:new")
	if err != nil || view.Revision != 1 || view.Reserved != 1 || view.Records["r:new"] != nil {
		t.Fatal("refused spending changed the native publication", err)
	}
	checkNamespace(t, s, client, id)
}
