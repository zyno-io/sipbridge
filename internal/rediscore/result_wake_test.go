package rediscore

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestResultWakeAtomicPublicationAndGeneration(t *testing.T) {
	s, _, id := fixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	hints := make(chan ResultHint, 2)
	wake, err := s.SubscribeWakeResults(ctx, "worker1", func(h ResultHint) { hints <- h })
	if err != nil {
		t.Fatal(err)
	}
	defer wake.Close()
	generation, changed, live := wake.Snapshot()
	if !live {
		t.Fatal("subscription not live")
	}
	rev := publish(t, s, id, Change{Field: "r:result-1", Value: []byte("pending")})
	hint := ResultHint{Context: id.Context, Incarnation: id.Incarnation, Sequence: 1}
	_, err = s.Commit(ctx, Publication{Identity: id, Revision: rev, Deadline: time.Now().Add(time.Minute), Changes: []Change{{Field: "r:result-1", Value: []byte("completed")}}, ResultHint: &hint})
	if err != nil {
		t.Fatal(err)
	}
	if err = wake.Receive(ctx); err != nil {
		t.Fatal(err)
	}
	if got := <-hints; got != hint {
		t.Fatalf("hint = %+v", got)
	}
	next, _, live := wake.Snapshot()
	if !live || next != generation {
		t.Fatal("result hint invalidated worker head")
	}
	select {
	case <-changed:
		t.Fatal("result hint woke worker scheduling")
	default:
	}
	_, err = s.Commit(ctx, Publication{Identity: id, Revision: rev, Deadline: time.Now().Add(time.Minute), Changes: []Change{{Field: "r:result-1", Value: []byte("bad")}}, ResultHint: &hint})
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("stale publication: %v", err)
	}
}

func TestResultHintBounds(t *testing.T) {
	h := ResultHint{Context: "context", Incarnation: "incarnation", Sequence: 1}
	b, err := encodeResultHint(h)
	if err != nil {
		t.Fatal(err)
	}
	got, err := decodeResultHint(b)
	if err != nil || got != h {
		t.Fatalf("roundtrip: %+v %v", got, err)
	}
	if _, err = decodeResultHint(append(b, 0)); !errors.Is(err, ErrCorrupt) {
		t.Fatal("trailing hint accepted")
	}
	h.Sequence = 0
	if _, err = encodeResultHint(h); !errors.Is(err, ErrConflict) {
		t.Fatal("zero sequence accepted")
	}
}

func TestResultWakeUnexpectedEpochFences(t *testing.T) {
	s, _, _ := fixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	wake, err := s.SubscribeWakeResults(ctx, "worker1", func(ResultHint) {})
	if err != nil {
		t.Fatal(err)
	}
	defer wake.Close()
	_, changed, _ := wake.Snapshot()
	if err = wake.subscription.Subscribe(ctx, wake.channel); err != nil {
		t.Fatal(err)
	}
	if err = wake.Receive(ctx); !errors.Is(err, ErrAuthority) {
		t.Fatalf("renewed epoch: %v", err)
	}
	_, _, live := wake.Snapshot()
	if live {
		t.Fatal("renewed epoch remained live")
	}
	select {
	case <-changed:
	default:
		t.Fatal("fence did not wake scheduler")
	}
}
