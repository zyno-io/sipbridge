package rediscore

import (
	"context"
	"testing"
	"time"
)

func TestUDPWorkerHintSharesWakeWithoutHeadInvalidation(t *testing.T) {
	s, _, id := fixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	hints := make(chan UDPHint, 1)
	wake, err := s.SubscribeWakeResults(ctx, "worker1", func(ResultHint) {}, func(h UDPHint) { hints <- h })
	if err != nil {
		t.Fatal(err)
	}
	defer wake.Close()
	generation, changed, _ := wake.Snapshot()
	rev := publish(t, s, id, Change{Field: "r:procedure-n", Value: []byte("old")})
	hint := UDPHint{Worker: "worker1", Context: id.Context, Incarnation: id.Incarnation, Procedure: "r:procedure-n", Stop: true}
	_, err = s.Commit(ctx, Publication{Identity: id, Revision: rev, Deadline: time.Now().Add(time.Second), Changes: []Change{{Field: hint.Procedure, Value: []byte("new")}}, UDPHint: &hint})
	if err != nil {
		t.Fatal(err)
	}
	if err = wake.Receive(ctx); err != nil {
		t.Fatal(err)
	}
	if got := <-hints; got != hint {
		t.Fatalf("wrong UDP hint: %+v", got)
	}
	next, _, live := wake.Snapshot()
	if !live || next != generation {
		t.Fatal("UDP hint invalidated worker head")
	}
	select {
	case <-changed:
		t.Fatal("UDP hint woke scheduler")
	default:
	}
}
