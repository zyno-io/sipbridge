package engine

import (
	"context"
	"testing"
	"time"

	pb "sipbridge/internal/controlpb"
)

func TestResidentTimerDispatchDoesNotBlockScheduler(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	e := &ResidentMemory{ctx: context.Background()}
	e.cfg.WriteCommitted = func(context.Context, *pb.FlowWrite) *pb.DeliveryResult { close(started); <-release; return nil }
	returned := make(chan struct{})
	go func() { e.dispatchResidentTimerOutputs([]*pb.FlowWrite{{}}); close(returned) }()
	select {
	case <-returned:
	case <-time.After(time.Second):
		t.Fatal("timer dispatch blocked on physical writer")
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("physical writer not started")
	}
	close(release)
	e.wg.Wait()
}
