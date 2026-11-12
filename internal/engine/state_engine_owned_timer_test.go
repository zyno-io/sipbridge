package engine

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"sipbridge/internal/state"
)

type ownedTimerReads struct {
	state.Backend
	reads sync.Map
}

func (s *ownedTimerReads) ReadState(ctx context.Context, ref state.Ref) (state.StateView, int64, error) {
	s.reads.Store(ref.ID, true)
	return s.Backend.ReadState(ctx, ref)
}

func seedOwnedEngineTimer(t *testing.T, s state.Backend, worker string, delay time.Duration) state.Ref {
	t.Helper()
	ref, err := s.GroupForCallID(uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	input := state.Input{ID: uuid.NewString(), Kind: "seed", Fingerprint: "seed", Create: true,
		DeadlineMS: time.Now().Add(time.Second).UnixMilli(), ReceiptUntilMS: time.Now().Add(2 * time.Second).UnixMilli()}
	read, err := s.ReadUpdate(context.Background(), ref, input)
	if err != nil || read.Update == nil {
		t.Fatal(err)
	}
	due := read.Update.NowMS + delay.Milliseconds()
	if _, err := s.CompareAndSwap(context.Background(), *read.Update, state.Payload{TimerWorker: worker,
		Timers: []state.Timer{{ID: "test-timer", Generation: "1", DueMS: due, DeadlineMS: due}}}, nil); err != nil {
		t.Fatal(err)
	}
	return read.Update.Record.Ref
}

func TestOwnedTimerWorkersReadOnlyTheirOwnTimerGroups(t *testing.T) {
	for _, backend := range []string{state.MemoryBackend, state.RedisBackend} {
		t.Run(backend, func(t *testing.T) {
			s, peer := schedulerTestStore(t, backend)
			left, right := &ownedTimerReads{Backend: s}, &ownedTimerReads{Backend: peer}
			a, _ := startSchedulerTestEngine(t, left)
			b, _ := startSchedulerTestEngine(t, right)
			first := seedOwnedEngineTimer(t, s, a.dispatchWorker(a.cfg.Process), 150*time.Millisecond)
			second := seedOwnedEngineTimer(t, s, b.dispatchWorker(b.cfg.Process), 200*time.Millisecond)
			schedulerAwait(t, time.Now().Add(2*time.Second), func() bool {
				one, _, errOne := s.ReadState(context.Background(), first)
				two, _, errTwo := s.ReadState(context.Background(), second)
				return errOne == nil && errTwo == nil && len(one.Payload.Timers) == 0 && len(two.Payload.Timers) == 0
			})
			if _, foreign := left.reads.Load(second.ID); foreign {
				t.Fatal("left worker decoded right worker's healthy timer context")
			}
			if _, foreign := right.reads.Load(first.ID); foreign {
				t.Fatal("right worker decoded left worker's healthy timer context")
			}
			if _, own := left.reads.Load(first.ID); !own {
				t.Fatal("left worker did not execute its timer")
			}
			if _, own := right.reads.Load(second.ID); !own {
				t.Fatal("right worker did not execute its timer")
			}
		})
	}
}

func TestOwnedTimerWorkerLossTransfersDueWork(t *testing.T) {
	for _, backend := range []string{state.MemoryBackend, state.RedisBackend} {
		t.Run(backend, func(t *testing.T) {
			s, peer := schedulerTestStore(t, backend)
			a, _ := startSchedulerTestEngine(t, s)
			b, _ := startSchedulerTestEngine(t, peer)
			ref := seedOwnedEngineTimer(t, s, a.dispatchWorker(a.cfg.Process), 150*time.Millisecond)
			a.Close()
			schedulerAwait(t, time.Now().Add(state.DispatchWorkerLifetime+2*time.Second), func() bool {
				view, _, err := s.ReadState(context.Background(), ref)
				return err == nil && view.TimerWorker == b.dispatchWorker(b.cfg.Process) && len(view.Payload.Timers) == 0
			})
		})
	}
}
