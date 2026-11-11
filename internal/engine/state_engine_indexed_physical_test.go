package engine

import (
	"bytes"
	"context"
	"testing"

	"sipbridge/internal/state"
)

func TestFollowerIndexedPhysicalDeliveryUsesRetainedWorkerWithoutTimerLeadership(t *testing.T) {
	for _, backend := range []string{state.MemoryBackend, state.RedisBackend} {
		t.Run(backend, func(t *testing.T) {
			f, ingress, conn := casOptionsFixture(t, backend)
			ctx, cancel := context.WithCancel(context.Background())
			f.engine.ctx, f.engine.cancel = ctx, cancel
			f.engine.work, f.engine.background = make(chan struct{}, 64), make(chan struct{}, 48)
			f.engine.dispatching = make(map[string]bool)
			f.engine.cfg.WriteCommitted = ingress.committedResult
			t.Cleanup(f.engine.Close)
			write := casOptionsResponse(t, f, "indexed-follower")
			ref, err := f.engine.group(write.Effect.Context)
			if err != nil {
				t.Fatal(err)
			}
			worker := f.engine.dispatchWorker(f.engine.cfg.Process)
			hints, err := f.engine.store.DispatchHints(ctx, worker, 8)
			if err != nil || len(hints) != 1 || hints[0].Ref.ID != ref.ID {
				t.Fatal("native response did not index the exact physical worker", err)
			}
			// Enter only the physical dispatcher, through a fresh backend load.
			// No shared timer discovery, leader lease or connected controller runs.
			f.engine.schedulePhysicalGroup(worker, ref)
			f.engine.wg.Wait()
			if conn.deadlines != 1 || !bytes.HasPrefix(conn.sent.Bytes(), []byte("SIP/2.0 200")) {
				t.Fatal("indexed retained effect failed to reach its physical follower")
			}
			if stats := f.engine.SchedulerStatistics(); stats.Leader || stats.Acquisitions != 0 {
				t.Fatal("physical delivery acquired timer leadership")
			}
			f.engine.schedulePhysicalGroup(worker, ref)
			f.engine.wg.Wait()
			if conn.deadlines != 1 {
				t.Fatal("acknowledged physical work was written twice")
			}
		})
	}
}
