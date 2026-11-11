package state

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestDispatchIndexesRouteExactWorkerAndFenceStaleAcknowledgement(t *testing.T) {
	backendContract(t, func(t *testing.T, s Backend) {
		ctx := context.Background()
		a, b := strings.Repeat("a", 64), strings.Repeat("b", 64)
		ref := contractRef(t, s, "physical-dispatch")
		before, err := s.DispatchHead(ctx, a)
		if err != nil || before.DueMS != 0 {
			t.Fatal("empty process index invented work", err)
		}
		update := contractRead(t, s, ref, contractInput("first", true))
		payload := Payload{Effects: []Effect{{ID: "physical", Kind: "deliver", Status: EffectUnknown, AttemptID: "attempt", DeadlineMS: time.Now().Add(time.Minute).UnixMilli(), Worker: a}}}
		if _, err := s.CompareAndSwap(ctx, update, payload, nil); err != nil {
			t.Fatal(err)
		}
		ref = update.Record.Ref
		stored, err := s.Load(ctx, ref)
		if err != nil || len(stored.Payload.Effects) != 1 || stored.Payload.Effects[0].Worker != a {
			t.Fatal("dispatch index was written but the retained codec lost its exact physical worker", err)
		}
		wait, cancel := context.WithTimeout(ctx, time.Second)
		cursor, err := s.WaitSchedule(wait, before.Cursor)
		cancel()
		if err != nil || cursor == before.Cursor {
			t.Fatal("physical recovery did not wake without a shared ready/timer head", err)
		}
		first, err := s.DispatchHints(ctx, a, 8)
		if err != nil || len(first) != 1 || first[0].Ref.ID != ref.ID {
			t.Fatal("exact worker did not receive its physical hint", first, err)
		}
		other, err := s.DispatchHints(ctx, b, 8)
		if err != nil || len(other) != 0 {
			t.Fatal("physical hint crossed process/epoch identity", other, err)
		}
		update = contractRead(t, s, ref, contractInput("new-revision", false))
		if _, err := s.CompareAndSwap(ctx, update, payload, nil); err != nil {
			t.Fatal(err)
		}
		if err := s.AckDispatch(ctx, a, ref, "1"); err != nil {
			t.Fatal(err)
		}
		latest, err := s.DispatchHints(ctx, a, 8)
		if err != nil || len(latest) != 1 {
			t.Fatal("stale acknowledgement erased newer work", latest, err)
		}
		if err := s.AckDispatch(ctx, a, ref, "2"); err != nil {
			t.Fatal(err)
		}
		latest, err = s.DispatchHints(ctx, a, 8)
		if err != nil || len(latest) != 0 {
			t.Fatal("acknowledged hint retained a busy loop", latest, err)
		}
		update = contractRead(t, s, ref, contractInput("recover", false))
		if _, err := s.CompareAndSwap(ctx, update, payload, nil); err != nil {
			t.Fatal(err)
		}
		latest, err = s.DispatchHints(ctx, a, 8)
		if err != nil || len(latest) != 1 {
			t.Fatal("next committed recovery did not republish physical work", latest, err)
		}
		update = contractRead(t, s, ref, contractInput("settled", false))
		if _, err := s.CompareAndSwap(ctx, update, Payload{}, nil); err != nil {
			t.Fatal(err)
		}
		if head, err := s.DispatchHead(ctx, a); err != nil || head.DueMS != 0 {
			t.Fatal("settlement retained the physical index", head, err)
		}
	})
}

func TestDispatchControllerWorkReachesEveryLiveWorkerIndependently(t *testing.T) {
	backendContract(t, func(t *testing.T, s Backend) {
		ctx := context.Background()
		a, b, c := strings.Repeat("a", 64), strings.Repeat("b", 64), strings.Repeat("c", 64)
		for _, worker := range []string{a, b} {
			if err := s.RegisterDispatchWorker(ctx, worker); err != nil {
				t.Fatal(err)
			}
		}
		ref := contractRef(t, s, "controller-dispatch")
		update := contractRead(t, s, ref, contractInput("first", true))
		payload := Payload{Effects: []Effect{{ID: "lookup", Kind: "credential", Status: EffectPending, DeadlineMS: time.Now().Add(time.Minute).UnixMilli(), Worker: AnyDispatchWorker}}}
		if _, err := s.CompareAndSwap(ctx, update, payload, nil); err != nil {
			t.Fatal(err)
		}
		ref = update.Record.Ref
		stored, err := s.Load(ctx, ref)
		if err != nil || len(stored.Payload.Effects) != 1 || stored.Payload.Effects[0].Worker != AnyDispatchWorker {
			t.Fatal("retained codec lost the controller executor wildcard", err)
		}
		for _, worker := range []string{a, b} {
			if hints, err := s.DispatchHints(ctx, worker, 8); err != nil || len(hints) != 1 {
				t.Fatal("controller work did not reach an eligible worker", worker, hints, err)
			}
		}
		if err := s.AckDispatch(ctx, a, ref, "1"); err != nil {
			t.Fatal(err)
		}
		if hints, err := s.DispatchHints(ctx, b, 8); err != nil || len(hints) != 1 {
			t.Fatal("one process acknowledged another's controller work", hints, err)
		}
		if err := s.RegisterDispatchWorker(ctx, c); err != nil {
			t.Fatal(err)
		}
		update = contractRead(t, s, ref, contractInput("recovery", false))
		if _, err := s.CompareAndSwap(ctx, update, payload, nil); err != nil {
			t.Fatal(err)
		}
		if hints, err := s.DispatchHints(ctx, c, 8); err != nil || len(hints) != 1 {
			t.Fatal("committed recovery missed a newly registered worker", hints, err)
		}
	})
}
