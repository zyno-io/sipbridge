package engine

import (
	"context"
	"time"

	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/state"
)

func (e *StateEngine) dispatchWorker(process *pb.ProcessRef) string {
	if process == nil {
		return ""
	}
	return e.id("physical-dispatch-worker", process.InstanceId, process.Epoch)
}

func (e *StateEngine) schedulePhysical() {
	worker := e.dispatchWorker(e.cfg.Process)
	for e.ctx.Err() == nil {
		e.physicalDispatchSession(worker)
		timer := time.NewTimer(100 * time.Millisecond)
		select {
		case <-e.ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

func (e *StateEngine) physicalDispatchSession(worker string) {
	ctx, cancel := context.WithCancel(e.ctx)
	defer cancel()
	readHead := func() (state.ScheduleHead, error) {
		e.scheduler.workerHeads.Add(1)
		read, stop := context.WithTimeout(ctx, 100*time.Millisecond)
		defer stop()
		return e.store.DispatchHead(read, worker)
	}
	head, err := readHead()
	if err != nil {
		return
	}
	registration, stop := context.WithTimeout(ctx, 100*time.Millisecond)
	err = e.store.RegisterDispatchWorker(registration, worker)
	stop()
	if err != nil {
		return
	}
	wake, failed := make(chan struct{}, 1), make(chan struct{}, 1)
	done := make(chan struct{})
	go func(cursor string) {
		defer close(done)
		for ctx.Err() == nil {
			next, err := e.store.WaitDispatch(ctx, worker, cursor)
			if err != nil {
				select {
				case failed <- struct{}{}:
				default:
				}
				return
			}
			if next != cursor {
				cursor = next
				select {
				case wake <- struct{}{}:
				default:
				}
			}
		}
	}(head.Cursor)
	defer func() { cancel(); <-done }()
	timer := time.NewTimer(time.Hour)
	defer timer.Stop()
	register := time.NewTicker(500 * time.Millisecond)
	defer register.Stop()
	var nextPass time.Time
	arm := func() {
		timer.Stop()
		if head.DueMS != 0 {
			timer.Reset(max(head.After, time.Until(nextPass), 0))
		}
	}
	arm()
	for ctx.Err() == nil {
		select {
		case <-ctx.Done():
			return
		case <-failed:
			return
		case <-register.C:
			registration, stop := context.WithTimeout(ctx, 100*time.Millisecond)
			err := e.store.RegisterDispatchWorker(registration, worker)
			stop()
			if err != nil {
				return
			}
			continue
		case <-wake:
			e.scheduler.workerWakes.Add(1)
		case <-timer.C:
			e.scheduler.workerBatches.Add(1)
			read, stop := context.WithTimeout(ctx, 100*time.Millisecond)
			hints, err := e.store.DispatchHints(read, worker, 8)
			stop()
			if err != nil {
				return
			}
			for _, hint := range hints {
				e.schedulePhysicalGroup(worker, hint.Ref)
			}
			nextPass = time.Now().Add(20 * time.Millisecond)
		}
		head, err = readHead()
		if err != nil {
			return
		}
		arm()
	}
}

func (e *StateEngine) schedulePhysicalGroup(worker string, ref state.Ref) {
	key := "physical:" + ref.ID
	e.lifecycleMu.Lock()
	if e.closed || e.physicalGroups[key] || len(e.physicalGroups) >= 16 {
		e.lifecycleMu.Unlock()
		return
	}
	select {
	case e.background <- struct{}{}:
	default:
		e.lifecycleMu.Unlock()
		return
	}
	select {
	case e.work <- struct{}{}:
	default:
		<-e.background
		e.lifecycleMu.Unlock()
		return
	}
	if e.physicalGroups == nil {
		e.physicalGroups = make(map[string]bool)
	}
	e.physicalGroups[key] = true
	e.wg.Add(1)
	e.lifecycleMu.Unlock()
	go func() {
		defer e.wg.Done()
		defer func() {
			<-e.work
			<-e.background
			e.lifecycleMu.Lock()
			delete(e.physicalGroups, key)
			e.lifecycleMu.Unlock()
		}()
		ctx, cancel := context.WithTimeout(e.ctx, 2*time.Second)
		defer cancel()
		view, now, err := e.store.ReadState(ctx, ref)
		if err != nil {
			return
		}
		current, revision := view.Ref, view.Revision
		if view.TimerWorker == worker {
			e.scheduler.ownedGroups.Add(1)
			plan := stateSchedulePlan{ref: current, pending: view.HasPending,
				retire: view.Payload.Retention.Terminal && view.Payload.ActiveTransactions == 0 && len(view.Payload.Effects) == 0 && len(view.Payload.Timers) == 0,
				timers: e.dueTimers(stateViewProtocolRecord(view), now), effects: e.dispatchableEffects(view.Payload.Effects, now, callbackIndex)}
			view = state.StateView{}
			e.runSchedulePlan(ctx, plan)
			_ = e.store.AckDispatch(ctx, worker, current, revision)
			return
		}
		var effects []state.Effect
		for _, effect := range e.dispatchableEffects(view.Payload.Effects, time.Now().UnixMilli(), callbackIndex) {
			if effect.Worker == worker || effect.Worker == state.AnyDispatchWorker {
				effects = append(effects, effect)
			}
		}
		view = state.StateView{} // No authoritative aggregate survives socket/RPC I/O.
		for _, effect := range effects {
			e.dispatchEffectWithin(ctx, current, effect, true)
		}
		_ = e.store.AckDispatch(ctx, worker, current, revision)
	}()
}
