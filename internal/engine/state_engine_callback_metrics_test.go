package engine

import (
	"testing"
	"time"

	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/state"
)

func TestCallbackStatisticsUseFixedLabelsWithoutStoreAccess(t *testing.T) {
	e := &StateEngine{cfg: Config{Process: &pb.ProcessRef{InstanceId: "private-process", Epoch: "private-epoch"}}}
	e.callbackMetrics.callback("present", callbackFresh, callbackAttempt)
	e.callbackMetrics.callback("present", callbackFresh, callbackResponse)
	e.callbackMetrics.callback("event", callbackContinuation, callbackResultError)
	e.callbackMetrics.callback("private-effect-id", callbackFresh, callbackAttempt)
	e.callbackMetrics.callback("present", callbackSourceCount, callbackAttempt)
	worker := e.dispatchWorker(e.cfg.Process)
	effects := []state.Effect{
		{Kind: "deliver", Status: state.EffectAuthorized, Worker: worker, DeadlineMS: time.Now().UnixMilli()},
		{Kind: "deliver", Status: state.EffectAuthorized, Worker: "private-remote-worker", DeadlineMS: time.Now().UnixMilli()},
		{Kind: "deliver", Status: state.EffectAuthorized, Worker: state.AnyDispatchWorker, DeadlineMS: time.Now().UnixMilli()},
	}
	e.dispatchableEffects(effects, time.Now().UnixMilli(), callbackFresh)
	callbacks, physical := e.CallbackStatistics() // No store is configured.
	if len(callbacks) != 3 || len(physical) != 3 {
		t.Fatalf("unexpected fixed counters: callbacks=%v physical=%v", callbacks, physical)
	}
	for _, item := range callbacks {
		if item.Count != 1 || item.Kind != "present" && item.Kind != "event" || item.Source != "fresh" && item.Source != "continuation" {
			t.Fatalf("unexpected callback labels/count: %+v", item)
		}
	}
	for _, item := range physical {
		if item.Count != 1 || item.Source != "fresh" || item.Locality != "local" && item.Locality != "remote" && item.Locality != "unknown" {
			t.Fatalf("unexpected physical labels/count: %+v", item)
		}
	}
}
