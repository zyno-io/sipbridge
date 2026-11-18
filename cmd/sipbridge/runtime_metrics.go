package main

import (
	"fmt"
	"io"
	"runtime"
	"runtime/metrics"
	"sipbridge/internal/engine"
	"sipbridge/internal/execution"
)

func writeRuntimeMetrics(w io.Writer) {
	fmt.Fprintf(w, "zt_sipbridge_runtime_gomaxprocs %d\nzt_sipbridge_runtime_goroutines %d\n", runtime.GOMAXPROCS(0), runtime.NumGoroutine())
	observations := [...]struct{ source, output string }{
		{"/cpu/classes/gc/total:cpu-seconds", "zt_sipbridge_runtime_gc_cpu_seconds_total"},
		{"/gc/heap/allocs:bytes", "zt_sipbridge_runtime_heap_alloc_bytes_total"},
		{"/gc/heap/allocs:objects", "zt_sipbridge_runtime_heap_alloc_objects_total"},
		{"/gc/heap/live:bytes", "zt_sipbridge_runtime_heap_live_bytes"},
		{"/gc/cycles/total:gc-cycles", "zt_sipbridge_runtime_gc_cycles_total"},
	}
	samples := make([]metrics.Sample, len(observations))
	for index, observation := range observations {
		samples[index].Name = observation.source
	}
	metrics.Read(samples)
	for index, sample := range samples {
		switch sample.Value.Kind() {
		case metrics.KindUint64:
			fmt.Fprintf(w, "%s %d\n", observations[index].output, sample.Value.Uint64())
		case metrics.KindFloat64:
			fmt.Fprintf(w, "%s %g\n", observations[index].output, sample.Value.Float64())
		}
	}
}

// Legacy statistic families are optional. A runtime without their collectors
// emits no synthetic values and performs no state scan for metrics.
func writeNativeMetrics(w io.Writer, runtime interface{}) {
	if source, ok := runtime.(interface {
		SchedulerStatistics() engine.SchedulerStatistics
	}); ok {
		scheduler := source.SchedulerStatistics()
		leader := 0
		if scheduler.Leader {
			leader = 1
		}
		fmt.Fprintf(w, "zt_sipbridge_scheduler_leader %d\nzt_sipbridge_scheduler_acquisitions_total %d\nzt_sipbridge_scheduler_head_reads_total %d\nzt_sipbridge_scheduler_hint_batches_total %d\nzt_sipbridge_scheduler_wakeups_total %d\nzt_sipbridge_scheduler_repair_pages_total %d\n", leader, scheduler.Acquisitions, scheduler.HeadReads, scheduler.HintBatches, scheduler.Wakeups, scheduler.RepairPages)
		for _, failure := range scheduler.Failures {
			fmt.Fprintf(w, "zt_sipbridge_scheduler_failures_total{stage=%q,class=%q} %d\n", failure.Stage, failure.Class, failure.Count)
		}
		fmt.Fprintf(w, "zt_sipbridge_worker_head_reads_total %d\nzt_sipbridge_worker_hint_batches_total %d\nzt_sipbridge_worker_wakeups_total %d\nzt_sipbridge_worker_owned_groups_total %d\n", scheduler.WorkerHeadReads, scheduler.WorkerBatches, scheduler.WorkerWakeups, scheduler.OwnedGroups)
	}
	if source, ok := runtime.(interface{ ExecutionStatistics() []execution.Statistic }); ok {
		for _, statistic := range source.ExecutionStatistics() {
			fmt.Fprintf(w, "zt_sipbridge_execution_total{input=%q,event=%q} %d\n", statistic.Kind, statistic.Event, statistic.Count)
		}
	}
	if source, ok := runtime.(interface {
		ResidentStatistics() engine.ResidentStatistics
	}); ok {
		statistics := source.ResidentStatistics()
		fmt.Fprintf(w, "zt_sipbridge_resident_contexts %d\nzt_sipbridge_resident_retained_bytes %d\nzt_sipbridge_resident_completion_reserved_bytes %d\n", statistics.Contexts, statistics.RetainedBytes, statistics.CompletionReservedBytes)
	}
	if source, ok := runtime.(interface{ OperationObservationStatistics() (uint64, uint64) }); ok {
		local, stored := source.OperationObservationStatistics()
		fmt.Fprintf(w, "zt_sipbridge_operation_observations_total{source=\"local\"} %d\nzt_sipbridge_operation_observations_total{source=\"store\"} %d\n", local, stored)
	}
	if source, ok := runtime.(interface {
		CallbackStatistics() ([]engine.CallbackStatistic, []engine.PhysicalLocalityStatistic)
	}); ok {
		callbacks, physical := source.CallbackStatistics()
		for _, statistic := range callbacks {
			fmt.Fprintf(w, "zt_sipbridge_callback_total{kind=%q,source=%q,event=%q} %d\n", statistic.Kind, statistic.Source, statistic.Event, statistic.Count)
		}
		for _, statistic := range physical {
			fmt.Fprintf(w, "zt_sipbridge_physical_candidate_total{source=%q,locality=%q} %d\n", statistic.Source, statistic.Locality, statistic.Count)
		}
	}
}
