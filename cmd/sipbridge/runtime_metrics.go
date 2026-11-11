package main

import (
	"fmt"
	"io"
	"runtime"
	"runtime/metrics"
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
