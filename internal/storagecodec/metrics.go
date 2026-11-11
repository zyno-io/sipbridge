package storagecodec

import (
	"fmt"
	"io"
	"sync/atomic"
	"time"
)

type operationCounters struct {
	calls, bytes, elapsed, failures atomic.Uint64
}

var codecCounters [9][2]operationCounters
var kindNames = [9]string{"invalid", "payload", "group", "input", "effect", "digest", "network_target", "policy", "disposition"}

func observe(kind byte, encode bool, started time.Time, size int, err error) {
	if int(kind) >= len(codecCounters) {
		kind = 0
	}
	operation := 0
	if encode {
		operation = 1
	}
	counters := &codecCounters[kind][operation]
	counters.calls.Add(1)
	counters.bytes.Add(uint64(size))
	counters.elapsed.Add(uint64(time.Since(started)))
	if err != nil {
		counters.failures.Add(1)
	}
}

// WriteMetrics exports fixed kind/operation labels only. No SIP identity, wire,
// credentials or extra store reads participate in metrics collection.
func WriteMetrics(writer io.Writer) {
	for kind := range codecCounters {
		for operation, name := range []string{"decode", "encode"} {
			counters := &codecCounters[kind][operation]
			labels := fmt.Sprintf("kind=%q,operation=%q", kindNames[kind], name)
			fmt.Fprintf(writer, "sipbridge_state_codec_operations_total{%s} %d\n", labels, counters.calls.Load())
			fmt.Fprintf(writer, "sipbridge_state_codec_bytes_total{%s} %d\n", labels, counters.bytes.Load())
			fmt.Fprintf(writer, "sipbridge_state_codec_seconds_total{%s} %.9f\n", labels, float64(counters.elapsed.Load())/float64(time.Second))
			fmt.Fprintf(writer, "sipbridge_state_codec_failures_total{%s} %d\n", labels, counters.failures.Load())
		}
	}
}
