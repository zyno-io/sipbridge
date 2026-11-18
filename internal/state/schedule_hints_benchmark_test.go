package state

import (
	"bytes"
	"context"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/google/uuid"
)

type hintCommandStat struct{ calls, micros int64 }

func hintCommandStats(b *testing.B, s *Store) map[string]hintCommandStat {
	b.Helper()
	info, err := s.client.Info(context.Background(), "commandstats").Result()
	if err != nil {
		b.Fatal(err)
	}
	stats := make(map[string]hintCommandStat)
	for _, line := range strings.Split(info, "\n") {
		name, encoded, ok := strings.Cut(strings.TrimSpace(line), ":")
		if !ok || !strings.HasPrefix(name, "cmdstat_") {
			continue
		}
		stat := hintCommandStat{}
		for _, field := range strings.Split(encoded, ",") {
			key, value, _ := strings.Cut(field, "=")
			number, _ := strconv.ParseInt(value, 10, 64)
			if key == "calls" {
				stat.calls = number
			} else if key == "usec" {
				stat.micros = number
			}
		}
		stats[strings.TrimPrefix(name, "cmdstat_")] = stat
	}
	return stats
}

// Run against an otherwise idle disposable Redis: commandstats include every
// client. One benchmark operation is the same eight-partition scheduler sweep.
func BenchmarkScheduleHints(b *testing.B) {
	address := os.Getenv("SIPBRIDGE_STATE_TEST_REDIS")
	if address == "" {
		b.Skip("set SIPBRIDGE_STATE_TEST_REDIS to an idle disposable Redis")
	}
	ctx := context.Background()
	s, err := Open(ctx, Config{Redis: RedisOptions{Address: address}, Prefix: "sipstate:hint-benchmark", Namespace: "bench-" + uuid.NewString(),
		NamespaceKey: bytes.Repeat([]byte{9}, 32), Generation: "generation", Versions: Versions{Schema: "1", Reducer: "1", Timers: "1"}, Partitions: 8})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		s.Close()
	})
	if err := s.Ready(ctx); err != nil {
		b.Fatal(err)
	}
	for _, batched := range []bool{false, true} {
		name := "partitions"
		if batched {
			name = "batch"
		}
		b.Run(name, func(b *testing.B) {
			poll := func() {
				count := 1
				if batched {
					count = int(s.PartitionCount())
				}
				for p := 0; p < int(s.PartitionCount()); p += count {
					batches, err := s.ScheduleHints(ctx, uint16(p), count, 8)
					if err != nil {
						b.Fatal(err)
					}
					for _, batch := range batches {
						if batch.ReadyError != nil || batch.DueError != nil {
							b.Fatalf("hint poll failed: %v / %v", batch.ReadyError, batch.DueError)
						}
					}
				}
			}
			b.StopTimer()
			poll() // Exclude script-cache initialization and its EVAL fallback.
			before := hintCommandStats(b, s)
			b.ReportAllocs()
			b.ResetTimer()
			b.StartTimer()
			for n := 0; n < b.N; n++ {
				poll()
			}
			b.StopTimer()
			after := hintCommandStats(b, s)
			b.ReportMetric(float64(after["evalsha"].calls-before["evalsha"].calls)/float64(b.N), "scripts/op")
			// The second INFO commandstats observation counts the first one.
			b.ReportMetric(float64(after["info"].calls-before["info"].calls-1)/float64(b.N), "INFO/op")
			b.ReportMetric(float64(after["zrangebyscore"].calls-before["zrangebyscore"].calls)/float64(b.N), "ZRANGEBYSCORE/op")
			b.ReportMetric(float64(after["evalsha"].micros-before["evalsha"].micros)*1000/float64(b.N), "Redis-script-ns/op")
		})
	}
}
