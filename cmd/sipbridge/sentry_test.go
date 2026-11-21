package main

import (
	"testing"
	"time"

	"github.com/getsentry/sentry-go"
	"sipbridge/internal/rediscore"
)

func TestDriverIncidentReporter(t *testing.T) {
	var captured []*sentry.Event
	capturing, release := make(chan struct{}), make(chan struct{})
	client, err := sentry.NewClient(sentry.ClientOptions{
		Dsn: "http://fixture@127.0.0.1/1",
		BeforeSend: func(event *sentry.Event, _ *sentry.EventHint) *sentry.Event {
			if len(captured) == 0 {
				close(capturing)
				<-release
			}
			captured = append(captured, event)
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	enqueue, stop := startDriverIncidentReporter(sentry.NewHub(client, sentry.NewScope()))
	finished := false
	defer func() {
		if !finished {
			close(release)
			stop()
		}
	}()
	enqueue(rediscore.DriverDiagnostic{Stage: "startup-refresh", Category: "corrupt"})
	for _, category := range []string{"expired", "authority", "timeout", "unknown", "other"} {
		enqueue(rediscore.DriverDiagnostic{Stage: "renew", Category: category, WorkerFenced: true})
	}
	corrupt := rediscore.DriverDiagnostic{Stage: "owner-head", Category: "corrupt", WorkerFenced: true}
	native := rediscore.DriverDiagnostic{Stage: "native-callback", Category: "native-record", WorkerFenced: true}
	enqueue(corrupt)
	select {
	case <-capturing:
	case <-time.After(time.Second):
		t.Fatal("background report did not start")
	}
	enqueued := make(chan struct{})
	go func() {
		defer close(enqueued)
		enqueue(native)
		enqueue(native)
		for i := 0; i < 100; i++ {
			enqueue(corrupt)
		}
	}()
	select {
	case <-enqueued:
	case <-time.After(time.Second):
		t.Fatal("enqueue waited for the reporter")
	}
	close(release)
	stop()
	finished = true
	if len(captured) != 2 {
		t.Fatalf("got %d reports, want 2", len(captured))
	}
	for i, expected := range []struct {
		name string
		d    rediscore.DriverDiagnostic
	}{
		{"sipbridge.redis_worker_corrupt_state", corrupt},
		{"sipbridge.redis_worker_invalid_native_record", native},
	} {
		event := captured[i]
		if event.Message != expected.name || event.Level != sentry.LevelError ||
			len(event.Fingerprint) != 1 || event.Fingerprint[0] != expected.name ||
			len(event.Tags) != 2 || event.Tags["stage"] != expected.d.Stage || event.Tags["category"] != expected.d.Category ||
			event.Request != nil || len(event.Exception) != 0 {
			t.Fatalf("unexpected report %d: %+v", i, event)
		}
	}
}
