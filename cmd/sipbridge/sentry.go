package main

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/getsentry/sentry-go"
	"sipbridge/internal/buildinfo"
	"sipbridge/internal/rediscore"
)

func initSentry() *sentry.Hub {
	dsn := strings.TrimSpace(os.Getenv("SENTRY_DSN"))
	if dsn == "" {
		return nil
	}
	client, err := sentry.NewClient(sentry.ClientOptions{
		Dsn:              dsn,
		Release:          "sipbridge@" + buildinfo.Version,
		AttachStacktrace: true,
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, "invalid SENTRY_DSN, disabling Sentry reporting")
		return nil
	}
	return sentry.NewHub(client, sentry.NewScope())
}

func startDriverIncidentReporter(hub *sentry.Hub) (func(rediscore.DriverDiagnostic), func()) {
	type queueEntry struct {
		diagnostic rediscore.DriverDiagnostic
		stop       bool
	}
	queue := make(chan queueEntry, 32)
	done := make(chan struct{})
	go func() {
		defer close(done)
		lastReport := make(map[string]time.Time, 2)
		for {
			entry := <-queue
			if entry.stop {
				return
			}
			d := entry.diagnostic
			if !d.WorkerFenced {
				continue
			}
			var eventName string
			switch d.Category {
			case "corrupt":
				eventName = "sipbridge.redis_worker_corrupt_state"
			case "native-record":
				eventName = "sipbridge.redis_worker_invalid_native_record"
			default:
				continue
			}
			now := time.Now()
			if now.Sub(lastReport[eventName]) < time.Minute {
				continue
			}
			lastReport[eventName] = now
			event := sentry.NewEvent()
			event.Level = sentry.LevelError
			event.Message = eventName
			event.Fingerprint = []string{eventName}
			event.Tags = map[string]string{"stage": d.Stage, "category": d.Category}
			hub.CaptureEvent(event)
		}
	}()
	enqueue := func(d rediscore.DriverDiagnostic) {
		select {
		case queue <- queueEntry{diagnostic: d}:
		default:
		}
	}
	stop := func() {
		queue <- queueEntry{stop: true}
		<-done
	}
	return enqueue, stop
}
