package rediscore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestDriverDiagnosticRedactsAndClassifies(t *testing.T) {
	native := errors.New("native secret")
	cases := []struct {
		err      error
		category string
	}{
		{fmt.Errorf("secret %w", context.DeadlineExceeded), "timeout"},
		{fmt.Errorf("secret %w", ErrClock), "clock"},
		{fmt.Errorf("secret %w", ErrCorrupt), "corrupt"},
		{fmt.Errorf("secret %w", ErrAuthority), "authority"},
		{fmt.Errorf("secret %w", ErrExpired), "expired"},
		{fmt.Errorf("secret %w", ErrUnknown), "unknown"},
		{fmt.Errorf("secret %w", native), "native-record"},
		{errors.New("sensitive context key and term"), "other"},
	}
	for _, tc := range cases {
		var got DriverDiagnostic
		d := &Driver{cfg: DriverConfig{NativeRecordError: native, OnDiagnostic: func(value DriverDiagnostic) { got = value }}}
		d.diagnostic("renew", tc.err, true)
		encoded, err := json.Marshal(got)
		if err != nil || got.Stage != "renew" || got.Category != tc.category || strings.Contains(string(encoded), "secret") || strings.Contains(string(encoded), "context") {
			t.Fatalf("unsafe classification %s: %s, %v", tc.category, encoded, err)
		}
	}
}

func TestDriverDiagnosticRunsAfterFenceOutsideLock(t *testing.T) {
	wake := &Wake{connected: true, generation: 1, changed: make(chan struct{})}
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var d *Driver
	d = &Driver{cfg: DriverConfig{OnDiagnostic: func(event DriverDiagnostic) {
		if d.Ready() == nil || !event.WorkerFenced || event.Stage != "renew" || event.Category != "authority" {
			t.Error("observer saw active authority or wrong event")
		}
		close(entered)
		<-release
		panic("observer failure")
	}}}
	d.wake = wake
	if !d.publish(&Store{}, wake, time.Now().Add(time.Second), true) {
		t.Fatal("failed to publish")
	}
	_, lease, _, err := d.Current()
	if err != nil {
		t.Fatal(err)
	}
	go func() { defer close(done); d.fenceDiagnostic(wake, "renew", ErrAuthority) }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("observer did not enter")
	}
	if lease.Err() == nil || d.Ready() == nil {
		t.Fatal("observer retained authority")
	}
	close(release)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("observer panic blocked fencing")
	}
}

func TestDriverDiagnosticIgnoresRepeatedAndStaleWake(t *testing.T) {
	events := make(chan DriverDiagnostic, 3)
	d := &Driver{cfg: DriverConfig{OnDiagnostic: func(event DriverDiagnostic) { events <- event }}}
	first := &Wake{connected: true, generation: 1, changed: make(chan struct{})}
	d.wake = first
	if !d.publish(&Store{}, first, time.Now().Add(10*time.Second), true) {
		t.Fatal("first publish failed")
	}
	d.subscriptionFailed(context.Background(), first, ErrAuthority)
	d.subscriptionFailed(context.Background(), first, ErrAuthority)
	second := &Wake{connected: true, generation: 1, changed: make(chan struct{})}
	d.wake = second
	if !d.publish(&Store{}, second, time.Now().Add(10*time.Second), true) {
		t.Fatal("second publish failed")
	}
	defer d.fence()
	d.subscriptionFailed(context.Background(), first, ErrAuthority)
	if d.Ready() != nil {
		t.Fatal("stale wake fenced replacement")
	}
	if len(events) != 1 {
		t.Fatalf("expected one diagnostic, got %d", len(events))
	}
	event := <-events
	if event.Stage != "subscription" || event.Category != "authority" {
		t.Fatalf("unexpected diagnostic: %+v", event)
	}
}
